#include "network/connection_handle.h"

#include "common/thread_context.h"
#include "folly/tracing/StaticTracepoint.h"
#include "loggers/network_logger.h"
#include "metrics/metrics_store.h"
#include "network/connection_dispatcher_task.h"
#include "network/connection_handle_factory.h"
#include "network/connection_handler_task.h"
#include "network/network_io_wrapper.h"

namespace noisepage::network {

FOLLY_SDT_DEFINE_SEMAPHORE(, network__features);

/** ConnectionHandleStateMachineTransition implements ConnectionHandle::StateMachine::Delta's transition function. */
class ConnectionHandleStateMachineTransition {
 public:
  // clang-format off
  /** Implement transition for ConnState::READ. */
  static ConnectionHandle::StateMachine::TransitionResult TransitionForRead(Transition transition) {
    switch (transition) {
      case Transition::NEED_READ:           return {ConnState::READ, WaitForRead};
      case Transition::NEED_READ_TIMEOUT:   return {ConnState::READ, WaitForReadWithTimeout};
      // Allegedly, the NEED_WRITE case happens only when we use SSL and are blocked on a write
      // during handshake. From noisepage's perspective we are still waiting for reads.
      case Transition::NEED_WRITE:          return {ConnState::READ, WaitForWrite};
      case Transition::PROCEED:             return {ConnState::PROCESS, Process};
      case Transition::TERMINATE:           return {ConnState::CLOSING, TryCloseConnection};
      case Transition::WAKEUP:              return {ConnState::READ, TryRead};
      default:                              throw std::runtime_error("Undefined transition!");
    }
  }

  /** Implement transition for ConnState::PROCESS. */
  static ConnectionHandle::StateMachine::TransitionResult TransitionForProcess(Transition transition) {
    switch (transition) {
      case Transition::NEED_READ:           return {ConnState::READ, TryRead};
      case Transition::NEED_READ_TIMEOUT:   return {ConnState::READ, WaitForReadWithTimeout};
      case Transition::NEED_RESULT:         return {ConnState::PROCESS, WaitForTerrier};
      case Transition::PROCEED:             return {ConnState::WRITE, TryWrite};
      case Transition::TERMINATE:           return {ConnState::CLOSING, TryCloseConnection};
      case Transition::WAKEUP:              return {ConnState::PROCESS, GetResult};
      default:                              throw std::runtime_error("Undefined transition!");
    }
  }

  /** Implement transition for ConnState::WRITE. */
  static ConnectionHandle::StateMachine::TransitionResult TransitionForWrite(Transition transition) {
    switch (transition) {
        // Allegedly, NEED_READ happens during ssl-rehandshake with the client.
      case Transition::NEED_READ:           return {ConnState::WRITE, WaitForRead};
      case Transition::NEED_WRITE:          return {ConnState::WRITE, WaitForWrite};
      case Transition::PROCEED:             return {ConnState::PROCESS, Process};
      case Transition::TERMINATE:           return {ConnState::CLOSING, TryCloseConnection};
      case Transition::WAKEUP:              return {ConnState::WRITE, TryWrite};
      default:                              throw std::runtime_error("Undefined transition!");
    }
  }

  /** Implement transition for ConnState::CLOSING. */
  static ConnectionHandle::StateMachine::TransitionResult TransitionForClosing(Transition transition) {
    switch (transition) {
      case Transition::NEED_READ:           return {ConnState::WRITE, WaitForRead};
      case Transition::NEED_WRITE:          return {ConnState::WRITE, WaitForWrite};
      case Transition::TERMINATE:           return {ConnState::CLOSING, TryCloseConnection};
      case Transition::WAKEUP:              return {ConnState::CLOSING, TryCloseConnection};
      default:                              throw std::runtime_error("Undefined transition!");
    }
  }
  // clang-format on

 private:
  /** Define a function (ManagedPointer<ConnectionHandle> -> Transition) that calls the underlying handle's function. */
#define DEF_HANDLE_WRAPPER_FN(function_name)                                               \
  static Transition function_name(const common::ManagedPointer<ConnectionHandle> handle) { \
    return handle->function_name();                                                        \
  }

  DEF_HANDLE_WRAPPER_FN(GetResult);
  DEF_HANDLE_WRAPPER_FN(Process);
  DEF_HANDLE_WRAPPER_FN(TryRead);
  DEF_HANDLE_WRAPPER_FN(TryWrite);
  DEF_HANDLE_WRAPPER_FN(TryCloseConnection);

#undef DEF_HANDLE_WRAPPER_FN

  /** Wait for the connection to become readable. */
  static Transition WaitForRead(const common::ManagedPointer<ConnectionHandle> handle) {
    handle->UpdateEventFlags(EV_READ | EV_PERSIST);
    return Transition::NONE;
  }

  /** Wait for the connection to become writable. */
  static Transition WaitForWrite(const common::ManagedPointer<ConnectionHandle> handle) {
    // Wait for the connection to become writable.
    handle->UpdateEventFlags(EV_WRITE | EV_PERSIST);
    return Transition::NONE;
  }

  /** Wait for the connection to become readable, or until a timeout happens. */
  static Transition WaitForReadWithTimeout(const common::ManagedPointer<ConnectionHandle> handle) {
    handle->UpdateEventFlags(EV_READ | EV_PERSIST | EV_TIMEOUT, READ_TIMEOUT);
    return Transition::NONE;
  }

  /** Stop listening to network events. This is used when control is completely ceded to Terrier, hence the name. */
  static Transition WaitForTerrier(const common::ManagedPointer<ConnectionHandle> handle) {
    handle->StopReceivingNetworkEvent();
    return Transition::NONE;
  }
};

ConnectionHandle::StateMachine::TransitionResult ConnectionHandle::StateMachine::Delta(ConnState state,
                                                                                       Transition transition) {
  // clang-format off
  switch (state) {
    case ConnState::READ:      return ConnectionHandleStateMachineTransition::TransitionForRead(transition);
    case ConnState::PROCESS:   return ConnectionHandleStateMachineTransition::TransitionForProcess(transition);
    case ConnState::WRITE:     return ConnectionHandleStateMachineTransition::TransitionForWrite(transition);
    case ConnState::CLOSING:   return ConnectionHandleStateMachineTransition::TransitionForClosing(transition);
    default:                   throw std::runtime_error("Undefined transition!");
  }
  // clang-format on
}

void ConnectionHandle::StateMachine::Accept(Transition action, const common::ManagedPointer<ConnectionHandle> handle) {
  Transition next = action;
  // Transition until there are no more transitions.
  while (next != Transition::NONE) {
    TransitionResult result = Delta(current_state_, next);
    current_state_ = result.first;
    try {
      next = result.second(handle);
    } catch (const NetworkProcessException &e) {
      // If an error occurs, the error is logged and the connection is terminated.
      NETWORK_LOG_ERROR("{0}\n", e.what());
      next = Transition::TERMINATE;
    }
  }
}

ConnectionHandle::ConnectionHandle(int sock_fd, common::ManagedPointer<ConnectionHandlerTask> task,
                                   common::ManagedPointer<trafficcop::TrafficCop> tcop,
                                   std::unique_ptr<ProtocolInterpreter> interpreter)
    : io_wrapper_(std::make_unique<NetworkIoWrapper>(sock_fd)),
      conn_handler_task_(task),
      traffic_cop_(tcop),
      protocol_interpreter_(std::move(interpreter)) {
  context_.SetCallback(Callback, this);
  context_.SetConnectionID(static_cast<connection_id_t>(sock_fd));
  //  std::cout << syscall(__NR_gettid) << std::endl;
}

ConnectionHandle::~ConnectionHandle() = default;

void ConnectionHandle::RegisterToReceiveEvents() {
  workpool_event_ = conn_handler_task_->RegisterManualEvent(
      [](int fd, int16_t flags, void *arg) { static_cast<ConnectionHandle *>(arg)->HandleEvent(fd, flags); }, this);

  network_event_ = conn_handler_task_->RegisterEvent(
      io_wrapper_->GetSocketFd(), EV_READ | EV_PERSIST,
      [](int fd, int16_t flags, void *arg) { static_cast<ConnectionHandle *>(arg)->HandleEvent(fd, flags); }, this);
}

void ConnectionHandle::HandleEvent(int fd, int16_t flags) {
  Transition t;
  if ((flags & EV_TIMEOUT) != 0) {
    // If the event was a timeout, this implies that the connection timed out. Terminate to disconnect.
    t = Transition::TERMINATE;
  } else {
    // Otherwise, something happened, so the state machine should wake up.
    t = Transition::WAKEUP;
  }
  state_machine_.Accept(t, common::ManagedPointer<ConnectionHandle>(this));
}

static void FlushNetworkFeatures(const common::ManagedPointer<network_features> features) {
  FOLLY_SDT_WITH_SEMAPHORE(, network__features, features.Get());
}

Transition ConnectionHandle::TryRead() {
  // TODO(Matt): there's an edge case here: what happens if it takes multiple TryRead's to build a packet? ie the query
  // data spans multiple packets. The network state machine is tricky

  // check first if we have data to flush
  if (flush_read_features_) {
    if (context_.read_features_.num_queries_ == 1) {
      // only flush metrics if we have a single query. Don't know how to model anything else right now.
      FlushNetworkFeatures(common::ManagedPointer(&context_.read_features_));
    }
    // reset read features
    flush_read_features_ = false;
    context_.read_features_ = {.operating_unit_ = NetworkOperatingUnit::READ};
  }

  // sample for a new data point
  const bool do_a_metrics_thing =
      common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::NETWORK) &&
      FOLLY_SDT_IS_ENABLED(, network__features);
  if (do_a_metrics_thing) {
    // perform read while profiling
    const auto socket_fd = io_wrapper_->GetSocketFd();
    FOLLY_SDT(, network__start, socket_fd);
    const auto read_transition = io_wrapper_->FillReadBuffer();
    FOLLY_SDT(, network__stop, socket_fd);
    context_.read_features_.bytes_ = io_wrapper_->GetReadBuffer()->Size();
    // flush at next opportunity
    flush_read_features_ = true;
    // return result of profiled op
    return read_transition;
  }
  return io_wrapper_->FillReadBuffer();
}

Transition ConnectionHandle::TryWrite() {
  if (io_wrapper_->ShouldFlush()) {
    // check first if we have data to flush
    if (flush_read_features_) {
      if (context_.read_features_.num_queries_ == 1) {
        // only flush metrics if we have a single query. Don't know how to model anything else right now.
        FlushNetworkFeatures(common::ManagedPointer(&context_.read_features_));
      }
      // reset read features
      flush_read_features_ = false;
      context_.read_features_ = {.operating_unit_ = NetworkOperatingUnit::READ};
    }

    // sample for a new data point
    const bool do_a_metrics_thing =
        common::thread_context.metrics_store_ != nullptr &&
        common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::NETWORK) &&
        FOLLY_SDT_IS_ENABLED(, network__features);
    if (do_a_metrics_thing) {
      // perform write while profiling
      context_.write_features_.bytes_ = io_wrapper_->GetWriteQueue()->Size();
      const auto socket_fd = io_wrapper_->GetSocketFd();
      FOLLY_SDT(, network__start, socket_fd);
      const auto write_transition = io_wrapper_->FlushAllWrites();
      FOLLY_SDT(, network__stop, socket_fd);
      if (context_.write_features_.num_queries_ == 1) {
        // only flush metrics if we have a single query. Don't know how to model anything else right now.
        FlushNetworkFeatures(common::ManagedPointer(&context_.write_features_));
      }
      context_.write_features_ = {.operating_unit_ = NetworkOperatingUnit::WRITE};
      // return result of profiled op
      return write_transition;
    }
    return io_wrapper_->FlushAllWrites();
  }
  return Transition::PROCEED;
}

Transition ConnectionHandle::Process() {
  auto transition = protocol_interpreter_->Process(io_wrapper_->GetReadBuffer(), io_wrapper_->GetWriteQueue(),
                                                   traffic_cop_, common::ManagedPointer(&context_));
  return transition;
}

Transition ConnectionHandle::GetResult() {
  // Wait until a network event happens.
  EventUtil::EventAdd(network_event_, EventUtil::WAIT_FOREVER);
  // TODO(WAN): It is not clear to me what this function is doing. If someone figures it out, please update comment.
  protocol_interpreter_->GetResult(io_wrapper_->GetWriteQueue());
  return Transition::PROCEED;
}

Transition ConnectionHandle::TryCloseConnection() {
  // Stop the protocol interpreter.
  protocol_interpreter_->Teardown(io_wrapper_->GetReadBuffer(), io_wrapper_->GetWriteQueue(), traffic_cop_,
                                  common::ManagedPointer(&context_));

  // Try to close the connection. If that fails, return whatever should have been done instead.
  // The connection must be closed before events are removed for safety reasons.
  Transition close = io_wrapper_->Close();
  if (close != Transition::PROCEED) {
    return close;
  }

  // Remove the network and worker pool events.
  conn_handler_task_->UnregisterEvent(network_event_);
  conn_handler_task_->UnregisterEvent(workpool_event_);

  return Transition::NONE;
}

void ConnectionHandle::UpdateEventFlags(int16_t flags, int timeout_secs) {
  // This callback function just calls HandleEvent().
  event_callback_fn handle_event = [](int fd, int16_t flags, void *arg) {
    static_cast<ConnectionHandle *>(arg)->HandleEvent(fd, flags);
  };

  // Update the flags for the event, casing on whether a timeout value needs to be specified.
  int conn_fd = io_wrapper_->GetSocketFd();
  if ((flags & EV_TIMEOUT) == 0) {
    // If there is no timeout specified, then the event will wait forever to be activated.
    conn_handler_task_->UpdateEvent(network_event_, conn_fd, flags, handle_event, this, EventUtil::WAIT_FOREVER);
  } else {
    // Otherwise if there is a timeout specified, then the event will fire once the timeout has passed.
    struct timeval timeout {
      timeout_secs, 0
    };
    conn_handler_task_->UpdateEvent(network_event_, conn_fd, flags, handle_event, this, &timeout);
  }
}

void ConnectionHandle::StopReceivingNetworkEvent() { EventUtil::EventDel(network_event_); }

void ConnectionHandle::Callback(void *callback_args) {
  // TODO(WAN): this is currently unused.
  auto *const handle = reinterpret_cast<ConnectionHandle *>(callback_args);
  NOISEPAGE_ASSERT(handle->state_machine_.CurrentState() == ConnState::PROCESS,
                   "Should be waking up a ConnectionHandle that's in PROCESS state waiting on query result.");
  event_active(handle->workpool_event_, EV_WRITE, 0);
}

void ConnectionHandle::ResetForReuse(connection_id_t connection_id, common::ManagedPointer<ConnectionHandlerTask> task,
                                     std::unique_ptr<ProtocolInterpreter> interpreter) {
  io_wrapper_->Restart();
  conn_handler_task_ = task;
  // TODO(WAN): the same traffic cop is kept because the ConnectionHandleFactory always uses the same traffic cop
  //  anyway, but if this ever changes then we'll need to revisit this.
  protocol_interpreter_ = std::move(interpreter);
  state_machine_ = ConnectionHandle::StateMachine();
  network_event_ = nullptr;
  workpool_event_ = nullptr;
  context_.Reset();
  context_.SetConnectionID(connection_id);
}

}  // namespace noisepage::network
