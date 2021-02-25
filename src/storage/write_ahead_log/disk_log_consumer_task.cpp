#include "storage/write_ahead_log/disk_log_consumer_task.h"

#include <thread>  // NOLINT

#include "common/scoped_timer.h"
#include "common/thread_context.h"
#include "folly/tracing/StaticTracepoint.h"
#include "metrics/metrics_store.h"

namespace noisepage::storage {

FOLLY_SDT_DEFINE_SEMAPHORE(, disk_log_consumer__features);

struct disk_log_consumer_features {
  const uint64_t num_bytes;
  const uint64_t num_buffers;
  const uint64_t interval;
};

void DiskLogConsumerTask::RunTask() {
  run_task_ = true;
  DiskLogConsumerTaskLoop();
}

void DiskLogConsumerTask::Terminate() {
  // If the task hasn't run yet, yield the thread until it's started
  while (!run_task_) std::this_thread::yield();
  NOISEPAGE_ASSERT(run_task_, "Cant terminate a task that isnt running");
  // Signal to terminate and force a flush so task persists before LogManager closes buffers
  run_task_ = false;
  disk_log_writer_thread_cv_.notify_one();
}

void DiskLogConsumerTask::WriteBuffersToLogFile() {
  // Persist all the filled buffers to the disk
  SerializedLogs logs;
  while (!filled_buffer_queue_->Empty()) {
    // Dequeue filled buffers and flush them to disk, as well as storing commit callbacks
    filled_buffer_queue_->Dequeue(&logs);
    if (logs.first != nullptr) {
      // Need the nullptr check because read-only txns don't serialize any buffers, but generate callbacks to be invoked
      current_data_written_ += logs.first->FlushBuffer();
    }
    commit_callbacks_.insert(commit_callbacks_.end(), logs.second.begin(), logs.second.end());
    // Enqueue the flushed buffer to the empty buffer queue
    if (logs.first != nullptr) {
      // nullptr check for the same reason as above
      empty_buffer_queue_->Enqueue(logs.first);
    }
  }
}

uint64_t DiskLogConsumerTask::PersistLogFile() {
  if (current_data_written_ > 0) {
    // Force the buffers to be written to disk. Because all buffers log to the same file, it suffices to call persist on
    // any buffer.
    buffers_->front().Persist();
  }
  const auto num_buffers = commit_callbacks_.size();
  // Execute the callbacks for the transactions that have been persisted
  for (auto &callback : commit_callbacks_) callback.first(callback.second);
  commit_callbacks_.clear();
  return num_buffers;
}

void DiskLogConsumerTask::DiskLogConsumerTaskLoop() {
  // input for this operating unit
  uint64_t num_bytes = 0, num_buffers = 0;

  // Keeps track of how much data we've written to the log file since the last persist
  current_data_written_ = 0;
  // Initialize sleep period
  auto curr_sleep = persist_interval_;
  auto next_sleep = curr_sleep;
  const std::chrono::microseconds max_sleep = std::chrono::microseconds(10000);
  // Time since last log file persist
  auto last_persist = std::chrono::high_resolution_clock::now();
  // Disk log consumer task thread spins in this loop. When notified or periodically, we wake up and process serialized
  // buffers

  bool logging_metrics_enabled =
      common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::LOGGING) &&
      FOLLY_SDT_IS_ENABLED(, disk_log_consumer__features);

  do {
    if (logging_metrics_enabled && !metrics_running_) {
      FOLLY_SDT(, disk_log_consumer__start);
      metrics_running_ = true;
    }

    curr_sleep = next_sleep;
    {
      // Wait until we are told to flush buffers
      std::unique_lock<std::mutex> lock(persist_lock_);
      // Wake up the task thread if:
      // 1) The serializer thread has signalled to persist all non-empty buffers to disk
      // 2) There is a filled buffer to write to the disk
      // 3) LogManager has shut down the task
      // 4) Our persist interval timed out

      bool signaled = disk_log_writer_thread_cv_.wait_for(
          lock, curr_sleep, [&] { return force_flush_ || !filled_buffer_queue_->Empty() || !run_task_; });
      next_sleep = signaled ? persist_interval_ : curr_sleep * 2;
      next_sleep = std::min(next_sleep, max_sleep);
    }

    // Flush all the buffers to the log file
    WriteBuffersToLogFile();

    // We persist the log file if the following conditions are met
    // 1) The persist interval amount of time has passed since the last persist
    // 2) We have written more data since the last persist than the threshold
    // 3) We are signaled to persist
    // 4) We are shutting down this task
    bool timeout = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() -
                                                                         last_persist) > curr_sleep;

    if (timeout || current_data_written_ > persist_threshold_ || force_flush_ || !run_task_) {
      std::unique_lock<std::mutex> lock(persist_lock_);
      num_buffers = PersistLogFile();
      num_bytes = current_data_written_;
      // Reset meta data
      last_persist = std::chrono::high_resolution_clock::now();
      current_data_written_ = 0;
      force_flush_ = false;

      // Signal anyone who forced a persist that the persist has finished
      persist_cv_.notify_all();
    }

    if (metrics_running_ && num_buffers > 0) {
      FOLLY_SDT(, disk_log_consumer__stop);
      disk_log_consumer_features feats = {.num_bytes = num_bytes,
                                          .num_buffers = num_buffers,
                                          .interval = static_cast<uint64_t>(persist_interval_.count())};
      FOLLY_SDT_WITH_SEMAPHORE(, disk_log_consumer__features, &feats);
      num_bytes = num_buffers = 0;
      logging_metrics_enabled =
          common::thread_context.metrics_store_ != nullptr &&
          common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::LOGGING) &&
          FOLLY_SDT_IS_ENABLED(, disk_log_consumer__features);
      metrics_running_ = false;
    }
  } while (run_task_);
  // Be extra sure we processed everything
  WriteBuffersToLogFile();
  PersistLogFile();
}
}  // namespace noisepage::storage
