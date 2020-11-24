#include "main/db_main.h"

// "For best performance in C++ programs, it is also recommended to override the global new and delete operators. For
// convenience, mimalloc provides mimalloc-new-delete.h which does this for you – just include it in a single(!) source
// file in your project." https://microsoft.github.io/mimalloc/using.html
#ifdef NOISEPAGE_USE_MIMALLOC
#include "mimalloc/include/mimalloc-new-delete.h"
#endif

#define __SETTING_GFLAGS_DEFINE__    // NOLINT
#include "settings/settings_defs.h"  // NOLINT
#undef __SETTING_GFLAGS_DEFINE__     // NOLINT

#include "execution/execution_util.h"

namespace noisepage {

void DBMain::Run() {
  NOISEPAGE_ASSERT(network_layer_ != DISABLED, "Trying to run without a NetworkLayer.");
  const auto server = network_layer_->GetServer();
  try {
    server->RunServer();
  } catch (NetworkProcessException &e) {
    return;
  }
  {
    std::unique_lock<std::mutex> lock(server->RunningMutex());
    server->RunningCV().wait(lock, [=] { return !(server->Running()); });
  }
}

void DBMain::ForceShutdown() {
  if (network_layer_ != DISABLED && network_layer_->GetServer()->Running()) {
    network_layer_->GetServer()->StopServer();
  }
}

DBMain::~DBMain() { ForceShutdown(); }

DBMain::ExecutionLayer::ExecutionLayer() { execution::ExecutionUtil::InitTPL(); }

DBMain::ExecutionLayer::~ExecutionLayer() { execution::ExecutionUtil::ShutdownTPL(); }

}  // namespace noisepage
