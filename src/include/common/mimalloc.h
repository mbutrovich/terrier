#pragma once

#include <cstring>

// clang-format will mess with the order, which we don't want.
// clang-format off
#include "mimalloc/include/mimalloc.h"
#include "mimalloc/include/mimalloc-types.h"
// clang-format on

namespace noisepage::common {
class Mimalloc {
 public:
  Mimalloc() = delete;

  static void ResetThreadStats() {
    auto *const stats = &(mi_heap_get_default()->tld->stats);
    std::memset(stats, 0, sizeof(mi_stats_t));
  }

  static int64_t ThreadPeakCommitted() {
    const auto *const stats = &(mi_heap_get_default()->tld->stats);
    return stats->page_committed.peak;
  }
};
}  // namespace noisepage::common
