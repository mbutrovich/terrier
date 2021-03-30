#pragma once

#include <fstream>

#include "common/perf_monitor.h"
#include "common/rusage_monitor.h"
#include "execution/util/cpu_info.h"
#include "metrics/metrics_util.h"

namespace noisepage::execution::exec {
class ExecutionContext;
}

namespace noisepage::common {

/**
 * Track the time and hardware resources spent for a particular event (operating unit). This is tracking resources at a
 * thread-local level, but in theory this can be extended to track the system global resource usage.
 */
class ResourceTracker {
  static constexpr bool COUNT_CHILDREN_THREADS = false;

 public:
  /**
   * Store the start time, the duration, the perf counters and the rusage counters for the tracked event
   */
  struct Metrics {
    /** The start time of the tracked event (microseconds since the "epoch") */
    uint64_t start_;
    /** The elapsed time of the tracked event (microseconds) */
    uint64_t elapsed_us_;
    /** The perf counters of the tracked event */
    PerfMonitor<COUNT_CHILDREN_THREADS>::PerfCounters counters_;
    /** The rusage counters of the tracked event */
    rusage rusage_;
    /** The number of the CPU on which the thread is currently executing */
    int cpu_id_;
    /** The memory consumption (in bytes) */
    uint64_t memory_b_;

    /**
     * Writes the metrics out to ofstreams
     * @param outfile opened ofstream to write to
     */
    void ToCSV(std::ofstream &outfile) const {
      NOISEPAGE_ASSERT(false, "why is this here");
    }

    /** Column headers to emit when writing to CSV */
    static constexpr std::string_view COLUMNS = {
        "start_time, cpu_id, cpu_cycles, instructions, cache_ref, cache_miss, ref_cpu_cycles, "
        "block_read, block_write, memory_b, elapsed_us"};
  };

  /**
   * Start the timer and resource monitors
   */
  void Start() {NOISEPAGE_ASSERT(false, "why is this here");
  }

  /**
   * Stop the timer and resource monitors
   */
  void Stop() {NOISEPAGE_ASSERT(false, "why is this here");
  }

  /**
   * Get the tracking results
   * @return the resource metrics for the tracked event
   */
  const Metrics &GetMetrics() { return metrics_; }

  /**
   * @return whether the tracker is running
   */
  bool IsRunning() const { return running_; }

 private:
  friend class execution::exec::ExecutionContext;

  /**
   * Since we cannot directly obtained the per-thread memory allocation from the OS, and to avoid introducing
   * dependency of the metrics system deep into the execution engine, we currently rely on customized
   * memory tracking and set the memory consumption separately.
   * @param memory_b memory in bytes
   */
  void SetMemory(const size_t memory_b) { metrics_.memory_b_ = memory_b; }

  // The struct to hold all the tracked resource metrics
  Metrics metrics_;

  bool running_ = false;
};

}  // namespace noisepage::common
