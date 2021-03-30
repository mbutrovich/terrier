#pragma once

#if __APPLE__
// nothing to include since it doesn't support perf events
#else
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#endif
#include <unistd.h>

#include <array>
#include <cstring>

#include "common/macros.h"

namespace noisepage::common {
/**
 * Wrapper around hw perf events provided by the Linux kernel. Instantiating and destroying PerfMonitors are a bit
 * expensive because they open multiple file descriptors (read: syscalls). Ideally you want to keep a PerfMonitor object
 * around for a portion of code you want to profile, and then just rely on Start() and Stop().
 * @tparam inherit true means that any threads spawned from this thread after the perf counter instantiation will be
 * accumulated into the parents' counters. This has performance implications. false otherwise (only count this thread's
 * counters, regardless of spawned threads)
 */
template <bool inherit>
class PerfMonitor {
 public:
  /**
   * Represents the struct read_format with PERF_FORMAT_GROUP enabled, PERF_FORMAT_TOTAL_TIME_ENABLED and
   * PERF_FORMAT_TOTAL_TIME_RUNNING disabled. http://www.man7.org/linux/man-pages/man2/perf_event_open.2.html
   */
  struct PerfCounters {
    /**
     * Should always be NUM_HW_EVENTS after a read since that's how many counters we have.
     */
    uint64_t num_counters_;

    /**
     * Total cycles. Be wary of what happens during CPU frequency scaling.
     */
    uint64_t cpu_cycles_;
    /**
     * Retired instructions. Be careful, these can be affected by various issues, most notably hardware interrupt
     * counts.
     */
    uint64_t instructions_;
    /**
     * Cache accesses. Usually this indicates Last Level Cache accesses but this may vary depending on your CPU.  This
     * may include prefetches and coherency messages; again this depends on the design of your CPU.
     */
    uint64_t cache_references_;
    /**
     * Cache misses.  Usually this indicates Last Level Cache misses.
     */
    uint64_t cache_misses_;
    // TODO(Matt): there seems to be a bug with enabling these counters along with the cache counters. When enabled,
    // just get 0s out of all of the counters. Eventually we might want them but can't enable them right now.
    // https://lkml.org/lkml/2018/2/13/810
    // uint64_t branch_instructions_;
    // uint64_t branch_misses_;
    /**
     * Bus cycles, which can be different from total cycles.
     *
     * TODO(wz2): Dated Nov 5th, 2020 on dev4. Recording {cycle,instr,cache-ref,cache-miss,bus,ref-cpu} causes all
     * the counters to get zeroed. This counter currently isn't exposed to the rest of the system so it is disabled
     * pending further investigation. Possibly might be related to limited intel performance counters per core.
     */
    // uint64_t bus_cycles_;
    /**
     * Total cycles; not affected by CPU frequency scaling.
     */
    uint64_t ref_cpu_cycles_;

    /**
     * compound assignment
     * @param rhs you know subtraction? this is the right side of that binary operator
     * @return reference to this
     */
    PerfCounters &operator-=(const PerfCounters &rhs) {
      this->cpu_cycles_ -= rhs.cpu_cycles_;
      this->instructions_ -= rhs.instructions_;
      this->cache_references_ -= rhs.cache_references_;
      this->cache_misses_ -= rhs.cache_misses_;
      // this->branch_instructions_ -= rhs.branch_instructions_;
      // this->branch_misses_ -= rhs.branch_misses_;
      // this->bus_cycles_ -= rhs.bus_cycles_;
      this->ref_cpu_cycles_ -= rhs.ref_cpu_cycles_;
      return *this;
    }

    /**
     * subtract implemented from compound assignment. passing lhs by value helps optimize chained a+b+c
     * @param lhs you know subtraction? this is the left side of that binary operator
     * @param rhs you know subtraction? this is the right side of that binary operator
     * @return
     */
    friend PerfCounters operator-(PerfCounters lhs, const PerfCounters &rhs) {
      lhs -= rhs;
      return lhs;
    }
  };

  /**
   * Create a perf monitor and open all of the necessary file descriptors.
   */
  PerfMonitor() { NOISEPAGE_ASSERT(false, "why is this here"); }

  ~PerfMonitor() { NOISEPAGE_ASSERT(false, "why is this here"); }

  DISALLOW_COPY_AND_MOVE(PerfMonitor)

  /**
   * Start monitoring perf counters
   */
  void Start() { NOISEPAGE_ASSERT(false, "why is this here"); }

  /**
   * Stop monitoring perf counters
   */
  void Stop() { NOISEPAGE_ASSERT(false, "why is this here"); }

  /**
   * Read out counters for the profiled period
   * @return struct representing the counters
   */
  PerfCounters Counters() const {
    NOISEPAGE_ASSERT(false, "why is this here");
    PerfCounters counters{};  // zero initialization
    return counters;
  }

  /**
   * Number of currently enabled HW perf events. Update this if more are added.
   */
  static constexpr uint8_t NUM_HW_EVENTS = 5;

 private:
  // set the first file descriptor to -1. Since event_files[0] is always passed into group_fd on
  // perf_event_open, this has the effect of making the first event the group leader. All subsequent syscalls can use
  // that fd if we are not inheriting child counters.
  std::array<int32_t, NUM_HW_EVENTS> event_files_{-1};
  bool valid_ = true;

#if __APPLE__
  // do nothing
#else

  bool running_ = false;
  static constexpr std::array<uint64_t, NUM_HW_EVENTS> HW_EVENTS{
      PERF_COUNT_HW_CPU_CYCLES, PERF_COUNT_HW_INSTRUCTIONS, PERF_COUNT_HW_CACHE_REFERENCES, PERF_COUNT_HW_CACHE_MISSES,
      PERF_COUNT_HW_REF_CPU_CYCLES};
#endif
};
}  // namespace noisepage::common
