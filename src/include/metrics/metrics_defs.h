#pragma once

namespace noisepage::metrics {

/**
 * Metric types
 */
enum class MetricsComponent : uint8_t {
  LOGGING,
  TRANSACTION,
  GARBAGECOLLECTION,
  EXECUTION,
  EXECUTION_PIPELINE,
  BIND_COMMAND,
  EXECUTE_COMMAND,
  QUERY_TRACE,
  NETWORK,
};

constexpr uint8_t NUM_COMPONENTS = 9;

}  // namespace noisepage::metrics
