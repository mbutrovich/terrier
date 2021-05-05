#pragma once

#include <algorithm>
#include <chrono>  //NOLINT
#include <fstream>
#include <list>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/resource_tracker.h"
#include "metrics/abstract_metric.h"
#include "metrics/metrics_util.h"
#include "network/network_defs.h"
#include "transaction/transaction_defs.h"

namespace noisepage::metrics {

/**
 * Raw data object for holding stats collected for the garbage collection
 */
class NetworkMetricRawData : public AbstractRawData {
 public:
  void Aggregate(AbstractRawData *const other) override {
    auto other_db_metric = dynamic_cast<NetworkMetricRawData *>(other);
    if (!other_db_metric->network_data_.empty()) {
      network_data_.splice(network_data_.cend(), other_db_metric->network_data_);
    }
  }

  /**
   * @return the type of the metric this object is holding the data for
   */
  MetricsComponent GetMetricType() const override { return MetricsComponent::NETWORK; }

  /**
   * Writes the data out to ofstreams
   * @param outfiles vector of ofstreams to write to that have been opened by the MetricsManager
   */
  void ToCSV(std::vector<std::ofstream> *const outfiles) final {
    NOISEPAGE_ASSERT(outfiles->size() == FILES.size(), "Number of files passed to metric is wrong.");
    NOISEPAGE_ASSERT(std::count_if(outfiles->cbegin(), outfiles->cend(),
                                   [](const std::ofstream &outfile) { return !outfile.is_open(); }) == 0,
                     "Not all files are open.");

    auto &outfile = (*outfiles)[0];

    for (const auto &data : network_data_) {
      outfile << data.features_.query_id_ << ", " << static_cast<uint32_t>(data.features_.operating_unit_) << ", "
              << data.features_.num_columns_ << ", " << data.features_.num_tuples_ << ", ";
      data.resource_metrics_.ToCSV(outfile);
      outfile << std::endl;
    }
    network_data_.clear();
  }

  /**
   * Files to use for writing to CSV.
   */
  static constexpr std::array<std::string_view, 1> FILES = {"./network.csv"};
  /**
   * Columns to use for writing to CSV.
   * Note: This includes the columns for the input feature, but not the output (resource counters)
   */
  static constexpr std::array<std::string_view, 1> FEATURE_COLUMNS = {"query_id, op_unit, num_columns, num_tuples"};

 private:
  friend class NetworkMetric;

  void RecordNetworkData(const network::network_features &features,
                         const common::ResourceTracker::Metrics &resource_metrics) {
    network_data_.emplace_back(features, resource_metrics);
  }

  enum class NetworkOperatingUnit : uint8_t { INVALID = 0, READ = 1, WRITE = 2 };

  struct NetworkData {
    NetworkData(const network::network_features &features, const common::ResourceTracker::Metrics &resource_metrics)
        : features_(features), resource_metrics_(resource_metrics) {}
    const network::network_features features_;
    const common::ResourceTracker::Metrics resource_metrics_;
  };

  std::list<NetworkData> network_data_;
};

/**
 * Metrics for the garbage collection components of the system: currently deallocation and unlinking
 */
class NetworkMetric : public AbstractMetric<NetworkMetricRawData> {
 private:
  friend class MetricsStore;

  void RecordNetworkData(const network::network_features &features,
                         const common::ResourceTracker::Metrics &resource_metrics) {
    GetRawData()->RecordNetworkData(features, resource_metrics);
  }
};
}  // namespace noisepage::metrics
