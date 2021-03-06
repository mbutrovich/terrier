#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "common/action_context.h"
#include "common/future.h"
#include "execution/exec_defs.h"
#include "metrics/query_trace_metric.h"
#include "self_driving/forecasting/workload_forecast.h"
#include "self_driving/planning/action/action_defs.h"
#include "self_driving/planning/mcts/monte_carlo_tree_search.h"

namespace noisepage::util {

/**
 * Utility class for recording forecast information to internal tables.
 */
class SelfDrivingRecordingUtil {
 public:
  /**
   * Records query metadata to internal database tables
   * @param qmetadata Query metadata to record
   * @param task_manager Task Manager to use for submitting jobs
   */
  static void RecordQueryMetadata(
      const std::unordered_map<execution::query_id_t, metrics::QueryTraceMetadata::QueryMetadata> &qmetadata,
      common::ManagedPointer<task::TaskManager> task_manager);

  /**
   * Record query parameters to internal database tables
   * @param timestamp_to_record Timestamp to record parameters at
   * @param params Parameters information to record
   * @param task_manager Task manager to use for submitting jobs
   * @param out_params Output map to update with parameter info
   */
  static void RecordQueryParameters(
      uint64_t timestamp_to_record,
      std::unordered_map<execution::query_id_t, common::ReservoirSampling<std::string>> *params,
      common::ManagedPointer<task::TaskManager> task_manager,
      std::unordered_map<execution::query_id_t, std::vector<std::string>> *out_params);

  /**
   * Record forecast clusters/query mapping to internal database tables
   * @param timestamp_to_record Timestamp to record mapping at
   * @param metadata Workload metadata
   * @param prediction Workload forecast to record
   * @param task_manager Task manager to use for submitting jobs
   */
  static void RecordForecastClusters(uint64_t timestamp_to_record, const selfdriving::WorkloadMetadata &metadata,
                                     const selfdriving::WorkloadForecastPrediction &prediction,
                                     common::ManagedPointer<task::TaskManager> task_manager);

  /**
   * Record forecast query frequency to internal database tables
   * @param timestamp_to_record Timestamp to record mapping at
   * @param metadata Workload metadata
   * @param prediction Workload forecast to record
   * @param task_manager Task manager to use for submitting jobs
   */
  static void RecordForecastQueryFrequencies(uint64_t timestamp_to_record,
                                             const selfdriving::WorkloadMetadata &metadata,
                                             const selfdriving::WorkloadForecastPrediction &prediction,
                                             common::ManagedPointer<task::TaskManager> task_manager);

  /**
   * Record chosen action to internal database tables
   * @param timestamp_to_record Timestamp at which action is applied
   * @param action_id Action identifier of the action
   * @param cost Cost estimation of applying the action
   * @param db_id Database OID that action is applied to
   * @param action_text SQL string representation of the action
   * @param task_manager Task Manager to use for submitting jobs
   */
  static void RecordAppliedAction(uint64_t timestamp_to_record, selfdriving::pilot::action_id_t action_id, double cost,
                                  catalog::db_oid_t db_id, const std::string &action_text,
                                  common::ManagedPointer<task::TaskManager> task_manager);

  /**
   * Record best actions to internal database tables
   * @param timestamp_to_record Timestamp at which action is applied
   * @param actions Actions to record to the database
   * @param task_manager Task Manager to use for submitting jobs
   */
  static void RecordBestActions(uint64_t timestamp_to_record,
                                const std::vector<std::vector<selfdriving::pilot::ActionTreeNode>> &actions,
                                common::ManagedPointer<task::TaskManager> task_manager);

  /**
   * Query string for inserting into noisepage_forecast_texts.
   * Query string used for recording metadata.
   * For parameters, see src/main/startup.sql
   */
  static constexpr char QUERY_TEXT_INSERT_STMT[] = "INSERT INTO noisepage_forecast_texts VALUES ($1, $2, $3, $4)";

  /**
   * Query string for inserting into noisepage_forecast_parameters.
   * Query string used for recording query parameters.
   * For parameters, see src/main/startup.sql
   */
  static constexpr char QUERY_PARAMETERS_INSERT_STMT[] =
      "INSERT INTO noisepage_forecast_parameters VALUES ($1, $2, $3)";

  /**
   * Query string for inserting into noisepage_forecast_clusters.
   * Query string used to record forecast clusters <-> query mappings
   * For parameters, see src/main/startup.sql
   */
  static constexpr char FORECAST_CLUSTERS_INSERT_STMT[] =
      "INSERT INTO noisepage_forecast_clusters VALUES ($1, $2, $3, $4)";

  /**
   * Query string for inserting into noisepage_forecast_forecasts.
   * Query string used to record forecast query frequencies
   * For parameters, see src/main/startup.sql
   */
  static constexpr char FORECAST_FORECASTS_INSERT_STMT[] =
      "INSERT INTO noisepage_forecast_forecasts VALUES ($1, $2, $3, $4)";

  /**
   * Query string for inserting into noisepage_applied_actions.
   * Query string used for recording the planned action.
   * For parameters, see src/main/startup.sql
   */
  static constexpr char APPLIED_ACTIONS_INSERT_STMT[] =
      "INSERT INTO noisepage_applied_actions VALUES ($1, $2, $3, $4, $5)";

  /**
   * Query string for inserting into noisepage_best_actions.
   * Query string used for recording good option choices.
   * For parameters, see src/main/startup.sql
   */
  static constexpr char BEST_ACTIONS_INSERT_STMT[] =
      "INSERT INTO noisepage_best_actions VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
};

}  // namespace noisepage::util
