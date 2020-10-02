#include "execution/exec/execution_context.h"

#include "brain/operating_unit.h"
#include "common/thread_context.h"
#include "execution/sql/value.h"
#include "folly/tracing/StaticTracepoint.h"
#include "metrics/metrics_store.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier::execution::exec {

FOLLY_SDT_DEFINE_SEMAPHORE(, pipeline__start)
FOLLY_SDT_DEFINE_SEMAPHORE(, pipeline__done)

uint32_t ExecutionContext::ComputeTupleSize(const planner::OutputSchema *schema) {
  uint32_t tuple_size = 0;
  for (const auto &col : schema->GetColumns()) {
    auto alignment = sql::ValUtil::GetSqlAlignment(col.GetType());
    if (!common::MathUtil::IsAligned(tuple_size, alignment)) {
      tuple_size = static_cast<uint32_t>(common::MathUtil::AlignTo(tuple_size, alignment));
    }
    tuple_size += sql::ValUtil::GetSqlSize(col.GetType());
  }
  return tuple_size;
}

void ExecutionContext::StartResourceTracker(metrics::MetricsComponent component) {
  TERRIER_ASSERT(component == metrics::MetricsComponent::EXECUTION,
                 "StartResourceTracker() invoked with incorrect MetricsComponent");

  if (common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentToRecord(component)) {
    // start the operating unit resource tracker
    common::thread_context.resource_tracker_.Start();
    mem_tracker_->Reset();
  }
}

void ExecutionContext::EndResourceTracker(const char *name, uint32_t len) {
  if (common::thread_context.metrics_store_ != nullptr && common::thread_context.resource_tracker_.IsRunning()) {
    common::thread_context.resource_tracker_.Stop();
    common::thread_context.resource_tracker_.SetMemory(mem_tracker_->GetAllocatedSize());
    const auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
    common::thread_context.metrics_store_->RecordExecutionData(name, len, execution_mode_, resource_metrics);
  }
}

void ExecutionContext::StartPipelineTracker(pipeline_id_t pipeline_id) {
  // TODO(Matt): use the USDT semaphore

  mem_tracker_->Reset();
  // Save a copy of the pipeline's features as the features will be updated in-place later.
  TERRIER_ASSERT(pipeline_operating_units_ != nullptr, "PipelineOperatingUnits should not be null");
  current_pipeline_features_id_ = pipeline_id;
  current_pipeline_features_ = pipeline_operating_units_->GetPipelineFeatures(pipeline_id);
  FOLLY_SDT(, pipeline__start);
}

#define MAX_FEATURES 8

struct features {
  uint32_t query_id;
  uint32_t pipeline_id;
  uint8_t execution_mode;
  uint8_t num_features;
  uint8_t features[MAX_FEATURES];
  uint32_t est_output_rows[MAX_FEATURES];
  uint16_t key_sizes[MAX_FEATURES];
  uint8_t num_keys[MAX_FEATURES];
  uint8_t est_cardinalities[MAX_FEATURES];
  uint8_t mem_factor[MAX_FEATURES];
};

void ExecutionContext::EndPipelineTracker(query_id_t query_id, pipeline_id_t pipeline_id) {
  //  auto mem_size = mem_tracker_->GetAllocatedSize();
  //  if (memory_use_override_) {
  //    mem_size = memory_use_override_value_;
  //  }
  //
  //  common::thread_context.metrics_store_->RecordPipelineData(query_id, pipeline_id, execution_mode_,
  //                                                            std::move(current_pipeline_features_),
  //                                                            resource_metrics);
  if (FOLLY_SDT_IS_ENABLED(, pipeline__done)) {
    struct features feats = {.query_id = query_id.UnderlyingValue(),
                             .pipeline_id = pipeline_id.UnderlyingValue(),
                             .execution_mode = execution_mode_,
                             .num_features = static_cast<uint8_t>(current_pipeline_features_.size())};

    for (uint8_t i = 0; i < feats.num_features; i++) {
      TERRIER_ASSERT(i < MAX_FEATURES, "Too many operators in this pipeline.");
      const auto &op_feature = current_pipeline_features_[i];
      feats.features[i] = static_cast<uint8_t>(op_feature.GetExecutionOperatingUnitType());
      feats.est_output_rows[i] = op_feature.GetNumRows();
      feats.key_sizes[i] = op_feature.GetKeySize();
      feats.num_keys[i] = op_feature.GetNumKeys();
      feats.est_cardinalities[i] = op_feature.GetCardinality();
      feats.mem_factor[i] = op_feature.GetMemFactor();
    }

    FOLLY_SDT_WITH_SEMAPHORE(, pipeline__done, &feats);
  }

  current_pipeline_features_.clear();
}

void ExecutionContext::GetFeature(uint32_t *value, pipeline_id_t pipeline_id, feature_id_t feature_id,
                                  brain::ExecutionOperatingUnitFeatureAttribute feature_attribute) {
  if (common::thread_context.metrics_store_ != nullptr && common::thread_context.resource_tracker_.IsRunning()) {
    TERRIER_ASSERT(pipeline_id == current_pipeline_features_id_, "That's not the current pipeline.");
    auto &features = current_pipeline_features_;
    for (auto &feature : features) {
      if (feature_id == feature.GetFeatureId()) {
        uint64_t val;
        switch (feature_attribute) {
          case brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS: {
            val = feature.GetNumRows();
            break;
          }
          case brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY: {
            val = feature.GetCardinality();
            break;
          }
          default:
            UNREACHABLE("Invalid feature attribute.");
        }
        *value = val;
        break;
      }
    }
  }
}

void ExecutionContext::RecordFeature(pipeline_id_t pipeline_id, feature_id_t feature_id,
                                     brain::ExecutionOperatingUnitFeatureAttribute feature_attribute, uint32_t value) {
  constexpr metrics::MetricsComponent component = metrics::MetricsComponent::EXECUTION_PIPELINE;

  if (common::thread_context.metrics_store_ != nullptr &&
      common::thread_context.metrics_store_->ComponentEnabled(component)) {
    TERRIER_ASSERT(pipeline_id == current_pipeline_features_id_, "That's not the current pipeline.");

    UNUSED_ATTRIBUTE bool recorded = false;
    auto &features = current_pipeline_features_;
    for (auto &feature : features) {
      if (feature_id == feature.GetFeatureId()) {
        switch (feature_attribute) {
          case brain::ExecutionOperatingUnitFeatureAttribute::NUM_ROWS: {
            feature.SetNumRows(value);
            recorded = true;
            break;
          }
          case brain::ExecutionOperatingUnitFeatureAttribute::CARDINALITY: {
            feature.SetCardinality(value);
            recorded = true;
            break;
          }
          case brain::ExecutionOperatingUnitFeatureAttribute::NUM_LOOPS: {
            feature.SetNumLoops(value);
            recorded = true;
            break;
          }
          default:
            UNREACHABLE("Invalid feature attribute.");
        }
        break;
      }
    }
    TERRIER_ASSERT(recorded, "Nothing was recorded. OperatingUnitRecorder hacks are probably necessary.");
  }
}

const parser::ConstantValueExpression &ExecutionContext::GetParam(const uint32_t param_idx) const {
  return (*params_)[param_idx];
}

}  // namespace terrier::execution::exec
