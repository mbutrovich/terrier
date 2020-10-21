#include "execution/exec/execution_context.h"

#include "brain/operating_unit.h"
#include "brain/operating_unit_util.h"
#include "common/thread_context.h"
#include "execution/sql/value.h"
#include "folly/tracing/StaticTracepoint.h"
#include "metrics/metrics_store.h"
#include "parser/expression/constant_value_expression.h"

namespace terrier::execution::exec {

FOLLY_SDT_DEFINE_SEMAPHORE(, pipeline__done);

OutputBuffer *ExecutionContext::OutputBufferNew() {
  if (schema_ == nullptr) {
    return nullptr;
  }

  // Use C++ placement new
  auto size = sizeof(OutputBuffer);
  auto *buffer = reinterpret_cast<OutputBuffer *>(mem_pool_->Allocate(size));
  new (buffer) OutputBuffer(mem_pool_.get(), schema_->GetColumns().size(), ComputeTupleSize(schema_), callback_);
  return buffer;
}

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

void ExecutionContext::RegisterThreadWithMetricsManager() {
  if (terrier::common::thread_context.metrics_store_ == nullptr && GetMetricsManager()) {
    GetMetricsManager()->RegisterThread();
  }
}

void ExecutionContext::CheckTrackersStopped() {
  if (terrier::common::thread_context.metrics_store_ != nullptr &&
      terrier::common::thread_context.resource_tracker_.IsRunning()) {
    UNREACHABLE("Resource Trackers should have stopped");
  }
}

void ExecutionContext::AggregateMetricsThread() {
  if (GetMetricsManager()) {
    GetMetricsManager()->Aggregate();
  }
}

void ExecutionContext::StartResourceTracker(metrics::MetricsComponent component) { UNREACHABLE("this is unused?"); }

void ExecutionContext::EndResourceTracker(const char *name, uint32_t len) { UNREACHABLE("this is unused?"); }

void ExecutionContext::StartPipelineTracker(pipeline_id_t pipeline_id) {
  mem_tracker_->Reset();
  FOLLY_SDT(, pipeline__start);
}

#define MAX_FEATURES 8

struct features {
  const uint32_t query_id;
  const uint32_t pipeline_id;
  const uint8_t execution_mode;
  const uint8_t num_features;
  const uint64_t memory_bytes;
  uint8_t features[MAX_FEATURES];
  uint32_t est_output_rows[MAX_FEATURES];
  uint16_t key_sizes[MAX_FEATURES];
  uint8_t num_keys[MAX_FEATURES];
  uint32_t est_cardinalities[MAX_FEATURES];
  uint8_t mem_factor[MAX_FEATURES];
  uint8_t num_loops[MAX_FEATURES];
  uint8_t num_concurrent[MAX_FEATURES];
};

void ExecutionContext::EndPipelineTracker(const query_id_t query_id, const pipeline_id_t pipeline_id,
                                          brain::ExecOUFeatureVector *ouvec) {
  if (common::thread_context.metrics_store_ != nullptr && FOLLY_SDT_IS_ENABLED(, pipeline__done)) {
    const auto mem_size = memory_use_override_ ? memory_use_override_value_ : mem_tracker_->GetAllocatedSize();

    TERRIER_ASSERT(pipeline_id == ouvec->pipeline_id_, "Incorrect feature vector pipeline id?");
    brain::ExecutionOperatingUnitFeatureVector features(ouvec->pipeline_features_->begin(),
                                                        ouvec->pipeline_features_->end());

    struct features feats = {.query_id = static_cast<uint32_t>(query_id),
                             .pipeline_id = static_cast<uint32_t>(pipeline_id),
                             .execution_mode = execution_mode_,
                             .num_features = static_cast<uint8_t>(features.size()),
                             .memory_bytes = mem_size};

    for (uint8_t i = 0; i < feats.num_features; i++) {
      TERRIER_ASSERT(i < MAX_FEATURES, "Too many operators in this pipeline.");
      const auto &op_feature = features[i];
      feats.features[i] = static_cast<uint8_t>(op_feature.GetExecutionOperatingUnitType());
      feats.est_output_rows[i] = static_cast<uint32_t>(op_feature.GetNumRows());
      feats.key_sizes[i] = static_cast<uint16_t>(op_feature.GetKeySize());
      feats.num_keys[i] = static_cast<uint8_t>(op_feature.GetNumKeys());
      feats.est_cardinalities[i] = static_cast<uint32_t>(op_feature.GetCardinality());
      feats.mem_factor[i] = static_cast<uint8_t>(op_feature.GetMemFactor() * UINT8_MAX);
      feats.num_loops[i] = static_cast<uint8_t>(op_feature.GetNumLoops());
      feats.num_concurrent[i] = static_cast<uint8_t>(op_feature.GetNumConcurrent());
    }
    FOLLY_SDT_WITH_SEMAPHORE(, pipeline__done, &feats);
  }
}

void ExecutionContext::InitializeOUFeatureVector(brain::ExecOUFeatureVector *ouvec, pipeline_id_t pipeline_id) {
  auto *vec = new (ouvec) brain::ExecOUFeatureVector();
  vec->pipeline_id_ = pipeline_id;

  auto &features = pipeline_operating_units_->GetPipelineFeatures(pipeline_id);
  vec->pipeline_features_ = std::make_unique<execution::sql::MemPoolVector<brain::ExecutionOperatingUnitFeature>>(
      features.begin(), features.end(), GetMemoryPool());

  // Update num_concurrent
  for (auto &feature : *vec->pipeline_features_) {
    feature.SetNumConcurrent(num_concurrent_estimate_);
  }
}

void ExecutionContext::InitializeParallelOUFeatureVector(brain::ExecOUFeatureVector *ouvec, pipeline_id_t pipeline_id) {
  auto *vec = new (ouvec) brain::ExecOUFeatureVector();
  vec->pipeline_id_ = pipeline_id;
  vec->pipeline_features_ =
      std::make_unique<execution::sql::MemPoolVector<brain::ExecutionOperatingUnitFeature>>(GetMemoryPool());

  bool found_blocking = false;
  brain::ExecutionOperatingUnitFeature feature;
  auto features = pipeline_operating_units_->GetPipelineFeatures(pipeline_id);
  for (auto &feat : features) {
    if (brain::OperatingUnitUtil::IsOperatingUnitTypeBlocking(feat.GetExecutionOperatingUnitType())) {
      TERRIER_ASSERT(!found_blocking, "Pipeline should only have 1 blocking");
      found_blocking = true;
      feature = feat;
    }
  }

  if (!found_blocking) {
    TERRIER_ASSERT(false, "Pipeline should have 1 blocking");
    return;
  }

  switch (feature.GetExecutionOperatingUnitType()) {
    case brain::ExecutionOperatingUnitType::HASHJOIN_BUILD:
      vec->pipeline_features_->emplace_back(brain::ExecutionOperatingUnitType::PARALLEL_MERGE_HASHJOIN, feature);
      break;
    case brain::ExecutionOperatingUnitType::AGGREGATE_BUILD:
      vec->pipeline_features_->emplace_back(brain::ExecutionOperatingUnitType::PARALLEL_MERGE_AGGBUILD, feature);
      break;
    case brain::ExecutionOperatingUnitType::SORT_BUILD:
      vec->pipeline_features_->emplace_back(brain::ExecutionOperatingUnitType::PARALLEL_SORT_STEP, feature);
      vec->pipeline_features_->emplace_back(brain::ExecutionOperatingUnitType::PARALLEL_SORT_MERGE_STEP, feature);
      break;
    case brain::ExecutionOperatingUnitType::CREATE_INDEX:
      vec->pipeline_features_->emplace_back(brain::ExecutionOperatingUnitType::CREATE_INDEX_MAIN, feature);
      break;
    default:
      TERRIER_ASSERT(false, "Unsupported parallel OU");
  }

  // Update num_concurrent
  for (auto &feature : *vec->pipeline_features_) {
    feature.SetNumConcurrent(num_concurrent_estimate_);
  }
}

const parser::ConstantValueExpression &ExecutionContext::GetParam(const uint32_t param_idx) const {
  return (*params_)[param_idx];
}

void ExecutionContext::RegisterHook(size_t hook_idx, HookFn hook) {
  TERRIER_ASSERT(hook_idx < hooks_.capacity(), "Incorrect number of reserved hooks");
  hooks_[hook_idx] = hook;
}

void ExecutionContext::InvokeHook(size_t hook_index, void *tls, void *arg) {
  if (hook_index < hooks_.size() && hooks_[hook_index] != nullptr) {
    hooks_[hook_index](this->query_state_, tls, arg);
  }
}

void ExecutionContext::InitHooks(size_t num_hooks) { hooks_.resize(num_hooks); }

}  // namespace terrier::execution::exec
