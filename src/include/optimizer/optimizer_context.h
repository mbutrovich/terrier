#pragma once

#include <unordered_map>
#include <utility>
#include <vector>

#include "common/settings.h"
#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/group_expression.h"
#include "optimizer/memo.h"
#include "optimizer/rule.h"
#include "optimizer/statistics/stats_storage.h"

namespace noisepage {

namespace transaction {
class TransactionContext;
}

namespace catalog {
class CatalogAccessor;
class Schema;
}  // namespace catalog

namespace planner {
class OutputSchema;
}

namespace optimizer {

class OptimizerTaskPool;
class RuleSet;

/**
 * OptimizerContext is a class containing pointers to various objects
 * that are required during the entire query optimization process.
 */
class OptimizerContext {
 public:
  /**
   * Constructor for OptimizerContext
   * @param cost_model Cost Model to be stored by OptimizerContext
   */
  explicit OptimizerContext(const common::ManagedPointer<AbstractCostModel> cost_model)
      : cost_model_(cost_model), task_pool_(nullptr) {}

  /**
   * Destructor
   *
   * Destroys cost_model and task_pool
   */
  ~OptimizerContext() {
    delete task_pool_;

    for (auto *ctx : track_list_) {
      delete ctx;
    }
  }

  /**
   * Gets the Memo
   * @returns Memo
   */
  Memo &GetMemo() { return memo_; }

  /**
   * Gets the RuleSet
   * @returns RuleSet
   */
  RuleSet &GetRuleSet() { return rule_set_; }

  /**
   * Gets the CatalogAccessor
   * @returns CatalogAccessor
   */
  catalog::CatalogAccessor *GetCatalogAccessor() { return accessor_; }

  /**
   * Gets the CTE Schema
   * @returns CTE Schema
   */
  catalog::Schema &GetCTESchema(const catalog::table_oid_t cte_oid) { return cte_schemas_.find(cte_oid)->second; }

  /**
   * Gets a vector of all temp oids of cte tables in the current query context
   * @return a vector of temporary table oids
   */
  std::vector<catalog::table_oid_t> GetCTETables() {
    std::vector<catalog::table_oid_t> keys;
    for (auto &it : cte_schemas_) {
      keys.push_back(it.first);
    }
    return keys;
  }

  /**
   * Gets the StatsStorage
   * @returns StatsStorage
   */
  StatsStorage *GetStatsStorage() { return stats_storage_; }

  /**
   * Gets the param list
   * @return list of param values
   */
  common::ManagedPointer<std::vector<parser::ConstantValueExpression>> GetParams() { return params_; }

  /**
   * Adds a OptimizationContext to the tracking list
   * @param ctx OptimizationContext to add to tracking
   */
  void AddOptimizationContext(OptimizationContext *ctx) { track_list_.push_back(ctx); }

  /**
   * Pushes a task to the task pool managed
   * @param task Task to push
   */
  void PushTask(OptimizerTask *task) { task_pool_->Push(task); }

  /**
   * Gets the cost model
   * @returns Cost Model
   */
  AbstractCostModel *GetCostModel() { return cost_model_.Get(); }

  /**
   * Gets the transaction
   * @returns transaction
   */
  transaction::TransactionContext *GetTxn() { return txn_; }

  /**
   * Sets the transaction
   * @param txn TransactionContext
   */
  void SetTxn(transaction::TransactionContext *txn) { txn_ = txn; }

  /**
   * Sets the CatalogAccessor
   * @param accessor CatalogAccessor
   */
  void SetCatalogAccessor(catalog::CatalogAccessor *accessor) { accessor_ = accessor; }

  /**
   * Sets the CTE Schema
   * @param table_id the temp table oid of the cte table we are setting a schema for
   * @param schema OutputSchema
   */
  void SetCTESchema(catalog::table_oid_t table_id, catalog::Schema schema) {
    cte_schemas_[table_id] = std::move(schema);
  }

  /**
   * Sets the StatsStorage
   * @param storage StatsStorage
   */
  void SetStatsStorage(StatsStorage *storage) { stats_storage_ = storage; }

  /**
   * Set the task pool tracked by the OptimizerContext.
   * Function passes ownership over task_pool
   * @param task_pool Pointer to OptimizerTaskPool
   */
  void SetTaskPool(OptimizerTaskPool *task_pool) {
    delete this->task_pool_;
    this->task_pool_ = task_pool;
  }

  /**
   * Set the param list
   * @param params list of param values
   */
  void SetParams(common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params) { params_ = params; }

  /**
   * Converts an AbstractOptimizerNode into a GroupExpression.
   * The GroupExpression is internal tracking that is focused on the concept
   * of groups rather than expressions/operators like AbstractOptimizerNode.
   *
   * Subtrees of the AbstractOptimizerNode are individually converted to
   * GroupExpressions and inserted into Memo, which allows for duplicate
   * detection. The root GroupExpression, however, is not automatically
   * inserted into Memo.
   *
   * @param node AbstractOptimizerNode to convert
   * @returns GroupExpression representing AbstractOptimizerNode
   */
  GroupExpression *MakeGroupExpression(common::ManagedPointer<AbstractOptimizerNode> node);

  /**
   * A group contains all logical/physically equivalent AbstractOptimizerNodes.
   * Function try to add an equivalent AbstractOptimizerNode by creating a new group.
   *
   * @param node AbstractOptimizerNode to add
   * @param gexpr Existing GroupExpression or new GroupExpression
   * @returns Whether the AbstractOptimizerNode has been added before
   */
  bool RecordOptimizerNodeIntoGroup(common::ManagedPointer<AbstractOptimizerNode> node, GroupExpression **gexpr) {
    return RecordOptimizerNodeIntoGroup(node, gexpr, UNDEFINED_GROUP);
  }

  /**
   * A group contains all logical/physically equivalent AbstractOptimizerNode.
   * Function adds a logically/physically equivalent AbstractOptimizerNode to the specified group.
   * This function is invoked by tasks which explore the plan search space
   * through rules (recording all "equivalent" expressions for cost/selection later).
   *
   * @param node AbstractOptimizerNode to record into the group
   * @param gexpr Existing GroupExpression or new GroupExpression
   * @param target_group ID of the Group that the AbstractOptimizerNode belongs to
   * @returns Whether the AbstractOptimizerNode has been added before
   */
  bool RecordOptimizerNodeIntoGroup(common::ManagedPointer<AbstractOptimizerNode> node, GroupExpression **gexpr,
                                    group_id_t target_group) {
    auto new_gexpr = MakeGroupExpression(node);
    auto ptr = memo_.InsertExpression(new_gexpr, target_group, false);
    NOISEPAGE_ASSERT(ptr, "Root of expr should not fail insertion");

    (*gexpr) = ptr;
    return (ptr == new_gexpr);
  }

  /**
   * Replaces the AbstractOptimizerNode in a given group.
   * This is used primarily for the rewrite stage of the Optimizer
   * (i.e predicate push-down, query unnesting)
   *
   * @param node AbstractOptimizerNode to store into the group
   * @param target_group ID of the Group to replace
   */
  void ReplaceRewriteExpression(common::ManagedPointer<AbstractOptimizerNode> node, group_id_t target_group) {
    memo_.EraseExpression(target_group);
    UNUSED_ATTRIBUTE auto ret = memo_.InsertExpression(MakeGroupExpression(node), target_group, false);
    NOISEPAGE_ASSERT(ret, "Root expr should always be inserted");
  }

  /**
   * Registers expr to be deleted on txn_ commit/abort
   * @param expr Expression to register
   */
  void RegisterExprWithTxn(parser::AbstractExpression *expr) {
    txn_->RegisterCommitAction([=]() { delete expr; });
    txn_->RegisterAbortAction([=]() { delete expr; });
  }

 private:
  Memo memo_;
  RuleSet rule_set_;
  common::ManagedPointer<AbstractCostModel> cost_model_;
  OptimizerTaskPool *task_pool_;
  catalog::CatalogAccessor *accessor_{};
  StatsStorage *stats_storage_{};
  transaction::TransactionContext *txn_{};
  std::vector<OptimizationContext *> track_list_;
  std::unordered_map<catalog::table_oid_t, catalog::Schema> cte_schemas_;
  common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params_;
};

}  // namespace optimizer
}  // namespace noisepage
