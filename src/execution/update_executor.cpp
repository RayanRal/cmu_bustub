//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"
#include "type/value_factory.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      table_info_{exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get()},
      child_executor_{std::move(child_executor)},
      is_finished_{false} {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  child_rids_.clear();
  child_rid_idx_ = 0;
  is_finished_ = false;

  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;
  while (child_executor_->Next(&child_tuples, &child_rids, 10)) {
    child_rids_.insert(child_rids_.end(), child_rids.begin(), child_rids.end());
    child_tuples.clear();
    child_rids.clear();
  }
}

auto UpdateExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  if (is_finished_) {
    return false;
  }

  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();

  int32_t count = 0;

  for (const auto &rid : child_rids_) {
    // Get the old tuple to evaluate expressions
    auto [meta, tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), rid);

    // Evaluate target expressions to produce new_tuple
    std::vector<Value> values;
    values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      values.emplace_back(expr->Evaluate(&tuple, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple(values, &table_info_->schema_);

    ModifyTuple(txn, txn_mgr, table_info_, rid, new_tuple, false);
    count++;
  }

  std::vector<Value> result_values;
  result_values.emplace_back(ValueFactory::GetIntegerValue(count));
  tuple_batch->emplace_back(Tuple(result_values, &GetOutputSchema()));

  is_finished_ = true;
  return true;
}

}  // namespace bustub
