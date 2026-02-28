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
  auto temp_ts = txn->GetTransactionTempTs();
  auto read_ts = txn->GetReadTs();

  int32_t count = 0;

  for (const auto &rid : child_rids_) {
    auto [meta, tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), rid);

    if (IsWriteWriteConflict(meta.ts_, read_ts, temp_ts)) {
      txn->SetTainted();
      throw ExecutionException("Write-write conflict in update");
    }

    // Evaluate target expressions to produce new_tuple
    std::vector<Value> values;
    values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      values.emplace_back(expr->Evaluate(&tuple, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple(values, &table_info_->schema_);

    if (meta.ts_ == temp_ts) {
      // Self-modification
      auto latest_undo_link = txn_mgr->GetUndoLink(rid);
      if (latest_undo_link.has_value() && latest_undo_link->IsValid()) {
        auto undo_log = txn_mgr->GetUndoLog(*latest_undo_link);
        if (latest_undo_link->prev_txn_ == temp_ts) {
          // Already has an undo log in this transaction.
          auto updated_log = GenerateUpdatedUndoLog(&table_info_->schema_, &tuple, &new_tuple, undo_log);
          txn->ModifyUndoLog(latest_undo_link->prev_log_idx_, updated_log);
        }
      }
      // Update table heap in-place
      table_info_->table_->UpdateTupleInPlace({temp_ts, false}, new_tuple, rid);
    } else {
      // First modification by this transaction
      auto new_log = GenerateNewUndoLog(&table_info_->schema_, &tuple, &new_tuple, meta.ts_, undo_link.value_or(UndoLink{}));
      auto log_link = txn->AppendUndoLog(new_log);

      TupleMeta new_meta = {temp_ts, false};
      UpdateTupleAndUndoLink(txn_mgr, rid, log_link, table_info_->table_.get(), txn, new_meta, new_tuple);
    }

    txn->AppendWriteSet(table_info_->oid_, rid);
    count++;
  }

  std::vector<Value> result_values;
  result_values.emplace_back(ValueFactory::GetIntegerValue(count));
  tuple_batch->emplace_back(Tuple(result_values, &GetOutputSchema()));

  is_finished_ = true;
  return true;
}

}  // namespace bustub
