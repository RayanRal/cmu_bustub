//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get()),
      is_finished_(false) {}

void DeleteExecutor::Init() {
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

auto DeleteExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
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
      throw ExecutionException("Write-write conflict in delete");
    }

    if (meta.ts_ == temp_ts) {
      // Self-modification
      auto latest_undo_link = txn_mgr->GetUndoLink(rid);
      if (latest_undo_link.has_value() && latest_undo_link->IsValid()) {
        auto undo_log = txn_mgr->GetUndoLog(*latest_undo_link);
        if (latest_undo_link->prev_txn_ == temp_ts) {
          // Already has an undo log in this transaction.
          auto updated_log = GenerateUpdatedUndoLog(&table_info_->schema_, &tuple, nullptr, undo_log);
          txn->ModifyUndoLog(latest_undo_link->prev_log_idx_, updated_log);
        }
      }
      // Update table heap (set as deleted). ALWAYS use temp_ts.
      TupleMeta new_meta = {temp_ts, true};
      table_info_->table_->UpdateTupleMeta(new_meta, rid);
    } else {
      // First modification by this transaction
      auto new_log = GenerateNewUndoLog(&table_info_->schema_, &tuple, nullptr, meta.ts_, undo_link.value_or(UndoLink{}));
      auto log_link = txn->AppendUndoLog(new_log);

      TupleMeta new_meta = {temp_ts, true};
      UpdateTupleAndUndoLink(txn_mgr, rid, log_link, table_info_->table_.get(), txn, new_meta, tuple);
    }

    txn->AppendWriteSet(table_info_->oid_, rid);
    count++;
  }

  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(count));
  tuple_batch->emplace_back(Tuple(values, &GetOutputSchema()));

  is_finished_ = true;
  return true;
}

}  // namespace bustub
