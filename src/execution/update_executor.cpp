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

  struct UpdateInfo {
    RID rid_;
    Tuple old_tuple_;
    Tuple new_tuple_;
    bool pk_changed_;
  };
  std::vector<UpdateInfo> updates;

  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  IndexInfo *primary_key_index = nullptr;
  for (auto &index_info : table_indexes) {
    if (index_info->is_primary_key_) {
      primary_key_index = index_info.get();
      break;
    }
  }

  for (const auto &rid : child_rids_) {
    // Get the old tuple to evaluate expressions
    auto [meta, tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), rid);
    auto undo_logs = CollectUndoLogs(rid, meta, tuple, undo_link, txn, txn_mgr);
    if (!undo_logs.has_value()) {
      continue;
    }
    auto reconstructed_tuple = ReconstructTuple(&table_info_->schema_, tuple, meta, *undo_logs);
    if (!reconstructed_tuple.has_value()) {
      continue;
    }

    // Evaluate target expressions to produce new_tuple
    std::vector<Value> values;
    values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      values.emplace_back(expr->Evaluate(&(*reconstructed_tuple), child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple(values, &table_info_->schema_);

    // Check if Primary Key has changed
    bool pk_changed = false;
    if (primary_key_index != nullptr) {
      auto old_pk = reconstructed_tuple->KeyFromTuple(table_info_->schema_, primary_key_index->key_schema_,
                                                      primary_key_index->index_->GetKeyAttrs());
      auto new_pk = new_tuple.KeyFromTuple(table_info_->schema_, primary_key_index->key_schema_,
                                           primary_key_index->index_->GetKeyAttrs());
      if (!IsTupleContentEqual(old_pk, new_pk)) {
        pk_changed = true;
      }
    }
    updates.push_back({rid, *reconstructed_tuple, new_tuple, pk_changed});
    count++;
  }

  // Phase 1: Apply all in-place updates and the "Delete" part of PK updates
  for (auto &update : updates) {
    if (!update.pk_changed_) {
      ModifyTuple(txn, txn_mgr, table_info_, update.rid_, update.new_tuple_, false);
    } else {
      ModifyTuple(txn, txn_mgr, table_info_, update.rid_, Tuple{}, true);
    }
  }

  // Phase 2: Apply the "Insert" part of PK updates
  for (auto &update : updates) {
    if (update.pk_changed_) {
      BUSTUB_ASSERT(primary_key_index != nullptr, "Primary key index must exist if PK changed");
      RID new_rid;
      bool found_deleted_slot = false;
      auto new_pk = update.new_tuple_.KeyFromTuple(table_info_->schema_, primary_key_index->key_schema_,
                                                   primary_key_index->index_->GetKeyAttrs());

      std::vector<RID> result;
      primary_key_index->index_->ScanKey(new_pk, &result, txn);
      if (!result.empty()) {
        new_rid = result[0];
        auto [new_meta, new_base_tuple, new_undo_link] =
            GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), new_rid);

        if (IsWriteWriteConflict(new_meta.ts_, txn->GetReadTs(), txn->GetTransactionTempTs())) {
          txn->SetTainted();
          throw ExecutionException("Write-write conflict in update (insert part)");
        }

        if (!new_meta.is_deleted_) {
          txn->SetTainted();
          throw ExecutionException("Duplicate key violation in update");
        }
        found_deleted_slot = true;
      }

      if (found_deleted_slot) {
        ModifyTuple(txn, txn_mgr, table_info_, new_rid, update.new_tuple_, false);
      } else {
        std::optional<RID> opt_new_rid =
            table_info_->table_->InsertTuple(TupleMeta{txn->GetTransactionTempTs(), false}, update.new_tuple_);
        if (!opt_new_rid.has_value()) {
          throw ExecutionException("Failed to insert during update");
        }
        new_rid = *opt_new_rid;
        txn->AppendWriteSet(table_info_->oid_, new_rid);

        for (auto &index_info : table_indexes) {
          auto key = update.new_tuple_.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                    index_info->index_->GetKeyAttrs());
          bool inserted = index_info->index_->InsertEntry(key, new_rid, txn);
          if (!inserted && index_info->is_primary_key_) {
            txn->SetTainted();
            throw ExecutionException("Duplicate key violation in update (index insert)");
          }
        }
      }
    }
  }

  std::vector<Value> result_values;
  result_values.emplace_back(ValueFactory::GetIntegerValue(count));
  tuple_batch->emplace_back(Tuple(result_values, &GetOutputSchema()));

  is_finished_ = true;
  return true;
}

}  // namespace bustub
