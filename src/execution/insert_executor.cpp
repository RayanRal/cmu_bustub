//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/insert_executor.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the insert */
void InsertExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
  is_finished_ = false;
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows inserted into the table
 * @param[out] rid_batch The next tuple RID batch produced by the insert (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with the number of inserted rows produced only once.
 */
auto InsertExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  if (is_finished_) {
    return false;
  }

  int32_t count = 0;
  std::vector<Tuple> child_tuple_batch;
  std::vector<RID> child_rid_batch;

  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();

  while (child_executor_->Next(&child_tuple_batch, &child_rid_batch, batch_size)) {
    for (const auto &tuple : child_tuple_batch) {
      auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      RID rid;
      bool found_deleted_slot = false;

      // 1. Check if the tuple already exists in the primary key index
      for (auto &index_info : table_indexes) {
        if (index_info->is_primary_key_) {
          auto key =
              tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
          std::vector<RID> result;
          index_info->index_->ScanKey(key, &result, txn);
          if (!result.empty()) {
            rid = result[0];
            auto [meta, base_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), rid);

            if (IsWriteWriteConflict(meta.ts_, txn->GetReadTs(), txn->GetTransactionTempTs())) {
              txn->SetTainted();
              throw ExecutionException("Write-write conflict in insert");
            }

            if (!meta.is_deleted_) {
              txn->SetTainted();
              throw ExecutionException("Duplicate key violation");
            }
            // Found a deleted slot with the same primary key, we can reuse it
            found_deleted_slot = true;
            break;
          }
        }
      }

      if (found_deleted_slot) {
        // 2. Reuse the deleted slot
        ModifyTuple(txn, txn_mgr, table_info_, rid, tuple, false);
      } else {
        // 3. Normal insert into table heap
        std::optional<RID> new_rid =
            table_info_->table_->InsertTuple(TupleMeta{txn->GetTransactionTempTs(), false}, tuple);
        if (!new_rid.has_value()) {
          continue;
        }
        rid = *new_rid;
        txn->AppendWriteSet(table_info_->oid_, rid);

        // 4. Insert into indexes
        for (auto &index_info : table_indexes) {
          auto key =
              tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
          bool inserted = index_info->index_->InsertEntry(key, rid, exec_ctx_->GetTransaction());

          if (!inserted && index_info->is_primary_key_) {
            txn->SetTainted();
            throw ExecutionException("Duplicate key violation");
          }
        }
      }
      count++;
    }
    child_tuple_batch.clear();
    child_rid_batch.clear();
  }

  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(count));
  tuple_batch->emplace_back(Tuple(values, &GetOutputSchema()));

  is_finished_ = true;
  return true;
}

}  // namespace bustub
