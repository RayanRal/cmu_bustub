//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() {
  auto *catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->GetTableOid());
  table_iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());

  auto *txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    txn->AppendScanPredicate(plan_->GetTableOid(), plan_->filter_predicate_);
  }
}

/**
 * Yield the next tuple batch from the seq scan.
 * @param[out] tuple_batch The next tuple batch produced by the scan
 * @param[out] rid_batch The next tuple RID batch produced by the scan
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                           size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());

  while (!table_iter_->IsEnd() && tuple_batch->size() < batch_size) {
    RID rid = table_iter_->GetRID();
    auto [meta, tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_info->table_.get(), rid);

    auto undo_logs = CollectUndoLogs(rid, meta, tuple, undo_link, txn, txn_mgr);
    if (undo_logs.has_value()) {
      auto reconstructed_tuple = ReconstructTuple(&table_info->schema_, tuple, meta, *undo_logs);
      if (reconstructed_tuple.has_value()) {
        if (plan_->filter_predicate_ == nullptr) {
          tuple_batch->push_back(std::move(*reconstructed_tuple));
          rid_batch->push_back(rid);
        } else {
          auto value = plan_->filter_predicate_->Evaluate(&(*reconstructed_tuple), plan_->OutputSchema());
          if (!value.IsNull() && value.GetAs<bool>()) {
            tuple_batch->push_back(std::move(*reconstructed_tuple));
            rid_batch->push_back(rid);
          }
        }
      }
    }
    ++(*table_iter_);
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
