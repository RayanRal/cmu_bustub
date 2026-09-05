//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  index_info_ = catalog->GetIndex(plan_->index_oid_);
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  if (tree_ == nullptr) {
    throw ExecutionException("Index scan requires a B+Tree index");
  }
}

void IndexScanExecutor::Init() {
  auto *txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    txn->AppendScanPredicate(plan_->table_oid_, plan_->filter_predicate_);
  }
  is_point_lookup_ = !plan_->pred_keys_.empty();
  is_range_scan_ = !is_point_lookup_ && !plan_->range_bounds_.empty();
  if (is_point_lookup_) {
    rids_.clear();
    rid_idx_ = 0;
    for (const auto &expr : plan_->pred_keys_) {
      Value val = expr->Evaluate(nullptr, plan_->OutputSchema());
      Tuple key_tuple({val}, index_info_->index_->GetKeySchema());
      tree_->ScanKey(key_tuple, &rids_, exec_ctx_->GetTransaction());
    }
  } else if (is_range_scan_) {
    // Materialize the RIDs upfront (like the point-lookup path above) instead of holding a live B+Tree iterator:
    // Update/Delete executors drain this scan in Init() and then mutate the same index in Next() while this scan
    // is still alive. A live iterator holds a read latch on a leaf page, so a later index write would deadlock.
    // Collecting RIDs here releases the iterator before returning. Exclusive endpoints are treated as inclusive
    // (a safe superset); the filter predicate rejects the extra tuples per row in Next().
    Schema *key_schema = index_info_->index_->GetKeySchema();
    std::vector<Value> low_vals;
    std::vector<Value> high_vals;
    low_vals.reserve(key_schema->GetColumnCount());
    high_vals.reserve(key_schema->GetColumnCount());
    for (uint32_t i = 0; i < key_schema->GetColumnCount(); ++i) {
      TypeId col_type = key_schema->GetColumn(i).GetType();
      if (i < plan_->range_bounds_.size() && plan_->range_bounds_[i].has_low_) {
        low_vals.push_back(plan_->range_bounds_[i].low_);
      } else {
        low_vals.push_back(Type::GetMinValue(col_type));
      }
      if (i < plan_->range_bounds_.size() && plan_->range_bounds_[i].has_high_) {
        high_vals.push_back(plan_->range_bounds_[i].high_);
      } else {
        high_vals.push_back(Type::GetMaxValue(col_type));
      }
    }
    Tuple low_tuple(low_vals, key_schema);
    IntegerKeyType_BTree low_key;
    low_key.SetFromKey(low_tuple);
    Tuple high_tuple(high_vals, key_schema);
    IntegerKeyType_BTree high_key;
    high_key.SetFromKey(high_tuple);
    IntegerComparatorType_BTree comparator(key_schema);
    rids_.clear();
    rid_idx_ = 0;
    auto iter = tree_->GetBeginIterator(low_key);
    while (!iter.IsEnd() && comparator((*iter).first, high_key) <= 0) {
      rids_.push_back((*iter).second);
      ++iter;
    }
  } else {
    iter_ = std::make_unique<BPlusTreeIndexIteratorForTwoIntegerColumn>(tree_->GetBeginIterator());
  }
}

auto IndexScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                             size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (tuple_batch->size() < batch_size) {
    RID rid;
    if (is_point_lookup_ || is_range_scan_) {
      if (rid_idx_ < rids_.size()) {
        rid = rids_[rid_idx_++];
      } else {
        break;
      }
    } else {
      if (iter_ && !iter_->IsEnd()) {
        rid = (*(*iter_)).second;
        ++(*iter_);
      } else {
        break;
      }
    }

    auto [meta, tuple, undo_link] =
        GetTupleAndUndoLink(exec_ctx_->GetTransactionManager(), table_info_->table_.get(), rid);
    auto undo_logs =
        CollectUndoLogs(rid, meta, tuple, undo_link, exec_ctx_->GetTransaction(), exec_ctx_->GetTransactionManager());
    if (undo_logs.has_value()) {
      auto reconstructed_tuple = ReconstructTuple(&table_info_->schema_, tuple, meta, *undo_logs);
      if (reconstructed_tuple.has_value()) {
        if (plan_->filter_predicate_ != nullptr) {
          auto value = plan_->filter_predicate_->Evaluate(&(*reconstructed_tuple), plan_->OutputSchema());
          if (!value.IsNull() && value.GetAs<bool>()) {
            tuple_batch->push_back(std::move(*reconstructed_tuple));
            rid_batch->push_back(rid);
          }
        } else {
          tuple_batch->push_back(std::move(*reconstructed_tuple));
          rid_batch->push_back(rid);
        }
      }
    }
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
