//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  auto txn_id = next_txn_id_++;
  auto txn = std::make_shared<Transaction>(txn_id, isolation_level);
  auto read_ts = last_commit_ts_.load();

  txn->read_ts_ = read_ts;

  txn_map_[txn_id] = txn;

  running_txns_.AddTxn(read_ts);

  return txn.get();
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  timestamp_t commit_ts = last_commit_ts_.load() + 1;

  for (const auto &[table_oid, rids] : txn->write_set_) {
    auto table_info = catalog_->GetTable(table_oid);
    for (const auto &rid : rids) {
      auto meta = table_info->table_->GetTupleMeta(rid);
      meta.ts_ = commit_ts;
      table_info->table_->UpdateTupleMeta(meta, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  txn->commit_ts_ = commit_ts;
  txn->state_ = TransactionState::COMMITTED;
  last_commit_ts_++;

  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running or tainted state");
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);

  // Revert changes
  for (const auto &[table_oid, rids] : txn->write_set_) {
    auto table_info = catalog_->GetTable(table_oid);
    for (const auto &rid : rids) {
      auto undo_link = GetUndoLink(rid);
      auto meta = table_info->table_->GetTupleMeta(rid);

      if (undo_link.has_value() && undo_link->IsValid() && undo_link->prev_txn_ == txn->GetTransactionId()) {
        // This transaction modified the tuple and has an undo log.
        auto undo_log = txn->GetUndoLog(undo_link->prev_log_idx_);

        // Reconstruct the original tuple data
        auto current_tuple = table_info->table_->GetTuple(rid).second;
        std::vector<Value> values;
        uint32_t col_count = table_info->schema_.GetColumnCount();
        values.reserve(col_count);

        // We need to fetch values from undo_log for modified fields, and keep current values for others.
        // Wait, current values in heap are the NEW (to be aborted) values.
        // But for fields NOT in modified_fields, they haven't been changed by this txn,
        // so they are still the original committed values.
        // So we just need to overwrite the modified fields with values from undo_log.

        for (uint32_t i = 0; i < col_count; i++) {
          if (undo_log.modified_fields_[i]) {
            // Restore from undo log
            // Need to find which index in undo_log.tuple_ corresponds to column i
            // We can construct a partial schema for the undo log tuple.
            // Or simpler: iterate modified_fields_ to build the schema.
            // Optimizing: let's build the partial schema once.
            std::vector<uint32_t> cols;
            for (uint32_t j = 0; j < col_count; j++) {
              if (undo_log.modified_fields_[j]) {
                cols.push_back(j);
              }
            }
            Schema log_schema = Schema::CopySchema(&table_info->schema_, cols);
            // Now find the index of 'i' in 'cols'
            uint32_t log_idx = 0;
            bool found = false;
            for(size_t k=0; k<cols.size(); k++) {
                if(cols[k] == i) {
                    log_idx = k;
                    found = true;
                    break;
                }
            }
            if(found) {
                values.push_back(undo_log.tuple_.GetValue(&log_schema, log_idx));
            } else {
                // Should not happen if modified_fields_[i] is true
                values.push_back(current_tuple.GetValue(&table_info->schema_, i)); 
            }
          } else {
            // Keep current value (it wasn't changed)
            values.push_back(current_tuple.GetValue(&table_info->schema_, i));
          }
        }
        Tuple restored_tuple(values, &table_info->schema_);

        // Restore metadata
        TupleMeta restored_meta = {undo_log.ts_, undo_log.is_deleted_};
        table_info->table_->UpdateTupleInPlace(restored_meta, restored_tuple, rid);

        // Restore undo link
        if (undo_log.prev_version_.IsValid()) {
            UpdateUndoLink(rid, undo_log.prev_version_);
        } else {
            // If previous version was invalid, it might mean this was the first modification to a fresh tuple?
            // No, if prev_version is invalid, it means there was no undo log before this one.
            // We should set the link to std::nullopt.
            UpdateUndoLink(rid, std::nullopt);
        }
      } else {
        // No undo log for this txn, but it's in the write set.
        // Check if it's a fresh insert by this txn.
        if (meta.ts_ == txn->GetTransactionId()) {
          // It's a fresh insert. Mark as deleted with ts=0.
          table_info->table_->UpdateTupleMeta({0, true}, rid);
          UpdateUndoLink(rid, std::nullopt);
        }
      }
    }
  }
}

void TransactionManager::GarbageCollection() {
  timestamp_t watermark = GetWatermark();
  std::unordered_set<txn_id_t> needed_txns;

  auto table_names = catalog_->GetTableNames();
  for (const auto &table_name : table_names) {
    auto table_info = catalog_->GetTable(table_name);
    auto iter = table_info->table_->MakeIterator();
    while (!iter.IsEnd()) {
      RID rid = iter.GetRID();
      auto [meta, tuple] = iter.GetTuple();
      auto undo_link = GetUndoLink(rid);
      if (undo_link.has_value() && undo_link->IsValid()) {
        if (meta.ts_ > watermark) {
          auto current_undo_link = *undo_link;
          while (current_undo_link.IsValid()) {
            needed_txns.insert(current_undo_link.prev_txn_);
            auto undo_log = GetUndoLog(current_undo_link);
            if (undo_log.ts_ <= watermark) {
              break;
            }
            current_undo_link = undo_log.prev_version_;
          }
        }
      }
      ++iter;
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  for (auto it = txn_map_.begin(); it != txn_map_.end();) {
    txn_id_t txn_id = it->first;
    auto txn = it->second;
    if (txn->GetTransactionState() == TransactionState::RUNNING ||
        txn->GetTransactionState() == TransactionState::TAINTED) {
      ++it;
    } else if (needed_txns.find(txn_id) != needed_txns.end()) {
      ++it;
    } else {
      it = txn_map_.erase(it);
    }
  }
}

}  // namespace bustub
