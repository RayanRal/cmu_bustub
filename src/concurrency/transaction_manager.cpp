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

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  if (txn->GetIsolationLevel() != IsolationLevel::SERIALIZABLE || txn->GetWriteSets().empty()) {
    return true;
  }

  for (const auto &[table_oid, predicates] : txn->GetScanPredicates()) {
    auto table_info = catalog_->GetTable(table_oid);
    auto iter = table_info->table_->MakeIterator();
    while (!iter.IsEnd()) {
      RID rid = iter.GetRID();
      auto [meta, tuple, undo_link] = GetTupleAndUndoLink(this, table_info->table_.get(), rid);

      if (meta.ts_ > txn->GetReadTs() && meta.ts_ < TXN_START_ID) {
        bool current_matches = false;
        if (!meta.is_deleted_) {
          for (const auto &pred : predicates) {
            if (pred == nullptr || pred->Evaluate(&tuple, table_info->schema_).GetAs<bool>()) {
              current_matches = true;
              break;
            }
          }
        }

        bool old_matches = false;
        auto undo_logs = CollectUndoLogs(rid, meta, tuple, undo_link, txn, this);
        if (undo_logs.has_value()) {
          auto reconstructed_tuple = ReconstructTuple(&table_info->schema_, tuple, meta, *undo_logs);
          if (reconstructed_tuple.has_value()) {
            for (const auto &pred : predicates) {
              if (pred == nullptr || pred->Evaluate(&(*reconstructed_tuple), table_info->schema_).GetAs<bool>()) {
                old_matches = true;
                break;
              }
            }
          }
        }

        if (current_matches || old_matches) {
          return false;
        }
      }
      ++iter;
    }
  }

  return true;
}

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
      auto [meta, tuple, undo_link] = GetTupleAndUndoLink(this, table_info->table_.get(), rid);
      if (meta.ts_ == txn->GetTransactionId()) {
        auto check_func = [txn_id = txn->GetTransactionId()](const TupleMeta &meta, const Tuple &tuple, RID rid,
                                                             std::optional<UndoLink> undo_link) -> bool {
          return meta.ts_ == txn_id;
        };
        TupleMeta new_meta = meta;
        new_meta.ts_ = commit_ts;
        UpdateTupleAndUndoLink(this, rid, undo_link, table_info->table_.get(), txn, new_meta, tuple, check_func);
      }
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

  // Snapshot the write set under a short lock, then release the mutex before doing I/O.
  // Holding txn_map_mutex_ across restores would block Begin/GC/Commit for the whole abort.
  // (State is published as ABORTED up front, same as before; visibility is timestamp-based.)
  auto write_set = txn->write_set_;
  {
    std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
    txn->state_ = TransactionState::ABORTED;
    running_txns_.RemoveTxn(txn->read_ts_);
  }

  for (const auto &[table_oid, rids] : write_set) {
    auto table_info = catalog_->GetTable(table_oid);
    for (const auto &rid : rids) {
      auto check_func = [txn_id = txn->GetTransactionId()](const TupleMeta &meta, const Tuple &tuple, RID rid,
                                                           std::optional<UndoLink> undo_link) -> bool {
        return meta.ts_ == txn_id;
      };

      auto [meta, tuple, undo_link] = GetTupleAndUndoLink(this, table_info->table_.get(), rid);
      if (meta.ts_ != txn->GetTransactionId()) {
        continue;
      }

      if (undo_link.has_value() && undo_link->IsValid() && undo_link->prev_txn_ == txn->GetTransactionId()) {
        auto undo_log = txn->GetUndoLog(undo_link->prev_log_idx_);
        TupleMeta restored_meta = {undo_log.ts_, undo_log.is_deleted_};
        std::optional<UndoLink> prev_undo_link =
            undo_log.prev_version_.IsValid() ? std::make_optional(undo_log.prev_version_) : std::nullopt;

        Tuple restored_tuple;
        if (undo_log.is_deleted_) {
          restored_tuple = tuple;
        } else {
          std::vector<Value> values;
          uint32_t col_count = table_info->schema_.GetColumnCount();
          values.reserve(col_count);
          Schema log_schema = GetUndoLogSchema(&table_info->schema_, undo_log.modified_fields_);
          for (uint32_t i = 0, log_idx = 0; i < col_count; i++) {
            if (undo_log.modified_fields_[i]) {
              values.push_back(undo_log.tuple_.GetValue(&log_schema, log_idx++));
            } else {
              values.push_back(tuple.GetValue(&table_info->schema_, i));
            }
          }
          restored_tuple = Tuple(values, &table_info->schema_);
        }

        UpdateTupleAndUndoLink(this, rid, prev_undo_link, table_info->table_.get(), txn, restored_meta, restored_tuple,
                               check_func);
      } else {
        UpdateTupleAndUndoLink(this, rid, std::nullopt, table_info->table_.get(), txn, {0, true}, tuple, check_func);
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
        txn->GetTransactionState() == TransactionState::TAINTED || needed_txns.find(txn_id) != needed_txns.end()) {
      ++it;
    } else {
      it = txn_map_.erase(it);
    }
  }
}

}  // namespace bustub
