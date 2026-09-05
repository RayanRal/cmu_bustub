//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  const auto &key_a = entry_a.first;
  const auto &key_b = entry_b.first;
  for (size_t i = 0; i < order_bys_.size(); ++i) {
    const auto &order_by = order_bys_[i];
    OrderByType type = std::get<0>(order_by);
    OrderByNullType null_type = std::get<1>(order_by);
    const auto &val_a = key_a[i];
    const auto &val_b = key_b[i];

    if (val_a.IsNull() && val_b.IsNull()) {
      continue;
    }

    bool is_asc = (type == OrderByType::ASC || type == OrderByType::DEFAULT);
    bool nulls_first = (null_type == OrderByNullType::NULLS_FIRST || (null_type == OrderByNullType::DEFAULT && is_asc));

    if (val_a.IsNull()) {
      return nulls_first;
    }
    if (val_b.IsNull()) {
      return !nulls_first;
    }

    if (val_a.CompareEquals(val_b) == CmpBool::CmpTrue) {
      continue;
    }

    if (is_asc) {
      return val_a.CompareLessThan(val_b) == CmpBool::CmpTrue;
    }
    return val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue;
  }
  return false;
}

/**
 * Generate sort key for a tuple based on the order by expressions.
 */
auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey key;
  key.reserve(order_bys.size());
  for (const auto &order_by : order_bys) {
    const auto &expr = std::get<2>(order_by);
    key.push_back(expr->Evaluate(&tuple, schema));
  }
  return key;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::vector<Value> values;
  uint32_t col_count = schema->GetColumnCount();
  values.reserve(col_count);

  bool is_deleted = base_meta.is_deleted_;

  for (uint32_t i = 0; i < col_count; ++i) {
    values.push_back(base_tuple.GetValue(schema, i));
  }

  // Partial-schema cache keyed by modified-column set (see below).
  std::vector<std::pair<std::vector<uint32_t>, Schema>> schema_cache;

  for (const auto &log : undo_logs) {
    is_deleted = log.is_deleted_;
    if (log.is_deleted_) {
      continue;
    }

    std::vector<uint32_t> modified_cols;
    for (uint32_t i = 0; i < col_count; ++i) {
      if (log.modified_fields_[i]) {
        modified_cols.push_back(i);
      }
    }

    if (!modified_cols.empty()) {
      // Long chains often repeat the same column image; reuse the partial schema instead of
      // reallocating per log. The reference is consumed immediately, so cache growth is safe.
      const Schema *partial_schema = nullptr;
      for (const auto &[cols, cached] : schema_cache) {
        if (cols == modified_cols) {
          partial_schema = &cached;
          break;
        }
      }
      if (partial_schema == nullptr) {
        schema_cache.emplace_back(modified_cols, Schema::CopySchema(schema, modified_cols));
        partial_schema = &schema_cache.back().second;
      }
      for (uint32_t i = 0; i < modified_cols.size(); ++i) {
        values[modified_cols[i]] = log.tuple_.GetValue(partial_schema, i);
      }
    }
  }

  if (is_deleted) {
    return std::nullopt;
  }

  return std::make_optional<Tuple>(values, schema);
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  timestamp_t read_ts = txn->GetReadTs();
  if (base_meta.ts_ <= read_ts || base_meta.ts_ == txn->GetTransactionTempTs()) {
    return std::make_optional(std::vector<UndoLog>{});
  }

  std::vector<UndoLog> logs;
  logs.reserve(4);
  std::optional<UndoLink> current_link = undo_link;
  while (current_link.has_value() && current_link->IsValid()) {
    // Single move per log (GetUndoLog returns by value); no extra copy.
    logs.push_back(txn_mgr->GetUndoLog(*current_link));
    if (logs.back().ts_ <= read_ts) {
      return std::make_optional(std::move(logs));
    }
    current_link = logs.back().prev_version_;
  }

  return std::nullopt;
}

auto IsWriteWriteConflict(timestamp_t base_ts, timestamp_t read_ts, timestamp_t temp_ts) -> bool {
  if (base_ts >= TXN_START_ID) {
    return base_ts != temp_ts;
  }
  return base_ts > read_ts;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  uint32_t col_count = schema->GetColumnCount();
  std::vector<bool> modified_fields(col_count, false);
  std::vector<uint32_t> modified_cols;
  std::vector<Value> values;

  // Whether the PREVIOUS version (base_tuple) was deleted.
  bool is_deleted = (base_tuple == nullptr);

  if (target_tuple == nullptr) {
    // Current operation is a DELETE. We need to store EVERYTHING from base_tuple.
    if (base_tuple != nullptr) {
      for (uint32_t i = 0; i < col_count; i++) {
        modified_fields[i] = true;
        modified_cols.push_back(i);
        values.push_back(base_tuple->GetValue(schema, i));
      }
    }
  } else {
    // Current operation is an UPDATE (or INSERT into deleted slot).
    if (base_tuple != nullptr) {
      for (uint32_t i = 0; i < col_count; i++) {
        auto base_val = base_tuple->GetValue(schema, i);
        auto target_val = target_tuple->GetValue(schema, i);
        if (!base_val.CompareExactlyEquals(target_val)) {
          modified_fields[i] = true;
          modified_cols.push_back(i);
          values.push_back(base_val);
        }
      }
    }
  }

  Schema log_schema = Schema::CopySchema(schema, modified_cols);
  return {is_deleted, modified_fields, Tuple(values, &log_schema), ts, prev_version};
}

auto GetUndoLogSchema(const Schema *base_schema, const std::vector<bool> &modified_fields) -> Schema {
  std::vector<uint32_t> modified_cols;
  for (uint32_t i = 0; i < modified_fields.size(); ++i) {
    if (modified_fields[i]) {
      modified_cols.push_back(i);
    }
  }
  return Schema::CopySchema(base_schema, modified_cols);
}

auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  uint32_t col_count = schema->GetColumnCount();
  std::vector<bool> modified_fields = log.modified_fields_;

  // existing_values stores the values from the committed version for already-modified fields.
  Schema old_log_schema = GetUndoLogSchema(schema, log.modified_fields_);
  std::vector<Value> existing_values;
  for (uint32_t i = 0, log_idx = 0; i < col_count; i++) {
    if (log.modified_fields_[i]) {
      existing_values.push_back(log.tuple_.GetValue(&old_log_schema, log_idx++));
    }
  }

  if (target_tuple == nullptr) {
    // Current operation is DELETE.
    // We need to ensure ALL fields are in modified_fields_.
    for (uint32_t i = 0; i < col_count; i++) {
      modified_fields[i] = true;
    }
  } else {
    // Current operation is UPDATE.
    if (base_tuple != nullptr) {
      for (uint32_t i = 0; i < col_count; i++) {
        auto base_val = base_tuple->GetValue(schema, i);
        auto target_val = target_tuple->GetValue(schema, i);
        if (!base_val.CompareExactlyEquals(target_val)) {
          modified_fields[i] = true;
        }
      }
    }
  }

  // Re-construct the full partial tuple from original committed values.
  std::vector<uint32_t> new_modified_cols;
  std::vector<Value> new_values;
  for (uint32_t i = 0, old_idx = 0; i < col_count; i++) {
    if (modified_fields[i]) {
      new_modified_cols.push_back(i);
      if (log.modified_fields_[i]) {
        // Was already modified, use the value from the log (committed value).
        new_values.push_back(existing_values[old_idx++]);
      } else {
        // Newly modified in this step. Use the value from the current heap (base_tuple).
        if (base_tuple != nullptr) {
          new_values.push_back(base_tuple->GetValue(schema, i));
        }
      }
    }
  }

  Schema new_log_schema = Schema::CopySchema(schema, new_modified_cols);
  return {log.is_deleted_, modified_fields, Tuple(new_values, &new_log_schema), log.ts_, log.prev_version_};
}

void ModifyTuple(Transaction *txn, TransactionManager *txn_mgr, const TableInfo *table_info, RID rid,
                 const Tuple &new_tuple, bool is_delete) {
  auto temp_ts = txn->GetTransactionTempTs();
  auto read_ts = txn->GetReadTs();

  auto [meta, tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_info->table_.get(), rid);

  if (IsWriteWriteConflict(meta.ts_, read_ts, temp_ts)) {
    txn->SetTainted();
    throw ExecutionException(fmt::format("Write-write conflict in {}", is_delete ? "delete" : "update"));
  }

  const Tuple *target_tuple = is_delete ? nullptr : &new_tuple;

  if (meta.ts_ == temp_ts) {
    // Self-modification
    auto latest_undo_link = txn_mgr->GetUndoLink(rid);
    if (latest_undo_link.has_value() && latest_undo_link->IsValid()) {
      auto undo_log = txn_mgr->GetUndoLog(*latest_undo_link);
      if (latest_undo_link->prev_txn_ == temp_ts) {
        // Already has an undo log in this transaction.
        auto updated_log =
            GenerateUpdatedUndoLog(&table_info->schema_, meta.is_deleted_ ? nullptr : &tuple, target_tuple, undo_log);
        txn->ModifyUndoLog(latest_undo_link->prev_log_idx_, updated_log);
      }
    }
    // Update table heap
    if (is_delete) {
      auto check_func = [temp_ts](const TupleMeta &meta, const Tuple &tuple, RID rid) -> bool {
        return meta.ts_ == temp_ts;
      };
      if (!table_info->table_->UpdateTupleInPlace({temp_ts, true}, tuple, rid, check_func)) {
        txn->SetTainted();
        throw ExecutionException("Write-write conflict in delete (self-modification)");
      }
    } else {
      auto check_func = [temp_ts](const TupleMeta &meta, const Tuple &tuple, RID rid) -> bool {
        return meta.ts_ == temp_ts;
      };
      if (!table_info->table_->UpdateTupleInPlace({temp_ts, false}, new_tuple, rid, check_func)) {
        txn->SetTainted();
        throw ExecutionException("Write-write conflict in update (self-modification)");
      }
    }
  } else {
    // First modification by this transaction
    auto new_log = GenerateNewUndoLog(&table_info->schema_, meta.is_deleted_ ? nullptr : &tuple, target_tuple, meta.ts_,
                                      undo_link.value_or(UndoLink{}));
    auto log_link = txn->AppendUndoLog(new_log);

    TupleMeta new_meta = {temp_ts, is_delete};
    auto check_func = [read_ts, temp_ts](const TupleMeta &meta, const Tuple &tuple, RID rid,
                                         std::optional<UndoLink> undo_link) -> bool {
      return !IsWriteWriteConflict(meta.ts_, read_ts, temp_ts);
    };

    if (is_delete) {
      if (!UpdateTupleAndUndoLink(txn_mgr, rid, log_link, table_info->table_.get(), txn, new_meta, tuple, check_func)) {
        txn->SetTainted();
        throw ExecutionException("Write-write conflict in delete");
      }
    } else {
      if (!UpdateTupleAndUndoLink(txn_mgr, rid, log_link, table_info->table_.get(), txn, new_meta, new_tuple,
                                  check_func)) {
        txn->SetTainted();
        throw ExecutionException("Write-write conflict in update");
      }
    }
  }

  txn->AppendWriteSet(table_info->oid_, rid);
}

void UpdateSecondaryIndexEntries(Transaction *txn, const TableInfo *table_info,
                                 const std::vector<std::shared_ptr<IndexInfo>> &indexes, const Tuple &old_tuple,
                                 const Tuple &new_tuple, RID rid) {
  for (const auto &index_info : indexes) {
    if (index_info->is_primary_key_) {
      continue;
    }
    auto old_key =
        old_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    auto new_key =
        new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    if (!IsTupleContentEqual(old_key, new_key)) {
      index_info->index_->DeleteEntry(old_key, rid, txn);
      index_info->index_->InsertEntry(new_key, rid, txn);
    }
  }
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  fmt::println(stderr, "debug_hook: {}", info);

  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    RID rid = iter.GetRID();
    auto [meta, tuple] = iter.GetTuple();

    std::string ts_str;
    if (meta.ts_ >= TXN_START_ID) {
      ts_str = fmt::format("txn{}", meta.ts_ ^ TXN_START_ID);
    } else {
      ts_str = fmt::format("{}", meta.ts_);
    }

    std::string tuple_str = meta.is_deleted_ ? "<del marker>" : tuple.ToString(&table_info->schema_);
    fmt::println(stderr, "RID={}/{} ts={} tuple={}", rid.GetPageId(), rid.GetSlotNum(), ts_str, tuple_str);

    auto undo_link = txn_mgr->GetUndoLink(rid);
    while (undo_link.has_value() && undo_link->IsValid()) {
      auto undo_log_opt = txn_mgr->GetUndoLogOptional(*undo_link);
      if (!undo_log_opt.has_value()) {
        break;
      }
      auto undo_log = *undo_log_opt;

      std::string log_ts_str;
      if (undo_log.ts_ >= TXN_START_ID) {
        log_ts_str = fmt::format("txn{}", undo_log.ts_ ^ TXN_START_ID);
      } else {
        log_ts_str = fmt::format("{}", undo_log.ts_);
      }

      std::string log_tuple_str;
      if (undo_log.is_deleted_) {
        log_tuple_str = "<del>";
      } else {
        Schema log_schema = GetUndoLogSchema(&table_info->schema_, undo_log.modified_fields_);
        log_tuple_str = undo_log.tuple_.ToString(&log_schema);
      }

      fmt::println(stderr, "  txn{}@{} {} ts={}", undo_link->prev_txn_ ^ TXN_START_ID, undo_link->prev_log_idx_,
                   log_tuple_str, log_ts_str);

      undo_link = undo_log.prev_version_;
    }

    ++iter;
  }
}

}  // namespace bustub
