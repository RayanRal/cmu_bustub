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
      Schema partial_schema = Schema::CopySchema(schema, modified_cols);
      for (uint32_t i = 0; i < modified_cols.size(); ++i) {
        values[modified_cols[i]] = log.tuple_.GetValue(&partial_schema, i);
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
  std::optional<UndoLink> current_link = undo_link;
  while (current_link.has_value() && current_link->IsValid()) {
    UndoLog log = txn_mgr->GetUndoLog(*current_link);
    logs.push_back(log);
    if (log.ts_ <= read_ts) {
      return std::make_optional(std::move(logs));
    }
    current_link = log.prev_version_;
  }

  return std::nullopt;
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
  UNIMPLEMENTED("not implemented");
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  UNIMPLEMENTED("not implemented");
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
      auto undo_log = txn_mgr->GetUndoLog(*undo_link);

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
        std::vector<uint32_t> modified_cols;
        for (uint32_t i = 0; i < undo_log.modified_fields_.size(); ++i) {
          if (undo_log.modified_fields_[i]) {
            modified_cols.push_back(i);
          }
        }
        Schema log_schema = Schema::CopySchema(&table_info->schema_, modified_cols);
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
