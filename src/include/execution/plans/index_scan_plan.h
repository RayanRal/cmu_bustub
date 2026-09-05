//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_plan.h
//
// Identification: src/include/execution/plans/index_scan_plan.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"
#include "type/value.h"

namespace bustub {
/**
 * A bound on one index key column for a range scan. Bounds are used only to position the scan (seek key) and for
 * early termination; the plan's `filter_predicate_` is always applied per tuple, so exclusive endpoints need no
 * special handling in the executor (the seek/termination keys treat them as inclusive, a safe superset).
 */
struct IndexRangeBound {
  bool has_low_{false};
  Value low_;
  bool low_inclusive_{true};
  bool has_high_{false};
  Value high_;
  bool high_inclusive_{true};
};
/**
 * IndexScanPlanNode identifies a table that should be scanned with an optional predicate.
 */
class IndexScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Creates a new index scan plan node with filter predicate.
   * @param output The output format of this scan plan node
   * @param table_oid The identifier of table to be scanned
   * @param filter_predicate The predicate pushed down to index scan.
   * @param pred_key The key for point lookup
   * @param range_bounds Per-key-column range bounds for a range scan (empty if not a range scan)
   */
  IndexScanPlanNode(SchemaRef output, table_oid_t table_oid, index_oid_t index_oid,
                    AbstractExpressionRef filter_predicate = nullptr, std::vector<AbstractExpressionRef> pred_keys = {},
                    std::vector<IndexRangeBound> range_bounds = {})
      : AbstractPlanNode(std::move(output), {}),
        table_oid_(table_oid),
        index_oid_(index_oid),
        filter_predicate_(std::move(filter_predicate)),
        pred_keys_(std::move(pred_keys)),
        range_bounds_(std::move(range_bounds)) {}

  auto GetType() const -> PlanType override { return PlanType::IndexScan; }

  /** @return the identifier of the table that should be scanned */
  auto GetIndexOid() const -> index_oid_t { return index_oid_; }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(IndexScanPlanNode);

  /** The table which the index is created on. */
  table_oid_t table_oid_;

  /** The index whose tuples should be scanned. */
  index_oid_t index_oid_;

  /** The predicate to filter in index scan.
   * For Fall 2023, after you implemented seqscan to indexscan optimizer rule,
   * we can use this predicate to do index point lookup
   */
  AbstractExpressionRef filter_predicate_;

  /**
   * The constant value keys to lookup.
   * For example when dealing "WHERE v = 1" we could store the constant value 1 here
   */
  std::vector<AbstractExpressionRef> pred_keys_;

  /**
   * Per-index-key-column range bounds for a range scan, in key column order. Empty when the node is a point
   * lookup (see `pred_keys_`) or a full index scan. Only a leading run of bounded columns is used for seeking;
   * remaining columns are covered by `filter_predicate_`.
   */
  std::vector<IndexRangeBound> range_bounds_;

  // Add anything you want here for index lookup

 protected:
  auto PlanNodeToString() const -> std::string override {
    if (!range_bounds_.empty()) {
      std::string bounds;
      for (size_t i = 0; i < range_bounds_.size(); ++i) {
        const auto &b = range_bounds_[i];
        bounds += fmt::format("col{}:[{},{}] ", i, b.has_low_ ? b.low_.ToString() : "-inf",
                              b.has_high_ ? b.high_.ToString() : "+inf");
      }
      return fmt::format("IndexScan {{ index_oid={}, range={}filter={} }}", index_oid_, bounds, filter_predicate_);
    }
    if (filter_predicate_) {
      return fmt::format("IndexScan {{ index_oid={}, filter={} }}", index_oid_, filter_predicate_);
    }
    return fmt::format("IndexScan {{ index_oid={} }}", index_oid_);
  }
};

}  // namespace bustub
