//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unordered_map>
#include <vector>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

namespace {

/** Bounds on one table column collected from ANDed `column OP constant` conjuncts. */
struct ColumnRange {
  bool has_eq_{false};
  Value eq_;
  bool has_low_{false};
  Value low_;
  bool low_inclusive_{true};
  bool has_high_{false};
  Value high_;
  bool high_inclusive_{true};
};

void TightenLow(ColumnRange &range, const Value &val, bool inclusive) {
  if (!range.has_low_) {
    range.has_low_ = true;
    range.low_ = val;
    range.low_inclusive_ = inclusive;
    return;
  }
  if (val.CompareGreaterThan(range.low_) == CmpBool::CmpTrue) {
    range.low_ = val;
    range.low_inclusive_ = inclusive;
  } else if (val.CompareEquals(range.low_) == CmpBool::CmpTrue) {
    range.low_inclusive_ = range.low_inclusive_ && inclusive;
  }
}

void TightenHigh(ColumnRange &range, const Value &val, bool inclusive) {
  if (!range.has_high_) {
    range.has_high_ = true;
    range.high_ = val;
    range.high_inclusive_ = inclusive;
    return;
  }
  if (val.CompareLessThan(range.high_) == CmpBool::CmpTrue) {
    range.high_ = val;
    range.high_inclusive_ = inclusive;
  } else if (val.CompareEquals(range.high_) == CmpBool::CmpTrue) {
    range.high_inclusive_ = range.high_inclusive_ && inclusive;
  }
}

/**
 * Collects per-column bounds from a conjunction of `column OP constant` comparisons. Sets `possible = false` if any
 * conjunct is not usable (non-AND logic, non-col-vs-const comparison, NULL or mistyped constant, join-side column).
 */
void ExtractColumnRanges(const AbstractExpressionRef &expr, const TableInfo *table_info,
                         std::unordered_map<uint32_t, ColumnRange> &ranges, bool &possible) {
  if (!possible) {
    return;
  }

  if (const auto *logic = dynamic_cast<const LogicExpression *>(expr.get()); logic != nullptr) {
    if (logic->logic_type_ == LogicType::And) {
      ExtractColumnRanges(logic->GetChildAt(0), table_info, ranges, possible);
      ExtractColumnRanges(logic->GetChildAt(1), table_info, ranges, possible);
    } else {
      possible = false;
    }
    return;
  }

  const auto *comparison = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (comparison == nullptr) {
    possible = false;
    return;
  }
  const auto *col = dynamic_cast<const ColumnValueExpression *>(comparison->GetChildAt(0).get());
  const auto *cst = dynamic_cast<const ConstantValueExpression *>(comparison->GetChildAt(1).get());
  bool flipped = false;
  if (col == nullptr || cst == nullptr) {
    col = dynamic_cast<const ColumnValueExpression *>(comparison->GetChildAt(1).get());
    cst = dynamic_cast<const ConstantValueExpression *>(comparison->GetChildAt(0).get());
    flipped = true;
  }
  if (col == nullptr || cst == nullptr || col->GetTupleIdx() != 0) {
    possible = false;
    return;
  }
  uint32_t col_idx = col->GetColIdx();
  if (col_idx >= table_info->schema_.GetColumnCount()) {
    possible = false;
    return;
  }
  const Value &val = cst->val_;
  if (val.IsNull() || val.GetTypeId() != table_info->schema_.GetColumn(col_idx).GetType()) {
    possible = false;
    return;
  }

  auto comp = comparison->comp_type_;
  if (flipped) {
    switch (comp) {
      case ComparisonType::GreaterThan:
        comp = ComparisonType::LessThan;
        break;
      case ComparisonType::GreaterThanOrEqual:
        comp = ComparisonType::LessThanOrEqual;
        break;
      case ComparisonType::LessThan:
        comp = ComparisonType::GreaterThan;
        break;
      case ComparisonType::LessThanOrEqual:
        comp = ComparisonType::GreaterThanOrEqual;
        break;
      default:
        break;
    }
  }

  ColumnRange &range = ranges[col_idx];
  switch (comp) {
    case ComparisonType::Equal:
      range.has_eq_ = true;
      range.eq_ = val;
      break;
    case ComparisonType::GreaterThan:
      TightenLow(range, val, false);
      break;
    case ComparisonType::GreaterThanOrEqual:
      TightenLow(range, val, true);
      break;
    case ComparisonType::LessThan:
      TightenHigh(range, val, false);
      break;
    case ComparisonType::LessThanOrEqual:
      TightenHigh(range, val, true);
      break;
    default:
      possible = false;
      break;
  }
}

/**
 * Tries to rewrite the seq scan as an index range scan: an AND of col-vs-const comparisons covering a leading
 * run of a B+Tree index's key columns. Returns nullptr when not applicable (legacy point-lookup path runs next).
 */
auto TrySeqScanAsIndexRangeScan(const SeqScanPlanNode &seq_scan, const std::shared_ptr<TableInfo> &table_info,
                                const std::vector<std::shared_ptr<IndexInfo>> &indices, SchemaRef output_schema)
    -> AbstractPlanNodeRef {
  std::unordered_map<uint32_t, ColumnRange> ranges;
  bool possible = true;
  ExtractColumnRanges(seq_scan.filter_predicate_, table_info.get(), ranges, possible);
  if (!possible || ranges.empty()) {
    return nullptr;
  }

  for (const auto &index : indices) {
    if (index->index_type_ != IndexType::BPlusTreeIndex) {
      continue;
    }
    const auto &key_attrs = index->index_->GetKeyAttrs();
    if (key_attrs.empty() || key_attrs.size() > 2) {
      continue;
    }
    std::vector<IndexRangeBound> bounds;
    for (uint32_t key_col : key_attrs) {
      auto it = ranges.find(key_col);
      if (it == ranges.end()) {
        break;
      }
      const ColumnRange &range = it->second;
      IndexRangeBound bound;
      if (range.has_eq_) {
        bound.has_low_ = true;
        bound.low_ = range.eq_;
        bound.has_high_ = true;
        bound.high_ = range.eq_;
      } else {
        if (!range.has_low_ && !range.has_high_) {
          break;
        }
        bound.has_low_ = range.has_low_;
        bound.low_ = range.low_;
        bound.low_inclusive_ = range.low_inclusive_;
        bound.has_high_ = range.has_high_;
        bound.high_ = range.high_;
        bound.high_inclusive_ = range.high_inclusive_;
      }
      bounds.push_back(bound);
    }
    if (bounds.empty()) {
      continue;
    }
    return std::make_shared<IndexScanPlanNode>(std::move(output_schema), table_info->oid_, index->index_oid_,
                                               seq_scan.filter_predicate_, std::vector<AbstractExpressionRef>{},
                                               std::move(bounds));
  }
  return nullptr;
}

}  // namespace

void ExtractEqualityConstants(const AbstractExpressionRef &expr, uint32_t &col_idx, std::vector<Value> &constants,
                              bool &possible) {
  if (!possible) {
    return;
  }

  if (const auto *logic = dynamic_cast<const LogicExpression *>(expr.get()); logic != nullptr) {
    if (logic->logic_type_ == LogicType::Or) {
      ExtractEqualityConstants(logic->GetChildAt(0), col_idx, constants, possible);
      ExtractEqualityConstants(logic->GetChildAt(1), col_idx, constants, possible);
    } else {
      possible = false;
    }
    return;
  }

  if (const auto *comparison = dynamic_cast<const ComparisonExpression *>(expr.get()); comparison != nullptr) {
    if (comparison->comp_type_ == ComparisonType::Equal) {
      const auto *left = dynamic_cast<const ColumnValueExpression *>(comparison->GetChildAt(0).get());
      const auto *right = dynamic_cast<const ConstantValueExpression *>(comparison->GetChildAt(1).get());

      if (left == nullptr || right == nullptr) {
        left = dynamic_cast<const ColumnValueExpression *>(comparison->GetChildAt(1).get());
        right = dynamic_cast<const ConstantValueExpression *>(comparison->GetChildAt(0).get());
      }

      if (left != nullptr && right != nullptr) {
        if (col_idx == 0xFFFFFFFF) {
          col_idx = left->GetColIdx();
        }
        if (col_idx == left->GetColIdx()) {
          constants.push_back(right->val_);
          return;
        }
      }
    }
  }
  possible = false;
}

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan.filter_predicate_ != nullptr) {
      const auto table_info = catalog_.GetTable(seq_scan.GetTableOid());
      const auto indices = catalog_.GetTableIndexes(table_info->name_);
      // The range rewrite runs first, so even single equalities (e.g. `v = 2`) become degenerate `[2,2]`
      // range scans; the legacy point-lookup path below now serves only OR-of-equalities.
      if (auto range_plan = TrySeqScanAsIndexRangeScan(seq_scan, table_info, indices, optimized_plan->output_schema_);
          range_plan != nullptr) {
        return range_plan;
      }
      uint32_t col_idx = 0xFFFFFFFF;
      std::vector<Value> constants;
      bool possible = true;
      ExtractEqualityConstants(seq_scan.filter_predicate_, col_idx, constants, possible);

      if (possible && col_idx != 0xFFFFFFFF) {
        for (const auto &index : indices) {
          const auto &columns = index->index_->GetKeyAttrs();
          if (columns.size() == 1 && columns[0] == col_idx) {
            std::vector<AbstractExpressionRef> pred_keys;
            pred_keys.reserve(constants.size());
            for (const auto &val : constants) {
              pred_keys.push_back(std::make_shared<ConstantValueExpression>(val));
            }
            return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_,
                                                       index->index_oid_, seq_scan.filter_predicate_,
                                                       std::move(pred_keys));
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
