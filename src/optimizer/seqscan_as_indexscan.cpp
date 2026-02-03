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

#include <vector>
#include "optimizer/optimizer.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {

void ExtractEqualityConstants(const AbstractExpressionRef &expr, uint32_t &col_idx, std::vector<Value> &constants, bool &possible) {
  if (!possible) return;

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
      uint32_t col_idx = 0xFFFFFFFF;
      std::vector<Value> constants;
      bool possible = true;
      ExtractEqualityConstants(seq_scan.filter_predicate_, col_idx, constants, possible);

      if (possible && col_idx != 0xFFFFFFFF) {
        const auto table_info = catalog_.GetTable(seq_scan.GetTableOid());
        const auto indices = catalog_.GetTableIndexes(table_info->name_);
        for (const auto &index : indices) {
          const auto &columns = index->index_->GetKeyAttrs();
          if (columns.size() == 1 && columns[0] == col_idx) {
            std::vector<AbstractExpressionRef> pred_keys;
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
