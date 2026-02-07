//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

void ExtractEquiConditions(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> *left_key_expressions,
                           std::vector<AbstractExpressionRef> *right_key_expressions, bool *success) {
  if (!*success) {
    return;
  }

  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get())) {
    if (logic_expr->logic_type_ == LogicType::And) {
      ExtractEquiConditions(logic_expr->GetChildAt(0), left_key_expressions, right_key_expressions, success);
      ExtractEquiConditions(logic_expr->GetChildAt(1), left_key_expressions, right_key_expressions, success);
      return;
    }
  } else if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get())) {
    if (comp_expr->comp_type_ == ComparisonType::Equal) {
      const auto *left_column = dynamic_cast<const ColumnValueExpression *>(comp_expr->GetChildAt(0).get());
      const auto *right_column = dynamic_cast<const ColumnValueExpression *>(comp_expr->GetChildAt(1).get());

      if (left_column != nullptr && right_column != nullptr) {
        if (left_column->GetTupleIdx() == 0 && right_column->GetTupleIdx() == 1) {
          left_key_expressions->push_back(comp_expr->GetChildAt(0));
          right_key_expressions->push_back(comp_expr->GetChildAt(1));
          return;
        }
        if (left_column->GetTupleIdx() == 1 && right_column->GetTupleIdx() == 0) {
          left_key_expressions->push_back(comp_expr->GetChildAt(1));
          right_key_expressions->push_back(comp_expr->GetChildAt(0));
          return;
        }
      }
    }
  }
  *success = false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    bool success = true;

    if (nlj_plan.Predicate() != nullptr) {
      ExtractEquiConditions(nlj_plan.Predicate(), &left_key_expressions, &right_key_expressions, &success);
      if (success && !left_key_expressions.empty()) {
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), std::move(left_key_expressions),
                                                  std::move(right_key_expressions), nlj_plan.GetJoinType());
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
