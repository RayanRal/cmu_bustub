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

#include <functional>
#include <iterator>
#include <memory>
#include <vector>
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "optimizer/optimizer.h"
#include "type/value_factory.h"

namespace bustub {

namespace {

/** Which side(s) of a join an expression references. */
enum class RefSide { NONE, LEFT, RIGHT, BOTH };

auto ClassifyRefs(const AbstractExpressionRef &expr) -> RefSide {
  bool left = false;
  bool right = false;
  std::function<void(const AbstractExpressionRef &)> visit = [&](const AbstractExpressionRef &node) {
    if (const auto *col = dynamic_cast<const ColumnValueExpression *>(node.get()); col != nullptr) {
      if (col->GetTupleIdx() == 0) {
        left = true;
      } else {
        right = true;
      }
    }
    for (const auto &child : node->GetChildren()) {
      visit(child);
    }
  };
  visit(expr);
  if (left && right) {
    return RefSide::BOTH;
  }
  if (left) {
    return RefSide::LEFT;
  }
  if (right) {
    return RefSide::RIGHT;
  }
  return RefSide::NONE;
}

/** Split a predicate into its top-level AND conjuncts. */
auto SplitConjuncts(const AbstractExpressionRef &expr) -> std::vector<AbstractExpressionRef> {
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
      logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    auto conjuncts = SplitConjuncts(logic_expr->GetChildAt(0));
    auto rest = SplitConjuncts(logic_expr->GetChildAt(1));
    conjuncts.insert(conjuncts.end(), std::make_move_iterator(rest.begin()), std::make_move_iterator(rest.end()));
    return conjuncts;
  }
  return {expr};
}

/**
 * Rewrite a single-side conjunct from the parent NLJ tuple space into a child NLJ's tuple space:
 * references `(from_side, c)` become `(0, c)` / `(1, c - child_left_width)` depending on which
 * grandchild they belong to.
 */
auto RewriteForChildJoin(const AbstractExpressionRef &expr, uint8_t from_side, size_t child_left_width)
    -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(RewriteForChildJoin(child, from_side, child_left_width));
  }
  if (const auto *col = dynamic_cast<const ColumnValueExpression *>(expr.get()); col != nullptr) {
    BUSTUB_ENSURE(col->GetTupleIdx() == from_side, "conjunct references unexpected join side");
    auto col_idx = col->GetColIdx();
    if (col_idx < child_left_width) {
      return std::make_shared<ColumnValueExpression>(0, col_idx, col->GetReturnType());
    }
    return std::make_shared<ColumnValueExpression>(1, col_idx - child_left_width, col->GetReturnType());
  }
  return expr->CloneWithChildren(children);
}

/**
 * Rewrite a residual conjunct from the NLJ tuple space into the joined-output tuple space:
 * `(0, c)` stays, `(1, c)` becomes `(0, left_width + c)`.
 */
auto RewriteForJoinOutput(const AbstractExpressionRef &expr, size_t left_width) -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(RewriteForJoinOutput(child, left_width));
  }
  if (const auto *col = dynamic_cast<const ColumnValueExpression *>(expr.get()); col != nullptr) {
    if (col->GetTupleIdx() == 1) {
      return std::make_shared<ColumnValueExpression>(0, left_width + col->GetColIdx(), col->GetReturnType());
    }
    return std::make_shared<ColumnValueExpression>(0, col->GetColIdx(), col->GetReturnType());
  }
  return expr->CloneWithChildren(children);
}

/** Fold conjuncts into a left-deep AND chain. Expects a non-empty vector. */
auto FoldConjuncts(std::vector<AbstractExpressionRef> conjuncts) -> AbstractExpressionRef {
  BUSTUB_ENSURE(!conjuncts.empty(), "cannot fold empty conjunct list");
  AbstractExpressionRef result = std::move(conjuncts.front());
  for (size_t i = 1; i < conjuncts.size(); i++) {
    result = std::make_shared<LogicExpression>(std::move(result), std::move(conjuncts[i]), LogicType::And);
  }
  return result;
}

/** Try to interpret a both-side conjunct as an equi-join key pair. Returns false if it is not one. */
auto ExtractKeyPair(const AbstractExpressionRef &expr, AbstractExpressionRef *left_key,
                    AbstractExpressionRef *right_key) -> bool {
  const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (comp_expr == nullptr || comp_expr->comp_type_ != ComparisonType::Equal) {
    return false;
  }
  const auto *left_column = dynamic_cast<const ColumnValueExpression *>(comp_expr->GetChildAt(0).get());
  const auto *right_column = dynamic_cast<const ColumnValueExpression *>(comp_expr->GetChildAt(1).get());
  if (left_column == nullptr || right_column == nullptr) {
    return false;
  }
  if (left_column->GetTupleIdx() == 0 && right_column->GetTupleIdx() == 1) {
    *left_key = comp_expr->GetChildAt(0);
    *right_key = comp_expr->GetChildAt(1);
    return true;
  }
  if (left_column->GetTupleIdx() == 1 && right_column->GetTupleIdx() == 0) {
    *left_key = comp_expr->GetChildAt(1);
    *right_key = comp_expr->GetChildAt(0);
    return true;
  }
  return false;
}

/** Extract equi-join key pairs from every conjunct. Returns false if any conjunct is not one. */
auto TryExtractAllKeys(const std::vector<AbstractExpressionRef> &conjuncts,
                       std::vector<AbstractExpressionRef> *left_keys, std::vector<AbstractExpressionRef> *right_keys)
    -> bool {
  for (const auto &conjunct : conjuncts) {
    AbstractExpressionRef left_key;
    AbstractExpressionRef right_key;
    if (!ExtractKeyPair(conjunct, &left_key, &right_key)) {
      return false;
    }
    left_keys->push_back(std::move(left_key));
    right_keys->push_back(std::move(right_key));
  }
  return !left_keys->empty();
}

}  // namespace

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    const auto &predicate = nlj_plan.Predicate();
    if (predicate == nullptr || IsPredicateTrue(predicate)) {
      return optimized_plan;
    }
    return ConvertNLJToHashJoin(nlj_plan);
  }

  return optimized_plan;
}

/**
 * Push single-side conjuncts into a child NLJ(true) inner join so deeper levels can convert too.
 * Rewrites the conjuncts into the child's tuple space, rebuilds the child with the folded predicate,
 * and converts it recursively. Clears `push` and returns true on success; otherwise leaves both untouched.
 */
auto Optimizer::TryPushIntoChildNLJ(AbstractPlanNodeRef *child, uint8_t from_side,
                                    std::vector<AbstractExpressionRef> *push) -> bool {
  if (push->empty() || (*child)->GetType() != PlanType::NestedLoopJoin) {
    return false;
  }
  const auto &child_nlj = dynamic_cast<const NestedLoopJoinPlanNode &>(**child);
  if (child_nlj.Predicate() == nullptr || !IsPredicateTrue(child_nlj.Predicate()) ||
      child_nlj.GetJoinType() != JoinType::INNER) {
    return false;
  }
  const size_t child_left_width = child_nlj.GetLeftPlan()->OutputSchema().GetColumnCount();
  std::vector<AbstractExpressionRef> rewritten;
  rewritten.reserve(push->size());
  for (auto &conjunct : *push) {
    rewritten.push_back(RewriteForChildJoin(conjunct, from_side, child_left_width));
  }
  auto rebuilt = std::make_shared<NestedLoopJoinPlanNode>(child_nlj.output_schema_, child_nlj.GetLeftPlan(),
                                                          child_nlj.GetRightPlan(), FoldConjuncts(std::move(rewritten)),
                                                          child_nlj.GetJoinType());
  *child = ConvertNLJToHashJoin(*rebuilt);
  push->clear();
  return true;
}

auto Optimizer::ConvertNLJToHashJoin(const NestedLoopJoinPlanNode &nlj_plan) -> AbstractPlanNodeRef {
  const auto &predicate = nlj_plan.Predicate();
  BUSTUB_ASSERT(predicate != nullptr && !IsPredicateTrue(predicate),
                "cross joins and NLJ(true) never reach hash join conversion");

  // Residuals and pushdown preserve semantics for INNER joins only; outer joins use legacy all-or-nothing below.
  if (nlj_plan.GetJoinType() != JoinType::INNER) {
    return ConvertNLJToHashJoinAllOrNothing(nlj_plan);
  }

  const size_t left_width = nlj_plan.GetLeftPlan()->OutputSchema().GetColumnCount();

  // Partition conjuncts by the join side(s) they reference.
  std::vector<AbstractExpressionRef> left_push;
  std::vector<AbstractExpressionRef> right_push;
  std::vector<AbstractExpressionRef> keep;
  for (auto &conjunct : SplitConjuncts(predicate)) {
    switch (ClassifyRefs(conjunct)) {
      case RefSide::LEFT:
        left_push.push_back(std::move(conjunct));
        break;
      case RefSide::RIGHT:
        right_push.push_back(std::move(conjunct));
        break;
      default:
        keep.push_back(std::move(conjunct));
        break;
    }
  }

  // Anything that cannot be pushed stays at this level and is evaluated above the hash join.
  auto left_child = nlj_plan.GetLeftPlan();
  auto right_child = nlj_plan.GetRightPlan();
  bool pushed_down = TryPushIntoChildNLJ(&left_child, 0, &left_push);
  // NB: |=, not || — both sides must always be attempted.
  pushed_down |= TryPushIntoChildNLJ(&right_child, 1, &right_push);
  std::vector<AbstractExpressionRef> residuals;
  residuals.reserve(left_push.size() + right_push.size() + keep.size());
  for (auto &conjunct : left_push) {
    residuals.push_back(RewriteForJoinOutput(conjunct, left_width));
  }
  for (auto &conjunct : right_push) {
    residuals.push_back(RewriteForJoinOutput(conjunct, left_width));
  }
  std::vector<AbstractExpressionRef> left_keys;
  std::vector<AbstractExpressionRef> right_keys;
  left_keys.reserve(keep.size());
  right_keys.reserve(keep.size());
  for (auto &conjunct : keep) {
    AbstractExpressionRef left_key;
    AbstractExpressionRef right_key;
    if (ExtractKeyPair(conjunct, &left_key, &right_key)) {
      left_keys.push_back(std::move(left_key));
      right_keys.push_back(std::move(right_key));
    } else {
      residuals.push_back(RewriteForJoinOutput(conjunct, left_width));
    }
  }

  if (left_keys.empty()) {
    // No hash join possible at this level; keep the NLJ over the (possibly converted) children with the
    // conjuncts that could not be pushed down, still in NLJ tuple space.
    if (!pushed_down) {
      return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, std::move(left_child),
                                                      std::move(right_child), predicate, nlj_plan.GetJoinType());
    }
    std::vector<AbstractExpressionRef> remaining;
    remaining.reserve(keep.size() + left_push.size() + right_push.size());
    for (auto &conjunct : keep) {
      remaining.push_back(conjunct);
    }
    for (auto &conjunct : left_push) {
      remaining.push_back(conjunct);
    }
    for (auto &conjunct : right_push) {
      remaining.push_back(conjunct);
    }
    AbstractExpressionRef remaining_pred =
        remaining.empty() ? std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true))
                          : FoldConjuncts(std::move(remaining));
    return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, std::move(left_child),
                                                    std::move(right_child), std::move(remaining_pred),
                                                    nlj_plan.GetJoinType());
  }

  auto hash_join =
      std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, std::move(left_child), std::move(right_child),
                                         std::move(left_keys), std::move(right_keys), nlj_plan.GetJoinType());
  if (residuals.empty()) {
    return hash_join;
  }
  return std::make_shared<FilterPlanNode>(nlj_plan.output_schema_, FoldConjuncts(std::move(residuals)),
                                          std::move(hash_join));
}

auto Optimizer::ConvertNLJToHashJoinAllOrNothing(const NestedLoopJoinPlanNode &nlj_plan) -> AbstractPlanNodeRef {
  std::vector<AbstractExpressionRef> left_keys;
  std::vector<AbstractExpressionRef> right_keys;
  if (!TryExtractAllKeys(SplitConjuncts(nlj_plan.Predicate()), &left_keys, &right_keys)) {
    return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                    nlj_plan.GetRightPlan(), nlj_plan.Predicate(),
                                                    nlj_plan.GetJoinType());
  }
  return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                            std::move(left_keys), std::move(right_keys), nlj_plan.GetJoinType());
}

}  // namespace bustub
