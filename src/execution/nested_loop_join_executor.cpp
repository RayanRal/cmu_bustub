//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  left_tuples_.clear();
  left_rids_.clear();
  left_idx_ = 0;

  right_tuples_.clear();
  right_rids_.clear();
  right_idx_ = 0;

  matched_ = false;
  is_right_eof_ = false;
  right_empty_ = false;
  saw_right_tuple_ = false;

  left_executor_->Next(&left_tuples_, &left_rids_, BUSTUB_BATCH_SIZE);
}

/** Emit one NULL-padded row for `left_tuple` (LEFT join over an empty right side). */
void NestedLoopJoinExecutor::EmitNullPaddedLeftRow(const Tuple &left_tuple, std::vector<Tuple> *tuple_batch,
                                                   std::vector<RID> *rid_batch) {
  std::vector<Value> values;
  values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                 right_executor_->GetOutputSchema().GetColumnCount());
  for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
  }
  for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
    values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
  }
  tuple_batch->emplace_back(std::move(values), &plan_->OutputSchema());
  rid_batch->emplace_back();
}

/**
 * Yield the next tuple batch from the join.
 * @param[out] tuple_batch The next tuple batch produced by the join
 * @param[out] rid_batch The next tuple RID batch produced by the join
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto NestedLoopJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (left_idx_ < left_tuples_.size()) {
    const auto &left_tuple = left_tuples_[left_idx_];

    if (right_empty_) {
      // Fast path: the right side is known to be empty, so skip its per-row Init/Next entirely.
      if (plan_->GetJoinType() == JoinType::INNER) {
        return !tuple_batch->empty();
      }
      EmitNullPaddedLeftRow(left_tuple, tuple_batch, rid_batch);

      // Move to next left tuple
      left_idx_++;
      if (left_idx_ >= left_tuples_.size()) {
        left_idx_ = 0;
        if (!left_executor_->Next(&left_tuples_, &left_rids_, batch_size)) {
          return !tuple_batch->empty();
        }
      }

      if (tuple_batch->size() >= batch_size) {
        return true;
      }
      continue;
    }

    while (!is_right_eof_) {
      if (right_idx_ >= right_tuples_.size()) {
        right_idx_ = 0;
        if (!right_executor_->Next(&right_tuples_, &right_rids_, batch_size)) {
          is_right_eof_ = true;
          break;
        }
        saw_right_tuple_ = true;
      }

      for (; right_idx_ < right_tuples_.size(); ++right_idx_) {
        const auto &right_tuple = right_tuples_[right_idx_];
        // A null predicate means cross join: every pair matches.
        bool is_match = plan_->Predicate() == nullptr;
        if (!is_match) {
          auto value = plan_->Predicate()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                        right_executor_->GetOutputSchema());
          is_match = !value.IsNull() && value.GetAs<bool>();
        }
        if (is_match) {
          matched_ = true;
          std::vector<Value> values;
          values.reserve(left_executor_->GetOutputSchema().GetColumnCount() +
                         right_executor_->GetOutputSchema().GetColumnCount());
          for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
            values.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
          }
          for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
            values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
          }
          tuple_batch->emplace_back(std::move(values), &plan_->OutputSchema());
          rid_batch->emplace_back();

          if (tuple_batch->size() >= batch_size) {
            ++right_idx_;
            return true;
          }
        }
      }
    }

    if (is_right_eof_) {
      if (!saw_right_tuple_) {
        // A full scan produced zero right tuples. The right child does not depend on the left tuple, so every
        // rescan is empty: latch and skip per-row Init/Next for all remaining left rows.
        right_empty_ = true;
      }
      if (plan_->GetJoinType() == JoinType::LEFT && !matched_) {
        EmitNullPaddedLeftRow(left_tuple, tuple_batch, rid_batch);
      }

      // Move to next left tuple
      left_idx_++;
      right_executor_->Init();
      right_tuples_.clear();
      right_rids_.clear();
      right_idx_ = 0;
      matched_ = false;
      is_right_eof_ = false;
      saw_right_tuple_ = false;

      if (left_idx_ >= left_tuples_.size()) {
        left_idx_ = 0;
        if (!left_executor_->Next(&left_tuples_, &left_rids_, batch_size)) {
          return !tuple_batch->empty();
        }
      }

      if (tuple_batch->size() >= batch_size) {
        return true;
      }
    }
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
