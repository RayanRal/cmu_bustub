//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                                 std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedIndexJoinExecutor::Init() {
  child_executor_->Init();
  auto *catalog = exec_ctx_->GetCatalog();
  inner_table_info_ = catalog->GetTable(plan_->GetInnerTableOid());
  index_info_ = catalog->GetIndex(plan_->GetIndexOid());

  left_tuples_.clear();
  left_rids_.clear();
  left_idx_ = 0;

  result_rids_.clear();
  result_idx_ = 0;
  is_new_left_tuple_ = true;

  child_executor_->Next(&left_tuples_, &left_rids_, BUSTUB_BATCH_SIZE);
}

auto NestedIndexJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                   size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (left_idx_ < left_tuples_.size()) {
    const auto &left_tuple = left_tuples_[left_idx_];

    if (is_new_left_tuple_) {
      auto key_value = plan_->key_predicate_->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
      Tuple key_tuple({key_value}, index_info_->index_->GetKeySchema());
      result_rids_.clear();
      index_info_->index_->ScanKey(key_tuple, &result_rids_, exec_ctx_->GetTransaction());
      result_idx_ = 0;
      is_new_left_tuple_ = false;
    }

    if (result_idx_ < result_rids_.size()) {
      for (; result_idx_ < result_rids_.size(); ++result_idx_) {
        Tuple inner_tuple;
        auto [meta, fetched_tuple] = inner_table_info_->table_->GetTuple(result_rids_[result_idx_]);
        if (meta.is_deleted_) {
          continue;
        }
        inner_tuple = std::move(fetched_tuple);

        std::vector<Value> values;
        values.reserve(child_executor_->GetOutputSchema().GetColumnCount() +
                       plan_->InnerTableSchema().GetColumnCount());
        for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); ++i) {
          values.push_back(inner_tuple.GetValue(&plan_->InnerTableSchema(), i));
        }
        tuple_batch->emplace_back(std::move(values), &plan_->OutputSchema());
        rid_batch->emplace_back();

        if (tuple_batch->size() >= batch_size) {
          ++result_idx_;
          return true;
        }
      }
    } else {
      // result_rids_ is empty or all processed
      if (plan_->GetJoinType() == JoinType::LEFT && result_idx_ == 0) {
        // Only if we haven't matched anything for this left tuple
        std::vector<Value> values;
        values.reserve(child_executor_->GetOutputSchema().GetColumnCount() +
                       plan_->InnerTableSchema().GetColumnCount());
        for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); ++i) {
          values.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
        }
        tuple_batch->emplace_back(std::move(values), &plan_->OutputSchema());
        rid_batch->emplace_back();
      }
    }

    // Move to next left tuple
    left_idx_++;
    is_new_left_tuple_ = true;
    result_idx_ = 0;
    result_rids_.clear();

    if (left_idx_ >= left_tuples_.size()) {
      left_idx_ = 0;
      if (!child_executor_->Next(&left_tuples_, &left_rids_, batch_size)) {
        return !tuple_batch->empty();
      }
    }

    if (tuple_batch->size() >= batch_size) {
      return true;
    }
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
