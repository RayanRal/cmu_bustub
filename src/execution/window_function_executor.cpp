#include "execution/executors/window_function_executor.h"
#include <algorithm>
#include "execution/execution_common.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new WindowFunctionExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The window aggregation plan to be executed
 */
WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the window aggregation */
void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  computed_tuples_.clear();
  cursor_ = 0;

  std::vector<Tuple> child_tuples;
  std::vector<Tuple> batch;
  std::vector<RID> rids;
  while (child_executor_->Next(&batch, &rids, BUSTUB_BATCH_SIZE)) {
    for (auto &tuple : batch) {
      child_tuples.push_back(std::move(tuple));
    }
    batch.clear();
    rids.clear();
  }

  if (child_tuples.empty()) {
    return;
  }

  size_t num_tuples = child_tuples.size();
  std::vector<uint32_t> original_indices(num_tuples);
  for (uint32_t i = 0; i < num_tuples; ++i) {
    original_indices[i] = i;
  }

  // Store results for each window function: map from column index -> vector of values
  std::unordered_map<uint32_t, std::vector<Value>> window_results;

  for (const auto &pair : plan_->window_functions_) {
    uint32_t col_idx = pair.first;
    const auto &wf = pair.second;
    std::vector<Value> results(num_tuples);

    // Sort original_indices based on (partition_by, order_by)
    std::sort(original_indices.begin(), original_indices.end(), [&](uint32_t idx_a, uint32_t idx_b) {
      const auto &tuple_a = child_tuples[idx_a];
      const auto &tuple_b = child_tuples[idx_b];

      // 1. Compare PARTITION BY
      for (const auto &expr : wf.partition_by_) {
        Value val_a = expr->Evaluate(&tuple_a, child_executor_->GetOutputSchema());
        Value val_b = expr->Evaluate(&tuple_b, child_executor_->GetOutputSchema());
        if (val_a.CompareLessThan(val_b) == CmpBool::CmpTrue) return true;
        if (val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue) return false;
      }

      // 2. Compare ORDER BY
      for (const auto &order_by : wf.order_by_) {
        OrderByType type = std::get<0>(order_by);
        OrderByNullType null_type = std::get<1>(order_by);
        const auto &expr = std::get<2>(order_by);
        Value val_a = expr->Evaluate(&tuple_a, child_executor_->GetOutputSchema());
        Value val_b = expr->Evaluate(&tuple_b, child_executor_->GetOutputSchema());

        if (val_a.IsNull() && val_b.IsNull()) continue;
        bool is_asc = (type == OrderByType::ASC || type == OrderByType::DEFAULT);
        bool nulls_first =
            (null_type == OrderByNullType::NULLS_FIRST || (null_type == OrderByNullType::DEFAULT && is_asc));

        if (val_a.IsNull()) return nulls_first;
        if (val_b.IsNull()) return !nulls_first;

        if (is_asc) {
          if (val_a.CompareLessThan(val_b) == CmpBool::CmpTrue) return true;
          if (val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue) return false;
        } else {
          if (val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue) return true;
          if (val_a.CompareLessThan(val_b) == CmpBool::CmpTrue) return false;
        }
      }
      return false;
    });

    auto is_same_partition = [&](uint32_t idx_a, uint32_t idx_b) {
      for (const auto &expr : wf.partition_by_) {
        Value val_a = expr->Evaluate(&child_tuples[idx_a], child_executor_->GetOutputSchema());
        Value val_b = expr->Evaluate(&child_tuples[idx_b], child_executor_->GetOutputSchema());
        if (val_a.CompareEquals(val_b) != CmpBool::CmpTrue) {
          if (!val_a.IsNull() || !val_b.IsNull()) return false;
        }
      }
      return true;
    };

    auto is_same_order = [&](uint32_t idx_a, uint32_t idx_b) {
      for (const auto &order_by : wf.order_by_) {
        const auto &expr = std::get<2>(order_by);
        Value val_a = expr->Evaluate(&child_tuples[idx_a], child_executor_->GetOutputSchema());
        Value val_b = expr->Evaluate(&child_tuples[idx_b], child_executor_->GetOutputSchema());
        if (val_a.CompareEquals(val_b) != CmpBool::CmpTrue) {
          if (!val_a.IsNull() || !val_b.IsNull()) return false;
        }
      }
      return true;
    };

    if (wf.order_by_.empty()) {
      uint32_t start = 0;
      while (start < num_tuples) {
        uint32_t end = start + 1;
        while (end < num_tuples && is_same_partition(original_indices[start], original_indices[end])) {
          end++;
        }

        Value acc;
        if (wf.type_ == WindowFunctionType::CountStarAggregate) {
          acc = ValueFactory::GetIntegerValue(0);
        } else {
          acc = ValueFactory::GetNullValueByType(
              wf.function_->Evaluate(&child_tuples[original_indices[start]], child_executor_->GetOutputSchema())
                  .GetTypeId());
        }

        for (uint32_t i = start; i < end; ++i) {
          Value val = wf.function_->Evaluate(&child_tuples[original_indices[i]], child_executor_->GetOutputSchema());
          switch (wf.type_) {
            case WindowFunctionType::CountStarAggregate:
              acc = acc.Add(ValueFactory::GetIntegerValue(1));
              break;
            case WindowFunctionType::CountAggregate:
              if (!val.IsNull())
                acc = acc.IsNull() ? ValueFactory::GetIntegerValue(1) : acc.Add(ValueFactory::GetIntegerValue(1));
              break;
            case WindowFunctionType::SumAggregate:
              if (!val.IsNull()) acc = acc.IsNull() ? val : acc.Add(val);
              break;
            case WindowFunctionType::MinAggregate:
              if (!val.IsNull()) acc = (acc.IsNull() || val.CompareLessThan(acc) == CmpBool::CmpTrue) ? val : acc;
              break;
            case WindowFunctionType::MaxAggregate:
              if (!val.IsNull()) acc = (acc.IsNull() || val.CompareGreaterThan(acc) == CmpBool::CmpTrue) ? val : acc;
              break;
            default:
              break;
          }
        }
        if (acc.IsNull() &&
            (wf.type_ == WindowFunctionType::CountAggregate || wf.type_ == WindowFunctionType::CountStarAggregate)) {
          acc = ValueFactory::GetIntegerValue(0);
        }
        for (uint32_t i = start; i < end; ++i) results[original_indices[i]] = acc;
        start = end;
      }
    } else {
      uint32_t start = 0;
      while (start < num_tuples) {
        uint32_t end = start + 1;
        while (end < num_tuples && is_same_partition(original_indices[start], original_indices[end])) {
          end++;
        }

        Value acc;
        uint32_t partition_count = 0;
        uint32_t current_rank = 1;

        uint32_t i = start;
        while (i < end) {
          uint32_t peer_group_end = i + 1;
          while (peer_group_end < end && is_same_order(original_indices[i], original_indices[peer_group_end])) {
            peer_group_end++;
          }

          if (wf.type_ == WindowFunctionType::Rank) {
            current_rank = (i - start) + 1;
            for (uint32_t j = i; j < peer_group_end; ++j) {
              results[original_indices[j]] = ValueFactory::GetIntegerValue(current_rank);
            }
          } else {
            // BusTub reference solution seems to use RANGE behavior for aggregates with ORDER BY.
            // Let's compute up to the end of the peer group.
            for (uint32_t j = i; j < peer_group_end; ++j) {
              Value val =
                  wf.function_->Evaluate(&child_tuples[original_indices[j]], child_executor_->GetOutputSchema());
              if (partition_count == 0 && j == i) {
                if (wf.type_ == WindowFunctionType::CountStarAggregate)
                  acc = ValueFactory::GetIntegerValue(0);
                else
                  acc = ValueFactory::GetNullValueByType(val.GetTypeId());
              }
              partition_count++;
              switch (wf.type_) {
                case WindowFunctionType::CountStarAggregate:
                  acc = acc.Add(ValueFactory::GetIntegerValue(1));
                  break;
                case WindowFunctionType::CountAggregate:
                  if (!val.IsNull())
                    acc = acc.IsNull() ? ValueFactory::GetIntegerValue(1) : acc.Add(ValueFactory::GetIntegerValue(1));
                  break;
                case WindowFunctionType::SumAggregate:
                  if (!val.IsNull()) acc = acc.IsNull() ? val : acc.Add(val);
                  break;
                case WindowFunctionType::MinAggregate:
                  if (!val.IsNull()) acc = (acc.IsNull() || val.CompareLessThan(acc) == CmpBool::CmpTrue) ? val : acc;
                  break;
                case WindowFunctionType::MaxAggregate:
                  if (!val.IsNull())
                    acc = (acc.IsNull() || val.CompareGreaterThan(acc) == CmpBool::CmpTrue) ? val : acc;
                  break;
                default:
                  break;
              }
            }
            Value res = acc;
            if (res.IsNull() && (wf.type_ == WindowFunctionType::CountAggregate ||
                                 wf.type_ == WindowFunctionType::CountStarAggregate)) {
              res = ValueFactory::GetIntegerValue(0);
            }
            for (uint32_t j = i; j < peer_group_end; ++j) {
              results[original_indices[j]] = res;
            }
          }
          i = peer_group_end;
        }
        start = end;
      }
    }
    window_results[col_idx] = std::move(results);
  }

  computed_tuples_.reserve(num_tuples);
  for (uint32_t i = 0; i < num_tuples; ++i) {
    uint32_t idx = original_indices[i];
    std::vector<Value> values;
    values.reserve(plan_->columns_.size());
    for (uint32_t col_idx = 0; col_idx < plan_->columns_.size(); ++col_idx) {
      if (plan_->window_functions_.count(col_idx) > 0) {
        values.push_back(window_results[col_idx][idx]);
      } else {
        values.push_back(plan_->columns_[col_idx]->Evaluate(&child_tuples[idx], child_executor_->GetOutputSchema()));
      }
    }
    computed_tuples_.emplace_back(values, &plan_->OutputSchema());
  }
}

auto WindowFunctionExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (cursor_ < computed_tuples_.size() && tuple_batch->size() < batch_size) {
    tuple_batch->push_back(computed_tuples_[cursor_]);
    rid_batch->push_back(computed_tuples_[cursor_].GetRid());
    cursor_++;
  }

  return !tuple_batch->empty();
}
}  // namespace bustub
