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

  std::unordered_map<uint32_t, std::vector<Value>> window_results;

  for (const auto &pair : plan_->window_functions_) {
    uint32_t col_idx = pair.first;
    const auto &wf = pair.second;
    std::vector<Value> results(num_tuples);

    // Evaluate partition/order keys once per tuple; sort and peer checks reuse the materialized keys.
    const size_t num_part_keys = wf.partition_by_.size();
    const size_t num_order_keys = wf.order_by_.size();
    std::vector<Value> part_keys(num_tuples * num_part_keys);
    std::vector<Value> order_keys(num_tuples * num_order_keys);
    for (uint32_t i = 0; i < num_tuples; ++i) {
      for (size_t k = 0; k < num_part_keys; ++k) {
        part_keys[i * num_part_keys + k] =
            wf.partition_by_[k]->Evaluate(&child_tuples[i], child_executor_->GetOutputSchema());
      }
      for (size_t k = 0; k < num_order_keys; ++k) {
        order_keys[i * num_order_keys + k] =
            std::get<2>(wf.order_by_[k])->Evaluate(&child_tuples[i], child_executor_->GetOutputSchema());
      }
    }

    std::sort(original_indices.begin(), original_indices.end(), [&](uint32_t idx_a, uint32_t idx_b) {
      for (size_t k = 0; k < num_part_keys; ++k) {
        const Value &val_a = part_keys[idx_a * num_part_keys + k];
        const Value &val_b = part_keys[idx_b * num_part_keys + k];
        if (val_a.CompareLessThan(val_b) == CmpBool::CmpTrue) {
          return true;
        }
        if (val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue) {
          return false;
        }
      }

      for (size_t k = 0; k < num_order_keys; ++k) {
        OrderByType type = std::get<0>(wf.order_by_[k]);
        OrderByNullType null_type = std::get<1>(wf.order_by_[k]);
        const Value &val_a = order_keys[idx_a * num_order_keys + k];
        const Value &val_b = order_keys[idx_b * num_order_keys + k];

        if (val_a.IsNull() && val_b.IsNull()) {
          continue;
        }
        bool is_asc = (type == OrderByType::ASC || type == OrderByType::DEFAULT);
        bool nulls_first =
            (null_type == OrderByNullType::NULLS_FIRST || (null_type == OrderByNullType::DEFAULT && is_asc));

        if (val_a.IsNull()) {
          return nulls_first;
        }
        if (val_b.IsNull()) {
          return !nulls_first;
        }

        if (is_asc) {
          if (val_a.CompareLessThan(val_b) == CmpBool::CmpTrue) {
            return true;
          }
          if (val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue) {
            return false;
          }
        } else {
          if (val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue) {
            return true;
          }
          if (val_a.CompareLessThan(val_b) == CmpBool::CmpTrue) {
            return false;
          }
        }
      }
      return false;
    });

    auto is_same_partition = [&](uint32_t idx_a, uint32_t idx_b) {
      for (size_t k = 0; k < num_part_keys; ++k) {
        const Value &val_a = part_keys[idx_a * num_part_keys + k];
        const Value &val_b = part_keys[idx_b * num_part_keys + k];
        if (val_a.CompareEquals(val_b) != CmpBool::CmpTrue) {
          if (!val_a.IsNull() || !val_b.IsNull()) {
            return false;
          }
        }
      }
      return true;
    };

    auto is_same_order = [&](uint32_t idx_a, uint32_t idx_b) {
      for (size_t k = 0; k < num_order_keys; ++k) {
        const Value &val_a = order_keys[idx_a * num_order_keys + k];
        const Value &val_b = order_keys[idx_b * num_order_keys + k];
        if (val_a.CompareEquals(val_b) != CmpBool::CmpTrue) {
          if (!val_a.IsNull() || !val_b.IsNull()) {
            return false;
          }
        }
      }
      return true;
    };

    auto accumulate = [&](Value &acc, const Value &val) {
      switch (wf.type_) {
        case WindowFunctionType::CountStarAggregate:
          acc = acc.Add(ValueFactory::GetIntegerValue(1));
          break;
        case WindowFunctionType::CountAggregate:
          if (!val.IsNull()) {
            acc = acc.IsNull() ? ValueFactory::GetIntegerValue(1) : acc.Add(ValueFactory::GetIntegerValue(1));
          }
          break;
        case WindowFunctionType::SumAggregate:
          if (!val.IsNull()) {
            acc = acc.IsNull() ? val : acc.Add(val);
          }
          break;
        case WindowFunctionType::MinAggregate:
          if (!val.IsNull()) {
            acc = (acc.IsNull() || val.CompareLessThan(acc) == CmpBool::CmpTrue) ? val : acc;
          }
          break;
        case WindowFunctionType::MaxAggregate:
          if (!val.IsNull()) {
            acc = (acc.IsNull() || val.CompareGreaterThan(acc) == CmpBool::CmpTrue) ? val : acc;
          }
          break;
        default:
          throw NotImplementedException("unsupported window function type");
      }
    };

    auto finalize_acc = [&](const Value &acc) -> Value {
      if (acc.IsNull() &&
          (wf.type_ == WindowFunctionType::CountAggregate || wf.type_ == WindowFunctionType::CountStarAggregate)) {
        return ValueFactory::GetIntegerValue(0);
      }
      return acc;
    };

    auto init_acc = [&](const Value &first_val) -> Value {
      if (wf.type_ == WindowFunctionType::CountStarAggregate) {
        return ValueFactory::GetIntegerValue(0);
      }
      return ValueFactory::GetNullValueByType(first_val.GetTypeId());
    };

    if (wf.order_by_.empty()) {
      uint32_t start = 0;
      while (start < num_tuples) {
        uint32_t end = start + 1;
        while (end < num_tuples && is_same_partition(original_indices[start], original_indices[end])) {
          end++;
        }

        Value first_val =
            wf.function_->Evaluate(&child_tuples[original_indices[start]], child_executor_->GetOutputSchema());
        Value acc = init_acc(first_val);
        for (uint32_t i = start; i < end; ++i) {
          Value val = wf.function_->Evaluate(&child_tuples[original_indices[i]], child_executor_->GetOutputSchema());
          accumulate(acc, val);
        }
        acc = finalize_acc(acc);
        for (uint32_t i = start; i < end; ++i) {
          results[original_indices[i]] = acc;
        }
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
            for (uint32_t j = i; j < peer_group_end; ++j) {
              Value val =
                  wf.function_->Evaluate(&child_tuples[original_indices[j]], child_executor_->GetOutputSchema());
              if (partition_count == 0 && j == i) {
                acc = init_acc(val);
              }
              partition_count++;
              accumulate(acc, val);
            }
            Value res = finalize_acc(acc);
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
