//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "common/config.h"
#include "execution/executors/aggregation_executor.h"

namespace bustub {

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_.Clear();

  bool has_tuple = false;
  std::vector<Tuple> child_batch;
  std::vector<RID> rid_batch;
  while (child_executor_->Next(&child_batch, &rid_batch, BUSTUB_BATCH_SIZE)) {
    has_tuple = true;
    for (const auto &tuple : child_batch) {
      aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
    }
  }

  if (!has_tuple && plan_->GetGroupBys().empty()) {
    aht_.InsertInitial(AggregateKey{});
  }

  aht_iterator_ = aht_.Begin();
}

/**
 * Yield the next tuple batch from the aggregation.
 * @param[out] tuple_batch The next batch of tuples produced by the aggregation
 * @param[out] rid_batch The next batch of tuple RIDs produced by the aggregation
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if any tuples were produced, `false` if there are no more tuples
 */

auto AggregationExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                               size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (aht_iterator_ != aht_.End() && tuple_batch->size() < batch_size) {
    auto &key = aht_iterator_.Key();
    auto &val = aht_iterator_.Val();

    std::vector<Value> values;
    values.reserve(key.group_bys_.size() + val.aggregates_.size());
    values.insert(values.end(), key.group_bys_.begin(), key.group_bys_.end());
    values.insert(values.end(), val.aggregates_.begin(), val.aggregates_.end());

    tuple_batch->emplace_back(values, &GetOutputSchema());
    rid_batch->emplace_back();

    ++aht_iterator_;
  }

  return !tuple_batch->empty();
}

/** Do not use or remove this function; otherwise, you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
