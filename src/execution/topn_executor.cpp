//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.cpp
//
// Identification: src/execution/topn_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/topn_executor.h"
#include <algorithm>
#include "execution/execution_common.h"

namespace bustub {

/**
 * Construct a new TopNExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The TopN plan to be executed
 */
TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the TopN */
void TopNExecutor::Init() {
  child_executor_->Init();

  TupleComparator cmp(plan_->GetOrderBy());
  pq_ = std::make_unique<std::priority_queue<SortEntry, std::vector<SortEntry>, TupleComparator>>(cmp);

  std::vector<Tuple> batch;
  std::vector<RID> rids;
  while (child_executor_->Next(&batch, &rids, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : batch) {
      SortKey key = GenerateSortKey(tuple, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
      pq_->push({std::move(key), tuple});
      if (pq_->size() > plan_->GetN()) {
        pq_->pop();
      }
    }
    batch.clear();
    rids.clear();
  }

  top_entries_.clear();
  while (!pq_->empty()) {
    top_entries_.push_back(pq_->top().second);
    pq_->pop();
  }
  std::reverse(top_entries_.begin(), top_entries_.end());
  cursor_ = 0;
}

/**
 * Yield the next tuple batch from the TopN.
 * @param[out] tuple_batch The next tuple batch produced by the TopN
 * @param[out] rid_batch The next tuple RID batch produced by the TopN
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto TopNExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
    -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (cursor_ < top_entries_.size() && tuple_batch->size() < batch_size) {
    tuple_batch->push_back(top_entries_[cursor_]);
    rid_batch->push_back(top_entries_[cursor_].GetRid());
    cursor_++;
  }

  return !tuple_batch->empty();
}

auto TopNExecutor::GetNumInHeap() -> size_t {
  if (pq_) {
    return pq_->size();
  }
  return top_entries_.size();
}

}  // namespace bustub
