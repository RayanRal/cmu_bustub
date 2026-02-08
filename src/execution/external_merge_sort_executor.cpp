//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <algorithm>
#include <vector>
#include "common/macros.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/intermediate_result_page.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {}

template <size_t K>
ExternalMergeSortExecutor<K>::~ExternalMergeSortExecutor() {
  if (final_run_.has_value()) {
    auto bpm = exec_ctx_->GetBufferPoolManager();
    for (auto page_id : final_run_->pages_) {
      bpm->DeletePage(page_id);
    }
  }
}

/** Initialize the external merge sort */
template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  child_executor_->Init();
  auto bpm = exec_ctx_->GetBufferPoolManager();

  // Clean up if re-initialized
  if (final_run_.has_value()) {
    for (auto page_id : final_run_->pages_) {
      bpm->DeletePage(page_id);
    }
    final_run_ = std::nullopt;
  }

  std::vector<MergeSortRun> runs;
  std::vector<Tuple> current_page_tuples;
  uint32_t current_page_size = 0;

  auto create_run = [&]() {
    if (current_page_tuples.empty()) {
      return;
    }
    std::vector<SortEntry> sort_entries;
    sort_entries.reserve(current_page_tuples.size());
    for (auto &tuple : current_page_tuples) {
      sort_entries.emplace_back(GenerateSortKey(tuple, plan_->GetOrderBy(), child_executor_->GetOutputSchema()), tuple);
    }
    std::sort(sort_entries.begin(), sort_entries.end(), cmp_);

    page_id_t page_id = bpm->NewPage();
    auto page_guard = bpm->WritePage(page_id);
    auto page = page_guard.AsMut<IntermediateResultPage>();
    page->Init();
    for (const auto &entry : sort_entries) {
      bool success = page->InsertTuple(entry.second);
      BUSTUB_ASSERT(success, "Tuple should fit in page during initial run creation");
    }
    runs.emplace_back(std::vector<page_id_t>{page_id}, bpm);
    current_page_tuples.clear();
    current_page_size = 0;
  };

  std::vector<Tuple> tuple_batch;
  std::vector<RID> rid_batch;
  while (child_executor_->Next(&tuple_batch, &rid_batch, BUSTUB_BATCH_SIZE)) {
    for (auto &tuple : tuple_batch) {
      uint32_t tuple_size = tuple.GetLength() + sizeof(int32_t) + sizeof(uint32_t);
      if (8 + current_page_size + tuple_size > BUSTUB_PAGE_SIZE) {
        create_run();
      }
      current_page_size += tuple_size;
      current_page_tuples.push_back(std::move(tuple));
    }
    tuple_batch.clear();
    rid_batch.clear();
  }
  create_run();

  // Phase 2: Merge runs
  while (runs.size() > 1) {
    std::vector<MergeSortRun> next_runs;
    for (size_t i = 0; i < runs.size(); i += 2) {
      if (i + 1 < runs.size()) {
        std::vector<page_id_t> merged_pages;
        auto it1 = runs[i].Begin();
        auto it2 = runs[i + 1].Begin();
        auto end1 = runs[i].End();
        auto end2 = runs[i + 1].End();

        std::optional<SortKey> key1, key2;
        if (it1 != end1) {
          key1 = GenerateSortKey(*it1, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
        }
        if (it2 != end2) {
          key2 = GenerateSortKey(*it2, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
        }

        page_id_t current_page_id = bpm->NewPage();
        auto current_page_guard = bpm->WritePage(current_page_id);
        auto current_page = current_page_guard.AsMut<IntermediateResultPage>();
        current_page->Init();
        merged_pages.push_back(current_page_id);

        while (it1 != end1 || it2 != end2) {
          Tuple next_tuple;
          if (it1 != end1 && (it2 == end2 || cmp_({*key1, *it1}, {*key2, *it2}))) {
            next_tuple = *it1;
            ++it1;
            if (it1 != end1) {
              key1 = GenerateSortKey(*it1, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
            }
          } else {
            next_tuple = *it2;
            ++it2;
            if (it2 != end2) {
              key2 = GenerateSortKey(*it2, plan_->GetOrderBy(), child_executor_->GetOutputSchema());
            }
          }

          if (!current_page->InsertTuple(next_tuple)) {
            current_page_guard.Drop();
            current_page_id = bpm->NewPage();
            current_page_guard = bpm->WritePage(current_page_id);
            current_page = current_page_guard.AsMut<IntermediateResultPage>();
            current_page->Init();
            merged_pages.push_back(current_page_id);
            bool success = current_page->InsertTuple(next_tuple);
            BUSTUB_ASSERT(success, "Tuple should fit in a new page");
          }
        }
        current_page_guard.Drop();

        for (auto page_id : runs[i].pages_) {
          bpm->DeletePage(page_id);
        }
        for (auto page_id : runs[i + 1].pages_) {
          bpm->DeletePage(page_id);
        }
        next_runs.emplace_back(std::move(merged_pages), bpm);
      } else {
        next_runs.push_back(std::move(runs[i]));
      }
    }
    runs = std::move(next_runs);
  }

  if (!runs.empty()) {
    final_run_ = std::move(runs[0]);
    final_iterator_ = final_run_->Begin();
  }
}

/**
 * Yield the next tuple batch from the external merge sort.
 * @param[out] tuple_batch The next tuple batch produced by the external merge sort.
 * @param[out] rid_batch The next tuple RID batch produced by the external merge sort.
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                        size_t batch_size) -> bool {
  if (!final_run_.has_value()) {
    return false;
  }

  tuple_batch->clear();
  rid_batch->clear();

  auto end = final_run_->End();
  while (tuple_batch->size() < batch_size && final_iterator_ != end) {
    auto tuple = *final_iterator_;
    tuple_batch->push_back(tuple);
    rid_batch->push_back(tuple.GetRid());
    ++final_iterator_;
  }

  return !tuple_batch->empty();
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
