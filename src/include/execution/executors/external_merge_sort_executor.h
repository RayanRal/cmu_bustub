//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/page/page_guard.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}

  auto GetPageCount() -> size_t { return pages_.size(); }

  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;
    template <size_t K>
    friend class ExternalMergeSortExecutor;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     */
    auto operator++() -> Iterator & {
      tuple_idx_++;
      if (tuple_idx_ >= num_tuples_in_page_) {
        page_idx_++;
        tuple_idx_ = 0;
        if (page_idx_ < run_->pages_.size()) {
          auto page_guard = run_->bpm_->ReadPage(run_->pages_[page_idx_]);
          auto page = page_guard.As<IntermediateResultPage>();
          num_tuples_in_page_ = page->GetNumTuples();
          current_page_guard_ = std::move(page_guard);
        } else {
          num_tuples_in_page_ = 0;
          current_page_guard_ = std::nullopt;
        }
      }
      return *this;
    }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     */
    auto operator*() -> Tuple {
      auto page = current_page_guard_->As<IntermediateResultPage>();
      return page->GetTuple(tuple_idx_);
    }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     */
    auto operator==(const Iterator &other) const -> bool {
      return run_ == other.run_ && page_idx_ == other.page_idx_ && tuple_idx_ == other.tuple_idx_;
    }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     */
    auto operator!=(const Iterator &other) const -> bool { return !(*this == other); }

   private:
    explicit Iterator(const MergeSortRun *run, size_t page_idx, uint32_t tuple_idx)
        : run_(run), page_idx_(page_idx), tuple_idx_(tuple_idx) {
      if (run_ != nullptr && page_idx_ < run_->pages_.size()) {
        auto page_guard = run_->bpm_->ReadPage(run_->pages_[page_idx_]);
        auto page = page_guard.As<IntermediateResultPage>();
        num_tuples_in_page_ = page->GetNumTuples();
        current_page_guard_ = std::move(page_guard);
      }
    }

    /** The sorted run that the iterator is iterating on. */
    const MergeSortRun *run_{nullptr};

    size_t page_idx_{0};
    uint32_t tuple_idx_{0};
    uint32_t num_tuples_in_page_{0};
    std::optional<ReadPageGuard> current_page_guard_{std::nullopt};
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   */
  auto Begin() -> Iterator { return Iterator(this, 0, 0); }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   */
  auto End() -> Iterator { return Iterator(this, pages_.size(), 0); }

  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;

  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  BufferPoolManager *bpm_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Spring 2025, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  ~ExternalMergeSortExecutor() override;

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  /** Child executor */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** Sorted runs */
  std::optional<MergeSortRun> final_run_;

  /** Iterator for the final run */
  MergeSortRun::Iterator final_iterator_;
};

}  // namespace bustub
