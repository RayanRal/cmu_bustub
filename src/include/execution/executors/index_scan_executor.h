//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "catalog/catalog.h"
#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  std::shared_ptr<TableInfo> table_info_;
  std::shared_ptr<IndexInfo> index_info_;
  BPlusTreeIndexForTwoIntegerColumn *tree_;
  std::vector<RID> rids_;
  size_t rid_idx_{0};
  std::unique_ptr<BPlusTreeIndexIteratorForTwoIntegerColumn> iter_;
  bool is_point_lookup_{false};
  /**
   * True for a range scan (`plan_->range_bounds_` non-empty): iteration starts at the lower-bound key and stops
   * past the upper-bound key. Column-level inclusivity needs no handling here: the seek/termination keys treat
   * exclusive endpoints as inclusive (a safe superset) and `plan_->filter_predicate_` rejects the rest per tuple.
   */
  bool is_range_scan_{false};
  /** Inclusive upper-bound key for early termination of a range scan. */
  IntegerKeyType_BTree high_key_;
  /** Comparator for range-scan bound checks, constructed over the index key schema. */
  std::optional<IntegerComparatorType_BTree> comparator_;
};
}  // namespace bustub
