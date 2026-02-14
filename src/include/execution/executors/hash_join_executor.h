//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/** HashJoinKey represents a key in a hash join operation */
struct HashJoinKey {
  std::vector<Value> keys_;

  /**
   * Compares two hash join keys for equality.
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join keys have equivalent values, `false` otherwise
   */
  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.keys_.size(); i++) {
      if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
        if (!keys_[i].IsNull() || !other.keys_[i].IsNull()) {
          return false;
        }
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {

/** Implements std::hash on HashJoinKey */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a hash JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  ~HashJoinExecutor() override;

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** @return The tuple as a HashJoinKey */
  auto MakeLeftJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as a HashJoinKey */
  auto MakeRightJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(tuple, right_child_->GetOutputSchema()));
    }
    return {keys};
  }

  void PartitionRelations();
  auto PrepareNextPartition() -> bool;
  void CleanupPartitions();

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  /** Partitions for left and right relations */
  static constexpr size_t NUM_PARTITIONS = 10;
  std::vector<std::vector<page_id_t>> left_partitions_;
  std::vector<std::vector<page_id_t>> right_partitions_;

  /** Current join state */
  size_t current_partition_idx_{0};
  std::unordered_map<HashJoinKey, std::vector<Tuple>> ht_;
  std::vector<Tuple> probe_tuples_;
  size_t probe_idx_{0};

  /** For handling the current probe tuple matches */
  std::vector<Tuple> current_matches_;
  size_t match_idx_{0};
  bool matched_{false};
};

}  // namespace bustub
