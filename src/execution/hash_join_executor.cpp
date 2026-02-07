//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/macros.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)),
      left_partitions_(NUM_PARTITIONS),
      right_partitions_(NUM_PARTITIONS) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

HashJoinExecutor::~HashJoinExecutor() { CleanupPartitions(); }

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();

  CleanupPartitions();
  left_partitions_.assign(NUM_PARTITIONS, {});
  right_partitions_.assign(NUM_PARTITIONS, {});

  PartitionRelations();

  current_partition_idx_ = 0;
  ht_.clear();
  probe_tuples_.clear();
  probe_idx_ = 0;
  current_matches_.clear();
  match_idx_ = 0;
  matched_ = false;

  PrepareNextPartition();
}

void HashJoinExecutor::PartitionRelations() {
  auto bpm = exec_ctx_->GetBufferPoolManager();
  std::vector<Tuple> child_batch;
  std::vector<RID> rid_batch;

  auto append_to_partition = [bpm](std::vector<page_id_t> &partition, const Tuple &tuple) {
    if (partition.empty()) {
      page_id_t page_id = bpm->NewPage();
      auto page_guard = bpm->WritePage(page_id);
      auto result_page = page_guard.AsMut<IntermediateResultPage>();
      result_page->Init();
      partition.push_back(page_id);
    }

    page_id_t last_page_id = partition.back();
    {
      auto page_guard = bpm->WritePage(last_page_id);
      auto result_page = page_guard.AsMut<IntermediateResultPage>();
      if (result_page->InsertTuple(tuple)) {
        return;
      }
    }

    // No space in last page, create new page
    page_id_t new_page_id = bpm->NewPage();
    auto page_guard = bpm->WritePage(new_page_id);
    auto result_page = page_guard.AsMut<IntermediateResultPage>();
    result_page->Init();
    result_page->InsertTuple(tuple);
    partition.push_back(new_page_id);
  };

  while (left_child_->Next(&child_batch, &rid_batch, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : child_batch) {
      HashJoinKey key = MakeLeftJoinKey(&tuple);
      size_t p_idx = std::hash<HashJoinKey>()(key) % NUM_PARTITIONS;
      append_to_partition(left_partitions_[p_idx], tuple);
    }
  }

  while (right_child_->Next(&child_batch, &rid_batch, BUSTUB_BATCH_SIZE)) {
    for (const auto &tuple : child_batch) {
      HashJoinKey key = MakeRightJoinKey(&tuple);
      size_t p_idx = std::hash<HashJoinKey>()(key) % NUM_PARTITIONS;
      append_to_partition(right_partitions_[p_idx], tuple);
    }
  }
}

auto HashJoinExecutor::PrepareNextPartition() -> bool {
  auto bpm = exec_ctx_->GetBufferPoolManager();
  auto load_partition = [bpm](const std::vector<page_id_t> &partition, std::vector<Tuple> &tuples) {
    tuples.clear();
    for (auto page_id : partition) {
      auto page_guard = bpm->ReadPage(page_id);
      auto result_page = page_guard.As<IntermediateResultPage>();
      for (uint32_t i = 0; i < result_page->GetNumTuples(); ++i) {
        tuples.push_back(result_page->GetTuple(i));
      }
    }
  };

  while (current_partition_idx_ < NUM_PARTITIONS) {
    ht_.clear();
    probe_tuples_.clear();

    std::vector<Tuple> build_tuples;
    load_partition(right_partitions_[current_partition_idx_], build_tuples);
    for (const auto &tuple : build_tuples) {
      ht_[MakeRightJoinKey(&tuple)].push_back(tuple);
    }

    load_partition(left_partitions_[current_partition_idx_], probe_tuples_);
    current_partition_idx_++;

    if (!probe_tuples_.empty()) {
      probe_idx_ = 0;
      match_idx_ = 0;
      matched_ = false;
      current_matches_.clear();
      return true;
    }
  }
  return false;
}

void HashJoinExecutor::CleanupPartitions() {
  auto bpm = exec_ctx_->GetBufferPoolManager();
  for (auto &partition : left_partitions_) {
    for (auto page_id : partition) {
      bpm->DeletePage(page_id);
    }
    partition.clear();
  }
  for (auto &partition : right_partitions_) {
    for (auto page_id : partition) {
      bpm->DeletePage(page_id);
    }
    partition.clear();
  }
}

auto HashJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                            size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (tuple_batch->size() < batch_size) {
    if (probe_idx_ >= probe_tuples_.size()) {
      if (!PrepareNextPartition()) {
        break;
      }
    }

    const auto &probe_tuple = probe_tuples_[probe_idx_];

    if (match_idx_ == 0) {
      HashJoinKey probe_key = MakeLeftJoinKey(&probe_tuple);
      if (ht_.count(probe_key) > 0) {
        current_matches_ = ht_[probe_key];
        matched_ = true;
      } else {
        current_matches_.clear();
        matched_ = false;
      }
    }

    if (match_idx_ < current_matches_.size()) {
      const auto &build_tuple = current_matches_[match_idx_];
      std::vector<Value> values;
      values.reserve(left_child_->GetOutputSchema().GetColumnCount() +
                     right_child_->GetOutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(probe_tuple.GetValue(&left_child_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(build_tuple.GetValue(&right_child_->GetOutputSchema(), i));
      }
      tuple_batch->emplace_back(values, &GetOutputSchema());
      rid_batch->emplace_back();
      match_idx_++;
    } else {
      if (!matched_ && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        values.reserve(left_child_->GetOutputSchema().GetColumnCount() +
                       right_child_->GetOutputSchema().GetColumnCount());
        for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(probe_tuple.GetValue(&left_child_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
          values.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
        }
        tuple_batch->emplace_back(values, &GetOutputSchema());
        rid_batch->emplace_back();
      }
      probe_idx_++;
      match_idx_ = 0;
      matched_ = false;
    }
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
