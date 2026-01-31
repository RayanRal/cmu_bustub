//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the insert */
void InsertExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
  is_finished_ = false;
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows inserted into the table
 * @param[out] rid_batch The next tuple RID batch produced by the insert (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with the number of inserted rows produced only once.
 */
auto InsertExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  if (is_finished_) {
    return false;
  }

  int32_t count = 0;
  std::vector<Tuple> child_tuple_batch;
  std::vector<RID> child_rid_batch;

  while (child_executor_->Next(&child_tuple_batch, &child_rid_batch, batch_size)) {
    for (const auto &tuple : child_tuple_batch) {
      // Insert into table heap
      std::optional<RID> rid = table_info_->table_->InsertTuple(TupleMeta{0, false}, tuple);
      if (!rid.has_value()) {
        continue;
      }

      // Update indexes
      auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto &index_info : table_indexes) {
        index_info->index_->InsertEntry(
            tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
            exec_ctx_->GetTransaction());
      }
      count++;
    }
    child_tuple_batch.clear();
    child_rid_batch.clear();
  }

  std::vector<Value> values;
  values.emplace_back(ValueFactory::GetIntegerValue(count));
  tuple_batch->emplace_back(Tuple(values, &GetOutputSchema()));

  is_finished_ = true;
  return true;
}

}  // namespace bustub
