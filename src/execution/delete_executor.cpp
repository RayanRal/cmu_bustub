//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the delete */
void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid()).get();
  is_finished_ = false;
}

/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the number of rows deleted from the table
 * @param[out] rid_batch The next tuple RID batch produced by the delete (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default: BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
 */
auto DeleteExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  if (is_finished_) {
    return false;
  }

  int32_t count = 0;
  std::vector<Tuple> child_tuple_batch;
  std::vector<RID> child_rid_batch;

  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  while (child_executor_->Next(&child_tuple_batch, &child_rid_batch, batch_size)) {
    for (size_t i = 0; i < child_tuple_batch.size(); ++i) {
      const auto &tuple = child_tuple_batch[i];
      const auto &rid = child_rid_batch[i];

      // Delete from table heap
      table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, rid);

      // Remove from indexes
      for (auto &index_info : table_indexes) {
        index_info->index_->DeleteEntry(
            tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), rid,
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
