//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor_test.cpp
//
// Identification: test/execution/seq_scan_executor_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <vector>

#include "common/bustub_instance.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "gtest/gtest.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(SeqScanExecutorTest, SimpleSeqScanTest) {
  // Initialize the system
  auto bustub_instance = std::make_unique<BusTubInstance>();
  auto *catalog = bustub_instance->catalog_.get();
  auto *txn_mgr = bustub_instance->txn_manager_.get();
  auto *bpm = bustub_instance->buffer_pool_manager_.get();
  auto *lock_mgr = bustub_instance->lock_manager_.get();

  // Create a transaction
  auto *txn = txn_mgr->Begin();

  // Create a table schema
  Column col1{"a", TypeId::INTEGER};
  Column col2{"b", TypeId::VARCHAR, 20};
  Schema schema({col1, col2});
  auto table_info = catalog->CreateTable(txn, "test_table", schema);

  // Insert some tuples
  std::vector<Tuple> inserted_tuples;
  for (int i = 0; i < 10; ++i) {
    std::vector<Value> values;
    values.emplace_back(ValueFactory::GetIntegerValue(i));
    values.emplace_back(ValueFactory::GetVarcharValue("value_" + std::to_string(i)));
    Tuple tuple(values, &schema);

    auto rid = table_info->table_->InsertTuple(TupleMeta{0, false}, tuple);
    ASSERT_TRUE(rid.has_value());
    inserted_tuples.push_back(tuple);
  }

  // Commit the transaction (although for SeqScan read uncommitted is often default in simple tests, we stick to the
  // flow)
  txn_mgr->Commit(txn);

  // Create a new transaction for scanning
  auto *scan_txn = txn_mgr->Begin();

  // Create the plan node
  auto plan = std::make_unique<SeqScanPlanNode>(std::make_shared<Schema>(schema), table_info->oid_, table_info->name_);

  // Create the executor context
  auto exec_ctx = std::make_unique<ExecutorContext>(scan_txn, catalog, bpm, txn_mgr, lock_mgr, false);

  // Create the executor
  auto executor = std::make_unique<SeqScanExecutor>(exec_ctx.get(), plan.get());

  // Initialize the executor
  executor->Init();

  // Iterate and verify
  std::vector<Tuple> result_tuples;
  std::vector<Tuple> batch_tuples;
  std::vector<RID> batch_rids;

  while (executor->Next(&batch_tuples, &batch_rids, 5)) {  // Use a small batch size
    for (const auto &tuple : batch_tuples) {
      result_tuples.push_back(tuple);
    }
  }

  // Verify the results
  ASSERT_EQ(result_tuples.size(), inserted_tuples.size());
  for (size_t i = 0; i < result_tuples.size(); ++i) {
    auto &t1 = inserted_tuples[i];
    auto &t2 = result_tuples[i];

    // Check content equality (simplified check based on values)
    EXPECT_EQ(t1.GetValue(&schema, 0).GetAs<int32_t>(), t2.GetValue(&schema, 0).GetAs<int32_t>());
    EXPECT_EQ(t1.GetValue(&schema, 1).GetAs<std::string>(), t2.GetValue(&schema, 1).GetAs<std::string>());
  }

  // Cleanup
  txn_mgr->Commit(scan_txn);
}

// NOLINTNEXTLINE
TEST(SeqScanExecutorTest, SeqScanDeletedTupleTest) {
  // Initialize the system
  auto bustub_instance = std::make_unique<BusTubInstance>();
  auto *catalog = bustub_instance->catalog_.get();
  auto *txn_mgr = bustub_instance->txn_manager_.get();
  auto *bpm = bustub_instance->buffer_pool_manager_.get();
  auto *lock_mgr = bustub_instance->lock_manager_.get();

  // Create a transaction
  auto *txn = txn_mgr->Begin();

  // Create a table schema
  Column col1{"a", TypeId::INTEGER};
  Schema schema({col1});
  auto table_info = catalog->CreateTable(txn, "test_table_deleted", schema);

  // Insert tuples
  std::vector<RID> rids;
  for (int i = 0; i < 5; ++i) {
    std::vector<Value> values;
    values.emplace_back(ValueFactory::GetIntegerValue(i));
    Tuple tuple(values, &schema);
    auto rid = table_info->table_->InsertTuple(TupleMeta{0, false}, tuple);
    ASSERT_TRUE(rid.has_value());
    rids.push_back(*rid);
  }

  // Delete the 3rd tuple (index 2, value 2)
  auto delete_rid = rids[2];
  table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, delete_rid);

  // Commit the transaction
  txn_mgr->Commit(txn);

  // Create a new transaction for scanning
  auto *scan_txn = txn_mgr->Begin();

  // Create the plan node
  auto plan = std::make_unique<SeqScanPlanNode>(std::make_shared<Schema>(schema), table_info->oid_, table_info->name_);

  // Create the executor context
  auto exec_ctx = std::make_unique<ExecutorContext>(scan_txn, catalog, bpm, txn_mgr, lock_mgr, false);

  // Create the executor
  auto executor = std::make_unique<SeqScanExecutor>(exec_ctx.get(), plan.get());

  // Initialize the executor
  executor->Init();

  // Iterate and verify
  std::vector<Tuple> result_tuples;
  std::vector<Tuple> batch_tuples;
  std::vector<RID> batch_rids;

  while (executor->Next(&batch_tuples, &batch_rids, 5)) {
    for (const auto &tuple : batch_tuples) {
      result_tuples.push_back(tuple);
    }
  }

  // Verify the results
  // We inserted 5, deleted 1, so we expect 4.
  ASSERT_EQ(result_tuples.size(), 4);

  // Verify values (should be 0, 1, 3, 4)
  std::vector<int> expected_values = {0, 1, 3, 4};
  for (size_t i = 0; i < result_tuples.size(); ++i) {
    EXPECT_EQ(result_tuples[i].GetValue(&schema, 0).GetAs<int32_t>(), expected_values[i]);
  }

  // Cleanup
  txn_mgr->Commit(scan_txn);
}

}  // namespace bustub
