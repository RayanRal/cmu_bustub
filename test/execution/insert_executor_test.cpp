//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor_test.cpp
//
// Identification: test/execution/insert_executor_test.cpp
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
#include "execution/executors/insert_executor.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/executors/values_executor.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/insert_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/values_plan.h"
#include "gtest/gtest.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(InsertExecutorTest, SimpleInsertTest) {
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

  // Create ValuesPlanNode
  std::vector<std::vector<AbstractExpressionRef>> values;
  for (int i = 0; i < 5; ++i) {
    std::vector<AbstractExpressionRef> row;
    row.emplace_back(std::make_shared<ConstantValueExpression>(ValueFactory::GetIntegerValue(i)));
    row.emplace_back(
        std::make_shared<ConstantValueExpression>(ValueFactory::GetVarcharValue("val" + std::to_string(i))));
    values.push_back(row);
  }

  auto values_plan = std::make_shared<ValuesPlanNode>(std::make_shared<Schema>(schema), values);

  // Create InsertPlanNode
  // Insert returns count (INTEGER)
  Schema insert_output_schema({Column("__bustub_internal.insert_rows", TypeId::INTEGER)});
  auto insert_plan =
      std::make_unique<InsertPlanNode>(std::make_shared<Schema>(insert_output_schema), values_plan, table_info->oid_);

  // Create ExecutorContext
  auto exec_ctx = std::make_unique<ExecutorContext>(txn, catalog, bpm, txn_mgr, lock_mgr, false);

  // Create ValuesExecutor (child)
  auto values_executor = std::make_unique<ValuesExecutor>(exec_ctx.get(), values_plan.get());

  // Create InsertExecutor
  auto insert_executor =
      std::make_unique<InsertExecutor>(exec_ctx.get(), insert_plan.get(), std::move(values_executor));

  // Init
  insert_executor->Init();

  // Execute
  std::vector<Tuple> result_tuples;
  std::vector<RID> result_rids;

  ASSERT_TRUE(insert_executor->Next(&result_tuples, &result_rids, 1));
  ASSERT_EQ(result_tuples.size(), 1);
  ASSERT_EQ(result_tuples[0].GetValue(&insert_output_schema, 0).GetAs<int32_t>(), 5);

  ASSERT_FALSE(insert_executor->Next(&result_tuples, &result_rids, 1));

  // Verify content via SeqScan
  auto seq_plan =
      std::make_unique<SeqScanPlanNode>(std::make_shared<Schema>(schema), table_info->oid_, table_info->name_);
  auto seq_executor = std::make_unique<SeqScanExecutor>(exec_ctx.get(), seq_plan.get());

  seq_executor->Init();

  int count = 0;
  std::vector<Tuple> scan_tuples;
  std::vector<RID> scan_rids;
  while (seq_executor->Next(&scan_tuples, &scan_rids, 10)) {
    count += scan_tuples.size();
    scan_tuples.clear();
  }

  ASSERT_EQ(count, 5);

  // Cleanup
  txn_mgr->Commit(txn);
}

}  // namespace bustub
