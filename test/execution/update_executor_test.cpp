//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor_test.cpp
//
// Identification: test/execution/update_executor_test.cpp
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
#include "execution/executors/update_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/update_plan.h"
#include "gtest/gtest.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(UpdateExecutorTest, SimpleUpdateTest) {
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

  // Insert some tuples initially
  for (int i = 0; i < 5; ++i) {
    std::vector<Value> values;
    values.emplace_back(ValueFactory::GetIntegerValue(i));
    values.emplace_back(ValueFactory::GetVarcharValue("original_" + std::to_string(i)));
    Tuple tuple(values, &schema);
    table_info->table_->InsertTuple(TupleMeta{0, false}, tuple);
  }

  txn_mgr->Commit(txn);

  // Create a new transaction for update
  auto *update_txn = txn_mgr->Begin();

  // Create SeqScanPlanNode as child of Update
  auto scan_plan =
      std::make_shared<SeqScanPlanNode>(std::make_shared<Schema>(schema), table_info->oid_, table_info->name_);

  // Create target expressions: col a stays same, col b becomes "modified"
  std::vector<AbstractExpressionRef> target_exprs;
  target_exprs.emplace_back(std::make_shared<ColumnValueExpression>(0, 0, col1));
  target_exprs.emplace_back(std::make_shared<ConstantValueExpression>(ValueFactory::GetVarcharValue("modified")));

  // Create UpdatePlanNode
  Schema update_output_schema({Column("__bustub_internal.update_rows", TypeId::INTEGER)});
  auto update_plan = std::make_unique<UpdatePlanNode>(std::make_shared<Schema>(update_output_schema), scan_plan,
                                                      table_info->oid_, target_exprs);

  // Create ExecutorContext
  auto exec_ctx = std::make_unique<ExecutorContext>(update_txn, catalog, bpm, txn_mgr, lock_mgr, false);

  // Create SeqScanExecutor (child)
  auto scan_executor = std::make_unique<SeqScanExecutor>(exec_ctx.get(), scan_plan.get());

  // Create UpdateExecutor
  auto update_executor = std::make_unique<UpdateExecutor>(exec_ctx.get(), update_plan.get(), std::move(scan_executor));

  // Init
  update_executor->Init();

  // Execute
  std::vector<Tuple> result_tuples;
  std::vector<RID> result_rids;

  ASSERT_TRUE(update_executor->Next(&result_tuples, &result_rids, 1));
  ASSERT_EQ(result_tuples.size(), 1);
  ASSERT_EQ(result_tuples[0].GetValue(&update_output_schema, 0).GetAs<int32_t>(), 5);

  ASSERT_FALSE(update_executor->Next(&result_tuples, &result_rids, 1));

  // Verify content via SeqScan
  auto verify_scan_plan =
      std::make_unique<SeqScanPlanNode>(std::make_shared<Schema>(schema), table_info->oid_, table_info->name_);
  auto verify_scan_executor = std::make_unique<SeqScanExecutor>(exec_ctx.get(), verify_scan_plan.get());

  verify_scan_executor->Init();

  int count = 0;
  std::vector<Tuple> scan_tuples;
  std::vector<RID> scan_rids;
  while (verify_scan_executor->Next(&scan_tuples, &scan_rids, 10)) {
    for (const auto &tuple : scan_tuples) {
      EXPECT_EQ(tuple.GetValue(&schema, 1).ToString(), "modified");
      count++;
    }
    scan_tuples.clear();
  }

  ASSERT_EQ(count, 5);

  // Cleanup
  txn_mgr->Commit(update_txn);
}

}  // namespace bustub
