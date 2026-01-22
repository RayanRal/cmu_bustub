#include "storage/index/b_plus_tree_index.h"
#include "test_util.h"  // NOLINT
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeIndexTest, IndexTest) {
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  // Schema: id bigint
  auto schema = ParseCreateStatement("id bigint");
  std::vector<uint32_t> key_attrs = {0};

  // Metadata
  auto metadata = std::make_unique<IndexMetadata>("test_idx", "test_table", schema.get(), key_attrs, true);

  // BPlusTreeIndex
  // Use BPlusTreeIndexForTwoIntegerColumn which maps to GenericKey<8>
  // Schema is 1 column of 8 bytes (BigInt).
  // GenericKey<8> fits.
  auto index = std::make_unique<BPlusTreeIndexForTwoIntegerColumn>(std::move(metadata), bpm);

  // Tuple
  std::vector<Value> values;
  values.emplace_back(TypeId::BIGINT, 100);
  Tuple key_tuple(values, schema.get());

  RID rid(1, 1);

  // Insert
  EXPECT_TRUE(index->InsertEntry(key_tuple, rid, nullptr));

  // Scan
  std::vector<RID> result;
  index->ScanKey(key_tuple, &result, nullptr);
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0], rid);

  // Delete
  index->DeleteEntry(key_tuple, rid, nullptr);

  // Scan again
  result.clear();
  index->ScanKey(key_tuple, &result, nullptr);
  EXPECT_EQ(result.size(), 0);

  delete bpm;
}

}  // namespace bustub
