#include "test_util.h"  // NOLINT
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeEdgeTest, EmptyTreeOperationsTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator);

  GenericKey<8> key;
  key.SetFromInteger(1);

  // Remove on empty
  tree.Remove(key);
  EXPECT_TRUE(tree.IsEmpty());

  // GetValue on empty
  std::vector<RID> result;
  EXPECT_FALSE(tree.GetValue(key, &result));

  // Iterator on empty
  auto it = tree.Begin();
  EXPECT_TRUE(it.IsEnd());
  EXPECT_EQ(it, tree.End());

  key.SetFromInteger(1);
  auto it2 = tree.Begin(key);
  EXPECT_TRUE(it2.IsEnd());
  EXPECT_EQ(it2, tree.End());

  delete bpm;
}

}  // namespace bustub
