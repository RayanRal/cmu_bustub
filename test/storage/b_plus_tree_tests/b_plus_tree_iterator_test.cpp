#include "test_util.h"  // NOLINT
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeIteratorTest, IteratorTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();
  // Small leaf to force splits/pages
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 3);

  GenericKey<8> key;
  RID rid;

  // Insert 1..10
  for (int i = 1; i <= 10; ++i) {
    key.SetFromInteger(i);
    rid.Set(1, i);
    tree.Insert(key, rid);
  }

  // 1. Begin() -> Smallest (1)
  auto iter = tree.Begin();
  EXPECT_FALSE(iter.IsEnd());
  EXPECT_EQ((*iter).first.GetAsInteger(), 1);
  EXPECT_EQ((*iter).second.GetSlotNum(), 1);

  // 2. Iteration (++)
  int expected = 1;
  for (auto it = tree.Begin(); it != tree.End(); ++it) {
    EXPECT_EQ((*it).first.GetAsInteger(), expected);
    EXPECT_EQ((*it).second.GetSlotNum(), expected);
    expected++;
  }
  EXPECT_EQ(expected, 11);

  // 3. Begin(key)
  // key = 5. Should point to 5.
  key.SetFromInteger(5);
  auto it5 = tree.Begin(key);
  EXPECT_EQ((*it5).first.GetAsInteger(), 5);

  // key = 0 (Smaller than min). Should point to 1.
  key.SetFromInteger(0);
  auto it0 = tree.Begin(key);
  EXPECT_EQ((*it0).first.GetAsInteger(), 1);

  // key = 11 (Larger than max). Should be End.
  key.SetFromInteger(11);
  auto it11 = tree.Begin(key);
  EXPECT_TRUE(it11.IsEnd());
  EXPECT_EQ(it11, tree.End());

  delete bpm;
}

}  // namespace bustub
