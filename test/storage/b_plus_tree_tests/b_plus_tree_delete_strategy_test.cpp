#include "storage/index/b_plus_tree.h"
#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "gtest/gtest.h"
#include "test_util.h"

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeDeleteStrategyTest, SimpleDeleteTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator);

  GenericKey<8> key;
  RID rid;
  key.SetFromInteger(1); rid.Set(1, 1);
  tree.Insert(key, rid);

  std::vector<RID> result;
  EXPECT_TRUE(tree.GetValue(key, &result));
  
  tree.Remove(key);
  result.clear();
  EXPECT_FALSE(tree.GetValue(key, &result));

  delete bpm;
}

TEST(BPlusTreeDeleteStrategyTest, LeafCoalesceTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();
  // Leaf Max = 3. Min = 1.
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 3);

  GenericKey<8> key;
  RID rid;

  // Insert 1, 2, 3, 4, 5.
  // 1, 2 -> L1 [1, 2]
  // 3 -> L1 [1, 2, 3]
  // 4 -> Split. L1 [1, 2], L2 [3, 4]
  // 5 -> L2 [3, 4, 5]
  
  for (int i = 1; i <= 5; ++i) {
    key.SetFromInteger(i); rid.Set(1, i);
    tree.Insert(key, rid);
  }

  // Current State:
  // Root -> L1 [1, 2], L2 [3, 4, 5].
  
  // Delete 1. L1 [2]. Size 1. (At Min).
  key.SetFromInteger(1); tree.Remove(key);
  
  // Delete 2. L1 []. Underflow.
  // Sibling L2 [3, 4, 5]. Size 3. Min 1.
  // Can borrow? Yes. Borrow 3 from L2.
  // This is Redistribute, not Coalesce.
  key.SetFromInteger(2); tree.Remove(key);
  
  // L1 [3]. L2 [4, 5].
  // Check values.
  std::vector<RID> result;
  key.SetFromInteger(3); EXPECT_TRUE(tree.GetValue(key, &result));

  // Now force Coalesce (Merge).
  // L1 [3]. L2 [4, 5].
  // Delete 4 from L2. L2 [5]. Size 1.
  key.SetFromInteger(4); tree.Remove(key);
  
  // L1 [3]. L2 [5].
  // Delete 5 from L2. L2 []. Underflow.
  // Sibling L1 [3]. Size 1. Min 1.
  // Can L1 spare? If it gives 3, it becomes 0. No.
  // So Merge L1 and L2.
  // Result: L1 [3]. (L2 gone).
  // Root -> L1.
  
  key.SetFromInteger(5); tree.Remove(key);
  
  // Verify 3 exists.
  key.SetFromInteger(3); EXPECT_TRUE(tree.GetValue(key, &result));

  // Verify page count decreased?
  // Hard to verify without peeking internals.
  // But if it crashed or lost keys, it would fail.

  delete bpm;
}

TEST(BPlusTreeDeleteStrategyTest, LeafRedistributeTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();
  // Leaf Max = 3. Min = 1.
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 3);

  GenericKey<8> key;
  RID rid;

  // Insert 1, 2, 3, 4.
  // L1 [1, 2], L2 [3, 4].
  for (int i = 1; i <= 4; ++i) {
    key.SetFromInteger(i); rid.Set(1, i);
    tree.Insert(key, rid);
  }

  // Delete 1. L1 [2]. Size 1.
  key.SetFromInteger(1); tree.Remove(key);
  
  // Delete 2. L1 []. Underflow.
  // L2 [3, 4]. Size 2. Min 1.
  // Borrow from L2?
  // If L2 gives 3. L2 [4] (Size 1 >= Min). OK.
  // L1 [3].
  key.SetFromInteger(2); tree.Remove(key);
  
  // Verify 3 is still there (moved to L1).
  std::vector<RID> result;
  key.SetFromInteger(3); EXPECT_TRUE(tree.GetValue(key, &result));
  
  // Verify 4 is there.
  result.clear();
  key.SetFromInteger(4); EXPECT_TRUE(tree.GetValue(key, &result));

  delete bpm;
}

TEST(BPlusTreeDeleteStrategyTest, InternalCoalesceTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();
  // Leaf Max = 3. Internal Max = 3 (Min 1).
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 3);

  GenericKey<8> key;
  RID rid;

  // Construct tree with 2 internal nodes (level 1) under root (level 2).
  // Need > 3 leaves.
  // L1, L2, L3, L4.
  // 1,2 -> L1
  // 3,4 -> L2
  // 5,6 -> L3
  // 7,8 -> L4
  // Root (Level 2) -> I1 [L1, L2], I2 [L3, L4]. (If Root Max=3, it can hold 3 children).
  // To get 2 internal nodes at level 1, Root must have split.
  // Root Max=3 implies split at 4 children.
  // So we need 4 leaves?
  // If 4 leaves: L1, L2, L3, L4.
  // Root: L1, L2, L3, L4 (Size 4 -> Split).
  // I1 [L1, L2], I2 [L3, L4]. New Root [I1, I2].
  
  for (int i = 1; i <= 8; ++i) {
    key.SetFromInteger(i); rid.Set(1, i);
    tree.Insert(key, rid);
  }
  
  // State:
  // Root (Level 2)
  //  -> I1 (Level 1) -> L1 [1, 2], L2 [3, 4]
  //  -> I2 (Level 1) -> L3 [5, 6], L4 [7, 8]
  
  key.SetFromInteger(8); tree.Remove(key);
  key.SetFromInteger(7); tree.Remove(key);
  
  // I2 has 1 child (L3).
  // Now empty L3.
  // Delete 6. L3 [5].
  key.SetFromInteger(6); tree.Remove(key);

  // This is Internal Redistribute.
  
  key.SetFromInteger(5); tree.Remove(key);
  
  // Now force Internal Merge.
  // Delete 3, 4 (L2).
  key.SetFromInteger(4); tree.Remove(key);
  key.SetFromInteger(3); tree.Remove(key);
  
  // Now tree should have 1, 2.
  std::vector<RID> result;
  key.SetFromInteger(1); EXPECT_TRUE(tree.GetValue(key, &result));
  key.SetFromInteger(2); result.clear(); EXPECT_TRUE(tree.GetValue(key, &result));
  
  // 3, 4, 5, 6, 7, 8 gone.
  key.SetFromInteger(3); result.clear(); EXPECT_FALSE(tree.GetValue(key, &result));

  delete bpm;
}

TEST(BPlusTreeDeleteStrategyTest, RootModificationTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator);

  GenericKey<8> key;
  RID rid;
  key.SetFromInteger(1); rid.Set(1, 1);
  tree.Insert(key, rid);

  tree.Remove(key);

  EXPECT_TRUE(tree.IsEmpty());

  tree.Insert(key, rid);
  EXPECT_FALSE(tree.IsEmpty());

  delete bpm;
}

} // namespace bustub
