#include <test_util.h>
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeInsertStrategyTest, SingleInsertTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator);

  GenericKey<8> key;
  RID rid;
  key.SetFromInteger(1);
  rid.Set(1, 1);

  EXPECT_TRUE(tree.Insert(key, rid));

  std::vector<RID> result;
  EXPECT_TRUE(tree.GetValue(key, &result));
  EXPECT_EQ(result.size(), 1);

  delete bpm;
}

TEST(BPlusTreeInsertStrategyTest, SequentialInsertTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator);

  GenericKey<8> key;
  RID rid;

  for (int i = 0; i < 100; ++i) {
    key.SetFromInteger(i);
    rid.Set(1, i);
    EXPECT_TRUE(tree.Insert(key, rid));
  }

  std::vector<RID> result;
  for (int i = 0; i < 100; ++i) {
    key.SetFromInteger(i);
    result.clear();
    EXPECT_TRUE(tree.GetValue(key, &result));
    EXPECT_EQ(result[0].GetSlotNum(), i);
  }

  delete bpm;
}

TEST(BPlusTreeInsertStrategyTest, ReverseInsertTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator);

  GenericKey<8> key;
  RID rid;

  for (int i = 99; i >= 0; --i) {
    key.SetFromInteger(i);
    rid.Set(1, i);
    EXPECT_TRUE(tree.Insert(key, rid));
  }

  std::vector<RID> result;
  for (int i = 0; i < 100; ++i) {
    key.SetFromInteger(i);
    result.clear();
    EXPECT_TRUE(tree.GetValue(key, &result));
    EXPECT_EQ(result[0].GetSlotNum(), i);
  }

  delete bpm;
}

TEST(BPlusTreeInsertStrategyTest, DuplicateInsertTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator);

  GenericKey<8> key;
  RID rid;
  key.SetFromInteger(1);
  rid.Set(1, 1);

  EXPECT_TRUE(tree.Insert(key, rid));
  EXPECT_FALSE(tree.Insert(key, rid));  // Duplicate

  delete bpm;
}

TEST(BPlusTreeInsertStrategyTest, LeafSplitTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();
  // leaf_max_size = 3.
  // Insert 1, 2. (Size 2). Insert 3 -> Full? Or Insert 3 -> Split?
  // Usually max size means capacity.
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 3);

  GenericKey<8> key;
  RID rid;

  // Insert 1, 2. Page has 2 keys.
  key.SetFromInteger(1);
  rid.Set(1, 1);
  tree.Insert(key, rid);
  key.SetFromInteger(2);
  rid.Set(1, 2);
  tree.Insert(key, rid);

  // Verify Root is Leaf
  page_id_t root_id = tree.GetRootPageId();
  auto page_guard = bpm->ReadPage(root_id);
  auto page = page_guard.As<BPlusTreePage>();
  EXPECT_TRUE(page->IsLeafPage());
  EXPECT_EQ(page->GetSize(), 2);
  page_guard.Drop();

  key.SetFromInteger(3);
  rid.Set(1, 3);
  tree.Insert(key, rid);

  key.SetFromInteger(4);
  rid.Set(1, 4);
  tree.Insert(key, rid);

  // Now it definitely should have split.
  root_id = tree.GetRootPageId();
  page_guard = bpm->ReadPage(root_id);
  page = page_guard.As<BPlusTreePage>();

  EXPECT_FALSE(page->IsLeafPage());

  // Check children
  auto internal = page_guard.As<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>>();
  EXPECT_EQ(internal->GetSize(), 2);  // 2 children (2 pointers)

  delete bpm;
}

TEST(BPlusTreeInsertStrategyTest, InternalSplitTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();
  // leaf_max_size = 3, internal_max_size = 2
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 2);

  GenericKey<8> key;
  RID rid;

  // Root must split.

  for (int i = 1; i <= 6; ++i) {
    key.SetFromInteger(i);
    rid.Set(1, i);
    tree.Insert(key, rid);
  }

  // Check Root
  page_id_t root_id = tree.GetRootPageId();
  auto page_guard = bpm->ReadPage(root_id);
  auto page = page_guard.As<BPlusTreePage>();

  EXPECT_FALSE(page->IsLeafPage());
  EXPECT_EQ(page->GetSize(), 2);  // New root should have 2 children

  delete bpm;
}

}  // namespace bustub
