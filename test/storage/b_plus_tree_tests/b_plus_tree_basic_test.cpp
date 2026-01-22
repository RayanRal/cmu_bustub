#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test/include/test_util.h"

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeBasicTest, IsEmptyTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  page_id_t page_id = bpm->NewPage();
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator);

  EXPECT_TRUE(tree.IsEmpty());

  GenericKey<8> key;
  RID rid;
  key.SetFromInteger(1);
  rid.Set(1, 1);

  tree.Insert(key, rid);
  EXPECT_FALSE(tree.IsEmpty());

  tree.Remove(key);
  EXPECT_TRUE(tree.IsEmpty());

  delete bpm;
}

TEST(BPlusTreeBasicTest, GetValueTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  page_id_t page_id = bpm->NewPage();
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator);

  GenericKey<8> key;
  RID rid;
  std::vector<RID> result;

  // Not found (empty)
  key.SetFromInteger(1);
  EXPECT_FALSE(tree.GetValue(key, &result));

  // Insert and find
  rid.Set(1, 100);
  tree.Insert(key, rid);

  result.clear();
  EXPECT_TRUE(tree.GetValue(key, &result));
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].GetPageId(), 1);
  EXPECT_EQ(result[0].GetSlotNum(), 100);

  // Not found (non-existent)
  key.SetFromInteger(2);
  result.clear();
  EXPECT_FALSE(tree.GetValue(key, &result));
  EXPECT_EQ(result.size(), 0);

  delete bpm;
}

TEST(BPlusTreeBasicTest, GetRootPageIdTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  page_id_t header_page_id = bpm->NewPage();
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page_id, bpm, comparator);

  EXPECT_EQ(tree.GetRootPageId(), INVALID_PAGE_ID);

  GenericKey<8> key;
  RID rid;
  key.SetFromInteger(1);
  rid.Set(1, 1);

  tree.Insert(key, rid);

  page_id_t root_id = tree.GetRootPageId();
  EXPECT_NE(root_id, INVALID_PAGE_ID);

  delete bpm;
}

}  // namespace bustub
