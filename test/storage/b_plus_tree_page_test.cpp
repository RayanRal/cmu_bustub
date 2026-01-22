#include "storage/page/b_plus_tree_page.h"
#include "common/rid.h"
#include "gtest/gtest.h"
#include "storage/index/generic_key.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

TEST(BPlusTreePageTest, InternalPageTest) {
  char buf[BUSTUB_PAGE_SIZE];
  auto *internal_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(buf);
  internal_page->Init(10);

  EXPECT_FALSE(internal_page->IsLeafPage());
  EXPECT_EQ(internal_page->GetSize(), 0);
  EXPECT_EQ(internal_page->GetMaxSize(), 10);
  EXPECT_EQ(internal_page->GetMinSize(), 5);

  GenericKey<8> key;
  key.SetFromInteger(42);
  internal_page->SetKeyAt(1, key);
  EXPECT_EQ(internal_page->KeyAt(1).GetAsInteger(), 42);

  internal_page->SetValueAt(0, 100);
  internal_page->SetValueAt(1, 101);
  EXPECT_EQ(internal_page->ValueAt(0), 100);
  EXPECT_EQ(internal_page->ValueAt(1), 101);

  internal_page->SetSize(2);
  EXPECT_EQ(internal_page->ValueIndex(100), 0);
  EXPECT_EQ(internal_page->ValueIndex(101), 1);
  EXPECT_EQ(internal_page->ValueIndex(999), -1);
}

TEST(BPlusTreePageTest, LeafPageTest) {
  char buf[BUSTUB_PAGE_SIZE];
  auto *leaf_page = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(buf);
  leaf_page->Init(10);

  EXPECT_TRUE(leaf_page->IsLeafPage());
  EXPECT_EQ(leaf_page->GetSize(), 0);
  EXPECT_EQ(leaf_page->GetMaxSize(), 10);
  EXPECT_EQ(leaf_page->GetMinSize(), 5);
  EXPECT_EQ(leaf_page->GetNextPageId(), INVALID_PAGE_ID);

  leaf_page->SetNextPageId(50);
  EXPECT_EQ(leaf_page->GetNextPageId(), 50);

  GenericKey<8> key;
  key.SetFromInteger(123);
  leaf_page->SetKeyAt(0, key);
  EXPECT_EQ(leaf_page->KeyAt(0).GetAsInteger(), 123);

  RID rid(1, 2);
  leaf_page->SetValueAt(0, rid);
  EXPECT_EQ(leaf_page->ValueAt(0), rid);
}

TEST(BPlusTreePageTest, LeafPageTombstoneTest) {
  // Use a leaf page with NumTombs=2
  char buf[BUSTUB_PAGE_SIZE];
  auto *leaf_page = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2> *>(buf);
  leaf_page->Init(10);

  EXPECT_EQ(leaf_page->GetTombstoneCount(), 0);

  // Setup keys
  GenericKey<8> key0, key1, key2;
  key0.SetFromInteger(10);
  key1.SetFromInteger(20);
  key2.SetFromInteger(30);

  leaf_page->SetKeyAt(0, key0);
  leaf_page->SetKeyAt(1, key1);
  leaf_page->SetKeyAt(2, key2);
  leaf_page->SetSize(3);

  // Set tombstones
  leaf_page->SetTombstoneAt(0, 1);  // Tombstone points to key at index 1 (20)
  leaf_page->SetTombstoneCount(1);

  auto tombstones = leaf_page->GetTombstones();
  ASSERT_EQ(tombstones.size(), 1);
  EXPECT_EQ(tombstones[0].GetAsInteger(), 20);

  leaf_page->SetTombstoneAt(1, 0);  // Tombstone points to key at index 0 (10)
  leaf_page->SetTombstoneCount(2);

  tombstones = leaf_page->GetTombstones();
  ASSERT_EQ(tombstones.size(), 2);
  EXPECT_EQ(tombstones[0].GetAsInteger(), 20);
  EXPECT_EQ(tombstones[1].GetAsInteger(), 10);

  // Test bound check (if tombstone points to invalid index, e.g. >= size)
  leaf_page->SetTombstoneAt(1, 5);  // Index 5 is out of bounds (Size is 3)
  tombstones = leaf_page->GetTombstones();
  ASSERT_EQ(tombstones.size(), 1);
  EXPECT_EQ(tombstones[0].GetAsInteger(), 20);
}

}  // namespace bustub
