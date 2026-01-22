#include "storage/page/b_plus_tree_leaf_page.h"
#include "common/rid.h"
#include "gtest/gtest.h"
#include "storage/index/generic_key.h"
#include "test_util.h"  // NOLINT

namespace bustub {

TEST(BPlusTreeLeafPageTest, InitTest) {
  char buf[BUSTUB_PAGE_SIZE];
  auto *leaf_page = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(buf);
  leaf_page->Init(10);

  EXPECT_TRUE(leaf_page->IsLeafPage());
  EXPECT_EQ(leaf_page->GetSize(), 0);
  EXPECT_EQ(leaf_page->GetMaxSize(), 10);
  EXPECT_EQ(leaf_page->GetNextPageId(), INVALID_PAGE_ID);
}

TEST(BPlusTreeLeafPageTest, DataManagementTest) {
  char buf[BUSTUB_PAGE_SIZE];
  auto *leaf_page = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(buf);
  leaf_page->Init(10);

  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  GenericKey<8> key;
  RID rid;

  // Insert
  key.SetFromInteger(10);
  rid.Set(1, 10);
  EXPECT_TRUE(leaf_page->Insert(key, rid, comparator));
  EXPECT_EQ(leaf_page->GetSize(), 1);

  key.SetFromInteger(5);
  rid.Set(1, 5);
  EXPECT_TRUE(leaf_page->Insert(key, rid, comparator));
  EXPECT_EQ(leaf_page->GetSize(), 2);

  key.SetFromInteger(15);
  rid.Set(1, 15);
  EXPECT_TRUE(leaf_page->Insert(key, rid, comparator));
  EXPECT_EQ(leaf_page->GetSize(), 3);

  // Check order (5, 10, 15)
  EXPECT_EQ(leaf_page->KeyAt(0).GetAsInteger(), 5);
  EXPECT_EQ(leaf_page->KeyAt(1).GetAsInteger(), 10);
  EXPECT_EQ(leaf_page->KeyAt(2).GetAsInteger(), 15);

  // Lookup
  key.SetFromInteger(10);
  int index = leaf_page->Lookup(key, comparator);
  EXPECT_NE(index, -1);
  EXPECT_EQ(leaf_page->KeyAt(index).GetAsInteger(), 10);
  EXPECT_EQ(leaf_page->ValueAt(index).GetSlotNum(), 10);

  key.SetFromInteger(7);
  EXPECT_EQ(leaf_page->Lookup(key, comparator), -1);

  // Remove
  key.SetFromInteger(10);
  EXPECT_TRUE(leaf_page->Remove(key, comparator));
  EXPECT_EQ(leaf_page->GetSize(), 2);
  EXPECT_EQ(leaf_page->KeyAt(0).GetAsInteger(), 5);
  EXPECT_EQ(leaf_page->KeyAt(1).GetAsInteger(), 15);

  EXPECT_FALSE(leaf_page->Remove(key, comparator));
  EXPECT_EQ(leaf_page->GetSize(), 2);
}

TEST(BPlusTreeLeafPageTest, TombstoneTest) {
  char buf[BUSTUB_PAGE_SIZE];
  // NumTombs = 3
  auto *leaf_page = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 3> *>(buf);
  leaf_page->Init(10);

  EXPECT_EQ(leaf_page->GetTombstoneCount(), 0);

  GenericKey<8> key;
  RID rid;
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  // Insert some keys
  for (int i = 0; i < 5; ++i) {
    key.SetFromInteger(i * 10);
    rid.Set(1, i * 10);
    leaf_page->Insert(key, rid, comparator);
  }
  // Keys: 0, 10, 20, 30, 40

  // Set Tombstone
  leaf_page->SetTombstoneAt(0, 2);  // Tombstone points to index 2 (key 20)
  leaf_page->SetTombstoneCount(1);

  auto tombstones = leaf_page->GetTombstones();
  ASSERT_EQ(tombstones.size(), 1);
  EXPECT_EQ(tombstones[0].GetAsInteger(), 20);

  leaf_page->SetTombstoneAt(1, 4);  // Tombstone points to index 4 (key 40)
  leaf_page->SetTombstoneCount(2);

  tombstones = leaf_page->GetTombstones();
  ASSERT_EQ(tombstones.size(), 2);
  EXPECT_EQ(tombstones[0].GetAsInteger(), 20);
  EXPECT_EQ(tombstones[1].GetAsInteger(), 40);
}

TEST(BPlusTreeLeafPageTest, MoveHalfToTest) {
  char buf1[BUSTUB_PAGE_SIZE];
  char buf2[BUSTUB_PAGE_SIZE];
  auto *page1 = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(buf1);
  auto *page2 = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(buf2);

  page1->Init(10);
  page2->Init(10);

  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  GenericKey<8> key;
  RID rid;

  // Fill page1 with 6 items: 10, 20, 30, 40, 50, 60
  for (int i = 1; i <= 6; ++i) {
    key.SetFromInteger(i * 10);
    rid.Set(1, i * 10);
    page1->Insert(key, rid, comparator);
  }

  // Move half to page2
  page1->MoveHalfTo(page2);

  // Expect splitting.
  // If size is 6, split usually leaves 3 and moves 3.
  EXPECT_EQ(page1->GetSize(), 3);
  EXPECT_EQ(page2->GetSize(), 3);

  EXPECT_EQ(page1->KeyAt(0).GetAsInteger(), 10);
  EXPECT_EQ(page1->KeyAt(1).GetAsInteger(), 20);
  EXPECT_EQ(page1->KeyAt(2).GetAsInteger(), 30);

  EXPECT_EQ(page2->KeyAt(0).GetAsInteger(), 40);
  EXPECT_EQ(page2->KeyAt(1).GetAsInteger(), 50);
  EXPECT_EQ(page2->KeyAt(2).GetAsInteger(), 60);

  // Verify next page id
  // MoveHalfTo usually updates next page id links?
  // It depends on implementation, but standard leaf split logic often sets next pointers if passing sibling.
  // The method signature is `void MoveHalfTo(BPlusTreeLeafPage *recipient)`.
  // Usually the logic is: this -> recipient.
  // Let's check if it does that or if the caller is responsible.
  // Based on common Bustub implementations, MoveHalfTo transfers keys.
  // The caller (Split) handles page ID linking.
  // However, `MoveHalfTo` might handle internal array moves.
}

TEST(BPlusTreeLeafPageTest, MoveAllToTest) {
  char buf1[BUSTUB_PAGE_SIZE];
  char buf2[BUSTUB_PAGE_SIZE];
  auto *page1 = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(buf1);
  auto *page2 = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(buf2);

  page1->Init(10);
  page2->Init(10);

  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  GenericKey<8> key;
  RID rid;

  // Page 1: 10, 20
  key.SetFromInteger(10);
  rid.Set(1, 10);
  page1->Insert(key, rid, comparator);
  key.SetFromInteger(20);
  rid.Set(1, 20);
  page1->Insert(key, rid, comparator);

  // Page 2: 30, 40
  key.SetFromInteger(30);
  rid.Set(1, 30);
  page2->Insert(key, rid, comparator);
  key.SetFromInteger(40);
  rid.Set(1, 40);
  page2->Insert(key, rid, comparator);

  // Move all from page2 to page1 (Merging)
  // Logic: recipient is page1.
  page2->MoveAllTo(page1);

  EXPECT_EQ(page1->GetSize(), 4);
  EXPECT_EQ(page2->GetSize(), 0);

  EXPECT_EQ(page1->KeyAt(0).GetAsInteger(), 10);
  EXPECT_EQ(page1->KeyAt(1).GetAsInteger(), 20);
  EXPECT_EQ(page1->KeyAt(2).GetAsInteger(), 30);
  EXPECT_EQ(page1->KeyAt(3).GetAsInteger(), 40);
}

TEST(BPlusTreeLeafPageTest, MoveFirstToEndOfTest) {
  char buf1[BUSTUB_PAGE_SIZE];
  char buf2[BUSTUB_PAGE_SIZE];
  auto *page1 = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(buf1);
  auto *page2 = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(buf2);

  page1->Init(10);
  page2->Init(10);

  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  GenericKey<8> key;
  RID rid;

  // Page 1 (Source): 30, 40
  key.SetFromInteger(30);
  rid.Set(1, 30);
  page1->Insert(key, rid, comparator);
  key.SetFromInteger(40);
  rid.Set(1, 40);
  page1->Insert(key, rid, comparator);

  // Page 2 (Recipient): 10, 20
  key.SetFromInteger(10);
  rid.Set(1, 10);
  page2->Insert(key, rid, comparator);
  key.SetFromInteger(20);
  rid.Set(1, 20);
  page2->Insert(key, rid, comparator);

  // Move first of page1 to end of page2
  // Typically used when page1 is right sibling of page2.
  page1->MoveFirstToEndOf(page2);

  EXPECT_EQ(page1->GetSize(), 1);
  EXPECT_EQ(page2->GetSize(), 3);

  EXPECT_EQ(page1->KeyAt(0).GetAsInteger(), 40);  // 30 is gone

  EXPECT_EQ(page2->KeyAt(0).GetAsInteger(), 10);
  EXPECT_EQ(page2->KeyAt(1).GetAsInteger(), 20);
  EXPECT_EQ(page2->KeyAt(2).GetAsInteger(), 30);  // 30 appended
}

TEST(BPlusTreeLeafPageTest, MoveLastToFrontOfTest) {
  char buf1[BUSTUB_PAGE_SIZE];
  char buf2[BUSTUB_PAGE_SIZE];
  auto *page1 = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(buf1);
  auto *page2 = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(buf2);

  page1->Init(10);
  page2->Init(10);

  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  GenericKey<8> key;
  RID rid;

  // Page 1 (Source): 10, 20
  key.SetFromInteger(10);
  rid.Set(1, 10);
  page1->Insert(key, rid, comparator);
  key.SetFromInteger(20);
  rid.Set(1, 20);
  page1->Insert(key, rid, comparator);

  // Page 2 (Recipient): 30, 40
  key.SetFromInteger(30);
  rid.Set(1, 30);
  page2->Insert(key, rid, comparator);
  key.SetFromInteger(40);
  rid.Set(1, 40);
  page2->Insert(key, rid, comparator);

  // Move last of page1 to front of page2
  // Typically used when page1 is left sibling of page2.
  page1->MoveLastToFrontOf(page2);

  EXPECT_EQ(page1->GetSize(), 1);
  EXPECT_EQ(page2->GetSize(), 3);

  EXPECT_EQ(page1->KeyAt(0).GetAsInteger(), 10);  // 20 is gone

  EXPECT_EQ(page2->KeyAt(0).GetAsInteger(), 20);  // 20 prepended
  EXPECT_EQ(page2->KeyAt(1).GetAsInteger(), 30);
  EXPECT_EQ(page2->KeyAt(2).GetAsInteger(), 40);
}

}  // namespace bustub
