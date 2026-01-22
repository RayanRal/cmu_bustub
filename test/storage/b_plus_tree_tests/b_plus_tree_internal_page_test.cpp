#include "storage/page/b_plus_tree_internal_page.h"
#include "gtest/gtest.h"
#include "storage/index/generic_key.h"
#include "test_util.h"  // NOLINT

namespace bustub {

TEST(BPlusTreeInternalPageTest, InitTest) {
  char buf[BUSTUB_PAGE_SIZE];
  auto *internal_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(buf);
  internal_page->Init(10);

  EXPECT_FALSE(internal_page->IsLeafPage());
  EXPECT_EQ(internal_page->GetSize(), 0);
  EXPECT_EQ(internal_page->GetMaxSize(), 10);
  EXPECT_EQ(internal_page->GetMinSize(), 5);
}

TEST(BPlusTreeInternalPageTest, DataTest) {
  char buf[BUSTUB_PAGE_SIZE];
  auto *internal_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(buf);
  internal_page->Init(10);

  GenericKey<8> key;
  page_id_t value;

  // Set Key/Value
  key.SetFromInteger(10);
  value = 100;
  internal_page->SetKeyAt(1, key);
  internal_page->SetValueAt(1, value);

  EXPECT_EQ(internal_page->KeyAt(1).GetAsInteger(), 10);
  EXPECT_EQ(internal_page->ValueAt(1), 100);

  internal_page->SetValueAt(0, 99);
  EXPECT_EQ(internal_page->ValueAt(0), 99);

  internal_page->SetSize(2);
  EXPECT_EQ(internal_page->ValueIndex(100), 1);
  EXPECT_EQ(internal_page->ValueIndex(99), 0);
  EXPECT_EQ(internal_page->ValueIndex(50), -1);
}

TEST(BPlusTreeInternalPageTest, LookupTest) {
  char buf[BUSTUB_PAGE_SIZE];
  auto *internal_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(buf);
  internal_page->Init(10);

  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  // Setup:
  // P0 | K1 | P1 | K2 | P2 | K3 | P3
  // Values: P0=100, K1=10, P1=101, K2=20, P2=102, K3=30, P3=103

  GenericKey<8> k1, k2, k3;
  k1.SetFromInteger(10);
  k2.SetFromInteger(20);
  k3.SetFromInteger(30);

  internal_page->SetKeyAt(1, k1);
  internal_page->SetKeyAt(2, k2);
  internal_page->SetKeyAt(3, k3);

  internal_page->SetValueAt(0, 100);
  internal_page->SetValueAt(1, 101);
  internal_page->SetValueAt(2, 102);
  internal_page->SetValueAt(3, 103);

  internal_page->SetSize(4);

  // Lookup 5 ( < 10 ) -> P0
  GenericKey<8> search_key;
  search_key.SetFromInteger(5);
  EXPECT_EQ(internal_page->Lookup(search_key, comparator), 100);

  search_key.SetFromInteger(10);
  EXPECT_EQ(internal_page->Lookup(search_key, comparator), 101);

  // Lookup 15 -> P1
  search_key.SetFromInteger(15);
  EXPECT_EQ(internal_page->Lookup(search_key, comparator), 101);

  // Lookup 20 -> P2
  search_key.SetFromInteger(20);
  EXPECT_EQ(internal_page->Lookup(search_key, comparator), 102);

  // Lookup 25 -> P2
  search_key.SetFromInteger(25);
  EXPECT_EQ(internal_page->Lookup(search_key, comparator), 102);

  // Lookup 30 -> P3
  search_key.SetFromInteger(30);
  EXPECT_EQ(internal_page->Lookup(search_key, comparator), 103);

  // Lookup 40 -> P3
  search_key.SetFromInteger(40);
  EXPECT_EQ(internal_page->Lookup(search_key, comparator), 103);
}

TEST(BPlusTreeInternalPageTest, PopulateNewRootTest) {
  char buf[BUSTUB_PAGE_SIZE];
  auto *internal_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(buf);
  internal_page->Init(10);

  GenericKey<8> key;
  key.SetFromInteger(50);

  // New Root: [OldValue] Key [NewValue]
  internal_page->PopulateNewRoot(100, key, 101);

  EXPECT_EQ(internal_page->GetSize(), 2);
  EXPECT_EQ(internal_page->ValueAt(0), 100);
  EXPECT_EQ(internal_page->KeyAt(1).GetAsInteger(), 50);
  EXPECT_EQ(internal_page->ValueAt(1), 101);
}

TEST(BPlusTreeInternalPageTest, MoveHalfToTest) {
  char buf1[BUSTUB_PAGE_SIZE];
  char buf2[BUSTUB_PAGE_SIZE];
  auto *page1 = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(buf1);
  auto *page2 = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(buf2);

  page1->Init(10);
  page2->Init(10);  // Recipient

  GenericKey<8> key;

  page1->SetValueAt(0, 100);
  for (int i = 1; i <= 5; ++i) {
    key.SetFromInteger(i * 10);
    page1->SetKeyAt(i, key);
    page1->SetValueAt(i, 100 + i);
  }
  page1->SetSize(6);

  // Move Half to page2
  page1->MoveHalfTo(page2);

  EXPECT_EQ(page1->GetSize(), 3);
  EXPECT_EQ(page2->GetSize(), 3);

  // Verify Page 1 content
  // P0=100, K1=10, P1=101, K2=20, P2=102
  EXPECT_EQ(page1->ValueAt(0), 100);
  EXPECT_EQ(page1->KeyAt(1).GetAsInteger(), 10);
  EXPECT_EQ(page1->ValueAt(1), 101);
  EXPECT_EQ(page1->KeyAt(2).GetAsInteger(), 20);
  EXPECT_EQ(page1->ValueAt(2), 102);

  // Verify Page 2 content

  EXPECT_EQ(page2->ValueAt(0), 103);
  EXPECT_EQ(page2->KeyAt(1).GetAsInteger(), 40);
  EXPECT_EQ(page2->ValueAt(1), 104);
  EXPECT_EQ(page2->KeyAt(2).GetAsInteger(), 50);
  EXPECT_EQ(page2->ValueAt(2), 105);
}

TEST(BPlusTreeInternalPageTest, MoveAllToTest) {
  char buf1[BUSTUB_PAGE_SIZE];
  char buf2[BUSTUB_PAGE_SIZE];
  auto *page1 = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(buf1);
  auto *page2 = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(buf2);

  page1->Init(10);
  page2->Init(10);

  GenericKey<8> key;
  // Page 2 (Recipient): P0=200, K1=10, P1=201
  page2->SetValueAt(0, 200);
  key.SetFromInteger(10);
  page2->SetKeyAt(1, key);
  page2->SetValueAt(1, 201);
  page2->SetSize(2);

  // Page 1 (Source): P0=202, K1=30, P1=203
  page1->SetValueAt(0, 202);
  key.SetFromInteger(30);
  page1->SetKeyAt(1, key);
  page1->SetValueAt(1, 203);
  page1->SetSize(2);

  // Merge. Middle Key = 20 (Separating Page 2 and Page 1)
  key.SetFromInteger(20);
  page1->MoveAllTo(page2, key);

  EXPECT_EQ(page2->GetSize(), 4);
  EXPECT_EQ(page1->GetSize(), 0);

  // Page 2 content:
  // P0=200, K1=10, P1=201, K2=20 (Middle), P2=202 (Old P0 of Page1), K3=30, P3=203
  EXPECT_EQ(page2->ValueAt(0), 200);
  EXPECT_EQ(page2->KeyAt(1).GetAsInteger(), 10);
  EXPECT_EQ(page2->ValueAt(1), 201);
  EXPECT_EQ(page2->KeyAt(2).GetAsInteger(), 20);  // Middle key becomes valid key
  EXPECT_EQ(page2->ValueAt(2), 202);
  EXPECT_EQ(page2->KeyAt(3).GetAsInteger(), 30);
  EXPECT_EQ(page2->ValueAt(3), 203);
}

TEST(BPlusTreeInternalPageTest, MoveFirstToEndOfTest) {
  char buf1[BUSTUB_PAGE_SIZE];
  char buf2[BUSTUB_PAGE_SIZE];
  auto *page1 = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(buf1);
  auto *page2 = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(buf2);

  page1->Init(10);
  page2->Init(10);

  GenericKey<8> key;
  // Page 2 (Recipient - Left): P0=200, K1=10, P1=201
  page2->SetValueAt(0, 200);
  key.SetFromInteger(10);
  page2->SetKeyAt(1, key);
  page2->SetValueAt(1, 201);
  page2->SetSize(2);

  // Page 1 (Source - Right): P0=202, K1=30, P1=203
  page1->SetValueAt(0, 202);
  key.SetFromInteger(30);
  page1->SetKeyAt(1, key);
  page1->SetValueAt(1, 203);
  page1->SetSize(2);

  key.SetFromInteger(20);
  page1->MoveFirstToEndOf(page2, key);

  // Result:
  // Page 2: Size 3. P0=200, K1=10, P1=201, K2=20, P2=202
  // Page 1: Size 1. P0=203 (Shifted).

  EXPECT_EQ(page2->GetSize(), 3);
  EXPECT_EQ(page2->KeyAt(2).GetAsInteger(), 20);
  EXPECT_EQ(page2->ValueAt(2), 202);

  EXPECT_EQ(page1->GetSize(), 1);
  EXPECT_EQ(page1->ValueAt(0), 203);
}

TEST(BPlusTreeInternalPageTest, MoveLastToFrontOfTest) {
  char buf1[BUSTUB_PAGE_SIZE];
  char buf2[BUSTUB_PAGE_SIZE];
  auto *page1 = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(buf1);
  auto *page2 = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(buf2);

  page1->Init(10);
  page2->Init(10);

  GenericKey<8> key;
  // Page 1 (Source - Left): P0=200, K1=10, P1=201
  page1->SetValueAt(0, 200);
  key.SetFromInteger(10);
  page1->SetKeyAt(1, key);
  page1->SetValueAt(1, 201);
  page1->SetSize(2);

  // Page 2 (Recipient - Right): P0=202, K1=30, P1=203
  page2->SetValueAt(0, 202);
  key.SetFromInteger(30);
  page2->SetKeyAt(1, key);
  page2->SetValueAt(1, 203);
  page2->SetSize(2);

  // Move Last of Page 1 to Front of Page 2
  // Middle Key = 20
  key.SetFromInteger(20);
  page1->MoveLastToFrontOf(page2, key);

  // Result:
  // Page 1: Size 1. P0=200. K1=10 removed.
  // Page 2: Size 3. P0=201 (Moved from P1 of Page1). K1=20 (Middle). P1=202 (Old P0). K2=30. P2=203.

  EXPECT_EQ(page1->GetSize(), 1);
  EXPECT_EQ(page1->ValueAt(0), 200);

  EXPECT_EQ(page2->GetSize(), 3);
  EXPECT_EQ(page2->ValueAt(0), 201);
  EXPECT_EQ(page2->KeyAt(1).GetAsInteger(), 20);  // Middle key inserted
  EXPECT_EQ(page2->ValueAt(1), 202);
  EXPECT_EQ(page2->KeyAt(2).GetAsInteger(), 30);
  EXPECT_EQ(page2->ValueAt(2), 203);
}

}  // namespace bustub
