//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return key_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { key_array_[index] = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return page_id_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { page_id_array_[index] = value; }

/*
 * Helper method to find the index associated with input "value"(page_id)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); ++i) {
    if (page_id_array_[i] == value) {
      return i;
    }
  }
  return -1;
}

/**
 * Lookup the value associated with the key.
 * @param key the key to search for
 * @param comparator the comparator
 * @return the value associated with the key
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  auto l = 1;
  auto r = GetSize() - 1;

  while (l <= r) {
    auto mid = l + (r - l) / 2;
    if (comparator(key_array_[mid], key) <= 0) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }

  return page_id_array_[l - 1];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  SetSize(2);
  page_id_array_[0] = old_value;
  key_array_[1] = new_key;
  page_id_array_[1] = new_value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  int idx = ValueIndex(old_value);
  // Shift
  for (int i = GetSize(); i > idx + 1; --i) {
    key_array_[i] = key_array_[i - 1];
    page_id_array_[i] = page_id_array_[i - 1];
  }
  key_array_[idx + 1] = new_key;
  page_id_array_[idx + 1] = new_value;
  ChangeSizeBy(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient) {
  int total = GetSize();
  int keep = (total + 1) / 2;
  int move_count = total - keep;

  recipient->Init(GetMaxSize());
  recipient->SetSize(move_count);

  for (int i = 0; i < move_count; ++i) {
    recipient->SetKeyAt(i, key_array_[keep + i]);
    recipient->SetValueAt(i, page_id_array_[keep + i]);
  }

  SetSize(keep);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  int start = recipient->GetSize();
  int move_count = GetSize();

  // The first key of `this` (which is invalid) is replaced by `middle_key`
  SetKeyAt(0, middle_key);

  for (int i = 0; i < move_count; ++i) {
    recipient->SetKeyAt(start + i, key_array_[i]);
    recipient->SetValueAt(start + i, page_id_array_[i]);
  }

  recipient->ChangeSizeBy(move_count);
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  int dest_idx = recipient->GetSize();
  recipient->SetKeyAt(dest_idx, middle_key);
  recipient->SetValueAt(dest_idx, ValueAt(0));
  recipient->ChangeSizeBy(1);

  // Shift this
  for (int i = 0; i < GetSize() - 1; ++i) {
    SetKeyAt(i, KeyAt(i + 1));
    SetValueAt(i, ValueAt(i + 1));
  }
  ChangeSizeBy(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  // Shift recipient
  for (int i = recipient->GetSize(); i > 0; --i) {
    recipient->SetKeyAt(i, recipient->KeyAt(i - 1));
    recipient->SetValueAt(i, recipient->ValueAt(i - 1));
  }

  recipient->SetKeyAt(1, middle_key);  // Old separator becomes K1
  recipient->SetValueAt(0, ValueAt(GetSize() - 1));
  recipient->ChangeSizeBy(1);

  ChangeSizeBy(-1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
