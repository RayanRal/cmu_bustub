//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method to set default values,
 * including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
  num_tombstones_ = 0;
}

/**
 * @brief Helper function for fetching tombstones of a page.
 * @return The last `NumTombs` keys with pending deletes in this page in order of recency (oldest at front).
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstones() const -> std::vector<KeyType> {
  std::vector<KeyType> tombs;
  for (int i = 0; i < num_tombstones_; ++i) {
    int key_idx = tombstones_[i];
    if (key_idx < GetSize()) {
      tombs.push_back(key_array_[key_idx]);
    }
  }
  return tombs;
}

/**
 * Helper methods to set/get next page id
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return key_array_[index]; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { key_array_[index] = key; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return rid_array_[index]; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { rid_array_[index] = value; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetTombstoneAt(int index, int key_idx) { tombstones_[index] = key_idx; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstoneAt(int index) const -> int { return tombstones_[index]; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstoneCount() const -> int { return num_tombstones_; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetTombstoneCount(int count) { num_tombstones_ = count; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsTombstone(int index) const -> bool {
  for (int i = 0; i < num_tombstones_; ++i) {
    if (tombstones_[i] == index) {
      return true;
    }
  }
  return false;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::AddTombstone(int &key_idx) {
  if (LEAF_PAGE_TOMB_CNT == 0) return;
  if (num_tombstones_ == LEAF_PAGE_TOMB_CNT) {
    int victim_idx = HandleTombstoneOverflow();
    if (key_idx > victim_idx) {
      key_idx--;
    }
  }
  tombstones_[num_tombstones_++] = key_idx;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveTombstone(int key_idx) {
  for (int i = 0; i < num_tombstones_; ++i) {
    if (tombstones_[i] == key_idx) {
      for (int j = i; j < num_tombstones_ - 1; ++j) {
        tombstones_[j] = tombstones_[j + 1];
      }
      num_tombstones_--;
      return;
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ShiftTombstones(int start_idx, int delta) {
  for (int i = 0; i < num_tombstones_; ++i) {
    if (tombstones_[i] >= start_idx) {
      tombstones_[i] += delta;
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::HandleTombstoneOverflow() -> int {
  int victim_idx = tombstones_[0];
  // Physically remove victim from key/rid arrays
  for (int i = victim_idx; i < GetSize() - 1; ++i) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }
  ChangeSizeBy(-1);

  // Shift all tombstones in list
  for (int i = 0; i < num_tombstones_ - 1; ++i) {
    tombstones_[i] = tombstones_[i + 1];
    if (tombstones_[i] > victim_idx) {
      tombstones_[i]--;
    }
  }
  num_tombstones_--;
  return victim_idx;
}

/**
 * Lookup the key.
 * @return Index of the key if found, -1 otherwise.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> int {
  int l = 0;
  int r = GetSize() - 1;
  while (l <= r) {
    int mid = l + (r - l) / 2;
    int cmp = comparator(key_array_[mid], key);
    if (cmp == 0) {
      return mid;
    }
    if (cmp < 0) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  return -1;
}

/**
 * Insert key and value.
 * @return true if inserted, false if duplicate (and not resurrected).
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  // 1. Find position
  int l = 0;
  int r = GetSize() - 1;
  while (l <= r) {
    int mid = l + (r - l) / 2;
    int cmp = comparator(key_array_[mid], key);
    if (cmp == 0) {
      // Duplicate found. Check tombstone.
      if (IsTombstone(mid)) {
        // Resurrect: remove from tombstones, update value.
        RemoveTombstone(mid);
        rid_array_[mid] = value;  // Update value
        return true;
      }
      return false;  // Duplicate and not tombstoned
    }
    if (cmp < 0) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }

  // Insert position is `l`.
  int target = l;

  // 2. Shift keys and values
  // Verify size
  if (GetSize() >= GetMaxSize()) {
    return false;  // Should split before calling Insert if full
  }

  for (int i = GetSize(); i > target; --i) {
    key_array_[i] = key_array_[i - 1];
    rid_array_[i] = rid_array_[i - 1];
  }

  key_array_[target] = key;
  rid_array_[target] = value;
  ChangeSizeBy(1);

  // 3. Update tombstones: any index >= target must be incremented
  ShiftTombstones(target, 1);

  return true;
}

/**
 * Remove key.
 * @return true if removed (or tombstoned), false if not found.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool {
  int target = Lookup(key, comparator);
  if (target == -1) {
    return false;
  }

  // Check if already tombstoned
  if (IsTombstone(target)) {
    return true;  // Already deleted
  }

  if (LEAF_PAGE_TOMB_CNT == 0) {
    for (int i = target; i < GetSize() - 1; ++i) {
      key_array_[i] = key_array_[i + 1];
      rid_array_[i] = rid_array_[i + 1];
    }
    ChangeSizeBy(-1);
    // Adjust tombstones
    ShiftTombstones(target, -1);
    return true;
  }

  AddTombstone(target);
  return true;
}

/**
 * Move half of entries to recipient.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int total = GetSize();
  int keep = total / 2;
  int move_count = total - keep;
  int start_idx = keep;

  recipient->SetPageType(IndexPageType::LEAF_PAGE);
  recipient->SetSize(move_count);
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(INVALID_PAGE_ID);

  for (int i = 0; i < move_count; ++i) {
    recipient->SetKeyAt(i, key_array_[start_idx + i]);
    recipient->SetValueAt(i, rid_array_[start_idx + i]);
  }

  SetSize(keep);

  // Handle tombstones
  int new_num_tombstones = 0;
  for (int i = 0; i < num_tombstones_; ++i) {
    int t_idx = tombstones_[i];
    if (t_idx < start_idx) {
      tombstones_[new_num_tombstones++] = t_idx;
    } else {
      int adjusted_idx = t_idx - start_idx;
      recipient->AddTombstone(adjusted_idx);
    }
  }
  num_tombstones_ = new_num_tombstones;
}

/**
 * Move all entries to recipient (Merge).
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  int start_offset = recipient->GetSize();
  int move_count = GetSize();

  for (int i = 0; i < move_count; ++i) {
    recipient->SetKeyAt(start_offset + i, key_array_[i]);
    recipient->SetValueAt(start_offset + i, rid_array_[i]);
  }
  recipient->ChangeSizeBy(move_count);
  recipient->SetNextPageId(GetNextPageId());

  // Merge tombstones
  for (int i = 0; i < num_tombstones_; ++i) {
    int adjusted_idx = tombstones_[i] + start_offset;
    int old_size = recipient->GetSize();
    recipient->AddTombstone(adjusted_idx);
    if (recipient->GetSize() < old_size) {
      start_offset--;
    }
  }

  SetSize(0);
  num_tombstones_ = 0;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  KeyType key = KeyAt(0);
  ValueType val = ValueAt(0);
  bool is_tomb = IsTombstone(0);

  // Remove 0 from this (shift)
  for (int i = 0; i < GetSize() - 1; ++i) {
    SetKeyAt(i, KeyAt(i + 1));
    SetValueAt(i, ValueAt(i + 1));
  }
  ChangeSizeBy(-1);

  // Adjust this tombstones
  if (is_tomb) {
    RemoveTombstone(0);
  }
  ShiftTombstones(0, -1);

  // Recipient appends
  int dest_idx = recipient->GetSize();
  recipient->SetKeyAt(dest_idx, key);
  recipient->SetValueAt(dest_idx, val);
  recipient->ChangeSizeBy(1);

  if (is_tomb) {
    recipient->AddTombstone(dest_idx);
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  int src_idx = GetSize() - 1;
  KeyType key = KeyAt(src_idx);
  ValueType val = ValueAt(src_idx);
  bool is_tomb = IsTombstone(src_idx);

  // Remove from this
  ChangeSizeBy(-1);
  if (is_tomb) {
    RemoveTombstone(src_idx);
  }

  // Recipient prepends (Insert at 0)
  for (int i = recipient->GetSize(); i > 0; --i) {
    recipient->SetKeyAt(i, recipient->KeyAt(i - 1));
    recipient->SetValueAt(i, recipient->ValueAt(i - 1));
  }
  recipient->SetKeyAt(0, key);
  recipient->SetValueAt(0, val);
  recipient->ChangeSizeBy(1);

  // Recipient tombstones adjust (increment all)
  recipient->ShiftTombstones(0, 1);

  if (is_tomb) {
    int dest_idx = 0;
    recipient->AddTombstone(dest_idx);
  }
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
