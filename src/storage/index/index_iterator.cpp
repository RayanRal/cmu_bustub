//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(TracedBufferPoolManager *bpm, ReadPageGuard guard, int index, page_id_t page_id)
    : bpm_(bpm), guard_(std::move(guard)), page_id_(page_id), index_(index) {
  // Skip tombstones initially
  while (page_id_ != INVALID_PAGE_ID) {
    auto leaf = guard_.As<LeafPage>();
    if (index_ >= leaf->GetSize()) {
      // Move to next page
      page_id_t next_page_id = leaf->GetNextPageId();
      guard_ = ReadPageGuard();

      if (next_page_id != INVALID_PAGE_ID) {
        guard_ = bpm_->ReadPage(next_page_id);
        page_id_ = next_page_id;
        index_ = 0;
        continue;  // Check again for new page
      }
      page_id_ = INVALID_PAGE_ID;
      index_ = 0;
      break;
    }

    if (leaf->IsTombstone(index_)) {
      index_++;
    } else {
      break;  // Found valid key
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<KeyType, ValueType> {
  auto leaf = guard_.As<LeafPage>();
  return {leaf->KeyAt(index_), leaf->ValueAt(index_)};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;

  // Skip tombstones and move pages
  while (page_id_ != INVALID_PAGE_ID) {
    auto leaf = guard_.As<LeafPage>();

    if (index_ >= leaf->GetSize()) {
      // Move to next page
      page_id_t next_page_id = leaf->GetNextPageId();
      guard_ = ReadPageGuard();

      if (next_page_id != INVALID_PAGE_ID) {
        guard_ = bpm_->ReadPage(next_page_id);
        page_id_ = next_page_id;
        index_ = 0;
        continue;
      }
      page_id_ = INVALID_PAGE_ID;
      index_ = 0;
      break;
    }

    if (leaf->IsTombstone(index_)) {
      index_++;
    } else {
      break;  // Found valid key
    }
  }

  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
