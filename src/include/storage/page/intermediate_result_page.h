//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// intermediate_result_page.h
//
// Identification: src/include/storage/page/intermediate_result_page.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "common/config.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort and hash join.
 * Supports variable-length tuples.
 */
class IntermediateResultPage {
 public:
  /**
   * Initialize the page.
   */
  void Init() {
    num_tuples_ = 0;
    free_space_offset_ = BUSTUB_PAGE_SIZE;
  }

  /**
   * Insert a tuple into the page.
   * @param tuple The tuple to insert
   * @return true if the tuple was successfully inserted, false if there is no enough space
   */
  auto InsertTuple(const Tuple &tuple) -> bool {
    uint32_t tuple_payload_size = tuple.GetLength();
    uint32_t total_tuple_size = sizeof(int32_t) + tuple_payload_size;
    uint32_t slot_size = sizeof(uint32_t);        // only offset is enough because Tuple stores its size
    uint32_t header_size = sizeof(uint32_t) * 2;  // num_tuples + free_space_offset

    if (free_space_offset_ < header_size + (num_tuples_ + 1) * slot_size + total_tuple_size) {
      return false;
    }

    free_space_offset_ -= total_tuple_size;
    tuple.SerializeTo(GetDataPtr() + free_space_offset_);

    auto slot_ptr = reinterpret_cast<uint32_t *>(GetDataPtr() + header_size + num_tuples_ * slot_size);
    slot_ptr[0] = free_space_offset_;

    num_tuples_++;
    return true;
  }

  /**
   * Get the tuple at the given index.
   * @param tuple_idx The index of the tuple to get
   * @return The tuple at the given index
   */
  auto GetTuple(uint32_t tuple_idx) const -> Tuple {
    uint32_t slot_size = sizeof(uint32_t);
    uint32_t header_size = sizeof(uint32_t) * 2;
    auto slot_ptr = reinterpret_cast<const uint32_t *>(GetDataPtr() + header_size + tuple_idx * slot_size);
    uint32_t offset = slot_ptr[0];

    Tuple tuple;
    tuple.DeserializeFrom(GetDataPtr() + offset);
    return tuple;
  }

  /** @return The number of tuples in the page */
  auto GetNumTuples() const -> uint32_t { return num_tuples_; }

 private:
  auto GetDataPtr() -> char * { return reinterpret_cast<char *>(this); }
  auto GetDataPtr() const -> const char * { return reinterpret_cast<const char *>(this); }

  uint32_t num_tuples_;
  uint32_t free_space_offset_;
};

static_assert(sizeof(IntermediateResultPage) <= BUSTUB_PAGE_SIZE);

}  // namespace bustub
