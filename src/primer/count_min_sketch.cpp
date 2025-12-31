//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <algorithm>
#include <stdexcept>
#include <string>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  if (width == 0 || depth == 0) {
    throw std::invalid_argument("Width and depth must be greater than zero.");
  }

  // Initialize the 2D matrix (depth rows, width columns) with zeros and assign it to sketch
  for (size_t i = 0; i < depth_; ++i) {
    sketch_.emplace_back(width_);
  }

  /** @fall2025 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept : width_(other.width_), depth_(other.depth_) {
  sketch_ = std::move(other.sketch_);
  hash_functions_ = std::move(other.hash_functions_);
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  if (this != &other) {
    width_ = other.width_;
    depth_ = other.depth_;
    sketch_ = std::move(other.sketch_);
    hash_functions_ = std::move(other.hash_functions_);
  }
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  for (size_t i = 0; i < depth_; i++) {
    const size_t col = hash_functions_[i](item);
    sketch_[i][col].fetch_add(1, std::memory_order_relaxed);
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  for (size_t i = 0; i < depth_; ++i) {
    for (size_t j = 0; j < width_; ++j) {
      sketch_[i][j].fetch_add(other.sketch_[i][j].load(std::memory_order_relaxed), std::memory_order_relaxed);
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t min_count = UINT32_MAX;
  for (size_t i = 0; i < depth_; ++i) {
    const size_t col = hash_functions_[i](item);
    if (const uint32_t current = sketch_[i][col].load(std::memory_order_relaxed); current < min_count) {
      min_count = current;
    }
  }
  return (min_count == UINT32_MAX) ? 0 : min_count;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  sketch_.clear();
  for (size_t i = 0; i < depth_; ++i) {
    sketch_.emplace_back(width_);
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  std::vector<std::pair<KeyType, uint32_t>> result;

  // Create pairs of (candidate, count)
  result.reserve(candidates.size());
  for (const auto &candidate : candidates) {
    result.emplace_back(candidate, Count(candidate));
  }

  // Sort by count in descending order
  std::sort(result.begin(), result.end(), [](const auto &a, const auto &b) { return a.second > b.second; });

  // Return top k (or all if fewer than k)
  if (result.size() > k) {
    result.resize(k);
  }

  return result;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
