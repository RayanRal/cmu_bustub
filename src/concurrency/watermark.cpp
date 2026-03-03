//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// watermark.cpp
//
// Identification: src/concurrency/watermark.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  std::lock_guard<std::mutex> lck(latch_);
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  auto &count = current_reads_[read_ts];
  if (count == 0) {
    min_heap_.push(read_ts);
  }
  count++;
  watermark_ = min_heap_.top();
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  std::lock_guard<std::mutex> lck(latch_);
  auto it = current_reads_.find(read_ts);
  if (it == current_reads_.end()) {
    return;
  }
  it->second--;
  if (it->second == 0) {
    current_reads_.erase(it);
  }

  while (!min_heap_.empty() && current_reads_.find(min_heap_.top()) == current_reads_.end()) {
    min_heap_.pop();
  }

  if (!min_heap_.empty()) {
    watermark_ = min_heap_.top();
  }
}

}  // namespace bustub
