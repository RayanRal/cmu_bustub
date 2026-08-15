# Buffer Pool Manager & ARC Replacer Performance and Correctness Review

## Summary of Findings

This document summarizes the code review of the Buffer Pool Manager, ARC Replacer, Disk Scheduler, and Page Guards implementation in CMU BusTub.

---

### 🚨 1. Critical Performance Bottleneck: Holding `bpm_latch_` During Disk I/O
- **Location**: [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp#L250-L288) (`CheckedWritePage`) and [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp#L364-L402) (`CheckedReadPage`)
- **Problem**: `future.get()` blocks execution while waiting for disk I/O to complete. Calling this while holding `bpm_latch_` locks the entire buffer pool manager.
- **Impact**: Under concurrent workloads, **all other threads are completely blocked** from reading, writing, pinning, or fetching pages from memory while one thread waits for disk operations.
- **Fix Pattern**:
  1. Reserve the frame and update internal page metadata while holding `bpm_latch_`.
  2. Release `bpm_latch_` (`latch.unlock()`).
  3. Perform `future.get()` (disk I/O) without holding the latch.
  4. Re-acquire the latch if remaining metadata needs updating.

---

### 🐢 2. $O(N)$ Linear Search on Every Page Eviction
- **Location**: [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp#L240-L247) (`CheckedWritePage`) and [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp#L354-L361) (`CheckedReadPage`)
- **Problem**: `page_table_` maps `page_id_t -> frame_id_t`. When `ArcReplacer::Evict()` selects a `frame_id`, a linear scan over `page_table_` is performed to locate which `page_id` belonged to that frame.
- **Impact**: Degrades eviction complexity from $O(1)$ to $O(N)$ where $N$ is the buffer pool size.
- **Fix Pattern**: Either store `page_id_` inside `FrameHeader` (or maintain a reverse mapping `frame_id -> page_id`), allowing instant $O(1)$ lookup upon eviction.

---

### 🔒 3. Unused Mutex / Dead Lock Code in `ArcReplacer`
- **Location**: [`src/include/buffer/arc_replacer.h`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/include/buffer/arc_replacer.h#L87) & [`src/buffer/arc_replacer.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/arc_replacer.cpp)
- **Problem**: `ArcReplacer` declares `std::mutex latch_;`, but **none** of the methods (`Evict`, `RecordAccess`, `SetEvictable`, `Remove`, `Size`) actually acquire `latch_`.
- **Impact**: Currently, `ArcReplacer` relies entirely on external synchronization by `BufferPoolManager`'s `bpm_latch_`. Having an unused `latch_` member field is misleading and dead code. Either acquire `latch_` inside `ArcReplacer` to make it thread-safe independently, or remove `latch_` if thread-safety is handled externally.

---

### ⚠️ 4. `WritePageGuard::Drop()` Unconditionally Marks Page Dirty
- **Location**: [`src/storage/page/page_guard.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/storage/page/page_guard.cpp#L296)
- **Problem**: Whenever a `WritePageGuard` is destroyed or dropped, it unconditionally sets `frame_->is_dirty_ = true`, even if the caller only read fields or didn't perform any actual mutations.
- **Impact**: Forces clean pages to be needlessly rewritten to disk upon eviction or flush, causing unnecessary disk I/O bandwidth usage.

---

### 🛠️ 5. Unimplemented Stub: `WritePageGuard::Flush()`
- **Location**: [`src/storage/page/page_guard.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/storage/page/page_guard.cpp#L277-L280)
- **Problem**: `WritePageGuard::Flush()` is an empty stub. If any component calls `guard.Flush()`, the page will not be written to disk.

---

### ⚡ 6. Heap Allocation Overhead per Access in `ArcReplacer`
- **Location**: [`src/buffer/arc_replacer.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/arc_replacer.cpp#L230), [`L252`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/arc_replacer.cpp#L252), [`L277`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/arc_replacer.cpp#L277)
- **Problem**: On ghost hits and cache misses, a new `std::shared_ptr<FrameStatus>` is allocated on the heap.
- **Impact**: Dynamic memory allocations (`make_shared`) on hot-path cache hits/misses introduce heap allocation overhead and lock contention inside the global memory allocator under heavy multi-threading.

---

### ⚡ 7. Redundant Memory Zeroing in `FrameHeader::Reset()`
- **Location**: [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp#L47)
- **Problem**: `Reset()` zeroes out all 4096 bytes (`BUSTUB_PAGE_SIZE`) of frame memory.
- **Impact**: Right after `Reset()`, `CheckedReadPage` immediately fills those 4096 bytes with data read from disk. Zeroing 4KB before immediately overwriting it with disk bytes is redundant CPU work.

---

## Overview Table

| Issue | Severity | Category | Target File |
| :--- | :---: | :--- | :--- |
| **Holding `bpm_latch_` during disk I/O** | 🔴 High | Concurrency / Bottleneck | [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp) |
| **$O(N)$ linear search on eviction** | 🟠 Medium | Algorithmic Inefficiency | [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp) |
| **Unused `latch_` in `ArcReplacer`** | 🟡 Low | Clean Code / Mutex | [`src/include/buffer/arc_replacer.h`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/include/buffer/arc_replacer.h) |
| **Unconditional `is_dirty_ = true` on `Drop()`** | 🟠 Medium | Correctness / I/O | [`src/storage/page/page_guard.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/storage/page/page_guard.cpp) |
| **Empty `WritePageGuard::Flush()` stub** | 🟠 Medium | Incomplete Feature | [`src/storage/page/page_guard.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/storage/page/page_guard.cpp) |
| **Heap allocations (`shared_ptr`) on hot path** | 🟡 Low | Memory Overhead | [`src/buffer/arc_replacer.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/arc_replacer.cpp) |
| **Redundant 4KB zeroing on `Reset()`** | 🟡 Low | Performance | [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp) |
