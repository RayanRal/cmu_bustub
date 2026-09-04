# Buffer Pool Manager & ARC Replacer Performance and Correctness Review

## Summary of Findings

This document summarizes the code review of the Buffer Pool Manager, ARC Replacer, Disk Scheduler, and Page Guards implementation in CMU BusTub.

---

### ✅ 1. Critical Performance Bottleneck: Holding `bpm_latch_` During Disk I/O — **[FIXED]**
- **Location**: [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp#L250-L288) (`CheckedWritePage`) and [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp#L364-L402) (`CheckedReadPage`)
- **Problem**: `future.get()` blocks execution while waiting for disk I/O to complete. Calling this while holding `bpm_latch_` locks the entire buffer pool manager.
- **Impact**: Under concurrent workloads, all other threads were blocked from accessing in-memory pages while one thread waited for disk I/O.
- **Fix Implemented**:
  1. Updated metadata under `bpm_latch_` and acquired frame-level `rwlatch_`.
  2. Released `bpm_latch_` before disk read/write `future.get()`.
  3. Other worker threads can now access unrelated pages in parallel.

---

### ✅ 2. $O(N)$ Linear Search on Every Page Eviction — **[FIXED]**
- **Location**: [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp#L240-L247) (`CheckedWritePage`) and [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp#L354-L361) (`CheckedReadPage`)
- **Problem**: `page_table_` maps `page_id_t -> frame_id_t`. When `ArcReplacer::Evict()` selects a `frame_id`, a linear scan over `page_table_` was performed to find the evicted `page_id`.
- **Impact**: Degraded eviction complexity from $O(1)$ to $O(N)$.
- **Fix Implemented**:
  1. Added `page_id_t page_id_` directly inside `FrameHeader`.
  2. Evictions now read `frame->page_id_` in $O(1)$ time.

---

### ✅ 3. Unused Mutex / Dead Lock Code in `ArcReplacer` — **[FIXED]**
- **Location**: [`src/include/buffer/arc_replacer.h`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/include/buffer/arc_replacer.h#L87) & [`src/buffer/arc_replacer.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/arc_replacer.cpp)
- **Problem**: `ArcReplacer` declared `std::mutex latch_;`, but none of the public methods (`Evict`, `RecordAccess`, `SetEvictable`, `Remove`, `Size`) acquired it.
- **Fix Implemented**: Added `std::scoped_lock lock(latch_)` to all public methods in `ArcReplacer`, ensuring internal thread-safety and removing dead code.

---

### ⚠️ 4. `WritePageGuard::Drop()` Unconditionally Marks Page Dirty
- **Location**: [`src/storage/page/page_guard.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/storage/page/page_guard.cpp#L296)
- **Problem**: Whenever a `WritePageGuard` is destroyed or dropped, it unconditionally sets `frame_->is_dirty_ = true`, even if the caller only read fields or didn't perform any actual mutations.
- **Impact**: Forces clean pages to be needlessly rewritten to disk upon eviction or flush, causing unnecessary disk I/O bandwidth usage.

---

### ✅ 5. Unimplemented Stub: `WritePageGuard::Flush()` — **[FIXED]**
- **Location**: [`src/storage/page/page_guard.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/storage/page/page_guard.cpp#L277-L280)
- **Problem**: `WritePageGuard::Flush()` was an empty stub (`// For now, do nothing.`).
- **Fix Implemented**: Implemented `WritePageGuard::Flush()` to check `frame_->is_dirty_`, schedule a write request with `disk_scheduler_`, wait for I/O completion, and reset `frame_->is_dirty_ = false`.

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

| Issue | Severity | Category | Target File | Status |
| :--- | :---: | :--- | :--- | :---: |
| **Holding `bpm_latch_` during disk I/O** | 🔴 High | Concurrency / Bottleneck | [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp) | ✅ **Fixed** |
| **$O(N)$ linear search on eviction** | 🟠 Medium | Algorithmic Inefficiency | [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp) | ✅ **Fixed** |
| **Unused `latch_` in `ArcReplacer`** | 🟡 Low | Clean Code / Mutex | [`src/include/buffer/arc_replacer.h`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/include/buffer/arc_replacer.h) | ✅ **Fixed** |
| **Unconditional `is_dirty_ = true` on `Drop()`** | 🟠 Medium | Correctness / I/O | [`src/storage/page/page_guard.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/storage/page/page_guard.cpp) | ⏳ Pending |
| **Empty `WritePageGuard::Flush()` stub** | 🟠 Medium | Incomplete Feature | [`src/storage/page/page_guard.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/storage/page/page_guard.cpp) | ✅ **Fixed** |
| **Heap allocations (`shared_ptr`) on hot path** | 🟡 Low | Memory Overhead | [`src/buffer/arc_replacer.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/arc_replacer.cpp) | ⏳ Pending |
| **Redundant 4KB zeroing on `Reset()`** | 🟡 Low | Performance | [`src/buffer/buffer_pool_manager.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/src/buffer/buffer_pool_manager.cpp) | ✅ **Fixed as side-effect** (hot path no longer calls `Reset()`) |

---

## `bustub-bpm-bench` numbers (flush moved outside `bpm_latch_`)

Command: `./bin/bustub-bpm-bench --duration 8000 --bpm-size 64 --db-size 6400 --scan-thread-n 8 --get-thread-n 8 [--latency 1]`
Branch `fix/bpm-latch-concurrency`, 3 runs per config. Scan = sequential `ReadPage`, Get = Zipfian `WritePage` (dirty).

**Before** = read outside latch, dirty flush **inside** latch. **After** = both flush + read outside latch
(`flushing_pages_` + `flushing_cv_` for stale-read protection, early `page_table_` publish + frame `rwlatch_` for sharing).

| Config | Before scan/s | After scan/s | Δ scan | Before get/s | After get/s | Δ get | Total Δ |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| no latency | ~22270 (22121/22229/22422) | ~29850 (29780/29783/30161) | **+34%** | ~24300 (24162/24311/24437) | ~27280 (27162/27320/27397) | **+12%** | **+23%** (46570 → 57130) |
| latency=1 | ~283 (282/283) | ~317 (299/316/335) | **+12%** | ~324 (317/331) | ~286 (275/285/298) | -12% | ~flat (607 → 603) |

Notes:
- No-latency (memory disk): latch hold time dominates; moving 4KB flush outside latch is a clear win on both scan and get.
- With simulated disk latency: scan (eviction-heavy) still wins; get dips because faster scans create more concurrent flush I/O pressure on the shared `DiskScheduler` and shift scheduling toward scans. Total system throughput is flat; scan tail latency improves.
- Correctness: `buffer_pool_manager_test` (7/7 incl. `ContentionTest`, `DeadlockTest`), `page_guard_test` (2/2), `arc_replacer_test` (2/2) pass.
