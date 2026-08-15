# BusTub Benchmarking & Performance Test Suites

This document provides a guide to all performance tests, end-to-end benchmarks, and component stress suites available in CMU BusTub.

---

## 1. 🗄️ Buffer Pool Manager Benchmark (`bustub-bpm-bench`)
- **Executable**: `build/bin/bustub-bpm-bench`
- **Source**: [`tools/bpm_bench/bpm_bench.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/tools/bpm_bench/bpm_bench.cpp)
- **What it measures**:
  - Multi-threaded sequential **scan throughput** (`scan/sec`) and Zipfian-distributed random **lookup/write throughput** (`get/sec`).
  - Evaluates lock contention on `bpm_latch_`, frame latching overhead, and cache eviction performance.
- **CLI Options**:
  - `--duration <ms>`: Total duration in milliseconds (default: 30000).
  - `--bpm-size <n>`: Number of frames in the buffer pool (default: 64).
  - `--db-size <n>`: Total pages in the database (default: 6400).
  - `--scan-thread-n <n>`: Number of concurrent scan worker threads (default: 8).
  - `--get-thread-n <n>`: Number of concurrent lookup/get worker threads (default: 8).
  - `--latency <0|1>`: Enable simulated disk latency.
- **How to run**:
  ```bash
  ./build/bin/bustub-bpm-bench --duration 10000 --bpm-size 64 --db-size 6400 --scan-thread-n 8 --get-thread-n 8
  ```
- **Output metrics**:
  ```text
  <<< BEGIN
  scan: 15324.87
  get: 1045.18
  >>> END
  ```

---

## 2. 🌲 B+ Tree Index Benchmark (`bustub-btree-bench`)
- **Executable**: `build/bin/bustub-btree-bench`
- **Source**: [`tools/btree_bench/btree_bench.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/tools/btree_bench/btree_bench.cpp)
- **What it measures**:
  - Concurrent index point lookup throughput (`read/sec`) and concurrent insert/delete throughput (`write/sec`) across 100,000 keys.
  - Measures latch crabbing efficiency, root latch contention, and page splits/merges.
- **CLI Options**:
  - `--duration <ms>`: Total benchmark runtime in milliseconds (default: 30000).
- **How to run**:
  ```bash
  ./build/bin/bustub-btree-bench --duration 10000
  ```
- **Output metrics**:
  ```text
  <<< BEGIN
  write: <ops/sec>
  read: <ops/sec>
  >>> END
  ```

---

## 3. 💳 End-to-End MVCC & Transaction Benchmark (`bustub-terrier-bench`)
- **Executable**: `build/bin/bustub-terrier-bench`
- **Source**: [`tools/terrier_bench/terrier.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/tools/terrier_bench/terrier.cpp)
- **What it measures**:
  - **Benchmark 1 (`--bench-1`)**: Snapshot Isolation (SI) financial token transfer transactions throughput.
  - **Benchmark 2 (`--bench-2`)**: Serializable Isolation join and transfer transaction throughput, conflict detection abort rates, and database garbage collection (GC) bloat ratio (`db_size per committed txn`).
- **CLI Options**:
  - `--bench-1`: Run benchmark under Snapshot Isolation.
  - `--bench-2`: Run benchmark under Serializable Isolation.
  - `--duration <ms>`: Total duration in milliseconds (default: 30000).
  - `--threads <n>`: Number of worker threads (default: 2).
  - `--terriers <n>`: Number of account entities (default: 10).
  - `--commit-threshold <n>`: Minimum number of committed txns required to pass.
- **How to run**:
  ```bash
  # Snapshot Isolation:
  ./build/bin/bustub-terrier-bench --bench-1 --duration 10000 --threads 8

  # Serializable Isolation:
  ./build/bin/bustub-terrier-bench --bench-2 --duration 10000 --threads 8
  ```
- **Output metrics**:
  ```text
  <<< BEGIN
  transfer: <txns/sec>
  join: <txns/sec>
  db_size: <num_tuples_and_undo_logs>
  db_size per committed txn: <ratio>
  >>> END
  ```

---

## 4. 🗃️ Hash Table Benchmark (`bustub-htable-bench`)
- **Executable**: `build/bin/bustub-htable-bench`
- **Source**: [`tools/htable_bench/htable_bench.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/tools/htable_bench/htable_bench.cpp)
- **What it measures**: Extendible hash table directory and bucket concurrent insert and lookup performance.

---

## 5. 🔬 Component Micro-Benchmarks & Stress Unit Tests

| Test Suite | Source File | What is Tested | Performance Assertion / Target |
| :--- | :--- | :--- | :--- |
| **`ArcReplacerPerformanceTest`** | [`test/buffer/arc_replacer_performance_test.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/test/buffer/arc_replacer_performance_test.cpp) | 10 rounds of 262K `RecordAccess` operations on a 1GB cache list | Average time `< 3.0s` |
| **`BufferPoolManagerTest.ContentionTest`** | [`test/buffer/buffer_pool_manager_test.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/test/buffer/buffer_pool_manager_test.cpp) | Multi-threaded page access and pinning contention | Fast completion without deadlocks |
| **`BufferPoolManagerTest.EvictableTest`** | [`test/buffer/buffer_pool_manager_test.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/test/buffer/buffer_pool_manager_test.cpp) | Rapid allocation and eviction cycles | High throughput page eviction |
| **`BPlusTreeSequentialScaleTest`** | [`test/storage/b_plus_tree_sequential_scale_test.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/test/storage/b_plus_tree_sequential_scale_test.cpp) | Large-scale sequential insertion, deletion, and iteration | Scale linearity |
| **`TxnTsTest.WatermarkPerformance`** | [`test/txn/txn_timestamp_test.cpp`](file:///Users/leonidchashnikov/Projects/cmu_bustub/test/txn/txn_timestamp_test.cpp) | Concurrent transaction timestamp tracking and watermark recalculations | High-frequency update scalability |
| **SQLLogicTest Leaderboard Queries** | [`test/sql/p3.leaderboard-q1.slt`](file:///Users/leonidchashnikov/Projects/cmu_bustub/test/sql/p3.leaderboard-q1.slt), `q2.slt`, `q3.slt` | Multi-way join, aggregation, and window function execution | Multi-pass repeat loop latency |
