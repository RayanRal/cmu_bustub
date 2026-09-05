# Performance Opportunities Checkpoint

Context: after the NLJ→HashJoin residual-Filter fix (branch `opt/nlj-hashjoin-residual`, q1 went from
"no pass in 280s" to 48.8s/pass in Debug), all five `test/sql/p3.leaderboard-*.slt` queries complete.
Single-pass Debug timings below are warm timings from reduced (`+timing:x1`) copies of the SLTs plus a
verifying pass; all results verified correct. Mock table sizes: `__mock_t9` = 10M rows, `__mock_t4/5/6_1m`
and `__mock_t7` = 1M rows, `__mock_t11` = 1M rows, `__mock_t10` = 10K rows, `__mock_t8` = 10 rows,
`__mock_t1` = 1M rows.

| Query | Debug single pass | Plan shape | Verdict |
|---|---|---|---|
| q1-window (`rank()` over 10M rows) | ~161s → **~110s** (scan-only baseline 58s; sort phase ~103s → ~52s) | TopN(10) → Filter(rank≤3) → Window → MockScan | Improved 2026-09-06: Schwartzian done; now scan-bound — next step is per-partition TopN pushdown (item 5) |
| q2 (LEFT JOIN + 32-col agg) | ~67s → **~34s** (join-only 42s → 9s, of which 6.5s is the inherent left scan) | Agg → NLJ-Left (`v < v4`) → t7 × Filter(`1=2`) → t8 | Fixed 2026-09-05: NLJ empty-side fast path; remainder is agg work |
| q1 (3-way join + agg) | ~49s | Filter → HJ → HJ (+ pushed Filters) | Fixed; residual hash/agg cost |
| q1-index (10-row lookup) | ~11s → **~3.8s** | IndexScan range `col0:[90,+inf] col1:[10,10]` over `t1xy` | Fixed 2026-09-06: AND-prefix range rewrite; all 1M rows have x≥90 so the full key range is visited — remaining cost is per-entry heap fetch + filter |
| q3 (plain 2-way join) | ~11s | Clean HJ, 10K × 1M | Healthy baseline, no action |

## 1. Window sort with interpreted comparator (q1-window, ~161s)

Root cause: `src/execution/window_function_executor.cpp:55-108` runs `std::sort` over 10M indices with a
comparator that calls `expr->Evaluate()` on both tuples per comparison (partition key + order key × 2
tuples ≈ 4 virtual-dispatch evaluations each). ~240M comparisons → on the order of 1B expression
evaluations in Debug. The rank/partition scan after the sort is linear and innocent.

Fix: Schwartzian transform — evaluate each tuple's partition/order keys once into flat key arrays
(~20M evals), sort indices over materialized `Value`s. Executor-local, no plan changes.
Complexity: medium. Expected impact: removes the large majority of sort evaluations. Risk: low.
Status (2026-09-06, DONE on `opt/nlj-hashjoin-residual`): implemented in
`src/execution/window_function_executor.cpp` (flat `part_keys`/`order_keys` arrays; comparator,
`is_same_partition`, `is_same_order` all run on materialized `Value`s with verbatim null/ASC/DESC
semantics). Measured: q1-window 161s → 110s; attribution via scan-only baseline (58s over 10M rows)
shows the sort phase halved (~103s → ~52s) and the query is now scan-bound. Verified: p3.20,
p3.17–19 green plus targeted ASC/DESC/NULL-partition/peer-group/agg semantics tests.
Follow-up (high complexity, optimizer + executor): per-partition TopN pushdown, since the query only
needs `rank <= 3` per partition plus outer `LIMIT 10` — a full 10M-row sort is asymptotically wasteful
versus per-partition top-3 heaps. Do the Schwartzian first and re-measure.

## 2. NLJ re-Init per left row over an empty right side (q2, ~42 of 67s)

Root cause: `src/execution/nested_loop_join_executor.cpp:132` calls `right_executor_->Init()` for every
one of 1M left tuples; the right subtree (`Filter(1=2)` over a 10-row scan) immediately returns empty.
Attribution measured by splitting the query: join-only ≈ 42s, agg-only ≈ 17–25s. The right side is
provably empty, yet no optimizer rule fires.

Fix (recommended first): dynamic empty-side fast path in the executor, ~10 lines. If the first left
tuple's full right scan yields zero tuples, latch `right_empty_` and skip `Init`/`Next` for all
remaining left rows (LEFT: emit padded directly; INNER: emit nothing). Expected impact: deletes most
of the 42s. Risk: minimal (empty rescan is empty under deterministic executors, which the code already
assumes). Complexity: low.
Status (2026-09-05, DONE on `opt/nlj-hashjoin-residual`): implemented in
`src/execution/nested_loop_join_executor.cpp` + header (`right_empty_` / `saw_right_tuple_`).
Measured: join-only 41951ms → 9405ms, full q2 ~67s → 33.5s. Remaining join time is the inherent
1M-row left scan (6.5s scan-only baseline) plus padded-row construction. All join SLTs green;
full non-leaderboard sweep shows only the 6 known pre-existing failures.
Follow-up (medium, optimizer): contradiction + empty-propagation rules — `Filter(false)` → empty
relation, empty right + LEFT join → left passthrough (with NULL padding to preserve schema), empty
either side + INNER → empty. More general, but the dynamic fix captures q2's value far more cheaply.

## 3. No composite/range index scan (q1-index, ~11s for 10 rows)

Root cause: `src/optimizer/seqscan_as_indexscan.cpp:24-99` only rewrites OR-chains of equalities on one
column against a single-column index (`possible=false` on `And`, `columns.size() == 1`). `x >= 90 AND
y = 10` on index `(x,y)` plans as SeqScan + Filter over 1M rows. The executor is also point-lookup-only
(`src/execution/index_scan_executor.cpp:42`, `is_point_lookup_ = !pred_keys_.empty()`).

Fix: extend the rule to match an AND of point/range prefix conjuncts on a composite index's leading
columns, add bound support to `IndexScanPlanNode`, implement B+Tree range iteration in the executor.
Expected impact: 11s → milliseconds (10 output rows) — the highest ratio win on the board.
Complexity: medium-high (the executor's MVCC/visibility path must handle range iteration correctly).
Status (2026-09-06, DONE on `opt/nlj-hashjoin-residual`): `IndexScanPlanNode.range_bounds_` +
AND-prefix matcher in `seqscan_as_indexscan.cpp` (B+Tree only, leading run of =, ≥, >, ≤, < on col-vs-const;
legacy OR path untouched) + seek-via-`Begin(low_key)` with high-key early termination in
`index_scan_executor.cpp`, reusing the existing MVCC fetch+filter loop unchanged. Measured: q1-index
11.1s → 3.8s with correct 10 rows. Correction to the original estimate: ms is unreachable here — all 1M
rows satisfy x≥90, so the full key range is visited and each entry pays a heap fetch + residual filter;
the win comes from ordered index traversal beating the heap scan per row. Verified: targeted
range/eq/non-leading semantics tests, all join/window SLTs, txn_index/scan/executor suites, full
non-leaderboard sweep (only the 6 known pre-existing failures).
Follow-up (possible, needs care): key-column pre-filter before the heap fetch (skip entries whose key
already violates a bound) — would cut most of the remaining 3.8s, but must be weighed against MVCC
snapshot semantics since index keys track the latest version while readers may see older ones.

## 4. Hash-table hygiene, cross-cutting (q1/q2/q3, minor)

Neither `HashJoinExecutor` (partitioned build, `src/execution/hash_join_executor.cpp:41-46`) nor the
aggregation executor pre-size (`reserve`) their hash tables, so every 1M-row build pays rehash growth.
Fix: `ht_.reserve(n)` with `n` from child cardinality or a running estimate — about one line per
executor. Expected impact: single-digit to low-double-digit percent on build-heavy queries.
Complexity: low. (The Agg's per-group update loop over 32 aggregates is fully interpreted, but that is
inherent to the Volcano model — leave it.)

## Suggested order

1. NLJ empty-side fast path — smallest, safest, recovers ~60% of q2.
2. Window key precomputation — biggest absolute win, attacks the 161s query.
3. Composite index range scan — biggest ratio win, most code surface.
4. Hash-table `reserve()` — trivial filler while measuring the above.
5. Rank TopN pushdown / empty-propagation rules — only if the leaderboard demands more after 1–4.
