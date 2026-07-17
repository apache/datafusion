# Hash Join Dynamic Filter Partitioned Expression Benchmark

## Setup

- Runner: `target/release-nonlto/dfbench hj` built from this working tree.
- Data: `benchmarks/data/tpch_sf1`. The local SF10 directory existed but table directories were empty, so this run uses the checked-in SF1 parquet data.
- Queries: HJ Q24 and Q25 from `benchmarks/src/hj.rs`.
- Iterations: 5 per wall-time case; wall-time table reports average and average excluding the first iteration.
- Metrics: one `EXPLAIN ANALYZE VERBOSE` execution per case using `target/release-nonlto/datafusion-cli`.
- Parquet settings: DataFusion defaults. In particular `datafusion.execution.parquet.pruning=true`, `enable_page_index=true`, `bloom_filter_on_read=true`, and `pushdown_filters=false`.
- Partitioned hash join is forced by the benchmark query path via `hash_join_single_partition_threshold=0` and `hash_join_single_partition_threshold_rows=0`; join reordering is disabled.

## Cases

| Case | Dynamic filter | `datafusion.optimizer.hash_join_dynamic_filter_partitioned_expr_style` | Notes |
| --- | --- | --- | --- |
| A | off | `n/a` | Default parquet settings; hash join dynamic filter disabled. |
| B | on | `case` | Default/current partition-routed CASE expression. |
| C | on | `partitioned_or` | OR of per-partition predicates, no partition-routing CASE. |
| D | on | `global` | Single global predicate where possible; map-backed partitions reuse existing hash lookups ORed together. |
| E | on | `global_bounds_case_membership` | Global min/max bounds plus partition-routed CASE for membership only. |

## Wall Time

| Query | Partitions | Case | Dynamic filter | Style | Rows | Iterations | Avg ms | Avg excl first ms |
| --- | ---: | --- | --- | --- | ---: | ---: | ---: | ---: |
| Q24 | 4 | A | off | `n/a` | 1 | 5 | 53.049 | 51.945 |
| Q24 | 4 | B | on | `case` | 1 | 5 | 53.081 | 52.333 |
| Q24 | 4 | C | on | `partitioned_or` | 1 | 5 | 54.038 | 53.546 |
| Q24 | 4 | D | on | `global` | 1 | 5 | 53.939 | 53.135 |
| Q24 | 4 | E | on | `global_bounds_case_membership` | 1 | 5 | 53.568 | 52.746 |
| Q24 | default (16) | A | off | `n/a` | 1 | 5 | 35.615 | 33.778 |
| Q24 | default (16) | B | on | `case` | 1 | 5 | 30.910 | 29.117 |
| Q24 | default (16) | C | on | `partitioned_or` | 1 | 5 | 34.037 | 32.202 |
| Q24 | default (16) | D | on | `global` | 1 | 5 | 33.685 | 31.900 |
| Q24 | default (16) | E | on | `global_bounds_case_membership` | 1 | 5 | 31.344 | 29.619 |
| Q24 | 64 | A | off | `n/a` | 1 | 5 | 37.340 | 35.202 |
| Q24 | 64 | B | on | `case` | 1 | 5 | 43.194 | 40.782 |
| Q24 | 64 | C | on | `partitioned_or` | 1 | 5 | 241.040 | 243.832 |
| Q24 | 64 | D | on | `global` | 1 | 5 | 42.060 | 40.685 |
| Q24 | 64 | E | on | `global_bounds_case_membership` | 1 | 5 | 43.400 | 42.896 |
| Q25 | 4 | A | off | `n/a` | 1 | 5 | 55.485 | 55.032 |
| Q25 | 4 | B | on | `case` | 1 | 5 | 57.131 | 55.970 |
| Q25 | 4 | C | on | `partitioned_or` | 1 | 5 | 56.734 | 56.151 |
| Q25 | 4 | D | on | `global` | 1 | 5 | 56.787 | 56.216 |
| Q25 | 4 | E | on | `global_bounds_case_membership` | 1 | 5 | 57.055 | 56.028 |
| Q25 | default (16) | A | off | `n/a` | 1 | 5 | 30.606 | 29.611 |
| Q25 | default (16) | B | on | `case` | 1 | 5 | 31.305 | 29.999 |
| Q25 | default (16) | C | on | `partitioned_or` | 1 | 5 | 33.425 | 31.469 |
| Q25 | default (16) | D | on | `global` | 1 | 5 | 31.336 | 29.319 |
| Q25 | default (16) | E | on | `global_bounds_case_membership` | 1 | 5 | 31.490 | 29.942 |
| Q25 | 64 | A | off | `n/a` | 1 | 5 | 37.184 | 35.877 |
| Q25 | 64 | B | on | `case` | 1 | 5 | 41.304 | 39.518 |
| Q25 | 64 | C | on | `partitioned_or` | 1 | 5 | 60.631 | 59.171 |
| Q25 | 64 | D | on | `global` | 1 | 5 | 41.952 | 40.715 |
| Q25 | 64 | E | on | `global_bounds_case_membership` | 1 | 5 | 42.740 | 40.284 |

## Metrics

Metric names are the real DataFusion metric names from `EXPLAIN ANALYZE VERBOSE`. `lineitem_*` columns refer to the probe-side parquet scan.

| Query | Partitions | Case | Explain duration ms | HashJoin elapsed_compute ms | HashJoin join_time ms | HashJoin build_time ms | HashJoin input_rows | HashJoin output_rows | lineitem output_rows | lineitem bytes_scanned | lineitem row_groups_pruned_statistics | lineitem row_groups_pruned_bloom_filter | lineitem row_groups_pruned_dynamic_filter | lineitem page_index_rows_pruned | lineitem page_index_pages_pruned | lineitem pushdown_rows_pruned | lineitem pushdown_rows_matched | lineitem row_pushdown_eval_time | lineitem statistics_eval_time | lineitem bloom_filter_eval_time | lineitem page_index_eval_time | lineitem time_elapsed_processing |
| --- | ---: | --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Q24 | 4 | A | 54.178 | 26.500 | 25.170 | 0.721 | 6.00 M | 2.36 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 0 total → 0 matched | 0 total → 0 matched | 0 | 0 | 8ns | 8ns | 356ns | 8ns | 52.95ms |
| Q24 | 4 | B | 56.984 | 26.490 | 25.390 | 0.608 | 6.00 M | 2.36 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 0 total → 0 matched | 0 total → 0 matched | 0 | 0 | 8ns | 8ns | 340ns | 8ns | 54.45ms |
| Q24 | 4 | C | 57.159 | 26.840 | 24.980 | 0.992 | 5.98 M | 2.36 K | 5.98 M | 10.38 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 5.98 M matched | 318 total → 317 matched | 0 | 0 | 12ns | 1.81ms | 3.30ms | 485.34µs | 58.11ms |
| Q24 | 4 | D | 58.477 | 27.240 | 25.190 | 1.090 | 5.98 M | 2.36 K | 5.98 M | 10.38 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 5.98 M matched | 318 total → 317 matched | 0 | 0 | 12ns | 1.71ms | 1.02ms | 434.31µs | 57.45ms |
| Q24 | 4 | E | 57.450 | 26.260 | 24.950 | 0.717 | 5.98 M | 2.36 K | 5.98 M | 10.38 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 5.98 M matched | 318 total → 317 matched | 0 | 0 | 12ns | 1.18ms | 1.00ms | 435.91µs | 57.57ms |
| Q24 | default (16) | A | 39.538 | 77.050 | 27.250 | 25.050 | 6.00 M | 2.36 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 0 total → 0 matched | 0 total → 0 matched | 0 | 0 | 32ns | 32ns | 1.40µs | 32ns | 79.41ms |
| Q24 | default (16) | B | 41.971 | 60.770 | 27.940 | 16.570 | 6.00 M | 2.36 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 0 total → 0 matched | 0 total → 0 matched | 0 | 0 | 32ns | 32ns | 1.31µs | 32ns | 95.56ms |
| Q24 | default (16) | C | 41.316 | 67.880 | 28.260 | 20.020 | 5.98 M | 2.36 K | 5.98 M | 10.38 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 5.98 M matched | 318 total → 317 matched | 0 | 0 | 48ns | 15.27ms | 19.38ms | 781.56µs | 123.45ms |
| Q24 | default (16) | D | 39.502 | 85.240 | 26.550 | 29.500 | 5.98 M | 2.36 K | 5.98 M | 10.38 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 5.98 M matched | 318 total → 317 matched | 0 | 0 | 48ns | 11.59ms | 1.46ms | 587.10µs | 104.82ms |
| Q24 | default (16) | E | 38.080 | 58.230 | 26.390 | 16.100 | 5.98 M | 2.36 K | 5.98 M | 10.38 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 5.98 M matched | 318 total → 317 matched | 0 | 0 | 48ns | 9.60ms | 1.28ms | 556.04µs | 105.54ms |
| Q24 | 64 | A | 38.019 | 162.480 | 22.130 | 70.590 | 6.00 M | 2.36 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 0 total → 0 matched | 0 total → 0 matched | 0 | 0 | 128ns | 128ns | 5.48µs | 128ns | 86.27ms |
| Q24 | 64 | B | 43.564 | 141.160 | 22.280 | 59.730 | 6.00 M | 2.36 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 0 total → 0 matched | 0 total → 0 matched | 0 | 0 | 128ns | 128ns | 5.57µs | 128ns | 169.84ms |
| Q24 | 64 | C | 225.477 | 166.700 | 20.870 | 73.160 | 5.16 M | 2.36 K | 5.16 M | 9.12 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 5.16 M matched | 318 total → 271 matched | 0 | 0 | 192ns | 299.02ms | 1.97s | 589.41ms | 2.91s |
| Q24 | 64 | D | 48.321 | 160.080 | 22.370 | 69.170 | 5.98 M | 2.36 K | 5.98 M | 10.38 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 5.98 M matched | 318 total → 317 matched | 0 | 0 | 192ns | 42.04ms | 2.00ms | 889.85µs | 197.14ms |
| Q24 | 64 | E | 70.410 | 181.590 | 32.560 | 74.750 | 5.98 M | 2.36 K | 5.98 M | 10.38 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 5.98 M matched | 318 total → 317 matched | 0 | 0 | 192ns | 42.91ms | 2.14ms | 779.35µs | 269.60ms |
| Q25 | 4 | A | 63.998 | 61.860 | 57.540 | 3.610 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 0 total → 0 matched | 0 total → 0 matched | 0 | 0 | 8ns | 8ns | 348ns | 8ns | 54.65ms |
| Q25 | 4 | B | 61.046 | 61.220 | 57.010 | 3.550 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 0 total → 0 matched | 0 total → 0 matched | 0 | 0 | 8ns | 8ns | 341ns | 8ns | 55.60ms |
| Q25 | 4 | C | 59.855 | 60.070 | 54.810 | 4.160 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 6.00 M matched | 318 total → 318 matched | 0 | 0 | 12ns | 481.98µs | 3.47ms | 447.33µs | 57.23ms |
| Q25 | 4 | D | 60.442 | 58.460 | 53.870 | 3.680 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 6.00 M matched | 318 total → 318 matched | 0 | 0 | 12ns | 286.78µs | 1.14ms | 438.05µs | 55.83ms |
| Q25 | 4 | E | 63.375 | 59.670 | 54.700 | 3.910 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 6.00 M matched | 318 total → 318 matched | 0 | 0 | 12ns | 291.42µs | 1.03ms | 435.74µs | 54.98ms |
| Q25 | default (16) | A | 37.570 | 88.370 | 45.890 | 22.710 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 0 total → 0 matched | 0 total → 0 matched | 0 | 0 | 32ns | 32ns | 1.41µs | 32ns | 83.34ms |
| Q25 | default (16) | B | 39.991 | 67.400 | 48.080 | 11.650 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 0 total → 0 matched | 0 total → 0 matched | 0 | 0 | 32ns | 32ns | 1.41µs | 32ns | 115.59ms |
| Q25 | default (16) | C | 40.702 | 99.330 | 51.320 | 25.240 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 6.00 M matched | 318 total → 318 matched | 0 | 0 | 48ns | 6.77ms | 19.49ms | 662.21µs | 112.69ms |
| Q25 | default (16) | D | 43.041 | 83.500 | 49.480 | 17.930 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 6.00 M matched | 318 total → 318 matched | 0 | 0 | 48ns | 2.07ms | 1.29ms | 542.06µs | 134.84ms |
| Q25 | default (16) | E | 40.561 | 87.910 | 44.970 | 22.280 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 6.00 M matched | 318 total → 318 matched | 0 | 0 | 48ns | 2.56ms | 1.28ms | 535.69µs | 98.13ms |
| Q25 | 64 | A | 47.981 | 188.210 | 57.500 | 66.220 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 0 total → 0 matched | 0 total → 0 matched | 0 | 0 | 128ns | 128ns | 5.17µs | 128ns | 122.44ms |
| Q25 | 64 | B | 50.694 | 187.110 | 56.100 | 66.340 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 0 total → 0 matched | 0 total → 0 matched | 0 | 0 | 128ns | 128ns | 5.21µs | 128ns | 185.83ms |
| Q25 | 64 | C | 68.684 | 187.660 | 54.660 | 67.250 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 6.00 M matched | 318 total → 318 matched | 0 | 0 | 192ns | 97.72ms | 203.31ms | 8.25ms | 517.76ms |
| Q25 | 64 | D | 46.003 | 179.370 | 47.590 | 66.750 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 6.00 M matched | 318 total → 318 matched | 0 | 0 | 192ns | 20.79ms | 1.85ms | 733.18µs | 177.60ms |
| Q25 | 64 | E | 46.845 | 163.870 | 48.530 | 58.420 | 6.00 M | 229.7 K | 6.00 M | 10.41 M | 53 total → 53 matched | 53 total → 53 matched | 0 | 6.00 M total → 6.00 M matched | 318 total → 318 matched | 0 | 0 | 192ns | 16.40ms | 1.87ms | 901.43µs | 175.55ms |

## Notes

- With default parquet settings, `pushdown_filters=false`, so `pushdown_rows_pruned`, `pushdown_rows_matched`, and `row_pushdown_eval_time` stay zero or `NOT RECORDED`; filtering benefit mostly shows up through parquet pruning metrics and join input/output metrics.
- `pushdown_filters` is the row-filter / late-materialization path during parquet decoding. It defaults off because it can add predicate-evaluation and selection-building work when the filter is not selective enough to pay for itself; row-group stats pruning, page-index pruning, and bloom-filter pruning are separate paths and remain enabled by default.
- Because this run uses SF1 and one parquet file per table, absolute wall times are small and sensitive to cache/warmup effects; compare relative behavior within the same query/partition group.
- Q24 is selective enough to show some benefit at the default partition count, but the CASE expression does not help row/page pruning here. Q25 is much less selective and dynamic filtering is mostly overhead in this SF1 setup.

## Queries

Q24:

```sql
SELECT count(*)
FROM (
  SELECT o_orderkey AS k
  FROM orders
  WHERE o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-07-02'
) o
JOIN (
  SELECT l_orderkey AS k
  FROM lineitem
) l ON o.k = l.k
```

Q25:

```sql
SELECT count(*)
FROM (
  SELECT o_orderkey AS k
  FROM orders
  WHERE o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-10-01'
) o
JOIN (
  SELECT l_orderkey AS k
  FROM lineitem
) l ON o.k = l.k
```

## Short Analysis

- Ranking the four dynamic-filter expression shapes by average wall time across the three partition settings (`4`, default `16`, and `64`) gives:

| Query | Rank | Style | Avg query time ms |
| --- | ---: | --- | ---: |
| Q24 | 1 | `case` | 42.395 |
| Q24 | 2 | `global_bounds_case_membership` | 42.771 |
| Q24 | 3 | `global` | 43.228 |
| Q24 | 4 | `partitioned_or` | 109.705 |
| Q25 | 1 | `case` | 43.247 |
| Q25 | 2 | `global` | 43.358 |
| Q25 | 3 | `global_bounds_case_membership` | 43.762 |
| Q25 | 4 | `partitioned_or` | 50.263 |

- The best expression shape in this run is narrowly `case`, but `global` and `global_bounds_case_membership` are close enough that the ranking should not be over-interpreted without a larger dataset and more repetitions. `partitioned_or` is the clear worst shape at high partition counts because it creates a large OR expression and spends heavily in pruning evaluation.
- `CaseExpr` is CPU intensive because it routes rows by iterating over the `WHEN` branches. It evaluates the partition expression once, evaluates each `THEN` predicate only for matching rows, and removes matched rows from the remainder batch, so it is not evaluating every partition predicate for every row. But with many partitions it still checks many branch predicates across the batch.
- Row-group pruning did not materially matter in this SF1 run. `lineitem row_groups_pruned_statistics` is `53 total -> 53 matched` for every case, and `lineitem row_groups_pruned_dynamic_filter` is `0`, so the dynamic filter did not eliminate row groups. The non-CASE shapes can participate in page-index pruning (`lineitem page_index_rows_pruned` / `lineitem page_index_pages_pruned`), but that benefit was small except for the very expensive `partitioned_or` case.
- Removing the CASE can lose partition-local selectivity. The CASE form tests only the predicate for the row's routed partition, while global OR-style filters admit a row if it matches any partition's predicate. For membership filters this can create false positives across partitions. In this benchmark that selectivity loss is modest for `global` and `global_bounds_case_membership`, but it is still the core tradeoff: simpler expressions are cheaper and more pruneable, while CASE is more precise for partition-routed membership.
