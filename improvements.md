# Adaptive Filter Pushdown Improvements

## 1. Projection-aware byte ratio (selectivity.rs, opener.rs, config.rs)

The byte ratio that decides whether a new filter starts as row-level or post-scan now only counts **extra** columns not already in the query projection. Previously, a filter on a column already being read (e.g. `WHERE URL LIKE ...` with `SELECT *`) was penalized for its byte cost even though those bytes are read regardless. Now such filters correctly get zero extra cost and start as row filters, enabling late materialization savings.

**Files:** `selectivity.rs` (partition_filters), `opener.rs` (passes `projection_columns`), `config.rs` (doc update)

## 2. Fix sort order for row filters (selectivity.rs)

Filters without effectiveness stats were sorted **largest first** (most expensive). Changed to **cheapest first** — cheap filters should run first to prune rows before expensive filters evaluate, reducing total work.

## 3. Fix demote_or_drop cycle for non-optional filters (selectivity.rs)

Non-optional PostScan filters with poor stats were repeatedly passed to `demote_or_drop`, which reset their stats each time, creating an infinite cycle where they never accumulated meaningful data. Now only optional filters (e.g. dynamic join filters) can be dropped; mandatory PostScan filters keep their stats and stay as post-scan.

## 4. Fix TrackerConfig::new() default (selectivity.rs)

`TrackerConfig::new()` had `byte_ratio_threshold: 0.2` but `config.rs` declared the default as `0.05`. Aligned the constructor to `0.05`.

## Benchmark Results

**ClickBench Partitioned** (pushdown_filters=true, target_partitions=1):
- Q23 (URL LIKE): 1.84x faster (1,285ms vs 2,349ms) — projection-aware fix
- Q22 (URL NOT LIKE): correctly kept post-scan, avoiding regression
- Total runtime roughly unchanged vs baseline

**TPC-DS SF1**: 6.3% faster overall (47.4s vs 50.6s), with large wins on:
- Q82: 4.75x faster
- Q37: 3.76x faster
- Q9: 2.72x faster
- Q24: 2.61x faster
