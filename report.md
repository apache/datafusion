# Wide-schema parquet read perf — investigation log

## Branches and upstream

- DataFusion: `adrian/wide-schema-perf` on `pydantic/datafusion` (remote `origin`).
  - Pushed at: <https://github.com/pydantic/datafusion/tree/adrian/wide-schema-perf>
  - Based on `main` at `147617df4` (the wide-schema benchmark commit).
- arrow-rs: `adrian/wide-schema-perf` on `pydantic/arrow-rs` (remote `origin`).
  - Pushed at: <https://github.com/pydantic/arrow-rs/tree/adrian/wide-schema-perf>
  - Based on `main` at `d3cad6e0a`.

The DataFusion workspace at `Cargo.toml` patches `arrow-*` and `parquet`
to the local arrow-rs checkout, so the two branches are tested together.

## Benchmark and reproduction

```
BENCH_SUBGROUP=wide   cargo bench --bench sql -- "wide_schema"
BENCH_SUBGROUP=narrow cargo bench --bench sql -- "wide_schema"
```

For faster iteration I drove queries through `datafusion-cli`:

```
./target/profiling/datafusion-cli -f /tmp/q_wide_norun.sql
./target/profiling/datafusion-cli -f /tmp/q_narrow_norun.sql
```

with each `.sql` running `CREATE EXTERNAL TABLE events ...` once and the
Q04 SELECT 10 times back-to-back. Profiling used `samply record
--unstable-presymbolicate --save-only` and a small Python aggregator
(see `/tmp/profile_top.py`, `/tmp/compare2.py`).

## Findings

The wide dataset has **1024 cols × 256 files × 50k rows** and the
narrow dataset has **8 cols × 256 files × 50k rows** — same row count,
same file count, same bytes scanned per query. So any wide-vs-narrow
gap is per-file CPU overhead that scales with schema width.

### 1. Metadata cache thrashing (default 50MB cache)

Per-file `ParquetMetaData` for the wide dataset is ~1.5MB; the dataset
total is ~400MB. The default `metadata_cache_limit` is 50MB, so only
~30 of 256 files fit and the cache constantly evicts entries that other
threads still want. Effect on Q04 hot-cache wall time as a function of
cache limit (cold + 5 hot, ms; lower is better):

| limit | cold | hot (median) |
|---|---|---|
| 0 (disabled) | 705 | 100 |
| 10M | 707 | 85 |
| 50M (default) | 716 | **136** ← worst |
| 100M | 702 | 95 |
| 200M | 698 | 120 |
| 300M | 654 | 110 |
| 400M | 591 | 55 |
| 500M | 623 | 52 |
| 1G | 669 | 80 |
| 4G | 648 | 63 |

The 50M default lands precisely in the worst regime: large enough that
threads compete on the cache mutex and re-fetch evicted entries, small
enough that almost nothing stays cached. With cache disabled (no lock
contention) it's actually faster than 50M. With the cache sized to fit
the working set (~400M+) it's ~2.5× faster than the default.

Profiling at 50M showed `_pthread_mutex_firstfit_lock_wait` at ~12% of
samples and `evict_entries` plus `Arc::drop_slow` of `ParquetMetaData`
at another ~3%.

### 2. O(N²) per-file CPU in `statistics_from_parquet_metadata`

`statistics_from_parquet_metadata` iterates every logical field and
calls `StatisticsConverter::try_new`, which internally does an O(N)
linear scan in `parquet_column` (the comment said "this could be made
more efficient (#TBD)"). Plus an O(N) `Fields::find` on the arrow
side. For the wide dataset that's `1024² × 256 ~ 268M` ops per query
just to set up stats.

The same `parquet_column` linear scan is hit again per row group from
`row_group_filter` and `page_filter` for each filter column.

### 3. Per-file arrow-schema reconstruction

Every file open went through `ArrowReaderMetadata::try_new`, which
calls `parquet_to_arrow_schema_and_fields` to walk every parquet leaf
and build the matching arrow `Schema` + `FieldLevels`. With 256 files
that's 256 full walks per query, even though they all share the same
logical schema. Plus the embedded arrow-IPC schema metadata is
flatbuffer-decoded per file.

In the warm-cache profile this showed up as:
- `ArrowReaderMetadata::try_new` 4.6%
- `parquet_to_arrow_schema_and_fields` 4.2%
- `flatbuffers::verifier::TableVerifier::visit_field` 1.4%
- `arrow_ipc::convert::fb_to_schema` 1%

### 4. Smaller per-file walks

- `apply_file_schema_type_coercions` always built a 1024-entry HashMap
  even when the early-return condition would fire and discard it.
- `DefaultFilesMetadataCache::put`/`evict_entries`/`remove` called
  `FileMetadata::memory_size()` (which walks the entire metadata
  structure) every time, even though the size doesn't change after
  insertion.

## Changes made

### arrow-rs (`adrian/wide-schema-perf`)

| File | Change |
|---|---|
| `parquet/src/schema/types.rs` | `SchemaDescriptor` precomputes a `root_to_first_leaf` inverse map at construction; new `root_first_leaf_index` accessor. |
| `parquet/src/arrow/mod.rs` | `parquet_column` uses `root_first_leaf_index` (O(N) scan → O(1) lookup). New re-export of `parquet_to_arrow_schema_and_field_levels`. |
| `parquet/src/arrow/schema/mod.rs` | New public `parquet_to_arrow_schema_and_field_levels` that returns both `(Schema, FieldLevels)` in one walk. |
| `parquet/src/arrow/arrow_reader/mod.rs` | New `ArrowReaderMetadata::from_field_levels` constructor that packages a precomputed `(metadata, schema, field_levels)` triple — bypasses `try_new`'s per-leaf walk. New `ArrowReaderOptions::supplied_schema()`/`skip_arrow_metadata()`/`virtual_columns()` accessors so callers can decide whether their cached arrow view is applicable. |
| `parquet/src/arrow/arrow_reader/statistics.rs` | New `StatisticsConverter::from_arrow_field` constructor that takes a resolved `(field, parquet_leaf_index)` pair, skipping the redundant arrow + parquet name lookups inside `try_new`. |
| `parquet/src/arrow/async_reader/mod.rs` | New `AsyncFileReader::get_arrow_reader_metadata` trait method (default impl delegates to `try_new`). `load_async` now goes through it so cache-aware readers can short-circuit. |

### DataFusion (`adrian/wide-schema-perf`)

| File | Change |
|---|---|
| `datafusion/datasource-parquet/src/metadata.rs` | `statistics_from_parquet_metadata` rewritten to be O(N) per file: precompute a logical→parquet leaf index map once, use the new `from_arrow_field` constructor in the loop, drop the redundant `parquet_column` lookup inside `summarize_column_statistics`. `CachedParquetMetaData` now holds a `OnceLock<CachedArrowView>` with the per-file arrow `Schema` and `FieldLevels`, lazily built from the cached parquet metadata. |
| `datafusion/datasource-parquet/src/reader.rs` | `CachedParquetFileReader` overrides `get_arrow_reader_metadata` so warm-cache hits return a fully-built `ArrowReaderMetadata` via `from_field_levels` instead of re-walking the parquet schema. The duplicate `CachedParquetMetaData` definition that lived in `reader.rs` was removed in favor of the one in `metadata.rs`. |
| `datafusion/datasource-parquet/src/file_format.rs` | `apply_file_schema_type_coercions` does the early-return check first; only builds the 1024-entry name → type HashMap when transforms are actually required. |
| `datafusion/execution/src/cache/file_metadata_cache.rs` | `DefaultFilesMetadataCache` stores `memory_size` alongside the entry so `put`/`evict_entries`/`remove` don't re-walk the metadata structure. |

## Measured impact (Q04, profiling build)

End-of-pass numbers:

| Scenario | Before | After | Δ |
|---|---|---|---|
| narrow, hot | ~25 ms | ~25 ms | (control) |
| wide @50M cache, **cold** | ~1010 ms | ~615 ms | **−39%** |
| wide @50M cache, hot | ~108 ms | ~87 ms | −19% |
| wide @2G cache, **cold** | ~830 ms | ~510 ms | **−39%** |
| wide @2G cache, hot | ~47 ms | ~38 ms | **−19%** |
| wide @50M, `collect_statistics=false` cold | ~1010 ms | **~257 ms** | **−75%** |

The last row is the same code as the row above it — only the
`datafusion.execution.collect_statistics` setting differs. See the
"Major finding" section below.

In the warm-cache profile after the fixes:
- `ArrowReaderMetadata::try_new` 4.6% → **1.3%**
- `parquet_to_arrow_field_levels_with_virtual` (per-file walk) ~4% → **~1.5%** (and that 1.5% is `infer_schema` during CREATE TABLE — once per session)
- `statistics_from_parquet_metadata` 3.2% → **1.7%**
- `flatbuffers verifier` + `fb_to_schema` ~2.4% → **~0.4%**

## Verification

- `cargo test -p datafusion-datasource-parquet --lib metadata::` — pass.
- `cargo test -p datafusion-execution --lib` — 63 pass.
- `cargo test -p parquet --features arrow --lib schema::` — 104 pass.
- `cargo test -p parquet --features arrow --lib arrow_reader::` — 104 pass.
- The 16 `row_filter` / `row_group_filter` test failures in the parquet
  datasource crate are environmental — they need the
  `parquet-testing` git submodule (set `PARQUET_TEST_DATA` or run
  `git submodule update --init`); they fail the same way without my
  changes.

## Still open

The default cache size is the dominant remaining issue. Beyond bumping
it, the structural fix is reducing what needs to be cached (e.g. cache
the basic metadata always, defer the page index until needed) and/or
caching downstream-derived state across files that share a schema.

Per-file work in the morsel planner / predicate construction
(`prepare_filters`, `build_pruning_predicates`, `apply_file_schema_type_coercions`,
`PruningPredicate::try_new`) is still O(N_columns) per file. For
queries that only reference a handful of columns this is the next
target — a "reduced" arrow schema containing only referenced columns,
and per-(physical_file_schema, predicate) caches for the predicate
machinery, would push it toward O(num_columns_referenced).

---

## Research log

Things tried after the initial round, in chronological order. Numbers
quoted are wide @50M / wide @2G / cold first-query, in milliseconds, on
the 1024-col × 256-file dataset and Q04. Baseline before any of this
work was 1010 / 832 / cold-first-query.

### Cache-size sweep (validates the thrashing thesis)

Default `metadata_cache_limit = 50MB`, wide metadata is ~1.5 MB/file ×
256 ≈ 400 MB. Sweep at hot (5 reps after the cold one):

| limit | cold | hot median |
|---|---|---|
| 0 (disabled) | 705 | 100 |
| 10M | 707 | 85 |
| **50M (default)** | 716 | **136** ← worst |
| 100M | 702 | 95 |
| 200M | 698 | 120 |
| 300M | 654 | 110 |
| 400M | 591 | 55 |
| 500M | 623 | 52 |
| 1G | 669 | 80 |
| 4G | 648 | 63 |

The default lands in the worst regime: *enough* fits that threads
contend on the cache mutex, but not enough to stop re-parsing. With
the cache flat-out disabled it's actually faster than at 50M because
the lock contention disappears. Implication: tuning the default cache
or shrinking what's cached is the largest single lever. (See "skip
page index" below.)

### Microbench: try_new vs cached clone vs apply_coercions noop

Added `wide_schema_microbench`. Findings:

- `ArrowReaderMetadata::try_new` is ~190ns/col linear — at 1024 cols,
  ~190 µs per file. Across 256 files / 12 threads ≈ 4 ms wall per query
  spent purely rebuilding the arrow view.
- The cached clone path (after this branch's changes) is **~4 ns flat**,
  independent of column count. ~43000× faster at 1024 cols.
- `apply_file_schema_type_coercions` no-op walk is ~10 ns/col linear
  even when nothing actually changes — that's why the next change made
  it return `None` when no field was actually coerced (the early-return
  was insufficient because the table having any Utf8 field would cause
  the function to walk all file fields).
- `PruningPredicate::try_new` on a 1-column predicate is ~2-3 µs and
  does **not** scale with schema width — it's predicate-driven and the
  schema is only consulted for the few columns the predicate names.
  Good news: this part is already where we want it.
- `StatisticsConverter::try_new` 4.9 ns vs `from_arrow_field` 3.5 ns —
  ~30% per call, called 1024×/file in the old `statistics_from_parquet_metadata`,
  so a few µs per file in absolute savings. Small in this micro view but
  the bigger win was the surrounding O(N²) → O(N) restructure.

### Dead end: `apply_file_schema_type_coercions` early-return

Changed the function to do the "any view/string?" check in a quick
first pass and only build the 1024-entry HashMap if we *might* coerce.
Then made it return `None` when no field was actually transformed (the
function was returning `Some(<identical schema>)` when the table had
e.g. a Utf8 column but the file already matched).

**Tested impact**: ~zero on Q04. Reason: with `schema_force_view_types =
true` (the default since #X), the inferred table schema has Utf8View
and every Utf8 field in the parquet file *does* get coerced — so
`any_changed` is `true` and the function returns `Some(...)` anyway.
The function does still get cheaper for cases where the file already
matches the view-typed table, but Q04 isn't one of those. Keeping the
change because it's a strict win for callers where it does fire.

### Dead end: skip page index in cache (cold improved, warm regressed)

Hypothesis: wide-schema metadata is dominated by page-index payload;
loading it eagerly bloats the cache and pays I/O & decode that's wasted
when the query doesn't need page-level pruning. So change
`fetch_metadata` to use `PageIndexPolicy::Skip`.

**Result**: cold improved meaningfully (700 → 578 ms wide @50M; 560 →
498 ms wide @2G — both ~12-17%). Warm regressed badly (42 → 60 ms wide
@2G — about +40%) because for queries that **do** need page-level
pruning (ours does), the opener's `load_page_index` then re-reads the
page index from disk every query and never updates the cache, so
subsequent queries pay the load again.

Reverted. The right fix is to also update the cache after
`load_page_index` — defer the page-index decode to first use, but still
amortise it across queries. That's a bigger surgery and didn't fit this
pass. Recorded as future work.

### Dead end (so far): `apply_file_schema_type_coercions` return-`None`-on-noop

Mostly subsumed by the above. The "any actually changed" tracking is
still present (cleaner), but on Q04 with default `force_view_types`
it doesn't trigger.

### Win: post-coercion `ArrowReaderMetadata` cache slot

The opener does `try_new` again after `apply_file_schema_type_coercions`
and after `coerce_int96_to_resolution` to rebuild field-levels with the
new schema. With wide schemas this is **~190 µs per file** of
recomputing parquet → arrow field-levels with a hint. Added a
single-slot `coerced_arm: Mutex<Option<(usize_supplied_schema_ptr,
ArrowReaderMetadata)>>` to `CachedParquetMetaData` and routed the
opener's coercion rebuilds through `AsyncFileReader::get_arrow_reader_metadata`
(by making `prepare_filters` async). Hits when subsequent files /
queries supply the same schema (same Arc), which is the common case
across all files in a single ListingTable scan with `force_view_types`
on.

### Major finding: `collect_statistics=true` is the cold-path tax

The default `datafusion.execution.collect_statistics = true` causes
per-file metadata + statistics inference to happen eagerly during
`list_files_for_scan` (which the first query triggers). For
1024-col × 256-file dataset on Q04:

| `collect_statistics` | cold | hot |
|---|---|---|
| `true` (default) | 655 ms | 86 ms |
| `false` | **253 ms** | 75 ms |

That's a **−61% cold improvement** for free. The per-file work hidden
behind this knob is `fetch_metadata` + `statistics_from_parquet_metadata`
× 256 files (parallel up to `meta_fetch_concurrency = 32`). For Q04
the result is then unused — `files_ranges_pruned_statistics = 267 →
267` (none pruned), so the inferred per-file stats don't actually help
the query.

For workloads that *do* benefit from file-level pruning (large
time-range filters, partition-style data), turning it off would hurt.
The right structural fix is one of:

- Make `Statistics::column_statistics` lazy — compute per column on
  first access. Then the optimizer only pays for the columns it
  actually inspects.
- Compute stats only for columns the optimizer is statically known to
  inspect (predicate columns + sort columns), not all `N` columns.
- Pre-fetch metadata in the background so the user's first query
  doesn't see the latency.

For now I am noting this as a documented knob — anyone running on cold
paths against wide schemas should set it false and accept the loss of
file-level pruning, or accept the cold tax.

### Open / next

- For COLD specifically, the dominant per-file CPU is the parquet
  thrift footer decode (~5% of total samples = ~25 ms across the run
  for our wide bench) plus building the arrow view (~3-5%). The decode
  is hard to shrink without a parquet-format change. The arrow view
  could be O(num_columns_referenced) if `parquet_to_arrow_field_levels`
  was called with a `ProjectionMask` for just the columns the query
  reads — but currently the metadata cache stores the full view because
  it's shared across queries with different projections. Splitting
  full-view-vs-projected-view is the next thing to try.
- The `infer_stats_and_ordering` path is still per-file per-query and
  computes statistics for **all** 1024 columns even though the query
  uses 4. Making `Statistics::column_statistics` lazy (compute per
  column on first access) would cut this directly.
- `Schema::index_of` and `Fields::find` are O(N) linear scans in arrow.
  Profile shows them at ~0.5% combined for Q04 — tiny in this benchmark
  but they're called from every PruningPredicate construction and their
  callers in DataFusion already take pains to avoid them in hot loops.
  Adding a lazy name → index hashmap to `Fields` would future-proof
  any code that lookups by name without thinking about it. Not done in
  this pass because the immediate impact is small.
