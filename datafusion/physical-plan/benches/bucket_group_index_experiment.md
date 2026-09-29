# Bucket group index experiment

This experiment starts at `f9df89e2b1077deaae9be106682c9f1e4d0d61d2`, the sixth commit of [DataFusion PR #25724](https://github.com/apache/datafusion/pull/25724). It changes only the multi-column `GroupValuesColumn` index used by a finished aggregation bucket. Set `DATAFUSION_BUCKET_INDEX=hashbrown-half` or `linear` to select a variant. Unset it for the existing hashbrown policy. Non-bucket tables always use the existing policy.

## Method

- Hardware: Apple M4, 10 cores, 32 GB RAM, macOS arm64. Rust 1.98.1.
- Release profile (`lto = true`, one codegen unit), Criterion: 10 samples, 100 ms warm-up, 300 ms measurement, 1,000 resamples. Three process-level rounds in A/B/C, C/B/A, A/B/C order. The CSV includes all per-round estimates and their Criterion intervals.
- Identical generated Arrow arrays, aggregation hash seed, group key storage, equality, and `GroupValuesColumn::intern` for all variants. `build` starts with an empty table and includes index allocation and growth. `reuse` fills a table, calls `clear_shrink(groups)`, and measures a second interning pass with its index capacity retained. Input array construction is outside timed sections.
- The bucket router uses a different hash seed from group interning, so the low index bits used by linear probing are not fixed by routing.
- The integer keys use two `Int64` columns. Text keys use one `Utf8View` and one `Int64`. The hot case gives 90% of rows one key and spreads the other 10% across groups. All values are generated from fixed deterministic arithmetic.
- The stand-alone index diagnostic measures insertion, hit and miss separately, without Arrow hashing or equality. It is diagnostic, not a substitute for full interning. Its 5 runs and actual slot counts are in [bucket_index_probe_results.csv](bucket_index_probe_results.csv).
- These measurements are synthetic. No ClickBench `hits.parquet` or `hits_partitioned` data was present locally, so no real Q11/Q12/Q14 result was verified.

## Full group interning results

Median of the three Criterion estimates, in nanoseconds per input row. Lower is better. The raw results are in [bucket_group_index_results.csv](bucket_group_index_results.csv).

| Key workload                    |     Groups / rows | Phase | Default hashbrown | Hashbrown <=50% | Linear <=50% |
| ------------------------------- | ----------------: | ----- | ----------------: | --------------: | -----------: |
| Two integers, unique            |         768 / 768 | build |              12.8 |            12.4 |      **9.6** |
| Two integers, unique            |         768 / 768 | reuse |               6.7 |             6.8 |      **6.5** |
| Two integers, 8 repeats         |    8,192 / 65,536 | build |               8.8 |         **7.9** |          7.9 |
| Two integers, 8 repeats         |    8,192 / 65,536 | reuse |               7.5 |             7.5 |      **7.2** |
| Two integers, unique            |   24,576 / 24,576 | build |          **13.5** |            13.7 |         17.3 |
| Two integers, unique            |   24,576 / 24,576 | reuse |               7.0 |         **6.4** |          7.1 |
| Two integers, unique            |   98,304 / 98,304 | build |          **16.9** |            17.3 |         23.9 |
| Two integers, unique            |   98,304 / 98,304 | reuse |           **7.9** |             9.6 |         10.5 |
| Two integers, unique            | 393,216 / 393,216 | build |              20.2 |        **18.4** |         29.8 |
| Two integers, unique            | 393,216 / 393,216 | reuse |          **12.7** |            13.4 |         16.9 |
| Short text + integer, 8 repeats |    6,144 / 49,152 | build |           **8.3** |             8.7 |          8.6 |
| Short text + integer, 8 repeats |    6,144 / 49,152 | reuse |           **7.0** |             7.1 |          7.7 |
| Long text + integer, 8 repeats  |    6,144 / 49,152 | build |          **11.6** |            11.8 |         11.9 |
| Long text + integer, 8 repeats  |    6,144 / 49,152 | reuse |          **10.4** |            10.5 |         11.0 |
| Two integers, 90% hot           |    6,144 / 61,440 | build |           **8.2** |             8.6 |          8.4 |
| Two integers, 90% hot           |    6,144 / 61,440 | reuse |               7.9 |         **7.5** |          7.7 |

At 8,192 groups, the two hashbrown policies end with the same number of physical slots. The build advantage of the half-full policy disappears on reuse, so earlier growth and moving fewer existing entries explains much of that benefit. Linear probing retains a small reuse advantage there, but loses sharply as the index grows. The separate index diagnostic also found slower linear hit and miss probes at most tested sizes. These observations do not support replacing hashbrown for general bucket workloads.

The untimed linear-probe diagnostic found p95 hit and miss probe counts of 3 and 5 at 6,144 through 393,216 groups; p99 counts were 5 and 8. Short probe sequences alone did not make this scalar table faster than hashbrown's control-byte probing. CPU cycle and cache-miss counters were not available in this sandbox, so cache behavior remains an inference from timings and footprint.

## Index footprint

Hashbrown capacity is a count of insertable elements, not physical slots. The table's reported capacity was rounded to its power-of-two physical slot count in the diagnostic. The physical-byte estimate is `slots * 16 + slots + 16` for hashbrown's entries and control bytes, and `slots * 16` for the linear table. Actual allocator overhead is excluded. The default DataFusion `map_size` accounting approximates bytes from insertable capacity and is lower than this physical estimate; the experiment accounts the physical estimate for the half-full variant and the vector allocation for linear.

| Distinct groups | Default slots / occupancy / index | Half-full slots / occupancy / index | Linear slots / occupancy / index |
| --------------: | --------------------------------: | ----------------------------------: | -------------------------------: |
|             768 |              1,024 / 75% / 17 KiB |              2,048 / 37.5% / 34 KiB |           2,048 / 37.5% / 32 KiB |
|           6,144 |             8,192 / 75% / 136 KiB |            16,384 / 37.5% / 272 KiB |         16,384 / 37.5% / 256 KiB |
|          24,576 |            32,768 / 75% / 544 KiB |          65,536 / 37.5% / 1,088 KiB |       65,536 / 37.5% / 1,024 KiB |
|          98,304 |          131,072 / 75% / 2.13 MiB |          262,144 / 37.5% / 4.25 MiB |          262,144 / 37.5% / 4 MiB |
|         393,216 |           524,288 / 75% / 8.5 MiB |          1,048,576 / 37.5% / 17 MiB |       1,048,576 / 37.5% / 16 MiB |

At the largest size, the linear index alone is 16 MiB, before group keys, hash buffers, or accumulators. A bucket does not automatically imply a cache-resident index.

`GroupValuesColumn::size()` after interning 393,216 two-integer groups reported 15.1 MiB for default hashbrown, 25.1 MiB for half-full hashbrown, and 24.1 MiB for linear. Correcting the default's approximate index accounting with the physical index estimate raises it to about 16.6 MiB. For 6,144 long-text groups with eight input rows each, the corresponding reported sizes were 3.27, 3.42, and 3.41 MiB. These figures include retained key buffers and the group hash buffer but exclude the aggregate accumulator and allocator overhead. [All measured worksets](bucket_group_workset_results.csv).

## Synthetic final aggregation

A physical final aggregate merged 600,000 partial state rows into 300,000 `(Int32, Utf8View)` groups with `COUNT` state. Bucketing used the PR's 262,144-group threshold and split once. Five release-process rounds alternated variant order; each run compared bucketing off with bucketing on and checked identical sorted `(key, count)` results. The timer covered physical plan collection, including bucket routing and output, but excluded input construction and sorting for verification. [Raw runs](bucket_final_aggregation_results.csv).

| Variant with bucketing on | Off median | On median | On/off |
| ------------------------- | ---------: | --------: | -----: |
| Default hashbrown         |   15.19 ms |  23.04 ms |  1.52x |
| Half-full hashbrown       |   15.24 ms |  22.99 ms |  1.51x |
| Linear probing            |   14.29 ms |  23.77 ms |  1.66x |

The three bucketed variants are within about 3% here, while bucket routing and delayed aggregation make all of them slower than the unbucketed case. This has one input partition and no Parquet scan; it measures a final aggregate over synthetic state, not a ClickBench query. The off timings also vary across process rounds, so these small on-variant differences are not a production speedup claim.

## Correctness and coverage

The index still stores a complete 64-bit hash and `GroupIndexView`. Existing collision lists and real group key equality remain in `GroupValuesColumn`; an equal hash is never treated as proof of an equal key. Tests cover forced probe collisions, growth, retention, clear and reuse, null/NaN/zero and short/long `Utf8View` group interning. The final aggregate test checks identical results with bucketing off, one split, recursive splitting, and spilling under a 1 MiB memory limit. It passed with all three index variants.

The ClickBench runner uses zero-based `q11.sql` through `q34.sql`. Q12 and Q33 use single string keys and bypass this multi-column index. Q11's `COUNT(DISTINCT)` has nested state and is excluded from bucketing by PR #25724. Q14, Q18, and Q31 have multi-column keys and can exercise it if bucketing activates. Q34 includes a constant grouping key whose optimized physical path needs confirmation with real data. This experiment cannot establish whether ClickBench Q12's reported wall-clock regression is fixed; this index does not run on its single-key path.

## Reproduction

```bash
cargo fmt --all
cargo test -p datafusion-physical-plan --lib bucket_index_variants_preserve_group_interning
cargo test -p datafusion-physical-plan --lib final_multi_column_bucket_index_matches_single_table
cargo bench -p datafusion-physical-plan --bench bucket_group_index --no-run
DATAFUSION_BUCKET_INDEX=linear target/release/deps/bucket_group_index-<hash> --bench --noplot --sample-size 10 --warm-up-time 0.1 --measurement-time 0.3 --nresamples 1000
DATAFUSION_BUCKET_PERF=1 DATAFUSION_BUCKET_INDEX=linear cargo test --release -p datafusion-physical-plan --lib final_multi_column_bucket_index_matches_single_table -- --nocapture
```

Repeat both benchmark commands with `DATAFUSION_BUCKET_INDEX` unset and set to `hashbrown-half`, alternating run order. The bench binary hash depends on the local build.
