// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Microbenchmarks for [`SelectivityTracker`] hot paths.
//!
//! These benches isolate the tracker from decoder/IO so we can iterate on
//! its data structures independently. The scenarios model the load a
//! ClickBench-style partitioned query puts on the tracker:
//!
//! - a file is opened and each of its row-group morsels asks the tracker
//!   where to place each user filter (`partition_filters`);
//! - inside each morsel the decoder hands us one `RecordBatch` at a time
//!   and each batch feeds selectivity stats to the tracker (`update`).
//!
//! With the default ClickBench-partitioned workload (100 files × ~2–3
//! row-group morsels × ~125 batches-per-morsel × ~1–3 filters-per-query),
//! the `update` path fires tens of thousands of times per query and
//! `partition_filters` fires hundreds — both on the scan critical path.
//!
//! Each bench reports the cost of a single representative operation so
//! the per-query overhead follows by simple multiplication.

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_datasource_parquet::selectivity::{
    FilterId, SelectivityTracker, TrackerConfig,
};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column;
use parquet::basic::{LogicalType, Type as PhysicalType};
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData,
};
use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor, Type as SchemaType};

/// How many files a ClickBench-partitioned query typically opens.
const NUM_FILES: usize = 100;
/// Morsels per file — two full-row-group chunks is typical for hits_partitioned.
const MORSELS_PER_FILE: usize = 3;
/// Batches per morsel (row_group_rows / batch_size ≈ 500k / 8k).
const BATCHES_PER_MORSEL: usize = 60;
/// Filters per query — matches the worst regressed ClickBench queries.
const FILTERS_PER_QUERY: usize = 3;

fn build_columns(n: usize) -> SchemaDescPtr {
    let fields: Vec<_> = (0..n)
        .map(|i| {
            let name = format!("c{i}");
            SchemaType::primitive_type_builder(&name, PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .build()
                .unwrap()
                .into()
        })
        .collect();
    let group = SchemaType::group_type_builder("schema")
        .with_fields(fields)
        .build()
        .unwrap();
    Arc::new(SchemaDescriptor::new(Arc::new(group)))
}

/// One file with `rg_count` row groups, each nominally `rows_per_rg` rows,
/// `bytes_per_col` compressed bytes per column.
fn build_metadata(
    rg_count: usize,
    rows_per_rg: i64,
    num_cols: usize,
    bytes_per_col: i64,
) -> ParquetMetaData {
    let schema = build_columns(num_cols);
    let row_groups: Vec<_> = (0..rg_count)
        .map(|_| {
            let cols = (0..num_cols)
                .map(|c| {
                    ColumnChunkMetaData::builder(schema.column(c))
                        .set_num_values(rows_per_rg)
                        .set_total_compressed_size(bytes_per_col)
                        .build()
                        .unwrap()
                })
                .collect();
            RowGroupMetaData::builder(schema.clone())
                .set_num_rows(rows_per_rg)
                .set_column_metadata(cols)
                .build()
                .unwrap()
        })
        .collect();
    let total_rows = rg_count as i64 * rows_per_rg;
    let file_meta = FileMetaData::new(1, total_rows, None, None, schema, None);
    ParquetMetaData::new(file_meta, row_groups)
}

/// Produce `F` user filters, each referencing a single column. Column 0 is
/// shared by filter 0 and the projection (filter-in-projection shape, as in
/// ClickBench Q14 `WHERE SearchPhrase <> ''`); the rest sit on columns
/// outside the projection.
fn make_filters(n: usize) -> Vec<(FilterId, Arc<dyn PhysicalExpr>)> {
    (0..n)
        .map(|i| {
            let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&format!("c{i}"), i));
            (i as FilterId, expr)
        })
        .collect()
}

/// Shared setup: tracker pre-warmed with one `partition_filters` call so
/// the filter stats / state entries exist. Models "second morsel onwards".
fn warm_tracker(
    config: TrackerConfig,
    filters: &[(FilterId, Arc<dyn PhysicalExpr>)],
    metadata: &ParquetMetaData,
) -> Arc<SelectivityTracker> {
    let tracker = Arc::new(config.build());
    // Seed with a round-trip so HashMap entries exist; otherwise the first
    // bench iteration pays the "new filter" insertion cost and later ones
    // don't.
    let _ = tracker.partition_filters_for_test(
        filters.to_vec(),
        &std::collections::HashSet::new(),
        1_000_000,
        metadata,
    );
    tracker
}

/// Per-batch `update` cost. This is the tightest loop — it fires once per
/// decoded batch per active filter. At ClickBench scale that's
/// NUM_FILES × MORSELS_PER_FILE × BATCHES_PER_MORSEL × FILTERS =
/// 54,000 calls per query, so every nanosecond here matters.
fn bench_update(c: &mut Criterion) {
    let metadata = build_metadata(2, 500_000, 4, 10_000_000);
    let filters = make_filters(FILTERS_PER_QUERY);
    let tracker = warm_tracker(TrackerConfig::new(), &filters, &metadata);

    let mut group = c.benchmark_group("selectivity_tracker/update");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("single_call", |b| {
        let id = filters[0].0;
        b.iter(|| {
            tracker.update(
                std::hint::black_box(id),
                std::hint::black_box(4_096),
                std::hint::black_box(8_192),
                std::hint::black_box(50_000),
                std::hint::black_box(65_536),
            );
        })
    });

    // A realistic per-batch hit: we update every active filter for this
    // batch. Mirrors `apply_post_scan_filters_with_stats` calling
    // `tracker.update` once per filter per batch.
    group.bench_function("per_batch_all_filters", |b| {
        b.iter(|| {
            for (id, _) in &filters {
                tracker.update(
                    std::hint::black_box(*id),
                    std::hint::black_box(4_096),
                    std::hint::black_box(8_192),
                    std::hint::black_box(50_000),
                    std::hint::black_box(65_536),
                );
            }
        })
    });
    group.finish();
}

/// Per-morsel `partition_filters` cost. Fires once per row-group morsel,
/// so NUM_FILES × MORSELS_PER_FILE ≈ 300 per query. We measure both the
/// "cold" (first) call and the "warm" (re-partition) case.
fn bench_partition_filters(c: &mut Criterion) {
    let metadata = build_metadata(2, 500_000, 4, 10_000_000);
    let filters = make_filters(FILTERS_PER_QUERY);
    let projection_bytes = 40_000_000usize;

    let mut group = c.benchmark_group("selectivity_tracker/partition_filters");
    group.bench_function("cold_first_call", |b| {
        b.iter_batched(
            || Arc::new(TrackerConfig::new().build()),
            |tracker| {
                std::hint::black_box(tracker.partition_filters_for_test(
                    filters.clone(),
                    &std::collections::HashSet::new(),
                    projection_bytes,
                    &metadata,
                ));
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // Warm case: tracker already has state for every filter, matches the
    // per-morsel path after morsel 0 of any file.
    let warm = warm_tracker(TrackerConfig::new(), &filters, &metadata);
    group.bench_function("warm_repeat_call", |b| {
        b.iter(|| {
            std::hint::black_box(warm.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                projection_bytes,
                &metadata,
            ));
        })
    });

    // Same warm case but after realistic stats have accumulated — this is
    // the path that also evaluates the confidence-bound promote/demote
    // branches. Seed the tracker with a credible number of `update` calls
    // before measuring.
    let promoted = warm_tracker(TrackerConfig::new(), &filters, &metadata);
    for _ in 0..500 {
        for (id, _) in &filters {
            promoted.update(*id, 3_000, 8_192, 50_000, 65_536);
        }
    }
    group.bench_function("warm_with_accumulated_stats", |b| {
        b.iter(|| {
            std::hint::black_box(promoted.partition_filters_for_test(
                filters.clone(),
                &std::collections::HashSet::new(),
                projection_bytes,
                &metadata,
            ));
        })
    });
    group.finish();
}

/// End-to-end "one file open" cost: one `partition_filters` per morsel
/// plus `update` per batch per filter. This matches what a single
/// ClickBench-partitioned file inflicts on the tracker and lets us read
/// the combined improvement from any optimization in one number.
fn bench_file_scan_simulation(c: &mut Criterion) {
    let metadata = build_metadata(2, 500_000, 4, 10_000_000);
    let filters = make_filters(FILTERS_PER_QUERY);
    let projection_bytes = 40_000_000usize;
    let warm = warm_tracker(TrackerConfig::new(), &filters, &metadata);

    let mut group = c.benchmark_group("selectivity_tracker/file_scan");
    group.throughput(criterion::Throughput::Elements(
        (MORSELS_PER_FILE * BATCHES_PER_MORSEL * FILTERS_PER_QUERY) as u64,
    ));
    group.bench_function("one_file", |b| {
        b.iter(|| {
            for _morsel in 0..MORSELS_PER_FILE {
                std::hint::black_box(warm.partition_filters_for_test(
                    filters.clone(),
                    &std::collections::HashSet::new(),
                    projection_bytes,
                    &metadata,
                ));
                for _batch in 0..BATCHES_PER_MORSEL {
                    for (id, _) in &filters {
                        warm.update(*id, 3_000, 8_192, 50_000, 65_536);
                    }
                }
            }
        })
    });
    group.finish();
}

/// Full-query simulation: [`NUM_FILES`] sequential file scans on a single
/// tracker instance. Closest approximation to the per-query tracker cost
/// a ClickBench user sees.
///
/// Parameterised on morsels-per-file so we can see how sensitive the
/// total cost is to the morsel-split fan-out.
fn bench_query_simulation(c: &mut Criterion) {
    let metadata = build_metadata(2, 500_000, 4, 10_000_000);
    let filters = make_filters(FILTERS_PER_QUERY);
    let projection_bytes = 40_000_000usize;

    let mut group = c.benchmark_group("selectivity_tracker/query");
    group.sample_size(20);
    for morsels in [1usize, 2, 3, 5] {
        group.bench_with_input(
            BenchmarkId::from_parameter(morsels),
            &morsels,
            |b, &morsels_per_file| {
                b.iter_batched(
                    || Arc::new(TrackerConfig::new().build()),
                    |tracker| {
                        for _file in 0..NUM_FILES {
                            for _morsel in 0..morsels_per_file {
                                std::hint::black_box(tracker.partition_filters_for_test(
                                    filters.clone(),
                                    &std::collections::HashSet::new(),
                                    projection_bytes,
                                    &metadata,
                                ));
                                for _batch in 0..BATCHES_PER_MORSEL {
                                    for (id, _) in &filters {
                                        tracker.update(*id, 3_000, 8_192, 50_000, 65_536);
                                    }
                                }
                            }
                        }
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_update,
    bench_partition_filters,
    bench_file_scan_simulation,
    bench_query_simulation,
);
criterion_main!(benches);
