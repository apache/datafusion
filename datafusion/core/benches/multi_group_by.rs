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

//! Benchmarks for multi-column GROUP BY performance comparing vectorized
//! (`GroupValuesColumn`) vs row-based (`GroupValuesRows`) implementations.
//!
//! Motivated by <https://github.com/apache/datafusion/issues/17850> which
//! showed vectorized can regress 33% for low-cardinality, high-row-count
//! scenarios (3 cols, 64 groups, 1B rows).
//!
//! Design:
//! - **Implementation comparison**: Int32 columns trigger the vectorized path;
//!   FixedSizeBinary(4) columns trigger the row-based fallback.
//! - **Execution-only timing**: Pre-optimizes the logical plan once; each
//!   iteration only does physical planning + execution.
//! - **Exact cardinality**: Sequential enumeration guarantees precise NDV.
//! - **Regression scenario**: Reproduces issue #17850 conditions (few groups,
//!   many rows) to identify the crossover point.

use arrow::array::{ArrayRef, FixedSizeBinaryArray, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::execution::SessionState;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_expr::LogicalPlan;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

const DEFAULT_NUM_ROWS: usize = 1_000_000;
const DEFAULT_BATCH_SIZE: usize = 8192;

#[derive(Clone, Copy, PartialEq, Eq)]
enum GroupMode {
    Vectorized,
    RowBased,
}

impl std::fmt::Display for GroupMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GroupMode::Vectorized => write!(f, "vectorized"),
            GroupMode::RowBased => write!(f, "row_based"),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DataPattern {
    /// Sequential enumeration: deterministic cycling through all group combinations.
    Sequential,
    /// Random: each column independently picks a value from 0..per_col_card using a seeded RNG.
    /// This matches the data generation in issue #17850 (random() * 4)::integer.
    Random,
}

impl std::fmt::Display for DataPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataPattern::Sequential => write!(f, "sequential"),
            DataPattern::Random => write!(f, "random"),
        }
    }
}

fn build_group_by_sql(num_cols: usize) -> String {
    let cols: Vec<String> = (0..num_cols).map(|i| format!("col_{i}")).collect();
    let col_list = cols.join(", ");
    format!("SELECT {col_list} FROM t GROUP BY {col_list}")
}

fn generate_parquet_file(
    num_cols: usize,
    num_distinct_groups: usize,
    num_rows: usize,
    batch_size: usize,
    mode: GroupMode,
    pattern: DataPattern,
) -> NamedTempFile {
    let fields: Vec<Field> = (0..num_cols)
        .map(|i| {
            let dt = match mode {
                GroupMode::Vectorized => DataType::Int32,
                GroupMode::RowBased => DataType::FixedSizeBinary(4),
            };
            Field::new(format!("col_{i}"), dt, false)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let per_col_card = (num_distinct_groups as f64)
        .powf(1.0 / num_cols as f64)
        .ceil() as usize;

    let mut temp_file = tempfile::Builder::new()
        .prefix("multi_group_by")
        .suffix(".parquet")
        .tempfile()
        .unwrap();

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(num_rows))
        .build();

    let mut writer =
        ArrowWriter::try_new(&mut temp_file, Arc::clone(&schema), Some(props)).unwrap();

    let mut rng = StdRng::seed_from_u64(42);
    let num_batches = num_rows / batch_size;
    for batch_idx in 0..num_batches {
        let batch_start = batch_idx * batch_size;
        let columns: Vec<ArrayRef> = (0..num_cols)
            .map(|col_idx| match mode {
                GroupMode::Vectorized => {
                    let values: Vec<i32> = (0..batch_size)
                        .map(|row| match pattern {
                            DataPattern::Sequential => {
                                let global_row = batch_start + row;
                                let group_id = global_row % num_distinct_groups;
                                let divisor = per_col_card.pow(col_idx as u32);
                                ((group_id / divisor) % per_col_card) as i32
                            }
                            DataPattern::Random => {
                                rng.random_range(0..per_col_card as i32)
                            }
                        })
                        .collect();
                    Arc::new(Int32Array::from(values)) as ArrayRef
                }
                GroupMode::RowBased => {
                    let values: Vec<Vec<u8>> = (0..batch_size)
                        .map(|row| {
                            let val = match pattern {
                                DataPattern::Sequential => {
                                    let global_row = batch_start + row;
                                    let group_id = global_row % num_distinct_groups;
                                    let divisor = per_col_card.pow(col_idx as u32);
                                    ((group_id / divisor) % per_col_card) as i32
                                }
                                DataPattern::Random => {
                                    rng.random_range(0..per_col_card as i32)
                                }
                            };
                            val.to_le_bytes().to_vec()
                        })
                        .collect();
                    let refs: Vec<&[u8]> = values.iter().map(|v| v.as_slice()).collect();
                    Arc::new(
                        FixedSizeBinaryArray::try_from_iter(refs.into_iter()).unwrap(),
                    ) as ArrayRef
                }
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();
    temp_file
}

struct BenchContext {
    session_state: SessionState,
    logical_plan: LogicalPlan,
    _temp_file: NamedTempFile,
}

fn prepare_context(
    rt: &Runtime,
    num_cols: usize,
    num_distinct_groups: usize,
    num_rows: usize,
    batch_size: usize,
    mode: GroupMode,
    pattern: DataPattern,
) -> BenchContext {
    let temp_file = generate_parquet_file(
        num_cols,
        num_distinct_groups,
        num_rows,
        batch_size,
        mode,
        pattern,
    );
    let path = temp_file.path().to_str().unwrap().to_string();

    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(batch_size);
    let ctx = SessionContext::new_with_config(config);
    let sql = build_group_by_sql(num_cols);

    let (session_state, logical_plan) = rt.block_on(async {
        ctx.register_parquet("t", &path, Default::default())
            .await
            .unwrap();
        // Warm the OS page cache
        let warmup_df = ctx.sql(&sql).await.unwrap();
        let _ = warmup_df.collect().await.unwrap();
        // Pre-optimize: parse SQL and produce optimized logical plan once
        let df = ctx.sql(&sql).await.unwrap();
        df.into_parts()
    });

    BenchContext {
        session_state,
        logical_plan,
        _temp_file: temp_file,
    }
}

fn execute_query(rt: &Runtime, state: &SessionState, plan: &LogicalPlan) {
    rt.block_on(async {
        let physical_plan = state.create_physical_plan(plan).await.unwrap();
        let task_ctx = state.task_ctx();
        let results = collect(physical_plan, task_ctx).await.unwrap();
        black_box(results);
    });
}

/// Experiment 1: Reproduce issue #17850 scenario.
/// 3 columns, cardinality 4 per column (=64 groups), varying row counts.
/// This is the exact configuration where vectorized regressed 33%.
fn bench_issue_17850_regression(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("issue_17850_regression");
    group.sample_size(10);

    let num_cols = 3;
    let num_groups = 64; // 4^3

    // Scale row count to find where overhead dominates
    for num_rows in [1_000_000, 5_000_000, 10_000_000, 20_000_000, 50_000_000] {
        for mode in [GroupMode::Vectorized, GroupMode::RowBased] {
            let b_ctx = prepare_context(
                &rt,
                num_cols,
                num_groups,
                num_rows,
                DEFAULT_BATCH_SIZE,
                mode,
                DataPattern::Sequential,
            );
            group.bench_with_input(
                BenchmarkId::new(format!("{mode}"), format!("{num_rows}_rows")),
                &num_rows,
                |b, _| {
                    b.iter(|| {
                        execute_query(&rt, &b_ctx.session_state, &b_ctx.logical_plan)
                    })
                },
            );
        }
    }
    group.finish();
}

/// Experiment 2: Low cardinality sweep.
/// Fixed 3-4 columns, very few groups (4-256), default row count.
/// Tests the per-batch overhead sensitivity.
fn bench_low_cardinality(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("low_cardinality");
    group.sample_size(15);

    // 3 cols with varying per-column cardinality
    for (num_cols, per_col_card) in
        [(3usize, 2usize), (3, 4), (3, 8), (4, 2), (4, 4), (4, 8)]
    {
        let num_groups = per_col_card.pow(num_cols as u32);
        for mode in [GroupMode::Vectorized, GroupMode::RowBased] {
            let b_ctx = prepare_context(
                &rt,
                num_cols,
                num_groups,
                DEFAULT_NUM_ROWS,
                DEFAULT_BATCH_SIZE,
                mode,
                DataPattern::Sequential,
            );
            group.bench_with_input(
                BenchmarkId::new(
                    format!("{mode}"),
                    format!("cols_{num_cols}_card_{per_col_card}_grp_{num_groups}"),
                ),
                &num_groups,
                |b, _| {
                    b.iter(|| {
                        execute_query(&rt, &b_ctx.session_state, &b_ctx.logical_plan)
                    })
                },
            );
        }
    }
    group.finish();
}

/// Experiment 3: Batch size sensitivity.
/// Tests how different batch sizes affect the per-batch overhead ratio.
/// Issue #17850 suggests vectorized has higher fixed per-batch cost.
fn bench_batch_size_sensitivity(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("batch_size_sensitivity");
    group.sample_size(10);

    let num_cols = 3;
    let num_groups = 64; // 4^3, same as issue #17850

    for batch_size in [1024, 4096, 8192, 16384, 32768] {
        for mode in [GroupMode::Vectorized, GroupMode::RowBased] {
            let b_ctx = prepare_context(
                &rt,
                num_cols,
                num_groups,
                DEFAULT_NUM_ROWS,
                batch_size,
                mode,
                DataPattern::Sequential,
            );
            group.bench_with_input(
                BenchmarkId::new(format!("{mode}"), format!("batch_{batch_size}")),
                &batch_size,
                |b, _| {
                    b.iter(|| {
                        execute_query(&rt, &b_ctx.session_state, &b_ctx.logical_plan)
                    })
                },
            );
        }
    }
    group.finish();
}

/// Experiment 4: Column count scaling.
/// Fixed low group count, increasing columns to isolate per-column overhead.
fn bench_column_scaling(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("column_scaling");
    group.sample_size(15);

    let fixed_low_cases: &[(usize, usize)] =
        &[(2, 100), (3, 125), (4, 81), (6, 729), (8, 256), (10, 1024)];

    for &(num_cols, num_groups) in fixed_low_cases {
        for mode in [GroupMode::Vectorized, GroupMode::RowBased] {
            let b_ctx = prepare_context(
                &rt,
                num_cols,
                num_groups,
                DEFAULT_NUM_ROWS,
                DEFAULT_BATCH_SIZE,
                mode,
                DataPattern::Sequential,
            );
            group.bench_with_input(
                BenchmarkId::new(
                    format!("{mode}"),
                    format!("cols_{num_cols}_grp_{num_groups}"),
                ),
                &num_cols,
                |b, _| {
                    b.iter(|| {
                        execute_query(&rt, &b_ctx.session_state, &b_ctx.logical_plan)
                    })
                },
            );
        }
    }
    group.finish();
}

/// Experiment 5: High cardinality column scaling.
/// ~1M groups with increasing column count.
fn bench_high_cardinality_scaling(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("high_cardinality_scaling");
    group.sample_size(10);

    for num_cols in [2, 3, 4, 6, 8, 10] {
        let num_groups = DEFAULT_NUM_ROWS; // 1M groups = ~unique per row
        for mode in [GroupMode::Vectorized, GroupMode::RowBased] {
            let b_ctx = prepare_context(
                &rt,
                num_cols,
                num_groups,
                DEFAULT_NUM_ROWS,
                DEFAULT_BATCH_SIZE,
                mode,
                DataPattern::Sequential,
            );
            group.bench_with_input(
                BenchmarkId::new(format!("{mode}"), format!("cols_{num_cols}_grp_1M")),
                &num_cols,
                |b, _| {
                    b.iter(|| {
                        execute_query(&rt, &b_ctx.session_state, &b_ctx.logical_plan)
                    })
                },
            );
        }
    }
    group.finish();
}

/// Experiment 6: Group count crossover search.
/// Fixed 4 columns, sweep group count to find crossover point.
fn bench_group_count_sweep(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("group_count_sweep");
    group.sample_size(15);

    for num_groups in [
        16, 64, 256, 1000, 5000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
    ] {
        for mode in [GroupMode::Vectorized, GroupMode::RowBased] {
            let b_ctx = prepare_context(
                &rt,
                4,
                num_groups,
                DEFAULT_NUM_ROWS,
                DEFAULT_BATCH_SIZE,
                mode,
                DataPattern::Sequential,
            );
            group.bench_with_input(
                BenchmarkId::new(format!("{mode}"), format!("grp_{num_groups}")),
                &num_groups,
                |b, _| {
                    b.iter(|| {
                        execute_query(&rt, &b_ctx.session_state, &b_ctx.logical_plan)
                    })
                },
            );
        }
    }
    group.finish();
}

/// Experiment 7: Random vs sequential data pattern.
/// Tests whether random access patterns (matching issue #17850's exact SQL using
/// `(random() * 4)::integer`) change the performance characteristics due to cache
/// effects in hash probing.
/// 3 cols, per_col_card=4 (64 theoretical groups), 1M and 5M rows.
fn bench_random_vs_sequential(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("random_vs_sequential");
    group.sample_size(10);

    let num_cols = 3;
    let num_groups = 64; // 4^3

    for num_rows in [1_000_000, 5_000_000] {
        for pattern in [DataPattern::Sequential, DataPattern::Random] {
            for mode in [GroupMode::Vectorized, GroupMode::RowBased] {
                let b_ctx = prepare_context(
                    &rt,
                    num_cols,
                    num_groups,
                    num_rows,
                    DEFAULT_BATCH_SIZE,
                    mode,
                    pattern,
                );
                group.bench_with_input(
                    BenchmarkId::new(
                        format!("{mode}/{pattern}"),
                        format!("{num_rows}_rows"),
                    ),
                    &num_rows,
                    |b, _| {
                        b.iter(|| {
                            execute_query(&rt, &b_ctx.session_state, &b_ctx.logical_plan)
                        })
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_issue_17850_regression,
    bench_low_cardinality,
    bench_batch_size_sensitivity,
    bench_column_scaling,
    bench_high_cardinality_scaling,
    bench_group_count_sweep,
    bench_random_vs_sequential,
);
criterion_main!(benches);
