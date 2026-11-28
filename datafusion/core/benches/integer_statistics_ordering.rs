// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Benchmarks planning and execution time separately for ordered integer
//! expressions over a Parquet file with exact column statistics.
//!
//! Workloads:
//! - `safe_arithmetic`: ordered integer arithmetic where exact bounds rule out
//!   overflow and allow sort elimination.
//! - `overflow_arithmetic`: ordered integer arithmetic whose bounds allow
//!   overflow, so the sort cannot be eliminated.
//! - `bounded_wide_projection`: 64-column arithmetic projection over bounded
//!   integer columns with `collect_statistics = true` and no sort removed.
//! - `unchanged_no_stats_wide_projection`: the same wide projection with
//!   `collect_statistics = false`.
//!
//! Run with:
//! ```sh
//! cargo bench -p datafusion --bench integer_statistics_ordering
//! ```

use std::fs::File;
use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::execution::SessionState;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_expr::LogicalPlan;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const NUM_ROWS: usize = 1_000_000;
const WRITE_BATCH_SIZE: usize = 50_000;
const EXTRA_INT_COLS: usize = 62;
const TARGET_PARTITIONS: usize = 4;

struct PreparedCase {
    name: &'static str,
    state: SessionState,
    task_ctx: Arc<TaskContext>,
    logical_plan: LogicalPlan,
    physical_plan: Arc<dyn ExecutionPlan>,
}

fn write_sorted_parquet(path: &Path) {
    let mut fields = Vec::with_capacity(2 + EXTRA_INT_COLS);
    fields.push(Field::new("a", DataType::Int64, false));
    fields.push(Field::new("b", DataType::Int64, false));
    for i in 0..EXTRA_INT_COLS {
        fields.push(Field::new(format!("c{i}"), DataType::Int64, false));
    }
    let schema = Arc::new(Schema::new(fields));

    let file = File::create(path).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).unwrap();

    let b_base = i64::MAX - (NUM_ROWS as i64 - 1);
    for start in (0..NUM_ROWS).step_by(WRITE_BATCH_SIZE) {
        let end = (start + WRITE_BATCH_SIZE).min(NUM_ROWS);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(2 + EXTRA_INT_COLS);
        columns.push(Arc::new(Int64Array::from_iter_values(
            (start..end).map(|i| i as i64),
        )));
        columns.push(Arc::new(Int64Array::from_iter_values(
            (start..end).map(|i| b_base + i as i64),
        )));
        for col_idx in 0..EXTRA_INT_COLS {
            columns.push(Arc::new(Int64Array::from_iter_values(
                (start..end).map(|i| ((i + col_idx) % 1000) as i64),
            )));
        }
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
}

async fn setup_context(parquet_path: &Path, collect_statistics: bool) -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(TARGET_PARTITIONS)
        .with_collect_statistics(collect_statistics);
    let ctx = SessionContext::new_with_config(config);
    let sql = format!(
        "CREATE EXTERNAL TABLE t STORED AS PARQUET \
         WITH ORDER (a ASC NULLS LAST) \
         WITH ORDER (b ASC NULLS LAST) \
         LOCATION '{}'",
        parquet_path.display()
    );
    ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    ctx
}

fn wide_projection_sql() -> String {
    let extra: Vec<String> = (0..EXTRA_INT_COLS)
        .map(|i| format!("c{i} + 1 AS p{i}"))
        .collect();
    format!("SELECT a + 1 AS x, b + 1 AS y, {} FROM t", extra.join(", "))
}

fn prepare_case(
    rt: &Runtime,
    name: &'static str,
    ctx: &SessionContext,
    sql: &str,
) -> PreparedCase {
    let state = ctx.state();
    let task_ctx = ctx.task_ctx();
    let logical_plan = rt.block_on(state.create_logical_plan(sql)).unwrap();
    let physical_plan = rt
        .block_on(state.create_physical_plan(&logical_plan))
        .unwrap();
    let batches = rt
        .block_on(collect(Arc::clone(&physical_plan), Arc::clone(&task_ctx)))
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, NUM_ROWS, "unexpected row count for {name}");

    PreparedCase {
        name,
        state,
        task_ctx,
        logical_plan,
        physical_plan,
    }
}

fn bench_integer_statistics_ordering(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let tmp_dir = TempDir::new().unwrap();
    let parquet_path = tmp_dir.path().join("ordered_ints.parquet");
    write_sorted_parquet(&parquet_path);

    let ctx_stats = rt.block_on(setup_context(&parquet_path, true));
    let ctx_no_stats = rt.block_on(setup_context(&parquet_path, false));
    let wide_sql = wide_projection_sql();

    let cases = [
        prepare_case(
            &rt,
            "safe_arithmetic",
            &ctx_stats,
            "SELECT a + 1 AS x FROM t ORDER BY x",
        ),
        prepare_case(
            &rt,
            "overflow_arithmetic",
            &ctx_stats,
            "SELECT b + 1 AS x FROM t ORDER BY x",
        ),
        prepare_case(&rt, "bounded_wide_projection", &ctx_stats, &wide_sql),
        prepare_case(
            &rt,
            "unchanged_no_stats_wide_projection",
            &ctx_no_stats,
            &wide_sql,
        ),
    ];

    let mut plan_group = c.benchmark_group("integer_stats_ordering/planning");
    for case in &cases {
        plan_group.bench_function(case.name, |b| {
            b.iter(|| {
                black_box(
                    rt.block_on(case.state.create_physical_plan(&case.logical_plan))
                        .unwrap(),
                )
            })
        });
    }
    plan_group.finish();

    let mut exec_group = c.benchmark_group("integer_stats_ordering/execution");
    exec_group.sample_size(20);
    for case in &cases {
        exec_group.bench_function(case.name, |b| {
            b.iter(|| {
                black_box(
                    rt.block_on(collect(
                        Arc::clone(&case.physical_plan),
                        Arc::clone(&case.task_ctx),
                    ))
                    .unwrap(),
                )
            })
        });
    }
    exec_group.finish();
}

criterion_group!(benches, bench_integer_statistics_ordering);
criterion_main!(benches);
