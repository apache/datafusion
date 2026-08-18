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

//! Benchmarks for scanning a parquet table without a projection.
//!
//! `scan_construction` builds the physical scan directly and is where not
//! materializing an identity projection shows up: the work it removes is
//! proportional to the table's width.
//!
//! `planning` and `execution` are the end-to-end context for that, and are
//! controls rather than targets. Physical planning of `SELECT *` is dominated
//! by expanding the wildcard and running the optimizer over one expression per
//! column, and a full scan is dominated by decoding; neither moves measurably.
//! The narrow `SELECT c0` variants push a genuine projection and should not
//! move either.

use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::{Builder, NamedTempFile};
use tokio::runtime::Runtime;

/// Rows per written batch, and per row group, for the execution benchmarks
const EXEC_BATCH_ROWS: usize = 8192;
/// Number of batches written for the execution benchmarks
const EXEC_BATCHES: usize = 64;
/// Columns in the table scanned by the execution benchmarks
const EXEC_COLUMNS: usize = 128;

fn schema(num_columns: usize) -> SchemaRef {
    Arc::new(Schema::new(
        (0..num_columns)
            .map(|i| Field::new(format!("c{i}"), DataType::Int32, true))
            .collect::<Vec<_>>(),
    ))
}

fn batch(schema: &SchemaRef, num_rows: usize) -> RecordBatch {
    let columns: Vec<ArrayRef> = (0..schema.fields().len())
        .map(|i| {
            let values = (0..num_rows).map(|row| (row * i) as i32);
            Arc::new(Int32Array::from_iter_values(values)) as ArrayRef
        })
        .collect();
    RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
}

/// Write `num_batches` batches of `num_rows` rows over a `num_columns` wide
/// Int32 schema, and register the result as `t` in a fresh context.
fn context(
    rt: &Runtime,
    num_columns: usize,
    num_rows: usize,
    num_batches: usize,
) -> (SessionContext, NamedTempFile) {
    let schema = schema(num_columns);
    let mut file = Builder::new().suffix(".parquet").tempfile().unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(num_rows))
        .build();
    let mut writer =
        ArrowWriter::try_new(&mut file, Arc::clone(&schema), Some(properties)).unwrap();
    let batch = batch(&schema, num_rows);
    for _ in 0..num_batches {
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();

    let path = file.path().display().to_string();
    assert!(Path::new(&path).exists(), "path not found");

    let ctx = SessionContext::new();
    rt.block_on(ctx.register_parquet("t", &path, Default::default()))
        .unwrap();
    (ctx, file)
}

fn physical_plan(ctx: &SessionContext, rt: &Runtime, sql: &str) {
    black_box(rt.block_on(async {
        ctx.sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }));
}

fn collect(ctx: &SessionContext, rt: &Runtime, sql: &str) {
    black_box(
        rt.block_on(async { ctx.sql(sql).await.unwrap().collect().await.unwrap() }),
    );
}

fn planning_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("planning");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));

    for num_columns in [100, 1000] {
        let (ctx, _file) = context(&rt, num_columns, 1, 1);

        group.bench_function(format!("select_all_{num_columns}_columns"), |b| {
            b.iter(|| physical_plan(&ctx, &rt, "SELECT * FROM t"))
        });
        group.bench_function(format!("select_one_of_{num_columns}_columns"), |b| {
            b.iter(|| physical_plan(&ctx, &rt, "SELECT c0 FROM t"))
        });
    }

    group.finish();
}

fn execution_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (ctx, _file) = context(&rt, EXEC_COLUMNS, EXEC_BATCH_ROWS, EXEC_BATCHES);

    let mut group = c.benchmark_group("execution");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("select_all", |b| {
        b.iter(|| collect(&ctx, &rt, "SELECT * FROM t"))
    });
    group.bench_function("select_one", |b| {
        b.iter(|| collect(&ctx, &rt, "SELECT c0 FROM t"))
    });

    group.finish();
}

/// Build the physical scan directly, without the SQL front end: this is the
/// work that is proportional to the table's width, and at these widths it is
/// swamped end-to-end by `SELECT *` expansion in the logical planner.
fn scan_construction_benchmarks(c: &mut Criterion) {
    use datafusion::datasource::physical_plan::ParquetSource;
    use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
    use datafusion_datasource::source::DataSourceExec;
    use datafusion_execution::object_store::ObjectStoreUrl;

    let mut group = c.benchmark_group("scan_construction");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));

    for num_columns in [1_000, 10_000, 100_000] {
        let schema = schema(num_columns);
        group.bench_function(format!("unprojected_{num_columns}_columns"), |b| {
            b.iter(|| {
                let source = ParquetSource::new(Arc::clone(&schema));
                let config = FileScanConfigBuilder::new(
                    ObjectStoreUrl::parse("test:///").unwrap(),
                    Arc::new(source),
                )
                .build();
                black_box(DataSourceExec::from_data_source(config))
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    scan_construction_benchmarks,
    planning_benchmarks,
    execution_benchmarks
);
criterion_main!(benches);
