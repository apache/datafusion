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

//! Benchmarks for row-filter pushdown with predicates that reach several struct
//! leaves under a common prefix.
//!
//! The existing `parquet_struct_query` bench exercises single-field struct
//! predicates only; `parquet_struct_projection` has no `WHERE` clause. Neither
//! drives the row-filter planner with multiple accesses under the same struct
//! root.
//!
//! Two properties of the planner shape these queries, and both are easy to get
//! wrong:
//!
//! * `execution.parquet.pushdown_filters` must be enabled. It defaults to
//!   `false`, in which case no row filter is built and every case below
//!   degenerates to a plain scan.
//! * The predicate must be a *single* conjunct. `build_row_filter` calls
//!   `split_conjunction` before building filter candidates, and each candidate
//!   collects its own access paths, so `s['a'] = 5 AND s['b'] = 5` becomes two
//!   independent single-access candidates and never reaches multi-access
//!   planning. The cases below use the `(s['a'] + s['b']) = 10` form so every
//!   access lands in one candidate.
//!
//! Nested access is written `s['inner']['x']`, which the planner represents as a
//! single flattened `get_field(s, 'inner', 'x')`. That form is pushdown-eligible;
//! a chained `get_field(get_field(s, 'inner'), 'x')` is not.
//!
//! Dataset schema:
//!
//! ```sql
//! CREATE TABLE t (
//!     id INT,
//!     s STRUCT<
//!         a INT, b INT, c INT, d INT, e INT,
//!         inner STRUCT<x INT, y INT, z INT>
//!     >
//! );
//! ```
//!
//! All struct leaves mirror the top-level `id`, so a sum of `n` leaves equals
//! `n * id` and every predicate is satisfied by exactly one row (`id = 5`).
//! Holding the match count fixed keeps the cases comparable, and each is
//! asserted to return that single row.

use arrow::array::{ArrayRef, Int32Array, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::instant::Instant;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};
use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

/// The number of batches to write
const NUM_BATCHES: usize = 128;
/// The number of rows in each record batch to write
const WRITE_RECORD_BATCH_SIZE: usize = 4096;
/// The number of rows in a row group
const ROW_GROUP_ROW_COUNT: usize = 65536;
/// The number of row groups expected
const EXPECTED_ROW_GROUPS: usize = 8;
/// Number of rows every predicate is expected to match.
const EXPECTED_MATCHES: usize = 1;

fn inner_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Int32, false),
        Field::new("z", DataType::Int32, false),
    ])
}

fn struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
        Field::new("d", DataType::Int32, false),
        Field::new("e", DataType::Int32, false),
        Field::new("inner", DataType::Struct(inner_struct_fields()), false),
    ])
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("s", DataType::Struct(struct_fields()), false),
    ]))
}

fn generate_batch(batch_id: usize) -> RecordBatch {
    let schema = schema();
    let len = WRITE_RECORD_BATCH_SIZE;

    // Sequential IDs give distinct per-row values so a predicate like
    // `s['a'] = 5` matches exactly one row, mirroring parquet_struct_query.
    let base_id = (batch_id * len) as i32;
    let id_values: Vec<i32> = (0..len).map(|i| base_id + i as i32).collect();
    let id_array = Arc::new(Int32Array::from(id_values.clone()));

    let leaf = || Arc::new(Int32Array::from(id_values.clone())) as ArrayRef;

    let inner_struct = StructArray::from(vec![
        (Arc::new(Field::new("x", DataType::Int32, false)), leaf()),
        (Arc::new(Field::new("y", DataType::Int32, false)), leaf()),
        (Arc::new(Field::new("z", DataType::Int32, false)), leaf()),
    ]);

    let struct_array = StructArray::from(vec![
        (Arc::new(Field::new("a", DataType::Int32, false)), leaf()),
        (Arc::new(Field::new("b", DataType::Int32, false)), leaf()),
        (Arc::new(Field::new("c", DataType::Int32, false)), leaf()),
        (Arc::new(Field::new("d", DataType::Int32, false)), leaf()),
        (Arc::new(Field::new("e", DataType::Int32, false)), leaf()),
        (
            Arc::new(Field::new(
                "inner",
                DataType::Struct(inner_struct_fields()),
                false,
            )),
            Arc::new(inner_struct) as ArrayRef,
        ),
    ]);

    RecordBatch::try_new(schema, vec![id_array, Arc::new(struct_array)]).unwrap()
}

fn generate_file() -> NamedTempFile {
    let now = Instant::now();
    let mut named_file = tempfile::Builder::new()
        .prefix("parquet_struct_shared_prefix_pushdown")
        .suffix(".parquet")
        .tempfile()
        .unwrap();

    println!("Generating parquet file - {}", named_file.path().display());
    let schema = schema();

    let properties = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(ROW_GROUP_ROW_COUNT))
        .build();

    let mut writer =
        ArrowWriter::try_new(&mut named_file, schema, Some(properties)).unwrap();

    for batch_id in 0..NUM_BATCHES {
        let batch = generate_batch(batch_id);
        writer.write(&batch).unwrap();
    }

    let metadata = writer.close().unwrap();
    let file_metadata = metadata.file_metadata();
    let expected_rows = WRITE_RECORD_BATCH_SIZE * NUM_BATCHES;
    assert_eq!(
        file_metadata.num_rows() as usize,
        expected_rows,
        "Expected {expected_rows} rows but got {}",
        file_metadata.num_rows()
    );
    assert_eq!(
        metadata.row_groups().len(),
        EXPECTED_ROW_GROUPS,
        "Expected {EXPECTED_ROW_GROUPS} row groups but got {}",
        metadata.row_groups().len()
    );

    println!(
        "Generated parquet file with {} rows and {} row groups in {:.2}s",
        file_metadata.num_rows(),
        metadata.row_groups().len(),
        now.elapsed().as_secs_f32()
    );

    named_file
}

fn create_context(file_path: &str, rt: &Runtime) -> SessionContext {
    let mut config = SessionConfig::new();
    // Row-filter pushdown is off by default. Without it no row filter is built,
    // and these benchmarks would time a plain scan for every predicate shape.
    config.options_mut().execution.parquet.pushdown_filters = true;

    let ctx = SessionContext::new_with_config(config);
    rt.block_on(ctx.register_parquet("t", file_path, Default::default()))
        .unwrap();
    ctx
}

fn query(ctx: &SessionContext, rt: &Runtime, sql: &str) {
    let ctx = ctx.clone();
    let sql = sql.to_string();
    let df = rt.block_on(ctx.sql(&sql)).unwrap();
    black_box(rt.block_on(df.collect()).unwrap());
}

/// Fails unless `sql` actually pushes a row filter into the Parquet decoder and
/// matches [`EXPECTED_MATCHES`] rows.
///
/// Guards the two silent-failure modes: a disabled `pushdown_filters` and a
/// predicate shape that turns out not to be pushdown-eligible. Either would
/// leave the benchmark timing a plain scan instead of row-filter pushdown.
///
/// Metrics are read off the executed plan rather than scraped from
/// `EXPLAIN ANALYZE` text, so the check does not depend on output formatting.
fn assert_pushdown_active(ctx: &SessionContext, rt: &Runtime, name: &str, sql: &str) {
    let (rows, pruned) = rt
        .block_on(async {
            let plan = ctx.sql(sql).await?.create_physical_plan().await?;
            let batches =
                datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
                    .await?;
            let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
            Ok::<_, datafusion_common::DataFusionError>((rows, rows_pruned(&plan)))
        })
        .unwrap();

    assert_eq!(
        rows, EXPECTED_MATCHES,
        "`{name}` matched {rows} rows, expected {EXPECTED_MATCHES}"
    );
    assert!(
        pruned > 0,
        "`{name}` pruned no rows via the Parquet row filter, so it does not \
         exercise row-filter pushdown (is `pushdown_filters` enabled, and is \
         the predicate a single pushdown-eligible conjunct?)"
    );
}

/// Total `pushdown_rows_pruned` reported anywhere in the executed plan.
fn rows_pruned(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> usize {
    let mut total = plan
        .metrics()
        .and_then(|metrics| metrics.sum_by_name("pushdown_rows_pruned"))
        .map(|value| value.as_usize())
        .unwrap_or(0);

    for child in plan.children() {
        total += rows_pruned(child);
    }

    total
}

fn criterion_benchmark(c: &mut Criterion) {
    let (file_path, temp_file) = match std::env::var("PARQUET_FILE") {
        Ok(file) => (file, None),
        Err(_) => {
            let temp_file = generate_file();
            (temp_file.path().display().to_string(), Some(temp_file))
        }
    };

    assert!(Path::new(&file_path).exists(), "path not found");
    println!("Using parquet file {file_path}");

    let rt = Runtime::new().unwrap();
    let ctx = create_context(&file_path, &rt);

    // Baseline: one access on a single struct leaf.
    let sql = "select id from t where s['a'] = 5";
    assert_pushdown_active(&ctx, &rt, "1_access", sql);
    c.bench_function("1_access", |b| b.iter(|| query(&ctx, &rt, sql)));

    // Two accesses inside one conjunct, sharing the struct root `s`.
    let sql = "select id from t where (s['a'] + s['b']) = 10";
    assert_pushdown_active(&ctx, &rt, "2_access_shared_root", sql);
    c.bench_function("2_access_shared_root", |b| b.iter(|| query(&ctx, &rt, sql)));

    // Three accesses sharing the struct root `s`.
    let sql = "select id from t where (s['a'] + s['b'] + s['c']) = 15";
    assert_pushdown_active(&ctx, &rt, "3_access_shared_root", sql);
    c.bench_function("3_access_shared_root", |b| b.iter(|| query(&ctx, &rt, sql)));

    // Five accesses sharing the struct root `s`, amplifying planning cost.
    let sql = "select id from t \
               where (s['a'] + s['b'] + s['c'] + s['d'] + s['e']) = 25";
    assert_pushdown_active(&ctx, &rt, "5_access_shared_root", sql);
    c.bench_function("5_access_shared_root", |b| b.iter(|| query(&ctx, &rt, sql)));

    // Two accesses sharing the deeper prefix `s.inner`.
    let sql = "select id from t where (s['inner']['x'] + s['inner']['y']) = 10";
    assert_pushdown_active(&ctx, &rt, "2_access_shared_nested_prefix", sql);
    c.bench_function("2_access_shared_nested_prefix", |b| {
        b.iter(|| query(&ctx, &rt, sql))
    });

    // Three accesses sharing the deeper prefix `s.inner`.
    let sql = "select id from t \
               where (s['inner']['x'] + s['inner']['y'] + s['inner']['z']) = 15";
    assert_pushdown_active(&ctx, &rt, "3_access_shared_nested_prefix", sql);
    c.bench_function("3_access_shared_nested_prefix", |b| {
        b.iter(|| query(&ctx, &rt, sql))
    });

    // Mix: two accesses on `s` leaves and two on `s.inner` leaves.
    let sql = "select id from t \
               where (s['a'] + s['b'] + s['inner']['x'] + s['inner']['y']) = 20";
    assert_pushdown_active(&ctx, &rt, "mixed_depth_shared_prefix", sql);
    c.bench_function("mixed_depth_shared_prefix", |b| {
        b.iter(|| query(&ctx, &rt, sql))
    });

    // Temporary file must outlive the benchmarks, it is deleted when dropped
    drop(temp_file);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
