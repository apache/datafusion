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

//! Benchmarks for row-filter pushdown with predicates that share a
//! common struct-field prefix.
//!
//! The existing `parquet_struct_query` bench exercises single-field
//! struct predicates only; `parquet_struct_projection` has no `WHERE`
//! clause. Neither drives the row-filter planner with multiple
//! conjunctions over the same struct root, which is the case the
//! `StructAccessTree` planning path is intended to accelerate.
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
//! All struct leaves mirror the top-level `id`, so any conjunction on
//! them selects the same rows and the bench measures planning + read
//! cost rather than selectivity differences.

use arrow::array::{ArrayRef, Int32Array, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;
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

fn create_context(file_path: &str) -> SessionContext {
    let ctx = SessionContext::new();
    let rt = Runtime::new().unwrap();
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

    let ctx = create_context(&file_path);
    let rt = Runtime::new().unwrap();

    // Baseline: one predicate on a single struct leaf.
    c.bench_function("single_field", |b| {
        b.iter(|| query(&ctx, &rt, "select id from t where s['a'] = 5"))
    });

    // Two predicates sharing the struct root `s`.
    c.bench_function("two_conjunct_shared_root", |b| {
        b.iter(|| {
            query(
                &ctx,
                &rt,
                "select id from t where s['a'] = 5 and s['b'] = 5",
            )
        })
    });

    // Three predicates sharing the struct root `s`.
    c.bench_function("three_conjunct_shared_root", |b| {
        b.iter(|| {
            query(
                &ctx,
                &rt,
                "select id from t \
                 where s['a'] = 5 and s['b'] = 5 and s['c'] = 5",
            )
        })
    });

    // Five predicates sharing the struct root `s`, amplifying planning cost.
    c.bench_function("five_conjunct_shared_root", |b| {
        b.iter(|| {
            query(
                &ctx,
                &rt,
                "select id from t \
                 where s['a'] = 5 and s['b'] = 5 and s['c'] = 5 \
                   and s['d'] = 5 and s['e'] = 5",
            )
        })
    });

    // Predicates sharing the nested prefix `s.inner`.
    c.bench_function("nested_shared_prefix", |b| {
        b.iter(|| {
            query(
                &ctx,
                &rt,
                "select id from t \
                 where s['inner']['x'] = 5 and s['inner']['y'] = 5",
            )
        })
    });

    // Mix: two predicates on `s` leaves and two on `s.inner` leaves.
    c.bench_function("mixed_depth_shared_prefix", |b| {
        b.iter(|| {
            query(
                &ctx,
                &rt,
                "select id from t \
                 where s['a'] = 5 and s['b'] = 5 \
                   and s['inner']['x'] = 5 and s['inner']['y'] = 5",
            )
        })
    });

    // Temporary file must outlive the benchmarks, it is deleted when dropped
    drop(temp_file);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
