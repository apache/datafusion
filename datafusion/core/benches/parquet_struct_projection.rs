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

//! Benchmarks for struct leaf-level projection pruning in Parquet.
//!
//! Measures the benefit of reading only the needed leaf columns from a
//! struct column. Three dataset shapes are tested:
//!
//! 1. **Narrow struct** (2 leaves): one 128 KiB UTF-8 field + one INT field
//! 2. **Wide struct** (5 leaves): four 128 KiB UTF-8 fields + one INT field
//! 3. **Nested struct** (3 leaves): `STRUCT<inner: STRUCT<large_string, small_int>, extra_string>`
//!
//! In all cases, projecting just the small integer should skip decoding
//! all of the large string leaves, including through nested struct levels.

use arrow::array::{ArrayRef, Int32Array, StringBuilder, StructArray};
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
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

const NUM_BATCHES: usize = 2;
const WRITE_RECORD_BATCH_SIZE: usize = 256;
const ROW_GROUP_ROW_COUNT: usize = 256;
const EXPECTED_ROW_GROUPS: usize = 2;
const LARGE_STRING_LEN: usize = 16 * 1024;

fn narrow_schema() -> SchemaRef {
    let struct_fields = Fields::from(vec![
        Field::new("large_string", DataType::Utf8, false),
        Field::new("small_int", DataType::Int32, false),
    ]);
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("s", DataType::Struct(struct_fields), false),
    ]))
}

fn narrow_batch(batch_id: usize) -> RecordBatch {
    let schema = narrow_schema();
    let len = WRITE_RECORD_BATCH_SIZE;

    let base_id = (batch_id * len) as i32;
    let id_values: Vec<i32> = (0..len).map(|i| base_id + i as i32).collect();
    let id_array = Arc::new(Int32Array::from(id_values.clone()));

    let small_int_array = Arc::new(Int32Array::from(id_values));

    let large_string: String = "x".repeat(LARGE_STRING_LEN);
    let mut string_builder = StringBuilder::new();
    for _ in 0..len {
        string_builder.append_value(&large_string);
    }
    let large_string_array = Arc::new(string_builder.finish());

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("large_string", DataType::Utf8, false)),
            large_string_array as ArrayRef,
        ),
        (
            Arc::new(Field::new("small_int", DataType::Int32, false)),
            small_int_array as ArrayRef,
        ),
    ]);

    RecordBatch::try_new(schema, vec![id_array, Arc::new(struct_array)]).unwrap()
}

fn wide_schema() -> SchemaRef {
    let struct_fields = Fields::from(vec![
        Field::new("str_a", DataType::Utf8, false),
        Field::new("str_b", DataType::Utf8, false),
        Field::new("str_c", DataType::Utf8, false),
        Field::new("str_d", DataType::Utf8, false),
        Field::new("small_int", DataType::Int32, false),
    ]);
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("s", DataType::Struct(struct_fields), false),
    ]))
}

fn wide_batch(batch_id: usize) -> RecordBatch {
    let schema = wide_schema();
    let len = WRITE_RECORD_BATCH_SIZE;

    let base_id = (batch_id * len) as i32;
    let id_values: Vec<i32> = (0..len).map(|i| base_id + i as i32).collect();
    let id_array = Arc::new(Int32Array::from(id_values.clone()));
    let small_int_array = Arc::new(Int32Array::from(id_values));

    let large_string: String = "x".repeat(LARGE_STRING_LEN);
    let mut string_fields: Vec<(Arc<Field>, ArrayRef)> = Vec::new();
    for name in &["str_a", "str_b", "str_c", "str_d"] {
        let mut sb = StringBuilder::new();
        for _ in 0..len {
            sb.append_value(&large_string);
        }
        string_fields.push((
            Arc::new(Field::new(*name, DataType::Utf8, false)),
            Arc::new(sb.finish()) as ArrayRef,
        ));
    }
    string_fields.push((
        Arc::new(Field::new("small_int", DataType::Int32, false)),
        small_int_array as ArrayRef,
    ));

    let struct_array = StructArray::from(string_fields);
    RecordBatch::try_new(schema, vec![id_array, Arc::new(struct_array)]).unwrap()
}

fn generate_file(
    schema: SchemaRef,
    batch_fn: fn(usize) -> RecordBatch,
    prefix: &str,
) -> NamedTempFile {
    let now = Instant::now();
    let mut named_file = tempfile::Builder::new()
        .prefix(prefix)
        .suffix(".parquet")
        .tempfile()
        .unwrap();

    println!("Generating parquet file - {}", named_file.path().display());

    let properties = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(ROW_GROUP_ROW_COUNT))
        .build();

    let mut writer =
        ArrowWriter::try_new(&mut named_file, schema, Some(properties)).unwrap();

    for batch_id in 0..NUM_BATCHES {
        let batch = batch_fn(batch_id);
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

fn create_context(rt: &Runtime, file_path: &str, table: &str) -> SessionContext {
    let ctx = SessionContext::new();
    rt.block_on(ctx.register_parquet(table, file_path, Default::default()))
        .unwrap();
    ctx
}

fn query(ctx: &SessionContext, rt: &Runtime, sql: &str) {
    let ctx = ctx.clone();
    let sql = sql.to_string();
    let df = rt.block_on(ctx.sql(&sql)).unwrap();
    black_box(rt.block_on(df.collect()).unwrap());
}

fn narrow_benchmarks(c: &mut Criterion) {
    let temp_file = generate_file(narrow_schema(), narrow_batch, "narrow_struct");
    let file_path = temp_file.path().display().to_string();
    assert!(Path::new(&file_path).exists(), "path not found");

    let rt = Runtime::new().unwrap();
    let ctx = create_context(&rt, &file_path, "t");

    let mut group = c.benchmark_group("narrow_struct");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    // baseline: full struct, must decode both leaves
    group.bench_function("select_struct", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT s FROM t"))
    });

    // pruned: skip large_string, read only small_int
    group.bench_function("select_small_field", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT s['small_int'] FROM t"))
    });

    // pruned: skip small_int, read only large_string
    group.bench_function("select_large_field", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT s['large_string'] FROM t"))
    });

    // no pruning: all columns
    group.bench_function("select_all", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT * FROM t"))
    });

    // top-level column + pruned struct sub-field
    group.bench_function("select_id_and_small_field", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT id, s['small_int'] FROM t"))
    });

    // aggregation on pruned sub-field, realistic analytical pattern
    group.bench_function("sum_small_field", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT SUM(s['small_int']) FROM t"))
    });

    group.finish();
    drop(temp_file);
}

fn wide_benchmarks(c: &mut Criterion) {
    let temp_file = generate_file(wide_schema(), wide_batch, "wide_struct");
    let file_path = temp_file.path().display().to_string();
    assert!(Path::new(&file_path).exists(), "path not found");

    let rt = Runtime::new().unwrap();
    let ctx = create_context(&rt, &file_path, "t");

    let mut group = c.benchmark_group("wide_struct");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    // baseline: full struct, must decode all 5 leaves
    group.bench_function("select_struct", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT s FROM t"))
    });

    // pruned: skip all 4 large string leaves
    group.bench_function("select_small_field", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT s['small_int'] FROM t"))
    });

    // pruned: read 1 of 4 string leaves + skip the rest
    group.bench_function("select_one_string_field", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT s['str_a'] FROM t"))
    });

    // pruned: read 2 of 4 string leaves
    group.bench_function("select_two_string_fields", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT s['str_a'], s['str_b'] FROM t"))
    });

    // no pruning: all columns
    group.bench_function("select_all", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT * FROM t"))
    });

    // aggregation on pruned sub-field, skips all 4 large leaves
    group.bench_function("sum_small_field", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT SUM(s['small_int']) FROM t"))
    });

    group.finish();
    drop(temp_file);
}

fn nested_schema() -> SchemaRef {
    let inner_fields = Fields::from(vec![
        Field::new("large_string", DataType::Utf8, false),
        Field::new("small_int", DataType::Int32, false),
    ]);
    let outer_fields = Fields::from(vec![
        Field::new("inner", DataType::Struct(inner_fields), false),
        Field::new("extra_string", DataType::Utf8, false),
    ]);
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("s", DataType::Struct(outer_fields), false),
    ]))
}

fn nested_batch(batch_id: usize) -> RecordBatch {
    let schema = nested_schema();
    let len = WRITE_RECORD_BATCH_SIZE;

    let base_id = (batch_id * len) as i32;
    let id_values: Vec<i32> = (0..len).map(|i| base_id + i as i32).collect();
    let id_array = Arc::new(Int32Array::from(id_values.clone()));
    let small_int_array = Arc::new(Int32Array::from(id_values));

    let large_string: String = "x".repeat(LARGE_STRING_LEN);

    let mut sb1 = StringBuilder::new();
    let mut sb2 = StringBuilder::new();
    for _ in 0..len {
        sb1.append_value(&large_string);
        sb2.append_value(&large_string);
    }

    let inner_struct = StructArray::from(vec![
        (
            Arc::new(Field::new("large_string", DataType::Utf8, false)),
            Arc::new(sb1.finish()) as ArrayRef,
        ),
        (
            Arc::new(Field::new("small_int", DataType::Int32, false)),
            small_int_array as ArrayRef,
        ),
    ]);

    let inner_fields = Fields::from(vec![
        Field::new("large_string", DataType::Utf8, false),
        Field::new("small_int", DataType::Int32, false),
    ]);
    let outer_struct = StructArray::from(vec![
        (
            Arc::new(Field::new("inner", DataType::Struct(inner_fields), false)),
            Arc::new(inner_struct) as ArrayRef,
        ),
        (
            Arc::new(Field::new("extra_string", DataType::Utf8, false)),
            Arc::new(sb2.finish()) as ArrayRef,
        ),
    ]);

    RecordBatch::try_new(schema, vec![id_array, Arc::new(outer_struct)]).unwrap()
}

fn nested_benchmarks(c: &mut Criterion) {
    let temp_file = generate_file(nested_schema(), nested_batch, "nested_struct");
    let file_path = temp_file.path().display().to_string();
    assert!(Path::new(&file_path).exists(), "path not found");

    let rt = Runtime::new().unwrap();
    let ctx = create_context(&rt, &file_path, "t");

    let mut group = c.benchmark_group("nested_struct");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    // baseline: full outer struct, decode all 3 leaves
    group.bench_function("select_struct", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT s FROM t"))
    });

    // pruned outer: read only inner struct, skip extra_string
    group.bench_function("select_inner_struct", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT s['inner'] FROM t"))
    });

    // pruned both levels: reach through outer + inner, skip both large strings
    group.bench_function("select_inner_small_field", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT s['inner']['small_int'] FROM t"))
    });

    // pruned outer only: skip inner struct entirely, read extra_string
    group.bench_function("select_extra_string", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT s['extra_string'] FROM t"))
    });

    // no pruning: all columns
    group.bench_function("select_all", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT * FROM t"))
    });

    // aggregation reaching through two levels of nesting
    group.bench_function("sum_inner_small_field", |b| {
        b.iter(|| query(&ctx, &rt, "SELECT SUM(s['inner']['small_int']) FROM t"))
    });

    group.finish();
    drop(temp_file);
}

fn flat_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("large_string", DataType::Utf8, false),
        Field::new("small_int", DataType::Int32, false),
    ]))
}

fn flat_batch(batch_id: usize) -> RecordBatch {
    let schema = flat_schema();
    let len = WRITE_RECORD_BATCH_SIZE;

    let base_id = (batch_id * len) as i32;
    let id_values: Vec<i32> = (0..len).map(|i| base_id + i as i32).collect();
    let id_array = Arc::new(Int32Array::from(id_values.clone()));
    let small_int_array = Arc::new(Int32Array::from(id_values));

    let large_string: String = "x".repeat(LARGE_STRING_LEN);
    let mut string_builder = StringBuilder::new();
    for _ in 0..len {
        string_builder.append_value(&large_string);
    }
    let large_string_array = Arc::new(string_builder.finish());

    RecordBatch::try_new(
        schema,
        vec![id_array, large_string_array as ArrayRef, small_int_array],
    )
    .unwrap()
}

/// Compare selecting a small field from a flat (top-level) schema vs from
/// inside a struct. Both files contain the same logical data — the only
/// difference is whether `small_int` lives at the top level or nested inside
/// a struct column.
fn flat_vs_struct_benchmarks(c: &mut Criterion) {
    let flat_file = generate_file(flat_schema(), flat_batch, "flat");
    let flat_path = flat_file.path().display().to_string();
    assert!(Path::new(&flat_path).exists(), "path not found");

    let struct_file = generate_file(narrow_schema(), narrow_batch, "narrow_struct_cmp");
    let struct_path = struct_file.path().display().to_string();
    assert!(Path::new(&struct_path).exists(), "path not found");

    let rt = Runtime::new().unwrap();
    let flat_ctx = create_context(&rt, &flat_path, "t");
    let struct_ctx = create_context(&rt, &struct_path, "t");

    let mut group = c.benchmark_group("flat_vs_struct");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    // small int: top-level vs struct field
    group.bench_function("flat_select_small_int", |b| {
        b.iter(|| query(&flat_ctx, &rt, "SELECT small_int FROM t"))
    });
    group.bench_function("struct_select_small_int", |b| {
        b.iter(|| query(&struct_ctx, &rt, "SELECT s['small_int'] FROM t"))
    });

    // large string: top-level vs struct field
    group.bench_function("flat_select_large_string", |b| {
        b.iter(|| query(&flat_ctx, &rt, "SELECT large_string FROM t"))
    });
    group.bench_function("struct_select_large_string", |b| {
        b.iter(|| query(&struct_ctx, &rt, "SELECT s['large_string'] FROM t"))
    });

    // aggregation: SUM of small int
    group.bench_function("flat_sum_small_int", |b| {
        b.iter(|| query(&flat_ctx, &rt, "SELECT SUM(small_int) FROM t"))
    });
    group.bench_function("struct_sum_small_int", |b| {
        b.iter(|| query(&struct_ctx, &rt, "SELECT SUM(s['small_int']) FROM t"))
    });

    group.finish();
    drop(flat_file);
    drop(struct_file);
}

criterion_group!(
    benches,
    narrow_benchmarks,
    wide_benchmarks,
    nested_benchmarks,
    flat_vs_struct_benchmarks,
);
criterion_main!(benches);
