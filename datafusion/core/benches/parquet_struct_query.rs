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

//! Benchmarks of SQL queries on struct columns in parquet data

use arrow::array::{ArrayRef, Int32Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;
use datafusion_common::instant::Instant;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};
use rand::distr::Alphanumeric;
use rand::prelude::*;
use rand::rng;
use std::hint::black_box;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

/// The number of batches to write
const NUM_BATCHES: usize = 128;
/// The number of rows in each record batch to write
const WRITE_RECORD_BATCH_SIZE: usize = 4096;
/// The number of rows in a row group
const ROW_GROUP_SIZE: usize = 65536;
/// The number of row groups expected
const EXPECTED_ROW_GROUPS: usize = 8;
/// The range for random string lengths
const STRING_LENGTH_RANGE: Range<usize> = 50..200;

fn schema() -> SchemaRef {
    let struct_fields = Fields::from(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]);
    let struct_type = DataType::Struct(struct_fields);

    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("s", struct_type, false),
    ]))
}

fn generate_strings(len: usize) -> ArrayRef {
    let mut rng = rng();
    Arc::new(StringArray::from_iter((0..len).map(|_| {
        let string_len = rng.random_range(STRING_LENGTH_RANGE.clone());
        Some(
            (0..string_len)
                .map(|_| char::from(rng.sample(Alphanumeric)))
                .collect::<String>(),
        )
    })))
}

fn generate_batch(batch_id: usize) -> RecordBatch {
    let schema = schema();
    let len = WRITE_RECORD_BATCH_SIZE;

    // Generate sequential IDs based on batch_id for uniqueness
    let base_id = (batch_id * len) as i32;
    let id_values: Vec<i32> = (0..len).map(|i| base_id + i as i32).collect();
    let id_array = Arc::new(Int32Array::from(id_values.clone()));

    // Create struct id array (matching top-level id)
    let struct_id_array = Arc::new(Int32Array::from(id_values));

    // Generate random strings for struct value field
    let value_array = generate_strings(len);

    // Construct StructArray
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("id", DataType::Int32, false)),
            struct_id_array as ArrayRef,
        ),
        (
            Arc::new(Field::new("value", DataType::Utf8, false)),
            value_array,
        ),
    ]);

    RecordBatch::try_new(schema, vec![id_array, Arc::new(struct_array)]).unwrap()
}

fn generate_file() -> NamedTempFile {
    let now = Instant::now();
    let mut named_file = tempfile::Builder::new()
        .prefix("parquet_struct_query")
        .suffix(".parquet")
        .tempfile()
        .unwrap();

    println!("Generating parquet file - {}", named_file.path().display());
    let schema = schema();

    let properties = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_size(ROW_GROUP_SIZE)
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
        "Expected {} rows but got {}",
        expected_rows,
        file_metadata.num_rows()
    );
    assert_eq!(
        metadata.row_groups().len(),
        EXPECTED_ROW_GROUPS,
        "Expected {} row groups but got {}",
        EXPECTED_ROW_GROUPS,
        metadata.row_groups().len()
    );

    println!(
        "Generated parquet file with {} rows and {} row groups in {} seconds",
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

    // Basic struct access
    c.bench_function("struct_access", |b| {
        b.iter(|| query(&ctx, &rt, "select id, s['id'] from t"))
    });

    // Filter queries
    c.bench_function("filter_struct_field_eq", |b| {
        b.iter(|| query(&ctx, &rt, "select id from t where s['id'] = 5"))
    });

    c.bench_function("filter_struct_field_with_select", |b| {
        b.iter(|| query(&ctx, &rt, "select id, s['id'] from t where s['id'] = 5"))
    });

    c.bench_function("filter_top_level_with_struct_select", |b| {
        b.iter(|| query(&ctx, &rt, "select s['id'] from t where id = 5"))
    });

    c.bench_function("filter_struct_string_length", |b| {
        b.iter(|| query(&ctx, &rt, "select id from t where length(s['value']) > 100"))
    });

    c.bench_function("filter_struct_range", |b| {
        b.iter(|| {
            query(
                &ctx,
                &rt,
                "select id from t where s['id'] > 100 and s['id'] < 200",
            )
        })
    });

    // Join queries (limited with WHERE id < 1000 for performance)
    c.bench_function("join_struct_to_struct", |b| {
        b.iter(|| query(
            &ctx,
            &rt,
            "select t1.id from t t1 join t t2 on t1.s['id'] = t2.s['id'] where t1.id < 1000"
        ))
    });

    c.bench_function("join_struct_to_toplevel", |b| {
        b.iter(|| query(
            &ctx,
            &rt,
            "select t1.id from t t1 join t t2 on t1.s['id'] = t2.id where t1.id < 1000"
        ))
    });

    c.bench_function("join_toplevel_to_struct", |b| {
        b.iter(|| query(
            &ctx,
            &rt,
            "select t1.id from t t1 join t t2 on t1.id = t2.s['id'] where t1.id < 1000"
        ))
    });

    c.bench_function("join_struct_to_struct_with_top_level", |b| {
        b.iter(|| query(
            &ctx,
            &rt,
            "select t1.id from t t1 join t t2 on t1.s['id'] = t2.s['id'] and t1.id = t2.id where t1.id < 1000"
        ))
    });

    c.bench_function("join_struct_and_struct_value", |b| {
        b.iter(|| query(
            &ctx,
            &rt,
            "select t1.s['id'], t2.s['value'] from t t1 join t t2 on t1.id = t2.id where t1.id < 1000"
        ))
    });

    // Group by queries
    c.bench_function("group_by_struct_field", |b| {
        b.iter(|| query(&ctx, &rt, "select s['id'] from t group by s['id']"))
    });

    c.bench_function("group_by_struct_select_toplevel", |b| {
        b.iter(|| query(&ctx, &rt, "select max(id) from t group by s['id']"))
    });

    c.bench_function("group_by_toplevel_select_struct", |b| {
        b.iter(|| query(&ctx, &rt, "select max(s['id']) from t group by id"))
    });

    c.bench_function("group_by_struct_with_count", |b| {
        b.iter(|| {
            query(
                &ctx,
                &rt,
                "select s['id'], count(*) from t group by s['id']",
            )
        })
    });

    c.bench_function("group_by_multiple_with_count", |b| {
        b.iter(|| {
            query(
                &ctx,
                &rt,
                "select id, s['id'], count(*) from t group by id, s['id']",
            )
        })
    });

    // Additional queries
    c.bench_function("order_by_struct_limit", |b| {
        b.iter(|| {
            query(
                &ctx,
                &rt,
                "select id, s['id'] from t order by s['id'] limit 1000",
            )
        })
    });

    c.bench_function("distinct_struct_field", |b| {
        b.iter(|| query(&ctx, &rt, "select distinct s['id'] from t"))
    });

    // Temporary file must outlive the benchmarks, it is deleted when dropped
    drop(temp_file);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
