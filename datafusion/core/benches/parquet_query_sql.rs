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

//! Benchmarks of SQL queries again parquet data

use arrow::array::{ArrayRef, DictionaryArray, PrimitiveArray, StringArray};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Float64Type, Int32Type, Int64Type, Schema,
    SchemaRef,
};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::instant::Instant;
use futures::stream::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};
use rand::distributions::uniform::SampleUniform;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use std::fs::File;
use std::io::Read;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use tempfile::NamedTempFile;

/// The number of batches to write
const NUM_BATCHES: usize = 2048;
/// The number of rows in each record batch to write
const WRITE_RECORD_BATCH_SIZE: usize = 1024;
/// The number of rows in a row group
const ROW_GROUP_SIZE: usize = 1024 * 1024;
/// The number of row groups expected
const EXPECTED_ROW_GROUPS: usize = 2;

fn schema() -> SchemaRef {
    let string_dictionary_type =
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

    Arc::new(Schema::new(vec![
        Field::new("dict_10_required", string_dictionary_type.clone(), false),
        Field::new("dict_10_optional", string_dictionary_type.clone(), true),
        Field::new("dict_100_required", string_dictionary_type.clone(), false),
        Field::new("dict_100_optional", string_dictionary_type.clone(), true),
        Field::new("dict_1000_required", string_dictionary_type.clone(), false),
        Field::new("dict_1000_optional", string_dictionary_type, true),
        Field::new("string_required", DataType::Utf8, false),
        Field::new("string_optional", DataType::Utf8, true),
        Field::new("i64_required", DataType::Int64, false),
        Field::new("i64_optional", DataType::Int64, true),
        Field::new("f64_required", DataType::Float64, false),
        Field::new("f64_optional", DataType::Float64, true),
    ]))
}

fn generate_batch() -> RecordBatch {
    let schema = schema();
    let len = WRITE_RECORD_BATCH_SIZE;
    RecordBatch::try_new(
        schema,
        vec![
            generate_string_dictionary("prefix", 10, len, 1.0),
            generate_string_dictionary("prefix", 10, len, 0.5),
            generate_string_dictionary("prefix", 100, len, 1.0),
            generate_string_dictionary("prefix", 100, len, 0.5),
            generate_string_dictionary("prefix", 1000, len, 1.0),
            generate_string_dictionary("prefix", 1000, len, 0.5),
            generate_strings(0..100, len, 1.0),
            generate_strings(0..100, len, 0.5),
            generate_primitive::<Int64Type>(len, 1.0, -2000..2000),
            generate_primitive::<Int64Type>(len, 0.5, -2000..2000),
            generate_primitive::<Float64Type>(len, 1.0, -1000.0..1000.0),
            generate_primitive::<Float64Type>(len, 0.5, -1000.0..1000.0),
        ],
    )
    .unwrap()
}

fn generate_string_dictionary(
    prefix: &str,
    cardinality: usize,
    len: usize,
    valid_percent: f64,
) -> ArrayRef {
    let mut rng = thread_rng();
    let strings: Vec<_> = (0..cardinality).map(|x| format!("{prefix}#{x}")).collect();

    Arc::new(DictionaryArray::<Int32Type>::from_iter((0..len).map(
        |_| {
            rng.gen_bool(valid_percent)
                .then(|| strings[rng.gen_range(0..cardinality)].as_str())
        },
    )))
}

fn generate_strings(
    string_length_range: Range<usize>,
    len: usize,
    valid_percent: f64,
) -> ArrayRef {
    let mut rng = thread_rng();
    Arc::new(StringArray::from_iter((0..len).map(|_| {
        rng.gen_bool(valid_percent).then(|| {
            let string_len = rng.gen_range(string_length_range.clone());
            (0..string_len)
                .map(|_| char::from(rng.sample(Alphanumeric)))
                .collect::<String>()
        })
    })))
}

fn generate_primitive<T>(
    len: usize,
    valid_percent: f64,
    range: Range<T::Native>,
) -> ArrayRef
where
    T: ArrowPrimitiveType,
    T::Native: SampleUniform,
{
    let mut rng = thread_rng();
    Arc::new(PrimitiveArray::<T>::from_iter((0..len).map(|_| {
        rng.gen_bool(valid_percent)
            .then(|| rng.gen_range(range.clone()))
    })))
}

fn generate_file() -> NamedTempFile {
    let now = Instant::now();
    let mut named_file = tempfile::Builder::new()
        .prefix("parquet_query_sql")
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

    for _ in 0..NUM_BATCHES {
        let batch = generate_batch();
        writer.write(&batch).unwrap();
    }

    let metadata = writer.close().unwrap();
    assert_eq!(
        metadata.num_rows as usize,
        WRITE_RECORD_BATCH_SIZE * NUM_BATCHES
    );
    assert_eq!(metadata.row_groups.len(), EXPECTED_ROW_GROUPS);

    println!(
        "Generated parquet file in {} seconds",
        now.elapsed().as_secs_f32()
    );

    named_file
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

    let partitions = 4;
    let config = SessionConfig::new().with_target_partitions(partitions);
    let context = SessionContext::new_with_config(config);

    let local_rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let query_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(partitions)
        .build()
        .unwrap();

    local_rt
        .block_on(context.register_parquet("t", file_path.as_str(), Default::default()))
        .unwrap();

    // We read the queries from a file so they can be changed without recompiling the benchmark
    let mut queries_file = File::open("benches/parquet_query_sql.sql").unwrap();
    let mut queries = String::new();
    queries_file.read_to_string(&mut queries).unwrap();

    for query in queries.split(';') {
        let query = query.trim();

        // Remove comment lines
        let query: Vec<_> = query.split('\n').filter(|x| !x.starts_with("--")).collect();
        let query = query.join(" ");

        // Ignore blank lines
        if query.is_empty() {
            continue;
        }

        c.bench_function(&format!("tokio: {query}"), |b| {
            b.iter(|| {
                let query = query.clone();
                let context = context.clone();
                let (sender, mut receiver) = futures::channel::mpsc::unbounded();

                // Spawn work to a separate tokio thread pool
                query_rt.spawn(async move {
                    let query = context.sql(&query).await.unwrap();
                    let mut stream = query.execute_stream().await.unwrap();

                    while let Some(next) = stream.next().await {
                        sender.unbounded_send(next).unwrap();
                    }
                });

                local_rt.block_on(async {
                    while receiver.next().await.transpose().unwrap().is_some() {}
                })
            });
        });
    }

    // Temporary file must outlive the benchmarks, it is deleted when dropped
    drop(temp_file);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
