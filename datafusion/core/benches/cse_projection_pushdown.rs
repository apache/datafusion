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

//! Benchmarks for the interaction between Common Subexpression Elimination
//! (CSE) and projection pushdown on parquet sources.
//!
//! Each query repeats a scalar function call several times, which the logical
//! CSE pass extracts into a single intermediate projection referenced by
//! column. These benchmarks measure the end-to-end cost of such queries, which
//! is dominated by how many times the extracted expression is ultimately
//! evaluated per row.

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::instant::Instant;
use futures::stream::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};
use rand::prelude::*;
use rand::rng;
use std::sync::Arc;
use tempfile::NamedTempFile;

const NUM_BATCHES: usize = 1024;
const BATCH_SIZE: usize = 1024;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float64, false),
        Field::new("b", DataType::Float64, false),
        Field::new("c", DataType::Int64, false),
    ]))
}

fn generate_batch() -> RecordBatch {
    let mut rng = rng();
    let len = BATCH_SIZE;

    let a: Float64Array = (0..len)
        .map(|_| Some(rng.random_range(1.0..1000.0)))
        .collect();
    let b: Float64Array = (0..len)
        .map(|_| Some(rng.random_range(1.0..1000.0)))
        .collect();
    let c: Int64Array = (0..len)
        .map(|_| Some(rng.random_range(1i64..1000)))
        .collect();

    RecordBatch::try_new(schema(), vec![Arc::new(a), Arc::new(b), Arc::new(c)]).unwrap()
}

fn generate_file() -> NamedTempFile {
    let now = Instant::now();
    let mut named_file = tempfile::Builder::new()
        .prefix("cse_projection_pushdown")
        .suffix(".parquet")
        .tempfile()
        .unwrap();

    println!("Generating parquet file - {}", named_file.path().display());

    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(1024 * 1024))
        .build();

    let mut writer =
        ArrowWriter::try_new(&mut named_file, schema(), Some(props)).unwrap();

    for _ in 0..NUM_BATCHES {
        let batch = generate_batch();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();

    println!(
        "Generated parquet file in {} seconds",
        now.elapsed().as_secs_f32()
    );

    named_file
}

fn criterion_benchmark(c: &mut Criterion) {
    let temp_file = generate_file();
    let file_path = temp_file.path().display().to_string();

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

    // Queries that repeat a scalar function call, which CSE extracts into a
    // single intermediate projection referenced by column.
    let queries = vec![
        // Same sqrt(a) appears 3 times.
        (
            "repeated_sqrt",
            "SELECT sqrt(a) + 1, sqrt(a) * 2, sqrt(a) / b FROM t",
        ),
        // power(a, 2) appears in multiple places.
        (
            "repeated_power",
            "SELECT power(a, 2) + b, power(a, 2) - b, power(a, 2) * c FROM t",
        ),
        // Deeper nesting: ln(abs(a)) repeated.
        (
            "repeated_nested_fn",
            "SELECT ln(abs(a)) + 1, ln(abs(a)) * b, ln(abs(a)) + c FROM t",
        ),
        // Mixed: some repeated, some unique.
        (
            "mixed_repeated_unique",
            "SELECT sqrt(a) + sqrt(a), abs(b), sqrt(a) * c FROM t",
        ),
        // A trivial function (abs) repeated.
        (
            "repeated_cheap_abs",
            "SELECT abs(a) + 1, abs(a) * 2, abs(a) / b FROM t",
        ),
        // Baseline: no repeated expressions (CSE does not fire).
        (
            "no_repeated_exprs",
            "SELECT sqrt(a), abs(b), power(a, 2) FROM t",
        ),
    ];

    for (name, query) in queries {
        c.bench_function(&format!("cse_pushdown: {name}"), |b| {
            b.iter(|| {
                let query = query.to_string();
                let context = context.clone();
                let (sender, mut receiver) = futures::channel::mpsc::unbounded();

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

    drop(temp_file);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
