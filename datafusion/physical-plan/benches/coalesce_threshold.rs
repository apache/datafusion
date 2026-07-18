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

//! Micro-benchmark for the coalesce bypass threshold trade-off.
//!
//! Simulates the input pattern a hash join produces: many small
//! result chunks per probing input chunk (high Chunk-Reducing Factor,
//! per SIGMOD 2025 "Data Chunk Compaction in Vectorized Execution").
//!
//! Compares three bypass thresholds:
//!   - `target/2` (4096) — current DataFusion default
//!   - `target/8` (1024) — moderate reduction
//!   - `target/64` (128) — the paper's Binary Compaction alpha
//!
//! For each threshold we feed a stream of small batches into the
//! coalescer, then read out completed batches. Wall time reflects the
//! sum of copy cost inside the coalescer plus the "downstream" cost of
//! processing each output batch (approximated by a trivial iteration).

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_physical_plan::coalesce::LimitedBatchCoalescer;

const TARGET_BATCH_SIZE: usize = 8192;
const NUM_INPUT_BATCHES: usize = 5000;

fn make_schema(wide: bool) -> SchemaRef {
    let mut fields = vec![Field::new("k", DataType::Int32, false)];
    if wide {
        for i in 0..10 {
            fields.push(Field::new(format!("v{i}"), DataType::Utf8, false));
        }
    } else {
        fields.push(Field::new("v", DataType::Int32, false));
    }
    Arc::new(Schema::new(fields))
}

fn build_batch(schema: &SchemaRef, num_rows: usize, seed: i32) -> RecordBatch {
    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = vec![Arc::new(
        Int32Array::from_iter_values((0..num_rows as i32).map(|i| seed + i)),
    )];
    for field in schema.fields().iter().skip(1) {
        match field.data_type() {
            DataType::Utf8 => {
                let values: Vec<String> = (0..num_rows)
                    .map(|i| format!("payload_{seed}_{i}"))
                    .collect();
                arrays.push(Arc::new(StringArray::from(values)));
            }
            DataType::Int32 => {
                arrays.push(Arc::new(Int32Array::from_iter_values(
                    (0..num_rows as i32).map(|i| seed * 1000 + i),
                )));
            }
            other => panic!("unsupported bench field type: {other}"),
        }
    }
    RecordBatch::try_new(Arc::clone(schema), arrays).unwrap()
}

/// Feed `NUM_INPUT_BATCHES` batches of `rows_per_batch` rows through a
/// coalescer configured with the given bypass threshold, then drain and
/// touch every output batch. Returns nothing — we only measure wall time.
fn run_coalesce(
    schema: SchemaRef,
    rows_per_batch: usize,
    bypass_threshold: Option<usize>,
) {
    let mut coalescer = LimitedBatchCoalescer::new_with_bypass_threshold(
        Arc::clone(&schema),
        TARGET_BATCH_SIZE,
        None,
        bypass_threshold,
    );

    for i in 0..NUM_INPUT_BATCHES {
        let batch = build_batch(&schema, rows_per_batch, i as i32);
        coalescer.push_batch(batch).unwrap();
        while let Some(out) = coalescer.next_completed_batch() {
            // Approximate downstream per-batch interpretation cost.
            std::hint::black_box(out.num_rows());
            std::hint::black_box(out.num_columns());
        }
    }
    coalescer.finish().unwrap();
    while let Some(out) = coalescer.next_completed_batch() {
        std::hint::black_box(out.num_rows());
        std::hint::black_box(out.num_columns());
    }
}

fn bench_thresholds(c: &mut Criterion) {
    let sizes = [64usize, 256, 1024, 4096];
    let thresholds: [(&str, Option<usize>); 4] = [
        ("default_target/2", None), // current DataFusion default: 4096
        ("target/8", Some(1024)),   // moderate
        ("target/32", Some(256)),   // aggressive
        ("target/64", Some(128)),   // paper's alpha
    ];

    for &wide in &[false, true] {
        let schema = make_schema(wide);
        let width_label = if wide { "wide_11col" } else { "narrow_2col" };
        for &rows in &sizes {
            let mut group =
                c.benchmark_group(format!("coalesce/{width_label}/rows_{rows}"));
            group.sample_size(20);
            for &(label, threshold) in &thresholds {
                group.bench_function(label, |b| {
                    b.iter(|| {
                        run_coalesce(Arc::clone(&schema), rows, threshold);
                    });
                });
            }
            group.finish();
        }
    }
}

criterion_group!(benches, bench_thresholds);
criterion_main!(benches);
