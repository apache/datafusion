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

//! Benchmarks the cost of splitting input batches across many coalescers.
//!
//! This benchmark intentionally runs without locks. It isolates the local cost
//! of slicing each input batch into many pieces and pushing those slices into
//! independent `LimitedBatchCoalescer` instances.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_physical_plan::coalesce::LimitedBatchCoalescer;

const NUM_BATCHES: usize = 1024;
const NUM_ROWS_PER_BATCH: usize = 8129;
const TARGET_BATCH_SIZE: usize = 8192;
const NUM_COALESCERS_CASES: [usize; 3] = [8, 64, 256];

struct CoalescerSplitInput {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl CoalescerSplitInput {
    fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("int_0", DataType::Int32, false),
            Field::new("string_0", DataType::Utf8View, false),
            Field::new("int_1", DataType::Int32, false),
            Field::new("string_1", DataType::Utf8View, false),
        ]));
        let batches = (0..NUM_BATCHES)
            .map(|batch_idx| make_mixed_batch(Arc::clone(&schema), batch_idx))
            .collect();

        Self { schema, batches }
    }

    fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }
}

fn make_mixed_batch(schema: SchemaRef, batch_idx: usize) -> RecordBatch {
    let columns = vec![
        make_int32_column(batch_idx, 0),
        make_string_view_column(batch_idx, 1),
        make_int32_column(batch_idx, 2),
        make_string_view_column(batch_idx, 3),
    ];
    RecordBatch::try_new(schema, columns).unwrap()
}

fn make_int32_column(batch_idx: usize, col_idx: usize) -> ArrayRef {
    let values: Vec<i32> = (0..NUM_ROWS_PER_BATCH)
        .map(|row_idx| {
            ((batch_idx * NUM_ROWS_PER_BATCH + row_idx + col_idx) % 4096) as i32
        })
        .collect();
    Arc::new(Int32Array::from(values))
}

fn make_string_view_column(batch_idx: usize, col_idx: usize) -> ArrayRef {
    let values: Vec<String> = (0..NUM_ROWS_PER_BATCH)
        .map(|row_idx| {
            let value = (batch_idx * NUM_ROWS_PER_BATCH + row_idx + col_idx) % 4096;
            format!("group_key_{value:016}")
        })
        .collect();
    Arc::new(StringViewArray::from(values))
}

fn bench_split_push(input: &CoalescerSplitInput, num_coalescers: usize) {
    let mut coalescers = make_coalescers(&input.schema, num_coalescers);
    let mut num_output_rows = 0;

    for batch in &input.batches {
        push_split_batch(batch, &mut coalescers, num_coalescers, &mut num_output_rows);
    }

    for coalescer in &mut coalescers {
        coalescer.finish().unwrap();
        drain_coalescer(coalescer, &mut num_output_rows);
    }

    black_box(num_output_rows);
}

fn make_coalescers(
    schema: &SchemaRef,
    num_coalescers: usize,
) -> Vec<LimitedBatchCoalescer> {
    (0..num_coalescers)
        .map(|_| LimitedBatchCoalescer::new(Arc::clone(schema), TARGET_BATCH_SIZE, None))
        .collect()
}

fn push_split_batch(
    batch: &RecordBatch,
    coalescers: &mut [LimitedBatchCoalescer],
    num_coalescers: usize,
    num_output_rows: &mut usize,
) {
    let rows_per_slice = batch.num_rows().div_ceil(num_coalescers);

    for (coalescer_idx, coalescer) in coalescers.iter_mut().enumerate() {
        let offset = coalescer_idx * rows_per_slice;
        if offset >= batch.num_rows() {
            break;
        }

        let slice_len = rows_per_slice.min(batch.num_rows() - offset);
        coalescer
            .push_batch(batch.slice(offset, slice_len))
            .unwrap();
        drain_coalescer(coalescer, num_output_rows);
    }
}

fn drain_coalescer(coalescer: &mut LimitedBatchCoalescer, num_output_rows: &mut usize) {
    while let Some(batch) = coalescer.next_completed_batch() {
        *num_output_rows += batch.num_rows();
        black_box(batch);
    }
}

fn bench_limited_coalescer_split(c: &mut Criterion) {
    let input = CoalescerSplitInput::new();
    let mut group = c.benchmark_group("limited_coalescer_split");
    group.sample_size(10);
    group.throughput(Throughput::Elements(input.num_rows() as u64));

    for num_coalescers in NUM_COALESCERS_CASES {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_coalescers),
            &num_coalescers,
            |bencher, &num_coalescers| {
                bencher.iter(|| bench_split_push(&input, num_coalescers));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_limited_coalescer_split);
criterion_main!(benches);
