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

//! Benchmarks for `HashAggregateOutputPartitionCoalescer` itself.
//!
//! This intentionally bypasses `RepartitionExec` / partitioning / async channels
//! and measures only:
//!
//! ```text
//! push_batch(relative_partition, RecordBatch)
//!   -> optional RecordBatch::slice
//!   -> flush_buffered
//!   -> concat_batches
//!   -> set_partition_runs_metadata
//! ```

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_physical_plan::repartition::HashAggregateOutputPartitionCoalescer;

const TARGET_BATCH_SIZE: usize = 8129;
const NUM_ROWS: usize = 1_000_000;
const MANY_SMALL_BATCH_SIZE: usize = 1024;
const INTERLEAVED_BATCH_SIZE: usize = 128;
const PARTITIONED_BATCH_SIZE: usize = 1024;
const NUM_RELATIVE_PARTITIONS: usize = 8;

#[derive(Clone, Copy)]
enum SchemaLayout {
    IntOnly,
    MixedString,
}

impl SchemaLayout {
    fn name(self) -> &'static str {
        match self {
            Self::IntOnly => "int_only",
            Self::MixedString => "mixed_string",
        }
    }

    fn schema(self) -> SchemaRef {
        let fields = match self {
            Self::IntOnly => vec![
                Field::new("k", DataType::Int32, false),
                Field::new("v0", DataType::Int32, false),
                Field::new("v1", DataType::Int32, false),
            ],
            Self::MixedString => vec![
                Field::new("k", DataType::Int32, false),
                Field::new("v0", DataType::Int32, false),
                Field::new("v1", DataType::Utf8, false),
            ],
        };
        Arc::new(Schema::new(fields))
    }
}

#[derive(Clone, Copy)]
enum InputShape {
    ManySmallBatches,
    InterleavedPartitions,
    PartitionedRuns,
}

impl InputShape {
    fn name(self) -> &'static str {
        match self {
            Self::ManySmallBatches => "many_1024_batches",
            Self::InterleavedPartitions => "interleaved_128_batches_8_partitions",
            Self::PartitionedRuns => "partitioned_1024_batches_8_runs",
        }
    }
}

struct BenchInput {
    schema: SchemaRef,
    batches: Vec<(usize, RecordBatch)>,
    num_rows: usize,
    shape: InputShape,
}

impl BenchInput {
    fn new(layout: SchemaLayout, shape: InputShape) -> Self {
        let schema = layout.schema();
        let batches = make_input_batches(&schema, layout, shape);
        let num_rows = batches.iter().map(|(_, batch)| batch.num_rows()).sum();
        Self {
            schema,
            batches,
            num_rows,
            shape,
        }
    }
}

fn make_input_batches(
    schema: &SchemaRef,
    layout: SchemaLayout,
    shape: InputShape,
) -> Vec<(usize, RecordBatch)> {
    match shape {
        InputShape::ManySmallBatches => {
            make_round_robin_batches(schema, layout, NUM_ROWS, MANY_SMALL_BATCH_SIZE, 1)
        }
        InputShape::InterleavedPartitions => make_round_robin_batches(
            schema,
            layout,
            NUM_ROWS,
            INTERLEAVED_BATCH_SIZE,
            NUM_RELATIVE_PARTITIONS,
        ),
        InputShape::PartitionedRuns => make_partitioned_run_batches(
            schema,
            layout,
            NUM_ROWS,
            PARTITIONED_BATCH_SIZE,
            NUM_RELATIVE_PARTITIONS,
        ),
    }
}

fn make_round_robin_batches(
    schema: &SchemaRef,
    layout: SchemaLayout,
    num_rows: usize,
    batch_size: usize,
    num_relative_partitions: usize,
) -> Vec<(usize, RecordBatch)> {
    (0..num_rows.div_ceil(batch_size))
        .map(|batch_idx| {
            let start = batch_idx * batch_size;
            let len = batch_size.min(num_rows - start);
            let relative_partition = batch_idx % num_relative_partitions;
            (relative_partition, make_batch(schema, layout, start, len))
        })
        .collect()
}

fn make_partitioned_run_batches(
    schema: &SchemaRef,
    layout: SchemaLayout,
    num_rows: usize,
    batch_size: usize,
    num_relative_partitions: usize,
) -> Vec<(usize, RecordBatch)> {
    let run_size = batch_size / num_relative_partitions;
    (0..num_rows.div_ceil(batch_size))
        .map(|batch_idx| {
            let start = batch_idx * batch_size;
            let len = batch_size.min(num_rows - start);
            let batch = make_batch(schema, layout, start, len);
            let mut runs = Vec::new();
            let mut remaining = len;
            for relative_partition in 0..num_relative_partitions {
                if remaining == 0 {
                    break;
                }
                let run_len = run_size.min(remaining);
                runs.push(format!("{relative_partition}:{run_len}"));
                remaining -= run_len;
            }
            let mut batch = batch;
            batch.schema_metadata_mut().insert(
                "datafusion.internal.hash_aggr_partition_runs".to_string(),
                runs.join(","),
            );
            (0, batch)
        })
        .collect()
}

fn make_batch(
    schema: &SchemaRef,
    layout: SchemaLayout,
    batch_start: usize,
    batch_size: usize,
) -> RecordBatch {
    let keys = make_int32_column(batch_start, batch_size, 0);
    let values = make_int32_column(batch_start, batch_size, 1);
    let columns = match layout {
        SchemaLayout::IntOnly => {
            vec![keys, values, make_int32_column(batch_start, batch_size, 2)]
        }
        SchemaLayout::MixedString => {
            vec![keys, values, make_string_column(batch_start, batch_size)]
        }
    };

    RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
}

fn make_int32_column(batch_start: usize, batch_size: usize, col_idx: usize) -> ArrayRef {
    Arc::new(Int32Array::from_iter_values((0..batch_size).map(
        |row_idx| {
            let value = batch_start + row_idx;
            ((value.wrapping_mul(31 + col_idx) + col_idx) % 1_000_003) as i32
        },
    )))
}

fn make_string_column(batch_start: usize, batch_size: usize) -> ArrayRef {
    Arc::new(StringArray::from_iter_values((0..batch_size).map(
        |row_idx| {
            let value = batch_start + row_idx;
            format!("payload_{value:016}")
        },
    )))
}

fn run_coalescer(input: &BenchInput) -> usize {
    let mut coalescer = HashAggregateOutputPartitionCoalescer::new(
        &input.schema,
        TARGET_BATCH_SIZE,
        NUM_RELATIVE_PARTITIONS,
    );

    let mut output_rows = 0;
    for (relative_partition, batch) in &input.batches {
        if matches!(input.shape, InputShape::PartitionedRuns) {
            coalescer.push_partitioned_batch(batch.clone()).unwrap();
        } else {
            coalescer
                .push_batch(*relative_partition, batch.clone())
                .unwrap();
        }
        while let Some(batch) = coalescer.next_completed_batch() {
            output_rows += batch.num_rows();
            black_box(batch);
        }
    }

    coalescer.finish().unwrap();
    while let Some(batch) = coalescer.next_completed_batch() {
        output_rows += batch.num_rows();
        black_box(batch);
    }

    output_rows
}

fn bench_hash_aggregate_output_coalescer(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_aggregate_output_coalescer");
    group.throughput(Throughput::Elements(NUM_ROWS as u64));

    for layout in [SchemaLayout::IntOnly, SchemaLayout::MixedString] {
        for shape in [
            InputShape::ManySmallBatches,
            InputShape::InterleavedPartitions,
            InputShape::PartitionedRuns,
        ] {
            let input = BenchInput::new(layout, shape);
            let id = BenchmarkId::new(layout.name(), shape.name());
            group.bench_function(id, |bencher| {
                bencher.iter(|| {
                    let output_rows = run_coalescer(&input);
                    assert_eq!(output_rows, input.num_rows);
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_hash_aggregate_output_coalescer);
criterion_main!(benches);
