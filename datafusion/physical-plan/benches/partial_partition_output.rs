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

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, PrimitiveArray, StringViewArray};
use arrow::compute::take_arrays;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt32Type};
use arrow::record_batch::RecordBatch;
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use datafusion_physical_plan::coalesce::LimitedBatchCoalescer;

const TARGET_BATCH_SIZE: usize = 8192;
const NUM_COLUMNS: usize = 4;
const NUM_ROWS_CASES: &[usize] = &[1 << 20, 1 << 24];
const STRING_MODULUS: usize = 2048;

struct BenchInput {
    batch: RecordBatch,
    partition_group_ids: Vec<Vec<u32>>,
    row_partitions: Vec<usize>,
    row_group_ids: Vec<u32>,
}

impl BenchInput {
    fn try_new(num_rows: usize, num_partitions: usize) -> Self {
        let schema = schema(NUM_COLUMNS);
        let columns = columns(num_rows, NUM_COLUMNS);
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let (partition_group_ids, row_partitions, row_group_ids) =
            group_partition_mapping(num_rows, num_partitions);

        Self {
            batch,
            partition_group_ids,
            row_partitions,
            row_group_ids,
        }
    }
}

fn schema(num_columns: usize) -> SchemaRef {
    let mut fields = Vec::with_capacity(num_columns);
    for column in 0..num_columns - 1 {
        fields.push(Field::new(format!("i{column}"), DataType::Int64, false));
    }
    fields.push(Field::new("s", DataType::Utf8View, false));
    Arc::new(Schema::new(fields))
}

fn columns(num_rows: usize, num_columns: usize) -> Vec<ArrayRef> {
    let mut columns = Vec::with_capacity(num_columns);
    for column in 0..num_columns - 1 {
        let multiplier = (column as i64) + 1;
        columns.push(Arc::new(Int64Array::from_iter_values(
            (0..num_rows).map(|row| row as i64 * multiplier),
        )) as ArrayRef);
    }

    columns.push(Arc::new(StringViewArray::from_iter_values(
        (0..num_rows).map(|row| format!("group-{}", row % STRING_MODULUS)),
    )) as ArrayRef);
    columns
}

fn group_partition_mapping(
    num_rows: usize,
    num_partitions: usize,
) -> (Vec<Vec<u32>>, Vec<usize>, Vec<u32>) {
    let mut partition_group_ids = vec![Vec::new(); num_partitions];
    let mut row_partitions = Vec::with_capacity(num_rows);
    let mut row_group_ids = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let partition = partition_for_group(row, num_partitions);
        row_partitions.push(partition);
        row_group_ids.push(row as u32);
        partition_group_ids[partition].push(row as u32);
    }

    (partition_group_ids, row_partitions, row_group_ids)
}

fn partition_for_group(group_id: usize, num_partitions: usize) -> usize {
    group_id.wrapping_mul(11400714819323198485usize) % num_partitions
}

fn partition_take_output(input: &BenchInput) -> usize {
    let mut num_output_rows = 0;
    for group_ids in &input.partition_group_ids {
        for group_ids in group_ids.chunks(TARGET_BATCH_SIZE) {
            let indices =
                PrimitiveArray::<UInt32Type>::from_iter_values(group_ids.iter().copied());
            let arrays = take_arrays(input.batch.columns(), &indices, None).unwrap();
            num_output_rows += arrays.first().map_or(0, |array| array.len());
        }
    }
    num_output_rows
}

fn repartition_then_coalesce(input: &BenchInput, num_partitions: usize) -> usize {
    let schema = input.batch.schema();
    let mut coalescers = (0..num_partitions)
        .map(|_| LimitedBatchCoalescer::new(Arc::clone(&schema), TARGET_BATCH_SIZE, None))
        .collect::<Vec<_>>();
    let mut partition_indices = vec![Vec::new(); num_partitions];
    let mut num_output_rows = 0;

    for offset in (0..input.batch.num_rows()).step_by(TARGET_BATCH_SIZE) {
        let len = TARGET_BATCH_SIZE.min(input.batch.num_rows() - offset);
        for indices in &mut partition_indices {
            indices.clear();
        }

        for row in offset..offset + len {
            let partition = input.row_partitions[row];
            partition_indices[partition].push(input.row_group_ids[row]);
        }

        for (partition, indices) in partition_indices.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }

            let indices =
                PrimitiveArray::<UInt32Type>::from_iter_values(indices.iter().copied());
            let arrays = take_arrays(input.batch.columns(), &indices, None).unwrap();
            let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
            coalescers[partition].push_batch(batch).unwrap();
            while let Some(batch) = coalescers[partition].next_completed_batch() {
                num_output_rows += batch.num_rows();
            }
        }
    }

    for coalescer in &mut coalescers {
        coalescer.finish().unwrap();
        while let Some(batch) = coalescer.next_completed_batch() {
            num_output_rows += batch.num_rows();
        }
    }

    num_output_rows
}

fn bench_partial_partition_output(c: &mut Criterion) {
    let mut group = c.benchmark_group("partial_partition_output");

    for num_rows in NUM_ROWS_CASES {
        group.throughput(Throughput::Elements(*num_rows as u64));

        for num_partitions in [8usize, 64, 512] {
            let parameter = format!("rows_{num_rows}_partitions_{num_partitions}");

            group.bench_function(
                BenchmarkId::new("partition_take", &parameter),
                |bencher| {
                    bencher.iter_batched_ref(
                        || BenchInput::try_new(*num_rows, num_partitions),
                        |input| partition_take_output(input),
                        BatchSize::LargeInput,
                    );
                },
            );

            group.bench_function(
                BenchmarkId::new("repartition_coalesce", &parameter),
                |bencher| {
                    bencher.iter_batched_ref(
                        || BenchInput::try_new(*num_rows, num_partitions),
                        |input| repartition_then_coalesce(input, num_partitions),
                        BatchSize::LargeInput,
                    );
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_partial_partition_output);
criterion_main!(benches);
