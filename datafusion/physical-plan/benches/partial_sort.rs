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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col};
use datafusion_physical_plan::sorts::partial_sort::PartialSortExec;
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, collect};

const NUM_BATCHES: usize = 32;
const BATCH_SIZE: usize = 8192;
const ROWS_PER_PREFIX: &[usize] = &[100, 1_000, 5_000, 8_192, 10_000, 20_000];

fn schema() -> SchemaRef {
    Arc::new(Schema::new(
        [
            "prefix",
            "suffix",
            "payload_0",
            "payload_1",
            "payload_2",
            "payload_3",
        ]
        .into_iter()
        .map(|name| Field::new(name, DataType::UInt64, false))
        .collect::<Vec<_>>(),
    ))
}

fn make_batches(rows_per_prefix: usize) -> Vec<RecordBatch> {
    let schema = schema();
    (0..NUM_BATCHES)
        .map(|batch_idx| {
            let rows = batch_idx * BATCH_SIZE..(batch_idx + 1) * BATCH_SIZE;
            let prefix = UInt64Array::from_iter_values(
                rows.clone()
                    .map(|row_idx| (row_idx / rows_per_prefix) as u64),
            );
            let suffix =
                UInt64Array::from_iter_values(rows.clone().map(|row_idx| {
                    (rows_per_prefix - row_idx % rows_per_prefix - 1) as u64
                }));
            let payload = |row_idx: usize| row_idx as u64;
            let payload_0 = UInt64Array::from_iter_values(rows.clone().map(payload));
            let payload_1 = UInt64Array::from_iter_values(
                rows.clone()
                    .map(|row_idx| payload(row_idx).wrapping_mul(31)),
            );
            let payload_2 = UInt64Array::from_iter_values(
                rows.clone().map(|row_idx| payload(row_idx).rotate_left(13)),
            );
            let payload_3 = UInt64Array::from_iter_values(
                rows.map(|row_idx| payload(row_idx).wrapping_neg()),
            );

            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(prefix) as ArrayRef,
                    Arc::new(suffix) as ArrayRef,
                    Arc::new(payload_0) as ArrayRef,
                    Arc::new(payload_1) as ArrayRef,
                    Arc::new(payload_2) as ArrayRef,
                    Arc::new(payload_3) as ArrayRef,
                ],
            )
            .unwrap()
        })
        .collect()
}

fn make_plan(batches: &[RecordBatch]) -> Arc<dyn ExecutionPlan> {
    let schema = batches[0].schema();
    let input =
        TestMemoryExec::try_new_exec(&[batches.to_vec()], Arc::clone(&schema), None)
            .unwrap();
    let ordering = LexOrdering::new([
        PhysicalSortExpr::new(col("prefix", &schema).unwrap(), SortOptions::default()),
        PhysicalSortExpr::new(col("suffix", &schema).unwrap(), SortOptions::default()),
    ])
    .unwrap();
    Arc::new(PartialSortExec::new(ordering, input, 1))
}

fn partial_sort_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let task_ctx = Arc::new(TaskContext::default());
    let mut group = c.benchmark_group("partial_sort");
    group.sample_size(10);

    for &rows_per_prefix in ROWS_PER_PREFIX {
        let batches = make_batches(rows_per_prefix);
        group.bench_function(BenchmarkId::new("rows_per_prefix", rows_per_prefix), |b| {
            b.iter_batched(
                || make_plan(&batches),
                |plan| {
                    let output = runtime
                        .block_on(collect(plan, Arc::clone(&task_ctx)))
                        .unwrap();
                    black_box(output);
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, partial_sort_benchmark);
criterion_main!(benches);
