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

//! Full TopK execution with no, occasional, and continuous row replacements.
//! Data generation and plan construction are not timed.
//!
//! Run with:
//! `cargo bench -p datafusion-physical-plan --bench topk --features test_utils`

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col};
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, collect};
use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
use tokio::runtime::Runtime;

const NUM_ROWS: usize = 100_000;
const BATCH_SIZE: usize = 8192;

#[derive(Clone, Copy, Debug)]
enum InputOrder {
    Ascending,
    Random,
    Descending,
}

impl InputOrder {
    fn name(self) -> &'static str {
        match self {
            Self::Ascending => "ascending",
            Self::Random => "random",
            Self::Descending => "descending",
        }
    }
}

fn make_batches(
    key_type: &DataType,
    payload_columns: usize,
    order: InputOrder,
) -> Vec<RecordBatch> {
    let mut values: Vec<u64> = (0..NUM_ROWS as u64).collect();
    match order {
        InputOrder::Ascending => {}
        InputOrder::Random => values.shuffle(&mut StdRng::seed_from_u64(42)),
        InputOrder::Descending => values.reverse(),
    }

    let mut fields = vec![Field::new("key", key_type.clone(), false)];
    fields.extend(
        (0..payload_columns)
            .map(|i| Field::new(format!("payload_{i}"), DataType::UInt64, false)),
    );
    let schema = Arc::new(Schema::new(fields));
    let prefix = "x".repeat(44);

    values
        .chunks(BATCH_SIZE)
        .map(|values| {
            // Allocate each batch independently so TopK can reclaim batches
            // without retaining the buffers for the entire input table.
            let key: ArrayRef = match key_type {
                DataType::UInt64 => {
                    Arc::new(UInt64Array::from_iter_values(values.iter().copied()))
                }
                DataType::Utf8 => Arc::new(StringArray::from_iter_values(
                    values.iter().map(|value| format!("{prefix}{value:020}")),
                )),
                _ => unreachable!("unsupported TopK benchmark key type"),
            };
            let mut columns = vec![key];
            for i in 0..payload_columns {
                columns.push(Arc::new(UInt64Array::from_iter_values(
                    values.iter().map(|value| value * (i + 1) as u64),
                )));
            }
            RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()
        })
        .collect()
}

fn make_plan(batches: &[RecordBatch], k: usize) -> Arc<dyn ExecutionPlan> {
    let schema = batches[0].schema();
    // Leave ordering unspecified even for ascending input. Otherwise SortExec
    // can use LimitStream instead of TopK.
    let input =
        TestMemoryExec::try_new_exec(&[batches.to_vec()], Arc::clone(&schema), None)
            .unwrap();
    let ordering = LexOrdering::new([PhysicalSortExpr::new(
        col("key", &schema).unwrap(),
        SortOptions::default(),
    )])
    .unwrap();
    Arc::new(SortExec::new(ordering, input).with_fetch(Some(k)))
}

fn topk_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(BATCH_SIZE)),
    );
    let mut group = c.benchmark_group("topk_replacements");
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    for (name, key_type, payload_columns, k) in [
        ("u64_narrow_k128", DataType::UInt64, 0, 128),
        ("utf8_64_narrow_k128", DataType::Utf8, 0, 128),
        ("utf8_64_wide_k128", DataType::Utf8, 16, 128),
        ("utf8_64_wide_k1024", DataType::Utf8, 16, 1024),
    ] {
        for order in [
            InputOrder::Ascending,
            InputOrder::Random,
            InputOrder::Descending,
        ] {
            let batches = make_batches(&key_type, payload_columns, order);

            group.bench_function(BenchmarkId::new(name, order.name()), |b| {
                b.iter_batched(
                    || make_plan(&batches, k),
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
    }

    group.finish();
}

criterion_group!(benches, topk_benchmark);
criterion_main!(benches);
