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

//! Microbenchmark for `RepartitionExec` in isolation.
//!
//! Stages a pre-built in-memory set of batches across N input partitions and
//! runs them through `RepartitionExec` to N output partitions. Drains all
//! output partitions concurrently so the measurement reflects the true
//! per-batch cost of the breaker: mpsc `send().await`, reservation lock,
//! metric timers, and `SpawnedTask`-per-input scheduling.
//!
//! Run with:
//! ```sh
//! cargo bench --bench repartition -- --sample-size=20
//! ```

use std::sync::Arc;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::Partitioning;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use futures::StreamExt;

const TOTAL_ROWS: usize = 1_000_000;
const BATCH_SIZE: usize = 8_192;
const PARTITIONS: usize = 16;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt64, false)]))
}

/// Build `PARTITIONS` input partitions, each containing
/// `TOTAL_ROWS / PARTITIONS` rows split into `BATCH_SIZE`-sized batches.
fn make_partitions(schema: &SchemaRef) -> Vec<Vec<RecordBatch>> {
    let per_partition = TOTAL_ROWS / PARTITIONS;
    (0..PARTITIONS)
        .map(|p| {
            let mut batches = Vec::new();
            let mut offset: u64 = (p * per_partition) as u64;
            let end: u64 = offset + per_partition as u64;
            while offset < end {
                let n = std::cmp::min(BATCH_SIZE as u64, end - offset) as usize;
                let arr: ArrayRef = Arc::new(UInt64Array::from_iter_values(
                    (offset..offset + n as u64).collect::<Vec<_>>(),
                ));
                batches
                    .push(RecordBatch::try_new(Arc::clone(schema), vec![arr]).unwrap());
                offset += n as u64;
            }
            batches
        })
        .collect()
}

fn build_plan(
    partitions: &[Vec<RecordBatch>],
    schema: &SchemaRef,
    partitioning: Partitioning,
) -> Arc<dyn ExecutionPlan> {
    let src = TestMemoryExec::try_new_exec(partitions, Arc::clone(schema), None).unwrap();
    Arc::new(RepartitionExec::try_new(src, partitioning).unwrap())
}

fn drain_all(
    rt: &tokio::runtime::Runtime,
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) {
    rt.block_on(async move {
        let out = plan.output_partitioning().partition_count();
        let mut handles = Vec::with_capacity(out);
        for p in 0..out {
            let mut stream = plan.execute(p, Arc::clone(&task_ctx)).unwrap();
            handles.push(tokio::spawn(async move {
                let mut rows = 0usize;
                while let Some(batch) = stream.next().await {
                    rows += batch.unwrap().num_rows();
                }
                rows
            }));
        }
        for h in handles {
            let _ = h.await.unwrap();
        }
    });
}

fn bench_repartition(c: &mut Criterion) {
    let schema = schema();
    let partitions = make_partitions(&schema);
    let hash_expr = vec![col("c0", &schema).unwrap()];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(PARTITIONS)
        .enable_all()
        .build()
        .unwrap();
    let task_ctx = Arc::new(TaskContext::default());

    c.bench_function("repartition/hash_16_to_16", |b| {
        b.iter_batched(
            || {
                build_plan(
                    &partitions,
                    &schema,
                    Partitioning::Hash(hash_expr.clone(), PARTITIONS),
                )
            },
            |plan| drain_all(&rt, plan, Arc::clone(&task_ctx)),
            BatchSize::LargeInput,
        )
    });

    c.bench_function("repartition/round_robin_16_to_16", |b| {
        b.iter_batched(
            || {
                build_plan(
                    &partitions,
                    &schema,
                    Partitioning::RoundRobinBatch(PARTITIONS),
                )
            },
            |plan| drain_all(&rt, plan, Arc::clone(&task_ctx)),
            BatchSize::LargeInput,
        )
    });

    c.bench_function("repartition/hash_16_to_1_coalesce", |b| {
        b.iter_batched(
            || {
                build_plan(
                    &partitions,
                    &schema,
                    Partitioning::Hash(hash_expr.clone(), 1),
                )
            },
            |plan| drain_all(&rt, plan, Arc::clone(&task_ctx)),
            BatchSize::LargeInput,
        )
    });
}

criterion_group!(benches, bench_repartition);
criterion_main!(benches);
