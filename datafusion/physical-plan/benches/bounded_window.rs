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

//! Benchmark for `BoundedWindowAggExec` with many partitions.
//!
//! The streaming window operator keeps per-partition state keyed by
//! `PartitionKey` (`Vec<ScalarValue>`) and probes it for every buffered
//! partition on every batch, so its performance is sensitive to both the
//! number of live partitions and the cost of hashing the keys. `Linear`
//! mode (input sorted by the ORDER BY column but not by the partition
//! columns) keeps every partition live until the input is exhausted and is
//! the stress case; `Sorted` mode prunes finished partitions eagerly and
//! serves as the control.

use std::sync::Arc;

use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_execution::TaskContext;
use datafusion_expr::{
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::windows::{BoundedWindowAggExec, create_window_expr};
use datafusion_physical_plan::{ExecutionPlan, InputOrderMode, collect};

const BATCH_SIZE: usize = 8192;
const N_BATCHES: usize = 16;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("pk", DataType::UInt64, false),
        Field::new("ts", DataType::UInt64, false),
    ]))
}

/// Batches with `ts` ascending across the whole input. When
/// `partitions_sorted` is false, partition keys round-robin over
/// `n_partitions` (the `Linear` layout); when true, the input is laid out
/// partition-by-partition (the `Sorted` layout).
fn make_batches(n_partitions: usize, partitions_sorted: bool) -> Vec<RecordBatch> {
    let total = BATCH_SIZE * N_BATCHES;
    let rows_per_partition = total / n_partitions;
    (0..N_BATCHES)
        .map(|b| {
            let start = b * BATCH_SIZE;
            let pk: UInt64Array = (start..start + BATCH_SIZE)
                .map(|i| {
                    if partitions_sorted {
                        Some((i / rows_per_partition) as u64)
                    } else {
                        Some((i % n_partitions) as u64)
                    }
                })
                .collect();
            let ts: UInt64Array = (start..start + BATCH_SIZE)
                .map(|i| Some(i as u64))
                .collect();
            RecordBatch::try_new(schema(), vec![Arc::new(pk), Arc::new(ts)]).unwrap()
        })
        .collect()
}

fn sort_expr(name: &str) -> PhysicalSortExpr {
    PhysicalSortExpr {
        expr: col(name, &schema()).unwrap(),
        options: Default::default(),
    }
}

/// `COUNT(ts) OVER (PARTITION BY pk ORDER BY ts
///  RANGE BETWEEN CURRENT ROW AND 10 FOLLOWING)`
fn window_exec(
    batches: Vec<RecordBatch>,
    mode: InputOrderMode,
    input_ordering: Vec<PhysicalSortExpr>,
) -> Arc<dyn ExecutionPlan> {
    let schema = schema();
    let source = TestMemoryExec::try_new(&[batches], Arc::clone(&schema), None)
        .expect("memory exec")
        .try_with_sort_information(LexOrdering::new(input_ordering).into_iter().collect())
        .expect("sort information");
    let input = Arc::new(TestMemoryExec::update_cache(&Arc::new(source)));
    let args = vec![col("ts", &schema).unwrap()];
    let partitionby_exprs = vec![col("pk", &schema).unwrap()];
    let orderby_exprs = vec![PhysicalSortExpr {
        expr: col("ts", &schema).unwrap(),
        options: Default::default(),
    }];
    let window_frame = WindowFrame::new_bounds(
        WindowFrameUnits::Range,
        WindowFrameBound::CurrentRow,
        WindowFrameBound::Following(ScalarValue::UInt64(Some(10))),
    );
    let window_expr = create_window_expr(
        &WindowFunctionDefinition::AggregateUDF(count_udaf()),
        "count".to_string(),
        &args,
        &partitionby_exprs,
        &orderby_exprs,
        Arc::new(window_frame),
        input.schema(),
        false,
        false,
        None,
    )
    .expect("window expr");
    Arc::new(
        BoundedWindowAggExec::try_new(vec![window_expr], input, mode, true)
            .expect("bounded window exec"),
    )
}

fn bounded_window_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("bounded_window_partitions");
    group.sample_size(10);

    for n_partitions in [100, 10_000] {
        let plan = window_exec(
            make_batches(n_partitions, false),
            InputOrderMode::Linear,
            vec![sort_expr("ts")],
        );
        group.bench_function(format!("linear {n_partitions} partitions"), |b| {
            b.iter(|| {
                let task_ctx = Arc::new(TaskContext::default());
                let batches = rt
                    .block_on(collect(Arc::clone(&plan), task_ctx))
                    .expect("execution");
                assert_eq!(
                    batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                    BATCH_SIZE * N_BATCHES
                );
            })
        });
    }

    // Control: the same query over partition-sorted input, where finished
    // partitions are pruned eagerly and the state maps stay small.
    let plan = window_exec(
        make_batches(10_000, true),
        InputOrderMode::Sorted,
        vec![sort_expr("pk"), sort_expr("ts")],
    );
    group.bench_function("sorted 10000 partitions", |b| {
        b.iter(|| {
            let task_ctx = Arc::new(TaskContext::default());
            let batches = rt
                .block_on(collect(Arc::clone(&plan), task_ctx))
                .expect("execution");
            assert_eq!(
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                BATCH_SIZE * N_BATCHES
            );
        })
    });

    group.finish();
}

criterion_group!(benches, bounded_window_benchmark);
criterion_main!(benches);
