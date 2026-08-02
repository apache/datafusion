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

//! Benchmarks for `BoundedWindowAggExec` with many partitions.
//!
//! The streaming window operator keeps per-partition state keyed by
//! `PartitionKey` (`Vec<ScalarValue>`) and, in `Linear` mode (input sorted
//! by the ORDER BY column but not by the partition columns), visits every
//! live partition on every batch while never retiring partitions until the
//! input is exhausted. The cases here stress that path in different ways:
//!
//! - `linear N partitions`: dense round-robin keys -- every partition
//!   receives rows in every batch, so per-visit fixed costs dominate.
//! - `linear sparse N partitions`: keys are clustered in time, so each
//!   batch touches only a small, fresh subset of keys while the set of live
//!   partitions keeps growing -- per-batch work on quiet partitions
//!   dominates.
//! - `linear rows N partitions`: the dense layout with a ROWS frame, whose
//!   results can only be finalized as more rows of the same partition
//!   arrive.
//! - `linear multi N partitions`: two window expressions over the dense
//!   layout, doubling the per-partition evaluation sweeps.
//! - `sorted N partitions`: control; input sorted by partition key, so
//!   finished partitions are pruned eagerly and the state maps stay small.

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
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::windows::{BoundedWindowAggExec, create_window_expr};
use datafusion_physical_plan::{ExecutionPlan, InputOrderMode, collect};

const BATCH_SIZE: usize = 8192;
const N_BATCHES: usize = 16;
/// Distinct partition keys per batch in the sparse layout. Each batch
/// introduces this many previously-unseen keys, so the total partition count
/// is `N_BATCHES * SPARSE_KEYS_PER_BATCH`.
const SPARSE_KEYS_PER_BATCH: usize = 2048;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("pk", DataType::UInt64, false),
        Field::new("ts", DataType::UInt64, false),
    ]))
}

/// Batches with `ts` ascending across the whole input and partition keys
/// chosen by `pk_of_row`.
fn make_batches(pk_of_row: impl Fn(usize) -> u64) -> Vec<RecordBatch> {
    (0..N_BATCHES)
        .map(|b| {
            let start = b * BATCH_SIZE;
            let pk: UInt64Array = (start..start + BATCH_SIZE)
                .map(|i| Some(pk_of_row(i)))
                .collect();
            let ts: UInt64Array = (start..start + BATCH_SIZE)
                .map(|i| Some(i as u64))
                .collect();
            RecordBatch::try_new(schema(), vec![Arc::new(pk), Arc::new(ts)]).unwrap()
        })
        .collect()
}

/// Round-robin over `n_partitions`: every partition receives rows in every
/// batch (when `n_partitions <= BATCH_SIZE`).
fn dense_batches(n_partitions: usize) -> Vec<RecordBatch> {
    make_batches(move |i| (i % n_partitions) as u64)
}

/// Keys clustered in time: batch `b` only contains keys in
/// `[b * SPARSE_KEYS_PER_BATCH, (b + 1) * SPARSE_KEYS_PER_BATCH)`, cycled so
/// that consecutive rows belong to different partitions. Previously-seen
/// keys never recur, but `Linear` mode cannot know that, so the live
/// partition set grows for the whole run.
fn sparse_batches() -> Vec<RecordBatch> {
    make_batches(|i| {
        ((i / BATCH_SIZE) * SPARSE_KEYS_PER_BATCH + (i % SPARSE_KEYS_PER_BATCH)) as u64
    })
}

/// Input laid out partition-by-partition (the `Sorted` layout).
fn sorted_batches(n_partitions: usize) -> Vec<RecordBatch> {
    let rows_per_partition = BATCH_SIZE * N_BATCHES / n_partitions;
    make_batches(move |i| (i / rows_per_partition) as u64)
}

fn sort_expr(name: &str) -> PhysicalSortExpr {
    PhysicalSortExpr {
        expr: col(name, &schema()).unwrap(),
        options: Default::default(),
    }
}

/// `RANGE BETWEEN CURRENT ROW AND 10 FOLLOWING`
fn range_frame() -> WindowFrame {
    WindowFrame::new_bounds(
        WindowFrameUnits::Range,
        WindowFrameBound::CurrentRow,
        WindowFrameBound::Following(ScalarValue::UInt64(Some(10))),
    )
}

/// `ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING`
fn rows_frame() -> WindowFrame {
    WindowFrame::new_bounds(
        WindowFrameUnits::Rows,
        WindowFrameBound::CurrentRow,
        WindowFrameBound::Following(ScalarValue::UInt64(Some(2))),
    )
}

/// `<agg>(ts) OVER (PARTITION BY pk ORDER BY ts <window_frame>)` for each
/// aggregate in `aggregates`.
fn window_exec(
    batches: Vec<RecordBatch>,
    mode: InputOrderMode,
    input_ordering: Vec<PhysicalSortExpr>,
    window_frame: &WindowFrame,
    aggregates: &[(WindowFunctionDefinition, &str)],
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
    let window_expr = aggregates
        .iter()
        .map(|(fun, name)| {
            create_window_expr(
                fun,
                name.to_string(),
                &args,
                &partitionby_exprs,
                &orderby_exprs,
                Arc::new(window_frame.clone()),
                input.schema(),
                false,
                false,
                None,
            )
            .expect("window expr")
        })
        .collect::<Vec<_>>();
    Arc::new(
        BoundedWindowAggExec::try_new(window_expr, input, mode, true)
            .expect("bounded window exec"),
    )
}

fn count() -> (WindowFunctionDefinition, &'static str) {
    (
        WindowFunctionDefinition::AggregateUDF(count_udaf()),
        "count",
    )
}

fn sum() -> (WindowFunctionDefinition, &'static str) {
    (WindowFunctionDefinition::AggregateUDF(sum_udaf()), "sum")
}

fn bounded_window_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("bounded_window_partitions");
    group.sample_size(10);

    let mut run_case = |name: String, plan: Arc<dyn ExecutionPlan>| {
        group.bench_function(name, |b| {
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
    };

    for n_partitions in [100, 10_000] {
        run_case(
            format!("linear {n_partitions} partitions"),
            window_exec(
                dense_batches(n_partitions),
                InputOrderMode::Linear,
                vec![sort_expr("ts")],
                &range_frame(),
                &[count()],
            ),
        );
    }

    run_case(
        format!(
            "linear sparse {} partitions",
            N_BATCHES * SPARSE_KEYS_PER_BATCH
        ),
        window_exec(
            sparse_batches(),
            InputOrderMode::Linear,
            vec![sort_expr("ts")],
            &range_frame(),
            &[count()],
        ),
    );

    run_case(
        "linear rows 10000 partitions".to_string(),
        window_exec(
            dense_batches(10_000),
            InputOrderMode::Linear,
            vec![sort_expr("ts")],
            &rows_frame(),
            &[count()],
        ),
    );

    run_case(
        "linear multi 10000 partitions".to_string(),
        window_exec(
            dense_batches(10_000),
            InputOrderMode::Linear,
            vec![sort_expr("ts")],
            &range_frame(),
            &[count(), sum()],
        ),
    );

    // Control: the same query over partition-sorted input, where finished
    // partitions are pruned eagerly and the state maps stay small.
    run_case(
        "sorted 10000 partitions".to_string(),
        window_exec(
            sorted_batches(10_000),
            InputOrderMode::Sorted,
            vec![sort_expr("pk"), sort_expr("ts")],
            &range_frame(),
            &[count()],
        ),
    );

    group.finish();
}

criterion_group!(benches, bounded_window_benchmark);
criterion_main!(benches);
