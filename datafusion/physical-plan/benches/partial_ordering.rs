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

use arrow::array::{ArrayRef, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::aggregates::order::GroupOrderingPartial;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, collect};

use criterion::{Criterion, criterion_group, criterion_main};

const BATCH_SIZE: usize = 8192;
const LOGICAL_RUNS: usize = 16;
const GROUPS_PER_RUN: usize = 2048;
const ROWS_PER_GROUP: usize = BATCH_SIZE / GROUPS_PER_RUN;

fn create_test_arrays(num_columns: usize) -> Vec<ArrayRef> {
    (0..num_columns)
        .map(|i| {
            Arc::new(Int32Array::from_iter_values(
                (0..BATCH_SIZE as i32).map(|x| x * (i + 1) as i32),
            )) as ArrayRef
        })
        .collect()
}
fn bench_new_groups(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_ordering_partial");

    // Test with 1, 2, 4, and 8 order indices
    for num_columns in [1, 2, 4, 8] {
        let order_indices: Vec<usize> = (0..num_columns).collect();

        group.bench_function(format!("order_indices_{num_columns}"), |b| {
            let batch_group_values = create_test_arrays(num_columns);
            let group_indices: Vec<usize> = (0..BATCH_SIZE).collect();

            b.iter(|| {
                let mut ordering =
                    GroupOrderingPartial::try_new(order_indices.clone()).unwrap();
                ordering
                    .new_groups(&batch_group_values, &group_indices, BATCH_SIZE)
                    .unwrap();
            });
        });
    }
    group.finish();
}

fn aggregate_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Build locally key-ordered logical runs whose key ranges are disjoint. The
/// runs are concatenated in descending range order so the resulting stream is
/// intentionally not globally sorted.
fn partition_disjoint_batches(schema: &SchemaRef) -> Vec<RecordBatch> {
    (0..LOGICAL_RUNS)
        .map(|run| {
            let key_offset = (LOGICAL_RUNS - run - 1) * GROUPS_PER_RUN;
            let keys =
                (0..BATCH_SIZE).map(|row| (key_offset + row / ROWS_PER_GROUP) as i32);
            let values = std::iter::repeat_n(1_i64, BATCH_SIZE);
            RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(Int32Array::from_iter_values(keys)),
                    Arc::new(Int64Array::from_iter_values(values)),
                ],
            )
            .unwrap()
        })
        .collect()
}

fn aggregate_plan(partition_disjoint: bool) -> Arc<dyn ExecutionPlan> {
    let schema = aggregate_schema();
    let batches = partition_disjoint_batches(&schema);
    let input = TestMemoryExec::try_new(&[batches], Arc::clone(&schema), None).unwrap();
    let input: Arc<dyn ExecutionPlan> = if partition_disjoint {
        Arc::new(
            input
                .try_with_group_contiguous_keys(vec![col("key", &schema).unwrap()])
                .unwrap(),
        )
    } else {
        Arc::new(input)
    };
    let group_by = PhysicalGroupBy::new_single(vec![(
        col("key", &schema).unwrap(),
        "key".to_string(),
    )]);
    let aggregate = Arc::new(
        AggregateExprBuilder::new(sum_udaf(), vec![col("value", &schema).unwrap()])
            .schema(Arc::clone(&schema))
            .alias("SUM(value)")
            .build()
            .unwrap(),
    );

    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![aggregate],
            vec![None],
            input,
            schema,
        )
        .unwrap(),
    )
}

fn bench_partition_disjoint_aggregate(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let task_ctx = Arc::new(
        TaskContext::default().with_session_config(
            SessionConfig::new()
                .with_batch_size(BATCH_SIZE)
                .set_bool("datafusion.execution.enable_migration_aggregate", true),
        ),
    );
    let mut group = c.benchmark_group("partition_disjoint_aggregate");

    for (name, partition_disjoint) in [
        ("unordered_hash", false),
        ("partition_disjoint_streaming", true),
    ] {
        let plan = aggregate_plan(partition_disjoint);
        group.bench_function(name, |b| {
            b.iter(|| {
                let batches = runtime
                    .block_on(collect(Arc::clone(&plan), Arc::clone(&task_ctx)))
                    .unwrap();
                assert_eq!(
                    batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    LOGICAL_RUNS * GROUPS_PER_RUN
                );
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_new_groups,
    bench_partition_disjoint_aggregate
);
criterion_main!(benches);
