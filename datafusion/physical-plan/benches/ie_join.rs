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

//! End-to-end IEJoin benchmarks, including materialization and sorting.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::{JoinSide, JoinType, NullEquality};
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::collect;
use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion_physical_plan::joins::{
    HashJoinExec, IEJoinCondition, IEJoinExec, NestedLoopJoinExec, PartitionMode,
};
use datafusion_physical_plan::test::TestMemoryExec;
use tokio::runtime::Runtime;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("start_at", DataType::Int64, false),
        Field::new("end_at", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
    ]))
}

fn make_input(rows: usize, offset: i64, schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
    let starts = (0..rows)
        .map(|row| row as i64 * 10 + offset)
        .collect::<Vec<_>>();
    let ends = starts.iter().map(|start| start + 12).collect::<Vec<_>>();
    let payload = (0..rows as i64).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(starts)),
            Arc::new(Int64Array::from(ends)),
            Arc::new(Int64Array::from(payload)),
        ],
    )
    .unwrap();
    TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(schema), None).unwrap()
}

fn condition_columns(start_index: usize, end_index: usize) -> [IEJoinCondition; 2] {
    [
        IEJoinCondition::new(
            Arc::new(Column::new("start_at", start_index)),
            Arc::new(Column::new("end_at", end_index)),
            Operator::LtEq,
        ),
        IEJoinCondition::new(
            Arc::new(Column::new("end_at", end_index)),
            Arc::new(Column::new("start_at", start_index)),
            Operator::GtEq,
        ),
    ]
}

fn join_filter(schema: &SchemaRef, start_index: usize, end_index: usize) -> JoinFilter {
    let column_indices = vec![
        ColumnIndex {
            index: start_index,
            side: JoinSide::Left,
        },
        ColumnIndex {
            index: end_index,
            side: JoinSide::Left,
        },
        ColumnIndex {
            index: start_index,
            side: JoinSide::Right,
        },
        ColumnIndex {
            index: end_index,
            side: JoinSide::Right,
        },
    ];
    let filter_schema = Arc::new(Schema::new(vec![
        schema.field(start_index).clone(),
        schema.field(end_index).clone(),
        schema.field(start_index).clone(),
        schema.field(end_index).clone(),
    ]));
    let first = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("start_at", 0)),
        Operator::LtEq,
        Arc::new(Column::new("end_at", 3)),
    )) as PhysicalExprRef;
    let second = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("end_at", 1)),
        Operator::GtEq,
        Arc::new(Column::new("start_at", 2)),
    )) as PhysicalExprRef;
    let expression =
        Arc::new(BinaryExpr::new(first, Operator::And, second)) as Arc<dyn PhysicalExpr>;
    JoinFilter::new(expression, column_indices, filter_schema)
}

fn keyed_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group", DataType::Int64, false),
        Field::new("start_at", DataType::Int64, false),
        Field::new("end_at", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
    ]))
}

fn make_keyed_input(
    rows: usize,
    offset: i64,
    groups: usize,
    schema: &SchemaRef,
) -> Arc<dyn ExecutionPlan> {
    let group = (0..rows)
        .map(|row| (row % groups) as i64)
        .collect::<Vec<_>>();
    let starts = (0..rows)
        .map(|row| (row / groups) as i64 * 10 + offset)
        .collect::<Vec<_>>();
    let ends = starts.iter().map(|start| start + 12).collect::<Vec<_>>();
    let payload = (0..rows as i64).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(group)),
            Arc::new(Int64Array::from(starts)),
            Arc::new(Int64Array::from(ends)),
            Arc::new(Int64Array::from(payload)),
        ],
    )
    .unwrap();
    TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(schema), None).unwrap()
}

fn equality_columns() -> Vec<(PhysicalExprRef, PhysicalExprRef)> {
    vec![(
        Arc::new(Column::new("group", 0)) as PhysicalExprRef,
        Arc::new(Column::new("group", 0)) as PhysicalExprRef,
    )]
}

fn run(plan: Arc<dyn ExecutionPlan>, runtime: &Runtime) -> usize {
    runtime.block_on(async {
        collect(plan, Arc::new(TaskContext::default()))
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    })
}

fn bench_ie_join(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let schema = schema();
    let mut group = c.benchmark_group("interval_overlap_join");

    for rows in [1_000, 5_000] {
        group.bench_with_input(BenchmarkId::new("ie_join", rows), &rows, |b, rows| {
            b.iter(|| {
                let left = make_input(*rows, 0, &schema);
                let right = make_input(*rows, 5, &schema);
                let plan = IEJoinExec::try_new(
                    left,
                    right,
                    vec![],
                    condition_columns(0, 1),
                    None,
                    JoinType::Inner,
                    NullEquality::NullEqualsNothing,
                )
                .unwrap();
                run(Arc::new(plan), &runtime)
            })
        });

        group.bench_with_input(
            BenchmarkId::new("nested_loop_join", rows),
            &rows,
            |b, rows| {
                b.iter(|| {
                    let left = make_input(*rows, 0, &schema);
                    let right = make_input(*rows, 5, &schema);
                    let plan = NestedLoopJoinExec::try_new(
                        left,
                        right,
                        Some(join_filter(&schema, 0, 1)),
                        &JoinType::Inner,
                        None,
                    )
                    .unwrap();
                    run(Arc::new(plan), &runtime)
                })
            },
        );
    }
    group.finish();
}

fn bench_keyed_ie_join(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let schema = keyed_schema();
    let mut group = c.benchmark_group("keyed_interval_overlap_join");
    let groups = 128;

    for rows in [10_000, 50_000] {
        group.bench_with_input(BenchmarkId::new("ie_join", rows), &rows, |b, rows| {
            b.iter(|| {
                let left = make_keyed_input(*rows, 0, groups, &schema);
                let right = make_keyed_input(*rows, 5, groups, &schema);
                let plan = IEJoinExec::try_new(
                    left,
                    right,
                    equality_columns(),
                    condition_columns(1, 2),
                    None,
                    JoinType::Inner,
                    NullEquality::NullEqualsNothing,
                )
                .unwrap();
                run(Arc::new(plan), &runtime)
            })
        });

        group.bench_with_input(BenchmarkId::new("hash_join", rows), &rows, |b, rows| {
            b.iter(|| {
                let left = make_keyed_input(*rows, 0, groups, &schema);
                let right = make_keyed_input(*rows, 5, groups, &schema);
                let plan = HashJoinExec::try_new(
                    left,
                    right,
                    equality_columns(),
                    Some(join_filter(&schema, 1, 2)),
                    &JoinType::Inner,
                    None,
                    PartitionMode::CollectLeft,
                    NullEquality::NullEqualsNothing,
                    false,
                )
                .unwrap();
                run(Arc::new(plan), &runtime)
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_ie_join, bench_keyed_ie_join);
criterion_main!(benches);
