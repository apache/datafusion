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

//! Criterion benchmark comparing existence (LeftSemi / LeftAnti) joins over a single
//! range predicate (`left.key < right.key`) evaluated two ways:
//!
//! - `PiecewiseMergeJoinExec` (with the required `SortExec` on the buffered/left side,
//!   as the physical planner would insert), and
//! - `NestedLoopJoinExec`, which is the fallback used when
//!   `enable_piecewise_merge_join` is off.
//!
//! Both plans compute the same result, so this measures the win from routing an
//! inequality-correlated `EXISTS` / `NOT EXISTS` to PWMJ instead of the O(n*m)
//! nested-loop join. The `SortExec` is included on the PWMJ side because it is a real
//! cost of that plan.
//!
//! ## Axes
//! - **join type**: LeftSemi (`EXISTS`) and LeftAnti (`NOT EXISTS`).
//! - **selectivity**: the fraction of left rows that have at least one matching right
//!   row, controlled by shifting the right-side key range. Semi output size grows with
//!   selectivity; Anti output size shrinks.

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::JoinSide;
use datafusion_common::JoinType;
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion_physical_plan::joins::{NestedLoopJoinExec, PiecewiseMergeJoinExec};
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr, collect};
use tokio::runtime::Runtime;

/// Two-column schema: (`key`, `payload`).
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("payload", DataType::Int32, false),
    ]))
}

/// Build a single-partition input of `num_rows` rows. Keys are drawn from
/// `[key_offset, key_offset + key_span)` in a fixed, reproducible pattern (no RNG so
/// the benchmark is deterministic).
fn build_exec(
    num_rows: usize,
    key_offset: i32,
    key_span: i32,
    schema: &SchemaRef,
) -> Arc<dyn ExecutionPlan> {
    let keys: Vec<i32> = (0..num_rows)
        .map(|i| key_offset + (i as i32 * 2_654_435_761u32 as i32).rem_euclid(key_span))
        .collect();
    let payload: Vec<i32> = (0..num_rows as i32).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(Int32Array::from(payload)),
        ],
    )
    .unwrap();

    // Slice into 8192-row batches to mirror a realistic streamed input.
    let batch_size = 8192;
    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = (batch.num_rows() - offset).min(batch_size);
        batches.push(batch.slice(offset, len));
        offset += len;
    }
    TestMemoryExec::try_new_exec(&[batches], Arc::clone(schema), None).unwrap()
}

/// `PiecewiseMergeJoinExec` over `left.key < right.key`, with the required `SortExec`
/// on the buffered (left) side. `<` requires the buffered side sorted descending.
fn pwmj_plan(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
) -> Arc<dyn ExecutionPlan> {
    let sort = LexOrdering::new(vec![PhysicalSortExpr::new(
        Arc::new(Column::new("key", 0)),
        SortOptions::new(true, true),
    )])
    .unwrap();
    let sorted_left = Arc::new(SortExec::new(sort, left));

    let on: (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>) = (
        Arc::new(Column::new("key", 0)),
        Arc::new(Column::new("key", 0)),
    );
    Arc::new(
        PiecewiseMergeJoinExec::try_new(
            sorted_left,
            right,
            on,
            Operator::Lt,
            join_type,
            1,
        )
        .unwrap(),
    )
}

/// `NestedLoopJoinExec` over the same `left.key < right.key` predicate.
fn nlj_plan(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
) -> Arc<dyn ExecutionPlan> {
    let intermediate_schema = Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("key", DataType::Int32, false),
    ]);
    let expr = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("key", 0)),
        Operator::Lt,
        Arc::new(Column::new("key", 1)),
    )) as Arc<dyn PhysicalExpr>;
    let column_indices = vec![
        ColumnIndex {
            index: 0,
            side: JoinSide::Left,
        },
        ColumnIndex {
            index: 0,
            side: JoinSide::Right,
        },
    ];
    let filter = JoinFilter::new(expr, column_indices, Arc::new(intermediate_schema));
    Arc::new(
        NestedLoopJoinExec::try_new(left, right, Some(filter), &join_type, None).unwrap(),
    )
}

fn run(plan: Arc<dyn ExecutionPlan>, rt: &Runtime) -> usize {
    let task_ctx = Arc::new(TaskContext::default());
    rt.block_on(async {
        let batches = collect(plan, task_ctx).await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    })
}

fn bench_pwmj_semi_anti(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let s = schema();

    // Left (buffered) is deliberately smaller than right (streamed); the streamed side
    // drives the loop in both operators.
    let left_rows = 20_000;
    let right_rows = 20_000;
    let key_span = 10_000;

    // Selectivity is set by how far the right key range sits above the left range.
    // - "high": right keys mostly above left keys  -> most left rows match (Semi large)
    // - "low":  right keys mostly below left keys   -> few left rows match  (Anti large)
    let regimes: [(&str, i32); 2] = [("sel_high", key_span), ("sel_low", -key_span)];

    let mut group = c.benchmark_group("pwmj_vs_nlj_semi_anti");
    // Nested-loop is O(n*m); keep sample counts modest so the suite finishes.
    group.sample_size(10);

    for (regime, right_offset) in regimes {
        for join_type in [JoinType::LeftSemi, JoinType::LeftAnti] {
            let jt = match join_type {
                JoinType::LeftSemi => "semi",
                JoinType::LeftAnti => "anti",
                _ => unreachable!(),
            };

            let build_inputs = || {
                (
                    build_exec(left_rows, 0, key_span, &s),
                    build_exec(right_rows, right_offset, key_span, &s),
                )
            };

            group.bench_function(
                BenchmarkId::new(format!("pwmj_{jt}_{regime}"), right_rows),
                |b| {
                    b.iter(|| {
                        let (left, right) = build_inputs();
                        run(pwmj_plan(left, right, join_type), &rt)
                    })
                },
            );

            group.bench_function(
                BenchmarkId::new(format!("nlj_{jt}_{regime}"), right_rows),
                |b| {
                    b.iter(|| {
                        let (left, right) = build_inputs();
                        run(nlj_plan(left, right, join_type), &rt)
                    })
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_pwmj_semi_anti);
criterion_main!(benches);
