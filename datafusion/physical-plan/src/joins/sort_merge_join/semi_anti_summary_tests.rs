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

//! Memory, stream, and predicate-eligibility coverage for existence summaries.
//! SQL results, data types, and batch boundaries are covered in
//! `sort_merge_join_matrix.slt`.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::{
    DataFusionError, JoinSide, JoinType, NullEquality, Result, ScalarValue,
    assert_contains,
};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal, NotExpr};
use futures::StreamExt;

use super::semi_anti_summary::SemiAntiComparison;
use crate::joins::SortMergeJoinExec;
use crate::joins::utils::{ColumnIndex, JoinFilter};
use crate::test::TestMemoryExec;
use crate::test::exec::MockExec;
use crate::{ExecutionPlan, PhysicalExpr, common};

fn column(index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(["left_value", "right_value"][index], index))
}

fn binary(
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(left, op, right))
}

fn filter(expr: Arc<dyn PhysicalExpr>, value_type: DataType) -> JoinFilter {
    JoinFilter::new(
        expr,
        vec![
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("left_value", value_type.clone(), true),
            Field::new("right_value", value_type, true),
        ])),
    )
}

fn batch(values: ArrayRef) -> Result<RecordBatch> {
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int32Array::from(vec![1; values.len()])) as ArrayRef,
        ),
        ("value", Arc::clone(&values)),
        (
            "id",
            Arc::new(Int32Array::from_iter_values(0..values.len() as i32)),
        ),
    ])
    .map_err(Into::into)
}

fn input(batch: &RecordBatch, chunk: usize) -> Result<Arc<dyn ExecutionPlan>> {
    let batches = (0..batch.num_rows())
        .step_by(chunk)
        .map(|offset| batch.slice(offset, chunk.min(batch.num_rows() - offset)))
        .collect::<Vec<_>>();
    Ok(TestMemoryExec::try_new_exec(
        &[batches],
        batch.schema(),
        None,
    )?)
}

fn join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    op: Operator,
) -> Result<SortMergeJoinExec> {
    let value_type = left.schema().field(1).data_type().clone();
    SortMergeJoinExec::try_new(
        left,
        right,
        vec![(
            Arc::new(Column::new("key", 0)),
            Arc::new(Column::new("key", 0)),
        )],
        Some(filter(binary(column(0), op, column(1)), value_type)),
        join_type,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
    )
}

async fn ids(plan: &dyn ExecutionPlan, ctx: Arc<TaskContext>) -> Result<Vec<i32>> {
    let mut ids = common::collect(plan.execute(0, ctx)?)
        .await?
        .iter()
        .flat_map(|batch| {
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    Ok(ids)
}

#[test]
fn only_single_cross_side_comparisons_are_eligible() -> Result<()> {
    let schema = batch(Arc::new(Int32Array::from(vec![1])))?.schema();
    // Exercise the physical predicate directly: SQL optimization can rewrite
    // NOT or push a side-local guard below the join before this parser sees it.
    for outer_is_left in [false, true] {
        for op in [
            Operator::NotEq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
            Operator::GtEq,
        ] {
            let direct = binary(column(0), op, column(1));
            assert!(
                SemiAntiComparison::try_new(
                    &filter(direct, DataType::Int32),
                    outer_is_left,
                    &schema,
                    &schema,
                )?
                .is_some()
            );
        }
        for op in [
            Operator::Eq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
            Operator::GtEq,
        ] {
            let negated = Arc::new(NotExpr::new(binary(column(0), op, column(1))));
            let comparison = SemiAntiComparison::try_new(
                &filter(negated, DataType::Int32),
                outer_is_left,
                &schema,
                &schema,
            )?
            .unwrap();
            let inner = batch(Arc::new(Int32Array::from(vec![None, Some(1), Some(3)])))?;
            let outer = batch(Arc::new(Int32Array::from(vec![
                None,
                Some(1),
                Some(2),
                Some(3),
            ])))?;
            assert_eq!(
                comparison.evaluate_inner_row(&inner, 0, &outer)?,
                BooleanArray::from(vec![false; 4]),
                "NULL inner row: NOT {op:?}, outer_is_left={outer_is_left}"
            );
            let expected_probe = match (op, outer_is_left) {
                (Operator::Eq, _) | (Operator::LtEq, true) | (Operator::GtEq, false) => {
                    vec![false, false, true, true]
                }
                (Operator::Lt, true) | (Operator::Gt, false) => {
                    vec![false, true, true, true]
                }
                (Operator::Gt, true) | (Operator::Lt, false) => {
                    vec![false, true, false, false]
                }
                (Operator::LtEq, false) | (Operator::GtEq, true) => {
                    vec![false, false, false, false]
                }
                _ => unreachable!(),
            };
            assert_eq!(
                comparison.evaluate_inner_row(&inner, 1, &outer)?,
                BooleanArray::from(expected_probe),
                "inner value 1: NOT {op:?}, outer_is_left={outer_is_left}"
            );
            let summary =
                comparison.summarize(&[inner.slice(0, 1), inner.slice(1, 2)])?;
            let expected = match (op, outer_is_left) {
                (Operator::LtEq, true) | (Operator::GtEq, false) => {
                    vec![false, false, true, true]
                }
                (Operator::GtEq, true) | (Operator::LtEq, false) => {
                    vec![false, true, true, false]
                }
                _ => vec![false, true, true, true],
            };
            assert_eq!(
                comparison.evaluate(&summary, &outer)?,
                BooleanArray::from(expected),
                "NOT {op:?}, outer_is_left={outer_is_left}"
            );
        }
        let comparison = binary(column(0), Operator::Lt, column(1));
        let guard = binary(
            column(1),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        );
        for expr in [
            binary(column(0), Operator::Eq, column(1)),
            binary(column(0), Operator::NotEq, column(0)),
            binary(Arc::clone(&comparison), Operator::And, guard),
            binary(Arc::clone(&comparison), Operator::Or, comparison),
            binary(
                binary(column(0), Operator::Plus, column(0)),
                Operator::Lt,
                column(1),
            ),
        ] {
            assert!(
                SemiAntiComparison::try_new(
                    &filter(expr, DataType::Int32),
                    outer_is_left,
                    &schema,
                    &schema,
                )?
                .is_none()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn large_strings_fall_back_when_summary_or_buffer_exceeds_memory() -> Result<()> {
    let values = [
        "a".repeat(16 * 1024),
        "a".repeat(16 * 1024),
        "z".repeat(16 * 1024),
        "b".repeat(16 * 1024),
        "b".repeat(16 * 1024),
        "b".repeat(16 * 1024),
        "b".repeat(16 * 1024),
    ];
    let outer = batch(Arc::new(StringArray::from(vec![values[0].as_str(), "m"])))?;
    let inner = batch(Arc::new(StringArray::from_iter_values(values.iter())))?;
    // The two initial probes cross input batches. For != the first probe
    // already matches "m", so its matched bit must survive the second probe.
    let buffered_size = (0..inner.num_rows())
        .map(|row| inner.slice(row, 1).get_array_memory_size())
        .sum::<usize>();
    for op in [Operator::NotEq, Operator::Lt] {
        let mut results = vec![];
        // The middle budget holds the group but cannot also admit the
        // conservative string-summary allowance. The last forces spilling.
        for (limit, spill) in [
            (None, false),
            (Some(buffered_size + 4096), false),
            (Some(32 * 1024), true),
        ] {
            let mut builder = RuntimeEnvBuilder::new();
            if let Some(limit) = limit {
                builder = builder
                    .with_memory_limit(limit, 1.0)
                    .with_disk_manager_builder(
                        DiskManagerBuilder::default()
                            .with_mode(DiskManagerMode::OsTmpDirectory),
                    );
            }
            let runtime = builder.build_arc()?;
            let ctx = Arc::new(TaskContext::default().with_runtime(Arc::clone(&runtime)));
            let plan =
                join(input(&outer, 2)?, input(&inner, 1)?, JoinType::LeftSemi, op)?;
            results.push(ids(&plan, ctx).await?);
            assert_eq!(plan.metrics().unwrap().spill_count().unwrap() > 0, spill);
            assert_eq!(runtime.memory_pool.reserved(), 0);
        }
        assert_eq!(results[0], vec![0, 1]);
        assert_eq!(results[0], results[1]);
        assert_eq!(results[0], results[2]);
    }
    Ok(())
}

#[tokio::test]
async fn buffered_comparison_propagates_upstream_errors() -> Result<()> {
    let outer = batch(Arc::new(Int32Array::from(vec![7])))?;
    let inner = batch(Arc::new(Int32Array::from(vec![1, 2])))?;
    let source: Arc<dyn ExecutionPlan> = Arc::new(
        MockExec::new(
            vec![
                Ok(inner.clone()),
                Err(DataFusionError::Execution("injected inner failure".into())),
            ],
            inner.schema(),
        )
        .with_use_task(false)
        .with_unknown_statistics(),
    );
    let plan = join(
        input(&outer, 1)?,
        source,
        JoinType::LeftSemi,
        Operator::NotEq,
    )?;
    let ctx = Arc::new(TaskContext::default());
    let pool = Arc::clone(ctx.memory_pool());
    let error = ids(&plan, ctx).await.unwrap_err();
    assert_contains!(error.to_string(), "injected inner failure");
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn dropping_stream_releases_buffered_group_and_summary() -> Result<()> {
    let outer = batch(Arc::new(Int32Array::from(vec![7; 4])))?;
    let row_ids = |batch: &RecordBatch| {
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    };
    // Neither of the first two inner rows matches. Both groups must build a
    // summary: one finds 9, while the all-NULL group keeps the anti rows.
    for (values, kind) in [
        (
            vec![
                Some(1),
                Some(2),
                Some(9),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
            ],
            JoinType::LeftSemi,
        ),
        (vec![None; 7], JoinType::LeftAnti),
    ] {
        let inner = batch(Arc::new(Int32Array::from(values)))?;
        // The same key first has a singleton slice, then two rows that can
        // benefit from a summary, then another singleton that reuses it.
        let outer_input = TestMemoryExec::try_new_exec(
            &[vec![
                outer.slice(0, 1),
                outer.slice(1, 2),
                outer.slice(3, 1),
            ]],
            outer.schema(),
            None,
        )?;
        let plan = join(outer_input, input(&inner, 7)?, kind, Operator::Lt)?;
        let ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(1)),
        );
        let pool = Arc::clone(ctx.memory_pool());
        let mut stream = plan.execute(0, ctx)?;
        let first = stream.next().await.transpose()?.unwrap();
        assert_eq!(row_ids(&first), vec![0]);
        assert_eq!(pool.reserved(), inner.get_array_memory_size());

        let second = stream.next().await.transpose()?.unwrap();
        assert_eq!(row_ids(&second), vec![1, 2]);
        let peak = plan
            .metrics()
            .unwrap()
            .sum_by_name("peak_mem_used")
            .unwrap()
            .as_usize();
        assert!(
            peak > inner.get_array_memory_size(),
            "summary storage must be admitted alongside the buffered group"
        );
        let cached_size = pool.reserved();
        assert!(
            cached_size > 0 && cached_size < peak,
            "keep the cached summary admitted after releasing the buffered group"
        );

        let last = stream.next().await.transpose()?.unwrap();
        assert_eq!(row_ids(&last), vec![3]);
        assert_eq!(
            pool.reserved(),
            cached_size,
            "reuse the summary for the final singleton slice"
        );
        drop(stream);
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}
