// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Memory, stream, and predicate-eligibility coverage for semi/anti join summaries.
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
    keyed_batch(vec![1; values.len()], values)
}

fn keyed_batch(keys: Vec<i32>, values: ArrayRef) -> Result<RecordBatch> {
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int32Array::from(keys)) as ArrayRef),
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

fn row_ids(batch: &RecordBatch) -> Vec<i32> {
    batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn single_row_context() -> Arc<TaskContext> {
    Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(1)),
    )
}

async fn ids(plan: &dyn ExecutionPlan, ctx: Arc<TaskContext>) -> Result<Vec<i32>> {
    let mut ids = common::collect(plan.execute(0, ctx)?)
        .await?
        .iter()
        .flat_map(row_ids)
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
            let summary = comparison
                .scan(&[inner.slice(0, 1), inner.slice(1, 2)], &outer, |_| false)?
                .unwrap();
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
async fn large_strings_produce_the_same_results_when_the_buffer_spills() -> Result<()> {
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
    for op in [Operator::NotEq, Operator::Lt] {
        for (limit, spill) in [(None, false), (Some(32 * 1024), true)] {
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
            assert_eq!(ids(&plan, ctx).await?, vec![0, 1]);
            assert_eq!(plan.metrics().unwrap().spill_count().unwrap() > 0, spill);
            assert_eq!(runtime.memory_pool.reserved(), 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn not_equal_reserves_both_compact_string_extrema() -> Result<()> {
    let min = "a".repeat(16 * 1024);
    let max = "z".repeat(32 * 1024);
    let mut values = vec![min.as_str(); 7];
    values[6] = max.as_str();
    let inner = batch(Arc::new(StringArray::from(values)))?;
    let outer = batch(Arc::new(StringArray::from(vec![min.as_str()])))?;
    let plan = join(
        input(&outer, 1)?,
        input(&inner, 7)?,
        JoinType::LeftSemi,
        Operator::NotEq,
    )?;
    let ctx = single_row_context();
    let pool = Arc::clone(ctx.memory_pool());
    let mut stream = plan.execute(0, ctx)?;
    assert_eq!(row_ids(&stream.next().await.transpose()?.unwrap()), [0]);

    let min_bytes = ScalarValue::Utf8(Some(min))
        .to_array()?
        .get_array_memory_size();
    let max_bytes = ScalarValue::Utf8(Some(max))
        .to_array()?
        .get_array_memory_size();
    let retained = pool.reserved();
    assert!(
        retained >= min_bytes + max_bytes,
        "both Arrow scalars are live"
    );
    assert!(
        retained < inner.get_array_memory_size(),
        "the parent buffer was released"
    );
    drop(stream);
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn oversized_extrema_fall_back_to_the_prorated_buffer() -> Result<()> {
    // Group 1 ends inside the batch. Its seven rows fit at the group's
    // per-row share, but even one of its two owned extrema exceeds the limit.
    let long = "x".repeat(16 * 1024);
    let mut keys = vec![2; 256];
    keys[..7].fill(1);
    let mut values = vec![""; keys.len()];
    values[..7].fill(long.as_str());
    let inner = keyed_batch(keys, Arc::new(StringArray::from(values)))?;
    let outer = batch(Arc::new(StringArray::from(vec![long.as_str(), "a"])))?;
    let buffered_size = inner
        .slice(0, 7)
        .get_array_memory_size()
        .div_ceil(inner.num_rows())
        * 7;
    assert!(buffered_size < long.len());
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(buffered_size, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()?;
    let ctx = Arc::new(TaskContext::default().with_runtime(Arc::clone(&runtime)));
    let plan = join(
        input(&outer, 1)?,
        input(&inner, inner.num_rows())?,
        JoinType::LeftAnti,
        Operator::NotEq,
    )?;
    assert_eq!(ids(&plan, ctx).await?, vec![0]);
    assert_eq!(plan.metrics().unwrap().spill_count().unwrap(), 0);
    assert_eq!(runtime.memory_pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn partial_prefix_keeps_the_tail_for_the_next_outer_batch() -> Result<()> {
    let outer = batch(Arc::new(Int32Array::from(vec![7, 16])))?;
    let mut values = vec![1; 32];
    values[2] = 9;
    values[31] = 17;
    let inner = batch(Arc::new(Int32Array::from(values)))?;
    let plan = join(
        input(&outer, 1)?,
        input(&inner, 32)?,
        JoinType::LeftSemi,
        Operator::Lt,
    )?;
    let ctx = single_row_context();
    let pool = Arc::clone(ctx.memory_pool());
    let mut stream = plan.execute(0, ctx)?;

    assert_eq!(row_ids(&stream.next().await.transpose()?.unwrap()), [0]);
    // The third inner row satisfied the first singleton. An early exit must
    // retain the whole buffer because the next singleton needs the last row.
    assert_eq!(pool.reserved(), inner.get_array_memory_size());
    assert_eq!(row_ids(&stream.next().await.transpose()?.unwrap()), [1]);
    assert!(stream.next().await.is_none());
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn singleton_outer_reuses_all_null_summary_until_key_end() -> Result<()> {
    let outer = keyed_batch(vec![1, 1, 2], Arc::new(Int32Array::from(vec![7; 3])))?;
    let mut keys = vec![1; 7];
    keys.push(3);
    let inner = keyed_batch(keys, Arc::new(Int32Array::from(vec![None; 8])))?;
    let plan = join(
        input(&outer, 1)?,
        input(&inner, 7)?,
        JoinType::LeftAnti,
        Operator::Lt,
    )?;
    let ctx = single_row_context();
    let pool = Arc::clone(ctx.memory_pool());
    let mut stream = plan.execute(0, ctx)?;

    assert_eq!(row_ids(&stream.next().await.transpose()?.unwrap()), [0]);
    let summary_bytes = pool.reserved();
    let group_bytes = inner.slice(0, 7).get_array_memory_size();
    assert!(summary_bytes > 0 && summary_bytes < group_bytes);
    assert_eq!(
        plan.metrics()
            .unwrap()
            .sum_by_name("peak_mem_used")
            .unwrap()
            .as_usize(),
        group_bytes,
        "summary admission must reuse the buffer reservation"
    );
    assert_eq!(row_ids(&stream.next().await.transpose()?.unwrap()), [1]);
    assert_eq!(pool.reserved(), summary_bytes);
    assert_eq!(row_ids(&stream.next().await.transpose()?.unwrap()), [2]);
    // The cursor is still at inner key 3, so draining the inner input cannot
    // account for releasing the summary from key 1.
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn cached_summary_is_released_on_outer_error_or_drop() -> Result<()> {
    let outer = batch(Arc::new(Int32Array::from(vec![7])))?;
    let inner = batch(Arc::new(Int32Array::from(vec![1; 7])))?;
    for read_error in [false, true] {
        let source: Arc<dyn ExecutionPlan> = Arc::new(
            MockExec::new(
                vec![
                    Ok(outer.clone()),
                    Err(DataFusionError::Execution("injected outer failure".into())),
                ],
                outer.schema(),
            )
            .with_use_task(false)
            .with_unknown_statistics(),
        );
        let plan = join(source, input(&inner, 7)?, JoinType::LeftAnti, Operator::Lt)?;
        let ctx = single_row_context();
        let pool = Arc::clone(ctx.memory_pool());
        let mut stream = plan.execute(0, ctx)?;
        assert_eq!(row_ids(&stream.next().await.transpose()?.unwrap()), [0]);
        assert!(pool.reserved() > 0 && pool.reserved() < inner.get_array_memory_size());
        if read_error {
            let error = stream.next().await.unwrap().unwrap_err();
            assert_contains!(error.to_string(), "injected outer failure");
        }
        drop(stream);
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}
