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

//! Differential execution tests for bounded semi/anti existence summaries.

use std::sync::Arc;
use std::task::Context;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int32Array,
    LargeStringArray, RecordBatch, StringArray, StringViewArray,
    TimestampMicrosecondArray,
};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::{
    DataFusionError, JoinSide, JoinType, NullEquality, Result, ScalarValue,
    assert_contains,
};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{
    BinaryExpr, CaseExpr, Column, IsNullExpr, Literal, NotExpr,
};

use crate::joins::SortMergeJoinExec;
use crate::joins::utils::{ColumnIndex, JoinFilter};
use crate::test::TestMemoryExec;
use crate::test::exec::MockExec;
use crate::{ExecutionPlan, PhysicalExpr, common};

const JOINS: [JoinType; 4] = [
    JoinType::LeftSemi,
    JoinType::LeftAnti,
    JoinType::RightSemi,
    JoinType::RightAnti,
];
type Row = (Option<i32>, Option<i32>, Option<bool>);

fn column(index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(
        ["left_value", "right_value", "left_guard", "right_guard"][index],
        index,
    ))
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
            ColumnIndex {
                index: 2,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("left_value", value_type.clone(), true),
            Field::new("right_value", value_type, true),
            Field::new("left_guard", DataType::Boolean, true),
            Field::new("right_guard", DataType::Boolean, true),
        ])),
    )
}

fn comparison(op: Operator) -> JoinFilter {
    filter(binary(column(0), op, column(1)), DataType::Int32)
}

fn batch(rows: &[Row]) -> Result<RecordBatch> {
    batch_values(
        rows,
        Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.1))),
    )
}

fn batch_values(rows: &[Row], values: ArrayRef) -> Result<RecordBatch> {
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.0))) as ArrayRef,
        ),
        ("value", values),
        (
            "guard",
            Arc::new(BooleanArray::from_iter(rows.iter().map(|r| r.2))),
        ),
        (
            "id",
            Arc::new(Int32Array::from_iter_values(0..rows.len() as i32)),
        ),
    ])
    .map_err(Into::into)
}

fn input(batch: &RecordBatch, chunk: usize) -> Result<Arc<dyn ExecutionPlan>> {
    // Empty batches between slices exercise both empty input and non-zero array offsets.
    let mut batches = vec![batch.slice(0, 0)];
    for offset in (0..batch.num_rows()).step_by(chunk) {
        batches.push(batch.slice(offset, chunk.min(batch.num_rows() - offset)));
        batches.push(batch.slice(offset, 0));
    }
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
    filter: JoinFilter,
    options: SortOptions,
    nulls: NullEquality,
) -> Result<SortMergeJoinExec> {
    SortMergeJoinExec::try_new(
        left,
        right,
        vec![(
            Arc::new(Column::new("key", 0)),
            Arc::new(Column::new("key", 0)),
        )],
        Some(filter),
        join_type,
        vec![options],
        nulls,
    )
}

fn config(batch_size: usize, enabled: bool) -> SessionConfig {
    let mut config = SessionConfig::new().with_batch_size(batch_size);
    config
        .options_mut()
        .execution
        .enable_sort_merge_join_existence_summary = enabled;
    config
}

fn context(batch_size: usize, enabled: bool) -> Arc<TaskContext> {
    Arc::new(TaskContext::default().with_session_config(config(batch_size, enabled)))
}

fn metric(plan: &dyn ExecutionPlan, name: &str) -> usize {
    plan.metrics()
        .unwrap()
        .iter()
        .filter(|metric| metric.value().name() == name)
        .map(|metric| metric.value().as_usize())
        .sum()
}

async fn ids(plan: &dyn ExecutionPlan, ctx: Arc<TaskContext>) -> Result<Vec<i32>> {
    let mut ids = common::collect(plan.execute(0, ctx)?)
        .await?
        .iter()
        .flat_map(|batch| {
            batch
                .column(3)
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

#[tokio::test]
async fn comparisons_match_generic_execution_across_groups_and_orientations() -> Result<()>
{
    let left = vec![
        (None, None, None),
        (None, Some(4), Some(true)),
        (Some(0), Some(7), Some(true)),
        (Some(1), None, None),
        (Some(1), Some(2), Some(true)),
        (Some(1), Some(2), Some(false)),
        (Some(1), Some(6), None),
        (Some(2), Some(3), Some(true)),
        (Some(4), Some(5), Some(false)),
    ];
    let right = vec![
        (None, Some(1), Some(true)),
        (Some(1), None, None),
        (Some(1), Some(2), Some(false)),
        (Some(1), Some(2), Some(true)),
        (Some(1), Some(6), Some(true)),
        (Some(2), None, None),
        (Some(3), Some(4), Some(false)),
    ];
    let mut cases = vec![];
    for op in [
        Operator::NotEq,
        Operator::Lt,
        Operator::LtEq,
        Operator::Gt,
        Operator::GtEq,
    ] {
        for kind in JOINS {
            cases.push((
                op,
                kind,
                SortOptions::default(),
                NullEquality::NullEqualsNothing,
            ));
        }
    }
    // Key ordering and null equality are independent of residual comparison.
    cases.extend([
        (
            Operator::NotEq,
            JoinType::LeftAnti,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            NullEquality::NullEqualsNull,
        ),
        (
            Operator::Lt,
            JoinType::RightSemi,
            SortOptions {
                descending: true,
                nulls_first: true,
            },
            NullEquality::NullEqualsNull,
        ),
        (
            Operator::LtEq,
            JoinType::LeftSemi,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            NullEquality::NullEqualsNothing,
        ),
    ]);
    for (op, kind, options, nulls) in cases {
        let sort = |rows: &[Row]| {
            let mut rows = rows.to_vec();
            rows.sort_by(|a, b| {
                let null_order = if options.nulls_first {
                    b.0.is_none().cmp(&a.0.is_none())
                } else {
                    a.0.is_none().cmp(&b.0.is_none())
                };
                null_order.then_with(|| {
                    if options.descending {
                        b.0.cmp(&a.0)
                    } else {
                        a.0.cmp(&b.0)
                    }
                })
            });
            rows
        };
        let (left, right) = (batch(&sort(&left))?, batch(&sort(&right))?);
        let mut outputs = vec![];
        for enabled in [false, true] {
            let plan = join(
                input(&left, 2)?,
                input(&right, 3)?,
                kind,
                comparison(op),
                options,
                nulls,
            )?;
            outputs.push(ids(&plan, context(2, enabled)).await?);
            assert_eq!(
                metric(&plan, "existence_summary_enabled"),
                usize::from(enabled)
            );
            if enabled {
                assert!(metric(&plan, "existence_summary_inner_rows") > 0);
            } else {
                assert!(plan.metrics().unwrap().iter().all(|metric| {
                    !metric.value().name().starts_with("existence_summary_")
                }));
            }
        }
        assert_eq!(
            outputs[0], outputs[1],
            "{kind:?} {op:?} {options:?} {nulls:?}"
        );
        if kind == JoinType::LeftSemi
            && options == SortOptions::default()
            && nulls == NullEquality::NullEqualsNothing
        {
            // Rows 4/5 equal the minimum; row 6 equals the maximum.
            let expected = match op {
                Operator::Lt => vec![4, 5],
                Operator::Gt => vec![6],
                Operator::NotEq | Operator::LtEq | Operator::GtEq => vec![4, 5, 6],
                _ => unreachable!(),
            };
            assert_eq!(outputs[1], expected, "{op:?}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn guarded_or_preserves_anti_rows_and_requires_an_inner_witness() -> Result<()> {
    let left = vec![
        (Some(0), Some(5), Some(true)),
        (Some(1), Some(2), Some(true)),
        (Some(1), Some(3), Some(false)),
        (Some(1), None, None),
        (Some(2), Some(7), Some(true)),
        (Some(3), None, Some(true)),
    ];
    let right = vec![
        (Some(1), Some(2), Some(false)),
        (Some(1), Some(4), Some(true)),
        (Some(1), None, None),
        (Some(2), Some(7), None),
    ];
    for kind in JOINS {
        for outer_only_or in [false, true] {
            let guarded = binary(
                binary(
                    column(2),
                    Operator::And,
                    binary(column(0), Operator::NotEq, column(1)),
                ),
                Operator::And,
                column(3),
            );
            let expr = if outer_only_or {
                binary(guarded, Operator::Or, Arc::new(IsNullExpr::new(column(0))))
            } else {
                binary(
                    guarded,
                    Operator::Or,
                    binary(column(0), Operator::Lt, column(1)),
                )
            };
            let expected = match (kind, outer_only_or) {
                (JoinType::LeftSemi, false) => vec![1, 2],
                (JoinType::LeftAnti, false) => vec![0, 3, 4, 5],
                (JoinType::RightSemi, false) => vec![1],
                (JoinType::RightAnti, false) => vec![0, 2, 3],
                (JoinType::LeftSemi, true) => vec![1, 3],
                (JoinType::LeftAnti, true) => vec![0, 2, 4, 5],
                (JoinType::RightSemi, true) => vec![0, 1, 2],
                (JoinType::RightAnti, true) => vec![3],
                _ => unreachable!(),
            };
            for enabled in [false, true] {
                let plan = join(
                    input(&batch(&left)?, 1)?,
                    input(&batch(&right)?, 2)?,
                    kind,
                    filter(Arc::clone(&expr), DataType::Int32),
                    SortOptions::default(),
                    NullEquality::NullEqualsNothing,
                )?;
                assert_eq!(
                    ids(&plan, context(1, enabled)).await?,
                    expected,
                    "{kind:?} {outer_only_or} {enabled}"
                );
                assert_eq!(
                    metric(&plan, "existence_summary_enabled"),
                    usize::from(enabled)
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn strings_dates_timestamps_and_decimals_match_generic_execution() -> Result<()> {
    let rows = vec![(Some(1), None, Some(true)); 5];
    let types: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![
            None,
            Some(""),
            Some("é"),
            Some("a"),
            Some("é"),
        ])),
        Arc::new(LargeStringArray::from(vec![
            None,
            Some(""),
            Some("é"),
            Some("a"),
            Some("é"),
        ])),
        Arc::new(StringViewArray::from(vec![
            None,
            Some(""),
            Some("out-of-line-string-z"),
            Some("out-of-line-string-a"),
            Some("out-of-line-string-z"),
        ])),
        Arc::new(Date32Array::from(vec![
            None,
            Some(-1),
            Some(0),
            Some(1),
            Some(1),
        ])),
        Arc::new(
            TimestampMicrosecondArray::from(vec![
                None,
                Some(-100),
                Some(0),
                Some(100),
                Some(100),
            ])
            .with_timezone("UTC"),
        ),
        Arc::new(
            Decimal128Array::from(vec![None, Some(-123), Some(0), Some(456), Some(456)])
                .with_precision_and_scale(20, 2)?,
        ),
    ];
    for values in types {
        let data_type = values.data_type().clone();
        let batch = batch_values(&rows, values)?;
        // NotEq uses scalar equality; ranges use array and scalar ordering.
        for op in [Operator::NotEq, Operator::Lt] {
            let mut outputs = vec![];
            for enabled in [false, true] {
                let plan = join(
                    input(&batch, 2)?,
                    input(&batch, 3)?,
                    JoinType::LeftSemi,
                    filter(binary(column(0), op, column(1)), data_type.clone()),
                    SortOptions::default(),
                    NullEquality::NullEqualsNothing,
                )?;
                outputs.push(ids(&plan, context(2, enabled)).await?);
                assert_eq!(
                    metric(&plan, "existence_summary_enabled"),
                    usize::from(enabled),
                    "{data_type:?} {op:?}"
                );
            }
            assert_eq!(outputs[0], outputs[1], "{data_type:?} {op:?}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn independent_cross_side_witnesses_use_generic_fallback() -> Result<()> {
    let values = batch(&[
        (Some(1), Some(0), Some(true)),
        (Some(1), Some(5), None),
        (Some(1), Some(10), Some(false)),
    ])?;
    // Min/max have separate witnesses for 5, but no row satisfies both clauses.
    let expr = binary(
        binary(column(0), Operator::Lt, column(1)),
        Operator::And,
        binary(column(0), Operator::Gt, column(1)),
    );
    for enabled in [false, true] {
        let plan = join(
            input(&values, 1)?,
            input(&values, 2)?,
            JoinType::LeftSemi,
            filter(Arc::clone(&expr), DataType::Int32),
            SortOptions::default(),
            NullEquality::NullEqualsNothing,
        )?;
        assert!(ids(&plan, context(2, enabled)).await?.is_empty());
        assert_eq!(metric(&plan, "existence_summary_enabled"), 0);
        assert_eq!(
            metric(&plan, "existence_summary_fallback"),
            usize::from(enabled)
        );
    }
    Ok(())
}

#[tokio::test]
async fn normalized_nullable_strings_and_negated_equality_keep_sql_semantics()
-> Result<()> {
    let left = batch_values(
        &[(Some(1), None, Some(true)); 4],
        Arc::new(StringArray::from(vec![
            None,
            Some(""),
            Some("a"),
            Some("out-of-line-value"),
        ])),
    )?;
    let right = batch_values(
        &[(Some(1), None, Some(true)); 2],
        Arc::new(StringArray::from(vec![None, Some("")])),
    )?;
    let normalize = |index| -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(CaseExpr::try_new(
            None,
            vec![(
                Arc::new(IsNullExpr::new(column(index))),
                Arc::new(Literal::new(ScalarValue::Utf8(Some(String::new())))),
            )],
            Some(column(index)),
        )?))
    };
    for kind in JOINS {
        let expected = match kind {
            JoinType::LeftSemi => vec![2, 3],
            JoinType::LeftAnti => vec![0, 1],
            JoinType::RightSemi => vec![0, 1],
            JoinType::RightAnti => vec![],
            _ => unreachable!(),
        };
        for enabled in [false, true] {
            let expr = Arc::new(NotExpr::new(binary(
                normalize(0)?,
                Operator::Eq,
                normalize(1)?,
            )));
            let plan = join(
                input(&left, 1)?,
                input(&right, 1)?,
                kind,
                filter(expr, DataType::Utf8),
                SortOptions::default(),
                NullEquality::NullEqualsNothing,
            )?;
            assert_eq!(
                ids(&plan, context(1, enabled)).await?,
                expected,
                "{kind:?} {enabled}"
            );
            assert_eq!(
                metric(&plan, "existence_summary_enabled"),
                usize::from(enabled)
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn empty_inputs_never_synthesize_a_witness() -> Result<()> {
    let nonempty = batch(&[(Some(1), None, Some(true))])?;
    let empty = nonempty.slice(0, 0);
    for (left, right, kind, expected) in [
        (&nonempty, &empty, JoinType::LeftAnti, vec![0]),
        (&nonempty, &empty, JoinType::LeftSemi, vec![]),
        (&empty, &nonempty, JoinType::RightAnti, vec![0]),
        (&empty, &nonempty, JoinType::LeftSemi, vec![]),
    ] {
        let plan = join(
            input(left, 1)?,
            input(right, 1)?,
            kind,
            // A true preserved-side guard still needs an inner row.
            filter(
                column(if kind == JoinType::RightAnti { 3 } else { 2 }),
                DataType::Int32,
            ),
            SortOptions::default(),
            NullEquality::NullEqualsNothing,
        )?;
        assert_eq!(ids(&plan, context(1, true)).await?, expected, "{kind:?}");
        assert_eq!(metric(&plan, "existence_summary_enabled"), 1);
    }
    Ok(())
}

#[tokio::test]
async fn large_group_summary_memory_is_independent_of_group_cardinality() -> Result<()> {
    let outer = batch(&[(Some(1), Some(7), Some(true)); 4])?;
    let mut sizes = vec![];
    for rows in [4096, 65536] {
        let inner = batch(&vec![(Some(1), Some(7), Some(true)); rows])?;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(128 * 1024, 1.0)
            .build_arc()?;
        let ctx = Arc::new(
            TaskContext::default()
                .with_runtime(Arc::clone(&runtime))
                .with_session_config(config(128, true)),
        );
        let plan = join(
            input(&outer, 2)?,
            input(&inner, 128)?,
            JoinType::LeftAnti,
            comparison(Operator::NotEq),
            SortOptions::default(),
            NullEquality::NullEqualsNothing,
        )?;
        assert_eq!(ids(&plan, ctx).await?, vec![0, 1, 2, 3]);
        assert_eq!(metric(&plan, "existence_summary_groups"), 1);
        assert_eq!(metric(&plan, "existence_summary_inner_rows"), rows);
        assert_eq!(metric(&plan, "existence_summary_probe_rows"), 4);
        let size = metric(&plan, "peak_mem_used");
        assert!(size > 0 && size < 4096, "unexpected summary size {size}");
        sizes.push(size);
        assert_eq!(metric(&plan, "spill_count"), 0);
        assert_eq!(runtime.memory_pool.reserved(), 0);
    }
    assert_eq!(sizes[0], sizes[1], "summary grew with equal-key row count");
    Ok(())
}

#[tokio::test]
async fn saturated_summary_still_propagates_upstream_errors() -> Result<()> {
    let outer = batch(&[(Some(1), Some(7), Some(true))])?;
    let inner = batch(&[
        (Some(1), Some(1), Some(true)),
        (Some(1), Some(2), Some(true)),
    ])?;
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
        comparison(Operator::NotEq),
        SortOptions::default(),
        NullEquality::NullEqualsNothing,
    )?;
    let ctx = context(1, true);
    let pool = Arc::clone(ctx.memory_pool());
    let error = ids(&plan, ctx).await.unwrap_err();
    assert_contains!(error.to_string(), "injected inner failure");
    assert_eq!(pool.reserved(), 0);
    Ok(())
}

#[tokio::test]
async fn summary_work_yields_and_dropping_stream_releases_memory() -> Result<()> {
    let single_row = batch(&[(Some(1), Some(7), Some(true))])?;
    let single_group = batch(&vec![(Some(1), Some(7), Some(true)); 8192])?;
    let many_groups = batch(
        &(0..8192)
            .map(|key| (Some(key), Some(7), Some(true)))
            .collect::<Vec<_>>(),
    )?;
    // Exercise reduction, probing, and work accumulated across small groups.
    // The inputs are immediately ready, so yielding must happen in the join.
    for (outer, inner, chunk) in [
        (&single_row, &single_group, 8192),
        (&single_group, &single_row, 8192),
        (&many_groups, &many_groups, 128),
    ] {
        let plan = join(
            input(outer, chunk)?,
            input(inner, chunk)?,
            JoinType::LeftSemi,
            comparison(Operator::NotEq),
            SortOptions::default(),
            NullEquality::NullEqualsNothing,
        )?;
        let ctx = context(128, true);
        let pool = Arc::clone(ctx.memory_pool());
        let mut stream = plan.execute(0, ctx)?;
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(stream.as_mut().poll_next(&mut cx).is_pending());
        let processed = metric(&plan, "existence_summary_inner_rows")
            + metric(&plan, "existence_summary_probe_rows");
        assert!(processed > 0);
        assert!(processed < outer.num_rows() + inner.num_rows());
        // Dropping a pending stream is how callers cancel execution upstream.
        drop(stream);
        assert_eq!(pool.reserved(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn oversized_string_representative_returns_memory_error_and_releases_pool()
-> Result<()> {
    let rows = vec![(Some(1), None, Some(true))];
    let outer = batch_values(&rows, Arc::new(StringArray::from(vec!["outer"])))?;
    let value = "x".repeat(1024 * 1024);
    let inner = batch_values(&rows, Arc::new(StringArray::from(vec![value.as_str()])))?;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(32 * 1024, 1.0)
        .build_arc()?;
    let ctx = Arc::new(
        TaskContext::default()
            .with_runtime(Arc::clone(&runtime))
            .with_session_config(config(128, true)),
    );
    let plan = join(
        input(&outer, 1)?,
        input(&inner, 1)?,
        JoinType::LeftSemi,
        filter(
            binary(column(0), Operator::NotEq, column(1)),
            DataType::Utf8,
        ),
        SortOptions::default(),
        NullEquality::NullEqualsNothing,
    )?;
    let error = ids(&plan, ctx).await.unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    assert_eq!(runtime.memory_pool.reserved(), 0);
    Ok(())
}
