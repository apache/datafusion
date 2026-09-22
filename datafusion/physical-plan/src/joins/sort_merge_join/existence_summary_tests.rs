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
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array,
    Int32Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
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

fn expected(
    left: &[Row],
    right: &[Row],
    join_type: JoinType,
    nulls: NullEquality,
    predicate: impl Fn(&Row, &Row) -> bool,
) -> Vec<i32> {
    let preserved_left = matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti);
    let semi = matches!(join_type, JoinType::LeftSemi | JoinType::RightSemi);
    let (outer, inner) = if preserved_left {
        (left, right)
    } else {
        (right, left)
    };
    outer
        .iter()
        .enumerate()
        .filter_map(|(id, outer)| {
            let exists = inner.iter().any(|inner| {
                let keys_match = outer.0 == inner.0
                    && (outer.0.is_some() || nulls == NullEquality::NullEqualsNull);
                let (left, right) = if preserved_left {
                    (outer, inner)
                } else {
                    (inner, outer)
                };
                keys_match && predicate(left, right)
            });
            (exists == semi).then_some(id as i32)
        })
        .collect()
}

fn compare(left: Option<i32>, op: Operator, right: Option<i32>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => match op {
            Operator::NotEq => left != right,
            Operator::Lt => left < right,
            Operator::LtEq => left <= right,
            Operator::Gt => left > right,
            Operator::GtEq => left >= right,
            _ => unreachable!(),
        },
        _ => false,
    }
}

#[tokio::test]
async fn atoms_match_scalar_oracle_across_batches_orders_and_null_keys() -> Result<()> {
    let left = vec![
        (None, None, None),
        (None, Some(4), Some(true)),
        (Some(0), Some(7), Some(true)),
        (Some(1), None, None),
        (Some(1), Some(2), Some(true)),
        (Some(1), Some(2), Some(false)),
        (Some(1), Some(8), None),
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
    for descending in [false, true] {
        for nulls_first in [false, true] {
            let options = SortOptions {
                descending,
                nulls_first,
            };
            let sort = |rows: &[Row]| {
                let mut rows = rows.to_vec();
                rows.sort_by(|a, b| match (a.0, b.0) {
                    (None, None) => std::cmp::Ordering::Equal,
                    (None, Some(_)) => {
                        if nulls_first {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Greater
                        }
                    }
                    (Some(_), None) => {
                        if nulls_first {
                            std::cmp::Ordering::Greater
                        } else {
                            std::cmp::Ordering::Less
                        }
                    }
                    (Some(a), Some(b)) => {
                        if descending {
                            b.cmp(&a)
                        } else {
                            a.cmp(&b)
                        }
                    }
                });
                rows
            };
            let (left, right) = (sort(&left), sort(&right));
            for nulls in [
                NullEquality::NullEqualsNothing,
                NullEquality::NullEqualsNull,
            ] {
                for op in [
                    Operator::NotEq,
                    Operator::Lt,
                    Operator::LtEq,
                    Operator::Gt,
                    Operator::GtEq,
                ] {
                    for kind in JOINS {
                        let expected = expected(&left, &right, kind, nulls, |l, r| {
                            compare(l.1, op, r.1)
                        });
                        for enabled in [false, true] {
                            let plan = join(
                                input(&batch(&left)?, 2)?,
                                input(&batch(&right)?, 3)?,
                                kind,
                                comparison(op),
                                options,
                                nulls,
                            )?;
                            assert_eq!(
                                ids(&plan, context(2, enabled)).await?,
                                expected,
                                "{kind:?} {op:?} enabled={enabled} {options:?} {nulls:?}"
                            );
                            assert_eq!(
                                metric(&plan, "existence_summary_enabled"),
                                usize::from(enabled)
                            );
                            if enabled {
                                assert!(
                                    metric(&plan, "existence_summary_inner_rows") > 0
                                );
                            } else {
                                assert!(plan.metrics().unwrap().iter().all(|metric| {
                                    !metric
                                        .value()
                                        .name()
                                        .starts_with("existence_summary_")
                                }));
                            }
                        }
                    }
                }
            }
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
        (Some(3), Some(4), Some(true)),
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
            let expected = expected(
                &left,
                &right,
                kind,
                NullEquality::NullEqualsNothing,
                |l, r| {
                    let guarded = l.2 == Some(true)
                        && r.2 == Some(true)
                        && compare(l.1, Operator::NotEq, r.1);
                    guarded
                        || if outer_only_or {
                            l.1.is_none()
                        } else {
                            compare(l.1, Operator::Lt, r.1)
                        }
                },
            );
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
        for op in [
            Operator::NotEq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
            Operator::GtEq,
        ] {
            for kind in JOINS {
                let mut outputs = vec![];
                for enabled in [false, true] {
                    let plan = join(
                        input(&batch, 2)?,
                        input(&batch, 3)?,
                        kind,
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
                assert_eq!(outputs[0], outputs[1], "{data_type:?} {op:?} {kind:?}");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn unsupported_pair_conjunction_and_float_use_generic_fallback() -> Result<()> {
    let rows = vec![
        (Some(1), Some(0), Some(true)),
        (Some(1), Some(5), None),
        (Some(1), Some(10), Some(false)),
    ];
    let integers = batch(&rows)?;
    // Independent min/max witnesses would incorrectly match the interval's gap.
    let interval = binary(
        binary(column(0), Operator::Lt, column(1)),
        Operator::And,
        binary(
            binary(
                column(0),
                Operator::Plus,
                Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
            ),
            Operator::Gt,
            column(1),
        ),
    );
    let incompatible_witnesses = binary(
        binary(column(0), Operator::Lt, column(1)),
        Operator::And,
        binary(column(0), Operator::Gt, column(1)),
    );
    let floats = batch_values(
        &rows,
        Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(-0.0),
            Some(0.0),
        ])),
    )?;
    for (batch, expr) in [
        (&integers, interval),
        (&integers, incompatible_witnesses),
        (&floats, binary(column(0), Operator::NotEq, column(1))),
    ] {
        for kind in JOINS {
            let mut outputs = vec![];
            for enabled in [false, true] {
                let plan = join(
                    input(batch, 1)?,
                    input(batch, 2)?,
                    kind,
                    filter(Arc::clone(&expr), batch.column(1).data_type().clone()),
                    SortOptions::default(),
                    NullEquality::NullEqualsNothing,
                )?;
                outputs.push(ids(&plan, context(2, enabled)).await?);
                assert_eq!(metric(&plan, "existence_summary_enabled"), 0);
                assert_eq!(
                    metric(&plan, "existence_summary_fallback"),
                    usize::from(enabled)
                );
            }
            assert_eq!(outputs[0], outputs[1]);
        }
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
async fn q21_semi_then_anti_use_separate_qualified_groups() -> Result<()> {
    // Candidate supplier 1: another supplier exists, but none of the other
    // suppliers is late. Supplier 2 is rejected by the anti join's late witness.
    let outer = batch(&[
        (Some(1), Some(1), Some(true)),
        (Some(1), Some(2), Some(true)),
        (Some(2), Some(1), Some(true)),
        (Some(3), Some(1), Some(true)),
    ])?;
    let all = batch(&[
        (Some(1), Some(1), Some(true)),
        (Some(1), Some(2), Some(false)),
        (Some(2), Some(1), Some(true)),
        (Some(3), Some(2), Some(true)),
    ])?;
    for enabled in [false, true] {
        let semi = Arc::new(join(
            input(&outer, 1)?,
            input(&all, 2)?,
            JoinType::LeftSemi,
            comparison(Operator::NotEq),
            SortOptions::default(),
            NullEquality::NullEqualsNothing,
        )?);
        let anti = join(
            semi,
            input(&all, 1)?,
            JoinType::LeftAnti,
            filter(
                binary(
                    binary(column(0), Operator::NotEq, column(1)),
                    Operator::And,
                    column(3),
                ),
                DataType::Int32,
            ),
            SortOptions::default(),
            NullEquality::NullEqualsNothing,
        )?;
        assert_eq!(ids(&anti, context(1, enabled)).await?, vec![0]);
    }
    Ok(())
}

#[tokio::test]
async fn empty_inputs_never_synthesize_a_witness() -> Result<()> {
    let nonempty = batch(&[(Some(1), None, Some(true))])?;
    let empty = nonempty.slice(0, 0);
    for kind in JOINS {
        for (left, right) in [(&empty, &nonempty), (&nonempty, &empty), (&empty, &empty)]
        {
            let mut outputs = vec![];
            for enabled in [false, true] {
                let plan = join(
                    input(left, 1)?,
                    input(right, 1)?,
                    kind,
                    filter(
                        binary(
                            column(2),
                            Operator::Or,
                            binary(column(0), Operator::NotEq, column(1)),
                        ),
                        DataType::Int32,
                    ),
                    SortOptions::default(),
                    NullEquality::NullEqualsNothing,
                )?;
                outputs.push(ids(&plan, context(1, enabled)).await?);
            }
            assert_eq!(outputs[0], outputs[1]);
        }
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
        let size = metric(&plan, "existence_summary_state_bytes");
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
    for (outer, inner) in [
        (&single_row, &single_group),
        (&single_group, &single_row),
        (&many_groups, &many_groups),
    ] {
        // Check both groups spanning many batches and oversized batches.
        for chunk in [128, 8192] {
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
