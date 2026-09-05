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

//! SortMergeJoin Testing Module
//!
//! This module currently contains the following test types in this order:
//!  - Join behaviour (left, right, full, inner, semi, anti, mark)
//!  - Batch spilling
//!  - Filter mask
//!
//! Add relevant tests under the specified sections.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use super::bitwise_stream::BitwiseSortMergeJoinStream;
use crate::joins::utils::{ColumnIndex, JoinFilter, JoinOn};
use crate::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use crate::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use crate::projection::{ProjectionExec, ProjectionExpr};
use crate::spill::spill_manager::SpillManager;
use crate::test::TestMemoryExec;
use crate::test::exec::BarrierExec;
use crate::test::{build_table_i32, build_table_i32_two_cols};
use crate::{ExecutionPlan, RecordBatchStream, common};
use crate::{
    expressions::Column, joins::sort_merge_join::filter::get_corrected_filter_mask,
    joins::sort_merge_join::materializing_stream::JoinedRecordBatches,
};
use arrow::array::{
    BinaryArray, BooleanArray, Date32Array, Date64Array, FixedSizeBinaryArray,
    Int32Array, RecordBatch, UInt64Array,
};
use arrow::compute::{BatchCoalescer, SortOptions, filter_record_batch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_ord::sort::SortColumn;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use datafusion_common::JoinType::*;
use datafusion_common::instant::Instant;
use datafusion_common::{
    JoinSide, internal_err,
    test_util::{batches_to_sort_string, batches_to_string},
};
use datafusion_common::{
    JoinType, NullEquality, Result, ScalarValue, assert_batches_eq, assert_contains,
};
use datafusion_common_runtime::JoinSet;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::disk_manager::{
    DiskManager, DiskManagerBuilder, DiskManagerMode,
};
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_execution::spill_file::{SpillFile, SpillWriter, TempFileFactory};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::BinaryExpr;
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_expr_common::physical_expr::PhysicalExprRef;
use futures::{Stream, StreamExt};
use insta::assert_snapshot;
use itertools::Itertools;
use std::collections::VecDeque;

fn build_table(
    a: (&str, &Vec<i32>),
    b: (&str, &Vec<i32>),
    c: (&str, &Vec<i32>),
) -> Arc<dyn ExecutionPlan> {
    let batch = build_table_i32(a, b, c);
    let schema = batch.schema();
    TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
}

fn build_table_from_batches(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    let schema = batches.first().unwrap().schema();
    TestMemoryExec::try_new_exec(&[batches], schema, None).unwrap()
}

fn build_date_table(
    a: (&str, &Vec<i32>),
    b: (&str, &Vec<i32>),
    c: (&str, &Vec<i32>),
) -> Arc<dyn ExecutionPlan> {
    let schema = Schema::new(vec![
        Field::new(a.0, DataType::Date32, false),
        Field::new(b.0, DataType::Date32, false),
        Field::new(c.0, DataType::Date32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Date32Array::from(a.1.clone())),
            Arc::new(Date32Array::from(b.1.clone())),
            Arc::new(Date32Array::from(c.1.clone())),
        ],
    )
    .unwrap();

    let schema = batch.schema();
    TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
}

fn build_date64_table(
    a: (&str, &Vec<i64>),
    b: (&str, &Vec<i64>),
    c: (&str, &Vec<i64>),
) -> Arc<dyn ExecutionPlan> {
    let schema = Schema::new(vec![
        Field::new(a.0, DataType::Date64, false),
        Field::new(b.0, DataType::Date64, false),
        Field::new(c.0, DataType::Date64, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Date64Array::from(a.1.clone())),
            Arc::new(Date64Array::from(b.1.clone())),
            Arc::new(Date64Array::from(c.1.clone())),
        ],
    )
    .unwrap();

    let schema = batch.schema();
    TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
}

fn build_binary_table(
    a: (&str, &Vec<&[u8]>),
    b: (&str, &Vec<i32>),
    c: (&str, &Vec<i32>),
) -> Arc<dyn ExecutionPlan> {
    let schema = Schema::new(vec![
        Field::new(a.0, DataType::Binary, false),
        Field::new(b.0, DataType::Int32, false),
        Field::new(c.0, DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(BinaryArray::from(a.1.clone())),
            Arc::new(Int32Array::from(b.1.clone())),
            Arc::new(Int32Array::from(c.1.clone())),
        ],
    )
    .unwrap();

    let schema = batch.schema();
    TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
}

fn build_fixed_size_binary_table(
    a: (&str, &Vec<&[u8]>),
    b: (&str, &Vec<i32>),
    c: (&str, &Vec<i32>),
) -> Arc<dyn ExecutionPlan> {
    let schema = Schema::new(vec![
        Field::new(a.0, DataType::FixedSizeBinary(3), false),
        Field::new(b.0, DataType::Int32, false),
        Field::new(c.0, DataType::Int32, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(a.1.iter().copied()).unwrap()),
            Arc::new(Int32Array::from(b.1.clone())),
            Arc::new(Int32Array::from(c.1.clone())),
        ],
    )
    .unwrap();

    let schema = batch.schema();
    TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
}

/// returns a table with 3 columns of i32 in memory
pub fn build_table_i32_nullable(
    a: (&str, &Vec<Option<i32>>),
    b: (&str, &Vec<Option<i32>>),
    c: (&str, &Vec<Option<i32>>),
) -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(a.0, DataType::Int32, true),
        Field::new(b.0, DataType::Int32, true),
        Field::new(c.0, DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(a.1.clone())),
            Arc::new(Int32Array::from(b.1.clone())),
            Arc::new(Int32Array::from(c.1.clone())),
        ],
    )
    .unwrap();
    TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
}

pub fn build_table_two_cols(
    a: (&str, &Vec<i32>),
    b: (&str, &Vec<i32>),
) -> Arc<dyn ExecutionPlan> {
    let batch = build_table_i32_two_cols(a, b);
    let schema = batch.schema();
    TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
}

fn join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    join_type: JoinType,
) -> Result<SortMergeJoinExec> {
    let sort_options = vec![SortOptions::default(); on.len()];
    SortMergeJoinExec::try_new(
        left,
        right,
        on,
        None,
        join_type,
        sort_options,
        NullEquality::NullEqualsNothing,
    )
}

fn join_with_options(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    join_type: JoinType,
    sort_options: Vec<SortOptions>,
    null_equality: NullEquality,
) -> Result<SortMergeJoinExec> {
    SortMergeJoinExec::try_new(
        left,
        right,
        on,
        None,
        join_type,
        sort_options,
        null_equality,
    )
}

fn join_with_filter(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    filter: JoinFilter,
    join_type: JoinType,
    sort_options: Vec<SortOptions>,
    null_equality: NullEquality,
) -> Result<SortMergeJoinExec> {
    SortMergeJoinExec::try_new(
        left,
        right,
        on,
        Some(filter),
        join_type,
        sort_options,
        null_equality,
    )
}

async fn join_collect(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    join_type: JoinType,
) -> Result<(Vec<String>, Vec<RecordBatch>)> {
    let sort_options = vec![SortOptions::default(); on.len()];
    join_collect_with_options(
        left,
        right,
        on,
        join_type,
        sort_options,
        NullEquality::NullEqualsNothing,
    )
    .await
}

async fn join_collect_with_filter(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    filter: JoinFilter,
    join_type: JoinType,
) -> Result<(Vec<String>, Vec<RecordBatch>)> {
    let sort_options = vec![SortOptions::default(); on.len()];

    let task_ctx = Arc::new(TaskContext::default());
    let join = join_with_filter(
        left,
        right,
        on,
        filter,
        join_type,
        sort_options,
        NullEquality::NullEqualsNothing,
    )?;
    let columns = columns(&join.schema());

    let stream = join.execute(0, task_ctx)?;
    let batches = common::collect(stream).await?;
    Ok((columns, batches))
}

async fn join_collect_with_options(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    join_type: JoinType,
    sort_options: Vec<SortOptions>,
    null_equality: NullEquality,
) -> Result<(Vec<String>, Vec<RecordBatch>)> {
    let task_ctx = Arc::new(TaskContext::default());
    let join =
        join_with_options(left, right, on, join_type, sort_options, null_equality)?;
    let columns = columns(&join.schema());

    let stream = join.execute(0, task_ctx)?;
    let batches = common::collect(stream).await?;
    Ok((columns, batches))
}

async fn join_collect_batch_size_equals_two(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    join_type: JoinType,
) -> Result<(Vec<String>, Vec<RecordBatch>)> {
    let task_ctx = TaskContext::default()
        .with_session_config(SessionConfig::new().with_batch_size(2));
    let task_ctx = Arc::new(task_ctx);
    let join = join(left, right, on, join_type)?;
    let columns = columns(&join.schema());

    let stream = join.execute(0, task_ctx)?;
    let batches = common::collect(stream).await?;
    Ok((columns, batches))
}

fn join_and_projection_for_pushdown(
    filter: Option<JoinFilter>,
) -> Result<(Arc<SortMergeJoinExec>, ProjectionExec)> {
    let left = build_table(
        ("a1", &vec![1, 2, 3]),
        ("b1", &vec![4, 5, 5]),
        ("c1", &vec![7, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30]),
        ("b2", &vec![4, 5, 5]),
        ("c2", &vec![70, 8, 90]),
    );
    let on = vec![(
        Arc::new(Column::new("b1", 1)) as _,
        Arc::new(Column::new("b2", 1)) as _,
    )];
    let join = Arc::new(SortMergeJoinExec::try_new(
        left,
        right,
        on,
        filter,
        Inner,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
    )?);
    let input: Arc<dyn ExecutionPlan> = Arc::clone(&join) as _;
    let projection = ProjectionExec::try_new(
        [
            ProjectionExpr {
                expr: Arc::new(Column::new("c1", 2)),
                alias: "c1".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b1", 1)),
                alias: "b1".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("c2", 5)),
                alias: "c2".to_string(),
            },
            ProjectionExpr {
                expr: Arc::new(Column::new("b2", 4)),
                alias: "b2".to_string(),
            },
        ],
        input,
    )?;

    Ok((join, projection))
}

/// `c1 < c2`, referencing the last column of each child.
fn filter_for_pushdown() -> JoinFilter {
    JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c1", 0)),
            Operator::Lt,
            Arc::new(Column::new("c2", 1)),
        )),
        vec![
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
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ])),
    )
}

#[test]
fn projection_pushdown_remaps_filter() -> Result<()> {
    let (join, projection) =
        join_and_projection_for_pushdown(Some(filter_for_pushdown()))?;

    let swapped = join
        .try_swapping_with_projection(&projection)?
        .expect("projection should be pushed below the join");
    let swapped = swapped
        .downcast_ref::<SortMergeJoinExec>()
        .expect("swapped plan should be a SortMergeJoinExec");

    let (left_on, right_on) = &swapped.on()[0];
    assert_eq!(left_on.downcast_ref::<Column>().unwrap().index(), 1);
    assert_eq!(right_on.downcast_ref::<Column>().unwrap().index(), 1);
    assert_eq!(
        swapped.filter().as_ref().unwrap().column_indices(),
        &[
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ]
    );

    Ok(())
}

#[tokio::test]
async fn projection_pushdown_remaps_filter_execute() -> Result<()> {
    let (join, projection) =
        join_and_projection_for_pushdown(Some(filter_for_pushdown()))?;
    let expected_schema = projection.schema();

    let swapped = join
        .try_swapping_with_projection(&projection)?
        .expect("projection should be pushed below the join");
    assert!(
        swapped.downcast_ref::<SortMergeJoinExec>().is_some(),
        "projection should not be embedded in the join"
    );
    assert_eq!(swapped.schema(), expected_schema);

    // The pushed-down children keep only `c*` and `b*`, so the filter must run
    // against the remapped indices: `c1 < c2` filters out the (8, 8) and (9, 8)
    // pairs of the `b1 = b2 = 5` group.
    let batches =
        common::collect(swapped.execute(0, Arc::new(TaskContext::default()))?).await?;

    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+
    | c1 | b1 | c2 | b2 |
    +----+----+----+----+
    | 7  | 4  | 70 | 4  |
    | 8  | 5  | 90 | 5  |
    | 9  | 5  | 90 | 5  |
    +----+----+----+----+
    ");

    Ok(())
}

#[test]
fn projection_pushdown_without_filter() -> Result<()> {
    let (join, projection) = join_and_projection_for_pushdown(None)?;

    let swapped = join
        .try_swapping_with_projection(&projection)?
        .expect("projection should be pushed below the join");
    let swapped = swapped
        .downcast_ref::<SortMergeJoinExec>()
        .expect("swapped plan should be a SortMergeJoinExec");

    assert!(swapped.filter().is_none());

    Ok(())
}

#[tokio::test]
async fn join_inner_one() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 3]),
        ("b1", &vec![4, 5, 5]), // this has a repetition
        ("c1", &vec![7, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![4, 5, 6]),
        ("c2", &vec![70, 80, 90]),
    );

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Inner).await?;

    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b1 | c1 | a2 | b1 | c2 |
    +----+----+----+----+----+----+
    | 1  | 4  | 7  | 10 | 4  | 70 |
    | 2  | 5  | 8  | 20 | 5  | 80 |
    | 3  | 5  | 9  | 20 | 5  | 80 |
    +----+----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_inner_two() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 2]),
        ("b2", &vec![1, 2, 2]),
        ("c1", &vec![7, 8, 9]),
    );
    let right = build_table(
        ("a1", &vec![1, 2, 3]),
        ("b2", &vec![1, 2, 2]),
        ("c2", &vec![70, 80, 90]),
    );
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        ),
    ];

    let (_columns, batches) = join_collect(left, right, on, Inner).await?;

    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b2 | c1 | a1 | b2 | c2 |
    +----+----+----+----+----+----+
    | 1  | 1  | 7  | 1  | 1  | 70 |
    | 2  | 2  | 8  | 2  | 2  | 80 |
    | 2  | 2  | 9  | 2  | 2  | 80 |
    +----+----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_inner_two_two() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 1, 2]),
        ("b2", &vec![1, 1, 2]),
        ("c1", &vec![7, 8, 9]),
    );
    let right = build_table(
        ("a1", &vec![1, 1, 3]),
        ("b2", &vec![1, 1, 2]),
        ("c2", &vec![70, 80, 90]),
    );
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        ),
    ];

    let (_columns, batches) = join_collect(left, right, on, Inner).await?;

    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b2 | c1 | a1 | b2 | c2 |
    +----+----+----+----+----+----+
    | 1  | 1  | 7  | 1  | 1  | 70 |
    | 1  | 1  | 7  | 1  | 1  | 80 |
    | 1  | 1  | 8  | 1  | 1  | 70 |
    | 1  | 1  | 8  | 1  | 1  | 80 |
    +----+----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_inner_with_nulls() -> Result<()> {
    let left = build_table_i32_nullable(
        ("a1", &vec![Some(1), Some(1), Some(2), Some(2)]),
        ("b2", &vec![None, Some(1), Some(2), Some(2)]), // null in key field
        ("c1", &vec![Some(1), None, Some(8), Some(9)]), // null in non-key field
    );
    let right = build_table_i32_nullable(
        ("a1", &vec![Some(1), Some(1), Some(2), Some(3)]),
        ("b2", &vec![None, Some(1), Some(2), Some(2)]),
        ("c2", &vec![Some(10), Some(70), Some(80), Some(90)]),
    );
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        ),
    ];

    let (_, batches) = join_collect(left, right, on, Inner).await?;
    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b2 | c1 | a1 | b2 | c2 |
    +----+----+----+----+----+----+
    | 1  | 1  |    | 1  | 1  | 70 |
    | 2  | 2  | 8  | 2  | 2  | 80 |
    | 2  | 2  | 9  | 2  | 2  | 80 |
    +----+----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_inner_with_nulls_with_options() -> Result<()> {
    let left = build_table_i32_nullable(
        ("a1", &vec![Some(2), Some(2), Some(1), Some(1)]),
        ("b2", &vec![Some(2), Some(2), Some(1), None]), // null in key field
        ("c1", &vec![Some(9), Some(8), None, Some(1)]), // null in non-key field
    );
    let right = build_table_i32_nullable(
        ("a1", &vec![Some(3), Some(2), Some(1), Some(1)]),
        ("b2", &vec![Some(2), Some(2), Some(1), None]),
        ("c2", &vec![Some(90), Some(80), Some(70), Some(10)]),
    );
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        ),
    ];
    let (_, batches) = join_collect_with_options(
        left,
        right,
        on,
        Inner,
        vec![
            SortOptions {
                descending: true,
                nulls_first: false,
            };
            2
        ],
        NullEquality::NullEqualsNull,
    )
    .await?;
    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b2 | c1 | a1 | b2 | c2 |
    +----+----+----+----+----+----+
    | 2  | 2  | 9  | 2  | 2  | 80 |
    | 2  | 2  | 8  | 2  | 2  | 80 |
    | 1  | 1  |    | 1  | 1  | 70 |
    | 1  |    | 1  | 1  |    | 10 |
    +----+----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_inner_output_two_batches() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 2]),
        ("b2", &vec![1, 2, 2]),
        ("c1", &vec![7, 8, 9]),
    );
    let right = build_table(
        ("a1", &vec![1, 2, 3]),
        ("b2", &vec![1, 2, 2]),
        ("c2", &vec![70, 80, 90]),
    );
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b2", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        ),
    ];

    let (_, batches) = join_collect_batch_size_equals_two(left, right, on, Inner).await?;
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(batches[1].num_rows(), 1);
    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b2 | c1 | a1 | b2 | c2 |
    +----+----+----+----+----+----+
    | 1  | 1  | 7  | 1  | 1  | 70 |
    | 2  | 2  | 8  | 2  | 2  | 80 |
    | 2  | 2  | 9  | 2  | 2  | 80 |
    +----+----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_left_one() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 3]),
        ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
        ("c1", &vec![7, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![4, 5, 6]),
        ("c2", &vec![70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Left).await?;
    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b1 | c1 | a2 | b1 | c2 |
    +----+----+----+----+----+----+
    | 1  | 4  | 7  | 10 | 4  | 70 |
    | 2  | 5  | 8  | 20 | 5  | 80 |
    | 3  | 7  | 9  |    |    |    |
    +----+----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_right_one() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 3]),
        ("b1", &vec![4, 5, 7]),
        ("c1", &vec![7, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![4, 5, 6]), // 6 does not exist on the left
        ("c2", &vec![70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Right).await?;
    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b1 | c1 | a2 | b1 | c2 |
    +----+----+----+----+----+----+
    | 1  | 4  | 7  | 10 | 4  | 70 |
    | 2  | 5  | 8  | 20 | 5  | 80 |
    |    |    |    | 30 | 6  | 90 |
    +----+----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_right_different_columns_count_with_filter() -> Result<()> {
    // select *
    // from t1
    // right join t2 on t1.b1 = t2.b1 and t1.a1 > t2.a2

    let left = build_table(
        ("a1", &vec![1, 21, 3]), // 21(t1.a1) > 20(t2.a2)
        ("b1", &vec![4, 5, 7]),
        ("c1", &vec![7, 8, 9]),
    );

    let right = build_table_two_cols(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![4, 5, 6]), // 6 does not exist on the left
    );

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a1", 0)),
            Operator::Gt,
            Arc::new(Column::new("a2", 1)),
        )),
        vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("a1", DataType::Int32, true),
            Field::new("a2", DataType::Int32, true),
        ])),
    );

    let (_, batches) = join_collect_with_filter(left, right, on, filter, Right).await?;

    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+----+
    | a1 | b1 | c1 | a2 | b1 |
    +----+----+----+----+----+
    |    |    |    | 10 | 4  |
    | 21 | 5  | 8  | 20 | 5  |
    |    |    |    | 30 | 6  |
    +----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_left_different_columns_count_with_filter() -> Result<()> {
    // select *
    // from t2
    // left join t1 on t2.b1 = t1.b1 and t2.a2 > t1.a1

    let left = build_table_two_cols(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![4, 5, 6]), // 6 does not exist on the right
    );

    let right = build_table(
        ("a1", &vec![1, 21, 3]), // 20(t2.a2) > 1(t1.a1)
        ("b1", &vec![4, 5, 7]),
        ("c1", &vec![7, 8, 9]),
    );

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a2", 0)),
            Operator::Gt,
            Arc::new(Column::new("a1", 1)),
        )),
        vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("a2", DataType::Int32, true),
            Field::new("a1", DataType::Int32, true),
        ])),
    );

    let (_, batches) = join_collect_with_filter(left, right, on, filter, Left).await?;

    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+----+
    | a2 | b1 | a1 | b1 | c1 |
    +----+----+----+----+----+
    | 10 | 4  | 1  | 4  | 7  |
    | 20 | 5  |    |    |    |
    | 30 | 6  |    |    |    |
    +----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_left_mark_different_columns_count_with_filter() -> Result<()> {
    // select *
    // from t2
    // left mark join t1 on t2.b1 = t1.b1 and t2.a2 > t1.a1

    let left = build_table_two_cols(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![4, 5, 6]), // 6 does not exist on the right
    );

    let right = build_table(
        ("a1", &vec![1, 21, 3]), // 20(t2.a2) > 1(t1.a1)
        ("b1", &vec![4, 5, 7]),
        ("c1", &vec![7, 8, 9]),
    );

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a2", 0)),
            Operator::Gt,
            Arc::new(Column::new("a1", 1)),
        )),
        vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("a2", DataType::Int32, true),
            Field::new("a1", DataType::Int32, true),
        ])),
    );

    let (_, batches) =
        join_collect_with_filter(left, right, on, filter, LeftMark).await?;

    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+-------+
    | a2 | b1 | mark  |
    +----+----+-------+
    | 10 | 4  | true  |
    | 20 | 5  | false |
    | 30 | 6  | false |
    +----+----+-------+
    ");
    Ok(())
}

#[tokio::test]
async fn join_right_mark_different_columns_count_with_filter() -> Result<()> {
    // select *
    // from t1
    // right mark join t2 on t1.b1 = t2.b1 and t1.a1 > t2.a2

    let left = build_table(
        ("a1", &vec![1, 21, 3]), // 21(t1.a1) > 20(t2.a2)
        ("b1", &vec![4, 5, 7]),
        ("c1", &vec![7, 8, 9]),
    );

    let right = build_table_two_cols(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![4, 5, 6]), // 6 does not exist on the left
    );

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a1", 0)),
            Operator::Gt,
            Arc::new(Column::new("a2", 1)),
        )),
        vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("a1", DataType::Int32, true),
            Field::new("a2", DataType::Int32, true),
        ])),
    );

    let (_, batches) =
        join_collect_with_filter(left, right, on, filter, RightMark).await?;

    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+-------+
    | a2 | b1 | mark  |
    +----+----+-------+
    | 10 | 4  | false |
    | 20 | 5  | true  |
    | 30 | 6  | false |
    +----+----+-------+
    ");
    Ok(())
}

#[tokio::test]
async fn join_full_one() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 3]),
        ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
        ("c1", &vec![7, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30]),
        ("b2", &vec![4, 5, 6]),
        ("c2", &vec![70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Full).await?;
    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b1 | c1 | a2 | b2 | c2 |
    +----+----+----+----+----+----+
    |    |    |    | 30 | 6  | 90 |
    | 1  | 4  | 7  | 10 | 4  | 70 |
    | 2  | 5  | 8  | 20 | 5  | 80 |
    | 3  | 7  | 9  |    |    |    |
    +----+----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_left_anti() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 2, 3, 5]),
        ("b1", &vec![4, 5, 5, 7, 7]), // 7 does not exist on the right
        ("c1", &vec![7, 8, 8, 9, 11]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![4, 5, 6]),
        ("c2", &vec![70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, LeftAnti).await?;

    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+
    | a1 | b1 | c1 |
    +----+----+----+
    | 3  | 7  | 9  |
    | 5  | 7  | 11 |
    +----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_right_anti_one_one() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 2]),
        ("b1", &vec![4, 5, 5]),
        ("c1", &vec![7, 8, 8]),
    );
    let right = build_table_two_cols(("a2", &vec![10, 20, 30]), ("b1", &vec![4, 5, 6]));
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, RightAnti).await?;
    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+
    | a2 | b1 |
    +----+----+
    | 30 | 6  |
    +----+----+
    ");

    let left2 = build_table(
        ("a1", &vec![1, 2, 2]),
        ("b1", &vec![4, 5, 5]),
        ("c1", &vec![7, 8, 8]),
    );
    let right2 = build_table(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![4, 5, 6]),
        ("c2", &vec![70, 80, 90]),
    );

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left2.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right2.schema())?) as _,
    )];

    let (_, batches2) = join_collect(left2, right2, on, RightAnti).await?;
    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches2), @r"
    +----+----+----+
    | a2 | b1 | c2 |
    +----+----+----+
    | 30 | 6  | 90 |
    +----+----+----+
    ");

    Ok(())
}

#[tokio::test]
async fn join_right_anti_two_two() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 2]),
        ("b1", &vec![4, 5, 5]),
        ("c1", &vec![7, 8, 8]),
    );
    let right = build_table_two_cols(("a2", &vec![10, 20, 30]), ("b1", &vec![4, 5, 6]));
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a2", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        ),
    ];

    let (_, batches) = join_collect(left, right, on, RightAnti).await?;
    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+
    | a2 | b1 |
    +----+----+
    | 10 | 4  |
    | 20 | 5  |
    | 30 | 6  |
    +----+----+
    ");

    let left = build_table(
        ("a1", &vec![1, 2, 2]),
        ("b1", &vec![4, 5, 5]),
        ("c1", &vec![7, 8, 8]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![4, 5, 6]),
        ("c2", &vec![70, 80, 90]),
    );

    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a2", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        ),
    ];

    let (_, batches) = join_collect(left, right, on, RightAnti).await?;
    let expected = [
        "+----+----+----+",
        "| a2 | b1 | c2 |",
        "+----+----+----+",
        "| 10 | 4  | 70 |",
        "| 20 | 5  | 80 |",
        "| 30 | 6  | 90 |",
        "+----+----+----+",
    ];
    // The output order is important as SMJ preserves sortedness
    assert_batches_eq!(expected, &batches);

    Ok(())
}

#[tokio::test]
async fn join_right_anti_two_with_filter() -> Result<()> {
    let left = build_table(("a1", &vec![1]), ("b1", &vec![10]), ("c1", &vec![30]));
    let right = build_table(("a1", &vec![1]), ("b1", &vec![10]), ("c2", &vec![20]));
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        ),
    ];
    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c2", 1)),
            Operator::Gt,
            Arc::new(Column::new("c1", 0)),
        )),
        vec![
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
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Int32, true),
        ])),
    );
    let (_, batches) =
        join_collect_with_filter(left, right, on, filter, RightAnti).await?;
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+
    | a1 | b1 | c2 |
    +----+----+----+
    | 1  | 10 | 20 |
    +----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_right_anti_filtered_with_mismatched_columns() -> Result<()> {
    let left = build_table_two_cols(("a1", &vec![31, 31]), ("b1", &vec![32, 33]));
    let right = build_table(
        ("a2", &vec![31, 31]),
        ("b2", &vec![32, 35]),
        ("c2", &vec![108, 109]),
    );
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a2", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        ),
    ];

    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("b1", 0)),
            Operator::LtEq,
            Arc::new(Column::new("c2", 1)),
        )),
        vec![
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("b1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ])),
    );

    let (_, batches) =
        join_collect_with_filter(left, right, on, filter, RightAnti).await?;

    let expected = [
        "+----+----+-----+",
        "| a2 | b2 | c2  |",
        "+----+----+-----+",
        "| 31 | 35 | 109 |",
        "+----+----+-----+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
async fn join_right_anti_with_nulls() -> Result<()> {
    let left = build_table_i32_nullable(
        ("a1", &vec![Some(0), Some(1), Some(2), Some(2), Some(3)]),
        ("b1", &vec![Some(3), Some(4), Some(5), None, Some(6)]),
        ("c2", &vec![Some(60), None, Some(80), Some(85), Some(90)]),
    );
    let right = build_table_i32_nullable(
        ("a1", &vec![Some(1), Some(2), Some(2), Some(3)]),
        ("b1", &vec![Some(4), Some(5), None, Some(6)]), // null in key field
        ("c2", &vec![Some(7), Some(8), Some(8), None]), // null in non-key field
    );
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        ),
    ];

    let (_, batches) = join_collect(left, right, on, RightAnti).await?;
    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+
    | a1 | b1 | c2 |
    +----+----+----+
    | 2  |    | 8  |
    +----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_right_anti_with_nulls_with_options() -> Result<()> {
    let left = build_table_i32_nullable(
        ("a1", &vec![Some(1), Some(2), Some(1), Some(0), Some(2)]),
        ("b1", &vec![Some(4), Some(5), Some(5), None, Some(5)]),
        ("c1", &vec![Some(7), Some(8), Some(8), Some(60), None]),
    );
    let right = build_table_i32_nullable(
        ("a1", &vec![Some(3), Some(2), Some(2), Some(1)]),
        ("b1", &vec![None, Some(5), Some(5), Some(4)]), // null in key field
        ("c2", &vec![Some(9), None, Some(8), Some(7)]), // null in non-key field
    );
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        ),
    ];

    let (_, batches) = join_collect_with_options(
        left,
        right,
        on,
        RightAnti,
        vec![
            SortOptions {
                descending: true,
                nulls_first: false,
            };
            2
        ],
        NullEquality::NullEqualsNull,
    )
    .await?;

    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+
    | a1 | b1 | c2 |
    +----+----+----+
    | 3  |    | 9  |
    | 2  | 5  |    |
    | 2  | 5  | 8  |
    +----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_right_anti_output_two_batches() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 2]),
        ("b1", &vec![4, 5, 5]),
        ("c1", &vec![7, 8, 8]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![4, 5, 6]),
        ("c2", &vec![70, 80, 90]),
    );
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a2", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        ),
    ];

    let (_, batches) =
        join_collect_batch_size_equals_two(left, right, on, LeftAnti).await?;
    // BitwiseSortMergeJoinStream uses a coalescer, so batch boundaries differ
    // from the old stream. Only assert data correctness.
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+
    | a1 | b1 | c1 |
    +----+----+----+
    | 1  | 4  | 7  |
    | 2  | 5  | 8  |
    | 2  | 5  | 8  |
    +----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_left_semi() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 2, 3]),
        ("b1", &vec![4, 5, 5, 7]), // 7 does not exist on the right
        ("c1", &vec![7, 8, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![4, 5, 6]), // 5 is double on the right
        ("c2", &vec![70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, LeftSemi).await?;
    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+
    | a1 | b1 | c1 |
    +----+----+----+
    | 1  | 4  | 7  |
    | 2  | 5  | 8  |
    | 2  | 5  | 8  |
    +----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_right_semi_one() -> Result<()> {
    let left = build_table(
        ("a1", &vec![10, 20, 30, 40]),
        ("b1", &vec![4, 5, 5, 6]),
        ("c1", &vec![70, 80, 90, 100]),
    );
    let right = build_table(
        ("a2", &vec![1, 2, 2, 3]),
        ("b1", &vec![4, 5, 5, 7]),
        ("c2", &vec![7, 8, 8, 9]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, RightSemi).await?;
    let expected = [
        "+----+----+----+",
        "| a2 | b1 | c2 |",
        "+----+----+----+",
        "| 1  | 4  | 7  |",
        "| 2  | 5  | 8  |",
        "| 2  | 5  | 8  |",
        "+----+----+----+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
async fn join_right_semi_two() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 2, 3]),
        ("b1", &vec![4, 5, 5, 6]),
        ("c1", &vec![70, 80, 90, 100]),
    );
    let right = build_table(
        ("a1", &vec![1, 2, 2, 3]),
        ("b1", &vec![4, 5, 5, 7]),
        ("c2", &vec![7, 8, 8, 9]),
    );
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        ),
    ];

    let (_, batches) = join_collect(left, right, on, RightSemi).await?;
    let expected = [
        "+----+----+----+",
        "| a1 | b1 | c2 |",
        "+----+----+----+",
        "| 1  | 4  | 7  |",
        "| 2  | 5  | 8  |",
        "| 2  | 5  | 8  |",
        "+----+----+----+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
async fn join_right_semi_two_with_filter() -> Result<()> {
    let left = build_table(("a1", &vec![1]), ("b1", &vec![10]), ("c1", &vec![30]));
    let right = build_table(("a1", &vec![1]), ("b1", &vec![10]), ("c2", &vec![20]));
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        ),
    ];
    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c2", 1)),
            Operator::Lt,
            Arc::new(Column::new("c1", 0)),
        )),
        vec![
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
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Int32, true),
        ])),
    );
    let (_, batches) =
        join_collect_with_filter(left, right, on, filter, RightSemi).await?;
    let expected = [
        "+----+----+----+",
        "| a1 | b1 | c2 |",
        "+----+----+----+",
        "| 1  | 10 | 20 |",
        "+----+----+----+",
    ];
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
async fn join_right_semi_with_nulls() -> Result<()> {
    let left = build_table_i32_nullable(
        ("a1", &vec![Some(0), Some(1), Some(2), Some(2), Some(3)]),
        ("b1", &vec![Some(3), Some(4), Some(5), None, Some(6)]),
        ("c2", &vec![Some(60), None, Some(80), Some(85), Some(90)]),
    );
    let right = build_table_i32_nullable(
        ("a1", &vec![Some(1), Some(2), Some(2), Some(3)]),
        ("b1", &vec![Some(4), Some(5), None, Some(6)]), // null in key field
        ("c2", &vec![Some(7), Some(8), Some(8), None]), // null in non-key field
    );
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        ),
    ];

    let (_, batches) = join_collect(left, right, on, RightSemi).await?;
    let expected = [
        "+----+----+----+",
        "| a1 | b1 | c2 |",
        "+----+----+----+",
        "| 1  | 4  | 7  |",
        "| 2  | 5  | 8  |",
        "| 3  | 6  |    |",
        "+----+----+----+",
    ];
    // The output order is important as SMJ preserves sortedness
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
async fn join_right_semi_with_nulls_with_options() -> Result<()> {
    let left = build_table_i32_nullable(
        ("a1", &vec![Some(3), Some(2), Some(1), Some(0), Some(2)]),
        ("b1", &vec![None, Some(5), Some(4), None, Some(5)]),
        ("c2", &vec![Some(90), Some(80), Some(70), Some(60), None]),
    );
    let right = build_table_i32_nullable(
        ("a1", &vec![Some(3), Some(2), Some(2), Some(1)]),
        ("b1", &vec![None, Some(5), Some(5), Some(4)]), // null in key field
        ("c2", &vec![Some(9), None, Some(8), Some(7)]), // null in non-key field
    );
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        ),
    ];

    let (_, batches) = join_collect_with_options(
        left,
        right,
        on,
        RightSemi,
        vec![
            SortOptions {
                descending: true,
                nulls_first: false,
            };
            2
        ],
        NullEquality::NullEqualsNull,
    )
    .await?;

    let expected = [
        "+----+----+----+",
        "| a1 | b1 | c2 |",
        "+----+----+----+",
        "| 3  |    | 9  |",
        "| 2  | 5  |    |",
        "| 2  | 5  | 8  |",
        "| 1  | 4  | 7  |",
        "+----+----+----+",
    ];
    // The output order is important as SMJ preserves sortedness
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
async fn join_right_semi_output_two_batches() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 2, 3]),
        ("b1", &vec![4, 5, 5, 6]),
        ("c1", &vec![70, 80, 90, 100]),
    );
    let right = build_table(
        ("a1", &vec![1, 2, 2, 3]),
        ("b1", &vec![4, 5, 5, 7]),
        ("c2", &vec![7, 8, 8, 9]),
    );
    let on = vec![
        (
            Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
        ),
        (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        ),
    ];

    let (_, batches) =
        join_collect_batch_size_equals_two(left, right, on, RightSemi).await?;
    let expected = [
        "+----+----+----+",
        "| a1 | b1 | c2 |",
        "+----+----+----+",
        "| 1  | 4  | 7  |",
        "| 2  | 5  | 8  |",
        "| 2  | 5  | 8  |",
        "+----+----+----+",
    ];
    // BitwiseSortMergeJoinStream uses a coalescer, so batch boundaries differ
    // from the old stream. Only assert data correctness.
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
    assert_batches_eq!(expected, &batches);
    Ok(())
}

#[tokio::test]
async fn join_left_mark() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 2, 3]),
        ("b1", &vec![4, 5, 5, 7]), // 7 does not exist on the right
        ("c1", &vec![7, 8, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30, 40]),
        ("b1", &vec![4, 4, 5, 6]), // 5 is double on the right
        ("c2", &vec![60, 70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, LeftMark).await?;
    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+-------+
    | a1 | b1 | c1 | mark  |
    +----+----+----+-------+
    | 1  | 4  | 7  | true  |
    | 2  | 5  | 8  | true  |
    | 2  | 5  | 8  | true  |
    | 3  | 7  | 9  | false |
    +----+----+----+-------+
    ");
    Ok(())
}

#[tokio::test]
async fn join_right_mark() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 2, 3]),
        ("b1", &vec![4, 5, 5, 7]), // 7 does not exist on the right
        ("c1", &vec![7, 8, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30, 40]),
        ("b1", &vec![4, 4, 5, 6]), // 5 is double on the left
        ("c2", &vec![60, 70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, RightMark).await?;
    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+-------+
    | a2 | b1 | c2 | mark  |
    +----+----+----+-------+
    | 10 | 4  | 60 | true  |
    | 20 | 4  | 70 | true  |
    | 30 | 5  | 80 | true  |
    | 40 | 6  | 90 | false |
    +----+----+----+-------+
    ");
    Ok(())
}

#[tokio::test]
async fn join_with_duplicated_column_names() -> Result<()> {
    let left = build_table(
        ("a", &vec![1, 2, 3]),
        ("b", &vec![4, 5, 7]),
        ("c", &vec![7, 8, 9]),
    );
    let right = build_table(
        ("a", &vec![10, 20, 30]),
        ("b", &vec![1, 2, 7]),
        ("c", &vec![70, 80, 90]),
    );
    let on = vec![(
        // join on a=b so there are duplicate column names on unjoined columns
        Arc::new(Column::new_with_schema("a", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Inner).await?;
    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +---+---+---+----+---+----+
    | a | b | c | a  | b | c  |
    +---+---+---+----+---+----+
    | 1 | 4 | 7 | 10 | 1 | 70 |
    | 2 | 5 | 8 | 20 | 2 | 80 |
    +---+---+---+----+---+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_date32() -> Result<()> {
    let left = build_date_table(
        ("a1", &vec![1, 2, 3]),
        ("b1", &vec![19107, 19108, 19108]), // this has a repetition
        ("c1", &vec![7, 8, 9]),
    );
    let right = build_date_table(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![19107, 19108, 19109]),
        ("c2", &vec![70, 80, 90]),
    );

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Inner).await?;

    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +------------+------------+------------+------------+------------+------------+
    | a1         | b1         | c1         | a2         | b1         | c2         |
    +------------+------------+------------+------------+------------+------------+
    | 1970-01-02 | 2022-04-25 | 1970-01-08 | 1970-01-11 | 2022-04-25 | 1970-03-12 |
    | 1970-01-03 | 2022-04-26 | 1970-01-09 | 1970-01-21 | 2022-04-26 | 1970-03-22 |
    | 1970-01-04 | 2022-04-26 | 1970-01-10 | 1970-01-21 | 2022-04-26 | 1970-03-22 |
    +------------+------------+------------+------------+------------+------------+
    ");
    Ok(())
}

#[tokio::test]
async fn join_date64() -> Result<()> {
    let left = build_date64_table(
        ("a1", &vec![1, 2, 3]),
        ("b1", &vec![1650703441000, 1650903441000, 1650903441000]), // this has a repetition
        ("c1", &vec![7, 8, 9]),
    );
    let right = build_date64_table(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![1650703441000, 1650503441000, 1650903441000]),
        ("c2", &vec![70, 80, 90]),
    );

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Inner).await?;

    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
    | a1                      | b1                  | c1                      | a2                      | b1                  | c2                      |
    +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
    | 1970-01-01T00:00:00.001 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.007 | 1970-01-01T00:00:00.010 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.070 |
    | 1970-01-01T00:00:00.002 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.008 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |
    | 1970-01-01T00:00:00.003 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.009 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |
    +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
    ");
    Ok(())
}

#[tokio::test]
async fn join_binary() -> Result<()> {
    let left = build_binary_table(
        (
            "a1",
            &vec![
                &[0xc0, 0xff, 0xee],
                &[0xde, 0xca, 0xde],
                &[0xfa, 0xca, 0xde],
            ],
        ),
        ("b1", &vec![5, 10, 15]), // this has a repetition
        ("c1", &vec![7, 8, 9]),
    );
    let right = build_binary_table(
        (
            "a1",
            &vec![
                &[0xc0, 0xff, 0xee],
                &[0xde, 0xca, 0xde],
                &[0xfa, 0xca, 0xde],
            ],
        ),
        ("b2", &vec![105, 110, 115]),
        ("c2", &vec![70, 80, 90]),
    );

    let on = vec![(
        Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Inner).await?;

    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +--------+----+----+--------+-----+----+
    | a1     | b1 | c1 | a1     | b2  | c2 |
    +--------+----+----+--------+-----+----+
    | c0ffee | 5  | 7  | c0ffee | 105 | 70 |
    | decade | 10 | 8  | decade | 110 | 80 |
    | facade | 15 | 9  | facade | 115 | 90 |
    +--------+----+----+--------+-----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_fixed_size_binary() -> Result<()> {
    let left = build_fixed_size_binary_table(
        (
            "a1",
            &vec![
                &[0xc0, 0xff, 0xee],
                &[0xde, 0xca, 0xde],
                &[0xfa, 0xca, 0xde],
            ],
        ),
        ("b1", &vec![5, 10, 15]), // this has a repetition
        ("c1", &vec![7, 8, 9]),
    );
    let right = build_fixed_size_binary_table(
        (
            "a1",
            &vec![
                &[0xc0, 0xff, 0xee],
                &[0xde, 0xca, 0xde],
                &[0xfa, 0xca, 0xde],
            ],
        ),
        ("b2", &vec![105, 110, 115]),
        ("c2", &vec![70, 80, 90]),
    );

    let on = vec![(
        Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("a1", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Inner).await?;

    // The output order is important as SMJ preserves sortedness
    assert_snapshot!(batches_to_string(&batches), @r"
    +--------+----+----+--------+-----+----+
    | a1     | b1 | c1 | a1     | b2  | c2 |
    +--------+----+----+--------+-----+----+
    | c0ffee | 5  | 7  | c0ffee | 105 | 70 |
    | decade | 10 | 8  | decade | 110 | 80 |
    | facade | 15 | 9  | facade | 115 | 90 |
    +--------+----+----+--------+-----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_left_sort_order() -> Result<()> {
    let left = build_table(
        ("a1", &vec![0, 1, 2, 3, 4, 5]),
        ("b1", &vec![3, 4, 5, 6, 6, 7]),
        ("c1", &vec![4, 5, 6, 7, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![0, 10, 20, 30, 40]),
        ("b2", &vec![2, 4, 6, 6, 8]),
        ("c2", &vec![50, 60, 70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Left).await?;
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b1 | c1 | a2 | b2 | c2 |
    +----+----+----+----+----+----+
    | 0  | 3  | 4  |    |    |    |
    | 1  | 4  | 5  | 10 | 4  | 60 |
    | 2  | 5  | 6  |    |    |    |
    | 3  | 6  | 7  | 20 | 6  | 70 |
    | 3  | 6  | 7  | 30 | 6  | 80 |
    | 4  | 6  | 8  | 20 | 6  | 70 |
    | 4  | 6  | 8  | 30 | 6  | 80 |
    | 5  | 7  | 9  |    |    |    |
    +----+----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_right_sort_order() -> Result<()> {
    let left = build_table(
        ("a1", &vec![0, 1, 2, 3]),
        ("b1", &vec![3, 4, 5, 7]),
        ("c1", &vec![6, 7, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![0, 10, 20, 30]),
        ("b2", &vec![2, 4, 5, 6]),
        ("c2", &vec![60, 70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Right).await?;
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b1 | c1 | a2 | b2 | c2 |
    +----+----+----+----+----+----+
    |    |    |    | 0  | 2  | 60 |
    | 1  | 4  | 7  | 10 | 4  | 70 |
    | 2  | 5  | 8  | 20 | 5  | 80 |
    |    |    |    | 30 | 6  | 90 |
    +----+----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_left_multiple_batches() -> Result<()> {
    let left_batch_1 = build_table_i32(
        ("a1", &vec![0, 1, 2]),
        ("b1", &vec![3, 4, 5]),
        ("c1", &vec![4, 5, 6]),
    );
    let left_batch_2 = build_table_i32(
        ("a1", &vec![3, 4, 5, 6]),
        ("b1", &vec![6, 6, 7, 9]),
        ("c1", &vec![7, 8, 9, 9]),
    );
    let right_batch_1 = build_table_i32(
        ("a2", &vec![0, 10, 20]),
        ("b2", &vec![2, 4, 6]),
        ("c2", &vec![50, 60, 70]),
    );
    let right_batch_2 = build_table_i32(
        ("a2", &vec![30, 40]),
        ("b2", &vec![6, 8]),
        ("c2", &vec![80, 90]),
    );
    let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
    let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Left).await?;
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b1 | c1 | a2 | b2 | c2 |
    +----+----+----+----+----+----+
    | 0  | 3  | 4  |    |    |    |
    | 1  | 4  | 5  | 10 | 4  | 60 |
    | 2  | 5  | 6  |    |    |    |
    | 3  | 6  | 7  | 20 | 6  | 70 |
    | 3  | 6  | 7  | 30 | 6  | 80 |
    | 4  | 6  | 8  | 20 | 6  | 70 |
    | 4  | 6  | 8  | 30 | 6  | 80 |
    | 5  | 7  | 9  |    |    |    |
    | 6  | 9  | 9  |    |    |    |
    +----+----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_right_multiple_batches() -> Result<()> {
    let right_batch_1 = build_table_i32(
        ("a2", &vec![0, 1, 2]),
        ("b2", &vec![3, 4, 5]),
        ("c2", &vec![4, 5, 6]),
    );
    let right_batch_2 = build_table_i32(
        ("a2", &vec![3, 4, 5, 6]),
        ("b2", &vec![6, 6, 7, 9]),
        ("c2", &vec![7, 8, 9, 9]),
    );
    let left_batch_1 = build_table_i32(
        ("a1", &vec![0, 10, 20]),
        ("b1", &vec![2, 4, 6]),
        ("c1", &vec![50, 60, 70]),
    );
    let left_batch_2 = build_table_i32(
        ("a1", &vec![30, 40]),
        ("b1", &vec![6, 8]),
        ("c1", &vec![80, 90]),
    );
    let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
    let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Right).await?;
    assert_snapshot!(batches_to_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b1 | c1 | a2 | b2 | c2 |
    +----+----+----+----+----+----+
    |    |    |    | 0  | 3  | 4  |
    | 10 | 4  | 60 | 1  | 4  | 5  |
    |    |    |    | 2  | 5  | 6  |
    | 20 | 6  | 70 | 3  | 6  | 7  |
    | 30 | 6  | 80 | 3  | 6  | 7  |
    | 20 | 6  | 70 | 4  | 6  | 8  |
    | 30 | 6  | 80 | 4  | 6  | 8  |
    |    |    |    | 5  | 7  | 9  |
    |    |    |    | 6  | 9  | 9  |
    +----+----+----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn join_full_multiple_batches() -> Result<()> {
    let left_batch_1 = build_table_i32(
        ("a1", &vec![0, 1, 2]),
        ("b1", &vec![3, 4, 5]),
        ("c1", &vec![4, 5, 6]),
    );
    let left_batch_2 = build_table_i32(
        ("a1", &vec![3, 4, 5, 6]),
        ("b1", &vec![6, 6, 7, 9]),
        ("c1", &vec![7, 8, 9, 9]),
    );
    let right_batch_1 = build_table_i32(
        ("a2", &vec![0, 10, 20]),
        ("b2", &vec![2, 4, 6]),
        ("c2", &vec![50, 60, 70]),
    );
    let right_batch_2 = build_table_i32(
        ("a2", &vec![30, 40]),
        ("b2", &vec![6, 8]),
        ("c2", &vec![80, 90]),
    );
    let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
    let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Full).await?;
    assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b1 | c1 | a2 | b2 | c2 |
    +----+----+----+----+----+----+
    |    |    |    | 0  | 2  | 50 |
    |    |    |    | 40 | 8  | 90 |
    | 0  | 3  | 4  |    |    |    |
    | 1  | 4  | 5  | 10 | 4  | 60 |
    | 2  | 5  | 6  |    |    |    |
    | 3  | 6  | 7  | 20 | 6  | 70 |
    | 3  | 6  | 7  | 30 | 6  | 80 |
    | 4  | 6  | 8  | 20 | 6  | 70 |
    | 4  | 6  | 8  | 30 | 6  | 80 |
    | 5  | 7  | 9  |    |    |    |
    | 6  | 9  | 9  |    |    |    |
    +----+----+----+----+----+----+
    ");
    Ok(())
}

/// Full outer join where the filter evaluates to NULL due to a nullable column.
/// NULL filter results must be treated as unmatched, not matched.
/// Reproducer for SPARK-43113.
#[tokio::test]
async fn join_full_null_filter_result() -> Result<()> {
    // Left: (a, b) all non-null, sorted on a
    let left = build_table_two_cols(
        ("a1", &vec![1, 1, 2, 2, 3, 3]),
        ("b1", &vec![1, 2, 1, 2, 1, 2]),
    );

    // Right: (a, b) with b nullable, sorted on a
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("a2", DataType::Int32, false),
        Field::new("b2", DataType::Int32, true),
    ]));
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![None, Some(2)])),
        ],
    )?;
    let right =
        TestMemoryExec::try_new_exec(&[vec![right_batch]], right_schema, None).unwrap();

    let on = vec![(
        Arc::new(Column::new_with_schema("a1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("a2", &right.schema())?) as _,
    )];

    // Filter: b1 < (b2 + 1) AND b1 < (a2 + 1)
    // When b2 is NULL, (b2 + 1) is NULL, so b1 < NULL is NULL → unmatched.
    let lit_1: PhysicalExprRef = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
    let b1_lt_b2_plus_1: PhysicalExprRef = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("b1", 0)),
        Operator::Lt,
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("b2", 1)),
            Operator::Plus,
            Arc::clone(&lit_1),
        )),
    ));
    let b1_lt_a2_plus_1: PhysicalExprRef = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("b1", 0)),
        Operator::Lt,
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a2", 2)),
            Operator::Plus,
            Arc::clone(&lit_1),
        )),
    ));
    let filter_expr: PhysicalExprRef = Arc::new(BinaryExpr::new(
        b1_lt_b2_plus_1,
        Operator::And,
        b1_lt_a2_plus_1,
    ));

    let filter = JoinFilter::new(
        filter_expr,
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
                index: 0,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("b1", DataType::Int32, true),
            Field::new("b2", DataType::Int32, true),
            Field::new("a2", DataType::Int32, true),
        ])),
    );

    let (_, batches) = join_collect_with_filter(left, right, on, filter, Full).await?;

    // r=(1,NULL): b2 is NULL → b1 < (NULL+1) is NULL → all a=1 rows unmatched
    // r=(2,2): b1 < 3 AND b1 < 3 → both l=(2,1) and l=(2,2) match
    // l=(3,*): no right row with a=3 → unmatched
    assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+----+----+
    | a1 | b1 | a2 | b2 |
    +----+----+----+----+
    |    |    | 1  |    |
    | 1  | 1  |    |    |
    | 1  | 2  |    |    |
    | 2  | 1  | 2  | 2  |
    | 2  | 2  | 2  | 2  |
    | 3  | 1  |    |    |
    | 3  | 2  |    |    |
    +----+----+----+----+
    ");
    Ok(())
}

#[tokio::test]
async fn overallocation_single_batch_no_spill() -> Result<()> {
    let left = build_table(
        ("a1", &vec![0, 1, 2, 3, 4, 5]),
        ("b1", &vec![1, 2, 3, 4, 5, 6]),
        ("c1", &vec![4, 5, 6, 7, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![0, 10, 20, 30, 40]),
        ("b2", &vec![1, 3, 4, 6, 8]),
        ("c2", &vec![50, 60, 70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    let join_types = vec![
        // Semi/anti/mark joins use BitwiseSortMergeJoinStream which only tracks
        // inner key buffer memory; tested in bitwise_sort_merge_join/tests.rs.
        Inner, Left, Right, Full,
    ];

    // Disable DiskManager to prevent spilling
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()?;
    let session_config = SessionConfig::default().with_batch_size(50);

    for join_type in join_types {
        let task_ctx = TaskContext::default()
            .with_session_config(session_config.clone())
            .with_runtime(Arc::clone(&runtime));
        let task_ctx = Arc::new(task_ctx);

        let join = join_with_options(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            join_type,
            sort_options.clone(),
            NullEquality::NullEqualsNothing,
        )?;

        let stream = join.execute(0, task_ctx)?;
        let err = common::collect(stream).await.unwrap_err();

        assert_contains!(err.to_string(), "Failed to allocate additional");
        assert_contains!(err.to_string(), "SMJStream[0]");
        assert_contains!(err.to_string(), "Disk spilling disabled");
        assert!(join.metrics().is_some());
        assert_eq!(join.metrics().unwrap().spill_count(), Some(0));
        assert_eq!(join.metrics().unwrap().spilled_bytes(), Some(0));
        assert_eq!(join.metrics().unwrap().spilled_rows(), Some(0));
    }

    Ok(())
}

#[tokio::test]
async fn overallocation_multi_batch_no_spill() -> Result<()> {
    let left_batch_1 = build_table_i32(
        ("a1", &vec![0, 1]),
        ("b1", &vec![1, 1]),
        ("c1", &vec![4, 5]),
    );
    let left_batch_2 = build_table_i32(
        ("a1", &vec![2, 3]),
        ("b1", &vec![1, 1]),
        ("c1", &vec![6, 7]),
    );
    let left_batch_3 = build_table_i32(
        ("a1", &vec![4, 5]),
        ("b1", &vec![1, 1]),
        ("c1", &vec![8, 9]),
    );
    let right_batch_1 = build_table_i32(
        ("a2", &vec![0, 10]),
        ("b2", &vec![1, 1]),
        ("c2", &vec![50, 60]),
    );
    let right_batch_2 = build_table_i32(
        ("a2", &vec![20, 30]),
        ("b2", &vec![1, 1]),
        ("c2", &vec![70, 80]),
    );
    let right_batch_3 =
        build_table_i32(("a2", &vec![40]), ("b2", &vec![1]), ("c2", &vec![90]));
    let left = build_table_from_batches(vec![left_batch_1, left_batch_2, left_batch_3]);
    let right =
        build_table_from_batches(vec![right_batch_1, right_batch_2, right_batch_3]);
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    let join_types = vec![
        // Semi/anti/mark joins use BitwiseSortMergeJoinStream which only tracks
        // inner key buffer memory; tested in bitwise_sort_merge_join/tests.rs.
        Inner, Left, Right, Full,
    ];

    // Disable DiskManager to prevent spilling
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()?;
    let session_config = SessionConfig::default().with_batch_size(50);

    for join_type in join_types {
        let task_ctx = TaskContext::default()
            .with_session_config(session_config.clone())
            .with_runtime(Arc::clone(&runtime));
        let task_ctx = Arc::new(task_ctx);
        let join = join_with_options(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            join_type,
            sort_options.clone(),
            NullEquality::NullEqualsNothing,
        )?;

        let stream = join.execute(0, task_ctx)?;
        let err = common::collect(stream).await.unwrap_err();

        assert_contains!(err.to_string(), "Failed to allocate additional");
        assert_contains!(err.to_string(), "SMJStream[0]");
        assert_contains!(err.to_string(), "Disk spilling disabled");
        assert!(join.metrics().is_some());
        assert_eq!(join.metrics().unwrap().spill_count(), Some(0));
        assert_eq!(join.metrics().unwrap().spilled_bytes(), Some(0));
        assert_eq!(join.metrics().unwrap().spilled_rows(), Some(0));
    }

    Ok(())
}

#[tokio::test]
async fn overallocation_single_batch_spill() -> Result<()> {
    let left = build_table(
        ("a1", &vec![0, 1, 2, 3, 4, 5]),
        ("b1", &vec![1, 2, 3, 4, 5, 6]),
        ("c1", &vec![4, 5, 6, 7, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![0, 10, 20, 30, 40]),
        ("b2", &vec![1, 3, 4, 6, 8]),
        ("c2", &vec![50, 60, 70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    let join_types = [
        // Semi/anti/mark joins use BitwiseSortMergeJoinStream which only tracks
        // inner key buffer memory; tested in bitwise_sort_merge_join/tests.rs.
        Inner, Left, Right, Full,
    ];

    // Enable DiskManager to allow spilling
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;

    for batch_size in [1, 50] {
        let session_config = SessionConfig::default().with_batch_size(batch_size);

        for join_type in &join_types {
            let task_ctx = TaskContext::default()
                .with_session_config(session_config.clone())
                .with_runtime(Arc::clone(&runtime));
            let task_ctx = Arc::new(task_ctx);

            let join = join_with_options(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                *join_type,
                sort_options.clone(),
                NullEquality::NullEqualsNothing,
            )?;

            let stream = join.execute(0, task_ctx)?;
            let spilled_join_result = common::collect(stream).await.unwrap();

            assert!(join.metrics().is_some());
            assert!(join.metrics().unwrap().spill_count().unwrap() > 0);
            assert!(join.metrics().unwrap().spilled_bytes().unwrap() > 0);
            assert!(join.metrics().unwrap().spilled_rows().unwrap() > 0);

            // Run the test with no spill configuration as
            let task_ctx_no_spill =
                TaskContext::default().with_session_config(session_config.clone());
            let task_ctx_no_spill = Arc::new(task_ctx_no_spill);

            let join = join_with_options(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                *join_type,
                sort_options.clone(),
                NullEquality::NullEqualsNothing,
            )?;
            let stream = join.execute(0, task_ctx_no_spill)?;
            let no_spilled_join_result = common::collect(stream).await.unwrap();

            assert!(join.metrics().is_some());
            assert_eq!(join.metrics().unwrap().spill_count(), Some(0));
            assert_eq!(join.metrics().unwrap().spilled_bytes(), Some(0));
            assert_eq!(join.metrics().unwrap().spilled_rows(), Some(0));
            // Compare spilled and non spilled data to check spill logic doesn't corrupt the data
            assert_eq!(spilled_join_result, no_spilled_join_result);
        }
    }

    Ok(())
}

#[tokio::test]
async fn overallocation_multi_batch_spill() -> Result<()> {
    let left_batch_1 = build_table_i32(
        ("a1", &vec![0, 1]),
        ("b1", &vec![1, 1]),
        ("c1", &vec![4, 5]),
    );
    let left_batch_2 = build_table_i32(
        ("a1", &vec![2, 3]),
        ("b1", &vec![1, 1]),
        ("c1", &vec![6, 7]),
    );
    let left_batch_3 = build_table_i32(
        ("a1", &vec![4, 5]),
        ("b1", &vec![1, 1]),
        ("c1", &vec![8, 9]),
    );
    let right_batch_1 = build_table_i32(
        ("a2", &vec![0, 10]),
        ("b2", &vec![1, 1]),
        ("c2", &vec![50, 60]),
    );
    let right_batch_2 = build_table_i32(
        ("a2", &vec![20, 30]),
        ("b2", &vec![1, 1]),
        ("c2", &vec![70, 80]),
    );
    let right_batch_3 =
        build_table_i32(("a2", &vec![40]), ("b2", &vec![1]), ("c2", &vec![90]));
    let left = build_table_from_batches(vec![left_batch_1, left_batch_2, left_batch_3]);
    let right =
        build_table_from_batches(vec![right_batch_1, right_batch_2, right_batch_3]);
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    let join_types = [
        // Semi/anti/mark joins use BitwiseSortMergeJoinStream which only tracks
        // inner key buffer memory; tested in bitwise_sort_merge_join/tests.rs.
        Inner, Left, Right, Full,
    ];

    // Enable DiskManager to allow spilling
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(500, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;

    for batch_size in [1, 50] {
        let session_config = SessionConfig::default().with_batch_size(batch_size);

        for join_type in &join_types {
            let task_ctx = TaskContext::default()
                .with_session_config(session_config.clone())
                .with_runtime(Arc::clone(&runtime));
            let task_ctx = Arc::new(task_ctx);
            let join = join_with_options(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                *join_type,
                sort_options.clone(),
                NullEquality::NullEqualsNothing,
            )?;

            let stream = join.execute(0, task_ctx)?;
            let spilled_join_result = common::collect(stream).await.unwrap();
            assert!(join.metrics().is_some());
            assert!(join.metrics().unwrap().spill_count().unwrap() > 0);
            assert!(join.metrics().unwrap().spilled_bytes().unwrap() > 0);
            assert!(join.metrics().unwrap().spilled_rows().unwrap() > 0);

            // For Full joins, get_required_batch_indices extends 0..batches.len(), so
            // poll_spilled_batches can restore all spilled batches at once via infallible
            // grow(). Verify accounting tracked the transient spike and cleaned up.
            let peak_mem = join
                .metrics()
                .and_then(|m| m.sum_by_name("peak_mem_used"))
                .map(|m| m.as_usize())
                .unwrap_or(0);
            assert!(
                peak_mem > 0,
                "peak_mem_used should be > 0 for {join_type:?} batch_size={batch_size}"
            );
            assert_eq!(
                runtime.memory_pool.reserved(),
                0,
                "memory should be fully released after {join_type:?} completes 
                (batch_size={batch_size}): infallible grow during restore must be balanced"
            );
            // Run the test with no spill configuration as
            let task_ctx_no_spill =
                TaskContext::default().with_session_config(session_config.clone());
            let task_ctx_no_spill = Arc::new(task_ctx_no_spill);

            let join = join_with_options(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                *join_type,
                sort_options.clone(),
                NullEquality::NullEqualsNothing,
            )?;
            let stream = join.execute(0, task_ctx_no_spill)?;
            let no_spilled_join_result = common::collect(stream).await.unwrap();

            assert!(join.metrics().is_some());
            assert_eq!(join.metrics().unwrap().spill_count(), Some(0));
            assert_eq!(join.metrics().unwrap().spilled_bytes(), Some(0));
            assert_eq!(join.metrics().unwrap().spilled_rows(), Some(0));
            // Compare spilled and non spilled data to check spill logic doesn't corrupt the data
            assert_eq!(spilled_join_result, no_spilled_join_result);
        }
    }

    Ok(())
}

/// Verifies that `peak_mem_used` reflects join_arrays memory on the spill path.
///
/// Uses a memory limit smaller than a single batch's `size_estimation` so that
/// every batch spills — the `Ok` arm of `allocate_reservation` is never hit.
/// Before the fix, `peak_mem_used` would stay 0 because `set_max` was only
/// called in the `Ok` arm. After the fix, the spill path calls
/// `grow(join_arrays_mem)` + `set_max`, so `peak_mem_used > 0`.
#[tokio::test]
async fn spill_join_arrays_memory_accounting() -> Result<()> {
    use arrow::array::Array;

    let left_batch = build_table_i32(
        ("a1", &vec![0, 1]),
        ("b1", &vec![1, 1]),
        ("c1", &vec![4, 5]),
    );
    let size_estimation = left_batch.get_array_memory_size()
        + Int32Array::from(vec![1, 1]).get_array_memory_size()
        + 2usize.next_power_of_two() * size_of::<usize>()
        + size_of::<std::ops::Range<usize>>()
        + size_of::<usize>();
    let join_arrays_mem = Int32Array::from(vec![1, 1]).get_array_memory_size();

    // Memory limit: too small for a full batch, large enough for join_arrays.
    // Every batch hits the Err arm → spills → grow(join_arrays_mem).
    let memory_limit = usize::midpoint(size_estimation, join_arrays_mem);
    assert!(
        memory_limit < size_estimation && memory_limit > join_arrays_mem,
        "limit {memory_limit} must be between join_arrays_mem {join_arrays_mem} \
         and size_estimation {size_estimation}"
    );

    let left_batches: Vec<RecordBatch> = (0..4)
        .map(|i| {
            build_table_i32(
                ("a1", &vec![i * 2, i * 2 + 1]),
                ("b1", &vec![1, 1]),
                ("c1", &vec![100 + i, 101 + i]),
            )
        })
        .collect();
    let left = build_table_from_batches(left_batches);

    let right_batches: Vec<RecordBatch> = (0..2)
        .map(|i| {
            build_table_i32(
                ("a2", &vec![i * 2, i * 2 + 1]),
                ("b2", &vec![1, 1]),
                ("c2", &vec![200 + i, 201 + i]),
            )
        })
        .collect();
    let right = build_table_from_batches(right_batches);

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(memory_limit, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;

    let session_config = SessionConfig::default().with_batch_size(50);
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(session_config)
            .with_runtime(Arc::clone(&runtime)),
    );

    let join = join_with_options(
        Arc::clone(&left),
        Arc::clone(&right),
        on.clone(),
        Inner,
        sort_options,
        NullEquality::NullEqualsNothing,
    )?;

    let stream = join.execute(0, task_ctx)?;
    let result = common::collect(stream).await.unwrap();

    assert!(!result.is_empty(), "Expected non-empty join result");

    let metrics = join.metrics().unwrap();
    assert!(
        metrics.spill_count().unwrap() > 0,
        "Expected spilling to occur"
    );

    // Before the fix, peak_mem_used was 0 here because set_max was only
    // called in the Ok arm of allocate_reservation, which is never reached
    // when every batch spills. After the fix, the spill path calls
    // grow(join_arrays_mem) + set_max unconditionally.
    let peak_mem = metrics
        .sum_by_name("peak_mem_used")
        .map(|m| m.as_usize())
        .unwrap_or(0);
    assert!(
        peak_mem >= join_arrays_mem,
        "peak_mem_used ({peak_mem}) should be >= join_arrays_mem ({join_arrays_mem})"
    );

    // All memory must be released (grow/shrink balanced, no underflow)
    assert_eq!(
        runtime.memory_pool.reserved(),
        0,
        "All memory should be released after join completes"
    );

    Ok(())
}

/// Test the no-headroom scenario: pool is so tight that even
/// join_arrays_mem exceeds the pool limit. With force-grow, the
/// reservation still tracks the join_arrays unconditionally so the
/// pool reflects actual memory usage.
#[tokio::test]
async fn spill_join_arrays_no_headroom() -> Result<()> {
    use arrow::array::Array;

    let join_arrays_mem = Int32Array::from(vec![1, 1]).get_array_memory_size();

    // Pool smaller than join_arrays_mem: try_grow(size_estimation) fails → spill.
    // Force-grow(join_arrays_mem) succeeds unconditionally → reserved_amount > 0.
    let memory_limit = join_arrays_mem / 2;
    assert!(
        memory_limit < join_arrays_mem,
        "limit {memory_limit} must be smaller than join_arrays_mem {join_arrays_mem}"
    );

    let left_batches: Vec<RecordBatch> = (0..4)
        .map(|i| {
            build_table_i32(
                ("a1", &vec![i * 2, i * 2 + 1]),
                ("b1", &vec![1, 1]),
                ("c1", &vec![100 + i, 101 + i]),
            )
        })
        .collect();
    let left = build_table_from_batches(left_batches);

    let right_batches: Vec<RecordBatch> = (0..2)
        .map(|i| {
            build_table_i32(
                ("a2", &vec![i * 2, i * 2 + 1]),
                ("b2", &vec![1, 1]),
                ("c2", &vec![200 + i, 201 + i]),
            )
        })
        .collect();
    let right = build_table_from_batches(right_batches);

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(memory_limit, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;

    let session_config = SessionConfig::default().with_batch_size(50);
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(session_config)
            .with_runtime(Arc::clone(&runtime)),
    );

    let join = join_with_options(
        Arc::clone(&left),
        Arc::clone(&right),
        on.clone(),
        Inner,
        sort_options,
        NullEquality::NullEqualsNothing,
    )?;

    let stream = join.execute(0, task_ctx)?;
    let result = common::collect(stream).await.unwrap();

    assert!(!result.is_empty(), "Expected non-empty join result");

    let metrics = join.metrics().unwrap();
    assert!(
        metrics.spill_count().unwrap() > 0,
        "Expected spilling to occur"
    );

    // Force-grow means peak_mem_used is always tracked, even when pool is tight.
    let peak_mem = metrics
        .sum_by_name("peak_mem_used")
        .map(|m| m.as_usize())
        .unwrap_or(0);
    assert!(
        peak_mem >= join_arrays_mem,
        "peak_mem_used ({peak_mem}) should be >= join_arrays_mem ({join_arrays_mem})"
    );

    // Pool should be fully released (grow/shrink balanced)
    assert_eq!(
        runtime.memory_pool.reserved(),
        0,
        "All memory should be released after join completes"
    );

    Ok(())
}

/// Build a c1 < c2 filter on the third column of each side.
fn build_c1_lt_c2_filter(left_schema: &Schema, right_schema: &Schema) -> JoinFilter {
    JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c1", 0)),
            Operator::Lt,
            Arc::new(Column::new("c2", 1)),
        )),
        vec![
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
            left_schema
                .field_with_name("c1")
                .unwrap()
                .clone()
                .with_nullable(true),
            right_schema
                .field_with_name("c2")
                .unwrap()
                .clone()
                .with_nullable(true),
        ])),
    )
}

#[tokio::test]
async fn spill_with_filter_deferred() -> Result<()> {
    let left = build_table(
        ("a1", &vec![0, 1, 2, 3, 4, 5]),
        ("b1", &vec![1, 2, 3, 4, 5, 6]),
        ("c1", &vec![4, 5, 6, 7, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![0, 10, 20, 30, 40]),
        ("b2", &vec![1, 3, 4, 6, 8]),
        ("c2", &vec![50, 60, 70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    let filter = build_c1_lt_c2_filter(&left.schema(), &right.schema());

    // Deferred filtering join types handled by the main MaterializingSortMergeJoinStream
    let join_types = [Left, Right, Full];

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;

    for batch_size in [1, 50] {
        let session_config = SessionConfig::default().with_batch_size(batch_size);

        for join_type in &join_types {
            // Run with spilling
            let task_ctx = Arc::new(
                TaskContext::default()
                    .with_session_config(session_config.clone())
                    .with_runtime(Arc::clone(&runtime)),
            );
            let join = join_with_filter(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                filter.clone(),
                *join_type,
                sort_options.clone(),
                NullEquality::NullEqualsNothing,
            )?;
            let stream = join.execute(0, task_ctx)?;
            let spilled_result = common::collect(stream).await.unwrap();

            assert!(join.metrics().is_some());
            assert!(
                join.metrics().unwrap().spill_count().unwrap() > 0,
                "Expected spilling for {join_type:?} batch_size={batch_size}"
            );

            // Run without spilling
            let task_ctx_no_spill = Arc::new(
                TaskContext::default().with_session_config(session_config.clone()),
            );
            let join_no_spill = join_with_filter(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                filter.clone(),
                *join_type,
                sort_options.clone(),
                NullEquality::NullEqualsNothing,
            )?;
            let stream = join_no_spill.execute(0, task_ctx_no_spill)?;
            let no_spill_result = common::collect(stream).await.unwrap();

            let spilled_str = batches_to_sort_string(&spilled_result);
            let no_spill_str = batches_to_sort_string(&no_spill_result);
            assert_eq!(
                spilled_str, no_spill_str,
                "Spill vs no-spill mismatch for {join_type:?} batch_size={batch_size}"
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn spill_with_filter_multi_batch() -> Result<()> {
    let left_batch_1 = build_table_i32(
        ("a1", &vec![0, 1]),
        ("b1", &vec![1, 1]),
        ("c1", &vec![4, 5]),
    );
    let left_batch_2 = build_table_i32(
        ("a1", &vec![2, 3]),
        ("b1", &vec![1, 1]),
        ("c1", &vec![6, 7]),
    );
    let left_batch_3 = build_table_i32(
        ("a1", &vec![4, 5]),
        ("b1", &vec![1, 1]),
        ("c1", &vec![8, 9]),
    );
    let right_batch_1 = build_table_i32(
        ("a2", &vec![0, 10]),
        ("b2", &vec![1, 1]),
        ("c2", &vec![50, 60]),
    );
    let right_batch_2 = build_table_i32(
        ("a2", &vec![20, 30]),
        ("b2", &vec![1, 1]),
        ("c2", &vec![70, 80]),
    );
    let right_batch_3 =
        build_table_i32(("a2", &vec![40]), ("b2", &vec![1]), ("c2", &vec![90]));
    let left = build_table_from_batches(vec![left_batch_1, left_batch_2, left_batch_3]);
    let right =
        build_table_from_batches(vec![right_batch_1, right_batch_2, right_batch_3]);
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    let filter = build_c1_lt_c2_filter(&left.schema(), &right.schema());

    let join_types = [Left, Right, Full];

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(500, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;

    for batch_size in [1, 50] {
        let session_config = SessionConfig::default().with_batch_size(batch_size);

        for join_type in &join_types {
            // Run with spilling
            let task_ctx = Arc::new(
                TaskContext::default()
                    .with_session_config(session_config.clone())
                    .with_runtime(Arc::clone(&runtime)),
            );
            let join = join_with_filter(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                filter.clone(),
                *join_type,
                sort_options.clone(),
                NullEquality::NullEqualsNothing,
            )?;
            let stream = join.execute(0, task_ctx)?;
            let spilled_result = common::collect(stream).await.unwrap();

            assert!(join.metrics().is_some());
            assert!(
                join.metrics().unwrap().spill_count().unwrap() > 0,
                "Expected spilling for {join_type:?} batch_size={batch_size}"
            );

            // Run without spilling
            let task_ctx_no_spill = Arc::new(
                TaskContext::default().with_session_config(session_config.clone()),
            );
            let join_no_spill = join_with_filter(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                filter.clone(),
                *join_type,
                sort_options.clone(),
                NullEquality::NullEqualsNothing,
            )?;
            let stream = join_no_spill.execute(0, task_ctx_no_spill)?;
            let no_spill_result = common::collect(stream).await.unwrap();

            let spilled_str = batches_to_sort_string(&spilled_result);
            let no_spill_str = batches_to_sort_string(&no_spill_result);
            assert_eq!(
                spilled_str, no_spill_str,
                "Spill vs no-spill mismatch for {join_type:?} batch_size={batch_size}"
            );
        }
    }

    Ok(())
}

/// FULL join where all buffered rows match on key but fail the filter.
/// Verifies produce_buffered_not_matched emits null-joined rows under spill.
#[tokio::test]
async fn spill_full_join_filter_not_matched() -> Result<()> {
    // c1 values (100..105) are always > c2 values (1..5), so c1 < c2 always fails
    let left = build_table(
        ("a1", &vec![0, 1, 2, 3, 4]),
        ("b1", &vec![1, 1, 1, 1, 1]),
        ("c1", &vec![100, 101, 102, 103, 104]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30, 40, 50]),
        ("b2", &vec![1, 1, 1, 1, 1]),
        ("c2", &vec![1, 2, 3, 4, 5]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    let filter = build_c1_lt_c2_filter(&left.schema(), &right.schema());

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;

    for batch_size in [1, 50] {
        let session_config = SessionConfig::default().with_batch_size(batch_size);

        // Run with spilling
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(session_config.clone())
                .with_runtime(Arc::clone(&runtime)),
        );
        let join = join_with_filter(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            filter.clone(),
            Full,
            sort_options.clone(),
            NullEquality::NullEqualsNothing,
        )?;
        let stream = join.execute(0, task_ctx)?;
        let spilled_result = common::collect(stream).await.unwrap();

        assert!(
            join.metrics().unwrap().spill_count().unwrap() > 0,
            "Expected spilling for FULL batch_size={batch_size}"
        );

        // Run without spilling
        let task_ctx_no_spill =
            Arc::new(TaskContext::default().with_session_config(session_config.clone()));
        let join_no_spill = join_with_filter(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            filter.clone(),
            Full,
            sort_options.clone(),
            NullEquality::NullEqualsNothing,
        )?;
        let stream = join_no_spill.execute(0, task_ctx_no_spill)?;
        let no_spill_result = common::collect(stream).await.unwrap();

        // All filter evaluations fail, so FULL join should produce:
        // - 5 rows with left columns + null right columns (unmatched left)
        // - 5 rows with null left columns + right columns (unmatched right)
        let total_rows: usize = no_spill_result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 10,
            "FULL join with all-failing filter should produce 10 rows, got {total_rows}"
        );

        let spilled_str = batches_to_sort_string(&spilled_result);
        let no_spill_str = batches_to_sort_string(&no_spill_result);
        assert_eq!(
            spilled_str, no_spill_str,
            "Spill vs no-spill mismatch for FULL join batch_size={batch_size}"
        );
    }

    Ok(())
}

fn build_joined_record_batches() -> Result<JoinedRecordBatches> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Int32, true),
    ]));

    let mut batches = JoinedRecordBatches {
        joined_batches: BatchCoalescer::new(Arc::clone(&schema), 8192),
        filter_metadata: crate::joins::sort_merge_join::filter::FilterMetadata::new(),
    };

    // Insert already prejoined non-filtered rows
    batches.joined_batches.push_batch(RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(Int32Array::from(vec![10, 10])),
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(Int32Array::from(vec![11, 9])),
        ],
    )?)?;

    batches.joined_batches.push_batch(RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![11])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![12])),
        ],
    )?)?;

    batches.joined_batches.push_batch(RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(Int32Array::from(vec![12, 12])),
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(Int32Array::from(vec![11, 13])),
        ],
    )?)?;

    batches.joined_batches.push_batch(RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![13])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![12])),
        ],
    )?)?;

    batches.joined_batches.push_batch(RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(Int32Array::from(vec![14, 14])),
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(Int32Array::from(vec![12, 11])),
        ],
    )?)?;

    let streamed_indices = vec![0, 0];
    batches
        .filter_metadata
        .batch_ids
        .extend(vec![0; streamed_indices.len()]);
    batches
        .filter_metadata
        .row_indices
        .extend(&UInt64Array::from(streamed_indices));

    let streamed_indices = vec![1];
    batches
        .filter_metadata
        .batch_ids
        .extend(vec![0; streamed_indices.len()]);
    batches
        .filter_metadata
        .row_indices
        .extend(&UInt64Array::from(streamed_indices));

    let streamed_indices = vec![0, 0];
    batches
        .filter_metadata
        .batch_ids
        .extend(vec![1; streamed_indices.len()]);
    batches
        .filter_metadata
        .row_indices
        .extend(&UInt64Array::from(streamed_indices));

    let streamed_indices = vec![0];
    batches
        .filter_metadata
        .batch_ids
        .extend(vec![2; streamed_indices.len()]);
    batches
        .filter_metadata
        .row_indices
        .extend(&UInt64Array::from(streamed_indices));

    let streamed_indices = vec![0, 0];
    batches
        .filter_metadata
        .batch_ids
        .extend(vec![3; streamed_indices.len()]);
    batches
        .filter_metadata
        .row_indices
        .extend(&UInt64Array::from(streamed_indices));

    batches
        .filter_metadata
        .filter_mask
        .extend(&BooleanArray::from(vec![true, false]));
    batches
        .filter_metadata
        .filter_mask
        .extend(&BooleanArray::from(vec![true]));
    batches
        .filter_metadata
        .filter_mask
        .extend(&BooleanArray::from(vec![false, true]));
    batches
        .filter_metadata
        .filter_mask
        .extend(&BooleanArray::from(vec![false]));
    batches
        .filter_metadata
        .filter_mask
        .extend(&BooleanArray::from(vec![false, false]));

    Ok(batches)
}

#[tokio::test]
async fn test_left_outer_join_filtered_mask() -> Result<()> {
    let mut joined_batches = build_joined_record_batches()?;
    let schema = joined_batches.joined_batches.schema();

    let output = joined_batches.concat_batches(&schema)?;
    let out_mask = joined_batches.filter_metadata.filter_mask.finish();
    let out_indices = joined_batches.filter_metadata.row_indices.finish();

    assert_eq!(
        get_corrected_filter_mask(
            Left,
            &UInt64Array::from(vec![0]),
            &[0usize],
            &BooleanArray::from(vec![true]),
            output.num_rows()
        )
        .unwrap(),
        BooleanArray::from(vec![true, false, false, false, false, false, false, false])
    );

    assert_eq!(
        get_corrected_filter_mask(
            Left,
            &UInt64Array::from(vec![0]),
            &[0usize],
            &BooleanArray::from(vec![false]),
            output.num_rows()
        )
        .unwrap(),
        BooleanArray::from(vec![false, false, false, false, false, false, false, false])
    );

    assert_eq!(
        get_corrected_filter_mask(
            Left,
            &UInt64Array::from(vec![0, 0]),
            &[0usize; 2],
            &BooleanArray::from(vec![true, true]),
            output.num_rows()
        )
        .unwrap(),
        BooleanArray::from(vec![true, true, false, false, false, false, false, false])
    );

    assert_eq!(
        get_corrected_filter_mask(
            Left,
            &UInt64Array::from(vec![0, 0, 0]),
            &[0usize; 3],
            &BooleanArray::from(vec![true, true, true]),
            output.num_rows()
        )
        .unwrap(),
        BooleanArray::from(vec![true, true, true, false, false, false, false, false])
    );

    assert_eq!(
        get_corrected_filter_mask(
            Left,
            &UInt64Array::from(vec![0, 0, 0]),
            &[0usize; 3],
            &BooleanArray::from(vec![true, false, true]),
            output.num_rows()
        )
        .unwrap(),
        BooleanArray::from(vec![
            Some(true),
            None,
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false)
        ])
    );

    assert_eq!(
        get_corrected_filter_mask(
            Left,
            &UInt64Array::from(vec![0, 0, 0]),
            &[0usize; 3],
            &BooleanArray::from(vec![false, false, true]),
            output.num_rows()
        )
        .unwrap(),
        BooleanArray::from(vec![
            None,
            None,
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false)
        ])
    );

    assert_eq!(
        get_corrected_filter_mask(
            Left,
            &UInt64Array::from(vec![0, 0, 0]),
            &[0usize; 3],
            &BooleanArray::from(vec![false, true, true]),
            output.num_rows()
        )
        .unwrap(),
        BooleanArray::from(vec![
            None,
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false)
        ])
    );

    assert_eq!(
        get_corrected_filter_mask(
            Left,
            &UInt64Array::from(vec![0, 0, 0]),
            &[0usize; 3],
            &BooleanArray::from(vec![false, false, false]),
            output.num_rows()
        )
        .unwrap(),
        BooleanArray::from(vec![
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false),
            Some(false)
        ])
    );

    let corrected_mask = get_corrected_filter_mask(
        Left,
        &out_indices,
        &joined_batches.filter_metadata.batch_ids,
        &out_mask,
        output.num_rows(),
    )
    .unwrap();

    assert_eq!(
        corrected_mask,
        BooleanArray::from(vec![
            Some(true),
            None,
            Some(true),
            None,
            Some(true),
            Some(false),
            None,
            Some(false)
        ])
    );

    let filtered_rb = filter_record_batch(&output, &corrected_mask)?;

    assert_snapshot!(batches_to_string(&[filtered_rb]), @r"
    +---+----+---+----+
    | a | b  | x | y  |
    +---+----+---+----+
    | 1 | 10 | 1 | 11 |
    | 1 | 11 | 1 | 12 |
    | 1 | 12 | 1 | 13 |
    +---+----+---+----+
    ");

    // output null rows

    let null_mask = arrow::compute::not(&corrected_mask)?;
    assert_eq!(
        null_mask,
        BooleanArray::from(vec![
            Some(false),
            None,
            Some(false),
            None,
            Some(false),
            Some(true),
            None,
            Some(true)
        ])
    );

    let null_joined_batch = filter_record_batch(&output, &null_mask)?;

    assert_snapshot!(batches_to_string(&[null_joined_batch]), @r"
    +---+----+---+----+
    | a | b  | x | y  |
    +---+----+---+----+
    | 1 | 13 | 1 | 12 |
    | 1 | 14 | 1 | 11 |
    +---+----+---+----+
    ");
    Ok(())
}

#[test]
fn test_partition_statistics() -> Result<()> {
    use crate::statistics::{StatisticsArgs, StatisticsContext};
    use datafusion_common::stats::Precision;

    let left = build_table(
        ("a1", &vec![1, 2, 3]),
        ("b1", &vec![4, 5, 5]),
        ("c1", &vec![7, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30]),
        ("b1", &vec![4, 5, 6]),
        ("c2", &vec![70, 80, 90]),
    );

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];

    // Test different join types to ensure partition_statistics works correctly for all
    let join_types = vec![
        (Inner, 6),     // left cols + right cols
        (Left, 6),      // left cols + right cols
        (Right, 6),     // left cols + right cols
        (Full, 6),      // left cols + right cols
        (LeftSemi, 3),  // only left cols
        (LeftAnti, 3),  // only left cols
        (RightSemi, 3), // only right cols
        (RightAnti, 3), // only right cols
    ];

    for (join_type, expected_cols) in join_types {
        let join_exec =
            join(Arc::clone(&left), Arc::clone(&right), on.clone(), join_type)?;

        // Test aggregate statistics (partition = None)
        // Should return meaningful statistics computed from both inputs
        let stats =
            StatisticsContext::new().compute(&join_exec, &StatisticsArgs::new())?;
        assert_eq!(
            stats.column_statistics.len(),
            expected_cols,
            "Aggregate stats column count failed for {join_type:?}"
        );
        // Verify that aggregate statistics have a meaningful num_rows (not Absent)
        assert!(
            stats.num_rows != Precision::Absent,
            "Aggregate stats should have meaningful num_rows for {join_type:?}, got {:?}",
            stats.num_rows
        );

        // Test partition-specific statistics (partition = Some(0))
        // The implementation correctly passes `partition` to children.
        // The inputs have a single partition, so the statistics for partition 0
        // match the aggregate statistics.
        let partition_stats = StatisticsContext::new()
            .compute(&join_exec, &StatisticsArgs::new().with_partition(Some(0)))?;
        assert_eq!(
            partition_stats.column_statistics.len(),
            expected_cols,
            "Partition stats column count failed for {join_type:?}"
        );
        assert_eq!(
            partition_stats.num_rows, stats.num_rows,
            "Partition stats num_rows should match aggregate stats for {join_type:?}"
        );
    }

    Ok(())
}

fn build_batches(
    a: (&str, &[Vec<bool>]),
    b: (&str, &[Vec<i32>]),
    c: (&str, &[Vec<i32>]),
) -> (Vec<RecordBatch>, SchemaRef) {
    assert_eq!(a.1.len(), b.1.len());
    let mut batches = vec![];

    let schema = Arc::new(Schema::new(vec![
        Field::new(a.0, DataType::Boolean, false),
        Field::new(b.0, DataType::Int32, false),
        Field::new(c.0, DataType::Int32, false),
    ]));

    for i in 0..a.1.len() {
        batches.push(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(BooleanArray::from(a.1[i].clone())),
                    Arc::new(Int32Array::from(b.1[i].clone())),
                    Arc::new(Int32Array::from(c.1[i].clone())),
                ],
            )
            .unwrap(),
        );
    }
    let schema = batches[0].schema();
    (batches, schema)
}

fn build_batched_finish_barrier_table(
    a: (&str, &[Vec<bool>]),
    b: (&str, &[Vec<i32>]),
    c: (&str, &[Vec<i32>]),
) -> (Arc<BarrierExec>, Arc<TestMemoryExec>) {
    let (batches, schema) = build_batches(a, b, c);

    let memory_exec = TestMemoryExec::try_new_exec(
        std::slice::from_ref(&batches),
        Arc::clone(&schema),
        None,
    )
    .unwrap();

    let barrier_exec = Arc::new(
        BarrierExec::new(vec![batches], schema)
            .with_log(false)
            .without_start_barrier()
            .with_finish_barrier(),
    );

    (barrier_exec, memory_exec)
}

/// Concat and sort batches by all the columns to make sure we can compare them with different join
fn prepare_record_batches_for_cmp(output: Vec<RecordBatch>) -> RecordBatch {
    let output_batch = arrow::compute::concat_batches(output[0].schema_ref(), &output)
        .expect("failed to concat batches");

    // Sort on all columns to make sure we have a deterministic order for the assertion
    let sort_columns = output_batch
        .columns()
        .iter()
        .map(|c| SortColumn {
            values: Arc::clone(c),
            options: None,
        })
        .collect::<Vec<_>>();

    let sorted_columns =
        arrow::compute::lexsort(&sort_columns, None).expect("failed to sort");

    RecordBatch::try_new(output_batch.schema(), sorted_columns)
        .expect("failed to create batch")
}

#[expect(clippy::too_many_arguments)]
async fn join_get_stream_and_get_expected(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    oracle_left: Arc<dyn ExecutionPlan>,
    oracle_right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    join_type: JoinType,
    filter: Option<JoinFilter>,
    batch_size: usize,
) -> Result<(SendableRecordBatchStream, RecordBatch)> {
    let sort_options = vec![SortOptions::default(); on.len()];
    let null_equality = NullEquality::NullEqualsNothing;
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::default().with_batch_size(batch_size)),
    );

    let expected_output = {
        let oracle = HashJoinExec::try_new(
            oracle_left,
            oracle_right,
            on.clone(),
            filter.clone(),
            &join_type,
            None,
            PartitionMode::Partitioned,
            null_equality,
            false,
        )?;

        let stream = oracle.execute(0, Arc::clone(&task_ctx))?;

        let batches = common::collect(stream).await?;

        prepare_record_batches_for_cmp(batches)
    };

    let join = SortMergeJoinExec::try_new(
        left,
        right,
        on,
        filter,
        join_type,
        sort_options,
        null_equality,
    )?;

    let stream = join.execute(0, task_ctx)?;

    Ok((stream, expected_output))
}

fn generate_data_for_emit_early_test(
    batch_size: usize,
    number_of_batches: usize,
    join_type: JoinType,
) -> (
    Arc<BarrierExec>,
    Arc<BarrierExec>,
    Arc<TestMemoryExec>,
    Arc<TestMemoryExec>,
) {
    let number_of_rows_per_batch = number_of_batches * batch_size;
    // Prepare data
    let left_a1 = (0..number_of_rows_per_batch as i32)
        .chunks(batch_size)
        .into_iter()
        .map(|chunk| chunk.collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let left_b1 = (0..1000000)
        .filter(|item| {
            match join_type {
                LeftAnti | RightAnti => {
                    let remainder = item % (batch_size as i32);

                    // Make sure to have one that match and one that don't
                    remainder == 0 || remainder == 1
                }
                // Have at least 1 that is not matching
                _ => item % batch_size as i32 != 0,
            }
        })
        .take(number_of_rows_per_batch)
        .chunks(batch_size)
        .into_iter()
        .map(|chunk| chunk.collect::<Vec<_>>())
        .collect::<Vec<_>>();

    let left_bool_col1 = left_a1
        .clone()
        .into_iter()
        .map(|b| {
            b.into_iter()
                // Mostly true but have some false that not overlap with the right column
                .map(|a| a % (batch_size as i32) != (batch_size as i32) - 2)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let (left, left_memory) = build_batched_finish_barrier_table(
        ("bool_col1", left_bool_col1.as_slice()),
        ("b1", left_b1.as_slice()),
        ("a1", left_a1.as_slice()),
    );

    let right_a2 = (0..number_of_rows_per_batch as i32)
        .map(|item| item * 11)
        .chunks(batch_size)
        .into_iter()
        .map(|chunk| chunk.collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let right_b1 = (0..1000000)
        .filter(|item| {
            match join_type {
                LeftAnti | RightAnti => {
                    let remainder = item % (batch_size as i32);

                    // Make sure to have one that match and one that don't
                    remainder == 1 || remainder == 2
                }
                // Have at least 1 that is not matching
                _ => item % batch_size as i32 != 1,
            }
        })
        .take(number_of_rows_per_batch)
        .chunks(batch_size)
        .into_iter()
        .map(|chunk| chunk.collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let right_bool_col2 = right_a2
        .clone()
        .into_iter()
        .map(|b| {
            b.into_iter()
                // Mostly true but have some false that not overlap with the left column
                .map(|a| a % (batch_size as i32) != (batch_size as i32) - 1)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let (right, right_memory) = build_batched_finish_barrier_table(
        ("bool_col2", right_bool_col2.as_slice()),
        ("b1", right_b1.as_slice()),
        ("a2", right_a2.as_slice()),
    );

    (left, right, left_memory, right_memory)
}

#[tokio::test]
async fn test_should_emit_early_when_have_enough_data_to_emit() -> Result<()> {
    for with_filtering in [false, true] {
        let join_types = vec![
            Inner, Left, Right, RightSemi, Full, LeftSemi, LeftAnti, LeftMark, RightMark,
        ];
        const BATCH_SIZE: usize = 10;
        for join_type in join_types {
            for output_batch_size in [
                BATCH_SIZE / 3,
                BATCH_SIZE / 2,
                BATCH_SIZE,
                BATCH_SIZE * 2,
                BATCH_SIZE * 3,
            ] {
                // Make sure the number of batches is enough for all join type to emit some output
                let number_of_batches = if output_batch_size <= BATCH_SIZE {
                    100
                } else {
                    // Have enough batches
                    (output_batch_size * 100) / BATCH_SIZE
                };

                let (left, right, left_memory, right_memory) =
                    generate_data_for_emit_early_test(
                        BATCH_SIZE,
                        number_of_batches,
                        join_type,
                    );

                let on = vec![(
                    Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
                    Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
                )];

                let join_filter = if with_filtering {
                    let filter = JoinFilter::new(
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("bool_col1", 0)),
                            Operator::And,
                            Arc::new(Column::new("bool_col2", 1)),
                        )),
                        vec![
                            ColumnIndex {
                                index: 0,
                                side: JoinSide::Left,
                            },
                            ColumnIndex {
                                index: 0,
                                side: JoinSide::Right,
                            },
                        ],
                        Arc::new(Schema::new(vec![
                            Field::new("bool_col1", DataType::Boolean, true),
                            Field::new("bool_col2", DataType::Boolean, true),
                        ])),
                    );
                    Some(filter)
                } else {
                    None
                };

                // select *
                // from t1
                // right join t2 on t1.b1 = t2.b1 and t1.bool_col1 AND t2.bool_col2
                let (mut output_stream, expected) = join_get_stream_and_get_expected(
                    Arc::clone(&left) as Arc<dyn ExecutionPlan>,
                    Arc::clone(&right) as Arc<dyn ExecutionPlan>,
                    left_memory as Arc<dyn ExecutionPlan>,
                    right_memory as Arc<dyn ExecutionPlan>,
                    on,
                    join_type,
                    join_filter,
                    output_batch_size,
                )
                .await?;

                let (output_batched, output_batches_after_finish) =
                  consume_stream_until_finish_barrier_reached(left, right, &mut output_stream).await.unwrap_or_else(|e| panic!("Failed to consume stream for join type: '{join_type}' and with filtering '{with_filtering}': {e:?}"));

                // It should emit more than that, but we are being generous
                // and to make sure the test pass for all
                const MINIMUM_OUTPUT_BATCHES: usize = 5;
                assert!(
                    MINIMUM_OUTPUT_BATCHES <= number_of_batches / 5,
                    "Make sure that the minimum output batches is realistic"
                );
                // Test to make sure that we are not waiting for input to be fully consumed to emit some output
                assert!(
                    output_batched.len() >= MINIMUM_OUTPUT_BATCHES,
                    "[Sort Merge Join {join_type}] Stream must have at least emit {} batches, but only got {} batches",
                    MINIMUM_OUTPUT_BATCHES,
                    output_batched.len()
                );

                // Just sanity test to make sure we are still producing valid output
                {
                    let output = [output_batched, output_batches_after_finish].concat();
                    let actual_prepared = prepare_record_batches_for_cmp(output);

                    assert_eq!(actual_prepared.columns(), expected.columns());
                }
            }
        }
    }
    Ok(())
}

/// Polls the stream until both barriers are reached,
/// collecting the emitted batches along the way.
///
/// If the stream is pending for too long (5s) without emitting any batches,
/// it panics to avoid hanging the test indefinitely.
///
/// Note: The left and right BarrierExec might be the input of the output stream
async fn consume_stream_until_finish_barrier_reached(
    left: Arc<BarrierExec>,
    right: Arc<BarrierExec>,
    output_stream: &mut SendableRecordBatchStream,
) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>)> {
    let mut switch_to_finish_barrier = false;
    let mut output_batched = vec![];
    let mut after_finish_barrier_reached = vec![];
    let mut background_task = JoinSet::new();

    let mut start_time_since_last_ready = Instant::now();
    loop {
        let next_item = output_stream.next();

        // Manual polling
        let poll_output = futures::poll!(next_item);

        // Wake up the stream to make sure it makes progress
        tokio::task::yield_now().await;

        match poll_output {
            Poll::Ready(Some(Ok(batch))) => {
                if batch.num_rows() == 0 {
                    return internal_err!("join stream should not emit empty batch");
                }
                if switch_to_finish_barrier {
                    after_finish_barrier_reached.push(batch);
                } else {
                    output_batched.push(batch);
                }
                start_time_since_last_ready = Instant::now();
            }
            Poll::Ready(Some(Err(e))) => return Err(e),
            Poll::Ready(None) if !switch_to_finish_barrier => {
                unreachable!("Stream should not end before manually finishing it")
            }
            Poll::Ready(None) => {
                break;
            }
            Poll::Pending => {
                if right.is_finish_barrier_reached()
                    && left.is_finish_barrier_reached()
                    && !switch_to_finish_barrier
                {
                    switch_to_finish_barrier = true;

                    let right = Arc::clone(&right);
                    background_task.spawn(async move {
                        right.wait_finish().await;
                    });
                    let left = Arc::clone(&left);
                    background_task.spawn(async move {
                        left.wait_finish().await;
                    });
                }

                // Make sure the test doesn't run forever
                if start_time_since_last_ready.elapsed() > Duration::from_secs(5) {
                    return internal_err!(
                        "Stream should have emitted data by now, but it's still pending. Output batches so far: {}",
                        output_batched.len()
                    );
                }
            }
        }
    }

    Ok((output_batched, after_finish_barrier_reached))
}

/// Exercises the multi-source interleave path in `materialize_right_columns`.
///
/// When the right (buffered) side is split into many small batches with unique
/// keys, a single `freeze_streamed()` call references multiple `BufferedBatch`es.
/// This forces the `interleave` kernel instead of the single-source `take` path.
/// Without this test, the interleave path has zero coverage from unit tests
/// (fuzz tests use ~100 unique keys across 1000 rows, so all keys fit in one
/// buffered batch).
#[tokio::test]
async fn join_filtered_with_multiple_buffered_batches() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("val_l", DataType::Int32, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("val_r", DataType::Int32, false),
    ]));

    // Left: single batch, keys 1..=6
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50, 60])),
        ],
    )?;
    let left = build_table_from_batches(vec![left_batch]);

    // Right: one row per batch so each key lives in a separate BufferedBatch
    let right_batches: Vec<RecordBatch> = (1..=6)
        .map(|k| {
            RecordBatch::try_new(
                Arc::clone(&right_schema),
                vec![
                    Arc::new(Int32Array::from(vec![k])),
                    Arc::new(Int32Array::from(vec![k * 100])),
                ],
            )
            .unwrap()
        })
        .collect();
    let right = build_table_from_batches(right_batches);

    let on: JoinOn = vec![(
        Arc::new(Column::new_with_schema("key", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("key", &right.schema())?) as _,
    )];

    // Filter: val_l + val_r < 350 — passes for keys 1-3, fails for 4-6
    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("val_l", 0)),
                Operator::Plus,
                Arc::new(Column::new("val_r", 1)),
            )),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(350)))),
        )),
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
            Field::new("val_l", DataType::Int32, true),
            Field::new("val_r", DataType::Int32, true),
        ])),
    );

    // Inner: only rows passing the filter
    let (_, batches) = join_collect_with_filter(
        Arc::clone(&left),
        Arc::clone(&right),
        on.clone(),
        filter.clone(),
        Inner,
    )
    .await?;
    let result = batches_to_sort_string(&batches);
    assert_snapshot!(result, @r"
    +-----+-------+-----+-------+
    | key | val_l | key | val_r |
    +-----+-------+-----+-------+
    | 1   | 10    | 1   | 100   |
    | 2   | 20    | 2   | 200   |
    | 3   | 30    | 3   | 300   |
    +-----+-------+-----+-------+
    ");

    // Left: unmatched left rows get null right columns
    let (_, batches) = join_collect_with_filter(
        Arc::clone(&left),
        Arc::clone(&right),
        on.clone(),
        filter.clone(),
        Left,
    )
    .await?;
    let result = batches_to_sort_string(&batches);
    assert_snapshot!(result, @r"
    +-----+-------+-----+-------+
    | key | val_l | key | val_r |
    +-----+-------+-----+-------+
    | 1   | 10    | 1   | 100   |
    | 2   | 20    | 2   | 200   |
    | 3   | 30    | 3   | 300   |
    | 4   | 40    |     |       |
    | 5   | 50    |     |       |
    | 6   | 60    |     |       |
    +-----+-------+-----+-------+
    ");

    // Full: unmatched rows on both sides get null columns
    let (_, batches) = join_collect_with_filter(
        Arc::clone(&left),
        Arc::clone(&right),
        on.clone(),
        filter.clone(),
        Full,
    )
    .await?;
    let result = batches_to_sort_string(&batches);
    assert_snapshot!(result, @r"
    +-----+-------+-----+-------+
    | key | val_l | key | val_r |
    +-----+-------+-----+-------+
    |     |       | 4   | 400   |
    |     |       | 5   | 500   |
    |     |       | 6   | 600   |
    | 1   | 10    | 1   | 100   |
    | 2   | 20    | 2   | 200   |
    | 3   | 30    | 3   | 300   |
    | 4   | 40    |     |       |
    | 5   | 50    |     |       |
    | 6   | 60    |     |       |
    +-----+-------+-----+-------+
    ");

    Ok(())
}

/// A single key group spanning many buffered batches, re-scanned once per
/// streamed row.
///
/// `pair_streamed_row_with_group` walks the group from buffered batch 0 for
/// *every* streamed row (`scanning_reset`), and freezes whenever `batch_size`
/// pairs have accumulated -- which happens mid-scan when `batch_size` is not a
/// multiple of the group size. So one `freeze_streamed()` can see chunks whose
/// `buffered_batch_idx` wraps (`.. 4, 5, 0, 1 ..`) or never reaches 0 at all,
/// rather than a single ascending run. `materialize_right_columns` maps those
/// indices to `interleave` source slots, so it must not assume either.
///
/// 6 one-row buffered batches x 2 streamed rows at `batch_size` 5 produces
/// freezes covering batches `[0,1,2,3,4]`, `[5,0,1,2,3]` (wrapped) and
/// `[4,5]` (no zero).
#[tokio::test]
async fn join_with_group_spanning_batches_rescanned_per_streamed_row() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("val_l", DataType::Int32, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("val_r", DataType::Int32, false),
    ]));

    // Two streamed rows sharing one key, so the buffered group is scanned twice.
    let left = build_table_from_batches(vec![RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(Int32Array::from(vec![10, 20])),
        ],
    )?]);

    // One row per batch, all the same key: the group spans all 6 batches.
    let right_batches: Vec<RecordBatch> = (1..=6)
        .map(|i| {
            RecordBatch::try_new(
                Arc::clone(&right_schema),
                vec![
                    Arc::new(Int32Array::from(vec![1])),
                    Arc::new(Int32Array::from(vec![i * 100])),
                ],
            )
            .unwrap()
        })
        .collect();
    let right = build_table_from_batches(right_batches);

    let on: JoinOn = vec![(
        Arc::new(Column::new_with_schema("key", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("key", &right.schema())?) as _,
    )];

    // 5 does not divide the 6-row group, so freezes land mid-scan.
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(5)),
    );
    let join = join(left, right, on, Inner)?;
    let batches = common::collect(join.execute(0, task_ctx)?).await?;

    assert_snapshot!(batches_to_sort_string(&batches), @r"
    +-----+-------+-----+-------+
    | key | val_l | key | val_r |
    +-----+-------+-----+-------+
    | 1   | 10    | 1   | 100   |
    | 1   | 10    | 1   | 200   |
    | 1   | 10    | 1   | 300   |
    | 1   | 10    | 1   | 400   |
    | 1   | 10    | 1   | 500   |
    | 1   | 10    | 1   | 600   |
    | 1   | 20    | 1   | 100   |
    | 1   | 20    | 1   | 200   |
    | 1   | 20    | 1   | 300   |
    | 1   | 20    | 1   | 400   |
    | 1   | 20    | 1   | 500   |
    | 1   | 20    | 1   | 600   |
    +-----+-------+-----+-------+
    ");

    Ok(())
}

/// A wrapped multi-source freeze that also carries a null buffered index.
///
/// `materialize_right_columns` has two independent offsets in play on the
/// interleave path: `batch_idx - min_batch_idx` addresses the source table,
/// and `+ source_offset` shifts past the null sentinel that occupies
/// `interleave` slot 0. Only their combination is interesting, and the two
/// halves are awkward to get into the same freeze: `freeze_dequeuing_buffered`
/// freezes before popping consumed batches, so a null-joined streamed row
/// normally lands in its own single-source freeze.
///
/// The one shape that combines them puts the unmatched streamed row *before*
/// a key group spanning several batches, with two streamed rows matching that
/// group so the scan wraps:
///
///   chunk sequence [0, 1, 2, 0, 1, 2], chunk 0 carrying the null
///
/// Streamed key 5 finds no buffered match, so `null_join_streamed_row` appends
/// a null pair at scan position 0; the two streamed 10s then each re-walk
/// batches 0..2 (`scanning_reset`), wrapping inside the same freeze.
#[tokio::test]
async fn join_wrapped_multi_source_freeze_with_null_buffered_index() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("val_l", DataType::Int32, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("val_r", DataType::Int32, false),
    ]));

    // Key 5 has no buffered match; the two 10s share one group.
    let left = build_table_from_batches(vec![RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![5, 10, 10])),
            Arc::new(Int32Array::from(vec![50, 101, 102])),
        ],
    )?]);

    // One row per batch, all key 10: the group spans all three batches.
    let right_batches: Vec<RecordBatch> = [1000, 2000, 3000]
        .into_iter()
        .map(|v| {
            RecordBatch::try_new(
                Arc::clone(&right_schema),
                vec![
                    Arc::new(Int32Array::from(vec![10])),
                    Arc::new(Int32Array::from(vec![v])),
                ],
            )
            .unwrap()
        })
        .collect();
    let right = build_table_from_batches(right_batches);

    let on: JoinOn = vec![(
        Arc::new(Column::new_with_schema("key", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("key", &right.schema())?) as _,
    )];

    let (_, batches) = join_collect(left, right, on, Left).await?;

    assert_snapshot!(batches_to_sort_string(&batches), @r"
    +-----+-------+-----+-------+
    | key | val_l | key | val_r |
    +-----+-------+-----+-------+
    | 10  | 101   | 10  | 1000  |
    | 10  | 101   | 10  | 2000  |
    | 10  | 101   | 10  | 3000  |
    | 10  | 102   | 10  | 1000  |
    | 10  | 102   | 10  | 2000  |
    | 10  | 102   | 10  | 3000  |
    | 5   | 50    |     |       |
    +-----+-------+-----+-------+
    ");

    Ok(())
}

/// Returns the column names on the schema
fn columns(schema: &Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

// ==================== BitwiseSortMergeJoinStream direct tests ====================
//
// These tests construct a BitwiseSortMergeJoinStream directly (bypassing exec)
// to exercise waiting on inputs and spill edge cases using PendingStream.

/// Create test memory/spill resources for stream-level tests.
fn test_stream_resources(
    inner_schema: SchemaRef,
    metrics: &ExecutionPlanMetricsSet,
) -> (
    datafusion_execution::memory_pool::MemoryReservation,
    SpillManager,
    Arc<datafusion_execution::runtime_env::RuntimeEnv>,
) {
    let ctx = TaskContext::default();
    let runtime_env = ctx.runtime_env();
    let reservation = MemoryConsumer::new("test").register(ctx.memory_pool());
    let spill_manager = SpillManager::new(
        Arc::clone(&runtime_env),
        SpillMetrics::new(metrics, 0),
        inner_schema,
    );
    (reservation, spill_manager, runtime_env)
}

/// A RecordBatch stream that yields Poll::Pending once before delivering
/// each batch at a specified index. This simulates the behavior of
/// repartitioned tokio::sync::mpsc channels where data isn't immediately
/// available.
struct PendingStream {
    batches: Vec<RecordBatch>,
    index: usize,
    /// If pending_before[i] is true, yield Pending once before delivering
    /// the batch at index i.
    pending_before: Vec<bool>,
    /// True if we've already yielded Pending for the current index.
    yielded_pending: bool,
    schema: SchemaRef,
}

impl PendingStream {
    fn new(batches: Vec<RecordBatch>, pending_before: Vec<bool>) -> Self {
        assert_eq!(batches.len(), pending_before.len());
        let schema = batches[0].schema();
        Self {
            batches,
            index: 0,
            pending_before,
            yielded_pending: false,
            schema,
        }
    }
}

impl Stream for PendingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.index >= self.batches.len() {
            return Poll::Ready(None);
        }
        if self.pending_before[self.index] && !self.yielded_pending {
            self.yielded_pending = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.yielded_pending = false;
        let batch = self.batches[self.index].clone();
        self.index += 1;
        Poll::Ready(Some(Ok(batch)))
    }
}

impl RecordBatchStream for PendingStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Helper: collect all output from a BitwiseSortMergeJoinStream.
async fn collect_stream(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    common::collect(stream).await
}

#[rstest::rstest]
#[case::empty_inner(true, false)]
#[case::matching_key(false, false)]
#[case::matching_key_with_filter(false, true)]
#[tokio::test]
async fn bitwise_emits_completed_batches_before_pending_outer(
    #[values(LeftMark, RightMark)] join_type: JoinType,
    #[case] empty_inner: bool,
    #[case] with_filter: bool,
) -> Result<()> {
    let outer_batches = vec![
        build_table_i32(
            ("id", &vec![0, 1]),
            ("key", &vec![1, 1]),
            ("value", &vec![10, 20]),
        ),
        build_table_i32(
            ("id", &vec![2, 3]),
            ("key", &vec![1, 1]),
            ("value", &vec![10, 20]),
        ),
    ];
    let inner_batch = if empty_inner {
        RecordBatch::new_empty(outer_batches[0].schema())
    } else {
        build_table_i32(("id", &vec![0]), ("key", &vec![1]), ("value", &vec![10]))
    };
    let outer = Box::pin(PendingStream::new(outer_batches.clone(), vec![false, true]));
    let inner = Box::pin(PendingStream::new(vec![inner_batch], vec![false]));
    let filter = with_filter.then(|| {
        JoinFilter::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("left_value", 0)),
                Operator::Eq,
                Arc::new(Column::new("right_value", 1)),
            )),
            vec![
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
                Field::new("left_value", DataType::Int32, false),
                Field::new("right_value", DataType::Int32, false),
            ])),
        )
    });
    let mut fields = outer.schema().fields().to_vec();
    fields.push(Arc::new(Field::new("mark", DataType::Boolean, false)));
    let schema = Arc::new(Schema::new(fields));
    let metrics = ExecutionPlanMetricsSet::new();
    let (reservation, spill_manager, runtime_env) =
        test_stream_resources(inner.schema(), &metrics);
    let mut stream = BitwiseSortMergeJoinStream::try_new(
        Arc::clone(&schema),
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
        outer,
        inner,
        vec![Arc::new(Column::new("key", 1))],
        vec![Arc::new(Column::new("key", 1))],
        filter,
        join_type,
        2,
        0,
        &metrics,
        reservation,
        spill_manager,
        runtime_env,
    )?;

    // The first outer batch already fills an output batch. Do not wait for the next
    // outer batch, even when draining unmatched rows or continuing the same key group.
    let first = match futures::poll!(stream.next()) {
        Poll::Ready(Some(batch)) => batch?,
        other => panic!("Completed output must precede pending outer input: {other:?}"),
    };
    let mut output = vec![first];
    output.extend(collect_stream(stream).await?);

    let expected = outer_batches
        .iter()
        .map(|batch| {
            let mut columns = batch.columns().to_vec();
            columns.push(Arc::new(BooleanArray::from(vec![
                !empty_inner,
                !empty_inner && !with_filter,
            ])));
            RecordBatch::try_new(Arc::clone(&schema), columns)
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(
        arrow::compute::concat_batches(&schema, &output)?,
        arrow::compute::concat_batches(&schema, &expected)?,
    );
    Ok(())
}

// ==================== join_time metric tests ====================
//
// These verify that `join_time` measures only the join's own work: waiting
// for either child input or for the consumer to take an emitted batch must
// not be counted.

/// Stream that sleeps `delay` before yielding each batch, to simulate a
/// slow input.
fn delayed_stream(
    batches: Vec<RecordBatch>,
    delay: Duration,
) -> SendableRecordBatchStream {
    let schema = batches[0].schema();
    Box::pin(crate::stream::RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(batches.into_iter().map(Ok)).then(move |item| async move {
            tokio::time::sleep(delay).await;
            item
        }),
    ))
}

/// Three 2-row batches with unique matching keys.
fn join_time_batches() -> Vec<RecordBatch> {
    vec![
        build_table_i32(
            ("a1", &vec![0, 1]),
            ("b1", &vec![1, 2]),
            ("c1", &vec![7, 8]),
        ),
        build_table_i32(
            ("a1", &vec![2, 3]),
            ("b1", &vec![3, 4]),
            ("c1", &vec![7, 8]),
        ),
        build_table_i32(
            ("a1", &vec![4, 5]),
            ("b1", &vec![5, 6]),
            ("c1", &vec![7, 8]),
        ),
    ]
}

/// Build a no-filter LeftSemi bitwise stream over the given input streams.
/// The small batch size makes each outer batch surface as its own output
/// batch, so a slow consumer test sees multiple emits.
fn join_time_test_join(
    outer: SendableRecordBatchStream,
    inner: SendableRecordBatchStream,
) -> (SendableRecordBatchStream, ExecutionPlanMetricsSet) {
    let metrics = ExecutionPlanMetricsSet::new();
    let outer_schema = outer.schema();
    let (reservation, spill_manager, runtime_env) =
        test_stream_resources(inner.schema(), &metrics);
    let stream = BitwiseSortMergeJoinStream::try_new(
        outer_schema,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
        outer,
        inner,
        vec![Arc::new(Column::new("b1", 1)) as PhysicalExprRef],
        vec![Arc::new(Column::new("b1", 1)) as PhysicalExprRef],
        None,
        LeftSemi,
        2,
        0,
        &metrics,
        reservation,
        spill_manager,
        runtime_env,
    )
    .unwrap();
    (stream, metrics)
}

fn join_time_of(metrics: &ExecutionPlanMetricsSet) -> Duration {
    Duration::from_nanos(
        metrics
            .clone_inner()
            .sum_by_name("join_time")
            .map(|m| m.as_usize())
            .unwrap_or(0) as u64,
    )
}

/// Run a join with the given injected `delay`, retrying with 4x the delay
/// (up to 3 attempts) when `join_time < delay` fails.
///
/// This de-flakes the check without masking real bugs: a genuine exclusion
/// bug makes `join_time` absorb the injected waits, so it scales with the
/// delay and fails at every escalation level. Only a fixed-size disturbance
/// (e.g. the OS preempting the test thread while the join_time clock is
/// running) is filtered out, since it cannot grow 4x with the delay.
///
/// `run` returns `(join_time, wall)` for one join execution. Deterministic
/// invariants (row counts, wall-time lower bounds) stay as asserts inside
/// `run` — deliberately: a panic there fails the test immediately without
/// retrying, since those cannot flake and escalation would only mask a real
/// bug. Likewise `Err` from `run` (join execution failure) propagates
/// immediately. Only the preemption-sensitive `join_time` check is retried.
async fn check_join_time_excluded<F, Fut>(mut run: F) -> Result<()>
where
    F: FnMut(Duration) -> Fut,
    Fut: Future<Output = Result<(Duration, Duration)>>,
{
    let mut delay = Duration::from_millis(50);
    for attempt in 0..3 {
        let (join_time, wall) = run(delay).await?;
        if join_time < delay {
            return Ok(());
        }
        assert!(
            attempt < 2,
            "join_time ({join_time:?}) should be well below the injected \
             delay ({delay:?}) even after escalating retries; wall {wall:?}"
        );
        delay *= 4;
    }
    unreachable!()
}

/// join_time must not include time spent waiting for the outer input.
#[tokio::test]
async fn join_time_excludes_outer_input_wait() -> Result<()> {
    check_join_time_excluded(|delay| async move {
        let outer = delayed_stream(join_time_batches(), delay);
        let inner = delayed_stream(join_time_batches(), Duration::ZERO);
        let (stream, metrics) = join_time_test_join(outer, inner);

        let start = Instant::now();
        let batches = collect_stream(stream).await?;
        let wall = start.elapsed();

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 6, "all outer rows should match");
        assert!(
            wall >= delay * 3,
            "outer delays should dominate wall time, got {wall:?}"
        );
        Ok((join_time_of(&metrics), wall))
    })
    .await
}

/// join_time must not include time spent waiting for the inner input.
#[tokio::test]
async fn join_time_excludes_inner_input_wait() -> Result<()> {
    check_join_time_excluded(|delay| async move {
        let outer = delayed_stream(join_time_batches(), Duration::ZERO);
        let inner = delayed_stream(join_time_batches(), delay);
        let (stream, metrics) = join_time_test_join(outer, inner);

        let start = Instant::now();
        let batches = collect_stream(stream).await?;
        let wall = start.elapsed();

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 6, "all outer rows should match");
        assert!(
            wall >= delay * 3,
            "inner delays should dominate wall time, got {wall:?}"
        );
        Ok((join_time_of(&metrics), wall))
    })
    .await
}

/// join_time must not include time the consumer spends holding an emitted
/// batch (the generator is suspended inside `emitter.emit` meanwhile).
#[tokio::test]
async fn join_time_excludes_consumer_wait() -> Result<()> {
    check_join_time_excluded(|delay| async move {
        let outer = delayed_stream(join_time_batches(), Duration::ZERO);
        let inner = delayed_stream(join_time_batches(), Duration::ZERO);
        let (mut stream, metrics) = join_time_test_join(outer, inner);

        let start = Instant::now();
        let mut output_batches = 0u32;
        while let Some(batch) = stream.next().await {
            batch?;
            output_batches += 1;
            // Simulate a slow consumer between emitted batches.
            tokio::time::sleep(delay).await;
        }
        let wall = start.elapsed();

        assert!(
            output_batches >= 3,
            "expected multiple emitted batches, got {output_batches}"
        );
        assert!(
            wall >= delay * output_batches,
            "consumer delays should dominate wall time, got {wall:?}"
        );
        Ok((join_time_of(&metrics), wall))
    })
    .await
}

/// Three 2-row batches with unique matching keys, right-side column names.
fn join_time_batches_right() -> Vec<RecordBatch> {
    vec![
        build_table_i32(
            ("a2", &vec![0, 1]),
            ("b2", &vec![1, 2]),
            ("c2", &vec![7, 8]),
        ),
        build_table_i32(
            ("a2", &vec![2, 3]),
            ("b2", &vec![3, 4]),
            ("c2", &vec![7, 8]),
        ),
        build_table_i32(
            ("a2", &vec![4, 5]),
            ("b2", &vec![5, 6]),
            ("c2", &vec![7, 8]),
        ),
    ]
}

/// Build a no-filter Inner materializing join over the given input streams.
/// The small batch size makes the output surface as multiple batches, so a
/// slow consumer test sees multiple emits.
fn materializing_join_time_test_join(
    streamed: SendableRecordBatchStream,
    buffered: SendableRecordBatchStream,
) -> (SendableRecordBatchStream, ExecutionPlanMetricsSet) {
    use crate::joins::sort_merge_join::materializing_stream::MaterializingSortMergeJoinStream;
    use crate::joins::sort_merge_join::metrics::SortMergeJoinMetrics;

    let metrics = ExecutionPlanMetricsSet::new();
    let out_schema = Arc::new(Schema::new(
        streamed
            .schema()
            .fields()
            .iter()
            .chain(buffered.schema().fields().iter())
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    ));
    let (reservation, spill_manager, runtime_env) =
        test_stream_resources(buffered.schema(), &metrics);
    let stream = MaterializingSortMergeJoinStream::try_new(
        out_schema,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
        streamed,
        buffered,
        vec![Arc::new(Column::new("b1", 1)) as _],
        vec![Arc::new(Column::new("b2", 1)) as _],
        None,
        Inner,
        2,
        SortMergeJoinMetrics::new(0, &metrics),
        reservation,
        spill_manager,
        runtime_env,
    )
    .unwrap();
    (stream, metrics)
}

/// join_time must not include time spent waiting for the streamed input.
#[tokio::test]
async fn materializing_join_time_excludes_streamed_input_wait() -> Result<()> {
    check_join_time_excluded(|delay| async move {
        let streamed = delayed_stream(join_time_batches(), delay);
        let buffered = delayed_stream(join_time_batches_right(), Duration::ZERO);
        let (stream, metrics) = materializing_join_time_test_join(streamed, buffered);

        let start = Instant::now();
        let batches = collect_stream(stream).await?;
        let wall = start.elapsed();

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 6, "all rows should match");
        assert!(
            wall >= delay * 3,
            "streamed delays should dominate wall time, got {wall:?}"
        );
        Ok((join_time_of(&metrics), wall))
    })
    .await
}

/// join_time must not include time spent waiting for the buffered input.
#[tokio::test]
async fn materializing_join_time_excludes_buffered_input_wait() -> Result<()> {
    check_join_time_excluded(|delay| async move {
        let streamed = delayed_stream(join_time_batches(), Duration::ZERO);
        let buffered = delayed_stream(join_time_batches_right(), delay);
        let (stream, metrics) = materializing_join_time_test_join(streamed, buffered);

        let start = Instant::now();
        let batches = collect_stream(stream).await?;
        let wall = start.elapsed();

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 6, "all rows should match");
        assert!(
            wall >= delay * 3,
            "buffered delays should dominate wall time, got {wall:?}"
        );
        Ok((join_time_of(&metrics), wall))
    })
    .await
}

/// join_time must not include time the consumer spends holding an emitted
/// batch (the generator is suspended inside `emitter.emit` meanwhile).
#[tokio::test]
async fn materializing_join_time_excludes_consumer_wait() -> Result<()> {
    check_join_time_excluded(|delay| async move {
        let streamed = delayed_stream(join_time_batches(), Duration::ZERO);
        let buffered = delayed_stream(join_time_batches_right(), Duration::ZERO);
        let (mut stream, metrics) = materializing_join_time_test_join(streamed, buffered);

        let start = Instant::now();
        let mut output_batches = 0u32;
        while let Some(batch) = stream.next().await {
            batch?;
            output_batches += 1;
            // Simulate a slow consumer between emitted batches.
            tokio::time::sleep(delay).await;
        }
        let wall = start.elapsed();

        assert!(
            output_batches >= 3,
            "expected multiple emitted batches, got {output_batches}"
        );
        assert!(
            wall >= delay * output_batches,
            "consumer delays should dominate wall time, got {wall:?}"
        );
        Ok((join_time_of(&metrics), wall))
    })
    .await
}

/// An inner key group spanning multiple inner batches must survive the inner
/// input returning Pending mid-way: inner rows delivered before the Pending
/// still take part in the filter evaluation.
///
/// Setup:
/// - Inner: 3 single-row batches, all with key=1, filter values c2=[10, 20, 30]
/// - Outer: 1 row, key=1, filter value c1=10
/// - Filter: c1 == c2 (only first inner row c2=10 matches)
/// - Pending injected before 3rd inner batch
///
/// Expected: outer row emitted (match via c2=10)
#[tokio::test]
async fn filter_buffer_pending_loses_inner_rows() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c1", DataType::Int32, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("a2", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c2", DataType::Int32, false),
    ]));

    // Outer: 1 row, key=1, c1=10
    let outer_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![1])), // join key
            Arc::new(Int32Array::from(vec![10])), // filter value
        ],
    )?;

    // Inner: 3 single-row batches, key=1, c2=[10, 20, 30]
    let inner_batch1 = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(vec![100])),
            Arc::new(Int32Array::from(vec![1])), // join key
            Arc::new(Int32Array::from(vec![10])), // matches filter
        ],
    )?;
    let inner_batch2 = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(vec![200])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![20])), // doesn't match
        ],
    )?;
    let inner_batch3 = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(vec![300])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![30])), // doesn't match
        ],
    )?;

    let outer: SendableRecordBatchStream = Box::pin(PendingStream::new(
        vec![outer_batch],
        vec![false], // outer delivers immediately
    ));
    let inner: SendableRecordBatchStream = Box::pin(PendingStream::new(
        vec![inner_batch1, inner_batch2, inner_batch3],
        vec![false, false, true], // Pending before 3rd batch
    ));

    // Filter: c1 == c2
    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c1", 0)),
            Operator::Eq,
            Arc::new(Column::new("c2", 1)),
        )),
        vec![
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
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ])),
    );

    let on_outer: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];
    let on_inner: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];

    let metrics = ExecutionPlanMetricsSet::new();
    let inner_schema = inner.schema();
    let (reservation, spill_manager, runtime_env) =
        test_stream_resources(inner_schema, &metrics);
    let stream = BitwiseSortMergeJoinStream::try_new(
        left_schema, // output schema = outer schema for semi
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
        outer,
        inner,
        on_outer,
        on_inner,
        Some(filter),
        LeftSemi,
        8192,
        0,
        &metrics,
        reservation,
        spill_manager,
        runtime_env,
    )?;

    let batches = collect_stream(stream).await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 1,
        "LeftSemi with filter: outer row should be emitted because \
         inner row c2=10 matches filter c1==c2. Got {total} rows."
    );
    Ok(())
}

/// A matched outer key group spanning a batch boundary must survive the outer
/// input returning Pending at that boundary: the rows continuing the key group
/// still count as matched, even though the inner side has already advanced
/// past the key.
///
/// Setup:
/// - Outer: 2 single-row batches, both with key=1 (key group spans boundary)
/// - Inner: 1 row with key=1
/// - Pending injected on outer before 2nd batch
///
/// Expected: both outer rows emitted
#[tokio::test]
async fn no_filter_boundary_pending_loses_outer_rows() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c1", DataType::Int32, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("a2", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c2", DataType::Int32, false),
    ]));

    // Outer: 2 single-row batches, both key=1
    let outer_batch1 = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![10])),
        ],
    )?;
    let outer_batch2 = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![1])), // same key
            Arc::new(Int32Array::from(vec![20])),
        ],
    )?;

    // Inner: 1 row, key=1
    let inner_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(vec![100])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![50])),
        ],
    )?;

    let outer: SendableRecordBatchStream = Box::pin(PendingStream::new(
        vec![outer_batch1, outer_batch2],
        vec![false, true], // Pending before 2nd outer batch
    ));
    let inner: SendableRecordBatchStream =
        Box::pin(PendingStream::new(vec![inner_batch], vec![false]));

    let on_outer: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];
    let on_inner: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];

    let metrics = ExecutionPlanMetricsSet::new();
    let inner_schema = inner.schema();
    let (reservation, spill_manager, runtime_env) =
        test_stream_resources(inner_schema, &metrics);
    let stream = BitwiseSortMergeJoinStream::try_new(
        left_schema,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
        outer,
        inner,
        on_outer,
        on_inner,
        None, // no filter
        LeftSemi,
        8192,
        0,
        &metrics,
        reservation,
        spill_manager,
        runtime_env,
    )?;

    let batches = collect_stream(stream).await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 2,
        "LeftSemi no filter: both outer rows (key=1) should be emitted \
         because inner has key=1. Got {total} rows."
    );
    Ok(())
}

/// Verifies no-filter semi/anti joins when a matching outer key group spans
/// multiple batches and the next outer batch is temporarily unavailable.
///
/// The outer input has an unmatched prefix row followed by a matching key
/// group that continues in the next batch. Both rows with key=1 should be
/// treated as matched. Returning `Pending` before the second batch makes the
/// join wait for the continuation while the key group is still open.
#[tokio::test]
async fn no_filter_boundary_pending_with_unmatched_prefix() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c1", DataType::Int32, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("a2", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c2", DataType::Int32, false),
    ]));

    // Key=0 is unmatched. Key=1 matches inner and spans the batch boundary.
    let outer_batch1 = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![0, 1])),
            Arc::new(Int32Array::from(vec![0, 1])),
            Arc::new(Int32Array::from(vec![0, 10])),
        ],
    )?;
    let outer_batch2 = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![1])), // same key
            Arc::new(Int32Array::from(vec![20])),
        ],
    )?;

    // Key=1 matches two outer rows. Key=2 keeps the inner input non-exhausted.
    let inner_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(vec![100, 200])),
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![50, 60])),
        ],
    )?;

    let on_outer: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];
    let on_inner: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];

    for (join_type, expected_a1) in [(LeftSemi, vec![1, 2]), (LeftAnti, vec![0])] {
        let outer: SendableRecordBatchStream = Box::pin(PendingStream::new(
            vec![outer_batch1.clone(), outer_batch2.clone()],
            vec![false, true], // Pending before 2nd outer batch
        ));
        let inner: SendableRecordBatchStream =
            Box::pin(PendingStream::new(vec![inner_batch.clone()], vec![false]));

        let metrics = ExecutionPlanMetricsSet::new();
        let inner_schema = inner.schema();
        let (reservation, spill_manager, runtime_env) =
            test_stream_resources(inner_schema, &metrics);
        let stream = BitwiseSortMergeJoinStream::try_new(
            Arc::clone(&left_schema),
            vec![SortOptions::default()],
            NullEquality::NullEqualsNothing,
            outer,
            inner,
            on_outer.clone(),
            on_inner.clone(),
            None, // no filter
            join_type,
            8192,
            0,
            &metrics,
            reservation,
            spill_manager,
            runtime_env,
        )?;

        let batches = collect_stream(stream).await?;
        let actual_a1 = batches
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                (0..batch.num_rows()).map(|row| values.value(row))
            })
            .collect::<Vec<_>>();
        assert_eq!(actual_a1, expected_a1, "{join_type:?}");
    }
    Ok(())
}

/// Same as the no-filter boundary case, with a filter: the outer key group
/// spans batches and the outer input returns Pending at the boundary.
///
/// Setup:
/// - Outer: 2 single-row batches, both key=1, c1=[10, 20]
/// - Inner: 1 row, key=1, c2=10
/// - Filter: c1 == c2 (first outer row matches, second doesn't)
/// - Pending before 2nd outer batch
///
/// Expected: 1 row (only the first outer row c1=10 passes the filter)
#[tokio::test]
async fn filtered_boundary_pending_outer_rows() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c1", DataType::Int32, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("a2", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c2", DataType::Int32, false),
    ]));

    let outer_batch1 = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![10])), // matches filter
        ],
    )?;
    let outer_batch2 = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![1])), // same key
            Arc::new(Int32Array::from(vec![20])), // doesn't match
        ],
    )?;

    let inner_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(vec![100])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![10])),
        ],
    )?;

    let outer: SendableRecordBatchStream = Box::pin(PendingStream::new(
        vec![outer_batch1, outer_batch2],
        vec![false, true], // Pending before 2nd outer batch
    ));
    let inner: SendableRecordBatchStream =
        Box::pin(PendingStream::new(vec![inner_batch], vec![false]));

    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c1", 0)),
            Operator::Eq,
            Arc::new(Column::new("c2", 1)),
        )),
        vec![
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
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ])),
    );

    let on_outer: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];
    let on_inner: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];

    let metrics = ExecutionPlanMetricsSet::new();
    let inner_schema = inner.schema();
    let (reservation, spill_manager, runtime_env) =
        test_stream_resources(inner_schema, &metrics);
    let stream = BitwiseSortMergeJoinStream::try_new(
        left_schema,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
        outer,
        inner,
        on_outer,
        on_inner,
        Some(filter),
        LeftSemi,
        8192,
        0,
        &metrics,
        reservation,
        spill_manager,
        runtime_env,
    )?;

    let batches = collect_stream(stream).await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 1,
        "LeftSemi filtered boundary: only first outer row (c1=10) matches \
         filter c1==c2. Got {total} rows."
    );
    Ok(())
}

// ── Bitwise stream spill tests ─────────────────────────────────────────────

struct FilteredBitwiseSpillFixture {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    sort_options: Vec<SortOptions>,
    filter: JoinFilter,
}

fn filtered_bitwise_spill_fixture() -> Result<FilteredBitwiseSpillFixture> {
    let left = build_table(
        ("a1", &vec![1, 2, 3, 4, 5, 6]),
        ("b1", &vec![1, 2, 3, 4, 5, 6]),
        ("c1", &vec![4, 5, 6, 7, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30, 40, 50]),
        ("b1", &vec![1, 3, 4, 6, 8]),
        ("c2", &vec![50, 60, 70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];
    let filter = build_c1_lt_c2_filter(left.schema().as_ref(), right.schema().as_ref());

    Ok(FilteredBitwiseSpillFixture {
        left,
        right,
        on,
        sort_options,
        filter,
    })
}

/// Exercises inner key group spilling under memory pressure.
///
/// Uses a tiny memory limit (100 bytes) with disk spilling enabled. Since our
/// operator only buffers inner rows when a filter is present, this test includes
/// a filter (c1 < c2, always true). Verifies:
/// 1. Spill metrics are recorded (spill_count, spilled_bytes, spilled_rows > 0)
/// 2. Results match a non-spilled run
#[tokio::test]
async fn bitwise_spill_with_filter() -> Result<()> {
    let FilteredBitwiseSpillFixture {
        left,
        right,
        on,
        sort_options,
        filter,
    } = filtered_bitwise_spill_fixture()?;

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;

    for batch_size in [1, 50] {
        let session_config = SessionConfig::default().with_batch_size(batch_size);

        for join_type in [LeftSemi, LeftAnti, RightSemi, RightAnti] {
            let task_ctx = Arc::new(
                TaskContext::default()
                    .with_session_config(session_config.clone())
                    .with_runtime(Arc::clone(&runtime)),
            );

            let join = SortMergeJoinExec::try_new(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                Some(filter.clone()),
                join_type,
                sort_options.clone(),
                NullEquality::NullEqualsNothing,
            )?;
            let stream = join.execute(0, task_ctx)?;
            let spilled_result = common::collect(stream).await.unwrap();

            assert!(
                join.metrics().is_some(),
                "metrics missing for {join_type:?}"
            );
            let metrics = join.metrics().unwrap();
            assert!(
                metrics.spill_count().unwrap() > 0,
                "expected spill_count > 0 for {join_type:?}, batch_size={batch_size}"
            );
            assert!(
                metrics.spilled_bytes().unwrap() > 0,
                "expected spilled_bytes > 0 for {join_type:?}, batch_size={batch_size}"
            );
            assert!(
                metrics.spilled_rows().unwrap() > 0,
                "expected spilled_rows > 0 for {join_type:?}, batch_size={batch_size}"
            );
            let join_time = metrics
                .sum_by_name("join_time")
                .map(|m| m.as_usize())
                .unwrap_or(0);
            assert!(
                join_time > 0,
                "expected join_time > 0 for {join_type:?}, batch_size={batch_size}"
            );
            let output_rows = metrics.output_rows().unwrap_or(0);
            let collected_rows: usize = spilled_result.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                output_rows, collected_rows,
                "output_rows metric should match collected rows for \
                 {join_type:?}, batch_size={batch_size}"
            );

            // Run without spilling and compare results
            let task_ctx_no_spill = Arc::new(
                TaskContext::default().with_session_config(session_config.clone()),
            );
            let join_no_spill = SortMergeJoinExec::try_new(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                Some(filter.clone()),
                join_type,
                sort_options.clone(),
                NullEquality::NullEqualsNothing,
            )?;
            let stream = join_no_spill.execute(0, task_ctx_no_spill)?;
            let no_spill_result = common::collect(stream).await.unwrap();

            let no_spill_metrics = join_no_spill.metrics().unwrap();
            assert_eq!(
                no_spill_metrics.spill_count(),
                Some(0),
                "unexpected spill for {join_type:?} without memory limit"
            );

            assert_eq!(
                spilled_result, no_spill_result,
                "spilled vs non-spilled results differ for {join_type:?}, batch_size={batch_size}"
            );
        }
    }

    Ok(())
}

/// Semi/anti/mark joins use `BitwiseSortMergeJoinStream`, which buffers the
/// inner key group for filter evaluation. When that buffer exhausts the memory
/// pool and the `DiskManager` has spilling disabled, the stream must surface a
/// clear "Disk spilling disabled" error instead of spilling or panicking.
#[tokio::test]
async fn bitwise_filtered_no_spill() -> Result<()> {
    let FilteredBitwiseSpillFixture {
        left,
        right,
        on,
        sort_options,
        filter,
    } = filtered_bitwise_spill_fixture()?;

    // Tiny memory pool with the DiskManager disabled: spilling is impossible.
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()?;
    let session_config = SessionConfig::default().with_batch_size(1);

    for join_type in [
        LeftSemi, LeftAnti, RightSemi, RightAnti, LeftMark, RightMark,
    ] {
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(session_config.clone())
                .with_runtime(Arc::clone(&runtime)),
        );

        let join = SortMergeJoinExec::try_new(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            Some(filter.clone()),
            join_type,
            sort_options.clone(),
            NullEquality::NullEqualsNothing,
        )?;
        let stream = join.execute(0, task_ctx)?;
        let err = common::collect(stream).await.unwrap_err();

        assert_contains!(err.to_string(), "Failed to allocate additional");
        assert_contains!(err.to_string(), "SMJStream[0]");
        assert_contains!(err.to_string(), "Disk spilling disabled");
        let metrics = join
            .metrics()
            .unwrap_or_else(|| panic!("metrics missing for {join_type:?}"));
        assert_eq!(
            metrics.spill_count(),
            Some(0),
            "unexpected spill for {join_type:?} with disk disabled",
        );
        assert_eq!(
            metrics.spilled_bytes(),
            Some(0),
            "unexpected spilled bytes for {join_type:?} with disk disabled",
        );
        assert_eq!(
            metrics.spilled_rows(),
            Some(0),
            "unexpected spilled rows for {join_type:?} with disk disabled",
        );
    }

    Ok(())
}

/// A single inner key group spanning several inner batches can spill more
/// than once under memory pressure. Every spilled slice must still be
/// evaluated against the outer rows — an earlier spill file must not be
/// dropped when a later slice of the same group spills. A dropped slice
/// surfaces differently per join type: semi loses its row, anti resurrects
/// it, and mark flips to `false`.
#[tokio::test]
async fn bitwise_multi_spill_inner_key_group() -> Result<()> {
    // Outer: one row with key 1, c1 = 5.
    let left = build_table(("a1", &vec![1]), ("b1", &vec![1]), ("c1", &vec![5]));

    // Inner: one key group (b2 = 1) spanning two batches. Only the first
    // batch satisfies the filter c1 < c2 (5 < 10); the second (5 < 0) does
    // not, so the outcome depends on the group's first spilled slice.
    let right = build_table_from_batches(vec![
        build_table_i32(("a2", &vec![10]), ("b2", &vec![1]), ("c2", &vec![10])),
        build_table_i32(("a2", &vec![20]), ("b2", &vec![1]), ("c2", &vec![0])),
    ]);

    // Right variants stream right and buffer left, so mirror the two-batch
    // key group onto left. Again, only its first slice satisfies c1 < c2.
    let mirrored_left = build_table_from_batches(vec![
        build_table_i32(("a1", &vec![10]), ("b1", &vec![1]), ("c1", &vec![0])),
        build_table_i32(("a1", &vec![20]), ("b1", &vec![1]), ("c1", &vec![10])),
    ]);
    let mirrored_right =
        build_table(("a2", &vec![1]), ("b2", &vec![1]), ("c2", &vec![5]));

    let on: JoinOn = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];
    let filter = build_c1_lt_c2_filter(left.schema().as_ref(), right.schema().as_ref());

    // 100-byte pool: every buffered slice fails its reservation, so each
    // inner batch of the key group spills separately.
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;

    for (join_type, expected_rows) in [
        (LeftSemi, 1),
        (LeftAnti, 0),
        (LeftMark, 1),
        (RightSemi, 1),
        (RightAnti, 0),
        (RightMark, 1),
    ] {
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::default().with_batch_size(1))
                .with_runtime(Arc::clone(&runtime)),
        );
        let (join_left, join_right) =
            if matches!(join_type, RightSemi | RightAnti | RightMark) {
                (&mirrored_left, &mirrored_right)
            } else {
                (&left, &right)
            };

        let join = SortMergeJoinExec::try_new(
            Arc::clone(join_left),
            Arc::clone(join_right),
            on.clone(),
            Some(filter.clone()),
            join_type,
            sort_options.clone(),
            NullEquality::NullEqualsNothing,
        )?;
        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        let output_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            output_rows, expected_rows,
            "unexpected output rows for {join_type:?}",
        );
        if matches!(join_type, LeftMark | RightMark) {
            let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
            let mark = batch
                .column_by_name("mark")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            assert!(
                mark.value(0),
                "mark must be true: the row matches the group's first spilled slice",
            );
        }

        let metrics = join.metrics().expect("must have metrics");
        assert_eq!(
            metrics.spill_count(),
            Some(1),
            "all overflows of one key group must share a single spill file for {join_type:?}",
        );
        assert_eq!(
            metrics.spilled_rows(),
            Some(2),
            "both inner slices of the group must be spilled for {join_type:?}",
        );
    }
    Ok(())
}

/// Under `NullEqualsNull`, a NULL-key inner group spanning a batch boundary
/// must behave like any equal-key group through the spill path: one group,
/// one spill file, matched by a NULL-key outer row.
#[tokio::test]
async fn bitwise_spill_null_key_group() -> Result<()> {
    // Outer: one row whose join key is NULL, c1 = 5.
    let left = build_table_i32_nullable(
        ("a1", &vec![Some(1)]),
        ("b1", &vec![None]),
        ("c1", &vec![Some(5)]),
    );

    // Inner: one NULL-key group spanning two batches; only the first batch
    // satisfies the filter (5 < 10), so the result depends on the group's
    // first spilled slice.
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("a2", DataType::Int32, true),
        Field::new("b2", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
    ]));
    let right_batches = vec![
        RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(10)])),
                Arc::new(Int32Array::from(vec![None::<i32>])),
                Arc::new(Int32Array::from(vec![Some(10)])),
            ],
        )?,
        RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(20)])),
                Arc::new(Int32Array::from(vec![None::<i32>])),
                Arc::new(Int32Array::from(vec![Some(0)])),
            ],
        )?,
    ];
    let right = build_table_from_batches(right_batches);

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];
    let filter = build_c1_lt_c2_filter(left.schema().as_ref(), right.schema().as_ref());

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::default().with_batch_size(1))
            .with_runtime(runtime),
    );

    let join = SortMergeJoinExec::try_new(
        left,
        right,
        on,
        Some(filter),
        LeftSemi,
        sort_options,
        NullEquality::NullEqualsNull,
    )?;
    let stream = join.execute(0, task_ctx)?;
    let batches = common::collect(stream).await?;

    let output_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        output_rows, 1,
        "NULL-key outer row must match the spilled NULL-key group under NullEqualsNull",
    );

    let metrics = join.metrics().expect("must have metrics");
    assert_eq!(
        metrics.spill_count(),
        Some(1),
        "the NULL-key group must span the batch boundary as one group with one spill file",
    );
    assert_eq!(
        metrics.spilled_rows(),
        Some(2),
        "both NULL-key slices must be spilled",
    );
    Ok(())
}

/// Once the inner key group has spilled, an outer key group spanning a batch
/// boundary must still be evaluated against the spilled inner rows — the
/// second outer batch's rows must not be treated as having no inner group to
/// match against.
///
/// Setup:
/// - Outer: 2 single-row batches, both key=1, c1=[10, 10]
/// - Inner: 1 batch with many rows all key=1 (enough to trigger spill)
/// - Filter: c1 == c2 (matches when c2=10)
/// - Memory limit: tiny (100 bytes) to force spilling
/// - Pending before 2nd outer batch, while the key group is still open
///
/// Expected: both outer rows match (semi=2 rows, anti=0 rows)
#[tokio::test]
async fn spill_filtered_boundary_loses_outer_rows() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c1", DataType::Int32, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("a2", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c2", DataType::Int32, false),
    ]));

    // Two single-row outer batches with the same key -- key group spans boundary
    let outer_batch1 = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![1])), // key=1
            Arc::new(Int32Array::from(vec![10])), // matches filter
        ],
    )?;
    let outer_batch2 = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![1])), // same key=1
            Arc::new(Int32Array::from(vec![10])), // also matches filter
        ],
    )?;

    // Inner: many rows with key=1 to force spilling, followed by key=2.
    // c2=10 so the filter c1==c2 passes for both outer rows.
    // The key=2 row ensures the inner cursor advances past the key group
    // (buffer_inner_key_group returns Ok(false) instead of Ok(true)).
    let n_inner = 200;
    let mut inner_a = vec![100; n_inner];
    inner_a.push(101);
    let mut inner_b = vec![1; n_inner];
    inner_b.push(2); // different key -- forces inner cursor past key=1
    let mut inner_c = vec![10; n_inner];
    inner_c.push(10);
    let inner_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(inner_a)),
            Arc::new(Int32Array::from(inner_b)),
            Arc::new(Int32Array::from(inner_c)),
        ],
    )?;

    // Filter: c1 == c2
    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c1", 0)),
            Operator::Eq,
            Arc::new(Column::new("c2", 1)),
        )),
        vec![
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
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ])),
    );

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;

    let on_outer: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];
    let on_inner: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];

    for join_type in [LeftSemi, LeftAnti] {
        let outer: SendableRecordBatchStream = Box::pin(PendingStream::new(
            vec![outer_batch1.clone(), outer_batch2.clone()],
            vec![false, true], // Pending before 2nd outer batch
        ));
        let inner: SendableRecordBatchStream =
            Box::pin(PendingStream::new(vec![inner_batch.clone()], vec![false]));

        let metrics = ExecutionPlanMetricsSet::new();
        let reservation = MemoryConsumer::new("test").register(&runtime.memory_pool);
        let spill_manager = SpillManager::new(
            Arc::clone(&runtime),
            SpillMetrics::new(&metrics, 0),
            Arc::clone(&right_schema),
        );

        let stream = BitwiseSortMergeJoinStream::try_new(
            Arc::clone(&left_schema),
            vec![SortOptions::default()],
            NullEquality::NullEqualsNothing,
            outer,
            inner,
            on_outer.clone(),
            on_inner.clone(),
            Some(filter.clone()),
            join_type,
            8192,
            0,
            &metrics,
            reservation,
            spill_manager,
            Arc::clone(&runtime),
        )?;

        let batches = collect_stream(stream).await?;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();

        match join_type {
            LeftSemi => {
                assert_eq!(
                    total, 2,
                    "LeftSemi spill+boundary: both outer rows match filter, \
                     expected 2 rows, got {total}"
                );
            }
            LeftAnti => {
                assert_eq!(
                    total, 0,
                    "LeftAnti spill+boundary: both outer rows match filter, \
                     expected 0 rows, got {total}"
                );
            }
            _ => unreachable!(),
        }
    }

    Ok(())
}

/// Verifies that `peak_mem_used` reflects spill read-back memory during
/// output materialization (multi-source path).
///
/// When spilled buffered batches are read back from disk to produce join
/// output, a scoped `MemoryReservation` (via `new_empty()`) tracks the
/// transient memory. Its `Drop` guarantees the pool is balanced on every
/// exit path — normal return or early `?` error.
#[tokio::test]
async fn spill_read_back_memory_accounting() -> Result<()> {
    use arrow::array::Array;

    let left_batch = build_table_i32(
        ("a1", &vec![0, 1]),
        ("b1", &vec![1, 1]),
        ("c1", &vec![4, 5]),
    );
    let size_estimation = left_batch.get_array_memory_size()
        + Int32Array::from(vec![1, 1]).get_array_memory_size()
        + 2usize.next_power_of_two() * size_of::<usize>()
        + size_of::<std::ops::Range<usize>>()
        + size_of::<usize>();

    // Memory limit too small for a full batch — forces spilling.
    let memory_limit = size_estimation / 2;

    // All rows share the same join key (b=1) to force multiple buffered
    // batches in the same key group — triggering spill read-back during
    // output materialization.
    let left_batches: Vec<RecordBatch> = (0..4)
        .map(|i| {
            build_table_i32(
                ("a1", &vec![i * 2, i * 2 + 1]),
                ("b1", &vec![1, 1]),
                ("c1", &vec![100 + i, 101 + i]),
            )
        })
        .collect();
    let left = build_table_from_batches(left_batches);

    let right_batches: Vec<RecordBatch> = (0..4)
        .map(|i| {
            build_table_i32(
                ("a2", &vec![i * 2, i * 2 + 1]),
                ("b2", &vec![1, 1]),
                ("c2", &vec![200 + i, 201 + i]),
            )
        })
        .collect();
    let right = build_table_from_batches(right_batches);

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(memory_limit, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;

    let session_config = SessionConfig::default().with_batch_size(50);
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(session_config)
            .with_runtime(Arc::clone(&runtime)),
    );

    let join = join_with_options(
        Arc::clone(&left),
        Arc::clone(&right),
        on.clone(),
        Inner,
        sort_options,
        NullEquality::NullEqualsNothing,
    )?;

    let stream = join.execute(0, task_ctx)?;
    let result = common::collect(stream).await.unwrap();

    assert!(!result.is_empty(), "Expected non-empty join result");

    let metrics = join.metrics().unwrap();
    assert!(
        metrics.spill_count().unwrap() > 0,
        "Expected spilling to occur"
    );

    // peak_mem_used should reflect the spill read-back: when buffered
    // batches are read from disk during output materialization, grow()
    // temporarily reserves size_estimation. This pushes peak above what
    // join_arrays_mem alone would show.
    let peak_mem = metrics
        .sum_by_name("peak_mem_used")
        .map(|m| m.as_usize())
        .unwrap_or(0);
    assert!(
        peak_mem >= size_estimation,
        "peak_mem_used ({peak_mem}) should be >= size_estimation ({size_estimation}) \
         because spill read-back temporarily loads full batch into memory"
    );

    // All memory must be released (grow/shrink balanced)
    assert_eq!(
        runtime.memory_pool.reserved(),
        0,
        "All memory should be released after join completes"
    );

    Ok(())
}

/// Verifies spill read-back memory tracking for the single-source path.
///
/// When only ONE buffered batch exists for a key group and it's spilled,
/// `fetch_right_columns_by_idxs` reads it back. A scoped `MemoryReservation`
/// (via `new_empty()`) tracks the transient memory and releases it on drop.
#[tokio::test]
async fn spill_read_back_single_source() -> Result<()> {
    use arrow::array::Array;

    let left_batch = build_table_i32(
        ("a1", &vec![0, 1]),
        ("b1", &vec![1, 1]),
        ("c1", &vec![4, 5]),
    );
    let size_estimation = left_batch.get_array_memory_size()
        + Int32Array::from(vec![1, 1]).get_array_memory_size()
        + 2usize.next_power_of_two() * size_of::<usize>()
        + size_of::<std::ops::Range<usize>>()
        + size_of::<usize>();

    // Memory limit too small for a full batch — forces spilling.
    let memory_limit = size_estimation / 2;

    // Multiple distinct keys so each key group has exactly ONE buffered batch.
    // This ensures the single-source path is exercised.
    let left_batches: Vec<RecordBatch> = (0..4)
        .map(|i| {
            build_table_i32(
                ("a1", &vec![i * 2, i * 2 + 1]),
                ("b1", &vec![i, i]),
                ("c1", &vec![100 + i, 101 + i]),
            )
        })
        .collect();
    let left = build_table_from_batches(left_batches);

    // One batch per key — each key group has single source
    let right_batches: Vec<RecordBatch> = (0..4)
        .map(|i| {
            build_table_i32(
                ("a2", &vec![i * 2, i * 2 + 1]),
                ("b2", &vec![i, i]),
                ("c2", &vec![200 + i, 201 + i]),
            )
        })
        .collect();
    let right = build_table_from_batches(right_batches);

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(memory_limit, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;

    let session_config = SessionConfig::default().with_batch_size(50);
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(session_config)
            .with_runtime(Arc::clone(&runtime)),
    );

    let join = join_with_options(
        Arc::clone(&left),
        Arc::clone(&right),
        on.clone(),
        Inner,
        sort_options,
        NullEquality::NullEqualsNothing,
    )?;

    let stream = join.execute(0, task_ctx)?;
    let result = common::collect(stream).await.unwrap();

    assert!(!result.is_empty(), "Expected non-empty join result");

    let metrics = join.metrics().unwrap();
    assert!(
        metrics.spill_count().unwrap() > 0,
        "Expected spilling to occur"
    );

    // peak_mem_used should reflect the single-batch read-back
    let peak_mem = metrics
        .sum_by_name("peak_mem_used")
        .map(|m| m.as_usize())
        .unwrap_or(0);
    assert!(
        peak_mem >= size_estimation,
        "peak_mem_used ({peak_mem}) should be >= size_estimation ({size_estimation}) \
         because single-source spill read-back loads full batch"
    );

    // All memory must be released
    assert_eq!(
        runtime.memory_pool.reserved(),
        0,
        "All memory should be released after join completes"
    );

    Ok(())
}

/// Small chunk size so even tiny test spill files are split into several
/// pieces, forcing multiple genuine suspend/resume cycles instead of one.
const PENDING_CHUNK_SIZE: usize = 16;

/// Splits real spill bytes into fixed-size chunks and yields `Poll::Pending`
/// before every chunk
struct PendingChunkedStream {
    chunks: VecDeque<Bytes>,
    yield_pending: bool,
}

impl PendingChunkedStream {
    fn new(bytes: Bytes) -> Self {
        let mut chunks = VecDeque::new();
        if bytes.is_empty() {
            chunks.push_back(bytes);
        } else {
            let mut remaining = bytes;
            while !remaining.is_empty() {
                let take = PENDING_CHUNK_SIZE.min(remaining.len());
                chunks.push_back(remaining.split_to(take));
            }
        }
        Self {
            chunks,
            yield_pending: true,
        }
    }
}

impl Stream for PendingChunkedStream {
    type Item = Result<Bytes>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.yield_pending {
            self.yield_pending = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        // Pending before every subsequent chunk as well.
        self.yield_pending = true;
        match self.chunks.pop_front() {
            Some(chunk) => Poll::Ready(Some(Ok(chunk))),
            None => Poll::Ready(None),
        }
    }
}

/// A `SpillFile` that delegates everything to a real local spill file,
/// except `read_stream`, which is forced through `PendingChunkedStream`.
struct PendingSpillFile {
    inner: Arc<dyn SpillFile>,
}

impl SpillFile for PendingSpillFile {
    fn path(&self) -> Option<&std::path::Path> {
        self.inner.path()
    }

    fn size(&self) -> Option<u64> {
        self.inner.size()
    }

    fn read_stream(&self) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>> {
        let path = self
            .inner
            .path()
            .expect("PendingSpillFile only wraps local files")
            .to_owned();

        let stream = futures::stream::once(async move {
            tokio::fs::read(&path)
                .await
                .map(Bytes::from)
                .map_err(datafusion_common::DataFusionError::IoError)
        })
        .flat_map(
            |read_result| -> Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>> {
                match read_result {
                    Ok(bytes) => Box::pin(PendingChunkedStream::new(bytes)),
                    Err(e) => Box::pin(futures::stream::once(async move { Err(e) })),
                }
            },
        );

        Ok(Box::pin(stream))
    }

    fn open_writer(&self) -> Result<Box<dyn SpillWriter>> {
        self.inner.open_writer()
    }
}

/// Wraps the default `OsTmpDirectory` factory so every spill file it
/// creates is a [`PendingSpillFile`].
struct PendingTempFileFactory {
    inner: Arc<DiskManager>,
}

impl TempFileFactory for PendingTempFileFactory {
    fn create_temp_file(&self, description: &str) -> Result<Arc<dyn SpillFile>> {
        Ok(Arc::new(PendingSpillFile {
            inner: self.inner.create_tmp_file(description)?,
        }))
    }
}

fn pending_disk_manager_builder() -> DiskManagerBuilder {
    let inner = Arc::new(
        DiskManagerBuilder::default()
            .with_mode(DiskManagerMode::OsTmpDirectory)
            .build()
            .unwrap(),
    );
    DiskManagerBuilder::default().with_mode(DiskManagerMode::Custom(Arc::new(
        PendingTempFileFactory { inner },
    )))
}

/// Materializing-side (Inner/Left/Right/Full) coverage: identical to
/// `overallocation_multi_batch_spill`, but every spill read goes through
/// `PendingSpillFile`, so `poll_spilled_batches` must actually hit and
/// recover from `Poll::Pending` mid-read.
#[tokio::test]
async fn materializing_spill_pending_stream() -> Result<()> {
    let left_batch_1 = build_table_i32(
        ("a1", &vec![0, 1]),
        ("b1", &vec![1, 1]),
        ("c1", &vec![4, 5]),
    );
    let left_batch_2 = build_table_i32(
        ("a1", &vec![2, 3]),
        ("b1", &vec![1, 1]),
        ("c1", &vec![6, 7]),
    );
    let right_batch_1 = build_table_i32(
        ("a2", &vec![0, 10]),
        ("b2", &vec![1, 1]),
        ("c2", &vec![50, 60]),
    );
    let right_batch_2 = build_table_i32(
        ("a2", &vec![20, 30]),
        ("b2", &vec![1, 1]),
        ("c2", &vec![70, 80]),
    );
    let left = build_table_from_batches(vec![left_batch_1, left_batch_2]);
    let right = build_table_from_batches(vec![right_batch_1, right_batch_2]);
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(500, 1.0)
        .with_disk_manager_builder(pending_disk_manager_builder())
        .build_arc()?;

    for join_type in [Inner, Left, Right, Full] {
        let task_ctx =
            Arc::new(TaskContext::default().with_runtime(Arc::clone(&runtime)));
        let join = join_with_options(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            join_type,
            sort_options.clone(),
            NullEquality::NullEqualsNothing,
        )?;
        let stream = join.execute(0, task_ctx)?;
        let spilled_result = common::collect(stream).await.unwrap();

        let metrics = join.metrics().unwrap();
        assert!(
            metrics.spill_count().unwrap() > 0,
            "expected spill_count > 0 for {join_type:?}"
        );

        // Compare against a no-spill run to make sure waiting on the
        // spill reads didn't corrupt or drop any data.
        let task_ctx_no_spill = Arc::new(TaskContext::default());
        let join_no_spill = join_with_options(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            join_type,
            sort_options.clone(),
            NullEquality::NullEqualsNothing,
        )?;
        let stream = join_no_spill.execute(0, task_ctx_no_spill)?;
        let no_spill_result = common::collect(stream).await.unwrap();

        assert_eq!(
            spilled_result, no_spill_result,
            "Pending-forced spill read produced different results for {join_type:?}"
        );
    }

    Ok(())
}

/// Bitwise-side (Semi/Anti) coverage: identical to `bitwise_spill_with_filter`,
/// but every spill read goes through `PendingSpillFile`, so reading the
/// spilled inner rows back must actually hit and recover from `Poll::Pending`
/// mid-read.
#[tokio::test]
async fn bitwise_spill_pending_stream() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 3, 4, 5, 6]),
        ("b1", &vec![1, 2, 3, 4, 5, 6]),
        ("c1", &vec![4, 5, 6, 7, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30, 40, 50]),
        ("b1", &vec![1, 3, 4, 6, 8]),
        ("c2", &vec![50, 60, 70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    // c1 < c2 is always true for matching keys — same filter as
    // bitwise_spill_with_filter, so the inner key group is buffered
    // (and spilled) rather than short-circuited.
    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c1", 0)),
            Operator::Lt,
            Arc::new(Column::new("c2", 1)),
        )),
        vec![
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
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ])),
    );

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .with_disk_manager_builder(pending_disk_manager_builder())
        .build_arc()?;

    for join_type in [LeftSemi, LeftAnti, RightSemi, RightAnti] {
        let task_ctx =
            Arc::new(TaskContext::default().with_runtime(Arc::clone(&runtime)));
        let join = SortMergeJoinExec::try_new(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            Some(filter.clone()),
            join_type,
            sort_options.clone(),
            NullEquality::NullEqualsNothing,
        )?;
        let stream = join.execute(0, task_ctx)?;
        let spilled_result = common::collect(stream).await.unwrap();

        let metrics = join.metrics().unwrap();
        assert!(
            metrics.spill_count().unwrap() > 0,
            "expected spill_count > 0 for {join_type:?}"
        );

        let task_ctx_no_spill = Arc::new(TaskContext::default());
        let join_no_spill = SortMergeJoinExec::try_new(
            Arc::clone(&left),
            Arc::clone(&right),
            on.clone(),
            Some(filter.clone()),
            join_type,
            sort_options.clone(),
            NullEquality::NullEqualsNothing,
        )?;
        let stream = join_no_spill.execute(0, task_ctx_no_spill)?;
        let no_spill_result = common::collect(stream).await.unwrap();

        assert_eq!(
            spilled_result, no_spill_result,
            "Pending-forced spill read produced different results for {join_type:?}"
        );
    }

    Ok(())
}

/// A projection names the columns to emit, so swapping the inputs must renumber it
/// rather than leave it pointing at the columns the other side now occupies.
#[tokio::test]
async fn swap_inputs_swaps_the_projection() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 3]),
        ("b1", &vec![10, 20, 30]),
        ("c1", &vec![100, 200, 300]),
    );
    let right = build_table(
        ("a2", &vec![1, 2, 3]),
        ("b2", &vec![11, 22, 33]),
        ("c2", &vec![111, 222, 333]),
    );
    let on: JoinOn = vec![(
        Arc::new(Column::new("a1", 0)) as _,
        Arc::new(Column::new("a2", 0)) as _,
    )];
    // One column from each side, in an order that tells the two sides apart.
    let join = SortMergeJoinExec::try_new(
        left,
        right,
        on,
        None,
        Inner,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
    )?
    .with_projection(Some(vec![4, 2]))?;

    let swapped = join.swap_inputs()?;
    assert_eq!(
        swapped.schema().fields(),
        join.schema().fields(),
        "swapping must not change what the join emits"
    );

    let task_ctx = Arc::new(TaskContext::default());
    let expected = common::collect(join.execute(0, Arc::clone(&task_ctx))?).await?;
    let actual = common::collect(swapped.execute(0, task_ctx)?).await?;
    assert_eq!(expected, actual);

    Ok(())
}

/// An empty projection still changes the output schema, and the row count has to
/// survive it: `SELECT count(1)` over a join needs the rows but none of the columns.
#[tokio::test]
async fn an_empty_projection_keeps_the_rows() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 3]),
        ("b1", &vec![10, 20, 30]),
        ("c1", &vec![100, 200, 300]),
    );
    let right = build_table(
        ("a2", &vec![1, 2, 3]),
        ("b2", &vec![11, 22, 33]),
        ("c2", &vec![111, 222, 333]),
    );
    let on: JoinOn = vec![(
        Arc::new(Column::new("a1", 0)) as _,
        Arc::new(Column::new("a2", 0)) as _,
    )];
    let join = SortMergeJoinExec::try_new(
        left,
        right,
        on,
        None,
        Inner,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
    )?
    .with_projection(Some(vec![]))?;

    assert_eq!(join.schema().fields().len(), 0);
    let batches =
        common::collect(join.execute(0, Arc::new(TaskContext::default()))?).await?;
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 3);

    Ok(())
}

/// Number of distinct join keys used by the streamed-order regression tests.
const ORDER_KEYS: i32 = 7;

/// Streamed side of the streamed-order tests: one row per key, ascending.
fn order_unique_side(names: [&str; 3]) -> RecordBatch {
    let keys: Vec<i32> = (0..ORDER_KEYS).collect();
    build_table_i32((names[0], &keys), (names[1], &keys), (names[2], &keys))
}

/// Buffered side of the streamed-order tests.
///
/// Keys 0..5 carry 20 rows each — wide enough that the deferred-filter gate
/// fires once per key and leaves a partial batch sitting in `output` — while
/// keys 5 and 6 carry a single row each, so their output only ever leaves
/// through the final flush. Mixing the two paths is what exposes reordering
/// between them.
fn order_skewed_side(names: [&str; 3]) -> RecordBatch {
    let (mut a, mut b, mut c) = (vec![], vec![], vec![]);
    for k in 0..ORDER_KEYS {
        for j in 0..if k < 5 { 20 } else { 1 } {
            a.push(k * 100 + j);
            b.push(k);
            c.push(j);
        }
    }
    build_table_i32((names[0], &a), (names[1], &b), (names[2], &c))
}

/// Run a deferred-filtered outer join over the skew shape above and return
/// the streamed key column of the output, concatenated across batches.
///
/// The filter is `<filter_column> < filter_lt` over the intermediate schema.
async fn collect_streamed_keys(
    join_type: JoinType,
    filter_column: ColumnIndex,
    filter_lt: i32,
) -> Result<Vec<i32>> {
    // RIGHT streams its *right* input (`maintains_input_order = [false, true]`),
    // so the duplicate groups always belong on whichever side is buffered.
    let (left, right) = if join_type == Right {
        (
            order_skewed_side(["a1", "b1", "c1"]),
            order_unique_side(["a2", "b2", "c2"]),
        )
    } else {
        (
            order_unique_side(["a1", "b1", "c1"]),
            order_skewed_side(["a2", "b2", "c2"]),
        )
    };

    let (left_schema, right_schema) = (left.schema(), right.schema());
    let left = TestMemoryExec::try_new_exec(&[vec![left]], left_schema, None)?;
    let right = TestMemoryExec::try_new_exec(&[vec![right]], right_schema, None)?;

    let on: JoinOn = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
    )];

    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(filter_lt)))),
        )) as PhysicalExprRef,
        vec![filter_column],
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)])),
    );

    let join = SortMergeJoinExec::try_new(
        left,
        right,
        on,
        Some(filter),
        join_type,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
    )?;

    // A small batch size keeps the gate firing often enough to interleave the
    // two output paths.
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::default().with_batch_size(8)),
    );
    let batches = common::collect(join.execute(0, task_ctx)?).await?;

    // Output is always [left cols.., right cols..], so the streamed key is
    // `a2` at index 3 for RIGHT and `a1` at index 0 otherwise.
    let key_col = if join_type == Right { 3 } else { 0 };
    Ok(batches
        .iter()
        .flat_map(|b| {
            b.column(key_col)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect())
}

/// `a1 < 0`, which never passes — so every streamed row is emitted
/// null-joined by the deferred-filtering pipeline.
fn never_passing_filter() -> (ColumnIndex, i32) {
    (
        ColumnIndex {
            index: 0,
            side: JoinSide::Left,
        },
        0,
    )
}

/// Regression test: deferred-filtered outer joins must not reorder their
/// output.
///
/// `LEFT JOIN` advertises `maintains_input_order = [true, false]`, so the
/// output must stay ordered on the streamed side. The final flush used to
/// emit its batch directly instead of through the `output` coalescer, so any
/// rows still buffered there from an earlier flush were emitted *after* it.
#[tokio::test]
async fn left_join_with_filter_preserves_streamed_order() -> Result<()> {
    let (filter_column, filter_lt) = never_passing_filter();
    let streamed_keys = collect_streamed_keys(Left, filter_column, filter_lt).await?;

    assert_eq!(
        streamed_keys,
        (0..ORDER_KEYS).collect::<Vec<_>>(),
        "LEFT JOIN output must stay ordered on the streamed side"
    );
    Ok(())
}

/// Mirror of [`left_join_with_filter_preserves_streamed_order`] for
/// `RIGHT JOIN`, which advertises `maintains_input_order = [false, true]` and
/// therefore streams its *right* input.
#[tokio::test]
async fn right_join_with_filter_preserves_streamed_order() -> Result<()> {
    let (filter_column, filter_lt) = never_passing_filter();
    let streamed_keys = collect_streamed_keys(Right, filter_column, filter_lt).await?;

    assert_eq!(
        streamed_keys,
        (0..ORDER_KEYS).collect::<Vec<_>>(),
        "RIGHT JOIN output must stay ordered on the streamed side"
    );
    Ok(())
}

/// Same shape, but with a filter that passes for *some* rows. The all-fail
/// cases above only exercise the null-joined path; here matched rows survive
/// the filter too, so the output mixes filter-passing and null-joined rows.
#[tokio::test]
async fn left_join_with_partial_filter_preserves_streamed_order() -> Result<()> {
    // `c2 < 3`: keys 0..5 keep three of their twenty buffered rows, keys 5
    // and 6 keep their single row.
    let filter_column = ColumnIndex {
        index: 2,
        side: JoinSide::Right,
    };
    let streamed_keys = collect_streamed_keys(Left, filter_column, 3).await?;

    let expected: Vec<i32> = (0..ORDER_KEYS)
        .flat_map(|k| std::iter::repeat_n(k, if k < 5 { 3 } else { 1 }))
        .collect();
    assert_eq!(
        streamed_keys, expected,
        "LEFT JOIN output must stay ordered on the streamed side, \
         with every surviving match present exactly once"
    );
    Ok(())
}
