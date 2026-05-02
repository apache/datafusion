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

use super::bitwise_stream::BitwiseSortMergeJoinStream;
use crate::joins::utils::{ColumnIndex, JoinFilter, JoinOn};
use crate::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use crate::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
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
use datafusion_common::JoinType::*;
use datafusion_common::{
    JoinSide, internal_err,
    test_util::{batches_to_sort_string, batches_to_string},
};
use datafusion_common::{
    JoinType, NullEquality, Result, ScalarValue, assert_batches_eq, assert_contains,
};
use datafusion_common_runtime::JoinSet;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::BinaryExpr;
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_expr_common::physical_expr::PhysicalExprRef;
use futures::{Stream, StreamExt};
use insta::assert_snapshot;
use itertools::Itertools;

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
            Arc::new(FixedSizeBinaryArray::from(a.1.clone())),
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
    use crate::ExecutionPlan;
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
        let stats = join_exec.partition_statistics(None)?;
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
        // Since the child TestMemoryExec returns unknown stats for specific partitions,
        // the join output will also have Absent num_rows. This is expected behavior
        // as the statistics depend on what the children can provide.
        let partition_stats = join_exec.partition_statistics(Some(0))?;
        assert_eq!(
            partition_stats.column_statistics.len(),
            expected_cols,
            "Partition stats column count failed for {join_type:?}"
        );
        // When children return unknown stats, the join's partition stats will be Absent
        assert!(
            partition_stats.num_rows == Precision::Absent,
            "Partition stats should have Absent num_rows when children return unknown for {join_type:?}, got {:?}",
            partition_stats.num_rows
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

    let mut start_time_since_last_ready = datafusion_common::instant::Instant::now();
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
                start_time_since_last_ready = datafusion_common::instant::Instant::now();
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
                if start_time_since_last_ready.elapsed()
                    > std::time::Duration::from_secs(5)
                {
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

/// Returns the column names on the schema
fn columns(schema: &Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

// ==================== BitwiseSortMergeJoinStream direct tests ====================
//
// These tests construct a BitwiseSortMergeJoinStream directly (bypassing exec)
// to exercise async re-entry and spill edge cases using PendingStream.

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
async fn collect_stream(stream: BitwiseSortMergeJoinStream) -> Result<Vec<RecordBatch>> {
    common::collect(Box::pin(stream)).await
}

/// Reproduces the buffer_inner_key_group re-entry bug:
///
/// When buffer_inner_key_group buffers inner rows across batch boundaries
/// and poll_next_inner_batch returns Pending mid-way, the ready! macro
/// exits poll_join. On re-entry, the merge-scan reaches Equal again and
/// calls buffer_inner_key_group a second time -- which starts with
/// clear(), destroying the partially collected inner rows. Previously
/// consumed batches are gone, so re-buffering misses them.
///
/// Setup:
/// - Inner: 3 single-row batches, all with key=1, filter values c2=[10, 20, 30]
/// - Outer: 1 row, key=1, filter value c1=10
/// - Filter: c1 == c2 (only first inner row c2=10 matches)
/// - Pending injected before 3rd inner batch
///
/// Without the bug: outer row emitted (match via c2=10)
/// With the bug: outer row missing (c2=10 batch lost on re-entry)
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

/// Reproduces the no-filter boundary Pending re-entry bug:
///
/// When an outer key group spans a batch boundary, the no-filter path
/// emits the current batch, then polls for the next outer batch. If
/// poll returns Pending, poll_join exits. On re-entry, without the
/// PendingBoundary fix, the new batch is processed fresh by the
/// merge-scan. Since inner already advanced past this key, the outer
/// rows with the matching key are skipped via Ordering::Less.
///
/// Setup:
/// - Outer: 2 single-row batches, both with key=1 (key group spans boundary)
/// - Inner: 1 row with key=1
/// - Pending injected on outer before 2nd batch
///
/// Without fix: only first outer row emitted (second lost on re-entry)
/// With fix: both outer rows emitted
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

/// Tests the filtered boundary Pending re-entry: outer key group spans
/// batches with a filter, and poll_next_outer_batch returns Pending.
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

/// Exercises inner key group spilling under memory pressure.
///
/// Uses a tiny memory limit (100 bytes) with disk spilling enabled. Since our
/// operator only buffers inner rows when a filter is present, this test includes
/// a filter (c1 < c2, always true). Verifies:
/// 1. Spill metrics are recorded (spill_count, spilled_bytes, spilled_rows > 0)
/// 2. Results match a non-spilled run
#[tokio::test]
async fn bitwise_spill_with_filter() -> Result<()> {
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

    // c1 < c2 is always true for matching keys
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

/// Reproduces a bug where `resume_boundary` for the Filtered pending case
/// only checks `inner_key_buffer.is_empty()` but ignores `inner_key_spill`.
/// After spilling, the in-memory buffer is cleared while the spill file
/// holds the data. If the outer key group spans a batch boundary, the
/// second outer batch's rows are never evaluated against the inner group.
///
/// Setup:
/// - Outer: 2 single-row batches, both key=1, c1=[10, 10]
/// - Inner: 1 batch with many rows all key=1 (enough to trigger spill)
/// - Filter: c1 == c2 (matches when c2=10)
/// - Memory limit: tiny (100 bytes) to force spilling
/// - Pending before 2nd outer batch to trigger boundary re-entry
///
/// Expected: both outer rows match (semi=2 rows, anti=0 rows)
/// Bug: second outer row is skipped because resume_boundary sees empty
///      inner_key_buffer and skips re-evaluation.
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
