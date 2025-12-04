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

use std::sync::Arc;

use arrow::array::{
    builder::{BooleanBuilder, UInt64Builder},
    BinaryArray, BooleanArray, Date32Array, Date64Array, FixedSizeBinaryArray,
    Int32Array, RecordBatch, UInt64Array,
};
use arrow::compute::{filter_record_batch, BatchCoalescer, SortOptions};
use arrow::datatypes::{DataType, Field, Schema};

use datafusion_common::JoinType::*;
use datafusion_common::{
    assert_batches_eq, assert_contains, JoinType, NullEquality, Result,
};
use datafusion_common::{
    test_util::{batches_to_sort_string, batches_to_string},
    JoinSide,
};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::BinaryExpr;
use insta::{allow_duplicates, assert_snapshot};

use crate::{
    expressions::Column,
    joins::sort_merge_join::stream::{get_corrected_filter_mask, JoinedRecordBatches},
};

use crate::joins::utils::{ColumnIndex, JoinFilter, JoinOn};
use crate::joins::SortMergeJoinExec;
use crate::test::TestMemoryExec;
use crate::test::{build_table_i32, build_table_i32_two_cols};
use crate::{common, ExecutionPlan};

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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 3  | 5  | 9  | 20 | 5  | 80 |
            +----+----+----+----+----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b2 | c1 | a1 | b2 | c2 |
            +----+----+----+----+----+----+
            | 1  | 1  | 7  | 1  | 1  | 70 |
            | 2  | 2  | 8  | 2  | 2  | 80 |
            | 2  | 2  | 9  | 2  | 2  | 80 |
            +----+----+----+----+----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b2 | c1 | a1 | b2 | c2 |
            +----+----+----+----+----+----+
            | 1  | 1  | 7  | 1  | 1  | 70 |
            | 1  | 1  | 7  | 1  | 1  | 80 |
            | 1  | 1  | 8  | 1  | 1  | 70 |
            | 1  | 1  | 8  | 1  | 1  | 80 |
            +----+----+----+----+----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b2 | c1 | a1 | b2 | c2 |
            +----+----+----+----+----+----+
            | 1  | 1  |    | 1  | 1  | 70 |
            | 2  | 2  | 8  | 2  | 2  | 80 |
            | 2  | 2  | 9  | 2  | 2  | 80 |
            +----+----+----+----+----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b2 | c1 | a1 | b2 | c2 |
            +----+----+----+----+----+----+
            | 2  | 2  | 9  | 2  | 2  | 80 |
            | 2  | 2  | 8  | 2  | 2  | 80 |
            | 1  | 1  |    | 1  | 1  | 70 |
            | 1  |    | 1  | 1  |    | 10 |
            +----+----+----+----+----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b2 | c1 | a1 | b2 | c2 |
            +----+----+----+----+----+----+
            | 1  | 1  | 7  | 1  | 1  | 70 |
            | 2  | 2  | 8  | 2  | 2  | 80 |
            | 2  | 2  | 9  | 2  | 2  | 80 |
            +----+----+----+----+----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 3  | 7  | 9  |    |    |    |
            +----+----+----+----+----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 | c2 |
            +----+----+----+----+----+----+
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            |    |    |    | 30 | 6  | 90 |
            +----+----+----+----+----+----+
            "#);
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

    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b1 |
            +----+----+----+----+----+
            |    |    |    | 10 | 4  |
            | 21 | 5  | 8  | 20 | 5  |
            |    |    |    | 30 | 6  |
            +----+----+----+----+----+
            "#);
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

    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+
            | a2 | b1 | a1 | b1 | c1 |
            +----+----+----+----+----+
            | 10 | 4  | 1  | 4  | 7  |
            | 20 | 5  |    |    |    |
            | 30 | 6  |    |    |    |
            +----+----+----+----+----+
            "#);
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

    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+-------+
            | a2 | b1 | mark  |
            +----+----+-------+
            | 10 | 4  | true  |
            | 20 | 5  | false |
            | 30 | 6  | false |
            +----+----+-------+
            "#);
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

    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+-------+
            | a2 | b1 | mark  |
            +----+----+-------+
            | 10 | 4  | false |
            | 20 | 5  | true  |
            | 30 | 6  | false |
            +----+----+-------+
            "#);
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
    assert_snapshot!(batches_to_sort_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            |    |    |    | 30 | 6  | 90 |
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            | 3  | 7  | 9  |    |    |    |
            +----+----+----+----+----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+
            | a1 | b1 | c1 |
            +----+----+----+
            | 3  | 7  | 9  |
            | 5  | 7  | 11 |
            +----+----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+
            | a2 | b1 |
            +----+----+
            | 30 | 6  |
            +----+----+
            "#);

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
    assert_snapshot!(batches_to_string(&batches2), @r#"
            +----+----+----+
            | a2 | b1 | c2 |
            +----+----+----+
            | 30 | 6  | 90 |
            +----+----+----+
            "#);

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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+
            | a2 | b1 |
            +----+----+
            | 10 | 4  |
            | 20 | 5  |
            | 30 | 6  |
            +----+----+
            "#);

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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+
            | a1 | b1 | c2 |
            +----+----+----+
            | 1  | 10 | 20 |
            +----+----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+
            | a1 | b1 | c2 |
            +----+----+----+
            | 2  |    | 8  |
            +----+----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+
            | a1 | b1 | c2 |
            +----+----+----+
            | 3  |    | 9  |
            | 2  | 5  |    |
            | 2  | 5  | 8  |
            +----+----+----+
            "#);
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
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(batches[1].num_rows(), 1);
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+
            | a1 | b1 | c1 |
            +----+----+----+
            | 1  | 4  | 7  |
            | 2  | 5  | 8  |
            | 2  | 5  | 8  |
            +----+----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+
            | a1 | b1 | c1 |
            +----+----+----+
            | 1  | 4  | 7  |
            | 2  | 5  | 8  |
            | 2  | 5  | 8  |
            +----+----+----+
            "#);
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
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(batches[1].num_rows(), 1);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+-------+
            | a1 | b1 | c1 | mark  |
            +----+----+----+-------+
            | 1  | 4  | 7  | true  |
            | 2  | 5  | 8  | true  |
            | 2  | 5  | 8  | true  |
            | 3  | 7  | 9  | false |
            +----+----+----+-------+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+-------+
            | a2 | b1 | c2 | mark  |
            +----+----+----+-------+
            | 10 | 4  | 60 | true  |
            | 20 | 4  | 70 | true  |
            | 30 | 5  | 80 | true  |
            | 40 | 6  | 90 | false |
            +----+----+----+-------+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +---+---+---+----+---+----+
            | a | b | c | a  | b | c  |
            +---+---+---+----+---+----+
            | 1 | 4 | 7 | 10 | 1 | 70 |
            | 2 | 5 | 8 | 20 | 2 | 80 |
            +---+---+---+----+---+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +------------+------------+------------+------------+------------+------------+
            | a1         | b1         | c1         | a2         | b1         | c2         |
            +------------+------------+------------+------------+------------+------------+
            | 1970-01-02 | 2022-04-25 | 1970-01-08 | 1970-01-11 | 2022-04-25 | 1970-03-12 |
            | 1970-01-03 | 2022-04-26 | 1970-01-09 | 1970-01-21 | 2022-04-26 | 1970-03-22 |
            | 1970-01-04 | 2022-04-26 | 1970-01-10 | 1970-01-21 | 2022-04-26 | 1970-03-22 |
            +------------+------------+------------+------------+------------+------------+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
            | a1                      | b1                  | c1                      | a2                      | b1                  | c2                      |
            +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
            | 1970-01-01T00:00:00.001 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.007 | 1970-01-01T00:00:00.010 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.070 |
            | 1970-01-01T00:00:00.002 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.008 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |
            | 1970-01-01T00:00:00.003 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.009 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |
            +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +--------+----+----+--------+-----+----+
            | a1     | b1 | c1 | a1     | b2  | c2 |
            +--------+----+----+--------+-----+----+
            | c0ffee | 5  | 7  | c0ffee | 105 | 70 |
            | decade | 10 | 8  | decade | 110 | 80 |
            | facade | 15 | 9  | facade | 115 | 90 |
            +--------+----+----+--------+-----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +--------+----+----+--------+-----+----+
            | a1     | b1 | c1 | a1     | b2  | c2 |
            +--------+----+----+--------+-----+----+
            | c0ffee | 5  | 7  | c0ffee | 105 | 70 |
            | decade | 10 | 8  | decade | 110 | 80 |
            | facade | 15 | 9  | facade | 115 | 90 |
            +--------+----+----+--------+-----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
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
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
            +----+----+----+----+----+----+
            | a1 | b1 | c1 | a2 | b2 | c2 |
            +----+----+----+----+----+----+
            |    |    |    | 0  | 2  | 60 |
            | 1  | 4  | 7  | 10 | 4  | 70 |
            | 2  | 5  | 8  | 20 | 5  | 80 |
            |    |    |    | 30 | 6  | 90 |
            +----+----+----+----+----+----+
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
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
            "#);
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
    assert_snapshot!(batches_to_string(&batches), @r#"
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
            "#);
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
    assert_snapshot!(batches_to_sort_string(&batches), @r#"
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
            "#);
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
        Inner, Left, Right, RightSemi, Full, LeftSemi, LeftAnti, LeftMark, RightMark,
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
        Inner, Left, Right, RightSemi, Full, LeftSemi, LeftAnti, LeftMark, RightMark,
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
        Inner, Left, Right, RightSemi, Full, LeftSemi, LeftAnti, LeftMark, RightMark,
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
        Inner, Left, Right, RightSemi, Full, LeftSemi, LeftAnti, LeftMark, RightMark,
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

fn build_joined_record_batches() -> Result<JoinedRecordBatches> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Int32, true),
    ]));

    let mut batches = JoinedRecordBatches {
        joined_batches: BatchCoalescer::new(Arc::clone(&schema), 8192),
        filter_mask: BooleanBuilder::new(),
        row_indices: UInt64Builder::new(),
        batch_ids: vec![],
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
    batches.batch_ids.extend(vec![0; streamed_indices.len()]);
    batches
        .row_indices
        .extend(&UInt64Array::from(streamed_indices));

    let streamed_indices = vec![1];
    batches.batch_ids.extend(vec![0; streamed_indices.len()]);
    batches
        .row_indices
        .extend(&UInt64Array::from(streamed_indices));

    let streamed_indices = vec![0, 0];
    batches.batch_ids.extend(vec![1; streamed_indices.len()]);
    batches
        .row_indices
        .extend(&UInt64Array::from(streamed_indices));

    let streamed_indices = vec![0];
    batches.batch_ids.extend(vec![2; streamed_indices.len()]);
    batches
        .row_indices
        .extend(&UInt64Array::from(streamed_indices));

    let streamed_indices = vec![0, 0];
    batches.batch_ids.extend(vec![3; streamed_indices.len()]);
    batches
        .row_indices
        .extend(&UInt64Array::from(streamed_indices));

    batches
        .filter_mask
        .extend(&BooleanArray::from(vec![true, false]));
    batches.filter_mask.extend(&BooleanArray::from(vec![true]));
    batches
        .filter_mask
        .extend(&BooleanArray::from(vec![false, true]));
    batches.filter_mask.extend(&BooleanArray::from(vec![false]));
    batches
        .filter_mask
        .extend(&BooleanArray::from(vec![false, false]));

    Ok(batches)
}

#[tokio::test]
async fn test_left_outer_join_filtered_mask() -> Result<()> {
    let mut joined_batches = build_joined_record_batches()?;
    let schema = joined_batches.joined_batches.schema();

    let output = joined_batches.concat_batches(&schema)?;
    let out_mask = joined_batches.filter_mask.finish();
    let out_indices = joined_batches.row_indices.finish();

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
        &joined_batches.batch_ids,
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

    assert_snapshot!(batches_to_string(&[filtered_rb]), @r#"
                +---+----+---+----+
                | a | b  | x | y  |
                +---+----+---+----+
                | 1 | 10 | 1 | 11 |
                | 1 | 11 | 1 | 12 |
                | 1 | 12 | 1 | 13 |
                +---+----+---+----+
            "#);

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

    assert_snapshot!(batches_to_string(&[null_joined_batch]), @r#"
                +---+----+---+----+
                | a | b  | x | y  |
                +---+----+---+----+
                | 1 | 13 | 1 | 12 |
                | 1 | 14 | 1 | 11 |
                +---+----+---+----+
            "#);
    Ok(())
}

#[tokio::test]
async fn test_semi_join_filtered_mask() -> Result<()> {
    for join_type in [LeftSemi, RightSemi] {
        let mut joined_batches = build_joined_record_batches()?;
        let schema = joined_batches.joined_batches.schema();

        let output = joined_batches.concat_batches(&schema)?;
        let out_mask = joined_batches.filter_mask.finish();
        let out_indices = joined_batches.row_indices.finish();

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0]),
                &[0usize],
                &BooleanArray::from(vec![true]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![true])
        );

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0]),
                &[0usize],
                &BooleanArray::from(vec![false]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![None])
        );

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0, 0]),
                &[0usize; 2],
                &BooleanArray::from(vec![true, true]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![Some(true), None])
        );

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![true, true, true]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![Some(true), None, None])
        );

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![true, false, true]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![Some(true), None, None])
        );

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![false, false, true]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![None, None, Some(true),])
        );

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![false, true, true]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![None, Some(true), None])
        );

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![false, false, false]),
                output.num_rows()
            )
            .unwrap(),
            BooleanArray::from(vec![None, None, None])
        );

        let corrected_mask = get_corrected_filter_mask(
            join_type,
            &out_indices,
            &joined_batches.batch_ids,
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
                None,
                None,
                None
            ])
        );

        let filtered_rb = filter_record_batch(&output, &corrected_mask)?;

        assert_batches_eq!(
            &[
                "+---+----+---+----+",
                "| a | b  | x | y  |",
                "+---+----+---+----+",
                "| 1 | 10 | 1 | 11 |",
                "| 1 | 11 | 1 | 12 |",
                "| 1 | 12 | 1 | 13 |",
                "+---+----+---+----+",
            ],
            &[filtered_rb]
        );

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
                None,
                None,
                None
            ])
        );

        let null_joined_batch = filter_record_batch(&output, &null_mask)?;

        assert_batches_eq!(
            &[
                "+---+---+---+---+",
                "| a | b | x | y |",
                "+---+---+---+---+",
                "+---+---+---+---+",
            ],
            &[null_joined_batch]
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_anti_join_filtered_mask() -> Result<()> {
    for join_type in [LeftAnti, RightAnti] {
        let mut joined_batches = build_joined_record_batches()?;
        let schema = joined_batches.joined_batches.schema();

        let output = joined_batches.concat_batches(&schema)?;
        let out_mask = joined_batches.filter_mask.finish();
        let out_indices = joined_batches.row_indices.finish();

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0]),
                &[0usize],
                &BooleanArray::from(vec![true]),
                1
            )
            .unwrap(),
            BooleanArray::from(vec![None])
        );

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0]),
                &[0usize],
                &BooleanArray::from(vec![false]),
                1
            )
            .unwrap(),
            BooleanArray::from(vec![Some(true)])
        );

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0, 0]),
                &[0usize; 2],
                &BooleanArray::from(vec![true, true]),
                2
            )
            .unwrap(),
            BooleanArray::from(vec![None, None])
        );

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![true, true, true]),
                3
            )
            .unwrap(),
            BooleanArray::from(vec![None, None, None])
        );

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![true, false, true]),
                3
            )
            .unwrap(),
            BooleanArray::from(vec![None, None, None])
        );

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![false, false, true]),
                3
            )
            .unwrap(),
            BooleanArray::from(vec![None, None, None])
        );

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![false, true, true]),
                3
            )
            .unwrap(),
            BooleanArray::from(vec![None, None, None])
        );

        assert_eq!(
            get_corrected_filter_mask(
                join_type,
                &UInt64Array::from(vec![0, 0, 0]),
                &[0usize; 3],
                &BooleanArray::from(vec![false, false, false]),
                3
            )
            .unwrap(),
            BooleanArray::from(vec![None, None, Some(true)])
        );

        let corrected_mask = get_corrected_filter_mask(
            join_type,
            &out_indices,
            &joined_batches.batch_ids,
            &out_mask,
            output.num_rows(),
        )
        .unwrap();

        assert_eq!(
            corrected_mask,
            BooleanArray::from(vec![
                None,
                None,
                None,
                None,
                None,
                Some(true),
                None,
                Some(true)
            ])
        );

        let filtered_rb = filter_record_batch(&output, &corrected_mask)?;

        allow_duplicates! {
            assert_snapshot!(batches_to_string(&[filtered_rb]), @r#"
                    +---+----+---+----+
                    | a | b  | x | y  |
                    +---+----+---+----+
                    | 1 | 13 | 1 | 12 |
                    | 1 | 14 | 1 | 11 |
                    +---+----+---+----+
            "#);
        }

        // output null rows
        let null_mask = arrow::compute::not(&corrected_mask)?;
        assert_eq!(
            null_mask,
            BooleanArray::from(vec![
                None,
                None,
                None,
                None,
                None,
                Some(false),
                None,
                Some(false),
            ])
        );

        let null_joined_batch = filter_record_batch(&output, &null_mask)?;

        allow_duplicates! {
            assert_snapshot!(batches_to_string(&[null_joined_batch]), @r#"
                        +---+---+---+---+
                        | a | b | x | y |
                        +---+---+---+---+
                        +---+---+---+---+
                "#);
        }
    }

    Ok(())
}

/// Returns the column names on the schema
fn columns(schema: &Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}
