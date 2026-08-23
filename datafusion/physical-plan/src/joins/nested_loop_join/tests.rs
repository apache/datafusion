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

//! Nested loop join tests.

use super::exec::*;
use std::sync::Arc;

use crate::ExecutionPlan;
use crate::joins::utils::{ColumnIndex, JoinFilter};
use crate::metrics::MetricsSet;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion_common::{JoinSide, Result, ScalarValue};
use datafusion_execution::TaskContext;
use datafusion_expr::JoinType;

use crate::statistics::{StatisticsArgs, StatisticsContext};
use crate::test::{TestMemoryExec, assert_join_metrics};
use crate::{
    common, expressions::Column, repartition::RepartitionExec, test::build_table_i32,
};

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field};
use datafusion_common::assert_contains;
use datafusion_common::test_util::batches_to_sort_string;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Literal};
use datafusion_physical_expr::{Partitioning, PhysicalExpr};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};

use insta::allow_duplicates;
use insta::assert_snapshot;
use rstest::rstest;

fn build_table(
    a: (&str, &Vec<i32>),
    b: (&str, &Vec<i32>),
    c: (&str, &Vec<i32>),
    batch_size: Option<usize>,
    sorted_column_names: Vec<&str>,
) -> Arc<dyn ExecutionPlan> {
    let batch = build_table_i32(a, b, c);
    let schema = batch.schema();

    let batches = if let Some(batch_size) = batch_size {
        let num_batches = batch.num_rows().div_ceil(batch_size);
        (0..num_batches)
            .map(|i| {
                let start = i * batch_size;
                let remaining_rows = batch.num_rows() - start;
                batch.slice(start, batch_size.min(remaining_rows))
            })
            .collect::<Vec<_>>()
    } else {
        vec![batch]
    };

    let mut sort_info = vec![];
    for name in sorted_column_names {
        let index = schema.index_of(name).unwrap();
        let sort_expr = PhysicalSortExpr::new(
            Arc::new(Column::new(name, index)),
            SortOptions::new(false, false),
        );
        sort_info.push(sort_expr);
    }
    let mut source = TestMemoryExec::try_new(&[batches], schema, None).unwrap();
    if let Some(ordering) = LexOrdering::new(sort_info) {
        source = source.try_with_sort_information(vec![ordering]).unwrap();
    }

    let source = Arc::new(source);
    Arc::new(TestMemoryExec::update_cache(&source))
}

fn build_left_table() -> Arc<dyn ExecutionPlan> {
    build_table(
        ("a1", &vec![5, 9, 11]),
        ("b1", &vec![5, 8, 8]),
        ("c1", &vec![50, 90, 110]),
        None,
        Vec::new(),
    )
}

fn build_right_table() -> Arc<dyn ExecutionPlan> {
    build_table(
        ("a2", &vec![12, 2, 10]),
        ("b2", &vec![10, 2, 10]),
        ("c2", &vec![40, 80, 100]),
        None,
        Vec::new(),
    )
}

fn prepare_join_filter() -> JoinFilter {
    let column_indices = vec![
        ColumnIndex {
            index: 1,
            side: JoinSide::Left,
        },
        ColumnIndex {
            index: 1,
            side: JoinSide::Right,
        },
    ];
    let intermediate_schema = Schema::new(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("x", DataType::Int32, true),
    ]);
    // left.b1!=8
    let left_filter = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("x", 0)),
        Operator::NotEq,
        Arc::new(Literal::new(ScalarValue::Int32(Some(8)))),
    )) as Arc<dyn PhysicalExpr>;
    // right.b2!=10
    let right_filter = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("x", 1)),
        Operator::NotEq,
        Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
    )) as Arc<dyn PhysicalExpr>;
    // filter = left.b1!=8 and right.b2!=10
    // after filter:
    // left table:
    // ("a1", &vec![5]),
    // ("b1", &vec![5]),
    // ("c1", &vec![50]),
    // right table:
    // ("a2", &vec![12, 2]),
    // ("b2", &vec![10, 2]),
    // ("c2", &vec![40, 80]),
    let filter_expression =
        Arc::new(BinaryExpr::new(left_filter, Operator::And, right_filter))
            as Arc<dyn PhysicalExpr>;

    JoinFilter::new(
        filter_expression,
        column_indices,
        Arc::new(intermediate_schema),
    )
}

pub(crate) async fn multi_partitioned_join_collect(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: &JoinType,
    join_filter: Option<JoinFilter>,
    context: Arc<TaskContext>,
) -> Result<(Vec<String>, Vec<RecordBatch>, MetricsSet)> {
    let partition_count = 4;

    // Redistributing right input
    let right = Arc::new(RepartitionExec::try_new(
        right,
        Partitioning::RoundRobinBatch(partition_count),
    )?) as Arc<dyn ExecutionPlan>;

    // Use the required distribution for nested loop join to test partition data
    let nested_loop_join =
        NestedLoopJoinExec::try_new(left, right, join_filter, join_type, None)?;
    let columns = columns(&nested_loop_join.schema());
    let mut batches = vec![];
    for i in 0..partition_count {
        let stream = nested_loop_join.execute(i, Arc::clone(&context))?;
        let more_batches = common::collect(stream).await?;
        batches.extend(
            more_batches
                .into_iter()
                .inspect(|b| {
                    assert!(b.num_rows() <= context.session_config().batch_size())
                })
                .filter(|b| b.num_rows() > 0)
                .collect::<Vec<_>>(),
        );
    }

    let metrics = nested_loop_join.metrics().unwrap();

    Ok((columns, batches, metrics))
}

fn new_task_ctx(batch_size: usize) -> Arc<TaskContext> {
    let base = TaskContext::default();
    // limit max size of intermediate batch used in nlj to 1
    let cfg = base.session_config().clone().with_batch_size(batch_size);
    Arc::new(base.with_session_config(cfg))
}

#[rstest]
#[tokio::test]
async fn join_inner_with_filter(#[values(1, 2, 16)] batch_size: usize) -> Result<()> {
    let task_ctx = new_task_ctx(batch_size);
    dbg!(&batch_size);
    let left = build_left_table();
    let right = build_right_table();
    let filter = prepare_join_filter();
    let (columns, batches, metrics) = multi_partitioned_join_collect(
        left,
        right,
        &JoinType::Inner,
        Some(filter),
        task_ctx,
    )
    .await?;

    assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b1 | c1 | a2 | b2 | c2 |
    +----+----+----+----+----+----+
    | 5  | 5  | 50 | 2  | 2  | 80 |
    +----+----+----+----+----+----+
    "));

    assert_join_metrics!(metrics, 1);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn join_left_with_filter(#[values(1, 2, 16)] batch_size: usize) -> Result<()> {
    let task_ctx = new_task_ctx(batch_size);
    let left = build_left_table();
    let right = build_right_table();

    let filter = prepare_join_filter();
    let (columns, batches, metrics) = multi_partitioned_join_collect(
        left,
        right,
        &JoinType::Left,
        Some(filter),
        task_ctx,
    )
    .await?;
    assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+-----+----+----+----+
    | a1 | b1 | c1  | a2 | b2 | c2 |
    +----+----+-----+----+----+----+
    | 11 | 8  | 110 |    |    |    |
    | 5  | 5  | 50  | 2  | 2  | 80 |
    | 9  | 8  | 90  |    |    |    |
    +----+----+-----+----+----+----+
    "));

    assert_join_metrics!(metrics, 3);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn join_right_with_filter(#[values(1, 2, 16)] batch_size: usize) -> Result<()> {
    let task_ctx = new_task_ctx(batch_size);
    let left = build_left_table();
    let right = build_right_table();

    let filter = prepare_join_filter();
    let (columns, batches, metrics) = multi_partitioned_join_collect(
        left,
        right,
        &JoinType::Right,
        Some(filter),
        task_ctx,
    )
    .await?;
    assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+----+----+----+-----+
    | a1 | b1 | c1 | a2 | b2 | c2  |
    +----+----+----+----+----+-----+
    |    |    |    | 10 | 10 | 100 |
    |    |    |    | 12 | 10 | 40  |
    | 5  | 5  | 50 | 2  | 2  | 80  |
    +----+----+----+----+----+-----+
    "));

    assert_join_metrics!(metrics, 3);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn join_full_with_filter(#[values(1, 2, 16)] batch_size: usize) -> Result<()> {
    let task_ctx = new_task_ctx(batch_size);
    let left = build_left_table();
    let right = build_right_table();

    let filter = prepare_join_filter();
    let (columns, batches, metrics) = multi_partitioned_join_collect(
        left,
        right,
        &JoinType::Full,
        Some(filter),
        task_ctx,
    )
    .await?;
    assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+-----+----+----+-----+
    | a1 | b1 | c1  | a2 | b2 | c2  |
    +----+----+-----+----+----+-----+
    |    |    |     | 10 | 10 | 100 |
    |    |    |     | 12 | 10 | 40  |
    | 11 | 8  | 110 |    |    |     |
    | 5  | 5  | 50  | 2  | 2  | 80  |
    | 9  | 8  | 90  |    |    |     |
    +----+----+-----+----+----+-----+
    "));

    assert_join_metrics!(metrics, 5);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn join_left_semi_with_filter(#[values(1, 2, 16)] batch_size: usize) -> Result<()> {
    let task_ctx = new_task_ctx(batch_size);
    let left = build_left_table();
    let right = build_right_table();

    let filter = prepare_join_filter();
    let (columns, batches, metrics) = multi_partitioned_join_collect(
        left,
        right,
        &JoinType::LeftSemi,
        Some(filter),
        task_ctx,
    )
    .await?;
    assert_eq!(columns, vec!["a1", "b1", "c1"]);
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+----+
    | a1 | b1 | c1 |
    +----+----+----+
    | 5  | 5  | 50 |
    +----+----+----+
    "));

    assert_join_metrics!(metrics, 1);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn join_left_anti_with_filter(#[values(1, 2, 16)] batch_size: usize) -> Result<()> {
    let task_ctx = new_task_ctx(batch_size);
    let left = build_left_table();
    let right = build_right_table();

    let filter = prepare_join_filter();
    let (columns, batches, metrics) = multi_partitioned_join_collect(
        left,
        right,
        &JoinType::LeftAnti,
        Some(filter),
        task_ctx,
    )
    .await?;
    assert_eq!(columns, vec!["a1", "b1", "c1"]);
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+-----+
    | a1 | b1 | c1  |
    +----+----+-----+
    | 11 | 8  | 110 |
    | 9  | 8  | 90  |
    +----+----+-----+
    "));

    assert_join_metrics!(metrics, 2);

    Ok(())
}

#[tokio::test]
async fn join_has_correct_stats() -> Result<()> {
    let left = build_left_table();
    let right = build_right_table();
    let nested_loop_join = NestedLoopJoinExec::try_new(
        left,
        right,
        None,
        &JoinType::Left,
        Some(vec![1, 2]),
    )?;
    let stats =
        StatisticsContext::new().compute(&nested_loop_join, &StatisticsArgs::new())?;
    assert_eq!(
        nested_loop_join.schema().fields().len(),
        stats.column_statistics.len(),
    );
    assert_eq!(2, stats.column_statistics.len());
    Ok(())
}

#[rstest]
#[tokio::test]
async fn join_right_semi_with_filter(
    #[values(1, 2, 16)] batch_size: usize,
) -> Result<()> {
    let task_ctx = new_task_ctx(batch_size);
    let left = build_left_table();
    let right = build_right_table();

    let filter = prepare_join_filter();
    let (columns, batches, metrics) = multi_partitioned_join_collect(
        left,
        right,
        &JoinType::RightSemi,
        Some(filter),
        task_ctx,
    )
    .await?;
    assert_eq!(columns, vec!["a2", "b2", "c2"]);
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+----+
    | a2 | b2 | c2 |
    +----+----+----+
    | 2  | 2  | 80 |
    +----+----+----+
    "));

    assert_join_metrics!(metrics, 1);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn join_right_anti_with_filter(
    #[values(1, 2, 16)] batch_size: usize,
) -> Result<()> {
    let task_ctx = new_task_ctx(batch_size);
    let left = build_left_table();
    let right = build_right_table();

    let filter = prepare_join_filter();
    let (columns, batches, metrics) = multi_partitioned_join_collect(
        left,
        right,
        &JoinType::RightAnti,
        Some(filter),
        task_ctx,
    )
    .await?;
    assert_eq!(columns, vec!["a2", "b2", "c2"]);
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+-----+
    | a2 | b2 | c2  |
    +----+----+-----+
    | 10 | 10 | 100 |
    | 12 | 10 | 40  |
    +----+----+-----+
    "));

    assert_join_metrics!(metrics, 2);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn join_left_mark_with_filter(#[values(1, 2, 16)] batch_size: usize) -> Result<()> {
    let task_ctx = new_task_ctx(batch_size);
    let left = build_left_table();
    let right = build_right_table();

    let filter = prepare_join_filter();
    let (columns, batches, metrics) = multi_partitioned_join_collect(
        left,
        right,
        &JoinType::LeftMark,
        Some(filter),
        task_ctx,
    )
    .await?;
    assert_eq!(columns, vec!["a1", "b1", "c1", "mark"]);
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+-----+-------+
    | a1 | b1 | c1  | mark  |
    +----+----+-----+-------+
    | 11 | 8  | 110 | false |
    | 5  | 5  | 50  | true  |
    | 9  | 8  | 90  | false |
    +----+----+-----+-------+
    "));

    assert_join_metrics!(metrics, 3);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn join_right_mark_with_filter(
    #[values(1, 2, 16)] batch_size: usize,
) -> Result<()> {
    let task_ctx = new_task_ctx(batch_size);
    let left = build_left_table();
    let right = build_right_table();

    let filter = prepare_join_filter();
    let (columns, batches, metrics) = multi_partitioned_join_collect(
        left,
        right,
        &JoinType::RightMark,
        Some(filter),
        task_ctx,
    )
    .await?;
    assert_eq!(columns, vec!["a2", "b2", "c2", "mark"]);

    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+-----+-------+
    | a2 | b2 | c2  | mark  |
    +----+----+-----+-------+
    | 10 | 10 | 100 | false |
    | 12 | 10 | 40  | false |
    | 2  | 2  | 80  | true  |
    +----+----+-----+-------+
    "));

    assert_join_metrics!(metrics, 3);

    Ok(())
}

#[tokio::test]
async fn test_overallocation() -> Result<()> {
    let left = build_table(
        ("a1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
        ("b1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
        ("c1", &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
        None,
        Vec::new(),
    );
    let right = build_table(
        ("a2", &vec![10, 11]),
        ("b2", &vec![12, 13]),
        ("c2", &vec![14, 15]),
        None,
        Vec::new(),
    );
    let filter = prepare_join_filter();

    // Join types that support memory-limited fallback should succeed
    // even under tight memory limits (they spill to disk instead of OOM).
    let fallback_join_types = vec![
        JoinType::Inner,
        JoinType::Left,
        JoinType::LeftSemi,
        JoinType::LeftAnti,
        JoinType::LeftMark,
        JoinType::Right,
        JoinType::RightSemi,
        JoinType::RightAnti,
        JoinType::RightMark,
    ];

    for join_type in &fallback_join_types {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(100, 1.0)
            .build_arc()?;
        let task_ctx = TaskContext::default().with_runtime(runtime);
        let task_ctx = Arc::new(task_ctx);

        // Should succeed via spill fallback, not OOM
        let _result = multi_partitioned_join_collect(
            Arc::clone(&left),
            Arc::clone(&right),
            join_type,
            Some(filter.clone()),
            task_ctx,
        )
        .await?;
    }

    // FULL JOIN with multiple right partitions is intentionally not
    // supported in the fallback path yet (cross-partition left-bitmap
    // coordination is missing). It should still OOM under tight memory.
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .build_arc()?;
    let task_ctx = TaskContext::default().with_runtime(runtime);
    let task_ctx = Arc::new(task_ctx);
    let err = multi_partitioned_join_collect(
        Arc::clone(&left),
        Arc::clone(&right),
        &JoinType::Full,
        Some(filter.clone()),
        task_ctx,
    )
    .await
    .unwrap_err();
    assert_contains!(err.to_string(), "Resources exhausted");

    Ok(())
}

/// Returns the column names on the schema
fn columns(schema: &Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

// ========================================================================
// Memory-limited execution tests
// ========================================================================

/// Helper to run a NLJ using partition 0 and collect results + metrics.
async fn join_collect(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: &JoinType,
    join_filter: Option<JoinFilter>,
    context: Arc<TaskContext>,
) -> Result<(Vec<String>, Vec<RecordBatch>, MetricsSet)> {
    let nested_loop_join =
        NestedLoopJoinExec::try_new(left, right, join_filter, join_type, None)?;
    let columns = columns(&nested_loop_join.schema());
    let stream = nested_loop_join.execute(0, context)?;
    let batches: Vec<RecordBatch> = common::collect(stream)
        .await?
        .into_iter()
        .filter(|b| b.num_rows() > 0)
        .collect();
    let metrics = nested_loop_join.metrics().unwrap();
    Ok((columns, batches, metrics))
}

/// Create a TaskContext with tight memory limit and disk spilling enabled.
fn task_ctx_with_memory_limit(
    memory_limit: usize,
    batch_size: usize,
) -> Result<Arc<TaskContext>> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(memory_limit, 1.0)
        .build_arc()?;
    let cfg = TaskContext::default()
        .session_config()
        .clone()
        .with_batch_size(batch_size);
    let task_ctx = TaskContext::default()
        .with_runtime(runtime)
        .with_session_config(cfg);
    Ok(Arc::new(task_ctx))
}

#[tokio::test]
async fn test_nlj_memory_limited_inner_join() -> Result<()> {
    // Use a very small memory limit to force OOM → fallback to spill.
    let task_ctx = task_ctx_with_memory_limit(50, 16)?;
    let left = build_left_table();
    let right = build_right_table();
    let filter = prepare_join_filter();

    let (columns, batches, metrics) =
        join_collect(left, right, &JoinType::Inner, Some(filter), task_ctx).await?;

    assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

    // Verify spill actually occurred (memory-limited path was taken)
    assert!(
        metrics.spill_count().unwrap_or(0) > 0,
        "Expected spilling to occur under tight memory limit"
    );

    // Result should be identical to the non-memory-limited case
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b1 | c1 | a2 | b2 | c2 |
    +----+----+----+----+----+----+
    | 5  | 5  | 50 | 2  | 2  | 80 |
    +----+----+----+----+----+----+
    "));
    Ok(())
}

#[tokio::test]
async fn test_nlj_memory_limited_left_join() -> Result<()> {
    let task_ctx = task_ctx_with_memory_limit(50, 16)?;
    let left = build_left_table();
    let right = build_right_table();
    let filter = prepare_join_filter();

    let (columns, batches, metrics) =
        join_collect(left, right, &JoinType::Left, Some(filter), task_ctx).await?;

    assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

    // Verify spill actually occurred
    assert!(
        metrics.spill_count().unwrap_or(0) > 0,
        "Expected spilling to occur under tight memory limit"
    );

    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+-----+----+----+----+
    | a1 | b1 | c1  | a2 | b2 | c2 |
    +----+----+-----+----+----+----+
    | 11 | 8  | 110 |    |    |    |
    | 5  | 5  | 50  | 2  | 2  | 80 |
    | 9  | 8  | 90  |    |    |    |
    +----+----+-----+----+----+----+
    "));
    Ok(())
}

#[tokio::test]
async fn test_nlj_fits_in_memory_no_spill() -> Result<()> {
    // Use a large memory limit — everything fits, no spilling needed.
    let task_ctx = task_ctx_with_memory_limit(10_000_000, 16)?;
    let left = build_left_table();
    let right = build_right_table();
    let filter = prepare_join_filter();

    let (columns, batches, metrics) =
        join_collect(left, right, &JoinType::Inner, Some(filter), task_ctx).await?;

    assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

    // Verify no spilling occurred (standard OnceFut path was used)
    assert_eq!(
        metrics.spill_count().unwrap_or(0),
        0,
        "Expected no spilling with generous memory limit"
    );

    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+----+----+----+----+
    | a1 | b1 | c1 | a2 | b2 | c2 |
    +----+----+----+----+----+----+
    | 5  | 5  | 50 | 2  | 2  | 80 |
    +----+----+----+----+----+----+
    "));
    Ok(())
}

#[tokio::test]
async fn test_nlj_memory_limited_empty_inputs() -> Result<()> {
    let task_ctx = task_ctx_with_memory_limit(50, 16)?;

    // Empty left table
    let empty_left = build_table(
        ("a1", &vec![]),
        ("b1", &vec![]),
        ("c1", &vec![]),
        None,
        Vec::new(),
    );
    let right = build_right_table();
    let filter = prepare_join_filter();

    let (_columns, batches, _metrics) =
        join_collect(empty_left, right, &JoinType::Inner, Some(filter), task_ctx).await?;
    assert!(batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0));

    // Empty right table
    let task_ctx2 = task_ctx_with_memory_limit(50, 16)?;
    let left = build_left_table();
    let empty_right = build_table(
        ("a2", &vec![]),
        ("b2", &vec![]),
        ("c2", &vec![]),
        None,
        Vec::new(),
    );
    let filter2 = prepare_join_filter();

    let (_columns, batches, _metrics) = join_collect(
        left,
        empty_right,
        &JoinType::Inner,
        Some(filter2),
        task_ctx2,
    )
    .await?;
    assert!(batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0));

    Ok(())
}

#[tokio::test]
async fn test_nlj_memory_limited_no_disk_falls_back_to_oom() -> Result<()> {
    // When disk is disabled, fallback is not possible and OOM should occur.
    use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build_arc()?;
    let task_ctx = Arc::new(TaskContext::default().with_runtime(runtime));

    let left = build_left_table();
    let right = build_right_table();
    let filter = prepare_join_filter();

    let err = join_collect(left, right, &JoinType::Inner, Some(filter), task_ctx)
        .await
        .unwrap_err();

    assert_contains!(err.to_string(), "Resources exhausted");
    Ok(())
}

#[tokio::test]
async fn test_nlj_memory_limited_right_join() -> Result<()> {
    let task_ctx = task_ctx_with_memory_limit(50, 16)?;
    let left = build_left_table();
    let right = build_right_table();
    let filter = prepare_join_filter();

    let (columns, batches, metrics) =
        join_collect(left, right, &JoinType::Right, Some(filter), task_ctx).await?;

    assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

    // Verify spill actually occurred
    assert!(
        metrics.spill_count().unwrap_or(0) > 0,
        "Expected spilling to occur under tight memory limit"
    );

    // Right join: all right rows appear. Unmatched right rows get NULLs on left.
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+----+----+----+-----+
    | a1 | b1 | c1 | a2 | b2 | c2  |
    +----+----+----+----+----+-----+
    |    |    |    | 10 | 10 | 100 |
    |    |    |    | 12 | 10 | 40  |
    | 5  | 5  | 50 | 2  | 2  | 80  |
    +----+----+----+----+----+-----+
    "));
    Ok(())
}

#[tokio::test]
async fn test_nlj_memory_limited_full_join() -> Result<()> {
    let task_ctx = task_ctx_with_memory_limit(50, 16)?;
    let left = build_left_table();
    let right = build_right_table();
    let filter = prepare_join_filter();

    let (columns, batches, metrics) =
        join_collect(left, right, &JoinType::Full, Some(filter), task_ctx).await?;

    assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b2", "c2"]);

    // Verify spill actually occurred
    assert!(
        metrics.spill_count().unwrap_or(0) > 0,
        "Expected spilling to occur under tight memory limit"
    );

    // Full join: unmatched from both sides appear with NULL padding.
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+-----+----+----+-----+
    | a1 | b1 | c1  | a2 | b2 | c2  |
    +----+----+-----+----+----+-----+
    |    |    |     | 10 | 10 | 100 |
    |    |    |     | 12 | 10 | 40  |
    | 11 | 8  | 110 |    |    |     |
    | 5  | 5  | 50  | 2  | 2  | 80  |
    | 9  | 8  | 90  |    |    |     |
    +----+----+-----+----+----+-----+
    "));
    Ok(())
}

#[tokio::test]
async fn test_nlj_memory_limited_right_semi_join() -> Result<()> {
    let task_ctx = task_ctx_with_memory_limit(50, 16)?;
    let left = build_left_table();
    let right = build_right_table();
    let filter = prepare_join_filter();

    let (columns, batches, metrics) =
        join_collect(left, right, &JoinType::RightSemi, Some(filter), task_ctx).await?;

    assert_eq!(columns, vec!["a2", "b2", "c2"]);

    assert!(
        metrics.spill_count().unwrap_or(0) > 0,
        "Expected spilling to occur under tight memory limit"
    );

    // Right semi: only right rows that matched at least one left row.
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+----+
    | a2 | b2 | c2 |
    +----+----+----+
    | 2  | 2  | 80 |
    +----+----+----+
    "));
    Ok(())
}

#[tokio::test]
async fn test_nlj_memory_limited_right_anti_join() -> Result<()> {
    let task_ctx = task_ctx_with_memory_limit(50, 16)?;
    let left = build_left_table();
    let right = build_right_table();
    let filter = prepare_join_filter();

    let (columns, batches, metrics) =
        join_collect(left, right, &JoinType::RightAnti, Some(filter), task_ctx).await?;

    assert_eq!(columns, vec!["a2", "b2", "c2"]);

    assert!(
        metrics.spill_count().unwrap_or(0) > 0,
        "Expected spilling to occur under tight memory limit"
    );

    // Right anti: right rows that did NOT match any left row.
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+-----+
    | a2 | b2 | c2  |
    +----+----+-----+
    | 10 | 10 | 100 |
    | 12 | 10 | 40  |
    +----+----+-----+
    "));
    Ok(())
}

#[tokio::test]
async fn test_nlj_memory_limited_right_mark_join() -> Result<()> {
    let task_ctx = task_ctx_with_memory_limit(50, 16)?;
    let left = build_left_table();
    let right = build_right_table();
    let filter = prepare_join_filter();

    let (columns, batches, metrics) =
        join_collect(left, right, &JoinType::RightMark, Some(filter), task_ctx).await?;

    assert_eq!(columns, vec!["a2", "b2", "c2", "mark"]);

    assert!(
        metrics.spill_count().unwrap_or(0) > 0,
        "Expected spilling to occur under tight memory limit"
    );

    // Right mark: all right rows with a bool column indicating match.
    allow_duplicates!(assert_snapshot!(batches_to_sort_string(&batches), @r"
    +----+----+-----+-------+
    | a2 | b2 | c2  | mark  |
    +----+----+-----+-------+
    | 10 | 10 | 100 | false |
    | 12 | 10 | 40  | false |
    | 2  | 2  | 80  | true  |
    +----+----+-----+-------+
    "));
    Ok(())
}
