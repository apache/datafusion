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

use insta::assert_snapshot;
use std::sync::Arc;
use std::{
    any::Any,
    pin::Pin,
    task::{Context, Poll},
};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{stats::Precision, ColumnStatistics, JoinType, ScalarValue};
use datafusion_common::{JoinSide, NullEquality};
use datafusion_common::{Result, Statistics};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, NegativeExpr};
use datafusion_physical_expr::intervals::utils::check_support;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion_physical_optimizer::join_selection::JoinSelection;
use datafusion_physical_optimizer::OptimizerContext;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::displayable;
use datafusion_physical_plan::joins::utils::ColumnIndex;
use datafusion_physical_plan::joins::utils::JoinFilter;
use datafusion_physical_plan::joins::{HashJoinExec, NestedLoopJoinExec, PartitionMode};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::ExecutionPlanProperties;
use datafusion_physical_plan::{
    execution_plan::{Boundedness, EmissionType},
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};

use futures::Stream;
use rstest::rstest;

/// Return statistics for empty table
fn empty_statistics() -> Statistics {
    Statistics {
        num_rows: Precision::Absent,
        total_byte_size: Precision::Absent,
        column_statistics: vec![ColumnStatistics::new_unknown()],
    }
}

/// Get table thresholds: (num_rows, byte_size)
fn get_thresholds() -> (usize, usize) {
    let optimizer_options = ConfigOptions::new().optimizer;
    (
        optimizer_options.hash_join_single_partition_threshold_rows,
        optimizer_options.hash_join_single_partition_threshold,
    )
}

/// Return statistics for small table
fn small_statistics() -> Statistics {
    let (threshold_num_rows, threshold_byte_size) = get_thresholds();
    Statistics {
        num_rows: Precision::Inexact(threshold_num_rows / 128),
        total_byte_size: Precision::Inexact(threshold_byte_size / 128),
        column_statistics: vec![ColumnStatistics::new_unknown()],
    }
}

/// Return statistics for big table
fn big_statistics() -> Statistics {
    let (threshold_num_rows, threshold_byte_size) = get_thresholds();
    Statistics {
        num_rows: Precision::Inexact(threshold_num_rows * 2),
        total_byte_size: Precision::Inexact(threshold_byte_size * 2),
        column_statistics: vec![ColumnStatistics::new_unknown()],
    }
}

/// Return statistics for big table
fn bigger_statistics() -> Statistics {
    let (threshold_num_rows, threshold_byte_size) = get_thresholds();
    Statistics {
        num_rows: Precision::Inexact(threshold_num_rows * 4),
        total_byte_size: Precision::Inexact(threshold_byte_size * 4),
        column_statistics: vec![ColumnStatistics::new_unknown()],
    }
}

fn create_big_and_small() -> (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) {
    let big = Arc::new(StatisticsExec::new(
        big_statistics(),
        Schema::new(vec![Field::new("big_col", DataType::Int32, false)]),
    ));

    let small = Arc::new(StatisticsExec::new(
        small_statistics(),
        Schema::new(vec![Field::new("small_col", DataType::Int32, false)]),
    ));
    (big, small)
}

/// Create a column statistics vector for a single column
/// that has the given min/max/distinct_count properties.
///
/// Given min/max will be mapped to a [`ScalarValue`] if
/// they are not `None`.
fn create_column_stats(
    min: Option<u64>,
    max: Option<u64>,
    distinct_count: Option<usize>,
) -> Vec<ColumnStatistics> {
    vec![ColumnStatistics {
        distinct_count: distinct_count
            .map(Precision::Inexact)
            .unwrap_or(Precision::Absent),
        min_value: min
            .map(|size| Precision::Inexact(ScalarValue::UInt64(Some(size))))
            .unwrap_or(Precision::Absent),
        max_value: max
            .map(|size| Precision::Inexact(ScalarValue::UInt64(Some(size))))
            .unwrap_or(Precision::Absent),
        ..Default::default()
    }]
}

/// Create join filter for NLJoinExec with expression `big_col > small_col`
/// where both columns are 0-indexed and come from left and right inputs respectively
fn nl_join_filter() -> Option<JoinFilter> {
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
    let intermediate_schema = Schema::new(vec![
        Field::new("big_col", DataType::Int32, false),
        Field::new("small_col", DataType::Int32, false),
    ]);
    let expression = Arc::new(BinaryExpr::new(
        Arc::new(Column::new_with_schema("big_col", &intermediate_schema).unwrap()),
        Operator::Gt,
        Arc::new(Column::new_with_schema("small_col", &intermediate_schema).unwrap()),
    )) as _;
    Some(JoinFilter::new(
        expression,
        column_indices,
        Arc::new(intermediate_schema),
    ))
}

/// Returns three plans with statistics of (min, max, distinct_count)
/// * big 100K rows @ (0, 50k, 50k)
/// * medium 10K rows @ (1k, 5k, 1k)
/// * small 1K rows @ (0, 100k, 1k)
fn create_nested_with_min_max() -> (
    Arc<dyn ExecutionPlan>,
    Arc<dyn ExecutionPlan>,
    Arc<dyn ExecutionPlan>,
) {
    let big = Arc::new(StatisticsExec::new(
        Statistics {
            num_rows: Precision::Inexact(100_000),
            column_statistics: create_column_stats(Some(0), Some(50_000), Some(50_000)),
            total_byte_size: Precision::Absent,
        },
        Schema::new(vec![Field::new("big_col", DataType::Int32, false)]),
    ));

    let medium = Arc::new(StatisticsExec::new(
        Statistics {
            num_rows: Precision::Inexact(10_000),
            column_statistics: create_column_stats(Some(1000), Some(5000), Some(1000)),
            total_byte_size: Precision::Absent,
        },
        Schema::new(vec![Field::new("medium_col", DataType::Int32, false)]),
    ));

    let small = Arc::new(StatisticsExec::new(
        Statistics {
            num_rows: Precision::Inexact(1000),
            column_statistics: create_column_stats(Some(0), Some(100_000), Some(1000)),
            total_byte_size: Precision::Absent,
        },
        Schema::new(vec![Field::new("small_col", DataType::Int32, false)]),
    ));

    (big, medium, small)
}

#[tokio::test]
async fn test_join_with_swap() {
    let (big, small) = create_big_and_small();

    let join = Arc::new(
        HashJoinExec::try_new(
            Arc::clone(&big),
            Arc::clone(&small),
            vec![(
                Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()),
                Arc::new(Column::new_with_schema("small_col", &small.schema()).unwrap()),
            )],
            None,
            &JoinType::Left,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
        )
        .unwrap(),
    );

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let optimized_join = JoinSelection::new()
        .optimize_plan(join, &optimizer_context)
        .unwrap();

    let swapping_projection = optimized_join
        .as_any()
        .downcast_ref::<ProjectionExec>()
        .expect("A proj is required to swap columns back to their original order");

    assert_eq!(swapping_projection.expr().len(), 2);
    let proj_expr = &swapping_projection.expr()[0];
    assert_eq!(proj_expr.alias, "big_col");
    assert_col_expr(&proj_expr.expr, "big_col", 1);
    let proj_expr = &swapping_projection.expr()[1];
    assert_eq!(proj_expr.alias, "small_col");
    assert_col_expr(&proj_expr.expr, "small_col", 0);

    let swapped_join = swapping_projection
        .input()
        .as_any()
        .downcast_ref::<HashJoinExec>()
        .expect("The type of the plan should not be changed");

    assert_eq!(
        swapped_join
            .left()
            .partition_statistics(None)
            .unwrap()
            .total_byte_size,
        Precision::Inexact(8192)
    );
    assert_eq!(
        swapped_join
            .right()
            .partition_statistics(None)
            .unwrap()
            .total_byte_size,
        Precision::Inexact(2097152)
    );
}

#[tokio::test]
async fn test_left_join_no_swap() {
    let (big, small) = create_big_and_small();

    let join = Arc::new(
        HashJoinExec::try_new(
            Arc::clone(&small),
            Arc::clone(&big),
            vec![(
                Arc::new(Column::new_with_schema("small_col", &small.schema()).unwrap()),
                Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()),
            )],
            None,
            &JoinType::Left,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
        )
        .unwrap(),
    );

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let optimized_join = JoinSelection::new()
        .optimize_plan(join, &optimizer_context)
        .unwrap();

    let swapped_join = optimized_join
        .as_any()
        .downcast_ref::<HashJoinExec>()
        .expect("The type of the plan should not be changed");

    assert_eq!(
        swapped_join
            .left()
            .partition_statistics(None)
            .unwrap()
            .total_byte_size,
        Precision::Inexact(8192)
    );
    assert_eq!(
        swapped_join
            .right()
            .partition_statistics(None)
            .unwrap()
            .total_byte_size,
        Precision::Inexact(2097152)
    );
}

#[tokio::test]
async fn test_join_with_swap_semi() {
    let join_types = [JoinType::LeftSemi, JoinType::LeftAnti];
    for join_type in join_types {
        let (big, small) = create_big_and_small();

        let join = HashJoinExec::try_new(
            Arc::clone(&big),
            Arc::clone(&small),
            vec![(
                Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()),
                Arc::new(Column::new_with_schema("small_col", &small.schema()).unwrap()),
            )],
            None,
            &join_type,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
        )
        .unwrap();

        let original_schema = join.schema();

        let session_config = SessionConfig::new();
        let optimizer_context = OptimizerContext::new(session_config.clone());
        let optimized_join = JoinSelection::new()
            .optimize_plan(Arc::new(join), &optimizer_context)
            .unwrap();

        let swapped_join = optimized_join
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect(
                "A proj is not required to swap columns back to their original order",
            );

        assert_eq!(swapped_join.schema().fields().len(), 1);
        assert_eq!(
            swapped_join
                .left()
                .partition_statistics(None)
                .unwrap()
                .total_byte_size,
            Precision::Inexact(8192)
        );
        assert_eq!(
            swapped_join
                .right()
                .partition_statistics(None)
                .unwrap()
                .total_byte_size,
            Precision::Inexact(2097152)
        );
        assert_eq!(original_schema, swapped_join.schema());
    }
}

#[tokio::test]
async fn test_join_with_swap_mark() {
    let join_types = [JoinType::LeftMark, JoinType::RightMark];
    for join_type in join_types {
        let (big, small) = create_big_and_small();

        let join = HashJoinExec::try_new(
            Arc::clone(&big),
            Arc::clone(&small),
            vec![(
                Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()),
                Arc::new(Column::new_with_schema("small_col", &small.schema()).unwrap()),
            )],
            None,
            &join_type,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
        )
        .unwrap();

        let original_schema = join.schema();

        let session_config = SessionConfig::new();
        let optimizer_context = OptimizerContext::new(session_config.clone());
        let optimized_join = JoinSelection::new()
            .optimize_plan(Arc::new(join), &optimizer_context)
            .unwrap();

        let swapped_join = optimized_join
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect(
                "A proj is not required to swap columns back to their original order",
            );

        assert_eq!(swapped_join.schema().fields().len(), 2);
        assert_eq!(
            swapped_join
                .left()
                .partition_statistics(None)
                .unwrap()
                .total_byte_size,
            Precision::Inexact(8192)
        );
        assert_eq!(
            swapped_join
                .right()
                .partition_statistics(None)
                .unwrap()
                .total_byte_size,
            Precision::Inexact(2097152)
        );
        assert_eq!(original_schema, swapped_join.schema());
    }
}

/// Compare the input plan with the plan after running the probe order optimizer.
macro_rules! assert_optimized {
    ($PLAN: expr, @$EXPECTED_LINES: literal $(,)?) => {

        let plan = Arc::new($PLAN);
        let session_config = SessionConfig::new();
        let optimizer_context = OptimizerContext::new(session_config.clone());
        let optimized = JoinSelection::new()
            .optimize_plan(plan.clone(), &optimizer_context)
            .unwrap();

        let plan_string = displayable(optimized.as_ref()).indent(true).to_string();
        let actual = plan_string.trim();

        assert_snapshot!(
            actual,
            @$EXPECTED_LINES
        );
    };
}

#[tokio::test]
async fn test_nested_join_swap() {
    let (big, medium, small) = create_nested_with_min_max();

    // Form the inner join: big JOIN small
    let child_join = HashJoinExec::try_new(
        Arc::clone(&big),
        Arc::clone(&small),
        vec![(
            col("big_col", &big.schema()).unwrap(),
            col("small_col", &small.schema()).unwrap(),
        )],
        None,
        &JoinType::Inner,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
    )
    .unwrap();
    let child_schema = child_join.schema();

    // Form join tree `medium LEFT JOIN (big JOIN small)`
    let join = HashJoinExec::try_new(
        Arc::clone(&medium),
        Arc::new(child_join),
        vec![(
            col("medium_col", &medium.schema()).unwrap(),
            col("small_col", &child_schema).unwrap(),
        )],
        None,
        &JoinType::Left,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
    )
    .unwrap();

    // Hash join uses the left side to build the hash table, and right side to probe it. We want
    // to keep left as small as possible, so if we can estimate (with a reasonable margin of error)
    // that the left side is smaller than the right side, we should swap the sides.
    //
    // The first hash join's left is 'small' table (with 1000 rows), and the second hash join's
    // left is the F(small IJ big) which has an estimated cardinality of 2000 rows (vs medium which
    // has an exact cardinality of 10_000 rows).
    assert_optimized!(
        join,
        @r"
    ProjectionExec: expr=[medium_col@2 as medium_col, big_col@0 as big_col, small_col@1 as small_col]
      HashJoinExec: mode=CollectLeft, join_type=Right, on=[(small_col@1, medium_col@0)]
        ProjectionExec: expr=[big_col@1 as big_col, small_col@0 as small_col]
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(small_col@0, big_col@0)]
            StatisticsExec: col_count=1, row_count=Inexact(1000)
            StatisticsExec: col_count=1, row_count=Inexact(100000)
        StatisticsExec: col_count=1, row_count=Inexact(10000)
    "
    );
}

#[tokio::test]
async fn test_join_no_swap() {
    let (big, small) = create_big_and_small();
    let join = Arc::new(
        HashJoinExec::try_new(
            Arc::clone(&small),
            Arc::clone(&big),
            vec![(
                Arc::new(Column::new_with_schema("small_col", &small.schema()).unwrap()),
                Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()),
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
        )
        .unwrap(),
    );

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let optimized_join = JoinSelection::new()
        .optimize_plan(join, &optimizer_context)
        .unwrap();

    let swapped_join = optimized_join
        .as_any()
        .downcast_ref::<HashJoinExec>()
        .expect("The type of the plan should not be changed");

    assert_eq!(
        swapped_join
            .left()
            .partition_statistics(None)
            .unwrap()
            .total_byte_size,
        Precision::Inexact(8192)
    );
    assert_eq!(
        swapped_join
            .right()
            .partition_statistics(None)
            .unwrap()
            .total_byte_size,
        Precision::Inexact(2097152)
    );
}

#[rstest(
    join_type,
    case::inner(JoinType::Inner),
    case::left(JoinType::Left),
    case::right(JoinType::Right),
    case::full(JoinType::Full)
)]
#[tokio::test]
async fn test_nl_join_with_swap(join_type: JoinType) {
    let (big, small) = create_big_and_small();

    let join = Arc::new(
        NestedLoopJoinExec::try_new(
            Arc::clone(&big),
            Arc::clone(&small),
            nl_join_filter(),
            &join_type,
            None,
        )
        .unwrap(),
    );

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let optimized_join = JoinSelection::new()
        .optimize_plan(join, &optimizer_context)
        .unwrap();

    let swapping_projection = optimized_join
        .as_any()
        .downcast_ref::<ProjectionExec>()
        .expect("A proj is required to swap columns back to their original order");

    assert_eq!(swapping_projection.expr().len(), 2);
    let proj_expr = &swapping_projection.expr()[0];
    assert_eq!(proj_expr.alias, "big_col");
    assert_col_expr(&proj_expr.expr, "big_col", 1);
    let proj_expr = &swapping_projection.expr()[1];
    assert_eq!(proj_expr.alias, "small_col");
    assert_col_expr(&proj_expr.expr, "small_col", 0);

    let swapped_join = swapping_projection
        .input()
        .as_any()
        .downcast_ref::<NestedLoopJoinExec>()
        .expect("The type of the plan should not be changed");

    // Assert join side of big_col swapped in filter expression
    let swapped_filter = swapped_join.filter().unwrap();
    let swapped_big_col_idx = swapped_filter.schema().index_of("big_col").unwrap();
    let swapped_big_col_side = swapped_filter
        .column_indices()
        .get(swapped_big_col_idx)
        .unwrap()
        .side;
    assert_eq!(
        swapped_big_col_side,
        JoinSide::Right,
        "Filter column side should be swapped"
    );

    assert_eq!(
        swapped_join
            .left()
            .partition_statistics(None)
            .unwrap()
            .total_byte_size,
        Precision::Inexact(8192)
    );
    assert_eq!(
        swapped_join
            .right()
            .partition_statistics(None)
            .unwrap()
            .total_byte_size,
        Precision::Inexact(2097152)
    );
}

#[rstest(
    join_type,
    case::left_semi(JoinType::LeftSemi),
    case::left_anti(JoinType::LeftAnti),
    case::right_semi(JoinType::RightSemi),
    case::right_anti(JoinType::RightAnti),
    case::right_mark(JoinType::RightMark)
)]
#[tokio::test]
async fn test_nl_join_with_swap_no_proj(join_type: JoinType) {
    let (big, small) = create_big_and_small();

    let join = Arc::new(
        NestedLoopJoinExec::try_new(
            Arc::clone(&big),
            Arc::clone(&small),
            nl_join_filter(),
            &join_type,
            None,
        )
        .unwrap(),
    );

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let optimized_join = JoinSelection::new()
        .optimize_plan(Arc::<NestedLoopJoinExec>::clone(&join), &optimizer_context)
        .unwrap();

    let swapped_join = optimized_join
        .as_any()
        .downcast_ref::<NestedLoopJoinExec>()
        .expect("The type of the plan should not be changed");

    // Assert before/after schemas are equal
    assert_eq!(
        join.schema(),
        swapped_join.schema(),
        "Join schema should not be modified while optimization"
    );

    // Assert join side of big_col swapped in filter expression
    let swapped_filter = swapped_join.filter().unwrap();
    let swapped_big_col_idx = swapped_filter.schema().index_of("big_col").unwrap();
    let swapped_big_col_side = swapped_filter
        .column_indices()
        .get(swapped_big_col_idx)
        .unwrap()
        .side;
    assert_eq!(
        swapped_big_col_side,
        JoinSide::Right,
        "Filter column side should be swapped"
    );

    assert_eq!(
        swapped_join
            .left()
            .partition_statistics(None)
            .unwrap()
            .total_byte_size,
        Precision::Inexact(8192)
    );
    assert_eq!(
        swapped_join
            .right()
            .partition_statistics(None)
            .unwrap()
            .total_byte_size,
        Precision::Inexact(2097152)
    );
}

#[rstest(
        join_type, projection, small_on_right,
        case::inner(JoinType::Inner, vec![1], true),
        case::left(JoinType::Left, vec![1], true),
        case::right(JoinType::Right, vec![1], true),
        case::full(JoinType::Full, vec![1], true),
        case::left_anti(JoinType::LeftAnti, vec![0], false),
        case::left_semi(JoinType::LeftSemi, vec![0], false),
        case::right_anti(JoinType::RightAnti, vec![0], true),
        case::right_semi(JoinType::RightSemi, vec![0], true),
    )]
#[tokio::test]
async fn test_hash_join_swap_on_joins_with_projections(
    join_type: JoinType,
    projection: Vec<usize>,
    small_on_right: bool,
) -> Result<()> {
    let (big, small) = create_big_and_small();

    let left = if small_on_right { &big } else { &small };
    let right = if small_on_right { &small } else { &big };

    let left_on = if small_on_right {
        "big_col"
    } else {
        "small_col"
    };
    let right_on = if small_on_right {
        "small_col"
    } else {
        "big_col"
    };

    let join = Arc::new(HashJoinExec::try_new(
        Arc::clone(left),
        Arc::clone(right),
        vec![(
            Arc::new(Column::new_with_schema(left_on, &left.schema())?),
            Arc::new(Column::new_with_schema(right_on, &right.schema())?),
        )],
        None,
        &join_type,
        Some(projection),
        PartitionMode::Partitioned,
        NullEquality::NullEqualsNothing,
    )?);

    let swapped = join
        .swap_inputs(PartitionMode::Partitioned)
        .expect("swap_hash_join must support joins with projections");
    let swapped_join = swapped.as_any().downcast_ref::<HashJoinExec>().expect(
            "ProjectionExec won't be added above if HashJoinExec contains embedded projection",
        );

    assert_eq!(swapped_join.projection, Some(vec![0_usize]));
    assert_eq!(swapped.schema().fields.len(), 1);
    assert_eq!(swapped.schema().fields[0].name(), "small_col");
    Ok(())
}

fn assert_col_expr(expr: &Arc<dyn PhysicalExpr>, name: &str, index: usize) {
    let col = expr
        .as_any()
        .downcast_ref::<Column>()
        .expect("Projection items should be Column expression");
    assert_eq!(col.name(), name);
    assert_eq!(col.index(), index);
}

#[tokio::test]
async fn test_join_selection_collect_left() {
    let big = Arc::new(StatisticsExec::new(
        big_statistics(),
        Schema::new(vec![Field::new("big_col", DataType::Int32, false)]),
    ));

    let small = Arc::new(StatisticsExec::new(
        small_statistics(),
        Schema::new(vec![Field::new("small_col", DataType::Int32, false)]),
    ));

    let empty = Arc::new(StatisticsExec::new(
        empty_statistics(),
        Schema::new(vec![Field::new("empty_col", DataType::Int32, false)]),
    ));

    let join_on = vec![(
        col("small_col", &small.schema()).unwrap(),
        col("big_col", &big.schema()).unwrap(),
    )];
    check_join_partition_mode(
        Arc::<StatisticsExec>::clone(&small),
        Arc::<StatisticsExec>::clone(&big),
        join_on,
        false,
        PartitionMode::CollectLeft,
    );

    let join_on = vec![(
        col("big_col", &big.schema()).unwrap(),
        col("small_col", &small.schema()).unwrap(),
    )];
    check_join_partition_mode(
        big,
        Arc::<StatisticsExec>::clone(&small),
        join_on,
        true,
        PartitionMode::CollectLeft,
    );

    let join_on = vec![(
        col("small_col", &small.schema()).unwrap(),
        col("empty_col", &empty.schema()).unwrap(),
    )];
    check_join_partition_mode(
        Arc::<StatisticsExec>::clone(&small),
        Arc::<StatisticsExec>::clone(&empty),
        join_on,
        false,
        PartitionMode::CollectLeft,
    );

    let join_on = vec![(
        col("empty_col", &empty.schema()).unwrap(),
        col("small_col", &small.schema()).unwrap(),
    )];
    check_join_partition_mode(empty, small, join_on, true, PartitionMode::CollectLeft);
}

#[tokio::test]
async fn test_join_selection_partitioned() {
    let bigger = Arc::new(StatisticsExec::new(
        bigger_statistics(),
        Schema::new(vec![Field::new("bigger_col", DataType::Int32, false)]),
    ));

    let big = Arc::new(StatisticsExec::new(
        big_statistics(),
        Schema::new(vec![Field::new("big_col", DataType::Int32, false)]),
    ));

    let empty = Arc::new(StatisticsExec::new(
        empty_statistics(),
        Schema::new(vec![Field::new("empty_col", DataType::Int32, false)]),
    ));

    let join_on = vec![(
        Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("bigger_col", &bigger.schema()).unwrap()) as _,
    )];
    check_join_partition_mode(
        Arc::<StatisticsExec>::clone(&big),
        Arc::<StatisticsExec>::clone(&bigger),
        join_on,
        false,
        PartitionMode::Partitioned,
    );

    let join_on = vec![(
        Arc::new(Column::new_with_schema("bigger_col", &bigger.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()) as _,
    )];
    check_join_partition_mode(
        bigger,
        Arc::<StatisticsExec>::clone(&big),
        join_on,
        true,
        PartitionMode::Partitioned,
    );

    let join_on = vec![(
        Arc::new(Column::new_with_schema("empty_col", &empty.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()) as _,
    )];
    check_join_partition_mode(
        Arc::<StatisticsExec>::clone(&empty),
        Arc::<StatisticsExec>::clone(&big),
        join_on,
        false,
        PartitionMode::Partitioned,
    );

    let join_on = vec![(
        Arc::new(Column::new_with_schema("big_col", &big.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("empty_col", &empty.schema()).unwrap()) as _,
    )];
    check_join_partition_mode(big, empty, join_on, false, PartitionMode::Partitioned);
}

fn check_join_partition_mode(
    left: Arc<StatisticsExec>,
    right: Arc<StatisticsExec>,
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    is_swapped: bool,
    expected_mode: PartitionMode,
) {
    let join = Arc::new(
        HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )
        .unwrap(),
    );

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let optimized_join = JoinSelection::new()
        .optimize_plan(join, &optimizer_context)
        .unwrap();

    if !is_swapped {
        let swapped_join = optimized_join
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("The type of the plan should not be changed");
        assert_eq!(*swapped_join.partition_mode(), expected_mode);
    } else {
        let swapping_projection = optimized_join
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .expect("A proj is required to swap columns back to their original order");
        let swapped_join = swapping_projection
            .input()
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("The type of the plan should not be changed");

        assert_eq!(*swapped_join.partition_mode(), expected_mode);
    }
}

#[derive(Debug)]
struct UnboundedStream {
    batch_produce: Option<usize>,
    count: usize,
    batch: RecordBatch,
}

impl Stream for UnboundedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(val) = self.batch_produce {
            if val <= self.count {
                return Poll::Ready(None);
            }
        }
        self.count += 1;
        Poll::Ready(Some(Ok(self.batch.clone())))
    }
}

impl RecordBatchStream for UnboundedStream {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
}

/// A mock execution plan that simply returns the provided data source characteristic
#[derive(Debug, Clone)]
pub struct UnboundedExec {
    batch_produce: Option<usize>,
    batch: RecordBatch,
    cache: PlanProperties,
}

impl UnboundedExec {
    /// Create new exec that clones the given record batch to its output.
    ///
    /// Set `batch_produce` to `Some(n)` to emit exactly `n` batches per partition.
    pub fn new(
        batch_produce: Option<usize>,
        batch: RecordBatch,
        partitions: usize,
    ) -> Self {
        let cache = Self::compute_properties(batch.schema(), batch_produce, partitions);
        Self {
            batch_produce,
            batch,
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        batch_produce: Option<usize>,
        n_partitions: usize,
    ) -> PlanProperties {
        let boundedness = if batch_produce.is_none() {
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            }
        } else {
            Boundedness::Bounded
        };
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(n_partitions),
            EmissionType::Incremental,
            boundedness,
        )
    }
}

impl DisplayAs for UnboundedExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "UnboundedExec: unbounded={}",
                    self.batch_produce.is_none(),
                )
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for UnboundedExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(UnboundedStream {
            batch_produce: self.batch_produce,
            count: 0,
            batch: self.batch.clone(),
        }))
    }
}

#[derive(Eq, PartialEq, Debug)]
pub enum SourceType {
    Unbounded,
    Bounded,
}

/// A mock execution plan that simply returns the provided statistics
#[derive(Debug, Clone)]
pub struct StatisticsExec {
    stats: Statistics,
    schema: Arc<Schema>,
    cache: PlanProperties,
}

impl StatisticsExec {
    pub fn new(stats: Statistics, schema: Schema) -> Self {
        assert_eq!(
                stats.column_statistics.len(), schema.fields().len(),
                "if defined, the column statistics vector length should be the number of fields"
            );
        let cache = Self::compute_properties(Arc::new(schema.clone()));
        Self {
            stats,
            schema: Arc::new(schema),
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(2),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for StatisticsExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "StatisticsExec: col_count={}, row_count={:?}",
                    self.schema.fields().len(),
                    self.stats.num_rows,
                )
            }

            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for StatisticsExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("This plan only serves for testing statistics")
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.stats.clone())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        Ok(if partition.is_some() {
            Statistics::new_unknown(&self.schema)
        } else {
            self.stats.clone()
        })
    }
}

#[test]
fn check_expr_supported() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let supported_expr = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("a", 0)),
        Operator::Plus,
        Arc::new(Column::new("a", 0)),
    )) as Arc<dyn PhysicalExpr>;
    assert!(check_support(&supported_expr, &schema));
    let supported_expr_2 = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
    assert!(check_support(&supported_expr_2, &schema));
    let unsupported_expr = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("a", 0)),
        Operator::Or,
        Arc::new(Column::new("a", 0)),
    )) as Arc<dyn PhysicalExpr>;
    assert!(!check_support(&unsupported_expr, &schema));
    let unsupported_expr_2 = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("a", 0)),
        Operator::Or,
        Arc::new(NegativeExpr::new(Arc::new(Column::new("a", 0)))),
    )) as Arc<dyn PhysicalExpr>;
    assert!(!check_support(&unsupported_expr_2, &schema));
}

struct TestCase {
    case: String,
    initial_sources_unbounded: (SourceType, SourceType),
    initial_join_type: JoinType,
    initial_mode: PartitionMode,
    expected_sources_unbounded: (SourceType, SourceType),
    expected_join_type: JoinType,
    expected_mode: PartitionMode,
    expecting_swap: bool,
}

#[tokio::test]
async fn test_join_with_swap_full() -> Result<()> {
    // NOTE: Currently, some initial conditions are not viable after join order selection.
    //       For example, full join always comes in partitioned mode. See the warning in
    //       function "swap". If this changes in the future, we should update these tests.
    let cases = vec![
        TestCase {
            case: "Bounded - Unbounded 1".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            initial_join_type: JoinType::Full,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            expected_join_type: JoinType::Full,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        },
        TestCase {
            case: "Unbounded - Bounded 2".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
            initial_join_type: JoinType::Full,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
            expected_join_type: JoinType::Full,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        },
        TestCase {
            case: "Bounded - Bounded 3".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            initial_join_type: JoinType::Full,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            expected_join_type: JoinType::Full,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        },
        TestCase {
            case: "Unbounded - Unbounded 4".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
            initial_join_type: JoinType::Full,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
            expected_join_type: JoinType::Full,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        },
    ];
    for case in cases.into_iter() {
        test_join_with_maybe_swap_unbounded_case(case).await?
    }
    Ok(())
}

#[tokio::test]
async fn test_cases_without_collect_left_check() -> Result<()> {
    let mut cases = vec![];
    let join_types = vec![JoinType::LeftSemi, JoinType::Inner];
    for join_type in join_types {
        cases.push(TestCase {
            case: "Unbounded - Bounded / CollectLeft".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::CollectLeft,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            expected_join_type: join_type.swap(),
            expected_mode: PartitionMode::CollectLeft,
            expecting_swap: true,
        });
        cases.push(TestCase {
            case: "Bounded - Unbounded / CollectLeft".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::CollectLeft,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::CollectLeft,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Unbounded - Unbounded / CollectLeft".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::CollectLeft,
            expected_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::CollectLeft,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Bounded - Bounded / CollectLeft".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::CollectLeft,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::CollectLeft,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Unbounded - Bounded / Partitioned".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            expected_join_type: join_type.swap(),
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: true,
        });
        cases.push(TestCase {
            case: "Bounded - Unbounded / Partitioned".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Bounded - Bounded / Partitioned".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Unbounded - Unbounded / Partitioned".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
    }

    for case in cases.into_iter() {
        test_join_with_maybe_swap_unbounded_case(case).await?
    }
    Ok(())
}

#[tokio::test]
async fn test_not_support_collect_left() -> Result<()> {
    let mut cases = vec![];
    // After [JoinSelection] optimization, these join types cannot run in CollectLeft mode except
    // [JoinType::LeftSemi]
    let the_ones_not_support_collect_left = vec![JoinType::Left, JoinType::LeftAnti];
    for join_type in the_ones_not_support_collect_left {
        cases.push(TestCase {
            case: "Unbounded - Bounded".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            expected_join_type: join_type.swap(),
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: true,
        });
        cases.push(TestCase {
            case: "Bounded - Unbounded".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Bounded - Bounded".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Unbounded - Unbounded".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
    }

    for case in cases.into_iter() {
        test_join_with_maybe_swap_unbounded_case(case).await?
    }
    Ok(())
}

#[tokio::test]
async fn test_not_supporting_swaps_possible_collect_left() -> Result<()> {
    let mut cases = vec![];
    let the_ones_not_support_collect_left =
        vec![JoinType::Right, JoinType::RightAnti, JoinType::RightSemi];
    for join_type in the_ones_not_support_collect_left {
        // We expect that (SourceType::Unbounded, SourceType::Bounded) will change, regardless of the
        // statistics.
        cases.push(TestCase {
            case: "Unbounded - Bounded / CollectLeft".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::CollectLeft,
            expected_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::CollectLeft,
            expecting_swap: false,
        });
        // We expect that (SourceType::Bounded, SourceType::Unbounded) will stay same, regardless of the
        // statistics.
        cases.push(TestCase {
            case: "Bounded - Unbounded / CollectLeft".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::CollectLeft,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::CollectLeft,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Unbounded - Unbounded / CollectLeft".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::CollectLeft,
            expected_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::CollectLeft,
            expecting_swap: false,
        });
        //
        cases.push(TestCase {
            case: "Bounded - Bounded / CollectLeft".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::CollectLeft,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::CollectLeft,
            expecting_swap: false,
        });
        // If cases are partitioned, only unbounded & bounded check will affect the order.
        cases.push(TestCase {
            case: "Unbounded - Bounded / Partitioned".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Bounded - Unbounded / Partitioned".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Bounded - Bounded / Partitioned".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Unbounded - Unbounded / Partitioned".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
            initial_join_type: join_type,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
            expected_join_type: join_type,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
    }

    for case in cases.into_iter() {
        test_join_with_maybe_swap_unbounded_case(case).await?
    }
    Ok(())
}

async fn test_join_with_maybe_swap_unbounded_case(t: TestCase) -> Result<()> {
    let left_unbounded = t.initial_sources_unbounded.0 == SourceType::Unbounded;
    let right_unbounded = t.initial_sources_unbounded.1 == SourceType::Unbounded;
    let left_exec = Arc::new(UnboundedExec::new(
        (!left_unbounded).then_some(1),
        RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Int32,
            false,
        )]))),
        2,
    )) as _;
    let right_exec = Arc::new(UnboundedExec::new(
        (!right_unbounded).then_some(1),
        RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "b",
            DataType::Int32,
            false,
        )]))),
        2,
    )) as _;

    let join = Arc::new(HashJoinExec::try_new(
        Arc::clone(&left_exec),
        Arc::clone(&right_exec),
        vec![(
            col("a", &left_exec.schema())?,
            col("b", &right_exec.schema())?,
        )],
        None,
        &t.initial_join_type,
        None,
        t.initial_mode,
        NullEquality::NullEqualsNothing,
    )?) as _;

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let optimized_join_plan =
        JoinSelection::new().optimize_plan(Arc::clone(&join), &optimizer_context)?;

    // If swap did happen
    let projection_added = optimized_join_plan.as_any().is::<ProjectionExec>();
    let plan = if projection_added {
        let proj = optimized_join_plan
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .expect("A proj is required to swap columns back to their original order");
        Arc::<dyn ExecutionPlan>::clone(proj.input())
    } else {
        optimized_join_plan
    };

    if let Some(HashJoinExec {
        left,
        right,
        join_type,
        mode,
        ..
    }) = plan.as_any().downcast_ref::<HashJoinExec>()
    {
        let left_changed = Arc::ptr_eq(left, &right_exec);
        let right_changed = Arc::ptr_eq(right, &left_exec);
        // If this is not equal, we have a bigger problem.
        assert_eq!(left_changed, right_changed);
        assert_eq!(
            (
                t.case.as_str(),
                if left.boundedness().is_unbounded() {
                    SourceType::Unbounded
                } else {
                    SourceType::Bounded
                },
                if right.boundedness().is_unbounded() {
                    SourceType::Unbounded
                } else {
                    SourceType::Bounded
                },
                join_type,
                mode,
                left_changed && right_changed
            ),
            (
                t.case.as_str(),
                t.expected_sources_unbounded.0,
                t.expected_sources_unbounded.1,
                &t.expected_join_type,
                &t.expected_mode,
                t.expecting_swap
            )
        );
    };
    Ok(())
}
