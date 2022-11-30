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

//! Select the proper PartitionMode and build side based on the avaliable statistics for hash join.
use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::config::OPT_HASH_JOIN_SINGLE_PARTITION_THRESHOLD;
use crate::execution::context::SessionConfig;
use crate::logical_expr::JoinType;
use crate::physical_plan::expressions::Column;
use crate::physical_plan::joins::{
    utils::{ColumnIndex, JoinFilter, JoinSide},
    CrossJoinExec, HashJoinExec, PartitionMode,
};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::{ExecutionPlan, PhysicalExpr};

use super::optimizer::PhysicalOptimizerRule;
use crate::error::Result;
use crate::physical_plan::rewrite::TreeNodeRewritable;

/// For hash join with the partition mode [PartitionMode::Auto], JoinSelection rule will make
/// a cost based decision to select which PartitionMode mode(Partitioned/CollectLeft) is optimal
/// based on the available statistics that the inputs have.
/// If the statistics information is not available, the partition mode will fall back to [PartitionMode::Partitioned].
///
/// JoinSelection rule will also reorder the build and probe phase of the hash joins
/// based on the avaliable statistics that the inputs have.
/// The rule optimizes the order such that the left (build) side of the join is the smallest.
/// If the statistics information is not available, the order stays the same as the original query.
/// JoinSelection rule will also swap the left and right sides for cross join to keep the left side
/// is the smallest.
#[derive(Default)]
pub struct JoinSelection {}

impl JoinSelection {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

// TODO we need some performance test for Right Semi/Right Join swap to Left Semi/Left Join in case that the right side is smaller but not much smaller.
// TODO In PrestoSQL, the optimizer flips join sides only if one side is much smaller than the other by more than SIZE_DIFFERENCE_THRESHOLD times, by default is is 8 times.
fn should_swap_join_order(left: &dyn ExecutionPlan, right: &dyn ExecutionPlan) -> bool {
    // Get the left and right table's total bytes
    // If both the left and right tables contain total_byte_size statistics,
    // use `total_byte_size` to determine `should_swap_join_order`, else use `num_rows`
    let (left_size, right_size) = match (
        left.statistics().total_byte_size,
        right.statistics().total_byte_size,
    ) {
        (Some(l), Some(r)) => (Some(l), Some(r)),
        _ => (left.statistics().num_rows, right.statistics().num_rows),
    };

    match (left_size, right_size) {
        (Some(l), Some(r)) => l > r,
        _ => false,
    }
}

fn supports_collect_by_size(
    plan: &dyn ExecutionPlan,
    collection_size_threshold: usize,
) -> bool {
    // Currently we do not trust the 0 value from stats, due to stats collection might have bug
    // TODO check the logic in datasource::get_statistics_with_limit()
    if let Some(size) = plan.statistics().total_byte_size {
        size != 0 && size < collection_size_threshold
    } else if let Some(row_count) = plan.statistics().num_rows {
        row_count != 0 && row_count < collection_size_threshold
    } else {
        false
    }
}

fn supports_swap(join_type: JoinType) -> bool {
    match join_type {
        JoinType::Inner
        | JoinType::Left
        | JoinType::Right
        | JoinType::Full
        | JoinType::LeftSemi
        | JoinType::RightSemi
        | JoinType::LeftAnti
        | JoinType::RightAnti => true,
    }
}

fn swap_join_type(join_type: JoinType) -> JoinType {
    match join_type {
        JoinType::Inner => JoinType::Inner,
        JoinType::Full => JoinType::Full,
        JoinType::Left => JoinType::Right,
        JoinType::Right => JoinType::Left,
        JoinType::LeftSemi => JoinType::RightSemi,
        JoinType::RightSemi => JoinType::LeftSemi,
        JoinType::LeftAnti => JoinType::RightAnti,
        JoinType::RightAnti => JoinType::LeftAnti,
    }
}

fn swap_hash_join(
    hash_join: &HashJoinExec,
    partition_mode: PartitionMode,
    left: &Arc<dyn ExecutionPlan>,
    right: &Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let new_join = HashJoinExec::try_new(
        Arc::clone(right),
        Arc::clone(left),
        hash_join
            .on()
            .iter()
            .map(|(l, r)| (r.clone(), l.clone()))
            .collect(),
        swap_join_filter(hash_join.filter()),
        &swap_join_type(*hash_join.join_type()),
        partition_mode,
        hash_join.null_equals_null(),
    )?;
    if matches!(
        hash_join.join_type(),
        JoinType::LeftSemi
            | JoinType::RightSemi
            | JoinType::LeftAnti
            | JoinType::RightAnti
    ) {
        Ok(Arc::new(new_join))
    } else {
        // TODO avoid adding ProjectionExec again and again, only adding Final Projection
        let proj = ProjectionExec::try_new(
            swap_reverting_projection(&left.schema(), &right.schema()),
            Arc::new(new_join),
        )?;
        Ok(Arc::new(proj))
    }
}

/// When the order of the join is changed by the optimizer,
/// the columns in the output should not be impacted.
/// This helper creates the expressions that will allow to swap
/// back the values from the original left as first columns and
/// those on the right next
fn swap_reverting_projection(
    left_schema: &Schema,
    right_schema: &Schema,
) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
    let right_cols = right_schema.fields().iter().enumerate().map(|(i, f)| {
        (
            Arc::new(Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>,
            f.name().to_owned(),
        )
    });
    let right_len = right_cols.len();
    let left_cols = left_schema.fields().iter().enumerate().map(|(i, f)| {
        (
            Arc::new(Column::new(f.name(), right_len + i)) as Arc<dyn PhysicalExpr>,
            f.name().to_owned(),
        )
    });

    left_cols.chain(right_cols).collect()
}

/// Swaps join sides for filter column indices and produces new JoinFilter
fn swap_join_filter(filter: Option<&JoinFilter>) -> Option<JoinFilter> {
    filter.map(|filter| {
        let column_indices = filter
            .column_indices()
            .iter()
            .map(|idx| {
                let side = if matches!(idx.side, JoinSide::Left) {
                    JoinSide::Right
                } else {
                    JoinSide::Left
                };
                ColumnIndex {
                    index: idx.index,
                    side,
                }
            })
            .collect();

        JoinFilter::new(
            filter.expression().clone(),
            column_indices,
            filter.schema().clone(),
        )
    })
}

impl PhysicalOptimizerRule for JoinSelection {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        session_config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let collect_left_threshold: usize = session_config
            .config_options()
            .read()
            .get_u64(OPT_HASH_JOIN_SINGLE_PARTITION_THRESHOLD)
            .unwrap_or_default()
            .try_into()
            .unwrap();
        plan.transform_up(&|plan| {
            if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
                match hash_join.partition_mode() {
                    PartitionMode::Auto => {
                        try_collect_left(hash_join, Some(collect_left_threshold))?
                            .map_or_else(
                                || Ok(Some(partitioned_hash_join(hash_join)?)),
                                |v| Ok(Some(v)),
                            )
                    }
                    PartitionMode::CollectLeft => try_collect_left(hash_join, None)?
                        .map_or_else(
                            || Ok(Some(partitioned_hash_join(hash_join)?)),
                            |v| Ok(Some(v)),
                        ),
                    PartitionMode::Partitioned => {
                        let left = hash_join.left();
                        let right = hash_join.right();
                        if should_swap_join_order(&**left, &**right)
                            && supports_swap(*hash_join.join_type())
                        {
                            Ok(Some(swap_hash_join(
                                hash_join,
                                PartitionMode::Partitioned,
                                left,
                                right,
                            )?))
                        } else {
                            Ok(None)
                        }
                    }
                }
            } else if let Some(cross_join) = plan.as_any().downcast_ref::<CrossJoinExec>()
            {
                let left = cross_join.left();
                let right = cross_join.right();
                if should_swap_join_order(&**left, &**right) {
                    let new_join =
                        CrossJoinExec::try_new(Arc::clone(right), Arc::clone(left))?;
                    // TODO avoid adding ProjectionExec again and again, only adding Final Projection
                    let proj = ProjectionExec::try_new(
                        swap_reverting_projection(&left.schema(), &right.schema()),
                        Arc::new(new_join),
                    )?;
                    Ok(Some(Arc::new(proj)))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        })
    }

    fn name(&self) -> &str {
        "join_selection"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Try to create the PartitionMode::CollectLeft HashJoinExec when possible.
/// The method will first consider the current join type and check whether it is applicable to run CollectLeft mode
/// and will try to swap the join if the orignal type is unapplicable to run CollectLeft.
/// When the collect_threshold is provided, the method will also check both the left side and right side sizes
///
/// For [JoinType::Full], it is alway unable to run CollectLeft mode and will return None.
/// For [JoinType::Left] and [JoinType::LeftAnti], can not run CollectLeft mode, should swap join type to [JoinType::Right] and [JoinType::RightAnti]
fn try_collect_left(
    hash_join: &HashJoinExec,
    collect_threshold: Option<usize>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let left = hash_join.left();
    let right = hash_join.right();
    let join_type = hash_join.join_type();

    let left_can_collect = match join_type {
        JoinType::Left | JoinType::Full | JoinType::LeftAnti => false,
        JoinType::Inner
        | JoinType::LeftSemi
        | JoinType::Right
        | JoinType::RightSemi
        | JoinType::RightAnti => collect_threshold.map_or(true, |threshold| {
            supports_collect_by_size(&**left, threshold)
        }),
    };
    let right_can_collect = match join_type {
        JoinType::Right | JoinType::Full | JoinType::RightAnti => false,
        JoinType::Inner
        | JoinType::RightSemi
        | JoinType::Left
        | JoinType::LeftSemi
        | JoinType::LeftAnti => collect_threshold.map_or(true, |threshold| {
            supports_collect_by_size(&**right, threshold)
        }),
    };
    match (left_can_collect, right_can_collect) {
        (true, true) => {
            if should_swap_join_order(&**left, &**right)
                && supports_swap(*hash_join.join_type())
            {
                Ok(Some(swap_hash_join(
                    hash_join,
                    PartitionMode::CollectLeft,
                    left,
                    right,
                )?))
            } else {
                Ok(Some(Arc::new(HashJoinExec::try_new(
                    Arc::clone(left),
                    Arc::clone(right),
                    hash_join.on().to_vec(),
                    hash_join.filter().cloned(),
                    hash_join.join_type(),
                    PartitionMode::CollectLeft,
                    hash_join.null_equals_null(),
                )?)))
            }
        }
        (true, false) => Ok(Some(Arc::new(HashJoinExec::try_new(
            Arc::clone(left),
            Arc::clone(right),
            hash_join.on().to_vec(),
            hash_join.filter().cloned(),
            hash_join.join_type(),
            PartitionMode::CollectLeft,
            hash_join.null_equals_null(),
        )?))),
        (false, true) => {
            if supports_swap(*hash_join.join_type()) {
                Ok(Some(swap_hash_join(
                    hash_join,
                    PartitionMode::CollectLeft,
                    left,
                    right,
                )?))
            } else {
                Ok(None)
            }
        }
        (false, false) => Ok(None),
    }
}

fn partitioned_hash_join(hash_join: &HashJoinExec) -> Result<Arc<dyn ExecutionPlan>> {
    let left = hash_join.left();
    let right = hash_join.right();
    if should_swap_join_order(&**left, &**right) && supports_swap(*hash_join.join_type())
    {
        swap_hash_join(hash_join, PartitionMode::Partitioned, left, right)
    } else {
        Ok(Arc::new(HashJoinExec::try_new(
            Arc::clone(left),
            Arc::clone(right),
            hash_join.on().to_vec(),
            hash_join.filter().cloned(),
            hash_join.join_type(),
            PartitionMode::Partitioned,
            hash_join.null_equals_null(),
        )?))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        physical_plan::{
            displayable, joins::PartitionMode, ColumnStatistics, Statistics,
        },
        test::exec::StatisticsExec,
    };

    use super::*;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;

    fn create_big_and_small() -> (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) {
        let big = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Some(10),
                total_byte_size: Some(100000),
                ..Default::default()
            },
            Schema::new(vec![Field::new("big_col", DataType::Int32, false)]),
        ));

        let small = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Some(100000),
                total_byte_size: Some(10),
                ..Default::default()
            },
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
    ) -> Option<Vec<ColumnStatistics>> {
        Some(vec![ColumnStatistics {
            distinct_count,
            min_value: min.map(|size| ScalarValue::UInt64(Some(size))),
            max_value: max.map(|size| ScalarValue::UInt64(Some(size))),
            ..Default::default()
        }])
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
                num_rows: Some(100_000),
                column_statistics: create_column_stats(
                    Some(0),
                    Some(50_000),
                    Some(50_000),
                ),
                ..Default::default()
            },
            Schema::new(vec![Field::new("big_col", DataType::Int32, false)]),
        ));

        let medium = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Some(10_000),
                column_statistics: create_column_stats(
                    Some(1000),
                    Some(5000),
                    Some(1000),
                ),
                ..Default::default()
            },
            Schema::new(vec![Field::new("medium_col", DataType::Int32, false)]),
        ));

        let small = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Some(1000),
                column_statistics: create_column_stats(
                    Some(0),
                    Some(100_000),
                    Some(1000),
                ),
                ..Default::default()
            },
            Schema::new(vec![Field::new("small_col", DataType::Int32, false)]),
        ));

        (big, medium, small)
    }

    #[tokio::test]
    async fn test_join_with_swap() {
        let (big, small) = create_big_and_small();

        let join = HashJoinExec::try_new(
            Arc::clone(&big),
            Arc::clone(&small),
            vec![(
                Column::new_with_schema("big_col", &big.schema()).unwrap(),
                Column::new_with_schema("small_col", &small.schema()).unwrap(),
            )],
            None,
            &JoinType::Left,
            PartitionMode::CollectLeft,
            &false,
        )
        .unwrap();

        let optimized_join = JoinSelection::new()
            .optimize(Arc::new(join), &SessionConfig::new())
            .unwrap();

        let swapping_projection = optimized_join
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .expect("A proj is required to swap columns back to their original order");

        assert_eq!(swapping_projection.expr().len(), 2);
        let (col, name) = &swapping_projection.expr()[0];
        assert_eq!(name, "big_col");
        assert_col_expr(col, "big_col", 1);
        let (col, name) = &swapping_projection.expr()[1];
        assert_eq!(name, "small_col");
        assert_col_expr(col, "small_col", 0);

        let swapped_join = swapping_projection
            .input()
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("The type of the plan should not be changed");

        assert_eq!(swapped_join.left().statistics().total_byte_size, Some(10));
        assert_eq!(
            swapped_join.right().statistics().total_byte_size,
            Some(100000)
        );
    }

    #[tokio::test]
    async fn test_left_join_with_swap() {
        let (big, small) = create_big_and_small();
        // Left out join should alway swap when the mode is PartitionMode::CollectLeft, even left side is small and right side is large
        let join = HashJoinExec::try_new(
            Arc::clone(&small),
            Arc::clone(&big),
            vec![(
                Column::new_with_schema("small_col", &small.schema()).unwrap(),
                Column::new_with_schema("big_col", &big.schema()).unwrap(),
            )],
            None,
            &JoinType::Left,
            PartitionMode::CollectLeft,
            &false,
        )
        .unwrap();

        let optimized_join = JoinSelection::new()
            .optimize(Arc::new(join), &SessionConfig::new())
            .unwrap();

        let swapping_projection = optimized_join
            .as_any()
            .downcast_ref::<ProjectionExec>()
            .expect("A proj is required to swap columns back to their original order");

        assert_eq!(swapping_projection.expr().len(), 2);
        println!("swapping_projection {:?}", swapping_projection);
        let (col, name) = &swapping_projection.expr()[0];
        assert_eq!(name, "small_col");
        assert_col_expr(col, "small_col", 1);
        let (col, name) = &swapping_projection.expr()[1];
        assert_eq!(name, "big_col");
        assert_col_expr(col, "big_col", 0);

        let swapped_join = swapping_projection
            .input()
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("The type of the plan should not be changed");

        assert_eq!(
            swapped_join.left().statistics().total_byte_size,
            Some(100000)
        );
        assert_eq!(swapped_join.right().statistics().total_byte_size, Some(10));
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
                    Column::new_with_schema("big_col", &big.schema()).unwrap(),
                    Column::new_with_schema("small_col", &small.schema()).unwrap(),
                )],
                None,
                &join_type,
                PartitionMode::Partitioned,
                &false,
            )
            .unwrap();

            let original_schema = join.schema();

            let optimized_join = JoinSelection::new()
                .optimize(Arc::new(join), &SessionConfig::new())
                .unwrap();

            let swapped_join = optimized_join
                .as_any()
                .downcast_ref::<HashJoinExec>()
                .expect(
                    "A proj is not required to swap columns back to their original order",
                );

            assert_eq!(swapped_join.schema().fields().len(), 1);

            assert_eq!(swapped_join.left().statistics().total_byte_size, Some(10));
            assert_eq!(
                swapped_join.right().statistics().total_byte_size,
                Some(100000)
            );

            assert_eq!(original_schema, swapped_join.schema());
        }
    }

    /// Compare the input plan with the plan after running the probe order optimizer.
    macro_rules! assert_optimized {
        ($EXPECTED_LINES: expr, $PLAN: expr) => {
            let expected_lines =
                $EXPECTED_LINES.iter().map(|s| *s).collect::<Vec<&str>>();

            let optimized = JoinSelection::new()
                .optimize(Arc::new($PLAN), &SessionConfig::new())
                .unwrap();

            let plan = displayable(optimized.as_ref()).indent().to_string();
            let actual_lines = plan.split("\n").collect::<Vec<&str>>();

            assert_eq!(
                &expected_lines, &actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
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
                Column::new_with_schema("big_col", &big.schema()).unwrap(),
                Column::new_with_schema("small_col", &small.schema()).unwrap(),
            )],
            None,
            &JoinType::Inner,
            PartitionMode::CollectLeft,
            &false,
        )
        .unwrap();
        let child_schema = child_join.schema();

        // Form join tree `medium LEFT JOIN (big JOIN small)`
        let join = HashJoinExec::try_new(
            Arc::clone(&medium),
            Arc::new(child_join),
            vec![(
                Column::new_with_schema("medium_col", &medium.schema()).unwrap(),
                Column::new_with_schema("small_col", &child_schema).unwrap(),
            )],
            None,
            &JoinType::Left,
            PartitionMode::CollectLeft,
            &false,
        )
        .unwrap();

        // Hash join uses the left side to build the hash table, and right side to probe it. We want
        // to keep left as small as possible, so if we can estimate (with a reasonable margin of error)
        // that the left side is smaller than the right side, we should swap the sides.
        //
        // The first hash join's left is 'small' table (with 1000 rows), and the second hash join's
        // left is the F(small IJ big) which has an estimated cardinality of 2000 rows (vs medium which
        // has an exact cardinality of 10_000 rows).
        let expected = [
            "ProjectionExec: expr=[medium_col@2 as medium_col, big_col@0 as big_col, small_col@1 as small_col]",
            "  HashJoinExec: mode=CollectLeft, join_type=Right, on=[(Column { name: \"small_col\", index: 1 }, Column { name: \"medium_col\", index: 0 })]",
            "    ProjectionExec: expr=[big_col@1 as big_col, small_col@0 as small_col]",
            "      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(Column { name: \"small_col\", index: 0 }, Column { name: \"big_col\", index: 0 })]",
            "        StatisticsExec: col_count=1, row_count=Some(1000)",
            "        StatisticsExec: col_count=1, row_count=Some(100000)",
            "    StatisticsExec: col_count=1, row_count=Some(10000)",
            ""
        ];
        assert_optimized!(expected, join);
    }

    #[tokio::test]
    async fn test_join_no_swap() {
        let (big, small) = create_big_and_small();
        let join = HashJoinExec::try_new(
            Arc::clone(&small),
            Arc::clone(&big),
            vec![(
                Column::new_with_schema("small_col", &small.schema()).unwrap(),
                Column::new_with_schema("big_col", &big.schema()).unwrap(),
            )],
            None,
            &JoinType::Inner,
            PartitionMode::CollectLeft,
            &false,
        )
        .unwrap();

        let optimized_join = JoinSelection::new()
            .optimize(Arc::new(join), &SessionConfig::new())
            .unwrap();

        let swapped_join = optimized_join
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("The type of the plan should not be changed");

        assert_eq!(swapped_join.left().statistics().total_byte_size, Some(10));
        assert_eq!(
            swapped_join.right().statistics().total_byte_size,
            Some(100000)
        );
    }

    #[tokio::test]
    async fn test_swap_reverting_projection() {
        let left_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);

        let right_schema = Schema::new(vec![Field::new("c", DataType::Int32, false)]);

        let proj = swap_reverting_projection(&left_schema, &right_schema);

        assert_eq!(proj.len(), 3);

        let (col, name) = &proj[0];
        assert_eq!(name, "a");
        assert_col_expr(col, "a", 1);

        let (col, name) = &proj[1];
        assert_eq!(name, "b");
        assert_col_expr(col, "b", 2);

        let (col, name) = &proj[2];
        assert_eq!(name, "c");
        assert_col_expr(col, "c", 0);
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
            Statistics {
                num_rows: Some(10000000),
                total_byte_size: Some(10000000),
                ..Default::default()
            },
            Schema::new(vec![Field::new("big_col", DataType::Int32, false)]),
        ));

        let small = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Some(10),
                total_byte_size: Some(10),
                ..Default::default()
            },
            Schema::new(vec![Field::new("small_col", DataType::Int32, false)]),
        ));

        let empty = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: None,
                total_byte_size: None,
                ..Default::default()
            },
            Schema::new(vec![Field::new("empty_col", DataType::Int32, false)]),
        ));

        let join_on = vec![(
            Column::new_with_schema("small_col", &small.schema()).unwrap(),
            Column::new_with_schema("big_col", &big.schema()).unwrap(),
        )];
        check_join_partition_mode(
            small.clone(),
            big.clone(),
            join_on,
            false,
            PartitionMode::CollectLeft,
        );

        let join_on = vec![(
            Column::new_with_schema("big_col", &big.schema()).unwrap(),
            Column::new_with_schema("small_col", &small.schema()).unwrap(),
        )];
        check_join_partition_mode(
            big,
            small.clone(),
            join_on,
            true,
            PartitionMode::CollectLeft,
        );

        let join_on = vec![(
            Column::new_with_schema("small_col", &small.schema()).unwrap(),
            Column::new_with_schema("empty_col", &empty.schema()).unwrap(),
        )];
        check_join_partition_mode(
            small.clone(),
            empty.clone(),
            join_on,
            false,
            PartitionMode::CollectLeft,
        );

        let join_on = vec![(
            Column::new_with_schema("empty_col", &empty.schema()).unwrap(),
            Column::new_with_schema("small_col", &small.schema()).unwrap(),
        )];
        check_join_partition_mode(
            empty,
            small,
            join_on,
            true,
            PartitionMode::CollectLeft,
        );
    }

    #[tokio::test]
    async fn test_join_selection_partitioned() {
        let big1 = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Some(10000000),
                total_byte_size: Some(10000000),
                ..Default::default()
            },
            Schema::new(vec![Field::new("big_col1", DataType::Int32, false)]),
        ));

        let big2 = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Some(20000000),
                total_byte_size: Some(20000000),
                ..Default::default()
            },
            Schema::new(vec![Field::new("big_col2", DataType::Int32, false)]),
        ));

        let empty = Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: None,
                total_byte_size: None,
                ..Default::default()
            },
            Schema::new(vec![Field::new("empty_col", DataType::Int32, false)]),
        ));

        let join_on = vec![(
            Column::new_with_schema("big_col1", &big1.schema()).unwrap(),
            Column::new_with_schema("big_col2", &big2.schema()).unwrap(),
        )];
        check_join_partition_mode(
            big1.clone(),
            big2.clone(),
            join_on,
            false,
            PartitionMode::Partitioned,
        );

        let join_on = vec![(
            Column::new_with_schema("big_col2", &big2.schema()).unwrap(),
            Column::new_with_schema("big_col1", &big1.schema()).unwrap(),
        )];
        check_join_partition_mode(
            big2,
            big1.clone(),
            join_on,
            true,
            PartitionMode::Partitioned,
        );

        let join_on = vec![(
            Column::new_with_schema("empty_col", &empty.schema()).unwrap(),
            Column::new_with_schema("big_col1", &big1.schema()).unwrap(),
        )];
        check_join_partition_mode(
            empty.clone(),
            big1.clone(),
            join_on,
            false,
            PartitionMode::Partitioned,
        );

        let join_on = vec![(
            Column::new_with_schema("big_col1", &big1.schema()).unwrap(),
            Column::new_with_schema("empty_col", &empty.schema()).unwrap(),
        )];
        check_join_partition_mode(
            big1,
            empty,
            join_on,
            false,
            PartitionMode::Partitioned,
        );
    }

    fn check_join_partition_mode(
        left: Arc<StatisticsExec>,
        right: Arc<StatisticsExec>,
        on: Vec<(Column, Column)>,
        is_swapped: bool,
        expected_mode: PartitionMode,
    ) {
        let join = HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            PartitionMode::Auto,
            &false,
        )
        .unwrap();

        let optimized_join = JoinSelection::new()
            .optimize(Arc::new(join), &SessionConfig::new())
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
                .expect(
                    "A proj is required to swap columns back to their original order",
                );
            let swapped_join = swapping_projection
                .input()
                .as_any()
                .downcast_ref::<HashJoinExec>()
                .expect("The type of the plan should not be changed");

            assert_eq!(*swapped_join.partition_mode(), expected_mode);
        }
    }
}
