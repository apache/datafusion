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

//! Utilizing exact statistics from sources to avoid scanning data
use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::execution::context::SessionConfig;
use crate::logical_expr::JoinType;
use crate::physical_plan::expressions::Column;
use crate::physical_plan::joins::{
    utils::{ColumnIndex, JoinFilter, JoinSide},
    CrossJoinExec, HashJoinExec,
};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::{ExecutionPlan, PhysicalExpr};

use super::optimizer::PhysicalOptimizerRule;
use super::utils::optimize_children;
use crate::error::Result;

/// BuildProbeOrder reorders the build and probe phase of
/// hash joins. This uses the amount of rows that a datasource has.
/// The rule optimizes the order such that the left (build) side of the join
/// is the smallest.
/// If the information is not available, the order stays the same,
/// so that it could be optimized manually in a query.
#[derive(Default)]
pub struct HashBuildProbeOrder {}

impl HashBuildProbeOrder {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

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

fn supports_swap(join_type: JoinType) -> bool {
    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => true,
        JoinType::Semi | JoinType::Anti => false,
    }
}

fn swap_join_type(join_type: JoinType) -> JoinType {
    match join_type {
        JoinType::Inner => JoinType::Inner,
        JoinType::Full => JoinType::Full,
        JoinType::Left => JoinType::Right,
        JoinType::Right => JoinType::Left,
        _ => unreachable!(),
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
fn swap_join_filter(filter: &Option<JoinFilter>) -> Option<JoinFilter> {
    match filter {
        Some(filter) => {
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

            Some(JoinFilter::new(
                filter.expression().clone(),
                column_indices,
                filter.schema().clone(),
            ))
        }
        None => None,
    }
}

impl PhysicalOptimizerRule for HashBuildProbeOrder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        session_config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = optimize_children(self, plan, session_config)?;
        if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            let left = hash_join.left();
            let right = hash_join.right();
            if should_swap_join_order(&**left, &**right)
                && supports_swap(*hash_join.join_type())
            {
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
                    *hash_join.partition_mode(),
                    hash_join.null_equals_null(),
                )?;
                let proj = ProjectionExec::try_new(
                    swap_reverting_projection(&left.schema(), &right.schema()),
                    Arc::new(new_join),
                )?;
                return Ok(Arc::new(proj));
            }
        } else if let Some(cross_join) = plan.as_any().downcast_ref::<CrossJoinExec>() {
            let left = cross_join.left();
            let right = cross_join.right();
            if should_swap_join_order(&**left, &**right) {
                let new_join =
                    CrossJoinExec::try_new(Arc::clone(right), Arc::clone(left))?;
                let proj = ProjectionExec::try_new(
                    swap_reverting_projection(&left.schema(), &right.schema()),
                    Arc::new(new_join),
                )?;
                return Ok(Arc::new(proj));
            }
        }
        Ok(plan)
    }

    fn name(&self) -> &str {
        "hash_build_probe_order"
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

        let optimized_join = HashBuildProbeOrder::new()
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

    /// Compare the input plan with the plan after running the probe order optimizer.
    macro_rules! assert_optimized {
        ($EXPECTED_LINES: expr, $PLAN: expr) => {
            let expected_lines =
                $EXPECTED_LINES.iter().map(|s| *s).collect::<Vec<&str>>();

            let optimized = HashBuildProbeOrder::new()
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
            &JoinType::Left,
            PartitionMode::CollectLeft,
            &false,
        )
        .unwrap();

        let optimized_join = HashBuildProbeOrder::new()
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
}
