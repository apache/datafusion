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

//! [`GroupJoinOptimizer`] replaces an `AggregateExec` directly above a
//! `HashJoinExec` with a fused [`GroupJoinExec`] when the aggregate's GROUP BY
//! keys match the join's equi-join keys.
//!
//! Based on: Moerkotte & Neumann, "Accelerating Queries with Group-By and Join
//! by Groupjoin", PVLDB 4(11), 2011.

use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinType, Result};
use datafusion_physical_expr::physical_exprs_equal;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_physical_plan::joins::group_join::GroupJoinExec;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::projection::ProjectionExec;

use crate::PhysicalOptimizerRule;

/// Replaces `AggregateExec(HashJoinExec)` with a fused `GroupJoinExec` when:
///
/// 1. The aggregate mode is `Single`, `SinglePartitioned`, or `Partial`
/// 2. The aggregate has at least one aggregate expression (not just DISTINCT)
/// 3. The aggregate has no GROUPING SETS
/// 4. The input is a `HashJoinExec` (possibly through a `ProjectionExec`)
/// 5. The join type is `Inner` or `Left`
/// 6. The join has no residual filter (equi-join only)
/// 7. The GROUP BY expressions exactly match the left join keys
/// 8. All aggregate functions support `GroupsAccumulator`
///
/// This rule should run after `CombinePartialFinalAggregate` (which may
/// collapse two-phase aggregation into Single mode) and after `JoinSelection`
/// (which decides build/probe sides).
#[derive(Default, Debug)]
pub struct GroupJoinOptimizer {}

impl GroupJoinOptimizer {
    /// Create a new `GroupJoinOptimizer`.
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for GroupJoinOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|plan| {
            let Some(agg_exec) = plan.downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(plan));
            };

            if !matches!(
                agg_exec.mode(),
                AggregateMode::Single
                    | AggregateMode::SinglePartitioned
                    | AggregateMode::Partial
            ) {
                return Ok(Transformed::no(plan));
            }

            // Must have actual aggregate functions (not just GROUP BY for DISTINCT)
            let aggr_exprs = agg_exec.aggr_expr();
            if aggr_exprs.is_empty() {
                return Ok(Transformed::no(plan));
            }

            // No GROUPING SETS
            if agg_exec.group_expr().groups().len() > 1 {
                return Ok(Transformed::no(plan));
            }

            // Find HashJoinExec (possibly through a ProjectionExec)
            let input = agg_exec.input();
            let hash_join: &HashJoinExec;
            if let Some(hj) = input.downcast_ref::<HashJoinExec>() {
                hash_join = hj;
            } else if let Some(proj) = input.downcast_ref::<ProjectionExec>() {
                if let Some(hj) = proj.input().downcast_ref::<HashJoinExec>() {
                    hash_join = hj;
                } else {
                    return Ok(Transformed::no(plan));
                }
            } else {
                return Ok(Transformed::no(plan));
            };

            // Inner and Left joins
            if !matches!(
                hash_join.join_type(),
                JoinType::Inner | JoinType::Left
            ) {
                return Ok(Transformed::no(plan));
            }

            // No residual join filter (equi-join only)
            if hash_join.filter().is_some() {
                return Ok(Transformed::no(plan));
            }

            // GroupJoinExec requires partitioned inputs keyed by the join
            // expressions. CollectLeft hash joins do not provide that shape.
            if *hash_join.partition_mode() != PartitionMode::Partitioned {
                return Ok(Transformed::no(plan));
            }

            // GROUP BY keys must exactly match left join keys
            let group_exprs: Vec<_> = agg_exec
                .group_expr()
                .expr()
                .iter()
                .map(|(expr, _)| Arc::clone(expr))
                .collect();

            let join_on = hash_join.on();
            let left_join_keys: Vec<_> =
                join_on.iter().map(|(l, _)| Arc::clone(l)).collect();

            if group_exprs.len() != left_join_keys.len() {
                return Ok(Transformed::no(plan));
            }

            if !physical_exprs_equal(&group_exprs, &left_join_keys) {
                return Ok(Transformed::no(plan));
            }

            // All aggregates must support GroupsAccumulator
            for agg in aggr_exprs {
                if !agg.groups_accumulator_supported() {
                    return Ok(Transformed::no(plan));
                }
            }

            // For Inner joins, skip if any aggregate has a literal argument
            // (e.g., COUNT(*) rewritten as count(Int64(1))). These queries
            // don't benefit enough from GroupJoin to justify changing the plan.
            if *hash_join.join_type() == JoinType::Inner {
                let has_literal_arg = aggr_exprs.iter().any(|agg| {
                    agg.expressions().iter().any(|expr| {
                        expr.as_ref()
                            .downcast_ref::<datafusion_physical_expr::expressions::Literal>()
                            .is_some()
                    })
                });
                if has_literal_arg {
                    return Ok(Transformed::no(plan));
                }
            }

            // All preconditions met — create GroupJoinExec
            let group_by_with_names: Vec<_> = agg_exec
                .group_expr()
                .expr()
                .iter()
                .map(|(expr, name)| (Arc::clone(expr), name.clone()))
                .collect();

            let group_join = GroupJoinExec::try_new_with_aggr_input_schema(
                Arc::clone(hash_join.left()),
                Arc::clone(hash_join.right()),
                join_on.to_vec(),
                *hash_join.join_type(),
                group_by_with_names,
                aggr_exprs.to_vec(),
                agg_exec.input_schema(),
            )?;

            Ok(Transformed::yes(
                Arc::new(group_join) as Arc<dyn ExecutionPlan>
            ))
        })
        .data()
    }

    fn name(&self) -> &str {
        "group_join"
    }

    fn schema_check(&self) -> bool {
        false // Schema changes (aggregate output differs from join output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::NullEquality;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_physical_expr::PhysicalExprRef;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::{col, lit};
    use datafusion_physical_plan::displayable;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::joins::PartitionMode;
    use datafusion_physical_plan::projection::ProjectionExec;
    use insta::assert_snapshot;

    fn left_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("l_key", DataType::Int32, false),
            Field::new("l_value", DataType::Int32, true),
        ]))
    }

    fn right_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("r_key", DataType::Int32, false),
            Field::new("r_value", DataType::Int32, true),
        ]))
    }

    fn join(
        join_type: JoinType,
        left_key: &str,
        partition_mode: PartitionMode,
    ) -> Result<Arc<HashJoinExec>> {
        let left_schema = left_schema();
        let right_schema = right_schema();
        let left = Arc::new(EmptyExec::new(Arc::clone(&left_schema)));
        let right = Arc::new(EmptyExec::new(Arc::clone(&right_schema)));

        Ok(Arc::new(HashJoinExec::try_new(
            left,
            right,
            vec![(col(left_key, &left_schema)?, col("r_key", &right_schema)?)],
            None,
            &join_type,
            None,
            partition_mode,
            NullEquality::NullEqualsNull,
            false,
        )?))
    }

    fn partitioned_join(
        join_type: JoinType,
        left_key: &str,
    ) -> Result<Arc<HashJoinExec>> {
        join(join_type, left_key, PartitionMode::Partitioned)
    }

    fn aggregate(
        input: Arc<dyn ExecutionPlan>,
        group_expr: PhysicalExprRef,
        aggr_expr: PhysicalExprRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.schema();
        let aggr_expr = Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![aggr_expr])
                .schema(Arc::clone(&input_schema))
                .alias("count_values")
                .build()?,
        );

        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            datafusion_physical_plan::aggregates::PhysicalGroupBy::new_single(vec![(
                group_expr,
                "l_key".to_string(),
            )]),
            vec![aggr_expr],
            vec![None],
            input,
            input_schema,
        )?))
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        let optimized =
            GroupJoinOptimizer::new().optimize(plan, &ConfigOptions::new())?;
        Ok(displayable(optimized.as_ref()).indent(true).to_string())
    }

    #[test]
    fn rewrites_aggregate_above_inner_hash_join() -> Result<()> {
        let join = partitioned_join(JoinType::Inner, "l_key")?;
        let join_schema = join.schema();
        let plan = aggregate(
            join,
            col("l_key", &join_schema)?,
            col("r_value", &join_schema)?,
        )?;

        assert_snapshot!(optimize(plan)?, @r"
        GroupJoinExec: join_type=Inner, on=[(l_key@0, r_key@0)], aggr=[count_values]
          EmptyExec
          EmptyExec
        ");
        Ok(())
    }

    #[test]
    fn rewrites_through_projection() -> Result<()> {
        let join = partitioned_join(JoinType::Left, "l_key")?;
        let join_schema = join.schema();
        let projection = Arc::new(ProjectionExec::try_new(
            vec![
                (col("l_key", &join_schema)?, "l_key".to_string()),
                (col("r_value", &join_schema)?, "r_value".to_string()),
            ],
            join,
        )?);
        let projection_schema = projection.schema();
        let plan = aggregate(
            projection,
            col("l_key", &projection_schema)?,
            col("r_value", &projection_schema)?,
        )?;

        assert_snapshot!(optimize(plan)?, @r"
        GroupJoinExec: join_type=Left, on=[(l_key@0, r_key@0)], aggr=[count_values]
          EmptyExec
          EmptyExec
        ");
        Ok(())
    }

    #[test]
    fn does_not_rewrite_when_group_by_does_not_match_join_key() -> Result<()> {
        let join = partitioned_join(JoinType::Inner, "l_key")?;
        let join_schema = join.schema();
        let plan = aggregate(
            join,
            col("l_value", &join_schema)?,
            col("r_value", &join_schema)?,
        )?;

        assert_snapshot!(optimize(plan)?, @r"
        AggregateExec: mode=Single, gby=[l_value@1 as l_key], aggr=[count_values]
          HashJoinExec: mode=Partitioned, join_type=Inner, on=[(l_key@0, r_key@0)], NullsEqual: true
            EmptyExec
            EmptyExec
        ");
        Ok(())
    }

    #[test]
    fn does_not_rewrite_unsupported_join_type() -> Result<()> {
        let join = partitioned_join(JoinType::Right, "l_key")?;
        let join_schema = join.schema();
        let plan = aggregate(
            join,
            col("l_key", &join_schema)?,
            col("r_value", &join_schema)?,
        )?;

        assert_snapshot!(optimize(plan)?, @r"
        AggregateExec: mode=Single, gby=[l_key@0 as l_key], aggr=[count_values]
          HashJoinExec: mode=Partitioned, join_type=Right, on=[(l_key@0, r_key@0)], NullsEqual: true
            EmptyExec
            EmptyExec
        ");
        Ok(())
    }

    #[test]
    fn does_not_rewrite_inner_join_with_literal_aggregate_argument() -> Result<()> {
        let join = partitioned_join(JoinType::Inner, "l_key")?;
        let join_schema = join.schema();
        let plan = aggregate(join, col("l_key", &join_schema)?, lit(1i64))?;

        assert_snapshot!(optimize(plan)?, @r"
        AggregateExec: mode=Single, gby=[l_key@0 as l_key], aggr=[count_values]
          HashJoinExec: mode=Partitioned, join_type=Inner, on=[(l_key@0, r_key@0)], NullsEqual: true
            EmptyExec
            EmptyExec
        ");
        Ok(())
    }

    #[test]
    fn does_not_rewrite_collect_left_hash_join() -> Result<()> {
        let join = join(JoinType::Left, "l_key", PartitionMode::CollectLeft)?;
        let join_schema = join.schema();
        let plan = aggregate(
            join,
            col("l_key", &join_schema)?,
            col("r_value", &join_schema)?,
        )?;

        assert_snapshot!(optimize(plan)?, @r"
        AggregateExec: mode=Single, gby=[l_key@0 as l_key], aggr=[count_values]
          HashJoinExec: mode=CollectLeft, join_type=Left, on=[(l_key@0, r_key@0)], NullsEqual: true
            EmptyExec
            EmptyExec
        ");
        Ok(())
    }
}
