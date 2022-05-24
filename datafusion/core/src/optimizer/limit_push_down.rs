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

//! Optimizer rule to push down LIMIT in the query plan
//! It will push down through projection, limits (taking the smaller limit)
use crate::error::Result;
use crate::execution::context::ExecutionProps;
use crate::logical_plan::plan::Projection;
use crate::logical_plan::{Limit, TableScan};
use crate::logical_plan::{LogicalPlan, Union};
use crate::optimizer::optimizer::OptimizerRule;
use datafusion_common::DataFusionError;
use datafusion_expr::logical_plan::{Join, JoinType, Offset};
use datafusion_expr::utils::from_plan;
use std::sync::Arc;

/// Optimization rule that tries pushes down LIMIT n
/// where applicable to reduce the amount of scanned / processed data
#[derive(Default)]
pub struct LimitPushDown {}

impl LimitPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn limit_push_down(
    _optimizer: &LimitPushDown,
    upper_limit: Option<usize>,
    plan: &LogicalPlan,
    _execution_props: &ExecutionProps,
    is_offset: bool,
) -> Result<LogicalPlan> {
    match (plan, upper_limit) {
        (LogicalPlan::Limit(Limit { n, input }), upper_limit) => {
            let new_limit: usize = if is_offset {
                *n + upper_limit.unwrap_or(0)
            } else {
                upper_limit.map(|x| std::cmp::min(x, *n)).unwrap_or(*n)
            };
            Ok(LogicalPlan::Limit(Limit {
                n: new_limit,
                // push down limit to plan (minimum of upper limit and current limit)
                input: Arc::new(limit_push_down(
                    _optimizer,
                    Some(new_limit),
                    input.as_ref(),
                    _execution_props,
                    false,
                )?),
            }))
        }
        (
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                filters,
                limit,
                projected_schema,
            }),
            Some(upper_limit),
        ) => Ok(LogicalPlan::TableScan(TableScan {
            table_name: table_name.clone(),
            source: source.clone(),
            projection: projection.clone(),
            filters: filters.clone(),
            limit: limit
                .map(|x| std::cmp::min(x, upper_limit))
                .or(Some(upper_limit)),
            projected_schema: projected_schema.clone(),
        })),
        (
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
                alias,
            }),
            upper_limit,
        ) => {
            // Push down limit directly (projection doesn't change number of rows)
            Ok(LogicalPlan::Projection(Projection {
                expr: expr.clone(),
                input: Arc::new(limit_push_down(
                    _optimizer,
                    upper_limit,
                    input.as_ref(),
                    _execution_props,
                    false,
                )?),
                schema: schema.clone(),
                alias: alias.clone(),
            }))
        }
        (
            LogicalPlan::Union(Union {
                inputs,
                alias,
                schema,
            }),
            Some(upper_limit),
        ) => {
            // Push down limit through UNION
            let new_inputs = inputs
                .iter()
                .map(|x| {
                    Ok(LogicalPlan::Limit(Limit {
                        n: upper_limit,
                        input: Arc::new(limit_push_down(
                            _optimizer,
                            Some(upper_limit),
                            x,
                            _execution_props,
                            false,
                        )?),
                    }))
                })
                .collect::<Result<_>>()?;
            Ok(LogicalPlan::Union(Union {
                inputs: new_inputs,
                alias: alias.clone(),
                schema: schema.clone(),
            }))
        }
        // offset 5 limit 10 then push limit 15 (5 + 10)
        // Limit should always be Offset's input
        (LogicalPlan::Offset(Offset { offset, input }), upper_limit) => {
            let new_limit = if let Some(ul) = upper_limit {
                ul + *offset
            } else {
                *offset
            };
            Ok(LogicalPlan::Offset(Offset {
                offset: *offset,
                input: Arc::new(limit_push_down(
                    _optimizer,
                    Some(new_limit),
                    input.as_ref(),
                    _execution_props,
                    true,
                )?),
            }))
        }
        (LogicalPlan::Join(Join { join_type, .. }), upper_limit) => match join_type {
            JoinType::Left => {
                //if LeftOuter join push limit to left
                generate_push_down_join(
                    _optimizer,
                    _execution_props,
                    plan,
                    upper_limit,
                    None,
                )
            }
            JoinType::Right =>
            // If RightOuter join  push limit to right
            {
                generate_push_down_join(
                    _optimizer,
                    _execution_props,
                    plan,
                    None,
                    upper_limit,
                )
            }
            _ => generate_push_down_join(_optimizer, _execution_props, plan, None, None),
        },
        // For other nodes we can't push down the limit
        // But try to recurse and find other limit nodes to push down
        _ => push_down_children_limit(_optimizer, _execution_props, plan),
    }
}

fn generate_push_down_join(
    _optimizer: &LimitPushDown,
    _execution_props: &ExecutionProps,
    join: &LogicalPlan,
    left_limit: Option<usize>,
    right_limit: Option<usize>,
) -> Result<LogicalPlan> {
    if let LogicalPlan::Join(Join {
        left,
        right,
        on,
        join_type,
        join_constraint,
        schema,
        null_equals_null,
    }) = join
    {
        return Ok(LogicalPlan::Join(Join {
            left: Arc::new(limit_push_down(
                _optimizer,
                left_limit,
                left.as_ref(),
                _execution_props,
                true,
            )?),
            right: Arc::new(limit_push_down(
                _optimizer,
                right_limit,
                right.as_ref(),
                _execution_props,
                true,
            )?),
            on: on.clone(),
            join_type: *join_type,
            join_constraint: *join_constraint,
            schema: schema.clone(),
            null_equals_null: *null_equals_null,
        }));
    } else {
        Err(DataFusionError::Internal(format!(
            "{:?} must be join type",
            join
        )))
    }
}

fn push_down_children_limit(
    _optimizer: &LimitPushDown,
    _execution_props: &ExecutionProps,
    plan: &LogicalPlan,
) -> Result<LogicalPlan> {
    let expr = plan.expressions();

    // apply the optimization to all inputs of the plan
    let inputs = plan.inputs();
    let new_inputs = inputs
        .iter()
        .map(|plan| limit_push_down(_optimizer, None, plan, _execution_props, false))
        .collect::<Result<Vec<_>>>()?;

    from_plan(plan, &expr, &new_inputs)
}

impl OptimizerRule for LimitPushDown {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        limit_push_down(self, None, plan, execution_props, false)
    }

    fn name(&self) -> &str {
        "limit_push_down"
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        logical_plan::{col, max, LogicalPlan, LogicalPlanBuilder},
        test::*,
    };
    use datafusion_expr::exists;
    use datafusion_expr::logical_plan::JoinType;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = LimitPushDown::new();
        let optimized_plan = rule
            .optimize(plan, &ExecutionProps::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn limit_pushdown_projection_table_provider() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(1000)?
            .build()?;

        // Should push the limit down to table provider
        // When it has a select
        let expected = "Limit: 1000\
        \n  Projection: #test.a\
        \n    TableScan: test projection=None, limit=1000";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
    #[test]
    fn limit_push_down_take_smaller_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(1000)?
            .limit(10)?
            .build()?;

        // Should push down the smallest limit
        // Towards table scan
        // This rule doesn't replace multiple limits
        let expected = "Limit: 10\
        \n  Limit: 10\
        \n    TableScan: test projection=None, limit=10";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_doesnt_push_down_aggregation() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .limit(1000)?
            .build()?;

        // Limit should *not* push down aggregate node
        let expected = "Limit: 1000\
        \n  Aggregate: groupBy=[[#test.a]], aggr=[[MAX(#test.b)]]\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_should_push_down_union() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .union(LogicalPlanBuilder::from(table_scan).build()?)?
            .limit(1000)?
            .build()?;

        // Limit should push down through union
        let expected = "Limit: 1000\
        \n  Union\
        \n    Limit: 1000\
        \n      TableScan: test projection=None, limit=1000\
        \n    Limit: 1000\
        \n      TableScan: test projection=None, limit=1000";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn multi_stage_limit_recurses_to_deeper_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(1000)?
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .limit(10)?
            .build()?;

        // Limit should use deeper LIMIT 1000, but Limit 10 shouldn't push down aggregation
        let expected = "Limit: 10\
        \n  Aggregate: groupBy=[[#test.a]], aggr=[[MAX(#test.b)]]\
        \n    Limit: 1000\
        \n      TableScan: test projection=None, limit=1000";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_pushdown_with_offset_projection_table_provider() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .offset(10)?
            .limit(1000)?
            .build()?;

        // Should push the limit down to table provider
        // When it has a select
        let expected = "Limit: 1000\
        \n  Offset: 10\
        \n    Projection: #test.a\
        \n      TableScan: test projection=None, limit=1010";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_pushdown_with_offset_after_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(1000)?
            .offset(10)?
            .build()?;

        let expected = "Offset: 10\
        \n  Limit: 1010\
        \n    Projection: #test.a\
        \n      TableScan: test projection=None, limit=1010";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_push_down_with_offset_take_smaller_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .offset(10)?
            .limit(1000)?
            .limit(10)?
            .build()?;

        // Should push down the smallest limit
        // Towards table scan
        // This rule doesn't replace multiple limits
        let expected = "Limit: 10\
        \n  Limit: 10\
        \n    Offset: 10\
        \n      TableScan: test projection=None, limit=20";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_doesnt_push_down_with_offset_aggregation() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .offset(10)?
            .limit(1000)?
            .build()?;

        // Limit should *not* push down aggregate node
        let expected = "Limit: 1000\
        \n  Offset: 10\
        \n    Aggregate: groupBy=[[#test.a]], aggr=[[MAX(#test.b)]]\
        \n      TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_should_push_down_with_offset_union() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .union(LogicalPlanBuilder::from(table_scan).build()?)?
            .offset(10)?
            .limit(1000)?
            .build()?;

        // Limit should push down through union
        let expected = "Limit: 1000\
        \n  Offset: 10\
        \n    Union\
        \n      Limit: 1010\
        \n        TableScan: test projection=None, limit=1010\
        \n      Limit: 1010\
        \n        TableScan: test projection=None, limit=1010";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_should_not_push_down_with_offset_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                &LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Inner,
                (vec!["a"], vec!["a"]),
            )?
            .limit(1000)?
            .offset(10)?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Offset: 10\
        \n  Limit: 1010\
        \n    Inner Join: #test.a = #test2.a\
        \n      TableScan: test projection=None\
        \n      TableScan: test2 projection=None";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_should_not_push_down_with_offset_sub_query() -> Result<()> {
        let table_scan_1 = test_table_scan_with_name("test1")?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let subquery = LogicalPlanBuilder::from(table_scan_1)
            .project(vec![col("a")])?
            .filter(col("a").eq(col("test1.a")))?
            .build()?;

        let outer_query = LogicalPlanBuilder::from(table_scan_2)
            .project(vec![col("a")])?
            .filter(exists(Arc::new(subquery)))?
            .limit(100)?
            .offset(10)?
            .build()?;

        // Limit pushdown Not supported in sub_query
        let expected = "Offset: 10\
        \n  Limit: 110\
        \n    Filter: EXISTS (Subquery: Filter: #test1.a = #test1.a\
        \n  Projection: #test1.a\
        \n    TableScan: test1 projection=None)\
        \n      Projection: #test2.a\
        \n        TableScan: test2 projection=None";

        assert_optimized_plan_eq(&outer_query, expected);

        Ok(())
    }

    #[test]
    fn limit_should_push_down_left_outer_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                &LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
            )?
            .limit(1000)?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: 1000\
        \n  Left Join: #test.a = #test2.a\
        \n    TableScan: test projection=None, limit=1000\
        \n    TableScan: test2 projection=None";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn limit_should_push_down_right_outer_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                &LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Right,
                (vec!["a"], vec!["a"]),
            )?
            .limit(1000)?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: 1000\
        \n  Right Join: #test.a = #test2.a\
        \n    TableScan: test projection=None\
        \n    TableScan: test2 projection=None, limit=1000";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
