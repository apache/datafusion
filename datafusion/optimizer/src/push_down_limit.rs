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
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::{
    logical_plan::{Join, JoinType, Limit, LogicalPlan, Sort, TableScan, Union},
    CrossJoin,
};
use std::sync::Arc;

/// Optimization rule that tries to push down LIMIT.
#[derive(Default)]
pub struct PushDownLimit {}

impl PushDownLimit {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn is_no_join_condition(join: &Join) -> bool {
    join.on.is_empty() && join.filter.is_none()
}

fn push_down_join(
    join: &Join,
    left_limit: Option<usize>,
    right_limit: Option<usize>,
) -> LogicalPlan {
    let left = match left_limit {
        Some(limit) => LogicalPlan::Limit(Limit {
            skip: 0,
            fetch: Some(limit),
            input: Arc::new((*join.left).clone()),
        }),
        None => (*join.left).clone(),
    };
    let right = match right_limit {
        Some(limit) => LogicalPlan::Limit(Limit {
            skip: 0,
            fetch: Some(limit),
            input: Arc::new((*join.right).clone()),
        }),
        None => (*join.right).clone(),
    };
    LogicalPlan::Join(Join {
        left: Arc::new(left),
        right: Arc::new(right),
        on: join.on.clone(),
        filter: join.filter.clone(),
        join_type: join.join_type,
        join_constraint: join.join_constraint,
        schema: join.schema.clone(),
        null_equals_null: join.null_equals_null,
    })
}

/// Push down Limit.
impl OptimizerRule for PushDownLimit {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let limit = match plan {
            LogicalPlan::Limit(limit) => limit,
            _ => return utils::optimize_children(self, plan, optimizer_config),
        };

        if let LogicalPlan::Limit(child_limit) = &*limit.input {
            let parent_skip = limit.skip;
            let parent_fetch = limit.fetch;

            // Merge limit
            // Parent range [child_skip + skip, child_skip + skip + fetch)
            // Child range [child_skip, child_skip + child_fetch)
            // Merge -> [child_skip + skip, min(child_skip + skip + fetch, child_skip + child_fetch) )
            // Merge LimitPlan -> [child_skip + skip, min(fetch, child_fetch - skip) )
            let new_fetch = match parent_fetch {
                Some(fetch) => match child_limit.fetch {
                    Some(child_fetch) => Some(std::cmp::min(
                        fetch,
                        fetch_minus_skip(child_fetch, parent_skip),
                    )),
                    None => Some(fetch),
                },
                _ => child_limit
                    .fetch
                    .map(|child_fetch| fetch_minus_skip(child_fetch, parent_skip)),
            };

            let plan = LogicalPlan::Limit(Limit {
                skip: child_limit.skip + limit.skip,
                fetch: new_fetch,
                input: Arc::new((*child_limit.input).clone()),
            });
            return self.optimize(&plan, optimizer_config);
        }

        let fetch = match limit.fetch {
            Some(fetch) => fetch,
            None => return utils::optimize_children(self, plan, optimizer_config),
        };
        let skip = limit.skip;

        let child_plan = &*limit.input;
        let plan = match child_plan {
            LogicalPlan::TableScan(scan) => {
                let limit = if fetch != 0 { fetch + skip } else { 0 };
                let new_input = LogicalPlan::TableScan(TableScan {
                    table_name: scan.table_name.clone(),
                    source: scan.source.clone(),
                    projection: scan.projection.clone(),
                    filters: scan.filters.clone(),
                    fetch: scan.fetch.map(|x| std::cmp::min(x, limit)).or(Some(limit)),
                    projected_schema: scan.projected_schema.clone(),
                });
                plan.with_new_inputs(&[new_input])?
            }
            LogicalPlan::Union(union) => {
                let new_inputs = union
                    .inputs
                    .iter()
                    .map(|x| {
                        Ok(Arc::new(LogicalPlan::Limit(Limit {
                            skip: 0,
                            fetch: Some(fetch + skip),
                            input: Arc::new((**x).clone()),
                        })))
                    })
                    .collect::<Result<_>>()?;
                let union = LogicalPlan::Union(Union {
                    inputs: new_inputs,
                    schema: union.schema.clone(),
                });
                plan.with_new_inputs(&[union])?
            }

            LogicalPlan::CrossJoin(cross_join) => {
                let left = &*cross_join.left;
                let right = &*cross_join.right;
                let new_left = LogicalPlan::Limit(Limit {
                    skip: 0,
                    fetch: Some(fetch + skip),
                    input: Arc::new(left.clone()),
                });
                let new_right = LogicalPlan::Limit(Limit {
                    skip: 0,
                    fetch: Some(fetch + skip),
                    input: Arc::new(right.clone()),
                });
                let new_cross_join = LogicalPlan::CrossJoin(CrossJoin {
                    left: Arc::new(new_left),
                    right: Arc::new(new_right),
                    schema: plan.schema().clone(),
                });
                plan.with_new_inputs(&[new_cross_join])?
            }

            LogicalPlan::Join(join) => {
                let limit = fetch + skip;
                let new_join = match join.join_type {
                    JoinType::Left | JoinType::Right | JoinType::Full
                        if is_no_join_condition(join) =>
                    {
                        // push left and right
                        push_down_join(join, Some(limit), Some(limit))
                    }
                    JoinType::LeftSemi | JoinType::LeftAnti
                        if is_no_join_condition(join) =>
                    {
                        // push left
                        push_down_join(join, Some(limit), None)
                    }
                    JoinType::RightSemi | JoinType::RightAnti
                        if is_no_join_condition(join) =>
                    {
                        // push right
                        push_down_join(join, None, Some(limit))
                    }
                    JoinType::Left => push_down_join(join, Some(limit), None),
                    JoinType::Right => push_down_join(join, None, Some(limit)),
                    _ => push_down_join(join, None, None),
                };
                plan.with_new_inputs(&[new_join])?
            }

            LogicalPlan::Sort(sort) => {
                let sort_fetch = skip + fetch;
                let new_sort = LogicalPlan::Sort(Sort {
                    expr: sort.expr.clone(),
                    input: Arc::new((*sort.input).clone()),
                    fetch: Some(
                        sort.fetch.map(|f| f.min(sort_fetch)).unwrap_or(sort_fetch),
                    ),
                });
                plan.with_new_inputs(&[new_sort])?
            }
            LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => {
                // commute
                let new_limit =
                    plan.with_new_inputs(&[
                        (*(child_plan.inputs().get(0).unwrap())).clone()
                    ])?;
                child_plan.with_new_inputs(&[new_limit])?
            }
            _ => plan.clone(),
        };

        utils::optimize_children(self, &plan, optimizer_config)
    }

    fn name(&self) -> &str {
        "push_down_limit"
    }
}

fn fetch_minus_skip(fetch: usize, skip: usize) -> usize {
    if skip > fetch {
        0
    } else {
        fetch - skip
    }
}

#[cfg(test)]
mod test {
    use std::vec;

    use super::*;
    use crate::test::*;
    use datafusion_expr::{
        col, exists,
        logical_plan::{builder::LogicalPlanBuilder, JoinType, LogicalPlan},
        max,
    };

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) -> Result<()> {
        let optimized_plan = PushDownLimit::new()
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");

        let formatted_plan = format!("{:?}", optimized_plan);

        assert_eq!(formatted_plan, expected);
        assert_eq!(optimized_plan.schema(), plan.schema());

        Ok(())
    }

    #[test]
    fn limit_pushdown_projection_table_provider() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(0, Some(1000))?
            .build()?;

        // Should push the limit down to table provider
        // When it has a select
        let expected = "Projection: test.a\
        \n  Limit: skip=0, fetch=1000\
        \n    TableScan: test, fetch=1000";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_push_down_take_smaller_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(0, Some(1000))?
            .limit(0, Some(10))?
            .build()?;

        // Should push down the smallest limit
        // Towards table scan
        // This rule doesn't replace multiple limits
        let expected = "Limit: skip=0, fetch=10\
        \n  TableScan: test, fetch=10";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_doesnt_push_down_aggregation() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .limit(0, Some(1000))?
            .build()?;

        // Limit should *not* push down aggregate node
        let expected = "Limit: skip=0, fetch=1000\
        \n  Aggregate: groupBy=[[test.a]], aggr=[[MAX(test.b)]]\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_should_push_down_union() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .union(LogicalPlanBuilder::from(table_scan).build()?)?
            .limit(0, Some(1000))?
            .build()?;

        // Limit should push down through union
        let expected = "Limit: skip=0, fetch=1000\
        \n  Union\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_push_down_sort() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![col("a")])?
            .limit(0, Some(10))?
            .build()?;

        // Should push down limit to sort
        let expected = "Limit: skip=0, fetch=10\
        \n  Sort: test.a, fetch=10\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_push_down_sort_skip() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![col("a")])?
            .limit(5, Some(10))?
            .build()?;

        // Should push down limit to sort
        let expected = "Limit: skip=5, fetch=10\
        \n  Sort: test.a, fetch=15\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn multi_stage_limit_recursive_to_deeper_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(0, Some(1000))?
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .limit(0, Some(10))?
            .build()?;

        // Limit should use deeper LIMIT 1000, but Limit 10 shouldn't push down aggregation
        let expected = "Limit: skip=0, fetch=10\
        \n  Aggregate: groupBy=[[test.a]], aggr=[[MAX(test.b)]]\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_pushdown_should_not_pushdown_limit_with_offset_only() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(10, None)?
            .build()?;

        // Should not push any limit down to table provider
        // When it has a select
        let expected = "Limit: skip=10, fetch=None\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_pushdown_with_offset_projection_table_provider() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(10, Some(1000))?
            .build()?;

        // Should push the limit down to table provider
        // When it has a select
        let expected = "Projection: test.a\
        \n  Limit: skip=10, fetch=1000\
        \n    TableScan: test, fetch=1010";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_pushdown_with_offset_after_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(0, Some(1000))?
            .limit(10, None)?
            .build()?;

        let expected = "Projection: test.a\
        \n  Limit: skip=10, fetch=990\
        \n    TableScan: test, fetch=1000";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_pushdown_with_limit_after_offset() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(10, None)?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Projection: test.a\
        \n  Limit: skip=10, fetch=1000\
        \n    TableScan: test, fetch=1010";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_push_down_with_offset_take_smaller_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(10, None)?
            .limit(0, Some(1000))?
            .limit(0, Some(10))?
            .build()?;

        let expected = "Limit: skip=10, fetch=10\
        \n  TableScan: test, fetch=20";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_doesnt_push_down_with_offset_aggregation() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .limit(10, Some(1000))?
            .build()?;

        // Limit should *not* push down aggregate node
        let expected = "Limit: skip=10, fetch=1000\
        \n  Aggregate: groupBy=[[test.a]], aggr=[[MAX(test.b)]]\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_should_push_down_with_offset_union() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .union(LogicalPlanBuilder::from(table_scan).build()?)?
            .limit(10, Some(1000))?
            .build()?;

        // Limit should push down through union
        let expected = "Limit: skip=10, fetch=1000\
        \n  Union\
        \n    Limit: skip=0, fetch=1010\
        \n      TableScan: test, fetch=1010\
        \n    Limit: skip=0, fetch=1010\
        \n      TableScan: test, fetch=1010";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_offset_should_not_push_down_with_offset_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                &LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Inner,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(10, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=10, fetch=1000\
        \n  Inner Join: test.a = test2.a\
        \n    TableScan: test\
        \n    TableScan: test2";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn offset_limit_should_not_push_down_with_offset_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                &LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Inner,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(10, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=10, fetch=1000\
        \n  Inner Join: test.a = test2.a\
        \n    TableScan: test\
        \n    TableScan: test2";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_offset_should_not_push_down_with_offset_sub_query() -> Result<()> {
        let table_scan_1 = test_table_scan_with_name("test1")?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let subquery = LogicalPlanBuilder::from(table_scan_1)
            .project(vec![col("a")])?
            .filter(col("a").eq(col("test1.a")))?
            .build()?;

        let outer_query = LogicalPlanBuilder::from(table_scan_2)
            .project(vec![col("a")])?
            .filter(exists(Arc::new(subquery)))?
            .limit(10, Some(100))?
            .build()?;

        // Limit pushdown Not supported in sub_query
        let expected = "Limit: skip=10, fetch=100\
        \n  Filter: EXISTS (<subquery>)\
        \n    Subquery:\
        \n      Filter: test1.a = test1.a\
        \n        Projection: test1.a\
        \n          TableScan: test1\
        \n    Projection: test2.a\
        \n      TableScan: test2";

        assert_optimized_plan_eq(&outer_query, expected)
    }

    #[test]
    fn offset_limit_should_not_push_down_with_offset_sub_query() -> Result<()> {
        let table_scan_1 = test_table_scan_with_name("test1")?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let subquery = LogicalPlanBuilder::from(table_scan_1)
            .project(vec![col("a")])?
            .filter(col("a").eq(col("test1.a")))?
            .build()?;

        let outer_query = LogicalPlanBuilder::from(table_scan_2)
            .project(vec![col("a")])?
            .filter(exists(Arc::new(subquery)))?
            .limit(10, Some(100))?
            .build()?;

        // Limit pushdown Not supported in sub_query
        let expected = "Limit: skip=10, fetch=100\
        \n  Filter: EXISTS (<subquery>)\
        \n    Subquery:\
        \n      Filter: test1.a = test1.a\
        \n        Projection: test1.a\
        \n          TableScan: test1\
        \n    Projection: test2.a\
        \n      TableScan: test2";

        assert_optimized_plan_eq(&outer_query, expected)
    }

    #[test]
    fn limit_should_push_down_join_without_condition() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;
        let left_keys: Vec<&str> = Vec::new();
        let right_keys: Vec<&str> = Vec::new();
        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                &LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::Left,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  Left Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_eq(&plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                &LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::Right,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  Right Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_eq(&plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                &LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::Full,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  Full Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_eq(&plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                &LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::LeftSemi,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  LeftSemi Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    TableScan: test2";

        assert_optimized_plan_eq(&plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                &LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::LeftAnti,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  LeftAnti Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    TableScan: test2";

        assert_optimized_plan_eq(&plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                &LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::RightSemi,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  RightSemi Join: \
        \n    TableScan: test\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_eq(&plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                &LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::RightAnti,
                (left_keys, right_keys),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  RightAnti Join: \
        \n    TableScan: test\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_eq(&plan, expected)
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
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=0, fetch=1000\
        \n  Left Join: test.a = test2.a\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    TableScan: test2";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_should_push_down_left_outer_join_with_offset() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                &LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(10, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=10, fetch=1000\
        \n  Left Join: test.a = test2.a\
        \n    Limit: skip=0, fetch=1010\
        \n      TableScan: test, fetch=1010\
        \n    TableScan: test2";

        assert_optimized_plan_eq(&plan, expected)
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
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=0, fetch=1000\
        \n  Right Join: test.a = test2.a\
        \n    TableScan: test\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_should_push_down_right_outer_join_with_offset() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                &LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Right,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(10, Some(1000))?
            .build()?;

        // Limit pushdown with offset supported in right outer join
        let expected = "Limit: skip=10, fetch=1000\
        \n  Right Join: test.a = test2.a\
        \n    TableScan: test\
        \n    Limit: skip=0, fetch=1010\
        \n      TableScan: test2, fetch=1010";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn limit_push_down_cross_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .cross_join(&LogicalPlanBuilder::from(table_scan_2).build()?)?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  CrossJoin:\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn skip_limit_push_down_cross_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .cross_join(&LogicalPlanBuilder::from(table_scan_2).build()?)?
            .limit(1000, Some(1000))?
            .build()?;

        let expected = "Limit: skip=1000, fetch=1000\
        \n  CrossJoin:\
        \n    Limit: skip=0, fetch=2000\
        \n      TableScan: test, fetch=2000\
        \n    Limit: skip=0, fetch=2000\
        \n      TableScan: test2, fetch=2000";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn merge_limit_result_empty() -> Result<()> {
        let scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(scan)
            .limit(0, Some(1000))?
            .limit(1000, None)?
            .build()?;

        let expected = "Limit: skip=1000, fetch=0\
        \n  TableScan: test, fetch=0";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn skip_great_than_fetch() -> Result<()> {
        let scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(scan)
            .limit(0, Some(1))?
            .limit(1000, None)?
            .build()?;

        let expected = "Limit: skip=1000, fetch=0\
        \n  TableScan: test, fetch=0";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn push_down_subquery_alias() -> Result<()> {
        let scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(scan)
            .alias("a")?
            .limit(0, Some(1))?
            .limit(1000, None)?
            .build()?;

        let expected = "SubqueryAlias: a\
        \n  Limit: skip=1000, fetch=0\
        \n    TableScan: test, fetch=0";

        assert_optimized_plan_eq(&plan, expected)
    }
}
