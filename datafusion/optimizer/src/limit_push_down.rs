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
use datafusion_expr::utils::from_plan;
use datafusion_expr::{
    logical_plan::{
        Join, JoinType, Limit, LogicalPlan, Projection, Sort, TableScan, Union,
    },
    CrossJoin,
};
use std::sync::Arc;

/// Optimization rule that tries to push down LIMIT.
#[derive(Default)]
pub struct LimitPushDown {}

impl LimitPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
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
impl OptimizerRule for LimitPushDown {
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
            let new_fetch = match child_limit.fetch {
                Some(child_fetch) => {
                    let ancestor_fetch = parent_fetch.map(|f| f + parent_skip);
                    let new_current_fetch = ancestor_fetch
                        .map_or(child_fetch, |x| std::cmp::min(x, child_fetch));
                    Some(new_current_fetch)
                }
                None => parent_fetch.map(|f| f + parent_skip),
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

        let plan = match &*limit.input {
            LogicalPlan::TableScan(scan) => {
                let limit = fetch + skip;
                let new_input = LogicalPlan::TableScan(TableScan {
                    table_name: scan.table_name.clone(),
                    source: scan.source.clone(),
                    projection: scan.projection.clone(),
                    filters: scan.filters.clone(),
                    fetch: scan.fetch.map(|x| std::cmp::min(x, limit)).or(Some(limit)),
                    projected_schema: scan.projected_schema.clone(),
                });
                from_plan(plan, &plan.expressions(), &[new_input])?
            }

            LogicalPlan::Projection(projection) => {
                let new_input = LogicalPlan::Limit(Limit {
                    skip,
                    fetch: Some(fetch),
                    input: Arc::new((*projection.input).clone()),
                });
                // Push down limit directly (projection doesn't change number of rows)
                LogicalPlan::Projection(Projection::try_new_with_schema_alias(
                    projection.expr.clone(),
                    Arc::new(new_input),
                    projection.schema.clone(),
                    projection.alias.clone(),
                )?)
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
                from_plan(plan, &plan.expressions(), &[union])?
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
                let new_input = LogicalPlan::CrossJoin(CrossJoin {
                    left: Arc::new(new_left),
                    right: Arc::new(new_right),
                    schema: plan.schema().clone(),
                });
                from_plan(plan, &plan.expressions(), &[new_input])?
            }

            LogicalPlan::Join(join) => {
                let limit = fetch + skip;
                let new_join = match join.join_type {
                    JoinType::Left => push_down_join(join, Some(limit), None),
                    JoinType::Right => push_down_join(join, None, Some(limit)),
                    _ => push_down_join(join, None, None),
                };
                from_plan(plan, &plan.expressions(), &[new_join])?
            }

            LogicalPlan::Sort(sort) => {
                let sort_fetch = skip + fetch;
                let new_input = LogicalPlan::Sort(Sort {
                    expr: sort.expr.clone(),
                    input: Arc::new((*sort.input).clone()),
                    fetch: Some(
                        sort.fetch.map(|f| f.min(sort_fetch)).unwrap_or(sort_fetch),
                    ),
                });
                from_plan(plan, &plan.expressions(), &[new_input])?
            }
            _ => plan.clone(),
        };

        utils::optimize_children(self, &plan, optimizer_config)
    }

    fn name(&self) -> &str {
        "limit_push_down"
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

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = LimitPushDown::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn multi_stage_limit_recurses_to_deeper_limit() -> Result<()> {
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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
        \n  Limit: skip=10, fetch=1000\
        \n    TableScan: test, fetch=1010";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&outer_query, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
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

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
