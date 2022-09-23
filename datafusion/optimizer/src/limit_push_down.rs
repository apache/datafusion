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
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{
    logical_plan::{
        Join, JoinType, Limit, LogicalPlan, Projection, Sort, TableScan, Union,
    },
    utils::from_plan,
};
use std::sync::Arc;

/// Optimization rule that tries pushes down LIMIT n
/// where applicable to reduce the amount of scanned / processed data.
#[derive(Default)]
pub struct LimitPushDown {}

impl LimitPushDown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Ancestor indicates the current ancestor in the LogicalPlan tree
/// when traversing down related to "limit push down".
enum Ancestor {
    /// Limit
    FromLimit { skip: usize, fetch: Option<usize> },
    /// Other nodes that don't affect the adjustment of "Limit"
    NotRelevant,
}

///
/// When doing limit push down with "skip" and "fetch" during traversal,
/// the "fetch" should be adjusted.
/// "Ancestor" is pushed down the plan tree, so that the current node
/// can adjust it's own "fetch".
///
/// If the current node is a Limit, its "fetch" is updated by:
/// 1. extended_fetch = extended the "fetch" with ancestor's "skip".
/// 2. min(extended_fetch, current node's fetch)
///
/// Current node's "skip" is never updated, it is
/// just a hint for the child to extend its "fetch".
///
/// When building a new Limit in Union, the "fetch" is calculated
/// by using ancestor's "fetch" and "skip".
///
/// When finally assign "limit" in TableScan, the "limit" is calculated
/// by using ancestor's "fetch" and "skip".
///
fn limit_push_down(
    _optimizer: &LimitPushDown,
    ancestor: Ancestor,
    plan: &LogicalPlan,
    _optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
    match (plan, ancestor) {
        (
            LogicalPlan::Limit(Limit {
                skip: current_skip,
                fetch: current_fetch,
                input,
            }),
            ancestor,
        ) => {
            let new_current_fetch = match ancestor {
                Ancestor::FromLimit {
                    skip: ancestor_skip,
                    fetch: ancestor_fetch,
                } => {
                    if let Some(fetch) = current_fetch {
                        // extend ancestor's fetch
                        let ancestor_fetch = ancestor_fetch.map(|f| f + ancestor_skip);

                        let new_current_fetch =
                            ancestor_fetch.map_or(*fetch, |x| std::cmp::min(x, *fetch));

                        Some(new_current_fetch)
                    } else {
                        // we dont have a "fetch", and we can push down our parent's "fetch"
                        // extend ancestor's fetch
                        ancestor_fetch.map(|f| f + ancestor_skip)
                    }
                }
                _ => *current_fetch,
            };

            Ok(LogicalPlan::Limit(Limit {
                // current node's "skip" is not updated, updating
                // this value would violate the semantics of Limit operator
                skip: *current_skip,
                fetch: new_current_fetch,
                input: Arc::new(limit_push_down(
                    _optimizer,
                    Ancestor::FromLimit {
                        // current node's "skip" is passing to the subtree
                        // so that the child can extend the "fetch"
                        skip: *current_skip,
                        fetch: new_current_fetch,
                    },
                    input.as_ref(),
                    _optimizer_config,
                )?),
            }))
        }
        (
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                filters,
                fetch,
                projected_schema,
            }),
            Ancestor::FromLimit {
                skip: ancestor_skip,
                fetch: Some(ancestor_fetch),
                ..
            },
        ) => {
            let ancestor_fetch = ancestor_fetch + ancestor_skip;
            Ok(LogicalPlan::TableScan(TableScan {
                table_name: table_name.clone(),
                source: source.clone(),
                projection: projection.clone(),
                filters: filters.clone(),
                fetch: fetch
                    .map(|x| std::cmp::min(x, ancestor_fetch))
                    .or(Some(ancestor_fetch)),
                projected_schema: projected_schema.clone(),
            }))
        }
        (
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
                alias,
            }),
            ancestor,
        ) => {
            // Push down limit directly (projection doesn't change number of rows)
            Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                expr.clone(),
                Arc::new(limit_push_down(
                    _optimizer,
                    ancestor,
                    input.as_ref(),
                    _optimizer_config,
                )?),
                schema.clone(),
                alias.clone(),
            )?))
        }
        (
            LogicalPlan::Union(Union {
                inputs,
                alias,
                schema,
            }),
            Ancestor::FromLimit {
                skip: ancestor_skip,
                fetch: Some(ancestor_fetch),
                ..
            },
        ) => {
            // Push down limit through UNION
            let ancestor_fetch = ancestor_fetch + ancestor_skip;
            let new_inputs = inputs
                .iter()
                .map(|x| {
                    Ok(Arc::new(LogicalPlan::Limit(Limit {
                        skip: 0,
                        fetch: Some(ancestor_fetch),
                        input: Arc::new(limit_push_down(
                            _optimizer,
                            Ancestor::FromLimit {
                                skip: 0,
                                fetch: Some(ancestor_fetch),
                            },
                            x,
                            _optimizer_config,
                        )?),
                    })))
                })
                .collect::<Result<_>>()?;
            Ok(LogicalPlan::Union(Union {
                inputs: new_inputs,
                alias: alias.clone(),
                schema: schema.clone(),
            }))
        }
        (
            LogicalPlan::Join(Join { join_type, .. }),
            Ancestor::FromLimit {
                skip: ancestor_skip,
                fetch: Some(ancestor_fetch),
                ..
            },
        ) => {
            let ancestor_fetch = ancestor_fetch + ancestor_skip;
            match join_type {
                JoinType::Left => {
                    //if LeftOuter join push limit to left
                    generate_push_down_join(
                        _optimizer,
                        _optimizer_config,
                        plan,
                        Some(ancestor_fetch),
                        None,
                    )
                }
                JoinType::Right =>
                // If RightOuter join  push limit to right
                {
                    generate_push_down_join(
                        _optimizer,
                        _optimizer_config,
                        plan,
                        None,
                        Some(ancestor_fetch),
                    )
                }
                _ => generate_push_down_join(
                    _optimizer,
                    _optimizer_config,
                    plan,
                    None,
                    None,
                ),
            }
        }
        (
            LogicalPlan::Sort(Sort { expr, input, fetch }),
            Ancestor::FromLimit {
                skip: ancestor_skip,
                fetch: Some(ancestor_fetch),
                ..
            },
        ) => {
            // Update Sort `fetch`, but simply recurse through children (sort should receive all input for sorting)
            let input = push_down_children_limit(_optimizer, _optimizer_config, input)?;
            let sort_fetch = ancestor_skip + ancestor_fetch;
            let plan = LogicalPlan::Sort(Sort {
                expr: expr.clone(),
                input: Arc::new(input),
                fetch: Some(fetch.map(|f| f.min(sort_fetch)).unwrap_or(sort_fetch)),
            });
            Ok(plan)
        }

        // For other nodes we can't push down the limit
        // But try to recurse and find other limit nodes to push down
        _ => push_down_children_limit(_optimizer, _optimizer_config, plan),
    }
}

fn generate_push_down_join(
    _optimizer: &LimitPushDown,
    _optimizer_config: &OptimizerConfig,
    join: &LogicalPlan,
    left_limit: Option<usize>,
    right_limit: Option<usize>,
) -> Result<LogicalPlan> {
    if let LogicalPlan::Join(Join {
        left,
        right,
        on,
        filter,
        join_type,
        join_constraint,
        schema,
        null_equals_null,
    }) = join
    {
        Ok(LogicalPlan::Join(Join {
            left: Arc::new(limit_push_down(
                _optimizer,
                Ancestor::FromLimit {
                    skip: 0,
                    fetch: left_limit,
                },
                left.as_ref(),
                _optimizer_config,
            )?),
            right: Arc::new(limit_push_down(
                _optimizer,
                Ancestor::FromLimit {
                    skip: 0,
                    fetch: right_limit,
                },
                right.as_ref(),
                _optimizer_config,
            )?),
            on: on.clone(),
            filter: filter.clone(),
            join_type: *join_type,
            join_constraint: *join_constraint,
            schema: schema.clone(),
            null_equals_null: *null_equals_null,
        }))
    } else {
        Err(DataFusionError::Internal(format!(
            "{:?} must be join type",
            join
        )))
    }
}

fn push_down_children_limit(
    _optimizer: &LimitPushDown,
    _optimizer_config: &OptimizerConfig,
    plan: &LogicalPlan,
) -> Result<LogicalPlan> {
    let expr = plan.expressions();

    // apply the optimization to all inputs of the plan
    let inputs = plan.inputs();
    let new_inputs = inputs
        .iter()
        .map(|plan| {
            limit_push_down(_optimizer, Ancestor::NotRelevant, plan, _optimizer_config)
        })
        .collect::<Result<Vec<_>>>()?;

    from_plan(plan, &expr, &new_inputs)
}

impl OptimizerRule for LimitPushDown {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        limit_push_down(self, Ancestor::NotRelevant, plan, optimizer_config)
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
        let expected = "Limit: skip=0, fetch=1000\
        \n  Projection: #test.a\
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
        \n  Limit: skip=0, fetch=10\
        \n    TableScan: test, fetch=10";

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
        \n  Aggregate: groupBy=[[#test.a]], aggr=[[MAX(#test.b)]]\
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
        \n  Sort: #test.a, fetch=10\
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
        \n  Sort: #test.a, fetch=15\
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
        \n  Aggregate: groupBy=[[#test.a]], aggr=[[MAX(#test.b)]]\
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
        let expected = "Limit: skip=10, fetch=1000\
        \n  Projection: #test.a\
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

        let expected = "Limit: skip=10, fetch=None\
        \n  Limit: skip=0, fetch=1000\
        \n    Projection: #test.a\
        \n      TableScan: test, fetch=1000";

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

        let expected = "Limit: skip=0, fetch=1000\
        \n  Limit: skip=10, fetch=1000\
        \n    Projection: #test.a\
        \n      TableScan: test, fetch=1010";

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

        // Should push down the smallest limit
        // Towards table scan
        // This rule doesn't replace multiple limits
        let expected = "Limit: skip=0, fetch=10\
        \n  Limit: skip=0, fetch=10\
        \n    Limit: skip=10, fetch=10\
        \n      TableScan: test, fetch=20";

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
        \n  Aggregate: groupBy=[[#test.a]], aggr=[[MAX(#test.b)]]\
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
        \n  Inner Join: #test.a = #test2.a\
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
        \n  Inner Join: #test.a = #test2.a\
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
        \n      Filter: #test1.a = #test1.a\
        \n        Projection: #test1.a\
        \n          TableScan: test1\
        \n    Projection: #test2.a\
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
        \n      Filter: #test1.a = #test1.a\
        \n        Projection: #test1.a\
        \n          TableScan: test1\
        \n    Projection: #test2.a\
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
        \n  Left Join: #test.a = #test2.a\
        \n    TableScan: test, fetch=1000\
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
        \n  Left Join: #test.a = #test2.a\
        \n    TableScan: test, fetch=1010\
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
        \n  Right Join: #test.a = #test2.a\
        \n    TableScan: test\
        \n    TableScan: test2, fetch=1000";

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
        \n  Right Join: #test.a = #test2.a\
        \n    TableScan: test\
        \n    TableScan: test2, fetch=1010";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
