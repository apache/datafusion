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

//! Optimizer rule to replace `LIMIT 0` or
//! `LIMIT whose ancestor LIMIT's skip is greater than or equal to current's fetch`
//! on a plan with an empty relation.
//! This rule also removes OFFSET 0 from the [LogicalPlan]
//! This saves time in planning and executing the query.
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::{
    logical_plan::{EmptyRelation, Limit, LogicalPlan},
    utils::from_plan,
};

/// Optimization rule that replaces LIMIT 0 or
/// LIMIT whose ancestor LIMIT's skip is greater than or equal to current's fetch
/// with an [LogicalPlan::EmptyRelation].
/// This rule also removes OFFSET 0 from the [LogicalPlan]
#[derive(Default)]
pub struct EliminateLimit;

impl EliminateLimit {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Ancestor indicates the current ancestor in the LogicalPlan tree
/// when traversing down related to "eliminate limit".
enum Ancestor {
    /// Limit
    FromLimit { skip: usize },
    /// Other nodes that don't affect the adjustment of "Limit"
    NotRelevant,
}

/// replaces LIMIT 0 with an [LogicalPlan::EmptyRelation]
/// replaces LIMIT node whose ancestor LIMIT's skip is greater than or equal to current's fetch
/// with an [LogicalPlan::EmptyRelation]
/// removes OFFSET 0 from the [LogicalPlan]
fn eliminate_limit(
    _optimizer: &EliminateLimit,
    ancestor: &Ancestor,
    plan: &LogicalPlan,
    _optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Limit(Limit {
            skip, fetch, input, ..
        }) => {
            let ancestor_skip = match ancestor {
                Ancestor::FromLimit { skip } => *skip,
                _ => 0,
            };
            // If ancestor's skip is equal or greater than current's fetch,
            // replaces with an [LogicalPlan::EmptyRelation].
            // For such query, the inner query(select * from xxx limit 5) should be optimized as an EmptyRelation:
            // select * from (select * from xxx limit 5) a limit 2 offset 5;
            match fetch {
                Some(fetch) => {
                    if *fetch == 0 || ancestor_skip >= *fetch {
                        return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: input.schema().clone(),
                        }));
                    }
                }
                None => {
                    if *skip == 0 {
                        // If there is no LIMIT and OFFSET is zero, LIMIT/OFFSET can be removed
                        return Ok(input.as_ref().clone());
                    }
                }
            }

            let expr = plan.expressions();

            // apply the optimization to all inputs of the plan
            let inputs = plan.inputs();
            let new_inputs = inputs
                .iter()
                .map(|plan| {
                    eliminate_limit(
                        _optimizer,
                        &Ancestor::FromLimit { skip: *skip },
                        plan,
                        _optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            from_plan(plan, &expr, &new_inputs)
        }
        // Rest: recurse and find possible LIMIT 0/Multi LIMIT OFFSET nodes
        _ => {
            // For those plans(projection/sort/..) which do not affect the output rows of sub-plans, we still use ancestor;
            // otherwise, use NotRelevant instead.
            let ancestor = match plan {
                LogicalPlan::Projection { .. } | LogicalPlan::Sort { .. } => ancestor,
                _ => &Ancestor::NotRelevant,
            };

            let expr = plan.expressions();

            // apply the optimization to all inputs of the plan
            let inputs = plan.inputs();
            let new_inputs = inputs
                .iter()
                .map(|plan| {
                    eliminate_limit(_optimizer, ancestor, plan, _optimizer_config)
                })
                .collect::<Result<Vec<_>>>()?;

            from_plan(plan, &expr, &new_inputs)
        }
    }
}

impl OptimizerRule for EliminateLimit {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        eliminate_limit(self, &Ancestor::NotRelevant, plan, optimizer_config)
    }

    fn name(&self) -> &str {
        "eliminate_limit"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_common::Column;
    use datafusion_expr::{
        col,
        logical_plan::{builder::LogicalPlanBuilder, JoinType},
        sum,
    };

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = EliminateLimit::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
        assert_eq!(plan.schema(), optimized_plan.schema());
    }

    #[test]
    fn limit_0_root() {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])
            .unwrap()
            .limit(0, Some(0))
            .unwrap()
            .build()
            .unwrap();
        // No aggregate / scan / limit
        let expected = "EmptyRelation";
        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn limit_0_nested() {
        let table_scan = test_table_scan().unwrap();
        let plan1 = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(vec![col("a")], vec![sum(col("b"))])
            .unwrap()
            .build()
            .unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])
            .unwrap()
            .limit(0, Some(0))
            .unwrap()
            .union(plan1)
            .unwrap()
            .build()
            .unwrap();

        // Left side is removed
        let expected = "Union\
            \n  EmptyRelation\
            \n  Aggregate: groupBy=[[#test.a]], aggr=[[SUM(#test.b)]]\
            \n    TableScan: test";
        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn limit_fetch_with_ancestor_limit_skip() {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])
            .unwrap()
            .limit(0, Some(2))
            .unwrap()
            .limit(2, None)
            .unwrap()
            .build()
            .unwrap();

        // No aggregate / scan / limit
        let expected = "Limit: skip=2, fetch=None\
            \n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn multi_limit_offset_sort_eliminate() {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])
            .unwrap()
            .limit(0, Some(2))
            .unwrap()
            .sort(vec![col("a")])
            .unwrap()
            .limit(2, Some(1))
            .unwrap()
            .build()
            .unwrap();

        let expected = "Limit: skip=2, fetch=1\
            \n  Sort: #test.a\
            \n    EmptyRelation";
        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn limit_fetch_with_ancestor_limit_fetch() {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])
            .unwrap()
            .limit(0, Some(2))
            .unwrap()
            .sort(vec![col("a")])
            .unwrap()
            .limit(0, Some(1))
            .unwrap()
            .build()
            .unwrap();

        let expected = "Limit: skip=0, fetch=1\
            \n  Sort: #test.a\
            \n    Limit: skip=0, fetch=2\
            \n      Aggregate: groupBy=[[#test.a]], aggr=[[SUM(#test.b)]]\
            \n        TableScan: test";
        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn limit_with_ancestor_limit() {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])
            .unwrap()
            .limit(2, Some(1))
            .unwrap()
            .sort(vec![col("a")])
            .unwrap()
            .limit(3, Some(1))
            .unwrap()
            .build()
            .unwrap();

        let expected = "Limit: skip=3, fetch=1\
            \n  Sort: #test.a\
            \n    EmptyRelation";
        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn limit_join_with_ancestor_limit() {
        let table_scan = test_table_scan().unwrap();
        let table_scan_inner = test_table_scan_with_name("test1").unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(2, Some(1))
            .unwrap()
            .join_using(
                &table_scan_inner,
                JoinType::Inner,
                vec![Column::from_name("a".to_string())],
            )
            .unwrap()
            .limit(3, Some(1))
            .unwrap()
            .build()
            .unwrap();

        let expected = "Limit: skip=3, fetch=1\
            \n  Inner Join: Using #test.a = #test1.a\
            \n    Limit: skip=2, fetch=1\
            \n      TableScan: test\
            \n    TableScan: test1";
        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn remove_zero_offset() {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])
            .unwrap()
            .limit(0, None)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Aggregate: groupBy=[[#test.a]], aggr=[[SUM(#test.b)]]\
            \n  TableScan: test";
        assert_optimized_plan_eq(&plan, expected);
    }
}
