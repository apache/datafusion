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

//! Optimizer rule to replace `LIMIT 0` on a plan with an empty relation.
//! This saves time in planning and executing the query.
use crate::error::Result;
use crate::logical_plan::LogicalPlan;
use crate::optimizer::optimizer::OptimizerRule;

use super::utils;
use crate::execution::context::ExecutionProps;

/// Optimization rule that replaces LIMIT 0 with an [LogicalPlan::EmptyRelation]
pub struct EliminateLimit;

impl EliminateLimit {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateLimit {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Limit { n, input } if *n == 0 => {
                Ok(LogicalPlan::EmptyRelation {
                    produce_one_row: false,
                    schema: input.schema().clone(),
                })
            }
            // Rest: recurse and find possible LIMIT 0 nodes
            _ => {
                let expr = plan.expressions();

                // apply the optimization to all inputs of the plan
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|plan| self.optimize(plan, execution_props))
                    .collect::<Result<Vec<_>>>()?;

                utils::from_plan(plan, &expr, &new_inputs)
            }
        }
    }

    fn name(&self) -> &str {
        "eliminate_limit"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::LogicalPlanBuilder;
    use crate::logical_plan::{col, sum};
    use crate::test::*;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = EliminateLimit::new();
        let optimized_plan = rule
            .optimize(plan, &ExecutionProps::new())
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
            .limit(0)
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
            .limit(0)
            .unwrap()
            .union(plan1)
            .unwrap()
            .build()
            .unwrap();

        // Left side is removed
        let expected = "Union\
            \n  EmptyRelation\
            \n  Aggregate: groupBy=[[#test.a]], aggr=[[SUM(#test.b)]]\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
    }
}
