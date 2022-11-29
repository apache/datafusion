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

//! Optimizer rule to merge SubqueryAlias.
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::logical_plan::LogicalPlan;

/// Optimization rule that merge SubqueryAlias.
#[derive(Default)]
pub struct MergeSubqueryAlias;

impl MergeSubqueryAlias {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for MergeSubqueryAlias {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let subquery_alias = match plan {
            LogicalPlan::SubqueryAlias(subquery_alias) => subquery_alias,
            _ => return utils::optimize_children(self, plan, optimizer_config),
        };

        let child_plan = &*(subquery_alias.input);
        match child_plan {
            LogicalPlan::SubqueryAlias(child_subquery_alias) => {
                let optimized_plan =
                    plan.with_new_inputs(&[(*(child_subquery_alias.input)).clone()])?;
                self.optimize(&optimized_plan, optimizer_config)
            }
            _ => utils::optimize_children(self, plan, optimizer_config),
        }
    }

    fn name(&self) -> &str {
        "merge_subquery_alias"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) -> Result<()> {
        let rule = MergeSubqueryAlias::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
        assert_eq!(plan.schema(), optimized_plan.schema());
        Ok(())
    }

    #[test]
    fn merge_two() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("a")?
            .alias("b")?
            .build()?;
        let expected = "SubqueryAlias: b\
        \n  TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn merge_three() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("a")?
            .alias("b")?
            .alias("c")?
            .build()?;
        let expected = "SubqueryAlias: c\
        \n  TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }
}
