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

//! Optimizer rule to replaces redundant aggregations on a plan.
//! This saves time in planning and executing the query.

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::display::ToStringifiedPlan;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::Result;
use datafusion_expr::{logical_plan::LogicalPlan, Aggregate, Distinct, Join};
use std::ops::Deref;

#[derive(Default)]
pub struct EliminateAggregate {
    group_bys: Vec<LogicalPlan>,
}

impl EliminateAggregate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {
            group_bys: Vec::new(),
        }
    }
}

impl OptimizerRule for EliminateAggregate {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // SELECT DISTINCT c3 FROM aggregate_test_100 GROUP BY c3 LIMIT 5;
        // SELECT c3 FROM aggregate_test_100 GROUP BY c3 LIMIT 5;

        // SELECT DISTINCT c3 FROM (SELECT c3 FROM a1 GROUP BY c3) GROUP BY c3 LIMIT 5;
        // SELECT DISTINCT c3 FROM (SELECT c3 FROM a1 GROUP BY c3) GROUP BY c3 LIMIT 5;

        // logical_plan
        // Limit: skip=0, fetch=5
        //     --Aggregate: groupBy=[[aggregate_test_100.c3]], aggr=[[]]
        //     ----TableScan: aggregate_test_100 projection=[c3]
        //
        // Limit: skip=0, fetch=5
        //     --Aggregate: groupBy=[[aggregate_test_100.c3]], aggr=[[]]
        //     ----Aggregate: groupBy=[[aggregate_test_100.c3]], aggr=[[]]
        //     ------TableScan: aggregate_test_100 projection=[c3]

        match plan {
            LogicalPlan::Distinct(Distinct::All(distinct)) => {
                let fields = distinct.schema().fields();
                let all_fields = (0..fields.len()).collect::<Vec<_>>();
                let func_deps = distinct.schema().functional_dependencies().clone();

                for func_dep in func_deps.iter() {
                    if func_dep.source_indices == all_fields {
                        return Ok(Some(distinct.inputs()[0].clone()));
                    }
                }
                return Ok(None);
            }
            LogicalPlan::Distinct(Distinct::On(distinct)) => {
                let fields = distinct.schema.fields();
                let all_fields = (0..fields.len()).collect::<Vec<_>>();
                let func_deps = distinct.schema.functional_dependencies().clone();

                for func_dep in func_deps.iter() {
                    if func_dep.source_indices == all_fields {
                        return Ok(Some(distinct.input.as_ref().clone()));
                    }
                }
                return Ok(None);
            }
            LogicalPlan::Aggregate(Aggregate { .. }) => Ok(None),
            LogicalPlan::Join(Join { .. }) => Ok(None),
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "eliminate_aggregate"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

#[cfg(test)]
mod tests {
    use crate::eliminate_aggregate::EliminateAggregate;
    use datafusion_common::Result;
    use datafusion_expr::{
        col, logical_plan::builder::LogicalPlanBuilder, Expr, LogicalPlan,
    };
    use std::sync::Arc;

    use crate::test::*;

    fn assert_optimized_plan_equal(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(
            Arc::new(EliminateAggregate::new()),
            plan.clone(),
            expected,
        )
    }

    #[test]
    fn eliminate_redundant_distinct_simple() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("c")], Vec::<Expr>::new())?
            .project(vec![col("c")])?
            .distinct()?
            .build()?;

        let expected = "Aggregate: groupBy=[[test.c]], aggr=[[]]\n  TableScan: test";
        // No aggregate / scan / limit
        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn eliminate_redundant_distinct_pair() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a"), col("b")], Vec::<Expr>::new())?
            .project(vec![col("a"), col("b")])?
            .distinct()?
            .build()?;

        // No aggregate / scan / limit
        let expected = "Distinct:\n  Projection: test.a, test.b\n    Aggregate: groupBy=[[test.a, test.b]], aggr=[[]]\n      TableScan: test";
        assert_optimized_plan_equal(&plan, expected)
    }
}
