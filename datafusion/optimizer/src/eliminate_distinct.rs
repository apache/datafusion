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
//! This reduces redundant Aggregations in PyhsicalPlan.
//!
//! This optimizer changes this kind of query
//!
//! SELECT DISTINCT c3 FROM aggregate_test_100 GROUP BY c3 LIMIT 5;
//! to this
//! SELECT c3 FROM aggregate_test_100 GROUP BY c3 LIMIT 5;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed};
use datafusion_common::Result;
use datafusion_expr::{logical_plan::LogicalPlan, Distinct};

#[derive(Default)]
pub struct EliminateDistinct {}

impl EliminateDistinct {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateDistinct {
    fn name(&self) -> &str {
        "eliminate_distinct"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<
        datafusion_common::tree_node::Transformed<LogicalPlan>,
        datafusion_common::DataFusionError,
    > {
        match plan {
            LogicalPlan::Distinct(Distinct::All(distinct)) => {
                let fields = distinct.schema().fields();
                let all_fields = (0..fields.len()).collect::<Vec<_>>();
                let func_deps = distinct.schema().functional_dependencies().clone();

                for func_dep in func_deps.iter() {
                    if func_dep.source_indices == all_fields {
                        return Ok(Transformed::yes(distinct.inputs()[0].clone()));
                    }
                }
                return Ok(Transformed::no(LogicalPlan::Distinct(Distinct::All(distinct))));
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::eliminate_distinct::EliminateDistinct;
    use datafusion_common::Result;
    use datafusion_expr::{
        col, logical_plan::builder::LogicalPlanBuilder, Expr, LogicalPlan,
    };
    use std::sync::Arc;

    use crate::test::*;

    fn assert_optimized_plan_equal(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(
            Arc::new(EliminateDistinct::new()),
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
        let expected = "Aggregate: groupBy=[[test.a, test.b]], aggr=[[]]\n  TableScan: test";
        assert_optimized_plan_equal(&plan, expected)
    }
}
