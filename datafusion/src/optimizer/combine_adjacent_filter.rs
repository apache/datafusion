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

//! Optimizer rule to combine the adjacent filter.
//! Example: [Filter: (a > 1)]->[Filter: (a < 10)] => [Filter: (a > 1) AND (a < 10)]
//! Note that this rule should be applied after filter push optimizer rule.
use datafusion_expr::{Expr, Operator};
use std::sync::Arc;

use crate::error::Result;
use crate::logical_plan::{plan::Filter, LogicalPlan};
use crate::optimizer::optimizer::OptimizerRule;

use super::utils;
use crate::execution::context::ExecutionProps;

/// Optimization rule that combine adjacent filter
#[derive(Default)]
pub struct CombineAdjacentFilter;

impl CombineAdjacentFilter {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn optimize(plan: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter(Filter {
            predicate: pred,
            input,
        }) => {
            let sub_plan = optimize(&**input);
            match sub_plan {
                LogicalPlan::Filter(Filter {
                    predicate: sub_pred,
                    input: sub_input,
                }) => LogicalPlan::Filter(Filter {
                    predicate: Expr::BinaryExpr {
                        left: Box::new(pred.clone()),
                        op: Operator::And,
                        right: Box::new(sub_pred),
                    },
                    input: Arc::new((*sub_input).clone()),
                }),
                _ => plan.clone(),
            }
        }
        _ => plan.clone(),
    }
}

impl OptimizerRule for CombineAdjacentFilter {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        let new_plan = optimize(plan);

        // Apply the optimization to all inputs of the plan
        let inputs = &new_plan.inputs();
        let new_inputs = inputs
            .iter()
            .map(|&new_plan| self.optimize(new_plan, execution_props))
            .collect::<Result<Vec<_>>>()?;

        utils::from_plan(&new_plan, &new_plan.expressions(), &new_inputs)
    }

    fn name(&self) -> &str {
        "combine_adjacent_filter"
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;
    use datafusion_expr::lit;

    use super::*;
    use crate::logical_plan::col;
    use crate::logical_plan::LogicalPlanBuilder;
    use crate::test::*;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = CombineAdjacentFilter::new();
        let optimized_plan = rule
            .optimize(plan, &ExecutionProps::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
        assert_eq!(plan.schema(), optimized_plan.schema());
    }

    #[test]
    fn double_filter() {
        let table_scan = test_table_scan().unwrap();
        let plan =
            LogicalPlanBuilder::from(table_scan)
                .project(vec![col("a")])
                .unwrap()
                .filter(col("a").eq(lit(1_i32)))
                .unwrap()
                .filter(col("a").eq(datafusion_expr::Expr::Literal(
                    ScalarValue::Boolean(Some(true)),
                )))
                .unwrap()
                .build()
                .unwrap();

        let expected = "Filter: #test.a = Boolean(true) AND #test.a = Int32(1)\
            \n  Projection: #test.a\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn thrice_filter() {
        let table_scan = test_table_scan().unwrap();
        let plan =
            LogicalPlanBuilder::from(table_scan)
                .project(vec![col("a")])
                .unwrap()
                .filter(col("a").eq(lit(1_i32)))
                .unwrap()
                .filter(col("a").eq(lit(2_i32)))
                .unwrap()
                .filter(col("a").eq(datafusion_expr::Expr::Literal(
                    ScalarValue::Boolean(Some(true)),
                )))
                .unwrap()
                .build()
                .unwrap();

        let expected = "Filter: #test.a = Boolean(true) AND #test.a = Int32(2) AND #test.a = Int32(1)\
            \n  Projection: #test.a\
            \n    TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn nested_double_filter() {
        let table_scan = test_table_scan().unwrap();
        let plan =
            LogicalPlanBuilder::from(table_scan)
                .project(vec![col("a")])
                .unwrap()
                .filter(col("a").eq(lit(1_i32)))
                .unwrap()
                .filter(col("a").eq(datafusion_expr::Expr::Literal(
                    ScalarValue::Boolean(Some(true)),
                )))
                .unwrap()
                .project(vec![col("a")])
                .unwrap()
                .filter(col("a").eq(lit(1_i32)))
                .unwrap()
                .filter(col("a").eq(datafusion_expr::Expr::Literal(
                    ScalarValue::Boolean(Some(true)),
                )))
                .unwrap()
                .build()
                .unwrap();

        let expected = "Filter: #test.a = Boolean(true) AND #test.a = Int32(1)\
        \n  Projection: #test.a\
        \n    Filter: #test.a = Boolean(true) AND #test.a = Int32(1)\
        \n      Projection: #test.a\
        \n        TableScan: test projection=None";
        assert_optimized_plan_eq(&plan, expected);
    }
}
