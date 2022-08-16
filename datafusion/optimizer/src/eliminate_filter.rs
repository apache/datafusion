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

//! Optimizer rule to replace `where false` on a plan with an empty relation.
//! This saves time in planning and executing the query.
//! Note that this rule should be applied after simplify expressions optimizer rule.
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    logical_plan::{EmptyRelation, Filter, LogicalPlan},
    utils::from_plan,
    Expr,
};

/// Optimization rule that elimanate the scalar value (true/false) filter with an [LogicalPlan::EmptyRelation]
#[derive(Default)]
pub struct EliminateFilter;

impl EliminateFilter {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateFilter {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(Filter {
                predicate: Expr::Literal(ScalarValue::Boolean(Some(v))),
                input,
            }) => {
                if !*v {
                    Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: input.schema().clone(),
                    }))
                } else {
                    self.optimize(input, optimizer_config)
                }
            }
            _ => {
                // Apply the optimization to all inputs of the plan
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|plan| self.optimize(plan, optimizer_config))
                    .collect::<Result<Vec<_>>>()?;

                from_plan(plan, &plan.expressions(), &new_inputs)
            }
        }
    }

    fn name(&self) -> &str {
        "eliminate_filter"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::{col, lit, logical_plan::builder::LogicalPlanBuilder, sum};

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = EliminateFilter::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
        assert_eq!(plan.schema(), optimized_plan.schema());
    }

    #[test]
    fn fliter_false() {
        let filter_expr = Expr::Literal(ScalarValue::Boolean(Some(false)));

        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])
            .unwrap()
            .filter(filter_expr)
            .unwrap()
            .build()
            .unwrap();

        // No aggregate / scan / limit
        let expected = "EmptyRelation";
        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn fliter_false_nested() {
        let filter_expr = Expr::Literal(ScalarValue::Boolean(Some(false)));

        let table_scan = test_table_scan().unwrap();
        let plan1 = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(vec![col("a")], vec![sum(col("b"))])
            .unwrap()
            .build()
            .unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])
            .unwrap()
            .filter(filter_expr)
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
    fn fliter_true() {
        let filter_expr = Expr::Literal(ScalarValue::Boolean(Some(true)));

        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])
            .unwrap()
            .filter(filter_expr)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Aggregate: groupBy=[[#test.a]], aggr=[[SUM(#test.b)]]\
        \n  TableScan: test";
        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn fliter_true_nested() {
        let filter_expr = Expr::Literal(ScalarValue::Boolean(Some(true)));

        let table_scan = test_table_scan().unwrap();
        let plan1 = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(vec![col("a")], vec![sum(col("b"))])
            .unwrap()
            .build()
            .unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])
            .unwrap()
            .filter(filter_expr)
            .unwrap()
            .union(plan1)
            .unwrap()
            .build()
            .unwrap();

        // Filter is removed
        let expected = "Union\
            \n  Aggregate: groupBy=[[#test.a]], aggr=[[SUM(#test.b)]]\
            \n    TableScan: test\
            \n  Aggregate: groupBy=[[#test.a]], aggr=[[SUM(#test.b)]]\
            \n    TableScan: test";
        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn fliter_from_subquery() {
        // SELECT a FROM (SELECT a FROM test WHERE FALSE) WHERE TRUE

        let false_filter = lit(false);
        let table_scan = test_table_scan().unwrap();
        let plan1 = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])
            .unwrap()
            .filter(false_filter)
            .unwrap()
            .build()
            .unwrap();

        let true_filter = lit(true);
        let plan = LogicalPlanBuilder::from(plan1)
            .project(vec![col("a")])
            .unwrap()
            .filter(true_filter)
            .unwrap()
            .build()
            .unwrap();

        // Filter is removed
        let expected = "Projection: #test.a\
            \n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected);
    }
}
