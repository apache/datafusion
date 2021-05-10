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

//! Optimizer rule to replace timestamp expressions to constants.
//! This saves time in planning and executing the query.
use crate::error::Result;
use crate::logical_plan::{Expr, LogicalPlan};
use crate::optimizer::optimizer::OptimizerRule;

use super::utils;
use crate::execution::context::ExecutionProps;
use crate::physical_plan::functions::BuiltinScalarFunction;
use crate::scalar::ScalarValue;
use chrono::{DateTime, Utc};

/// Optimization rule that replaces timestamp expressions with their values evaluated
pub struct TimestampEvaluation {}

impl TimestampEvaluation {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }

    /// Recursive function to optimize the now expression
    pub fn rewrite_expr(&self, exp: &Expr, date_time: &DateTime<Utc>) -> Result<Expr> {
        let expressions = utils::expr_sub_expressions(exp).unwrap();
        let expressions = expressions
            .iter()
            .map(|e| self.rewrite_expr(e, date_time))
            .collect::<Result<Vec<_>>>()?;

        let exp = match exp {
            Expr::ScalarFunction {
                fun: BuiltinScalarFunction::Now,
                ..
            } => Expr::Literal(ScalarValue::TimestampNanosecond(Some(
                date_time.timestamp_nanos(),
            ))),
            _ => exp.clone(),
        };
        utils::rewrite_expression(&exp, &expressions)
    }

    fn optimize_with_datetime(
        &self,
        plan: &LogicalPlan,
        date_time: &DateTime<Utc>,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Projection { .. } => {
                let exprs = plan
                    .expressions()
                    .iter()
                    .map(|exp| self.rewrite_expr(exp, date_time).unwrap())
                    .collect::<Vec<_>>();

                // apply the optimization to all inputs of the plan
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|plan| self.optimize_with_datetime(*plan, date_time))
                    .collect::<Result<Vec<_>>>()?;

                utils::from_plan(plan, &exprs, &new_inputs)
            }
            _ => {
                let expr = plan.expressions();

                // apply the optimization to all inputs of the plan
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|plan| self.optimize_with_datetime(*plan, date_time))
                    .collect::<Result<Vec<_>>>()?;

                utils::from_plan(plan, &expr, &new_inputs)
            }
        }
    }
}

impl OptimizerRule for TimestampEvaluation {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        self.optimize_with_datetime(plan, &props.query_execution_start_time.unwrap())
    }

    fn name(&self) -> &str {
        "timestamp_evaluation"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::LogicalPlanBuilder;
    use crate::test::*;

    fn get_optimized_plan_formatted(plan: &LogicalPlan) -> String {
        let rule = TimestampEvaluation::new();
        let execution_props = ExecutionProps {
            query_execution_start_time: Some(chrono::Utc::now()),
        };

        let optimized_plan = rule
            .optimize(plan, &execution_props)
            .expect("failed to optimize plan");
        return format!("{:?}", optimized_plan);
    }

    #[test]
    fn single_now() {
        let table_scan = test_table_scan().unwrap();
        let proj = vec![Expr::ScalarFunction {
            args: vec![],
            fun: BuiltinScalarFunction::Now,
        }];
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Projection: TimestampNanosecond(";
        assert!(get_optimized_plan_formatted(&plan).starts_with(expected));
    }

    #[test]
    fn double_now() {
        let table_scan = test_table_scan().unwrap();
        let proj = vec![
            Expr::ScalarFunction {
                args: vec![],
                fun: BuiltinScalarFunction::Now,
            },
            Expr::Alias(
                Box::new(Expr::ScalarFunction {
                    args: vec![],
                    fun: BuiltinScalarFunction::Now,
                }),
                "t2".to_string(),
            ),
        ];
        let plan = LogicalPlanBuilder::from(&table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let actual = get_optimized_plan_formatted(&plan);
        println!("output is {}", &actual);
        let expected_start = "Projection: TimestampNanosecond(";
        assert!(actual.starts_with(expected_start));

        let expected_end = ") AS t2\
             \n  TableScan: test projection=None";
        assert!(actual.ends_with(expected_end));
    }
}
