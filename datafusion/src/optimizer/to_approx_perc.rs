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

//! espression/function to approx_percentile optimizer rule

use crate::error::Result;
use crate::execution::context::ExecutionProps;
use crate::logical_plan::plan::Aggregate;
use crate::logical_plan::{Expr, LogicalPlan};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use crate::physical_plan::aggregates;
use crate::scalar::ScalarValue;

/// espression/function to approx_percentile optimizer rule
///  ```text
///    SELECT F1(s)
///    ...
///
///    Into
///
///    SELECT APPROX_PERCENTILE_CONT(s, lit(n)) as "F1(s)"
///    ...
///  ```
pub struct ToApproxPerc {}

impl ToApproxPerc {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ToApproxPerc {
    fn default() -> Self {
        Self::new()
    }
}

fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Aggregate(Aggregate {
            input,
            aggr_expr,
            schema,
            group_expr,
        }) => {
            let new_aggr_expr = aggr_expr
                .iter()
                .map(|agg_expr| replace_with_percentile(agg_expr).unwrap())
                .collect::<Vec<_>>();

            Ok(LogicalPlan::Aggregate(Aggregate {
                input: input.clone(),
                aggr_expr: new_aggr_expr,
                schema: schema.clone(),
                group_expr: group_expr.clone(),
            }))
        }
        _ => optimize_children(plan),
    }
}

fn optimize_children(plan: &LogicalPlan) -> Result<LogicalPlan> {
    let expr = plan.expressions();
    let inputs = plan.inputs();
    let new_inputs = inputs
        .iter()
        .map(|plan| optimize(plan))
        .collect::<Result<Vec<_>>>()?;
    utils::from_plan(plan, &expr, &new_inputs)
}

fn replace_with_percentile(expr: &Expr) -> Result<Expr> {
    match expr {
        Expr::AggregateFunction {
            fun,
            args,
            distinct,
        } => {
            let mut new_args = args.clone();
            let mut new_func = fun.clone();
            if fun == &aggregates::AggregateFunction::ApproxMedian {
                new_args.push(Expr::Literal(ScalarValue::Float64(Some(0.5_f64))));
                new_func = aggregates::AggregateFunction::ApproxPercentileCont;
            }

            Ok(Expr::AggregateFunction {
                fun: new_func,
                args: new_args,
                distinct: *distinct,
            })
        }
        _ => Ok(expr.clone()),
    }
}

impl OptimizerRule for ToApproxPerc {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        _execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        optimize(plan)
    }
    fn name(&self) -> &str {
        "ToApproxPerc"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{col, LogicalPlanBuilder};
    use crate::physical_plan::aggregates;
    use crate::test::*;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = ToApproxPerc::new();
        let optimized_plan = rule
            .optimize(plan, &ExecutionProps::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{}", optimized_plan.display_indent_schema());
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn median_1() -> Result<()> {
        let table_scan = test_table_scan()?;
        let expr = Expr::AggregateFunction {
            fun: aggregates::AggregateFunction::ApproxMedian,
            distinct: false,
            args: vec![col("b")],
        };

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![expr])?
            .build()?;

        // Rewrite to use approx_percentile
        let expected = "Aggregate: groupBy=[[]], aggr=[[APPROXPERCENTILECONT(#test.b, Float64(0.5))]] [APPROXMEDIAN(test.b):UInt32;N]\
                            \n  TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
