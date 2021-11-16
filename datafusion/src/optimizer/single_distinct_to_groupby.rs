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

//! single distinct to group by optimizer rule

use crate::error::Result;
use crate::execution::context::ExecutionProps;
use crate::logical_plan::{DFSchema, Expr, LogicalPlan};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use std::sync::Arc;

/// single distinct to group by optimizer rule
///   - Aggregation
///          GROUP BY (k)
///          F1(DISTINCT s0, s1, ...),
///          F2(DISTINCT s0, s1, ...),
///       - X
///
///   into
///
///   - Aggregation
///            GROUP BY (k)
///            F1(x)
///            F2(x)
///       - Aggregation
///               GROUP BY (k, s0, s1, ...)
///            - X
///   </pre>
///   <p>
pub struct SingleDistinctToGroupBy {}

impl SingleDistinctToGroupBy {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn optimize(plan: &LogicalPlan, execution_props: &ExecutionProps) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Aggregate {
            input,
            aggr_expr,
            schema: _,
            group_expr,
        } => {
            match is_single_agg(plan) {
                true => {
                    let mut all_group_args: Vec<Expr> = Vec::new();
                    all_group_args.append(&mut group_expr.clone());
                    // remove distinct and collection args
                    let mut new_aggr_expr = aggr_expr
                        .iter()
                        .map(|aggfunc| match aggfunc {
                            Expr::AggregateFunction { fun, args, .. } => {
                                all_group_args.append(&mut args.clone());
                                Expr::AggregateFunction {
                                    fun: fun.clone(),
                                    args: args.clone(),
                                    distinct: false,
                                }
                            }
                            _ => aggfunc.clone(),
                        })
                        .collect::<Vec<_>>();

                    let all_field = all_group_args
                        .iter()
                        .map(|expr| expr.to_field(input.schema()).unwrap())
                        .collect::<Vec<_>>();

                    let grouped_schema = Arc::new(DFSchema::new(all_field).unwrap());
                    let new_aggregate = LogicalPlan::Aggregate {
                        input: input.clone(),
                        group_expr: all_group_args,
                        aggr_expr: Vec::new(),
                        schema: grouped_schema,
                    };
                    let mut expres = group_expr.clone();
                    expres.append(&mut new_aggr_expr);
                    utils::from_plan(plan, &expres, &[new_aggregate])
                }
                false => {
                    let expr = plan.expressions();
                    // apply the optimization to all inputs of the plan
                    let inputs = plan.inputs();

                    let new_inputs = inputs
                        .iter()
                        .map(|plan| optimize(plan, execution_props))
                        .collect::<Result<Vec<_>>>()?;

                    utils::from_plan(plan, &expr, &new_inputs)
                }
            }
        }
        _ => {
            let expr = plan.expressions();
            let inputs = plan.inputs();
            let new_inputs = inputs
                .iter()
                .map(|plan| optimize(plan, execution_props))
                .collect::<Result<Vec<_>>>()?;
            utils::from_plan(plan, &expr, &new_inputs)
        }
    }
}

fn is_single_agg(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate {
            input: _,
            aggr_expr,
            ..
        } => {
            let mut distinct_agg_num = 0;
            aggr_expr.iter().for_each(|aggfunc| {
                if let Expr::AggregateFunction {
                    fun: _,
                    args: _,
                    distinct,
                } = aggfunc
                {
                    if *distinct {
                        distinct_agg_num += 1;
                    }
                }
            });
            !aggr_expr.is_empty() && aggr_expr.len() == distinct_agg_num
        }
        _ => false,
    }
}

impl OptimizerRule for SingleDistinctToGroupBy {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        optimize(plan, execution_props)
    }
    fn name(&self) -> &str {
        "SingleDistinctAggregationToGroupBy"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{col, count, count_distinct, max, LogicalPlanBuilder};
    use crate::test::*;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = SingleDistinctToGroupBy::new();
        let optimized_plan = rule
            .optimize(plan, &ExecutionProps::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{}", optimized_plan.display_indent_schema());
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn not_exist_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(#test.b)]] [MAX(test.b):UInt32;N]\
                            \n  TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn single_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![count_distinct(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[COUNT(#test.b)]] [COUNT(DISTINCT test.b):UInt64;N]\
                            \n  Aggregate: groupBy=[[#test.b]], aggr=[[]] [b:UInt32]\
                            \n    TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn single_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![count_distinct(col("b"))])?
            .build()?;

        let expected = "Aggregate: groupBy=[[#test.a]], aggr=[[COUNT(#test.b)]] [a:UInt32, COUNT(DISTINCT test.b):UInt64;N]\
                            \n  Aggregate: groupBy=[[#test.a, #test.b]], aggr=[[]] [a:UInt32, b:UInt32]\
                            \n    TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn two_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count_distinct(col("b")), count_distinct(col("c"))],
            )?
            .build()?;

        let expected = "Aggregate: groupBy=[[#test.a]], aggr=[[COUNT(#test.b), COUNT(#test.c)]] [a:UInt32, COUNT(DISTINCT test.b):UInt64;N, COUNT(DISTINCT test.c):UInt64;N]\
                            \n  Aggregate: groupBy=[[#test.a, #test.b, #test.c]], aggr=[[]] [a:UInt32, b:UInt32, c:UInt32]\
                            \n    TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn distinct_and_common() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count_distinct(col("b")), count(col("c"))],
            )?
            .build()?;

        let expected = "Aggregate: groupBy=[[#test.a]], aggr=[[COUNT(DISTINCT #test.b), COUNT(#test.c)]] [a:UInt32, COUNT(DISTINCT test.b):UInt64;N, COUNT(test.c):UInt64;N]\
                            \n  TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
