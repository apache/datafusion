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
use crate::logical_plan::plan::{Aggregate, Projection};
use crate::logical_plan::{col, columnize_expr, DFSchema, Expr, LogicalPlan};
use crate::physical_plan::aggregates;
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use hashbrown::HashSet;
use std::sync::Arc;

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
            .map(|agg_expr| match agg_expr {
                Expr::AggregateFunction { fun, args, .. } => {
                    let mut new_args = args.clone();
                    match fun {
                        aggregates::AggregateFunction::ApproxMedian => {
                            //new_args.push(lit(0.5_f64));
                            Expr::AggregateFunction {
                                fun: aggregates::AggregateFunction::ApproxPercentileCont,
                                args: new_args,
                                distinct: false,
                            }
                        }
                        _ => agg_expr.clone(),
                    }
                }
                _ => agg_expr.clone(),
            })
            .collect::<Vec<_>>();

            Ok(LogicalPlan::Aggregate(Aggregate {
                input: input.clone(),
                aggr_expr: new_aggr_expr,
                schema: schema.clone(),
                group_expr: group_expr.clone(),
            }))
        }
        _ => Ok(plan.clone())
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::logical_plan::{col, count, count_distinct, lit, max, LogicalPlanBuilder};
//     use crate::physical_plan::aggregates;
//     use crate::test::*;

//     fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
//         let rule = SingleDistinctToGroupBy::new();
//         let optimized_plan = rule
//             .optimize(plan, &ExecutionProps::new())
//             .expect("failed to optimize plan");
//         let formatted_plan = format!("{}", optimized_plan.display_indent_schema());
//         assert_eq!(formatted_plan, expected);
//     }

//     #[test]
//     fn not_exist_distinct() -> Result<()> {
//         let table_scan = test_table_scan()?;

//         let plan = LogicalPlanBuilder::from(table_scan)
//             .aggregate(Vec::<Expr>::new(), vec![max(col("b"))])?
//             .build()?;

//         // Do nothing
//         let expected = "Aggregate: groupBy=[[]], aggr=[[MAX(#test.b)]] [MAX(test.b):UInt32;N]\
//                             \n  TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

//         assert_optimized_plan_eq(&plan, expected);
//         Ok(())
//     }

//     #[test]
//     fn single_distinct() -> Result<()> {
//         let table_scan = test_table_scan()?;

//         let plan = LogicalPlanBuilder::from(table_scan)
//             .aggregate(Vec::<Expr>::new(), vec![count_distinct(col("b"))])?
//             .build()?;

//         // Should work
//         let expected = "Projection: #COUNT(alias1) AS COUNT(DISTINCT test.b) [COUNT(DISTINCT test.b):UInt64;N]\
//                             \n  Aggregate: groupBy=[[]], aggr=[[COUNT(#alias1)]] [COUNT(alias1):UInt64;N]\
//                             \n    Aggregate: groupBy=[[#test.b AS alias1]], aggr=[[]] [alias1:UInt32]\
//                             \n      TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

//         assert_optimized_plan_eq(&plan, expected);
//         Ok(())
//     }

//     #[test]
//     fn single_distinct_expr() -> Result<()> {
//         let table_scan = test_table_scan()?;

//         let plan = LogicalPlanBuilder::from(table_scan)
//             .aggregate(Vec::<Expr>::new(), vec![count_distinct(lit(2) * col("b"))])?
//             .build()?;

//         let expected = "Projection: #COUNT(alias1) AS COUNT(DISTINCT Int32(2) * test.b) [COUNT(DISTINCT Int32(2) * test.b):UInt64;N]\
//                             \n  Aggregate: groupBy=[[]], aggr=[[COUNT(#alias1)]] [COUNT(alias1):UInt64;N]\
//                             \n    Aggregate: groupBy=[[Int32(2) * #test.b AS alias1]], aggr=[[]] [alias1:Int32]\
//                             \n      TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

//         assert_optimized_plan_eq(&plan, expected);
//         Ok(())
//     }

//     #[test]
//     fn single_distinct_and_groupby() -> Result<()> {
//         let table_scan = test_table_scan()?;

//         let plan = LogicalPlanBuilder::from(table_scan)
//             .aggregate(vec![col("a")], vec![count_distinct(col("b"))])?
//             .build()?;

//         // Should work
//         let expected = "Projection: #test.a AS a, #COUNT(alias1) AS COUNT(DISTINCT test.b) [a:UInt32, COUNT(DISTINCT test.b):UInt64;N]\
//                             \n  Aggregate: groupBy=[[#test.a]], aggr=[[COUNT(#alias1)]] [a:UInt32, COUNT(alias1):UInt64;N]\
//                             \n    Aggregate: groupBy=[[#test.a, #test.b AS alias1]], aggr=[[]] [a:UInt32, alias1:UInt32]\
//                             \n      TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

//         assert_optimized_plan_eq(&plan, expected);
//         Ok(())
//     }

//     #[test]
//     fn two_distinct_and_groupby() -> Result<()> {
//         let table_scan = test_table_scan()?;

//         let plan = LogicalPlanBuilder::from(table_scan)
//             .aggregate(
//                 vec![col("a")],
//                 vec![count_distinct(col("b")), count_distinct(col("c"))],
//             )?
//             .build()?;

//         // Do nothing
//         let expected = "Aggregate: groupBy=[[#test.a]], aggr=[[COUNT(DISTINCT #test.b), COUNT(DISTINCT #test.c)]] [a:UInt32, COUNT(DISTINCT test.b):UInt64;N, COUNT(DISTINCT test.c):UInt64;N]\
//                             \n  TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

//         assert_optimized_plan_eq(&plan, expected);
//         Ok(())
//     }

//     #[test]
//     fn one_field_two_distinct_and_groupby() -> Result<()> {
//         let table_scan = test_table_scan()?;

//         let plan = LogicalPlanBuilder::from(table_scan)
//             .aggregate(
//                 vec![col("a")],
//                 vec![
//                     count_distinct(col("b")),
//                     Expr::AggregateFunction {
//                         fun: aggregates::AggregateFunction::Max,
//                         distinct: true,
//                         args: vec![col("b")],
//                     },
//                 ],
//             )?
//             .build()?;
//         // Should work
//         let expected = "Projection: #test.a AS a, #COUNT(alias1) AS COUNT(DISTINCT test.b), #MAX(alias1) AS MAX(DISTINCT test.b) [a:UInt32, COUNT(DISTINCT test.b):UInt64;N, MAX(DISTINCT test.b):UInt32;N]\
//                             \n  Aggregate: groupBy=[[#test.a]], aggr=[[COUNT(#alias1), MAX(#alias1)]] [a:UInt32, COUNT(alias1):UInt64;N, MAX(alias1):UInt32;N]\
//                             \n    Aggregate: groupBy=[[#test.a, #test.b AS alias1]], aggr=[[]] [a:UInt32, alias1:UInt32]\
//                             \n      TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

//         assert_optimized_plan_eq(&plan, expected);
//         Ok(())
//     }

//     #[test]
//     fn distinct_and_common() -> Result<()> {
//         let table_scan = test_table_scan()?;

//         let plan = LogicalPlanBuilder::from(table_scan)
//             .aggregate(
//                 vec![col("a")],
//                 vec![count_distinct(col("b")), count(col("c"))],
//             )?
//             .build()?;

//         // Do nothing
//         let expected = "Aggregate: groupBy=[[#test.a]], aggr=[[COUNT(DISTINCT #test.b), COUNT(#test.c)]] [a:UInt32, COUNT(DISTINCT test.b):UInt64;N, COUNT(test.c):UInt64;N]\
//                             \n  TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

//         assert_optimized_plan_eq(&plan, expected);
//         Ok(())
//     }
// }
