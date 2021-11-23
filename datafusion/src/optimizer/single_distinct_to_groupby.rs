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
use crate::logical_plan::plan::{Aggregate, Projection};
use crate::logical_plan::{columnize_expr, DFSchema, Expr, LogicalPlan};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use hashbrown::HashSet;
use std::sync::Arc;

/// single distinct to group by optimizer rule
///  ```text
///    SELECT F1(DISTINCT s),F2(DISTINCT s)
///    ...
///    GROUP BY k
///
///    Into
///
///    SELECT F1(s),F2(s)
///    FROM (
///      SELECT s, k ... GROUP BY s, k
///    )
///    GROUP BY k
///  ```
pub struct SingleDistinctToGroupBy {}

impl SingleDistinctToGroupBy {
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
            if is_single_distinct_agg(plan) {
                let mut group_fields_set = HashSet::new();
                let mut all_group_args = group_expr.clone();
                // remove distinct and collection args
                let new_aggr_expr = aggr_expr
                    .iter()
                    .map(|agg_expr| match agg_expr {
                        Expr::AggregateFunction { fun, args, .. } => {
                            // is_single_distinct_agg ensure args.len=1
                            if group_fields_set
                                .insert(args[0].name(input.schema()).unwrap())
                            {
                                all_group_args.push(args[0].clone());
                            }
                            Expr::AggregateFunction {
                                fun: fun.clone(),
                                args: args.clone(),
                                distinct: false,
                            }
                        }
                        _ => agg_expr.clone(),
                    })
                    .collect::<Vec<_>>();

                let all_field = all_group_args
                    .iter()
                    .map(|expr| expr.to_field(input.schema()).unwrap())
                    .collect::<Vec<_>>();

                let grouped_schema = DFSchema::new(all_field).unwrap();
                let grouped_agg = LogicalPlan::Aggregate(Aggregate {
                    input: input.clone(),
                    group_expr: all_group_args,
                    aggr_expr: Vec::new(),
                    schema: Arc::new(grouped_schema.clone()),
                });
                let grouped_agg = optimize_children(&grouped_agg);
                let final_agg_schema = Arc::new(
                    DFSchema::new(
                        group_expr
                            .iter()
                            .chain(new_aggr_expr.iter())
                            .map(|expr| expr.to_field(&grouped_schema).unwrap())
                            .collect::<Vec<_>>(),
                    )
                    .unwrap(),
                );

                let final_agg = LogicalPlan::Aggregate(Aggregate {
                    input: Arc::new(grouped_agg.unwrap()),
                    group_expr: group_expr.clone(),
                    aggr_expr: new_aggr_expr,
                    schema: final_agg_schema.clone(),
                });

                //so the aggregates are displayed in the same way even after the rewrite
                let mut alias_expr: Vec<Expr> = Vec::new();
                final_agg
                    .expressions()
                    .iter()
                    .enumerate()
                    .for_each(|(i, field)| {
                        alias_expr.push(columnize_expr(
                            field.clone().alias(schema.clone().fields()[i].name()),
                            &final_agg_schema,
                        ));
                    });

                Ok(LogicalPlan::Projection(Projection {
                    expr: alias_expr,
                    input: Arc::new(final_agg),
                    schema: schema.clone(),
                    alias: Option::None,
                }))
            } else {
                optimize_children(plan)
            }
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

fn is_single_distinct_agg(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(Aggregate {
            input, aggr_expr, ..
        }) => {
            let mut fields_set = HashSet::new();
            aggr_expr
                .iter()
                .filter(|expr| {
                    let mut is_distinct = false;
                    if let Expr::AggregateFunction { distinct, args, .. } = expr {
                        is_distinct = *distinct;
                        args.iter().for_each(|expr| {
                            fields_set.insert(expr.name(input.schema()).unwrap());
                        })
                    }
                    is_distinct
                })
                .count()
                == aggr_expr.len()
                && fields_set.len() == 1
        }
        _ => false,
    }
}

impl OptimizerRule for SingleDistinctToGroupBy {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        _execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        optimize(plan)
    }
    fn name(&self) -> &str {
        "SingleDistinctAggregationToGroupBy"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{col, count, count_distinct, max, LogicalPlanBuilder};
    use crate::physical_plan::aggregates;
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

        // Do nothing
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

        // Should work
        let expected = "Projection: #COUNT(test.b) AS COUNT(DISTINCT test.b) [COUNT(DISTINCT test.b):UInt64;N]\
                            \n  Aggregate: groupBy=[[]], aggr=[[COUNT(#test.b)]] [COUNT(test.b):UInt64;N]\
                            \n    Aggregate: groupBy=[[#test.b]], aggr=[[]] [b:UInt32]\
                            \n      TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn single_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![count_distinct(col("b"))])?
            .build()?;

        // Should work
        let expected = "Projection: #test.a AS a, #COUNT(test.b) AS COUNT(DISTINCT test.b) [a:UInt32, COUNT(DISTINCT test.b):UInt64;N]\
                            \n  Aggregate: groupBy=[[#test.a]], aggr=[[COUNT(#test.b)]] [a:UInt32, COUNT(test.b):UInt64;N]\
                            \n    Aggregate: groupBy=[[#test.a, #test.b]], aggr=[[]] [a:UInt32, b:UInt32]\
                            \n      TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

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

        // Do nothing
        let expected = "Aggregate: groupBy=[[#test.a]], aggr=[[COUNT(DISTINCT #test.b), COUNT(DISTINCT #test.c)]] [a:UInt32, COUNT(DISTINCT test.b):UInt64;N, COUNT(DISTINCT test.c):UInt64;N]\
                            \n  TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn one_field_two_distinct_and_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    count_distinct(col("b")),
                    Expr::AggregateFunction {
                        fun: aggregates::AggregateFunction::Max,
                        distinct: true,
                        args: vec![col("b")],
                    },
                ],
            )?
            .build()?;
        // Should work
        let expected = "Projection: #test.a AS a, #COUNT(test.b) AS COUNT(DISTINCT test.b), #MAX(test.b) AS MAX(DISTINCT test.b) [a:UInt32, COUNT(DISTINCT test.b):UInt64;N, MAX(DISTINCT test.b):UInt32;N]\
                            \n  Aggregate: groupBy=[[#test.a]], aggr=[[COUNT(#test.b), MAX(#test.b)]] [a:UInt32, COUNT(test.b):UInt64;N, MAX(test.b):UInt32;N]\
                            \n    Aggregate: groupBy=[[#test.a, #test.b]], aggr=[[]] [a:UInt32, b:UInt32]\
                            \n      TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

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

        // Do nothing
        let expected = "Aggregate: groupBy=[[#test.a]], aggr=[[COUNT(DISTINCT #test.b), COUNT(#test.c)]] [a:UInt32, COUNT(DISTINCT test.b):UInt64;N, COUNT(test.c):UInt64;N]\
                            \n  TableScan: test projection=None [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
