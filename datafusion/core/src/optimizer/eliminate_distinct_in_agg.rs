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
use crate::error::Result;
use crate::execution::context::ExecutionProps;
use crate::logical_plan::plan::{Aggregate, Projection};
use crate::logical_plan::ExprSchemable;
use crate::logical_plan::{columnize_expr, DFSchema, Expr, LogicalPlan};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::utils;
use datafusion_expr::AggregateFunction;
use std::sync::Arc;

/// Optimization rule that elimanate the distinct in aggregation.
#[derive(Default)]
pub struct EliminateDistinctInAgg;

impl EliminateDistinctInAgg {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

// Before optimize:
//   Aggregate: groupBy=[[]], aggr=[[MAX(DISTINCT #test.c1)]]
// After optimize:
//   Projection: #MAX(#test.c1) AS MAX(DISTINCT test.c1)
//     Aggregate: groupBy=[[]], aggr=[[MAX(#test.c1)]]
impl OptimizerRule for EliminateDistinctInAgg {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        if is_max_or_min(plan) {
            if let LogicalPlan::Aggregate(Aggregate {
                input,
                aggr_expr,
                schema,
                group_expr,
            }) = plan
            {
                let len = group_expr.len();
                let mut all_args = group_expr.clone();
                // remove distinct
                let new_aggr_expr = aggr_expr
                    .iter()
                    .map(|agg_expr| match agg_expr {
                        Expr::AggregateFunction {
                            fun,
                            args,
                            distinct,
                        } => {
                            if *distinct {
                                let expr = Expr::AggregateFunction {
                                    fun: fun.clone(),
                                    args: args.clone(),
                                    distinct: false,
                                };
                                all_args.push(expr.clone());
                                expr
                            } else {
                                agg_expr.clone()
                            }
                        }
                        _ => agg_expr.clone(),
                    })
                    .collect::<Vec<_>>();

                if all_args.len() == len {
                    let inputs = plan.inputs();
                    let new_inputs = inputs
                        .iter()
                        .map(|plan| self.optimize(plan, execution_props))
                        .collect::<Result<Vec<_>>>()?;

                    return utils::from_plan(plan, &plan.expressions(), &new_inputs);
                }

                let all_field = all_args
                    .iter()
                    .map(|expr| expr.to_field(input.schema()).unwrap())
                    .collect::<Vec<_>>();

                println!("all_field: {:?}", all_field);

                let agg_schema = DFSchema::new_with_metadata(
                    all_field,
                    input.schema().metadata().clone(),
                )
                .unwrap();

                let agg = LogicalPlan::Aggregate(Aggregate {
                    input: input.clone(),
                    aggr_expr: new_aggr_expr,
                    schema: Arc::new(agg_schema),
                    group_expr: group_expr.clone(),
                });

                let mut alias_expr: Vec<Expr> = Vec::new();
                agg.expressions().iter().enumerate().for_each(|(i, field)| {
                    alias_expr.push(columnize_expr(
                        field.clone().alias(schema.clone().fields()[i].name()),
                        schema,
                    ));
                });

                // projection
                return Ok(LogicalPlan::Projection(Projection {
                    expr: alias_expr,
                    input: Arc::new(agg),
                    schema: schema.clone(),
                    alias: Option::None,
                }));
            }
        };

        // Apply the optimization to all inputs of the plan
        let inputs = plan.inputs();
        let new_inputs = inputs
            .iter()
            .map(|plan| self.optimize(plan, execution_props))
            .collect::<Result<Vec<_>>>()?;

        utils::from_plan(plan, &plan.expressions(), &new_inputs)
    }

    fn name(&self) -> &str {
        "eliminate_distinct_in_agg"
    }
}

fn is_max_or_min(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(Aggregate { aggr_expr, .. }) => {
            aggr_expr.iter().all(|expr| match expr {
                Expr::AggregateFunction { fun, .. } => {
                    matches!(fun, AggregateFunction::Max | AggregateFunction::Min)
                }
                _ => false,
            })
        }
        _ => false,
    }
}
