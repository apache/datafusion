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

use crate::analyzer::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::utils::COUNT_STAR_EXPANSION;
use datafusion_expr::{aggregate_function, lit, Aggregate, Expr, LogicalPlan, Window};
use std::ops::Deref;
use std::sync::Arc;

pub struct CountWildcardRule {}

impl Default for CountWildcardRule {
    fn default() -> Self {
        CountWildcardRule::new()
    }
}

impl CountWildcardRule {
    pub fn new() -> Self {
        CountWildcardRule {}
    }
}
impl AnalyzerRule for CountWildcardRule {
    fn analyze(&self, plan: &LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        let new_plan = match plan {
            LogicalPlan::Window(window) => {
                let inputs = plan.inputs();
                let window_expr = window.clone().window_expr;
                let window_expr = handle_wildcard(window_expr).unwrap();
                LogicalPlan::Window(Window {
                    input: Arc::new(inputs.get(0).unwrap().deref().clone()),
                    window_expr,
                    schema: plan.schema().clone(),
                })
            }

            LogicalPlan::Aggregate(aggregate) => {
                let inputs = plan.inputs();
                let aggr_expr = aggregate.clone().aggr_expr;
                let aggr_expr = handle_wildcard(aggr_expr).unwrap();
                LogicalPlan::Aggregate(
                    Aggregate::try_new_with_schema(
                        Arc::new(inputs.get(0).unwrap().deref().clone()),
                        aggregate.clone().group_expr,
                        aggr_expr,
                        plan.schema().clone(),
                    )
                    .unwrap(),
                )
            }
            _ => plan.clone(),
        };
        Ok(new_plan)
    }

    fn name(&self) -> &str {
        "count_wildcard_rule"
    }
}

//handle Count(Expr:Wildcard) with DataFrame API
pub fn handle_wildcard(exprs: Vec<Expr>) -> Result<Vec<Expr>> {
    let exprs: Vec<Expr> = exprs
        .iter()
        .map(|expr| match expr {
            Expr::AggregateFunction(AggregateFunction {
                fun: aggregate_function::AggregateFunction::Count,
                args,
                distinct,
                filter,
            }) if args.len() == 1 => match args[0] {
                Expr::Wildcard => Expr::AggregateFunction(AggregateFunction {
                    fun: aggregate_function::AggregateFunction::Count,
                    args: vec![lit(COUNT_STAR_EXPANSION)],
                    distinct: *distinct,
                    filter: filter.clone(),
                }),
                _ => expr.clone(),
            },
            _ => expr.clone(),
        })
        .collect();
    Ok(exprs)
}
