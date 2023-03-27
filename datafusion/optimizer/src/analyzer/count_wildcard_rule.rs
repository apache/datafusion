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

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::Result;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::utils::COUNT_STAR_EXPANSION;
use datafusion_expr::{aggregate_function, lit, Aggregate, Expr, LogicalPlan, Window};

use crate::analyzer::AnalyzerRule;

/// Rewrite `Count(Expr:Wildcard)` to `Count(Expr:Literal)`.
/// Resolve issue: https://github.com/apache/arrow-datafusion/issues/5473.
pub struct CountWildcardRule {}

impl CountWildcardRule {
    pub fn new() -> Self {
        CountWildcardRule {}
    }
}

impl AnalyzerRule for CountWildcardRule {
    fn analyze(&self, plan: &LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.clone().transform_down(&analyze_internal)
    }

    fn name(&self) -> &str {
        "count_wildcard_rule"
    }
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Window(window) => {
            let window_expr = handle_wildcard(&window.window_expr);
            Ok(Transformed::Yes(LogicalPlan::Window(Window {
                input: window.input.clone(),
                window_expr,
                schema: window.schema,
            })))
        }
        LogicalPlan::Aggregate(agg) => {
            let aggr_expr = handle_wildcard(&agg.aggr_expr);
            Ok(Transformed::Yes(LogicalPlan::Aggregate(
                Aggregate::try_new_with_schema(
                    agg.input.clone(),
                    agg.group_expr.clone(),
                    aggr_expr,
                    agg.schema,
                )?,
            )))
        }
        _ => Ok(Transformed::No(plan)),
    }
}

// handle Count(Expr:Wildcard) with DataFrame API
pub fn handle_wildcard(exprs: &[Expr]) -> Vec<Expr> {
    exprs
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
        .collect()
}
