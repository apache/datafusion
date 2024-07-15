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

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::Result;
use datafusion_common::{config::ConfigOptions, tree_node::TransformedResult};
use datafusion_expr::utils::COUNT_STAR_EXPANSION;
use datafusion_expr::{
    expr::{AggregateFunction, AggregateFunctionDefinition, WindowFunction},
    LogicalPlan, WindowFunctionDefinition,
};
use datafusion_expr::{lit, Expr};

use crate::utils::NamePreserver;
use crate::AnalyzerRule;

/// Rewrite `Count()` to `Count(Expr:Literal(1))`.
#[derive(Default)]
pub struct CountEmptyRule {}

impl CountEmptyRule {
    /// Create a new instance of the rule
    pub fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for CountEmptyRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_down_with_subqueries(analyze_internal).data()
    }

    fn name(&self) -> &str {
        "count_empty_rule"
    }
}

fn is_count_empty_aggregate(aggregate_function: &AggregateFunction) -> bool {
    matches!(aggregate_function,
        AggregateFunction {
            func_def: AggregateFunctionDefinition::UDF(udf),
            args,
            ..
        } if udf.name() == "count" && args.is_empty())
}

fn is_count_empty_window_aggregate(window_function: &WindowFunction) -> bool {
    let args = &window_function.args;
    matches!(window_function.fun,
        WindowFunctionDefinition::AggregateUDF(ref udaf)
            if udaf.name() == "count" && args.is_empty())
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let name_preserver = NamePreserver::new(&plan);
    plan.map_expressions(|expr| {
        let original_name = name_preserver.save(&expr)?;
        let transformed_expr = expr.transform_up(|expr| match expr {
            Expr::WindowFunction(mut window_function)
                if is_count_empty_window_aggregate(&window_function) =>
            {
                window_function.args = vec![lit(COUNT_STAR_EXPANSION)];
                Ok(Transformed::yes(Expr::WindowFunction(window_function)))
            }
            Expr::AggregateFunction(mut aggregate_function)
                if is_count_empty_aggregate(&aggregate_function) =>
            {
                aggregate_function.args = vec![lit(COUNT_STAR_EXPANSION)];
                Ok(Transformed::yes(Expr::AggregateFunction(
                    aggregate_function,
                )))
            }
            _ => Ok(Transformed::no(expr)),
        })?;
        transformed_expr.map_data(|data| original_name.restore(data))
    })
}
