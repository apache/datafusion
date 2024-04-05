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

//! [`ApplyFunctionRewrites`] to replace `Expr`s with function calls (e.g `||` to array_concat`)

use super::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::expr_rewriter::{rewrite_preserving_name, FunctionRewrite};
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{Expr, LogicalPlan};
use std::sync::Arc;

/// Analyzer rule that invokes [`FunctionRewrite`]s on expressions
#[derive(Default)]
pub struct ApplyFunctionRewrites {
    /// Expr --> Function writes to apply
    function_rewrites: Vec<Arc<dyn FunctionRewrite + Send + Sync>>,
}

impl ApplyFunctionRewrites {
    pub fn new(function_rewrites: Vec<Arc<dyn FunctionRewrite + Send + Sync>>) -> Self {
        Self { function_rewrites }
    }
}

impl AnalyzerRule for ApplyFunctionRewrites {
    fn name(&self) -> &str {
        "apply_function_rewrites"
    }

    fn analyze(&self, plan: LogicalPlan, options: &ConfigOptions) -> Result<LogicalPlan> {
        self.analyze_internal(&plan, options)
    }
}

impl ApplyFunctionRewrites {
    fn analyze_internal(
        &self,
        plan: &LogicalPlan,
        options: &ConfigOptions,
    ) -> Result<LogicalPlan> {
        // optimize child plans first
        let new_inputs = plan
            .inputs()
            .iter()
            .map(|p| self.analyze_internal(p, options))
            .collect::<Result<Vec<_>>>()?;

        // get schema representing all available input fields. This is used for data type
        // resolution only, so order does not matter here
        let mut schema = merge_schema(new_inputs.iter().collect());

        if let LogicalPlan::TableScan(ts) = plan {
            let source_schema = DFSchema::try_from_qualified_schema(
                ts.table_name.clone(),
                &ts.source.schema(),
            )?;
            schema.merge(&source_schema);
        }

        let mut expr_rewrite = OperatorToFunctionRewriter {
            function_rewrites: &self.function_rewrites,
            options,
            schema: &schema,
        };

        let new_expr = plan
            .expressions()
            .into_iter()
            .map(|expr| {
                // ensure names don't change:
                // https://github.com/apache/arrow-datafusion/issues/3555
                rewrite_preserving_name(expr, &mut expr_rewrite)
            })
            .collect::<Result<Vec<_>>>()?;

        plan.with_new_exprs(new_expr, new_inputs)
    }
}
struct OperatorToFunctionRewriter<'a> {
    function_rewrites: &'a [Arc<dyn FunctionRewrite + Send + Sync>],
    options: &'a ConfigOptions,
    schema: &'a DFSchema,
}

impl<'a> TreeNodeRewriter for OperatorToFunctionRewriter<'a> {
    type Node = Expr;

    fn f_up(&mut self, mut expr: Expr) -> Result<Transformed<Expr>> {
        // apply transforms one by one
        let mut transformed = false;
        for rewriter in self.function_rewrites.iter() {
            let result = rewriter.rewrite(expr, self.schema, self.options)?;
            if result.transformed {
                transformed = true;
            }
            expr = result.data
        }

        Ok(if transformed {
            Transformed::yes(expr)
        } else {
            Transformed::no(expr)
        })
    }
}
