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

//! [`Analyzer`] and [`AnalyzerRule`]

use std::fmt::Debug;
use std::sync::Arc;

use log::debug;

use datafusion_common::config::ConfigOptions;
use datafusion_common::instant::Instant;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::expr::Exists;
use datafusion_expr::expr::InSubquery;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::{Expr, LogicalPlan};

use crate::analyzer::count_wildcard_rule::CountWildcardRule;
use crate::analyzer::expand_wildcard_rule::ExpandWildcardRule;
use crate::analyzer::inline_table_scan::InlineTableScan;
use crate::analyzer::subquery::check_subquery_expr;
use crate::analyzer::type_coercion::TypeCoercion;
use crate::utils::log_plan;

use self::function_rewrite::ApplyFunctionRewrites;

pub mod count_wildcard_rule;
pub mod expand_wildcard_rule;
pub mod function_rewrite;
pub mod inline_table_scan;
pub mod subquery;
pub mod type_coercion;

/// [`AnalyzerRule`]s transform [`LogicalPlan`]s in some way to make
/// the plan valid prior to the rest of the DataFusion optimization process.
///
/// This is different than an [`OptimizerRule`](crate::OptimizerRule)
/// which must preserve the semantics of the `LogicalPlan`, while computing
/// results in a more optimal way.
///
/// For example, an `AnalyzerRule` may resolve [`Expr`]s into more specific
/// forms such as a subquery reference, or do type coercion to ensure the types
/// of operands are correct.
///
/// Use [`SessionState::add_analyzer_rule`] to register additional
/// `AnalyzerRule`s.
///
/// [`SessionState::add_analyzer_rule`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html#method.add_analyzer_rule
pub trait AnalyzerRule: Debug {
    /// Rewrite `plan`
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan>;

    /// A human readable name for this analyzer rule
    fn name(&self) -> &str;
}

/// A rule-based Analyzer.
///
/// An `Analyzer` transforms a `LogicalPlan`
/// prior to the rest of the DataFusion optimization process.
#[derive(Clone, Debug)]
pub struct Analyzer {
    /// Expr --> Function writes to apply prior to analysis passes
    pub function_rewrites: Vec<Arc<dyn FunctionRewrite + Send + Sync>>,
    /// All rules to apply
    pub rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
}

impl Default for Analyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl Analyzer {
    /// Create a new analyzer using the recommended list of rules
    pub fn new() -> Self {
        let rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![
            Arc::new(InlineTableScan::new()),
            // Every rule that will generate [Expr::Wildcard] should be placed in front of [ExpandWildcardRule].
            Arc::new(ExpandWildcardRule::new()),
            // [Expr::Wildcard] should be expanded before [TypeCoercion]
            Arc::new(TypeCoercion::new()),
            Arc::new(CountWildcardRule::new()),
        ];
        Self::with_rules(rules)
    }

    /// Create a new analyzer with the given rules
    pub fn with_rules(rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>) -> Self {
        Self {
            function_rewrites: vec![],
            rules,
        }
    }

    /// Add a function rewrite rule
    pub fn add_function_rewrite(
        &mut self,
        rewrite: Arc<dyn FunctionRewrite + Send + Sync>,
    ) {
        self.function_rewrites.push(rewrite);
    }

    /// return the list of function rewrites in this analyzer
    pub fn function_rewrites(&self) -> &[Arc<dyn FunctionRewrite + Send + Sync>] {
        &self.function_rewrites
    }

    /// Analyze the logical plan by applying analyzer rules, and
    /// do necessary check and fail the invalid plans
    pub fn execute_and_check<F>(
        &self,
        plan: LogicalPlan,
        config: &ConfigOptions,
        mut observer: F,
    ) -> Result<LogicalPlan>
    where
        F: FnMut(&LogicalPlan, &dyn AnalyzerRule),
    {
        let start_time = Instant::now();
        let mut new_plan = plan;

        // Create an analyzer pass that rewrites `Expr`s to function_calls, as
        // appropriate.
        //
        // Note this is run before all other rules since it rewrites based on
        // the argument types (List or Scalar), and TypeCoercion may cast the
        // argument types from Scalar to List.
        let expr_to_function: Option<Arc<dyn AnalyzerRule + Send + Sync>> =
            if self.function_rewrites.is_empty() {
                None
            } else {
                Some(Arc::new(ApplyFunctionRewrites::new(
                    self.function_rewrites.clone(),
                )))
            };
        let rules = expr_to_function.iter().chain(self.rules.iter());

        // TODO add common rule executor for Analyzer and Optimizer
        for rule in rules {
            new_plan = rule.analyze(new_plan, config).map_err(|e| {
                DataFusionError::Context(rule.name().to_string(), Box::new(e))
            })?;
            log_plan(rule.name(), &new_plan);
            observer(&new_plan, rule.as_ref());
        }
        // for easier display in explain output
        check_plan(&new_plan).map_err(|e| {
            DataFusionError::Context("check_analyzed_plan".to_string(), Box::new(e))
        })?;
        log_plan("Final analyzed plan", &new_plan);
        debug!("Analyzer took {} ms", start_time.elapsed().as_millis());
        Ok(new_plan)
    }
}

/// Do necessary check and fail the invalid plan
fn check_plan(plan: &LogicalPlan) -> Result<()> {
    plan.apply_with_subqueries(|plan: &LogicalPlan| {
        plan.apply_expressions(|expr| {
            // recursively look for subqueries
            expr.apply(|expr| {
                match expr {
                    Expr::Exists(Exists { subquery, .. })
                    | Expr::InSubquery(InSubquery { subquery, .. })
                    | Expr::ScalarSubquery(subquery) => {
                        check_subquery_expr(plan, &subquery.subquery, expr)?;
                    }
                    _ => {}
                };
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })
    .map(|_| ())
}
