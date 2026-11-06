// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`Analyzer`] and [`AnalyzerRule`]

use std::fmt::Debug;
use std::sync::Arc;

use log::debug;

use datafusion_common::config::ConfigOptions;
use datafusion_common::instant::Instant;
use datafusion_common::{ColumnIdGenerator, Result};
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::{InvariantLevel, LogicalPlan};

use crate::analyzer::resolve_grouping_function::ResolveGroupingFunction;
use crate::analyzer::type_coercion::TypeCoercion;
use crate::column_id_check::ColumnIdCheck;
use crate::optimizer::RootNames;
use crate::utils::log_plan;

use self::function_rewrite::ApplyFunctionRewrites;

pub mod function_rewrite;
pub mod resolve_grouping_function;
pub mod type_coercion;

/// [`AnalyzerRule`]s transform [`LogicalPlan`]s in some way to make
/// the plan valid prior to the rest of the DataFusion optimization process.
///
/// `AnalyzerRule`s are different than an [`OptimizerRule`](crate::OptimizerRule)s
/// which must preserve the semantics of the `LogicalPlan`, while computing
/// results in a more optimal way.
///
/// For example, an `AnalyzerRule` may resolve [`Expr`](datafusion_expr::Expr)s into more specific
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

/// Rule-based Analyzer.
///
/// Applies [`FunctionRewrite`]s and [`AnalyzerRule`]s to transform a
/// [`LogicalPlan`] in preparation for execution.
///
/// For example, the `Analyzer` applies type coercion to ensure the types of
/// operands match the types required by functions.
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
            Arc::new(ResolveGroupingFunction::new()),
            Arc::new(TypeCoercion::new()),
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
        // verify the logical plan required invariants at the start, before analyzer
        plan.check_invariants(InvariantLevel::Always)
            .map_err(|e| e.context("Invalid input plan passed to Analyzer"))?;

        let start_time = Instant::now();
        // References written by name (a plan built with node constructors,
        // or by a planner that does not bind) are bound once, here: they get
        // the id of the field they resolve to by name in the scope of their
        // node. From here on, a reference with an id is looked up by id only.
        // A reference to a field without an id (a scan built without a
        // generator, an extension node that does not support column ids)
        // keeps resolving by name.
        let plan = plan
            .with_bound_column_references()
            .map_err(|e| e.context("Binding the column references before the analyzer"))?
            .data;
        let mut column_id_check = ColumnIdCheck::new(&plan);
        // Rules may rename the outputs they rewrite (names come from the
        // definitions); the names of the root (and of the input of a write)
        // are restored once, at the end
        let output_names = RootNames::of(&plan);
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
            new_plan = rule
                .analyze(new_plan, config)
                .map_err(|e| e.context(rule.name()))?;
            log_plan(rule.name(), &new_plan);
            observer(&new_plan, rule.as_ref());
        }

        // the root keeps the names it started with (analyzer rules have no
        // generator: a renamed output that stops being a pass-through gets
        // an id after the plan's)
        let generator = ColumnIdGenerator::new_after(new_plan.max_column_id());
        let restored = output_names
            .restore(new_plan, &generator)
            .map_err(|e| e.context("Restoring the output names after the analyzer"))?;
        new_plan = restored.data;
        if restored.transformed {
            // the references to renamed outputs (the expressions of a MERGE
            // INTO read the input of the write) follow
            new_plan = new_plan.with_relabeled_columns()?.data;
        }
        column_id_check.after_analyzer(&new_plan)?;

        // verify at the end, after the last LP analyzer pass, that the plan is executable.
        new_plan
            .check_invariants(InvariantLevel::Executable)
            .map_err(|e| e.context("Invalid (non-executable) plan after Analyzer"))?;

        log_plan("Final analyzed plan", &new_plan);
        debug!("Analyzer took {} ms", start_time.elapsed().as_millis());
        Ok(new_plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::test_table_scan;
    use datafusion_expr::logical_plan::Filter;
    use datafusion_expr::{col, lit};

    /// A reference written by name in a plan built with a node constructor
    /// gets the id of the column it reads when the analyzer starts
    #[test]
    fn binds_references_written_by_name() -> Result<()> {
        let scan = test_table_scan()?;
        let a_id = scan.schema().field_id(0);
        assert!(a_id.is_some(), "the test scan has ids");
        let plan =
            LogicalPlan::Filter(Filter::try_new(col("a").eq(lit(1u32)), Arc::new(scan))?);
        let analyzed =
            Analyzer::new().execute_and_check(plan, &ConfigOptions::new(), |_, _| {})?;
        let LogicalPlan::Filter(filter) = analyzed else {
            panic!("not a filter: {analyzed}");
        };
        let ids: Vec<_> = filter
            .predicate
            .column_refs()
            .into_iter()
            .map(|col| col.id)
            .collect();
        assert_eq!(ids, vec![a_id]);
        Ok(())
    }
}
