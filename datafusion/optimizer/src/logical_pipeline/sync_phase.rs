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

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::{InvariantLevel, LogicalPlan, assert_expected_schema};

use crate::analyzer::AnalyzerRule;
use crate::analyzer::function_rewrite::ApplyFunctionRewrites;
use crate::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use crate::plan_signature::LogicalPlanSignature;

use super::Strategy;

/// A named, activatable phase whose rules are applied synchronously.
///
/// Use [`SyncOptimizationPhase`] for [`OptimizerRule`]s.
/// For analysis rules use [`SyncAnalysisPhase`], which is a concrete struct
/// rather than a type alias so it can carry its own `function_rewrites` field
/// without polluting this generic type.
pub struct SyncPhase<T: ?Sized> {
    pub(super) name: String,
    pub enabled: bool,
    pub strategy: Strategy,
    pub rules: Vec<Arc<T>>,
}

impl<T: ?Sized> Clone for SyncPhase<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            enabled: self.enabled,
            strategy: self.strategy.clone(),
            rules: self.rules.clone(),
        }
    }
}

impl<T: ?Sized + Debug> Debug for SyncPhase<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncPhase")
            .field("name", &self.name)
            .field("enabled", &self.enabled)
            .field("strategy", &self.strategy)
            .field("rules", &self.rules)
            .finish()
    }
}

impl<T: ?Sized> SyncPhase<T> {
    pub fn new(name: impl Into<String>, strategy: Strategy) -> Self {
        Self {
            name: name.into(),
            enabled: true,
            strategy,
            rules: Vec::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Synchronous analysis phase: runs [`AnalyzerRule`]s and optional
/// [`FunctionRewrite`]s in order.
///
/// This is a concrete struct rather than a type alias over [`SyncPhase`] so
/// that `function_rewrites` lives here exclusively and does not appear on the
/// generic [`SyncPhase<T>`] (which is also used for optimization rules that
/// have no need for function rewrites).
#[derive(Clone, Debug)]
pub struct SyncAnalysisPhase {
    pub(super) name: String,
    pub enabled: bool,
    pub strategy: Strategy,
    pub rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    /// [`FunctionRewrite`]s prepended before analysis rules each pass so that
    /// `TypeCoercion` operates on already-rewritten expressions.
    pub function_rewrites: Vec<Arc<dyn FunctionRewrite + Send + Sync>>,
}

impl SyncAnalysisPhase {
    pub fn new(name: impl Into<String>, strategy: Strategy) -> Self {
        Self {
            name: name.into(),
            enabled: true,
            strategy,
            rules: Vec::new(),
            function_rewrites: Vec::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Apply all analysis rules in order, repeating per [`Strategy`].
    ///
    /// Checks plan invariants before and after applying rules.
    /// [`FunctionRewrite`]s run first each pass so that `TypeCoercion`
    /// operates on already-rewritten expressions.
    pub fn apply(
        &self,
        plan: LogicalPlan,
        config: &ConfigOptions,
    ) -> Result<LogicalPlan> {
        if !self.enabled || (self.rules.is_empty() && self.function_rewrites.is_empty()) {
            return Ok(plan);
        }

        plan.check_invariants(InvariantLevel::Always)
            .map_err(|e| e.context("Invalid input plan passed to analysis phase"))?;

        let passes = match &self.strategy {
            Strategy::Once => 1,
            Strategy::FixedPoint { max_passes } => max_passes.unwrap_or(8),
        };

        let fn_rewrite_rule: Option<Arc<dyn AnalyzerRule + Send + Sync>> =
            if self.function_rewrites.is_empty() {
                None
            } else {
                Some(Arc::new(ApplyFunctionRewrites::new(
                    self.function_rewrites.clone(),
                )))
            };

        let mut plan = plan;
        let mut previous_plans = HashSet::with_capacity(16);
        previous_plans.insert(LogicalPlanSignature::new(&plan));

        'outer: for _ in 0..passes {
            if let Some(rule) = &fn_rewrite_rule {
                plan = rule.analyze(plan, config)?;
            }
            for rule in &self.rules {
                plan = rule.analyze(plan, config)?;
            }
            if !previous_plans.insert(LogicalPlanSignature::new(&plan)) {
                break 'outer;
            }
        }

        plan.check_invariants(InvariantLevel::Executable)
            .map_err(|e| e.context("Invalid plan after analysis phase"))?;

        Ok(plan)
    }

    /// Like [`Self::apply`] but calls `observer` after each rule for EXPLAIN output.
    pub fn apply_observed(
        &self,
        plan: LogicalPlan,
        config: &ConfigOptions,
        observer: &mut dyn FnMut(&LogicalPlan, &str),
    ) -> Result<LogicalPlan> {
        if !self.enabled || (self.rules.is_empty() && self.function_rewrites.is_empty()) {
            return Ok(plan);
        }

        plan.check_invariants(InvariantLevel::Always)
            .map_err(|e| e.context("Invalid input plan passed to analysis phase"))?;

        let passes = match &self.strategy {
            Strategy::Once => 1,
            Strategy::FixedPoint { max_passes } => max_passes.unwrap_or(8),
        };

        let fn_rewrite_rule: Option<Arc<dyn AnalyzerRule + Send + Sync>> =
            if self.function_rewrites.is_empty() {
                None
            } else {
                Some(Arc::new(ApplyFunctionRewrites::new(
                    self.function_rewrites.clone(),
                )))
            };

        let mut plan = plan;
        let mut previous_plans = HashSet::with_capacity(16);
        previous_plans.insert(LogicalPlanSignature::new(&plan));

        'outer: for _ in 0..passes {
            if let Some(rule) = &fn_rewrite_rule {
                plan = rule.analyze(plan, config)?;
                observer(&plan, rule.name());
            }
            for rule in &self.rules {
                plan = rule.analyze(plan, config)?;
                observer(&plan, rule.name());
            }
            if !previous_plans.insert(LogicalPlanSignature::new(&plan)) {
                break 'outer;
            }
        }

        plan.check_invariants(InvariantLevel::Executable)
            .map_err(|e| e.context("Invalid plan after analysis phase"))?;

        Ok(plan)
    }
}

impl SyncPhase<dyn OptimizerRule + Send + Sync> {
    /// Apply all optimization rules in order, respecting [`ApplyOrder`] recursion.
    ///
    /// Uses [`LogicalPlanSignature`] to detect fixpoint convergence early.
    /// Checks plan invariants before the first pass and schema invariants after
    /// each rule.
    pub fn apply(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<LogicalPlan> {
        if !self.enabled || self.rules.is_empty() {
            return Ok(plan);
        }

        plan.check_invariants(InvariantLevel::Executable)
            .map_err(|e| e.context("Invalid input plan passed to optimization phase"))?;

        let passes = match &self.strategy {
            Strategy::Once => 1,
            Strategy::FixedPoint { max_passes } => {
                max_passes.unwrap_or_else(|| config.options().optimizer.max_passes)
            }
        };

        let mut plan = plan;
        let starting_schema = Arc::clone(plan.schema());
        let mut previous_plans = HashSet::with_capacity(16);
        previous_plans.insert(LogicalPlanSignature::new(&plan));

        'outer: for _ in 0..passes {
            for rule in &self.rules {
                let rule_starting_schema = Arc::clone(plan.schema());
                let result: Transformed<LogicalPlan> = match rule.apply_order() {
                    Some(order) => plan.rewrite_with_subqueries(
                        &mut RuleRewriter::new(order, rule.as_ref(), config),
                    )?,
                    None => rule.rewrite(plan, config)?,
                };
                plan = result.data;
                assert_expected_schema(&rule_starting_schema, &plan).map_err(|e| {
                    e.context(format!(
                        "Optimizer rule '{}' changed the schema",
                        rule.name()
                    ))
                })?;
                #[cfg(debug_assertions)]
                plan.check_invariants(InvariantLevel::Executable)
                    .map_err(|e| {
                        e.context(format!(
                            "Invalid plan after optimizer rule '{}'",
                            rule.name()
                        ))
                    })?;
            }
            let plan_is_fresh = previous_plans.insert(LogicalPlanSignature::new(&plan));
            if !plan_is_fresh {
                break 'outer;
            }
        }

        assert_expected_schema(&starting_schema, &plan)
            .map_err(|e| e.context("Optimizer changed top-level schema"))?;

        Ok(plan)
    }

    /// Like [`Self::apply`] but calls `observer` after each rule for EXPLAIN output.
    pub fn apply_observed(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
        observer: &mut dyn FnMut(&LogicalPlan, &str),
    ) -> Result<LogicalPlan> {
        if !self.enabled || self.rules.is_empty() {
            return Ok(plan);
        }

        plan.check_invariants(InvariantLevel::Executable)
            .map_err(|e| e.context("Invalid input plan passed to optimization phase"))?;

        let passes = match &self.strategy {
            Strategy::Once => 1,
            Strategy::FixedPoint { max_passes } => {
                max_passes.unwrap_or_else(|| config.options().optimizer.max_passes)
            }
        };

        let mut plan = plan;
        let starting_schema = Arc::clone(plan.schema());
        let mut previous_plans = HashSet::with_capacity(16);
        previous_plans.insert(LogicalPlanSignature::new(&plan));

        'outer: for _ in 0..passes {
            for rule in &self.rules {
                let rule_starting_schema = Arc::clone(plan.schema());
                let result: Transformed<LogicalPlan> = match rule.apply_order() {
                    Some(order) => plan.rewrite_with_subqueries(
                        &mut RuleRewriter::new(order, rule.as_ref(), config),
                    )?,
                    None => rule.rewrite(plan, config)?,
                };
                plan = result.data;
                assert_expected_schema(&rule_starting_schema, &plan).map_err(|e| {
                    e.context(format!(
                        "Optimizer rule '{}' changed the schema",
                        rule.name()
                    ))
                })?;
                #[cfg(debug_assertions)]
                plan.check_invariants(InvariantLevel::Executable)
                    .map_err(|e| {
                        e.context(format!(
                            "Invalid plan after optimizer rule '{}'",
                            rule.name()
                        ))
                    })?;
                observer(&plan, rule.name());
            }
            let plan_is_fresh = previous_plans.insert(LogicalPlanSignature::new(&plan));
            if !plan_is_fresh {
                break 'outer;
            }
        }

        assert_expected_schema(&starting_schema, &plan)
            .map_err(|e| e.context("Optimizer changed top-level schema"))?;

        Ok(plan)
    }
}

pub type SyncOptimizationPhase = SyncPhase<dyn OptimizerRule + Send + Sync>;

/// [`TreeNodeRewriter`] wrapper that applies a single [`OptimizerRule`] with
/// the correct traversal order. Mirrors the `Rewriter` in `optimizer.rs`.
struct RuleRewriter<'a> {
    apply_order: ApplyOrder,
    rule: &'a dyn OptimizerRule,
    config: &'a dyn OptimizerConfig,
}

impl<'a> RuleRewriter<'a> {
    fn new(
        apply_order: ApplyOrder,
        rule: &'a dyn OptimizerRule,
        config: &'a dyn OptimizerConfig,
    ) -> Self {
        Self {
            apply_order,
            rule,
            config,
        }
    }
}

impl TreeNodeRewriter for RuleRewriter<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if self.apply_order == ApplyOrder::TopDown {
            self.rule.rewrite(node, self.config)
        } else {
            Ok(Transformed::no(node))
        }
    }

    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if self.apply_order == ApplyOrder::BottomUp {
            self.rule.rewrite(node, self.config)
        } else {
            Ok(Transformed::no(node))
        }
    }
}
