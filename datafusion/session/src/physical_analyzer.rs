// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Physical analyzer interfaces.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_physical_plan::ExecutionPlan;

use crate::physical_optimizer::PhysicalOptimizerContext;

/// A `PhysicalAnalyzerRule` runs before requirements are enforced. The
/// analyzer phase runs the rules that must see the plan while each
/// operator's [`ExecutionPlan::required_input_distribution`] and
/// [`ExecutionPlan::required_input_ordering`] are only declared, then
/// `EnsureRequirements`, which repartitions and sorts the inputs to meet them.
/// Every [`PhysicalOptimizerRule`] after it receives a plan that meets its
/// requirements and must return one.
///
/// A rule belongs in the analyzer when `EnsureRequirements` reads what the
/// rule changes (a requirement, an ordering, or statistics), as with the join
/// mode `join_selection` picks, or when the rule needs the plan before
/// enforcement adds repartitions and sorts, as with `WindowTopN`. A rule that
/// reads what enforcement produced, such as `LimitPushdown`, is a
/// [`PhysicalOptimizerRule`]. Both phases hold
/// performance rules; what differs is the plan each one receives. A rule is
/// one or the other: moving it to the other phase means implementing the
/// other trait.
///
/// An analyzer rule must:
///
/// - Keep the query's results unchanged. It may leave a distribution or
///   ordering requirement unmet; `EnsureRequirements` at the end of the phase
///   meets it.
/// - Stay correct when the plan already contains repartitions, sorts, or
///   operators it does not know, as happens when a plan is optimized again.
///   It may skip its rewrite there, and decides what is safe around an
///   operator only from what the operator declares.
///
/// # Where analyzer rules run
///
/// [`DefaultPhysicalPlanner::optimize_physical_plan`] runs every analyzer rule,
/// then every optimizer rule. A rule appended to the analyzer list runs after
/// `EnsureRequirements`, so it must leave the plan valid.
///
/// ```text
/// ExecutionPlan  (every operator declares its distribution and ordering
///                 requirements; nothing satisfies them yet)
///                                │
///                                ▼
/// ┌─ PhysicalAnalyzer phase ── before enforcement ───────────────┐
/// │ rules that must run before EnsureRequirements                │
/// │ EnsureRequirements: enforce distribution, ordering           │
/// └──────────────────────────────┬───────────────────────────────┘
///                                │  every requirement is met
///                                ▼
/// ┌─ PhysicalOptimizer phase ── after enforcement ───────────────┐
/// │ every rule: valid plan in, valid plan out                    │
/// └──────────────────────────────┬───────────────────────────────┘
///                                ▼
///                   valid, optimized ExecutionPlan
/// ```
///
/// [`DefaultPhysicalPlanner::optimize_physical_plan`]: https://docs.rs/datafusion/latest/datafusion/physical_planner/struct.DefaultPhysicalPlanner.html#method.optimize_physical_plan
///
/// [`PhysicalOptimizerRule`]: crate::physical_optimizer::PhysicalOptimizerRule
/// [`ExecutionPlan::required_input_distribution`]: datafusion_physical_plan::ExecutionPlan::required_input_distribution
/// [`ExecutionPlan::required_input_ordering`]: datafusion_physical_plan::ExecutionPlan::required_input_ordering
pub trait PhysicalAnalyzerRule: Debug + std::any::Any {
    /// Rewrite `plan`. See the trait docs for what the result must keep.
    ///
    /// This is the primary method. Rules that need access to the statistics
    /// registry should override [`analyze_with_context`](Self::analyze_with_context)
    /// instead.
    fn analyze(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Rewrite `plan` with access to extended context (statistics registry, etc.).
    ///
    /// The default implementation calls [`analyze`](Self::analyze) with the
    /// config options from the context. This mirrors
    /// [`PhysicalOptimizerRule::optimize_with_context`], so analyzer rules
    /// have the same statistics-registry access as optimizer rules.
    ///
    /// [`PhysicalOptimizerRule::optimize_with_context`]: crate::physical_optimizer::PhysicalOptimizerRule::optimize_with_context
    fn analyze_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.analyze(plan, context.config_options())
    }

    /// A human readable name for this analyzer rule.
    fn name(&self) -> &str;

    /// A flag to indicate whether the physical planner should validate that
    /// the rule will not change the schema of the plan after the rewrite.
    ///
    /// This mirrors [`PhysicalOptimizerRule::schema_check`]; the default is
    /// `true`.
    ///
    /// [`PhysicalOptimizerRule::schema_check`]: crate::physical_optimizer::PhysicalOptimizerRule::schema_check
    fn schema_check(&self) -> bool {
        true
    }
}
