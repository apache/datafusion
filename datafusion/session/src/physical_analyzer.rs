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

//! Physical analyzer interfaces.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_physical_plan::ExecutionPlan;

use crate::physical_optimizer::PhysicalOptimizerContext;

/// A `PhysicalAnalyzerRule` transforms an [`ExecutionPlan`] to make the plan
/// *valid* prior to the rest of the DataFusion physical optimization process.
///
/// `PhysicalAnalyzerRule`s are different from [`PhysicalOptimizerRule`]s: an
/// optimizer rule must preserve the semantics of an already-valid plan while
/// computing the same results in a more efficient way, whereas an analyzer
/// rule is what *establishes* those semantics by satisfying the invariants
/// every operator declares.
///
/// For example, an analyzer rule may repartition an [`ExecutionPlan`]'s input
/// to match [`ExecutionPlan::required_input_distribution`] or insert a
/// `SortExec` to match [`ExecutionPlan::required_input_ordering`].
///
/// This mirrors the logical layer's split between [`AnalyzerRule`] (make the
/// plan valid) and [`OptimizerRule`] (make the plan faster).
///
/// # Ordering
///
/// Analyzer rules run as their own phase, before the optimizer rules, in
/// [`DefaultPhysicalPlanner::optimize_physical_plan`]: every analyzer rule runs,
/// then every optimizer rule. Within the analyzer, the rules that *decide* what
/// the plan requires run first (a join's partition mode, which predicates reach
/// a source), and requirement enforcement (distribution, ordering) runs last on
/// that final shape. An optimizer rule can therefore assume it receives a valid
/// plan. The one optimizer rule that still changes a requirement, `WindowTopN`,
/// re-establishes validity itself.
///
/// ```text
/// ExecutionPlan  (every operator declares its distribution and ordering
///                 requirements; nothing satisfies them yet)
///                                │
///                                ▼
/// ┌─ PhysicalAnalyzer phase ── make the plan VALID ──────────────┐
/// │ 1. OutputRequirements (add)   establish the output boundary  │
/// │ 2. JoinSelection              resolve PartitionMode::Auto    │
/// │ 3. FilterPushdown             push predicates into sources   │
/// │ 4. EnforceDistribution        required_input_distribution    │
/// │ 5. EnforceSorting             required_input_ordering        │
/// └──────────────────────────────┬───────────────────────────────┘
///                                │  invariant: the plan is valid
///                                ▼
/// ┌─ PhysicalOptimizer phase ── make the plan FASTER ────────────┐
/// │ every rule receives a valid plan and leaves a valid plan     │
/// │                                                              │
/// │ WindowTopN  ───▶ re-enforce distribution + ordering          │
/// └──────────────────────────────┬───────────────────────────────┘
///                                ▼
///                   valid, optimized ExecutionPlan
/// ```
///
/// [`DefaultPhysicalPlanner::optimize_physical_plan`]: https://docs.rs/datafusion/latest/datafusion/physical_planner/struct.DefaultPhysicalPlanner.html#method.optimize_physical_plan
/// [`AnalyzerRule`]: https://docs.rs/datafusion/latest/datafusion/optimizer/analyzer/trait.AnalyzerRule.html
/// [`OptimizerRule`]: https://docs.rs/datafusion/latest/datafusion/optimizer/trait.OptimizerRule.html
///
/// [`PhysicalOptimizerRule`]: crate::physical_optimizer::PhysicalOptimizerRule
/// [`ExecutionPlan::required_input_distribution`]: datafusion_physical_plan::ExecutionPlan::required_input_distribution
/// [`ExecutionPlan::required_input_ordering`]: datafusion_physical_plan::ExecutionPlan::required_input_ordering
pub trait PhysicalAnalyzerRule: Debug + std::any::Any {
    /// Rewrite `plan` so that it satisfies the invariants this rule enforces.
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
    /// [`PhysicalOptimizerRule::optimize_with_context`], so enforcement passes
    /// keep the same statistics-registry access they had while they were
    /// optimizer rules.
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
    /// This mirrors [`PhysicalOptimizerRule::schema_check`]; enforcement passes
    /// preserve the schema, so the default is `true`.
    ///
    /// [`PhysicalOptimizerRule::schema_check`]: crate::physical_optimizer::PhysicalOptimizerRule::schema_check
    fn schema_check(&self) -> bool {
        true
    }
}
