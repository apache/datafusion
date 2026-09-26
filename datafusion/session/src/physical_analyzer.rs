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
/// This mirrors the logical layer's split between `AnalyzerRule` (make the
/// plan valid) and `OptimizerRule` (make the plan faster).
///
/// # Ordering
///
/// Analyzer rules run as their own phase, conceptually before the optimizer
/// rules that assume a valid plan. Note that the built-in planner does not
/// literally run every analyzer before every optimizer: to preserve the
/// hand-tuned order of the default pipeline it runs the analyzer phase at the
/// position `EnsureRequirements` historically occupied (see
/// [`DefaultPhysicalPlanner::optimize_physical_plan`]), because some default
/// optimizer rules (notably join selection) must run before enforcement. A
/// custom rule should therefore not assume it sees the raw initial plan, only
/// that requirement enforcement has not yet run when it does.
///
/// [`DefaultPhysicalPlanner::optimize_physical_plan`]: https://docs.rs/datafusion/latest/datafusion/physical_planner/struct.DefaultPhysicalPlanner.html#method.optimize_physical_plan
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
