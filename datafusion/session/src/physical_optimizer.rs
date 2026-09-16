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

//! Physical optimizer interfaces.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::operator_statistics::StatisticsRegistry;

/// Context available to physical optimizer rules.
///
/// This trait provides access to configuration options and an optional statistics
/// registry for enhanced statistics lookup.
pub trait PhysicalOptimizerContext: Send + Sync {
    /// Returns the configuration options.
    fn config_options(&self) -> &ConfigOptions;

    /// Returns the statistics registry for enhanced statistics lookup.
    ///
    /// Returns `None` if no registry is configured, in which case rules
    /// should fall back to using [`ExecutionPlan::partition_statistics`].
    fn statistics_registry(&self) -> Option<&StatisticsRegistry> {
        None
    }
}

/// `PhysicalOptimizerRule` transforms one [`ExecutionPlan`] into another which
/// computes the same results, but in a potentially more efficient way.
///
/// Use [`SessionState::add_physical_optimizer_rule`] to register additional
/// `PhysicalOptimizerRule`s.
///
/// [`SessionState::add_physical_optimizer_rule`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html#method.add_physical_optimizer_rule
pub trait PhysicalOptimizerRule: Debug + std::any::Any {
    /// Rewrite `plan` to an optimized form.
    ///
    /// This is the primary optimization method. For rules that need access to
    /// the statistics registry, override [`optimize_with_context`](Self::optimize_with_context) instead.
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Rewrite `plan` with access to extended context (statistics registry, etc.).
    ///
    /// Override this method if you need access to the statistics registry for
    /// enhanced statistics lookup. The default implementation simply calls
    /// [`optimize`](Self::optimize) with the config options from the context.
    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize(plan, context.config_options())
    }

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;

    /// Whether this rule is a pure function of the plan it is given, so that
    /// running it again on a plan object it previously returned cannot change
    /// anything.
    ///
    /// When a rule opts in, the optimizer remembers the plan the rule last
    /// returned and skips the call when handed back that exact object. Plans
    /// are reference counted and a rule returns its input untouched when it
    /// has nothing to do, so this is an exact, allocation-free signal — no
    /// hashing or structural comparison is involved.
    ///
    /// This is off by default and only consulted when
    /// `datafusion.optimizer.skip_unchanged_physical_rules` is enabled. It
    /// pays off for rule lists that run the same rule more than once, which
    /// is common when downstream rewrites are inserted after the built-in
    /// requirement enforcement and each needs its requirements re-enforced.
    ///
    /// Leave this `false` for any rule whose output depends on state outside
    /// the plan — session state that can change between invocations,
    /// counters, randomness — since the same input would no longer imply the
    /// same output. Debug builds verify the claim: when a skip would fire,
    /// the rule is run anyway and the result is asserted to be unchanged.
    ///
    /// The optimizer reads this from the rule it holds, so a rule that runs
    /// *other* rules inside its own [`optimize`] must forward their answer —
    /// in practice `all()` over the rules it wraps, since the wrapper is only
    /// skippable if every rule it would have run is. A wrapper that leaves
    /// this at the default silently opts its inner rules out:
    ///
    /// ```text
    /// fn skip_if_unchanged(&self) -> bool {
    ///     self.wrapped.iter().all(|rule| rule.skip_if_unchanged())
    /// }
    /// ```
    ///
    /// [`optimize`]: PhysicalOptimizerRule::optimize
    fn skip_if_unchanged(&self) -> bool {
        false
    }

    /// A flag to indicate whether the physical planner should validate that the rule will not
    /// change the schema of the plan after the rewriting.
    /// Some of the optimization rules might change the nullable properties of the schema
    /// and should disable the schema check.
    fn schema_check(&self) -> bool;
}
