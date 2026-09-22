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

    /// A flag to indicate whether the physical planner should validate that the rule will not
    /// change the schema of the plan after the rewriting.
    /// Some of the optimization rules might change the nullable properties of the schema
    /// and should disable the schema check.
    ///
    /// The planner reads this from the rule it holds, so a rule that runs
    /// *other* rules inside its own [`optimize`] must account for what the
    /// whole wrapped transformation does to the schema, not just its own body:
    ///
    /// * A wrapper around a single rule should forward that rule's value.
    /// * A wrapper that runs multiple rules should return `true` only if the
    ///   complete wrapped transformation preserves the schema contract. When
    ///   that is derived solely from the wrapped rules, every wrapped rule must
    ///   enable the check (`all()`, not `any()`): if any wrapped rule is allowed
    ///   to change the schema, returning `true` makes the checker assert a
    ///   change it should tolerate.
    /// * A wrapper mixing schema-preserving and schema-changing rules cannot be
    ///   expressed as a single flag; validate per-rule inside the wrapper so the
    ///   inner checks are retained.
    ///
    /// When the flag can be derived from the wrapped rules, `all()` (not
    /// `any()`) is the safe combinator:
    ///
    /// ```text
    /// fn schema_check(&self) -> bool {
    ///     self.wrapped.iter().all(|rule| rule.schema_check())
    /// }
    /// ```
    ///
    /// Returning a blanket `false` still silently disables validation for
    /// everything inside, including rules that asked for it.
    ///
    /// [`optimize`]: PhysicalOptimizerRule::optimize
    fn schema_check(&self) -> bool;

    /// Whether this rule may be applied to its own output, i.e. whether it is
    /// idempotent: an application to a plan it has already settled must
    /// change nothing.
    ///
    /// The optimizer runs a rule that returns `true` to convergence at each
    /// of its call sites, re-applying it until the plan's signature repeats
    /// (fixpoint or cycle) or `max_passes` is reached. A rule that returns
    /// `false`, the default, runs exactly once per call site: how often it
    /// runs stays entirely a property of the chain that scheduled it.
    ///
    /// Only declare this for rules whose specification promises it.
    /// Enforcement passes qualify by definition: enforcing requirements on a
    /// plan that already satisfies them must be a no-op. Most optimizations
    /// do not: a rewrite whose trigger pattern survives in its own output
    /// re-fires on re-application (a pushdown that leaves the original
    /// operator in place will push the same thing again).
    fn idempotent(&self) -> bool {
        false
    }
}
