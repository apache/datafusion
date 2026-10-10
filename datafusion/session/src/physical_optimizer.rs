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

use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, Statistics};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::operator_statistics::StatisticsRegistry;
use datafusion_physical_plan::statistics::{StatisticsArgs, StatisticsContext};

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

    /// Returns a [`StatisticsContext`] shared by every rule of one optimizer
    /// run, so statistics computed by one rule are reused by later rules.
    ///
    /// The context must be built from [`Self::statistics_registry`]. Its cache
    /// entries hold the plan nodes they were computed for, so it is safe to
    /// share across plan rewrites (see [`StatisticsContext`]).
    ///
    /// Returns `None` if no shared context is available, in which case rules
    /// create their own context.
    fn statistics_context(&self) -> Option<&StatisticsContext> {
        None
    }

    /// Computes the statistics of `plan` with [`Self::statistics_context`],
    /// or, if it is `None`, with a new context built from
    /// [`Self::statistics_registry`].
    ///
    /// Rules that compute statistics should use this, so they reuse the
    /// statistics computed by earlier rules and consult the same statistics
    /// providers as the built-in rules.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_schema::Schema;
    /// # use datafusion_common::config::ConfigOptions;
    /// # use datafusion_physical_plan::ExecutionPlan;
    /// # use datafusion_physical_plan::empty::EmptyExec;
    /// # use datafusion_physical_plan::statistics::StatisticsArgs;
    /// # use datafusion_session::PhysicalOptimizerContext;
    /// # struct MyContext(ConfigOptions);
    /// # impl PhysicalOptimizerContext for MyContext {
    /// #     fn config_options(&self) -> &ConfigOptions {
    /// #         &self.0
    /// #     }
    /// # }
    /// # let context = MyContext(ConfigOptions::new());
    /// let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
    /// let statistics = context.compute_statistics(&plan, &StatisticsArgs::new())?;
    /// # Ok::<(), datafusion_common::DataFusionError>(())
    /// ```
    fn compute_statistics(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        with_statistics_context(self, |stats_ctx| stats_ctx.compute_arc(plan, args))
    }
}

/// Calls `f` with [`PhysicalOptimizerContext::statistics_context`], or, if it
/// is `None`, with a new context built from
/// [`PhysicalOptimizerContext::statistics_registry`].
///
/// Use this to pass one [`StatisticsContext`] down a traversal. To compute
/// the statistics of a single plan, use
/// [`PhysicalOptimizerContext::compute_statistics`].
pub fn with_statistics_context<C, R>(
    context: &C,
    f: impl FnOnce(&StatisticsContext) -> R,
) -> R
where
    C: PhysicalOptimizerContext + ?Sized,
{
    if let Some(shared) = context.statistics_context() {
        return f(shared);
    }
    let local = match context.statistics_registry() {
        Some(registry) => StatisticsContext::new_with_registry(registry.clone()),
        None => StatisticsContext::new(),
    };
    f(&local)
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
}
