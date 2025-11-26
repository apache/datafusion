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

//! Physical optimizer traits

use std::fmt::Debug;
use std::sync::Arc;

use crate::aggregate_statistics::AggregateStatistics;
use crate::coalesce_batches::CoalesceBatches;
use crate::combine_partial_final_agg::CombinePartialFinalAggregate;
use crate::enforce_distribution::EnforceDistribution;
use crate::enforce_sorting::EnforceSorting;
use crate::ensure_coop::EnsureCooperative;
use crate::filter_pushdown::FilterPushdown;
use crate::join_selection::JoinSelection;
use crate::limit_pushdown::LimitPushdown;
use crate::limited_distinct_aggregation::LimitedDistinctAggregation;
use crate::output_requirements::OutputRequirements;
use crate::projection_pushdown::ProjectionPushdown;
use crate::sanity_checker::SanityCheckPlan;
use crate::topk_aggregation::TopKAggregation;
use crate::update_aggr_exprs::OptimizeAggregateOrder;

use crate::limit_pushdown_past_window::LimitPushPastWindows;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{internal_err, Result};
use datafusion_execution::config::SessionConfig;
use datafusion_physical_plan::ExecutionPlan;

/// Context for optimizing physical plans.
///
/// This context provides access to session configuration and optimizer extensions.
///
/// Similar to [`TaskContext`] which provides context during execution,
/// [`OptimizerContext`] provides context during optimization.
///
/// [`TaskContext`]: https://docs.rs/datafusion/latest/datafusion/execution/struct.TaskContext.html
#[derive(Debug, Clone)]
pub struct OptimizerContext {
    /// Session configuration
    session_config: SessionConfig,
}

impl OptimizerContext {
    /// Create a new OptimizerContext
    pub fn new(session_config: SessionConfig) -> Self {
        Self { session_config }
    }

    /// Return a reference to the session configuration
    pub fn session_config(&self) -> &SessionConfig {
        &self.session_config
    }
}

/// `PhysicalOptimizerRule` transforms one [`ExecutionPlan`] into another which
/// computes the same results, but in a potentially more efficient way.
///
/// Use [`SessionState::add_physical_optimizer_rule`] to register additional
/// `PhysicalOptimizerRule`s.
///
/// [`SessionState::add_physical_optimizer_rule`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html#method.add_physical_optimizer_rule
pub trait PhysicalOptimizerRule: Debug {
    /// Rewrite `plan` to an optimized form with additional context
    ///
    /// This is the preferred method for implementing optimization rules as it
    /// provides access to the full optimizer context including session configuration.
    ///
    /// The default implementation delegates to [`PhysicalOptimizerRule::optimize`] for
    /// backwards compatibility with existing implementations.
    ///
    /// New implementations should override this method instead of `optimize()`.
    ///
    /// Once [`PhysicalOptimizerRule::optimize`] is deprecated and removed, this
    /// default implementation will be removed and this method will become required.
    /// This change is scheduled for DataFusion 54.0.0.
    fn optimize_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &OptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Default implementation: delegate to the old method for backwards compatibility
        #[allow(deprecated)]
        self.optimize(plan, context.session_config().options())
    }

    /// Rewrite `plan` to an optimized form
    ///
    /// This method is kept for backwards compatibility. New implementations
    /// should implement [`optimize_plan`](Self::optimize_plan) instead, which
    /// provides access to additional context.
    ///
    /// The default implementation returns an error indicating that neither
    /// `optimize` nor `optimize_plan` was properly implemented. At least one
    /// of these methods must be overridden.
    #[deprecated(
        since = "52.0.0",
        note = "use `PhysicalOptimizerRule::optimize_plan` instead"
    )]
    fn optimize(
        &self,
        _plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        internal_err!(
            "PhysicalOptimizerRule '{}' must implement either optimize() or optimize_plan()",
            self.name()
        )
    }

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;

    /// A flag to indicate whether the physical planner should validate that the rule will not
    /// change the schema of the plan after the rewriting.
    /// Some of the optimization rules might change the nullable properties of the schema
    /// and should disable the schema check.
    fn schema_check(&self) -> bool;
}

/// A rule-based physical optimizer.
#[derive(Clone, Debug)]
pub struct PhysicalOptimizer {
    /// All rules to apply
    pub rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
}

impl Default for PhysicalOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizer {
    /// Create a new optimizer using the recommended list of rules
    pub fn new() -> Self {
        let rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = vec![
            // If there is a output requirement of the query, make sure that
            // this information is not lost across different rules during optimization.
            Arc::new(OutputRequirements::new_add_mode()),
            Arc::new(AggregateStatistics::new()),
            // Statistics-based join selection will change the Auto mode to a real join implementation,
            // like collect left, or hash join, or future sort merge join, which will influence the
            // EnforceDistribution and EnforceSorting rules as they decide whether to add additional
            // repartitioning and local sorting steps to meet distribution and ordering requirements.
            // Therefore, it should run before EnforceDistribution and EnforceSorting.
            Arc::new(JoinSelection::new()),
            // The LimitedDistinctAggregation rule should be applied before the EnforceDistribution rule,
            // as that rule may inject other operations in between the different AggregateExecs.
            // Applying the rule early means only directly-connected AggregateExecs must be examined.
            Arc::new(LimitedDistinctAggregation::new()),
            // The FilterPushdown rule tries to push down filters as far as it can.
            // For example, it will push down filtering from a `FilterExec` to `DataSourceExec`.
            // Note that this does not push down dynamic filters (such as those created by a `SortExec` operator in TopK mode),
            // those are handled by the later `FilterPushdown` rule.
            // See `FilterPushdownPhase` for more details.
            Arc::new(FilterPushdown::new()),
            // The EnforceDistribution rule is for adding essential repartitioning to satisfy distribution
            // requirements. Please make sure that the whole plan tree is determined before this rule.
            // This rule increases parallelism if doing so is beneficial to the physical plan; i.e. at
            // least one of the operators in the plan benefits from increased parallelism.
            Arc::new(EnforceDistribution::new()),
            // The CombinePartialFinalAggregate rule should be applied after the EnforceDistribution rule
            Arc::new(CombinePartialFinalAggregate::new()),
            // The EnforceSorting rule is for adding essential local sorting to satisfy the required
            // ordering. Please make sure that the whole plan tree is determined before this rule.
            // Note that one should always run this rule after running the EnforceDistribution rule
            // as the latter may break local sorting requirements.
            Arc::new(EnforceSorting::new()),
            // Run once after the local sorting requirement is changed
            Arc::new(OptimizeAggregateOrder::new()),
            // TODO: `try_embed_to_hash_join` in the ProjectionPushdown rule would be block by the CoalesceBatches, so add it before CoalesceBatches. Maybe optimize it in the future.
            Arc::new(ProjectionPushdown::new()),
            // The CoalesceBatches rule will not influence the distribution and ordering of the
            // whole plan tree. Therefore, to avoid influencing other rules, it should run last.
            Arc::new(CoalesceBatches::new()),
            // Remove the ancillary output requirement operator since we are done with the planning
            // phase.
            Arc::new(OutputRequirements::new_remove_mode()),
            // The aggregation limiter will try to find situations where the accumulator count
            // is not tied to the cardinality, i.e. when the output of the aggregation is passed
            // into an `order by max(x) limit y`. In this case it will copy the limit value down
            // to the aggregation, allowing it to use only y number of accumulators.
            Arc::new(TopKAggregation::new()),
            // Tries to push limits down through window functions, growing as appropriate
            // This can possibly be combined with [LimitPushdown]
            // It needs to come after [EnforceSorting]
            Arc::new(LimitPushPastWindows::new()),
            // The LimitPushdown rule tries to push limits down as far as possible,
            // replacing operators with fetching variants, or adding limits
            // past operators that support limit pushdown.
            Arc::new(LimitPushdown::new()),
            // The ProjectionPushdown rule tries to push projections towards
            // the sources in the execution plan. As a result of this process,
            // a projection can disappear if it reaches the source providers, and
            // sequential projections can merge into one. Even if these two cases
            // are not present, the load of executors such as join or union will be
            // reduced by narrowing their input tables.
            Arc::new(ProjectionPushdown::new()),
            Arc::new(EnsureCooperative::new()),
            // This FilterPushdown handles dynamic filters that may have references to the source ExecutionPlan.
            // Therefore it should be run at the end of the optimization process since any changes to the plan may break the dynamic filter's references.
            // See `FilterPushdownPhase` for more details.
            Arc::new(FilterPushdown::new_post_optimization()),
            // The SanityCheckPlan rule checks whether the order and
            // distribution requirements of each node in the plan
            // is satisfied. It will also reject non-runnable query
            // plans that use pipeline-breaking operators on infinite
            // input(s). The rule generates a diagnostic error
            // message for invalid plans. It makes no changes to the
            // given query plan; i.e. it only acts as a final
            // gatekeeping rule.
            Arc::new(SanityCheckPlan::new()),
        ];

        Self::with_rules(rules)
    }

    /// Create a new optimizer with the given rules
    pub fn with_rules(rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>) -> Self {
        Self { rules }
    }
}
