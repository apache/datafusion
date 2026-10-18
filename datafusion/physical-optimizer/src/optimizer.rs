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

//! [`PhysicalOptimizer`] for optimizing [`ExecutionPlan`]s
//!
//! [`ExecutionPlan`]: datafusion_physical_plan::ExecutionPlan

use std::fmt::Debug;
use std::sync::Arc;

use crate::aggregate_statistics::AggregateStatistics;
use crate::combine_partial_final_agg::CombinePartialFinalAggregate;
use crate::ensure_coop::EnsureCooperative;
use crate::ensure_requirements::EnsureRequirements;
use crate::filter_pushdown::FilterPushdown;
use crate::join_selection::JoinSelection;
use crate::limit_pushdown::LimitPushdown;
use crate::limited_distinct_aggregation::LimitedDistinctAggregation;
use crate::output_requirements::OutputRequirements;
use crate::projection_pushdown::ProjectionPushdown;
use crate::sanity_checker::SanityCheckPlan;
use crate::topk_aggregation::TopKAggregation;
use crate::topk_repartition::TopKRepartition;
use crate::update_aggr_exprs::OptimizeAggregateOrder;

use crate::hash_join_buffering::HashJoinBuffering;
use crate::limit_pushdown_past_window::LimitPushPastWindows;
use crate::pushdown_sort::PushdownSort;
use crate::window_topn::WindowTopN;
use datafusion_common::config::ConfigOptions;
use datafusion_physical_plan::statistics::StatisticsContext;

// Re-export from this module for backwards compatibility.
pub use datafusion_session::{PhysicalOptimizerContext, PhysicalOptimizerRule};

pub use datafusion_session::with_statistics_context;

/// Simple context wrapping [`ConfigOptions`] for backward compatibility.
///
/// This struct provides a minimal implementation of [`PhysicalOptimizerContext`]
/// that supplies configuration options and a [`StatisticsContext`] without a
/// statistics registry. Used when no statistics registry is available or
/// needed.
pub struct ConfigOnlyContext<'a> {
    config: &'a ConfigOptions,
    statistics_context: StatisticsContext,
}

impl<'a> ConfigOnlyContext<'a> {
    /// Create a new context wrapping the given config options.
    pub fn new(config: &'a ConfigOptions) -> Self {
        Self {
            config,
            // No providers, matching `statistics_registry()`, which is `None`
            statistics_context: StatisticsContext::new(),
        }
    }
}

impl PhysicalOptimizerContext for ConfigOnlyContext<'_> {
    fn config_options(&self) -> &ConfigOptions {
        self.config
    }

    fn statistics_context(&self) -> Option<&StatisticsContext> {
        Some(&self.statistics_context)
    }
}

/// A rule-based physical optimizer.
///
/// Applies a *Pipeline* (an ordered list) of [`PhysicalOptimizerRule`]s in a single pass.
///
/// # Physical Optimizer Contract
///
/// [`PhysicalOptimizer::new`] defines the default sequence of
/// [`PhysicalOptimizerRule`]s. The optimizer applies them in order, in a single
/// pass. This section describes the contract between the optimizer rules, both
/// for projects that extend or reorder the default rule list and for
/// contributors adding new built-in rules.
///
/// ## Invariants
///
/// The following properties hold for all rules (built-in and custom):
///
/// 1. **Correctness.** The returned plan must a) be valid (it can be executed
///    without internal errors) and b) produce the same results as the input
///    plan. A rule may change the operators in the plan, such as changing
///    which operator implements a join, but it may never change the query's
///    semantics.
///
/// 2. **Reorderable.** Because each rule preserves correctness, running the
///    rules in a different order must still yield a valid plan that produces
///    the same results.
///
/// ### Exceptions
///
/// Some built-in rules do not yet fully satisfy the invariants above because
/// they are responsible for making an initial plan *executable* rather than for
/// optimizing an existing plan:
///
/// - [`EnsureRequirements`] inserts the repartitioning and sorting needed to
///   satisfy the distribution and ordering requirements.
/// - [`OutputRequirements`] records output requirements.
/// - [`SanityCheckPlan`] rejects plans whose requirements are not satisfied.
///   It should run last.
///
/// Custom pipelines must keep these rules in a compatible relative order. We
/// are working to remove these exceptions, so that all rules can be reordered
/// freely.
///
/// ## Optimization Coverage Is Not Guaranteed
///
/// The invariants above guarantee *correctness*, not that every optimization is
/// applied. Built-in rules often match specific plan shapes that may only occur
/// at a certain point in the default pipeline. Adding, removing, or reordering
/// rules can change plan shapes, so a built-in rule may silently miss an
/// optimization that would apply in the default order. To keep their
/// implementations simple, built-in rules typically only recognize shapes that
/// the default pipeline produces.
///
/// For example, [`CombinePartialFinalAggregate`] runs after
/// [`EnsureRequirements`] and only merges a `Partial` aggregate that is the
/// direct child of a `Final` aggregate.
///
/// ```text
/// -- Canonical shape (default pipeline):
/// -- built-in rules recognize and optimize this
/// Limit
///   Aggregate(Final)
///     Aggregate(Partial)
///
/// -- Non-canonical shape (repartitioning inserted first):
/// -- built-in rules skip the optimization
/// Limit
///   Aggregate(Final)
///     Repartition
///       Aggregate(Partial)
/// ```
///
/// ## Guidance for Custom Pipelines
///
/// We recommend keeping the default rule order and using the supported
/// configuration options to disable individual optimizations. For example, `SET
/// datafusion.optimizer.enable_distinct_aggregation_soft_limit = false`
/// disables the distinct aggregation soft-limit optimization. These variations
/// are tested and preserve both correctness and optimization coverage.
///
/// If you do build a custom pipeline:
///
/// - **Reordering must still yield a valid plan.** Downstream projects are
///   encouraged to add tests that run their pipeline end to end and check
///   results, as DataFusion's own tests only cover the default order.
/// - **Test for reduced optimization coverage,** as described above, and verify
///   the plans you care about with `EXPLAIN`.
/// - **Prefer adding a rule over extending a built-in one.** We discourage
///   extending a built-in rule to match plan shapes it will not encounter in
///   the default pipeline, because such extensions are difficult to test and
///   maintain. Instead, add a custom rule that rewrites your plan into the
///   canonical shape recognized by the built-in rules. If extending a built-in
///   rule still seems necessary, please open a DataFusion issue to discuss it.
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
        // NOTEs:
        // - The order of rules in this list is important, as it determines the
        //   order in which they are applied.
        // - Adding a new rule here is expensive as it will be applied to all
        //   queries, and will likely increase the optimization time. Please extend
        //   existing rules when possible, rather than adding a new rule.
        let rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = vec![
            // If there is a output requirement of the query, make sure that
            // this information is not lost across different rules during optimization.
            Arc::new(OutputRequirements::new_add_mode()),
            Arc::new(AggregateStatistics::new()),
            // Statistics-based join selection will change the Auto mode to a real join implementation,
            // like collect left, or hash join, or future sort merge join, which will influence the
            // EnsureRequirements rule as it decides whether to add additional repartitioning and
            // local sorting steps to meet distribution and ordering requirements. Therefore, it
            // should run before EnsureRequirements.
            Arc::new(JoinSelection::new()),
            // The LimitedDistinctAggregation rule should be applied before EnsureRequirements,
            // as that rule may inject other operations in between the different AggregateExecs.
            // Applying the rule early means only directly-connected AggregateExecs must be examined.
            Arc::new(LimitedDistinctAggregation::new()),
            // The FilterPushdown rule tries to push down filters as far as it can.
            // For example, it will push down filtering from a `FilterExec` to `DataSourceExec`.
            // Note that this does not push down dynamic filters (such as those created by a `SortExec` operator in TopK mode),
            // those are handled by the later `FilterPushdown` rule.
            // See `FilterPushdownPhase` for more details.
            Arc::new(FilterPushdown::new()),
            // WindowTopN: replaces Filter(rn<=K) → Window(ROW_NUMBER)
            // with Window(ROW_NUMBER) → PartitionedTopKExec(fetch=K).
            // Must run before EnsureRequirements (so it can rewrite against the
            // window's declared ordering without pattern-matching a SortExec)
            // and before ProjectionPushdown (which embeds projections into FilterExec).
            Arc::new(WindowTopN::new()),
            // Ensures each input plan satisfies the distribution and ordering
            // requirements declared by `ExecutionPlan::required_input_distribution`
            // and `ExecutionPlan::required_input_ordering`.
            //
            // If the requirements are already satisfied, this rule leaves the plan
            // unchanged. For example, it does not add sorting when the input is a
            // file scan whose existing order already satisfies the required ordering.
            // Otherwise, this rule inserts the necessary repartitioning and sorting
            // operators.
            //
            // This used to be implemented as two separate rules: `EnforceDistribution`
            // and `EnforceSorting`. It is now a single idempotent rule that decides
            // distribution and sorting together in one bottom-up pass, so the
            // `pushdown_sorts` step no longer breaks distribution invariants set
            // earlier in the pipeline. See the module-level doc on
            // [`EnsureRequirements`](crate::ensure_requirements) for the per-phase
            // breakdown, and <https://github.com/apache/datafusion/issues/21973>
            // for the original failure mode.
            Arc::new(EnsureRequirements::new()),
            // The CombinePartialFinalAggregate rule should be applied after distribution enforcement
            Arc::new(CombinePartialFinalAggregate::new()),
            // Run once after the local sorting requirement is changed
            Arc::new(OptimizeAggregateOrder::new()),
            // TODO: `try_embed_to_hash_join` in the ProjectionPushdown rule would be block by the CoalesceBatches, so add it before CoalesceBatches. Maybe optimize it in the future.
            Arc::new(ProjectionPushdown::new()),
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
            // It needs to come after [EnsureRequirements] (which handles sort enforcement)
            Arc::new(LimitPushPastWindows::new()),
            // The HashJoinBuffering rule adds a BufferExec node with the configured capacity
            // in the prob side of hash joins. That way, the probe side gets eagerly polled before
            // the build side is completely finished.
            Arc::new(HashJoinBuffering::new()),
            // The LimitPushdown rule tries to push limits down as far as possible,
            // replacing operators with fetching variants, or adding limits
            // past operators that support limit pushdown.
            Arc::new(LimitPushdown::new()),
            // TopKRepartition pushes TopK (Sort with fetch) below Hash
            // repartition when the partition key is a prefix of the sort key.
            // This reduces data volume before a hash shuffle. It must run
            // after LimitPushdown so that the TopK already exists on the SortExec.
            Arc::new(TopKRepartition::new()),
            // The ProjectionPushdown rule tries to push projections towards
            // the sources in the execution plan. As a result of this process,
            // a projection can disappear if it reaches the source providers, and
            // sequential projections can merge into one. Even if these two cases
            // are not present, the load of executors such as join or union will be
            // reduced by narrowing their input tables.
            Arc::new(ProjectionPushdown::new()),
            // PushdownSort: Detect sorts that can be pushed down to data sources.
            Arc::new(PushdownSort::new()),
            Arc::new(EnsureCooperative::new()),
            // This FilterPushdown handles dynamic filters that may have references to the source ExecutionPlan.
            // Therefore, it should be run at the end of the optimization process since any changes to the plan may break the dynamic filter's references.
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
    ///
    /// See the [`PhysicalOptimizer`](Self) documentation for the contract
    /// governing non-default optimizer pipelines.
    pub fn with_rules(rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>) -> Self {
        Self { rules }
    }
}
