// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`PhysicalOptimizer`] for optimizing [`ExecutionPlan`]s
//!
//! [`ExecutionPlan`]: datafusion_physical_plan::ExecutionPlan

use std::fmt::Debug;
use std::sync::Arc;

use crate::combine_partial_final_agg::CombinePartialFinalAggregate;
use crate::ensure_coop::EnsureCooperative;
use crate::filter_pushdown::DynamicFilterPushdown;
use crate::limit_pushdown::LimitPushdown;
use crate::output_requirements::RemoveOutputRequirements;
use crate::projection_pushdown::ProjectionPushdown;
use crate::topk_aggregation::TopKAggregation;
use crate::topk_repartition::TopKRepartition;
use crate::update_aggr_exprs::OptimizeAggregateOrder;

use crate::hash_join_buffering::HashJoinBuffering;
use crate::limit_pushdown_past_window::LimitPushPastWindows;
use crate::pushdown_sort::PushdownSort;
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
/// The [`PhysicalAnalyzer`] phase runs before these rules and ends with
/// [`EnsureRequirements`], so the first rule already receives a plan that
/// meets every distribution and ordering requirement. Until
/// [`RemoveOutputRequirements`] runs, the plan also holds the
/// `OutputRequirementExec` markers that record the query's output ordering;
/// they are not executable.
///
/// [`PhysicalAnalyzer`]: crate::analyzer::PhysicalAnalyzer
/// [`EnsureRequirements`]: crate::ensure_requirements::EnsureRequirements
///
/// ## Invariants
///
/// The following properties hold for all rules (built-in and custom):
///
/// 1. **Correctness.** The returned plan must a) be valid (it can be executed
///    without internal errors, and it meets every distribution and ordering
///    requirement its operators declare, as [`check_plan_tree_sanity`] checks)
///    and b) produce the same results as the input plan. A rule may change the
///    operators in the plan, such as changing which operator implements a
///    join, but it may never change the query's semantics.
///
/// 2. **Reorderable.** Because each rule preserves correctness, running the
///    rules in a different order must still yield a valid plan that produces
///    the same results.
///
/// ### Exceptions
///
/// One built-in rule does not yet fully satisfy the invariants above:
///
/// - [`RemoveOutputRequirements`] drops the markers that record the
///   query's output requirements, so the rules after it no longer see them.
///   Rules that must keep the query's output ordering run before it.
///
/// Custom pipelines must keep this rule in a compatible relative order. We
/// are working to remove this exception, so that all rules can be reordered
/// freely.
///
/// After the last rule, including rules added by the user, the physical
/// planner rejects the plan if its requirements are not satisfied (see
/// [`check_plan_tree_sanity`]).
///
/// [`check_plan_tree_sanity`]: crate::sanity_checker::check_plan_tree_sanity
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
/// See <https://github.com/apache/datafusion/issues/26481> for more details.
///
/// If you do build a custom pipeline:
///
/// - **End a custom analyzer list with [`EnsureRequirements`],** so the
///   optimizer rules receive a valid plan.
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
            // The rules up to and including `EnsureRequirements` run in the
            // analyzer phase (see `PhysicalAnalyzer`), so every rule here
            // receives a plan whose requirements are already enforced.
            // The CombinePartialFinalAggregate rule should be applied after distribution enforcement
            Arc::new(CombinePartialFinalAggregate::new()),
            // Run once after the local sorting requirement is changed
            Arc::new(OptimizeAggregateOrder::new()),
            // TODO: `try_embed_to_hash_join` in the ProjectionPushdown rule would be block by the CoalesceBatches, so add it before CoalesceBatches. Maybe optimize it in the future.
            Arc::new(ProjectionPushdown::new()),
            // Remove the ancillary output requirement operator since we are done with the planning
            // phase.
            Arc::new(RemoveOutputRequirements::new()),
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
            Arc::new(DynamicFilterPushdown::new()),
            // There is no `SanityCheckPlan` rule at the end of this list. The
            // physical planner runs the same check on the final plan, after
            // every rule including the ones appended by the user. See
            // `crate::sanity_checker`.
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
