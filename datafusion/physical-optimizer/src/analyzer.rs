// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Physical analyzer

use std::sync::Arc;

use crate::aggregate_statistics::AggregateStatistics;
use crate::eliminate_constant_join::EliminateConstantJoin;
use crate::ensure_requirements::EnsureRequirements;
use crate::filter_pushdown::FilterPushdown;
use crate::join_selection::JoinSelection;
use crate::limited_distinct_aggregation::LimitedDistinctAggregation;
use crate::output_requirements::AddOutputRequirements;
use crate::window_topn::WindowTopN;

// Re-export from this module for convenience.
pub use datafusion_session::PhysicalAnalyzerRule;

/// A rule-based physical analyzer.
///
/// The analyzer phase runs the rules that must see the plan before its
/// distribution and ordering requirements are enforced, then
/// `EnsureRequirements`, so every rule of the
/// [`PhysicalOptimizer`](crate::optimizer::PhysicalOptimizer) phase receives a
/// plan that meets them. See [`PhysicalAnalyzerRule`] for details.
#[derive(Clone, Debug)]
pub struct PhysicalAnalyzer {
    /// All rules to apply
    pub rules: Vec<Arc<dyn PhysicalAnalyzerRule + Send + Sync>>,
}

impl Default for PhysicalAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalAnalyzer {
    /// Create a new analyzer using the recommended list of rules
    pub fn new() -> Self {
        // `AddOutputRequirements` records the query's output ordering and
        // distribution for `EnsureRequirements`, which meets every requirement.
        // The others are performance rules whose results `EnsureRequirements`
        // reads, or that need the plan before enforcement changes it.
        let rules: Vec<Arc<dyn PhysicalAnalyzerRule + Send + Sync>> = vec![
            // If there is a output requirement of the query, make sure that
            // this information is not lost across different rules during optimization.
            Arc::new(AddOutputRequirements::new()),
            Arc::new(AggregateStatistics::new()),
            // Runs after AggregateStatistics, which can fold a scalar subquery into a
            // row of literals, and before EnsureRequirements, so that the filter
            // replacing a join against such a row keeps the other input's ordering.
            // Also runs before FilterPushdown, which pushes that filter into the scan.
            Arc::new(EliminateConstantJoin::new()),
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
            // those are handled by `DynamicFilterPushdown` in the optimizer list.
            // See `FilterPushdownPhase` for more details.
            Arc::new(FilterPushdown::new()),
            // WindowTopN: replaces Filter(rn<=K) → Window(ROW_NUMBER) with
            // PartitionedTopKExec(fetch=K), which emits `rn` itself; the
            // window node is kept above it only when it has other window
            // expressions to compute.
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
        ];
        Self::with_rules(rules)
    }

    /// Create a new analyzer with the given rules
    pub fn with_rules(rules: Vec<Arc<dyn PhysicalAnalyzerRule + Send + Sync>>) -> Self {
        Self { rules }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The default analyzer is the former optimizer prefix, in the same order,
    /// ending with enforcement.
    #[test]
    fn default_analyzer_ends_with_enforcement() {
        let analyzer = PhysicalAnalyzer::new();
        let names: Vec<&str> = analyzer.rules.iter().map(|r| r.name()).collect();
        assert_eq!(
            names,
            vec![
                "AddOutputRequirements",
                "aggregate_statistics",
                "EliminateConstantJoin",
                "join_selection",
                "LimitedDistinctAggregation",
                "FilterPushdown",
                "WindowTopN",
                "EnsureRequirements",
            ]
        );
    }
}
