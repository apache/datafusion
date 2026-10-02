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

//! Physical analyzer

use std::sync::Arc;

use crate::aggregate_statistics::AggregateStatistics;
use crate::ensure_requirements::EnsureRequirements;
use crate::filter_pushdown::FilterPushdown;
use crate::join_selection::JoinSelection;
use crate::limited_distinct_aggregation::LimitedDistinctAggregation;
use crate::output_requirements::OutputRequirements;
use crate::window_topn::WindowTopN;

// Re-export from this module for convenience.
pub use datafusion_session::PhysicalAnalyzerRule;

/// A rule-based physical analyzer.
///
/// The analyzer phase runs before the
/// [`PhysicalOptimizer`](crate::optimizer::PhysicalOptimizer) phase and ends
/// with requirement enforcement, so every optimizer rule receives a valid plan.
/// See [`PhysicalAnalyzerRule`] for details.
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
        // This is the prefix of the former single optimizer list, up to and
        // including `EnsureRequirements`, in the same order, so the split into
        // two phases changes no plan. The rules ahead of `EnsureRequirements`
        // run here because enforcement has to see their output: `JoinSelection`
        // resolves `PartitionMode::Auto`, which declares no requirement; the
        // others rewrite aggregates and windows that enforcement then
        // repartitions and sorts. Moving the ones that are optimizations into
        // the optimizer phase is follow-up work.
        let rules: Vec<Arc<dyn PhysicalAnalyzerRule + Send + Sync>> = vec![
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
    use crate::optimizer::PhysicalOptimizer;

    /// The default analyzer is the former optimizer prefix, in the same order,
    /// ending with enforcement.
    #[test]
    fn default_analyzer_ends_with_enforcement() {
        let analyzer = PhysicalAnalyzer::new();
        let names: Vec<&str> = analyzer.rules.iter().map(|r| r.name()).collect();
        assert_eq!(
            names,
            vec![
                "OutputRequirements",
                "aggregate_statistics",
                "join_selection",
                "LimitedDistinctAggregation",
                "FilterPushdown",
                "WindowTopN",
                "EnsureRequirements",
            ]
        );
    }

    /// The rules that moved to the analyzer are not run a second time by the
    /// default optimizer. `OutputRequirements` is the exception by design: its
    /// add mode runs in the analyzer and its remove mode in the optimizer.
    #[test]
    fn default_optimizer_does_not_repeat_analyzer_rules() {
        let analyzer_names: Vec<String> = PhysicalAnalyzer::new()
            .rules
            .iter()
            .map(|r| r.name().to_string())
            .filter(|n| n != "OutputRequirements")
            .collect();
        let optimizer_names: Vec<String> = PhysicalOptimizer::new()
            .rules
            .iter()
            .map(|r| r.name().to_string())
            .collect();
        for name in analyzer_names {
            assert!(
                !optimizer_names.contains(&name),
                "{name} runs in the analyzer and must not repeat in the optimizer, got {optimizer_names:?}"
            );
        }
    }
}
