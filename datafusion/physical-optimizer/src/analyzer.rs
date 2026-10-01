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

use crate::ensure_requirements::EnsureRequirements;
use crate::filter_pushdown::FilterPushdown;
use crate::join_selection::JoinSelection;
use crate::output_requirements::OutputRequirements;

// Re-export from this module for convenience.
pub use datafusion_session::PhysicalAnalyzerRule;

/// A rule-based physical analyzer.
///
/// Analyzer rules make the plan *valid*: they enforce the invariants every
/// operator declares (distribution, ordering) rather than making the plan
/// faster, mirroring the logical layer's `Analyzer`/`Optimizer` split. The
/// default planner runs the whole analyzer phase before the
/// [`PhysicalOptimizer`](crate::optimizer::PhysicalOptimizer) phase, so every
/// optimizer rule can assume it receives a valid plan; see
/// [`PhysicalAnalyzerRule`] for details.
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
        // The analyzer phase first runs the rules that decide what the plan
        // requires, then enforces those requirements, so enforcement runs once
        // on the plan's final shape. Everything after it is optimization.
        let rules: Vec<Arc<dyn PhysicalAnalyzerRule + Send + Sync>> = vec![
            // Establish the output-requirement boundary first, so enforcement can
            // see it (parallelize top-level scans below it, preserve the query's
            // final ordering). The matching remove pass runs late in the optimizer.
            Arc::new(OutputRequirements::new_add_mode()),
            // The rules that decide what the plan *requires* run before the
            // rules that materialise it. A hash join starts as
            // `PartitionMode::Auto`, which declares no distribution requirement
            // and cannot execute; resolving it here means enforcement sees the
            // join's real requirements. Pushing predicates into sources changes
            // the statistics the distribution decisions read, so that happens
            // here too. Neither rule has to re-enforce anything afterwards.
            Arc::new(JoinSelection::new_before_enforcement()),
            Arc::new(FilterPushdown::new()),
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

    /// The analyzer decides requirements first (join modes, pushed-down
    /// predicates), then enforces distribution and ordering, in that order.
    #[test]
    fn default_analyzer_decides_requirements_then_enforces_them() {
        let analyzer = PhysicalAnalyzer::new();
        let names: Vec<&str> = analyzer.rules.iter().map(|r| r.name()).collect();
        assert_eq!(
            names,
            vec![
                "OutputRequirements",
                "join_selection",
                "FilterPushdown",
                "EnsureRequirements"
            ]
        );
    }

    /// Enforcement (`EnsureRequirements`) lives in the analyzer; the optimizer
    /// keeps only the sort *optimizations* (`OptimizeSorts`), not enforcement.
    /// `WindowTopN` is the one optimizer rule that still disturbs requirements,
    /// and it re-establishes them itself.
    #[test]
    fn default_optimizer_has_no_enforcement_rules() {
        let names: Vec<String> = PhysicalOptimizer::new()
            .rules
            .iter()
            .map(|r| r.name().to_string())
            .collect();
        assert!(
            !names.iter().any(|n| n == "EnsureRequirements"),
            "EnsureRequirements should not be an optimizer rule, got {names:?}"
        );
        assert!(
            names.iter().any(|n| n == "OptimizeSorts"),
            "OptimizeSorts (the sort optimizations) should stay an optimizer rule, got {names:?}"
        );
    }

    /// The enforcement analyzer rule keeps its schema-check contract on.
    #[test]
    fn enforcement_rule_schema_check_is_on() {
        let analyzer = PhysicalAnalyzer::new();
        let rule = analyzer
            .rules
            .iter()
            .find(|r| r.name() == "EnsureRequirements")
            .expect("EnsureRequirements present in analyzer");
        assert!(rule.schema_check());
    }
}
