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

use crate::ensure_requirements::{EnforceDistribution, EnforceSorting};
use crate::output_requirements::OutputRequirements;

// Re-export from this module for convenience.
pub use datafusion_session::PhysicalAnalyzerRule;

/// A rule-based physical analyzer.
///
/// Analyzer rules make the plan *valid*: they enforce the invariants every
/// operator declares (distribution, ordering) rather than making the plan
/// faster, mirroring the logical layer's `Analyzer`/`Optimizer` split. They run
/// as a distinct phase relative to the [`PhysicalOptimizer`](crate::optimizer::PhysicalOptimizer)
/// rules; see [`PhysicalAnalyzerRule`] for how the default planner places that
/// phase (it is not run strictly before every optimizer rule).
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
        // All enforcement runs here, first, as the analyzer phase: it makes the
        // plan *valid* (distribution first, then ordering) before any optimizer
        // rule sees it, mirroring the logical Analyzer/Optimizer split. The
        // optimizer rules that change these requirements (`JoinSelection`,
        // `WindowTopN`, `FilterPushdown`) re-establish validity themselves, so
        // enforcement is not repeated as an optimizer pass. The sort
        // *optimizations* (`OptimizeSorts`) are not enforcement and stay in the
        // optimizer phase.
        let rules: Vec<Arc<dyn PhysicalAnalyzerRule + Send + Sync>> = vec![
            // Establish the output-requirement boundary first, so enforcement can
            // see it (parallelize top-level scans below it, preserve the query's
            // final ordering). The matching remove pass runs late in the optimizer.
            Arc::new(OutputRequirements::new_add_mode()),
            Arc::new(EnforceDistribution::new()),
            Arc::new(EnforceSorting::new()),
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

    /// All enforcement runs first, as the analyzer: the output-requirement
    /// boundary is established, then distribution, then ordering, before any
    /// optimizer rule runs. The optimizer rules that change these requirements
    /// re-establish validity themselves, so enforcement is not repeated there.
    #[test]
    fn default_analyzer_enforces_all_requirements_first() {
        let analyzer = PhysicalAnalyzer::new();
        let names: Vec<&str> = analyzer.rules.iter().map(|r| r.name()).collect();
        assert_eq!(
            names,
            vec![
                "OutputRequirements",
                "EnforceDistribution",
                "EnforceSorting"
            ]
        );
    }

    /// The monolithic `EnsureRequirements` is no longer registered in either
    /// default list; the default pipeline uses the decomposed rules.
    #[test]
    fn default_lists_do_not_use_monolithic_ensure_requirements() {
        let analyzer_has = PhysicalAnalyzer::new()
            .rules
            .iter()
            .any(|r| r.name() == "EnsureRequirements");
        let optimizer_has = PhysicalOptimizer::new()
            .rules
            .iter()
            .any(|r| r.name() == "EnsureRequirements");
        assert!(!analyzer_has && !optimizer_has);
    }

    /// All enforcement (`EnforceDistribution`, `EnforceSorting`) lives in the
    /// analyzer; the optimizer keeps only the sort *optimizations*
    /// (`OptimizeSorts`), not enforcement. `JoinSelection` / `WindowTopN` /
    /// `FilterPushdown` re-establish validity themselves after they change it.
    #[test]
    fn default_optimizer_has_no_enforcement_rules() {
        let names: Vec<String> = PhysicalOptimizer::new()
            .rules
            .iter()
            .map(|r| r.name().to_string())
            .collect();
        assert!(
            !names.iter().any(|n| n == "EnforceDistribution"),
            "EnforceDistribution should not be an optimizer rule, got {names:?}"
        );
        assert!(
            !names.iter().any(|n| n == "EnforceSorting"),
            "EnforceSorting should not be an optimizer rule, got {names:?}"
        );
        assert!(
            names.iter().any(|n| n == "OptimizeSorts"),
            "OptimizeSorts (the sort optimizations) should stay an optimizer rule, got {names:?}"
        );
    }

    /// The enforcement analyzer rules keep their schema-check contract on.
    #[test]
    fn enforcement_rules_schema_check_is_on() {
        let analyzer = PhysicalAnalyzer::new();
        for name in ["EnforceDistribution", "EnforceSorting"] {
            let rule = analyzer
                .rules
                .iter()
                .find(|r| r.name() == name)
                .unwrap_or_else(|| panic!("{name} present in analyzer"));
            assert!(rule.schema_check(), "{name} schema_check should be on");
        }
    }
}
