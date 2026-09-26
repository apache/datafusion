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

use crate::ensure_requirements::EnforceDistribution;

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
        // Enforcement runs first, as an analyzer, making the plan
        // distribution-valid before any optimizer rule sees it. `EnforceDistribution`
        // is self-contained: it parallelizes top-level scans itself, so no
        // `OutputRequirements` boundary rule is needed here. The only optimizer
        // rules that change the required distribution (`JoinSelection`,
        // `WindowTopN`) re-establish it themselves, so it is enforced exactly once
        // here. Ordering enforcement (`EnforceSorting`) and the sort optimizations
        // (`OptimizeSorts`) still run in the optimizer phase (ordering enforcement
        // is not idempotent and reads the settled partitioning).
        let rules: Vec<Arc<dyn PhysicalAnalyzerRule + Send + Sync>> =
            vec![Arc::new(EnforceDistribution::new())];
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

    /// Enforcement runs first, as the analyzer: distribution is enforced before
    /// any optimizer rule runs. `EnforceDistribution` is the sole analyzer rule
    /// (it parallelizes top-level scans itself, so no `OutputRequirements`
    /// boundary rule is needed here).
    #[test]
    fn default_analyzer_enforces_distribution_first() {
        let analyzer = PhysicalAnalyzer::new();
        let names: Vec<&str> = analyzer.rules.iter().map(|r| r.name()).collect();
        assert_eq!(names, vec!["EnforceDistribution"]);
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

    /// Distribution is enforced once, in the analyzer; the optimizer keeps only
    /// ordering enforcement and the sort optimizations, in order
    /// `EnforceSorting` → `OptimizeSorts`. `EnforceDistribution` is not a
    /// standalone optimizer rule any more (`JoinSelection` / `WindowTopN`
    /// re-establish distribution themselves).
    #[test]
    fn default_optimizer_has_sorting_rules_in_order_without_enforce_distribution() {
        let names: Vec<String> = PhysicalOptimizer::new()
            .rules
            .iter()
            .map(|r| r.name().to_string())
            .collect();
        assert!(
            !names.iter().any(|n| n == "EnforceDistribution"),
            "EnforceDistribution should not be a standalone optimizer rule, got {names:?}"
        );
        let pos = |name: &str| names.iter().position(|n| n == name);
        let (s, o) = (
            pos("EnforceSorting").expect("EnforceSorting present"),
            pos("OptimizeSorts").expect("OptimizeSorts present"),
        );
        assert!(
            s < o,
            "expected EnforceSorting < OptimizeSorts, got {names:?}"
        );
    }

    /// `EnforceDistribution` keeps its schema-check contract on in the analyzer.
    #[test]
    fn enforce_distribution_schema_check_is_on() {
        let rule = PhysicalAnalyzer::new()
            .rules
            .into_iter()
            .find(|r| r.name() == "EnforceDistribution")
            .expect("EnforceDistribution present in analyzer");
        assert!(rule.schema_check());
    }
}
