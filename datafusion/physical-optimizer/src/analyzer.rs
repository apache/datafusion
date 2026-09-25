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
        let rules: Vec<Arc<dyn PhysicalAnalyzerRule + Send + Sync>> = vec![
            // Enforces the distribution requirements declared by
            // `ExecutionPlan::required_input_distribution` (inserting the
            // repartition / coalesce operators needed for a valid, parallel
            // plan). It runs first so the optimizer rules see a
            // distribution-valid plan; the optimizer phase re-runs it after any
            // rule that changes distribution (e.g. `JoinSelection`).
            //
            // Ordering enforcement (`EnforceSorting`) and the sort optimizations
            // (`OptimizeSorts`) run later in the optimizer phase, not here,
            // because ordering enforcement is not idempotent and depends on
            // rules like `JoinSelection` / `WindowTopN` having run first.
            Arc::new(EnforceDistribution::new()),
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

    /// The default analyzer enforces distribution via `EnforceDistribution`.
    #[test]
    fn default_analyzer_enforces_distribution() {
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

    /// The default optimizer runs the decomposed enforcement/optimization rules
    /// in the required relative order: `EnforceDistribution` → `EnforceSorting`
    /// → `OptimizeSorts`.
    #[test]
    fn default_optimizer_has_decomposed_rules_in_order() {
        let names: Vec<String> = PhysicalOptimizer::new()
            .rules
            .iter()
            .map(|r| r.name().to_string())
            .collect();
        let pos = |name: &str| names.iter().position(|n| n == name);
        let (d, s, o) = (
            pos("EnforceDistribution").expect("EnforceDistribution present"),
            pos("EnforceSorting").expect("EnforceSorting present"),
            pos("OptimizeSorts").expect("OptimizeSorts present"),
        );
        assert!(
            d < s && s < o,
            "expected EnforceDistribution < EnforceSorting < OptimizeSorts, got {names:?}"
        );
    }

    /// The analyzer rule keeps its schema-check contract on.
    #[test]
    fn enforce_distribution_schema_check_is_on() {
        let analyzer = PhysicalAnalyzer::new();
        assert!(analyzer.rules[0].schema_check());
    }
}
