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
            // and `EnforceSorting`. It is now a single rule that decides distribution
            // and sorting together in one bottom-up pass. See the module-level doc on
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

    /// The default analyzer enforces requirements via `EnsureRequirements`.
    #[test]
    fn default_analyzer_enforces_requirements() {
        let analyzer = PhysicalAnalyzer::new();
        let names: Vec<&str> = analyzer.rules.iter().map(|r| r.name()).collect();
        assert_eq!(names, vec!["EnsureRequirements"]);
    }

    /// `EnsureRequirements` moved to the analyzer phase, so it must no longer
    /// appear in the default optimizer list (otherwise it would run twice).
    #[test]
    fn default_optimizer_does_not_enforce_requirements() {
        let has_ensure = PhysicalOptimizer::new()
            .rules
            .iter()
            .any(|r| r.name() == "EnsureRequirements");
        assert!(
            !has_ensure,
            "EnsureRequirements should be a PhysicalAnalyzerRule, not an optimizer rule"
        );
    }

    /// A default analyzer with a schema-preserving enforcement rule keeps the
    /// `schema_check` contract on.
    #[test]
    fn ensure_requirements_schema_check_is_on() {
        let analyzer = PhysicalAnalyzer::new();
        assert!(analyzer.rules[0].schema_check());
    }
}
