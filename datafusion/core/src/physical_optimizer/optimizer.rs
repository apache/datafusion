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

use std::sync::Arc;

use crate::config::ConfigOptions;
use crate::physical_optimizer::aggregate_statistics::AggregateStatistics;
use crate::physical_optimizer::coalesce_batches::CoalesceBatches;
use crate::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use crate::physical_optimizer::dist_enforcement::EnforceDistribution;
use crate::physical_optimizer::global_sort_selection::GlobalSortSelection;
use crate::physical_optimizer::join_selection::JoinSelection;
use crate::physical_optimizer::pipeline_checker::PipelineChecker;
use crate::physical_optimizer::pipeline_fixer::PipelineFixer;
use crate::physical_optimizer::repartition::Repartition;
use crate::physical_optimizer::sort_enforcement::EnforceSorting;
use crate::{error::Result, physical_plan::ExecutionPlan};

/// `PhysicalOptimizerRule` transforms one ['ExecutionPlan'] into another which
/// computes the same results, but in a potentially more efficient
/// way.
pub trait PhysicalOptimizerRule {
    /// Rewrite `plan` to an optimized form
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;

    /// A flag to indicate whether the physical planner should valid the rule will not
    /// change the schema of the plan after the rewriting.
    /// Some of the optimization rules might change the nullable properties of the schema
    /// and should disable the schema check.
    fn schema_check(&self) -> bool;
}

/// A rule-based physical optimizer.
#[derive(Clone)]
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
            Arc::new(AggregateStatistics::new()),
            // Statistics-based join selection will change the Auto mode to a real join implementation,
            // like collect left, or hash join, or future sort merge join, which will influence the
            // EnforceDistribution and EnforceSorting rules as they decide whether to add additional
            // repartitioning and local sorting steps to meet distribution and ordering requirements.
            // Therefore, it should run before EnforceDistribution and EnforceSorting.
            Arc::new(JoinSelection::new()),
            // If the query is processing infinite inputs, the PipelineFixer rule applies the
            // necessary transformations to make the query runnable (if it is not already runnable).
            // If the query can not be made runnable, the rule emits an error with a diagnostic message.
            // Since the transformations it applies may alter output partitioning properties of operators
            // (e.g. by swapping hash join sides), this rule runs before EnforceDistribution.
            Arc::new(PipelineFixer::new()),
            // In order to increase the parallelism, the Repartition rule will change the
            // output partitioning of some operators in the plan tree, which will influence
            // other rules. Therefore, it should run as soon as possible. It is optional because:
            // - It's not used for the distributed engine, Ballista.
            // - It's conflicted with some parts of the EnforceDistribution, since it will
            //   introduce additional repartitioning while EnforceDistribution aims to
            //   reduce unnecessary repartitioning.
            Arc::new(Repartition::new()),
            // - Currently it will depend on the partition number to decide whether to change the
            // single node sort to parallel local sort and merge. Therefore, GlobalSortSelection
            // should run after the Repartition.
            // - Since it will change the output ordering of some operators, it should run
            // before JoinSelection and EnforceSorting, which may depend on that.
            Arc::new(GlobalSortSelection::new()),
            // The EnforceDistribution rule is for adding essential repartition to satisfy the required
            // distribution. Please make sure that the whole plan tree is determined before this rule.
            Arc::new(EnforceDistribution::new()),
            // The CombinePartialFinalAggregate rule should be applied after the EnforceDistribution rule
            Arc::new(CombinePartialFinalAggregate::new()),
            // The EnforceSorting rule is for adding essential local sorting to satisfy the required
            // ordering. Please make sure that the whole plan tree is determined before this rule.
            // Note that one should always run this rule after running the EnforceDistribution rule
            // as the latter may break local sorting requirements.
            Arc::new(EnforceSorting::new()),
            // The CoalesceBatches rule will not influence the distribution and ordering of the
            // whole plan tree. Therefore, to avoid influencing other rules, it should run last.
            Arc::new(CoalesceBatches::new()),
            // The PipelineChecker rule will reject non-runnable query plans that use
            // pipeline-breaking operators on infinite input(s). The rule generates a
            // diagnostic error message when this happens. It makes no changes to the
            // given query plan; i.e. it only acts as a final gatekeeping rule.
            Arc::new(PipelineChecker::new()),
        ];

        Self::with_rules(rules)
    }

    /// Create a new optimizer with the given rules
    pub fn with_rules(rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>) -> Self {
        Self { rules }
    }

    /// Remove the first rule that [PhysicalOptimizerRule::name] equals to the given name.
    /// Return whether a rule is removed.
    pub fn remove_rule(&mut self, name: &str) -> bool {
        let mut index_to_move = None;
        for (index, rule) in self.rules.iter().enumerate() {
            if rule.name() == name {
                index_to_move = Some(index);
                break;
            }
        }

        if let Some(index) = index_to_move {
            self.rules.remove(index);
        }

        index_to_move.is_some()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn remove_enforce_sorting_rule() {
        let mut optimizer = PhysicalOptimizer::new();
        assert!(optimizer.remove_rule(EnforceSorting {}.name()));
        assert!(!optimizer.remove_rule(EnforceSorting {}.name()));
    }
}
