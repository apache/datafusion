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

//! [`AdaptiveHashJoin`] lets partitioned hash joins pick their mode at runtime

use std::sync::Arc;

use datafusion_common::config::{ConfigOptions, OptimizerOptions};
use datafusion_common::{JoinType, Result};
use datafusion_physical_expr::{Distribution, Partitioning};
use datafusion_physical_plan::execution_plan::{
    ChildrenPropertiesMode, ReplaceChildrenOptions, replace_children_if_necessary,
};
use datafusion_physical_plan::joins::{
    AdaptiveHashJoinExec, HashJoinExec, PartitionMode,
};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::statistics::StatisticsArgs;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::PhysicalOptimizerRule;
use crate::optimizer::{ConfigOnlyContext, PhysicalOptimizerContext};

/// Replaces each `Partitioned` [`HashJoinExec`] that hash repartitions its
/// probe side with an [`AdaptiveHashJoinExec`], which picks between
/// `CollectLeft` and `Partitioned` at runtime, once it has read its build side.
/// See `datafusion.optimizer.enable_adaptive_hash_join`.
///
/// The planner picks the mode of a hash join from estimated statistics, and
/// only collects the build side into a single partition when the estimate is
/// below `hash_join_single_partition_threshold`. Estimates for filtered scans
/// and for join and aggregate outputs are rough, so large queries often
/// repartition a huge probe side to join against a build side that turns out
/// to be small.
///
/// The adaptive join keeps the properties and the dynamic filter of the join
/// it wraps, so this rule runs after dynamic filters have been pushed down,
/// and the rest of the plan is unchanged. A join whose probe side is already
/// hash partitioned keeps its mode: with no repartition to save on the probe
/// side, collecting the build side would only lose the parallel build.
#[derive(Debug, Default)]
pub struct AdaptiveHashJoin {}

impl AdaptiveHashJoin {
    /// Creates the rule
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for AdaptiveHashJoin {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize_with_context(plan, &ConfigOnlyContext::new(config))
    }

    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !context.config_options().optimizer.enable_adaptive_hash_join {
            return Ok(plan);
        }
        rewrite(plan, false, context)
    }

    fn name(&self) -> &str {
        "AdaptiveHashJoin"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `required` is whether an ancestor relies on `plan` being hash partitioned,
/// with no repartition in between.
fn rewrite(
    plan: Arc<dyn ExecutionPlan>,
    required: bool,
    context: &dyn PhysicalOptimizerContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(join) = plan.downcast_ref::<HashJoinExec>()
        && *join.partition_mode() == PartitionMode::Partitioned
        && !join.null_aware
        && let Some(probe) = hash_repartition_input(join.right())
    {
        let options = &context.config_options().optimizer;
        let probe_rows = context
            .compute_statistics(probe, &StatisticsArgs::new())?
            .num_rows
            .get_value()
            .copied();
        let join = AdaptiveHashJoinExec::try_new(
            join,
            max_build_rows(join.join_type(), probe_rows, options),
            options.adaptive_hash_join_threshold,
            required,
        )?;
        // Without a repartition, the build side must stay hash partitioned
        // for the join to run `Partitioned`
        let build_required = !join.repartitions_build();
        let [build, probe] = [0, 1].map(|i| Arc::clone(join.children()[i]));
        let children = vec![
            rewrite(build, build_required, context)?,
            rewrite(probe, false, context)?,
        ];
        return Arc::new(join).replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
        );
    }

    // A node that is key partitioned without repartitioning got that from
    // its children, or only from its probe side for a `CollectLeft` join
    let inherits = required
        && plan.downcast_ref::<RepartitionExec>().is_none()
        && matches!(
            plan.output_partitioning(),
            Partitioning::Hash(..) | Partitioning::Range(..)
        );
    let collect_left = plan
        .downcast_ref::<HashJoinExec>()
        .is_some_and(|join| *join.partition_mode() == PartitionMode::CollectLeft);
    let requirements = plan.input_distribution_requirements();
    let co_partitioned = requirements.is_co_partitioned();
    let children: Vec<_> = plan.children().into_iter().cloned().collect();
    let mut new_children = Vec::with_capacity(children.len());
    for (i, (child, distribution)) in children
        .into_iter()
        .zip(requirements.into_per_child())
        .enumerate()
    {
        let child_required = co_partitioned
            || is_key_distribution(&distribution)
            || (inherits && !(collect_left && i == 0));
        new_children.push(rewrite(child, child_required, context)?);
    }
    // The adaptive joins keep the properties of the joins they replace, so
    // the ancestors keep theirs, and the dynamic filters they produce
    replace_children_if_necessary(plan, new_children)
}

/// The largest build side, in rows, that a join collects into a single
/// partition at runtime.
fn max_build_rows(
    join_type: &JoinType,
    probe_rows: Option<usize>,
    options: &OptimizerOptions,
) -> usize {
    // With a single partition, these joins update one shared bitmap of matched
    // build-side rows from every probe thread, so only small ones pay off
    let tracks_build_side = matches!(
        join_type,
        JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::LeftMark
            | JoinType::Full
    );
    let max_rows = if tracks_build_side {
        options.hash_join_single_partition_threshold_rows
    } else {
        options.adaptive_hash_join_threshold_rows
    };
    match probe_rows {
        Some(probe_rows) if options.adaptive_hash_join_max_probe_ratio > 0.0 => {
            let limit = probe_rows as f64 * options.adaptive_hash_join_max_probe_ratio;
            max_rows.min(limit as usize)
        }
        _ => max_rows,
    }
}

#[expect(
    deprecated,
    reason = "HashPartitioned is still accepted during the KeyPartitioned migration"
)]
fn is_key_distribution(distribution: &Distribution) -> bool {
    matches!(
        distribution,
        Distribution::KeyPartitioned(_) | Distribution::HashPartitioned(_)
    )
}

/// Returns the input of `plan` if it is a hash repartition.
fn hash_repartition_input(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<&Arc<dyn ExecutionPlan>> {
    let repartition = plan.downcast_ref::<RepartitionExec>()?;
    (matches!(repartition.partitioning(), Partitioning::Hash(..))
        && !repartition.preserve_order())
    .then(|| repartition.input())
}
