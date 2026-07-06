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

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_physical_expr::{
    Distribution, EquivalenceProperties, LexOrdering, LexRequirement, Partitioning,
    PhysicalExpr, physical_exprs_equal,
};
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::tree_node::PlanContext;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// This utility function adds a `SortExec` above an operator according to the
/// given ordering requirements while preserving the original partitioning.
///
/// Note that this updates the plan in both the `PlanContext.children` and
/// the `PlanContext.plan`'s children. Therefore its not required to sync
/// the child plans with [`PlanContext::update_plan_from_children`].
pub fn add_sort_above<T: Clone + Default>(
    node: PlanContext<T>,
    sort_requirements: LexRequirement,
    fetch: Option<usize>,
) -> PlanContext<T> {
    let mut sort_reqs: Vec<_> = sort_requirements.into();
    sort_reqs.retain(|sort_expr| {
        node.plan
            .equivalence_properties()
            .is_expr_constant(&sort_expr.expr)
            .is_none()
    });
    let sort_exprs = sort_reqs.into_iter().map(Into::into).collect::<Vec<_>>();
    let Some(ordering) = LexOrdering::new(sort_exprs) else {
        return node;
    };
    let mut new_sort = SortExec::new(ordering, Arc::clone(&node.plan)).with_fetch(fetch);
    if node.plan.output_partitioning().partition_count() > 1 {
        new_sort = new_sort.with_preserve_partitioning(true);
    }
    PlanContext::new(Arc::new(new_sort), T::default(), vec![node])
}

/// Like [`add_sort_above`], but also inserts a [`SortPreservingMergeExec`] when
/// the parent distribution requires a single partition and the input has
/// multiple partitions. This prevents `SortExec(preserve_partitioning=true)`
/// from violating `SinglePartition` requirements.
pub fn add_sort_above_with_distribution<T: Clone + Default>(
    node: PlanContext<T>,
    sort_requirements: LexRequirement,
    fetch: Option<usize>,
    required_distribution: &Distribution,
) -> PlanContext<T> {
    let mut sort_reqs: Vec<_> = sort_requirements.into();
    sort_reqs.retain(|sort_expr| {
        node.plan
            .equivalence_properties()
            .is_expr_constant(&sort_expr.expr)
            .is_none()
    });
    let sort_exprs = sort_reqs.into_iter().map(Into::into).collect::<Vec<_>>();
    let Some(ordering) = LexOrdering::new(sort_exprs) else {
        return node;
    };
    let input_has_multiple_partitions =
        node.plan.output_partitioning().partition_count() > 1;

    let mut new_sort =
        SortExec::new(ordering.clone(), Arc::clone(&node.plan)).with_fetch(fetch);
    if input_has_multiple_partitions {
        new_sort = new_sort.with_preserve_partitioning(true);
    }

    let sort_node = PlanContext::new(Arc::new(new_sort), T::default(), vec![node]);

    // If the parent requires SinglePartition and the input has multiple partitions,
    // wrap the partition-preserving sort in SortPreservingMergeExec.
    if matches!(required_distribution, Distribution::SinglePartition)
        && input_has_multiple_partitions
    {
        PlanContext::new(
            Arc::new(
                SortPreservingMergeExec::new(ordering, Arc::clone(&sort_node.plan))
                    .with_fetch(fetch),
            ),
            T::default(),
            vec![sort_node],
        )
    } else {
        sort_node
    }
}

/// This utility function adds a `SortExec` above an operator according to the
/// given ordering requirements while preserving the original partitioning. If
/// requirement is already satisfied no `SortExec` is added.
pub fn add_sort_above_with_check<T: Clone + Default>(
    node: PlanContext<T>,
    sort_requirements: LexRequirement,
    fetch: Option<usize>,
) -> Result<PlanContext<T>> {
    if !node
        .plan
        .equivalence_properties()
        .ordering_satisfy_requirement(sort_requirements.clone())?
    {
        Ok(add_sort_above(node, sort_requirements, fetch))
    } else {
        Ok(node)
    }
}

/// Checks whether the given operator is a [`SortExec`].
pub fn is_sort(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<SortExec>()
}

/// Checks whether the given operator is a window;
/// i.e. either a [`WindowAggExec`] or a [`BoundedWindowAggExec`].
pub fn is_window(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<WindowAggExec>() || plan.is::<BoundedWindowAggExec>()
}

/// Checks whether the given operator is a [`UnionExec`].
pub fn is_union(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<UnionExec>()
}

/// Checks whether the given operator is a [`SortPreservingMergeExec`].
pub fn is_sort_preserving_merge(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<SortPreservingMergeExec>()
}

/// Checks whether the given operator is a [`CoalescePartitionsExec`].
pub fn is_coalesce_partitions(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<CoalescePartitionsExec>()
}

/// Checks whether the given operator is a [`RepartitionExec`].
pub fn is_repartition(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<RepartitionExec>()
}

/// TODO: remove once Range generally satisfies KeyPartitioned requirements
/// through Partitioning::satisfaction.
/// See <https://github.com/apache/datafusion/issues/23266>.
///
/// Checks whether range partitioning satisfies a key partitioning requirement.
/// This is intentionally separate from general partitioning satisfaction while
/// range reuse is rolled out operator by operator.
pub(crate) fn range_partitioning_satisfies_key_partitioning(
    partitioning: &Partitioning,
    required_exprs: &[Arc<dyn PhysicalExpr>],
    eq_properties: &EquivalenceProperties,
    allow_subset: bool,
) -> bool {
    match partitioning {
        Partitioning::Range(range) => {
            let partition_exprs = range
                .ordering()
                .iter()
                .map(|sort_expr| Arc::clone(&sort_expr.expr))
                .collect::<Vec<_>>();

            if partition_exprs.is_empty() || required_exprs.is_empty() {
                return false;
            }

            let eq_group = eq_properties.eq_group();
            let normalized_partition_exprs = partition_exprs
                .iter()
                .map(|expr| eq_group.normalize_expr(Arc::clone(expr)))
                .collect::<Vec<_>>();
            let normalized_required_exprs = required_exprs
                .iter()
                .map(|expr| eq_group.normalize_expr(Arc::clone(expr)))
                .collect::<Vec<_>>();

            if physical_exprs_equal(
                &normalized_required_exprs,
                &normalized_partition_exprs,
            ) {
                return true;
            }

            allow_subset
                && normalized_partition_exprs.len() < normalized_required_exprs.len()
                && normalized_partition_exprs.iter().all(|partition_expr| {
                    normalized_required_exprs
                        .iter()
                        .any(|required_expr| partition_expr.eq(required_expr))
                })
        }
        _ => false,
    }
}

/// TODO: remove once Range generally satisfies KeyPartitioned requirements
/// through Partitioning::satisfaction.
/// See <https://github.com/apache/datafusion/issues/23266>.
///
/// Checks whether an aggregate can reuse range partitioning to satisfy its key
/// partitioning requirement.
pub(crate) fn aggregate_can_reuse_range_partitioning(
    plan: &Arc<dyn ExecutionPlan>,
) -> bool {
    plan.downcast_ref::<AggregateExec>()
        .is_some_and(|aggregate| {
            matches!(
                aggregate.mode(),
                AggregateMode::FinalPartitioned | AggregateMode::SinglePartitioned
            ) && !aggregate.group_expr().has_grouping_set()
        })
}

/// Checks whether the given operator is a limit;
/// i.e. either a [`LocalLimitExec`] or a [`GlobalLimitExec`].
pub fn is_limit(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<GlobalLimitExec>() || plan.is::<LocalLimitExec>()
}
