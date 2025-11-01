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
use datafusion_physical_expr::{LexOrdering, LexRequirement};
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
    plan.as_any().is::<SortExec>()
}

/// Checks whether the given operator is a window;
/// i.e. either a [`WindowAggExec`] or a [`BoundedWindowAggExec`].
pub fn is_window(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<WindowAggExec>() || plan.as_any().is::<BoundedWindowAggExec>()
}

/// Checks whether the given operator is a [`UnionExec`].
pub fn is_union(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<UnionExec>()
}

/// Checks whether the given operator is a [`SortPreservingMergeExec`].
pub fn is_sort_preserving_merge(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<SortPreservingMergeExec>()
}

/// Checks whether the given operator is a [`CoalescePartitionsExec`].
pub fn is_coalesce_partitions(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<CoalescePartitionsExec>()
}

/// Checks whether the given operator is a [`RepartitionExec`].
pub fn is_repartition(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<RepartitionExec>()
}

/// Checks whether the given operator is a limit;
/// i.e. either a [`LocalLimitExec`] or a [`GlobalLimitExec`].
pub fn is_limit(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<GlobalLimitExec>() || plan.as_any().is::<LocalLimitExec>()
}
