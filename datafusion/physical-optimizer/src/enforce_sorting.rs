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

//! EnforceSorting optimizer rule inspects the physical plan with respect
//! to local sorting requirements and does the following:
//! - Adds a [`SortExec`] when a requirement is not met,
//! - Removes an already-existing [`SortExec`] if it is possible to prove
//!   that this sort is unnecessary
//!
//! The rule can work on valid *and* invalid physical plans with respect to
//! sorting requirements, but always produces a valid physical plan in this sense.
//!
//! A non-realistic but easy to follow example for sort removals: Assume that we
//! somehow get the fragment
//!
//! ```text
//! SortExec: expr=[nullable_col@0 ASC]
//!   SortExec: expr=[non_nullable_col@1 ASC]
//! ```
//!
//! in the physical plan. The first sort is unnecessary since its result is overwritten
//! by another [`SortExec`]. Therefore, this rule removes it from the physical plan.

use std::sync::Arc;

use crate::sort_pushdown::{assign_initial_requirements, pushdown_sorts, SortPushDown};

use crate::replace_with_order_preserving_variants::{
    replace_with_order_preserving_variants, OrderPreservationContext,
};
use crate::utils::{
    add_sort_above, add_sort_above_with_check, is_coalesce_partitions, is_limit,
    is_repartition, is_sort, is_sort_preserving_merge, is_union, is_window,
};
use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::tree_node::PlanContext;
use datafusion_physical_plan::windows::{
    get_best_fitting_window, BoundedWindowAggExec, WindowAggExec,
};
use datafusion_physical_plan::{Distribution, ExecutionPlan, InputOrderMode};

use datafusion_common::plan_err;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::{PhysicalSortExpr, PhysicalSortRequirement};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::partial_sort::PartialSortExec;
use datafusion_physical_plan::ExecutionPlanProperties;

use crate::PhysicalOptimizerRule;
use itertools::izip;

/// This rule inspects [`SortExec`]'s in the given physical plan and removes the
/// ones it can prove unnecessary.
#[derive(Default)]
pub struct EnforceSorting {}

impl EnforceSorting {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// This object is used within the [`EnforceSorting`] rule to track the closest
/// [`SortExec`] descendant(s) for every child of a plan. The data attribute
/// stores whether the plan is a `SortExec` or is connected to a `SortExec`
/// via its children.
type PlanWithCorrespondingSort = PlanContext<bool>;

fn update_sort_ctx_children(
    mut node: PlanWithCorrespondingSort,
    data: bool,
) -> Result<PlanWithCorrespondingSort> {
    for child_node in node.children.iter_mut() {
        let plan = &child_node.plan;
        child_node.data = if is_sort(plan) {
            // Initiate connection:
            true
        } else if is_limit(plan) {
            // There is no sort linkage for this path, it starts at a limit.
            false
        } else {
            let is_spm = is_sort_preserving_merge(plan);
            let required_orderings = plan.required_input_ordering();
            let flags = plan.maintains_input_order();
            // Add parent node to the tree if there is at least one child with
            // a sort connection:
            izip!(flags, required_orderings).any(|(maintains, required_ordering)| {
                let propagates_ordering =
                    (maintains && required_ordering.is_none()) || is_spm;
                let connected_to_sort =
                    child_node.children.iter().any(|child| child.data);
                propagates_ordering && connected_to_sort
            })
        }
    }

    node.data = data;
    node.update_plan_from_children()
}

/// This object is used within the [`EnforceSorting`] rule to track the closest
/// [`CoalescePartitionsExec`] descendant(s) for every child of a plan. The data
/// attribute stores whether the plan is a `CoalescePartitionsExec` or is
/// connected to a `CoalescePartitionsExec` via its children.
type PlanWithCorrespondingCoalescePartitions = PlanContext<bool>;

fn update_coalesce_ctx_children(
    coalesce_context: &mut PlanWithCorrespondingCoalescePartitions,
) {
    let children = &coalesce_context.children;
    coalesce_context.data = if children.is_empty() {
        // Plan has no children, it cannot be a `CoalescePartitionsExec`.
        false
    } else if is_coalesce_partitions(&coalesce_context.plan) {
        // Initiate a connection:
        true
    } else {
        children.iter().enumerate().any(|(idx, node)| {
            // Only consider operators that don't require a single partition,
            // and connected to some `CoalescePartitionsExec`:
            node.data
                && !matches!(
                    coalesce_context.plan.required_input_distribution()[idx],
                    Distribution::SinglePartition
                )
        })
    };
}

/// The boolean flag `repartition_sorts` defined in the config indicates
/// whether we elect to transform [`CoalescePartitionsExec`] + [`SortExec`] cascades
/// into [`SortExec`] + [`SortPreservingMergeExec`] cascades, which enables us to
/// perform sorting in parallel.
impl PhysicalOptimizerRule for EnforceSorting {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan_requirements = PlanWithCorrespondingSort::new_default(plan);
        // Execute a bottom-up traversal to enforce sorting requirements,
        // remove unnecessary sorts, and optimize sort-sensitive operators:
        let adjusted = plan_requirements.transform_up(ensure_sorting)?.data;
        let new_plan = if config.optimizer.repartition_sorts {
            let plan_with_coalesce_partitions =
                PlanWithCorrespondingCoalescePartitions::new_default(adjusted.plan);
            let parallel = plan_with_coalesce_partitions
                .transform_up(parallelize_sorts)
                .data()?;
            parallel.plan
        } else {
            adjusted.plan
        };

        let plan_with_pipeline_fixer = OrderPreservationContext::new_default(new_plan);
        let updated_plan = plan_with_pipeline_fixer
            .transform_up(|plan_with_pipeline_fixer| {
                replace_with_order_preserving_variants(
                    plan_with_pipeline_fixer,
                    false,
                    true,
                    config,
                )
            })
            .data()?;

        // Execute a top-down traversal to exploit sort push-down opportunities
        // missed by the bottom-up traversal:
        let mut sort_pushdown = SortPushDown::new_default(updated_plan.plan);
        assign_initial_requirements(&mut sort_pushdown);
        let adjusted = sort_pushdown.transform_down(pushdown_sorts)?.data;

        adjusted
            .plan
            .transform_up(|plan| Ok(Transformed::yes(replace_with_partial_sort(plan)?)))
            .data()
    }

    fn name(&self) -> &str {
        "EnforceSorting"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn replace_with_partial_sort(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan_any = plan.as_any();
    if let Some(sort_plan) = plan_any.downcast_ref::<SortExec>() {
        let child = sort_plan.children()[0].clone();
        if !child.execution_mode().is_unbounded() {
            return Ok(plan);
        }

        // here we're trying to find the common prefix for sorted columns that is required for the
        // sort and already satisfied by the given ordering
        let child_eq_properties = child.equivalence_properties();
        let sort_req = PhysicalSortRequirement::from_sort_exprs(sort_plan.expr());

        let mut common_prefix_length = 0;
        while child_eq_properties
            .ordering_satisfy_requirement(&sort_req[0..common_prefix_length + 1])
        {
            common_prefix_length += 1;
        }
        if common_prefix_length > 0 {
            return Ok(Arc::new(
                PartialSortExec::new(
                    sort_plan.expr().to_vec(),
                    sort_plan.input().clone(),
                    common_prefix_length,
                )
                .with_preserve_partitioning(sort_plan.preserve_partitioning())
                .with_fetch(sort_plan.fetch()),
            ));
        }
    }
    Ok(plan)
}

/// This function turns plans of the form
/// ```text
///      "SortExec: expr=\[a@0 ASC\]",
///      "  CoalescePartitionsExec",
///      "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
/// ```
/// to
/// ```text
///      "SortPreservingMergeExec: \[a@0 ASC\]",
///      "  SortExec: expr=\[a@0 ASC\]",
///      "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
/// ```
/// by following connections from [`CoalescePartitionsExec`]s to [`SortExec`]s.
/// By performing sorting in parallel, we can increase performance in some scenarios.
pub fn parallelize_sorts(
    mut requirements: PlanWithCorrespondingCoalescePartitions,
) -> Result<Transformed<PlanWithCorrespondingCoalescePartitions>> {
    // TODO: make this fucntion private once https://github.com/apache/datafusion/issues/11502 is resolved
    update_coalesce_ctx_children(&mut requirements);

    if requirements.children.is_empty() || !requirements.children[0].data {
        // We only take an action when the plan is either a `SortExec`, a
        // `SortPreservingMergeExec` or a `CoalescePartitionsExec`, and they
        // all have a single child. Therefore, if the first child has no
        // connection, we can return immediately.
        Ok(Transformed::no(requirements))
    } else if (is_sort(&requirements.plan)
        || is_sort_preserving_merge(&requirements.plan))
        && requirements.plan.output_partitioning().partition_count() <= 1
    {
        // Take the initial sort expressions and requirements
        let (sort_exprs, fetch) = get_sort_exprs(&requirements.plan)?;
        let sort_reqs = PhysicalSortRequirement::from_sort_exprs(sort_exprs);
        let sort_exprs = sort_exprs.to_vec();

        // If there is a connection between a `CoalescePartitionsExec` and a
        // global sort that satisfy the requirements (i.e. intermediate
        // executors don't require single partition), then we can replace
        // the `CoalescePartitionsExec` + `SortExec` cascade with a `SortExec`
        // + `SortPreservingMergeExec` cascade to parallelize sorting.
        requirements = remove_corresponding_coalesce_in_sub_plan(requirements)?;
        // We also need to remove the self node since `remove_corresponding_coalesce_in_sub_plan`
        // deals with the children and their children and so on.
        requirements = requirements.children.swap_remove(0);

        requirements = add_sort_above_with_check(requirements, sort_reqs, fetch);

        let spm = SortPreservingMergeExec::new(sort_exprs, requirements.plan.clone());
        Ok(Transformed::yes(
            PlanWithCorrespondingCoalescePartitions::new(
                Arc::new(spm.with_fetch(fetch)),
                false,
                vec![requirements],
            ),
        ))
    } else if is_coalesce_partitions(&requirements.plan) {
        // There is an unnecessary `CoalescePartitionsExec` in the plan.
        // This will handle the recursive `CoalescePartitionsExec` plans.
        requirements = remove_corresponding_coalesce_in_sub_plan(requirements)?;
        // For the removal of self node which is also a `CoalescePartitionsExec`.
        requirements = requirements.children.swap_remove(0);

        Ok(Transformed::yes(
            PlanWithCorrespondingCoalescePartitions::new(
                Arc::new(CoalescePartitionsExec::new(requirements.plan.clone())),
                false,
                vec![requirements],
            ),
        ))
    } else {
        Ok(Transformed::yes(requirements))
    }
}

/// This function enforces sorting requirements and makes optimizations without
/// violating these requirements whenever possible.
pub fn ensure_sorting(
    mut requirements: PlanWithCorrespondingSort,
) -> Result<Transformed<PlanWithCorrespondingSort>> {
    // TODO: make this function private once https://github.com/apache/datafusion/issues/11502 is resolved
    requirements = update_sort_ctx_children(requirements, false)?;

    // Perform naive analysis at the beginning -- remove already-satisfied sorts:
    if requirements.children.is_empty() {
        return Ok(Transformed::no(requirements));
    }
    let maybe_requirements = analyze_immediate_sort_removal(requirements);
    requirements = if !maybe_requirements.transformed {
        maybe_requirements.data
    } else {
        return Ok(maybe_requirements);
    };

    let plan = &requirements.plan;
    let mut updated_children = vec![];
    for (idx, (required_ordering, mut child)) in plan
        .required_input_ordering()
        .into_iter()
        .zip(requirements.children.into_iter())
        .enumerate()
    {
        let physical_ordering = child.plan.output_ordering();

        if let Some(required) = required_ordering {
            let eq_properties = child.plan.equivalence_properties();
            if !eq_properties.ordering_satisfy_requirement(&required) {
                // Make sure we preserve the ordering requirements:
                if physical_ordering.is_some() {
                    child = update_child_to_remove_unnecessary_sort(idx, child, plan)?;
                }
                child = add_sort_above(child, required, None);
                child = update_sort_ctx_children(child, true)?;
            }
        } else if physical_ordering.is_none()
            || !plan.maintains_input_order()[idx]
            || is_union(plan)
        {
            // We have a `SortExec` whose effect may be neutralized by another
            // order-imposing operator, remove this sort:
            child = update_child_to_remove_unnecessary_sort(idx, child, plan)?;
        }
        updated_children.push(child);
    }
    requirements.children = updated_children;
    // For window expressions, we can remove some sorts when we can
    // calculate the result in reverse:
    let child_node = &requirements.children[0];
    if is_window(plan) && child_node.data {
        return adjust_window_sort_removal(requirements).map(Transformed::yes);
    } else if is_sort_preserving_merge(plan)
        && child_node.plan.output_partitioning().partition_count() <= 1
    {
        // This `SortPreservingMergeExec` is unnecessary, input already has a
        // single partition.
        let child_node = requirements.children.swap_remove(0);
        return Ok(Transformed::yes(child_node));
    }

    update_sort_ctx_children(requirements, false).map(Transformed::yes)
}

/// Analyzes a given [`SortExec`] (`plan`) to determine whether its input
/// already has a finer ordering than it enforces.
fn analyze_immediate_sort_removal(
    mut node: PlanWithCorrespondingSort,
) -> Transformed<PlanWithCorrespondingSort> {
    if let Some(sort_exec) = node.plan.as_any().downcast_ref::<SortExec>() {
        let sort_input = sort_exec.input();
        // If this sort is unnecessary, we should remove it:
        if sort_input
            .equivalence_properties()
            .ordering_satisfy(sort_exec.properties().output_ordering().unwrap_or(&[]))
        {
            node.plan = if !sort_exec.preserve_partitioning()
                && sort_input.output_partitioning().partition_count() > 1
            {
                // Replace the sort with a sort-preserving merge:
                let expr = sort_exec.expr().to_vec();
                Arc::new(SortPreservingMergeExec::new(expr, sort_input.clone())) as _
            } else {
                // Remove the sort:
                node.children = node.children.swap_remove(0).children;
                sort_input.clone()
            };
            for child in node.children.iter_mut() {
                child.data = false;
            }
            node.data = false;
            return Transformed::yes(node);
        }
    }
    Transformed::no(node)
}

/// Adjusts a [`WindowAggExec`] or a [`BoundedWindowAggExec`] to determine
/// whether it may allow removing a sort.
fn adjust_window_sort_removal(
    mut window_tree: PlanWithCorrespondingSort,
) -> Result<PlanWithCorrespondingSort> {
    // Window operators have a single child we need to adjust:
    let child_node = remove_corresponding_sort_from_sub_plan(
        window_tree.children.swap_remove(0),
        matches!(
            window_tree.plan.required_input_distribution()[0],
            Distribution::SinglePartition
        ),
    )?;
    window_tree.children.push(child_node);

    let plan = window_tree.plan.as_any();
    let child_plan = &window_tree.children[0].plan;
    let (window_expr, new_window) =
        if let Some(exec) = plan.downcast_ref::<WindowAggExec>() {
            let window_expr = exec.window_expr();
            let new_window =
                get_best_fitting_window(window_expr, child_plan, &exec.partition_keys)?;
            (window_expr, new_window)
        } else if let Some(exec) = plan.downcast_ref::<BoundedWindowAggExec>() {
            let window_expr = exec.window_expr();
            let new_window =
                get_best_fitting_window(window_expr, child_plan, &exec.partition_keys)?;
            (window_expr, new_window)
        } else {
            return plan_err!("Expected WindowAggExec or BoundedWindowAggExec");
        };

    window_tree.plan = if let Some(new_window) = new_window {
        // We were able to change the window to accommodate the input, use it:
        new_window
    } else {
        // We were unable to change the window to accommodate the input, so we
        // will insert a sort.
        let reqs = window_tree
            .plan
            .required_input_ordering()
            .swap_remove(0)
            .unwrap_or_default();

        // Satisfy the ordering requirement so that the window can run:
        let mut child_node = window_tree.children.swap_remove(0);
        child_node = add_sort_above(child_node, reqs, None);
        let child_plan = child_node.plan.clone();
        window_tree.children.push(child_node);

        if window_expr.iter().all(|e| e.uses_bounded_memory()) {
            Arc::new(BoundedWindowAggExec::try_new(
                window_expr.to_vec(),
                child_plan,
                window_expr[0].partition_by().to_vec(),
                InputOrderMode::Sorted,
            )?) as _
        } else {
            Arc::new(WindowAggExec::try_new(
                window_expr.to_vec(),
                child_plan,
                window_expr[0].partition_by().to_vec(),
            )?) as _
        }
    };

    window_tree.data = false;
    Ok(window_tree)
}

/// Removes the [`CoalescePartitionsExec`] from the plan in `node`.
fn remove_corresponding_coalesce_in_sub_plan(
    mut requirements: PlanWithCorrespondingCoalescePartitions,
) -> Result<PlanWithCorrespondingCoalescePartitions> {
    let plan = &requirements.plan;
    let children = &mut requirements.children;
    if is_coalesce_partitions(&children[0].plan) {
        // We can safely use the 0th index since we have a `CoalescePartitionsExec`.
        let mut new_child_node = children[0].children.swap_remove(0);
        while new_child_node.plan.output_partitioning() == plan.output_partitioning()
            && is_repartition(&new_child_node.plan)
            && is_repartition(plan)
        {
            new_child_node = new_child_node.children.swap_remove(0)
        }
        children[0] = new_child_node;
    } else {
        requirements.children = requirements
            .children
            .into_iter()
            .map(|node| {
                if node.data {
                    remove_corresponding_coalesce_in_sub_plan(node)
                } else {
                    Ok(node)
                }
            })
            .collect::<Result<_>>()?;
    }

    requirements.update_plan_from_children()
}

/// Updates child to remove the unnecessary sort below it.
fn update_child_to_remove_unnecessary_sort(
    child_idx: usize,
    mut node: PlanWithCorrespondingSort,
    parent: &Arc<dyn ExecutionPlan>,
) -> Result<PlanWithCorrespondingSort> {
    if node.data {
        let requires_single_partition = matches!(
            parent.required_input_distribution()[child_idx],
            Distribution::SinglePartition
        );
        node = remove_corresponding_sort_from_sub_plan(node, requires_single_partition)?;
    }
    node.data = false;
    Ok(node)
}

/// Removes the sort from the plan in `node`.
fn remove_corresponding_sort_from_sub_plan(
    mut node: PlanWithCorrespondingSort,
    requires_single_partition: bool,
) -> Result<PlanWithCorrespondingSort> {
    // A `SortExec` is always at the bottom of the tree.
    if is_sort(&node.plan) {
        node = node.children.swap_remove(0);
    } else {
        let mut any_connection = false;
        let required_dist = node.plan.required_input_distribution();
        node.children = node
            .children
            .into_iter()
            .enumerate()
            .map(|(idx, child)| {
                if child.data {
                    any_connection = true;
                    remove_corresponding_sort_from_sub_plan(
                        child,
                        matches!(required_dist[idx], Distribution::SinglePartition),
                    )
                } else {
                    Ok(child)
                }
            })
            .collect::<Result<_>>()?;
        if any_connection || node.children.is_empty() {
            node = update_sort_ctx_children(node, false)?;
        }

        // Replace with variants that do not preserve order.
        if is_sort_preserving_merge(&node.plan) {
            node.children = node.children.swap_remove(0).children;
            node.plan = node.plan.children().swap_remove(0).clone();
        } else if let Some(repartition) =
            node.plan.as_any().downcast_ref::<RepartitionExec>()
        {
            node.plan = Arc::new(RepartitionExec::try_new(
                node.children[0].plan.clone(),
                repartition.properties().output_partitioning().clone(),
            )?) as _;
        }
    };
    // Deleting a merging sort may invalidate distribution requirements.
    // Ensure that we stay compliant with such requirements:
    if requires_single_partition && node.plan.output_partitioning().partition_count() > 1
    {
        // If there is existing ordering, to preserve ordering use
        // `SortPreservingMergeExec` instead of a `CoalescePartitionsExec`.
        let plan = node.plan.clone();
        let plan = if let Some(ordering) = plan.output_ordering() {
            Arc::new(SortPreservingMergeExec::new(ordering.to_vec(), plan)) as _
        } else {
            Arc::new(CoalescePartitionsExec::new(plan)) as _
        };
        node = PlanWithCorrespondingSort::new(plan, false, vec![node]);
        node = update_sort_ctx_children(node, false)?;
    }
    Ok(node)
}

/// Converts an [ExecutionPlan] trait object to a [PhysicalSortExpr] slice when possible.
fn get_sort_exprs(
    sort_any: &Arc<dyn ExecutionPlan>,
) -> Result<(&[PhysicalSortExpr], Option<usize>)> {
    if let Some(sort_exec) = sort_any.as_any().downcast_ref::<SortExec>() {
        Ok((sort_exec.expr(), sort_exec.fetch()))
    } else if let Some(spm) = sort_any.as_any().downcast_ref::<SortPreservingMergeExec>()
    {
        Ok((spm.expr(), spm.fetch()))
    } else {
        plan_err!("Given ExecutionPlan is not a SortExec or a SortPreservingMergeExec")
    }
}
