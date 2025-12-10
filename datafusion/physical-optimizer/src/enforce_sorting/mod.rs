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

pub mod replace_with_order_preserving_variants;
pub mod sort_pushdown;

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use crate::enforce_sorting::replace_with_order_preserving_variants::{
    OrderPreservationContext, replace_with_order_preserving_variants,
};
use crate::enforce_sorting::sort_pushdown::{
    SortPushDown, assign_initial_requirements, pushdown_sorts,
};
use crate::output_requirements::OutputRequirementExec;
use crate::utils::{
    add_sort_above, add_sort_above_with_check, is_coalesce_partitions, is_limit,
    is_repartition, is_sort, is_sort_preserving_merge, is_window,
};

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::plan_err;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::{Distribution, Partitioning};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, LexRequirement};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::partial_sort::PartialSortExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::tree_node::PlanContext;
use datafusion_physical_plan::windows::{
    BoundedWindowAggExec, WindowAggExec, get_best_fitting_window,
};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties, InputOrderMode};

use itertools::izip;

/// This rule inspects [`SortExec`]'s in the given physical plan in order to
/// remove unnecessary sorts, and optimize sort performance across the plan.
#[derive(Default, Debug)]
pub struct EnforceSorting {}

impl EnforceSorting {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// This context object is used within the [`EnforceSorting`] rule to track the closest
/// [`SortExec`] descendant(s) for every child of a plan. The data attribute
/// stores whether the plan is a `SortExec` or is connected to a `SortExec`
/// via its children.
pub type PlanWithCorrespondingSort = PlanContext<bool>;

/// For a given node, update the `PlanContext.data` attribute.
///
/// If the node is a `SortExec`, or any of the node's children are a `SortExec`,
/// then set the attribute to true.
///
/// This requires a bottom-up traversal was previously performed, updating the
/// children previously.
fn update_sort_ctx_children_data(
    mut node_and_ctx: PlanWithCorrespondingSort,
    data: bool,
) -> Result<PlanWithCorrespondingSort> {
    // Update `child.data` for all children.
    for child_node in node_and_ctx.children.iter_mut() {
        let child_plan = &child_node.plan;
        child_node.data = if is_sort(child_plan) {
            // child is sort
            true
        } else if is_limit(child_plan) {
            // There is no sort linkage for this path, it starts at a limit.
            false
        } else {
            // If a descendent is a sort, and the child maintains the sort.
            let is_spm = is_sort_preserving_merge(child_plan);
            let required_orderings = child_plan.required_input_ordering();
            let flags = child_plan.maintains_input_order();
            // Add parent node to the tree if there is at least one child with
            // a sort connection:
            izip!(flags, required_orderings).any(|(maintains, required_ordering)| {
                let propagates_ordering =
                    (maintains && required_ordering.is_none()) || is_spm;
                // `connected_to_sort` only returns the correct answer with bottom-up traversal
                let connected_to_sort =
                    child_node.children.iter().any(|child| child.data);
                propagates_ordering && connected_to_sort
            })
        }
    }

    // set data attribute on current node
    node_and_ctx.data = data;

    Ok(node_and_ctx)
}

/// This object is used within the [`EnforceSorting`] rule to track the closest
/// [`CoalescePartitionsExec`] descendant(s) for every child of a plan. The data
/// attribute stores whether the plan is a `CoalescePartitionsExec` or is
/// connected to a `CoalescePartitionsExec` via its children.
///
/// The tracker halts at each [`SortExec`] (where the SPM will act to replace the coalesce).
///
/// This requires a bottom-up traversal was previously performed, updating the
/// children previously.
pub type PlanWithCorrespondingCoalescePartitions = PlanContext<bool>;

/// Discovers the linked Coalesce->Sort cascades.
///
/// This linkage is used in [`remove_bottleneck_in_subplan`] to selectively
/// remove the linked coalesces in the subplan. Then afterwards, an SPM is added
/// at the root of the subplan (just after the sort) in order to parallelize sorts.
/// Refer to the [`parallelize_sorts`] for more details on sort parallelization.
///
/// Example of linked Coalesce->Sort:
/// ```text
/// SortExec ctx.data=false, to halt remove_bottleneck_in_subplan)
///   ...nodes...   ctx.data=true (e.g. are linked in cascade)
///     Coalesce  ctx.data=true (e.g. is a coalesce)
/// ```
///
/// The link should not be continued (and the coalesce not removed) if the distribution
/// is changed between the Coalesce->Sort cascade. Example:
/// ```text
/// SortExec ctx.data=false, to halt remove_bottleneck_in_subplan)
///   AggregateExec  ctx.data=false, to stop the link
///     ...nodes...   ctx.data=true (e.g. are linked in cascade)
///       Coalesce  ctx.data=true (e.g. is a coalesce)
/// ```
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

/// Performs optimizations based upon a series of subrules.
/// Refer to each subrule for detailed descriptions of the optimizations performed:
/// Subrule application is ordering dependent.
///
/// Optimizer consists of 5 main parts which work sequentially
/// 1. [`ensure_sorting`] Works down-to-top to be able to remove unnecessary [`SortExec`]s, [`SortPreservingMergeExec`]s
///    add [`SortExec`]s if necessary by a requirement and adjusts window operators.
/// 2. [`parallelize_sorts`] (Optional, depends on the `repartition_sorts` configuration)
///    Responsible to identify and remove unnecessary partition unifier operators
///    such as [`SortPreservingMergeExec`], [`CoalescePartitionsExec`] follows [`SortExec`]s does possible simplifications.
/// 3. [`replace_with_order_preserving_variants()`] Replaces with alternative operators, for example can merge
///    a [`SortExec`] and a [`CoalescePartitionsExec`] into one [`SortPreservingMergeExec`]
///    or a [`SortExec`] + [`RepartitionExec`] combination into an order preserving [`RepartitionExec`]
/// 4. [`sort_pushdown`] Works top-down. Responsible to push down sort operators as deep as possible in the plan.
/// 5. `replace_with_partial_sort` Checks if it's possible to replace [`SortExec`]s with [`PartialSortExec`] operators
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
        let adjusted = pushdown_sorts(sort_pushdown)?;
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

/// Only interested with [`SortExec`]s and their unbounded children.
/// If the plan is not a [`SortExec`] or its child is not unbounded, returns the original plan.
/// Otherwise, by checking the requirement satisfaction searches for a replacement chance.
/// If there's one replaces the [`SortExec`] plan with a [`PartialSortExec`]
fn replace_with_partial_sort(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan_any = plan.as_any();
    let Some(sort_plan) = plan_any.downcast_ref::<SortExec>() else {
        return Ok(plan);
    };

    // It's safe to get first child of the SortExec
    let child = Arc::clone(sort_plan.children()[0]);
    if !child.boundedness().is_unbounded() {
        return Ok(plan);
    }

    // Here we're trying to find the common prefix for sorted columns that is required for the
    // sort and already satisfied by the given ordering
    let child_eq_properties = child.equivalence_properties();
    let sort_exprs = sort_plan.expr().clone();

    let mut common_prefix_length = 0;
    while child_eq_properties
        .ordering_satisfy(sort_exprs[0..common_prefix_length + 1].to_vec())?
    {
        common_prefix_length += 1;
    }
    if common_prefix_length > 0 {
        return Ok(Arc::new(
            PartialSortExec::new(
                sort_exprs,
                Arc::clone(sort_plan.input()),
                common_prefix_length,
            )
            .with_preserve_partitioning(sort_plan.preserve_partitioning())
            .with_fetch(sort_plan.fetch()),
        ));
    }
    Ok(plan)
}

/// Transform [`CoalescePartitionsExec`] + [`SortExec`] cascades into [`SortExec`]
/// + [`SortPreservingMergeExec`] cascades, as illustrated below.
///
/// A [`CoalescePartitionsExec`] + [`SortExec`] cascade combines partitions
/// first, and then sorts:
/// ```text
///   ┌ ─ ─ ─ ─ ─ ┐
///    ┌─┬─┬─┐
///   ││B│A│D│... ├──┐
///    └─┴─┴─┘       │
///   └ ─ ─ ─ ─ ─ ┘  │  ┌────────────────────────┐   ┌ ─ ─ ─ ─ ─ ─ ┐   ┌────────┐    ┌ ─ ─ ─ ─ ─ ─ ─ ┐
///    Partition 1   │  │        Coalesce        │    ┌─┬─┬─┬─┬─┐      │        │     ┌─┬─┬─┬─┬─┐
///                  ├──▶(no ordering guarantees)│──▶││B│E│A│D│C│...───▶  Sort  ├───▶││A│B│C│D│E│... │
///                  │  │                        │    └─┴─┴─┴─┴─┘      │        │     └─┴─┴─┴─┴─┘
///   ┌ ─ ─ ─ ─ ─ ┐  │  └────────────────────────┘   └ ─ ─ ─ ─ ─ ─ ┘   └────────┘    └ ─ ─ ─ ─ ─ ─ ─ ┘
///    ┌─┬─┐         │                                 Partition                       Partition
///   ││E│C│ ...  ├──┘
///    └─┴─┘
///   └ ─ ─ ─ ─ ─ ┘
///    Partition 2
/// ```
///
///
/// A [`SortExec`] + [`SortPreservingMergeExec`] cascade sorts each partition
/// first, then merges partitions while preserving the sort:
/// ```text
///   ┌ ─ ─ ─ ─ ─ ┐   ┌────────┐   ┌ ─ ─ ─ ─ ─ ┐
///    ┌─┬─┬─┐        │        │    ┌─┬─┬─┐
///   ││B│A│D│... │──▶│  Sort  │──▶││A│B│D│... │──┐
///    └─┴─┴─┘        │        │    └─┴─┴─┘       │
///   └ ─ ─ ─ ─ ─ ┘   └────────┘   └ ─ ─ ─ ─ ─ ┘  │  ┌─────────────────────┐    ┌ ─ ─ ─ ─ ─ ─ ─ ┐
///    Partition 1                  Partition 1   │  │                     │     ┌─┬─┬─┬─┬─┐
///                                               ├──▶ SortPreservingMerge ├───▶││A│B│C│D│E│... │
///                                               │  │                     │     └─┴─┴─┴─┴─┘
///   ┌ ─ ─ ─ ─ ─ ┐   ┌────────┐   ┌ ─ ─ ─ ─ ─ ┐  │  └─────────────────────┘    └ ─ ─ ─ ─ ─ ─ ─ ┘
///    ┌─┬─┐          │        │    ┌─┬─┐         │                               Partition
///   ││E│C│ ...  │──▶│  Sort  ├──▶││C│E│ ...  │──┘
///    └─┴─┘          │        │    └─┴─┘
///   └ ─ ─ ─ ─ ─ ┘   └────────┘   └ ─ ─ ─ ─ ─ ┘
///    Partition 2                  Partition 2
/// ```
///
/// The latter [`SortExec`] + [`SortPreservingMergeExec`] cascade performs
/// sorting first on a per-partition basis, thereby parallelizing the sort.
///
/// The outcome is that plans of the form
/// ```text
///      "SortExec: expr=\[a@0 ASC\]",
///      "  ...nodes..."
///      "    CoalescePartitionsExec",
///      "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
/// ```
/// are transformed into
/// ```text
///      "SortPreservingMergeExec: \[a@0 ASC\]",
///      "  SortExec: expr=\[a@0 ASC\]",
///      "    ...nodes..."
///      "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
/// ```
/// by following connections from [`CoalescePartitionsExec`]s to [`SortExec`]s.
/// By performing sorting in parallel, we can increase performance in some
/// scenarios.
///
/// This optimization requires that there are no nodes between the [`SortExec`]
/// and the [`CoalescePartitionsExec`], which requires single partitioning. Do
/// not parallelize when the following scenario occurs:
/// ```text
///      "SortExec: expr=\[a@0 ASC\]",
///      "  ...nodes requiring single partitioning..."
///      "    CoalescePartitionsExec",
///      "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
/// ```
///
/// **Steps**
/// 1. Checks if the plan is either a [`SortExec`], a [`SortPreservingMergeExec`],
///    or a [`CoalescePartitionsExec`]. Otherwise, does nothing.
/// 2. If the plan is a [`SortExec`] or a final [`SortPreservingMergeExec`]
///    (i.e. output partitioning is 1):
///      - Check for [`CoalescePartitionsExec`] in children. If found, check if
///        it can be removed (with possible [`RepartitionExec`]s). If so, remove
///        (see `remove_bottleneck_in_subplan`).
///      - If the plan is satisfying the ordering requirements, add a `SortExec`.
///      - Add an SPM above the plan and return.
/// 3. If the plan is a [`CoalescePartitionsExec`]:
///      - Check if it can be removed (with possible [`RepartitionExec`]s).
///        If so, remove (see `remove_bottleneck_in_subplan`).
pub fn parallelize_sorts(
    mut requirements: PlanWithCorrespondingCoalescePartitions,
) -> Result<Transformed<PlanWithCorrespondingCoalescePartitions>> {
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
        let sort_reqs = LexRequirement::from(sort_exprs.clone());
        let sort_exprs = sort_exprs.clone();

        // If there is a connection between a `CoalescePartitionsExec` and a
        // global sort that satisfy the requirements (i.e. intermediate
        // executors don't require single partition), then we can replace
        // the `CoalescePartitionsExec` + `SortExec` cascade with a `SortExec`
        // + `SortPreservingMergeExec` cascade to parallelize sorting.
        requirements = remove_bottleneck_in_subplan(requirements)?;
        // We also need to remove the self node since `remove_corresponding_coalesce_in_sub_plan`
        // deals with the children and their children and so on.
        requirements = requirements.children.swap_remove(0);

        requirements = add_sort_above_with_check(requirements, sort_reqs, fetch)?;

        let spm =
            SortPreservingMergeExec::new(sort_exprs, Arc::clone(&requirements.plan));
        Ok(Transformed::yes(
            PlanWithCorrespondingCoalescePartitions::new(
                Arc::new(spm.with_fetch(fetch)),
                false,
                vec![requirements],
            ),
        ))
    } else if is_coalesce_partitions(&requirements.plan) {
        let fetch = requirements.plan.fetch();
        // There is an unnecessary `CoalescePartitionsExec` in the plan.
        // This will handle the recursive `CoalescePartitionsExec` plans.
        requirements = remove_bottleneck_in_subplan(requirements)?;
        // For the removal of self node which is also a `CoalescePartitionsExec`.
        requirements = requirements.children.swap_remove(0);

        Ok(Transformed::yes(
            PlanWithCorrespondingCoalescePartitions::new(
                Arc::new(
                    CoalescePartitionsExec::new(Arc::clone(&requirements.plan))
                        .with_fetch(fetch),
                ),
                false,
                vec![requirements],
            ),
        ))
    } else {
        Ok(Transformed::yes(requirements))
    }
}

/// This function enforces sorting requirements and makes optimizations without
/// violating these requirements whenever possible. Requires a bottom-up traversal.
///
/// **Steps**
/// 1. Analyze if there are any immediate removals of [`SortExec`]s. If so,
///    removes them (see `analyze_immediate_sort_removal`).
/// 2. For each child of the plan, if the plan requires an input ordering:
///      - Checks if ordering is satisfied with the child. If not:
///          - If the child has an output ordering, removes the unnecessary
///            `SortExec`.
///          - Adds sort above the child plan.
///      - (Plan not requires input ordering)
///          - Checks if the `SortExec` is neutralized in the plan. If so,
///            removes it.
/// 3. Check and modify window operator:
///      - Checks if the plan is a window operator, and connected with a sort.
///        If so, either tries to update the window definition or removes
///        unnecessary [`SortExec`]s (see `adjust_window_sort_removal`).
/// 4. Check and remove possibly unnecessary SPM:
///       -  Checks if the plan is SPM and child 1 output partitions, if so
///          decides this SPM is unnecessary and removes it from the plan.
pub fn ensure_sorting(
    mut requirements: PlanWithCorrespondingSort,
) -> Result<Transformed<PlanWithCorrespondingSort>> {
    requirements = update_sort_ctx_children_data(requirements, false)?;

    // Perform naive analysis at the beginning -- remove already-satisfied sorts:
    if requirements.children.is_empty() {
        return Ok(Transformed::no(requirements));
    }
    let maybe_requirements = analyze_immediate_sort_removal(requirements)?;
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
            let req = required.into_single();
            if !eq_properties.ordering_satisfy_requirement(req.clone())? {
                // Make sure we preserve the ordering requirements:
                if physical_ordering.is_some() {
                    child = update_child_to_remove_unnecessary_sort(idx, child, plan)?;
                }
                child = add_sort_above(
                    child,
                    req,
                    plan.as_any()
                        .downcast_ref::<OutputRequirementExec>()
                        .map(|output| output.fetch())
                        .unwrap_or(None),
                );
                child = update_sort_ctx_children_data(child, true)?;
            }
        } else if physical_ordering.is_none() || !plan.maintains_input_order()[idx] {
            // We have a `SortExec` whose effect may be neutralized by another
            // order-imposing operator, remove this sort:
            child = update_child_to_remove_unnecessary_sort(idx, child, plan)?;
        }
        updated_children.push(child);
    }
    requirements.children = updated_children;
    requirements = requirements.update_plan_from_children()?;
    // For window expressions, we can remove some sorts when we can
    // calculate the result in reverse:
    let child_node = &requirements.children[0];
    if is_window(&requirements.plan) && child_node.data {
        return adjust_window_sort_removal(requirements).map(Transformed::yes);
    } else if is_sort_preserving_merge(&requirements.plan)
        && child_node.plan.output_partitioning().partition_count() <= 1
    {
        // This `SortPreservingMergeExec` is unnecessary, input already has a
        // single partition and no fetch is required.
        let mut child_node = requirements.children.swap_remove(0);
        if let Some(fetch) = requirements.plan.fetch() {
            // Add the limit exec if the original SPM had a fetch:
            child_node.plan =
                Arc::new(LocalLimitExec::new(Arc::clone(&child_node.plan), fetch));
        }
        return Ok(Transformed::yes(child_node));
    }
    update_sort_ctx_children_data(requirements, false).map(Transformed::yes)
}

/// Analyzes if there are any immediate sort removals by checking the `SortExec`s
/// and their ordering requirement satisfactions with children
/// If the sort is unnecessary, either replaces it with [`SortPreservingMergeExec`]/`LimitExec`
/// or removes the [`SortExec`].
/// Otherwise, returns the original plan
fn analyze_immediate_sort_removal(
    mut node: PlanWithCorrespondingSort,
) -> Result<Transformed<PlanWithCorrespondingSort>> {
    let Some(sort_exec) = node.plan.as_any().downcast_ref::<SortExec>() else {
        return Ok(Transformed::no(node));
    };
    let sort_input = sort_exec.input();
    // Check if the sort is unnecessary:
    let properties = sort_exec.properties();
    if let Some(ordering) = properties.output_ordering().cloned() {
        let eqp = sort_input.equivalence_properties();
        if !eqp.ordering_satisfy(ordering)? {
            return Ok(Transformed::no(node));
        }
    }
    node.plan = if !sort_exec.preserve_partitioning()
        && sort_input.output_partitioning().partition_count() > 1
    {
        // Replace the sort with a sort-preserving merge:
        Arc::new(
            SortPreservingMergeExec::new(
                sort_exec.expr().clone(),
                Arc::clone(sort_input),
            )
            .with_fetch(sort_exec.fetch()),
        ) as _
    } else {
        // Remove the sort:
        node.children = node.children.swap_remove(0).children;
        if let Some(fetch) = sort_exec.fetch() {
            // If the sort has a fetch, we need to add a limit:
            if properties.output_partitioning().partition_count() == 1 {
                Arc::new(GlobalLimitExec::new(Arc::clone(sort_input), 0, Some(fetch)))
            } else {
                Arc::new(LocalLimitExec::new(Arc::clone(sort_input), fetch))
            }
        } else {
            Arc::clone(sort_input)
        }
    };
    for child in node.children.iter_mut() {
        child.data = false;
    }
    node.data = false;
    Ok(Transformed::yes(node))
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
                get_best_fitting_window(window_expr, child_plan, &exec.partition_keys())?;
            (window_expr, new_window)
        } else if let Some(exec) = plan.downcast_ref::<BoundedWindowAggExec>() {
            let window_expr = exec.window_expr();
            let new_window =
                get_best_fitting_window(window_expr, child_plan, &exec.partition_keys())?;
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
        let reqs = window_tree.plan.required_input_ordering().swap_remove(0);

        // Satisfy the ordering requirement so that the window can run:
        let mut child_node = window_tree.children.swap_remove(0);
        if let Some(reqs) = reqs {
            child_node = add_sort_above(child_node, reqs.into_single(), None);
        }
        let child_plan = Arc::clone(&child_node.plan);
        window_tree.children.push(child_node);

        if window_expr.iter().all(|e| e.uses_bounded_memory()) {
            Arc::new(BoundedWindowAggExec::try_new(
                window_expr.to_vec(),
                child_plan,
                InputOrderMode::Sorted,
                !window_expr[0].partition_by().is_empty(),
            )?) as _
        } else {
            Arc::new(WindowAggExec::try_new(
                window_expr.to_vec(),
                child_plan,
                !window_expr[0].partition_by().is_empty(),
            )?) as _
        }
    };

    window_tree.data = false;
    Ok(window_tree)
}

/// Removes parallelization-reducing, avoidable [`CoalescePartitionsExec`]s from
/// the plan in `node`. After the removal of such `CoalescePartitionsExec`s from
/// the plan, some of the remaining `RepartitionExec`s might become unnecessary.
/// Removes such `RepartitionExec`s from the plan as well.
fn remove_bottleneck_in_subplan(
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
                    remove_bottleneck_in_subplan(node)
                } else {
                    Ok(node)
                }
            })
            .collect::<Result<_>>()?;
    }
    let mut new_reqs = requirements.update_plan_from_children()?;
    if let Some(repartition) = new_reqs.plan.as_any().downcast_ref::<RepartitionExec>() {
        let input_partitioning = repartition.input().output_partitioning();
        // We can remove this repartitioning operator if it is now a no-op:
        let mut can_remove = input_partitioning.eq(repartition.partitioning());
        // We can also remove it if we ended up with an ineffective RR:
        if let Partitioning::RoundRobinBatch(n_out) = repartition.partitioning() {
            can_remove |= *n_out == input_partitioning.partition_count();
        }
        if can_remove {
            new_reqs = new_reqs.children.swap_remove(0)
        }
    }
    Ok(new_reqs)
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
    if let Some(sort_exec) = node.plan.as_any().downcast_ref::<SortExec>() {
        // Do not remove sorts with fetch:
        if sort_exec.fetch().is_none() {
            node = node.children.swap_remove(0);
        }
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
        node = node.update_plan_from_children()?;
        if any_connection || node.children.is_empty() {
            node = update_sort_ctx_children_data(node, false)?;
        }

        // Replace with variants that do not preserve order.
        if is_sort_preserving_merge(&node.plan) {
            node.children = node.children.swap_remove(0).children;
            node.plan = Arc::clone(node.plan.children().swap_remove(0));
        } else if let Some(repartition) =
            node.plan.as_any().downcast_ref::<RepartitionExec>()
        {
            node.plan = Arc::new(RepartitionExec::try_new(
                Arc::clone(&node.children[0].plan),
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
        let plan = Arc::clone(&node.plan);
        let fetch = plan.fetch();
        let plan = if let Some(ordering) = plan.output_ordering() {
            Arc::new(
                SortPreservingMergeExec::new(ordering.clone(), plan).with_fetch(fetch),
            ) as _
        } else {
            Arc::new(CoalescePartitionsExec::new(plan)) as _
        };
        node = PlanWithCorrespondingSort::new(plan, false, vec![node]);
        node = update_sort_ctx_children_data(node, false)?;
    }
    Ok(node)
}

/// Converts an [ExecutionPlan] trait object to a [LexOrdering] reference when possible.
fn get_sort_exprs(
    sort_any: &Arc<dyn ExecutionPlan>,
) -> Result<(&LexOrdering, Option<usize>)> {
    if let Some(sort_exec) = sort_any.as_any().downcast_ref::<SortExec>() {
        Ok((sort_exec.expr(), sort_exec.fetch()))
    } else if let Some(spm) = sort_any.as_any().downcast_ref::<SortPreservingMergeExec>()
    {
        Ok((spm.expr(), spm.fetch()))
    } else {
        plan_err!("Given ExecutionPlan is not a SortExec or a SortPreservingMergeExec")
    }
}

// Tests are in tests/cases/enforce_sorting.rs
