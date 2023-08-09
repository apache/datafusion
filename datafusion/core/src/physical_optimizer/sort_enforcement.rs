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
//! - Adds a [SortExec] when a requirement is not met,
//! - Removes an already-existing [SortExec] if it is possible to prove
//!   that this sort is unnecessary
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

use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_optimizer::replace_with_order_preserving_variants::{
    replace_with_order_preserving_variants, OrderPreservationContext,
};
use crate::physical_optimizer::sort_pushdown::{pushdown_sorts, SortPushDown};
use crate::physical_optimizer::utils::{
    add_sort_above, find_indices, is_coalesce_partitions, is_limit, is_repartition,
    is_sort, is_sort_preserving_merge, is_sorted, is_union, is_window,
    merge_and_order_indices, set_difference,
};
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::windows::{
    BoundedWindowAggExec, PartitionSearchMode, WindowAggExec,
};
use crate::physical_plan::{with_new_children_if_necessary, Distribution, ExecutionPlan};
use arrow::datatypes::SchemaRef;
use datafusion_common::tree_node::{Transformed, TreeNode, VisitRecursion};
use datafusion_common::utils::{get_at_indices, longest_consecutive_prefix};
use datafusion_common::DataFusionError;
use datafusion_physical_expr::utils::{
    convert_to_expr, get_indices_of_matching_exprs, ordering_satisfy,
    ordering_satisfy_requirement_concrete,
};
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr, PhysicalSortRequirement};
use itertools::{concat, izip, Itertools};
use std::sync::Arc;

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

/// This object implements a tree that we use while keeping track of paths
/// leading to [`SortExec`]s.
#[derive(Debug, Clone)]
pub(crate) struct ExecTree {
    /// The `ExecutionPlan` associated with this node
    pub plan: Arc<dyn ExecutionPlan>,
    /// Child index of the plan in its parent
    pub idx: usize,
    /// Children of the plan that would need updating if we remove leaf executors
    pub children: Vec<ExecTree>,
}

impl ExecTree {
    /// Create new Exec tree
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        idx: usize,
        children: Vec<ExecTree>,
    ) -> Self {
        ExecTree {
            plan,
            idx,
            children,
        }
    }

    /// This function returns the executors at the leaves of the tree.
    fn get_leaves(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        if self.children.is_empty() {
            vec![self.plan.clone()]
        } else {
            concat(self.children.iter().map(|e| e.get_leaves()))
        }
    }
}

/// This object is used within the [`EnforceSorting`] rule to track the closest
/// [`SortExec`] descendant(s) for every child of a plan.
#[derive(Debug, Clone)]
struct PlanWithCorrespondingSort {
    plan: Arc<dyn ExecutionPlan>,
    // For every child, keep a subtree of `ExecutionPlan`s starting from the
    // child until the `SortExec`(s) -- could be multiple for n-ary plans like
    // Union -- that determine the output ordering of the child. If the child
    // has no connection to any sort, simply store None (and not a subtree).
    sort_onwards: Vec<Option<ExecTree>>,
}

impl PlanWithCorrespondingSort {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let length = plan.children().len();
        PlanWithCorrespondingSort {
            plan,
            sort_onwards: vec![None; length],
        }
    }

    pub fn new_from_children_nodes(
        children_nodes: Vec<PlanWithCorrespondingSort>,
        parent_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let children_plans = children_nodes
            .iter()
            .map(|item| item.plan.clone())
            .collect::<Vec<_>>();
        let sort_onwards = children_nodes
            .into_iter()
            .enumerate()
            .map(|(idx, item)| {
                let plan = &item.plan;
                // Leaves of `sort_onwards` are `SortExec` operators, which impose
                // an ordering. This tree collects all the intermediate executors
                // that maintain this ordering. If we just saw a order imposing
                // operator, we reset the tree and start accumulating.
                if is_sort(plan) {
                    return Some(ExecTree::new(item.plan, idx, vec![]));
                } else if is_limit(plan) {
                    // There is no sort linkage for this path, it starts at a limit.
                    return None;
                }
                let is_spm = is_sort_preserving_merge(plan);
                let required_orderings = plan.required_input_ordering();
                let flags = plan.maintains_input_order();
                let children = izip!(flags, item.sort_onwards, required_orderings)
                    .filter_map(|(maintains, element, required_ordering)| {
                        if (required_ordering.is_none() && maintains) || is_spm {
                            element
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<ExecTree>>();
                if !children.is_empty() {
                    // Add parent node to the tree if there is at least one
                    // child with a subtree:
                    Some(ExecTree::new(item.plan, idx, children))
                } else {
                    // There is no sort linkage for this child, do nothing.
                    None
                }
            })
            .collect();

        let plan = with_new_children_if_necessary(parent_plan, children_plans)?.into();
        Ok(PlanWithCorrespondingSort { plan, sort_onwards })
    }

    pub fn children(&self) -> Vec<PlanWithCorrespondingSort> {
        self.plan
            .children()
            .into_iter()
            .map(|child| PlanWithCorrespondingSort::new(child))
            .collect()
    }
}

impl TreeNode for PlanWithCorrespondingSort {
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        let children = self.children();
        for child in children {
            match op(&child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }

        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if children.is_empty() {
            Ok(self)
        } else {
            let children_nodes = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;
            PlanWithCorrespondingSort::new_from_children_nodes(children_nodes, self.plan)
        }
    }
}

/// This object is used within the [EnforceSorting] rule to track the closest
/// [`CoalescePartitionsExec`] descendant(s) for every child of a plan.
#[derive(Debug, Clone)]
struct PlanWithCorrespondingCoalescePartitions {
    plan: Arc<dyn ExecutionPlan>,
    // For every child, keep a subtree of `ExecutionPlan`s starting from the
    // child until the `CoalescePartitionsExec`(s) -- could be multiple for
    // n-ary plans like Union -- that affect the output partitioning of the
    // child. If the child has no connection to any `CoalescePartitionsExec`,
    // simply store None (and not a subtree).
    coalesce_onwards: Vec<Option<ExecTree>>,
}

impl PlanWithCorrespondingCoalescePartitions {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let length = plan.children().len();
        PlanWithCorrespondingCoalescePartitions {
            plan,
            coalesce_onwards: vec![None; length],
        }
    }

    pub fn new_from_children_nodes(
        children_nodes: Vec<PlanWithCorrespondingCoalescePartitions>,
        parent_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let children_plans = children_nodes
            .iter()
            .map(|item| item.plan.clone())
            .collect();
        let coalesce_onwards = children_nodes
            .into_iter()
            .enumerate()
            .map(|(idx, item)| {
                // Leaves of the `coalesce_onwards` tree are `CoalescePartitionsExec`
                // operators. This tree collects all the intermediate executors that
                // maintain a single partition. If we just saw a `CoalescePartitionsExec`
                // operator, we reset the tree and start accumulating.
                let plan = item.plan;
                if plan.children().is_empty() {
                    // Plan has no children, there is nothing to propagate.
                    None
                } else if is_coalesce_partitions(&plan) {
                    Some(ExecTree::new(plan, idx, vec![]))
                } else {
                    let children = item
                        .coalesce_onwards
                        .into_iter()
                        .flatten()
                        .filter(|item| {
                            // Only consider operators that don't require a
                            // single partition.
                            !matches!(
                                plan.required_input_distribution()[item.idx],
                                Distribution::SinglePartition
                            )
                        })
                        .collect::<Vec<_>>();
                    if children.is_empty() {
                        None
                    } else {
                        Some(ExecTree::new(plan, idx, children))
                    }
                }
            })
            .collect();
        let plan = with_new_children_if_necessary(parent_plan, children_plans)?.into();
        Ok(PlanWithCorrespondingCoalescePartitions {
            plan,
            coalesce_onwards,
        })
    }

    pub fn children(&self) -> Vec<PlanWithCorrespondingCoalescePartitions> {
        self.plan
            .children()
            .into_iter()
            .map(|child| PlanWithCorrespondingCoalescePartitions::new(child))
            .collect()
    }
}

impl TreeNode for PlanWithCorrespondingCoalescePartitions {
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        let children = self.children();
        for child in children {
            match op(&child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }

        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if children.is_empty() {
            Ok(self)
        } else {
            let children_nodes = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;
            PlanWithCorrespondingCoalescePartitions::new_from_children_nodes(
                children_nodes,
                self.plan,
            )
        }
    }
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
        let plan_requirements = PlanWithCorrespondingSort::new(plan);
        // Execute a bottom-up traversal to enforce sorting requirements,
        // remove unnecessary sorts, and optimize sort-sensitive operators:
        let adjusted = plan_requirements.transform_up(&ensure_sorting)?;
        let new_plan = if config.optimizer.repartition_sorts {
            let plan_with_coalesce_partitions =
                PlanWithCorrespondingCoalescePartitions::new(adjusted.plan);
            let parallel =
                plan_with_coalesce_partitions.transform_up(&parallelize_sorts)?;
            parallel.plan
        } else {
            adjusted.plan
        };
        let plan_with_pipeline_fixer = OrderPreservationContext::new(new_plan);
        let updated_plan =
            plan_with_pipeline_fixer.transform_up(&|plan_with_pipeline_fixer| {
                replace_with_order_preserving_variants(
                    plan_with_pipeline_fixer,
                    false,
                    true,
                    config,
                )
            })?;

        // Execute a top-down traversal to exploit sort push-down opportunities
        // missed by the bottom-up traversal:
        let sort_pushdown = SortPushDown::init(updated_plan.plan);
        let adjusted = sort_pushdown.transform_down(&pushdown_sorts)?;
        Ok(adjusted.plan)
    }

    fn name(&self) -> &str {
        "EnforceSorting"
    }

    fn schema_check(&self) -> bool {
        true
    }
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
fn parallelize_sorts(
    requirements: PlanWithCorrespondingCoalescePartitions,
) -> Result<Transformed<PlanWithCorrespondingCoalescePartitions>> {
    let plan = requirements.plan;
    let mut coalesce_onwards = requirements.coalesce_onwards;
    if plan.children().is_empty() || coalesce_onwards[0].is_none() {
        // We only take an action when the plan is either a SortExec, a
        // SortPreservingMergeExec or a CoalescePartitionsExec, and they
        // all have a single child. Therefore, if the first child is `None`,
        // we can return immediately.
        return Ok(Transformed::No(PlanWithCorrespondingCoalescePartitions {
            plan,
            coalesce_onwards,
        }));
    } else if (is_sort(&plan) || is_sort_preserving_merge(&plan))
        && plan.output_partitioning().partition_count() <= 1
    {
        // If there is a connection between a CoalescePartitionsExec and a
        // global sort that satisfy the requirements (i.e. intermediate
        // executors don't require single partition), then we can replace
        // the CoalescePartitionsExec + Sort cascade with a SortExec +
        // SortPreservingMergeExec cascade to parallelize sorting.
        let mut prev_layer = plan.clone();
        update_child_to_remove_coalesce(&mut prev_layer, &mut coalesce_onwards[0])?;
        let (sort_exprs, fetch) = get_sort_exprs(&plan)?;
        add_sort_above(&mut prev_layer, sort_exprs.to_vec(), fetch)?;
        let spm = SortPreservingMergeExec::new(sort_exprs.to_vec(), prev_layer)
            .with_fetch(fetch);
        return Ok(Transformed::Yes(PlanWithCorrespondingCoalescePartitions {
            plan: Arc::new(spm),
            coalesce_onwards: vec![None],
        }));
    } else if is_coalesce_partitions(&plan) {
        // There is an unnecessary `CoalescePartitionsExec` in the plan.
        let mut prev_layer = plan.clone();
        update_child_to_remove_coalesce(&mut prev_layer, &mut coalesce_onwards[0])?;
        let new_plan = plan.with_new_children(vec![prev_layer])?;
        return Ok(Transformed::Yes(PlanWithCorrespondingCoalescePartitions {
            plan: new_plan,
            coalesce_onwards: vec![None],
        }));
    }

    Ok(Transformed::Yes(PlanWithCorrespondingCoalescePartitions {
        plan,
        coalesce_onwards,
    }))
}

/// This function enforces sorting requirements and makes optimizations without
/// violating these requirements whenever possible.
fn ensure_sorting(
    requirements: PlanWithCorrespondingSort,
) -> Result<Transformed<PlanWithCorrespondingSort>> {
    // Perform naive analysis at the beginning -- remove already-satisfied sorts:
    if requirements.plan.children().is_empty() {
        return Ok(Transformed::No(requirements));
    }
    let plan = requirements.plan;
    let mut children = plan.children();
    let mut sort_onwards = requirements.sort_onwards;
    if let Some(result) = analyze_immediate_sort_removal(&plan, &sort_onwards) {
        return Ok(Transformed::Yes(result));
    }
    for (idx, (child, sort_onwards, required_ordering)) in izip!(
        children.iter_mut(),
        sort_onwards.iter_mut(),
        plan.required_input_ordering()
    )
    .enumerate()
    {
        let physical_ordering = child.output_ordering();
        match (required_ordering, physical_ordering) {
            (Some(required_ordering), Some(physical_ordering)) => {
                if !ordering_satisfy_requirement_concrete(
                    physical_ordering,
                    &required_ordering,
                    || child.equivalence_properties(),
                    || child.ordering_equivalence_properties(),
                ) {
                    // Make sure we preserve the ordering requirements:
                    update_child_to_remove_unnecessary_sort(child, sort_onwards, &plan)?;
                    let sort_expr =
                        PhysicalSortRequirement::to_sort_exprs(required_ordering);
                    add_sort_above(child, sort_expr, None)?;
                    if is_sort(child) {
                        *sort_onwards = Some(ExecTree::new(child.clone(), idx, vec![]));
                    } else {
                        *sort_onwards = None;
                    }
                }
            }
            (Some(required), None) => {
                // Ordering requirement is not met, we should add a `SortExec` to the plan.
                let sort_expr = PhysicalSortRequirement::to_sort_exprs(required);
                add_sort_above(child, sort_expr, None)?;
                *sort_onwards = Some(ExecTree::new(child.clone(), idx, vec![]));
            }
            (None, Some(_)) => {
                // We have a `SortExec` whose effect may be neutralized by
                // another order-imposing operator. Remove this sort.
                if !plan.maintains_input_order()[idx] || is_union(&plan) {
                    update_child_to_remove_unnecessary_sort(child, sort_onwards, &plan)?;
                }
            }
            (None, None) => {}
        }
    }
    // For window expressions, we can remove some sorts when we can
    // calculate the result in reverse:
    if is_window(&plan) {
        if let Some(tree) = &mut sort_onwards[0] {
            if let Some(result) = analyze_window_sort_removal(tree, &plan)? {
                return Ok(Transformed::Yes(result));
            }
        }
    } else if is_sort_preserving_merge(&plan)
        && children[0].output_partitioning().partition_count() <= 1
    {
        // This SortPreservingMergeExec is unnecessary, input already has a
        // single partition.
        return Ok(Transformed::Yes(PlanWithCorrespondingSort {
            plan: children[0].clone(),
            sort_onwards: vec![sort_onwards[0].clone()],
        }));
    }
    Ok(Transformed::Yes(PlanWithCorrespondingSort {
        plan: plan.with_new_children(children)?,
        sort_onwards,
    }))
}

/// Analyzes a given [`SortExec`] (`plan`) to determine whether its input
/// already has a finer ordering than it enforces.
fn analyze_immediate_sort_removal(
    plan: &Arc<dyn ExecutionPlan>,
    sort_onwards: &[Option<ExecTree>],
) -> Option<PlanWithCorrespondingSort> {
    if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        let sort_input = sort_exec.input().clone();
        // If this sort is unnecessary, we should remove it:
        if ordering_satisfy(
            sort_input.output_ordering(),
            sort_exec.output_ordering(),
            || sort_input.equivalence_properties(),
            || sort_input.ordering_equivalence_properties(),
        ) {
            // Since we know that a `SortExec` has exactly one child,
            // we can use the zero index safely:
            return Some(
                if !sort_exec.preserve_partitioning()
                    && sort_input.output_partitioning().partition_count() > 1
                {
                    // Replace the sort with a sort-preserving merge:
                    let new_plan: Arc<dyn ExecutionPlan> =
                        Arc::new(SortPreservingMergeExec::new(
                            sort_exec.expr().to_vec(),
                            sort_input,
                        ));
                    let new_tree = ExecTree::new(
                        new_plan.clone(),
                        0,
                        sort_onwards.iter().flat_map(|e| e.clone()).collect(),
                    );
                    PlanWithCorrespondingSort {
                        plan: new_plan,
                        sort_onwards: vec![Some(new_tree)],
                    }
                } else {
                    // Remove the sort:
                    PlanWithCorrespondingSort {
                        plan: sort_input,
                        sort_onwards: sort_onwards.to_vec(),
                    }
                },
            );
        }
    }
    None
}

/// Analyzes a [`WindowAggExec`] or a [`BoundedWindowAggExec`] to determine
/// whether it may allow removing a sort.
fn analyze_window_sort_removal(
    sort_tree: &mut ExecTree,
    window_exec: &Arc<dyn ExecutionPlan>,
) -> Result<Option<PlanWithCorrespondingSort>> {
    let (window_expr, partition_keys) = if let Some(exec) =
        window_exec.as_any().downcast_ref::<BoundedWindowAggExec>()
    {
        (exec.window_expr(), &exec.partition_keys)
    } else if let Some(exec) = window_exec.as_any().downcast_ref::<WindowAggExec>() {
        (exec.window_expr(), &exec.partition_keys)
    } else {
        return Err(DataFusionError::Plan(
            "Expects to receive either WindowAggExec of BoundedWindowAggExec".to_string(),
        ));
    };
    let partitionby_exprs = window_expr[0].partition_by();
    let orderby_sort_keys = window_expr[0].order_by();

    // search_flags stores return value of the can_skip_sort.
    // `None` case represents `SortExec` cannot be removed.
    // `PartitionSearch` mode stores at which mode executor should work to remove
    // `SortExec` before it,
    // `bool` stores whether or not we need to reverse window expressions to remove `SortExec`.
    let mut search_flags = None;
    for sort_any in sort_tree.get_leaves() {
        // Variable `sort_any` will either be a `SortExec` or a
        // `SortPreservingMergeExec`, and both have a single child.
        // Therefore, we can use the 0th index without loss of generality.
        let sort_input = &sort_any.children()[0];
        let flags = can_skip_sort(partitionby_exprs, orderby_sort_keys, sort_input)?;
        if flags.is_some() && (search_flags.is_none() || search_flags == flags) {
            search_flags = flags;
            continue;
        }
        // We can not skip the sort, or window reversal requirements are not
        // uniform; then sort removal is not possible -- we immediately return.
        return Ok(None);
    }
    let (should_reverse, partition_search_mode) = if let Some(search_flags) = search_flags
    {
        search_flags
    } else {
        // We can not skip the sort return:
        return Ok(None);
    };
    let is_unbounded = unbounded_output(window_exec);
    if !is_unbounded && partition_search_mode != PartitionSearchMode::Sorted {
        // Executor has bounded input and `partition_search_mode` is not `PartitionSearchMode::Sorted`
        // in this case removing the sort is not helpful, return:
        return Ok(None);
    };

    let new_window_expr = if should_reverse {
        window_expr
            .iter()
            .map(|e| e.get_reverse_expr())
            .collect::<Option<Vec<_>>>()
    } else {
        Some(window_expr.to_vec())
    };
    if let Some(window_expr) = new_window_expr {
        let requires_single_partition = matches!(
            window_exec.required_input_distribution()[sort_tree.idx],
            Distribution::SinglePartition
        );
        let mut new_child = remove_corresponding_sort_from_sub_plan(
            sort_tree,
            requires_single_partition,
        )?;
        let new_schema = new_child.schema();

        let uses_bounded_memory = window_expr.iter().all(|e| e.uses_bounded_memory());
        // If all window expressions can run with bounded memory, choose the
        // bounded window variant:
        let new_plan = if uses_bounded_memory {
            Arc::new(BoundedWindowAggExec::try_new(
                window_expr,
                new_child,
                new_schema,
                partition_keys.to_vec(),
                partition_search_mode,
            )?) as _
        } else {
            if partition_search_mode != PartitionSearchMode::Sorted {
                // For `WindowAggExec` to work correctly PARTITION BY columns should be sorted.
                // Hence, if `partition_search_mode` is not `PartitionSearchMode::Sorted` we should convert
                // input ordering such that it can work with PartitionSearchMode::Sorted (add `SortExec`).
                // Effectively `WindowAggExec` works only in PartitionSearchMode::Sorted mode.
                let reqs = window_exec
                    .required_input_ordering()
                    .swap_remove(0)
                    .unwrap_or(vec![]);
                let sort_expr = PhysicalSortRequirement::to_sort_exprs(reqs);
                add_sort_above(&mut new_child, sort_expr, None)?;
            };
            Arc::new(WindowAggExec::try_new(
                window_expr,
                new_child,
                new_schema,
                partition_keys.to_vec(),
            )?) as _
        };
        return Ok(Some(PlanWithCorrespondingSort::new(new_plan)));
    }
    Ok(None)
}

/// Updates child to remove the unnecessary [`CoalescePartitionsExec`] below it.
fn update_child_to_remove_coalesce(
    child: &mut Arc<dyn ExecutionPlan>,
    coalesce_onwards: &mut Option<ExecTree>,
) -> Result<()> {
    if let Some(coalesce_onwards) = coalesce_onwards {
        *child = remove_corresponding_coalesce_in_sub_plan(coalesce_onwards, child)?;
    }
    Ok(())
}

/// Removes the [`CoalescePartitionsExec`] from the plan in `coalesce_onwards`.
fn remove_corresponding_coalesce_in_sub_plan(
    coalesce_onwards: &mut ExecTree,
    parent: &Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(if is_coalesce_partitions(&coalesce_onwards.plan) {
        // We can safely use the 0th index since we have a `CoalescePartitionsExec`.
        let mut new_plan = coalesce_onwards.plan.children()[0].clone();
        while new_plan.output_partitioning() == parent.output_partitioning()
            && is_repartition(&new_plan)
            && is_repartition(parent)
        {
            new_plan = new_plan.children()[0].clone()
        }
        new_plan
    } else {
        let plan = coalesce_onwards.plan.clone();
        let mut children = plan.children();
        for item in &mut coalesce_onwards.children {
            children[item.idx] = remove_corresponding_coalesce_in_sub_plan(item, &plan)?;
        }
        plan.with_new_children(children)?
    })
}

/// Updates child to remove the unnecessary sort below it.
fn update_child_to_remove_unnecessary_sort(
    child: &mut Arc<dyn ExecutionPlan>,
    sort_onwards: &mut Option<ExecTree>,
    parent: &Arc<dyn ExecutionPlan>,
) -> Result<()> {
    if let Some(sort_onwards) = sort_onwards {
        let requires_single_partition = matches!(
            parent.required_input_distribution()[sort_onwards.idx],
            Distribution::SinglePartition
        );
        *child = remove_corresponding_sort_from_sub_plan(
            sort_onwards,
            requires_single_partition,
        )?;
    }
    *sort_onwards = None;
    Ok(())
}

/// Removes the sort from the plan in `sort_onwards`.
fn remove_corresponding_sort_from_sub_plan(
    sort_onwards: &mut ExecTree,
    requires_single_partition: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    // A `SortExec` is always at the bottom of the tree.
    let mut updated_plan = if is_sort(&sort_onwards.plan) {
        sort_onwards.plan.children()[0].clone()
    } else {
        let plan = &sort_onwards.plan;
        let mut children = plan.children();
        for item in &mut sort_onwards.children {
            let requires_single_partition = matches!(
                plan.required_input_distribution()[item.idx],
                Distribution::SinglePartition
            );
            children[item.idx] =
                remove_corresponding_sort_from_sub_plan(item, requires_single_partition)?;
        }
        if is_sort_preserving_merge(plan) {
            children[0].clone()
        } else {
            plan.clone().with_new_children(children)?
        }
    };
    // Deleting a merging sort may invalidate distribution requirements.
    // Ensure that we stay compliant with such requirements:
    if requires_single_partition
        && updated_plan.output_partitioning().partition_count() > 1
    {
        // If there is existing ordering, to preserve ordering use SortPreservingMergeExec
        // instead of CoalescePartitionsExec.
        if let Some(ordering) = updated_plan.output_ordering() {
            updated_plan = Arc::new(SortPreservingMergeExec::new(
                ordering.to_vec(),
                updated_plan,
            ));
        } else {
            updated_plan = Arc::new(CoalescePartitionsExec::new(updated_plan.clone()));
        }
    }
    Ok(updated_plan)
}

/// Converts an [ExecutionPlan] trait object to a [PhysicalSortExpr] slice when possible.
fn get_sort_exprs(
    sort_any: &Arc<dyn ExecutionPlan>,
) -> Result<(&[PhysicalSortExpr], Option<usize>)> {
    if let Some(sort_exec) = sort_any.as_any().downcast_ref::<SortExec>() {
        Ok((sort_exec.expr(), sort_exec.fetch()))
    } else if let Some(sort_preserving_merge_exec) =
        sort_any.as_any().downcast_ref::<SortPreservingMergeExec>()
    {
        Ok((
            sort_preserving_merge_exec.expr(),
            sort_preserving_merge_exec.fetch(),
        ))
    } else {
        Err(DataFusionError::Plan(
            "Given ExecutionPlan is not a SortExec or a SortPreservingMergeExec"
                .to_string(),
        ))
    }
}

/// Compares physical ordering (output ordering of input executor) with
/// `partitionby_exprs` and `orderby_keys`
/// to decide whether existing ordering is sufficient to run current window executor.
/// A `None` return value indicates that we can not remove the sort in question (input ordering is not
/// sufficient to run current window executor).
/// A `Some((bool, PartitionSearchMode))` value indicates window executor can be run with existing input ordering
/// (Hence we can remove [`SortExec`] before it).
/// `bool` represents whether we should reverse window executor to remove [`SortExec`] before it.
/// `PartitionSearchMode` represents, in which mode Window Executor should work with existing ordering.
fn can_skip_sort(
    partitionby_exprs: &[Arc<dyn PhysicalExpr>],
    orderby_keys: &[PhysicalSortExpr],
    input: &Arc<dyn ExecutionPlan>,
) -> Result<Option<(bool, PartitionSearchMode)>> {
    let physical_ordering = if let Some(physical_ordering) = input.output_ordering() {
        physical_ordering
    } else {
        // If there is no physical ordering, there is no way to remove a
        // sort, so immediately return.
        return Ok(None);
    };
    let orderby_exprs = convert_to_expr(orderby_keys);
    let physical_ordering_exprs = convert_to_expr(physical_ordering);
    let equal_properties = || input.equivalence_properties();
    // indices of the order by expressions among input ordering expressions
    let ob_indices = get_indices_of_matching_exprs(
        &orderby_exprs,
        &physical_ordering_exprs,
        equal_properties,
    );
    if ob_indices.len() != orderby_exprs.len() {
        // If all order by expressions are not in the input ordering,
        // there is no way to remove a sort -- immediately return:
        return Ok(None);
    }
    // indices of the partition by expressions among input ordering expressions
    let pb_indices = get_indices_of_matching_exprs(
        partitionby_exprs,
        &physical_ordering_exprs,
        equal_properties,
    );
    let ordered_merged_indices = merge_and_order_indices(&pb_indices, &ob_indices);
    // Indices of order by columns that doesn't seen in partition by
    // Equivalently (Order by columns) ∖ (Partition by columns) where `∖` represents set difference.
    let unique_ob_indices = set_difference(&ob_indices, &pb_indices);
    if !is_sorted(&unique_ob_indices) {
        // ORDER BY indices should be ascending ordered
        return Ok(None);
    }
    let first_n = longest_consecutive_prefix(ordered_merged_indices);
    let furthest_ob_index = *unique_ob_indices.last().unwrap_or(&0);
    // Cannot skip sort if last order by index is not within consecutive prefix.
    // For instance, if input is ordered by a,b,c,d
    // for expression `PARTITION BY a, ORDER BY b, d`, `first_n` would be 2 (meaning a, b defines a prefix for input ordering)
    // Whereas `furthest_ob_index` would be 3 (column d occurs at the 3rd index of the existing ordering.)
    // Hence existing ordering is not sufficient to run current Executor.
    // However, for expression `PARTITION BY a, ORDER BY b, c, d`, `first_n` would be 4 (meaning a, b, c, d defines a prefix for input ordering)
    // Similarly, `furthest_ob_index` would be 3 (column d occurs at the 3rd index of the existing ordering.)
    // Hence existing ordering would be sufficient to run current Executor.
    if first_n <= furthest_ob_index {
        return Ok(None);
    }
    let input_orderby_columns = get_at_indices(physical_ordering, &unique_ob_indices)?;
    let expected_orderby_columns =
        get_at_indices(orderby_keys, find_indices(&ob_indices, &unique_ob_indices)?)?;
    let should_reverse = if let Some(should_reverse) = check_alignments(
        &input.schema(),
        &input_orderby_columns,
        &expected_orderby_columns,
    )? {
        should_reverse
    } else {
        // If ordering directions are not aligned. We cannot calculate result without changing existing ordering.
        return Ok(None);
    };

    let ordered_pb_indices = pb_indices.iter().copied().sorted().collect::<Vec<_>>();
    // Determine how many elements in the partition by columns defines a consecutive range from zero.
    let first_n = longest_consecutive_prefix(&ordered_pb_indices);
    let mode = if first_n == partitionby_exprs.len() {
        // All of the partition by columns defines a consecutive range from zero.
        PartitionSearchMode::Sorted
    } else if first_n > 0 {
        // All of the partition by columns defines a consecutive range from zero.
        let ordered_range = &ordered_pb_indices[0..first_n];
        let input_pb_exprs = get_at_indices(&physical_ordering_exprs, ordered_range)?;
        let partially_ordered_indices = get_indices_of_matching_exprs(
            &input_pb_exprs,
            partitionby_exprs,
            equal_properties,
        );
        PartitionSearchMode::PartiallySorted(partially_ordered_indices)
    } else {
        // None of the partition by columns defines a consecutive range from zero.
        PartitionSearchMode::Linear
    };

    Ok(Some((should_reverse, mode)))
}

fn check_alignments(
    schema: &SchemaRef,
    physical_ordering: &[PhysicalSortExpr],
    required: &[PhysicalSortExpr],
) -> Result<Option<bool>> {
    let res = izip!(physical_ordering, required)
        .map(|(lhs, rhs)| check_alignment(schema, lhs, rhs))
        .collect::<Result<Option<Vec<_>>>>()?;
    Ok(if let Some(res) = res {
        if !res.is_empty() {
            let first = res[0];
            let all_same = res.into_iter().all(|elem| elem == first);
            all_same.then_some(first)
        } else {
            Some(false)
        }
    } else {
        // Cannot skip some of the requirements in the input.
        None
    })
}

/// Compares `physical_ordering` and `required` ordering, decides whether
/// alignments match. A `None` return value indicates that current column is
/// not aligned. A `Some(bool)` value indicates otherwise, and signals whether
/// we should reverse the window expression in order to avoid sorting.
fn check_alignment(
    input_schema: &SchemaRef,
    physical_ordering: &PhysicalSortExpr,
    required: &PhysicalSortExpr,
) -> Result<Option<bool>> {
    Ok(if required.expr.eq(&physical_ordering.expr) {
        let physical_opts = physical_ordering.options;
        let required_opts = required.options;
        if required.expr.nullable(input_schema)? {
            let reverse = physical_opts == !required_opts;
            (reverse || physical_opts == required_opts).then_some(reverse)
        } else {
            // If the column is not nullable, NULLS FIRST/LAST is not important.
            Some(physical_opts.descending != required_opts.descending)
        }
    } else {
        None
    })
}

// Get output (un)boundedness information for the given `plan`.
pub(crate) fn unbounded_output(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let result = if plan.children().is_empty() {
        plan.unbounded_output(&[])
    } else {
        let children_unbounded_output = plan
            .children()
            .iter()
            .map(unbounded_output)
            .collect::<Vec<_>>();
        plan.unbounded_output(&children_unbounded_output)
    };
    result.unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_optimizer::dist_enforcement::EnforceDistribution;
    use crate::physical_optimizer::test_utils::{
        aggregate_exec, bounded_window_exec, coalesce_batches_exec,
        coalesce_partitions_exec, filter_exec, get_plan_string, global_limit_exec,
        hash_join_exec, limit_exec, local_limit_exec, memory_exec, parquet_exec,
        parquet_exec_sorted, repartition_exec, sort_exec, sort_expr, sort_expr_options,
        sort_merge_join_exec, sort_preserving_merge_exec, union_exec,
    };
    use crate::physical_plan::repartition::RepartitionExec;
    use crate::physical_plan::windows::PartitionSearchMode::{
        Linear, PartiallySorted, Sorted,
    };
    use crate::physical_plan::{displayable, Partitioning};
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::test::csv_exec_sorted;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::Result;
    use datafusion_expr::JoinType;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::expressions::{col, NotExpr};
    use datafusion_physical_expr::PhysicalSortExpr;
    use std::sync::Arc;
    use crate::physical_optimizer::dist_enforcement_v2::print_plan;

    fn create_test_schema() -> Result<SchemaRef> {
        let nullable_column = Field::new("nullable_col", DataType::Int32, true);
        let non_nullable_column = Field::new("non_nullable_col", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![nullable_column, non_nullable_column]));

        Ok(schema)
    }

    fn create_test_schema2() -> Result<SchemaRef> {
        let col_a = Field::new("col_a", DataType::Int32, true);
        let col_b = Field::new("col_b", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![col_a, col_b]));
        Ok(schema)
    }

    // Generate a schema which consists of 5 columns (a, b, c, d, e)
    fn create_test_schema3() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, false);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, false);
        let e = Field::new("e", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e]));
        Ok(schema)
    }

    #[tokio::test]
    async fn test_is_column_aligned_nullable() -> Result<()> {
        let schema = create_test_schema()?;
        let params = vec![
            ((true, true), (false, false), Some(true)),
            ((true, true), (false, true), None),
            ((true, true), (true, false), None),
            ((true, false), (false, true), Some(true)),
            ((true, false), (false, false), None),
            ((true, false), (true, true), None),
        ];
        for (
            (physical_desc, physical_nulls_first),
            (req_desc, req_nulls_first),
            expected,
        ) in params
        {
            let physical_ordering = PhysicalSortExpr {
                expr: col("nullable_col", &schema)?,
                options: SortOptions {
                    descending: physical_desc,
                    nulls_first: physical_nulls_first,
                },
            };
            let required_ordering = PhysicalSortExpr {
                expr: col("nullable_col", &schema)?,
                options: SortOptions {
                    descending: req_desc,
                    nulls_first: req_nulls_first,
                },
            };
            let res = check_alignment(&schema, &physical_ordering, &required_ordering)?;
            assert_eq!(res, expected);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_is_column_aligned_non_nullable() -> Result<()> {
        let schema = create_test_schema()?;

        let params = vec![
            ((true, true), (false, false), Some(true)),
            ((true, true), (false, true), Some(true)),
            ((true, true), (true, false), Some(false)),
            ((true, false), (false, true), Some(true)),
            ((true, false), (false, false), Some(true)),
            ((true, false), (true, true), Some(false)),
        ];
        for (
            (physical_desc, physical_nulls_first),
            (req_desc, req_nulls_first),
            expected,
        ) in params
        {
            let physical_ordering = PhysicalSortExpr {
                expr: col("non_nullable_col", &schema)?,
                options: SortOptions {
                    descending: physical_desc,
                    nulls_first: physical_nulls_first,
                },
            };
            let required_ordering = PhysicalSortExpr {
                expr: col("non_nullable_col", &schema)?,
                options: SortOptions {
                    descending: req_desc,
                    nulls_first: req_nulls_first,
                },
            };
            let res = check_alignment(&schema, &physical_ordering, &required_ordering)?;
            assert_eq!(res, expected);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_can_skip_ordering_exhaustive() -> Result<()> {
        let test_schema = create_test_schema3()?;
        // Columns a,c are nullable whereas b,d are not nullable.
        // Source is sorted by a ASC NULLS FIRST, b ASC NULLS FIRST, c ASC NULLS FIRST, d ASC NULLS FIRST
        // Column e is not ordered.
        let sort_exprs = vec![
            sort_expr("a", &test_schema),
            sort_expr("b", &test_schema),
            sort_expr("c", &test_schema),
            sort_expr("d", &test_schema),
        ];
        let exec_unbounded = csv_exec_sorted(&test_schema, sort_exprs, true);

        // test cases consists of vector of tuples. Where each tuple represents a single test case.
        // First field in the tuple is Vec<str> where each element in the vector represents PARTITION BY columns
        // For instance `vec!["a", "b"]` corresponds to PARTITION BY a, b
        // Second field in the tuple is Vec<str> where each element in the vector represents ORDER BY columns
        // For instance, vec!["c"], corresponds to ORDER BY c ASC NULLS FIRST, (ordering is default ordering. We do not check
        // for reversibility in this test).
        // Third field in the tuple is Option<PartitionSearchMode>, which corresponds to expected algorithm mode.
        // None represents that existing ordering is not sufficient to run executor with any one of the algorithms
        // (We need to add SortExec to be able to run it).
        // Some(PartitionSearchMode) represents, we can run algorithm with existing ordering; and algorithm should work in
        // PartitionSearchMode.
        let test_cases = vec![
            (vec!["a"], vec!["a"], Some(Sorted)),
            (vec!["a"], vec!["b"], Some(Sorted)),
            (vec!["a"], vec!["c"], None),
            (vec!["a"], vec!["a", "b"], Some(Sorted)),
            (vec!["a"], vec!["b", "c"], Some(Sorted)),
            (vec!["a"], vec!["a", "c"], None),
            (vec!["a"], vec!["a", "b", "c"], Some(Sorted)),
            (vec!["b"], vec!["a"], Some(Linear)),
            (vec!["b"], vec!["b"], None),
            (vec!["b"], vec!["c"], None),
            (vec!["b"], vec!["a", "b"], Some(Linear)),
            (vec!["b"], vec!["b", "c"], None),
            (vec!["b"], vec!["a", "c"], Some(Linear)),
            (vec!["b"], vec!["a", "b", "c"], Some(Linear)),
            (vec!["c"], vec!["a"], Some(Linear)),
            (vec!["c"], vec!["b"], None),
            (vec!["c"], vec!["c"], None),
            (vec!["c"], vec!["a", "b"], Some(Linear)),
            (vec!["c"], vec!["b", "c"], None),
            (vec!["c"], vec!["a", "c"], Some(Linear)),
            (vec!["c"], vec!["a", "b", "c"], Some(Linear)),
            (vec!["b", "a"], vec!["a"], Some(Sorted)),
            (vec!["b", "a"], vec!["b"], Some(Sorted)),
            (vec!["b", "a"], vec!["c"], Some(Sorted)),
            (vec!["b", "a"], vec!["a", "b"], Some(Sorted)),
            (vec!["b", "a"], vec!["b", "c"], Some(Sorted)),
            (vec!["b", "a"], vec!["a", "c"], Some(Sorted)),
            (vec!["b", "a"], vec!["a", "b", "c"], Some(Sorted)),
            (vec!["c", "b"], vec!["a"], Some(Linear)),
            (vec!["c", "b"], vec!["b"], None),
            (vec!["c", "b"], vec!["c"], None),
            (vec!["c", "b"], vec!["a", "b"], Some(Linear)),
            (vec!["c", "b"], vec!["b", "c"], None),
            (vec!["c", "b"], vec!["a", "c"], Some(Linear)),
            (vec!["c", "b"], vec!["a", "b", "c"], Some(Linear)),
            (vec!["c", "a"], vec!["a"], Some(PartiallySorted(vec![1]))),
            (vec!["c", "a"], vec!["b"], Some(PartiallySorted(vec![1]))),
            (vec!["c", "a"], vec!["c"], Some(PartiallySorted(vec![1]))),
            (
                vec!["c", "a"],
                vec!["a", "b"],
                Some(PartiallySorted(vec![1])),
            ),
            (
                vec!["c", "a"],
                vec!["b", "c"],
                Some(PartiallySorted(vec![1])),
            ),
            (
                vec!["c", "a"],
                vec!["a", "c"],
                Some(PartiallySorted(vec![1])),
            ),
            (
                vec!["c", "a"],
                vec!["a", "b", "c"],
                Some(PartiallySorted(vec![1])),
            ),
            (vec!["c", "b", "a"], vec!["a"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["b"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["c"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["a", "b"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["b", "c"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["a", "c"], Some(Sorted)),
            (vec!["c", "b", "a"], vec!["a", "b", "c"], Some(Sorted)),
        ];
        for (case_idx, test_case) in test_cases.iter().enumerate() {
            let (partition_by_columns, order_by_params, expected) = &test_case;
            let mut partition_by_exprs = vec![];
            for col_name in partition_by_columns {
                partition_by_exprs.push(col(col_name, &test_schema)?);
            }

            let mut order_by_exprs = vec![];
            for col_name in order_by_params {
                let expr = col(col_name, &test_schema)?;
                // Give default ordering, this is same with input ordering direction
                // In this test we do check for reversibility.
                let options = SortOptions::default();
                order_by_exprs.push(PhysicalSortExpr { expr, options });
            }
            let res =
                can_skip_sort(&partition_by_exprs, &order_by_exprs, &exec_unbounded)?;
            // Since reversibility is not important in this test. Convert Option<(bool, PartitionSearchMode)> to Option<PartitionSearchMode>
            let res = res.map(|(_, mode)| mode);
            assert_eq!(
                res, *expected,
                "Unexpected result for in unbounded test case#: {case_idx:?}, case: {test_case:?}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_can_skip_ordering() -> Result<()> {
        let test_schema = create_test_schema3()?;
        // Columns a,c are nullable whereas b,d are not nullable.
        // Source is sorted by a ASC NULLS FIRST, b ASC NULLS FIRST, c ASC NULLS FIRST, d ASC NULLS FIRST
        // Column e is not ordered.
        let sort_exprs = vec![
            sort_expr("a", &test_schema),
            sort_expr("b", &test_schema),
            sort_expr("c", &test_schema),
            sort_expr("d", &test_schema),
        ];
        let exec_unbounded = csv_exec_sorted(&test_schema, sort_exprs, true);

        // test cases consists of vector of tuples. Where each tuple represents a single test case.
        // First field in the tuple is Vec<str> where each element in the vector represents PARTITION BY columns
        // For instance `vec!["a", "b"]` corresponds to PARTITION BY a, b
        // Second field in the tuple is Vec<(str, bool, bool)> where each element in the vector represents ORDER BY columns
        // For instance, vec![("c", false, false)], corresponds to ORDER BY c ASC NULLS LAST,
        // similarly, vec![("c", true, true)], corresponds to ORDER BY c DESC NULLS FIRST,
        // Third field in the tuple is Option<(bool, PartitionSearchMode)>, which corresponds to expected result.
        // None represents that existing ordering is not sufficient to run executor with any one of the algorithms
        // (We need to add SortExec to be able to run it).
        // Some((bool, PartitionSearchMode)) represents, we can run algorithm with existing ordering. Algorithm should work in
        // PartitionSearchMode, bool field represents whether we should reverse window expressions to run executor with existing ordering.
        // For instance, `Some((false, PartitionSearchMode::Sorted))`, represents that we shouldn't reverse window expressions. And algorithm
        // should work in Sorted mode to work with existing ordering.
        let test_cases = vec![
            // PARTITION BY a, b ORDER BY c ASC NULLS LAST
            (vec!["a", "b"], vec![("c", false, false)], None),
            // ORDER BY c ASC NULLS FIRST
            (vec![], vec![("c", false, true)], None),
            // PARTITION BY b, ORDER BY c ASC NULLS FIRST
            (vec!["b"], vec![("c", false, true)], None),
            // PARTITION BY a, ORDER BY c ASC NULLS FIRST
            (vec!["a"], vec![("c", false, true)], None),
            // PARTITION BY b, ORDER BY c ASC NULLS FIRST
            (
                vec!["a", "b"],
                vec![("c", false, true), ("e", false, true)],
                None,
            ),
            // PARTITION BY a, ORDER BY b ASC NULLS FIRST
            (vec!["a"], vec![("b", false, true)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY a ASC NULLS FIRST
            (vec!["a"], vec![("a", false, true)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY a ASC NULLS LAST
            (vec!["a"], vec![("a", false, false)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY a DESC NULLS FIRST
            (vec!["a"], vec![("a", true, true)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY a DESC NULLS LAST
            (vec!["a"], vec![("a", true, false)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY b ASC NULLS LAST
            (vec!["a"], vec![("b", false, false)], Some((false, Sorted))),
            // PARTITION BY a, ORDER BY b DESC NULLS LAST
            (vec!["a"], vec![("b", true, false)], Some((true, Sorted))),
            // PARTITION BY a, b ORDER BY c ASC NULLS FIRST
            (
                vec!["a", "b"],
                vec![("c", false, true)],
                Some((false, Sorted)),
            ),
            // PARTITION BY b, a ORDER BY c ASC NULLS FIRST
            (
                vec!["b", "a"],
                vec![("c", false, true)],
                Some((false, Sorted)),
            ),
            // PARTITION BY a, b ORDER BY c DESC NULLS LAST
            (
                vec!["a", "b"],
                vec![("c", true, false)],
                Some((true, Sorted)),
            ),
            // PARTITION BY e ORDER BY a ASC NULLS FIRST
            (
                vec!["e"],
                vec![("a", false, true)],
                // For unbounded, expects to work in Linear mode. Shouldn't reverse window function.
                Some((false, Linear)),
            ),
            // PARTITION BY b, c ORDER BY a ASC NULLS FIRST, c ASC NULLS FIRST
            (
                vec!["b", "c"],
                vec![("a", false, true), ("c", false, true)],
                Some((false, Linear)),
            ),
            // PARTITION BY b ORDER BY a ASC NULLS FIRST
            (vec!["b"], vec![("a", false, true)], Some((false, Linear))),
            // PARTITION BY a, e ORDER BY b ASC NULLS FIRST
            (
                vec!["a", "e"],
                vec![("b", false, true)],
                Some((false, PartiallySorted(vec![0]))),
            ),
            // PARTITION BY a, c ORDER BY b ASC NULLS FIRST
            (
                vec!["a", "c"],
                vec![("b", false, true)],
                Some((false, PartiallySorted(vec![0]))),
            ),
            // PARTITION BY c, a ORDER BY b ASC NULLS FIRST
            (
                vec!["c", "a"],
                vec![("b", false, true)],
                Some((false, PartiallySorted(vec![1]))),
            ),
            // PARTITION BY d, b, a ORDER BY c ASC NULLS FIRST
            (
                vec!["d", "b", "a"],
                vec![("c", false, true)],
                Some((false, PartiallySorted(vec![2, 1]))),
            ),
            // PARTITION BY e, b, a ORDER BY c ASC NULLS FIRST
            (
                vec!["e", "b", "a"],
                vec![("c", false, true)],
                Some((false, PartiallySorted(vec![2, 1]))),
            ),
            // PARTITION BY d, a ORDER BY b ASC NULLS FIRST
            (
                vec!["d", "a"],
                vec![("b", false, true)],
                Some((false, PartiallySorted(vec![1]))),
            ),
            // PARTITION BY b, ORDER BY b, a ASC NULLS FIRST
            (
                vec!["a"],
                vec![("b", false, true), ("a", false, true)],
                Some((false, Sorted)),
            ),
            // ORDER BY b, a ASC NULLS FIRST
            (vec![], vec![("b", false, true), ("a", false, true)], None),
        ];
        for (case_idx, test_case) in test_cases.iter().enumerate() {
            let (partition_by_columns, order_by_params, expected) = &test_case;
            let mut partition_by_exprs = vec![];
            for col_name in partition_by_columns {
                partition_by_exprs.push(col(col_name, &test_schema)?);
            }

            let mut order_by_exprs = vec![];
            for (col_name, descending, nulls_first) in order_by_params {
                let expr = col(col_name, &test_schema)?;
                let options = SortOptions {
                    descending: *descending,
                    nulls_first: *nulls_first,
                };
                order_by_exprs.push(PhysicalSortExpr { expr, options });
            }

            assert_eq!(
                can_skip_sort(&partition_by_exprs, &order_by_exprs, &exec_unbounded)?,
                *expected,
                "Unexpected result for in unbounded test case#: {case_idx:?}, case: {test_case:?}"
            );
        }

        Ok(())
    }

    /// Runs the sort enforcement optimizer and asserts the plan
    /// against the original and expected plans
    ///
    /// `$EXPECTED_PLAN_LINES`: input plan
    /// `$EXPECTED_OPTIMIZED_PLAN_LINES`: optimized plan
    /// `$PLAN`: the plan to optimized
    /// `REPARTITION_SORTS`: Flag to set `config.options.optimizer.repartition_sorts` option.
    ///
    macro_rules! assert_optimized {
        ($EXPECTED_PLAN_LINES: expr, $EXPECTED_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr, $REPARTITION_SORTS: expr) => {
            let config = SessionConfig::new().with_repartition_sorts($REPARTITION_SORTS);
            let session_ctx = SessionContext::with_config(config);
            let state = session_ctx.state();

            let physical_plan = $PLAN;
            let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
            let actual: Vec<&str> = formatted.trim().lines().collect();

            let expected_plan_lines: Vec<&str> = $EXPECTED_PLAN_LINES
                .iter().map(|s| *s).collect();

            assert_eq!(
                expected_plan_lines, actual,
                "\n**Original Plan Mismatch\n\nexpected:\n\n{expected_plan_lines:#?}\nactual:\n\n{actual:#?}\n\n"
            );

            let expected_optimized_lines: Vec<&str> = $EXPECTED_OPTIMIZED_PLAN_LINES
                .iter().map(|s| *s).collect();

            // Run the actual optimizer
            let optimized_physical_plan =
                EnforceSorting::new().optimize(physical_plan, state.config_options())?;

            // Get string representation of the plan
            let actual = get_plan_string(&optimized_physical_plan);
            assert_eq!(
                expected_optimized_lines, actual,
                "\n**Optimized Plan Mismatch\n\nexpected:\n\n{expected_optimized_lines:#?}\nactual:\n\n{actual:#?}\n\n"
            );

        };
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let input = sort_exec(vec![sort_expr("non_nullable_col", &schema)], source);
        let physical_plan = sort_exec(vec![sort_expr("nullable_col", &schema)], input);

        let expected_input = vec![
            "SortExec: expr=[nullable_col@0 ASC]",
            "  SortExec: expr=[non_nullable_col@1 ASC]",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[nullable_col@0 ASC]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort_window_multilayer() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);

        let sort_exprs = vec![sort_expr_options(
            "non_nullable_col",
            &source.schema(),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        )];
        let sort = sort_exec(sort_exprs.clone(), source);
        // Add dummy layer propagating Sort above, to test whether sort can be removed from multi layer before
        let coalesce_batches = coalesce_batches_exec(sort);

        let window_agg =
            bounded_window_exec("non_nullable_col", sort_exprs, coalesce_batches);

        let sort_exprs = vec![sort_expr_options(
            "non_nullable_col",
            &window_agg.schema(),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )];

        let sort = sort_exec(sort_exprs.clone(), window_agg);

        // Add dummy layer propagating Sort above, to test whether sort can be removed from multi layer before
        let filter = filter_exec(
            Arc::new(NotExpr::new(
                col("non_nullable_col", schema.as_ref()).unwrap(),
            )),
            sort,
        );

        let physical_plan = bounded_window_exec("non_nullable_col", sort_exprs, filter);

        let expected_input = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "  FilterExec: NOT non_nullable_col@1",
            "    SortExec: expr=[non_nullable_col@1 ASC NULLS LAST]",
            "      BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "        CoalesceBatchesExec: target_batch_size=128",
            "          SortExec: expr=[non_nullable_col@1 DESC]",
            "            MemoryExec: partitions=1, partition_sizes=[0]",
        ];

        let expected_optimized = vec![
            "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(NULL) }]",
            "  FilterExec: NOT non_nullable_col@1",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "      CoalesceBatchesExec: target_batch_size=128",
            "        SortExec: expr=[non_nullable_col@1 DESC]",
            "          MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_add_required_sort() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);

        let sort_exprs = vec![sort_expr("nullable_col", &schema)];

        let physical_plan = sort_preserving_merge_exec(sort_exprs, source);

        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[nullable_col@0 ASC]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort1() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let sort = sort_exec(sort_exprs.clone(), source);
        let spm = sort_preserving_merge_exec(sort_exprs, sort);

        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let sort = sort_exec(sort_exprs.clone(), spm);
        let physical_plan = sort_preserving_merge_exec(sort_exprs, sort);
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC]",
            "    SortPreservingMergeExec: [nullable_col@0 ASC]",
            "      SortExec: expr=[nullable_col@0 ASC]",
            "        MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[nullable_col@0 ASC]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort2() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let sort_exprs = vec![sort_expr("non_nullable_col", &schema)];
        let sort = sort_exec(sort_exprs.clone(), source);
        let spm = sort_preserving_merge_exec(sort_exprs, sort);

        let sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort2 = sort_exec(sort_exprs.clone(), spm);
        let spm2 = sort_preserving_merge_exec(sort_exprs, sort2);

        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let sort3 = sort_exec(sort_exprs, spm2);
        let physical_plan = repartition_exec(repartition_exec(sort3));

        let expected_input = vec![
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "        SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "          SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "            SortExec: expr=[non_nullable_col@1 ASC]",
            "              MemoryExec: partitions=1, partition_sizes=[0]",
        ];

        let expected_optimized = vec![
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort3() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let sort_exprs = vec![sort_expr("non_nullable_col", &schema)];
        let sort = sort_exec(sort_exprs.clone(), source);
        let spm = sort_preserving_merge_exec(sort_exprs, sort);

        let sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let repartition_exec = repartition_exec(spm);
        let sort2 = Arc::new(
            SortExec::new(sort_exprs.clone(), repartition_exec)
                .with_preserve_partitioning(true),
        ) as _;
        let spm2 = sort_preserving_merge_exec(sort_exprs, sort2);

        let physical_plan = aggregate_exec(spm2);

        // When removing a `SortPreservingMergeExec`, make sure that partitioning
        // requirements are not violated. In some cases, we may need to replace
        // it with a `CoalescePartitionsExec` instead of directly removing it.
        let expected_input = vec![
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "          SortExec: expr=[non_nullable_col@1 ASC]",
            "            MemoryExec: partitions=1, partition_sizes=[0]",
        ];

        let expected_optimized = vec![
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort4() -> Result<()> {
        let schema = create_test_schema()?;
        let source1 = repartition_exec(memory_exec(&schema));

        let source2 = repartition_exec(memory_exec(&schema));
        let union = union_exec(vec![source1, source2]);

        let sort_exprs = vec![sort_expr("non_nullable_col", &schema)];
        // let sort = sort_exec(sort_exprs.clone(), union);
        let sort = Arc::new(
            SortExec::new(sort_exprs.clone(), union).with_preserve_partitioning(true),
        ) as _;
        let spm = sort_preserving_merge_exec(sort_exprs, sort);

        let filter = filter_exec(
            Arc::new(NotExpr::new(
                col("non_nullable_col", schema.as_ref()).unwrap(),
            )),
            spm,
        );

        let sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let physical_plan = sort_exec(sort_exprs, filter);

        // When removing a `SortPreservingMergeExec`, make sure that partitioning
        // requirements are not violated. In some cases, we may need to replace
        // it with a `CoalescePartitionsExec` instead of directly removing it.
        let expected_input = vec![
            "SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  FilterExec: NOT non_nullable_col@1",
            "    SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "      SortExec: expr=[non_nullable_col@1 ASC]",
            "        UnionExec",
            "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[0]",
            "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[0]",
        ];

        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "    FilterExec: NOT non_nullable_col@1",
            "      UnionExec",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "          MemoryExec: partitions=1, partition_sizes=[0]",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "          MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort5() -> Result<()> {
        let left_schema = create_test_schema2()?;
        let right_schema = create_test_schema3()?;
        let left_input = memory_exec(&left_schema);
        let parquet_sort_exprs = vec![sort_expr("a", &right_schema)];
        let right_input = parquet_exec_sorted(&right_schema, parquet_sort_exprs);

        let on = vec![(
            Column::new_with_schema("col_a", &left_schema)?,
            Column::new_with_schema("c", &right_schema)?,
        )];
        let join = hash_join_exec(left_input, right_input, on, None, &JoinType::Inner)?;
        let physical_plan = sort_exec(vec![sort_expr("a", &join.schema())], join);

        let expected_input = vec![
            "SortExec: expr=[a@2 ASC]",
            "  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(col_a@0, c@2)]",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC]",
        ];

        let expected_optimized = vec![
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(col_a@0, c@2)]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
            "  ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_spm1() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let input = sort_preserving_merge_exec(
            vec![sort_expr("non_nullable_col", &schema)],
            source,
        );
        let input2 = sort_preserving_merge_exec(
            vec![sort_expr("non_nullable_col", &schema)],
            input,
        );
        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("nullable_col", &schema)], input2);

        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "    SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[nullable_col@0 ASC]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_do_not_remove_sort_with_limit() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort = sort_exec(sort_exprs.clone(), source1);
        let limit = limit_exec(sort);

        let parquet_sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs);

        let union = union_exec(vec![source2, limit]);
        let repartition = repartition_exec(union);
        let physical_plan = sort_preserving_merge_exec(sort_exprs, repartition);

        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "    UnionExec",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "      GlobalLimitExec: skip=0, fetch=100",
            "        LocalLimitExec: fetch=100",
            "          SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "            ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];

        // We should keep the bottom `SortExec`.
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "      UnionExec",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "        GlobalLimitExec: skip=0, fetch=100",
            "          LocalLimitExec: fetch=100",
            "            SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "              ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_change_wrong_sorting() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort = sort_exec(vec![sort_exprs[0].clone()], source);
        let physical_plan = sort_preserving_merge_exec(sort_exprs, sort);
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC]",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_change_wrong_sorting2() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let spm1 = sort_preserving_merge_exec(sort_exprs.clone(), source);
        let sort2 = sort_exec(vec![sort_exprs[0].clone()], spm1);
        let physical_plan =
            sort_preserving_merge_exec(vec![sort_exprs[1].clone()], sort2);

        let expected_input = vec![
            "SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC]",
            "    SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[non_nullable_col@1 ASC]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_sorted() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let sort = sort_exec(sort_exprs.clone(), source1);

        let source2 = parquet_exec_sorted(&schema, sort_exprs.clone());

        let union = union_exec(vec![source2, sort]);
        let physical_plan = sort_preserving_merge_exec(sort_exprs, union);

        // one input to the union is already sorted, one is not.
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should not add a sort at the output of the union, input plan should not be changed
        let expected_optimized = expected_input.clone();
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let sort = sort_exec(sort_exprs.clone(), source1);

        let parquet_sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs);

        let union = union_exec(vec![source2, sort]);
        let physical_plan = sort_preserving_merge_exec(sort_exprs, union);

        // one input to the union is already sorted, one is not.
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should not add a sort at the output of the union, input plan should not be changed
        let expected_optimized = expected_input.clone();
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted2() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort = sort_exec(sort_exprs.clone(), source1);

        let parquet_sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs);

        let union = union_exec(vec![source2, sort]);
        let physical_plan = sort_preserving_merge_exec(sort_exprs, union);

        // Input is an invalid plan. In this case rule should add required sorting in appropriate places.
        // First ParquetExec has output ordering(nullable_col@0 ASC). However, it doesn't satisfy the
        // required ordering of SortPreservingMergeExec.
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];

        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted3() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort1 = sort_exec(sort_exprs1, source1.clone());
        let sort_exprs2 = vec![sort_expr("nullable_col", &schema)];
        let sort2 = sort_exec(sort_exprs2, source1);

        let parquet_sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs.clone());

        let union = union_exec(vec![sort1, source2, sort2]);
        let physical_plan = sort_preserving_merge_exec(parquet_sort_exprs, union);

        // First input to the union is not Sorted (SortExec is finer than required ordering by the SortPreservingMergeExec above).
        // Second input to the union is already Sorted (matches with the required ordering by the SortPreservingMergeExec above).
        // Third input to the union is not Sorted (SortExec is matches required ordering by the SortPreservingMergeExec above).
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should adjust sorting in the first input of the union such that it is not unnecessarily fine
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted4() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort_exprs2 = vec![sort_expr("nullable_col", &schema)];
        let sort1 = sort_exec(sort_exprs2.clone(), source1.clone());
        let sort2 = sort_exec(sort_exprs2.clone(), source1);

        let source2 = parquet_exec_sorted(&schema, sort_exprs2);

        let union = union_exec(vec![sort1, source2, sort2]);
        let physical_plan = sort_preserving_merge_exec(sort_exprs1, union);

        // Ordering requirement of the `SortPreservingMergeExec` is not met.
        // Should modify the plan to ensure that all three inputs to the
        // `UnionExec` satisfy the ordering, OR add a single sort after
        // the `UnionExec` (both of which are equally good for this example).
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted5() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort_exprs2 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr_options(
                "non_nullable_col",
                &schema,
                SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            ),
        ];
        let sort_exprs3 = vec![sort_expr("nullable_col", &schema)];
        let sort1 = sort_exec(sort_exprs1, source1.clone());
        let sort2 = sort_exec(sort_exprs2, source1);

        let union = union_exec(vec![sort1, sort2]);
        let physical_plan = sort_preserving_merge_exec(sort_exprs3, union);

        // The `UnionExec` doesn't preserve any of the inputs ordering in the
        // example below. However, we should be able to change the unnecessarily
        // fine `SortExec`s below with required `SortExec`s that are absolutely necessary.
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 DESC NULLS LAST]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted6() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs1 = vec![sort_expr("nullable_col", &schema)];
        let sort1 = sort_exec(sort_exprs1, source1.clone());
        let sort_exprs2 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let repartition = repartition_exec(source1);
        let spm = sort_preserving_merge_exec(sort_exprs2, repartition);

        let parquet_sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs.clone());

        let union = union_exec(vec![sort1, source2, spm]);
        let physical_plan = sort_preserving_merge_exec(parquet_sort_exprs, union);

        // The plan is not valid as it is -- the input ordering requirement
        // of the `SortPreservingMergeExec` under the third child of the
        // `UnionExec` is not met. We should add a `SortExec` below it.
        // At the same time, this ordering requirement is unnecessarily fine.
        // The final plan should be valid AND the ordering of the third child
        // shouldn't be finer than necessary.
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // Should adjust the requirement in the third input of the union so
        // that it is not unnecessarily fine.
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted7() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort_exprs3 = vec![sort_expr("nullable_col", &schema)];
        let sort1 = sort_exec(sort_exprs1.clone(), source1.clone());
        let sort2 = sort_exec(sort_exprs1, source1);

        let union = union_exec(vec![sort1, sort2]);
        let physical_plan = sort_preserving_merge_exec(sort_exprs3, union);

        // Union has unnecessarily fine ordering below it. We should be able to replace them with absolutely necessary ordering.
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // Union preserves the inputs ordering and we should not change any of the SortExecs under UnionExec
        let expected_output = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_output, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted8() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort_exprs2 = vec![
            sort_expr_options(
                "nullable_col",
                &schema,
                SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            ),
            sort_expr_options(
                "non_nullable_col",
                &schema,
                SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            ),
        ];
        let sort1 = sort_exec(sort_exprs1, source1.clone());
        let sort2 = sort_exec(sort_exprs2, source1);

        let physical_plan = union_exec(vec![sort1, sort2]);

        // The `UnionExec` doesn't preserve any of the inputs ordering in the
        // example below.
        let expected_input = vec![
            "UnionExec",
            "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "  SortExec: expr=[nullable_col@0 DESC NULLS LAST,non_nullable_col@1 DESC NULLS LAST]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // Since `UnionExec` doesn't preserve ordering in the plan above.
        // We shouldn't keep SortExecs in the plan.
        let expected_optimized = vec![
            "UnionExec",
            "  ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "  ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_window_multi_path_sort() -> Result<()> {
        let schema = create_test_schema()?;

        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort_exprs2 = vec![sort_expr("nullable_col", &schema)];
        // reverse sorting of sort_exprs2
        let sort_exprs3 = vec![sort_expr_options(
            "nullable_col",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        )];
        let source1 = parquet_exec_sorted(&schema, sort_exprs1);
        let source2 = parquet_exec_sorted(&schema, sort_exprs2);
        let sort1 = sort_exec(sort_exprs3.clone(), source1);
        let sort2 = sort_exec(sort_exprs3.clone(), source2);

        let union = union_exec(vec![sort1, sort2]);
        let spm = sort_preserving_merge_exec(sort_exprs3.clone(), union);
        let physical_plan = bounded_window_exec("nullable_col", sort_exprs3, spm);

        // The `WindowAggExec` gets its sorting from multiple children jointly.
        // During the removal of `SortExec`s, it should be able to remove the
        // corresponding SortExecs together. Also, the inputs of these `SortExec`s
        // are not necessarily the same to be able to remove them.
        let expected_input = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "  SortPreservingMergeExec: [nullable_col@0 DESC NULLS LAST]",
            "    UnionExec",
            "      SortExec: expr=[nullable_col@0 DESC NULLS LAST]",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC]",
            "      SortExec: expr=[nullable_col@0 DESC NULLS LAST]",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        ];
        let expected_optimized = vec![
            "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(NULL) }]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC]",
            "    UnionExec",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_window_multi_path_sort2() -> Result<()> {
        let schema = create_test_schema()?;

        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort_exprs2 = vec![sort_expr("nullable_col", &schema)];
        let source1 = parquet_exec_sorted(&schema, sort_exprs2.clone());
        let source2 = parquet_exec_sorted(&schema, sort_exprs2.clone());
        let sort1 = sort_exec(sort_exprs1.clone(), source1);
        let sort2 = sort_exec(sort_exprs1.clone(), source2);

        let union = union_exec(vec![sort1, sort2]);
        let spm = Arc::new(SortPreservingMergeExec::new(sort_exprs1, union)) as _;
        let physical_plan = bounded_window_exec("nullable_col", sort_exprs2, spm);

        // The `WindowAggExec` can get its required sorting from the leaf nodes directly.
        // The unnecessary SortExecs should be removed
        let expected_input = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "    UnionExec",
            "      SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "      SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        ];
        let expected_optimized = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC]",
            "    UnionExec",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_union_inputs_different_sorted_with_limit() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let sort_exprs1 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];
        let sort_exprs2 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr_options(
                "non_nullable_col",
                &schema,
                SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            ),
        ];
        let sort_exprs3 = vec![sort_expr("nullable_col", &schema)];
        let sort1 = sort_exec(sort_exprs1, source1.clone());

        let sort2 = sort_exec(sort_exprs2, source1);
        let limit = local_limit_exec(sort2);
        let limit = global_limit_exec(limit);

        let union = union_exec(vec![sort1, limit]);
        let physical_plan = sort_preserving_merge_exec(sort_exprs3, union);

        // Should not change the unnecessarily fine `SortExec`s because there is `LimitExec`
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    GlobalLimitExec: skip=0, fetch=100",
            "      LocalLimitExec: fetch=100",
            "        SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 DESC NULLS LAST]",
            "          ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    GlobalLimitExec: skip=0, fetch=100",
            "      LocalLimitExec: fetch=100",
            "        SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 DESC NULLS LAST]",
            "          ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_sort_merge_join_order_by_left() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;

        let left = parquet_exec(&left_schema);
        let right = parquet_exec(&right_schema);

        // Join on (nullable_col == col_a)
        let join_on = vec![(
            Column::new_with_schema("nullable_col", &left.schema()).unwrap(),
            Column::new_with_schema("col_a", &right.schema()).unwrap(),
        )];

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
        ];
        for join_type in join_types {
            let join =
                sort_merge_join_exec(left.clone(), right.clone(), &join_on, &join_type);
            let sort_exprs = vec![
                sort_expr("nullable_col", &join.schema()),
                sort_expr("non_nullable_col", &join.schema()),
            ];
            let physical_plan = sort_preserving_merge_exec(sort_exprs.clone(), join);

            let join_plan = format!(
                "SortMergeJoin: join_type={join_type}, on=[(nullable_col@0, col_a@0)]"
            );
            let join_plan2 = format!(
                "  SortMergeJoin: join_type={join_type}, on=[(nullable_col@0, col_a@0)]"
            );
            let expected_input = vec![
                "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
                join_plan2.as_str(),
                "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
            ];
            let expected_optimized = match join_type {
                JoinType::Inner
                | JoinType::Left
                | JoinType::LeftSemi
                | JoinType::LeftAnti => {
                    // can push down the sort requirements and save 1 SortExec
                    vec![
                        join_plan.as_str(),
                        "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
                        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                        "  SortExec: expr=[col_a@0 ASC]",
                        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
                    ]
                }
                _ => {
                    // can not push down the sort requirements
                    vec![
                        "SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
                        join_plan2.as_str(),
                        "    SortExec: expr=[nullable_col@0 ASC]",
                        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                        "    SortExec: expr=[col_a@0 ASC]",
                        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
                    ]
                }
            };
            assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_sort_merge_join_order_by_right() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;

        let left = parquet_exec(&left_schema);
        let right = parquet_exec(&right_schema);

        // Join on (nullable_col == col_a)
        let join_on = vec![(
            Column::new_with_schema("nullable_col", &left.schema()).unwrap(),
            Column::new_with_schema("col_a", &right.schema()).unwrap(),
        )];

        let join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::RightAnti,
        ];
        for join_type in join_types {
            let join =
                sort_merge_join_exec(left.clone(), right.clone(), &join_on, &join_type);
            let sort_exprs = vec![
                sort_expr("col_a", &join.schema()),
                sort_expr("col_b", &join.schema()),
            ];
            let physical_plan = sort_preserving_merge_exec(sort_exprs, join);

            let join_plan = format!(
                "SortMergeJoin: join_type={join_type}, on=[(nullable_col@0, col_a@0)]"
            );
            let spm_plan = match join_type {
                JoinType::RightAnti => {
                    "SortPreservingMergeExec: [col_a@0 ASC,col_b@1 ASC]"
                }
                _ => "SortPreservingMergeExec: [col_a@2 ASC,col_b@3 ASC]",
            };
            let join_plan2 = format!(
                "  SortMergeJoin: join_type={join_type}, on=[(nullable_col@0, col_a@0)]"
            );
            let expected_input = vec![
                spm_plan,
                join_plan2.as_str(),
                "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
            ];
            let expected_optimized = match join_type {
                JoinType::Inner | JoinType::Right | JoinType::RightAnti => {
                    // can push down the sort requirements and save 1 SortExec
                    vec![
                        join_plan.as_str(),
                        "  SortExec: expr=[nullable_col@0 ASC]",
                        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                        "  SortExec: expr=[col_a@0 ASC,col_b@1 ASC]",
                        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
                    ]
                }
                _ => {
                    // can not push down the sort requirements for Left and Full join.
                    vec![
                        "SortExec: expr=[col_a@2 ASC,col_b@3 ASC]",
                        join_plan2.as_str(),
                        "    SortExec: expr=[nullable_col@0 ASC]",
                        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                        "    SortExec: expr=[col_a@0 ASC]",
                        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
                    ]
                }
            };
            assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_sort_merge_join_complex_order_by() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;

        let left = parquet_exec(&left_schema);
        let right = parquet_exec(&right_schema);

        // Join on (nullable_col == col_a)
        let join_on = vec![(
            Column::new_with_schema("nullable_col", &left.schema()).unwrap(),
            Column::new_with_schema("col_a", &right.schema()).unwrap(),
        )];

        let join = sort_merge_join_exec(left, right, &join_on, &JoinType::Inner);

        // order by (col_b, col_a)
        let sort_exprs1 = vec![
            sort_expr("col_b", &join.schema()),
            sort_expr("col_a", &join.schema()),
        ];
        let physical_plan = sort_preserving_merge_exec(sort_exprs1, join.clone());

        let expected_input = vec![
            "SortPreservingMergeExec: [col_b@3 ASC,col_a@2 ASC]",
            "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
        ];

        // can not push down the sort requirements, need to add SortExec
        let expected_optimized = vec![
            "SortExec: expr=[col_b@3 ASC,col_a@2 ASC]",
            "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[col_a@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        // order by (nullable_col, col_b, col_a)
        let sort_exprs2 = vec![
            sort_expr("nullable_col", &join.schema()),
            sort_expr("col_b", &join.schema()),
            sort_expr("col_a", &join.schema()),
        ];
        let physical_plan = sort_preserving_merge_exec(sort_exprs2, join);

        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,col_b@3 ASC,col_a@2 ASC]",
            "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
        ];

        // can not push down the sort requirements, need to add SortExec
        let expected_optimized = vec![
            "SortExec: expr=[nullable_col@0 ASC,col_b@3 ASC,col_a@2 ASC]",
            "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[col_a@0 ASC]",
            "      ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_sort_window_exec() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);

        let sort_exprs1 = vec![sort_expr("nullable_col", &schema)];
        let sort_exprs2 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];

        let sort1 = sort_exec(sort_exprs1.clone(), source);
        let window_agg1 =
            bounded_window_exec("non_nullable_col", sort_exprs1.clone(), sort1);
        let window_agg2 =
            bounded_window_exec("non_nullable_col", sort_exprs2, window_agg1);
        // let filter_exec = sort_exec;
        let physical_plan =
            bounded_window_exec("non_nullable_col", sort_exprs1, window_agg2);

        let expected_input = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "      SortExec: expr=[nullable_col@0 ASC]",
            "        MemoryExec: partitions=1, partition_sizes=[0]",
        ];

        let expected_optimized = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "      SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "        MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_multilayer_coalesce_partitions() -> Result<()> {
        let schema = create_test_schema()?;

        let source1 = parquet_exec(&schema);
        let repartition = repartition_exec(source1);
        let coalesce = Arc::new(CoalescePartitionsExec::new(repartition)) as _;
        // Add dummy layer propagating Sort above, to test whether sort can be removed from multi layer before
        let filter = filter_exec(
            Arc::new(NotExpr::new(
                col("non_nullable_col", schema.as_ref()).unwrap(),
            )),
            coalesce,
        );
        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let physical_plan = sort_exec(sort_exprs, filter);

        // CoalescePartitionsExec and SortExec are not directly consecutive. In this case
        // we should be able to parallelize Sorting also (given that executors in between don't require)
        // single partition.
        let expected_input = vec![
            "SortExec: expr=[nullable_col@0 ASC]",
            "  FilterExec: NOT non_nullable_col@1",
            "    CoalescePartitionsExec",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC]",
            "    FilterExec: NOT non_nullable_col@1",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_coalesce_propagate() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);
        let repartition = repartition_exec(source);
        let coalesce_partitions = Arc::new(CoalescePartitionsExec::new(repartition));
        let repartition = repartition_exec(coalesce_partitions);
        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        // Add local sort
        let sort = Arc::new(
            SortExec::new(sort_exprs.clone(), repartition)
                .with_preserve_partitioning(true),
        ) as _;
        let spm = sort_preserving_merge_exec(sort_exprs.clone(), sort);
        let sort = sort_exec(sort_exprs, spm);

        let physical_plan = sort.clone();
        // Sort Parallelize rule should end Coalesce + Sort linkage when Sort is Global Sort
        // Also input plan is not valid as it is. We need to add SortExec before SortPreservingMergeExec.
        let expected_input = vec![
            "SortExec: expr=[nullable_col@0 ASC]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CoalescePartitionsExec",
            "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_lost_ordering_bounded() -> Result<()> {
        let schema = create_test_schema3()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, false);
        let repartition_rr = repartition_exec(source);
        let repartition_hash = Arc::new(RepartitionExec::try_new(
            repartition_rr,
            Partitioning::Hash(vec![col("c", &schema).unwrap()], 10),
        )?) as _;
        let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
        let physical_plan = sort_exec(vec![sort_expr("a", &schema)], coalesce_partitions);

        let expected_input = vec![
            "SortExec: expr=[a@0 ASC]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], has_header=false",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC]",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], has_header=false",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_lost_ordering_unbounded() -> Result<()> {
        let schema = create_test_schema3()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec(source);
        let repartition_hash = Arc::new(RepartitionExec::try_new(
            repartition_rr,
            Partitioning::Hash(vec![col("c", &schema).unwrap()], 10),
        )?) as _;
        let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
        let physical_plan = sort_exec(vec![sort_expr("a", &schema)], coalesce_partitions);

        let expected_input = vec![
            "SortExec: expr=[a@0 ASC]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC], has_header=false",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortPreservingRepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC], has_header=false",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_lost_ordering_unbounded_parallelize_off() -> Result<()> {
        let schema = create_test_schema3()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec(source);
        let repartition_hash = Arc::new(RepartitionExec::try_new(
            repartition_rr,
            Partitioning::Hash(vec![col("c", &schema).unwrap()], 10),
        )?) as _;
        let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
        let physical_plan = sort_exec(vec![sort_expr("a", &schema)], coalesce_partitions);

        let expected_input = vec![
            "SortExec: expr=[a@0 ASC]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC], has_header=false",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortPreservingRepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC], has_header=false",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan, false);
        Ok(())
    }
}

mod tests_bug {
    use crate::physical_plan::{collect, displayable, ExecutionPlan};
    use crate::prelude::SessionContext;
    use arrow::util::pretty::print_batches;
    use datafusion_common::Result;
    use datafusion_execution::config::SessionConfig;
    use std::sync::Arc;

    fn print_plan(plan: &Arc<dyn ExecutionPlan>) -> () {
        let formatted = crate::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();
        println!("{:#?}", actual);
    }

    #[tokio::test]
    async fn test_repartition_bug() -> Result<()> {
        let config = SessionConfig::new().with_target_partitions(2);
        let ctx = SessionContext::with_config(config);

        ctx.sql(
            "CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER) as VALUES
          (0, 0, 0),
          (1, 1, 1),
          (2, 2, 2),
          (3, 3, 3)",
        )
        .await?;

        let sql = "SELECT l.col0, LAST_VALUE(r.col1 ORDER BY r.col0) as last_col1
        FROM tab0 as l
        JOIN tab0 as r
        ON l.col0 = r.col0
        GROUP BY l.col0, l.col1, l.col2
        ORDER BY l.col0";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);

        let displayable_plan = displayable(physical_plan.as_ref());
        let formatted = format!("{}", displayable_plan.indent(false));
        let expected = {
            vec![
                "SortPreservingMergeExec: [col0@0 ASC NULLS LAST]",
                "  SortExec: expr=[col0@0 ASC NULLS LAST]",
                "    ProjectionExec: expr=[col0@0 as col0, LAST_VALUE(r.col1) ORDER BY [r.col0 ASC NULLS LAST]@3 as last_col1]",
                "      AggregateExec: mode=FinalPartitioned, gby=[col0@0 as col0, col1@1 as col1, col2@2 as col2], aggr=[LAST_VALUE(r.col1)]",
                "        CoalesceBatchesExec: target_batch_size=8192",
                "          RepartitionExec: partitioning=Hash([col0@0, col1@1, col2@2], 2), input_partitions=2",
                "            AggregateExec: mode=Partial, gby=[col0@0 as col0, col1@1 as col1, col2@2 as col2], aggr=[LAST_VALUE(r.col1)], ordering_mode=PartiallyOrdered",
                "              SortExec: expr=[col0@3 ASC NULLS LAST]",
                "                CoalesceBatchesExec: target_batch_size=8192",
                "                  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(col0@0, col0@0)]",
                "                    CoalesceBatchesExec: target_batch_size=8192",
                "                      RepartitionExec: partitioning=Hash([col0@0], 2), input_partitions=2",
                "                        MemoryExec: partitions=2, partition_sizes=[1, 0]",
                "                    CoalesceBatchesExec: target_batch_size=8192",
                "                      RepartitionExec: partitioning=Hash([col0@0], 2), input_partitions=2",
                "                        MemoryExec: partitions=2, partition_sizes=[1, 0]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        print_batches(&batches)?;

        // assert_eq!(0, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_subquery_bug() -> Result<()> {
        let config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::with_config(config);

        ctx.sql(
            "CREATE TABLE t2(t2_id INT, t2_name TEXT, t2_int INT) AS VALUES
            (11, 'z', 3),
            (22, 'y', 1),
            (44, 'x', 3),
            (55, 'w', 3);",
        )
        .await?;

        ctx.sql(
            "CREATE TABLE t1(t1_id INT, t1_name TEXT, t1_int INT) AS VALUES
            (11, 'a', 1),
            (22, 'b', 2),
            (33, 'c', 3),
            (44, 'd', 4);",
        )
        .await?;

        let sql = "SELECT t1_id, (SELECT sum(t2_int) FROM t2 WHERE t2.t2_id = t1.t1_id) as t2_sum from t1";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);

        let displayable_plan = displayable(physical_plan.as_ref());
        let formatted = format!("{}", displayable_plan.indent(false));
        let expected = {
            vec![
                "ProjectionExec: expr=[t1_id@0 as t1_id, SUM(t2.t2_int)@1 as t2_sum]",
                "  CoalesceBatchesExec: target_batch_size=8192",
                "    HashJoinExec: mode=Partitioned, join_type=Left, on=[(t1_id@0, t2_id@1)]",
                "      CoalesceBatchesExec: target_batch_size=8192",
                "        RepartitionExec: partitioning=Hash([t1_id@0], 4), input_partitions=4",
                "          MemoryExec: partitions=4, partition_sizes=[1, 0, 0, 0]",
                "      ProjectionExec: expr=[SUM(t2.t2_int)@1 as SUM(t2.t2_int), t2_id@0 as t2_id]",
                "        AggregateExec: mode=FinalPartitioned, gby=[t2_id@0 as t2_id], aggr=[SUM(t2.t2_int)]",
                "          CoalesceBatchesExec: target_batch_size=8192",
                "            RepartitionExec: partitioning=Hash([t2_id@0], 4), input_partitions=4",
                "              AggregateExec: mode=Partial, gby=[t2_id@0 as t2_id], aggr=[SUM(t2.t2_int)]",
                "                MemoryExec: partitions=4, partition_sizes=[1, 0, 0, 0]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        print_batches(&batches)?;

        // assert_eq!(0, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_group_by_bug() -> Result<()> {
        let config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::with_config(config);

        ctx.sql(
            "CREATE UNBOUNDED EXTERNAL TABLE annotated_data_infinite2 (
              a0 INTEGER,
              a INTEGER,
              b INTEGER,
              c INTEGER,
              d INTEGER
            )
            STORED AS CSV
            WITH HEADER ROW
            WITH ORDER (a ASC, b ASC, c ASC)
            LOCATION 'tests/data/window_2.csv';",
        )
        .await?;

        let sql = "SELECT a, b, FIRST_VALUE(c ORDER BY a DESC) as first_c
          FROM annotated_data_infinite2
          GROUP BY a, b";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);

        let displayable_plan = displayable(physical_plan.as_ref());
        let formatted = format!("{}", displayable_plan.indent(false));
        let expected = {
            vec![
                "ProjectionExec: expr=[a@0 as a, b@1 as b, FIRST_VALUE(annotated_data_infinite2.c) ORDER BY [annotated_data_infinite2.a DESC NULLS FIRST]@2 as first_c]",
                "  AggregateExec: mode=FinalPartitioned, gby=[a@0 as a, b@1 as b], aggr=[LAST_VALUE(annotated_data_infinite2.c)], ordering_mode=FullyOrdered",
                "    CoalesceBatchesExec: target_batch_size=8192",
                "      RepartitionExec: partitioning=Hash([a@0, b@1], 4), input_partitions=1",
                "        AggregateExec: mode=Partial, gby=[a@0 as a, b@1 as b], aggr=[FIRST_VALUE(annotated_data_infinite2.c)], ordering_mode=FullyOrdered",
                "          CsvExec: file_groups={1 group: [[Users/akurmustafa/projects/synnada/arrow-datafusion-synnada/datafusion/core/tests/data/window_2.csv]]}, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST, b@1 ASC NULLS LAST, c@2 ASC NULLS LAST], has_header=true",            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        print_batches(&batches)?;

        // assert_eq!(0, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_group_by_bug2() -> Result<()> {
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::with_config(config);

        ctx.sql(
            "CREATE UNBOUNDED EXTERNAL TABLE annotated_data_infinite2 (
              a0 INTEGER,
              a INTEGER,
              b INTEGER,
              c INTEGER,
              d INTEGER
            )
            STORED AS CSV
            WITH HEADER ROW
            WITH ORDER (a ASC, b ASC, c ASC)
            LOCATION 'tests/data/window_2.csv';",
        )
        .await?;

        let sql = "SELECT a, b, FIRST_VALUE(c ORDER BY a DESC) as first_c
          FROM annotated_data_infinite2
          GROUP BY a, b";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);

        let displayable_plan = displayable(physical_plan.as_ref());
        let formatted = format!("{}", displayable_plan.indent(false));
        let expected = {
            vec![
                "ProjectionExec: expr=[a@0 as a, b@1 as b, FIRST_VALUE(annotated_data_infinite2.c) ORDER BY [annotated_data_infinite2.a DESC NULLS FIRST]@2 as first_c]",
                "  AggregateExec: mode=Single, gby=[a@0 as a, b@1 as b], aggr=[LAST_VALUE(annotated_data_infinite2.c)], ordering_mode=FullyOrdered",
                "    CsvExec: file_groups={1 group: [[Users/akurmustafa/projects/synnada/arrow-datafusion-synnada/datafusion/core/tests/data/window_2.csv]]}, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST, b@1 ASC NULLS LAST, c@2 ASC NULLS LAST], has_header=true",            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        print_batches(&batches)?;

        // assert_eq!(0, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_group_by_pk() -> Result<()> {
        let config = SessionConfig::new().with_target_partitions(8);
        let ctx = SessionContext::with_config(config);

        ctx.sql(
            "CREATE TABLE sales_global_with_pk (zip_code INT,
          country VARCHAR(3),
          sn INT,
          ts TIMESTAMP,
          currency VARCHAR(3),
          amount FLOAT,
          primary key(sn)
        ) as VALUES
          (0, 'GRC', 0, '2022-01-01 06:00:00'::timestamp, 'EUR', 30.0),
          (1, 'FRA', 1, '2022-01-01 08:00:00'::timestamp, 'EUR', 50.0),
          (1, 'TUR', 2, '2022-01-01 11:30:00'::timestamp, 'TRY', 75.0),
          (1, 'FRA', 3, '2022-01-02 12:00:00'::timestamp, 'EUR', 200.0),
          (1, 'TUR', 4, '2022-01-03 10:00:00'::timestamp, 'TRY', 100.0)",
        )
        .await?;

        let sql = "SELECT *
          FROM(SELECT *, SUM(l.amount) OVER(ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as sum_amount
            FROM sales_global_with_pk AS l
          ) as l
          GROUP BY l.sn
          ORDER BY l.sn";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);

        let displayable_plan = displayable(physical_plan.as_ref());
        let formatted = format!("{}", displayable_plan.indent(false));
        let expected = {
            vec![
                "SortPreservingMergeExec: [sn@2 ASC NULLS LAST]",
                "  SortExec: expr=[sn@2 ASC NULLS LAST]",
                "    ProjectionExec: expr=[zip_code@1 as zip_code, country@2 as country, sn@0 as sn, ts@3 as ts, currency@4 as currency, amount@5 as amount, sum_amount@6 as sum_amount]",
                "      AggregateExec: mode=FinalPartitioned, gby=[sn@0 as sn, zip_code@1 as zip_code, country@2 as country, ts@3 as ts, currency@4 as currency, amount@5 as amount, sum_amount@6 as sum_amount], aggr=[]",
                "        CoalesceBatchesExec: target_batch_size=8192",
                "          RepartitionExec: partitioning=Hash([sn@0, zip_code@1, country@2, ts@3, currency@4, amount@5, sum_amount@6], 8), input_partitions=8",
                "            AggregateExec: mode=Partial, gby=[sn@2 as sn, zip_code@0 as zip_code, country@1 as country, ts@3 as ts, currency@4 as currency, amount@5 as amount, sum_amount@6 as sum_amount], aggr=[]",
                "              RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
                "                ProjectionExec: expr=[zip_code@0 as zip_code, country@1 as country, sn@2 as sn, ts@3 as ts, currency@4 as currency, amount@5 as amount, SUM(l.amount) ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@6 as sum_amount]",
                "                  BoundedWindowAggExec: wdw=[SUM(l.amount) ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING: Ok(Field { name: \"SUM(l.amount) ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }], mode=[Sorted]",
                "                    CoalescePartitionsExec",
                "                      MemoryExec: partitions=8, partition_sizes=[1, 0, 0, 0, 0, 0, 0, 0]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        print_batches(&batches)?;

        // assert_eq!(0, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_window_bug() -> Result<()> {
        let config = SessionConfig::new().with_target_partitions(8);
        let ctx = SessionContext::with_config(config);

        let sql = "WITH _sample_data AS (
         SELECT 1 as a, 'aa' AS b
         UNION ALL
         SELECT 3 as a, 'aa' AS b
         UNION ALL
         SELECT 5 as a, 'bb' AS b
         UNION ALL
         SELECT 7 as a, 'bb' AS b
            ), _data2 AS (
         SELECT
         row_number() OVER (PARTITION BY s.b ORDER BY s.a) AS seq,
         s.a,
         s.b
         FROM _sample_data s
            )
            SELECT d.b, MAX(d.a) AS max_a
            FROM _data2 d
            GROUP BY d.b
            ORDER BY d.b;";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);

        let displayable_plan = displayable(physical_plan.as_ref());
        let formatted = format!("{}", displayable_plan.indent(false));
        let expected = {
            vec![
                "SortPreservingMergeExec: [b@0 ASC NULLS LAST]",
                "  SortExec: expr=[b@0 ASC NULLS LAST]",
                "    ProjectionExec: expr=[b@0 as b, MAX(d.a)@1 as max_a]",
                "      AggregateExec: mode=FinalPartitioned, gby=[b@0 as b], aggr=[MAX(d.a)]",
                "        CoalesceBatchesExec: target_batch_size=8192",
                "          RepartitionExec: partitioning=Hash([b@0], 8), input_partitions=8",
                "            AggregateExec: mode=Partial, gby=[b@1 as b], aggr=[MAX(d.a)]",
                "              RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4",
                "                UnionExec",
                "                  ProjectionExec: expr=[1 as a, aa as b]",
                "                    EmptyExec: produce_one_row=true",
                "                  ProjectionExec: expr=[3 as a, aa as b]",
                "                    EmptyExec: produce_one_row=true",
                "                  ProjectionExec: expr=[5 as a, bb as b]",
                "                    EmptyExec: produce_one_row=true",
                "                  ProjectionExec: expr=[7 as a, bb as b]",
                "                    EmptyExec: produce_one_row=true",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        print_batches(&batches)?;

        // assert_eq!(0, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_first_value() -> Result<()> {
        let config = SessionConfig::new().with_target_partitions(8);
        let ctx = SessionContext::with_config(config);

        ctx.sql(
            "CREATE TABLE sales_global (zip_code INT,
          country VARCHAR(3),
          sn INT,
          ts TIMESTAMP,
          currency VARCHAR(3),
          amount FLOAT
        ) as VALUES
          (0, 'GRC', 0, '2022-01-01 06:00:00'::timestamp, 'EUR', 30.0),
          (1, 'FRA', 1, '2022-01-01 08:00:00'::timestamp, 'EUR', 50.0),
          (1, 'TUR', 2, '2022-01-01 11:30:00'::timestamp, 'TRY', 75.0),
          (1, 'FRA', 3, '2022-01-02 12:00:00'::timestamp, 'EUR', 200.0),
          (1, 'TUR', 4, '2022-01-03 10:00:00'::timestamp, 'TRY', 100.0),
          (0, 'GRC', 4, '2022-01-03 10:00:00'::timestamp, 'EUR', 80.0)",
        )
        .await?;

        let sql = "SELECT FIRST_VALUE(amount ORDER BY ts ASC) AS fv1,
      LAST_VALUE(amount ORDER BY ts ASC) AS fv2
      FROM sales_global";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);

        let displayable_plan = displayable(physical_plan.as_ref());
        let formatted = format!("{}", displayable_plan.indent(false));
        let expected = {
            vec![
                "ProjectionExec: expr=[FIRST_VALUE(sales_global.amount) ORDER BY [sales_global.ts ASC NULLS LAST]@0 as fv1, LAST_VALUE(sales_global.amount) ORDER BY [sales_global.ts ASC NULLS LAST]@1 as fv2]",
                "  AggregateExec: mode=Final, gby=[], aggr=[FIRST_VALUE(sales_global.amount), LAST_VALUE(sales_global.amount)]",
                "    CoalescePartitionsExec",
                "      AggregateExec: mode=Partial, gby=[], aggr=[FIRST_VALUE(sales_global.amount), LAST_VALUE(sales_global.amount)]",
                "        SortExec: expr=[ts@0 ASC NULLS LAST]",
                "          MemoryExec: partitions=8, partition_sizes=[1, 0, 0, 0, 0, 0, 0, 0]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        print_batches(&batches)?;

        // assert_eq!(0, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_joins() -> Result<()> {
        let mut config = SessionConfig::new()
            .with_repartition_joins(false)
            .with_target_partitions(1)
            .with_round_robin_repartition(true)
            .with_batch_size(4096);
        let ctx = SessionContext::with_config(config.clone());

        ctx.sql(
            "CREATE TABLE join_t1(t1_id INT UNSIGNED, t1_name VARCHAR, t1_int INT UNSIGNED)
        AS VALUES
        (11, 'a', 1),
        (22, 'b', 2),
        (33, 'c', 3),
        (44, 'd', 4);",
        )
            .await?;

        ctx.sql(
            "CREATE TABLE join_t2(t2_id INT UNSIGNED, t2_name VARCHAR, t2_int INT UNSIGNED)
            AS VALUES
            (11, 'z', 3),
            (22, 'y', 1),
            (44, 'x', 3),
            (55, 'w', 3);",
        )
            .await?;

        let mut state = ctx.state();
        state.config_mut().options_mut().execution.target_partitions = 2;
        // let config = config.with_target_partitions(2);
        let ctx = SessionContext::with_state(state);

        // let ctx = SessionContext::with_config(config);
        let sql = "select *, join_t1.t1_id + 11
        from join_t1, join_t2
        where join_t1.t1_id + 11 = join_t2.t2_id";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);

        let displayable_plan = displayable(physical_plan.as_ref());
        let formatted = format!("{}", displayable_plan.indent(false));
        let expected = {
            vec![
                "ProjectionExec: expr=[t1_id@0 as t1_id, t1_name@1 as t1_name, t1_int@2 as t1_int, t2_id@3 as t2_id, t2_name@4 as t2_name, t2_int@5 as t2_int, CAST(t1_id@0 AS Int64) + 11 as join_t1.t1_id + Int64(11)]",
                "  ProjectionExec: expr=[t1_id@0 as t1_id, t1_name@1 as t1_name, t1_int@2 as t1_int, t2_id@4 as t2_id, t2_name@5 as t2_name, t2_int@6 as t2_int]",
                "    CoalesceBatchesExec: target_batch_size=4096",
                "      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(join_t1.t1_id + Int64(11)@3, CAST(join_t2.t2_id AS Int64)@3)]",
                "        CoalescePartitionsExec",
                "          ProjectionExec: expr=[t1_id@0 as t1_id, t1_name@1 as t1_name, t1_int@2 as t1_int, CAST(t1_id@0 AS Int64) + 11 as join_t1.t1_id + Int64(11)]",
                "            RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
                "              MemoryExec: partitions=1, partition_sizes=[1]",
                "        ProjectionExec: expr=[t2_id@0 as t2_id, t2_name@1 as t2_name, t2_int@2 as t2_int, CAST(t2_id@0 AS Int64) as CAST(join_t2.t2_id AS Int64)]",
                "          RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
                "            MemoryExec: partitions=1, partition_sizes=[1]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        print_batches(&batches)?;

        // assert_eq!(0, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_union1() -> Result<()> {
        let mut config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::with_config(config.clone());

        ctx.sql(
            "CREATE TABLE t1(
              id INT,
              name TEXT,
            ) as VALUES
              (1, 'Alex'),
              (2, 'Bob'),
              (3, 'Alice')
            ;",
        )
        .await?;

        ctx.sql(
            "CREATE TABLE t2(
              id TINYINT,
              name TEXT,
            ) as VALUES
              (1, 'Alex'),
              (2, 'Bob'),
              (3, 'John')
            ;",
        )
        .await?;

        let mut state = ctx.state();
        // state.config_mut().options_mut().execution.target_partitions = 2;
        let ctx = SessionContext::with_state(state);

        let sql = "(
            SELECT name FROM t1
            EXCEPT
            SELECT name FROM t2
        )
        UNION ALL
        (
            SELECT name FROM t2
            EXCEPT
            SELECT name FROM t1
        )";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);

        let displayable_plan = displayable(physical_plan.as_ref());
        let formatted = format!("{}", displayable_plan.indent(false));
        let expected = {
            vec![
                "InterleaveExec",
                "  CoalesceBatchesExec: target_batch_size=8192",
                "    HashJoinExec: mode=Partitioned, join_type=LeftAnti, on=[(name@0, name@0)]",
                "      AggregateExec: mode=FinalPartitioned, gby=[name@0 as name], aggr=[]",
                "        CoalesceBatchesExec: target_batch_size=8192",
                "          RepartitionExec: partitioning=Hash([name@0], 4), input_partitions=4",
                "            AggregateExec: mode=Partial, gby=[name@0 as name], aggr=[]",
                "              MemoryExec: partitions=4, partition_sizes=[1, 0, 0, 0]",
                "      CoalesceBatchesExec: target_batch_size=8192",
                "        RepartitionExec: partitioning=Hash([name@0], 4), input_partitions=4",
                "          MemoryExec: partitions=4, partition_sizes=[1, 0, 0, 0]",
                "  CoalesceBatchesExec: target_batch_size=8192",
                "    HashJoinExec: mode=Partitioned, join_type=LeftAnti, on=[(name@0, name@0)]",
                "      AggregateExec: mode=FinalPartitioned, gby=[name@0 as name], aggr=[]",
                "        CoalesceBatchesExec: target_batch_size=8192",
                "          RepartitionExec: partitioning=Hash([name@0], 4), input_partitions=4",
                "            AggregateExec: mode=Partial, gby=[name@0 as name], aggr=[]",
                "              MemoryExec: partitions=4, partition_sizes=[1, 0, 0, 0]",
                "      CoalesceBatchesExec: target_batch_size=8192",
                "        RepartitionExec: partitioning=Hash([name@0], 4), input_partitions=4",
                "          MemoryExec: partitions=4, partition_sizes=[1, 0, 0, 0]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        print_batches(&batches)?;

        // assert_eq!(0, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_join_disable1() -> Result<()> {
        let mut config = SessionConfig::new()
            .with_target_partitions(4)
            .with_repartition_joins(false);
        let ctx = SessionContext::with_config(config.clone());
        ctx.sql(
            "CREATE EXTERNAL TABLE annotated_data (
              a0 INTEGER,
              a INTEGER,
              b INTEGER,
              c INTEGER,
              d INTEGER
            )
            STORED AS CSV
            WITH HEADER ROW
            WITH ORDER (a ASC, b ASC, c ASC)
            LOCATION 'tests/data/window_2.csv';",
        )
        .await?;

        let mut state = ctx.state();
        // state.config_mut().options_mut().execution.target_partitions = 2;
        let ctx = SessionContext::with_state(state);

        let sql = "SELECT t2.a
             FROM annotated_data as t1
             INNER JOIN annotated_data as t2
             ON t1.c = t2.c ORDER BY t2.a
             LIMIT 5";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);

        let displayable_plan = displayable(physical_plan.as_ref());
        let formatted = format!("{}", displayable_plan.indent(false));
        let expected = {
            vec![
                "GlobalLimitExec: skip=0, fetch=5",
                "  SortPreservingMergeExec: [a@0 ASC NULLS LAST], fetch=5",
                "    ProjectionExec: expr=[a@1 as a]",
                "      CoalesceBatchesExec: target_batch_size=8192",
                "        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(c@0, c@1)]",
                "          CsvExec: file_groups={1 group: [[Users/akurmustafa/projects/synnada/arrow-datafusion-synnada/datafusion/core/tests/data/window_2.csv]]}, projection=[c], has_header=true",
                "          RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1",
                "            CsvExec: file_groups={1 group: [[Users/akurmustafa/projects/synnada/arrow-datafusion-synnada/datafusion/core/tests/data/window_2.csv]]}, projection=[a, c], output_ordering=[a@0 ASC NULLS LAST], has_header=true",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        print_batches(&batches)?;

        // assert_eq!(0, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_join_disable2() -> Result<()> {
        let mut config = SessionConfig::new()
            .with_target_partitions(4)
            .with_repartition_joins(false);
        let ctx = SessionContext::with_config(config.clone());
        ctx.sql(
            "CREATE EXTERNAL TABLE annotated_data (
              a0 INTEGER,
              a INTEGER,
              b INTEGER,
              c INTEGER,
              d INTEGER
            )
            STORED AS CSV
            WITH HEADER ROW
            WITH ORDER (a ASC, b ASC, c ASC)
            LOCATION 'tests/data/window_2.csv';",
        )
        .await?;

        let mut state = ctx.state();
        // state.config_mut().options_mut().execution.target_partitions = 2;
        let ctx = SessionContext::with_state(state);

        let sql = "SELECT t2.a as a2, t2.b
            FROM annotated_data as t1
            RIGHT SEMI JOIN annotated_data as t2
            ON t1.d = t2.d AND t1.c = t2.c
            WHERE t2.d = 3
            ORDER BY a2, t2.b
        LIMIT 10";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);

        let displayable_plan = displayable(physical_plan.as_ref());
        let formatted = format!("{}", displayable_plan.indent(false));
        let expected = {
            vec![
                "GlobalLimitExec: skip=0, fetch=10",
                "  SortPreservingMergeExec: [a2@0 ASC NULLS LAST,b@1 ASC NULLS LAST], fetch=10",
                "    ProjectionExec: expr=[a@0 as a2, b@1 as b]",
                "      CoalesceBatchesExec: target_batch_size=8192",
                "        HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(d@1, d@3), (c@0, c@2)]",
                "          CsvExec: file_groups={1 group: [[Users/akurmustafa/projects/synnada/arrow-datafusion-synnada/datafusion/core/tests/data/window_2.csv]]}, projection=[c, d], has_header=true",
                "          CoalesceBatchesExec: target_batch_size=8192",
                "            FilterExec: d@3 = 3",
                "              RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1",
                "                CsvExec: file_groups={1 group: [[Users/akurmustafa/projects/synnada/arrow-datafusion-synnada/datafusion/core/tests/data/window_2.csv]]}, projection=[a, b, c, d], output_ordering=[a@0 ASC NULLS LAST, b@1 ASC NULLS LAST, c@2 ASC NULLS LAST], has_header=true",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        print_batches(&batches)?;

        // assert_eq!(0, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_joins2() -> Result<()> {
        let mut config = SessionConfig::new().with_target_partitions(1);
        config.options_mut().optimizer.prefer_hash_join = false;
        let ctx = SessionContext::with_config(config.clone());
        ctx.sql(
            "CREATE TABLE hashjoin_datatype_table_t1_source(c1 INT, c2 BIGINT, c3 DECIMAL(5,2), c4 VARCHAR)
AS VALUES
(1,    86400000,  1.23,    'abc'),
(2,    172800000, 456.00,  'def'),
(null, 259200000, 789.000, 'ghi'),
(3,    null,      -123.12, 'jkl')
;",
        )
            .await?;

        ctx.sql(
            "CREATE TABLE hashjoin_datatype_table_t1
AS SELECT
  arrow_cast(c1, 'Date32') as c1,
  arrow_cast(c2, 'Date64') as c2,
  c3,
  arrow_cast(c4, 'Dictionary(Int32, Utf8)') as c4
FROM
  hashjoin_datatype_table_t1_source",
        )
        .await?;

        ctx.sql(
            "CREATE TABLE hashjoin_datatype_table_t2_source(c1 INT, c2 BIGINT, c3 DECIMAL(10,2), c4 VARCHAR)
AS VALUES
(1,    86400000,  -123.12,   'abc'),
(null, null,      100000.00, 'abcdefg'),
(null, 259200000, 0.00,      'qwerty'),
(3,   null,       789.000,   'qwe')
;",
        )
            .await?;

        ctx.sql(
            "CREATE TABLE hashjoin_datatype_table_t2
AS SELECT
  arrow_cast(c1, 'Date32') as c1,
  arrow_cast(c2, 'Date64') as c2,
  c3,
  arrow_cast(c4, 'Dictionary(Int32, Utf8)') as c4
FROM
  hashjoin_datatype_table_t2_source",
        )
        .await?;

        let mut state = ctx.state();
        state.config_mut().options_mut().execution.target_partitions = 2;
        let ctx = SessionContext::with_state(state);

        let sql = "select * from hashjoin_datatype_table_t1 t1 join hashjoin_datatype_table_t2 t2 on t1.c1 = t2.c1";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);

        let displayable_plan = displayable(physical_plan.as_ref());
        let formatted = format!("{}", displayable_plan.indent(false));
        let expected = {
            vec![
                "SortMergeJoin: join_type=Inner, on=[(c1@0, c1@0)]",
                "  SortExec: expr=[c1@0 ASC]",
                "    CoalesceBatchesExec: target_batch_size=8192",
                "      RepartitionExec: partitioning=Hash([c1@0], 2), input_partitions=2",
                "        RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
                "          MemoryExec: partitions=1, partition_sizes=[1]",
                "  SortExec: expr=[c1@0 ASC]",
                "    CoalesceBatchesExec: target_batch_size=8192",
                "      RepartitionExec: partitioning=Hash([c1@0], 2), input_partitions=2",
                "        RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
                "          MemoryExec: partitions=1, partition_sizes=[1]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        print_batches(&batches)?;

        // assert_eq!(0, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_joins3() -> Result<()> {
        let mut config = SessionConfig::new().with_target_partitions(1);
        config.options_mut().optimizer.prefer_hash_join = false;
        let ctx = SessionContext::with_config(config.clone());
        ctx.sql(
            "CREATE TABLE hashjoin_datatype_table_t1_source(c1 INT, c2 BIGINT, c3 DECIMAL(5,2), c4 VARCHAR)
AS VALUES
(1,    86400000,  1.23,    'abc'),
(2,    172800000, 456.00,  'def'),
(null, 259200000, 789.000, 'ghi'),
(3,    null,      -123.12, 'jkl')
;",
        )
            .await?;

        ctx.sql(
            "CREATE TABLE hashjoin_datatype_table_t1
AS SELECT
  arrow_cast(c1, 'Date32') as c1,
  arrow_cast(c2, 'Date64') as c2,
  c3,
  arrow_cast(c4, 'Dictionary(Int32, Utf8)') as c4
FROM
  hashjoin_datatype_table_t1_source",
        )
        .await?;

        ctx.sql(
            "CREATE TABLE hashjoin_datatype_table_t2_source(c1 INT, c2 BIGINT, c3 DECIMAL(10,2), c4 VARCHAR)
AS VALUES
(1,    86400000,  -123.12,   'abc'),
(null, null,      100000.00, 'abcdefg'),
(null, 259200000, 0.00,      'qwerty'),
(3,   null,       789.000,   'qwe')
;",
        )
            .await?;

        ctx.sql(
            "CREATE TABLE hashjoin_datatype_table_t2
AS SELECT
  arrow_cast(c1, 'Date32') as c1,
  arrow_cast(c2, 'Date64') as c2,
  c3,
  arrow_cast(c4, 'Dictionary(Int32, Utf8)') as c4
FROM
  hashjoin_datatype_table_t2_source",
        )
        .await?;

        let mut state = ctx.state();
        state.config_mut().options_mut().execution.target_partitions = 2;
        let ctx = SessionContext::with_state(state);

        let sql = "select * from hashjoin_datatype_table_t1 t1 right join hashjoin_datatype_table_t2 t2 on t1.c3 = t2.c3";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);

        let displayable_plan = displayable(physical_plan.as_ref());
        let formatted = format!("{}", displayable_plan.indent(false));
        let expected = {
            vec![
                "ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3, c4@3 as c4, c1@5 as c1, c2@6 as c2, c3@7 as c3, c4@8 as c4]",
                "  SortMergeJoin: join_type=Right, on=[(CAST(t1.c3 AS Decimal128(10, 2))@4, c3@2)]",
                "    SortExec: expr=[CAST(t1.c3 AS Decimal128(10, 2))@4 ASC]",
                "      CoalesceBatchesExec: target_batch_size=8192",
                "        RepartitionExec: partitioning=Hash([CAST(t1.c3 AS Decimal128(10, 2))@4], 2), input_partitions=2",
                "          ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3, c4@3 as c4, CAST(c3@2 AS Decimal128(10, 2)) as CAST(t1.c3 AS Decimal128(10, 2))]",
                "            RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
                "              MemoryExec: partitions=1, partition_sizes=[1]",
                "    SortExec: expr=[c3@2 ASC]",
                "      CoalesceBatchesExec: target_batch_size=8192",
                "        RepartitionExec: partitioning=Hash([c3@2], 2), input_partitions=2",
                "          RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
                "            MemoryExec: partitions=1, partition_sizes=[1]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        print_batches(&batches)?;

        // assert_eq!(0, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_intersections() -> Result<()> {
        let mut config = SessionConfig::new().with_target_partitions(4);
        config.options_mut().optimizer.prefer_hash_join = true;
        let ctx = SessionContext::with_config(config.clone());

        ctx.sql(
            "CREATE EXTERNAL TABLE alltypes_plain STORED AS PARQUET LOCATION '../../parquet-testing/data/alltypes_plain.parquet';",
        )
            .await?;

        let mut state = ctx.state();
        state.config_mut().options_mut().execution.target_partitions = 4;
        let ctx = SessionContext::with_state(state);

        let sql = "SELECT int_col, double_col FROM alltypes_plain where int_col > 0 INTERSECT ALL SELECT int_col, double_col FROM alltypes_plain LIMIT 4";

        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        print_plan(&physical_plan);

        let displayable_plan = displayable(physical_plan.as_ref());
        let formatted = format!("{}", displayable_plan.indent(false));
        let expected = {
            vec![
                "GlobalLimitExec: skip=0, fetch=4",
                "  CoalescePartitionsExec",
                "    CoalesceBatchesExec: target_batch_size=8192",
                "      HashJoinExec: mode=Partitioned, join_type=LeftSemi, on=[(int_col@0, int_col@0), (double_col@1, double_col@1)]",
                "        CoalesceBatchesExec: target_batch_size=8192",
                "          RepartitionExec: partitioning=Hash([int_col@0, double_col@1], 4), input_partitions=4",
                "            CoalesceBatchesExec: target_batch_size=8192",
                "              FilterExec: int_col@0 > 0",
                "                RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1",
                "                  ParquetExec: file_groups={1 group: [[Users/akurmustafa/projects/synnada/arrow-datafusion-synnada/parquet-testing/data/alltypes_plain.parquet]]}, projection=[int_col, double_col], predicate=int_col@4 > 0, pruning_predicate=int_col_max@0 > 0",
                "        CoalesceBatchesExec: target_batch_size=8192",
                "          RepartitionExec: partitioning=Hash([int_col@0, double_col@1], 4), input_partitions=4",
                "            RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1",
                "              ParquetExec: file_groups={1 group: [[Users/akurmustafa/projects/synnada/arrow-datafusion-synnada/parquet-testing/data/alltypes_plain.parquet]]}, projection=[int_col, double_col]",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected, actual,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        print_batches(&batches)?;

        // assert_eq!(0, 1);
        Ok(())
    }
}
