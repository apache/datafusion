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
//! by another SortExec. Therefore, this rule removes it from the physical plan.
use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_optimizer::utils::add_sort_above;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::tree_node::TreeNodeRewritable;
use crate::physical_plan::union::UnionExec;
use crate::physical_plan::windows::{
    BoundedWindowAggExec, PartitionSearchMode, WindowAggExec,
};
use crate::physical_plan::{with_new_children_if_necessary, Distribution, ExecutionPlan};
use datafusion_common::DataFusionError;
use datafusion_physical_expr::utils::{
    convert_to_expr, get_indices_of_matching_exprs, ordering_satisfy,
    ordering_satisfy_concrete,
};
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
use hashbrown::HashSet;
use itertools::{concat, izip, Itertools};
use std::hash::Hash;
use std::sync::Arc;

/// This rule inspects `SortExec`'s in the given physical plan and removes the
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
/// leading to `SortExec`s.
#[derive(Debug, Clone)]
struct ExecTree {
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

/// This object is used within the [EnforceSorting] rule to track the closest
/// `SortExec` descendant(s) for every child of a plan.
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
                let is_union = plan.as_any().is::<UnionExec>();
                // If the executor is a `UnionExec`, and it has an output ordering;
                // then it at least partially maintains some child's output ordering.
                // Therefore, we propagate this information upwards.
                let partially_maintains = is_union && plan.output_ordering().is_some();
                let required_orderings = plan.required_input_ordering();
                let flags = plan.maintains_input_order();
                let children = izip!(flags, item.sort_onwards, required_orderings)
                    .filter_map(|(maintains, element, required_ordering)| {
                        if (required_ordering.is_none()
                            && (maintains || partially_maintains))
                            || is_spm
                        {
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

        let plan = with_new_children_if_necessary(parent_plan, children_plans)?;
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

impl TreeNodeRewritable for PlanWithCorrespondingSort {
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
/// `CoalescePartitionsExec` descendant(s) for every child of a plan.
#[derive(Debug, Clone)]
struct PlanWithCorrespondingCoalescePartitions {
    plan: Arc<dyn ExecutionPlan>,
    // For every child, keep a subtree of `ExecutionPlan`s starting from the
    // child until the `CoalescePartitionsExec`(s) -- could be multiple for
    // n-ary plans like Union -- that affect the output partitioning of the
    // child. If the child has no connection to any `CoalescePartitionsExec`,
    // simplify store None (and not a subtree).
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
                if plan.as_any().is::<CoalescePartitionsExec>() {
                    Some(ExecTree::new(plan, idx, vec![]))
                } else if plan.children().is_empty() {
                    // Plan has no children, there is nothing to propagate.
                    None
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
        let plan = with_new_children_if_necessary(parent_plan, children_plans)?;
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

impl TreeNodeRewritable for PlanWithCorrespondingCoalescePartitions {
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
/// whether we elect to transform CoalescePartitionsExec + SortExec cascades
/// into SortExec + SortPreservingMergeExec cascades, which enables us to
/// perform sorting in parallel.
impl PhysicalOptimizerRule for EnforceSorting {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan_requirements = PlanWithCorrespondingSort::new(plan);
        let adjusted = plan_requirements.transform_up(&ensure_sorting)?;
        if config.optimizer.repartition_sorts {
            let plan_with_coalesce_partitions =
                PlanWithCorrespondingCoalescePartitions::new(adjusted.plan);
            let parallel =
                plan_with_coalesce_partitions.transform_up(&parallelize_sorts)?;
            Ok(parallel.plan)
        } else {
            Ok(adjusted.plan)
        }
    }

    fn name(&self) -> &str {
        "EnforceSorting"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// This function turns plans of the form
///      "SortExec: expr=[a@0 ASC]",
///      "  CoalescePartitionsExec",
///      "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
/// to
///      "SortPreservingMergeExec: [a@0 ASC]",
///      "  SortExec: expr=[a@0 ASC]",
///      "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
/// by following connections from `CoalescePartitionsExec`s to `SortExec`s.
/// By performing sorting in parallel, we can increase performance in some scenarios.
fn parallelize_sorts(
    requirements: PlanWithCorrespondingCoalescePartitions,
) -> Result<Option<PlanWithCorrespondingCoalescePartitions>> {
    let plan = requirements.plan;
    if plan.children().is_empty() {
        return Ok(None);
    }
    let mut coalesce_onwards = requirements.coalesce_onwards;
    // We know that `plan` has children, so `coalesce_onwards` is non-empty.
    if coalesce_onwards[0].is_some() {
        if (is_sort(&plan) || is_sort_preserving_merge(&plan))
            // Make sure that Sort is actually global sort
            && plan.output_partitioning().partition_count() == 1
        {
            // If there is a connection between a `CoalescePartitionsExec` and a
            // Global Sort that satisfy the requirements (i.e. intermediate
            // executors  don't require single partition), then we can
            // replace the `CoalescePartitionsExec`+ GlobalSort cascade with
            // the `SortExec` + `SortPreservingMergeExec`
            // cascade to parallelize sorting.
            let mut prev_layer = plan.clone();
            update_child_to_remove_coalesce(&mut prev_layer, &mut coalesce_onwards[0])?;
            let sort_exprs = get_sort_exprs(&plan)?;
            add_sort_above(&mut prev_layer, sort_exprs.to_vec())?;
            let spm = SortPreservingMergeExec::new(sort_exprs.to_vec(), prev_layer);
            return Ok(Some(PlanWithCorrespondingCoalescePartitions {
                plan: Arc::new(spm),
                coalesce_onwards: vec![None],
            }));
        } else if plan.as_any().is::<CoalescePartitionsExec>() {
            // There is an unnecessary `CoalescePartitionExec` in the plan.
            let mut prev_layer = plan.clone();
            update_child_to_remove_coalesce(&mut prev_layer, &mut coalesce_onwards[0])?;
            let new_plan = plan.with_new_children(vec![prev_layer])?;
            return Ok(Some(PlanWithCorrespondingCoalescePartitions {
                plan: new_plan,
                coalesce_onwards: vec![None],
            }));
        }
    }
    Ok(Some(PlanWithCorrespondingCoalescePartitions {
        plan,
        coalesce_onwards,
    }))
}

/// Checks whether the given executor is a limit;
/// i.e. either a `WindowAggExec` or a `BoundedWindowAggExec`.
fn is_window(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<WindowAggExec>() || plan.as_any().is::<BoundedWindowAggExec>()
}

/// Checks whether the given executor is a limit;
/// i.e. either a `LocalLimitExec` or a `GlobalLimitExec`.
fn is_limit(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<GlobalLimitExec>() || plan.as_any().is::<LocalLimitExec>()
}

/// Checks whether the given executor is a `SortExec`.
fn is_sort(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<SortExec>()
}

/// Checks whether the given executor is a `SortPreservingMergeExec`.
fn is_sort_preserving_merge(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<SortPreservingMergeExec>()
}

/// This function enforces sorting requirements and makes optimizations without
/// violating these requirements whenever possible.
fn ensure_sorting(
    requirements: PlanWithCorrespondingSort,
) -> Result<Option<PlanWithCorrespondingSort>> {
    // Perform naive analysis at the beginning -- remove already-satisfied sorts:
    let plan = requirements.plan;
    let mut children = plan.children();
    if children.is_empty() {
        return Ok(None);
    }
    let mut sort_onwards = requirements.sort_onwards;
    if let Some(result) = analyze_immediate_sort_removal(&plan, &sort_onwards) {
        return Ok(Some(result));
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
                let is_ordering_satisfied = ordering_satisfy_concrete(
                    physical_ordering,
                    required_ordering,
                    || child.equivalence_properties(),
                );
                if !is_ordering_satisfied {
                    // Make sure we preserve the ordering requirements:
                    update_child_to_remove_unnecessary_sort(child, sort_onwards, &plan)?;
                    let sort_expr = required_ordering.to_vec();
                    add_sort_above(child, sort_expr)?;
                    if is_sort(child) {
                        *sort_onwards = Some(ExecTree::new(child.clone(), idx, vec![]));
                    } else {
                        *sort_onwards = None;
                    }
                }
            }
            (Some(required), None) => {
                // Ordering requirement is not met, we should add a `SortExec` to the plan.
                add_sort_above(child, required.to_vec())?;
                *sort_onwards = Some(ExecTree::new(child.clone(), idx, vec![]));
            }
            (None, Some(_)) => {
                // We have a `SortExec` whose effect may be neutralized by
                // another order-imposing operator. Remove or update this sort:
                if !plan.maintains_input_order()[idx] {
                    let count = plan.output_ordering().map_or(0, |e| e.len());
                    if (count > 0) && !is_sort(&plan) {
                        update_child_to_change_finer_sort(child, sort_onwards, count)?;
                    } else {
                        update_child_to_remove_unnecessary_sort(
                            child,
                            sort_onwards,
                            &plan,
                        )?;
                    }
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
                return Ok(Some(result));
            }
        }
        if let Some(exec) = plan.as_any().downcast_ref::<BoundedWindowAggExec>() {
            let window_expr = exec.window_expr();
            let search_mode = &exec.partition_search_mode;
            let input = exec.input();
            let res = can_skip_ordering_util(
                window_expr[0].partition_by(),
                window_expr[0].order_by(),
                input,
            )?;
            if let Some((_, new_mode)) = res {
                if new_mode > *search_mode {
                    let new_plan = Arc::new(BoundedWindowAggExec::try_new(
                        window_expr.to_vec(),
                        input.clone(),
                        input.schema(),
                        exec.partition_keys.clone(),
                        input.output_ordering().map(|elem| elem.to_vec()),
                        new_mode,
                    )?) as _;
                    return Ok(Some(PlanWithCorrespondingSort::new(new_plan)));
                }
            }
        };
    }
    Ok(Some(PlanWithCorrespondingSort {
        plan: plan.with_new_children(children)?,
        sort_onwards,
    }))
}

/// Analyzes a given `SortExec` (`plan`) to determine whether its input already
/// has a finer ordering than this `SortExec` enforces.
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

/// Analyzes a [WindowAggExec] or a [BoundedWindowAggExec] to determine whether
/// it may allow removing a sort.
fn analyze_window_sort_removal(
    sort_tree: &mut ExecTree,
    window_exec: &Arc<dyn ExecutionPlan>,
) -> Result<Option<PlanWithCorrespondingSort>> {
    let (window_expr, partition_keys, search_mode) = if let Some(exec) =
        window_exec.as_any().downcast_ref::<BoundedWindowAggExec>()
    {
        // println!("partition_search_mode:{:?}", exec.partition_search_mode);
        (
            exec.window_expr(),
            &exec.partition_keys,
            exec.partition_search_mode.clone(),
        )
    } else if let Some(exec) = window_exec.as_any().downcast_ref::<WindowAggExec>() {
        (
            exec.window_expr(),
            &exec.partition_keys,
            PartitionSearchMode::Sorted,
        )
    } else {
        return Err(DataFusionError::Plan(
            "Expects to receive either WindowAggExec of BoundedWindowAggExec".to_string(),
        ));
    };
    let orderby_sort_keys = window_expr[0].order_by();

    let (should_reverse, partition_search_mode) = if let Some(res) =
        can_skip_ordering(sort_tree, window_expr[0].partition_by(), orderby_sort_keys)?
    {
        res
    } else {
        // cannot skip sort
        return Ok(None);
    };
    // println!("search_mode:{:?}", search_mode);
    // println!("partition_search_mode:{:?}", partition_search_mode);
    // println!("should_reverse:{:?}", should_reverse);
    if search_mode > partition_search_mode {
        // println!("cannot skip sort, because relaxed");
        // More relaxed than requirement
        // cannot skip sort
        return Ok(None);
    }

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
        let physical_ordering = new_child.output_ordering().map(|elem| elem.to_vec());
        let new_plan = if uses_bounded_memory {
            Arc::new(BoundedWindowAggExec::try_new(
                window_expr,
                new_child,
                new_schema,
                partition_keys.to_vec(),
                physical_ordering,
                partition_search_mode,
            )?) as _
        } else {
            match partition_search_mode {
                PartitionSearchMode::Sorted => Arc::new(WindowAggExec::try_new(
                    window_expr,
                    new_child,
                    new_schema,
                    partition_keys.to_vec(),
                    physical_ordering,
                )?) as _,
                _ => {
                    // For `WindowAggExec` to work correctly PARTITION BY columns should be sorted.
                    // Hence if `PartitionSearchMode` is not `Sorted` we should satisfy required ordering.
                    // Effectively `WindowAggExec` works only in PartitionSearchMode::Sorted mode.
                    let sort_keys =
                        window_exec.required_input_ordering()[0].unwrap_or(&[]);
                    add_sort_above(&mut new_child, sort_keys.to_vec())?;
                    Arc::new(WindowAggExec::try_new(
                        window_expr,
                        new_child,
                        new_schema,
                        partition_keys.to_vec(),
                        Some(sort_keys.to_vec()),
                    )?) as _
                }
            }
        };
        return Ok(Some(PlanWithCorrespondingSort::new(new_plan)));
    }
    Ok(None)
}

fn can_skip_ordering_util(
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
    // indices of the partition by expressions among input ordering expressions
    let pb_indices = get_indices_of_matching_exprs(
        partitionby_exprs,
        &physical_ordering_exprs,
        equal_properties,
    );
    let ordered_merged_indices = get_ordered_merged_indices(&pb_indices, &ob_indices);
    let is_merge_consecutive = is_consecutive_from_zero(&ordered_merged_indices);
    // Check whether (partition by columns) ∪ (order by columns) equal to the (partition by columns).
    // Where `∪` represents set  union.
    let all_partition = compare_set_equality(&ordered_merged_indices, &pb_indices);
    let contains_all_orderbys = ob_indices.len() == orderby_keys.len();
    // Indices of order by columns that doesn't seen in partition by
    // Equivalently (Order by columns) ∖ (Partition by columns) where `∖` represents set difference.
    let unique_ob_indices = get_set_diff_indices(&ob_indices, &pb_indices);
    // Check whether values are in the form n, n+1, n+2, .. n+k.
    let is_unique_ob_indices_consecutive = is_consecutive(&unique_ob_indices);
    let input_orderby_columns = get_at_indices(physical_ordering, &unique_ob_indices)?;
    let expected_orderby_columns = get_at_indices(
        orderby_keys,
        &find_match_indices(&unique_ob_indices, &ob_indices),
    )?;
    let schema = input.schema();
    let nullables = input_orderby_columns
        .iter()
        .map(|elem| elem.expr.nullable(&schema))
        .collect::<Result<Vec<_>>>()?;
    let is_same_ordering = izip!(
        &input_orderby_columns,
        &expected_orderby_columns,
        &nullables
    )
    .all(|(input, expected, is_nullable)| {
        if *is_nullable {
            input.options == expected.options
        } else {
            input.options.descending == expected.options.descending
        }
    });
    let should_reverse = izip!(
        &input_orderby_columns,
        &expected_orderby_columns,
        &nullables
    )
    .all(|(input, expected, is_nullable)| {
        if *is_nullable {
            input.options == !expected.options
        } else {
            // have reversed direction
            input.options.descending != expected.options.descending
        }
    });
    let is_aligned = is_same_ordering || should_reverse;

    // Determine If 0th column in the sort_keys comes from partition by
    let is_first_pb = pb_indices.contains(&0);
    let can_skip_sort = (is_merge_consecutive || (all_partition && is_first_pb))
        && contains_all_orderbys
        && is_aligned
        && is_unique_ob_indices_consecutive;
    if !can_skip_sort {
        return Ok(None);
    }

    let ordered_pb_indices = pb_indices.iter().copied().sorted().collect::<Vec<_>>();
    let is_pb_consecutive = is_consecutive_from_zero(&ordered_pb_indices);
    let contains_all_partition_bys = pb_indices.len() == partitionby_exprs.len();
    let mode = if (is_first_pb && is_pb_consecutive && contains_all_partition_bys)
        || partitionby_exprs.is_empty()
    {
        let first_n = calc_ordering_range(&ordered_pb_indices);
        assert_eq!(first_n, partitionby_exprs.len());
        PartitionSearchMode::Sorted
    } else if is_first_pb {
        let first_n = calc_ordering_range(&ordered_pb_indices);
        assert!(first_n < partitionby_exprs.len());
        let ordered_range = &ordered_pb_indices[0..first_n];
        let partially_ordered_indices = find_match_indices(&pb_indices, ordered_range);
        PartitionSearchMode::PartiallySorted(partially_ordered_indices)
    } else {
        PartitionSearchMode::Linear
    };

    Ok(Some((should_reverse, mode)))
}

// Returns Option<(bool, PartitionSearchMode)>
// None means SortExecs cannot removed from the plan.
// Some means that we can remove SortExecs.
// first entry in the tuple indicates whether we need to reverse window function to calculate true result with existing ordering
// Second entry indicates at which mode PartitionSearcher should work to produce correct result given existing ordering.
fn can_skip_ordering(
    sort_tree: &ExecTree,
    partitionby_exprs: &[Arc<dyn PhysicalExpr>],
    orderby_keys: &[PhysicalSortExpr],
) -> Result<Option<(bool, PartitionSearchMode)>> {
    let mut res = None;
    for sort_any in sort_tree.get_leaves() {
        // Variable `sort_any` will either be a `SortExec` or a
        // `SortPreservingMergeExec`, and both have a single child.
        // Therefore, we can use the 0th index without loss of generality.
        let sort_input = sort_any.children()[0].clone();
        // TODO: Once we can ensure that required ordering information propagates with
        //       the necessary lineage information, compare `physical_ordering` and the
        //       ordering required by the window executor instead of `sort_output_ordering`.
        //       This will enable us to handle cases such as (a,b) -> Sort -> (a,b,c) -> Required(a,b).
        //       Currently, we can not remove such sorts.
        if let Some((should_reverse, mode)) =
            can_skip_ordering_util(partitionby_exprs, orderby_keys, &sort_input)?
        {
            if let Some((first_should_reverse, first_mode)) = &res {
                if *first_should_reverse != should_reverse || first_mode != &mode {
                    return Ok(None);
                }
            } else {
                res = Some((should_reverse, mode));
            };
        } else {
            return Ok(None);
        };
    }
    Ok(res)
}

/// Updates child to remove the unnecessary `CoalescePartitions` below it.
fn update_child_to_remove_coalesce(
    child: &mut Arc<dyn ExecutionPlan>,
    coalesce_onwards: &mut Option<ExecTree>,
) -> Result<()> {
    if let Some(coalesce_onwards) = coalesce_onwards {
        *child = remove_corresponding_coalesce_in_sub_plan(coalesce_onwards)?;
    }
    Ok(())
}

/// Removes the `CoalescePartitions` from the plan in `coalesce_onwards`.
fn remove_corresponding_coalesce_in_sub_plan(
    coalesce_onwards: &mut ExecTree,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(
        if coalesce_onwards
            .plan
            .as_any()
            .is::<CoalescePartitionsExec>()
        {
            // We can safely use the 0th index since we have a `CoalescePartitionsExec`.
            coalesce_onwards.plan.children()[0].clone()
        } else {
            let plan = coalesce_onwards.plan.clone();
            let mut children = plan.children();
            for item in &mut coalesce_onwards.children {
                children[item.idx] = remove_corresponding_coalesce_in_sub_plan(item)?;
            }
            plan.with_new_children(children)?
        },
    )
}

/// Updates child to remove the unnecessary sorting below it.
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
    if is_sort(&sort_onwards.plan) {
        Ok(sort_onwards.plan.children()[0].clone())
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
            let child = &children[0];
            if requires_single_partition
                && child.output_partitioning().partition_count() > 1
            {
                Ok(Arc::new(CoalescePartitionsExec::new(child.clone())))
            } else {
                Ok(child.clone())
            }
        } else {
            plan.clone().with_new_children(children)
        }
    }
}

/// Updates child to modify the unnecessarily fine sorting below it.
fn update_child_to_change_finer_sort(
    child: &mut Arc<dyn ExecutionPlan>,
    sort_onwards: &mut Option<ExecTree>,
    n_sort_expr: usize,
) -> Result<()> {
    if let Some(sort_onwards) = sort_onwards {
        *child = change_finer_sort_in_sub_plan(sort_onwards, n_sort_expr)?;
    }
    Ok(())
}

/// Change the unnecessarily fine sort in `sort_onwards`.
fn change_finer_sort_in_sub_plan(
    sort_onwards: &mut ExecTree,
    n_sort_expr: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = &sort_onwards.plan;
    // A `SortExec` is always at the bottom of the tree.
    if is_sort(plan) {
        let mut prev_layer = plan.children()[0].clone();
        let new_sort_expr = get_sort_exprs(plan)?[0..n_sort_expr].to_vec();
        add_sort_above(&mut prev_layer, new_sort_expr)?;
        *sort_onwards = ExecTree::new(prev_layer.clone(), sort_onwards.idx, vec![]);
        Ok(prev_layer)
    } else {
        let mut children = plan.children();
        for item in &mut sort_onwards.children {
            children[item.idx] = change_finer_sort_in_sub_plan(item, n_sort_expr)?;
        }
        if is_sort_preserving_merge(plan) {
            let new_sort_expr = get_sort_exprs(plan)?[0..n_sort_expr].to_vec();
            let updated_plan = Arc::new(SortPreservingMergeExec::new(
                new_sort_expr,
                children[0].clone(),
            )) as Arc<dyn ExecutionPlan>;
            sort_onwards.plan = updated_plan.clone();
            Ok(updated_plan)
        } else {
            plan.clone().with_new_children(children)
        }
    }
}

/// Converts an [ExecutionPlan] trait object to a [PhysicalSortExpr] slice when possible.
fn get_sort_exprs(sort_any: &Arc<dyn ExecutionPlan>) -> Result<&[PhysicalSortExpr]> {
    if let Some(sort_exec) = sort_any.as_any().downcast_ref::<SortExec>() {
        Ok(sort_exec.expr())
    } else if let Some(sort_preserving_merge_exec) =
        sort_any.as_any().downcast_ref::<SortPreservingMergeExec>()
    {
        Ok(sort_preserving_merge_exec.expr())
    } else {
        Err(DataFusionError::Plan(
            "Given ExecutionPlan is not a SortExec or a SortPreservingMergeExec"
                .to_string(),
        ))
    }
}

// Find the indices of each element if the to_search vector inside the searched vector
fn find_match_indices<T: PartialEq>(to_search: &[T], searched: &[T]) -> Vec<usize> {
    let mut result = vec![];
    for item in to_search {
        if let Some(idx) = searched.iter().position(|e| e.eq(item)) {
            result.push(idx);
        }
    }
    result
}

// Compares the equality of two vectors independent of the ordering and duplicates
// See https://stackoverflow.com/a/42748484/10554257
fn compare_set_equality<T>(a: &[T], b: &[T]) -> bool
where
    T: Eq + Hash,
{
    let a: HashSet<_> = a.iter().collect();
    let b: HashSet<_> = b.iter().collect();
    a == b
}

/// Create a new vector from the elements at the `indices` of `searched` vector
pub fn get_at_indices<T: Clone>(searched: &[T], indices: &[usize]) -> Result<Vec<T>> {
    let mut result = vec![];
    for idx in indices {
        result.push(searched[*idx].clone());
    }
    Ok(result)
}

// Merges vectors `in1` and `in2` (removes duplicates) then sorts the result.
fn get_ordered_merged_indices(in1: &[usize], in2: &[usize]) -> Vec<usize> {
    let set: HashSet<_> = in1.iter().chain(in2.iter()).copied().collect();
    let mut res: Vec<_> = set.into_iter().collect();
    res.sort();
    res
}

// Checks if the vector in the form 0,1,2...n (Consecutive starting from zero)
// Assumes input has ascending order
fn is_consecutive_from_zero(in1: &[usize]) -> bool {
    in1.iter().enumerate().all(|(idx, elem)| idx == *elem)
}

// Checks if the vector in the form 1,2,3,..n (Consecutive) not necessarily starting from zero
// Assumes input has ascending order
fn is_consecutive(in1: &[usize]) -> bool {
    if !in1.is_empty() {
        in1.iter()
            .zip(in1[0]..in1[0] + in1.len())
            .all(|(lhs, rhs)| *lhs == rhs)
    } else {
        true
    }
}

// Returns the vector consisting of elements inside `in1` that are not inside `in2`.
// Resulting vector have the same ordering as `in1` (except elements inside `in2` are removed.)
fn get_set_diff_indices(in1: &[usize], in2: &[usize]) -> Vec<usize> {
    let mut res = vec![];
    for lhs in in1 {
        if !in2.iter().contains(lhs) {
            res.push(*lhs);
        }
    }
    res
}

// Find the largest range that satisfy 0,1,2 .. n in the `in1`
// For 0,1,2,4,5 we would produce 3. meaning 0,1,2 is the largest consecutive range (starting from zero).
// For 1,2,3,4 we would produce 0. Meaning there is no consecutive range (starting from zero).
fn calc_ordering_range(in1: &[usize]) -> usize {
    let mut count = 0;
    for (idx, elem) in in1.iter().enumerate() {
        if idx != *elem {
            break;
        } else {
            count += 1
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::file_format::file_type::FileCompressionType;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::physical_optimizer::dist_enforcement::EnforceDistribution;
    use crate::physical_plan::aggregates::PhysicalGroupBy;
    use crate::physical_plan::aggregates::{AggregateExec, AggregateMode};
    use crate::physical_plan::file_format::{CsvExec, FileScanConfig, ParquetExec};
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::memory::MemoryExec;
    use crate::physical_plan::repartition::RepartitionExec;
    use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use crate::physical_plan::union::UnionExec;
    use crate::physical_plan::windows::create_window_expr;
    use crate::physical_plan::{displayable, Partitioning};
    use crate::prelude::SessionContext;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::{Result, Statistics};
    use datafusion_expr::{AggregateFunction, WindowFrame, WindowFunction};
    use datafusion_physical_expr::expressions::{col, NotExpr};
    use datafusion_physical_expr::PhysicalSortExpr;
    use std::sync::Arc;

    fn create_test_schema() -> Result<SchemaRef> {
        let nullable_column = Field::new("nullable_col", DataType::Int32, true);
        let non_nullable_column = Field::new("non_nullable_col", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![nullable_column, non_nullable_column]));

        Ok(schema)
    }

    // Generate a schema which consists of 5 columns (a, b, c, d, e)
    fn create_test_schema2() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, false);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, false);
        let e = Field::new("e", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e]));

        Ok(schema)
    }

    // Util function to get string representation of a physical plan
    fn get_plan_string(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
        let formatted = displayable(plan.as_ref()).indent().to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();
        actual.iter().map(|elem| elem.to_string()).collect()
    }

    #[tokio::test]
    async fn test_can_skip_ordering() -> Result<()> {
        let test_schema = create_test_schema2()?;
        // Columns a,c are nullable whereas b,d are not nullable.
        // Source is sorted by a ASC NULLS FIRST, b ASC NULLS FIRST, c ASC NULLS FIRST, d ASC NULLS FIRST
        // Column e is not ordered.
        let sort_exprs = vec![
            sort_expr("a", &test_schema),
            sort_expr("b", &test_schema),
            sort_expr("c", &test_schema),
            sort_expr("d", &test_schema),
        ];
        let exec_unbounded = csv_exec_sorted(&test_schema, sort_exprs.clone(), true);
        let exec_bounded = csv_exec_sorted(&test_schema, sort_exprs, false);

        let test_cases = vec![
            // PARTITION BY a, b ORDER BY c ASC NULLS LAST
            (
                vec!["a", "b"],
                // c is nullable hence we cannot calculate result in reverse order without changing ordering.
                vec![("c", false, false)],
                // For unbounded Cannot skip sorting
                None,
            ),
            // ORDER BY c ASC NULLS FIRST
            (
                vec![],
                vec![("c", false, true)],
                // For unbounded Cannot skip sorting
                None,
            ),
            // PARTITION BY b, ORDER BY c ASC NULLS FIRST
            (
                vec!["b"],
                vec![("c", false, true)],
                // For unbounded Cannot skip sorting
                None,
            ),
            // PARTITION BY b, ORDER BY c ASC NULLS FIRST
            (
                vec!["a"],
                vec![("c", false, true)],
                // Cannot skip sorting
                None,
            ),
            // PARTITION BY b, ORDER BY c ASC NULLS FIRST
            (
                vec!["a", "b"],
                vec![("c", false, true), ("e", false, true)],
                // For unbounded Cannot skip sorting
                None,
            ),
            // PARTITION BY a, ORDER BY b ASC NULLS FIRST
            (
                vec!["a"],
                vec![("b", false, true)],
                // For both unbounded and bounded Expects to work in Sorted mode.
                // Shouldn't reverse window function.
                Some((false, PartitionSearchMode::Sorted)),
            ),
            // PARTITION BY a, ORDER BY b ASC NULLS LAST
            (
                vec!["a"],
                vec![("b", false, false)],
                // For both unbounded and bounded Expects to work in Sorted mode.
                // Shouldn't reverse window function.
                Some((false, PartitionSearchMode::Sorted)),
            ),
            // PARTITION BY a, ORDER BY b DESC NULLS LAST
            (
                vec!["a"],
                vec![("b", true, false)],
                // For both unbounded and bounded Expects to work in Sorted mode.
                // Should reverse window function.
                Some((true, PartitionSearchMode::Sorted)),
            ),
            // PARTITION BY a, b ORDER BY c ASC NULLS FIRST
            (
                vec!["a", "b"],
                vec![("c", false, true)],
                // For both unbounded and bounded Expects to work in Sorted mode.
                // Shouldn't reverse window function.
                Some((false, PartitionSearchMode::Sorted)),
            ),
            // PARTITION BY a, b ORDER BY c DESC NULLS LAST
            (
                vec!["a", "b"],
                vec![("c", true, false)],
                // For both unbounded and bounded Expects to work in Sorted mode.
                // Should reverse window function.
                Some((true, PartitionSearchMode::Sorted)),
            ),
            // PARTITION BY e ORDER BY a ASC NULLS FIRST
            (
                vec!["e"],
                vec![("a", false, true)],
                // For unbounded, expects to work in Linear mode. Shouldn't reverse window function.
                Some((false, PartitionSearchMode::Linear)),
            ),
            // PARTITION BY b, c ORDER BY a ASC NULLS FIRST, c ASC NULLS FIRST
            (
                vec!["b", "c"],
                vec![("a", false, true), ("c", false, true)],
                // For unbounded, Expects to work in Linear mode. Shouldn't reverse window function.
                Some((false, PartitionSearchMode::Linear)),
            ),
            // PARTITION BY b ORDER BY a ASC NULLS FIRST
            (
                vec!["b"],
                vec![("a", false, true)],
                // For unbounded, Expects to work in Linear mode. Shouldn't reverse window function.
                Some((false, PartitionSearchMode::Linear)),
            ),
            // PARTITION BY a, e ORDER BY b ASC NULLS FIRST
            (
                vec!["a", "e"],
                vec![("b", false, true)],
                // For unbounded, Expects to work in PartiallySorted mode. Shouldn't reverse window function.
                Some((false, PartitionSearchMode::PartiallySorted(vec![0]))),
            ),
            // PARTITION BY a, c ORDER BY b ASC NULLS FIRST
            (
                vec!["a", "c"],
                vec![("b", false, true)],
                // For unbounded, Expects to work in PartiallySorted mode. Shouldn't reverse window function.
                Some((false, PartitionSearchMode::PartiallySorted(vec![0]))),
            ),
        ];
        for test_case in test_cases {
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
                can_skip_ordering_util(
                    &partition_by_exprs,
                    &order_by_exprs,
                    &exec_unbounded
                )?,
                *expected,
                "Unexpected result for in unbounded test case: {:?}",
                test_case
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sorted_merged_indices() -> Result<()> {
        assert_eq!(
            get_ordered_merged_indices(&[0, 3, 4], &[1, 3, 5]),
            vec![0, 1, 3, 4, 5]
        );
        // Result should be ordered, even if inputs are not
        assert_eq!(
            get_ordered_merged_indices(&[3, 0, 4], &[5, 1, 3]),
            vec![0, 1, 3, 4, 5]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_is_consecutive_from_zero() -> Result<()> {
        assert!(!is_consecutive_from_zero(&[0, 3, 4]));
        assert!(is_consecutive_from_zero(&[0, 1, 2]));
        assert!(is_consecutive_from_zero(&[]));
        Ok(())
    }

    #[tokio::test]
    async fn test_get_set_diff_indices() -> Result<()> {
        assert_eq!(get_set_diff_indices(&[0, 3, 4], &[1, 2]), vec![0, 3, 4]);
        assert_eq!(get_set_diff_indices(&[0, 3, 4], &[1, 2, 4]), vec![0, 3]);
        // return value should have same ordering with the in1
        assert_eq!(get_set_diff_indices(&[3, 4, 0], &[1, 2, 4]), vec![3, 0]);
        Ok(())
    }

    #[tokio::test]
    async fn test_calc_ordering_range() -> Result<()> {
        assert_eq!(calc_ordering_range(&[0, 3, 4]), 1);
        assert_eq!(calc_ordering_range(&[0, 1, 3, 4]), 2);
        assert_eq!(calc_ordering_range(&[0, 1, 2, 3, 4]), 5);
        assert_eq!(calc_ordering_range(&[1, 2, 3, 4]), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_find_match_indices() -> Result<()> {
        assert_eq!(find_match_indices(&[0, 3, 4], &[0, 3, 4]), vec![0, 1, 2]);
        assert_eq!(find_match_indices(&[0, 4, 3], &[0, 3, 4]), vec![0, 2, 1]);
        assert_eq!(find_match_indices(&[0, 4, 3, 5], &[0, 3, 4]), vec![0, 2, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn test_compare_set_equality() -> Result<()> {
        assert!(compare_set_equality(&[4, 3, 2], &[3, 2, 4]));
        assert!(!compare_set_equality(&[4, 3, 2, 1], &[3, 2, 4]));
        Ok(())
    }

    /// Runs the sort enforcement optimizer and asserts the plan
    /// against the original and expected plans
    ///
    /// `$EXPECTED_PLAN_LINES`: input plan
    /// `$EXPECTED_OPTIMIZED_PLAN_LINES`: optimized plan
    /// `$PLAN`: the plan to optimized
    ///
    macro_rules! assert_optimized {
        ($EXPECTED_PLAN_LINES: expr, $EXPECTED_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr) => {
            let session_ctx = SessionContext::new();
            let state = session_ctx.state();

            let physical_plan = $PLAN;
            let formatted = displayable(physical_plan.as_ref()).indent().to_string();
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
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[nullable_col@0 ASC]",
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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

        let window_agg = window_exec("non_nullable_col", sort_exprs, sort);

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

        // let filter_exec = sort_exec;
        let physical_plan = window_exec("non_nullable_col", sort_exprs, filter);

        let expected_input = vec![
            "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "  FilterExec: NOT non_nullable_col@1",
            "    SortExec: expr=[non_nullable_col@1 ASC NULLS LAST]",
            "      WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "        SortExec: expr=[non_nullable_col@1 DESC]",
            "          MemoryExec: partitions=0, partition_sizes=[]",
        ];

        let expected_optimized = vec![
            "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(NULL) }]",
            "  FilterExec: NOT non_nullable_col@1",
            "    WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "      SortExec: expr=[non_nullable_col@1 DESC]",
            "        MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC]",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            "        MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            "              MemoryExec: partitions=0, partition_sizes=[]",
        ];

        let expected_optimized = vec![
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=0",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let sort2 = sort_exec(sort_exprs.clone(), repartition_exec);
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
            "            MemoryExec: partitions=0, partition_sizes=[]",
        ];

        let expected_optimized = vec![
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=0",
            "      MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let limit = local_limit_exec(sort);
        let limit = global_limit_exec(limit);

        let parquet_sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs);

        let union = union_exec(vec![source2, limit]);
        let repartition = repartition_exec(union);
        let physical_plan = sort_preserving_merge_exec(sort_exprs, repartition);

        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "    UnionExec",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "      GlobalLimitExec: skip=0, fetch=100",
            "        LocalLimitExec: fetch=100",
            "          SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "            ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];

        // We should keep the bottom `SortExec`.
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "      UnionExec",
            "        ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "        GlobalLimitExec: skip=0, fetch=100",
            "          LocalLimitExec: fetch=100",
            "            SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "              ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should not add a sort at the output of the union, input plan should not be changed
        let expected_optimized = expected_input.clone();
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should not add a sort at the output of the union, input plan should not be changed
        let expected_optimized = expected_input.clone();
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        // First ParquetExec has output ordering(nullable_col@0 ASC). However, it doesn't satisfy required ordering
        // of SortPreservingMergeExec. Hence rule should remove unnecessary sort for second child of the UnionExec
        // and put a sort above Union to satisfy required ordering.
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should remove unnecessary sorting from below and move it to top
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "    UnionExec",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should adjust sorting in the first input of the union such that it is not unnecessarily fine
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "    UnionExec",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC,non_nullable_col@1 DESC NULLS LAST]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // Should adjust the requirement in the third input of the union so
        // that it is not unnecessarily fine.
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortPreservingMergeExec: [nullable_col@0 ASC]",
            "      SortExec: expr=[nullable_col@0 ASC]",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "          ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let physical_plan = window_exec("nullable_col", sort_exprs3, union);

        // The `WindowAggExec` gets its sorting from multiple children jointly.
        // During the removal of `SortExec`s, it should be able to remove the
        // corresponding SortExecs together. Also, the inputs of these `SortExec`s
        // are not necessarily the same to be able to remove them.
        let expected_input = vec![
            "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "  UnionExec",
            "    SortExec: expr=[nullable_col@0 DESC NULLS LAST]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: expr=[nullable_col@0 DESC NULLS LAST]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(NULL) }]",
            "  UnionExec",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            "        ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC]",
            "    FilterExec: NOT non_nullable_col@1",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    // With new change in SortEnforcement EnforceSorting->EnforceDistribution->EnforceSorting
    // should produce same result with EnforceDistribution+EnforceSorting
    // This enables us to use EnforceSorting possibly before EnforceDistribution
    // Given that it will be called at least once after last EnforceDistribution. The reason is that
    // EnforceDistribution may invalidate ordering invariant.
    async fn test_commutativity() -> Result<()> {
        let schema = create_test_schema()?;

        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let memory_exec = memory_exec(&schema);
        let sort_exprs = vec![sort_expr("nullable_col", &schema)];
        let window = window_exec("nullable_col", sort_exprs.clone(), memory_exec);
        let repartition = repartition_exec(window);

        let orig_plan = Arc::new(SortExec::new_with_partitioning(
            sort_exprs,
            repartition,
            false,
            None,
        )) as Arc<dyn ExecutionPlan>;

        let mut plan = orig_plan.clone();
        let rules = vec![
            Arc::new(EnforceDistribution::new()) as Arc<dyn PhysicalOptimizerRule>,
            Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
        ];
        for rule in rules {
            plan = rule.optimize(plan, state.config_options())?;
        }
        let first_plan = plan.clone();

        let mut plan = orig_plan.clone();
        let rules = vec![
            Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
            Arc::new(EnforceDistribution::new()) as Arc<dyn PhysicalOptimizerRule>,
            Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
        ];
        for rule in rules {
            plan = rule.optimize(plan, state.config_options())?;
        }
        let second_plan = plan.clone();

        assert_eq!(get_plan_string(&first_plan), get_plan_string(&second_plan));
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
        let sort = Arc::new(SortExec::new_with_partitioning(
            sort_exprs.clone(),
            repartition,
            true,
            None,
        )) as _;
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
            "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=0",
            "            MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=0",
            "        MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    /// make PhysicalSortExpr with default options
    fn sort_expr(name: &str, schema: &Schema) -> PhysicalSortExpr {
        sort_expr_options(name, schema, SortOptions::default())
    }

    /// PhysicalSortExpr with specified options
    fn sort_expr_options(
        name: &str,
        schema: &Schema,
        options: SortOptions,
    ) -> PhysicalSortExpr {
        PhysicalSortExpr {
            expr: col(name, schema).unwrap(),
            options,
        }
    }

    fn memory_exec(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(MemoryExec::try_new(&[], schema.clone(), None).unwrap())
    }

    fn sort_exec(
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        Arc::new(SortExec::try_new(sort_exprs, input, None).unwrap())
    }

    fn sort_preserving_merge_exec(
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        Arc::new(SortPreservingMergeExec::new(sort_exprs, input))
    }

    fn filter_exec(
        predicate: Arc<dyn PhysicalExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(FilterExec::try_new(predicate, input).unwrap())
    }

    fn window_exec(
        col_name: &str,
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs: Vec<_> = sort_exprs.into_iter().collect();
        let schema = input.schema();

        Arc::new(
            WindowAggExec::try_new(
                vec![create_window_expr(
                    &WindowFunction::AggregateFunction(AggregateFunction::Count),
                    "count".to_owned(),
                    &[col(col_name, &schema).unwrap()],
                    &[],
                    &sort_exprs,
                    Arc::new(WindowFrame::new(true)),
                    schema.as_ref(),
                )
                .unwrap()],
                input.clone(),
                input.schema(),
                vec![],
                Some(sort_exprs),
            )
            .unwrap(),
        )
    }

    /// Create a non sorted parquet exec
    fn parquet_exec(schema: &SchemaRef) -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema.clone(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        ))
    }

    // Created a sorted parquet exec
    fn parquet_exec_sorted(
        schema: &SchemaRef,
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();

        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema.clone(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: Some(sort_exprs),
                infinite_source: false,
            },
            None,
            None,
        ))
    }

    // Created a sorted parquet exec
    fn csv_exec_sorted(
        schema: &SchemaRef,
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        infinite_source: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();

        Arc::new(CsvExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema.clone(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: Some(sort_exprs),
                infinite_source,
            },
            false,
            0,
            FileCompressionType::UNCOMPRESSED,
        ))
    }

    fn union_exec(input: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        Arc::new(UnionExec::new(input))
    }

    fn local_limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(LocalLimitExec::new(input, 100))
    }

    fn global_limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(GlobalLimitExec::new(input, 0, Some(100)))
    }

    fn repartition_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(10)).unwrap(),
        )
    }

    fn aggregate_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                PhysicalGroupBy::default(),
                vec![],
                input,
                schema,
            )
            .unwrap(),
        )
    }
}
