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
//! "SortExec: [nullable_col@0 ASC]",
//! "  SortExec: [non_nullable_col@1 ASC]",
//! in the physical plan. The first sort is unnecessary since its result is overwritten
//! by another SortExec. Therefore, this rule removes it from the physical plan.
use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_optimizer::utils::add_sort_above_child;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::rewrite::TreeNodeRewritable;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::union::UnionExec;
use crate::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use crate::physical_plan::{with_new_children_if_necessary, Distribution, ExecutionPlan};
use arrow::datatypes::SchemaRef;
use datafusion_common::{reverse_sort_options, DataFusionError};
use datafusion_physical_expr::utils::{ordering_satisfy, ordering_satisfy_concrete};
use datafusion_physical_expr::window::WindowExpr;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
use itertools::izip;
use std::iter::zip;
use std::sync::Arc;

/// This rule inspects SortExec's in the given physical plan and removes the
/// ones it can prove unnecessary. The boolean flag `parallelize_sorts`
/// indicates whether we elect to transform CoalescePartitionsExec + SortExec
/// cascades into SortExec + SortPreservingMergeExec cascades, which enables
/// us to perform sorting parallel.
#[derive(Default)]
pub struct EnforceSorting {
    parallelize_sorts: bool,
}

impl EnforceSorting {
    #[allow(missing_docs)]
    pub fn new(parallelize_sorts: bool) -> Self {
        Self { parallelize_sorts }
    }
}

#[derive(Debug, Clone)]
struct ExecTree {
    /// child index of the plan in its parent executor
    pub idx: usize,
    /// Children of the `plan` that needs to be updated if bottom executor is removed
    pub children: Vec<ExecTree>,
    /// ExecutionPlan node of the tree
    pub plan: Arc<dyn ExecutionPlan>,
}

impl ExecTree {
    /// Executor at the leaf of the tree
    fn bottom_executors(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        if !self.children.is_empty() {
            let mut res = vec![];
            for child in &self.children {
                res.extend(child.bottom_executors())
            }
            res
        } else {
            vec![self.plan.clone()]
        }
    }
}

/// This is a "data class" we use within the [EnforceSorting] rule that
/// tracks the closest `SortExec` descendant for every child of a plan.
#[derive(Debug, Clone)]
struct PlanWithCorrespondingSort {
    plan: Arc<dyn ExecutionPlan>,
    // For every child, keep a tree of `ExecutionPlan`s starting from the
    // child till SortExecs that determine ordering of child. If child has linkage to nay Sort
    // do not keep a tree for that child (e.g use None).
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
            let children_requirements = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;
            let sort_onwards = children_requirements
                .iter()
                .enumerate()
                .map(|(idx, item)| {
                    let flags = item.plan.maintains_input_order();
                    let required_ordering = item.plan.required_input_ordering();
                    let mut res = vec![];
                    for (maintains, element, required_ordering) in
                        izip!(flags, item.sort_onwards.clone(), required_ordering)
                    {
                        if (maintains
                              // partially maintains ordering
                              || (item.plan.as_any().is::<UnionExec>() && item.plan.output_ordering().is_some()))
                            && element.is_some()
                            && required_ordering.is_none()
                            // Sorts under limit are not removable.
                            && !is_limit(&item.plan)
                        {
                            res.push(element.clone().unwrap());
                        }
                    }
                    // The `sort_onwards` list starts from the sort-introducing operator
                    // (e.g `SortExec`, `SortPreservingMergeExec`) and collects all the
                    // intermediate executors that maintain this ordering. If we are at
                    // the beginning, we reset tree, and start from bottom.
                    if is_sort(&item.plan) {
                        Some(ExecTree {
                            idx,
                            plan: item.plan.clone(),
                            children: vec![],
                        })
                    } // Add parent node to the tree
                    else if !res.is_empty() {
                        Some(ExecTree {
                            idx,
                            plan: item.plan.clone(),
                            children: res,
                        })
                    } else {
                        // There is no sort linkage for this child
                        None
                    }
                })
                .collect::<Vec<_>>();
            let children_plans = children_requirements
                .iter()
                .map(|item| item.plan.clone())
                .collect::<Vec<_>>();
            let plan = with_new_children_if_necessary(self.plan, children_plans)?;
            Ok(PlanWithCorrespondingSort { plan, sort_onwards })
        }
    }
}

/// This is a "data class" we use within the [EnforceSorting] rule that
/// tracks the closest `CoalescePartitionsExec` descendant of a plan.
#[derive(Debug, Clone)]
struct PlanWithCorrespondingCoalescePartitions {
    plan: Arc<dyn ExecutionPlan>,
    // Keep a vector of `ExecutionPlan`s starting from the closest
    // `CoalescePartitionsExec` till the current plan. Since we are sure
    // that a `CoalescePartitionsExec` can only be propagated on executors
    // with a single child, we use a single vector (not one for each child).
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
            let children_requirements = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;
            let mut coalesce_onwards = vec![];
            for (idx, item) in children_requirements.iter().enumerate() {
                if item.plan.as_any().is::<CoalescePartitionsExec>() {
                    coalesce_onwards.push(Some(ExecTree {
                        idx,
                        children: vec![],
                        plan: item.plan.clone(),
                    }))
                } else if !item.plan.children().is_empty() {
                    let requires_single_partition = matches!(
                        item.plan.required_input_distribution()[0],
                        Distribution::SinglePartition
                    );
                    // The Executor above(not necessarily immediately above) CoalescePartitionExec doesn't require SinglePartition
                    // hence CoalescePartitionExec is not a requirement of this executor.
                    // We can propagate it upwards.
                    let mut res = vec![];
                    for elem in item.coalesce_onwards.iter().flatten() {
                        res.push(elem.clone());
                    }
                    if !requires_single_partition && !res.is_empty() {
                        coalesce_onwards.push(Some(ExecTree {
                            idx,
                            children: res,
                            plan: item.plan.clone(),
                        }));
                    } else {
                        coalesce_onwards.push(None);
                    }
                } else {
                    coalesce_onwards.push(None);
                }
            }
            let children_plans = children_requirements
                .iter()
                .map(|item| item.plan.clone())
                .collect::<Vec<_>>();
            let plan = with_new_children_if_necessary(self.plan, children_plans)?;
            Ok(PlanWithCorrespondingCoalescePartitions {
                plan,
                coalesce_onwards,
            })
        }
    }
}

impl PhysicalOptimizerRule for EnforceSorting {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Execute a post-order traversal to adjust input key ordering:
        let plan_requirements = PlanWithCorrespondingSort::new(plan);
        let adjusted = plan_requirements.transform_up(&ensure_sorting)?;
        if self.parallelize_sorts {
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
///      "SortExec: [a@0 ASC]",
///      "  CoalescePartitionsExec",
///      "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
/// to
///      "SortPreservingMergeExec: [a@0 ASC]",
///      "  SortExec: [a@0 ASC]",
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
    if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        let sort_expr = sort_exec.expr();
        // If there is link between CoalescePartitionsExec and SortExec that satisfy requirements
        // (e.g all Executors in between doesn't require to work on SinglePartition).
        // We can replace CoalescePartitionsExec with SortExec and SortExec with SortPreservingMergeExec
        // respectively to parallelize sorting.
        if coalesce_onwards[0].is_some() {
            let mut prev_layer = plan.clone();
            update_child_to_change_coalesce(
                &mut prev_layer,
                &mut coalesce_onwards[0],
                sort_exec,
            )?;

            let spm = SortPreservingMergeExec::new(sort_expr.to_vec(), prev_layer);
            return Ok(Some(PlanWithCorrespondingCoalescePartitions {
                plan: Arc::new(spm),
                coalesce_onwards: vec![None],
            }));
        }
    } else if plan.as_any().is::<CoalescePartitionsExec>()
        && coalesce_onwards[0].is_some()
    {
        let mut prev_layer = plan.clone();
        update_child_to_remove_unnecessary_coalesce(
            &mut prev_layer,
            &mut coalesce_onwards[0],
        )?;
        let new_plan = plan.with_new_children(vec![prev_layer])?;
        return Ok(Some(PlanWithCorrespondingCoalescePartitions {
            plan: new_plan,
            coalesce_onwards: vec![None],
        }));
    }

    Ok(Some(PlanWithCorrespondingCoalescePartitions {
        plan,
        coalesce_onwards,
    }))
}

// Checks whether executor is limit executor (e.g either LocalLimitExec or GlobalLimitExec)
fn is_limit(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<GlobalLimitExec>() || plan.as_any().is::<LocalLimitExec>()
}

// Checks whether executor is Sort
// TODO: Add support for SortPreservingMergeExec also.
fn is_sort(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().is::<SortExec>()
}

fn ensure_sorting(
    requirements: PlanWithCorrespondingSort,
) -> Result<Option<PlanWithCorrespondingSort>> {
    // Perform naive analysis at the beginning -- remove already-satisfied sorts:
    if let Some(result) = analyze_immediate_sort_removal(&requirements)? {
        return Ok(Some(result));
    }
    let plan = &requirements.plan;
    let mut new_children = plan.children().clone();
    let mut new_onwards = requirements.sort_onwards.clone();
    for (idx, (child, sort_onwards, required_ordering)) in izip!(
        new_children.iter_mut(),
        new_onwards.iter_mut(),
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
                    update_child_to_remove_unnecessary_sort(child, sort_onwards)?;
                    let sort_expr = required_ordering.to_vec();
                    *child = add_sort_above_child(child, sort_expr)?;
                    // sort_onwards.push((idx, child.clone()))
                    *sort_onwards = Some(ExecTree {
                        idx,
                        plan: child.clone(),
                        children: vec![],
                    })
                }
                if let Some(sort_onward) = sort_onwards {
                    // For window expressions, we can remove some sorts when we can
                    // calculate the result in reverse:
                    if let Some(exec) = plan.as_any().downcast_ref::<WindowAggExec>() {
                        if let Some(result) = analyze_window_sort_removal(
                            exec.window_expr(),
                            &exec.partition_keys,
                            sort_onward,
                        )? {
                            return Ok(Some(result));
                        }
                    } else if let Some(exec) = requirements
                        .plan
                        .as_any()
                        .downcast_ref::<BoundedWindowAggExec>()
                    {
                        if let Some(result) = analyze_window_sort_removal(
                            exec.window_expr(),
                            &exec.partition_keys,
                            sort_onward,
                        )? {
                            return Ok(Some(result));
                        }
                    }
                    // TODO: Once we can ensure that required ordering information propagates with
                    //       necessary lineage information, compare `sort_input_ordering` and `required_ordering`.
                    //       This will enable us to handle cases such as (a,b) -> Sort -> (a,b,c) -> Required(a,b).
                    //       Currently, we can not remove such sorts.
                }
            }
            (Some(required), None) => {
                // Ordering requirement is not met, we should add a SortExec to the plan.
                let sort_expr = required.to_vec();
                *child = add_sort_above_child(child, sort_expr)?;
                *sort_onwards = Some(ExecTree {
                    idx,
                    plan: child.clone(),
                    children: vec![],
                })
            }
            (None, Some(_)) => {
                // We have a SortExec whose effect may be neutralized by a order-imposing
                // operator. In this case, remove this sort:
                if !plan.maintains_input_order()[idx] {
                    if plan.output_ordering().is_some() && !is_sort(plan) {
                        let count = plan.output_ordering().unwrap().len();
                        update_child_to_change_finer_sort(child, sort_onwards, count)?;
                    } else {
                        update_child_to_remove_unnecessary_sort(child, sort_onwards)?;
                    }
                }
            }
            (None, None) => {}
        }
    }
    if plan.children().is_empty() {
        Ok(Some(requirements))
    } else {
        let new_plan = requirements.plan.with_new_children(new_children)?;
        Ok(Some(PlanWithCorrespondingSort {
            plan: new_plan,
            sort_onwards: new_onwards,
        }))
    }
}

/// Analyzes a given `SortExec` to determine whether its input already has
/// a finer ordering than this `SortExec` enforces.
fn analyze_immediate_sort_removal(
    requirements: &PlanWithCorrespondingSort,
) -> Result<Option<PlanWithCorrespondingSort>> {
    if let Some(sort_exec) = requirements.plan.as_any().downcast_ref::<SortExec>() {
        let sort_input = sort_exec.input().clone();
        // If this sort is unnecessary, we should remove it:
        if ordering_satisfy(
            sort_input.output_ordering(),
            sort_exec.output_ordering(),
            || sort_input.equivalence_properties(),
        ) {
            // Since we know that a `SortExec` has exactly one child,
            // we can use the zero index safely:
            let mut new_onwards = requirements.sort_onwards[0].clone();
            let mut children = vec![];
            if let Some(tmp) = &new_onwards {
                children.push(tmp.clone());
            }
            let updated_plan = if !sort_exec.preserve_partitioning()
                && sort_input.output_partitioning().partition_count() > 1
            {
                // Replace the sort with a sort-preserving merge:
                let new_plan: Arc<dyn ExecutionPlan> = Arc::new(
                    SortPreservingMergeExec::new(sort_exec.expr().to_vec(), sort_input),
                );
                // new_onwards.push((0, new_plan.clone()));
                new_onwards = Some(ExecTree {
                    idx: 0,
                    plan: new_plan.clone(),
                    children,
                });
                new_plan
            } else {
                // Remove the sort:
                sort_input
            };
            return Ok(Some(PlanWithCorrespondingSort {
                plan: updated_plan,
                sort_onwards: vec![new_onwards],
            }));
        }
    }
    Ok(None)
}

/// Analyzes a [WindowAggExec] or a [BoundedWindowAggExec] to determine whether
/// it may allow removing a sort.
fn analyze_window_sort_removal(
    window_expr: &[Arc<dyn WindowExpr>],
    partition_keys: &[Arc<dyn PhysicalExpr>],
    sort_onward: &mut ExecTree,
) -> Result<Option<PlanWithCorrespondingSort>> {
    let bottom_sorts = sort_onward.bottom_executors();
    let mut can_skip_sortings = vec![];
    let mut should_reverses = vec![];
    let mut physical_ordering_common = vec![];
    for sort_any in bottom_sorts {
        let sort_output_ordering = sort_any.output_ordering();
        // Variable `sort_any` will either be a `SortExec` or a
        // `SortPreservingMergeExec`, and both have single child.
        // Therefore, we can use the 0th index without loss of generality.
        let sort_input = sort_any.children()[0].clone();
        let physical_ordering = sort_input.output_ordering();
        let required_ordering = sort_output_ordering.ok_or_else(|| {
            DataFusionError::Plan("A SortExec should have output ordering".to_string())
        })?;
        let physical_ordering = if let Some(physical_ordering) = physical_ordering {
            physical_ordering
        } else {
            // If there is no physical ordering, there is no way to remove a sort -- immediately return:
            return Ok(None);
        };
        if physical_ordering.len() < physical_ordering_common.len()
            || physical_ordering_common.is_empty()
        {
            physical_ordering_common = physical_ordering.to_vec();
        }
        let (can_skip_sorting, should_reverse) = can_skip_sort(
            window_expr[0].partition_by(),
            required_ordering,
            &sort_input.schema(),
            physical_ordering,
        )?;
        can_skip_sortings.push(can_skip_sorting);
        should_reverses.push(should_reverse);
    }
    let can_skip_sorting = can_skip_sortings.iter().all(|elem| *elem);
    let can_skip_sorting = can_skip_sorting
        && should_reverses
            .iter()
            .all(|elem| *elem == should_reverses[0]);
    let should_reverse = should_reverses[0];
    if can_skip_sorting {
        let new_window_expr = if should_reverse {
            window_expr
                .iter()
                .map(|e| e.get_reverse_expr())
                .collect::<Option<Vec<_>>>()
        } else {
            Some(window_expr.to_vec())
        };
        if let Some(window_expr) = new_window_expr {
            let new_child = remove_corresponding_sort_from_sub_plan(sort_onward)?;
            let new_schema = new_child.schema();

            let uses_bounded_memory = window_expr.iter().all(|e| e.uses_bounded_memory());
            // If all window exprs can run with bounded memory choose bounded window variant
            let new_plan = if uses_bounded_memory {
                Arc::new(BoundedWindowAggExec::try_new(
                    window_expr,
                    new_child,
                    new_schema,
                    partition_keys.to_vec(),
                    Some(physical_ordering_common),
                )?) as _
            } else {
                Arc::new(WindowAggExec::try_new(
                    window_expr,
                    new_child,
                    new_schema,
                    partition_keys.to_vec(),
                    Some(physical_ordering_common),
                )?) as _
            };
            return Ok(Some(PlanWithCorrespondingSort::new(new_plan)));
        }
    }
    Ok(None)
}

/// Updates child to remove the unnecessary sorting below it.
fn update_child_to_change_coalesce(
    child: &mut Arc<dyn ExecutionPlan>,
    sort_onwards: &mut Option<ExecTree>,
    sort_exec: &SortExec,
) -> Result<()> {
    if let Some(sort_onwards) = sort_onwards {
        *child = change_corresponding_coalesce_in_sub_plan(sort_onwards, sort_exec)?;
    }
    Ok(())
}

/// Updates child to remove the unnecessary sorting below it.
fn update_child_to_remove_unnecessary_coalesce(
    child: &mut Arc<dyn ExecutionPlan>,
    sort_onwards: &mut Option<ExecTree>,
) -> Result<()> {
    if let Some(sort_onwards) = sort_onwards {
        *child = remove_corresponding_coalesce_in_sub_plan(sort_onwards)?;
    }
    Ok(())
}

/// Updates child to remove the unnecessary sorting below it.
fn update_child_to_remove_unnecessary_sort(
    child: &mut Arc<dyn ExecutionPlan>,
    sort_onwards: &mut Option<ExecTree>,
) -> Result<()> {
    if let Some(sort_onwards) = sort_onwards {
        *child = remove_corresponding_sort_from_sub_plan(sort_onwards)?;
    }
    Ok(())
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

/// Removes the sort from the plan in `sort_onwards`.
fn change_corresponding_coalesce_in_sub_plan(
    sort_onwards: &mut ExecTree,
    sort_exec: &SortExec,
) -> Result<Arc<dyn ExecutionPlan>> {
    let res = if sort_onwards.plan.as_any().is::<CoalescePartitionsExec>() {
        let coalesce_input = sort_onwards.plan.children()[0].clone();
        let sort_expr = sort_exec.expr();
        if !ordering_satisfy(coalesce_input.output_ordering(), Some(sort_expr), || {
            sort_exec.equivalence_properties()
        }) {
            add_sort_above_child(&coalesce_input, sort_expr.to_vec())?
        } else {
            coalesce_input
        }
    } else {
        let res = sort_onwards.plan.clone();
        let mut children = res.children();
        for elem in &mut sort_onwards.children {
            children[elem.idx] =
                change_corresponding_coalesce_in_sub_plan(elem, sort_exec)?;
        }
        res.with_new_children(children)?
    };

    Ok(res)
}

/// Removes the sort from the plan in `sort_onwards`.
fn remove_corresponding_coalesce_in_sub_plan(
    sort_onwards: &mut ExecTree,
) -> Result<Arc<dyn ExecutionPlan>> {
    let res = if sort_onwards.plan.as_any().is::<CoalescePartitionsExec>() {
        sort_onwards.plan.children()[0].clone()
    } else {
        let res = sort_onwards.plan.clone();
        let mut children = res.children();
        for elem in &mut sort_onwards.children {
            children[elem.idx] = remove_corresponding_coalesce_in_sub_plan(elem)?;
        }
        res.with_new_children(children)?
    };

    Ok(res)
}

/// Removes the sort from the plan in `sort_onwards`.
fn remove_corresponding_sort_from_sub_plan(
    sort_onwards: &mut ExecTree,
) -> Result<Arc<dyn ExecutionPlan>> {
    let res = if is_sort(&sort_onwards.plan) {
        sort_onwards.plan.children()[0].clone()
    } else {
        let res = sort_onwards.plan.clone();
        let mut children = res.children();
        for elem in &mut sort_onwards.children {
            children[elem.idx] = remove_corresponding_sort_from_sub_plan(elem)?;
        }
        res.with_new_children(children)?
    };

    Ok(res)
}

/// Change the unnecessarily fine sort in `sort_onwards`.
fn change_finer_sort_in_sub_plan(
    sort_onwards: &mut ExecTree,
    n_sort_expr: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let res = if is_sort(&sort_onwards.plan) {
        let prev_layer = sort_onwards.plan.children()[0].clone();
        let new_sort_expr = get_sort_exprs(&sort_onwards.plan)?[0..n_sort_expr].to_vec();
        add_sort_above_child(&prev_layer, new_sort_expr)?
    } else {
        let res = sort_onwards.plan.clone();
        let mut children = res.children();
        for elem in &mut sort_onwards.children {
            children[elem.idx] = remove_corresponding_sort_from_sub_plan(elem)?;
        }
        res.with_new_children(children)?
    };

    Ok(res)
}

#[derive(Debug)]
/// This structure stores extra column information required to remove unnecessary sorts.
pub struct ColumnInfo {
    is_aligned: bool,
    reverse: bool,
    is_partition: bool,
}

/// Compares physical ordering and required ordering of all `PhysicalSortExpr`s and returns a tuple.
/// The first element indicates whether these `PhysicalSortExpr`s can be removed from the physical plan.
/// The second element is a flag indicating whether we should reverse the sort direction in order to
/// remove physical sort expressions from the plan.
pub fn can_skip_sort(
    partition_keys: &[Arc<dyn PhysicalExpr>],
    required: &[PhysicalSortExpr],
    input_schema: &SchemaRef,
    physical_ordering: &[PhysicalSortExpr],
) -> Result<(bool, bool)> {
    if required.len() > physical_ordering.len() {
        return Ok((false, false));
    }
    let mut col_infos = vec![];
    for (sort_expr, physical_expr) in zip(required, physical_ordering) {
        let column = sort_expr.expr.clone();
        let is_partition = partition_keys.iter().any(|e| e.eq(&column));
        let (is_aligned, reverse) =
            check_alignment(input_schema, physical_expr, sort_expr);
        col_infos.push(ColumnInfo {
            is_aligned,
            reverse,
            is_partition,
        });
    }
    let partition_by_sections = col_infos
        .iter()
        .filter(|elem| elem.is_partition)
        .collect::<Vec<_>>();
    let can_skip_partition_bys = if partition_by_sections.is_empty() {
        true
    } else {
        let first_reverse = partition_by_sections[0].reverse;
        let can_skip_partition_bys = partition_by_sections
            .iter()
            .all(|c| c.is_aligned && c.reverse == first_reverse);
        can_skip_partition_bys
    };
    let order_by_sections = col_infos
        .iter()
        .filter(|elem| !elem.is_partition)
        .collect::<Vec<_>>();
    let (can_skip_order_bys, should_reverse_order_bys) = if order_by_sections.is_empty() {
        (true, false)
    } else {
        let first_reverse = order_by_sections[0].reverse;
        let can_skip_order_bys = order_by_sections
            .iter()
            .all(|c| c.is_aligned && c.reverse == first_reverse);
        (can_skip_order_bys, first_reverse)
    };
    let can_skip = can_skip_order_bys && can_skip_partition_bys;
    Ok((can_skip, should_reverse_order_bys))
}

/// Compares `physical_ordering` and `required` ordering, returns a tuple
/// indicating (1) whether this column requires sorting, and (2) whether we
/// should reverse the window expression in order to avoid sorting.
fn check_alignment(
    input_schema: &SchemaRef,
    physical_ordering: &PhysicalSortExpr,
    required: &PhysicalSortExpr,
) -> (bool, bool) {
    if required.expr.eq(&physical_ordering.expr) {
        let nullable = required.expr.nullable(input_schema).unwrap();
        let physical_opts = physical_ordering.options;
        let required_opts = required.options;
        let is_reversed = if nullable {
            physical_opts == reverse_sort_options(required_opts)
        } else {
            // If the column is not nullable, NULLS FIRST/LAST is not important.
            physical_opts.descending != required_opts.descending
        };
        let can_skip = !nullable || is_reversed || (physical_opts == required_opts);
        (can_skip, is_reversed)
    } else {
        (false, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::physical_optimizer::dist_enforcement::EnforceDistribution;
    use crate::physical_plan::file_format::{FileScanConfig, ParquetExec};
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

    // Util function to get string representation of a physical plan
    fn get_plan_string(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
        let formatted = displayable(plan.as_ref()).indent().to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();
        actual.iter().map(|elem| elem.to_string()).collect()
    }

    #[tokio::test]
    async fn test_is_column_aligned_nullable() -> Result<()> {
        let schema = create_test_schema()?;
        let params = vec![
            ((true, true), (false, false), (true, true)),
            ((true, true), (false, true), (false, false)),
            ((true, true), (true, false), (false, false)),
            ((true, false), (false, true), (true, true)),
            ((true, false), (false, false), (false, false)),
            ((true, false), (true, true), (false, false)),
        ];
        for (
            (physical_desc, physical_nulls_first),
            (req_desc, req_nulls_first),
            (is_aligned_expected, reverse_expected),
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
            let (is_aligned, reverse) =
                check_alignment(&schema, &physical_ordering, &required_ordering);
            assert_eq!(is_aligned, is_aligned_expected);
            assert_eq!(reverse, reverse_expected);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_is_column_aligned_non_nullable() -> Result<()> {
        let schema = create_test_schema()?;

        let params = vec![
            ((true, true), (false, false), (true, true)),
            ((true, true), (false, true), (true, true)),
            ((true, true), (true, false), (true, false)),
            ((true, false), (false, true), (true, true)),
            ((true, false), (false, false), (true, true)),
            ((true, false), (true, true), (true, false)),
        ];
        for (
            (physical_desc, physical_nulls_first),
            (req_desc, req_nulls_first),
            (is_aligned_expected, reverse_expected),
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
            let (is_aligned, reverse) =
                check_alignment(&schema, &physical_ordering, &required_ordering);
            assert_eq!(is_aligned, is_aligned_expected);
            assert_eq!(reverse, reverse_expected);
        }

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
                EnforceSorting::new(true).optimize(physical_plan, state.config_options())?;
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
            "SortExec: [nullable_col@0 ASC]",
            "  SortExec: [non_nullable_col@1 ASC]",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortExec: [nullable_col@0 ASC]",
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
            "    SortExec: [non_nullable_col@1 ASC NULLS LAST]",
            "      WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "        SortExec: [non_nullable_col@1 DESC]",
            "          MemoryExec: partitions=0, partition_sizes=[]",
        ];

        let expected_optimized = vec![
            "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(NULL) }]",
            "  FilterExec: NOT non_nullable_col@1",
            "    WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "      SortExec: [non_nullable_col@1 DESC]",
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
            "  SortExec: [nullable_col@0 ASC]",
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
            "  SortExec: [nullable_col@0 ASC]",
            "    SortPreservingMergeExec: [nullable_col@0 ASC]",
            "      SortExec: [nullable_col@0 ASC]",
            "        MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC]",
            "    SortExec: [nullable_col@0 ASC]",
            "      MemoryExec: partitions=0, partition_sizes=[]",
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
            "  SortExec: [nullable_col@0 ASC]",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
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
            "    SortExec: [nullable_col@0 ASC]",
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
            "    SortExec: [nullable_col@0 ASC]",
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
            "    SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should remove unnecessary sorting from below and move it to top
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
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
            "    SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: [nullable_col@0 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should adjust sorting in the first input of the union such that it is not unnecessarily fine
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: [nullable_col@0 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: [nullable_col@0 ASC]",
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

        // First input to the union is not Sorted (SortExec is finer than required ordering by the SortPreservingMergeExec above).
        // Second input to the union is already Sorted (matches with the required ordering by the SortPreservingMergeExec above).
        // Third input to the union is not Sorted (SortExec is matches required ordering by the SortPreservingMergeExec above).
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    SortExec: [nullable_col@0 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: [nullable_col@0 ASC]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should adjust sorting in the first input of the union such that it is not unnecessarily fine
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "    UnionExec",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
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
        let repartition = Arc::new(RepartitionExec::try_new(
            window,
            Partitioning::RoundRobinBatch(2),
        )?) as Arc<dyn ExecutionPlan>;

        let orig_plan = Arc::new(SortExec::new_with_partitioning(
            sort_exprs,
            repartition,
            false,
            None,
        )) as Arc<dyn ExecutionPlan>;

        let mut plan = orig_plan.clone();
        let rules = vec![
            Arc::new(EnforceDistribution::new()) as Arc<dyn PhysicalOptimizerRule>,
            Arc::new(EnforceSorting::new(true)) as Arc<dyn PhysicalOptimizerRule>,
        ];
        for rule in rules {
            plan = rule.optimize(plan, state.config_options())?;
        }
        let first_plan = plan.clone();

        let mut plan = orig_plan.clone();
        let rules = vec![
            Arc::new(EnforceSorting::new(true)) as Arc<dyn PhysicalOptimizerRule>,
            Arc::new(EnforceDistribution::new()) as Arc<dyn PhysicalOptimizerRule>,
            Arc::new(EnforceSorting::new(true)) as Arc<dyn PhysicalOptimizerRule>,
        ];
        for rule in rules {
            plan = rule.optimize(plan, state.config_options())?;
        }
        let second_plan = plan.clone();

        assert_eq!(get_plan_string(&first_plan), get_plan_string(&second_plan));
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
    ) -> Arc<ParquetExec> {
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

    fn union_exec(input: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        Arc::new(UnionExec::new(input))
    }
}
