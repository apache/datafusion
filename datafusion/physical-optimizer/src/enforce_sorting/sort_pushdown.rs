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

use std::fmt::Debug;
use std::sync::Arc;

use crate::utils::{
    add_sort_above, is_sort, is_sort_preserving_merge, is_union, is_window,
};

use arrow::datatypes::SchemaRef;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{internal_err, HashSet, JoinSide, Result};
use datafusion_expr::JoinType;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{
    add_offset_to_physical_sort_exprs, EquivalenceProperties,
};
use datafusion_physical_expr_common::sort_expr::{
    LexOrdering, LexRequirement, OrderingRequirements, PhysicalSortExpr,
    PhysicalSortRequirement,
};
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::utils::{
    calculate_join_output_ordering, ColumnIndex,
};
use datafusion_physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::tree_node::PlanContext;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// This is a "data class" we use within the [`EnforceSorting`] rule to push
/// down [`SortExec`] in the plan. In some cases, we can reduce the total
/// computational cost by pushing down `SortExec`s through some executors. The
/// object carries the parent required ordering and the (optional) `fetch` value
/// of the parent node as its data.
///
/// [`EnforceSorting`]: crate::enforce_sorting::EnforceSorting
#[derive(Default, Clone, Debug)]
pub struct ParentRequirements {
    ordering_requirement: Option<OrderingRequirements>,
    fetch: Option<usize>,
}

pub type SortPushDown = PlanContext<ParentRequirements>;

/// Assigns the ordering requirement of the root node to the its children.
pub fn assign_initial_requirements(sort_push_down: &mut SortPushDown) {
    let reqs = sort_push_down.plan.required_input_ordering();
    for (child, requirement) in sort_push_down.children.iter_mut().zip(reqs) {
        child.data = ParentRequirements {
            ordering_requirement: requirement,
            // If the parent has a fetch value, assign it to the children
            // Or use the fetch value of the child.
            fetch: child.plan.fetch(),
        };
    }
}

/// Tries to push down the sort requirements as far as possible, if decides a `SortExec` is unnecessary removes it.
pub fn pushdown_sorts(sort_push_down: SortPushDown) -> Result<SortPushDown> {
    sort_push_down
        .transform_down(pushdown_sorts_helper)
        .map(|transformed| transformed.data)
}

fn min_fetch(f1: Option<usize>, f2: Option<usize>) -> Option<usize> {
    match (f1, f2) {
        (Some(f1), Some(f2)) => Some(f1.min(f2)),
        (Some(_), _) => f1,
        (_, Some(_)) => f2,
        _ => None,
    }
}

fn pushdown_sorts_helper(
    mut sort_push_down: SortPushDown,
) -> Result<Transformed<SortPushDown>> {
    let plan = sort_push_down.plan;
    let parent_fetch = sort_push_down.data.fetch;

    let Some(parent_requirement) = sort_push_down.data.ordering_requirement.clone()
    else {
        // If there are no ordering requirements from the parent, nothing to do
        // unless we have a sort.
        if is_sort(&plan) {
            let Some(sort_ordering) = plan.output_ordering().cloned() else {
                return internal_err!("SortExec should have output ordering");
            };
            // The sort is unnecessary, just propagate the stricter fetch and
            // ordering requirements.
            let fetch = min_fetch(plan.fetch(), parent_fetch);
            sort_push_down = sort_push_down
                .children
                .swap_remove(0)
                .update_plan_from_children()?;
            sort_push_down.data.fetch = fetch;
            sort_push_down.data.ordering_requirement =
                Some(OrderingRequirements::from(sort_ordering));
            // Recursive call to helper, so it doesn't transform_down and miss
            // the new node (previous child of sort):
            return pushdown_sorts_helper(sort_push_down);
        }
        sort_push_down.plan = plan;
        return Ok(Transformed::no(sort_push_down));
    };

    let eqp = plan.equivalence_properties();
    let satisfy_parent =
        eqp.ordering_satisfy_requirement(parent_requirement.first().clone())?;

    if is_sort(&plan) {
        let Some(sort_ordering) = plan.output_ordering().cloned() else {
            return internal_err!("SortExec should have output ordering");
        };

        let sort_fetch = plan.fetch();
        let parent_is_stricter = eqp.requirements_compatible(
            parent_requirement.first().clone(),
            sort_ordering.clone().into(),
        );

        // Remove the current sort as we are either going to prove that it is
        // unnecessary, or replace it with a stricter sort.
        sort_push_down = sort_push_down
            .children
            .swap_remove(0)
            .update_plan_from_children()?;
        if !satisfy_parent && !parent_is_stricter {
            // The sort was imposing a different ordering than the one being
            // pushed down. Replace it with a sort that matches the pushed-down
            // ordering, and continue the pushdown.
            // Add back the sort:
            sort_push_down = add_sort_above(
                sort_push_down,
                parent_requirement.into_single(),
                parent_fetch,
            );
            // Update pushdown requirements:
            sort_push_down.children[0].data = ParentRequirements {
                ordering_requirement: Some(OrderingRequirements::from(sort_ordering)),
                fetch: sort_fetch,
            };
            return Ok(Transformed::yes(sort_push_down));
        } else {
            // Sort was unnecessary, just propagate the stricter fetch and
            // ordering requirements:
            sort_push_down.data.fetch = min_fetch(sort_fetch, parent_fetch);
            let current_is_stricter = eqp.requirements_compatible(
                sort_ordering.clone().into(),
                parent_requirement.first().clone(),
            );
            sort_push_down.data.ordering_requirement = if current_is_stricter {
                Some(OrderingRequirements::from(sort_ordering))
            } else {
                Some(parent_requirement)
            };
            // Recursive call to helper, so it doesn't transform_down and miss
            // the new node (previous child of sort):
            return pushdown_sorts_helper(sort_push_down);
        }
    }

    sort_push_down.plan = plan;
    if satisfy_parent {
        // For non-sort operators which satisfy ordering:
        let reqs = sort_push_down.plan.required_input_ordering();

        for (child, order) in sort_push_down.children.iter_mut().zip(reqs) {
            child.data.ordering_requirement = order;
            child.data.fetch = min_fetch(parent_fetch, child.data.fetch);
        }
    } else if let Some(adjusted) = pushdown_requirement_to_children(
        &sort_push_down.plan,
        parent_requirement.clone(),
        parent_fetch,
    )? {
        // For operators that can take a sort pushdown, continue with updated
        // requirements:
        let current_fetch = sort_push_down.plan.fetch();
        for (child, order) in sort_push_down.children.iter_mut().zip(adjusted) {
            child.data.ordering_requirement = order;
            child.data.fetch = min_fetch(current_fetch, parent_fetch);
        }
        sort_push_down.data.ordering_requirement = None;
    } else {
        // Can not push down requirements, add new `SortExec`:
        sort_push_down = add_sort_above(
            sort_push_down,
            parent_requirement.into_single(),
            parent_fetch,
        );
        assign_initial_requirements(&mut sort_push_down);
    }
    Ok(Transformed::yes(sort_push_down))
}

/// Calculate the pushdown ordering requirements for children.
/// If sort cannot be pushed down, return None.
fn pushdown_requirement_to_children(
    plan: &Arc<dyn ExecutionPlan>,
    parent_required: OrderingRequirements,
    parent_fetch: Option<usize>,
) -> Result<Option<Vec<Option<OrderingRequirements>>>> {
    // If there is a limit on the parent plan we cannot push it down through operators that change the cardinality.
    // E.g. consider if LIMIT 2 is applied below a FilteExec that filters out 1/2 of the rows we'll end up with 1 row instead of 2.
    // If the LIMIT is applied after the FilterExec and the FilterExec returns > 2 rows we'll end up with 2 rows (correct).
    if parent_fetch.is_some() && !plan.supports_limit_pushdown() {
        return Ok(None);
    }
    // Note: we still need to check the cardinality effect of the plan here, because the
    // limit pushdown is not always safe, even if the plan supports it. Here's an example:
    //
    // UnionExec advertises `supports_limit_pushdown() == true` because it can
    // forward a LIMIT k to each of its children—i.e. apply “LIMIT k” separately
    // on each branch before merging them together.
    //
    // However, UnionExec’s `cardinality_effect() == GreaterEqual` (it sums up
    // all child row counts), so pushing a global TopK/LIMIT through it would
    // break the semantics of “take the first k rows of the combined result.”
    //
    // For example, with two branches A and B and k = 3:
    //   — Global LIMIT: take the first 3 rows from (A ∪ B) after merging.
    //   — Pushed down: take 3 from A, 3 from B, then merge → up to 6 rows!
    //
    // That’s why we still block on cardinality: even though UnionExec can
    // push a LIMIT to its children, its GreaterEqual effect means it cannot
    // preserve the global TopK semantics.
    if parent_fetch.is_some() {
        match plan.cardinality_effect() {
            CardinalityEffect::Equal => {
                // safe: only true sources (e.g. CoalesceBatchesExec, ProjectionExec) pass
            }
            _ => return Ok(None),
        }
    }

    let maintains_input_order = plan.maintains_input_order();
    if is_window(plan) {
        let mut required_input_ordering = plan.required_input_ordering();
        let maybe_child_requirement = required_input_ordering.swap_remove(0);
        let child_plan = plan.children().swap_remove(0);
        let Some(child_req) = maybe_child_requirement else {
            return Ok(None);
        };
        match determine_children_requirement(&parent_required, &child_req, child_plan) {
            RequirementsCompatibility::Satisfy => Ok(Some(vec![Some(child_req)])),
            RequirementsCompatibility::Compatible(adjusted) => {
                // If parent requirements are more specific than output ordering
                // of the window plan, then we can deduce that the parent expects
                // an ordering from the columns created by window functions. If
                // that's the case, we block the pushdown of sort operation.
                if !plan
                    .equivalence_properties()
                    .ordering_satisfy_requirement(parent_required.into_single())?
                {
                    return Ok(None);
                }

                Ok(Some(vec![adjusted]))
            }
            RequirementsCompatibility::NonCompatible => Ok(None),
        }
    } else if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        let Some(sort_ordering) = sort_exec.properties().output_ordering().cloned()
        else {
            return internal_err!("SortExec should have output ordering");
        };
        sort_exec
            .properties()
            .eq_properties
            .requirements_compatible(
                parent_required.first().clone(),
                sort_ordering.into(),
            )
            .then(|| Ok(vec![Some(parent_required)]))
            .transpose()
    } else if plan.fetch().is_some()
        && plan.supports_limit_pushdown()
        && plan
            .maintains_input_order()
            .into_iter()
            .all(|maintain| maintain)
    {
        // Push down through operator with fetch when:
        // - requirement is aligned with output ordering
        // - it preserves ordering during execution
        let Some(ordering) = plan.properties().output_ordering() else {
            return Ok(Some(vec![Some(parent_required)]));
        };
        if plan.properties().eq_properties.requirements_compatible(
            parent_required.first().clone(),
            ordering.clone().into(),
        ) {
            Ok(Some(vec![Some(parent_required)]))
        } else {
            Ok(None)
        }
    } else if is_union(plan) {
        // `UnionExec` does not have real sort requirements for its input, we
        // just propagate the sort requirements down:
        Ok(Some(vec![Some(parent_required); plan.children().len()]))
    } else if let Some(smj) = plan.as_any().downcast_ref::<SortMergeJoinExec>() {
        let left_columns_len = smj.left().schema().fields().len();
        let parent_ordering: Vec<PhysicalSortExpr> = parent_required
            .first()
            .iter()
            .cloned()
            .map(Into::into)
            .collect();
        let eqp = smj.properties().equivalence_properties();
        match expr_source_side(eqp, parent_ordering, smj.join_type(), left_columns_len) {
            Some((JoinSide::Left, ordering)) => try_pushdown_requirements_to_join(
                smj,
                parent_required.into_single(),
                ordering,
                JoinSide::Left,
            ),
            Some((JoinSide::Right, ordering)) => {
                let right_offset =
                    smj.schema().fields.len() - smj.right().schema().fields.len();
                let ordering = add_offset_to_physical_sort_exprs(
                    ordering,
                    -(right_offset as isize),
                )?;
                try_pushdown_requirements_to_join(
                    smj,
                    parent_required.into_single(),
                    ordering,
                    JoinSide::Right,
                )
            }
            _ => {
                // Can not decide the expr side for SortMergeJoinExec, can not push down
                Ok(None)
            }
        }
    } else if maintains_input_order.is_empty()
        || !maintains_input_order.iter().any(|o| *o)
        || plan.as_any().is::<RepartitionExec>()
        || plan.as_any().is::<FilterExec>()
        // TODO: Add support for Projection push down
        || plan.as_any().is::<ProjectionExec>()
        || pushdown_would_violate_requirements(&parent_required, plan.as_ref())
    {
        // If the current plan is a leaf node or can not maintain any of the input ordering, can not pushed down requirements.
        // For RepartitionExec, we always choose to not push down the sort requirements even the RepartitionExec(input_partition=1) could maintain input ordering.
        // Pushing down is not beneficial
        Ok(None)
    } else if is_sort_preserving_merge(plan) {
        let new_ordering = LexOrdering::from(parent_required.first().clone());
        let mut spm_eqs = plan.equivalence_properties().clone();
        let old_ordering = spm_eqs.output_ordering().unwrap();
        // Sort preserving merge will have new ordering, one requirement above is pushed down to its below.
        let change = spm_eqs.reorder(new_ordering)?;
        if !change || spm_eqs.ordering_satisfy(old_ordering)? {
            // Can push-down through SortPreservingMergeExec, because parent requirement is finer
            // than SortPreservingMergeExec output ordering.
            Ok(Some(vec![Some(parent_required)]))
        } else {
            // Do not push-down through SortPreservingMergeExec when
            // ordering requirement invalidates requirement of sort preserving merge exec.
            Ok(None)
        }
    } else if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        handle_hash_join(hash_join, parent_required)
    } else {
        handle_custom_pushdown(plan, parent_required, maintains_input_order)
    }
    // TODO: Add support for Projection push down
}

/// Return true if pushing the sort requirements through a node would violate
/// the input sorting requirements for the plan
fn pushdown_would_violate_requirements(
    parent_required: &OrderingRequirements,
    child: &dyn ExecutionPlan,
) -> bool {
    child
        .required_input_ordering()
        .into_iter()
        // If there is no requirement, pushing down would not violate anything.
        .flatten()
        .any(|child_required| {
            // Check if the plan's requirements would still be satisfied if we
            // pushed down the parent requirements:
            child_required
                .into_single()
                .iter()
                .zip(parent_required.first().iter())
                .all(|(c, p)| !c.compatible(p))
        })
}

/// Determine children requirements:
/// - If children requirements are more specific, do not push down parent
///   requirements.
/// - If parent requirements are more specific, push down parent requirements.
/// - If they are not compatible, need to add a sort.
fn determine_children_requirement(
    parent_required: &OrderingRequirements,
    child_requirement: &OrderingRequirements,
    child_plan: &Arc<dyn ExecutionPlan>,
) -> RequirementsCompatibility {
    let eqp = child_plan.equivalence_properties();
    if eqp.requirements_compatible(
        child_requirement.first().clone(),
        parent_required.first().clone(),
    ) {
        // Child requirements are more specific, no need to push down.
        RequirementsCompatibility::Satisfy
    } else if eqp.requirements_compatible(
        parent_required.first().clone(),
        child_requirement.first().clone(),
    ) {
        // Parent requirements are more specific, adjust child's requirements
        // and push down the new requirements:
        RequirementsCompatibility::Compatible(Some(parent_required.clone()))
    } else {
        RequirementsCompatibility::NonCompatible
    }
}

fn try_pushdown_requirements_to_join(
    smj: &SortMergeJoinExec,
    parent_required: LexRequirement,
    sort_exprs: Vec<PhysicalSortExpr>,
    push_side: JoinSide,
) -> Result<Option<Vec<Option<OrderingRequirements>>>> {
    let mut smj_required_orderings = smj.required_input_ordering();

    let ordering = LexOrdering::new(sort_exprs.clone());
    let (new_left_ordering, new_right_ordering) = match push_side {
        JoinSide::Left => {
            let mut left_eq_properties = smj.left().equivalence_properties().clone();
            left_eq_properties.reorder(sort_exprs)?;
            let Some(left_requirement) = smj_required_orderings.swap_remove(0) else {
                return Ok(None);
            };
            if !left_eq_properties
                .ordering_satisfy_requirement(left_requirement.into_single())?
            {
                return Ok(None);
            }
            // After re-ordering, requirement is still satisfied:
            (ordering.as_ref(), smj.right().output_ordering())
        }
        JoinSide::Right => {
            let mut right_eq_properties = smj.right().equivalence_properties().clone();
            right_eq_properties.reorder(sort_exprs)?;
            let Some(right_requirement) = smj_required_orderings.swap_remove(1) else {
                return Ok(None);
            };
            if !right_eq_properties
                .ordering_satisfy_requirement(right_requirement.into_single())?
            {
                return Ok(None);
            }
            // After re-ordering, requirement is still satisfied:
            (smj.left().output_ordering(), ordering.as_ref())
        }
        JoinSide::None => return Ok(None),
    };
    let join_type = smj.join_type();
    let probe_side = SortMergeJoinExec::probe_side(&join_type);
    let new_output_ordering = calculate_join_output_ordering(
        new_left_ordering,
        new_right_ordering,
        join_type,
        smj.left().schema().fields.len(),
        &smj.maintains_input_order(),
        Some(probe_side),
    )?;
    let mut smj_eqs = smj.properties().equivalence_properties().clone();
    if let Some(new_output_ordering) = new_output_ordering {
        // smj will have this ordering when its input changes.
        smj_eqs.reorder(new_output_ordering)?;
    }
    let should_pushdown = smj_eqs.ordering_satisfy_requirement(parent_required)?;
    Ok(should_pushdown.then(|| {
        let mut required_input_ordering = smj.required_input_ordering();
        let new_req = ordering.map(Into::into);
        match push_side {
            JoinSide::Left => {
                required_input_ordering[0] = new_req;
            }
            JoinSide::Right => {
                required_input_ordering[1] = new_req;
            }
            JoinSide::None => unreachable!(),
        }
        required_input_ordering
    }))
}

fn expr_source_side(
    eqp: &EquivalenceProperties,
    mut ordering: Vec<PhysicalSortExpr>,
    join_type: JoinType,
    left_columns_len: usize,
) -> Option<(JoinSide, Vec<PhysicalSortExpr>)> {
    // TODO: Handle the case where a prefix of the ordering comes from the left
    //       and a suffix from the right.
    match join_type {
        JoinType::Inner
        | JoinType::Left
        | JoinType::Right
        | JoinType::Full
        | JoinType::LeftMark
        | JoinType::RightMark => {
            let eq_group = eqp.eq_group();
            let mut right_ordering = ordering.clone();
            let (mut valid_left, mut valid_right) = (true, true);
            for (left, right) in ordering.iter_mut().zip(right_ordering.iter_mut()) {
                let col = left.expr.as_any().downcast_ref::<Column>()?;
                let eq_class = eq_group.get_equivalence_class(&left.expr);
                if col.index() < left_columns_len {
                    if valid_right {
                        valid_right = eq_class.is_some_and(|cls| {
                            for expr in cls.iter() {
                                if expr
                                    .as_any()
                                    .downcast_ref::<Column>()
                                    .is_some_and(|c| c.index() >= left_columns_len)
                                {
                                    right.expr = Arc::clone(expr);
                                    return true;
                                }
                            }
                            false
                        });
                    }
                } else if valid_left {
                    valid_left = eq_class.is_some_and(|cls| {
                        for expr in cls.iter() {
                            if expr
                                .as_any()
                                .downcast_ref::<Column>()
                                .is_some_and(|c| c.index() < left_columns_len)
                            {
                                left.expr = Arc::clone(expr);
                                return true;
                            }
                        }
                        false
                    });
                };
                if !(valid_left || valid_right) {
                    return None;
                }
            }
            if valid_left {
                Some((JoinSide::Left, ordering))
            } else if valid_right {
                Some((JoinSide::Right, right_ordering))
            } else {
                // TODO: Handle the case where we can push down to both sides.
                None
            }
        }
        JoinType::LeftSemi | JoinType::LeftAnti => ordering
            .iter()
            .all(|e| e.expr.as_any().is::<Column>())
            .then_some((JoinSide::Left, ordering)),
        JoinType::RightSemi | JoinType::RightAnti => ordering
            .iter()
            .all(|e| e.expr.as_any().is::<Column>())
            .then_some((JoinSide::Right, ordering)),
    }
}

/// Handles the custom pushdown of parent-required sorting requirements down to
/// the child execution plans, considering whether the input order is maintained.
///
/// # Arguments
///
/// * `plan` - A reference to an `ExecutionPlan` for which the pushdown will be applied.
/// * `parent_required` - The sorting requirements expected by the parent node.
/// * `maintains_input_order` - A vector of booleans indicating whether each child
///   maintains the input order.
///
/// # Returns
///
/// Returns `Ok(Some(Vec<Option<LexRequirement>>))` if the sorting requirements can be
/// pushed down, `Ok(None)` if not. On error, returns a `Result::Err`.
fn handle_custom_pushdown(
    plan: &Arc<dyn ExecutionPlan>,
    parent_required: OrderingRequirements,
    maintains_input_order: Vec<bool>,
) -> Result<Option<Vec<Option<OrderingRequirements>>>> {
    // If the plan has no children, return early:
    if plan.children().is_empty() {
        return Ok(None);
    }

    // Collect all unique column indices used in the parent-required sorting
    // expression:
    let requirement = parent_required.into_single();
    let all_indices: HashSet<usize> = requirement
        .iter()
        .flat_map(|order| {
            collect_columns(&order.expr)
                .iter()
                .map(|col| col.index())
                .collect::<HashSet<_>>()
        })
        .collect();

    // Get the number of fields in each child's schema:
    let children_schema_lengths: Vec<usize> = plan
        .children()
        .iter()
        .map(|c| c.schema().fields().len())
        .collect();

    // Find the index of the order-maintaining child:
    let Some(maintained_child_idx) = maintains_input_order
        .iter()
        .enumerate()
        .find(|(_, m)| **m)
        .map(|pair| pair.0)
    else {
        return Ok(None);
    };

    // Check if all required columns come from the order-maintaining child:
    let start_idx = children_schema_lengths[..maintained_child_idx]
        .iter()
        .sum::<usize>();
    let end_idx = start_idx + children_schema_lengths[maintained_child_idx];
    let all_from_maintained_child =
        all_indices.iter().all(|i| i >= &start_idx && i < &end_idx);

    // If all columns are from the maintained child, update the parent requirements:
    if all_from_maintained_child {
        let sub_offset = children_schema_lengths
            .iter()
            .take(maintained_child_idx)
            .sum::<usize>();
        // Transform the parent-required expression for the child schema by
        // adjusting columns:
        let updated_parent_req = requirement
            .into_iter()
            .map(|req| {
                let child_schema = plan.children()[maintained_child_idx].schema();
                let updated_columns = req
                    .expr
                    .transform_up(|expr| {
                        if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                            let new_index = col.index() - sub_offset;
                            Ok(Transformed::yes(Arc::new(Column::new(
                                child_schema.field(new_index).name(),
                                new_index,
                            ))))
                        } else {
                            Ok(Transformed::no(expr))
                        }
                    })?
                    .data;
                Ok(PhysicalSortRequirement::new(updated_columns, req.options))
            })
            .collect::<Result<Vec<_>>>()?;

        // Prepare the result, populating with the updated requirements for children that maintain order
        let result = maintains_input_order
            .iter()
            .map(|&maintains_order| {
                if maintains_order {
                    LexRequirement::new(updated_parent_req.clone())
                        .map(OrderingRequirements::new)
                } else {
                    None
                }
            })
            .collect();

        Ok(Some(result))
    } else {
        Ok(None)
    }
}

// For hash join we only maintain the input order for the right child
// for join type: Inner, Right, RightSemi, RightAnti
fn handle_hash_join(
    plan: &HashJoinExec,
    parent_required: OrderingRequirements,
) -> Result<Option<Vec<Option<OrderingRequirements>>>> {
    // If the plan has no children or does not maintain the right side ordering,
    // return early:
    if !plan.maintains_input_order()[1] {
        return Ok(None);
    }

    // Collect all unique column indices used in the parent-required sorting expression
    let requirement = parent_required.into_single();
    let all_indices: HashSet<_> = requirement
        .iter()
        .flat_map(|order| {
            collect_columns(&order.expr)
                .into_iter()
                .map(|col| col.index())
                .collect::<HashSet<_>>()
        })
        .collect();

    let column_indices = build_join_column_index(plan);
    let projected_indices: Vec<_> = if let Some(projection) = &plan.projection {
        projection.iter().map(|&i| &column_indices[i]).collect()
    } else {
        column_indices.iter().collect()
    };
    let len_of_left_fields = projected_indices
        .iter()
        .filter(|ci| ci.side == JoinSide::Left)
        .count();

    let all_from_right_child = all_indices.iter().all(|i| *i >= len_of_left_fields);

    // If all columns are from the right child, update the parent requirements
    if all_from_right_child {
        // Transform the parent-required expression for the child schema by adjusting columns
        let updated_parent_req = requirement
            .into_iter()
            .map(|req| {
                let child_schema = plan.children()[1].schema();
                let updated_columns = req
                    .expr
                    .transform_up(|expr| {
                        if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                            let index = projected_indices[col.index()].index;
                            Ok(Transformed::yes(Arc::new(Column::new(
                                child_schema.field(index).name(),
                                index,
                            ))))
                        } else {
                            Ok(Transformed::no(expr))
                        }
                    })?
                    .data;
                Ok(PhysicalSortRequirement::new(updated_columns, req.options))
            })
            .collect::<Result<Vec<_>>>()?;

        // Populating with the updated requirements for children that maintain order
        Ok(Some(vec![
            None,
            LexRequirement::new(updated_parent_req).map(OrderingRequirements::new),
        ]))
    } else {
        Ok(None)
    }
}

// this function is used to build the column index for the hash join
// push down sort requirements to the right child
fn build_join_column_index(plan: &HashJoinExec) -> Vec<ColumnIndex> {
    let map_fields = |schema: SchemaRef, side: JoinSide| {
        schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, _)| ColumnIndex { index, side })
            .collect::<Vec<_>>()
    };

    match plan.join_type() {
        JoinType::Inner | JoinType::Right => {
            map_fields(plan.left().schema(), JoinSide::Left)
                .into_iter()
                .chain(map_fields(plan.right().schema(), JoinSide::Right))
                .collect::<Vec<_>>()
        }
        JoinType::RightSemi | JoinType::RightAnti => {
            map_fields(plan.right().schema(), JoinSide::Right)
        }
        _ => unreachable!("unexpected join type: {}", plan.join_type()),
    }
}

/// Define the Requirements Compatibility
#[derive(Debug)]
enum RequirementsCompatibility {
    /// Requirements satisfy
    Satisfy,
    /// Requirements compatible
    Compatible(Option<OrderingRequirements>),
    /// Requirements not compatible
    NonCompatible,
}
