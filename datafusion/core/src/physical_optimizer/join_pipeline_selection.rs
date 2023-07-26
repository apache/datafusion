// Copyright (C) Synnada, Inc. - All Rights Reserved.
// This file does not contain any Apache Software Foundation copyrighted code.

use crate::physical_optimizer::join_selection::{
    swap_join_type, swap_reverting_projection,
};
use crate::physical_optimizer::sort_enforcement::unbounded_output;
use crate::physical_optimizer::utils::is_hash_join;
use crate::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
use arrow_schema::SortOptions;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, VisitRecursion};
use datafusion_common::{DataFusionError, JoinType};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::{
    get_indices_of_matching_sort_exprs_with_order_eq,
    ordering_satisfy_requirement_concrete,
};
use datafusion_physical_expr::PhysicalSortRequirement;
use itertools::{iproduct, izip, Itertools};
use std::sync::Arc;

/// This object is used within the JoinSelection rule to track the closest
/// [`HashJoinExec`] descendant(s) for every child of a plan.
#[derive(Debug, Clone)]
pub struct PlanWithCorrespondingHashJoin {
    plan: Arc<dyn ExecutionPlan>,
    // For every child, we keep a subtree of `ExecutionPlan`s starting from the
    // child until the `HashJoinExec`(s) that affect the output ordering of the
    // child. If the child has no connection to any `HashJoinExec`, simply store
    // `None` (and not a subtree).
    hash_join_onwards: Vec<Option<MultipleExecTree>>,
}

impl PlanWithCorrespondingHashJoin {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let length = plan.children().len();
        PlanWithCorrespondingHashJoin {
            plan,
            hash_join_onwards: vec![None; length],
        }
    }

    pub fn new_from_children_nodes(
        children_nodes: Vec<PlanWithCorrespondingHashJoin>,
        parent_plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion_common::Result<Self> {
        let children_plans = children_nodes
            .iter()
            .map(|item| item.plan.clone())
            .collect();
        let hash_join_onwards = children_nodes
            .into_iter()
            .enumerate()
            .map(|(idx, item)| {
                let plan = item.plan;
                if plan.children().is_empty() {
                    // Plan has no children, there is nothing to propagate.
                    None
                } else if is_hash_join(&plan)
                    && item.hash_join_onwards.iter().all(|e| e.is_none())
                {
                    Some(MultipleExecTree::new(vec![plan], idx, vec![]))
                } else {
                    let required_orderings = plan.required_input_ordering();
                    let flags = plan.maintains_input_order();
                    let children =
                        izip!(flags, item.hash_join_onwards, required_orderings)
                            .filter_map(|(maintains, element, required_ordering)| {
                                if (required_ordering.is_none() && maintains)
                                    || is_hash_join(&plan)
                                {
                                    element
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<MultipleExecTree>>();
                    if children.is_empty() {
                        None
                    } else {
                        Some(MultipleExecTree::new(vec![plan], idx, children))
                    }
                }
            })
            .collect();
        let plan = with_new_children_if_necessary(parent_plan, children_plans)?.into();
        Ok(PlanWithCorrespondingHashJoin {
            plan,
            hash_join_onwards,
        })
    }

    pub fn children(&self) -> Vec<PlanWithCorrespondingHashJoin> {
        self.plan
            .children()
            .into_iter()
            .map(|child| PlanWithCorrespondingHashJoin::new(child))
            .collect()
    }
}

impl TreeNode for PlanWithCorrespondingHashJoin {
    fn apply_children<F>(&self, op: &mut F) -> datafusion_common::Result<VisitRecursion>
    where
        F: FnMut(&Self) -> datafusion_common::Result<VisitRecursion>,
    {
        for child in self.children() {
            match op(&child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }

        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> datafusion_common::Result<Self>
    where
        F: FnMut(Self) -> datafusion_common::Result<Self>,
    {
        let children = self.children();
        if children.is_empty() {
            Ok(self)
        } else {
            let children_nodes = children
                .into_iter()
                .map(transform)
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            PlanWithCorrespondingHashJoin::new_from_children_nodes(
                children_nodes,
                self.plan,
            )
        }
    }
}

/// This function swaps the inputs of the given SMJ operator.
fn swap_sort_merge_join(
    hash_join: &HashJoinExec,
    keys: Vec<(Column, Column)>,
    sort_options: Vec<SortOptions>,
) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
    let left = hash_join.left();
    let right = hash_join.right();
    let swapped_join_type = swap_join_type(hash_join.join_type);
    if matches!(swapped_join_type, JoinType::RightSemi) {
        return Err(DataFusionError::Plan(
            "RightSemi is not supported for SortMergeJoin".to_owned(),
        ));
    }
    let new_join = SortMergeJoinExec::try_new(
        Arc::clone(right),
        Arc::clone(left),
        keys.iter().map(|(l, r)| (r.clone(), l.clone())).collect(),
        swap_join_type(hash_join.join_type),
        sort_options,
        hash_join.null_equals_null,
    )?;
    if matches!(
        hash_join.join_type,
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightAnti
    ) {
        Ok(Arc::new(new_join))
    } else {
        let proj = ProjectionExec::try_new(
            swap_reverting_projection(&left.schema(), &right.schema()),
            Arc::new(new_join),
        )?;
        Ok(Arc::new(proj))
    }
}

/// This object implements a tree that we use while keeping track of paths
/// leading to [`HashJoinExec`]s.
#[derive(Debug, Clone)]
pub struct MultipleExecTree {
    /// The `ExecutionPlan`s associated with this node.
    pub plans: Vec<Arc<dyn ExecutionPlan>>,
    /// Child index of the plan in its parent.
    pub idx: usize,
    /// Children of the plan that would need updating if we remove leaf executors.
    pub children: Vec<MultipleExecTree>,
}

impl MultipleExecTree {
    /// Create new Exec tree.
    pub fn new(
        plans: Vec<Arc<dyn ExecutionPlan>>,
        idx: usize,
        children: Vec<MultipleExecTree>,
    ) -> Self {
        MultipleExecTree {
            plans,
            idx,
            children,
        }
    }
}

/// Creates a variety of join cases for a given subtree of plans within a local ordering constraint.
///
/// This function recursively traverses the provided `MultipleExecTree` (a tree structure of execution plans),
/// and generates new execution plans that are candidate to preserve required order.
///
/// For each child there will be more than one possible plan that will be propagated to upper.
/// Imagine a parent plan that have 2 child. If obe child has 3 possible plans and other has 2,
/// we propagate 6 possible plans (cartesian product) to the top.
///
/// The generation of new plans is guided by the `ConfigOptions` provided, which might contain specific rules
/// and preferences for the execution plans.
///
/// # Arguments
///
/// * `hash_join_onwards`: A mutable reference to a `MultipleExecTree` object, which contains the current subtree
/// of execution plans that need to be processed.
///
/// * `config_options`: A reference to a `ConfigOptions` object, which provides the configuration settings that guide
/// the creation of new execution plans.
///
/// # Returns
///
/// This function returns a `Result` that contains a `Vec` of `Arc<dyn ExecutionPlan>` objects if the execution
/// plans are successfully created. Each `ExecutionPlan` within the `Vec` is a unique execution plan that are candidate to preserve required order.
/// If any error occurs during the creation of execution plans,
/// the function returns an `Err`.
fn create_join_cases_local_preserve_order(
    hash_join_onwards: &mut MultipleExecTree,
    config_options: &ConfigOptions,
) -> datafusion_common::Result<Vec<Arc<dyn ExecutionPlan>>> {
    // Clone all the plans in the given subtree:
    let plans = hash_join_onwards.plans.clone();

    // Iterate over the children of the subtree, update their plans recursively:
    for item in &mut hash_join_onwards.children {
        item.plans = create_join_cases_local_preserve_order(item, config_options)?;
    }

    // If there is at least one plan...
    if let Some(first_plan) = plans.get(0) {
        // Create a vector of child possibilities for each child index.
        // If a child with the same index exists in `hash_join_onwards.children`,
        // use its plans. Otherwise, use the child from the first plan.
        let get_children: Vec<_> = first_plan
            .children()
            .into_iter()
            .enumerate()
            .map(|(child_index, child)| {
                if let Some(item) = hash_join_onwards
                    .children
                    .iter()
                    .find(|item| item.idx == child_index)
                {
                    item.plans.clone()
                } else {
                    vec![child]
                }
            })
            .collect();

        // Get the cartesian product of all possible children:
        let possible_children = get_children
            .into_iter()
            .map(|v| v.into_iter())
            .multi_cartesian_product();

        // Compute new plans by combining each plan with each possible set of children.
        // Replace the original plans with these new ones:
        hash_join_onwards.plans = iproduct!(plans.into_iter(), possible_children)
            .map(|(plan, children)| {
                let plan = with_new_children_if_necessary(plan, children)?.into();
                Ok(plan)
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;
    }

    // For each plan, check if it is a `HashJoinExec`. If so, check if we can convert
    // it. If not, use the original plan.
    let plans: Vec<_> = hash_join_onwards
        .plans
        .iter()
        .flat_map(|plan| match plan.as_any().downcast_ref::<HashJoinExec>() {
            Some(hash_join) => {
                match check_hash_join_convertable(hash_join, config_options) {
                    Ok(Some(result)) => result,
                    _ => vec![plan.clone()],
                }
            }
            None => vec![plan.clone()],
        })
        .collect();

    Ok(plans)
}

/// Checks if a given `HashJoinExec` can be converted into another form of execution plans,
/// primarily into `SortMergeJoinExec` while still preserving the required conditions.
///
/// This function first extracts key properties from the `HashJoinExec` and then determines
/// the possibility of conversion based on these properties. The conversion mainly involves
/// replacing `HashJoinExec` with `SortMergeJoinExec` for cases where both left and right children
/// are unbounded, and the output ordering can be satisfied.
///
/// If conversion is feasible, this function creates the corresponding `SortMergeJoinExec` instances,
/// handles the possibility of swapping join types, and returns these new plans.
///
/// # Arguments
///
/// * `hash_join`: A reference to a `HashJoinExec` object, which is the hash join execution plan
/// that we are checking for convertibility.
///
/// * `_config_options`: A reference to a `ConfigOptions` object, which provides the configuration settings.
/// However, these options are not used in the current function.
///
/// # Returns
///
/// This function returns a `Result` that contains an `Option`. If the `HashJoinExec` can be converted,
/// the `Option` will contain a `Vec` of `Arc<dyn ExecutionPlan>` objects, each representing a new execution plan.
/// If conversion is not possible, the function returns `None`. Any errors that occur during the conversion process
/// will result in an `Err` being returned.
fn check_hash_join_convertable(
    hash_join: &HashJoinExec,
    _config_options: &ConfigOptions,
) -> datafusion_common::Result<Option<Vec<Arc<dyn ExecutionPlan>>>> {
    let filter = hash_join.filter();
    let (on_left, on_right): (Vec<_>, Vec<_>) = hash_join.on.iter().cloned().unzip();
    let left_order = hash_join.left().output_ordering();
    let right_order = hash_join.right().output_ordering();
    let is_left_unbounded = unbounded_output(hash_join.left());
    let is_right_unbounded = unbounded_output(hash_join.right());
    match (
        is_left_unbounded,
        is_right_unbounded,
        filter,
        left_order,
        right_order,
    ) {
        (true, true, None, Some(left_order), Some(right_order)) => {
            // Get left key(s)' sort options:
            let left_satisfied = get_indices_of_matching_sort_exprs_with_order_eq(
                left_order,
                &on_left,
                || hash_join.left().equivalence_properties(),
                || hash_join.left().ordering_equivalence_properties(),
            );
            // Get right key(s)' sort options:
            let right_satisfied = get_indices_of_matching_sort_exprs_with_order_eq(
                right_order,
                &on_right,
                || hash_join.right().equivalence_properties(),
                || hash_join.right().ordering_equivalence_properties(),
            );

            if let (
                Some((left_satisfied, left_indices)),
                Some((right_satisfied, right_indices)),
            ) = (left_satisfied, right_satisfied)
            {
                // Check if the indices are equal and the sort options are aligned:
                if left_indices == right_indices
                    && left_satisfied
                        .iter()
                        .zip(right_satisfied.iter())
                        .all(|(l, r)| l == r)
                {
                    let adjusted_keys = left_indices
                        .iter()
                        .map(|index| hash_join.on[*index].clone())
                        .collect::<Vec<_>>();
                    let mut plans: Vec<Arc<dyn ExecutionPlan>> = vec![];
                    // SortMergeJoin does not support RightSemi
                    if !matches!(hash_join.join_type, JoinType::RightSemi) {
                        plans.push(Arc::new(SortMergeJoinExec::try_new(
                            hash_join.left.clone(),
                            hash_join.right.clone(),
                            adjusted_keys.clone(),
                            hash_join.join_type,
                            left_satisfied,
                            hash_join.null_equals_null,
                        )?))
                    }
                    if !matches!(swap_join_type(hash_join.join_type), JoinType::RightSemi)
                    {
                        plans.push(swap_sort_merge_join(
                            hash_join,
                            adjusted_keys,
                            right_satisfied,
                        )?);
                    }
                    return Ok(Some(plans));
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

/// Generates and filters a set of execution plans based on a specified ordering requirement.
///
/// This function leverages the `create_join_cases_local_preserve_order` function to generate
/// a variety of execution plans from a given `MultipleExecTree`. It then filters these plans
/// using the `find_suitable_plans` function to only keep those that meet the required ordering.
///
/// # Arguments
///
/// * `hash_join_onward`: A mutable reference to a `MultipleExecTree` object, which contains
/// the current subtree of execution plans to be processed.
///
/// * `required_ordering`: A reference to a slice of `PhysicalSortRequirement` objects,
/// which define the desired ordering for the resulting execution plans.
///
/// * `config_options`: A reference to a `ConfigOptions` object, which provides the configuration
/// settings that guide the creation of new execution plans.
///
/// # Returns
///
/// This function returns a `Result` that contains a `Vec` of `Arc<dyn ExecutionPlan>` objects
/// if the execution plans meeting the ordering requirements are successfully created.
/// If any error occurs during this process, the function returns an `Err`.

fn get_meeting_the_plan_with_required_order(
    hash_join_onward: &mut MultipleExecTree,
    required_ordering: &[PhysicalSortRequirement],
    config_options: &ConfigOptions,
) -> datafusion_common::Result<Vec<Arc<dyn ExecutionPlan>>> {
    let possible_plans =
        create_join_cases_local_preserve_order(hash_join_onward, config_options)?;
    Ok(find_suitable_plans(possible_plans, required_ordering))
}

/// Filters a list of execution plans to keep only those that meet a specified ordering requirement.
///
/// This function iterates through the provided list of `ExecutionPlan` objects and filters out those
/// that do not satisfy the required ordering. The function leverages the `ordering_satisfy_requirement_concrete`
/// function to check if the provided ordering of an execution plan meets the required ordering.
///
/// # Arguments
///
/// * `plans`: A vector of `Arc<dyn ExecutionPlan>` objects. These are the initial execution plans that
/// need to be filtered.
///
/// * `required_ordering`: A reference to a slice of `PhysicalSortRequirement` objects,
/// which define the desired ordering that the execution plans should satisfy.
///
/// # Returns
///
/// The function returns a vector of `Arc<dyn ExecutionPlan>` objects. Each `ExecutionPlan` in the vector
/// is a plan from the input vector that satisfies the required ordering.
fn find_suitable_plans(
    plans: Vec<Arc<dyn ExecutionPlan>>,
    required_ordering: &[PhysicalSortRequirement],
) -> Vec<Arc<dyn ExecutionPlan>> {
    plans
        .into_iter()
        .filter(|plan| {
            plan.output_ordering().map_or(false, |provided_ordering| {
                ordering_satisfy_requirement_concrete(
                    provided_ordering,
                    required_ordering,
                    || plan.equivalence_properties(),
                    || plan.ordering_equivalence_properties(),
                )
            })
        })
        .collect()
}

/// This function chooses the "best" (i.e. cost optimal) plan among all the
/// feasible plans given in `possible_plans`.
///
/// TODO: Until the Datafusion can identify execution costs of the plans, we
///       are selecting the first feasible plan.
fn select_best_plan(
    possible_plans: Vec<Arc<dyn ExecutionPlan>>,
) -> Option<Arc<dyn ExecutionPlan>> {
    possible_plans.first().cloned()
}

/// This subrule tries to modify joins in order to preserve output ordering(s).
/// This will enable downstream rules, such as `EnforceSorting`, to optimize
/// away costly pipeline-breaking sort operations.
pub fn select_joins_to_preserve_order_subrule(
    requirements: PlanWithCorrespondingHashJoin,
    config_options: &ConfigOptions,
) -> datafusion_common::Result<Transformed<PlanWithCorrespondingHashJoin>> {
    // If there are no child nodes, return as is:
    if requirements.plan.children().is_empty() {
        return Ok(Transformed::No(requirements));
    }
    let PlanWithCorrespondingHashJoin {
        plan,
        mut hash_join_onwards,
    } = requirements;

    // If the plan has a required ordering:
    if plan.required_input_ordering().iter().any(|e| e.is_some()) {
        let mut children = plan.children();
        let mut is_transformed = false;

        // Jointly iterate over child nodes and required orderings:
        for (child, required_ordering, hash_join_onward) in izip!(
            children.iter_mut(),
            plan.required_input_ordering().iter(),
            hash_join_onwards.iter_mut()
        ) {
            let required_ordering = match required_ordering {
                Some(req) => req,
                None => continue,
            };
            let hash_join_onward = match hash_join_onward {
                Some(hj) => hj,
                None => continue,
            };
            // Get possible plans meeting the ordering requirements:
            let possible_plans = get_meeting_the_plan_with_required_order(
                hash_join_onward,
                required_ordering,
                config_options,
            )?;
            // If there is a plan that is more optimal, choose it:
            if let Some(plan) = select_best_plan(possible_plans) {
                *child = plan;
                is_transformed = true;
            }
        }
        // If a transformation has occurred, return the new plan:
        if is_transformed {
            return Ok(Transformed::Yes(PlanWithCorrespondingHashJoin {
                plan: plan.with_new_children(children)?,
                hash_join_onwards,
            }));
        }
    } else if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        // If the plan is a `SortExec`, create child requirements:
        let child_requirement = sort_exec
            .expr()
            .iter()
            .cloned()
            .map(PhysicalSortRequirement::from)
            .collect::<Vec<_>>();
        if let Some(hash_join_onward) = hash_join_onwards.get_mut(0).unwrap() {
            // Get possible plans meeting the ordering requirements:
            let possible_plans = get_meeting_the_plan_with_required_order(
                hash_join_onward,
                &child_requirement,
                config_options,
            )?;
            // If there is a plan that is more optimal, choose it:
            if let Some(new_child) = select_best_plan(possible_plans) {
                return Ok(Transformed::Yes(PlanWithCorrespondingHashJoin {
                    plan: new_child,
                    hash_join_onwards: vec![None],
                }));
            }
        };
    };
    // If no transformation is possible or required, return as is:
    Ok(Transformed::No(PlanWithCorrespondingHashJoin {
        plan,
        hash_join_onwards,
    }))
}

/// This function takes the last step to finalize the recursive analysis made
/// by the subrule `select_joins_to_preserve_order_subrule`. If the main plan
/// has an output ordering at the very top, our aim is to maintain that order.
/// For example, this function enables us to handle the plans like:
/// [
///     "FilterExec: NOT d@6",
///     "  HashJoinExec: mode=Partitioned, join_type=Inner, on=\[(c@2, c@2)\], filter=0@0 + 0 > 1@1 - 3 AND 0@0 + 0 < 1@1 + 3",
///     "    StreamingTableExec: partition_sizes=0, projection=\[a, b, c\], infinite_source=true, output_ordering=\[a@0 ASC\]",
///     "    BoundedWindowAggExec: wdw= \[count: Ok(Field { .. }), frame: WindowFrame { .. }], mode=\[Sorted\]",
///     "      HashJoinExec: mode=Partitioned, join_type=Inner, on=\[(c@2, c@2)], filter=0@0 + 0 > 1@1 - 3 AND 0@0 + 0 < 1@1 + 3",
///     "        StreamingTableExec: partition_sizes=0, projection=\[a, b, c\], infinite_source=true, output_ordering=\[a@0 ASC\]",
///     "        StreamingTableExec: partition_sizes=0, projection=\[d, e, c\], infinite_source=true, output_ordering=\[d@0 ASC\]",
/// ]
pub fn finalize_order_preserving_joins_at_root(
    requirements: PlanWithCorrespondingHashJoin,
    config_options: &ConfigOptions,
) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
    let PlanWithCorrespondingHashJoin {
        plan,
        mut hash_join_onwards,
    } = requirements;
    // If the top operator is a `SortExec`, or requires an output ordering, we
    // already make the necessary `HashJoin` replacements in the subrule
    // `select_joins_to_preserve_order_subrule`.
    if plan.as_any().is::<SortExec>()
        || plan.required_input_ordering().iter().any(|e| e.is_some())
    {
        return Ok(plan);
    }
    // Check if the plan has an output ordering. If not, return early.
    let required_ordering = match plan.output_ordering() {
        Some(sort_exprs) => sort_exprs
            .iter()
            .cloned()
            .map(PhysicalSortRequirement::from)
            .collect::<Vec<_>>(),
        None => return Ok(plan),
    };

    // Handle terminal hash joins:
    if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        if let Ok(Some(plans)) = check_hash_join_convertable(hash_join, config_options) {
            let suitable_plans = find_suitable_plans(plans, &required_ordering);
            if let Some(plan) = select_best_plan(suitable_plans) {
                return Ok(plan);
            }
        }
    }
    let children_cases = hash_join_onwards
        .iter_mut()
        .filter_map(|hj| {
            hj.as_mut()
                .map(|hj| create_join_cases_local_preserve_order(hj, config_options))
        })
        .collect::<datafusion_common::Result<Vec<_>>>()?
        .into_iter()
        .multi_cartesian_product();
    let possible_plans = children_cases
        .map(|children| plan.clone().with_new_children(children))
        .collect::<datafusion_common::Result<Vec<_>>>()?;
    let suitable_plans = find_suitable_plans(possible_plans, &required_ordering);
    if let Some(plan) = select_best_plan(suitable_plans) {
        return Ok(plan);
    }
    Ok(plan)
}

#[cfg(test)]
mod order_preserving_join_swap_tests {
    use std::sync::Arc;

    use crate::physical_optimizer::join_selection::JoinSelection;
    use crate::physical_optimizer::sort_enforcement::EnforceSorting;
    use crate::physical_optimizer::test_utils::{
        memory_exec_with_sort, sort_expr_options,
    };
    use crate::physical_optimizer::PhysicalOptimizerRule;
    use crate::physical_plan::joins::utils::{ColumnIndex, JoinSide};
    use crate::physical_plan::windows::create_window_expr;
    use crate::physical_plan::{displayable, ExecutionPlan};
    use crate::prelude::SessionContext;
    use crate::{
        assert_optimized_orthogonal,
        physical_optimizer::test_utils::{
            bounded_window_exec, filter_exec, hash_join_exec, prunable_filter, sort_exec,
            sort_expr, streaming_table_exec,
        },
    };
    use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
    use datafusion_common::Result;
    use datafusion_expr::{BuiltInWindowFunction, JoinType, WindowFrame, WindowFunction};
    use datafusion_physical_expr::expressions::{col, Column, NotExpr};
    use datafusion_physical_expr::PhysicalSortExpr;

    // Util function to get string representation of a physical plan
    fn get_plan_string(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
        let formatted = displayable(plan.as_ref()).indent(true).to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();
        actual.iter().map(|elem| elem.to_string()).collect()
    }

    fn create_test_schema() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, false);
        let c = Field::new("c", DataType::Int32, true);

        let schema = Arc::new(Schema::new(vec![a, b, c]));
        Ok(schema)
    }

    fn create_test_schema2() -> Result<SchemaRef> {
        let d = Field::new("d", DataType::Int32, false);
        let e = Field::new("e", DataType::Int32, false);
        let c = Field::new("c", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![d, e, c]));
        Ok(schema)
    }

    fn col_indices(name: &str, schema: &Schema, side: JoinSide) -> ColumnIndex {
        ColumnIndex {
            index: schema.index_of(name).unwrap(),
            side,
        }
    }

    #[tokio::test]
    async fn test_multiple_options_for_sort_merge_joins() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;
        let left_input =
            streaming_table_exec(&left_schema, Some(vec![sort_expr("a", &left_schema)]));
        let right_input = streaming_table_exec(
            &right_schema,
            Some(vec![sort_expr("d", &right_schema)]),
        );
        let on = vec![(
            Column::new_with_schema("a", &left_schema)?,
            Column::new_with_schema("d", &right_schema)?,
        )];
        let join = hash_join_exec(left_input, right_input, on, None, &JoinType::Inner)?;
        let join_schema = join.schema();
        let window_sort_expr = vec![sort_expr("d", &join_schema)];
        let sort = sort_exec(window_sort_expr.clone(), join);

        // Second layer
        let left_input =
            streaming_table_exec(&left_schema, Some(vec![sort_expr("a", &left_schema)]));
        let right_input = bounded_window_exec("b", window_sort_expr, sort);
        let right_schema = right_input.schema();
        let on = vec![(
            Column::new_with_schema("a", &left_schema)?,
            Column::new_with_schema("d", &join_schema)?,
        )];
        let join = hash_join_exec(left_input, right_input, on, None, &JoinType::Inner)?;

        // Third layer
        let left_input = join.clone();
        let left_schema = join.schema();
        let right_input = streaming_table_exec(
            &right_schema,
            Some(vec![sort_expr("e", &right_schema)]),
        );
        let on = vec![(
            Column::new_with_schema("a", &left_schema)?,
            Column::new_with_schema("e", &right_schema)?,
        )];
        let join = hash_join_exec(left_input, right_input, on, None, &JoinType::Inner)?;
        let join_schema = join.schema();
        // Third join
        let window_sort_expr = vec![sort_expr("a", &join_schema)];
        let sort = sort_exec(window_sort_expr.clone(), join);
        let physical_plan = bounded_window_exec("b", window_sort_expr, sort);

        let expected_input = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "  SortExec: expr=[a@0 ASC]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, e@4)]",
            "      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, d@3)]",
            "        StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "        BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "          SortExec: expr=[d@3 ASC]",
            "            HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, d@0)]",
            "              StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "              StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC]",
            "      StreamingTableExec: partition_sizes=0, projection=[a, b, c, d, e, c, count], infinite_source=true, output_ordering=[e@4 ASC]",
        ];
        let expected_optimized = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "  SortMergeJoin: join_type=Inner, on=[(a@0, e@4)]",
            "    SortMergeJoin: join_type=Inner, on=[(a@0, d@3)]",
            "      StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "      BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "        SortMergeJoin: join_type=Inner, on=[(a@0, d@0)]",
            "          StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "          StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC]",
            "    StreamingTableExec: partition_sizes=0, projection=[a, b, c, d, e, c, count], infinite_source=true, output_ordering=[e@4 ASC]",
        ];
        assert_optimized_orthogonal!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_multilayer_joins_with_sort_preserve() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;
        let left_input =
            streaming_table_exec(&left_schema, Some(vec![sort_expr("a", &left_schema)]));
        let right_input = streaming_table_exec(
            &right_schema,
            Some(vec![sort_expr("d", &right_schema)]),
        );
        let on = vec![(
            Column::new_with_schema("a", &left_schema)?,
            Column::new_with_schema("d", &right_schema)?,
        )];
        let join = hash_join_exec(left_input, right_input, on, None, &JoinType::Inner)?;
        let join_schema = join.schema();
        let window_sort_expr = vec![sort_expr("d", &join_schema)];
        let sort = sort_exec(window_sort_expr.clone(), join);
        // Second layer
        let left_input =
            streaming_table_exec(&left_schema, Some(vec![sort_expr("a", &left_schema)]));
        let right_input = bounded_window_exec("b", window_sort_expr, sort);
        let right_schema = right_input.schema();
        let on = vec![(
            Column::new_with_schema("a", &left_schema)?,
            Column::new_with_schema("d", &right_schema)?,
        )];
        let join = hash_join_exec(left_input, right_input, on, None, &JoinType::Inner)?;
        let join_schema = join.schema();
        let window_sort_expr = vec![sort_expr("d", &join_schema)];
        let physical_plan = sort_exec(window_sort_expr, join);

        // We expect that EnforceSorting will remove the SortExec.
        let expected_input = vec![
            "SortExec: expr=[d@6 ASC]",
            "  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, d@3)]",
            "    StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "      SortExec: expr=[d@3 ASC]",
            "        HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, d@0)]",
            "          StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "          StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC]",
        ];
        let expected_optimized = vec![
            "SortMergeJoin: join_type=Inner, on=[(a@0, d@3)]",
            "  StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "    SortMergeJoin: join_type=Inner, on=[(a@0, d@0)]",
            "      StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "      StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC]",
        ];
        assert_optimized_orthogonal!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_multilayer_joins_with_sort_preserve_2() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;
        let left_input =
            streaming_table_exec(&left_schema, Some(vec![sort_expr("a", &left_schema)]));
        let right_input = streaming_table_exec(
            &right_schema,
            Some(vec![sort_expr("d", &right_schema)]),
        );
        let on = vec![(
            Column::new_with_schema("a", &left_schema)?,
            Column::new_with_schema("d", &right_schema)?,
        )];
        let join = hash_join_exec(left_input, right_input, on, None, &JoinType::Inner)?;
        let join_schema = join.schema();
        let window_sort_expr = vec![sort_expr("d", &join_schema)];
        let sort = sort_exec(window_sort_expr.clone(), join);
        // Second layer
        let left_input =
            streaming_table_exec(&left_schema, Some(vec![sort_expr("a", &left_schema)]));
        let right_input = bounded_window_exec("b", window_sort_expr, sort);
        let right_schema = right_input.schema();
        let on = vec![(
            Column::new_with_schema("a", &left_schema)?,
            Column::new_with_schema("d", &right_schema)?,
        )];
        let join = hash_join_exec(left_input, right_input, on, None, &JoinType::Inner)?;
        let join_schema = join.schema();
        let window_sort_expr = vec![sort_expr("d", &join_schema)];
        let sort = sort_exec(window_sort_expr, join);
        let physical_plan = filter_exec(
            Arc::new(NotExpr::new(col("d", join_schema.as_ref()).unwrap())),
            sort,
        );

        // We expect that EnforceSorting will remove the SortExec.
        let expected_input = vec![
            "FilterExec: NOT d@6",
            "  SortExec: expr=[d@6 ASC]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, d@3)]",
            "      StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "      BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "        SortExec: expr=[d@3 ASC]",
            "          HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, d@0)]",
            "            StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "            StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC]",
        ];
        let expected_optimized = vec![
            "FilterExec: NOT d@6",
            "  SortMergeJoin: join_type=Inner, on=[(a@0, d@3)]",
            "    StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "      SortMergeJoin: join_type=Inner, on=[(a@0, d@0)]",
            "        StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "        StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC]",
        ];
        assert_optimized_orthogonal!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_options_for_sort_merge_joins_different_joins() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;
        let left_input =
            streaming_table_exec(&left_schema, Some(vec![sort_expr("a", &left_schema)]));
        let right_input = streaming_table_exec(
            &right_schema,
            Some(vec![sort_expr("d", &right_schema)]),
        );
        let on = vec![(
            Column::new_with_schema("a", &left_schema)?,
            Column::new_with_schema("d", &right_schema)?,
        )];
        let join = hash_join_exec(left_input, right_input, on, None, &JoinType::Right)?;
        let join_schema = join.schema();
        let window_sort_expr = vec![sort_expr("d", &join_schema)];
        let sort = sort_exec(window_sort_expr.clone(), join);

        // Second layer
        let left_input =
            streaming_table_exec(&left_schema, Some(vec![sort_expr("a", &left_schema)]));
        let right_input = bounded_window_exec("b", window_sort_expr, sort);
        let right_schema = right_input.schema();
        let on = vec![(
            Column::new_with_schema("a", &left_schema)?,
            Column::new_with_schema("d", &join_schema)?,
        )];
        let join = hash_join_exec(left_input, right_input, on, None, &JoinType::Left)?;

        // Third layer
        let left_input = join.clone();
        let left_schema = join.schema();
        let right_input = streaming_table_exec(
            &right_schema,
            Some(vec![sort_expr("e", &right_schema)]),
        );
        let on = vec![(
            Column::new_with_schema("a", &left_schema)?,
            Column::new_with_schema("e", &right_schema)?,
        )];
        let join = hash_join_exec(left_input, right_input, on, None, &JoinType::Left)?;
        let join_schema = join.schema();
        // Third join
        let window_sort_expr = vec![sort_expr("a", &join_schema)];
        let sort = sort_exec(window_sort_expr.clone(), join);
        let physical_plan = bounded_window_exec("b", window_sort_expr, sort);

        let expected_input = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "  SortExec: expr=[a@0 ASC]",
            "    HashJoinExec: mode=Partitioned, join_type=Left, on=[(a@0, e@4)]",
            "      HashJoinExec: mode=Partitioned, join_type=Left, on=[(a@0, d@3)]",
            "        StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "        BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "          SortExec: expr=[d@3 ASC]",
            "            HashJoinExec: mode=Partitioned, join_type=Right, on=[(a@0, d@0)]",
            "              StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "              StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC]",
            "      StreamingTableExec: partition_sizes=0, projection=[a, b, c, d, e, c, count], infinite_source=true, output_ordering=[e@4 ASC]",
        ];
        let expected_optimized = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "  SortMergeJoin: join_type=Left, on=[(a@0, e@4)]",
            "    SortMergeJoin: join_type=Left, on=[(a@0, d@3)]",
            "      StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "      BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "        SortMergeJoin: join_type=Right, on=[(a@0, d@0)]",
            "          StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC]",
            "          StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC]",
            "    StreamingTableExec: partition_sizes=0, projection=[a, b, c, d, e, c, count], infinite_source=true, output_ordering=[e@4 ASC]",
        ];
        assert_optimized_orthogonal!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    pub fn bounded_window_exec_row_number(
        col_name: &str,
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs: Vec<_> = sort_exprs.into_iter().collect();
        let schema = input.schema();

        Arc::new(
            crate::physical_plan::windows::BoundedWindowAggExec::try_new(
                vec![create_window_expr(
                    &WindowFunction::BuiltInWindowFunction(
                        BuiltInWindowFunction::RowNumber,
                    ),
                    "row_number".to_owned(),
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
                crate::physical_plan::windows::PartitionSearchMode::Sorted,
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_order_equivalance() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;
        let left_input = streaming_table_exec(
            &left_schema,
            Some(vec![sort_expr_options(
                "a",
                &left_schema,
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            )]),
        );
        let window_sort_expr = vec![sort_expr_options(
            "d",
            &right_schema,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )];
        let right_input =
            streaming_table_exec(&right_schema, Some(window_sort_expr.clone()));

        let window = bounded_window_exec_row_number("d", window_sort_expr, right_input);
        let on = vec![(
            Column::new_with_schema("a", &left_schema)?,
            Column::new_with_schema("row_number", &window.schema())?,
        )];
        let join = hash_join_exec(left_input, window, on, None, &JoinType::Inner)?;
        let physical_plan = sort_exec(
            vec![sort_expr_options(
                "row_number",
                &join.schema(),
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            )],
            join,
        );
        let expected_input = vec![
            "SortExec: expr=[row_number@6 ASC NULLS LAST]",
            "  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, row_number@3)]",
            "    StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
            "    BoundedWindowAggExec: wdw=[row_number: Ok(Field { name: \"row_number\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "      StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC NULLS LAST]",
        ];
        let expected_optimized = vec![
            "SortMergeJoin: join_type=Inner, on=[(a@0, row_number@3)]",
            "  StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
            "  BoundedWindowAggExec: wdw=[row_number: Ok(Field { name: \"row_number\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "    StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC NULLS LAST]",
        ];
        assert_optimized_orthogonal!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_order_equivalance_2() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;
        let asc_null_last = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let left_input = streaming_table_exec(
            &left_schema,
            Some(vec![sort_expr_options("a", &left_schema, asc_null_last)]),
        );
        let right_input = streaming_table_exec(
            &right_schema,
            Some(vec![sort_expr_options("d", &right_schema, asc_null_last)]),
        );
        let window_sort_expr = vec![sort_expr_options("d", &right_schema, asc_null_last)];
        let window = bounded_window_exec_row_number("d", window_sort_expr, right_input);
        let on = vec![(
            Column::new_with_schema("a", &left_schema)?,
            Column::new_with_schema("d", &window.schema())?,
        )];
        let join = hash_join_exec(left_input, window, on, None, &JoinType::Inner)?;
        let physical_plan = sort_exec(
            vec![sort_expr_options(
                "row_number",
                &join.schema(),
                asc_null_last,
            )],
            join,
        );
        let expected_input = vec![
            "SortExec: expr=[row_number@6 ASC NULLS LAST]",
            "  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, d@0)]",
            "    StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
            "    BoundedWindowAggExec: wdw=[row_number: Ok(Field { name: \"row_number\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "      StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC NULLS LAST]",
        ];
        let expected_optimized = vec![
            "ProjectionExec: expr=[a@4 as a, b@5 as b, c@6 as c, d@0 as d, e@1 as e, c@2 as c, row_number@3 as row_number]",
            "  SortMergeJoin: join_type=Inner, on=[(d@0, a@0)]",
            "    BoundedWindowAggExec: wdw=[row_number: Ok(Field { name: \"row_number\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "      StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC NULLS LAST]",
            "    StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
        assert_optimized_orthogonal!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_order_equivalance_3() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;
        let asc_null_last = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let left_input = streaming_table_exec(
            &left_schema,
            Some(vec![sort_expr_options("a", &left_schema, asc_null_last)]),
        );
        let right_input = streaming_table_exec(
            &right_schema,
            Some(vec![sort_expr_options("d", &right_schema, asc_null_last)]),
        );
        let window_sort_expr = vec![sort_expr_options("d", &right_schema, asc_null_last)];
        let window = bounded_window_exec_row_number("d", window_sort_expr, right_input);
        let on = vec![(
            Column::new_with_schema("a", &left_schema)?,
            Column::new_with_schema("d", &window.schema())?,
        )];
        let join =
            hash_join_exec(left_input.clone(), window, on, None, &JoinType::Inner)?;
        let on = vec![(
            Column::new_with_schema("row_number", &join.schema())?,
            Column::new_with_schema("a", &left_schema)?,
        )];
        let join_2 = hash_join_exec(join, left_input, on, None, &JoinType::Inner)?;
        let physical_plan = sort_exec(
            vec![sort_expr_options("a", &join_2.schema(), asc_null_last)],
            join_2,
        );
        let expected_input = vec![
            "SortExec: expr=[a@0 ASC NULLS LAST]",
            "  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(row_number@6, a@0)]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, d@0)]",
            "      StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
            "      BoundedWindowAggExec: wdw=[row_number: Ok(Field { name: \"row_number\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "        StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC NULLS LAST]",
            "    StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
        let expected_optimized = vec![
            "SortMergeJoin: join_type=Inner, on=[(row_number@6, a@0)]",
            "  ProjectionExec: expr=[a@4 as a, b@5 as b, c@6 as c, d@0 as d, e@1 as e, c@2 as c, row_number@3 as row_number]",
            "    SortMergeJoin: join_type=Inner, on=[(d@0, a@0)]",
            "      BoundedWindowAggExec: wdw=[row_number: Ok(Field { name: \"row_number\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "        StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC NULLS LAST]",
            "      StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
            "  StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
        assert_optimized_orthogonal!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_order_equivalence_4() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;
        let asc_null_last = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let left_input = streaming_table_exec(
            &left_schema,
            Some(vec![
                sort_expr_options("a", &left_schema, asc_null_last),
                sort_expr_options("b", &left_schema, asc_null_last),
            ]),
        );
        let right_input = streaming_table_exec(
            &right_schema,
            Some(vec![
                sort_expr_options("d", &right_schema, asc_null_last),
                sort_expr_options("e", &right_schema, asc_null_last),
            ]),
        );
        let window_sort_expr = vec![sort_expr_options("d", &right_schema, asc_null_last)];
        let window = bounded_window_exec_row_number("d", window_sort_expr, right_input);
        let on = vec![
            (
                Column::new_with_schema("a", &left_schema)?,
                Column::new_with_schema("d", &window.schema())?,
            ),
            (
                Column::new_with_schema("b", &left_schema)?,
                Column::new_with_schema("e", &window.schema())?,
            ),
        ];
        let join = hash_join_exec(left_input, window, on, None, &JoinType::Inner)?;
        let physical_plan = sort_exec(
            vec![sort_expr_options(
                "row_number",
                &join.schema(),
                asc_null_last,
            )],
            join,
        );
        let expected_input = vec![
            "SortExec: expr=[row_number@6 ASC NULLS LAST]",
            "  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(a@0, d@0), (b@1, e@1)]",
            "    StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST, b@1 ASC NULLS LAST]",
            "    BoundedWindowAggExec: wdw=[row_number: Ok(Field { name: \"row_number\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "      StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC NULLS LAST, e@1 ASC NULLS LAST]",
        ];
        let expected_optimized = vec![
            "ProjectionExec: expr=[a@4 as a, b@5 as b, c@6 as c, d@0 as d, e@1 as e, c@2 as c, row_number@3 as row_number]",
            "  SortMergeJoin: join_type=Inner, on=[(d@0, a@0), (e@1, b@1)]",
            "    BoundedWindowAggExec: wdw=[row_number: Ok(Field { name: \"row_number\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "      StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC NULLS LAST, e@1 ASC NULLS LAST]",
            "    StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST, b@1 ASC NULLS LAST]",
        ];
        assert_optimized_orthogonal!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_order_equivalence_5() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;
        let asc_null_last = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let left_input = streaming_table_exec(
            &left_schema,
            Some(vec![
                sort_expr_options("a", &left_schema, asc_null_last),
                sort_expr_options("b", &left_schema, asc_null_last),
            ]),
        );
        let right_input = streaming_table_exec(
            &right_schema,
            Some(vec![
                sort_expr_options("d", &right_schema, asc_null_last),
                sort_expr_options("e", &right_schema, asc_null_last),
            ]),
        );
        let window_sort_expr = vec![sort_expr_options("d", &right_schema, asc_null_last)];
        let window = bounded_window_exec_row_number("d", window_sort_expr, right_input);
        let on = vec![
            (
                Column::new_with_schema("b", &left_schema)?,
                Column::new_with_schema("e", &window.schema())?,
            ),
            (
                Column::new_with_schema("a", &left_schema)?,
                Column::new_with_schema("d", &window.schema())?,
            ),
        ];
        let join = hash_join_exec(left_input, window, on, None, &JoinType::Inner)?;
        let physical_plan = sort_exec(
            vec![sort_expr_options(
                "row_number",
                &join.schema(),
                asc_null_last,
            )],
            join,
        );
        // This plan requires
        //  - Key replace
        //  - Child swap
        // to satisfy the SortExec requirement.
        let expected_input = vec![
            "SortExec: expr=[row_number@6 ASC NULLS LAST]",
            "  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(b@1, e@1), (a@0, d@0)]",
            "    StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST, b@1 ASC NULLS LAST]",
            "    BoundedWindowAggExec: wdw=[row_number: Ok(Field { name: \"row_number\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "      StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC NULLS LAST, e@1 ASC NULLS LAST]",
        ];
        let expected_optimized = vec![
            "ProjectionExec: expr=[a@4 as a, b@5 as b, c@6 as c, d@0 as d, e@1 as e, c@2 as c, row_number@3 as row_number]",
            "  SortMergeJoin: join_type=Inner, on=[(d@0, a@0), (e@1, b@1)]",
            "    BoundedWindowAggExec: wdw=[row_number: Ok(Field { name: \"row_number\", data_type: UInt64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "      StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true, output_ordering=[d@0 ASC NULLS LAST, e@1 ASC NULLS LAST]",
            "    StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST, b@1 ASC NULLS LAST]",
        ];
        assert_optimized_orthogonal!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_not_change_join() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;
        let left_input =
            memory_exec_with_sort(&left_schema, Some(vec![sort_expr("a", &left_schema)]));
        let right_input = memory_exec_with_sort(
            &right_schema,
            Some(vec![sort_expr("d", &right_schema)]),
        );
        let prunable_filter = prunable_filter(
            col_indices("a", &left_schema, JoinSide::Left),
            col_indices("d", &right_schema, JoinSide::Right),
        );
        let on = vec![(
            Column::new_with_schema("c", &left_schema)?,
            Column::new_with_schema("c", &right_schema)?,
        )];
        let join = hash_join_exec(
            left_input,
            right_input,
            on,
            Some(prunable_filter),
            &JoinType::Inner,
        )?;
        let join_schema = join.schema();
        // Requires a order that no possible join exchange satisfy.
        let window_sort_expr = vec![sort_expr("e", &join_schema)];
        let physical_plan = bounded_window_exec("d", window_sort_expr, join);

        let expected_input = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@2, c@2)], filter=0@0 + 0 > 1@1 - 3 AND 0@0 + 0 < 1@1 + 3",
            "    MemoryExec: partitions=0, partition_sizes=[]",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }], mode=[Sorted]",
            "  SortExec: expr=[e@4 ASC]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@2, c@2)], filter=0@0 + 0 > 1@1 - 3 AND 0@0 + 0 < 1@1 + 3",
            "      MemoryExec: partitions=0, partition_sizes=[]",
            "      MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized_orthogonal!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_hash_join_streamable() -> Result<()> {
        let left_schema = create_test_schema()?;
        let right_schema = create_test_schema2()?;
        let left_input = streaming_table_exec(&left_schema, None);
        let right_input = streaming_table_exec(&right_schema, None);
        let prunable_filter = prunable_filter(
            col_indices("a", &left_schema, JoinSide::Left),
            col_indices("d", &right_schema, JoinSide::Right),
        );
        let on = vec![(
            Column::new_with_schema("c", &left_schema)?,
            Column::new_with_schema("c", &right_schema)?,
        )];
        let physical_plan = hash_join_exec(
            left_input,
            right_input,
            on,
            Some(prunable_filter),
            &JoinType::Inner,
        )?;

        let expected_input = vec![
            "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@2, c@2)], filter=0@0 + 0 > 1@1 - 3 AND 0@0 + 0 < 1@1 + 3",
            "  StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true",
            "  StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true",
        ];
        let expected_optimized = vec![
            "SymmetricHashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@2, c@2)], filter=0@0 + 0 > 1@1 - 3 AND 0@0 + 0 < 1@1 + 3",
            "  StreamingTableExec: partition_sizes=0, projection=[a, b, c], infinite_source=true",
            "  StreamingTableExec: partition_sizes=0, projection=[d, e, c], infinite_source=true",
        ];
        assert_optimized_orthogonal!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }
}
