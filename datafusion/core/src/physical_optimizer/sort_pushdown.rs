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
use crate::physical_optimizer::utils::{add_sort_above, is_limit, is_union, is_window};
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::joins::utils::JoinSide;
use crate::physical_plan::joins::SortMergeJoinExec;
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
use datafusion_common::tree_node::{Transformed, TreeNode, VisitRecursion};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::JoinType;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::{
    ordering_satisfy_requirement, requirements_compatible,
};
use datafusion_physical_expr::{PhysicalSortExpr, PhysicalSortRequirement};
use itertools::izip;
use std::ops::Deref;
use std::sync::Arc;

/// This is a "data class" we use within the [`EnforceSorting`] rule to push
/// down [`SortExec`] in the plan. In some cases, we can reduce the total
/// computational cost by pushing down `SortExec`s through some executors.
///
/// [`EnforceSorting`]: crate::physical_optimizer::sort_enforcement::EnforceSorting
#[derive(Debug, Clone)]
pub(crate) struct SortPushDown {
    /// Current plan
    pub plan: Arc<dyn ExecutionPlan>,
    /// Parent required sort ordering
    required_ordering: Option<Vec<PhysicalSortRequirement>>,
    /// The adjusted request sort ordering to children.
    /// By default they are the same as the plan's required input ordering, but can be adjusted based on parent required sort ordering properties.
    adjusted_request_ordering: Vec<Option<Vec<PhysicalSortRequirement>>>,
}

impl SortPushDown {
    pub fn init(plan: Arc<dyn ExecutionPlan>) -> Self {
        let request_ordering = plan.required_input_ordering();
        SortPushDown {
            plan,
            required_ordering: None,
            adjusted_request_ordering: request_ordering,
        }
    }

    pub fn children(&self) -> Vec<SortPushDown> {
        izip!(
            self.plan.children().into_iter(),
            self.adjusted_request_ordering.clone().into_iter(),
        )
        .map(|(child, from_parent)| {
            let child_request_ordering = child.required_input_ordering();
            SortPushDown {
                plan: child,
                required_ordering: from_parent,
                adjusted_request_ordering: child_request_ordering,
            }
        })
        .collect()
    }
}

impl TreeNode for SortPushDown {
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

    fn map_children<F>(mut self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if !children.is_empty() {
            let children_plans = children
                .into_iter()
                .map(transform)
                .map(|r| r.map(|s| s.plan))
                .collect::<Result<Vec<_>>>()?;

            match with_new_children_if_necessary(self.plan, children_plans)? {
                Transformed::Yes(plan) | Transformed::No(plan) => {
                    self.plan = plan;
                }
            }
        };
        Ok(self)
    }
}

pub(crate) fn pushdown_sorts(
    requirements: SortPushDown,
) -> Result<Transformed<SortPushDown>> {
    let plan = &requirements.plan;
    let parent_required = requirements.required_ordering.as_deref();
    const ERR_MSG: &str = "Expects parent requirement to contain something";
    let err = || DataFusionError::Plan(ERR_MSG.to_string());
    if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        let mut new_plan = plan.clone();
        if !ordering_satisfy_requirement(
            plan.output_ordering(),
            parent_required,
            || plan.equivalence_properties(),
            || plan.ordering_equivalence_properties(),
        ) {
            // If the current plan is a SortExec, modify it to satisfy parent requirements:
            let parent_required_expr = PhysicalSortRequirement::to_sort_exprs(
                parent_required.ok_or_else(err)?.iter().cloned(),
            );
            new_plan = sort_exec.input.clone();
            add_sort_above(&mut new_plan, parent_required_expr)?;
        };
        let required_ordering = new_plan
            .output_ordering()
            .map(PhysicalSortRequirement::from_sort_exprs);
        // Since new_plan is a SortExec, we can safely get the 0th index.
        let child = &new_plan.children()[0];
        if let Some(adjusted) =
            pushdown_requirement_to_children(child, required_ordering.as_deref())?
        {
            // Can push down requirements
            Ok(Transformed::Yes(SortPushDown {
                plan: child.clone(),
                required_ordering: None,
                adjusted_request_ordering: adjusted,
            }))
        } else {
            // Can not push down requirements
            Ok(Transformed::Yes(SortPushDown::init(new_plan)))
        }
    } else {
        // Executors other than SortExec
        if ordering_satisfy_requirement(
            plan.output_ordering(),
            parent_required,
            || plan.equivalence_properties(),
            || plan.ordering_equivalence_properties(),
        ) {
            // Satisfies parent requirements, immediately return.
            return Ok(Transformed::Yes(SortPushDown {
                required_ordering: None,
                ..requirements
            }));
        }
        // Can not satisfy the parent requirements, check whether the requirements can be pushed down:
        if let Some(adjusted) = pushdown_requirement_to_children(plan, parent_required)? {
            Ok(Transformed::Yes(SortPushDown {
                plan: plan.clone(),
                required_ordering: None,
                adjusted_request_ordering: adjusted,
            }))
        } else {
            // Can not push down requirements, add new SortExec:
            let parent_required_expr = PhysicalSortRequirement::to_sort_exprs(
                parent_required.ok_or_else(err)?.iter().cloned(),
            );
            let mut new_plan = plan.clone();
            add_sort_above(&mut new_plan, parent_required_expr)?;
            Ok(Transformed::Yes(SortPushDown::init(new_plan)))
        }
    }
}

fn pushdown_requirement_to_children(
    plan: &Arc<dyn ExecutionPlan>,
    parent_required: Option<&[PhysicalSortRequirement]>,
) -> Result<Option<Vec<Option<Vec<PhysicalSortRequirement>>>>> {
    const ERR_MSG: &str = "Expects parent requirement to contain something";
    let err = || DataFusionError::Plan(ERR_MSG.to_string());
    let maintains_input_order = plan.maintains_input_order();
    if is_window(plan) {
        let required_input_ordering = plan.required_input_ordering();
        let request_child = required_input_ordering[0].as_deref();
        let child_plan = plan.children()[0].clone();
        match determine_children_requirement(parent_required, request_child, child_plan) {
            RequirementsCompatibility::Satisfy => {
                Ok(Some(vec![request_child.map(|r| r.to_vec())]))
            }
            RequirementsCompatibility::Compatible(adjusted) => Ok(Some(vec![adjusted])),
            RequirementsCompatibility::NonCompatible => Ok(None),
        }
    } else if is_union(plan) {
        // UnionExec does not have real sort requirements for its input. Here we change the adjusted_request_ordering to UnionExec's output ordering and
        // propagate the sort requirements down to correct the unnecessary descendant SortExec under the UnionExec
        Ok(Some(vec![
            parent_required.map(|elem| elem.to_vec());
            plan.children().len()
        ]))
    } else if let Some(smj) = plan.as_any().downcast_ref::<SortMergeJoinExec>() {
        // If the current plan is SortMergeJoinExec
        let left_columns_len = smj.left.schema().fields().len();
        let parent_required_expr = PhysicalSortRequirement::to_sort_exprs(
            parent_required.ok_or_else(err)?.iter().cloned(),
        );
        let expr_source_side =
            expr_source_sides(&parent_required_expr, smj.join_type, left_columns_len);
        match expr_source_side {
            Some(JoinSide::Left) if maintains_input_order[0] => {
                try_pushdown_requirements_to_join(
                    plan,
                    parent_required,
                    parent_required_expr,
                    JoinSide::Left,
                )
            }
            Some(JoinSide::Right) if maintains_input_order[1] => {
                let new_right_required = match smj.join_type {
                    JoinType::Inner | JoinType::Right => shift_right_required(
                        parent_required.ok_or_else(err)?,
                        left_columns_len,
                    )?,
                    JoinType::RightSemi | JoinType::RightAnti => {
                        parent_required.ok_or_else(err)?.to_vec()
                    }
                    _ => Err(DataFusionError::Plan(
                        "Unexpected SortMergeJoin type here".to_string(),
                    ))?,
                };
                try_pushdown_requirements_to_join(
                    plan,
                    Some(new_right_required.deref()),
                    parent_required_expr,
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
        || is_limit(plan)
    {
        // If the current plan is a leaf node or can not maintain any of the input ordering, can not pushed down requirements.
        // For RepartitionExec, we always choose to not push down the sort requirements even the RepartitionExec(input_partition=1) could maintain input ordering.
        // Pushing down is not beneficial
        Ok(None)
    } else {
        Ok(Some(vec![
            parent_required.map(|elem| elem.to_vec());
            plan.children().len()
        ]))
    }
    // TODO: Add support for Projection push down
}

/// Determine the children requirements
/// If the children requirements are more specific, do not push down the parent requirements
/// If the the parent requirements are more specific, push down the parent requirements
/// If they are not compatible, need to add Sort.
fn determine_children_requirement(
    parent_required: Option<&[PhysicalSortRequirement]>,
    request_child: Option<&[PhysicalSortRequirement]>,
    child_plan: Arc<dyn ExecutionPlan>,
) -> RequirementsCompatibility {
    if requirements_compatible(
        request_child,
        parent_required,
        || child_plan.ordering_equivalence_properties(),
        || child_plan.equivalence_properties(),
    ) {
        // request child requirements are more specific, no need to push down the parent requirements
        RequirementsCompatibility::Satisfy
    } else if requirements_compatible(
        parent_required,
        request_child,
        || child_plan.ordering_equivalence_properties(),
        || child_plan.equivalence_properties(),
    ) {
        // parent requirements are more specific, adjust the request child requirements and push down the new requirements
        let adjusted = parent_required.map(|r| r.to_vec());
        RequirementsCompatibility::Compatible(adjusted)
    } else {
        RequirementsCompatibility::NonCompatible
    }
}

fn try_pushdown_requirements_to_join(
    plan: &Arc<dyn ExecutionPlan>,
    parent_required: Option<&[PhysicalSortRequirement]>,
    sort_expr: Vec<PhysicalSortExpr>,
    push_side: JoinSide,
) -> Result<Option<Vec<Option<Vec<PhysicalSortRequirement>>>>> {
    let child_idx = match push_side {
        JoinSide::Left => 0,
        JoinSide::Right => 1,
    };
    let required_input_ordering = plan.required_input_ordering();
    let request_child = required_input_ordering[child_idx].as_deref();
    let child_plan = plan.children()[child_idx].clone();
    match determine_children_requirement(parent_required, request_child, child_plan) {
        RequirementsCompatibility::Satisfy => Ok(None),
        RequirementsCompatibility::Compatible(adjusted) => {
            let new_adjusted = match push_side {
                JoinSide::Left => {
                    vec![adjusted, required_input_ordering[1].clone()]
                }
                JoinSide::Right => {
                    vec![required_input_ordering[0].clone(), adjusted]
                }
            };
            Ok(Some(new_adjusted))
        }
        RequirementsCompatibility::NonCompatible => {
            // Can not push down, add new SortExec
            add_sort_above(&mut plan.clone(), sort_expr)?;
            Ok(None)
        }
    }
}

fn expr_source_sides(
    required_exprs: &[PhysicalSortExpr],
    join_type: JoinType,
    left_columns_len: usize,
) -> Option<JoinSide> {
    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
            let all_column_sides = required_exprs
                .iter()
                .filter_map(|r| {
                    r.expr.as_any().downcast_ref::<Column>().map(|col| {
                        if col.index() < left_columns_len {
                            JoinSide::Left
                        } else {
                            JoinSide::Right
                        }
                    })
                })
                .collect::<Vec<_>>();

            // If the exprs are all coming from one side, the requirements can be pushed down
            if all_column_sides.len() != required_exprs.len() {
                None
            } else if all_column_sides
                .iter()
                .all(|side| matches!(side, JoinSide::Left))
            {
                Some(JoinSide::Left)
            } else if all_column_sides
                .iter()
                .all(|side| matches!(side, JoinSide::Right))
            {
                Some(JoinSide::Right)
            } else {
                None
            }
        }
        JoinType::LeftSemi | JoinType::LeftAnti => required_exprs
            .iter()
            .all(|e| e.expr.as_any().downcast_ref::<Column>().is_some())
            .then_some(JoinSide::Left),
        JoinType::RightSemi | JoinType::RightAnti => required_exprs
            .iter()
            .all(|e| e.expr.as_any().downcast_ref::<Column>().is_some())
            .then_some(JoinSide::Right),
    }
}

fn shift_right_required(
    parent_required: &[PhysicalSortRequirement],
    left_columns_len: usize,
) -> Result<Vec<PhysicalSortRequirement>> {
    let new_right_required: Vec<PhysicalSortRequirement> = parent_required
        .iter()
        .filter_map(|r| {
            let Some(col) = r.expr.as_any().downcast_ref::<Column>() else {
                return None;
            };

            if col.index() < left_columns_len {
                return None;
            }

            let new_col =
                Arc::new(Column::new(col.name(), col.index() - left_columns_len));
            Some(r.clone().with_expr(new_col))
        })
        .collect::<Vec<_>>();
    if new_right_required.len() == parent_required.len() {
        Ok(new_right_required)
    } else {
        Err(DataFusionError::Plan(
            "Expect to shift all the parent required column indexes for SortMergeJoin"
                .to_string(),
        ))
    }
}

/// Define the Requirements Compatibility
#[derive(Debug)]
enum RequirementsCompatibility {
    /// Requirements satisfy
    Satisfy,
    /// Requirements compatible
    Compatible(Option<Vec<PhysicalSortRequirement>>),
    /// Requirements not compatible
    NonCompatible,
}
