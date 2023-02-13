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
//! SortExec: [nullable_co  l@0 ASC]
//!   SortExec: [non_nullable_col@1 ASC]
//! ```
//!
//! in the physical plan. The first sort is unnecessary since its result is overwritten
//! by another SortExec. Therefore, this rule removes it from the physical plan.
use crate::config::ConfigOptions;
use crate::error::Result;
use crate::execution::context::TaskContext;
use crate::physical_optimizer::utils::add_sort_above_child;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::joins::utils::JoinSide;
use crate::physical_plan::joins::SortMergeJoinExec;
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::projection::ProjectionExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::rewrite::TreeNodeRewritable;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::union::UnionExec;
use crate::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use crate::physical_plan::{
    displayable, with_new_children_if_necessary, DisplayFormatType, ExecutionPlan,
    Partitioning, SendableRecordBatchStream,
};
use arrow::datatypes::SchemaRef;
use datafusion_common::{reverse_sort_options, DataFusionError, Statistics};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::{
    create_sort_expr_from_requirement, map_requirement_before_projection,
    ordering_satisfy, ordering_satisfy_requirement, requirements_compatible,
};
use datafusion_physical_expr::{
    EquivalenceProperties, PhysicalExpr, PhysicalSortExpr, PhysicalSortRequirements,
};
use itertools::izip;
use std::any::Any;
use std::iter::zip;
use std::sync::Arc;

/// This rule inspects SortExec's in the given physical plan and removes the
/// ones it can prove unnecessary.
#[derive(Default)]
pub struct TopDownEnforceSorting {}

impl TopDownEnforceSorting {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// This is a "data class" we use within the [EnforceSorting] rule
#[derive(Debug, Clone)]
struct PlanWithSortRequirements {
    /// Current plan
    plan: Arc<dyn ExecutionPlan>,
    /// Whether the plan could impact the final result ordering
    impact_result_ordering: bool,
    /// Parent required sort ordering
    required_ordering: Option<Vec<PhysicalSortRequirements>>,
    /// The adjusted request sort ordering to children.
    /// By default they are the same as the plan's required input ordering, but can be adjusted based on parent required sort ordering properties.
    adjusted_request_ordering: Vec<Option<Vec<PhysicalSortRequirements>>>,
}

impl PlanWithSortRequirements {
    pub fn init(plan: Arc<dyn ExecutionPlan>) -> Self {
        let impact_result_ordering = plan.output_ordering().is_some()
            || plan.output_partitioning().partition_count() == 1
            || plan.as_any().downcast_ref::<GlobalLimitExec>().is_some()
            || plan.as_any().downcast_ref::<LocalLimitExec>().is_some();
        let request_ordering = plan.required_input_ordering();
        PlanWithSortRequirements {
            plan,
            impact_result_ordering,
            required_ordering: None,
            adjusted_request_ordering: request_ordering,
        }
    }

    pub fn new_without_impact_result_ordering(plan: Arc<dyn ExecutionPlan>) -> Self {
        let request_ordering = plan.required_input_ordering();
        PlanWithSortRequirements {
            plan,
            impact_result_ordering: false,
            required_ordering: None,
            adjusted_request_ordering: request_ordering,
        }
    }

    pub fn children(&self) -> Vec<PlanWithSortRequirements> {
        let plan_children = self.plan.children();
        assert_eq!(plan_children.len(), self.adjusted_request_ordering.len());
        let child_impact_result_ordering = if self
            .plan
            .as_any()
            .downcast_ref::<GlobalLimitExec>()
            .is_some()
            || self
                .plan
                .as_any()
                .downcast_ref::<LocalLimitExec>()
                .is_some()
        {
            true
        } else if self.plan.as_any().downcast_ref::<SortExec>().is_some() {
            false
        } else if self.plan.as_any().downcast_ref::<UnionExec>().is_some() {
            self.plan.output_ordering().is_some() && self.impact_result_ordering
        } else {
            self.plan.maintains_input_order().iter().all(|o| *o)
                && self.impact_result_ordering
        };
        println!(
            "child_impact_result_ordering {:?}",
            child_impact_result_ordering
        );
        plan_children
            .into_iter()
            .zip(self.adjusted_request_ordering.clone().into_iter())
            .map(|(child, required)| {
                let from_parent = required;
                let child_request_ordering = child.required_input_ordering();
                PlanWithSortRequirements {
                    plan: child,
                    impact_result_ordering: child_impact_result_ordering,
                    required_ordering: from_parent,
                    adjusted_request_ordering: child_request_ordering,
                }
            })
            .collect()
    }
}

impl TreeNodeRewritable for PlanWithSortRequirements {
    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if children.is_empty() {
            Ok(self)
        } else {
            let new_children = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;

            let children_plans = new_children
                .iter()
                .map(|elem| elem.plan.clone())
                .collect::<Vec<_>>();
            let plan = with_new_children_if_necessary(self.plan, children_plans)?;
            Ok(PlanWithSortRequirements {
                plan,
                impact_result_ordering: self.impact_result_ordering,
                required_ordering: self.required_ordering,
                adjusted_request_ordering: self.adjusted_request_ordering,
            })
        }
    }
}

impl PhysicalOptimizerRule for TopDownEnforceSorting {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Execute a Top-Down process(Preorder Traversal) to ensure the sort requirements:
        let plan_requirements = PlanWithSortRequirements::init(plan);
        let adjusted = plan_requirements.transform_down(&ensure_sorting)?;
        // Execute a Top-Down process(Preorder Traversal) to remove all the unnecessary Sort
        let adjusted_plan = adjusted.plan.transform_down(&|plan| {
            if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
                if ordering_satisfy(
                    sort_exec.input().output_ordering(),
                    sort_exec.output_ordering(),
                    || sort_exec.input().equivalence_properties(),
                ) {
                    Ok(Some(Arc::new(TombStoneExec::new(
                        sort_exec.input().clone(),
                    ))))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        })?;
        // Remove the TombStoneExec
        let final_plan = adjusted_plan.transform_up(&|plan| {
            if let Some(tombstone_exec) = plan.as_any().downcast_ref::<TombStoneExec>() {
                Ok(Some(tombstone_exec.input.clone()))
            } else {
                Ok(None)
            }
        })?;
        Ok(final_plan)
    }

    fn name(&self) -> &str {
        "EnforceSorting2"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn ensure_sorting(
    requirements: PlanWithSortRequirements,
) -> Result<Option<PlanWithSortRequirements>> {
    println!(
        "=== Current plan ===\n{}\n",
        displayable(requirements.plan.as_ref()).indent()
    );
    println!(
        "impact_result_ordering: {:?}, parent required_ordering {:?}, adjusted request ordering {:?}",
        requirements.impact_result_ordering, requirements.required_ordering, requirements.adjusted_request_ordering,
    );
    if let Some(sort_exec) = requirements.plan.as_any().downcast_ref::<SortExec>() {
        // Remove unnecessary global SortExec
        if !sort_exec.preserve_partitioning() {
            if !requirements.impact_result_ordering
                && requirements.required_ordering.is_none()
            {
                println!("remove sort_exec due to no need to keep ordering");
                return Ok(Some(PlanWithSortRequirements {
                    plan: Arc::new(TombStoneExec::new(sort_exec.input().clone())),
                    impact_result_ordering: false,
                    required_ordering: None,
                    adjusted_request_ordering: vec![None],
                }));
            } else if ordering_satisfy(
                sort_exec.input().output_ordering(),
                sort_exec.output_ordering(),
                || sort_exec.input().equivalence_properties(),
            ) {
                println!("remove sort_exec due to child already satisfy");
                return Ok(Some(PlanWithSortRequirements {
                    plan: Arc::new(TombStoneExec::new(sort_exec.input().clone())),
                    impact_result_ordering: true,
                    required_ordering: None,
                    adjusted_request_ordering: vec![requirements.required_ordering],
                }));
            }
        }
    } else if let Some(sort_pres_exec) = requirements
        .plan
        .as_any()
        .downcast_ref::<SortPreservingMergeExec>()
    {
        // SortPreservingMergeExec + SortExec(local/global) is the same as the global SortExec
        // Remove unnecessary SortPreservingMergeExec + SortExec(local/global)
        if let Some(child_sort_exec) =
            sort_pres_exec.input().as_any().downcast_ref::<SortExec>()
        {
            if sort_pres_exec.expr() == child_sort_exec.expr() {
                if !requirements.impact_result_ordering
                    && requirements.required_ordering.is_none()
                {
                    println!("remove SortPreservingMergeExec + SortExec due to no need to keep ordering");
                    return Ok(Some(PlanWithSortRequirements {
                        plan: Arc::new(TombStoneExec::new(
                            child_sort_exec.input().clone(),
                        )),
                        impact_result_ordering: false,
                        required_ordering: None,
                        adjusted_request_ordering: vec![None],
                    }));
                } else if ordering_satisfy(
                    child_sort_exec.input().output_ordering(),
                    child_sort_exec.output_ordering(),
                    || child_sort_exec.input().equivalence_properties(),
                ) && child_sort_exec
                    .input()
                    .output_partitioning()
                    .partition_count()
                    == 1
                {
                    println!("remove SortPreservingMergeExec + SortExec due to child already satisfy");
                    return Ok(Some(PlanWithSortRequirements {
                        plan: Arc::new(TombStoneExec::new(
                            child_sort_exec.input().clone(),
                        )),
                        impact_result_ordering: true,
                        required_ordering: None,
                        adjusted_request_ordering: vec![requirements.required_ordering],
                    }));
                }
            }
        } else {
            if !sort_pres_exec.satisfy_distribution() {
                // Remove unnecessary SortPreservingMergeExec only
                if !requirements.impact_result_ordering {
                    println!(
                        "remove SortPreservingMergeExec due to no need to keep ordering"
                    );
                    return Ok(Some(PlanWithSortRequirements {
                        plan: Arc::new(TombStoneExec::new(
                            sort_pres_exec.input().clone(),
                        )),
                        impact_result_ordering: false,
                        required_ordering: None,
                        adjusted_request_ordering: vec![requirements.required_ordering],
                    }));
                } else if ordering_satisfy(
                    sort_pres_exec.input().output_ordering(),
                    Some(sort_pres_exec.expr()),
                    || sort_pres_exec.input().equivalence_properties(),
                ) && sort_pres_exec
                    .input()
                    .output_partitioning()
                    .partition_count()
                    == 1
                {
                    println!(
                        "remove SortPreservingMergeExec due to child already satisfy"
                    );
                    return Ok(Some(PlanWithSortRequirements {
                        plan: Arc::new(TombStoneExec::new(
                            sort_pres_exec.input().clone(),
                        )),
                        impact_result_ordering: true,
                        required_ordering: None,
                        adjusted_request_ordering: vec![requirements.required_ordering],
                    }));
                }
            }
        }
    }
    println!("no removing");
    let plan = &requirements.plan;
    let parent_required = requirements.required_ordering.as_deref();
    if ordering_satisfy_requirement(plan.output_ordering(), parent_required, || {
        plan.equivalence_properties()
    }) {
        // Can satisfy the parent requirements, clear the requirements
        println!(
            "Can satisfy the parent requirements, impact_result_ordering {:?}",
            requirements.impact_result_ordering
        );
        if plan.as_any().downcast_ref::<WindowAggExec>().is_some()
            || plan
                .as_any()
                .downcast_ref::<BoundedWindowAggExec>()
                .is_some()
        {
            let request_child = requirements.adjusted_request_ordering[0].as_deref();
            let reversed_request_child = reverse_window_sort_requirements(request_child);

            if should_reverse_window_sort_requirements(
                plan.clone(),
                request_child,
                reversed_request_child.as_deref(),
            ) {
                println!("Should reverse top window sort_requirements");
                let (window_expr, input_schema, partition_keys) = if let Some(exec) =
                    plan.as_any().downcast_ref::<BoundedWindowAggExec>()
                {
                    (
                        exec.window_expr(),
                        exec.input_schema(),
                        exec.partition_keys.clone(),
                    )
                } else if let Some(exec) = plan.as_any().downcast_ref::<WindowAggExec>() {
                    (
                        exec.window_expr(),
                        exec.input_schema(),
                        exec.partition_keys.clone(),
                    )
                } else {
                    return Err(DataFusionError::Plan(
                        "Expects to receive either WindowAggExec of BoundedWindowAggExec"
                            .to_string(),
                    ));
                };
                let new_window_expr = window_expr
                    .iter()
                    .map(|e| e.get_reverse_expr())
                    .collect::<Option<Vec<_>>>();
                let new_physical_ordering = create_sort_expr_from_requirement(
                    reversed_request_child.clone().unwrap().as_ref(),
                );
                if let Some(window_expr) = new_window_expr {
                    let uses_bounded_memory =
                        window_expr.iter().all(|e| e.uses_bounded_memory());
                    // If all window expressions can run with bounded memory, choose the
                    // bounded window variant:
                    let new_plan = if uses_bounded_memory {
                        Arc::new(BoundedWindowAggExec::try_new(
                            window_expr,
                            plan.children()[0].clone(),
                            input_schema,
                            partition_keys,
                            Some(new_physical_ordering),
                        )?) as _
                    } else {
                        Arc::new(WindowAggExec::try_new(
                            window_expr,
                            plan.children()[0].clone(),
                            input_schema,
                            partition_keys,
                            Some(new_physical_ordering),
                        )?) as _
                    };
                    println!("Reverse WindowAggExec expressions and push down the reversed requirements");

                    return Ok(Some(PlanWithSortRequirements {
                        plan: new_plan,
                        impact_result_ordering: false,
                        required_ordering: None,
                        adjusted_request_ordering: vec![reversed_request_child],
                    }));
                }
            } else {
                println!("Should not reverse top window sort_requirements");
            }
        } else if let Some(_) = plan.as_any().downcast_ref::<SortExec>() {
            return Ok(Some(PlanWithSortRequirements {
                plan: plan.clone(),
                impact_result_ordering: false,
                required_ordering: None,
                adjusted_request_ordering: requirements.adjusted_request_ordering.clone(),
            }));
        }
        return Ok(Some(PlanWithSortRequirements {
            plan: plan.clone(),
            impact_result_ordering: requirements.impact_result_ordering,
            required_ordering: None,
            adjusted_request_ordering: requirements.adjusted_request_ordering,
        }));
    } else if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        println!("Modify current SortExec to satisfy the parent requirements");
        // If the current plan is a SortExec, update the SortExec to satisfy the parent requirements
        let parent_required_expr =
            create_sort_expr_from_requirement(parent_required.unwrap());
        let new_plan = add_sort_above_child(
            &sort_exec.input,
            parent_required_expr,
            sort_exec.fetch(),
        )?;
        return Ok(Some(
            PlanWithSortRequirements::new_without_impact_result_ordering(new_plan),
        ));
    } else {
        println!("Can not satisfy the parent requirements, try to push down");
        // Can not satisfy the parent requirements, check whether should push down the requirements. Add new SortExec when the parent requirements can not be pushed down
        let parent_required_expr =
            create_sort_expr_from_requirement(parent_required.unwrap());
        let maintains_input_order = plan.maintains_input_order();
        // If the current plan is a leaf node or can not maintain any of the input ordering, can not pushed down requirements.
        // For RepartitionExec, we always choose to not push down the sort requirements even the RepartitionExec(input_partition=1) could maintain input ordering.
        // For UnionExec, we can always push down
        if (maintains_input_order.is_empty()
            || !maintains_input_order.iter().any(|o| *o)
            || plan.as_any().downcast_ref::<RepartitionExec>().is_some()
            || plan.as_any().downcast_ref::<FilterExec>().is_some()
            || plan.as_any().downcast_ref::<GlobalLimitExec>().is_some()
            || plan.as_any().downcast_ref::<LocalLimitExec>().is_some())
            && plan.as_any().downcast_ref::<UnionExec>().is_none()
        {
            let new_plan = add_sort_above_child(plan, parent_required_expr, None)?;
            return Ok(Some(
                PlanWithSortRequirements::new_without_impact_result_ordering(new_plan),
            ));
        } else if let Some(window_agg_exec) =
            plan.as_any().downcast_ref::<WindowAggExec>()
        {
            let window_expr = window_agg_exec.window_expr();
            let request_child = requirements.adjusted_request_ordering[0].as_deref();
            if requirements_compatible(request_child, parent_required, || {
                plan.children()[0].equivalence_properties()
            }) {
                println!("WindowAggExec child requirements are more specific, no need to add SortExec");
                return Ok(Some(PlanWithSortRequirements {
                    plan: plan.clone(),
                    impact_result_ordering: true,
                    required_ordering: None,
                    adjusted_request_ordering: requirements.adjusted_request_ordering,
                }));
            } else if requirements_compatible(parent_required, request_child, || {
                plan.children()[0].equivalence_properties()
            }) {
                println!("Parent requirements are more specific, adjust WindowAggExec child requirements and push down the requirements");
                let adjusted = parent_required.map(|r| r.to_vec());
                return Ok(Some(PlanWithSortRequirements {
                    plan: plan.clone(),
                    impact_result_ordering: true,
                    required_ordering: None,
                    adjusted_request_ordering: vec![adjusted],
                }));
            } else {
                let should_reverse = can_reverse_window_request(
                    window_expr[0].partition_by(),
                    parent_required,
                    request_child,
                    &window_agg_exec.input().schema(),
                );
                if should_reverse {
                    let new_window_expr = window_expr
                        .iter()
                        .map(|e| e.get_reverse_expr())
                        .collect::<Option<Vec<_>>>();
                    if let Some(window_expr) = new_window_expr {
                        let new_plan = Arc::new(WindowAggExec::try_new(
                            window_expr,
                            window_agg_exec.children()[0].clone(),
                            window_agg_exec.input_schema(),
                            window_agg_exec.partition_keys.clone(),
                            Some(parent_required_expr.to_vec()),
                        )?) as _;
                        println!("Reverse WindowAggExec expressions and push down the requirements");
                        return Ok(Some(
                            PlanWithSortRequirements::new_without_impact_result_ordering(
                                new_plan,
                            ),
                        ));
                    } else {
                        println!("Can not push down, add new SortExec");
                        let new_plan =
                            add_sort_above_child(plan, parent_required_expr, None)?;
                        return Ok(Some(
                            PlanWithSortRequirements::new_without_impact_result_ordering(
                                new_plan,
                            ),
                        ));
                    }
                } else {
                    // Can not push down, add new SortExec
                    println!("Can not push down, add new SortExec");
                    let new_plan =
                        add_sort_above_child(plan, parent_required_expr, None)?;
                    return Ok(Some(
                        PlanWithSortRequirements::new_without_impact_result_ordering(
                            new_plan,
                        ),
                    ));
                }
            }
        } else if let Some(window_agg_exec) =
            plan.as_any().downcast_ref::<BoundedWindowAggExec>()
        {
            let window_expr = window_agg_exec.window_expr();
            let request_child = &plan.required_input_ordering()[0];
            if requirements_compatible(request_child.as_deref(), parent_required, || {
                plan.children()[0].equivalence_properties()
            }) {
                println!("BoundedWindowAggExec child requirements are more specific, no need to add SortExec");
                return Ok(Some(PlanWithSortRequirements {
                    plan: plan.clone(),
                    impact_result_ordering: true,
                    required_ordering: None,
                    adjusted_request_ordering: requirements.adjusted_request_ordering,
                }));
            } else if requirements_compatible(
                parent_required,
                request_child.as_deref(),
                || plan.children()[0].equivalence_properties(),
            ) {
                println!("Parent requirements are more specific, adjust BoundedWindowAggExec child requirements and push down the requirements");
                let adjusted = parent_required.map(|r| r.to_vec());
                return Ok(Some(PlanWithSortRequirements {
                    plan: plan.clone(),
                    impact_result_ordering: true,
                    required_ordering: None,
                    adjusted_request_ordering: vec![adjusted],
                }));
            } else {
                let should_reverse = can_reverse_window_request(
                    window_expr[0].partition_by(),
                    parent_required,
                    request_child.as_deref(),
                    &window_agg_exec.input().schema(),
                );
                if should_reverse {
                    let new_window_expr = window_expr
                        .iter()
                        .map(|e| e.get_reverse_expr())
                        .collect::<Option<Vec<_>>>();
                    if let Some(window_expr) = new_window_expr {
                        let new_plan = Arc::new(BoundedWindowAggExec::try_new(
                            window_expr,
                            window_agg_exec.children()[0].clone(),
                            window_agg_exec.input_schema(),
                            window_agg_exec.partition_keys.clone(),
                            Some(parent_required_expr.to_vec()),
                        )?) as _;
                        println!("Reverse BoundedWindowAggExec expressions and push down the requirements");
                        return Ok(Some(
                            PlanWithSortRequirements::new_without_impact_result_ordering(
                                new_plan,
                            ),
                        ));
                    } else {
                        println!("Can not push down, add new SortExec");
                        let new_plan =
                            add_sort_above_child(plan, parent_required_expr, None)?;
                        return Ok(Some(
                            PlanWithSortRequirements::new_without_impact_result_ordering(
                                new_plan,
                            ),
                        ));
                    }
                } else {
                    // Can not push down, add new SortExec
                    println!("Can not push down, add new SortExec");
                    let new_plan =
                        add_sort_above_child(plan, parent_required_expr, None)?;
                    return Ok(Some(
                        PlanWithSortRequirements::new_without_impact_result_ordering(
                            new_plan,
                        ),
                    ));
                }
            }
        } else if let Some(smj) = plan.as_any().downcast_ref::<SortMergeJoinExec>() {
            // If the current plan is SortMergeJoinExec
            let left_columns_len = smj.left.schema().fields().len();
            let expr_source_side =
                expr_source_sides(&parent_required_expr, left_columns_len);
            match expr_source_side {
                Some(JoinSide::Left) if maintains_input_order[0] => {
                    if requirements_compatible(
                        plan.required_input_ordering()[0].as_deref(),
                        parent_required,
                        || plan.children()[0].equivalence_properties(),
                    ) {
                        println!("Requirements are compatible with SMJ");
                        return Ok(Some(PlanWithSortRequirements {
                            plan: plan.clone(),
                            impact_result_ordering: true,
                            required_ordering: None,
                            adjusted_request_ordering: requirements
                                .adjusted_request_ordering,
                        }));
                    } else {
                        // Can not push down, add new SortExec
                        println!("Can not push down, add new SortExec");
                        let new_plan =
                            add_sort_above_child(plan, parent_required_expr, None)?;
                        return Ok(Some(
                            PlanWithSortRequirements::new_without_impact_result_ordering(
                                new_plan,
                            ),
                        ));
                    }
                }
                Some(JoinSide::Right) if maintains_input_order[1] => {
                    let shift_right_required =
                        shift_right_required(parent_required.unwrap(), left_columns_len);
                    if requirements_compatible(
                        plan.required_input_ordering()[1].as_deref(),
                        shift_right_required.as_deref(),
                        || plan.children()[1].equivalence_properties(),
                    ) {
                        println!("Requirements are compatible with SMJ");
                        return Ok(Some(PlanWithSortRequirements {
                            plan: plan.clone(),
                            impact_result_ordering: true,
                            required_ordering: None,
                            adjusted_request_ordering: requirements
                                .adjusted_request_ordering,
                        }));
                    } else {
                        // Can not push down, add new SortExec
                        println!("Can not push down, add new SortExec");
                        let new_plan =
                            add_sort_above_child(plan, parent_required_expr, None)?;
                        return Ok(Some(
                            PlanWithSortRequirements::new_without_impact_result_ordering(
                                new_plan,
                            ),
                        ));
                    }
                }
                _ => {
                    println!("Can not decide the expr side for SortMergeJoinExec, can not push down, add SortExec");
                    let new_plan =
                        add_sort_above_child(plan, parent_required_expr, None)?;
                    return Ok(Some(
                        PlanWithSortRequirements::new_without_impact_result_ordering(
                            new_plan,
                        ),
                    ));
                }
            }
        } else if plan.required_input_ordering().iter().any(Option::is_some) {
            // If the current plan has its own ordering requirements to its children, check whether the requirements
            // are compatible with the parent requirements.
            println!(
                "the current plan has its own ordering requirements, {:?}",
                plan.required_input_ordering()
            );

            let plan_children = plan.children();
            let compatible_with_children = izip!(
                maintains_input_order.iter(),
                plan.required_input_ordering().into_iter(),
                plan_children.iter()
            )
            .map(|(can_push_down, request_child, child)| {
                *can_push_down
                    && requirements_compatible(
                        request_child.as_deref(),
                        parent_required,
                        || child.equivalence_properties(),
                    )
            })
            .collect::<Vec<_>>();
            println!(
                "plan.equivalence_properties() {:?}",
                plan.equivalence_properties()
            );
            println!("compatible_with_children {:?}", compatible_with_children);
            if compatible_with_children.iter().all(|a| *a) {
                // Requirements are compatible, not need to push down.
                println!("Requirements are compatible, no need to push down");
                return Ok(Some(PlanWithSortRequirements {
                    plan: plan.clone(),
                    impact_result_ordering: true,
                    required_ordering: None,
                    adjusted_request_ordering: requirements.adjusted_request_ordering,
                }));
            } else {
                let can_adjust_child_requirements = plan
                    .required_input_ordering()
                    .into_iter()
                    .zip(plan_children.iter())
                    .map(|(request_child, child)| {
                        requirements_compatible(
                            parent_required,
                            request_child.as_deref(),
                            || child.equivalence_properties(),
                        )
                    })
                    .collect::<Vec<_>>();
                if can_adjust_child_requirements.iter().all(|a| *a) {
                    // Adjust child requirements and push down the requirements
                    println!("Adjust child requirements and push down the requirements");
                    let adjusted = parent_required.map(|r| r.to_vec());
                    return Ok(Some(PlanWithSortRequirements {
                        plan: plan.clone(),
                        impact_result_ordering: true,
                        required_ordering: None,
                        adjusted_request_ordering: vec![
                            adjusted;
                            can_adjust_child_requirements
                                .len()
                        ],
                    }));
                } else {
                    // Can not push down, add new SortExec
                    println!("Can not push down, add new SortExec");
                    let new_plan =
                        add_sort_above_child(plan, parent_required_expr, None)?;
                    return Ok(Some(
                        PlanWithSortRequirements::new_without_impact_result_ordering(
                            new_plan,
                        ),
                    ));
                }
            }
        } else {
            // The current plan does not have its own ordering requirements to its children, consider push down the requirements
            if let Some(ProjectionExec { expr, .. }) =
                plan.as_any().downcast_ref::<ProjectionExec>()
            {
                // For Projection, we need to transform the requirements to the columns before the Projection
                // And then to push down the requirements
                let new_requirement =
                    map_requirement_before_projection(parent_required, expr);
                if new_requirement.is_some() {
                    println!("Push requirements down to Projection");
                    Ok(Some(PlanWithSortRequirements {
                        plan: plan.clone(),
                        impact_result_ordering: true,
                        required_ordering: None,
                        adjusted_request_ordering: vec![new_requirement],
                    }))
                } else {
                    // Can not push down, add new SortExec
                    println!(
                        "Can not push requirements down to Projection, add SortExec"
                    );
                    let new_plan =
                        add_sort_above_child(plan, parent_required_expr, None)?;
                    return Ok(Some(
                        PlanWithSortRequirements::new_without_impact_result_ordering(
                            new_plan,
                        ),
                    ));
                }
            } else {
                println!("Push down requirements.");
                return Ok(Some(PlanWithSortRequirements {
                    plan: plan.clone(),
                    impact_result_ordering: requirements.impact_result_ordering,
                    required_ordering: None,
                    adjusted_request_ordering: vec![
                        requirements.required_ordering;
                        requirements
                            .adjusted_request_ordering
                            .len()
                    ],
                }));
            }
        }
    }
}

fn expr_source_sides(
    required_exprs: &[PhysicalSortExpr],
    left_columns_len: usize,
) -> Option<JoinSide> {
    let all_column_sides = required_exprs
        .iter()
        .filter_map(|r| {
            if let Some(col) = r.expr.as_any().downcast_ref::<Column>() {
                if col.index() < left_columns_len {
                    Some(JoinSide::Left)
                } else {
                    Some(JoinSide::Right)
                }
            } else {
                None
            }
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

fn shift_right_required(
    parent_required: &[PhysicalSortRequirements],
    left_columns_len: usize,
) -> Option<Vec<PhysicalSortRequirements>> {
    let new_right_required: Vec<PhysicalSortRequirements> = parent_required
        .iter()
        .filter_map(|r| {
            if let Some(col) = r.expr.as_any().downcast_ref::<Column>() {
                if col.index() >= left_columns_len {
                    Some(PhysicalSortRequirements {
                        expr: Arc::new(Column::new(
                            col.name(),
                            col.index() - left_columns_len,
                        )) as Arc<dyn PhysicalExpr>,
                        sort_options: r.sort_options.clone(),
                    })
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // if the parent required are all comming from the right side, the requirements can be pushdown
    if new_right_required.len() != parent_required.len() {
        None
    } else {
        Some(new_right_required)
    }
}

#[derive(Debug)]
/// This structure stores extra column information required to remove unnecessary sorts.
pub struct ColumnInfo {
    reverse: bool,
    is_partition: bool,
}

fn can_reverse_window_request(
    partition_keys: &[Arc<dyn PhysicalExpr>],
    required: Option<&[PhysicalSortRequirements]>,
    request_ordering: Option<&[PhysicalSortRequirements]>,
    input_schema: &SchemaRef,
) -> bool {
    match (required, request_ordering) {
        (_, None) => false,
        (None, Some(_)) => false,
        (Some(required), Some(request_ordering)) => {
            if required.len() > request_ordering.len() {
                return false;
            }
            let mut col_infos = vec![];
            for (required_expr, request_expr) in zip(required, request_ordering) {
                let column = required_expr.expr.clone();
                let is_partition = partition_keys.iter().any(|e| e.eq(&column));
                let reverse = check_alignment(input_schema, request_expr, required_expr);
                col_infos.push(ColumnInfo {
                    reverse,
                    is_partition,
                });
            }
            let order_by_sections = col_infos
                .iter()
                .filter(|elem| !elem.is_partition)
                .collect::<Vec<_>>();
            let should_reverse_order_bys = if order_by_sections.is_empty() {
                false
            } else {
                let first_reverse = order_by_sections[0].reverse;
                first_reverse
            };
            should_reverse_order_bys
        }
    }
}

/// Compares window expression's `window_request` and `parent_required_expr` ordering, returns
/// whether we should reverse the window expression's ordering in order to meet parent's requirements.
fn check_alignment(
    input_schema: &SchemaRef,
    window_request: &PhysicalSortRequirements,
    parent_required_expr: &PhysicalSortRequirements,
) -> bool {
    if parent_required_expr.expr.eq(&window_request.expr)
        && window_request.sort_options.is_some()
        && parent_required_expr.sort_options.is_some()
    {
        let nullable = parent_required_expr.expr.nullable(input_schema).unwrap();
        let window_request_opts = window_request.sort_options.unwrap();
        let parent_required_opts = parent_required_expr.sort_options.unwrap();
        let is_reversed = if nullable {
            window_request_opts == reverse_sort_options(parent_required_opts)
        } else {
            // If the column is not nullable, NULLS FIRST/LAST is not important.
            window_request_opts.descending != parent_required_opts.descending
        };
        is_reversed
    } else {
        false
    }
}

fn reverse_window_sort_requirements(
    request_child: Option<&[PhysicalSortRequirements]>,
) -> Option<Vec<PhysicalSortRequirements>> {
    let reversed_request = request_child.map(|request| {
        request
            .iter()
            .map(|req| match req.sort_options {
                None => req.clone(),
                Some(ops) => PhysicalSortRequirements {
                    expr: req.expr.clone(),
                    sort_options: Some(reverse_sort_options(ops)),
                },
            })
            .collect::<Vec<_>>()
    });
    reversed_request
}

fn should_reverse_window_sort_requirements(
    window_plan: Arc<dyn ExecutionPlan>,
    top_requirement: Option<&[PhysicalSortRequirements]>,
    top_reversed_requirement: Option<&[PhysicalSortRequirements]>,
) -> bool {
    if top_requirement.is_none() {
        return false;
    }
    let flags = window_plan
        .children()
        .into_iter()
        .map(|child| {
            // If the child is leaf node, check the output ordering
            if child.children().is_empty()
                && ordering_satisfy_requirement(
                    child.output_ordering(),
                    top_requirement,
                    || child.equivalence_properties(),
                )
            {
                false
            } else if child.children().is_empty()
                && ordering_satisfy_requirement(
                    child.output_ordering(),
                    top_reversed_requirement,
                    || child.equivalence_properties(),
                )
            {
                true
            } else if child.as_any().downcast_ref::<WindowAggExec>().is_some()
                || child
                    .as_any()
                    .downcast_ref::<BoundedWindowAggExec>()
                    .is_some()
            {
                // If the child is WindowExec, check the child requirements
                if requirements_compatible(
                    top_requirement,
                    child.required_input_ordering()[0].as_deref(),
                    || child.equivalence_properties(),
                ) || requirements_compatible(
                    child.required_input_ordering()[0].as_deref(),
                    top_requirement,
                    || child.equivalence_properties(),
                ) || requirements_compatible(
                    top_reversed_requirement,
                    child.required_input_ordering()[0].as_deref(),
                    || child.equivalence_properties(),
                ) || requirements_compatible(
                    child.required_input_ordering()[0].as_deref(),
                    top_reversed_requirement,
                    || child.equivalence_properties(),
                ) {
                    should_reverse_window_sort_requirements(
                        child,
                        top_requirement,
                        top_reversed_requirement,
                    )
                } else {
                    if requirements_compatible(
                        top_reversed_requirement,
                        window_plan.required_input_ordering()[0].as_deref(),
                        || window_plan.equivalence_properties(),
                    ) || requirements_compatible(
                        window_plan.required_input_ordering()[0].as_deref(),
                        top_reversed_requirement,
                        || window_plan.equivalence_properties(),
                    ) {
                        true
                    } else {
                        false
                    }
                }
            } else if requirements_compatible(
                top_reversed_requirement,
                window_plan.required_input_ordering()[0].as_deref(),
                || window_plan.equivalence_properties(),
            ) || requirements_compatible(
                window_plan.required_input_ordering()[0].as_deref(),
                top_reversed_requirement,
                || window_plan.equivalence_properties(),
            ) {
                true
            } else {
                false
            }
        })
        .collect::<Vec<_>>();

    flags.iter().all(|o| *o)
}

/// A TombStoneExec execution plan generated during optimization process, should be removed finally
#[derive(Debug)]
struct TombStoneExec {
    /// The input plan
    pub input: Arc<dyn ExecutionPlan>,
}

impl TombStoneExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }
}

impl ExecutionPlan for TombStoneExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        self.input.equivalence_properties()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TombStoneExec::new(children[0].clone())))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::Internal(format!(
            "TombStoneExec, invalid plan"
        )))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "TombStoneExec")
            }
        }
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::physical_plan::aggregates::PhysicalGroupBy;
    use crate::physical_plan::aggregates::{AggregateExec, AggregateMode};
    use crate::physical_plan::displayable;
    use crate::physical_plan::file_format::{FileScanConfig, ParquetExec};
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::memory::MemoryExec;
    use crate::physical_plan::repartition::RepartitionExec;
    use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use crate::physical_plan::union::UnionExec;
    use crate::physical_plan::windows::create_window_expr;
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
            ((true, true), (false, false), true),
            ((true, true), (false, true), false),
            ((true, true), (true, false), false),
            ((true, false), (false, true), true),
            ((true, false), (false, false), false),
            ((true, false), (true, true), false),
        ];
        for (
            (physical_desc, physical_nulls_first),
            (req_desc, req_nulls_first),
            reverse_expected,
        ) in params
        {
            let physical_ordering = PhysicalSortRequirements {
                expr: col("nullable_col", &schema)?,
                sort_options: Some(SortOptions {
                    descending: physical_desc,
                    nulls_first: physical_nulls_first,
                }),
            };
            let required_ordering = PhysicalSortRequirements {
                expr: col("nullable_col", &schema)?,
                sort_options: Some(SortOptions {
                    descending: req_desc,
                    nulls_first: req_nulls_first,
                }),
            };
            let reverse =
                check_alignment(&schema, &physical_ordering, &required_ordering);
            assert_eq!(reverse, reverse_expected);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_is_column_aligned_non_nullable() -> Result<()> {
        let schema = create_test_schema()?;

        let params = vec![
            ((true, true), (false, false), true),
            ((true, true), (false, true), true),
            ((true, true), (true, false), false),
            ((true, false), (false, true), true),
            ((true, false), (false, false), true),
            ((true, false), (true, true), false),
        ];
        for (
            (physical_desc, physical_nulls_first),
            (req_desc, req_nulls_first),
            reverse_expected,
        ) in params
        {
            let physical_ordering = PhysicalSortRequirements {
                expr: col("non_nullable_col", &schema)?,
                sort_options: Some(SortOptions {
                    descending: physical_desc,
                    nulls_first: physical_nulls_first,
                }),
            };
            let required_ordering = PhysicalSortRequirements {
                expr: col("non_nullable_col", &schema)?,
                sort_options: Some(SortOptions {
                    descending: req_desc,
                    nulls_first: req_nulls_first,
                }),
            };
            let reverse =
                check_alignment(&schema, &physical_ordering, &required_ordering);
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
                TopDownEnforceSorting::new().optimize(physical_plan, state.config_options())?;
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
            "SortExec: [nullable_col@0 ASC], global=true",
            "  SortExec: [non_nullable_col@1 ASC], global=true",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortExec: [nullable_col@0 ASC], global=true",
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
            "    SortExec: [non_nullable_col@1 ASC NULLS LAST], global=true",
            "      WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "        SortExec: [non_nullable_col@1 DESC], global=true",
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
    async fn test_multiple_sort_window_exec() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);

        let sort_exprs1 = vec![sort_expr("nullable_col", &schema)];
        let sort_exprs2 = vec![
            sort_expr("nullable_col", &schema),
            sort_expr("non_nullable_col", &schema),
        ];

        let sort1 = sort_exec(sort_exprs1.clone(), source);
        let window_agg1 = window_exec("non_nullable_col", sort_exprs1.clone(), sort1);
        let window_agg2 = window_exec("non_nullable_col", sort_exprs2, window_agg1);
        // let filter_exec = sort_exec;
        let physical_plan = window_exec("non_nullable_col", sort_exprs1, window_agg2);

        let expected_input = vec![
            "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "    WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "      SortExec: [nullable_col@0 ASC], global=true",
            "        MemoryExec: partitions=0, partition_sizes=[]",
        ];

        let expected_optimized = vec![
            "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "    WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "      SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
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
            "  SortExec: [nullable_col@0 ASC], global=true",
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
            "  SortExec: [nullable_col@0 ASC], global=true",
            "    SortPreservingMergeExec: [nullable_col@0 ASC]",
            "      SortExec: [nullable_col@0 ASC], global=true",
            "        MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: [nullable_col@0 ASC], global=true",
            "    MemoryExec: partitions=0, partition_sizes=[]",
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
            "    SortExec: [nullable_col@0 ASC], global=true",
            "      SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "        SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "          SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "            SortExec: [non_nullable_col@1 ASC], global=true",
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
            "    SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "          SortExec: [non_nullable_col@1 ASC], global=true",
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
    async fn test_remove_unnecessary_sort4() -> Result<()> {
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
            "  SortExec: [nullable_col@0 ASC], global=true",
            "    SortPreservingMergeExec: [nullable_col@0 ASC]",
            "      SortExec: [nullable_col@0 ASC], global=true",
            "        MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: [nullable_col@0 ASC], global=true",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_sort5() -> Result<()> {
        let schema = create_test_schema()?;
        let source = memory_exec(&schema);

        let input = sort_exec(vec![sort_expr("non_nullable_col", &schema)], source);
        let input2 = sort_exec(
            vec![
                sort_expr("nullable_col", &schema),
                sort_expr("non_nullable_col", &schema),
            ],
            input,
        );
        let physical_plan = sort_exec(vec![sort_expr("nullable_col", &schema)], input2);

        let expected_input = vec![
            "SortExec: [nullable_col@0 ASC], global=true",
            "  SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "    SortExec: [non_nullable_col@1 ASC], global=true",
            "      MemoryExec: partitions=0, partition_sizes=[]",
        ];
        // Keep the middle SortExec
        let expected_optimized = [
            "SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
        let physical_plan = sort_exec(vec![sort_expr("nullable_col", &schema)], input);

        let expected_input = vec![
            "SortExec: [nullable_col@0 ASC], global=true",
            "  SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortExec: [nullable_col@0 ASC], global=true",
            "  MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_remove_unnecessary_spm2() -> Result<()> {
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
            "      MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: [nullable_col@0 ASC], global=true",
            "    MemoryExec: partitions=0, partition_sizes=[]",
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
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "      GlobalLimitExec: skip=0, fetch=100",
            "        LocalLimitExec: fetch=100",
            "          SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "            ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];

        // We should keep the bottom `SortExec`.
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=false",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
            "      UnionExec",
            "        ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "        GlobalLimitExec: skip=0, fetch=100",
            "          LocalLimitExec: fetch=100",
            "            SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
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
            "  SortExec: [nullable_col@0 ASC], global=true",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "    MemoryExec: partitions=0, partition_sizes=[]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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
            "  SortExec: [nullable_col@0 ASC], global=true",
            "    SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "      MemoryExec: partitions=0, partition_sizes=[]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "  SortExec: [non_nullable_col@1 ASC], global=true",
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
            "    SortExec: [nullable_col@0 ASC], global=true",
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
            "    SortExec: [nullable_col@0 ASC], global=true",
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
            "    SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];

        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
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
            "    SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: [nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        // should adjust sorting in the first input of the union such that it is not unnecessarily fine
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: [nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: [nullable_col@0 ASC], global=true",
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
            "    SortExec: [nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: [nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC,non_nullable_col@1 ASC]",
            "  UnionExec",
            "    SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
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
            "    SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: [nullable_col@0 ASC,non_nullable_col@1 DESC NULLS LAST], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: [nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: [nullable_col@0 ASC], global=true",
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
            "    SortExec: [nullable_col@0 ASC], global=true",
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
            "    SortExec: [nullable_col@0 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: [nullable_col@0 ASC], global=false",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
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

        // Union doesn't preserve any of the inputs ordering. However, we should be able to change unnecessarily fine
        // SortExecs under UnionExec with required SortExecs that are absolutely necessary.
        let expected_input = vec![
            "SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  UnionExec",
            "    SortExec: [nullable_col@0 ASC,non_nullable_col@1 ASC], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    SortExec: [nullable_col@0 ASC,non_nullable_col@1 DESC NULLS LAST], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        ];
        assert_optimized!(expected_input, expected_input, physical_plan);
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
        let reversed_sort_exprs2 = vec![sort_expr_options(
            "nullable_col",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        )];
        let source1 = parquet_exec_sorted(&schema, sort_exprs1);
        let source2 = parquet_exec_sorted(&schema, sort_exprs2);
        let sort1 = sort_exec(reversed_sort_exprs2.clone(), source1);
        let sort2 = sort_exec(reversed_sort_exprs2.clone(), source2);

        let union = union_exec(vec![sort1, sort2]);
        let physical_plan = window_exec("nullable_col", reversed_sort_exprs2, union);

        // The `WindowAggExec` gets its sorting from multiple children jointly.
        // During the removal of `SortExec`s, it should be able to remove the
        // corresponding SortExecs together. Also, the inputs of these `SortExec`s
        // are not necessarily the same to be able to remove them.
        let expected_input = vec![
            "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow }]",
            "  UnionExec",
            "    SortExec: [nullable_col@0 DESC NULLS LAST], global=true",
            "      ParquetExec: limit=None, partitions={1 group: [[x]]}, output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], projection=[nullable_col, non_nullable_col]",
            "    SortExec: [nullable_col@0 DESC NULLS LAST], global=true",
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

    fn limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        global_limit_exec(local_limit_exec(input))
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
