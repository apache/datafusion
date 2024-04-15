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

use datafusion_common::Result;
use datafusion_common::{
    config::ConfigOptions,
    tree_node::{Transformed, TransformedResult, TreeNode},
};
use datafusion_physical_expr::{
    reverse_order_bys, AggregateExpr, EquivalenceProperties, PhysicalSortRequirement,
};
use datafusion_physical_plan::aggregates::concat_slices;
use datafusion_physical_plan::{
    aggregates::AggregateExec, ExecutionPlan, ExecutionPlanProperties,
};
use std::sync::Arc;

use datafusion_physical_plan::windows::get_ordered_partition_by_indices;

use super::PhysicalOptimizerRule;

/// The optimizer rule check the ordering requirements of the aggregate expressions.
/// And convert between FIRST_VALUE and LAST_VALUE if possible.
/// For example, If we have an ascending values and we want LastValue from the descending requirement,
/// it is equivalent to FirstValue with the current ascending ordering.
///
/// The concrete example is that, says we have values c1 with [1, 2, 3], which is an ascending order.
/// If we want LastValue(c1 order by desc), which is the first value of reversed c1 [3, 2, 1],
/// so we can convert the aggregate expression to FirstValue(c1 order by asc),
/// since the current ordering is already satisfied, it saves our time!
#[derive(Default)]
pub struct OptimizeAggregateOrder {}

impl OptimizeAggregateOrder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for OptimizeAggregateOrder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&replace_with_reverse_aggregator_when_beneficial)
            .data()
    }

    fn name(&self) -> &str {
        "OptimizeAggregateOrder"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn replace_with_reverse_aggregator_when_beneficial(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(aggr_exec) = plan.as_any().downcast_ref::<AggregateExec>() {
        if !aggr_exec.mode().is_first_stage() {
            return Ok(Transformed::no(plan));
        }
        let input = aggr_exec.input();
        let mut aggr_expr = aggr_exec.aggr_expr().to_vec();
        let group_by = aggr_exec.group_by();

        let input_eq_properties = input.equivalence_properties();
        let groupby_exprs = group_by.input_exprs();
        // If existing ordering satisfies a prefix of the GROUP BY expressions,
        // prefix requirements with this section. In this case, aggregation will
        // work more efficiently.
        let indices = get_ordered_partition_by_indices(&groupby_exprs, input);
        let requirement = indices
            .iter()
            .map(|&idx| PhysicalSortRequirement {
                expr: groupby_exprs[idx].clone(),
                options: None,
            })
            .collect::<Vec<_>>();

        try_convert_reverse_if_better(&requirement, &mut aggr_expr, input_eq_properties)?;

        let aggr_exec = aggr_exec.with_new_aggr_exprs(aggr_expr);

        Ok(Transformed::yes(
            Arc::new(aggr_exec) as Arc<dyn ExecutionPlan>
        ))
    } else {
        Ok(Transformed::no(plan))
    }
}

/// Get the common requirement that satisfies all the aggregate expressions.
///
/// # Parameters
///
/// - `aggr_exprs`: A slice of `Arc<dyn AggregateExpr>` containing all the
///   aggregate expressions.
/// - `eq_properties`: A reference to an `EquivalenceProperties` instance
///   representing equivalence properties for ordering.
///
/// # Returns
///
/// A `LexRequirement` instance, which is the requirement that satisfies all the
/// aggregate requirements. Returns an error in case of conflicting requirements.
///
/// Similar to the one in datafusion/physical-plan/src/aggregates/mod.rs, but this
/// function care only the possible conversion between FIRST_VALUE and LAST_VALUE
fn try_convert_reverse_if_better(
    prefix_requirement: &[PhysicalSortRequirement],
    aggr_exprs: &mut [Arc<dyn AggregateExpr>],
    eq_properties: &EquivalenceProperties,
) -> Result<()> {
    for aggr_expr in aggr_exprs.iter_mut() {
        let aggr_req = aggr_expr.order_bys().unwrap_or(&[]);
        let reverse_aggr_req = reverse_order_bys(aggr_req);
        let aggr_req = PhysicalSortRequirement::from_sort_exprs(aggr_req);
        let reverse_aggr_req =
            PhysicalSortRequirement::from_sort_exprs(&reverse_aggr_req);

        if aggr_expr.order_sensitivity().is_order_hard_required() || aggr_req.is_empty() {
            // If ordering for the aggregator is absolute requirement
            // or there is no requirement for the aggregator, shouldn't update the aggregator
            continue;
        }
        if eq_properties
            .ordering_satisfy_requirement(&concat_slices(prefix_requirement, &aggr_req))
        {
            // Existing ordering satisfy the requirement of the aggregator
            *aggr_expr = aggr_expr.clone().with_requirement_satisfied(true)?;
        } else if eq_properties.ordering_satisfy_requirement(&concat_slices(
            prefix_requirement,
            &reverse_aggr_req,
        )) {
            // Converting to reverse enables more efficient execution
            // given the existing ordering:
            if let Some(aggr_expr_rev) = aggr_expr.reverse_expr() {
                *aggr_expr = aggr_expr_rev;
            } else {
                // If reverse execution is not possible, cannot update current aggregate expression.
                continue;
            }
            *aggr_expr = aggr_expr.clone().with_requirement_satisfied(true)?;
            // Requirement is not satisfied with existing ordering.
        } else {
            // Requirement is not satisfied for the aggregator (Please note that: Aggregator can still work in this case, guaranteed by order sensitive flag being false.
            // However, It will be inefficient compared to version where requirement is satisfied).
            *aggr_expr = aggr_expr.clone().with_requirement_satisfied(false)?;
        }
    }

    Ok(())
}
