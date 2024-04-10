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

use std::sync::Arc;

use datafusion_common::{
    config::ConfigOptions,
    not_impl_err,
    tree_node::{Transformed, TransformedResult, TreeNode},
};
use datafusion_physical_expr::{
    aggregate::is_order_sensitive,
    expressions::{FirstValue, LastValue},
    physical_exprs_contains, reverse_order_bys, AggregateExpr, EquivalenceProperties,
    LexOrdering, LexRequirement, PhysicalSortRequirement,
};
use datafusion_physical_plan::{
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    metrics::ExecutionPlanMetricsSet,
    ExecutionPlan, ExecutionPlanProperties,
};

use crate::error::Result;

use super::PhysicalOptimizerRule;

#[derive(Default)]
pub struct SimplifyOrdering {}

impl SimplifyOrdering {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for SimplifyOrdering {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(&get_common_requirement_of_aggregate_input)
            .data()
    }

    fn name(&self) -> &str {
        "SimpleOrdering"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn get_common_requirement_of_aggregate_input(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let mut is_transformed = false;

    let plan = if let Some(aggr_exec) = plan.as_any().downcast_ref::<AggregateExec>() {
        let input = if let Some(aggr_exec) =
            aggr_exec.input().as_any().downcast_ref::<AggregateExec>()
        {
            // println!("AggregateExec");
            let input = aggr_exec.input().clone();
            let mut aggr_expr = aggr_exec.aggr_expr().to_vec();
            let group_by = aggr_exec.group_by();
            let mode = aggr_exec.mode().clone();

            use datafusion_physical_expr::equivalence::collapse_lex_req;
            use datafusion_physical_plan::windows::get_ordered_partition_by_indices;

            let input_eq_properties = input.equivalence_properties();
            let groupby_exprs = group_by.input_exprs();
            let indices = get_ordered_partition_by_indices(&groupby_exprs, &input);
            let mut new_requirement = indices
                .iter()
                .map(|&idx| PhysicalSortRequirement {
                    expr: groupby_exprs[idx].clone(),
                    options: None,
                })
                .collect::<Vec<_>>();

            // print!("my new_requirement: {:?}", new_requirement);
            // println!("my aggr_expr: {:?}", aggr_expr);
            // println!("my group_by: {:?}", group_by);
            // println!("my input_eq_properties: {:?}", input_eq_properties);
            // println!("my mode: {:?}", mode);
            let req = get_aggregate_exprs_requirement(
                &new_requirement,
                &mut aggr_expr,
                &group_by,
                input_eq_properties,
                &mode,
            )?;
            // println!("my req: {:?}", req);
            new_requirement.extend(req);
            new_requirement = collapse_lex_req(new_requirement);
            let required_input_ordering =
                (!new_requirement.is_empty()).then_some(new_requirement);

            // println!(
            //     "my required_input_ordering: {:?}",
            //     required_input_ordering
            // );
            let filter_expr = aggr_exec.filter_expr().to_vec();

            // let metrics = aggr_exec.metrics();

            let p = AggregateExec {
                mode,
                group_by: group_by.clone(),
                aggr_expr,
                filter_expr,
                input,
                schema: aggr_exec.schema().clone(),
                input_schema: aggr_exec.input_schema().clone(),
                metrics: ExecutionPlanMetricsSet::new(),
                required_input_ordering,
                limit: None,
                input_order_mode: aggr_exec.input_order_mode().clone(),
                cache: aggr_exec.cache().clone(),
            };

            is_transformed = true;

            Arc::new(p) as Arc<dyn ExecutionPlan>
        } else {
            aggr_exec.input().clone()
        };

        // TODO: modify the input of aggr_exec

        // aggr_exec as Arc<dyn ExecutionPlan>

        Arc::new(AggregateExec {
            mode: aggr_exec.mode().clone(),
            group_by: aggr_exec.group_by().clone(),
            aggr_expr: aggr_exec.aggr_expr().to_vec(),
            filter_expr: aggr_exec.filter_expr().to_vec(),
            input,
            schema: aggr_exec.schema().clone(),
            input_schema: aggr_exec.input_schema().clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            required_input_ordering: aggr_exec.required_input_ordering()[0].clone(),
            limit: aggr_exec.limit().clone(),
            input_order_mode: aggr_exec.input_order_mode().clone(),
            cache: aggr_exec.cache().clone(),
        }) as Arc<dyn ExecutionPlan>
    } else {
        plan
    };

    if is_transformed {
        Ok(Transformed::yes(plan))
    } else {
        Ok(Transformed::no(plan))
    }
}

/// Determines the lexical ordering requirement for an aggregate expression.
///
/// # Parameters
///
/// - `aggr_expr`: A reference to an `Arc<dyn AggregateExpr>` representing the
///   aggregate expression.
/// - `group_by`: A reference to a `PhysicalGroupBy` instance representing the
///   physical GROUP BY expression.
/// - `agg_mode`: A reference to an `AggregateMode` instance representing the
///   mode of aggregation.
///
/// # Returns
///
/// A `LexOrdering` instance indicating the lexical ordering requirement for
/// the aggregate expression.
fn get_aggregate_expr_req(
    aggr_expr: &Arc<dyn AggregateExpr>,
    group_by: &PhysicalGroupBy,
    agg_mode: &AggregateMode,
) -> LexOrdering {
    // If the aggregation function is not order sensitive, or the aggregation
    // is performing a "second stage" calculation, or all aggregate function
    // requirements are inside the GROUP BY expression, then ignore the ordering
    // requirement.
    if !is_order_sensitive(aggr_expr) || !agg_mode.is_first_stage() {
        return vec![];
    }

    let mut req = aggr_expr.order_bys().unwrap_or_default().to_vec();

    // In non-first stage modes, we accumulate data (using `merge_batch`) from
    // different partitions (i.e. merge partial results). During this merge, we
    // consider the ordering of each partial result. Hence, we do not need to
    // use the ordering requirement in such modes as long as partial results are
    // generated with the correct ordering.
    if group_by.is_single() {
        // Remove all orderings that occur in the group by. These requirements
        // will definitely be satisfied -- Each group by expression will have
        // distinct values per group, hence all requirements are satisfied.
        let physical_exprs = group_by.input_exprs();
        req.retain(|sort_expr| {
            !physical_exprs_contains(&physical_exprs, &sort_expr.expr)
        });
    }
    req
}

/// Computes the finer ordering for between given existing ordering requirement
/// of aggregate expression.
///
/// # Parameters
///
/// * `existing_req` - The existing lexical ordering that needs refinement.
/// * `aggr_expr` - A reference to an aggregate expression trait object.
/// * `group_by` - Information about the physical grouping (e.g group by expression).
/// * `eq_properties` - Equivalence properties relevant to the computation.
/// * `agg_mode` - The mode of aggregation (e.g., Partial, Final, etc.).
///
/// # Returns
///
/// An `Option<LexOrdering>` representing the computed finer lexical ordering,
/// or `None` if there is no finer ordering; e.g. the existing requirement and
/// the aggregator requirement is incompatible.
fn finer_ordering(
    existing_req: &LexOrdering,
    aggr_expr: &Arc<dyn AggregateExpr>,
    group_by: &PhysicalGroupBy,
    eq_properties: &EquivalenceProperties,
    agg_mode: &AggregateMode,
) -> Option<LexOrdering> {
    let aggr_req = get_aggregate_expr_req(aggr_expr, group_by, agg_mode);
    eq_properties.get_finer_ordering(existing_req, &aggr_req)
}

/// Concatenates the given slices.
fn concat_slices<T: Clone>(lhs: &[T], rhs: &[T]) -> Vec<T> {
    [lhs, rhs].concat()
}

/// Get the common requirement that satisfies all the aggregate expressions.
///
/// # Parameters
///
/// - `aggr_exprs`: A slice of `Arc<dyn AggregateExpr>` containing all the
///   aggregate expressions.
/// - `group_by`: A reference to a `PhysicalGroupBy` instance representing the
///   physical GROUP BY expression.
/// - `eq_properties`: A reference to an `EquivalenceProperties` instance
///   representing equivalence properties for ordering.
/// - `agg_mode`: A reference to an `AggregateMode` instance representing the
///   mode of aggregation.
///
/// # Returns
///
/// A `LexRequirement` instance, which is the requirement that satisfies all the
/// aggregate requirements. Returns an error in case of conflicting requirements.
fn get_aggregate_exprs_requirement(
    prefix_requirement: &[PhysicalSortRequirement],
    aggr_exprs: &mut [Arc<dyn AggregateExpr>],
    group_by: &PhysicalGroupBy,
    eq_properties: &EquivalenceProperties,
    agg_mode: &AggregateMode,
) -> Result<LexRequirement> {
    let mut requirement = vec![];
    for aggr_expr in aggr_exprs.iter_mut() {
        let aggr_req = aggr_expr.order_bys().unwrap_or(&[]);
        let reverse_aggr_req = reverse_order_bys(aggr_req);
        let aggr_req = PhysicalSortRequirement::from_sort_exprs(aggr_req);
        let reverse_aggr_req =
            PhysicalSortRequirement::from_sort_exprs(&reverse_aggr_req);

        if let Some(first_value) = aggr_expr.as_any().downcast_ref::<FirstValue>() {
            let mut first_value = first_value.clone();
            if eq_properties.ordering_satisfy_requirement(&concat_slices(
                prefix_requirement,
                &aggr_req,
            )) {
                first_value = first_value.with_requirement_satisfied(true);
                *aggr_expr = Arc::new(first_value) as _;
            } else if eq_properties.ordering_satisfy_requirement(&concat_slices(
                prefix_requirement,
                &reverse_aggr_req,
            )) {
                // Converting to LAST_VALUE enables more efficient execution
                // given the existing ordering:
                let mut last_value = first_value.convert_to_last();
                last_value = last_value.with_requirement_satisfied(true);
                *aggr_expr = Arc::new(last_value) as _;
            } else {
                // Requirement is not satisfied with existing ordering.
                first_value = first_value.with_requirement_satisfied(false);
                *aggr_expr = Arc::new(first_value) as _;
            }
            continue;
        }
        if let Some(last_value) = aggr_expr.as_any().downcast_ref::<LastValue>() {
            let mut last_value = last_value.clone();
            if eq_properties.ordering_satisfy_requirement(&concat_slices(
                prefix_requirement,
                &aggr_req,
            )) {
                last_value = last_value.with_requirement_satisfied(true);
                *aggr_expr = Arc::new(last_value) as _;
            } else if eq_properties.ordering_satisfy_requirement(&concat_slices(
                prefix_requirement,
                &reverse_aggr_req,
            )) {
                // Converting to FIRST_VALUE enables more efficient execution
                // given the existing ordering:
                let mut first_value = last_value.convert_to_first();
                first_value = first_value.with_requirement_satisfied(true);
                *aggr_expr = Arc::new(first_value) as _;
            } else {
                // Requirement is not satisfied with existing ordering.
                last_value = last_value.with_requirement_satisfied(false);
                *aggr_expr = Arc::new(last_value) as _;
            }
            continue;
        }
        if let Some(finer_ordering) =
            finer_ordering(&requirement, aggr_expr, group_by, eq_properties, agg_mode)
        {
            if eq_properties.ordering_satisfy(&finer_ordering) {
                // Requirement is satisfied by existing ordering
                requirement = finer_ordering;
                continue;
            }
        }
        if let Some(reverse_aggr_expr) = aggr_expr.reverse_expr() {
            if let Some(finer_ordering) = finer_ordering(
                &requirement,
                &reverse_aggr_expr,
                group_by,
                eq_properties,
                agg_mode,
            ) {
                if eq_properties.ordering_satisfy(&finer_ordering) {
                    // Reverse requirement is satisfied by exiting ordering.
                    // Hence reverse the aggregator
                    requirement = finer_ordering;
                    *aggr_expr = reverse_aggr_expr;
                    continue;
                }
            }
        }
        if let Some(finer_ordering) =
            finer_ordering(&requirement, aggr_expr, group_by, eq_properties, agg_mode)
        {
            // There is a requirement that both satisfies existing requirement and current
            // aggregate requirement. Use updated requirement
            requirement = finer_ordering;
            continue;
        }
        if let Some(reverse_aggr_expr) = aggr_expr.reverse_expr() {
            if let Some(finer_ordering) = finer_ordering(
                &requirement,
                &reverse_aggr_expr,
                group_by,
                eq_properties,
                agg_mode,
            ) {
                // There is a requirement that both satisfies existing requirement and reverse
                // aggregate requirement. Use updated requirement
                requirement = finer_ordering;
                *aggr_expr = reverse_aggr_expr;
                continue;
            }
        }
        // Neither the existing requirement and current aggregate requirement satisfy the other, this means
        // requirements are conflicting. Currently, we do not support
        // conflicting requirements.
        return not_impl_err!(
            "Conflicting ordering requirements in aggregate functions is not supported"
        );
    }
    Ok(PhysicalSortRequirement::from_sort_exprs(&requirement))
}
