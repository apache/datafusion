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

use arrow::compute::kernels::aggregate;
use datafusion_common::{
    config::ConfigOptions,
    not_impl_err,
    tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRewriter},
};
use datafusion_physical_expr::{
    aggregate::is_order_sensitive,
    expressions::{FirstValue, LastValue},
    physical_exprs_contains, reverse_order_bys, AggregateExpr, EquivalenceProperties,
    LexOrdering, LexRequirement, PhysicalSortRequirement,
};
use datafusion_physical_plan::{
    aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy},
    coalesce_partitions::CoalescePartitionsExec,
    ExecutionPlan, ExecutionPlanProperties,
};

use datafusion_physical_expr::equivalence::collapse_lex_req;
use datafusion_physical_plan::windows::get_ordered_partition_by_indices;

use crate::error::Result;

use super::{output_requirements::OutputRequirementExec, PhysicalOptimizerRule};

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
        let res = plan
            .transform_down(&get_common_requirement_of_aggregate_input)
            .data();

        // println!("res: {:?}", res);
        res
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
    let children = plan.children();

    let new_c: Option<Transformed<Arc<dyn ExecutionPlan>>> = if children.is_empty() {
        None
    } else {
        assert_eq!(children.len(), 1, "AggregateExec should have one child");
        let c = children[0].clone();

        // for c in children {
        //     let new_c = get_common_requirement_of_aggregate_input(c.clone())?;
        //     if new_c.transformed {
        //         is_transformed = true;
        //     }
        // }

        let new_c = get_common_requirement_of_aggregate_input(c)?;
        Some(new_c)
    };

    let plan = optimize_internal(plan)?;
    // println!("t: {} plan: {:?}", plan.transformed, plan);

    if let Some(c) = new_c {
        if !c.transformed {
            return Ok(plan);
        }

        let plan = plan.data;

        // TODO: support more types of ExecutionPlan
        if let Some(aggr_exec) = plan.as_any().downcast_ref::<AggregateExec>() {
            let p = aggr_exec.clone_with_input(c.data);
            return Ok(Transformed::yes(Arc::new(p) as Arc<dyn ExecutionPlan>));
        } else if let Some(coalesce_exec) =
            plan.as_any().downcast_ref::<CoalescePartitionsExec>()
        {
            let p = coalesce_exec.clone_with_input(c.data);
            return Ok(Transformed::yes(Arc::new(p) as Arc<dyn ExecutionPlan>));
        } else if let Some(out_req_exec) =
            plan.as_any().downcast_ref::<OutputRequirementExec>()
        {
            let p = out_req_exec.clone_with_input(c.data);
            return Ok(Transformed::yes(Arc::new(p) as Arc<dyn ExecutionPlan>));
        } else {
            return not_impl_err!("Unsupported ExecutionPlan type: {}", plan.name());
        }
    }

    return Ok(plan);
}

fn optimize_internal(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(aggr_exec) = plan.as_any().downcast_ref::<AggregateExec>() {
        if aggr_exec.mode() != &AggregateMode::Partial {
            return Ok(Transformed::no(plan));
        }

        let input = aggr_exec.input().clone();
        let mut aggr_expr = aggr_exec.aggr_expr().to_vec();
        let group_by = aggr_exec.group_by();
        let mode = aggr_exec.mode();

        let input_eq_properties = input.equivalence_properties();
        let groupby_exprs = group_by.input_exprs();
        // If existing ordering satisfies a prefix of the GROUP BY expressions,
        // prefix requirements with this section. In this case, aggregation will
        // work more efficiently.
        let indices = get_ordered_partition_by_indices(&groupby_exprs, &input);
        let mut new_requirement = indices
            .iter()
            .map(|&idx| PhysicalSortRequirement {
                expr: groupby_exprs[idx].clone(),
                options: None,
            })
            .collect::<Vec<_>>();

        let req = get_aggregate_exprs_requirement(
            &new_requirement,
            &mut aggr_expr,
            &group_by,
            input_eq_properties,
            mode,
        )?;
        new_requirement.extend(req);
        new_requirement = collapse_lex_req(new_requirement);
        let required_input_ordering =
            (!new_requirement.is_empty()).then_some(new_requirement);

        let p = aggr_exec.clone_with_required_input_ordering(required_input_ordering);

        let res = Arc::new(p) as Arc<dyn ExecutionPlan>;
        Ok(Transformed::yes(res))
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


mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
    use datafusion_physical_expr::{expressions::{col, OrderSensitiveArrayAgg}, PhysicalSortExpr};

    fn create_test_schema() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, true);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, true);
        let e = Field::new("e", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e]));

        Ok(schema)
    }

    #[tokio::test]
    async fn test_get_finest_requirements() -> Result<()> {
        let test_schema = create_test_schema()?;
        // Assume column a and b are aliases
        // Assume also that a ASC and c DESC describe the same global ordering for the table. (Since they are ordering equivalent).
        let options1 = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let mut eq_properties = EquivalenceProperties::new(test_schema);
        // Columns a and b are equal.
        eq_properties.add_equal_conditions(col_a, col_b);
        // Aggregate requirements are
        // [None], [a ASC], [a ASC, b ASC, c ASC], [a ASC, b ASC] respectively
        let order_by_exprs = vec![
            None,
            Some(vec![PhysicalSortExpr {
                expr: col_a.clone(),
                options: options1,
            }]),
            Some(vec![
                PhysicalSortExpr {
                    expr: col_a.clone(),
                    options: options1,
                },
                PhysicalSortExpr {
                    expr: col_b.clone(),
                    options: options1,
                },
                PhysicalSortExpr {
                    expr: col_c.clone(),
                    options: options1,
                },
            ]),
            Some(vec![
                PhysicalSortExpr {
                    expr: col_a.clone(),
                    options: options1,
                },
                PhysicalSortExpr {
                    expr: col_b.clone(),
                    options: options1,
                },
            ]),
        ];
        let common_requirement = vec![
            PhysicalSortExpr {
                expr: col_a.clone(),
                options: options1,
            },
            PhysicalSortExpr {
                expr: col_c.clone(),
                options: options1,
            },
        ];
        let mut aggr_exprs = order_by_exprs
            .into_iter()
            .map(|order_by_expr| {
                Arc::new(OrderSensitiveArrayAgg::new(
                    col_a.clone(),
                    "array_agg",
                    DataType::Int32,
                    false,
                    vec![],
                    order_by_expr.unwrap_or_default(),
                )) as _
            })
            .collect::<Vec<_>>();
        let group_by = PhysicalGroupBy::new_single(vec![]);
        let res = get_aggregate_exprs_requirement(
            &[],
            &mut aggr_exprs,
            &group_by,
            &eq_properties,
            &AggregateMode::Partial,
        )?;
        let res = PhysicalSortRequirement::to_sort_exprs(res);
        assert_eq!(res, common_requirement);
        Ok(())
    }
}