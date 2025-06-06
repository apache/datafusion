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

//! An optimizer rule that checks ordering requirements of aggregate expressions
//! and modifies the expressions to work more efficiently if possible.

use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{plan_datafusion_err, Result};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalSortRequirement};
use datafusion_physical_plan::aggregates::{concat_slices, AggregateExec};
use datafusion_physical_plan::windows::get_ordered_partition_by_indices;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::PhysicalOptimizerRule;

/// This optimizer rule checks ordering requirements of aggregate expressions.
///
/// There are 3 kinds of aggregators in terms of ordering requirements:
/// - `AggregateOrderSensitivity::Insensitive`, meaning that ordering is not
///   important.
/// - `AggregateOrderSensitivity::HardRequirement`, meaning that the aggregator
///   requires a specific ordering.
/// - `AggregateOrderSensitivity::Beneficial`, meaning that the aggregator can
///   handle unordered input, but can run more efficiently if its input conforms
///   to a specific ordering.
///
/// This rule analyzes aggregate expressions of type `Beneficial` to see whether
/// their input ordering requirements are satisfied. If this is the case, the
/// aggregators are modified to run in a more efficient mode.
#[derive(Default, Debug)]
pub struct OptimizeAggregateOrder {}

impl OptimizeAggregateOrder {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for OptimizeAggregateOrder {
    /// Applies the `OptimizeAggregateOrder` rule to the provided execution plan.
    ///
    /// This function traverses the execution plan tree, identifies `AggregateExec` nodes,
    /// and optimizes their aggregate expressions based on existing input orderings.
    /// If optimizations are applied, it returns a modified execution plan.
    ///
    /// # Arguments
    ///
    /// * `plan` - The root of the execution plan to optimize.
    /// * `_config` - Configuration options (currently unused).
    ///
    /// # Returns
    ///
    /// A `Result` containing the potentially optimized execution plan or an error.
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            if let Some(aggr_exec) = plan.as_any().downcast_ref::<AggregateExec>() {
                // Final stage implementations do not rely on ordering -- those
                // ordering fields may be pruned out by first stage aggregates.
                // Hence, necessary information for proper merge is added during
                // the first stage to the state field, which the final stage uses.
                if !aggr_exec.mode().is_first_stage() {
                    return Ok(Transformed::no(plan));
                }
                let input = aggr_exec.input();
                let mut aggr_exprs = aggr_exec.aggr_expr().to_vec();

                let groupby_exprs = aggr_exec.group_expr().input_exprs();
                // If the existing ordering satisfies a prefix of the GROUP BY
                // expressions, prefix requirements with this section. In this
                // case, aggregation will work more efficiently.
                let indices = get_ordered_partition_by_indices(&groupby_exprs, input)?;
                let requirement = indices
                    .iter()
                    .map(|&idx| {
                        PhysicalSortRequirement::new(
                            Arc::clone(&groupby_exprs[idx]),
                            None,
                        )
                    })
                    .collect::<Vec<_>>();

                aggr_exprs = try_convert_aggregate_if_better(
                    aggr_exprs,
                    &requirement,
                    input.equivalence_properties(),
                )?;

                let aggr_exec = aggr_exec.with_new_aggr_exprs(aggr_exprs);

                Ok(Transformed::yes(Arc::new(aggr_exec) as _))
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "OptimizeAggregateOrder"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Tries to convert each aggregate expression to a potentially more efficient
/// version.
///
/// # Parameters
///
/// * `aggr_exprs` - A vector of `AggregateFunctionExpr` representing the
///   aggregate expressions to be optimized.
/// * `prefix_requirement` - An array slice representing the ordering
///   requirements preceding the aggregate expressions.
/// * `eq_properties` - A reference to the `EquivalenceProperties` object
///   containing ordering information.
///
/// # Returns
///
/// Returns `Ok(converted_aggr_exprs)` if the conversion process completes
/// successfully. Any errors occurring during the conversion process are
/// passed through.
fn try_convert_aggregate_if_better(
    aggr_exprs: Vec<Arc<AggregateFunctionExpr>>,
    prefix_requirement: &[PhysicalSortRequirement],
    eq_properties: &EquivalenceProperties,
) -> Result<Vec<Arc<AggregateFunctionExpr>>> {
    aggr_exprs
        .into_iter()
        .map(|aggr_expr| {
            let order_bys = aggr_expr.order_bys();
            // If the aggregate expression benefits from input ordering, and
            // there is an actual ordering enabling this, try to update the
            // aggregate expression to benefit from the existing ordering.
            // Otherwise, leave it as is.
            if !aggr_expr.order_sensitivity().is_beneficial() {
                Ok(aggr_expr)
            } else if !order_bys.is_empty() {
                if eq_properties.ordering_satisfy_requirement(concat_slices(
                    prefix_requirement,
                    &order_bys
                        .iter()
                        .map(|e| e.clone().into())
                        .collect::<Vec<_>>(),
                ))? {
                    // Existing ordering satisfies the aggregator requirements:
                    aggr_expr.with_beneficial_ordering(true)?.map(Arc::new)
                } else if eq_properties.ordering_satisfy_requirement(concat_slices(
                    prefix_requirement,
                    &order_bys
                        .iter()
                        .map(|e| e.reverse().into())
                        .collect::<Vec<_>>(),
                ))? {
                    // Converting to reverse enables more efficient execution
                    // given the existing ordering (if possible):
                    aggr_expr
                        .reverse_expr()
                        .map(Arc::new)
                        .unwrap_or(aggr_expr)
                        .with_beneficial_ordering(true)?
                        .map(Arc::new)
                } else {
                    // There is no beneficial ordering present -- aggregation
                    // will still work albeit in a less efficient mode.
                    aggr_expr.with_beneficial_ordering(false)?.map(Arc::new)
                }
                .ok_or_else(|| {
                    plan_datafusion_err!(
                    "Expects an aggregate expression that can benefit from input ordering"
                )
                })
            } else {
                Ok(aggr_expr)
            }
        })
        .collect()
}
