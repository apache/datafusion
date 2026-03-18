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

//! CombinePartialFinalAggregate optimizer rule checks the adjacent Partial and Final AggregateExecs
//! and try to combine them if necessary.
//!
//! Also combines Partial and FinalPartitioned aggregates separated by a RepartitionExec
//! into a SinglePartitioned aggregate, which is particularly beneficial for the
//! `SingleDistinctToGroupBy` pattern — reducing 4 aggregate layers to 2.

use std::sync::Arc;

use datafusion_common::error::Result;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::repartition::RepartitionExec;

use crate::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::{PhysicalExpr, physical_exprs_equal};

/// CombinePartialFinalAggregate optimizer rule combines the adjacent Partial and Final AggregateExecs
/// into a Single AggregateExec if their grouping exprs and aggregate exprs equal.
///
/// It also combines `FinalPartitioned → RepartitionExec → Partial` into
/// `SinglePartitioned → RepartitionExec`, eliminating one aggregate layer.
/// This fires twice on the 4-layer `SingleDistinctToGroupBy` pattern,
/// reducing it to 2 aggregate layers.
///
/// This rule should be applied after the EnforceDistribution and EnforceSorting rules
#[derive(Default, Debug)]
pub struct CombinePartialFinalAggregate {}

impl CombinePartialFinalAggregate {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for CombinePartialFinalAggregate {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|plan| {
            // Check if the plan is AggregateExec
            let Some(agg_exec) = plan.as_any().downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(plan));
            };

            if !matches!(
                agg_exec.mode(),
                AggregateMode::Final | AggregateMode::FinalPartitioned
            ) {
                return Ok(Transformed::no(plan));
            }

            // Try combining the 4-layer distinct pattern into 2 layers
            if let Some(optimized) = try_combine_stacked_aggregates(agg_exec)? {
                return Ok(Transformed::yes(optimized));
            }

            // Check if the input is AggregateExec (original combine logic)
            let Some(input_agg_exec) =
                agg_exec.input().as_any().downcast_ref::<AggregateExec>()
            else {
                return Ok(Transformed::no(plan));
            };

            let transformed = if *input_agg_exec.mode() == AggregateMode::Partial
                && can_combine(
                    (
                        agg_exec.group_expr(),
                        agg_exec.aggr_expr(),
                        agg_exec.filter_expr(),
                    ),
                    (
                        input_agg_exec.group_expr(),
                        input_agg_exec.aggr_expr(),
                        input_agg_exec.filter_expr(),
                    ),
                ) {
                let mode = if agg_exec.mode() == &AggregateMode::Final {
                    AggregateMode::Single
                } else {
                    AggregateMode::SinglePartitioned
                };
                AggregateExec::try_new(
                    mode,
                    input_agg_exec.group_expr().clone(),
                    input_agg_exec.aggr_expr().to_vec(),
                    input_agg_exec.filter_expr().to_vec(),
                    Arc::clone(input_agg_exec.input()),
                    input_agg_exec.input_schema(),
                )
                .map(|combined_agg| {
                    combined_agg.with_limit_options(agg_exec.limit_options())
                })
                .ok()
                .map(Arc::new)
            } else {
                None
            };
            Ok(if let Some(transformed) = transformed {
                Transformed::yes(transformed)
            } else {
                Transformed::no(plan)
            })
        })
        .data()
    }

    fn name(&self) -> &str {
        "CombinePartialFinalAggregate"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Combines the 4-layer aggregate pattern from `SingleDistinctToGroupBy` into 2 layers.
///
/// Matches:
/// ```text
/// Agg(FinalPartitioned, G_outer) → Repart(G_outer) → Agg(Partial, G_outer) →
///   Agg(FinalPartitioned, G_inner) → Repart(G_inner) → Agg(Partial, G_inner)
/// ```
///
/// Replaces with:
/// ```text
/// Agg(SinglePartitioned, G_outer) → Repart(G_outer) →
///   Agg(SinglePartitioned, G_inner) → Repart(G_inner)
/// ```
///
/// Each `FinalPartitioned → Repartition → Partial` triple is combined into
/// `SinglePartitioned → Repartition`. The repartition is rebuilt with the Partial's
/// group-by expressions (referencing the raw input schema) since the Partial is removed.
fn try_combine_stacked_aggregates(
    outer_final: &AggregateExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if *outer_final.mode() != AggregateMode::FinalPartitioned {
        return Ok(None);
    }

    // Match: outer_final → outer_repart → outer_partial
    let Some(outer_repart) = outer_final
        .input()
        .as_any()
        .downcast_ref::<RepartitionExec>()
    else {
        return Ok(None);
    };
    let Some(outer_partial) = outer_repart
        .input()
        .as_any()
        .downcast_ref::<AggregateExec>()
    else {
        return Ok(None);
    };
    if *outer_partial.mode() != AggregateMode::Partial {
        return Ok(None);
    }
    if !can_combine(
        (
            outer_final.group_expr(),
            outer_final.aggr_expr(),
            outer_final.filter_expr(),
        ),
        (
            outer_partial.group_expr(),
            outer_partial.aggr_expr(),
            outer_partial.filter_expr(),
        ),
    ) {
        return Ok(None);
    }

    // Match: inner_final → inner_repart → inner_partial
    let Some(inner_final) = outer_partial
        .input()
        .as_any()
        .downcast_ref::<AggregateExec>()
    else {
        return Ok(None);
    };
    if *inner_final.mode() != AggregateMode::FinalPartitioned {
        return Ok(None);
    }
    let Some(inner_repart) = inner_final
        .input()
        .as_any()
        .downcast_ref::<RepartitionExec>()
    else {
        return Ok(None);
    };
    let Some(inner_partial) = inner_repart
        .input()
        .as_any()
        .downcast_ref::<AggregateExec>()
    else {
        return Ok(None);
    };
    if *inner_partial.mode() != AggregateMode::Partial {
        return Ok(None);
    }
    if !can_combine(
        (
            inner_final.group_expr(),
            inner_final.aggr_expr(),
            inner_final.filter_expr(),
        ),
        (
            inner_partial.group_expr(),
            inner_partial.aggr_expr(),
            inner_partial.filter_expr(),
        ),
    ) {
        return Ok(None);
    }

    // Verify inner group-by is a superset of outer (the stacked/distinct pattern)
    if inner_final.group_expr().expr().len() <= outer_final.group_expr().expr().len() {
        return Ok(None);
    }

    // Build the inner SinglePartitioned:
    // SinglePartitioned(G_inner) → Repartition(Hash(G_inner)) → [original input]
    let inner_target = inner_repart
        .properties()
        .output_partitioning()
        .partition_count();
    let inner_repart_exprs: Vec<Arc<dyn PhysicalExpr>> = inner_partial
        .group_expr()
        .expr()
        .iter()
        .map(|(e, _)| Arc::clone(e))
        .collect();
    let new_inner_repart = Arc::new(RepartitionExec::try_new(
        Arc::clone(inner_partial.input()),
        datafusion_physical_expr::Partitioning::Hash(inner_repart_exprs, inner_target),
    )?);
    let new_inner = Arc::new(AggregateExec::try_new(
        AggregateMode::SinglePartitioned,
        inner_partial.group_expr().clone(),
        inner_partial.aggr_expr().to_vec(),
        inner_partial.filter_expr().to_vec(),
        new_inner_repart,
        inner_partial.input_schema(),
    )?);

    // Build the outer SinglePartitioned:
    // SinglePartitioned(G_outer) → Repartition(Hash(G_outer)) → [inner]
    //
    // The outer_partial's group-by references the raw input (inner's output).
    // After replacing the inner, its output schema matches inner_partial's output
    // (same group-by, same agg), so outer_partial's expressions are still valid.
    let outer_target = outer_repart
        .properties()
        .output_partitioning()
        .partition_count();
    let outer_repart_exprs: Vec<Arc<dyn PhysicalExpr>> = outer_partial
        .group_expr()
        .expr()
        .iter()
        .map(|(e, _)| Arc::clone(e))
        .collect();
    let new_outer_repart = Arc::new(RepartitionExec::try_new(
        new_inner,
        datafusion_physical_expr::Partitioning::Hash(outer_repart_exprs, outer_target),
    )?);
    let new_outer = AggregateExec::try_new(
        AggregateMode::SinglePartitioned,
        outer_partial.group_expr().clone(),
        outer_partial.aggr_expr().to_vec(),
        outer_partial.filter_expr().to_vec(),
        new_outer_repart,
        outer_partial.input_schema(),
    )?
    .with_limit_options(outer_final.limit_options());

    Ok(Some(Arc::new(new_outer)))
}

type GroupExprsRef<'a> = (
    &'a PhysicalGroupBy,
    &'a [Arc<AggregateFunctionExpr>],
    &'a [Option<Arc<dyn PhysicalExpr>>],
);

fn can_combine(final_agg: GroupExprsRef, partial_agg: GroupExprsRef) -> bool {
    let (final_group_by, final_aggr_expr, final_filter_expr) = final_agg;
    let (input_group_by, input_aggr_expr, input_filter_expr) = partial_agg;

    // Compare output expressions of the partial, and input expressions of the final operator.
    physical_exprs_equal(
        &input_group_by.output_exprs(),
        &final_group_by.input_exprs(),
    ) && input_group_by.groups() == final_group_by.groups()
        && input_group_by.null_expr().len() == final_group_by.null_expr().len()
        && input_group_by
            .null_expr()
            .iter()
            .zip(final_group_by.null_expr().iter())
            .all(|((lhs_expr, lhs_str), (rhs_expr, rhs_str))| {
                lhs_expr.eq(rhs_expr) && lhs_str == rhs_str
            })
        && final_aggr_expr.len() == input_aggr_expr.len()
        && final_aggr_expr
            .iter()
            .zip(input_aggr_expr.iter())
            .all(|(final_expr, partial_expr)| final_expr.eq(partial_expr))
        && final_filter_expr.len() == input_filter_expr.len()
        && final_filter_expr.iter().zip(input_filter_expr.iter()).all(
            |(final_expr, partial_expr)| match (final_expr, partial_expr) {
                (Some(l), Some(r)) => l.eq(r),
                (None, None) => true,
                _ => false,
            },
        )
}

// See tests in datafusion/core/tests/physical_optimizer
