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
//! and try to combine them if necessary

use std::sync::Arc;

use datafusion_common::error::Result;
use datafusion_common::internal_err;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateKind, AggregateMode, PhysicalGroupBy,
};

use crate::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::{PhysicalExpr, physical_exprs_equal};

/// CombinePartialFinalAggregate optimizer rule combines the adjacent Partial and Final AggregateExecs
/// into a Single AggregateExec if their grouping exprs and aggregate exprs equal.
///
/// This rule should be applied after the `EnsureRequirements` rule (which
/// handles both distribution and sorting enforcement).
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
            let Some(agg_exec) = plan.downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(plan));
            };

            if !matches!(
                agg_exec.mode(),
                AggregateMode::Final | AggregateMode::FinalPartitioned
            ) {
                return Ok(Transformed::no(plan));
            }

            // Check if the input is AggregateExec
            let Some(input_agg_exec) = agg_exec.input().downcast_ref::<AggregateExec>()
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
                let Ok(mut combined_agg) = AggregateExec::try_new(
                    mode,
                    input_agg_exec.group_expr().clone(),
                    input_agg_exec.aggr_expr().to_vec(),
                    input_agg_exec.filter_expr().to_vec(),
                    Arc::clone(input_agg_exec.input()),
                    input_agg_exec.input_schema(),
                ) else {
                    return Ok(Transformed::no(plan));
                };

                // Re-apply distinct limit optimization.
                // 
                // The optimizer related to aggregates are (in order):
                // - 1. Initial planning: always final/partial two stage
                // - 2. `LimitedDistinctAggregation`: push limit into `AggregateExec`,
                //   and build `AggregateKind::DistinctLimit`
                // - 3. `CombinePartialFinalAggregate`: the current optimization
                //
                // Here it restores previously applied optimization in step 2
                let combined_aggr_kind = match (&agg_exec.kind, &input_agg_exec.kind) {
                    (AggregateKind::General { .. }, AggregateKind::General { .. }) => {
                        combined_agg.kind
                    }
                    (
                        AggregateKind::DistinctLimit { limit, .. },
                        AggregateKind::DistinctLimit {
                            group_by,
                            limit: partial_limit,
                        },
                    ) if limit == partial_limit => AggregateKind::DistinctLimit {
                        // The combined aggregate groups raw input, like the partial.
                        group_by: Arc::clone(group_by),
                        limit: *limit,
                    },
                    _ => {
                        return internal_err!(
                            "The AggregateKind should stay either (a) General (b) DistinctLimit introduced with the previous optimizer pass `LimitedDistinctAggregation`, it's impossible to have other variant"
                        );
                    }
                };
                combined_agg.kind = combined_aggr_kind;
                Some(Arc::new(combined_agg))
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
