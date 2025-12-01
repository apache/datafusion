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

//! Utilizing exact statistics from sources to avoid scanning data
use datafusion_common::scalar::ScalarValue;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::Result;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion_physical_plan::udaf::{AggregateFunctionExpr, StatisticsArgs};
use datafusion_physical_plan::{expressions, ExecutionPlan};
use std::sync::Arc;

use crate::{OptimizerContext, PhysicalOptimizerRule};

/// Optimizer that uses available statistics for aggregate functions
#[derive(Default, Debug)]
pub struct AggregateStatistics {}

impl AggregateStatistics {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for AggregateStatistics {
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    #[allow(clippy::only_used_in_recursion)] // See https://github.com/rust-lang/rust-clippy/issues/14566
    fn optimize_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &OptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(partial_agg_exec) = take_optimizable(&*plan) {
            let partial_agg_exec = partial_agg_exec
                .as_any()
                .downcast_ref::<AggregateExec>()
                .expect("take_optimizable() ensures that this is a AggregateExec");
            let stats = partial_agg_exec.input().partition_statistics(None)?;
            let mut projections = vec![];
            for expr in partial_agg_exec.aggr_expr() {
                let field = expr.field();
                let args = expr.expressions();
                let statistics_args = StatisticsArgs {
                    statistics: &stats,
                    return_type: field.data_type(),
                    is_distinct: expr.is_distinct(),
                    exprs: args.as_slice(),
                };
                if let Some((optimizable_statistic, name)) =
                    take_optimizable_value_from_statistics(&statistics_args, expr)
                {
                    projections.push(ProjectionExpr {
                        expr: expressions::lit(optimizable_statistic),
                        alias: name.to_owned(),
                    });
                } else {
                    // TODO: we need all aggr_expr to be resolved (cf TODO fullres)
                    break;
                }
            }

            // TODO fullres: use statistics even if not all aggr_expr could be resolved
            if projections.len() == partial_agg_exec.aggr_expr().len() {
                // input can be entirely removed
                Ok(Arc::new(ProjectionExec::try_new(
                    projections,
                    Arc::new(PlaceholderRowExec::new(plan.schema())),
                )?))
            } else {
                plan.map_children(|child| {
                    self.optimize_plan(child, context).map(Transformed::yes)
                })
                .data()
            }
        } else {
            plan.map_children(|child| {
                self.optimize_plan(child, context).map(Transformed::yes)
            })
            .data()
        }
    }

    fn name(&self) -> &str {
        "aggregate_statistics"
    }

    /// This rule will change the nullable properties of the schema, disable the schema check.
    fn schema_check(&self) -> bool {
        false
    }
}

/// assert if the node passed as argument is a final `AggregateExec` node that can be optimized:
/// - its child (with possible intermediate layers) is a partial `AggregateExec` node
/// - they both have no grouping expression
///
/// If this is the case, return a ref to the partial `AggregateExec`, else `None`.
/// We would have preferred to return a casted ref to AggregateExec but the recursion requires
/// the `ExecutionPlan.children()` method that returns an owned reference.
fn take_optimizable(node: &dyn ExecutionPlan) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(final_agg_exec) = node.as_any().downcast_ref::<AggregateExec>() {
        if !final_agg_exec.mode().is_first_stage()
            && final_agg_exec.group_expr().is_empty()
        {
            let mut child = Arc::clone(final_agg_exec.input());
            loop {
                if let Some(partial_agg_exec) =
                    child.as_any().downcast_ref::<AggregateExec>()
                {
                    if partial_agg_exec.mode().is_first_stage()
                        && partial_agg_exec.group_expr().is_empty()
                        && partial_agg_exec.filter_expr().iter().all(|e| e.is_none())
                    {
                        return Some(child);
                    }
                }
                if let [childrens_child] = child.children().as_slice() {
                    child = Arc::clone(childrens_child);
                } else {
                    break;
                }
            }
        }
    }
    None
}

/// If this agg_expr is a max that is exactly defined in the statistics, return it.
fn take_optimizable_value_from_statistics(
    statistics_args: &StatisticsArgs,
    agg_expr: &AggregateFunctionExpr,
) -> Option<(ScalarValue, String)> {
    let value = agg_expr.fun().value_from_stats(statistics_args);
    value.map(|val| (val, agg_expr.name().to_string()))
}

// See tests in datafusion/core/tests/physical_optimizer
