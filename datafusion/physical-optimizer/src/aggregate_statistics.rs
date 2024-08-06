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
use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::scalar::ScalarValue;
use datafusion_common::Result;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::{expressions, AggregateExpr, ExecutionPlan, Statistics};

use crate::PhysicalOptimizerRule;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_physical_plan::udaf::AggregateFunctionExpr;

/// Optimizer that uses available statistics for aggregate functions
#[derive(Default)]
pub struct AggregateStatistics {}

impl AggregateStatistics {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for AggregateStatistics {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(partial_agg_exec) = take_optimizable(&*plan) {
            let partial_agg_exec = partial_agg_exec
                .as_any()
                .downcast_ref::<AggregateExec>()
                .expect("take_optimizable() ensures that this is a AggregateExec");
            let stats = partial_agg_exec.input().statistics()?;
            let mut projections = vec![];
            for expr in partial_agg_exec.aggr_expr() {
                if let Some((non_null_rows, name)) =
                    take_optimizable_column_and_table_count(&**expr, &stats)
                {
                    projections.push((expressions::lit(non_null_rows), name.to_owned()));
                } else if let Some((min, name)) = take_optimizable_min(&**expr, &stats) {
                    projections.push((expressions::lit(min), name.to_owned()));
                } else if let Some((max, name)) = take_optimizable_max(&**expr, &stats) {
                    projections.push((expressions::lit(max), name.to_owned()));
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
                    self.optimize(child, _config).map(Transformed::yes)
                })
                .data()
            }
        } else {
            plan.map_children(|child| self.optimize(child, _config).map(Transformed::yes))
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

/// If this agg_expr is a count that can be exactly derived from the statistics, return it.
fn take_optimizable_column_and_table_count(
    agg_expr: &dyn AggregateExpr,
    stats: &Statistics,
) -> Option<(ScalarValue, String)> {
    let col_stats = &stats.column_statistics;
    if is_non_distinct_count(agg_expr) {
        if let Precision::Exact(num_rows) = stats.num_rows {
            let exprs = agg_expr.expressions();
            if exprs.len() == 1 {
                // TODO optimize with exprs other than Column
                if let Some(col_expr) =
                    exprs[0].as_any().downcast_ref::<expressions::Column>()
                {
                    let current_val = &col_stats[col_expr.index()].null_count;
                    if let &Precision::Exact(val) = current_val {
                        return Some((
                            ScalarValue::Int64(Some((num_rows - val) as i64)),
                            agg_expr.name().to_string(),
                        ));
                    }
                } else if let Some(lit_expr) =
                    exprs[0].as_any().downcast_ref::<expressions::Literal>()
                {
                    if lit_expr.value() == &COUNT_STAR_EXPANSION {
                        return Some((
                            ScalarValue::Int64(Some(num_rows as i64)),
                            agg_expr.name().to_string(),
                        ));
                    }
                }
            }
        }
    }
    None
}

/// If this agg_expr is a min that is exactly defined in the statistics, return it.
fn take_optimizable_min(
    agg_expr: &dyn AggregateExpr,
    stats: &Statistics,
) -> Option<(ScalarValue, String)> {
    if let Precision::Exact(num_rows) = &stats.num_rows {
        match *num_rows {
            0 => {
                // MIN/MAX with 0 rows is always null
                if is_min(agg_expr) {
                    if let Ok(min_data_type) =
                        ScalarValue::try_from(agg_expr.field().unwrap().data_type())
                    {
                        return Some((min_data_type, agg_expr.name().to_string()));
                    }
                }
            }
            value if value > 0 => {
                let col_stats = &stats.column_statistics;
                if is_min(agg_expr) {
                    let exprs = agg_expr.expressions();
                    if exprs.len() == 1 {
                        // TODO optimize with exprs other than Column
                        if let Some(col_expr) =
                            exprs[0].as_any().downcast_ref::<expressions::Column>()
                        {
                            if let Precision::Exact(val) =
                                &col_stats[col_expr.index()].min_value
                            {
                                if !val.is_null() {
                                    return Some((
                                        val.clone(),
                                        agg_expr.name().to_string(),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    None
}

/// If this agg_expr is a max that is exactly defined in the statistics, return it.
fn take_optimizable_max(
    agg_expr: &dyn AggregateExpr,
    stats: &Statistics,
) -> Option<(ScalarValue, String)> {
    if let Precision::Exact(num_rows) = &stats.num_rows {
        match *num_rows {
            0 => {
                // MIN/MAX with 0 rows is always null
                if is_max(agg_expr) {
                    if let Ok(max_data_type) =
                        ScalarValue::try_from(agg_expr.field().unwrap().data_type())
                    {
                        return Some((max_data_type, agg_expr.name().to_string()));
                    }
                }
            }
            value if value > 0 => {
                let col_stats = &stats.column_statistics;
                if is_max(agg_expr) {
                    let exprs = agg_expr.expressions();
                    if exprs.len() == 1 {
                        // TODO optimize with exprs other than Column
                        if let Some(col_expr) =
                            exprs[0].as_any().downcast_ref::<expressions::Column>()
                        {
                            if let Precision::Exact(val) =
                                &col_stats[col_expr.index()].max_value
                            {
                                if !val.is_null() {
                                    return Some((
                                        val.clone(),
                                        agg_expr.name().to_string(),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    None
}

// TODO: Move this check into AggregateUDFImpl
// https://github.com/apache/datafusion/issues/11153
fn is_non_distinct_count(agg_expr: &dyn AggregateExpr) -> bool {
    if let Some(agg_expr) = agg_expr.as_any().downcast_ref::<AggregateFunctionExpr>() {
        if agg_expr.fun().name() == "count" && !agg_expr.is_distinct() {
            return true;
        }
    }
    false
}

// TODO: Move this check into AggregateUDFImpl
// https://github.com/apache/datafusion/issues/11153
fn is_min(agg_expr: &dyn AggregateExpr) -> bool {
    if let Some(agg_expr) = agg_expr.as_any().downcast_ref::<AggregateFunctionExpr>() {
        if agg_expr.fun().name().to_lowercase() == "min" {
            return true;
        }
    }
    false
}

// TODO: Move this check into AggregateUDFImpl
// https://github.com/apache/datafusion/issues/11153
fn is_max(agg_expr: &dyn AggregateExpr) -> bool {
    if let Some(agg_expr) = agg_expr.as_any().downcast_ref::<AggregateFunctionExpr>() {
        if agg_expr.fun().name().to_lowercase() == "max" {
            return true;
        }
    }
    false
}

// See tests in datafusion/core/tests/physical_optimizer
