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

//! An optimizer rule that detects aggregate operations that could use a limited bucket count

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::aggregates::LimitOptions;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, topk_types_supported,
};
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::limit::GlobalLimitExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use itertools::Itertools;

/// An optimizer rule that passes a `limit` hint to aggregations if the whole result is not needed
#[derive(Debug)]
pub struct TopKAggregation {}

impl TopKAggregation {
    /// Create a new `LimitAggregation`
    pub fn new() -> Self {
        Self {}
    }

    fn transform_agg(
        aggr: &AggregateExec,
        sort_columns: &[(String, bool)],
        limit: usize,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        if aggr.filter_expr().iter().any(|e| e.is_some()) {
            return None;
        }

        // Check if this is ordering by an aggregate function (MIN/MAX)
        // This path requires exactly one sort column.
        if let Some((field, desc)) = aggr.get_minmax_desc() {
            if sort_columns.len() != 1 {
                return None;
            }
            let (order_by, order_desc) = &sort_columns[0];
            // MIN/MAX path requires single group key and type support
            let (group_key, _group_key_alias) =
                aggr.group_expr().expr().iter().exactly_one().ok()?;
            let kt = group_key.data_type(&aggr.input().schema()).ok()?;
            let vt = field.data_type().clone();
            if !topk_types_supported(&kt, &vt) {
                return None;
            }
            // ensure the sort direction matches aggregate function
            if desc != *order_desc {
                return None;
            }
            // ensure the sort is on the same field as the aggregate output
            if *order_by != *field.name() {
                return None;
            }
            let new_aggr = AggregateExec::with_new_limit_options(
                aggr,
                Some(LimitOptions::new_with_order(limit, *order_desc)),
            );
            return Some(Arc::new(new_aggr));
        }

        if aggr.aggr_expr().is_empty() {
            // DISTINCT path: GROUP BY without aggregates, ordering on group key.
            // Requires exactly one sort column.
            if sort_columns.len() != 1 {
                return None;
            }
            let (order_by, order_desc) = &sort_columns[0];
            let (_group_key, group_key_alias) =
                aggr.group_expr().expr().iter().exactly_one().ok()?;
            let (group_key_expr, _) =
                aggr.group_expr().expr().iter().exactly_one().ok()?;
            let kt = group_key_expr.data_type(&aggr.input().schema()).ok()?;
            if !topk_types_supported(&kt, &kt) {
                return None;
            }
            if *order_by != *group_key_alias {
                return None;
            }
            let new_aggr = AggregateExec::with_new_limit_options(
                aggr,
                Some(LimitOptions::new_with_order(limit, *order_desc)),
            );
            return Some(Arc::new(new_aggr));
        }

        // General aggregate TopK: only applies to modes that produce final
        // results (Partial results are incomplete and can't be top-K filtered)
        match aggr.mode() {
            AggregateMode::Final
            | AggregateMode::FinalPartitioned
            | AggregateMode::Single
            | AggregateMode::SinglePartitioned => {}
            _ => return None,
        }

        // Sort columns can reference any output column (group keys or aggregates).
        // Resolve each sort column name to its index in the aggregate output schema.
        let schema = aggr.schema();
        let topk_sort_cols: Option<Vec<(usize, bool)>> = sort_columns
            .iter()
            .map(|(col_name, desc)| {
                let (col_idx, _) = schema.column_with_name(col_name)?;
                Some((col_idx, *desc))
            })
            .collect();
        let topk_sort_cols = topk_sort_cols?;

        let new_aggr = AggregateExec::with_new_limit_options(
            aggr,
            Some(LimitOptions::new_with_topk_emit(limit, topk_sort_cols)),
        );
        Some(Arc::new(new_aggr))
    }

    fn transform_sort(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        let sort = plan.as_any().downcast_ref::<SortExec>()?;

        let children = sort.children();
        let child = children.into_iter().exactly_one().ok()?;
        let order = sort.properties().output_ordering()?;
        let limit = sort.fetch()?;

        // Extract all sort columns — each must be a simple Column reference
        let mut sort_col_names: Vec<(String, bool)> = Vec::new();
        for sort_expr in order.iter() {
            let col = sort_expr.expr.as_any().downcast_ref::<Column>()?;
            sort_col_names.push((col.name().to_string(), sort_expr.options.descending));
        }
        if sort_col_names.is_empty() {
            return None;
        }

        let mut cardinality_preserved = true;
        let closure = |plan: Arc<dyn ExecutionPlan>| {
            if !cardinality_preserved {
                return Ok(Transformed::no(plan));
            }
            if let Some(aggr) = plan.as_any().downcast_ref::<AggregateExec>() {
                // either we run into an Aggregate and transform it
                match Self::transform_agg(aggr, &sort_col_names, limit) {
                    None => cardinality_preserved = false,
                    Some(plan) => return Ok(Transformed::yes(plan)),
                }
            } else if let Some(proj) = plan.as_any().downcast_ref::<ProjectionExec>() {
                // track renames due to successive projections — for all sort columns
                for proj_expr in proj.expr() {
                    let Some(src_col) = proj_expr.expr.as_any().downcast_ref::<Column>()
                    else {
                        continue;
                    };
                    for (col_name, _) in sort_col_names.iter_mut() {
                        if *col_name == proj_expr.alias {
                            *col_name = src_col.name().to_string();
                        }
                    }
                }
            } else {
                // or we continue down through types that don't reduce cardinality
                match plan.cardinality_effect() {
                    CardinalityEffect::Equal | CardinalityEffect::GreaterEqual => {}
                    CardinalityEffect::Unknown | CardinalityEffect::LowerEqual => {
                        cardinality_preserved = false;
                    }
                }
            }
            Ok(Transformed::no(plan))
        };
        let child = Arc::clone(child).transform_down(closure).data().ok()?;

        // If the child now satisfies the sort ordering (because the aggregate
        // declares sorted output via topk_emit), eliminate the SortExec and
        // replace it with a GlobalLimitExec.
        // Note: ordering is only declared for Single/Final modes (not
        // FinalPartitioned) to avoid EnforceSorting replacing
        // SortPreservingMergeExec with CoalescePartitionsExec.
        if !sort.preserve_partitioning()
            && child
                .equivalence_properties()
                .ordering_satisfy(sort.expr().clone())
                .unwrap_or(false)
        {
            return Some(Arc::new(GlobalLimitExec::new(child, 0, Some(limit))));
        }

        let sort = SortExec::new(sort.expr().clone(), child)
            .with_fetch(sort.fetch())
            .with_preserve_partitioning(sort.preserve_partitioning());
        Some(Arc::new(sort))
    }
}

impl Default for TopKAggregation {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for TopKAggregation {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if config.optimizer.enable_topk_aggregation {
            plan.transform_down(|plan| {
                Ok(if let Some(plan) = TopKAggregation::transform_sort(&plan) {
                    Transformed::yes(plan)
                } else {
                    Transformed::no(plan)
                })
            })
            .data()
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "LimitAggregation"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// see `aggregate.slt` for tests
