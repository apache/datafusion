// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! An optimizer rule that detects aggregate operations that could use a limited bucket count

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use itertools::Itertools;

/// An optimizer rule that passes a `limit` hint to aggregations if the whole result is not needed
#[derive(Debug)]
pub struct TopKAggregation {}

impl TopKAggregation {
    /// Create a new `LimitAggregation`
    pub fn new() -> Self {
        Self {}
    }

    /// Push the bound into eligible aggregates to retain fewer groups; keep the
    /// sort to enforce the final ordering and row count.
    ///
    /// ```txt
    /// Before:
    /// Sort(fetch=K)
    ///   Aggregate
    ///     input
    ///
    /// After:
    /// Sort(fetch=K)
    ///   Aggregate(TopK(k))
    ///     input
    /// ```
    fn transform_sort(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        let sort = plan.downcast_ref::<SortExec>()?;

        let children = sort.children();
        let child = children.into_iter().exactly_one().ok()?;
        let order = sort.properties().output_ordering()?;
        let order = order.iter().exactly_one().ok()?;
        let sort_options = order.options;
        let order = order.expr.downcast_ref::<Column>()?;
        // The sort column is followed by its index in the output of each node
        // down to the first aggregate: two columns can have the same name (a
        // projection can compute `-max(v) AS "max(v)"` over the aggregate's
        // `max(v)`). Below an aggregate (to its partial aggregate) the name of
        // the aggregate output identifies it.
        let mut sort_col_index = Some(order.index());
        let mut sort_col_name = order.name().to_string();
        let limit = sort.fetch()?;

        let mut cardinality_preserved = true;
        let closure = |plan: Arc<dyn ExecutionPlan>| {
            if !cardinality_preserved {
                return Ok(Transformed::no(plan));
            }
            if let Some(aggr) = plan.downcast_ref::<AggregateExec>() {
                // the column at the index must be the only one with its name
                let identified = sort_col_index.is_none_or(|i| {
                    let schema = aggr.schema();
                    schema.field(i).name() == &sort_col_name
                        && schema
                            .fields()
                            .iter()
                            .filter(|field| field.name() == &sort_col_name)
                            .count()
                            == 1
                });
                sort_col_index = None;
                match identified
                    .then(|| {
                        aggr.clone().try_optimize_topk(
                            limit,
                            &sort_col_name,
                            sort_options,
                        )
                    })
                    .flatten()
                {
                    None => cardinality_preserved = false,
                    Some(aggr) => return Ok(Transformed::yes(Arc::new(aggr.data))),
                }
            } else if let Some(proj) = plan.downcast_ref::<ProjectionExec>() {
                match sort_col_index {
                    // follow the sort column through the projection: it must
                    // be a column of the projection's input
                    Some(i) => match proj.expr()[i].expr.downcast_ref::<Column>() {
                        Some(src_col) => {
                            sort_col_index = Some(src_col.index());
                            sort_col_name = src_col.name().to_string();
                        }
                        None => cardinality_preserved = false,
                    },
                    // track renames due to successive projections
                    None => {
                        for proj_expr in proj.expr() {
                            if proj_expr.alias != sort_col_name {
                                continue;
                            }
                            match proj_expr.expr.downcast_ref::<Column>() {
                                Some(src_col) => {
                                    sort_col_name = src_col.name().to_string()
                                }
                                None => cardinality_preserved = false,
                            }
                            break;
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
                // the column keeps its index if the node passes it through
                if let Some(i) = sort_col_index {
                    let passes_through = match plan.children().as_slice() {
                        [child] => {
                            let child_schema = child.schema();
                            i < child_schema.fields().len()
                                && plan.schema().field(i) == child_schema.field(i)
                        }
                        _ => false,
                    };
                    if !passes_through {
                        cardinality_preserved = false;
                    }
                }
            }
            Ok(Transformed::no(plan))
        };
        let child = Arc::clone(child).transform_down(closure).data().ok()?;
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
