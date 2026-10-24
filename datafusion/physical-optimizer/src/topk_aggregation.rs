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
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::materialized_cte::MaterializedCteExec;
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
        let mut sort_col = order.clone();
        let limit = sort.fetch()?;

        let mut cardinality_preserved = true;
        let closure = |plan: Arc<dyn ExecutionPlan>| {
            if !cardinality_preserved {
                return Ok(Transformed::no(plan));
            }
            if let Some(aggr) = plan.downcast_ref::<AggregateExec>() {
                match aggr
                    .clone()
                    .try_optimize_topk(limit, sort_col.name(), sort_options)
                {
                    None => cardinality_preserved = false,
                    Some(aggr) => return Ok(Transformed::yes(Arc::new(aggr.data))),
                }
            } else if plan.downcast_ref::<MaterializedCteExec>().is_some() {
                // Its body produces rows; the output schema comes from the continuation.
                cardinality_preserved = false;
            } else if let Some(proj) = plan.downcast_ref::<ProjectionExec>() {
                // Only follow direct columns: computed expressions can change group ranking.
                let src_col = proj
                    .expr()
                    .get(sort_col.index())
                    .and_then(|proj_expr| proj_expr.expr.downcast_ref::<Column>());
                if let Some(src_col) = src_col {
                    sort_col = src_col.clone();
                } else {
                    cardinality_preserved = false;
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
