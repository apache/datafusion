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

use crate::{OptimizerContext, PhysicalOptimizerRule};
use arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::Result;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::ExecutionPlan;
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
        order_by: &str,
        order_desc: bool,
        limit: usize,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        // ensure the sort direction matches aggregate function
        let (field, desc) = aggr.get_minmax_desc()?;
        if desc != order_desc {
            return None;
        }
        let group_key = aggr.group_expr().expr().iter().exactly_one().ok()?;
        let kt = group_key.0.data_type(&aggr.input().schema()).ok()?;
        if !kt.is_primitive()
            && kt != DataType::Utf8
            && kt != DataType::Utf8View
            && kt != DataType::LargeUtf8
        {
            return None;
        }
        if aggr.filter_expr().iter().any(|e| e.is_some()) {
            return None;
        }

        // ensure the sort is on the same field as the aggregate output
        if order_by != field.name() {
            return None;
        }

        // We found what we want: clone, copy the limit down, and return modified node
        let new_aggr = AggregateExec::try_new(
            *aggr.mode(),
            aggr.group_expr().clone(),
            aggr.aggr_expr().to_vec(),
            aggr.filter_expr().to_vec(),
            Arc::clone(aggr.input()),
            aggr.input_schema(),
        )
        .expect("Unable to copy Aggregate!")
        .with_limit(Some(limit));
        Some(Arc::new(new_aggr))
    }

    fn transform_sort(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        let sort = plan.as_any().downcast_ref::<SortExec>()?;

        let children = sort.children();
        let child = children.into_iter().exactly_one().ok()?;
        let order = sort.properties().output_ordering()?;
        let order = order.iter().exactly_one().ok()?;
        let order_desc = order.options.descending;
        let order = order.expr.as_any().downcast_ref::<Column>()?;
        let mut cur_col_name = order.name().to_string();
        let limit = sort.fetch()?;

        let mut cardinality_preserved = true;
        let closure = |plan: Arc<dyn ExecutionPlan>| {
            if !cardinality_preserved {
                return Ok(Transformed::no(plan));
            }
            if let Some(aggr) = plan.as_any().downcast_ref::<AggregateExec>() {
                // either we run into an Aggregate and transform it
                match Self::transform_agg(aggr, &cur_col_name, order_desc, limit) {
                    None => cardinality_preserved = false,
                    Some(plan) => return Ok(Transformed::yes(plan)),
                }
            } else if let Some(proj) = plan.as_any().downcast_ref::<ProjectionExec>() {
                // track renames due to successive projections
                for proj_expr in proj.expr() {
                    let Some(src_col) = proj_expr.expr.as_any().downcast_ref::<Column>()
                    else {
                        continue;
                    };
                    if proj_expr.alias == cur_col_name {
                        cur_col_name = src_col.name().to_string();
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
    fn optimize_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &OptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = context.session_config().options();
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
