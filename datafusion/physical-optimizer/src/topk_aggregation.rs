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
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, LimitOptions, TopKCountSharedState, TopKKind,
};
use datafusion_physical_plan::aggregates::{is_topk_count_pattern, topk_types_supported};
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

    fn transform_agg(
        aggr: &AggregateExec,
        order_by: &str,
        order_desc: bool,
        limit: usize,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        if aggr.filter_expr().iter().any(|e| e.is_some()) {
            return None;
        }

        // MIN/MAX / distinct paths only support a single group key (their
        // stream, `GroupedTopKAggregateStream`, is single-column). The
        // count path uses `GroupValues` which handles any group-column
        // combination, so `single_group` is checked only for the first two
        // shapes and skipped for shape #3.
        let single_group = aggr.group_expr().expr().iter().exactly_one().ok();

        // Three supported shapes:
        //   1. MIN/MAX single-aggregate: ORDER BY MIN(x) / MAX(x)
        //   2. GROUP-BY-only distinct-like: no aggregates, ORDER BY the group key
        //   3. count(*)/count(col): ORDER BY count(...)  (Final/Single mode only)
        //      — supports single OR multi-column GROUP BY
        let limit_options = if let Some((field, desc)) = aggr.get_minmax_desc() {
            let (group_key, _) = single_group?;
            let kt = group_key.data_type(&aggr.input().schema()).ok()?;
            let vt = field.data_type().clone();
            if !topk_types_supported(&kt, &vt) {
                return None;
            }
            if desc != order_desc {
                return None;
            }
            if order_by != field.name() {
                return None;
            }
            LimitOptions::new_with_order(limit, order_desc)
        } else if aggr.aggr_expr().is_empty() {
            let (group_key, group_key_alias) = single_group?;
            let kt = group_key.data_type(&aggr.input().schema()).ok()?;
            if !topk_types_supported(&kt, &kt) {
                return None;
            }
            if order_by != group_key_alias {
                return None;
            }
            LimitOptions::new_with_order(limit, order_desc)
        } else if let Some(count_field) = is_topk_count_pattern(aggr)
            && matches!(
                aggr.mode(),
                AggregateMode::Final
                    | AggregateMode::FinalPartitioned
                    | AggregateMode::Single
                    | AggregateMode::SinglePartitioned
            )
            && order_by == count_field.name()
        {
            LimitOptions::new_count(limit, order_desc)
        } else {
            return None;
        };

        // We found what we want: clone, copy the limit down, and return modified node
        let new_aggr = AggregateExec::with_new_limit_options(aggr, Some(limit_options));

        // Partial-side CA temporarily disabled for correctness
        // investigation (see PR discussion). Only mark the Final
        // aggregate; the standard hash path handles Partial.
        Some(Arc::new(new_aggr))
    }

    fn transform_sort(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        let sort = plan.downcast_ref::<SortExec>()?;

        let children = sort.children();
        let child = children.into_iter().exactly_one().ok()?;
        let order = sort.properties().output_ordering()?;
        let order = order.iter().exactly_one().ok()?;
        let order_desc = order.options.descending;
        let order = order.expr.downcast_ref::<Column>()?;
        let mut cur_col_name = order.name().to_string();
        let limit = sort.fetch()?;

        let mut cardinality_preserved = true;
        let closure = |plan: Arc<dyn ExecutionPlan>| {
            if !cardinality_preserved {
                return Ok(Transformed::no(plan));
            }
            if let Some(aggr) = plan.downcast_ref::<AggregateExec>() {
                // either we run into an Aggregate and transform it
                match Self::transform_agg(aggr, &cur_col_name, order_desc, limit) {
                    None => cardinality_preserved = false,
                    Some(plan) => return Ok(Transformed::yes(plan)),
                }
            } else if let Some(proj) = plan.downcast_ref::<ProjectionExec>() {
                // track renames due to successive projections
                for proj_expr in proj.expr() {
                    let Some(src_col) = proj_expr.expr.downcast_ref::<Column>() else {
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

/// Walk `input` down through `RepartitionExec` / `CoalesceBatchesExec`
/// (or any node that preserves cardinality of a single input) and
/// re-attach the matching Partial `AggregateExec` with the same
/// `shared` state and a `Partial`-mode `LimitOptions`. Returns
/// `Some(rewritten_input)` if a Partial was found and rewritten, else
/// `None` (in which case the caller keeps the original input untouched).
///
/// Kept intentionally small: only rewrite the FIRST Partial we find on
/// the direct child chain. `Final ← Coalesce ← Repartition ← Partial`
/// is the canonical plan shape produced by the physical planner for
/// this aggregate pattern.
fn mark_upstream_partial(
    input: &Arc<dyn ExecutionPlan>,
    shared: &Arc<TopKCountSharedState>,
    final_limit_options: LimitOptions,
) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(partial_agg) = input.downcast_ref::<AggregateExec>()
        && matches!(partial_agg.mode(), AggregateMode::Partial)
    {
        let partial_options = LimitOptions::new_count(final_limit_options.limit(), true);
        let rewritten =
            partial_agg.with_count_topk_shared(Some(partial_options), Arc::clone(shared));
        return Some(Arc::new(rewritten));
    }

    // Otherwise, walk into the single-child chain (Coalesce, Repartition,
    // Projection, Filter, ...). Bail on multi-child nodes and joins.
    let children = input.children();
    if children.len() != 1 {
        return None;
    }
    let child = children[0];
    let rewritten_child = mark_upstream_partial(child, shared, final_limit_options)?;
    input.clone().with_new_children(vec![rewritten_child]).ok()
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
