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

use crate::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::ScalarValue;
use datafusion_expr::{LimitEffect, WindowFrameBound, WindowFrameUnits};
use datafusion_physical_expr::window::{
    PlainAggregateWindowExpr, SlidingAggregateWindowExpr, StandardWindowExpr,
    StandardWindowFunctionExpr, WindowExpr,
};
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::limit::GlobalLimitExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::windows::{BoundedWindowAggExec, WindowUDFExpr};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::cmp;
use std::cmp::max;
use std::sync::Arc;

/// This rule inspects [`ExecutionPlan`]'s attempting to find fetch limits that were not pushed
/// down by `LimitPushdown` because [BoundedWindowAggExec]s were "in the way". If the window is
/// bounded by [WindowFrameUnits::Rows] then we calculate the adjustment needed to grow the limit
/// and continue pushdown.
#[derive(Default, Clone, Debug)]
pub struct LimitPushPastWindows;

impl LimitPushPastWindows {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for LimitPushPastWindows {
    fn optimize(
        &self,
        original: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if !config.optimizer.enable_window_limits {
            return Ok(original);
        }
        let mut latest_limit: Option<usize> = None;
        let mut lookahead = 0;
        let result = original.transform_down(|node| {
            // helper closure to DRY out most the early return cases
            let mut reset = |node,
                             max: &mut usize|
             -> datafusion_common::Result<
                Transformed<Arc<dyn ExecutionPlan>>,
            > {
                latest_limit = None;
                *max = 0;
                Ok(Transformed::no(node))
            };

            // traversing sides of joins will require more thought
            if node.children().len() > 1 {
                return reset(node, &mut lookahead);
            }

            // grab the latest limit we see
            if let Some(limit) = node.as_any().downcast_ref::<GlobalLimitExec>() {
                latest_limit = limit.fetch().map(|fetch| fetch + limit.skip());
                lookahead = 0;
                return Ok(Transformed::no(node));
            }
            if let Some(limit) = node.as_any().downcast_ref::<SortPreservingMergeExec>() {
                latest_limit = limit.fetch();
                lookahead = 0;
                return Ok(Transformed::no(node));
            }

            // grow the limit if we hit a window function
            if let Some(window) = node.as_any().downcast_ref::<BoundedWindowAggExec>() {
                let mut max_rel = 0;
                let mut max_abs = 0;
                for expr in window.window_expr().iter() {
                    // grow based on function requirements
                    match grow_limit(expr) {
                        LimitEffect::None => {}
                        LimitEffect::Unknown => return reset(node, &mut lookahead),
                        LimitEffect::Relative(rel) => max_rel = max_rel.max(rel),
                        LimitEffect::Absolute(abs) => max_abs = max_abs.max(abs),
                    }

                    // grow based on frames
                    let frame = expr.get_window_frame();
                    if frame.units != WindowFrameUnits::Rows {
                        return reset(node, &mut lookahead); // expression-based limits?
                    }
                    let Some(end_bound) = bound_to_usize(&frame.end_bound) else {
                        return reset(node, &mut lookahead);
                    };
                    lookahead = max(end_bound, lookahead);
                }

                // finish grow
                lookahead = lookahead.max(lookahead + max_rel);
                lookahead = lookahead.max(max_abs);
                return Ok(Transformed::no(node));
            }

            // Apply the limit if we hit a sortpreservingmerge node
            if let Some(spm) = node.as_any().downcast_ref::<SortPreservingMergeExec>() {
                let latest = latest_limit.take();
                let Some(fetch) = latest else {
                    lookahead = 0;
                    return Ok(Transformed::no(node));
                };
                let fetch = match spm.fetch() {
                    None => fetch + lookahead,
                    Some(existing) => cmp::min(existing, fetch + lookahead),
                };
                let spm: Arc<dyn ExecutionPlan> = spm.with_fetch(Some(fetch)).unwrap();
                lookahead = 0;
                return Ok(Transformed::complete(spm));
            }

            // Apply the limit if we hit a sort node
            if let Some(sort) = node.as_any().downcast_ref::<SortExec>() {
                let latest = latest_limit.take();
                let Some(fetch) = latest else {
                    lookahead = 0;
                    return Ok(Transformed::no(node));
                };
                let fetch = match sort.fetch() {
                    None => fetch + lookahead,
                    Some(existing) => cmp::min(existing, fetch + lookahead),
                };
                let sort: Arc<dyn ExecutionPlan> = Arc::new(sort.with_fetch(Some(fetch)));
                lookahead = 0;
                return Ok(Transformed::complete(sort));
            }

            // nodes along the way
            if !node.supports_limit_pushdown() {
                // TODO: avoid combinatorial explosion of rules*nodes
                // instead of white listing rules (supports push_down rule)
                // use properties like CardinalityEffect, or LimitEffect
                return reset(node, &mut lookahead);
            }
            if let Some(part) = node.as_any().downcast_ref::<RepartitionExec>() {
                let output = part.partitioning().partition_count();
                let input = part.input().output_partitioning().partition_count();
                if output < input {
                    return reset(node, &mut lookahead);
                }
            }
            match node.cardinality_effect() {
                CardinalityEffect::Unknown => return reset(node, &mut lookahead),
                CardinalityEffect::LowerEqual => return reset(node, &mut lookahead),
                CardinalityEffect::Equal => {}
                CardinalityEffect::GreaterEqual => {}
            }

            Ok(Transformed::no(node))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "LimitPushPastWindows"
    }

    fn schema_check(&self) -> bool {
        false // we don't change the schema
    }
}

/// Examines the `WindowExpr` and decides:
/// 1. The expression does not change the window size
/// 2. The expression grows it by X amount
/// 3. We don't know
///
/// # Arguments
///
/// * `expr` the expression to examine
///
/// # Returns
///
/// `false` if we can't optimize
fn grow_limit(expr: &Arc<dyn WindowExpr>) -> LimitEffect {
    // White list aggregates
    if expr.as_any().is::<PlainAggregateWindowExpr>()
        || expr.as_any().is::<SlidingAggregateWindowExpr>()
    {
        return LimitEffect::None;
    }

    // Grab the window function
    let Some(swe) = expr.as_any().downcast_ref::<StandardWindowExpr>() else {
        return LimitEffect::Unknown; // should be only remaining type
    };
    let swfe = swe.get_standard_func_expr();
    let Some(udf) = swfe.as_any().downcast_ref::<WindowUDFExpr>() else {
        return LimitEffect::Unknown; // should be only remaining type
    };

    // Return its effect
    udf.limit_effect()
}

fn bound_to_usize(bound: &WindowFrameBound) -> Option<usize> {
    match bound {
        WindowFrameBound::Preceding(_) => Some(0),
        WindowFrameBound::CurrentRow => Some(0),
        WindowFrameBound::Following(ScalarValue::UInt64(Some(scalar))) => {
            Some(*scalar as usize)
        }
        _ => None,
    }
}
