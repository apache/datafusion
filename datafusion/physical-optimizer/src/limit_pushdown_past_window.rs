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
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::{LimitEffect, WindowFrameBound, WindowFrameUnits};
use datafusion_physical_expr::window::{
    PlainAggregateWindowExpr, SlidingAggregateWindowExpr, StandardWindowExpr,
    StandardWindowFunctionExpr, WindowExpr,
};
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::windows::{BoundedWindowAggExec, WindowUDFExpr};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::cmp;
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

#[derive(Eq, PartialEq)]
enum Phase {
    FindOrGrow,
    Apply,
}

#[derive(Default)]
struct TraverseState {
    pub limit: Option<usize>,
    pub lookahead: usize,
}

impl TraverseState {
    pub fn reset_limit(&mut self, limit: Option<usize>) {
        self.limit = limit;
        self.lookahead = 0;
    }

    pub fn max_lookahead(&mut self, new_val: usize) {
        self.lookahead = self.lookahead.max(new_val);
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
        let mut ctx = TraverseState::default();
        let mut phase = Phase::FindOrGrow;
        let result = original.transform_down(|node| {
            // helper closure to DRY out most the early return cases
            let reset = |node,
                         ctx: &mut TraverseState|
             -> datafusion_common::Result<
                Transformed<Arc<dyn ExecutionPlan>>,
            > {
                ctx.limit = None;
                ctx.lookahead = 0;
                Ok(Transformed::no(node))
            };

            // traversing sides of joins will require more thought
            if node.children().len() > 1 {
                return reset(node, &mut ctx);
            }

            // grab the latest limit we see
            if phase == Phase::FindOrGrow && get_limit(&node, &mut ctx) {
                return Ok(Transformed::no(node));
            }

            // grow the limit if we hit a window function
            if let Some(window) = node.downcast_ref::<BoundedWindowAggExec>() {
                phase = Phase::Apply;
                if !grow_limit(window, &mut ctx) {
                    return reset(node, &mut ctx);
                }
                return Ok(Transformed::no(node));
            }

            // Apply the limit if we hit a sortpreservingmerge node
            if phase == Phase::Apply
                && let Some(out) = apply_limit(&node, &mut ctx)
            {
                return Ok(out);
            }

            // nodes along the way
            if !node.supports_limit_pushdown() {
                return reset(node, &mut ctx);
            }
            if let Some(part) = node.downcast_ref::<RepartitionExec>() {
                let output = part.partitioning().partition_count();
                let input = part.input().output_partitioning().partition_count();
                if output < input {
                    return reset(node, &mut ctx);
                }
            }
            match node.cardinality_effect() {
                CardinalityEffect::Unknown => return reset(node, &mut ctx),
                CardinalityEffect::LowerEqual => return reset(node, &mut ctx),
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

fn grow_limit(window: &BoundedWindowAggExec, ctx: &mut TraverseState) -> bool {
    let mut max_rel = 0;
    for expr in window.window_expr().iter() {
        // grow based on function requirements
        match get_limit_effect(expr) {
            LimitEffect::None => {}
            LimitEffect::Unknown => return false,
            LimitEffect::Relative(rel) => max_rel = max_rel.max(rel),
            LimitEffect::Absolute(val) => {
                let cur = ctx.limit.unwrap_or(0);
                ctx.limit = Some(cur.max(val))
            }
        }

        // grow based on frames
        let frame = expr.get_window_frame();
        if frame.units != WindowFrameUnits::Rows {
            return false; // expression-based limits not statically evaluatable
        }
        let Some(end_bound) = bound_to_usize(&frame.end_bound) else {
            return false; // can't optimize unbounded window expressions
        };
        ctx.max_lookahead(end_bound);
    }

    // finish grow
    ctx.max_lookahead(ctx.lookahead + max_rel);
    true
}

fn apply_limit(
    node: &Arc<dyn ExecutionPlan>,
    ctx: &mut TraverseState,
) -> Option<Transformed<Arc<dyn ExecutionPlan>>> {
    if !node.is::<SortExec>() && !node.is::<SortPreservingMergeExec>() {
        return None;
    }
    let latest = ctx.limit.take();
    let Some(fetch) = latest else {
        ctx.limit = None;
        ctx.lookahead = 0;
        return Some(Transformed::no(Arc::clone(node)));
    };
    let fetch = match node.fetch() {
        None => fetch + ctx.lookahead,
        Some(existing) => cmp::min(existing, fetch + ctx.lookahead),
    };
    Some(Transformed::complete(node.with_fetch(Some(fetch)).unwrap()))
}

fn get_limit(node: &Arc<dyn ExecutionPlan>, ctx: &mut TraverseState) -> bool {
    if let Some(limit) = node.downcast_ref::<GlobalLimitExec>() {
        ctx.reset_limit(limit.fetch().map(|fetch| fetch + limit.skip()));
        return true;
    }
    // In distributed execution, GlobalLimitExec becomes LocalLimitExec
    // per partition. Handle it the same way (LocalLimitExec has no skip).
    if let Some(limit) = node.downcast_ref::<LocalLimitExec>() {
        ctx.reset_limit(Some(limit.fetch()));
        return true;
    }
    if let Some(limit) = node.downcast_ref::<SortPreservingMergeExec>() {
        ctx.reset_limit(limit.fetch());
        return true;
    }
    false
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
/// The effect on the limit
fn get_limit_effect(expr: &Arc<dyn WindowExpr>) -> LimitEffect {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::WindowFrame;
    use datafusion_functions_window::row_number::row_number_udwf;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion_physical_plan::InputOrderMode;
    use datafusion_physical_plan::displayable;
    use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
    use datafusion_physical_plan::windows::{
        BoundedWindowAggExec, create_udwf_window_expr,
    };
    use insta::assert_snapshot;

    fn plan_str(plan: &dyn ExecutionPlan) -> String {
        displayable(plan).indent(true).to_string()
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
    }

    /// Build: LocalLimitExec or GlobalLimitExec → BoundedWindowAggExec(row_number) → SortExec
    fn build_window_plan(
        use_local_limit: bool,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let s = schema();
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(PlaceholderRowExec::new(Arc::clone(&s)));

        let ordering =
            LexOrdering::new(vec![PhysicalSortExpr::new_default(col("a", &s)?).asc()])
                .unwrap();

        let sort: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(ordering.clone(), input).with_preserve_partitioning(true),
        );

        let window_expr = Arc::new(StandardWindowExpr::new(
            create_udwf_window_expr(
                &row_number_udwf(),
                &[],
                &s,
                "row_number".to_string(),
                false,
            )?,
            &[],
            ordering.as_ref(),
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            )),
        ));

        let window: Arc<dyn ExecutionPlan> = Arc::new(BoundedWindowAggExec::try_new(
            vec![window_expr],
            sort,
            InputOrderMode::Sorted,
            true,
        )?);

        let limit: Arc<dyn ExecutionPlan> = if use_local_limit {
            Arc::new(LocalLimitExec::new(window, 100))
        } else {
            Arc::new(GlobalLimitExec::new(window, 0, Some(100)))
        };

        Ok(limit)
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let mut config = ConfigOptions::new();
        config.optimizer.enable_window_limits = true;
        LimitPushPastWindows::new().optimize(plan, &config).unwrap()
    }

    /// GlobalLimitExec above a windowed sort should push fetch into the SortExec.
    #[test]
    fn global_limit_pushes_past_window() {
        let plan = build_window_plan(false).unwrap();
        let optimized = optimize(plan);
        assert_snapshot!(plan_str(optimized.as_ref()), @r#"
        GlobalLimitExec: skip=0, fetch=100
          BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
            SortExec: TopK(fetch=100), expr=[a@0 ASC], preserve_partitioning=[true]
              PlaceholderRowExec
        "#);
    }

    /// LocalLimitExec above a windowed sort should also push fetch into the SortExec.
    /// This is the case in distributed execution where GlobalLimitExec becomes LocalLimitExec.
    #[test]
    fn local_limit_pushes_past_window() {
        let plan = build_window_plan(true).unwrap();
        let optimized = optimize(plan);
        assert_snapshot!(plan_str(optimized.as_ref()), @r#"
        LocalLimitExec: fetch=100
          BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
            SortExec: TopK(fetch=100), expr=[a@0 ASC], preserve_partitioning=[true]
              PlaceholderRowExec
        "#);
    }
}
