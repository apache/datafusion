// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::PhysicalOptimizerRule;
use crate::row_bound::RowBound;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{ExecutionPlan, LimitBehavior, WindowLookahead};
use std::sync::Arc;

/// This rule inspects [`ExecutionPlan`]'s attempting to find fetch limits that were not pushed
/// down by `LimitPushdown` because windows ([`LimitBehavior::Window`]) were "in the way". If a
/// window's lookahead is known, the limit grows by it and pushdown continues.
#[derive(Default, Clone, Debug)]
pub struct LimitPushPastWindows;

impl LimitPushPastWindows {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Eq, PartialEq, Default)]
enum Phase {
    #[default]
    FindOrGrow,
    Apply,
}

#[derive(Default)]
struct TraverseState {
    pub limit: Option<usize>,
    pub lookahead: usize,
    pub phase: Phase,
}

impl TraverseState {
    /// Forget the chain we were walking: no limit in hand, nothing to grow by,
    /// and back to looking for the next limit.
    pub fn reset(&mut self) {
        self.limit = None;
        self.lookahead = 0;
        self.phase = Phase::FindOrGrow;
    }

    /// Start a chain from a limit we just found. The phase is deliberately left
    /// alone: this only runs while we are still in [`Phase::FindOrGrow`].
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
        let result = original.transform_down(|node| {
            // helper closure to DRY out most the early return cases
            let reset = |node,
                         ctx: &mut TraverseState|
             -> datafusion_common::Result<
                Transformed<Arc<dyn ExecutionPlan>>,
            > {
                ctx.reset();
                Ok(Transformed::no(node))
            };

            // traversing sides of joins will require more thought
            if node.children().len() > 1 {
                return reset(node, &mut ctx);
            }

            // grab the latest limit we see
            if ctx.phase == Phase::FindOrGrow && get_limit(&node, &mut ctx) {
                return Ok(Transformed::no(node));
            }

            // grow the limit if we hit a window function
            if let LimitBehavior::Window(lookahead) = node.limit_behavior() {
                ctx.phase = Phase::Apply;
                if !grow_limit(lookahead, &mut ctx) {
                    return reset(node, &mut ctx);
                }
                return Ok(Transformed::no(node));
            }

            // Apply the limit if we hit a sortpreservingmerge node
            if ctx.phase == Phase::Apply
                && let Some(out) = apply_limit(&node, &mut ctx)
            {
                return Ok(out);
            }

            // nodes along the way
            match node.limit_behavior() {
                LimitBehavior::CopyToChildren | LimitBehavior::MoveToInput => {}
                LimitBehavior::Merge if node.fetch().is_none() => {}
                LimitBehavior::Stops
                | LimitBehavior::Limit { .. }
                | LimitBehavior::Merge
                | LimitBehavior::Window(_) => return reset(node, &mut ctx),
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

fn grow_limit(lookahead: Option<WindowLookahead>, ctx: &mut TraverseState) -> bool {
    let Some(lookahead) = lookahead else {
        return false;
    };
    if let Some(min_rows) = lookahead.min_rows {
        ctx.limit = Some(ctx.limit.unwrap_or(0).max(min_rows));
    }
    ctx.max_lookahead(lookahead.frame_end);
    // Frame bounds come straight from the query and can be as large as
    // `u64::MAX`, so saturate instead of overflowing. A saturated lookahead
    // makes the `checked_add` in `apply_limit` fail, which skips the pushdown.
    ctx.max_lookahead(ctx.lookahead.saturating_add(lookahead.relative));
    true
}

fn apply_limit(
    node: &Arc<dyn ExecutionPlan>,
    ctx: &mut TraverseState,
) -> Option<Transformed<Arc<dyn ExecutionPlan>>> {
    if !node.is::<SortExec>() && !node.is::<SortPreservingMergeExec>() {
        return None;
    }
    let latest = ctx.limit;
    let lookahead = ctx.lookahead;
    ctx.reset();
    let Some(fetch) = latest else {
        return Some(Transformed::no(Arc::clone(node)));
    };
    // A fetch that overflows can't drop any rows, so skip the pushdown rather
    // than turning the sort into a TopK that holds every row and can't spill.
    let Some(fetch) = fetch.checked_add(lookahead) else {
        return Some(Transformed::no(Arc::clone(node)));
    };
    let fetch = match node.fetch() {
        None => fetch,
        Some(existing) => existing.min(fetch),
    };
    // Carry on past this sort rather than stopping. `ctx` was reset above, so it
    // now holds exactly what it held at the start of the traversal, which makes
    // walking the subtree below equivalent to running the rule on it afresh --
    // and that subtree may hold another independent limit over another window.
    Some(Transformed::yes(node.with_fetch(Some(fetch)).unwrap()))
}

fn get_limit(node: &Arc<dyn ExecutionPlan>, ctx: &mut TraverseState) -> bool {
    // In distributed execution, GlobalLimitExec becomes LocalLimitExec
    // per partition. Handle it the same way (LocalLimitExec has no skip).
    if let Some(limit) = RowBound::of_limit_node(node) {
        ctx.reset_limit(limit.input_rows());
        return true;
    }
    if let Some(limit) = node.downcast_ref::<SortPreservingMergeExec>() {
        ctx.reset_limit(limit.fetch());
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::WindowFrame;
    use datafusion_expr::{WindowFrameBound, WindowFrameUnits};
    use datafusion_functions_window::row_number::row_number_udwf;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr::window::StandardWindowExpr;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion_physical_plan::InputOrderMode;
    use datafusion_physical_plan::displayable;
    use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
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
