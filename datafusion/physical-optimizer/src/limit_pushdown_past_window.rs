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
        *self = Self::default();
    }

    /// Start a chain from a limit we just found, abandoning whatever chain we
    /// were walking: that one never found a sort to apply to, and this limit is
    /// the one that governs everything below here.
    pub fn reset_limit(&mut self, limit: Option<usize>) {
        self.limit = limit;
        self.lookahead = 0;
        self.phase = Phase::FindOrGrow;
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

            // Grab the latest limit we see. The phase only has to hold us back at
            // a `SortPreservingMergeExec`, which is both a limit source and an
            // apply target; a plain limit node is never an apply target, so
            // reading it is always safe -- and it is more current than the chain
            // we were on, which has not found a sort to apply to.
            if (ctx.phase == Phase::FindOrGrow || !node.is::<SortPreservingMergeExec>())
                && get_limit(&node, &mut ctx)
            {
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
    // Only raise a limit we actually have. With nothing in hand there is no
    // limit to raise, and inventing one truncates a sort that has no `LIMIT`
    // above it at all.
    if let (Some(min_rows), Some(cur)) = (lookahead.min_rows, ctx.limit) {
        ctx.limit = Some(cur.max(min_rows));
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
    // Taking the state both reads the chain we were walking and forgets it.
    let TraverseState {
        limit, lookahead, ..
    } = std::mem::take(ctx);
    let Some(fetch) = limit else {
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
    // Carry on past this sort rather than stopping. `ctx` is back to its default
    // above, which is exactly what it held at the start of the traversal, so
    // walking the subtree below is equivalent to running the rule on it afresh
    // -- and that subtree may hold another independent limit over another window.
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
    use arrow::datatypes::{DataType, Field, FieldRef, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::WindowFrame;
    use datafusion_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
    use datafusion_expr::{
        LimitEffect, PartitionEvaluator, Signature, WindowFrameBound, WindowFrameUnits,
        WindowUDF, WindowUDFImpl,
    };
    use datafusion_functions_window::row_number::{RowNumber, row_number_udwf};
    use datafusion_physical_expr::PhysicalExpr;
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

    /// Wrap `input` in `SortExec -> BoundedWindowAggExec(udwf)`, both ordered by `a`.
    /// The frame ends at the current row, so the window adds no lookahead of its own
    /// and the fetch a test asserts on comes only from what the rule carried down.
    fn sort_then_window(
        udwf: &Arc<WindowUDF>,
        name: &str,
        input: Arc<dyn ExecutionPlan>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let s = input.schema();

        let ordering =
            LexOrdering::new(vec![PhysicalSortExpr::new_default(col("a", &s)?).asc()])
                .unwrap();

        let sort: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(ordering.clone(), input).with_preserve_partitioning(true),
        );

        let window_expr = Arc::new(StandardWindowExpr::new(
            create_udwf_window_expr(udwf, &[], &s, name.to_string(), false)?,
            &[],
            ordering.as_ref(),
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            )),
        ));

        Ok(Arc::new(BoundedWindowAggExec::try_new(
            vec![window_expr],
            sort,
            InputOrderMode::Sorted,
            true,
        )?))
    }

    /// Build: LocalLimitExec or GlobalLimitExec → BoundedWindowAggExec(row_number) → SortExec
    fn build_window_plan(
        use_local_limit: bool,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(schema()));
        let window = sort_then_window(&row_number_udwf(), "row_number", input)?;

        Ok(if use_local_limit {
            Arc::new(LocalLimitExec::new(window, 100))
        } else {
            Arc::new(GlobalLimitExec::new(window, 0, Some(100)))
        })
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

    /// A `row_number` wrapper that claims it always needs at least 200 input rows.
    /// No in-tree window function returns [`LimitEffect::Absolute`], so wrapping one
    /// is the only way to reach that arm of `grow_limit`.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct AtLeast200(RowNumber);

    impl WindowUDFImpl for AtLeast200 {
        fn name(&self) -> &str {
            "at_least_200"
        }

        fn signature(&self) -> &Signature {
            self.0.signature()
        }

        fn partition_evaluator(
            &self,
            args: PartitionEvaluatorArgs,
        ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
            self.0.partition_evaluator(args)
        }

        fn field(&self, args: WindowUDFFieldArgs) -> datafusion_common::Result<FieldRef> {
            self.0.field(args)
        }

        fn limit_effect(&self, _args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect {
            LimitEffect::Absolute(200)
        }
    }

    /// Build `[GlobalLimitExec(100) →] BoundedWindowAggExec(at_least_200) → SortExec
    /// → PlaceholderRowExec`. Without the limit there is no fetch anywhere in the plan.
    fn build_absolute_plan(
        with_limit: bool,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(schema()));
        let udwf = Arc::new(WindowUDF::from(AtLeast200(RowNumber::new())));
        let window = sort_then_window(&udwf, "at_least_200", input)?;

        Ok(if with_limit {
            Arc::new(GlobalLimitExec::new(window, 0, Some(100)))
        } else {
            window
        })
    }

    /// With no limit in the plan there is nothing to raise, so the sort must stay a
    /// full sort. Turning it into a TopK here would drop rows the query asked for.
    #[test]
    fn absolute_limit_effect_does_not_invent_a_limit() {
        let plan = build_absolute_plan(false).unwrap();
        let optimized = optimize(plan);
        assert_snapshot!(plan_str(optimized.as_ref()), @r#"
        BoundedWindowAggExec: wdw=[at_least_200: Field { "at_least_200": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
          SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
            PlaceholderRowExec
        "#);
    }

    /// A limit that is in the plan is still raised to the window's floor: 100 rows
    /// are asked for, the window needs 200, so the sort fetches 200.
    #[test]
    fn absolute_limit_effect_raises_an_existing_limit() {
        let plan = build_absolute_plan(true).unwrap();
        let optimized = optimize(plan);
        assert_snapshot!(plan_str(optimized.as_ref()), @r#"
        GlobalLimitExec: skip=0, fetch=100
          BoundedWindowAggExec: wdw=[at_least_200: Field { "at_least_200": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
            SortExec: TopK(fetch=200), expr=[a@0 ASC], preserve_partitioning=[true]
              PlaceholderRowExec
        "#);
    }
}
