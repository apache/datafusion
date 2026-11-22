// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::PhysicalOptimizerRule;
use crate::row_bound::RowBound;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::windows::BoundedWindowAggExec;
use datafusion_physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, LimitBehavior, Partitioning, WindowLookahead,
};
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
    /// The ordering of the merge the limit came from, in the expression space of
    /// the node being visited.
    pub merge_exprs: Option<Vec<Arc<dyn PhysicalExpr>>>,
    /// Whether it separates the groups of every window crossed so far.
    pub partitions_separated: bool,
    /// The narrowest `PARTITION BY` over the windows crossed so far.
    pub narrowest_partition_by: usize,
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
    pub fn reset_limit(
        &mut self,
        limit: Option<usize>,
        merge_exprs: Option<Vec<Arc<dyn PhysicalExpr>>>,
    ) {
        self.limit = limit;
        self.lookahead = 0;
        self.phase = Phase::FindOrGrow;
        self.partitions_separated = merge_exprs.is_some();
        self.merge_exprs = merge_exprs;
        self.narrowest_partition_by = usize::MAX;
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
                ctx.partitions_separated &= separates_partitions(&node, &ctx);
                ctx.narrowest_partition_by =
                    ctx.narrowest_partition_by.min(partition_by_width(&node));
                carry_down(&node, &mut ctx);
                return Ok(Transformed::no(node));
            }

            // Apply the limit if we hit a sortpreservingmerge node
            if ctx.phase == Phase::Apply
                && let Some(out) = apply_limit(&node, &mut ctx)
            {
                return Ok(out);
            }

            // The ordering only holds further down while the nodes in between
            // keep their input's order.
            if node.output_ordering().is_none() {
                ctx.merge_exprs = None;
                ctx.partitions_separated = false;
            }
            carry_down(&node, &mut ctx);

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
    // A window reads ahead of every row the windows above it need, so stacked
    // windows add their lookaheads. Expressions within one window share theirs,
    // which `WindowLookahead` has already taken the maximum over.
    //
    // Frame bounds come straight from the query and can be as large as
    // `u64::MAX`, so saturate instead of overflowing. A saturated lookahead
    // makes the `checked_add` in `apply_limit` fail, which skips the pushdown.
    ctx.lookahead = ctx
        .lookahead
        .saturating_add(lookahead.frame_end)
        .saturating_add(lookahead.relative);
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
        limit,
        lookahead,
        partitions_separated,
        narrowest_partition_by,
        ..
    } = std::mem::take(ctx);
    // A fetch that overflows can't drop any rows, so skip the pushdown rather
    // than turning the sort into a TopK that holds every row and can't spill.
    let fetch = limit
        .and_then(|limit| limit.checked_add(lookahead))
        .map(|fetch| node.fetch().map_or(fetch, |existing| existing.min(fetch)));
    // A `SortPreservingMergeExec` emits its input's first rows in order, so its
    // fetch bounds every partition below it too: start the next chain from it.
    // This sits before the early return below on purpose -- with no limit in hand
    // the merge is left alone, but its own fetch is still a bound to carry down.
    if let Some(merge) = node.downcast_ref::<SortPreservingMergeExec>() {
        ctx.reset_limit(fetch.or_else(|| node.fetch()), Some(merge_ordering(merge)));
    }
    let Some(fetch) = fetch else {
        return Some(Transformed::no(Arc::clone(node)));
    };
    // A node with more than one output partition keeps the globally first
    // `fetch` rows, not the first rows of each partition the window reads. An
    // ordered merge above the window makes it a bound again: a row the window
    // got wrong lost to `fetch` rows that all reach the merge before it.
    if node.output_partitioning().partition_count() > 1
        && !(partitions_separated && keeps_partitions_whole(node, narrowest_partition_by))
    {
        return Some(Transformed::no(Arc::clone(node)));
    }
    // Carry on past this sort rather than stopping: the subtree below may hold
    // another independent limit over another window.
    Some(Transformed::yes(node.with_fetch(Some(fetch)).unwrap()))
}

fn get_limit(node: &Arc<dyn ExecutionPlan>, ctx: &mut TraverseState) -> bool {
    // In distributed execution, GlobalLimitExec becomes LocalLimitExec
    // per partition. Handle it the same way (LocalLimitExec has no skip).
    if let Some(limit) = RowBound::of_limit_node(node) {
        ctx.reset_limit(limit.input_rows(), None);
        return true;
    }
    if let Some(merge) = node.downcast_ref::<SortPreservingMergeExec>() {
        ctx.reset_limit(merge.fetch(), Some(merge_ordering(merge)));
        return true;
    }
    false
}

/// Whether every window partition `node` reads is whole within one of its
/// output partitions, so that a row's own earlier rows share its partition.
fn keeps_partitions_whole(node: &Arc<dyn ExecutionPlan>, rank_needed: usize) -> bool {
    let Partitioning::Hash(hash_exprs, _) = node.output_partitioning() else {
        return false;
    };
    node.downcast_ref::<SortExec>().is_some_and(|sort| {
        hash_exprs.len() <= rank_needed
            && hash_exprs
                .iter()
                .zip(sort.expr().iter())
                .all(|(hashed, sorted)| hashed.eq(&sorted.expr))
    })
}

/// The expressions of `merge`'s ordering that order its rows: one that is
/// constant in its input orders nothing.
fn merge_ordering(merge: &SortPreservingMergeExec) -> Vec<Arc<dyn PhysicalExpr>> {
    let input = merge.input().equivalence_properties();
    merge
        .expr()
        .iter()
        .filter(|sort_expr| input.is_expr_constant(&sort_expr.expr).is_none())
        .map(|sort_expr| Arc::clone(&sort_expr.expr))
        .collect()
}

/// Whether the ordering in hand sorts `node`'s rows into the groups its windows
/// read: every `PARTITION BY` expression has to be one of the ordering's own. A
/// window with no `PARTITION BY` reads one group that no ordering separates.
fn separates_partitions(node: &Arc<dyn ExecutionPlan>, ctx: &TraverseState) -> bool {
    let (Some(exprs), Some(window)) = (
        ctx.merge_exprs.as_ref(),
        node.downcast_ref::<BoundedWindowAggExec>(),
    ) else {
        return false;
    };
    !window.window_expr().is_empty()
        && window.window_expr().iter().all(|window_expr| {
            !window_expr.partition_by().is_empty()
                && window_expr
                    .partition_by()
                    .iter()
                    .all(|key| exprs.iter().any(|expr| expr.eq(key)))
        })
}

/// The narrowest `PARTITION BY` of `node`'s window expressions, or `usize::MAX`
/// for anything else -- [`separates_partitions`] blocks those on its own.
fn partition_by_width(node: &Arc<dyn ExecutionPlan>) -> usize {
    let Some(window) = node.downcast_ref::<BoundedWindowAggExec>() else {
        return usize::MAX;
    };
    window
        .window_expr()
        .iter()
        .map(|expr| expr.partition_by().len())
        .min()
        .unwrap_or(usize::MAX)
}

/// Carry the merge's ordering from `node`'s output into its input's expression
/// space: a projection moves and renames columns, so the ordering is rewritten
/// through it, and a node that changes the schema any other way ends the carry.
fn carry_down(node: &Arc<dyn ExecutionPlan>, ctx: &mut TraverseState) {
    let Some(exprs) = ctx.merge_exprs.take() else {
        return;
    };
    let Some(child) = node.children().into_iter().next() else {
        return;
    };
    ctx.merge_exprs = if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
        exprs
            .iter()
            .map(|expr| projection.projection_expr().unproject_expr(expr).ok())
            .collect()
    } else if node.schema() == child.schema() {
        Some(exprs)
    } else {
        None
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, FieldRef, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_expr::WindowFrame;
    use datafusion_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
    use datafusion_expr::{
        LimitEffect, PartitionEvaluator, Signature, WindowFrameBound, WindowFrameUnits,
        WindowUDF, WindowUDFImpl,
    };
    use datafusion_functions_window::row_number::{RowNumber, row_number_udwf};
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, col, lit};
    use datafusion_physical_expr::window::StandardWindowExpr;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion_physical_plan::InputOrderMode;
    use datafusion_physical_plan::Partitioning;
    use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion_physical_plan::displayable;
    use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
    use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
    use datafusion_physical_plan::projection::ProjectionExec;
    use datafusion_physical_plan::repartition::RepartitionExec;
    use datafusion_physical_plan::windows::{
        BoundedWindowAggExec, create_udwf_window_expr,
    };
    use insta::assert_snapshot;

    fn plan_str(plan: &dyn ExecutionPlan) -> String {
        displayable(plan).indent(true).to_string()
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]))
    }

    fn ordered_by_a(
        input: &Arc<dyn ExecutionPlan>,
    ) -> datafusion_common::Result<LexOrdering> {
        let s = input.schema();
        Ok(
            LexOrdering::new(vec![PhysicalSortExpr::new_default(col("a", &s)?).asc()])
                .unwrap(),
        )
    }

    /// Wrap `input` in `SortExec -> BoundedWindowAggExec(udwf)`, both ordered by `a`.
    fn sort_then_window(
        udwf: &Arc<WindowUDF>,
        name: &str,
        input: Arc<dyn ExecutionPlan>,
        input_order_mode: InputOrderMode,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let ordering = ordered_by_a(&input)?;
        let sort: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(ordering, input).with_preserve_partitioning(true));
        window_over(udwf, name, sort, input_order_mode)
    }

    /// The frame ends at the current row, so the window adds no lookahead of its own
    /// and the fetch a test asserts on comes only from what the rule carried down.
    fn window_over(
        udwf: &Arc<WindowUDF>,
        name: &str,
        input: Arc<dyn ExecutionPlan>,
        input_order_mode: InputOrderMode,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let s = input.schema();
        let ordering = ordered_by_a(&input)?;

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
            input,
            input_order_mode,
            true,
        )?))
    }

    /// Build: LocalLimitExec or GlobalLimitExec → BoundedWindowAggExec(row_number) → SortExec
    fn build_window_plan(
        use_local_limit: bool,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(schema()));
        let window = sort_then_window(
            &row_number_udwf(),
            "row_number",
            input,
            InputOrderMode::Sorted,
        )?;

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

    /// The plan on both sides of the rule, in the shape `OptimizationTest` in
    /// the core tests prints: a test that claims the rule left the plan alone
    /// then shows that as two identical halves.
    fn both_sides(plan: Arc<dyn ExecutionPlan>) -> String {
        let input = indented(plan.as_ref());
        let output = indented(optimize(plan).as_ref());
        format!("input:\n{input}output:\n{output}")
    }

    fn indented(plan: &dyn ExecutionPlan) -> String {
        plan_str(plan)
            .lines()
            .flat_map(|line| ["  ", line, "\n"])
            .collect()
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

    /// A window whose input is not grouped by partition key reads ahead by an
    /// amount no frame bound describes, so the rule has to leave the sort below it
    /// alone. This is the plan from [`global_limit_pushes_past_window`] with only
    /// the input order mode changed.
    #[test]
    fn interleaved_partitions_block_the_pushdown() {
        let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(schema()));
        let window = sort_then_window(
            &row_number_udwf(),
            "row_number",
            input,
            InputOrderMode::Linear,
        )
        .unwrap();
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(window, 0, Some(100)));
        let optimized = optimize(plan);
        assert_snapshot!(plan_str(optimized.as_ref()), @r#"
        GlobalLimitExec: skip=0, fetch=100
          BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Linear]
            SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
              PlaceholderRowExec
        "#);
    }

    fn hash_repartition(partitions: usize) -> Arc<dyn ExecutionPlan> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(schema()));
        let s = input.schema();
        let hash = Partitioning::Hash(vec![col("a", &s).unwrap()], partitions);
        Arc::new(RepartitionExec::try_new(input, hash).unwrap())
    }

    /// The window reads each partition of its input on its own, so a fetch on a
    /// sort that keeps more than one of them does not bound what it needs.
    #[test]
    fn a_partitioned_sort_does_not_take_the_fetch() {
        let window = sort_then_window(
            &row_number_udwf(),
            "row_number",
            hash_repartition(4),
            InputOrderMode::Sorted,
        )
        .unwrap();
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(window, 0, Some(100)));
        assert_snapshot!(both_sides(plan), @r#"
        input:
          GlobalLimitExec: skip=0, fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1
                  PlaceholderRowExec
        output:
          GlobalLimitExec: skip=0, fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1
                  PlaceholderRowExec
        "#);
    }

    /// It is the partition count that blocks the pushdown, not the repartition.
    #[test]
    fn a_sort_with_one_output_partition_still_takes_the_fetch() {
        let window = sort_then_window(
            &row_number_udwf(),
            "row_number",
            hash_repartition(1),
            InputOrderMode::Sorted,
        )
        .unwrap();
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(window, 0, Some(100)));
        assert_snapshot!(both_sides(plan), @r#"
        input:
          GlobalLimitExec: skip=0, fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([a@0], 1), input_partitions=1
                  PlaceholderRowExec
        output:
          GlobalLimitExec: skip=0, fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortExec: TopK(fetch=100), expr=[a@0 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([a@0], 1), input_partitions=1
                  PlaceholderRowExec
        "#);
    }

    /// A merge emits its input's first rows in order from one partition, so it
    /// takes the fetch however many partitions it reads.
    #[test]
    fn a_merge_below_a_window_still_takes_the_fetch() {
        let input = hash_repartition(4);
        let ordering = ordered_by_a(&input).unwrap();
        let sort: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(ordering.clone(), input).with_preserve_partitioning(true),
        );
        let merge: Arc<dyn ExecutionPlan> =
            Arc::new(SortPreservingMergeExec::new(ordering, sort));
        let window = window_over(
            &row_number_udwf(),
            "row_number",
            merge,
            InputOrderMode::Sorted,
        )
        .unwrap();
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(window, 0, Some(100)));
        assert_snapshot!(both_sides(plan), @r#"
        input:
          GlobalLimitExec: skip=0, fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortPreservingMergeExec: [a@0 ASC]
                SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                  RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1
                    PlaceholderRowExec
        output:
          GlobalLimitExec: skip=0, fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortPreservingMergeExec: [a@0 ASC], fetch=100
                SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                  RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1
                    PlaceholderRowExec
        "#);
    }

    fn ordered_by(
        input: &Arc<dyn ExecutionPlan>,
        columns: &[&str],
    ) -> datafusion_common::Result<LexOrdering> {
        let s = input.schema();
        let exprs = columns
            .iter()
            .map(|c| Ok(PhysicalSortExpr::new_default(col(c, &s)?).asc()))
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        Ok(LexOrdering::new(exprs).unwrap())
    }

    /// The sort is ordered by the same columns the window partitions by, so the
    /// window can read it in order.
    fn sort_then_partitioned_window(
        input: Arc<dyn ExecutionPlan>,
        columns: &[&str],
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let ordering = ordered_by(&input, columns)?;
        let sort: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(ordering, input).with_preserve_partitioning(true));
        let s = sort.schema();
        let partition_by = columns
            .iter()
            .map(|c| col(c, &s))
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        let window_expr = Arc::new(StandardWindowExpr::new(
            create_udwf_window_expr(
                &row_number_udwf(),
                &[],
                &s,
                "row_number".to_string(),
                false,
            )?,
            &partition_by,
            &[],
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

    /// A merge that orders the window's partitions apart makes the fetch a bound
    /// on what the window reads after all.
    #[test]
    fn a_merge_that_orders_the_partitions_apart_takes_the_fetch() {
        let window = sort_then_partitioned_window(hash_repartition(4), &["a"]).unwrap();
        let ordering = ordered_by(&window, &["a"]).unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            SortPreservingMergeExec::new(ordering, window).with_fetch(Some(100)),
        );
        assert_snapshot!(both_sides(plan), @r#"
        input:
          SortPreservingMergeExec: [a@0 ASC], fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1
                  PlaceholderRowExec
        output:
          SortPreservingMergeExec: [a@0 ASC], fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortExec: TopK(fetch=100), expr=[a@0 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1
                  PlaceholderRowExec
        "#);
    }

    /// The merge orders by `a` while the window partitions by `a, b`: within one
    /// value of `a` it may emit the partitions in any order.
    #[test]
    fn a_merge_that_leaves_partitions_tied_does_not() {
        let s = schema();
        let hash =
            Partitioning::Hash(vec![col("a", &s).unwrap(), col("b", &s).unwrap()], 4);
        let window =
            sort_then_partitioned_window(repartition(hash), &["a", "b"]).unwrap();
        let ordering = ordered_by(&window, &["a"]).unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            SortPreservingMergeExec::new(ordering, window).with_fetch(Some(100)),
        );
        assert_snapshot!(both_sides(plan), @r#"
        input:
          SortPreservingMergeExec: [a@0 ASC], fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([a@0, b@1], 4), input_partitions=1
                  PlaceholderRowExec
        output:
          SortPreservingMergeExec: [a@0 ASC], fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([a@0, b@1], 4), input_partitions=1
                  PlaceholderRowExec
        "#);
    }

    fn repartition(partitioning: Partitioning) -> Arc<dyn ExecutionPlan> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(schema()));
        Arc::new(RepartitionExec::try_new(input, partitioning).unwrap())
    }

    fn merge_with_fetch(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let ordering = ordered_by(&input, &["a"]).unwrap();
        Arc::new(SortPreservingMergeExec::new(ordering, input).with_fetch(Some(100)))
    }

    /// A `CoalescePartitionsExec` between the merge and the window does not keep
    /// its input's order, so the merge's ordering says nothing below it.
    #[test]
    fn an_unordered_node_under_the_merge_blocks_the_pushdown() {
        let window = sort_then_partitioned_window(hash_repartition(4), &["a"]).unwrap();
        let coalesce: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(window));
        assert_snapshot!(both_sides(merge_with_fetch(coalesce)), @r#"
        input:
          SortPreservingMergeExec: [a@0 ASC], fetch=100
            CoalescePartitionsExec
              BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
                SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                  RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1
                    PlaceholderRowExec
        output:
          SortPreservingMergeExec: [a@0 ASC], fetch=100
            CoalescePartitionsExec
              BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
                SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                  RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1
                    PlaceholderRowExec
        "#);
    }

    /// Round robin scatters a window partition over several of the sort's.
    #[test]
    fn a_round_robin_input_blocks_the_pushdown() {
        let input = repartition(Partitioning::RoundRobinBatch(4));
        let window = sort_then_partitioned_window(input, &["a"]).unwrap();
        assert_snapshot!(both_sides(merge_with_fetch(window)), @r#"
        input:
          SortPreservingMergeExec: [a@0 ASC], fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
                  PlaceholderRowExec
        output:
          SortPreservingMergeExec: [a@0 ASC], fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
                  PlaceholderRowExec
        "#);
    }

    #[test]
    fn a_hash_on_another_column_blocks_the_pushdown() {
        let s = schema();
        let input = repartition(Partitioning::Hash(vec![col("b", &s).unwrap()], 4));
        let window = sort_then_partitioned_window(input, &["a"]).unwrap();
        assert_snapshot!(both_sides(merge_with_fetch(window)), @r#"
        input:
          SortPreservingMergeExec: [a@0 ASC], fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([b@1], 4), input_partitions=1
                  PlaceholderRowExec
        output:
          SortPreservingMergeExec: [a@0 ASC], fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([b@1], 4), input_partitions=1
                  PlaceholderRowExec
        "#);
    }

    /// A hash wider than the `PARTITION BY` splits one window partition across
    /// buckets, even though it is a prefix of the sort's ordering.
    #[test]
    fn a_hash_wider_than_the_partition_by_blocks_the_pushdown() {
        let s = schema();
        let hash =
            Partitioning::Hash(vec![col("a", &s).unwrap(), col("b", &s).unwrap()], 4);
        let window = sort_then_partitioned_window(repartition(hash), &["a"]).unwrap();
        assert_snapshot!(both_sides(merge_with_fetch(window)), @r#"
        input:
          SortPreservingMergeExec: [a@0 ASC], fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([a@0, b@1], 4), input_partitions=1
                  PlaceholderRowExec
        output:
          SortPreservingMergeExec: [a@0 ASC], fetch=100
            BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
              SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                RepartitionExec: partitioning=Hash([a@0, b@1], 4), input_partitions=1
                  PlaceholderRowExec
        "#);
    }

    fn wide_hash() -> Arc<dyn ExecutionPlan> {
        let s = schema();
        repartition(Partitioning::Hash(
            vec![col("a", &s).unwrap(), col("b", &s).unwrap()],
            4,
        ))
    }

    /// The shape `SELECT ..., <expr> AS <name>` plans to.
    fn projection_adding(
        input: Arc<dyn ExecutionPlan>,
        expr: Arc<dyn PhysicalExpr>,
        name: &str,
    ) -> Arc<dyn ExecutionPlan> {
        let s = input.schema();
        let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = s
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                (
                    Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                    field.name().to_string(),
                )
            })
            .collect();
        exprs.push((expr, name.to_string()));
        Arc::new(ProjectionExec::try_new(exprs, input).unwrap())
    }

    /// The ordering is rewritten through the projection, so `a2` is read as the
    /// `a` the window partitions by. The `topk_repartition` benchmark plans this.
    #[test]
    fn a_merge_ordered_by_an_alias_above_a_projection_takes_the_fetch() {
        let window = sort_then_partitioned_window(hash_repartition(4), &["a"]).unwrap();
        let s = window.schema();
        let projection = projection_adding(window, col("a", &s).unwrap(), "a2");
        let ordering = ordered_by(&projection, &["a2"]).unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            SortPreservingMergeExec::new(ordering, projection).with_fetch(Some(100)),
        );
        assert_snapshot!(both_sides(plan), @r#"
        input:
          SortPreservingMergeExec: [a2@3 ASC], fetch=100
            ProjectionExec: expr=[a@0 as a, b@1 as b, row_number@2 as row_number, a@0 as a2]
              BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
                SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                  RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1
                    PlaceholderRowExec
        output:
          SortPreservingMergeExec: [a2@3 ASC], fetch=100
            ProjectionExec: expr=[a@0 as a, b@1 as b, row_number@2 as row_number, a@0 as a2]
              BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
                SortExec: TopK(fetch=100), expr=[a@0 ASC], preserve_partitioning=[true]
                  RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1
                    PlaceholderRowExec
        "#);
    }

    /// `a2` is `a`, so `[a, a2]` separates the groups of a window partitioned by
    /// `a` and not those of one partitioned by `a, b`, two expressions or not.
    #[test]
    fn a_merge_ordered_by_an_alias_of_a_key_does_not_cover_the_other_key() {
        let window = sort_then_partitioned_window(wide_hash(), &["a", "b"]).unwrap();
        let s = window.schema();
        let projection = projection_adding(window, col("a", &s).unwrap(), "a2");
        let ordering = ordered_by(&projection, &["a", "a2"]).unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            SortPreservingMergeExec::new(ordering, projection).with_fetch(Some(100)),
        );
        assert_snapshot!(both_sides(plan), @r#"
        input:
          SortPreservingMergeExec: [a@0 ASC, a2@3 ASC], fetch=100
            ProjectionExec: expr=[a@0 as a, b@1 as b, row_number@2 as row_number, a@0 as a2]
              BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
                SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[true]
                  RepartitionExec: partitioning=Hash([a@0, b@1], 4), input_partitions=1
                    PlaceholderRowExec
        output:
          SortPreservingMergeExec: [a@0 ASC, a2@3 ASC], fetch=100
            ProjectionExec: expr=[a@0 as a, b@1 as b, row_number@2 as row_number, a@0 as a2]
              BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
                SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[true]
                  RepartitionExec: partitioning=Hash([a@0, b@1], 4), input_partitions=1
                    PlaceholderRowExec
        "#);
    }

    /// `b % 2` leaves rows whose `b` differs tied. The rule does not tell which
    /// expressions keep the groups apart, so only the key itself passes.
    #[test]
    fn a_merge_ordered_by_an_expression_of_a_key_blocks_the_pushdown() {
        let window = sort_then_partitioned_window(wide_hash(), &["a", "b"]).unwrap();
        let s = window.schema();
        let modulo = Arc::new(BinaryExpr::new(
            col("b", &s).unwrap(),
            Operator::Modulo,
            lit(2_i64),
        )) as Arc<dyn PhysicalExpr>;
        let projection = projection_adding(window, modulo, "b2");
        let ordering = ordered_by(&projection, &["a", "b2"]).unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            SortPreservingMergeExec::new(ordering, projection).with_fetch(Some(100)),
        );
        assert_snapshot!(both_sides(plan), @r#"
        input:
          SortPreservingMergeExec: [a@0 ASC, b2@3 ASC], fetch=100
            ProjectionExec: expr=[a@0 as a, b@1 as b, row_number@2 as row_number, b@1 % 2 as b2]
              BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
                SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[true]
                  RepartitionExec: partitioning=Hash([a@0, b@1], 4), input_partitions=1
                    PlaceholderRowExec
        output:
          SortPreservingMergeExec: [a@0 ASC, b2@3 ASC], fetch=100
            ProjectionExec: expr=[a@0 as a, b@1 as b, row_number@2 as row_number, b@1 % 2 as b2]
              BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
                SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[true]
                  RepartitionExec: partitioning=Hash([a@0, b@1], 4), input_partitions=1
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
        let window =
            sort_then_window(&udwf, "at_least_200", input, InputOrderMode::Sorted)?;

        Ok(if with_limit {
            Arc::new(GlobalLimitExec::new(window, 0, Some(100)))
        } else {
            window
        })
    }

    /// Build `GlobalLimitExec(100) → BoundedWindowAggExec(row_number) → SortExec
    /// → BoundedWindowAggExec(at_least_200) → SortExec → PlaceholderRowExec`.
    ///
    /// The outer sort consumes the limit, so the inner window is reached with
    /// nothing in hand and the inner sort has no limit above it at all. Only this
    /// PR makes the shape reachable: before it, applying a fetch ended the walk,
    /// so nothing below the outer sort was ever visited.
    fn build_nested_absolute_plan() -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(schema()));
        let udwf = Arc::new(WindowUDF::from(AtLeast200(RowNumber::new())));
        let inner =
            sort_then_window(&udwf, "at_least_200", input, InputOrderMode::Sorted)?;
        let outer = sort_then_window(
            &row_number_udwf(),
            "row_number",
            inner,
            InputOrderMode::Sorted,
        )?;

        Ok(Arc::new(GlobalLimitExec::new(outer, 0, Some(100))))
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

    /// The nested variant: the limit is spent on the outer sort, so the inner
    /// window's `Absolute(200)` has nothing to raise. It must not become a fetch
    /// on the inner sort, which no `LIMIT` sits above.
    #[test]
    fn absolute_limit_effect_does_not_invent_a_limit_below_an_applied_sort() {
        let plan = build_nested_absolute_plan().unwrap();
        let optimized = optimize(plan);
        assert_snapshot!(plan_str(optimized.as_ref()), @r#"
        GlobalLimitExec: skip=0, fetch=100
          BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
            SortExec: TopK(fetch=100), expr=[a@0 ASC], preserve_partitioning=[true], sort_prefix=[a@0 ASC]
              BoundedWindowAggExec: wdw=[at_least_200: Field { "at_least_200": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
                SortExec: expr=[a@0 ASC], preserve_partitioning=[true]
                  PlaceholderRowExec
        "#);
    }
}
