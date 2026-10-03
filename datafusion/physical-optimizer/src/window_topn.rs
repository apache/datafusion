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

//! [`WindowTopN`] optimizer rule for per-partition top-K window queries.
//!
//! Detects queries of the form:
//!
//! ```sql
//! SELECT * FROM (
//!     SELECT *, ROW_NUMBER() OVER (PARTITION BY pk ORDER BY val) as rn
//!     FROM t
//! ) WHERE rn <= K;
//! ```
//!
//! or with `RANK()` / `DENSE_RANK()` in place of `ROW_NUMBER()`:
//!
//! ```sql
//! SELECT * FROM (
//!     SELECT *, RANK() OVER (PARTITION BY pk ORDER BY val) as rk
//!     FROM t
//! ) WHERE rk <= K;
//! ```
//!
//! The rewrite has two forms, chosen by whether the window's input already
//! satisfies the `(partition_keys, order_keys)` ordering that the window
//! requires:
//!
//! - **Input already ordered**: insert a streaming
//!   [`StreamingPartitionedTopKExec`] between the window and its child. It culls
//!   rows to the per-partition top-K in a single pass with O(1) state. This is a
//!   pure win, so it is applied **unconditionally**.
//! - **Input not ordered**: insert a heap-based [`PartitionedTopKExec`] below the
//!   window instead, which maintains per-partition top-K state rather than
//!   sorting the whole dataset. This can regress at high partition cardinality,
//!   so it is gated on the `enable_window_topn` config flag.
//!
//! Both drop the `FilterExec`. The appropriate [`WindowFnKind`] is forwarded to
//! the chosen operator. `RANK` and `DENSE_RANK` require a non-empty `ORDER BY`
//! clause (otherwise all rows tie at rank 1 and the optimization is degenerate).
//!
//! See [`PartitionedTopKExec`] / [`StreamingPartitionedTopKExec`] for details on
//! the replacement operators.
//!
//! [`PartitionedTopKExec`]: datafusion_physical_plan::sorts::partitioned_topk::PartitionedTopKExec
//! [`StreamingPartitionedTopKExec`]: datafusion_physical_plan::sorts::streaming_partitioned_topk::StreamingPartitionedTopKExec
//! [`WindowFnKind`]: datafusion_physical_plan::sorts::partitioned_topk::WindowFnKind

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion_physical_expr::window::StandardWindowExpr;
use datafusion_physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion_physical_plan::execution_plan::replace_children_if_necessary;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::partitioned_topk::{
    PartitionedTopKExec, WindowFnKind,
};
use datafusion_physical_plan::sorts::streaming_partitioned_topk::StreamingPartitionedTopKExec;
use datafusion_physical_plan::windows::{BoundedWindowAggExec, WindowUDFExpr};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// Physical optimizer rule that converts per-partition `ROW_NUMBER`,
/// `RANK`, and `DENSE_RANK` top-K queries into a more efficient plan
/// using [`PartitionedTopKExec`].
///
/// # Pattern Detected
///
/// ```text
/// FilterExec(<ranking fn output> <= K)
///   [optional ProjectionExec/RepartitionExec]
///     BoundedWindowAggExec(<ranking fn> PARTITION BY ... ORDER BY ...)
///       <child>            // the window's input, already ordered or not
/// ```
///
/// # Replacement
///
/// If `<child>` already produces `(partition_keys, order_keys)` ordering, a
/// streaming `StreamingPartitionedTopKExec` is inserted between the window and
/// the child:
///
/// ```text
/// [optional ProjectionExec/RepartitionExec]
///   BoundedWindowAggExec(...)
///     StreamingPartitionedTopKExec(fn=<row_number|rank>, partition_keys, order_keys, fetch=K)
///       <ordered child>
/// ```
///
/// Otherwise a heap-based `PartitionedTopKExec` is inserted in the same place:
///
/// ```text
/// [optional ProjectionExec/RepartitionExec]
///   BoundedWindowAggExec(...)
///     PartitionedTopKExec(fn=<row_number|rank|dense_rank>, partition_keys, order_keys, fetch=K)
///       <child>
/// ```
///
/// In both cases the `FilterExec` is removed entirely. The heap operator
/// maintains per-partition top-K state (a heap for `ROW_NUMBER`, a heap plus
/// boundary ties for `RANK`, a K-bounded distinct-ob map for `DENSE_RANK`)
/// instead of sorting the whole dataset; the streaming operator computes the
/// per-partition rank in a single pass with O(1) state.
///
/// # Supported Predicates
///
/// - `rn <= K` → fetch = K
/// - `rn < K` → fetch = K - 1
/// - `K >= rn` (flipped) → fetch = K
/// - `K > rn` (flipped) → fetch = K - 1
///
/// # When the Rule Fires
///
/// The shared match requires all of:
/// - The plan matches `FilterExec → [ProjectionExec/RepartitionExec...] → BoundedWindowAggExec`
/// - The window function is `ROW_NUMBER`, `RANK`, or `DENSE_RANK`
/// - Every window expression in the `BoundedWindowAggExec` is `ROW_NUMBER`,
///   `RANK`, or `DENSE_RANK` over the same `PARTITION BY` / `ORDER BY`. A
///   sibling that reads pruned rows (e.g. `LEAD`, or an aggregate whose
///   frame is not strictly backward-looking) would be computed over the
///   pruned input and give wrong results.
/// - The window function has a `PARTITION BY` clause (global top-K is
///   already handled by `SortExec` with `fetch`)
/// - At least one `ORDER BY` key survives past the `PARTITION BY` prefix
///   (so the operator has a non-empty ORDER BY). This rejects both a
///   missing `ORDER BY` and one fully covered by the partition prefix
///   such as `PARTITION BY pk ORDER BY pk`; for `RANK` / `DENSE_RANK`
///   such orderings also make every row tie at rank 1 (degenerate).
/// - The filter predicate compares the window output column to an integer
///   literal using `<=`, `<`, `>=`, or `>`
///
/// The **streaming** rewrite additionally requires the child's output ordering to
/// already satisfy `(partition_keys, order_keys)`; it fires **regardless** of
/// `enable_window_topn`. The **heap** rewrite handles everything else and only
/// fires when `enable_window_topn` is `true`.
///
/// [`PartitionedTopKExec`]: datafusion_physical_plan::sorts::partitioned_topk::PartitionedTopKExec
#[derive(Default, Clone, Debug)]
pub struct WindowTopN;

impl WindowTopN {
    pub fn new() -> Self {
        Self
    }

    /// Match a `FilterExec → [ProjectionExec/RepartitionExec...] →
    /// BoundedWindowAggExec` chain and extract the pieces both rewrites need.
    ///
    /// Returns `None` unless all shared guards hold: a supported ranking
    /// function, every sibling window expression prune-safe, a non-empty
    /// `PARTITION BY`, and at least one effective `ORDER BY` key.
    fn match_window(plan: &Arc<dyn ExecutionPlan>) -> Option<MatchedWindow> {
        // Step 1: Match FilterExec at the top
        let filter = plan.downcast_ref::<FilterExec>()?;

        // Don't handle filters with projections
        if filter.projection().is_some() {
            return None;
        }

        // Step 2: Extract limit from predicate (rn <= K, rn < K, etc.)
        let (col_idx, limit_n) = extract_window_limit(filter.predicate())?;

        // A predicate such as `rn < 1` (or the flipped `1 > rn`) yields a fetch of
        // 0. `ROW_NUMBER`/`RANK`/`DENSE_RANK` are always >= 1, so no row can satisfy
        // it and the correct result is empty. `PartitionedTopKExec` requires `k > 0`
        // and would panic on `k = 0`, so bail out here and let the regular
        // `FilterExec` produce the (empty) result instead of rewriting.
        if limit_n == 0 {
            return None;
        }

        // Step 3: Walk through optional ProjectionExec and RepartitionExec to find BoundedWindowAggExec
        let (window_exec, intermediates) = find_window_below(filter.input())?;
        let window_exec_typed = window_exec.downcast_ref::<BoundedWindowAggExec>()?;

        // Step 4: Verify col_idx references a supported window function output column
        let input_field_count = window_exec_typed.input().schema().fields().len();
        if col_idx < input_field_count {
            return None; // Filter is on an input column, not a window column
        }
        let window_expr_idx = col_idx - input_field_count;
        let window_exprs = window_exec_typed.window_expr();
        if window_expr_idx >= window_exprs.len() {
            return None;
        }
        let fn_kind = supported_window_fn(&window_exprs[window_expr_idx])?;

        // Tail-pruning drops the rows that rank after the retained top-K,
        // and every window expression in this `BoundedWindowAggExec` is
        // then evaluated over the *pruned* input. The rewrite is only valid
        // if each expression's value for a *retained* row is unaffected by
        // the dropped rows. ROW_NUMBER / RANK / DENSE_RANK over the same
        // PARTITION BY / ORDER BY satisfy this — each depends only on rows
        // at or before the current row in that order, all of which are
        // retained. A sibling like `LEAD(x)` reads following (pruned) rows,
        // so at the retained boundary it would resolve to a dropped row and
        // give a wrong result. Bail out unless every window expression is a
        // supported ranking function sharing the matched expression's
        // partition/order keys.
        //
        // This guard is shared deliberately: it inspects only the window
        // expressions, so it is independent of which rewrite (streaming or
        // heap) is chosen, and both prune tails.
        let matched_expr = &window_exprs[window_expr_idx];
        let all_prune_safe = window_exprs.iter().all(|e| {
            supported_window_fn(e).is_some()
                && e.partition_by() == matched_expr.partition_by()
                && e.order_by() == matched_expr.order_by()
        });
        if !all_prune_safe {
            return None;
        }

        // Step 5: Validate PARTITION BY / ORDER BY and collect sort keys from the window expr
        let partition_by = window_exprs[window_expr_idx].partition_by();
        let partition_prefix_len = partition_by.len();

        // Without PARTITION BY, this is just a global top-K which
        // SortExec with fetch already handles efficiently.
        if partition_prefix_len == 0 {
            return None;
        }

        // Step 6: Build PartitionedTopKExec from the window's partition/order keys
        let order_by = window_exprs[window_expr_idx].order_by();
        let expr_iterator = partition_by
            .iter()
            .map(|e| PhysicalSortExpr::new_default(Arc::clone(e)))
            .chain(order_by.iter().cloned());
        let expr = LexOrdering::new(expr_iterator)?;

        // `PartitionedTopKExec` derives its ORDER BY keys from the ordering
        // *beyond* the partition prefix (`expr[partition_prefix_len..]`).
        // That slice is empty in two cases:
        //   * no ORDER BY at all (e.g. `ROW_NUMBER() OVER (PARTITION BY pk)`);
        //   * ORDER BY keys fully covered by the partition prefix (e.g.
        //     `DENSE_RANK() OVER (PARTITION BY pk ORDER BY pk)`, whose
        //     deduplicated ordering is just `[pk]`).
        // With zero order keys the operator panics on execution (it
        // requires at least one), and for RANK / DENSE_RANK every row
        // would tie at rank 1 (a degenerate, unbounded retained set).
        // `order_by()` alone does not catch the second case — it reports
        // `[pk]` even though no order key survives past the partition
        // prefix — so guard on the effective order-key count instead.
        if expr.len() <= partition_prefix_len {
            return None;
        }

        Some(MatchedWindow {
            window_exec: Arc::clone(&window_exec),
            window_expr_idx,
            fn_kind,
            expr,
            partition_prefix_len,
            limit_n,
            intermediates,
        })
    }

    /// Case 1 — the child already produces `(partition_keys, order_keys)`
    /// ordering. Insert a [`StreamingPartitionedTopKExec`] between the window and
    /// its child; drop the `FilterExec`.
    fn try_streaming(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        let matched = Self::match_window(plan)?;

        let window = matched.window_exec.downcast_ref::<BoundedWindowAggExec>()?;
        let child = window.input();

        let window_expr = &window.window_expr()[matched.window_expr_idx];
        let (expr, partition_prefix_len) = streaming_ordering(
            child,
            window_expr.partition_by(),
            window_expr.order_by(),
        )?;

        let streaming = StreamingPartitionedTopKExec::try_new(
            Arc::clone(child),
            expr,
            partition_prefix_len,
            matched.limit_n,
            matched.fn_kind,
        )
        .ok()?;

        rebuild(
            &matched.window_exec,
            Arc::new(streaming),
            matched.intermediates,
        )
    }

    /// Case 2 — the child's ordering does not already satisfy the window's
    /// `(partition_keys, order_keys)`. Insert a heap-based
    /// [`PartitionedTopKExec`] below the window; drop the `FilterExec`. Gated on
    /// `enable_window_topn` by the caller.
    fn try_heap(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        let matched = Self::match_window(plan)?;
        let window = matched.window_exec.downcast_ref::<BoundedWindowAggExec>()?;

        let partitioned_topk = PartitionedTopKExec::try_new(
            Arc::clone(window.input()),
            matched.expr,
            matched.partition_prefix_len,
            matched.limit_n,
            matched.fn_kind,
        )
        .ok()?;

        rebuild(
            &matched.window_exec,
            Arc::new(partitioned_topk),
            matched.intermediates,
        )
    }
}

impl PhysicalOptimizerRule for WindowTopN {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let enable_heap = config.optimizer.enable_window_topn;

        plan.transform_down(|node| {
            // The streaming rewrite (already-sorted input) is a pure win and is
            // applied unconditionally. The heap rewrite (unsorted input) can
            // regress at high partition cardinality, so it stays behind the
            // `enable_window_topn` flag.
            if let Some(transformed) = WindowTopN::try_streaming(&node) {
                Ok(Transformed::yes(transformed))
            } else if enable_heap {
                if let Some(transformed) = WindowTopN::try_heap(&node) {
                    Ok(Transformed::yes(transformed))
                } else {
                    Ok(Transformed::no(node))
                }
            } else {
                Ok(Transformed::no(node))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "WindowTopN"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Shared pieces extracted from a matched
/// `FilterExec → [Projection/Repartition...] → BoundedWindowAggExec` chain.
struct MatchedWindow {
    /// The `BoundedWindowAggExec` node (as `Arc<dyn ExecutionPlan>`).
    window_exec: Arc<dyn ExecutionPlan>,
    /// Index of the ranking window expression within the window's expressions.
    window_expr_idx: usize,
    /// Which ranking function was matched.
    fn_kind: WindowFnKind,
    /// `(PARTITION BY keys, ORDER BY keys)` as a lex ordering, derived from the
    /// window expression. Used as-is by the heap rewrite; the streaming rewrite
    /// derives its own from the child's concrete output ordering instead.
    expr: LexOrdering,
    /// Number of `PARTITION BY` keys.
    partition_prefix_len: usize,
    /// Per-partition row limit (K) derived from the predicate.
    limit_n: usize,
    /// `ProjectionExec`/`RepartitionExec` nodes between the filter and the
    /// window, to be rebuilt above the new child.
    intermediates: Vec<Arc<dyn ExecutionPlan>>,
}

/// Rebuild the window over `new_child`, then replay the intermediate
/// `Projection`/`Repartition` nodes above it (the `FilterExec` is dropped).
fn rebuild(
    window_exec: &Arc<dyn ExecutionPlan>,
    new_child: Arc<dyn ExecutionPlan>,
    intermediates: Vec<Arc<dyn ExecutionPlan>>,
) -> Option<Arc<dyn ExecutionPlan>> {
    let mut result =
        replace_children_if_necessary(Arc::clone(window_exec), vec![new_child]).ok()?;
    for node in intermediates.into_iter().rev() {
        result = replace_children_if_necessary(node, vec![result]).ok()?;
    }
    Some(result)
}

/// Derive the `(expr, partition_prefix_len)` for a streaming rewrite from the
/// child's concrete output ordering.
///
/// Requires the child ordering to begin with the `PARTITION BY` columns (in any
/// order — partition keys need only be grouped) followed by the `ORDER BY`
/// expressions matched exactly (expr + options). Using the child's real sort
/// options means the operator's declared `required_input_ordering` matches what
/// is already present, so `EnforceSorting` won't insert a redundant sort.
///
/// Returns `None` if the ordering doesn't line up — the node then falls through
/// to the heap path or the default plan.
fn streaming_ordering(
    child: &Arc<dyn ExecutionPlan>,
    partition_by: &[Arc<dyn PhysicalExpr>],
    order_by: &[PhysicalSortExpr],
) -> Option<(LexOrdering, usize)> {
    let partition_prefix_len = partition_by.len();
    let total = partition_prefix_len + order_by.len();

    let ordering = child.output_ordering()?;
    if ordering.len() < total {
        return None;
    }

    // ORDER BY suffix must match exactly (expression and sort options).
    for (actual, expected) in ordering[partition_prefix_len..total].iter().zip(order_by) {
        if actual != expected {
            return None;
        }
    }

    // Partition prefix must be exactly the PARTITION BY columns (grouping —
    // direction/nulls don't matter for boundary detection).
    for pb in partition_by {
        if !ordering[..partition_prefix_len]
            .iter()
            .any(|e| e.expr.eq(pb))
        {
            return None;
        }
    }

    let expr = LexOrdering::new(ordering[..total].iter().cloned())?;
    Some((expr, partition_prefix_len))
}

/// Extract a window limit from a predicate expression.
///
/// Returns `(column_index, fetch)` if the predicate constrains a column
/// to at most N rows.
///
/// # Supported Patterns
///
/// | Predicate | Returns |
/// |-----------|---------|
/// | `Column(idx) <= Literal(N)` | `(idx, N)` |
/// | `Column(idx) < Literal(N)` | `(idx, N-1)` |
/// | `Literal(N) >= Column(idx)` | `(idx, N)` |
/// | `Literal(N) > Column(idx)` | `(idx, N-1)` |
///
/// # Examples
///
/// - `rn <= 5` → `Some((2, 5))` (assuming rn is column index 2)
/// - `rn < 3` → `Some((2, 2))`
/// - `10 >= rn` → `Some((2, 10))`
/// - `rn = 1` → `None` (equality not supported)
/// - `val <= 5` → `Some((1, 5))` (caller must verify it's a window column)
fn extract_window_limit(predicate: &Arc<dyn PhysicalExpr>) -> Option<(usize, usize)> {
    let binary = predicate.downcast_ref::<BinaryExpr>()?;
    let op = binary.op();
    let left = binary.left();
    let right = binary.right();

    // Try Column op Literal
    if let (Some(col), Some(lit_val)) = (
        left.downcast_ref::<Column>(),
        right.downcast_ref::<Literal>(),
    ) {
        let n = scalar_to_usize(lit_val.value())?;
        return match *op {
            Operator::LtEq => Some((col.index(), n)),
            Operator::Lt => Some((col.index(), n - 1)),
            _ => None,
        };
    }

    // Try Literal op Column (flipped)
    if let (Some(lit_val), Some(col)) = (
        left.downcast_ref::<Literal>(),
        right.downcast_ref::<Column>(),
    ) {
        let n = scalar_to_usize(lit_val.value())?;
        return match *op {
            Operator::GtEq => Some((col.index(), n)),
            Operator::Gt => Some((col.index(), n - 1)),
            _ => None,
        };
    }

    None
}

/// Convert a [`ScalarValue`] to `usize` if it's a positive integer.
///
/// Returns `None` for null values, zero, negative integers, and
/// non-integer types (floats, strings, decimals, etc.).
fn scalar_to_usize(value: &ScalarValue) -> Option<usize> {
    if !value.data_type().is_integer() {
        return None;
    }
    let casted = value.cast_to(&DataType::UInt64).ok()?;
    match casted {
        ScalarValue::UInt64(Some(v)) if v > 0 => usize::try_from(v).ok(),
        _ => None,
    }
}

/// Identify which supported ranking window function `expr` is.
///
/// Downcasts through `StandardWindowExpr` → `WindowUDFExpr` and checks
/// the UDF name. Returns:
/// - `Some(WindowFnKind::RowNumber)` for `"row_number"`
/// - `Some(WindowFnKind::Rank)` for `"rank"`
/// - `Some(WindowFnKind::DenseRank)` for `"dense_rank"`
/// - `None` for everything else
fn supported_window_fn(
    expr: &Arc<dyn datafusion_physical_expr::window::WindowExpr>,
) -> Option<WindowFnKind> {
    let swe = expr.as_any().downcast_ref::<StandardWindowExpr>()?;
    let swfe = swe.get_standard_func_expr();
    let udf = swfe.as_any().downcast_ref::<WindowUDFExpr>()?;
    match udf.fun().name() {
        "row_number" => Some(WindowFnKind::RowNumber),
        "rank" => Some(WindowFnKind::Rank),
        "dense_rank" => Some(WindowFnKind::DenseRank),
        _ => None,
    }
}

type PlanAndIntermediates = (Arc<dyn ExecutionPlan>, Vec<Arc<dyn ExecutionPlan>>);

/// Walk below a plan node looking for a [`BoundedWindowAggExec`].
///
/// Handles sequences of `ProjectionExec` and `RepartitionExec`.
/// This is safe because `PartitionedTopKExec` can be pushed below them:
/// projections only provide aliases, and pushing the limit below repartitions
/// is safe because the limit is computed per-partition.
///
/// Returns the window exec and a list of intermediate nodes to rebuild,
/// or `None` if no `BoundedWindowAggExec` is found.
fn find_window_below(plan: &Arc<dyn ExecutionPlan>) -> Option<PlanAndIntermediates> {
    let mut current = Arc::clone(plan);
    let mut intermediates = Vec::new();

    loop {
        if current.downcast_ref::<BoundedWindowAggExec>().is_some() {
            return Some((current, intermediates));
        } else if current.downcast_ref::<ProjectionExec>().is_some()
            || current.downcast_ref::<RepartitionExec>().is_some()
        {
            let next = Arc::clone(current.children().first()?);
            intermediates.push(current);
            current = next;
        } else {
            return None;
        }
    }
}
