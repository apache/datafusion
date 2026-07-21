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
//! or with `RANK()` in place of `ROW_NUMBER()`:
//!
//! ```sql
//! SELECT * FROM (
//!     SELECT *, RANK() OVER (PARTITION BY pk ORDER BY val) as rk
//!     FROM t
//! ) WHERE rk <= K;
//! ```
//!
//! And replaces the `FilterExec → BoundedWindowAggExec → SortExec` pipeline
//! with `BoundedWindowAggExec → PartitionedTopKExec(fetch=K)`, removing both
//! the `FilterExec` and `SortExec`.
//!
//! The appropriate [`WindowFnKind`] is forwarded to `PartitionedTopKExec`.
//! RANK requires a non-empty `ORDER BY` clause (otherwise all rows tie at
//! rank 1 and the optimization is degenerate).
//!
//! See [`PartitionedTopKExec`] for details on the replacement operator.
//!
//! [`PartitionedTopKExec`]: datafusion_physical_plan::sorts::partitioned_topk::PartitionedTopKExec
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
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::partitioned_topk::{
    PartitionedTopKExec, WindowFnKind,
};
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::windows::{BoundedWindowAggExec, WindowUDFExpr};

/// Physical optimizer rule that converts per-partition `ROW_NUMBER` and
/// `RANK` top-K queries into a more efficient plan using
/// [`PartitionedTopKExec`].
///
/// # Pattern Detected
///
/// ```text
/// FilterExec(<ranking fn output> <= K)
///   [optional ProjectionExec]
///     BoundedWindowAggExec(<ranking fn> PARTITION BY ... ORDER BY ...)
///       SortExec(partition_keys, order_keys)
/// ```
///
/// # Replacement
///
/// ```text
/// [optional ProjectionExec]
///   BoundedWindowAggExec(<ranking fn> PARTITION BY ... ORDER BY ...)
///     PartitionedTopKExec(fn=<row_number|rank>, partition_keys, order_keys, fetch=K)
/// ```
///
/// The `FilterExec` is removed entirely. The `SortExec` is replaced by
/// `PartitionedTopKExec`, which maintains a per-partition top-K heap (and,
/// for `RANK`, a sibling ties `Vec`) instead of sorting the whole dataset.
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
/// All of the following must be true:
/// - Config flag `enable_window_topn` is `true`
/// - The plan matches `FilterExec → [ProjectionExec] → BoundedWindowAggExec → SortExec`
/// - The window function is `ROW_NUMBER` or `RANK` (not `DENSE_RANK`)
/// - The window function has a `PARTITION BY` clause (global top-K is
///   already handled by `SortExec` with `fetch`)
/// - For `RANK`: a non-empty `ORDER BY` clause (otherwise all rows tie
///   at rank 1 — the optimization is useless and the boundary-tie storage
///   would be unbounded)
/// - The filter predicate compares the window output column to an integer
///   literal using `<=`, `<`, `>=`, or `>`
///
/// [`PartitionedTopKExec`]: datafusion_physical_plan::sorts::partitioned_topk::PartitionedTopKExec
#[derive(Default, Clone, Debug)]
pub struct WindowTopN;

impl WindowTopN {
    pub fn new() -> Self {
        Self
    }

    /// Attempt to transform a single plan node.
    ///
    /// Returns `Some(new_plan)` if the node matches the
    /// `FilterExec → [ProjectionExec] → BoundedWindowAggExec → SortExec`
    /// pattern and can be rewritten, or `None` if the node should be
    /// left unchanged.
    fn try_transform(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        // Step 1: Match FilterExec at the top
        let filter = plan.downcast_ref::<FilterExec>()?;

        // Don't handle filters with projections
        if filter.projection().is_some() {
            return None;
        }

        // Step 2: Extract limit from predicate (rn <= K, rn < K, etc.)
        let (col_idx, limit_n) = extract_window_limit(filter.predicate())?;

        // Step 3: Walk through optional ProjectionExec and RepartitionExec to find BoundedWindowAggExec
        let child = filter.input();
        let (window_exec, intermediates) = find_window_below(child)?;

        // Step 4: Verify col_idx references a supported window function output column
        let window_exec_typed = window_exec.downcast_ref::<BoundedWindowAggExec>()?;
        let sort_exec = window_exec_typed.input().downcast_ref::<SortExec>()?;
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

        // Step 5: child of window is SortExec (verified above)
        let sort_child = sort_exec.input();

        // Step 6: Determine partition_prefix_len from the window expression
        let partition_by = window_exprs[window_expr_idx].partition_by();
        let partition_prefix_len = partition_by.len();

        // Without PARTITION BY, this is just a global top-K which
        // SortExec with fetch already handles efficiently.
        if partition_prefix_len == 0 {
            return None;
        }

        // For RANK: an empty ORDER BY makes every row tie at rank 1 —
        // the optimization is degenerate (we'd retain the entire input)
        // and tie storage would be unbounded.
        if matches!(fn_kind, WindowFnKind::Rank)
            && window_exprs[window_expr_idx].order_by().is_empty()
        {
            return None;
        }

        // Step 7: Build PartitionedTopKExec using SortExec's expressions
        let partitioned_topk = PartitionedTopKExec::try_new(
            Arc::clone(sort_child),
            sort_exec.expr().clone(),
            partition_prefix_len,
            limit_n,
            fn_kind,
        )
        .ok()?;

        // Step 8: Rebuild window with new child
        let mut result = window_exec
            .with_new_children(vec![Arc::new(partitioned_topk)])
            .ok()?;

        // Step 9: Rebuild intermediate nodes (ProjectionExec/RepartitionExec)
        for node in intermediates.into_iter().rev() {
            result = node.with_new_children(vec![result]).ok()?;
        }

        Some(result)
    }
}

impl PhysicalOptimizerRule for WindowTopN {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.optimizer.enable_window_topn {
            return Ok(plan);
        }

        plan.transform_down(|node| {
            Ok(
                if let Some(transformed) = WindowTopN::try_transform(&node) {
                    Transformed::yes(transformed)
                } else {
                    Transformed::no(node)
                },
            )
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
fn extract_window_limit(
    predicate: &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
) -> Option<(usize, usize)> {
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
/// - `None` for everything else (e.g. `dense_rank`)
fn supported_window_fn(
    expr: &Arc<dyn datafusion_physical_expr::window::WindowExpr>,
) -> Option<WindowFnKind> {
    let swe = expr.as_any().downcast_ref::<StandardWindowExpr>()?;
    let swfe = swe.get_standard_func_expr();
    let udf = swfe.as_any().downcast_ref::<WindowUDFExpr>()?;
    match udf.fun().name() {
        "row_number" => Some(WindowFnKind::RowNumber),
        "rank" => Some(WindowFnKind::Rank),
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
