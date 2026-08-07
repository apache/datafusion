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
//! And replaces the `FilterExec → BoundedWindowAggExec → SortExec` pipeline
//! with `BoundedWindowAggExec → PartitionedTopKExec(fetch=K)`, removing both
//! the `FilterExec` and `SortExec`.
//!
//! See [`PartitionedTopKExec`]
//! for details on the replacement operator.

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
use datafusion_physical_plan::sorts::partitioned_topk::PartitionedTopKExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::windows::{BoundedWindowAggExec, WindowUDFExpr};

/// Physical optimizer rule that converts per-partition `ROW_NUMBER` top-K
/// queries into a more efficient plan using [`PartitionedTopKExec`].
///
/// # Pattern Detected
///
/// ```text
/// FilterExec(rn <= K)
///   [optional ProjectionExec]
///     BoundedWindowAggExec(ROW_NUMBER PARTITION BY ... ORDER BY ...)
///       SortExec(partition_keys, order_keys)
/// ```
///
/// # Replacement
///
/// ```text
/// [optional ProjectionExec]
///   BoundedWindowAggExec(ROW_NUMBER PARTITION BY ... ORDER BY ...)
///     PartitionedTopKExec(partition_keys, order_keys, fetch=K)
/// ```
///
/// The `FilterExec` is removed entirely (all output rows have `rn ∈ {1..K}`).
/// The `SortExec` is replaced by `PartitionedTopKExec` which maintains a
/// per-partition top-K heap instead of sorting the entire dataset.
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
/// - The window function is `ROW_NUMBER` (not `RANK`, `DENSE_RANK`, etc.)
/// - `ROW_NUMBER` has a `PARTITION BY` clause (global top-K is already
///   handled by `SortExec` with `fetch`)
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

        // Step 3: Walk through optional ProjectionExec to find BoundedWindowAggExec
        let child = filter.input();
        let (window_exec, proj_between) = find_window_below(child)?;

        // Step 4: Verify col_idx references a ROW_NUMBER window output column
        let input_field_count = window_exec.input().schema().fields().len();
        if col_idx < input_field_count {
            return None; // Filter is on an input column, not a window column
        }
        let window_expr_idx = col_idx - input_field_count;
        let window_exprs = window_exec.window_expr();
        if window_expr_idx >= window_exprs.len() {
            return None;
        }
        if !is_row_number(&window_exprs[window_expr_idx]) {
            return None;
        }

        // Step 5: Verify child of window is SortExec
        let sort_exec = window_exec.input().downcast_ref::<SortExec>()?;
        let sort_child = sort_exec.input();

        // Step 6: Determine partition_prefix_len from the window expression
        let partition_by = window_exprs[window_expr_idx].partition_by();
        let partition_prefix_len = partition_by.len();

        // Without PARTITION BY, this is just a global top-K which
        // SortExec with fetch already handles efficiently.
        if partition_prefix_len == 0 {
            return None;
        }

        // Step 7: Build PartitionedTopKExec using SortExec's expressions
        let partitioned_topk = PartitionedTopKExec::try_new(
            Arc::clone(sort_child),
            sort_exec.expr().clone(),
            partition_prefix_len,
            limit_n,
        )
        .ok()?;

        // Step 8: Rebuild window with new child
        let new_window = Arc::clone(&child_as_arc(window_exec))
            .with_new_children(vec![Arc::new(partitioned_topk)])
            .ok()?;

        // Step 9: If ProjectionExec was between Filter and Window, rebuild it
        let result = match proj_between {
            Some(proj) => Arc::clone(&child_as_arc(proj))
                .with_new_children(vec![new_window])
                .ok()?,
            None => new_window,
        };

        Some(result)
    }
}

/// Helper to get an `Arc<dyn ExecutionPlan>` from a reference.
/// We need this because `with_new_children` takes `Arc<Self>`.
fn child_as_arc<T: ExecutionPlan + Clone + 'static>(plan: &T) -> Arc<dyn ExecutionPlan> {
    Arc::new(plan.clone())
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

/// Check if a window expression is `ROW_NUMBER`.
///
/// Downcasts through `StandardWindowExpr` → `WindowUDFExpr` and checks
/// that the UDF name is `"row_number"`. Returns `false` for all other
/// window functions (e.g., `RANK`, `DENSE_RANK`, `SUM`).
fn is_row_number(expr: &Arc<dyn datafusion_physical_expr::window::WindowExpr>) -> bool {
    let Some(swe) = expr.as_any().downcast_ref::<StandardWindowExpr>() else {
        return false;
    };
    let swfe = swe.get_standard_func_expr();
    let Some(udf) = swfe.as_any().downcast_ref::<WindowUDFExpr>() else {
        return false;
    };
    udf.fun().name() == "row_number"
}

/// Walk below a plan node looking for a [`BoundedWindowAggExec`].
///
/// Handles two cases:
/// - Direct child: `FilterExec → BoundedWindowAggExec`
/// - With projection: `FilterExec → ProjectionExec → BoundedWindowAggExec`
///
/// Returns the window exec and an optional `ProjectionExec` in between,
/// or `None` if no `BoundedWindowAggExec` is found within one or two levels.
fn find_window_below(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<(&BoundedWindowAggExec, Option<&ProjectionExec>)> {
    // Direct child is BoundedWindowAggExec
    if let Some(window) = plan.downcast_ref::<BoundedWindowAggExec>() {
        return Some((window, None));
    }

    // Child is ProjectionExec with BoundedWindowAggExec below
    if let Some(proj) = plan.downcast_ref::<ProjectionExec>() {
        let proj_child = proj.input();
        if let Some(window) = proj_child.downcast_ref::<BoundedWindowAggExec>() {
            return Some((window, Some(proj)));
        }
    }

    None
}
