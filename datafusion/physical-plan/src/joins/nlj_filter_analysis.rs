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

//! Expression analysis for dynamic filter pushdown in Nested Loop Joins.
//!
//! Unlike HashJoin which has explicit equi-join keys (`on: JoinOn`), NLJ only has
//! a `JoinFilter` with arbitrary expressions. This module walks the filter expression
//! to extract `(probe_col, operator, build_col)` pairs that can be converted into
//! bounds-based dynamic filters.
//!
//! For example, `e.ts BETWEEN w.start AND w.end` (which expands to
//! `e.ts >= w.start AND e.ts <= w.end`) yields two bound pairs:
//! - probe.ts >= build.start → push `probe.ts >= min(build.start)`
//! - probe.ts <= build.end   → push `probe.ts <= max(build.end)`

use std::collections::HashMap;
use std::sync::Arc;

use crate::joins::join_filter::JoinFilter;
use crate::joins::utils::ColumnIndex;

use arrow::datatypes::Schema;
use datafusion_common::{JoinSide, Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, lit};
use datafusion_physical_expr::utils::split_conjunction;
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};

/// How to derive the probe-side bound from a build-side column's min/max.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(clippy::enum_variant_names)]
pub(crate) enum BoundType {
    /// probe >= build → push `probe >= min(build)`
    ProbeGtEq,
    /// probe > build → push `probe > min(build)`
    ProbeGt,
    /// probe <= build → push `probe <= max(build)`
    ProbeLtEq,
    /// probe < build → push `probe < max(build)`
    ProbeLt,
    /// probe = build → push `probe >= min(build) AND probe <= max(build)`
    ProbeEq,
}

/// A pair linking a probe-side column to a build-side column with a derivation rule.
#[derive(Debug, Clone)]
pub(crate) struct BoundPair {
    /// Column index in the probe (right) child's schema
    pub probe_col_idx: usize,
    /// Column index in the build (left) child's schema
    pub build_col_idx: usize,
    /// How to derive the probe-side bound
    pub bound_type: BoundType,
}

/// Walk the JoinFilter expression tree and extract bound pairs.
///
/// Only handles top-level AND conjuncts that are simple BinaryExpr comparisons
/// between a build-side column and a probe-side column. Skips OR, function calls,
/// nested expressions, and literal comparisons.
///
/// # How it works
///
/// The JoinFilter expression uses an **intermediate schema** where column positions
/// map to actual left/right columns via `column_indices`. For example:
///
/// ```text
/// SQL: SELECT * FROM events e JOIN windows w ON e.ts BETWEEN w.start AND w.end
///
/// JoinFilter:
///   expression: Col(0) >= Col(1) AND Col(0) <= Col(2)
///   column_indices: [
///     0 → ColumnIndex { index: 0, side: Right }  // e.ts (probe)
///     1 → ColumnIndex { index: 0, side: Left  }  // w.start (build)
///     2 → ColumnIndex { index: 1, side: Left  }  // w.end (build)
///   ]
///
/// Step 1: split_conjunction → [Col(0) >= Col(1), Col(0) <= Col(2)]
///
/// Step 2: For "Col(0) >= Col(1)":
///   Col(0) → column_indices[0] → Right (probe), original index 0 (e.ts)
///   Col(1) → column_indices[1] → Left (build), original index 0 (w.start)
///   Pattern: probe >= build → BoundType::ProbeGtEq
///   Result: BoundPair { probe_col_idx: 0, build_col_idx: 0, ProbeGtEq }
///
/// Step 3: For "Col(0) <= Col(2)":
///   Col(0) → Right (probe), e.ts
///   Col(2) → Left (build), original index 1 (w.end)
///   Pattern: probe <= build → BoundType::ProbeLtEq
///   Result: BoundPair { probe_col_idx: 0, build_col_idx: 1, ProbeLtEq }
///
/// Output: [
///   BoundPair { probe=e.ts, build=w.start, ProbeGtEq },
///   BoundPair { probe=e.ts, build=w.end,   ProbeLtEq },
/// ]
/// ```
///
/// At runtime, these pairs tell us:
/// - Collect min(w.start), max(w.end) from build batch
/// - Push to probe scan: `e.ts >= min(w.start) AND e.ts <= max(w.end)`
pub(crate) fn extract_bound_pairs(filter: &JoinFilter) -> Vec<BoundPair> {
    let expr = filter.expression();
    let column_indices = filter.column_indices();
    let conjuncts = split_conjunction(expr);

    let mut pairs = Vec::new();
    for conjunct in conjuncts {
        if let Some(pair) = try_extract_bound_pair(conjunct.as_ref(), column_indices) {
            pairs.push(pair);
        }
    }
    pairs
}

/// Try to extract a BoundPair from a single expression.
/// Returns Some if the expression is `Column <cmp_op> Column` where one side is
/// build (Left) and the other is probe (Right).
fn try_extract_bound_pair(
    expr: &dyn PhysicalExpr,
    column_indices: &[ColumnIndex],
) -> Option<BoundPair> {
    let binary = expr.downcast_ref::<BinaryExpr>()?;
    let op = *binary.op();

    // Only handle comparison operators
    if !matches!(
        op,
        Operator::Eq | Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
    ) {
        return None;
    }

    // Both children must be Column references into the intermediate schema
    let left_col = binary.left().downcast_ref::<Column>()?;
    let right_col = binary.right().downcast_ref::<Column>()?;

    let left_idx = left_col.index();
    let right_idx = right_col.index();

    // Look up which side each column belongs to
    let left_ci = column_indices.get(left_idx)?;
    let right_ci = column_indices.get(right_idx)?;

    // We need one build (Left) and one probe (Right)
    match (left_ci.side, right_ci.side) {
        (JoinSide::Left, JoinSide::Right) => {
            // Expression: build_col <op> probe_col
            // Flip to probe perspective: probe_col <flipped_op> build_col
            let bound_type = flip_operator_to_probe_perspective(op)?;
            Some(BoundPair {
                probe_col_idx: right_ci.index,
                build_col_idx: left_ci.index,
                bound_type,
            })
        }
        (JoinSide::Right, JoinSide::Left) => {
            // Expression: probe_col <op> build_col (already in probe perspective)
            let bound_type = operator_to_bound_type(op)?;
            Some(BoundPair {
                probe_col_idx: left_ci.index,
                build_col_idx: right_ci.index,
                bound_type,
            })
        }
        _ => None, // Both same side, or JoinSide::None
    }
}

/// Convert operator to BoundType when expression is `probe <op> build`.
fn operator_to_bound_type(op: Operator) -> Option<BoundType> {
    match op {
        Operator::Eq => Some(BoundType::ProbeEq),
        Operator::Gt => Some(BoundType::ProbeGt),
        Operator::GtEq => Some(BoundType::ProbeGtEq),
        Operator::Lt => Some(BoundType::ProbeLt),
        Operator::LtEq => Some(BoundType::ProbeLtEq),
        _ => None,
    }
}

/// Flip operator when expression is `build <op> probe` to probe perspective.
///
/// When the JoinFilter expression has the build column on the left and probe on
/// the right, we need to flip the operator to express it from the probe's point
/// of view, since the pushed predicate applies to the probe side.
///
/// # Examples
///
/// ```text
/// Expression: w.start <= e.ts   (build <= probe)
/// Flipped:    e.ts >= w.start   (probe >= build)
/// → BoundType::ProbeGtEq → push: e.ts >= min(w.start)
///
/// Expression: w.end > e.ts      (build > probe)
/// Flipped:    e.ts < w.end      (probe < build)
/// → BoundType::ProbeLt → push: e.ts < max(w.end)
///
/// Expression: w.id = e.id       (build = probe)
/// Flipped:    e.id = w.id       (equality is symmetric)
/// → BoundType::ProbeEq → push: e.id >= min(w.id) AND e.id <= max(w.id)
/// ```
fn flip_operator_to_probe_perspective(op: Operator) -> Option<BoundType> {
    match op {
        Operator::Eq => Some(BoundType::ProbeEq),
        Operator::Gt => Some(BoundType::ProbeLt), // build > probe ≡ probe < build
        Operator::GtEq => Some(BoundType::ProbeLtEq), // build >= probe ≡ probe <= build
        Operator::Lt => Some(BoundType::ProbeGt), // build < probe ≡ probe > build
        Operator::LtEq => Some(BoundType::ProbeGtEq), // build <= probe ≡ probe >= build
        _ => None,
    }
}

/// Compute min/max values for build-side columns referenced by bound pairs.
///
/// Called after the build side is fully collected into a single merged batch.
/// Only computes min/max for columns actually referenced in the bound pairs
/// (avoids unnecessary work for columns not involved in cross-side comparisons).
///
/// # Example
///
/// ```text
/// Build batch (windows table):
///   col 0 (start): [100, 200, 300]
///   col 1 (end):   [150, 250, 400]
///   col 2 (name):  ["a", "b", "c"]   ← not referenced in any BoundPair
///
/// Bound pairs: [
///   BoundPair { build_col_idx: 0, ... },  // references w.start
///   BoundPair { build_col_idx: 1, ... },  // references w.end
/// ]
///
/// Result:
///   mins  = { 0 → ScalarValue::Int32(100), 1 → ScalarValue::Int32(150) }
///   maxes = { 0 → ScalarValue::Int32(300), 1 → ScalarValue::Int32(400) }
///
/// col 2 (name) is skipped — not in any BoundPair.
/// ```
///
/// Returns two maps: `build_col_idx → min_value` and `build_col_idx → max_value`.
/// Null values are skipped during min/max computation.
pub(crate) fn compute_build_bounds(
    batch: &arrow::record_batch::RecordBatch,
    bound_pairs: &[BoundPair],
) -> Result<(HashMap<usize, ScalarValue>, HashMap<usize, ScalarValue>)> {
    let mut mins = HashMap::new();
    let mut maxes = HashMap::new();

    // Collect unique build column indices that need min/max
    let build_cols: std::collections::HashSet<usize> =
        bound_pairs.iter().map(|p| p.build_col_idx).collect();

    for &col_idx in &build_cols {
        let col = batch.column(col_idx);
        let num_rows = col.len();

        if num_rows == 0 {
            continue;
        }

        // Find min and max by iterating through non-null values
        let mut min_val: Option<ScalarValue> = None;
        let mut max_val: Option<ScalarValue> = None;

        for row in 0..num_rows {
            if col.is_null(row) {
                continue;
            }
            let val = ScalarValue::try_from_array(col, row)?;
            min_val = Some(match min_val {
                None => val.clone(),
                Some(current) => {
                    if val.partial_cmp(&current) == Some(std::cmp::Ordering::Less) {
                        val.clone()
                    } else {
                        current
                    }
                }
            });
            max_val = Some(match max_val {
                None => val,
                Some(current) => {
                    if val.partial_cmp(&current) == Some(std::cmp::Ordering::Greater) {
                        val
                    } else {
                        current
                    }
                }
            });
        }

        if let (Some(min), Some(max)) = (min_val, max_val) {
            mins.insert(col_idx, min);
            maxes.insert(col_idx, max);
        }
    }

    Ok((mins, maxes))
}

/// Build a probe-side predicate from BoundPairs and computed min/max values.
///
/// Combines the bound pairs with actual min/max values to produce a filter
/// expression that can be pushed to the probe-side scan for row group pruning.
///
/// # Example
///
/// ```text
/// Input:
///   pairs = [
///     BoundPair { probe_col_idx: 0 (e.ts), build_col_idx: 0 (w.start), ProbeGtEq },
///     BoundPair { probe_col_idx: 0 (e.ts), build_col_idx: 1 (w.end),   ProbeLtEq },
///   ]
///   build_mins  = { 0 → 100, 1 → 150 }
///   build_maxes = { 0 → 300, 1 → 400 }
///
/// Processing:
///   Pair 1 (ProbeGtEq): e.ts >= min(w.start) → e.ts >= 100
///   Pair 2 (ProbeLtEq): e.ts <= max(w.end)   → e.ts <= 400
///
/// Output: e.ts >= 100 AND e.ts <= 400
/// ```
///
/// This predicate is pushed to the probe scan via `DynamicFilterPhysicalExpr::update()`.
/// The scan uses it for Parquet row group pruning (skip groups where
/// `ts_max < 100` or `ts_min > 400`).
///
/// Returns None if no predicates can be built (e.g., all build columns are null).
pub(crate) fn build_probe_predicate(
    pairs: &[BoundPair],
    build_mins: &HashMap<usize, ScalarValue>,
    build_maxes: &HashMap<usize, ScalarValue>,
    probe_schema: &Schema,
) -> Option<Arc<dyn PhysicalExpr>> {
    let mut predicates: Vec<Arc<dyn PhysicalExpr>> = Vec::new();

    for pair in pairs {
        let probe_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(
            probe_schema.field(pair.probe_col_idx).name(),
            pair.probe_col_idx,
        ));

        match pair.bound_type {
            BoundType::ProbeGtEq => {
                if let Some(min_val) = build_mins.get(&pair.build_col_idx)
                    && !min_val.is_null()
                {
                    predicates.push(Arc::new(BinaryExpr::new(
                        probe_col,
                        Operator::GtEq,
                        lit(min_val.clone()),
                    )));
                }
            }
            BoundType::ProbeGt => {
                if let Some(min_val) = build_mins.get(&pair.build_col_idx)
                    && !min_val.is_null()
                {
                    predicates.push(Arc::new(BinaryExpr::new(
                        probe_col,
                        Operator::Gt,
                        lit(min_val.clone()),
                    )));
                }
            }
            BoundType::ProbeLtEq => {
                if let Some(max_val) = build_maxes.get(&pair.build_col_idx)
                    && !max_val.is_null()
                {
                    predicates.push(Arc::new(BinaryExpr::new(
                        probe_col,
                        Operator::LtEq,
                        lit(max_val.clone()),
                    )));
                }
            }
            BoundType::ProbeLt => {
                if let Some(max_val) = build_maxes.get(&pair.build_col_idx)
                    && !max_val.is_null()
                {
                    predicates.push(Arc::new(BinaryExpr::new(
                        probe_col,
                        Operator::Lt,
                        lit(max_val.clone()),
                    )));
                }
            }
            BoundType::ProbeEq => {
                if let Some(min_val) = build_mins.get(&pair.build_col_idx)
                    && !min_val.is_null()
                {
                    predicates.push(Arc::new(BinaryExpr::new(
                        Arc::clone(&probe_col),
                        Operator::GtEq,
                        lit(min_val.clone()),
                    )));
                }
                if let Some(max_val) = build_maxes.get(&pair.build_col_idx)
                    && !max_val.is_null()
                {
                    predicates.push(Arc::new(BinaryExpr::new(
                        probe_col,
                        Operator::LtEq,
                        lit(max_val.clone()),
                    )));
                }
            }
        }
    }

    if predicates.is_empty() {
        None
    } else {
        Some(
            predicates
                .into_iter()
                .reduce(|acc, pred| {
                    Arc::new(BinaryExpr::new(acc, Operator::And, pred))
                        as Arc<dyn PhysicalExpr>
                })
                .unwrap(),
        )
    }
}

/// Get the unique probe-side column indices referenced by the bound pairs.
/// Used to create DynamicFilterPhysicalExpr children.
pub(crate) fn probe_columns_from_pairs(
    pairs: &[BoundPair],
    probe_schema: &Schema,
) -> Vec<PhysicalExprRef> {
    let mut seen = std::collections::HashSet::new();
    let mut cols = Vec::new();
    for pair in pairs {
        if seen.insert(pair.probe_col_idx) {
            cols.push(Arc::new(Column::new(
                probe_schema.field(pair.probe_col_idx).name(),
                pair.probe_col_idx,
            )) as PhysicalExprRef);
        }
    }
    cols
}
