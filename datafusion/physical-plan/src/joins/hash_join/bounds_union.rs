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

//! Merging the per-partition build-side ranges of a partitioned hash join into
//! a single, routing-free conjunct.
//!
//! A partitioned hash join knows `[min, max]` per join key column *per build
//! partition*. Evaluating those bounds exactly requires knowing which partition
//! a probe row routes to, which is why they live inside the routing `CASE`.
//!
//! This module computes the set-theoretic **union** of those ranges instead.
//! The union does not need routing, so it can be pushed as its own top-level
//! conjunct where [`split_conjunction`] can see it — which is what lets the
//! Parquet reader prune row groups with it and evaluate it as a separate,
//! cheap `ArrowPredicate` before the expensive membership check.
//!
//! [`split_conjunction`]: datafusion_physical_expr::split_conjunction
//!
//! # This is a relaxation
//!
//! The union is a *superset* of what the `CASE` accepts: a probe key that falls
//! inside partition 1's range but routes to partition 0 passes the union and is
//! rejected by the `CASE`. That is sound — the membership half behind it is
//! exact — but slightly less selective. [`MergedBounds::relaxation`] estimates
//! how much, assuming keys are uniformly scattered across partitions.
//!
//! # Multi-column keys
//!
//! With more than one join key the per-partition bounds describe a *box*, and
//! the union of boxes is not a box. This module merges **each column
//! independently** and emits the product of the merged per-column ranges, which
//! is a superset of the true union (it also admits the "corners" that no single
//! partition covers). That is still sound, and the corner loss is reflected in
//! the reported relaxation.

use std::sync::Arc;

use super::shared_bounds::PartitionBounds;

use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, lit};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};

/// Upper bound on how many `OR`'d ranges a single join key column may
/// contribute. Past this the ranges are collapsed to their convex hull: more
/// terms cost evaluation time on every probe batch, and a long `OR` chain stops
/// being useful for row-group pruning long before it stops being correct.
const MAX_RANGES_PER_COLUMN: usize = 8;

/// A closed `[min, max]` range over one join key column.
type Range = (ScalarValue, ScalarValue);

/// The union of every build partition's ranges, merged per column.
#[derive(Debug, Default, PartialEq)]
pub(super) struct MergedBounds {
    /// One entry per join key column, positionally matching the join's
    /// `on_right` expressions. An empty entry means the column has no usable
    /// bounds (some partition did not report any, or the values are `NULL`) and
    /// therefore contributes no term — constraining it would drop valid rows.
    per_column: Vec<Vec<Range>>,
    /// Expected fraction of probe keys that the merged bounds admit but the
    /// per-partition `CASE` would have rejected, assuming keys scatter
    /// uniformly over partitions.
    ///
    /// `None` when the key types have no numeric measure (e.g. strings), so no
    /// estimate can be formed.
    relaxation: Option<f64>,
}

impl MergedBounds {
    /// Expected fraction of probe keys wrongly admitted by the merged bounds.
    /// See [`MergedBounds::relaxation`](#structfield.relaxation).
    pub(super) fn relaxation(&self) -> Option<f64> {
        self.relaxation
    }

    /// True when the merge produced nothing to filter on, so the conjunct would
    /// be a pure cost with no pruning power.
    pub(super) fn is_degenerate(&self) -> bool {
        self.per_column.iter().all(|ranges| ranges.is_empty())
    }
}

/// Merges the per-partition ranges of every reported, non-empty build partition
/// into the minimal set of disjoint ranges covering their union.
///
/// `partitions` must contain **every** partition that can accept a probe row.
/// Omitting one (e.g. a partition whose content is unknown because it was
/// canceled) would produce bounds that reject rows that partition could match.
pub(super) fn merge_partition_bounds(
    num_columns: usize,
    partitions: &[&PartitionBounds],
) -> MergedBounds {
    if partitions.is_empty() {
        return MergedBounds::default();
    }

    let mut per_column = Vec::with_capacity(num_columns);
    for column in 0..num_columns {
        per_column.push(merge_column(column, partitions));
    }

    let relaxation = estimate_relaxation(&per_column, partitions);
    MergedBounds {
        per_column,
        relaxation,
    }
}

/// Merges one column's ranges across partitions, or returns an empty set when
/// any partition failed to report a usable range for it.
fn merge_column(column: usize, partitions: &[&PartitionBounds]) -> Vec<Range> {
    let mut ranges = Vec::with_capacity(partitions.len());
    for bounds in partitions {
        // A partition with no usable range for this column could match any
        // value, so the union over this column is unbounded and the column
        // must not be constrained at all.
        let Some(column_bounds) = bounds.get_column_bounds(column) else {
            return Vec::new();
        };
        if column_bounds.min.is_null() || column_bounds.max.is_null() {
            return Vec::new();
        }
        ranges.push((column_bounds.min.clone(), column_bounds.max.clone()));
    }

    // `ScalarValue`'s ordering is the same one the emitted `>=` / `<=`
    // comparisons use, so sorting by lower bound and sweeping merges exactly
    // the ranges that overlap. Non-comparable values (a type mismatch between
    // partitions, which would be a construction bug) sort as equal and are
    // conservatively merged into their neighbour, which only ever widens.
    ranges
        .sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let mut merged: Vec<Range> = Vec::with_capacity(ranges.len());
    for (min, max) in ranges {
        match merged.last_mut() {
            // Overlapping (or touching): extend the open range in place.
            Some((_, current_max)) if min <= *current_max => {
                if max > *current_max {
                    *current_max = max;
                }
            }
            _ => merged.push((min, max)),
        }
    }

    if merged.len() > MAX_RANGES_PER_COLUMN {
        let min = merged.first().expect("non-empty").0.clone();
        let max = merged
            .iter()
            .map(|(_, max)| max)
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .expect("non-empty")
            .clone();
        merged = vec![(min, max)];
    }

    merged
}

/// Estimates the fraction of probe keys the merged bounds admit but the
/// per-partition `CASE` rejects.
///
/// A probe key routes to one partition uniformly at random, so the probability
/// it is *correctly* admitted is the average, over partitions, of the share of
/// the merged region that partition covers. With independent per-column merging
/// that share is the product of the per-column width ratios.
fn estimate_relaxation(
    per_column: &[Vec<Range>],
    partitions: &[&PartitionBounds],
) -> Option<f64> {
    let mut union_widths = Vec::with_capacity(per_column.len());
    for (column, ranges) in per_column.iter().enumerate() {
        if ranges.is_empty() {
            continue;
        }
        let mut total = 0.0;
        for (min, max) in ranges {
            total += width(min, max)?;
        }
        union_widths.push((column, total));
    }
    if union_widths.is_empty() {
        return None;
    }

    let mut covered = 0.0;
    for bounds in partitions {
        let mut share = 1.0;
        for (column, union_width) in &union_widths {
            // A zero-width union means every partition pins the column to the
            // same single value, so this column separates nothing.
            if *union_width == 0.0 {
                continue;
            }
            let column_bounds = bounds.get_column_bounds(*column)?;
            share *= width(&column_bounds.min, &column_bounds.max)? / union_width;
        }
        covered += share;
    }

    Some(1.0 - covered / partitions.len() as f64)
}

/// Numeric width of a range, or `None` for types with no numeric measure.
fn width(min: &ScalarValue, max: &ScalarValue) -> Option<f64> {
    let to_f64 = |value: &ScalarValue| match value.cast_to(&DataType::Float64) {
        Ok(ScalarValue::Float64(Some(value))) if value.is_finite() => Some(value),
        _ => None,
    };
    Some(to_f64(max)? - to_f64(min)?)
}

/// Builds `(col >= min AND col <= max) OR …` per column, `AND`'d across columns.
///
/// Returns `None` when the merge is degenerate, so the caller can emit a
/// constant instead of paying to evaluate a predicate that prunes nothing.
pub(super) fn create_merged_bounds_predicate(
    on_right: &[PhysicalExprRef],
    merged: &MergedBounds,
) -> Option<Arc<dyn PhysicalExpr>> {
    let mut column_predicates: Vec<Arc<dyn PhysicalExpr>> = Vec::new();

    for (right_expr, ranges) in on_right.iter().zip(merged.per_column.iter()) {
        let Some(column_predicate) = ranges
            .iter()
            .map(|(min, max)| range_predicate(right_expr, min, max))
            .reduce(|acc, range| {
                Arc::new(BinaryExpr::new(acc, Operator::Or, range))
                    as Arc<dyn PhysicalExpr>
            })
        else {
            continue;
        };
        column_predicates.push(column_predicate);
    }

    column_predicates.into_iter().reduce(|acc, predicate| {
        Arc::new(BinaryExpr::new(acc, Operator::And, predicate)) as Arc<dyn PhysicalExpr>
    })
}

fn range_predicate(
    right_expr: &PhysicalExprRef,
    min: &ScalarValue,
    max: &ScalarValue,
) -> Arc<dyn PhysicalExpr> {
    let min_expr = Arc::new(BinaryExpr::new(
        Arc::clone(right_expr),
        Operator::GtEq,
        lit(min.clone()),
    )) as Arc<dyn PhysicalExpr>;
    let max_expr = Arc::new(BinaryExpr::new(
        Arc::clone(right_expr),
        Operator::LtEq,
        lit(max.clone()),
    )) as Arc<dyn PhysicalExpr>;
    Arc::new(BinaryExpr::new(min_expr, Operator::And, max_expr)) as Arc<dyn PhysicalExpr>
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::joins::hash_join::shared_bounds::ColumnBounds;

    use datafusion_physical_expr::expressions::Column;

    fn partition(ranges: &[(i32, i32)]) -> PartitionBounds {
        PartitionBounds::new(
            ranges
                .iter()
                .map(|(min, max)| {
                    ColumnBounds::new(
                        ScalarValue::Int32(Some(*min)),
                        ScalarValue::Int32(Some(*max)),
                    )
                })
                .collect(),
        )
    }

    fn merge(num_columns: usize, partitions: &[PartitionBounds]) -> MergedBounds {
        let refs = partitions.iter().collect::<Vec<_>>();
        merge_partition_bounds(num_columns, &refs)
    }

    fn ranges(merged: &MergedBounds, column: usize) -> Vec<(i32, i32)> {
        merged.per_column[column]
            .iter()
            .map(|(min, max)| match (min, max) {
                (ScalarValue::Int32(Some(min)), ScalarValue::Int32(Some(max))) => {
                    (*min, *max)
                }
                other => panic!("expected Int32 range, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn overlapping_ranges_collapse_to_one() {
        let merged = merge(1, &[partition(&[(0, 10)]), partition(&[(5, 20)])]);
        assert_eq!(ranges(&merged, 0), vec![(0, 20)]);
    }

    #[test]
    fn disjoint_ranges_stay_separate() {
        let merged = merge(1, &[partition(&[(100, 110)]), partition(&[(0, 10)])]);
        assert_eq!(ranges(&merged, 0), vec![(0, 10), (100, 110)]);
    }

    #[test]
    fn contained_range_is_absorbed() {
        let merged = merge(1, &[partition(&[(0, 100)]), partition(&[(10, 20)])]);
        assert_eq!(ranges(&merged, 0), vec![(0, 100)]);
    }

    #[test]
    fn too_many_disjoint_ranges_collapse_to_convex_hull() {
        let partitions = (0..MAX_RANGES_PER_COLUMN + 1)
            .map(|i| partition(&[(i as i32 * 100, i as i32 * 100 + 1)]))
            .collect::<Vec<_>>();
        let merged = merge(1, &partitions);
        assert_eq!(
            ranges(&merged, 0),
            vec![(0, MAX_RANGES_PER_COLUMN as i32 * 100 + 1)]
        );
    }

    #[test]
    fn a_partition_without_bounds_disables_the_column() {
        // The second partition reports nothing for column 0, so any value could
        // route there and the column must stay unconstrained.
        let merged = merge(1, &[partition(&[(0, 10)]), partition(&[])]);
        assert!(merged.per_column[0].is_empty());
        assert!(merged.is_degenerate());
    }

    #[test]
    fn null_bounds_disable_the_column() {
        let with_null = PartitionBounds::new(vec![ColumnBounds::new(
            ScalarValue::Int32(None),
            ScalarValue::Int32(None),
        )]);
        let merged = merge(1, &[partition(&[(0, 10)]), with_null]);
        assert!(merged.per_column[0].is_empty());
    }

    #[test]
    fn columns_are_merged_independently() {
        let merged = merge(
            2,
            &[
                partition(&[(0, 10), (100, 110)]),
                partition(&[(20, 30), (0, 5)]),
            ],
        );
        assert_eq!(ranges(&merged, 0), vec![(0, 10), (20, 30)]);
        assert_eq!(ranges(&merged, 1), vec![(0, 5), (100, 110)]);
    }

    #[test]
    fn relaxation_is_zero_when_every_partition_spans_the_union() {
        let merged = merge(1, &[partition(&[(0, 100)]), partition(&[(0, 100)])]);
        assert_eq!(merged.relaxation(), Some(0.0));
    }

    #[test]
    fn relaxation_is_small_when_partitions_nearly_span_the_union() {
        // The hash-scattered case: every partition sees nearly the whole key
        // range, so the union is barely wider than any single partition.
        let merged = merge(1, &[partition(&[(0, 99)]), partition(&[(1, 100)])]);
        let relaxation = merged.relaxation().expect("numeric bounds");
        assert!(
            (relaxation - 0.01).abs() < 1e-9,
            "expected ~1% relaxation, got {relaxation}"
        );
    }

    #[test]
    fn relaxation_counts_only_the_disjoint_ranges_kept() {
        // Two non-overlapping partitions of equal width: a key routes to one of
        // them, so half of the two-range union is wrongly admitted. Keeping the
        // ranges disjoint is what keeps this at 0.5 instead of ~0.98, which is
        // what collapsing to the convex hull [0, 100] would cost.
        let merged = merge(1, &[partition(&[(0, 1)]), partition(&[(99, 100)])]);
        assert_eq!(ranges(&merged, 0), vec![(0, 1), (99, 100)]);
        assert_eq!(merged.relaxation(), Some(0.5));
    }

    #[test]
    fn relaxation_is_unknown_for_non_numeric_keys() {
        let utf8 = |min: &str, max: &str| {
            PartitionBounds::new(vec![ColumnBounds::new(
                ScalarValue::from(min),
                ScalarValue::from(max),
            )])
        };
        let merged = merge(1, &[utf8("a", "m"), utf8("n", "z")]);
        assert_eq!(merged.relaxation(), None);
        // The ranges themselves are still usable: only the estimate is missing.
        assert!(!merged.is_degenerate());
    }

    #[test]
    fn predicate_ors_disjoint_ranges_and_ands_columns() {
        let merged = merge(
            2,
            &[
                partition(&[(0, 10), (0, 5)]),
                partition(&[(100, 110), (0, 5)]),
            ],
        );
        let on_right: Vec<PhysicalExprRef> =
            vec![Arc::new(Column::new("a", 0)), Arc::new(Column::new("b", 1))];
        let predicate = create_merged_bounds_predicate(&on_right, &merged)
            .expect("expected a bounds predicate");
        assert_eq!(
            format!("{predicate}"),
            "(a@0 >= 0 AND a@0 <= 10 OR a@0 >= 100 AND a@0 <= 110) AND b@1 >= 0 AND b@1 <= 5"
        );
    }

    #[test]
    fn degenerate_merge_has_no_predicate() {
        let merged = merge(1, &[partition(&[(0, 10)]), partition(&[])]);
        let on_right: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("a", 0))];
        assert!(create_merged_bounds_predicate(&on_right, &merged).is_none());
    }
}
