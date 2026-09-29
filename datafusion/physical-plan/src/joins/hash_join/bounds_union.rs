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

//! Merges the per-partition build-side ranges of a partitioned hash join into
//! one bounds predicate that does not need routing.
//!
//! A partitioned hash join knows `[min, max]` for each join key column in each
//! build partition. To test a probe row against the bounds of one partition,
//! the filter must first compute the partition of the row, thus these bounds
//! are inside the routing `CASE`.
//!
//! This module computes the union of the ranges instead. The union does not
//! need routing, so the join can push it as a separate dynamic filter: the
//! pruning code can use it for files, row groups and pages.
//!
//! # This is a relaxation
//!
//! The union accepts more rows than the per-partition bounds: a probe key that
//! is in the range of partition 1 but routes to partition 0 passes the union.
//! This is correct because the join (and the membership filter) remove these
//! rows.
//!
//! With hash partitioning, every partition usually spans almost the full key
//! range, so the union is one range per column, which rejects few rows. It
//! is still useful when the build keys are a narrow slice of a probe side
//! that is clustered by the key. With range partitioning, the
//! partitions hold disjoint key ranges, and the union can keep them as
//! separate ranges (up to [`MAX_RANGES_PER_COLUMN`]), so it also rejects the
//! probe keys in the gaps between them.
//!
//! # Multi-column keys
//!
//! With more than one join key the per-partition bounds describe a box, and a
//! union of boxes is not a box. Each column is merged independently, and the
//! predicate is the product of the merged per-column ranges. This is a
//! superset of the union, thus it is also correct.

use std::cmp::Ordering;
use std::sync::Arc;

use super::shared_bounds::PartitionBounds;

use datafusion_common::ScalarValue;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, lit};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};

/// Maximum number of disjoint ranges for one join key column when the probe
/// side is range partitioned. More ranges are merged into one range from the
/// smallest minimum to the largest maximum: each range adds evaluation cost
/// for each probe batch, and the pruning code gets little from a long `OR`
/// chain.
///
/// With hash partitioning the gaps between the ranges of the partitions are
/// random, not a property of the data, so the ranges are always merged into
/// one (see [`merge_partition_bounds`]).
pub(super) const MAX_RANGES_PER_COLUMN: usize = 8;

/// A closed `[min, max]` range of one join key column.
pub(super) type Range = (ScalarValue, ScalarValue);

/// Merges the bounds of the given partitions into, for each of the
/// `num_columns` key columns, the smallest set of disjoint ranges that contains
/// the bounds of every partition. If a column gets more than
/// `max_ranges_per_column` ranges, they are merged into one range.
///
/// `partitions` must contain every partition that can hold a build row. If a
/// partition is missing (for example, a canceled partition with unknown
/// contents), the result can reject probe rows that match.
///
/// An empty set for a column means that the column has no usable bounds and
/// the predicate must not constrain it. This occurs when a partition has no
/// bounds for the column, or when two bounds cannot be compared. NULL bounds
/// are skipped: they occur only when every key of the column in that partition
/// is NULL, and a NULL key cannot satisfy a range check in any case. If every
/// bound of a column is NULL, the column gets an empty set.
pub(super) fn merge_partition_bounds(
    num_columns: usize,
    partitions: &[&PartitionBounds],
    max_ranges_per_column: usize,
) -> Vec<Vec<Range>> {
    (0..num_columns)
        .map(|column| merge_column(column, partitions, max_ranges_per_column))
        .collect()
}

fn merge_column(
    column: usize,
    partitions: &[&PartitionBounds],
    max_ranges: usize,
) -> Vec<Range> {
    let mut ranges: Vec<Range> = Vec::with_capacity(partitions.len());
    for bounds in partitions {
        // A partition without bounds for this column can hold any value, so
        // the column must stay unconstrained.
        let Some(column_bounds) = bounds.get_column_bounds(column) else {
            return Vec::new();
        };
        if column_bounds.min.is_null() || column_bounds.max.is_null() {
            continue;
        }
        ranges.push((column_bounds.min.clone(), column_bounds.max.clone()));
    }

    // Values of different types cannot be compared. This is not expected (all
    // partitions have the same key types), but in that case the column stays
    // unconstrained instead of producing an incorrect range.
    let Some((first, _)) = ranges.first() else {
        return Vec::new();
    };
    let comparable = ranges.iter().all(|(min, max)| {
        min.partial_cmp(first).is_some() && max.partial_cmp(first).is_some()
    });
    if !comparable {
        return Vec::new();
    }

    // `ScalarValue` uses the same order as the `>=` and `<=` comparisons of
    // the predicate, so a sort by minimum and a sweep merge exactly the ranges
    // that overlap.
    ranges.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let mut merged: Vec<Range> = Vec::with_capacity(ranges.len());
    for (min, max) in ranges {
        match merged.last_mut() {
            Some((_, current_max))
                if min.partial_cmp(current_max) != Some(Ordering::Greater) =>
            {
                if max.partial_cmp(current_max) == Some(Ordering::Greater) {
                    *current_max = max;
                }
            }
            _ => merged.push((min, max)),
        }
    }

    // The ranges are sorted by minimum and disjoint, so the last one has the
    // largest maximum.
    if merged.len() > max_ranges {
        let min = merged[0].0.clone();
        let max = merged[merged.len() - 1].1.clone();
        merged = vec![(min, max)];
    }
    merged
}

/// Creates the predicate for merged bounds: for each column with ranges,
/// `col >= min AND col <= max`, with the ranges of one column combined with
/// `OR`, and the columns combined with `AND`.
///
/// Returns `None` if no column has ranges.
pub(super) fn create_merged_bounds_predicate(
    on_right: &[PhysicalExprRef],
    merged: &[Vec<Range>],
) -> Option<Arc<dyn PhysicalExpr>> {
    on_right
        .iter()
        .zip(merged)
        .filter_map(|(right_expr, ranges)| {
            ranges
                .iter()
                .map(|(min, max)| range_predicate(right_expr, min, max))
                .reduce(|acc, range| {
                    Arc::new(BinaryExpr::new(acc, Operator::Or, range))
                        as Arc<dyn PhysicalExpr>
                })
        })
        .reduce(|acc, predicate| {
            Arc::new(BinaryExpr::new(acc, Operator::And, predicate))
                as Arc<dyn PhysicalExpr>
        })
}

/// Creates the predicate `expr >= min AND expr <= max`.
pub(super) fn range_predicate(
    expr: &PhysicalExprRef,
    min: &ScalarValue,
    max: &ScalarValue,
) -> Arc<dyn PhysicalExpr> {
    let min_expr = Arc::new(BinaryExpr::new(
        Arc::clone(expr),
        Operator::GtEq,
        lit(min.clone()),
    )) as Arc<dyn PhysicalExpr>;
    let max_expr = Arc::new(BinaryExpr::new(
        Arc::clone(expr),
        Operator::LtEq,
        lit(max.clone()),
    )) as Arc<dyn PhysicalExpr>;
    Arc::new(BinaryExpr::new(min_expr, Operator::And, max_expr))
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

    fn merge(num_columns: usize, partitions: &[PartitionBounds]) -> Vec<Vec<Range>> {
        let refs = partitions.iter().collect::<Vec<_>>();
        merge_partition_bounds(num_columns, &refs, MAX_RANGES_PER_COLUMN)
    }

    fn ranges(merged: &[Vec<Range>], column: usize) -> Vec<(i32, i32)> {
        merged[column]
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
    fn overlapping_ranges_merge_into_one() {
        let merged = merge(1, &[partition(&[(0, 10)]), partition(&[(5, 20)])]);
        assert_eq!(ranges(&merged, 0), vec![(0, 20)]);
    }

    #[test]
    fn touching_ranges_merge_into_one() {
        let merged = merge(1, &[partition(&[(0, 10)]), partition(&[(10, 20)])]);
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
    fn too_many_disjoint_ranges_merge_into_one() {
        let partitions = (0..=MAX_RANGES_PER_COLUMN)
            .map(|i| partition(&[(i as i32 * 100, i as i32 * 100 + 1)]))
            .collect::<Vec<_>>();
        let merged = merge(1, &partitions);
        assert_eq!(
            ranges(&merged, 0),
            vec![(0, MAX_RANGES_PER_COLUMN as i32 * 100 + 1)]
        );
    }

    #[test]
    fn one_range_per_column_merges_disjoint_ranges() {
        let partitions = [partition(&[(100, 110)]), partition(&[(0, 10)])];
        let refs = partitions.iter().collect::<Vec<_>>();
        let merged = merge_partition_bounds(1, &refs, 1);
        assert_eq!(ranges(&merged, 0), vec![(0, 110)]);
    }

    #[test]
    fn partition_without_bounds_leaves_column_unconstrained() {
        // The second partition has no bounds for column 0, so it can hold any
        // value.
        let merged = merge(1, &[partition(&[(0, 10)]), partition(&[])]);
        assert!(merged[0].is_empty());
    }

    #[test]
    fn null_bounds_are_skipped() {
        let all_null = PartitionBounds::new(vec![ColumnBounds::new(
            ScalarValue::Int32(None),
            ScalarValue::Int32(None),
        )]);
        let merged = merge(1, &[partition(&[(0, 10)]), all_null.clone()]);
        assert_eq!(ranges(&merged, 0), vec![(0, 10)]);

        let merged = merge(1, &[all_null]);
        assert!(merged[0].is_empty());
    }

    #[test]
    fn incomparable_bounds_leave_column_unconstrained() {
        let utf8 = PartitionBounds::new(vec![ColumnBounds::new(
            ScalarValue::from("a"),
            ScalarValue::from("b"),
        )]);
        let merged = merge(1, &[partition(&[(0, 10)]), utf8]);
        assert!(merged[0].is_empty());
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
    fn predicate_ors_disjoint_ranges_and_conjoins_columns() {
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
            predicate.to_string(),
            "(a@0 >= 0 AND a@0 <= 10 OR a@0 >= 100 AND a@0 <= 110) AND b@1 >= 0 AND b@1 <= 5"
        );
    }

    #[test]
    fn unconstrained_columns_have_no_predicate() {
        let merged = merge(1, &[partition(&[(0, 10)]), partition(&[])]);
        let on_right: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("a", 0))];
        assert!(create_merged_bounds_predicate(&on_right, &merged).is_none());
    }
}
