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

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use datafusion_common::{Column, ScalarValue};
use datafusion_physical_expr::RangePartitioning;
use datafusion_physical_expr::expressions::Column as PhysicalColumn;

use crate::PruningStatistics;

/// Bounds implied by the effective split points of a [`RangePartitioning`].
///
/// Each partition is a container for [`crate::PruningPredicate`]. The returned
/// mask can be used by scans or physical operators without changing partition
/// identities. The range metadata must describe the rows' actual distribution.
///
/// Currently, min/max bounds are provided for a single direct integer column.
/// Other keys are unknown, including lexicographic multi-column ranges: applying
/// each component's split points independently would not give valid bounds.
/// Exclusive integer endpoints are converted to closed bounds with checked
/// arithmetic. Overflow leaves the bound unknown rather than wrapping it.
///
/// # Public Only for Internal Use:
/// `datafusion-catalog-listing` uses this adapter to prune declared file groups.
/// It can also be shared by physical optimizers without depending on catalog
/// implementations; it is not yet a supported downstream extension point.
#[doc(hidden)]
pub struct RangePruningStatistics<'a> {
    range: &'a RangePartitioning,
}

impl<'a> RangePruningStatistics<'a> {
    /// Create statistics from normalized physical range metadata.
    pub fn new(range: &'a RangePartitioning) -> Self {
        Self { range }
    }

    fn matches_column(&self, column: &Column) -> bool {
        let [sort] = self.range.ordering().as_ref() else {
            return false;
        };
        sort.expr
            .downcast_ref::<PhysicalColumn>()
            .is_some_and(|key| key.name() == column.name())
    }

    fn bounds(&self, column: &Column, minimum: bool) -> Option<ArrayRef> {
        if !self.matches_column(column) {
            return None;
        }
        let data_type = self.range.split_points().first()?.values()[0].data_type();
        if !data_type.is_integer() {
            return None;
        }
        let unknown = ScalarValue::try_from(&data_type).ok()?;
        let one = ScalarValue::new_one(&data_type).ok()?;
        let descending = self.range.ordering()[0].options.descending;
        // In sort order, the preceding split is inclusive and the following
        // split exclusive. DESC reverses their roles as numeric min/max.
        let exclusive = minimum == descending;
        ScalarValue::iter_to_array((0..self.num_containers()).map(|index| {
            let split = if exclusive {
                self.range.split_points().get(index)
            } else {
                index
                    .checked_sub(1)
                    .and_then(|i| self.range.split_points().get(i))
            };
            let Some(value) = split.map(|point| &point.values()[0]) else {
                return unknown.clone();
            };
            if value.is_null() {
                return unknown.clone();
            }
            if exclusive {
                if minimum {
                    value.add_checked(&one)
                } else {
                    value.sub_checked(&one)
                }
                .unwrap_or_else(|_| unknown.clone())
            } else {
                value.clone()
            }
        }))
        .ok()
    }
}

impl PruningStatistics for RangePruningStatistics<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.bounds(column, true)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.bounds(column, false)
    }

    fn num_containers(&self) -> usize {
        self.range.partition_count()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        if !self.matches_column(column) {
            return None;
        }
        // NULL compares equal to a NULL split and belongs after that split.
        // Without such a split it belongs in the first/last sort-order bucket.
        let null_partition = if self.range.ordering()[0].options.nulls_first {
            self.range
                .split_points()
                .iter()
                .take_while(|point| point.values()[0].is_null())
                .count()
        } else {
            self.num_containers() - 1
        };
        Some(Arc::new(UInt64Array::from_iter(
            (0..self.num_containers())
                .map(|index| (index != null_partition).then_some(0)),
        )))
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        None
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{DFSchema, Result, SplitPoint};
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
    use datafusion_expr::{Expr, col, lit};
    use datafusion_physical_expr::create_physical_expr;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};

    use crate::PruningPredicateBuilder;

    fn range(
        values: Vec<ScalarValue>,
        descending: bool,
        nulls_first: bool,
    ) -> RangePartitioning {
        RangePartitioning::try_new(
            LexOrdering::new(vec![PhysicalSortExpr::new(
                Arc::new(PhysicalColumn::new("k", 0)),
                SortOptions {
                    descending,
                    nulls_first,
                },
            )])
            .unwrap(),
            values
                .into_iter()
                .map(|value| SplitPoint::new(vec![value]))
                .collect(),
        )
        .unwrap()
    }

    fn prune(range: &RangePartitioning, expr: Expr, data_type: DataType) -> Vec<bool> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", data_type, true),
            Field::new("other", DataType::Int32, true),
        ]));
        let expr = create_physical_expr(
            &expr,
            &DFSchema::try_from(Arc::clone(&schema)).unwrap(),
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        )
        .unwrap();
        PruningPredicateBuilder::new()
            .with_file_schema(schema)
            .try_build(expr)
            .unwrap()
            .prune(&RangePruningStatistics::new(range))
            .unwrap()
    }

    #[test]
    fn integer_boundaries_and_compound_predicates() {
        let ascending = range(
            [10, 20, 30].into_iter().map(ScalarValue::from).collect(),
            false,
            false,
        );
        for (expr, expected) in [
            (col("k").lt(lit(10_i32)), vec![true, false, false, false]),
            (col("k").gt_eq(lit(30_i32)), vec![false, false, false, true]),
            (col("k").eq(lit(20_i32)), vec![false, false, true, false]),
            (
                col("k").gt_eq(lit(10_i32)).and(col("k").lt(lit(20_i32))),
                vec![false, true, false, false],
            ),
            (
                col("k").lt(lit(10_i32)).or(col("k").gt_eq(lit(30_i32))),
                vec![true, false, false, true],
            ),
            (col("other").eq(lit(5_i32)), vec![true; 4]),
        ] {
            assert_eq!(prune(&ascending, expr, DataType::Int32), expected);
        }
        let descending = range(
            [30, 20, 10].into_iter().map(ScalarValue::from).collect(),
            true,
            false,
        );
        assert_eq!(
            prune(&descending, col("k").eq(lit(20_i32)), DataType::Int32),
            [false, false, true, false]
        );
        assert_eq!(
            prune(&descending, col("k").lt(lit(10_i32)), DataType::Int32),
            [false, false, false, true]
        );
        assert_eq!(
            prune(&descending, col("k").gt(lit(30_i32)), DataType::Int32),
            [true, false, false, false]
        );
    }

    #[test]
    fn all_int8_values_and_nulls_are_conservative() -> Result<()> {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let mut values = if descending {
                    vec![127_i8, 0, -128]
                } else {
                    vec![-128_i8, 0, 127]
                }
                .into_iter()
                .map(ScalarValue::from)
                .collect::<Vec<_>>();
                if nulls_first {
                    values.insert(0, ScalarValue::Int8(None));
                } else {
                    values.push(ScalarValue::Int8(None));
                }
                let range = range(values, descending, nulls_first);
                let compare = |row: Option<i8>, point: &ScalarValue| {
                    let ScalarValue::Int8(point) = point else {
                        unreachable!()
                    };
                    match (row, point) {
                        (None, None) => Ordering::Equal,
                        (None, _) => {
                            if nulls_first {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                        (_, None) => {
                            if nulls_first {
                                Ordering::Greater
                            } else {
                                Ordering::Less
                            }
                        }
                        (Some(row), Some(point)) => {
                            if descending {
                                point.cmp(&row)
                            } else {
                                row.cmp(point)
                            }
                        }
                    }
                };
                for threshold in [-128_i8, -1, 0, 1, 127] {
                    for (expr, matches) in [
                        (
                            col("k").lt(lit(threshold)),
                            (|v, t| v < t) as fn(i8, i8) -> bool,
                        ),
                        (
                            col("k").gt_eq(lit(threshold)),
                            (|v, t| v >= t) as fn(i8, i8) -> bool,
                        ),
                        (
                            col("k").eq(lit(threshold)),
                            (|v, t| v == t) as fn(i8, i8) -> bool,
                        ),
                    ] {
                        let retained = prune(&range, expr, DataType::Int8);
                        for row in i8::MIN..=i8::MAX {
                            if matches(row, threshold) {
                                let index =
                                    range.split_points().partition_point(|point| {
                                        compare(Some(row), &point.values()[0])
                                            != Ordering::Less
                                    });
                                assert!(
                                    retained[index],
                                    "lost {row}, threshold {threshold}, DESC={descending}, NULLS FIRST={nulls_first}"
                                );
                            }
                        }
                    }
                }
                let retained = prune(&range, col("k").is_null(), DataType::Int8);
                let null_index = range.split_points().partition_point(|point| {
                    compare(None, &point.values()[0]) != Ordering::Less
                });
                assert!(retained[null_index]);
                assert_eq!(retained.iter().filter(|keep| **keep).count(), 1);
            }
        }
        Ok(())
    }

    #[test]
    fn sampled_layout_uses_effective_boundaries() {
        let sampled = range(
            [10, 20, 30, 40, 50]
                .into_iter()
                .map(ScalarValue::from)
                .collect(),
            false,
            false,
        );
        let scaled = sampled.scale(3).unwrap();
        assert_eq!(
            prune(&scaled, col("k").lt(lit(20_i32)), DataType::Int32),
            [true, false, false]
        );
    }

    #[test]
    fn unsupported_keys_and_empty_boundaries_stay_unknown() {
        let empty = range(vec![], false, false);
        assert_eq!(
            prune(&empty, col("k").lt(lit(20_i32)), DataType::Int32),
            [true]
        );

        let strings = range(vec![ScalarValue::from("m")], false, false);
        assert_eq!(
            prune(&strings, col("k").eq(lit("z")), DataType::Utf8),
            [true, true]
        );

        let multi_key = RangePartitioning::try_new(
            LexOrdering::new(vec![
                PhysicalSortExpr::new(
                    Arc::new(PhysicalColumn::new("k", 0)),
                    SortOptions::default(),
                ),
                PhysicalSortExpr::new(
                    Arc::new(PhysicalColumn::new("other", 1)),
                    SortOptions::default(),
                ),
            ])
            .unwrap(),
            vec![SplitPoint::new(vec![
                ScalarValue::from(10_i32),
                ScalarValue::from(20_i32),
            ])],
        )
        .unwrap();
        assert_eq!(
            prune(&multi_key, col("k").eq(lit(10_i32)), DataType::Int32),
            [true, true]
        );
    }

    #[test]
    fn unsigned_endpoints_do_not_wrap() {
        let ascending = range(
            vec![ScalarValue::from(0_u8), ScalarValue::from(255_u8)],
            false,
            false,
        );
        assert_eq!(
            prune(&ascending, col("k").gt_eq(lit(255_u8)), DataType::UInt8),
            [true, false, true]
        );
        let descending = range(
            vec![ScalarValue::from(255_u8), ScalarValue::from(0_u8)],
            true,
            false,
        );
        assert_eq!(
            prune(&descending, col("k").lt_eq(lit(0_u8)), DataType::UInt8),
            [true, false, true]
        );
    }
}
