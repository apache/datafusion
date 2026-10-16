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

use arrow::datatypes::{DataType, Schema};
use datafusion_common::ScalarValue;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_expr::logical_plan::RangePartitioning;
use datafusion_expr::{Expr, Operator};
use std::cmp::Ordering;

/// Discard files only when a simple filter cannot match the declared range.
/// Keep the empty group in place: its index is meaningful to range joins.
pub(crate) fn prune_range_file_groups(
    file_groups: &mut [FileGroup],
    range: &RangePartitioning,
    filters: &[Expr],
    schema: &Schema,
) {
    let [sort_expr] = range.ordering() else {
        return;
    };
    let Expr::Column(column) = sort_expr.expr.as_ref() else {
        return;
    };
    let Ok(field) = schema.field_with_name(&column.name) else {
        return;
    };
    let data_type = field.data_type();
    // These types have the same total order in SQL comparisons and split points.
    if !matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    ) {
        return;
    }

    for (index, file_group) in file_groups.iter_mut().enumerate() {
        let lower = index
            .checked_sub(1)
            .and_then(|i| range.split_points().get(i))
            .and_then(|split| split.values().first());
        let upper = range
            .split_points()
            .get(index)
            .and_then(|split| split.values().first());
        let (min, max) = if sort_expr.asc {
            (
                lower.map(|value| (value, false)),
                upper.map(|value| (value, true)),
            )
        } else {
            (
                upper.map(|value| (value, true)),
                lower.map(|value| (value, false)),
            )
        };

        if filters
            .iter()
            .any(|filter| !may_match(filter, &column.name, data_type, min, max))
        {
            *file_group = FileGroup::new(vec![]);
        }
    }
}

type Bound<'a> = Option<(&'a ScalarValue, bool)>;

fn may_match(
    expr: &Expr,
    column: &str,
    data_type: &DataType,
    min: Bound<'_>,
    max: Bound<'_>,
) -> bool {
    let Expr::BinaryExpr(binary) = expr else {
        return true;
    };
    match binary.op {
        Operator::And => {
            may_match(&binary.left, column, data_type, min, max)
                && may_match(&binary.right, column, data_type, min, max)
        }
        Operator::Or => {
            may_match(&binary.left, column, data_type, min, max)
                || may_match(&binary.right, column, data_type, min, max)
        }
        op => {
            let (value, op) = match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(key), Expr::Literal(value, _)) if key.name == column => {
                    (value, op)
                }
                (Expr::Literal(value, _), Expr::Column(key)) if key.name == column => {
                    let inverse = match op {
                        Operator::Lt => Operator::Gt,
                        Operator::LtEq => Operator::GtEq,
                        Operator::Gt => Operator::Lt,
                        Operator::GtEq => Operator::LtEq,
                        _ => op,
                    };
                    (value, inverse)
                }
                _ => return true,
            };
            if value.is_null() || value.data_type() != *data_type {
                return true;
            }
            let min_cmp = compare_bound(min, value, data_type);
            let max_cmp = compare_bound(max, value, data_type);
            match op {
                Operator::Eq => {
                    !matches!(
                        min_cmp,
                        Some((Ordering::Greater, _)) | Some((Ordering::Equal, true))
                    ) && !matches!(
                        max_cmp,
                        Some((Ordering::Less, _)) | Some((Ordering::Equal, true))
                    )
                }
                Operator::Lt => {
                    !matches!(min_cmp, Some((Ordering::Greater | Ordering::Equal, _)))
                }
                Operator::LtEq => !matches!(
                    min_cmp,
                    Some((Ordering::Greater, _)) | Some((Ordering::Equal, true))
                ),
                Operator::Gt => {
                    !matches!(max_cmp, Some((Ordering::Less | Ordering::Equal, _)))
                }
                Operator::GtEq => !matches!(
                    max_cmp,
                    Some((Ordering::Less, _)) | Some((Ordering::Equal, true))
                ),
                _ => true,
            }
        }
    }
}

fn compare_bound(
    bound: Bound<'_>,
    value: &ScalarValue,
    data_type: &DataType,
) -> Option<(Ordering, bool)> {
    let (bound, exclusive) = bound?;
    if bound.is_null() || bound.data_type() != *data_type {
        return None;
    }
    bound.try_cmp(value).ok().map(|cmp| (cmp, exclusive))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion_common::SplitPoint;
    use datafusion_datasource::PartitionedFile;
    use datafusion_expr::expr::Sort;
    use datafusion_expr::{col, lit};

    fn retained(expr: Expr, asc: bool) -> Vec<bool> {
        let split_values = if asc { [10, 20, 30] } else { [30, 20, 10] };
        let range = RangePartitioning::try_new(
            vec![Sort::new(col("k"), asc, false)],
            split_values
                .into_iter()
                .map(|value| SplitPoint::new(vec![ScalarValue::Int32(Some(value))]))
                .collect(),
        )
        .unwrap();
        let schema = Schema::new(vec![Field::new("k", DataType::Int32, true)]);
        let mut groups = (0..4)
            .map(|index| {
                FileGroup::new(vec![PartitionedFile::new(format!("part-{index}"), 1)])
            })
            .collect::<Vec<_>>();
        prune_range_file_groups(&mut groups, &range, &[expr], &schema);
        groups.iter().map(|group| !group.is_empty()).collect()
    }

    #[test]
    fn ascending_boundaries_and_literal_on_left() {
        assert_eq!(
            retained(col("k").lt(lit(10_i32)), true),
            [true, false, false, false]
        );
        assert_eq!(
            retained(col("k").lt_eq(lit(10_i32)), true),
            [true, true, false, false]
        );
        assert_eq!(
            retained(col("k").eq(lit(10_i32)), true),
            [false, true, false, false]
        );
        assert_eq!(
            retained(col("k").gt(lit(30_i32)), true),
            [false, false, false, true]
        );
        assert_eq!(
            retained(col("k").gt_eq(lit(30_i32)), true),
            [false, false, false, true]
        );
        assert_eq!(
            retained(lit(10_i32).gt(col("k")), true),
            [true, false, false, false]
        );
    }

    #[test]
    fn descending_boundaries() {
        assert_eq!(
            retained(col("k").lt(lit(10_i32)), false),
            [false, false, false, true]
        );
        assert_eq!(
            retained(col("k").eq(lit(20_i32)), false),
            [false, false, true, false]
        );
        assert_eq!(
            retained(col("k").gt(lit(30_i32)), false),
            [true, false, false, false]
        );
    }

    #[test]
    fn compound_and_unsupported_predicates() {
        assert_eq!(
            retained(
                col("k").gt_eq(lit(10_i32)).and(col("k").lt(lit(20_i32))),
                true
            ),
            [false, true, false, false]
        );
        assert_eq!(
            retained(
                col("k").lt(lit(10_i32)).or(col("k").gt_eq(lit(30_i32))),
                true
            ),
            [true, false, false, true]
        );
        assert_eq!(retained(col("other").lt(lit(10_i32)), true), [true; 4]);
        assert_eq!(retained(col("k").lt(lit(10_i64)), true), [true; 4]);
    }

    #[test]
    fn null_split_point_is_not_used_as_a_numeric_bound() {
        let range = RangePartitioning::try_new(
            vec![Sort::new(col("k"), true, true)],
            vec![
                SplitPoint::new(vec![ScalarValue::Int32(None)]),
                SplitPoint::new(vec![ScalarValue::Int32(Some(10))]),
            ],
        )
        .unwrap();
        let schema = Schema::new(vec![Field::new("k", DataType::Int32, true)]);
        let mut groups = (0..3)
            .map(|index| {
                FileGroup::new(vec![PartitionedFile::new(format!("part-{index}"), 1)])
            })
            .collect::<Vec<_>>();
        prune_range_file_groups(
            &mut groups,
            &range,
            &[col("k").lt(lit(10_i32))],
            &schema,
        );
        assert_eq!(
            groups.iter().map(|g| !g.is_empty()).collect::<Vec<_>>(),
            [true, true, false]
        );
    }
}
