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

//! Rewrite expressions based on external expression value range guarantees.

use std::borrow::Cow;

use crate::{expr::InList, lit, Between, BinaryExpr, Expr};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{DataFusionError, HashMap, Result};
use datafusion_expr_common::interval_arithmetic::{Interval, NullableInterval};

struct GuaranteeRewriter<'a> {
    guarantees: &'a HashMap<&'a Expr, &'a NullableInterval>,
}

/// Rewrite expressions to incorporate guarantees.
///
/// Guarantees are a mapping from an expression (which currently is always a
/// column reference) to a [NullableInterval]. The interval represents the known
/// possible values of the column.
///
/// For example, if we know that a column is not null and has values in the
/// range [1, 10), we can rewrite `x IS NULL` to `false` or `x < 10` to `true`.
///
/// If the set of guarantees will be used to rewrite multiple expressions consider using
/// [rewrite_with_guarantees_map] instead.
pub fn rewrite_with_guarantees<'a>(
    expr: Expr,
    guarantees: impl IntoIterator<Item = &'a (Expr, NullableInterval)>,
) -> Result<Transformed<Expr>> {
    let guarantees_map: HashMap<&Expr, &NullableInterval> =
        guarantees.into_iter().map(|(k, v)| (k, v)).collect();
    rewrite_with_guarantees_map(expr, &guarantees_map)
}

/// Rewrite expressions to incorporate guarantees.
///
/// Guarantees are a mapping from an expression (which currently is always a
/// column reference) to a [NullableInterval]. The interval represents the known
/// possible values of the column.
///
/// For example, if we know that a column is not null and has values in the
/// range [1, 10), we can rewrite `x IS NULL` to `false` or `x < 10` to `true`.
pub fn rewrite_with_guarantees_map<'a>(
    expr: Expr,
    guarantees: &'a HashMap<&'a Expr, &'a NullableInterval>,
) -> Result<Transformed<Expr>> {
    let mut rewriter = GuaranteeRewriter { guarantees };
    expr.rewrite(&mut rewriter)
}

impl TreeNodeRewriter for GuaranteeRewriter<'_> {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if self.guarantees.is_empty() {
            return Ok(Transformed::no(expr));
        }

        let new_expr = match &expr {
            Expr::IsNull(inner) => match self.guarantees.get(inner.as_ref()) {
                Some(NullableInterval::Null { .. }) => Some(lit(true)),
                Some(NullableInterval::NotNull { .. }) => Some(lit(false)),
                _ => None,
            },
            Expr::IsNotNull(inner) => match self.guarantees.get(inner.as_ref()) {
                Some(NullableInterval::Null { .. }) => Some(lit(false)),
                Some(NullableInterval::NotNull { .. }) => Some(lit(true)),
                _ => None,
            },
            Expr::Between(b) => self.rewrite_between(b)?,
            Expr::BinaryExpr(b) => self.rewrite_binary_expr(&b)?,
            Expr::InList(i) => self.rewrite_inlist(i)?,
            _ => None,
        };

        if let Some(e) = new_expr {
            return Ok(Transformed::yes(e));
        }

        match self.guarantees.get(&expr) {
            Some(interval) => {
                // If an expression collapses to a single value, replace it with a literal
                if let Some(value) = interval.single_value() {
                    Ok(Transformed::yes(lit(value)))
                } else {
                    Ok(Transformed::no(expr))
                }
            }
            _ => Ok(Transformed::no(expr)),
        }
    }
}

impl GuaranteeRewriter<'_> {
    fn rewrite_between(
        &mut self,
        between: &Between,
    ) -> Result<Option<Expr>, DataFusionError> {
        let (Some(interval), Expr::Literal(low, _), Expr::Literal(high, _)) = (
            self.guarantees.get(between.expr.as_ref()),
            between.low.as_ref(),
            between.high.as_ref(),
        ) else {
            return Ok(None);
        };

        let Ok(values) = Interval::try_new(low.clone(), high.clone()) else {
            // If we can't create an interval from the literals, be conservative and simply leave
            // the expression unmodified.
            return Ok(None);
        };

        let expr_interval = NullableInterval::NotNull { values };

        let contains = expr_interval.contains(*interval)?;

        if contains.is_certainly_true() {
            Ok(Some(lit(!between.negated)))
        } else if contains.is_certainly_false() {
            Ok(Some(lit(between.negated)))
        } else {
            Ok(None)
        }
    }

    fn rewrite_binary_expr(
        &mut self,
        b: &&BinaryExpr,
    ) -> Result<Option<Expr>, DataFusionError> {
        // The left or right side of expression might either have a guarantee
        // or be a literal. Either way, we can resolve them to a NullableInterval.
        let left_interval = self
            .guarantees
            .get(b.left.as_ref())
            .map(|interval| Cow::Borrowed(*interval))
            .or_else(|| {
                if let Expr::Literal(value, _) = b.left.as_ref() {
                    Some(Cow::Owned(value.clone().into()))
                } else {
                    None
                }
            });
        let right_interval = self
            .guarantees
            .get(b.right.as_ref())
            .map(|interval| Cow::Borrowed(*interval))
            .or_else(|| {
                if let Expr::Literal(value, _) = b.right.as_ref() {
                    Some(Cow::Owned(value.clone().into()))
                } else {
                    None
                }
            });

        Ok(match (left_interval, right_interval) {
            (Some(left_interval), Some(right_interval)) => {
                let result =
                    left_interval.apply_operator(&b.op, right_interval.as_ref())?;
                if result.is_certainly_true() {
                    Some(lit(true))
                } else if result.is_certainly_false() {
                    Some(lit(false))
                } else {
                    None
                }
            }
            _ => None,
        })
    }

    fn rewrite_inlist(&mut self, i: &InList) -> Result<Option<Expr>, DataFusionError> {
        let Some(interval) = self.guarantees.get(i.expr.as_ref()) else {
            return Ok(None);
        };

        // Can remove items from the list that don't match the guarantee
        let new_list: Vec<Expr> = i
            .list
            .iter()
            .filter_map(|expr| {
                if let Expr::Literal(item, _) = expr {
                    match interval.contains(NullableInterval::from(item.clone())) {
                        // If we know for certain the value isn't in the column's interval,
                        // we can skip checking it.
                        Ok(interval) if interval.is_certainly_false() => None,
                        Ok(_) => Some(Ok(expr.clone())),
                        Err(e) => Some(Err(e)),
                    }
                } else {
                    Some(Ok(expr.clone()))
                }
            })
            .collect::<Result<_, DataFusionError>>()?;

        Ok(Some(Expr::InList(InList {
            expr: i.expr.clone(),
            list: new_list,
            negated: i.negated,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{col, Operator};
    use arrow::datatypes::DataType;
    use datafusion_common::tree_node::TransformedResult;
    use datafusion_common::ScalarValue;

    #[test]
    fn test_not_null_guarantee() {
        // IsNull / IsNotNull can be rewritten to true / false
        let guarantees = [
            // Note: AlwaysNull case handled by test_column_single_value test,
            // since it's a special case of a column with a single value.
            (
                col("x"),
                NullableInterval::NotNull {
                    values: Interval::make_unbounded(&DataType::Int32).unwrap(),
                },
            ),
        ];

        // x IS NULL => guaranteed false
        let is_null_cases = vec![
            (col("x").is_null(), Some(lit(false))),
            (col("x").is_not_null(), Some(lit(true))),
            (col("x").between(lit(1), lit(2)), None),
            (col("x").between(lit(1), lit(-2)), None),
        ];

        for case in is_null_cases {
            let output = rewrite_with_guarantees(case.0.clone(), guarantees.iter())
                .data()
                .unwrap();
            let expected = match case.1 {
                None => case.0,
                Some(expected) => expected,
            };

            assert_eq!(output, expected);
        }
    }

    fn validate_simplified_cases<T>(
        guarantees: &[(Expr, NullableInterval)],
        cases: &[(Expr, T)],
    ) where
        ScalarValue: From<T>,
        T: Clone,
    {
        for (expr, expected_value) in cases {
            let output = rewrite_with_guarantees(expr.clone(), guarantees.iter())
                .data()
                .unwrap();
            let expected = lit(ScalarValue::from(expected_value.clone()));
            assert_eq!(
                output, expected,
                "{expr} simplified to {output}, but expected {expected}"
            );
        }
    }

    fn validate_unchanged_cases(guarantees: &[(Expr, NullableInterval)], cases: &[Expr]) {
        for expr in cases {
            let output = rewrite_with_guarantees(expr.clone(), guarantees.iter())
                .data()
                .unwrap();
            assert_eq!(
                &output, expr,
                "{expr} was simplified to {output}, but expected it to be unchanged"
            );
        }
    }

    #[test]
    fn test_inequalities_non_null_unbounded() {
        let guarantees = [
            // y ∈ [2021-01-01, ∞) (not null)
            (
                col("x"),
                NullableInterval::NotNull {
                    values: Interval::try_new(
                        ScalarValue::Date32(Some(18628)),
                        ScalarValue::Date32(None),
                    )
                    .unwrap(),
                },
            ),
        ];

        // (original_expr, expected_simplification)
        let simplified_cases = &[
            (col("x").lt(lit(ScalarValue::Date32(Some(18628)))), false),
            (col("x").lt_eq(lit(ScalarValue::Date32(Some(17000)))), false),
            (col("x").gt(lit(ScalarValue::Date32(Some(18627)))), true),
            (col("x").gt_eq(lit(ScalarValue::Date32(Some(18628)))), true),
            (col("x").eq(lit(ScalarValue::Date32(Some(17000)))), false),
            (col("x").not_eq(lit(ScalarValue::Date32(Some(17000)))), true),
            (
                col("x").between(
                    lit(ScalarValue::Date32(Some(16000))),
                    lit(ScalarValue::Date32(Some(17000))),
                ),
                false,
            ),
            (
                col("x").not_between(
                    lit(ScalarValue::Date32(Some(16000))),
                    lit(ScalarValue::Date32(Some(17000))),
                ),
                true,
            ),
            (
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(col("x")),
                    op: Operator::IsDistinctFrom,
                    right: Box::new(lit(ScalarValue::Null)),
                }),
                true,
            ),
            (
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(col("x")),
                    op: Operator::IsDistinctFrom,
                    right: Box::new(lit(ScalarValue::Date32(Some(17000)))),
                }),
                true,
            ),
        ];

        validate_simplified_cases(&guarantees, simplified_cases);

        let unchanged_cases = &[
            col("x").lt(lit(ScalarValue::Date32(Some(19000)))),
            col("x").lt_eq(lit(ScalarValue::Date32(Some(19000)))),
            col("x").gt(lit(ScalarValue::Date32(Some(19000)))),
            col("x").gt_eq(lit(ScalarValue::Date32(Some(19000)))),
            col("x").eq(lit(ScalarValue::Date32(Some(19000)))),
            col("x").not_eq(lit(ScalarValue::Date32(Some(19000)))),
            col("x").between(
                lit(ScalarValue::Date32(Some(18000))),
                lit(ScalarValue::Date32(Some(19000))),
            ),
            col("x").not_between(
                lit(ScalarValue::Date32(Some(18000))),
                lit(ScalarValue::Date32(Some(19000))),
            ),
        ];

        validate_unchanged_cases(&guarantees, unchanged_cases);
    }

    #[test]
    fn test_inequalities_maybe_null() {
        let guarantees = [
            // x ∈ ("abc", "def"]? (maybe null)
            (
                col("x"),
                NullableInterval::MaybeNull {
                    values: Interval::try_new(
                        ScalarValue::from("abc"),
                        ScalarValue::from("def"),
                    )
                    .unwrap(),
                },
            ),
        ];

        // (original_expr, expected_simplification)
        let simplified_cases = &[
            (
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(col("x")),
                    op: Operator::IsDistinctFrom,
                    right: Box::new(lit("z")),
                }),
                true,
            ),
            (
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(col("x")),
                    op: Operator::IsNotDistinctFrom,
                    right: Box::new(lit("z")),
                }),
                false,
            ),
        ];

        validate_simplified_cases(&guarantees, simplified_cases);

        let unchanged_cases = &[
            col("x").lt(lit("z")),
            col("x").lt_eq(lit("z")),
            col("x").gt(lit("a")),
            col("x").gt_eq(lit("a")),
            col("x").eq(lit("abc")),
            col("x").not_eq(lit("a")),
            col("x").between(lit("a"), lit("z")),
            col("x").not_between(lit("a"), lit("z")),
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(col("x")),
                op: Operator::IsDistinctFrom,
                right: Box::new(lit(ScalarValue::Null)),
            }),
        ];

        validate_unchanged_cases(&guarantees, unchanged_cases);
    }

    #[test]
    fn test_column_single_value() {
        let scalars = [
            ScalarValue::Null,
            ScalarValue::Int32(Some(1)),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(None),
            ScalarValue::from("abc"),
            ScalarValue::LargeUtf8(Some("def".to_string())),
            ScalarValue::Date32(Some(18628)),
            ScalarValue::Date32(None),
            ScalarValue::Decimal128(Some(1000), 19, 2),
        ];

        for scalar in scalars {
            let guarantees = [(col("x"), NullableInterval::from(scalar.clone()))];

            let output = rewrite_with_guarantees(col("x"), guarantees.iter())
                .data()
                .unwrap();
            assert_eq!(output, Expr::Literal(scalar.clone(), None));
        }
    }

    #[test]
    fn test_in_list() {
        let guarantees = [
            // x ∈ [1, 10] (not null)
            (
                col("x"),
                NullableInterval::NotNull {
                    values: Interval::try_new(
                        ScalarValue::Int32(Some(1)),
                        ScalarValue::Int32(Some(10)),
                    )
                    .unwrap(),
                },
            ),
        ];

        // These cases should be simplified so the list doesn't contain any
        // values the guarantee says are outside the range.
        // (column_name, starting_list, negated, expected_list)
        let cases = &[
            // x IN (9, 11) => x IN (9)
            ("x", vec![9, 11], false, vec![9]),
            // x IN (10, 2) => x IN (10, 2)
            ("x", vec![10, 2], false, vec![10, 2]),
            // x NOT IN (9, 11) => x NOT IN (9)
            ("x", vec![9, 11], true, vec![9]),
            // x NOT IN (0, 22) => x NOT IN ()
            ("x", vec![0, 22], true, vec![]),
        ];

        for (column_name, starting_list, negated, expected_list) in cases {
            let expr = col(*column_name).in_list(
                starting_list
                    .iter()
                    .map(|v| lit(ScalarValue::Int32(Some(*v))))
                    .collect(),
                *negated,
            );
            let output = rewrite_with_guarantees(expr.clone(), guarantees.iter())
                .data()
                .unwrap();
            let expected_list = expected_list
                .iter()
                .map(|v| lit(ScalarValue::Int32(Some(*v))))
                .collect();
            assert_eq!(
                output,
                Expr::InList(InList {
                    expr: Box::new(col(*column_name)),
                    list: expected_list,
                    negated: *negated,
                })
            );
        }
    }
}
