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

//! Simplifier implementation for [`ExprSimplifier::with_guarantees()`]
//!
//! [`ExprSimplifier::with_guarantees()`]: crate::simplify_expressions::expr_simplifier::ExprSimplifier::with_guarantees
use datafusion_common::{tree_node::TreeNodeRewriter, DataFusionError, Result};
use datafusion_expr::{
    expr::InList,
    interval_arithmetic::{Interval, NullableInterval},
    lit, Between, BinaryExpr, Expr,
};
use std::collections::HashMap;

/// Rewrite expressions to incorporate guarantees.
///
/// Guarantees are a mapping from an expression (which currently is always a
/// column reference) to a [NullableInterval]. The interval represents the known
/// possible values of the column. Using these known values, expressions are
/// rewritten so they can be simplified using `ConstEvaluator` and `Simplifier`.
///
/// For example, if we know that a column is not null and has values in the
/// range [1, 10), we can rewrite `x IS NULL` to `false` or `x < 10` to `true`.
///
/// See a full example in [`ExprSimplifier::with_guarantees()`].
///
/// [`ExprSimplifier::with_guarantees()`]: crate::simplify_expressions::expr_simplifier::ExprSimplifier::with_guarantees
pub(crate) struct GuaranteeRewriter<'a> {
    guarantees: HashMap<&'a Expr, &'a NullableInterval>,
}

impl<'a> GuaranteeRewriter<'a> {
    pub fn new(
        guarantees: impl IntoIterator<Item = &'a (Expr, NullableInterval)>,
    ) -> Self {
        Self {
            guarantees: guarantees.into_iter().map(|(k, v)| (k, v)).collect(),
        }
    }
}

impl<'a> TreeNodeRewriter for GuaranteeRewriter<'a> {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        if self.guarantees.is_empty() {
            return Ok(expr);
        }

        match &expr {
            Expr::IsNull(inner) => match self.guarantees.get(inner.as_ref()) {
                Some(NullableInterval::Null { .. }) => Ok(lit(true)),
                Some(NullableInterval::NotNull { .. }) => Ok(lit(false)),
                _ => Ok(expr),
            },
            Expr::IsNotNull(inner) => match self.guarantees.get(inner.as_ref()) {
                Some(NullableInterval::Null { .. }) => Ok(lit(false)),
                Some(NullableInterval::NotNull { .. }) => Ok(lit(true)),
                _ => Ok(expr),
            },
            Expr::Between(Between {
                expr: inner,
                negated,
                low,
                high,
            }) => {
                if let (Some(interval), Expr::Literal(low), Expr::Literal(high)) = (
                    self.guarantees.get(inner.as_ref()),
                    low.as_ref(),
                    high.as_ref(),
                ) {
                    let expr_interval = NullableInterval::NotNull {
                        values: Interval::try_new(low.clone(), high.clone())?,
                    };

                    let contains = expr_interval.contains(*interval)?;

                    if contains.is_certainly_true() {
                        Ok(lit(!negated))
                    } else if contains.is_certainly_false() {
                        Ok(lit(*negated))
                    } else {
                        Ok(expr)
                    }
                } else {
                    Ok(expr)
                }
            }

            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                // We only support comparisons for now
                if !op.is_comparison_operator() {
                    return Ok(expr);
                };

                // Check if this is a comparison between a column and literal
                let (col, op, value) = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(_), Expr::Literal(value)) => (left, *op, value),
                    (Expr::Literal(value), Expr::Column(_)) => {
                        // If we can swap the op, we can simplify the expression
                        if let Some(op) = op.swap() {
                            (right, op, value)
                        } else {
                            return Ok(expr);
                        }
                    }
                    _ => return Ok(expr),
                };

                if let Some(col_interval) = self.guarantees.get(col.as_ref()) {
                    let result =
                        col_interval.apply_operator(&op, &value.clone().into())?;
                    if result.is_certainly_true() {
                        Ok(lit(true))
                    } else if result.is_certainly_false() {
                        Ok(lit(false))
                    } else {
                        Ok(expr)
                    }
                } else {
                    Ok(expr)
                }
            }

            // Columns (if interval is collapsed to a single value)
            Expr::Column(_) => {
                if let Some(col_interval) = self.guarantees.get(&expr) {
                    if let Some(value) = col_interval.single_value() {
                        Ok(lit(value))
                    } else {
                        Ok(expr)
                    }
                } else {
                    Ok(expr)
                }
            }

            Expr::InList(InList {
                expr: inner,
                list,
                negated,
            }) => {
                if let Some(interval) = self.guarantees.get(inner.as_ref()) {
                    // Can remove items from the list that don't match the guarantee
                    let new_list: Vec<Expr> = list
                        .iter()
                        .filter_map(|expr| {
                            if let Expr::Literal(item) = expr {
                                match interval
                                    .contains(&NullableInterval::from(item.clone()))
                                {
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

                    Ok(Expr::InList(InList {
                        expr: inner.clone(),
                        list: new_list,
                        negated: *negated,
                    }))
                } else {
                    Ok(expr)
                }
            }

            _ => Ok(expr),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::{tree_node::TreeNode, ScalarValue};
    use datafusion_expr::{col, lit, Operator};

    #[test]
    fn test_null_handling() {
        // IsNull / IsNotNull can be rewritten to true / false
        let guarantees = vec![
            // Note: AlwaysNull case handled by test_column_single_value test,
            // since it's a special case of a column with a single value.
            (
                col("x"),
                NullableInterval::NotNull {
                    values: Default::default(),
                },
            ),
        ];
        let mut rewriter = GuaranteeRewriter::new(guarantees.iter());

        // x IS NULL => guaranteed false
        let expr = col("x").is_null();
        let output = expr.clone().rewrite(&mut rewriter).unwrap();
        assert_eq!(output, lit(false));

        // x IS NOT NULL => guaranteed true
        let expr = col("x").is_not_null();
        let output = expr.clone().rewrite(&mut rewriter).unwrap();
        assert_eq!(output, lit(true));
    }

    fn validate_simplified_cases<T>(rewriter: &mut GuaranteeRewriter, cases: &[(Expr, T)])
    where
        ScalarValue: From<T>,
        T: Clone,
    {
        for (expr, expected_value) in cases {
            let output = expr.clone().rewrite(rewriter).unwrap();
            let expected = lit(ScalarValue::from(expected_value.clone()));
            assert_eq!(
                output, expected,
                "{} simplified to {}, but expected {}",
                expr, output, expected
            );
        }
    }

    fn validate_unchanged_cases(rewriter: &mut GuaranteeRewriter, cases: &[Expr]) {
        for expr in cases {
            let output = expr.clone().rewrite(rewriter).unwrap();
            assert_eq!(
                &output, expr,
                "{} was simplified to {}, but expected it to be unchanged",
                expr, output
            );
        }
    }

    #[test]
    fn test_inequalities_non_null_bounded() {
        let guarantees = vec![
            // x ∈ [1, 3] (not null)
            (
                col("x"),
                NullableInterval::NotNull {
                    values: Interval::try_new(
                        ScalarValue::Int32(Some(1)),
                        ScalarValue::Int32(Some(3)),
                    )
                    .unwrap(),
                },
            ),
        ];

        let mut rewriter = GuaranteeRewriter::new(guarantees.iter());

        // (original_expr, expected_simplification)
        let simplified_cases = &[
            (col("x").lt_eq(lit(0)), false),
            (col("x").lt_eq(lit(3)), true),
            (col("x").gt(lit(3)), false),
            (col("x").gt(lit(0)), true),
            (col("x").eq(lit(0)), false),
            (col("x").not_eq(lit(0)), true),
            (col("x").between(lit(0), lit(5)), true),
            (col("x").between(lit(5), lit(10)), false),
            (col("x").not_between(lit(0), lit(5)), false),
            (col("x").not_between(lit(5), lit(10)), true),
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
                    right: Box::new(lit(5)),
                }),
                true,
            ),
        ];

        validate_simplified_cases(&mut rewriter, simplified_cases);

        let unchanged_cases = &[
            col("x").gt(lit(2)),
            col("x").lt_eq(lit(2)),
            col("x").eq(lit(2)),
            col("x").not_eq(lit(2)),
            col("x").between(lit(3), lit(5)),
            col("x").not_between(lit(3), lit(10)),
        ];

        validate_unchanged_cases(&mut rewriter, unchanged_cases);
    }

    #[test]
    fn test_inequalities_non_null_unbounded() {
        let guarantees = vec![
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
        let mut rewriter = GuaranteeRewriter::new(guarantees.iter());

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

        validate_simplified_cases(&mut rewriter, simplified_cases);

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

        validate_unchanged_cases(&mut rewriter, unchanged_cases);
    }

    #[test]
    fn test_inequalities_maybe_null() {
        let guarantees = vec![
            // x ∈ ("abc", "def"]? (maybe null)
            (
                col("x"),
                NullableInterval::MaybeNull {
                    values: Interval::try_new(
                        ScalarValue::Utf8(Some("abc".to_string())),
                        ScalarValue::Utf8(Some("def".to_string())),
                    )
                    .unwrap(),
                },
            ),
        ];
        let mut rewriter = GuaranteeRewriter::new(guarantees.iter());

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

        validate_simplified_cases(&mut rewriter, simplified_cases);

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

        validate_unchanged_cases(&mut rewriter, unchanged_cases);
    }

    #[test]
    fn test_column_single_value() {
        let scalars = [
            ScalarValue::Null,
            ScalarValue::Int32(Some(1)),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(None),
            ScalarValue::Utf8(Some("abc".to_string())),
            ScalarValue::LargeUtf8(Some("def".to_string())),
            ScalarValue::Date32(Some(18628)),
            ScalarValue::Date32(None),
            ScalarValue::Decimal128(Some(1000), 19, 2),
        ];

        for scalar in scalars {
            let guarantees = vec![(col("x"), NullableInterval::from(scalar.clone()))];
            let mut rewriter = GuaranteeRewriter::new(guarantees.iter());

            let output = col("x").rewrite(&mut rewriter).unwrap();
            assert_eq!(output, Expr::Literal(scalar.clone()));
        }
    }

    #[test]
    fn test_in_list() {
        let guarantees = vec![
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
        let mut rewriter = GuaranteeRewriter::new(guarantees.iter());

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
            let output = expr.clone().rewrite(&mut rewriter).unwrap();
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
