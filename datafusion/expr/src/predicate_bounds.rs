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

use crate::{Between, BinaryExpr, Expr, ExprSchemable};
use arrow::datatypes::DataType;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{ExprSchema, Result, ScalarValue};
use datafusion_expr_common::interval_arithmetic::NullableInterval;
use datafusion_expr_common::operator::Operator;

/// Computes the output interval for the given boolean expression based on statically
/// available information.
///
/// # Arguments
///
/// * `predicate` - The boolean expression to analyze
/// * `is_null` - A callback function that provides additional nullability information for
///   expressions. When called with an expression, it should return:
///   - `Some(true)` if the expression is known to evaluate to NULL
///   - `Some(false)` if the expression is known to NOT evaluate to NULL
///   - `None` if the nullability cannot be determined
///
///   This callback allows the caller to provide context-specific knowledge about expression
///   nullability that cannot be determined from the schema alone. For example, it can be used
///   to indicate that a particular column reference is known to be NULL in a specific context,
///   or that certain expressions will never be NULL based on runtime constraints.
///
/// * `input_schema` - Schema information for resolving expression types and nullability
///
/// # Return Value
///
/// The function returns a [NullableInterval] that describes the possible boolean values the
/// predicate can evaluate to.
///
pub(super) fn evaluate_bounds(
    predicate: &Expr,
    input_schema: &dyn ExprSchema,
) -> Result<NullableInterval> {
    let evaluator = PredicateBoundsEvaluator { input_schema };
    evaluator.evaluate_bounds(predicate)
}

struct PredicateBoundsEvaluator<'a> {
    input_schema: &'a dyn ExprSchema,
}

impl PredicateBoundsEvaluator<'_> {
    /// Derives the bounds of the given boolean expression
    fn evaluate_bounds(&self, predicate: &Expr) -> Result<NullableInterval> {
        Ok(match predicate {
            Expr::Literal(scalar, _) => {
                // Interpret literals as boolean, coercing if necessary
                match scalar {
                    ScalarValue::Null => NullableInterval::UNKNOWN,
                    ScalarValue::Boolean(b) => match b {
                        Some(true) => NullableInterval::TRUE,
                        Some(false) => NullableInterval::FALSE,
                        None => NullableInterval::UNKNOWN,
                    },
                    _ => {
                        let b = Expr::Literal(scalar.cast_to(&DataType::Boolean)?, None);
                        self.evaluate_bounds(&b)?
                    }
                }
            }
            Expr::IsNull(e) => {
                // If `e` is not nullable, then `e IS NULL` is provably false
                if !e.nullable(self.input_schema)? {
                    NullableInterval::FALSE
                } else {
                    match e.get_type(self.input_schema)? {
                        // If `e` is a boolean expression, check if `e` is provably 'unknown'.
                        DataType::Boolean => self.evaluate_bounds(e)?.is_unknown()?,
                        // If `e` is not a boolean expression, check if `e` is provably null
                        _ => self.is_null(e),
                    }
                }
            }
            Expr::IsNotNull(e) => {
                // If `e` is not nullable, then `e IS NOT NULL` is provably true
                if !e.nullable(self.input_schema)? {
                    NullableInterval::TRUE
                } else {
                    match e.get_type(self.input_schema)? {
                        // If `e` is a boolean expression, try to evaluate it and test for not unknown
                        DataType::Boolean => {
                            self.evaluate_bounds(e)?.is_unknown()?.not()?
                        }
                        // If `e` is not a boolean expression, check if `e` is provably null
                        _ => self.is_null(e).not()?,
                    }
                }
            }
            Expr::IsTrue(e) => self.evaluate_bounds(e)?.is_true()?,
            Expr::IsNotTrue(e) => self.evaluate_bounds(e)?.is_true()?.not()?,
            Expr::IsFalse(e) => self.evaluate_bounds(e)?.is_false()?,
            Expr::IsNotFalse(e) => self.evaluate_bounds(e)?.is_false()?.not()?,
            Expr::IsUnknown(e) => self.evaluate_bounds(e)?.is_unknown()?,
            Expr::IsNotUnknown(e) => self.evaluate_bounds(e)?.is_unknown()?.not()?,
            Expr::Not(e) => self.evaluate_bounds(e)?.not()?,
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => NullableInterval::and(
                &self.evaluate_bounds(left)?,
                &self.evaluate_bounds(right)?,
            )?,
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Or,
                right,
            }) => NullableInterval::or(
                &self.evaluate_bounds(left)?,
                &self.evaluate_bounds(right)?,
            )?,
            e => {
                let is_null = self.is_null(e);

                // If an expression is null, then it's value is UNKNOWN
                let maybe_null =
                    is_null.contains_value(ScalarValue::Boolean(Some(true)))?;

                let maybe_not_null =
                    is_null.contains_value(ScalarValue::Boolean(Some(false)))?;

                match (maybe_null, maybe_not_null) {
                    (true, true) | (false, false) => NullableInterval::ANY_TRUTH_VALUE,
                    (true, false) => NullableInterval::UNKNOWN,
                    (false, true) => NullableInterval::TRUE_OR_FALSE,
                }
            }
        })
    }

    /// Determines if the given expression can evaluate to `NULL`.
    ///
    /// This method only returns sets containing `TRUE`, `FALSE`, or both.
    fn is_null(&self, expr: &Expr) -> NullableInterval {
        // Fast path for literals
        if let Expr::Literal(scalar, _) = expr {
            if scalar.is_null() {
                return NullableInterval::TRUE;
            } else {
                return NullableInterval::FALSE;
            }
        }

        // If `expr` is not nullable, we can be certain `expr` is not null
        if let Ok(false) = expr.nullable(self.input_schema) {
            return NullableInterval::FALSE;
        }

        // `expr` is nullable, so our default answer for `is null` is going to be `{ TRUE, FALSE }`.
        // Try to see if we can narrow it down to just one option.
        match expr {
            Expr::BinaryExpr(BinaryExpr { op, .. }) if op.returns_null_on_null() => {
                self.is_null_if_any_child_null(expr)
            }
            Expr::Alias(_)
            | Expr::Cast(_)
            | Expr::Like(_)
            | Expr::Negative(_)
            | Expr::Not(_)
            | Expr::SimilarTo(_) => self.is_null_if_any_child_null(expr),
            Expr::Between(Between {
                expr, low, high, ..
            }) if self.is_null(expr).is_certainly_true()
                || (self.is_null(low.as_ref()).is_certainly_true()
                    && self.is_null(high.as_ref()).is_certainly_true()) =>
            {
                // Between is always null if the left side is null
                // or both the low and high bounds are null
                NullableInterval::TRUE
            }
            _ => NullableInterval::TRUE_OR_FALSE,
        }
    }

    fn is_null_if_any_child_null(&self, expr: &Expr) -> NullableInterval {
        // These expressions are null if any of their direct children is null
        // If any child is inconclusive, the result for this expression is also inconclusive
        let mut is_null = NullableInterval::FALSE;

        let _ = expr.apply_children(|child| {
            let child_is_null = self.is_null(child);

            if child_is_null.contains_value(ScalarValue::Boolean(Some(true)))? {
                // If a child might be null, then the result may also be null
                is_null = NullableInterval::TRUE_OR_FALSE;
            }

            if !child_is_null.contains_value(ScalarValue::Boolean(Some(false)))? {
                // If the child is never not null, then the result can also never be not null
                // and we can stop traversing the children
                is_null = NullableInterval::TRUE;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        });

        is_null
    }
}

#[cfg(test)]
mod tests {
    use crate::expr::ScalarFunction;
    use crate::predicate_bounds::evaluate_bounds;
    use crate::{
        binary_expr, col, create_udf, is_false, is_not_false, is_not_null, is_not_true,
        is_not_unknown, is_null, is_true, is_unknown, lit, not, Expr,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{DFSchema, Result, ScalarValue};
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use datafusion_expr_common::interval_arithmetic::NullableInterval;
    use datafusion_expr_common::operator::Operator::{And, Eq, Or};
    use datafusion_expr_common::signature::Volatility;
    use std::ops::Neg;
    use std::sync::Arc;

    fn eval_bounds(predicate: &Expr) -> Result<NullableInterval> {
        let schema = DFSchema::try_from(Schema::empty())?;
        evaluate_bounds(predicate, &schema)
    }

    #[test]
    fn evaluate_bounds_literal() {
        #[rustfmt::skip]
        let cases = vec![
            (lit(ScalarValue::Null), NullableInterval::UNKNOWN),
            (lit(false), NullableInterval::FALSE),
            (lit(true), NullableInterval::TRUE),
            (lit(0), NullableInterval::FALSE),
            (lit(1), NullableInterval::TRUE),
            (lit(ScalarValue::Utf8(None)), NullableInterval::UNKNOWN),
        ];

        for case in cases {
            assert_eq!(
                eval_bounds(&case.0).unwrap(),
                case.1,
                "Failed for {}",
                case.0
            );
        }

        assert!(eval_bounds(&lit("foo")).is_err());
    }

    #[test]
    fn evaluate_bounds_and() {
        let null = lit(ScalarValue::Null);
        let zero = lit(0);
        let one = lit(1);
        let t = lit(true);
        let f = lit(false);
        let func = make_scalar_func_expr();

        #[rustfmt::skip]
        let cases = vec![
            (binary_expr(null.clone(), And, null.clone()), NullableInterval::UNKNOWN),
            (binary_expr(null.clone(), And, one.clone()), NullableInterval::UNKNOWN),
            (binary_expr(null.clone(), And, zero.clone()), NullableInterval::FALSE),
            (binary_expr(one.clone(), And, one.clone()), NullableInterval::TRUE),
            (binary_expr(one.clone(), And, zero.clone()), NullableInterval::FALSE),
            (binary_expr(null.clone(), And, t.clone()), NullableInterval::UNKNOWN),
            (binary_expr(t.clone(), And, null.clone()), NullableInterval::UNKNOWN),
            (binary_expr(null.clone(), And, f.clone()), NullableInterval::FALSE),
            (binary_expr(f.clone(), And, null.clone()), NullableInterval::FALSE),
            (binary_expr(t.clone(), And, t.clone()), NullableInterval::TRUE),
            (binary_expr(t.clone(), And, f.clone()), NullableInterval::FALSE),
            (binary_expr(f.clone(), And, t.clone()), NullableInterval::FALSE),
            (binary_expr(f.clone(), And, f.clone()), NullableInterval::FALSE),
            (binary_expr(t.clone(), And, func.clone()), NullableInterval::ANY_TRUTH_VALUE),
            (binary_expr(func.clone(), And, t.clone()), NullableInterval::ANY_TRUTH_VALUE),
            (binary_expr(f.clone(), And, func.clone()), NullableInterval::FALSE),
            (binary_expr(func.clone(), And, f.clone()), NullableInterval::FALSE),
            (binary_expr(null.clone(), And, func.clone()), NullableInterval::FALSE_OR_UNKNOWN),
            (binary_expr(func.clone(), And, null.clone()), NullableInterval::FALSE_OR_UNKNOWN),
        ];

        for case in cases {
            assert_eq!(
                eval_bounds(&case.0).unwrap(),
                case.1,
                "Failed for {}",
                case.0
            );
        }
    }

    #[test]
    fn evaluate_bounds_or() {
        let null = lit(ScalarValue::Null);
        let zero = lit(0);
        let one = lit(1);
        let t = lit(true);
        let f = lit(false);
        let func = make_scalar_func_expr();

        #[rustfmt::skip]
        let cases = vec![
            (binary_expr(null.clone(), Or, null.clone()), NullableInterval::UNKNOWN),
            (binary_expr(null.clone(), Or, one.clone()), NullableInterval::TRUE),
            (binary_expr(null.clone(), Or, zero.clone()), NullableInterval::UNKNOWN),
            (binary_expr(one.clone(), Or, one.clone()), NullableInterval::TRUE),
            (binary_expr(one.clone(), Or, zero.clone()), NullableInterval::TRUE),
            (binary_expr(null.clone(), Or, t.clone()), NullableInterval::TRUE),
            (binary_expr(t.clone(), Or, null.clone()), NullableInterval::TRUE),
            (binary_expr(null.clone(), Or, f.clone()), NullableInterval::UNKNOWN),
            (binary_expr(f.clone(), Or, null.clone()), NullableInterval::UNKNOWN),
            (binary_expr(t.clone(), Or, t.clone()), NullableInterval::TRUE),
            (binary_expr(t.clone(), Or, f.clone()), NullableInterval::TRUE),
            (binary_expr(f.clone(), Or, t.clone()), NullableInterval::TRUE),
            (binary_expr(f.clone(), Or, f.clone()), NullableInterval::FALSE),
            (binary_expr(t.clone(), Or, func.clone()), NullableInterval::TRUE),
            (binary_expr(func.clone(), Or, t.clone()), NullableInterval::TRUE),
            (binary_expr(f.clone(), Or, func.clone()), NullableInterval::ANY_TRUTH_VALUE),
            (binary_expr(func.clone(), Or, f.clone()), NullableInterval::ANY_TRUTH_VALUE),
            (binary_expr(null.clone(), Or, func.clone()), NullableInterval::TRUE_OR_UNKNOWN),
            (binary_expr(func.clone(), Or, null.clone()), NullableInterval::TRUE_OR_UNKNOWN),
        ];

        for case in cases {
            assert_eq!(
                eval_bounds(&case.0).unwrap(),
                case.1,
                "Failed for {}",
                case.0
            );
        }
    }

    #[test]
    fn evaluate_bounds_not() {
        let null = lit(ScalarValue::Null);
        let zero = lit(0);
        let one = lit(1);
        let t = lit(true);
        let f = lit(false);
        let func = make_scalar_func_expr();

        #[rustfmt::skip]
        let cases = vec![
            (not(null.clone()), NullableInterval::UNKNOWN),
            (not(one.clone()), NullableInterval::FALSE),
            (not(zero.clone()), NullableInterval::TRUE),
            (not(t.clone()), NullableInterval::FALSE),
            (not(f.clone()), NullableInterval::TRUE),
            (not(func.clone()), NullableInterval::ANY_TRUTH_VALUE),
        ];

        for case in cases {
            assert_eq!(
                eval_bounds(&case.0).unwrap(),
                case.1,
                "Failed for {}",
                case.0
            );
        }
    }

    #[test]
    fn evaluate_bounds_is() {
        let null = lit(ScalarValue::Null);
        let zero = lit(0);
        let one = lit(1);
        let t = lit(true);
        let f = lit(false);
        let col = col("col");
        let nullable_schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "col",
            DataType::UInt8,
            true,
        )]))
        .unwrap();
        let not_nullable_schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "col",
            DataType::UInt8,
            false,
        )]))
        .unwrap();

        #[rustfmt::skip]
        let cases = vec![
            (is_null(null.clone()), NullableInterval::TRUE),
            (is_null(one.clone()), NullableInterval::FALSE),
            (is_null(binary_expr(null.clone(), Eq, null.clone())), NullableInterval::TRUE),
            (is_not_null(null.clone()), NullableInterval::FALSE),
            (is_not_null(one.clone()), NullableInterval::TRUE),
            (is_not_null(binary_expr(null.clone(), Eq, null.clone())), NullableInterval::FALSE),
            (is_true(null.clone()), NullableInterval::FALSE),
            (is_true(t.clone()), NullableInterval::TRUE),
            (is_true(f.clone()), NullableInterval::FALSE),
            (is_true(zero.clone()), NullableInterval::FALSE),
            (is_true(one.clone()), NullableInterval::TRUE),
            (is_true(binary_expr(null.clone(), Eq, null.clone())), NullableInterval::FALSE),
            (is_not_true(null.clone()), NullableInterval::TRUE),
            (is_not_true(t.clone()), NullableInterval::FALSE),
            (is_not_true(f.clone()), NullableInterval::TRUE),
            (is_not_true(zero.clone()), NullableInterval::TRUE),
            (is_not_true(one.clone()), NullableInterval::FALSE),
            (is_not_true(binary_expr(null.clone(), Eq, null.clone())), NullableInterval::TRUE),
            (is_false(null.clone()), NullableInterval::FALSE),
            (is_false(t.clone()), NullableInterval::FALSE),
            (is_false(f.clone()), NullableInterval::TRUE),
            (is_false(zero.clone()), NullableInterval::TRUE),
            (is_false(one.clone()), NullableInterval::FALSE),
            (is_false(binary_expr(null.clone(), Eq, null.clone())), NullableInterval::FALSE),
            (is_not_false(null.clone()), NullableInterval::TRUE),
            (is_not_false(t.clone()), NullableInterval::TRUE),
            (is_not_false(f.clone()), NullableInterval::FALSE),
            (is_not_false(zero.clone()), NullableInterval::FALSE),
            (is_not_false(one.clone()), NullableInterval::TRUE),
            (is_not_false(binary_expr(null.clone(), Eq, null.clone())), NullableInterval::TRUE),
            (is_unknown(null.clone()), NullableInterval::TRUE),
            (is_unknown(t.clone()), NullableInterval::FALSE),
            (is_unknown(f.clone()), NullableInterval::FALSE),
            (is_unknown(zero.clone()), NullableInterval::FALSE),
            (is_unknown(one.clone()), NullableInterval::FALSE),
            (is_unknown(binary_expr(null.clone(), Eq, null.clone())), NullableInterval::TRUE),
            (is_not_unknown(null.clone()), NullableInterval::FALSE),
            (is_not_unknown(t.clone()), NullableInterval::TRUE),
            (is_not_unknown(f.clone()), NullableInterval::TRUE),
            (is_not_unknown(zero.clone()), NullableInterval::TRUE),
            (is_not_unknown(one.clone()), NullableInterval::TRUE),
            (is_not_unknown(binary_expr(null.clone(), Eq, null.clone())), NullableInterval::FALSE),
        ];

        for case in cases {
            assert_eq!(
                eval_bounds(&case.0).unwrap(),
                case.1,
                "Failed for {}",
                case.0
            );
        }

        #[rustfmt::skip]
        let cases = vec![
            (is_null(col.clone()), &nullable_schema, NullableInterval::TRUE_OR_FALSE),
            (is_null(col.clone()), &not_nullable_schema, NullableInterval::FALSE),
            (is_null(binary_expr(col.clone(), Eq, col.clone())), &nullable_schema, NullableInterval::TRUE_OR_FALSE),
            (is_null(binary_expr(col.clone(), Eq, col.clone())), &not_nullable_schema, NullableInterval::FALSE),
            (is_not_null(col.clone()), &nullable_schema, NullableInterval::TRUE_OR_FALSE),
            (is_not_null(col.clone()), &not_nullable_schema, NullableInterval::TRUE),
            (is_not_null(binary_expr(col.clone(), Eq, col.clone())), &nullable_schema, NullableInterval::TRUE_OR_FALSE),
            (is_not_null(binary_expr(col.clone(), Eq, col.clone())), &not_nullable_schema, NullableInterval::TRUE),
        ];

        for case in cases {
            assert_eq!(
                evaluate_bounds(&case.0, case.1).unwrap(),
                case.2,
                "Failed for {}",
                case.0
            );
        }
    }

    #[test]
    fn evaluate_bounds_between() {
        let null = lit(ScalarValue::Null);
        let zero = lit(0);

        #[rustfmt::skip]
        let cases = vec![
            (zero.clone().between(zero.clone(), zero.clone()), NullableInterval::TRUE_OR_FALSE),
            (null.clone().between(zero.clone(), zero.clone()), NullableInterval::UNKNOWN),
            (zero.clone().between(null.clone(), zero.clone()), NullableInterval::ANY_TRUTH_VALUE),
            (zero.clone().between(zero.clone(), null.clone()), NullableInterval::ANY_TRUTH_VALUE),
            (zero.clone().between(null.clone(), null.clone()), NullableInterval::UNKNOWN),
            (null.clone().between(null.clone(), null.clone()), NullableInterval::UNKNOWN),
        ];

        for case in cases {
            assert_eq!(
                eval_bounds(&case.0).unwrap(),
                case.1,
                "Failed for {}",
                case.0
            );
        }
    }

    #[test]
    fn evaluate_bounds_binary_op() {
        let null = lit(ScalarValue::Null);
        let zero = lit(0);
        let col = col("col");
        let nullable_schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "col",
            DataType::Utf8,
            true,
        )]))
        .unwrap();
        let not_nullable_schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "col",
            DataType::Utf8,
            false,
        )]))
        .unwrap();

        #[rustfmt::skip]
        let cases = vec![
            (binary_expr(zero.clone(), Eq, zero.clone()), NullableInterval::TRUE_OR_FALSE),
            (binary_expr(null.clone(), Eq, zero.clone()), NullableInterval::UNKNOWN),
            (binary_expr(zero.clone(), Eq, null.clone()), NullableInterval::UNKNOWN),
            (binary_expr(null.clone(), Eq, null.clone()), NullableInterval::UNKNOWN),
        ];

        for case in cases {
            assert_eq!(
                eval_bounds(&case.0).unwrap(),
                case.1,
                "Failed for {}",
                case.0
            );
        }

        #[rustfmt::skip]
        let cases = vec![
            (binary_expr(zero.clone(), Eq, col.clone()), NullableInterval::TRUE_OR_FALSE),
            (binary_expr(col.clone(), Eq, zero.clone()), NullableInterval::TRUE_OR_FALSE),
        ];

        for case in cases {
            assert_eq!(
                evaluate_bounds(&case.0, &not_nullable_schema).unwrap(),
                case.1,
                "Failed for {}",
                case.0
            );

            assert_eq!(
                evaluate_bounds(&case.0, &nullable_schema).unwrap(),
                NullableInterval::ANY_TRUTH_VALUE,
                "Failed for {}",
                case.0
            );
        }
    }

    #[test]
    fn evaluate_bounds_negative() {
        let null = lit(ScalarValue::Null);
        let zero = lit(0);

        #[rustfmt::skip]
        let cases = vec![
            (zero.clone().neg(), NullableInterval::TRUE_OR_FALSE),
            (null.clone().neg(), NullableInterval::UNKNOWN),
        ];

        for case in cases {
            assert_eq!(
                eval_bounds(&case.0).unwrap(),
                case.1,
                "Failed for {}",
                case.0
            );
        }
    }

    #[test]
    fn evaluate_bounds_like() {
        let null = lit(ScalarValue::Null);
        let expr = lit("foo");
        let pattern = lit("f.*");
        let col = col("col");
        let nullable_schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "col",
            DataType::Utf8,
            true,
        )]))
        .unwrap();
        let not_nullable_schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "col",
            DataType::Utf8,
            false,
        )]))
        .unwrap();

        #[rustfmt::skip]
        let cases = vec![
            (expr.clone().like(pattern.clone()), NullableInterval::TRUE_OR_FALSE),
            (null.clone().like(pattern.clone()), NullableInterval::UNKNOWN),
            (expr.clone().like(null.clone()), NullableInterval::UNKNOWN),
            (null.clone().like(null.clone()), NullableInterval::UNKNOWN),
        ];

        for case in cases {
            assert_eq!(
                eval_bounds(&case.0).unwrap(),
                case.1,
                "Failed for {}",
                case.0
            );
        }

        #[rustfmt::skip]
        let cases = vec![
            (col.clone().like(pattern.clone()), NullableInterval::TRUE_OR_FALSE),
            (expr.clone().like(col.clone()), NullableInterval::TRUE_OR_FALSE),
        ];

        for case in cases {
            assert_eq!(
                evaluate_bounds(&case.0, &not_nullable_schema).unwrap(),
                case.1,
                "Failed for {}",
                case.0
            );

            assert_eq!(
                evaluate_bounds(&case.0, &nullable_schema).unwrap(),
                NullableInterval::ANY_TRUTH_VALUE,
                "Failed for {}",
                case.0
            );
        }
    }

    #[test]
    fn evaluate_bounds_udf() {
        let func = make_scalar_func_expr();

        #[rustfmt::skip]
        let cases = vec![
            (func.clone(), NullableInterval::ANY_TRUTH_VALUE),
            (not(func.clone()), NullableInterval::ANY_TRUTH_VALUE),
            (binary_expr(func.clone(), And, func.clone()), NullableInterval::ANY_TRUTH_VALUE),
        ];

        for case in cases {
            assert_eq!(eval_bounds(&case.0).unwrap(), case.1);
        }
    }

    fn make_scalar_func_expr() -> Expr {
        let scalar_func_impl =
            |_: &[ColumnarValue]| Ok(ColumnarValue::Scalar(ScalarValue::Null));
        let udf = create_udf(
            "foo",
            vec![],
            DataType::Boolean,
            Volatility::Stable,
            Arc::new(scalar_func_impl),
        );
        Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(udf), vec![]))
    }
}
