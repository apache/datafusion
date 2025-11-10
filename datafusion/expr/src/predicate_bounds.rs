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

use crate::{BinaryExpr, Expr, ExprSchemable};
use arrow::datatypes::DataType;
use bitflags::bitflags;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, ExprSchema, Result, ScalarValue};
use datafusion_expr_common::interval_arithmetic::{Interval, NullableInterval};
use datafusion_expr_common::operator::Operator;

bitflags! {
    /// A set representing the possible outcomes of a SQL boolean expression
    #[derive(PartialEq, Eq, Clone, Debug)]
    struct TernarySet: u8 {
        const TRUE = 0b1;
        const FALSE = 0b10;
        const UNKNOWN = 0b100;
    }
}

impl TernarySet {
    /// Returns the set of possible values after applying the `is true` test on all
    /// values in this set.
    /// The resulting set can only contain 'TRUE' and/or 'FALSE', never 'UNKNOWN'.
    fn is_true(&self) -> Self {
        let mut is_true = Self::empty();
        if self.contains(Self::TRUE) {
            is_true.toggle(Self::TRUE);
        }
        if self.intersects(Self::UNKNOWN | Self::FALSE) {
            is_true.toggle(Self::FALSE);
        }
        is_true
    }

    /// Returns the set of possible values after applying the `is false` test on all
    /// values in this set.
    /// The resulting set can only contain 'TRUE' and/or 'FALSE', never 'UNKNOWN'.
    fn is_false(&self) -> Self {
        let mut is_false = Self::empty();
        if self.contains(Self::FALSE) {
            is_false.toggle(Self::TRUE);
        }
        if self.intersects(Self::UNKNOWN | Self::TRUE) {
            is_false.toggle(Self::FALSE);
        }
        is_false
    }

    /// Returns the set of possible values after applying the `is unknown` test on all
    /// values in this set.
    /// The resulting set can only contain 'TRUE' and/or 'FALSE', never 'UNKNOWN'.
    fn is_unknown(&self) -> Self {
        let mut is_unknown = Self::empty();
        if self.contains(Self::UNKNOWN) {
            is_unknown.toggle(Self::TRUE);
        }
        if self.intersects(Self::TRUE | Self::FALSE) {
            is_unknown.toggle(Self::FALSE);
        }
        is_unknown
    }

    /// Returns the set of possible values after applying SQL three-valued logical NOT
    /// on each value in `value`.
    ///
    /// This method uses the following truth table.
    ///
    /// ```text
    ///  A  | ¬A
    /// ----|----
    ///  F  |  T
    ///  U  |  U
    ///  T  |  F
    /// ```
    fn not(set: Self) -> Self {
        let mut not = Self::empty();
        if set.contains(Self::TRUE) {
            not.toggle(Self::FALSE);
        }
        if set.contains(Self::FALSE) {
            not.toggle(Self::TRUE);
        }
        if set.contains(Self::UNKNOWN) {
            not.toggle(Self::UNKNOWN);
        }
        not
    }

    /// Returns the set of possible values after applying SQL three-valued logical AND
    /// on each combination of values from `lhs` and `rhs`.
    ///
    /// This method uses the following truth table.
    ///
    /// ```text
    /// A ∧ B │ F U T
    /// ──────┼──────
    ///     F │ F F F
    ///     U │ F U U
    ///     T │ F U T
    /// ```
    fn and(lhs: Self, rhs: Self) -> Self {
        if lhs.is_empty() || rhs.is_empty() {
            return Self::empty();
        }

        let mut and = Self::empty();
        if lhs.contains(Self::FALSE) || rhs.contains(Self::FALSE) {
            and.toggle(Self::FALSE);
        }

        if (lhs.contains(Self::UNKNOWN) && rhs.intersects(Self::TRUE | Self::UNKNOWN))
            || (rhs.contains(Self::UNKNOWN) && lhs.intersects(Self::TRUE | Self::UNKNOWN))
        {
            and.toggle(Self::UNKNOWN);
        }

        if lhs.contains(Self::TRUE) && rhs.contains(Self::TRUE) {
            and.toggle(Self::TRUE);
        }

        and
    }

    /// Returns the set of possible values after applying SQL three-valued logical OR
    /// on each combination of values from `lhs` and `rhs`.
    ///
    /// This method uses the following truth table.
    ///
    /// ```text
    /// A ∨ B │ F U T
    /// ──────┼──────
    ///     F │ F U T
    ///     U │ U U T
    ///     T │ T T T
    /// ```
    fn or(lhs: Self, rhs: Self) -> Self {
        let mut or = Self::empty();
        if lhs.contains(Self::TRUE) || rhs.contains(Self::TRUE) {
            or.toggle(Self::TRUE);
        }

        if (lhs.contains(Self::UNKNOWN) && rhs.intersects(Self::FALSE | Self::UNKNOWN))
            || (rhs.contains(Self::UNKNOWN)
                && lhs.intersects(Self::FALSE | Self::UNKNOWN))
        {
            or.toggle(Self::UNKNOWN);
        }

        if lhs.contains(Self::FALSE) && rhs.contains(Self::FALSE) {
            or.toggle(Self::FALSE);
        }

        or
    }
}

impl TryFrom<&ScalarValue> for TernarySet {
    type Error = DataFusionError;

    fn try_from(value: &ScalarValue) -> Result<Self> {
        Ok(match value {
            ScalarValue::Null => TernarySet::UNKNOWN,
            ScalarValue::Boolean(b) => match b {
                Some(true) => TernarySet::TRUE,
                Some(false) => TernarySet::FALSE,
                None => TernarySet::UNKNOWN,
            },
            _ => {
                let b = value.cast_to(&DataType::Boolean)?;
                Self::try_from(&b)?
            }
        })
    }
}

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
/// predicate can evaluate to. The return value will be one of the following:
///
/// * `NullableInterval::NotNull { values: Interval::CERTAINLY_TRUE }` - The predicate will
///   always evaluate to TRUE (never FALSE or NULL)
///
/// * `NullableInterval::NotNull { values: Interval::CERTAINLY_FALSE }` - The predicate will
///   always evaluate to FALSE (never TRUE or NULL)
///
/// * `NullableInterval::NotNull { values: Interval::UNCERTAIN }` - The predicate will never
///   evaluate to NULL, but may be either TRUE or FALSE
///
/// * `NullableInterval::Null { datatype: DataType::Boolean }` - The predicate will always
///   evaluate to NULL (SQL UNKNOWN in three-valued logic)
///
/// * `NullableInterval::MaybeNull { values: Interval::CERTAINLY_TRUE }` - The predicate may
///   evaluate to TRUE or NULL, but never FALSE
///
/// * `NullableInterval::MaybeNull { values: Interval::CERTAINLY_FALSE }` - The predicate may
///   evaluate to FALSE or NULL, but never TRUE
///
/// * `NullableInterval::MaybeNull { values: Interval::UNCERTAIN }` - The predicate may
///   evaluate to any of TRUE, FALSE, or NULL
///
pub(super) fn evaluate_bounds(
    predicate: &Expr,
    certainly_null_expr: Option<&Expr>,
    input_schema: &dyn ExprSchema,
) -> Result<NullableInterval> {
    let evaluator = PredicateBoundsEvaluator {
        input_schema,
        certainly_null_expr: certainly_null_expr.map(unwrap_certainly_null_expr),
    };
    let possible_results = evaluator.evaluate_bounds(predicate)?;

    let interval = if possible_results.is_empty() || possible_results == TernarySet::all()
    {
        NullableInterval::MaybeNull {
            values: Interval::UNCERTAIN,
        }
    } else if possible_results == TernarySet::TRUE {
        NullableInterval::NotNull {
            values: Interval::CERTAINLY_TRUE,
        }
    } else if possible_results == TernarySet::FALSE {
        NullableInterval::NotNull {
            values: Interval::CERTAINLY_FALSE,
        }
    } else if possible_results == TernarySet::UNKNOWN {
        NullableInterval::Null {
            datatype: DataType::Boolean,
        }
    } else {
        let t = possible_results.contains(TernarySet::TRUE);
        let f = possible_results.contains(TernarySet::FALSE);
        let values = if t && f {
            Interval::UNCERTAIN
        } else if t {
            Interval::CERTAINLY_TRUE
        } else {
            Interval::CERTAINLY_FALSE
        };

        if possible_results.contains(TernarySet::UNKNOWN) {
            NullableInterval::MaybeNull { values }
        } else {
            NullableInterval::NotNull { values }
        }
    };

    Ok(interval)
}

/// Returns the innermost [Expr] that is provably null if `expr` is null.
fn unwrap_certainly_null_expr(expr: &Expr) -> &Expr {
    match expr {
        Expr::Not(e) => unwrap_certainly_null_expr(e),
        Expr::Negative(e) => unwrap_certainly_null_expr(e),
        Expr::Cast(e) => unwrap_certainly_null_expr(e.expr.as_ref()),
        _ => expr,
    }
}

struct PredicateBoundsEvaluator<'a> {
    input_schema: &'a dyn ExprSchema,
    certainly_null_expr: Option<&'a Expr>,
}

impl PredicateBoundsEvaluator<'_> {
    /// Derives the bounds of the given boolean expression
    fn evaluate_bounds(&self, predicate: &Expr) -> Result<TernarySet> {
        Ok(match predicate {
            Expr::Literal(scalar, _) => {
                // Interpret literals as boolean, coercing if necessary
                TernarySet::try_from(scalar)?
            }
            Expr::IsNull(e) => {
                // If `e` is not nullable, then `e IS NULL` is provably false
                if !e.nullable(self.input_schema)? {
                    TernarySet::FALSE
                } else {
                    match e.get_type(self.input_schema)? {
                        // If `e` is a boolean expression, check if `e` is provably 'unknown'.
                        DataType::Boolean => self.evaluate_bounds(e)?.is_unknown(),
                        // If `e` is not a boolean expression, check if `e` is provably null
                        _ => self.is_null(e),
                    }
                }
            }
            Expr::IsNotNull(e) => {
                // If `e` is not nullable, then `e IS NOT NULL` is provably true
                if !e.nullable(self.input_schema)? {
                    TernarySet::TRUE
                } else {
                    match e.get_type(self.input_schema)? {
                        // If `e` is a boolean expression, try to evaluate it and test for not unknown
                        DataType::Boolean => {
                            TernarySet::not(self.evaluate_bounds(e)?.is_unknown())
                        }
                        // If `e` is not a boolean expression, check if `e` is provably null
                        _ => TernarySet::not(self.is_null(e)),
                    }
                }
            }
            Expr::IsTrue(e) => self.evaluate_bounds(e)?.is_true(),
            Expr::IsNotTrue(e) => TernarySet::not(self.evaluate_bounds(e)?.is_true()),
            Expr::IsFalse(e) => self.evaluate_bounds(e)?.is_false(),
            Expr::IsNotFalse(e) => TernarySet::not(self.evaluate_bounds(e)?.is_false()),
            Expr::IsUnknown(e) => self.evaluate_bounds(e)?.is_unknown(),
            Expr::IsNotUnknown(e) => {
                TernarySet::not(self.evaluate_bounds(e)?.is_unknown())
            }
            Expr::Not(e) => TernarySet::not(self.evaluate_bounds(e)?),
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => {
                TernarySet::and(self.evaluate_bounds(left)?, self.evaluate_bounds(right)?)
            }
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Or,
                right,
            }) => {
                TernarySet::or(self.evaluate_bounds(left)?, self.evaluate_bounds(right)?)
            }
            e => {
                let mut result = TernarySet::empty();
                let is_null = self.is_null(e);

                // If an expression is null, then it's value is UNKNOWN
                if is_null.contains(TernarySet::TRUE) {
                    result |= TernarySet::UNKNOWN
                }

                // If an expression is not null, then it's either TRUE or FALSE
                if is_null.contains(TernarySet::FALSE) {
                    result |= TernarySet::TRUE | TernarySet::FALSE
                }

                result
            }
        })
    }

    /// Determines if the given expression can evaluate to `NULL`.
    ///
    /// This method only returns sets containing `TRUE`, `FALSE`, or both.
    fn is_null(&self, expr: &Expr) -> TernarySet {
        // Fast path for literals
        if let Expr::Literal(scalar, _) = expr {
            if scalar.is_null() {
                return TernarySet::TRUE;
            } else {
                return TernarySet::FALSE;
            }
        }

        // If `expr` is not nullable, we can be certain `expr` is not null
        if let Ok(false) = expr.nullable(self.input_schema) {
            return TernarySet::FALSE;
        }

        // Check if the expression is the `certainly_null_expr` that was passed in.
        if let Some(certainly_null_expr) = &self.certainly_null_expr {
            if expr.eq(certainly_null_expr) {
                return TernarySet::TRUE;
            }
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
            _ => TernarySet::TRUE | TernarySet::FALSE,
        }
    }

    fn is_null_if_any_child_null(&self, expr: &Expr) -> TernarySet {
        // These expressions are null if any of their direct children is null
        // If any child is inconclusive, the result for this expression is also inconclusive
        let mut is_null = TernarySet::FALSE.clone();
        let _ = expr.apply_children(|child| {
            let child_is_null = self.is_null(child);

            if child_is_null.contains(TernarySet::TRUE) {
                // If a child might be null, then the result may also be null
                is_null.insert(TernarySet::TRUE);
            }

            if !child_is_null.contains(TernarySet::FALSE) {
                // If the child is never not null, then the result can also never be not null
                // and we can stop traversing the children
                is_null.remove(TernarySet::FALSE);
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
    use crate::predicate_bounds::{evaluate_bounds, TernarySet};
    use crate::{
        binary_expr, col, create_udf, is_false, is_not_false, is_not_null, is_not_true,
        is_not_unknown, is_null, is_true, is_unknown, lit, not, Expr,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{DFSchema, ExprSchema, Result, ScalarValue};
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use datafusion_expr_common::operator::Operator::{And, Eq, Or};
    use datafusion_expr_common::signature::Volatility;
    use std::sync::Arc;

    #[test]
    fn tristate_bool_from_scalar() {
        let cases = vec![
            (ScalarValue::Null, TernarySet::UNKNOWN),
            (ScalarValue::Boolean(None), TernarySet::UNKNOWN),
            (ScalarValue::Boolean(Some(true)), TernarySet::TRUE),
            (ScalarValue::Boolean(Some(false)), TernarySet::FALSE),
            (ScalarValue::UInt8(None), TernarySet::UNKNOWN),
            (ScalarValue::UInt8(Some(0)), TernarySet::FALSE),
            (ScalarValue::UInt8(Some(1)), TernarySet::TRUE),
        ];

        for case in cases {
            assert_eq!(TernarySet::try_from(&case.0).unwrap(), case.1);
        }

        let error_cases = vec![ScalarValue::Utf8(Some("abc".to_string()))];

        for case in error_cases {
            assert!(TernarySet::try_from(&case).is_err());
        }
    }

    #[test]
    fn tristate_bool_not() {
        let cases = vec![
            (TernarySet::UNKNOWN, TernarySet::UNKNOWN),
            (TernarySet::TRUE, TernarySet::FALSE),
            (TernarySet::FALSE, TernarySet::TRUE),
            (
                TernarySet::TRUE | TernarySet::FALSE,
                TernarySet::TRUE | TernarySet::FALSE,
            ),
            (
                TernarySet::TRUE | TernarySet::UNKNOWN,
                TernarySet::FALSE | TernarySet::UNKNOWN,
            ),
            (
                TernarySet::FALSE | TernarySet::UNKNOWN,
                TernarySet::TRUE | TernarySet::UNKNOWN,
            ),
            (
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
            ),
        ];

        for case in cases {
            assert_eq!(TernarySet::not(case.0), case.1);
        }
    }

    #[test]
    fn tristate_bool_and() {
        let cases = vec![
            (
                TernarySet::UNKNOWN,
                TernarySet::UNKNOWN,
                TernarySet::UNKNOWN,
            ),
            (TernarySet::UNKNOWN, TernarySet::TRUE, TernarySet::UNKNOWN),
            (TernarySet::UNKNOWN, TernarySet::FALSE, TernarySet::FALSE),
            (TernarySet::TRUE, TernarySet::TRUE, TernarySet::TRUE),
            (TernarySet::TRUE, TernarySet::FALSE, TernarySet::FALSE),
            (TernarySet::FALSE, TernarySet::FALSE, TernarySet::FALSE),
            (
                TernarySet::TRUE | TernarySet::FALSE,
                TernarySet::FALSE,
                TernarySet::FALSE,
            ),
            (
                TernarySet::TRUE | TernarySet::FALSE,
                TernarySet::TRUE,
                TernarySet::TRUE | TernarySet::FALSE,
            ),
            (
                TernarySet::TRUE | TernarySet::UNKNOWN,
                TernarySet::TRUE,
                TernarySet::TRUE | TernarySet::UNKNOWN,
            ),
            (
                TernarySet::TRUE | TernarySet::UNKNOWN,
                TernarySet::TRUE | TernarySet::FALSE,
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
            ),
            (
                TernarySet::FALSE | TernarySet::UNKNOWN,
                TernarySet::TRUE,
                TernarySet::FALSE | TernarySet::UNKNOWN,
            ),
            (
                TernarySet::FALSE | TernarySet::UNKNOWN,
                TernarySet::TRUE | TernarySet::FALSE,
                TernarySet::FALSE | TernarySet::UNKNOWN,
            ),
            (
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
                TernarySet::TRUE,
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
            ),
            (
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
                TernarySet::TRUE | TernarySet::FALSE,
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
            ),
            (
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
            ),
        ];

        for case in cases {
            assert_eq!(
                TernarySet::and(case.0.clone(), case.1.clone()),
                case.2.clone(),
                "{:?} & {:?} = {:?}",
                case.0.clone(),
                case.1.clone(),
                case.2.clone()
            );
            assert_eq!(
                TernarySet::and(case.1.clone(), case.0.clone()),
                case.2.clone(),
                "{:?} & {:?} = {:?}",
                case.1.clone(),
                case.0.clone(),
                case.2.clone()
            );
        }
    }

    #[test]
    fn tristate_bool_or() {
        let cases = vec![
            (
                TernarySet::UNKNOWN,
                TernarySet::UNKNOWN,
                TernarySet::UNKNOWN,
            ),
            (TernarySet::UNKNOWN, TernarySet::TRUE, TernarySet::TRUE),
            (TernarySet::UNKNOWN, TernarySet::FALSE, TernarySet::UNKNOWN),
            (TernarySet::TRUE, TernarySet::TRUE, TernarySet::TRUE),
            (TernarySet::TRUE, TernarySet::FALSE, TernarySet::TRUE),
            (TernarySet::FALSE, TernarySet::FALSE, TernarySet::FALSE),
            (
                TernarySet::TRUE | TernarySet::FALSE,
                TernarySet::FALSE,
                TernarySet::TRUE | TernarySet::FALSE,
            ),
            (
                TernarySet::TRUE | TernarySet::UNKNOWN,
                TernarySet::TRUE,
                TernarySet::TRUE,
            ),
            (
                TernarySet::FALSE | TernarySet::UNKNOWN,
                TernarySet::TRUE,
                TernarySet::TRUE,
            ),
            (
                TernarySet::FALSE | TernarySet::UNKNOWN,
                TernarySet::TRUE | TernarySet::FALSE,
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
            ),
            (
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
                TernarySet::TRUE,
                TernarySet::TRUE,
            ),
            (
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
                TernarySet::TRUE | TernarySet::FALSE,
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
            ),
            (
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
                TernarySet::TRUE | TernarySet::FALSE | TernarySet::UNKNOWN,
            ),
        ];

        for case in cases {
            assert_eq!(
                TernarySet::or(case.0.clone(), case.1.clone()),
                case.2.clone(),
                "{:?} | {:?} = {:?}",
                case.0.clone(),
                case.1.clone(),
                case.2.clone()
            );
            assert_eq!(
                TernarySet::or(case.1.clone(), case.0.clone()),
                case.2.clone(),
                "{:?} | {:?} = {:?}",
                case.1.clone(),
                case.0.clone(),
                case.2.clone()
            );
        }
    }

    fn try_eval_predicate_bounds(
        predicate: &Expr,
        evaluates_to_null: Option<&Expr>,
        input_schema: &dyn ExprSchema,
    ) -> Result<Option<bool>> {
        let bounds = evaluate_bounds(predicate, evaluates_to_null, input_schema)?;

        Ok(if bounds.is_certainly_true() {
            Some(true)
        } else if bounds.is_certainly_not_true() {
            Some(false)
        } else {
            None
        })
    }

    fn eval_predicate_bounds(
        predicate: &Expr,
        evaluates_to_null: Option<&Expr>,
        input_schema: &dyn ExprSchema,
    ) -> Option<bool> {
        try_eval_predicate_bounds(predicate, evaluates_to_null, input_schema).unwrap()
    }

    fn try_eval_bounds(predicate: &Expr) -> Result<Option<bool>> {
        let schema = DFSchema::try_from(Schema::empty())?;
        try_eval_predicate_bounds(predicate, None, &schema)
    }

    fn eval_bounds(predicate: &Expr) -> Option<bool> {
        try_eval_bounds(predicate).unwrap()
    }

    #[test]
    fn evaluate_bounds_literal() {
        assert_eq!(eval_bounds(&lit(ScalarValue::Null)), Some(false));

        assert_eq!(eval_bounds(&lit(false)), Some(false));
        assert_eq!(eval_bounds(&lit(true)), Some(true));

        assert_eq!(eval_bounds(&lit(0)), Some(false));
        assert_eq!(eval_bounds(&lit(1)), Some(true));

        assert_eq!(eval_bounds(&lit(ScalarValue::Utf8(None))), Some(false));
        assert!(try_eval_bounds(&lit("foo")).is_err());
    }

    #[test]
    fn evaluate_bounds_and() {
        let null = lit(ScalarValue::Null);
        let zero = lit(0);
        let one = lit(1);
        let t = lit(true);
        let f = lit(false);
        let func = make_scalar_func_expr();

        assert_eq!(
            eval_bounds(&binary_expr(null.clone(), And, null.clone())),
            Some(false)
        );
        assert_eq!(
            eval_bounds(&binary_expr(null.clone(), And, one.clone())),
            Some(false)
        );
        assert_eq!(
            eval_bounds(&binary_expr(null.clone(), And, zero.clone())),
            Some(false)
        );

        assert_eq!(
            eval_bounds(&binary_expr(one.clone(), And, one.clone())),
            Some(true)
        );
        assert_eq!(
            eval_bounds(&binary_expr(one.clone(), And, zero.clone())),
            Some(false)
        );

        assert_eq!(
            eval_bounds(&binary_expr(null.clone(), And, t.clone())),
            Some(false)
        );
        assert_eq!(
            eval_bounds(&binary_expr(t.clone(), And, null.clone())),
            Some(false)
        );
        assert_eq!(
            eval_bounds(&binary_expr(null.clone(), And, f.clone())),
            Some(false)
        );
        assert_eq!(
            eval_bounds(&binary_expr(f.clone(), And, null.clone())),
            Some(false)
        );

        assert_eq!(
            eval_bounds(&binary_expr(t.clone(), And, t.clone())),
            Some(true)
        );
        assert_eq!(
            eval_bounds(&binary_expr(t.clone(), And, f.clone())),
            Some(false)
        );
        assert_eq!(
            eval_bounds(&binary_expr(f.clone(), And, t.clone())),
            Some(false)
        );
        assert_eq!(
            eval_bounds(&binary_expr(f.clone(), And, f.clone())),
            Some(false)
        );

        assert_eq!(
            eval_bounds(&binary_expr(t.clone(), And, func.clone())),
            None
        );
        assert_eq!(
            eval_bounds(&binary_expr(func.clone(), And, t.clone())),
            None
        );
        assert_eq!(
            eval_bounds(&binary_expr(f.clone(), And, func.clone())),
            Some(false)
        );
        assert_eq!(
            eval_bounds(&binary_expr(func.clone(), And, f.clone())),
            Some(false)
        );
        assert_eq!(
            eval_bounds(&binary_expr(null.clone(), And, func.clone())),
            Some(false)
        );
        assert_eq!(
            eval_bounds(&binary_expr(func.clone(), And, null.clone())),
            Some(false)
        );
    }

    #[test]
    fn evaluate_bounds_or() {
        let null = lit(ScalarValue::Null);
        let zero = lit(0);
        let one = lit(1);
        let t = lit(true);
        let f = lit(false);
        let func = make_scalar_func_expr();

        assert_eq!(
            eval_bounds(&binary_expr(null.clone(), Or, null.clone())),
            Some(false)
        );
        assert_eq!(
            eval_bounds(&binary_expr(null.clone(), Or, one.clone())),
            Some(true)
        );
        assert_eq!(
            eval_bounds(&binary_expr(null.clone(), Or, zero.clone())),
            Some(false)
        );

        assert_eq!(
            eval_bounds(&binary_expr(one.clone(), Or, one.clone())),
            Some(true)
        );
        assert_eq!(
            eval_bounds(&binary_expr(one.clone(), Or, zero.clone())),
            Some(true)
        );

        assert_eq!(
            eval_bounds(&binary_expr(null.clone(), Or, t.clone())),
            Some(true)
        );
        assert_eq!(
            eval_bounds(&binary_expr(t.clone(), Or, null.clone())),
            Some(true)
        );
        assert_eq!(
            eval_bounds(&binary_expr(null.clone(), Or, f.clone())),
            Some(false)
        );
        assert_eq!(
            eval_bounds(&binary_expr(f.clone(), Or, null.clone())),
            Some(false)
        );

        assert_eq!(
            eval_bounds(&binary_expr(t.clone(), Or, t.clone())),
            Some(true)
        );
        assert_eq!(
            eval_bounds(&binary_expr(t.clone(), Or, f.clone())),
            Some(true)
        );
        assert_eq!(
            eval_bounds(&binary_expr(f.clone(), Or, t.clone())),
            Some(true)
        );
        assert_eq!(
            eval_bounds(&binary_expr(f.clone(), Or, f.clone())),
            Some(false)
        );

        assert_eq!(
            eval_bounds(&binary_expr(t.clone(), Or, func.clone())),
            Some(true)
        );
        assert_eq!(
            eval_bounds(&binary_expr(func.clone(), Or, t.clone())),
            Some(true)
        );
        assert_eq!(eval_bounds(&binary_expr(f.clone(), Or, func.clone())), None);
        assert_eq!(eval_bounds(&binary_expr(func.clone(), Or, f.clone())), None);
        assert_eq!(
            eval_bounds(&binary_expr(null.clone(), Or, func.clone())),
            None
        );
        assert_eq!(
            eval_bounds(&binary_expr(func.clone(), Or, null.clone())),
            None
        );
    }

    #[test]
    fn evaluate_bounds_not() {
        let null = lit(ScalarValue::Null);
        let zero = lit(0);
        let one = lit(1);
        let t = lit(true);
        let f = lit(false);
        let func = make_scalar_func_expr();

        assert_eq!(eval_bounds(&not(null.clone())), Some(false));
        assert_eq!(eval_bounds(&not(one.clone())), Some(false));
        assert_eq!(eval_bounds(&not(zero.clone())), Some(true));

        assert_eq!(eval_bounds(&not(t.clone())), Some(false));
        assert_eq!(eval_bounds(&not(f.clone())), Some(true));

        assert_eq!(eval_bounds(&not(func.clone())), None);
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

        assert_eq!(eval_bounds(&is_null(null.clone())), Some(true));
        assert_eq!(eval_bounds(&is_null(one.clone())), Some(false));
        let predicate = &is_null(col.clone());
        assert_eq!(
            eval_predicate_bounds(predicate, Some(&col), &nullable_schema),
            Some(true)
        );
        let predicate = &is_null(col.clone());
        assert_eq!(
            eval_predicate_bounds(predicate, Some(&col), &not_nullable_schema),
            Some(false)
        );

        assert_eq!(eval_bounds(&is_not_null(null.clone())), Some(false));
        assert_eq!(eval_bounds(&is_not_null(one.clone())), Some(true));
        let predicate = &is_not_null(col.clone());
        assert_eq!(
            eval_predicate_bounds(predicate, Some(&col), &nullable_schema),
            Some(false)
        );
        let predicate = &is_not_null(col.clone());
        assert_eq!(
            eval_predicate_bounds(predicate, Some(&col), &not_nullable_schema),
            Some(true)
        );

        assert_eq!(eval_bounds(&is_true(null.clone())), Some(false));
        assert_eq!(eval_bounds(&is_true(t.clone())), Some(true));
        assert_eq!(eval_bounds(&is_true(f.clone())), Some(false));
        assert_eq!(eval_bounds(&is_true(zero.clone())), Some(false));
        assert_eq!(eval_bounds(&is_true(one.clone())), Some(true));

        assert_eq!(eval_bounds(&is_not_true(null.clone())), Some(true));
        assert_eq!(eval_bounds(&is_not_true(t.clone())), Some(false));
        assert_eq!(eval_bounds(&is_not_true(f.clone())), Some(true));
        assert_eq!(eval_bounds(&is_not_true(zero.clone())), Some(true));
        assert_eq!(eval_bounds(&is_not_true(one.clone())), Some(false));

        assert_eq!(eval_bounds(&is_false(null.clone())), Some(false));
        assert_eq!(eval_bounds(&is_false(t.clone())), Some(false));
        assert_eq!(eval_bounds(&is_false(f.clone())), Some(true));
        assert_eq!(eval_bounds(&is_false(zero.clone())), Some(true));
        assert_eq!(eval_bounds(&is_false(one.clone())), Some(false));

        assert_eq!(eval_bounds(&is_not_false(null.clone())), Some(true));
        assert_eq!(eval_bounds(&is_not_false(t.clone())), Some(true));
        assert_eq!(eval_bounds(&is_not_false(f.clone())), Some(false));
        assert_eq!(eval_bounds(&is_not_false(zero.clone())), Some(false));
        assert_eq!(eval_bounds(&is_not_false(one.clone())), Some(true));

        assert_eq!(eval_bounds(&is_unknown(null.clone())), Some(true));
        assert_eq!(eval_bounds(&is_unknown(t.clone())), Some(false));
        assert_eq!(eval_bounds(&is_unknown(f.clone())), Some(false));
        assert_eq!(eval_bounds(&is_unknown(zero.clone())), Some(false));
        assert_eq!(eval_bounds(&is_unknown(one.clone())), Some(false));

        assert_eq!(eval_bounds(&is_not_unknown(null.clone())), Some(false));
        assert_eq!(eval_bounds(&is_not_unknown(t.clone())), Some(true));
        assert_eq!(eval_bounds(&is_not_unknown(f.clone())), Some(true));
        assert_eq!(eval_bounds(&is_not_unknown(zero.clone())), Some(true));
        assert_eq!(eval_bounds(&is_not_unknown(one.clone())), Some(true));
    }

    #[test]
    fn evaluate_bounds_udf() {
        let func = make_scalar_func_expr();

        assert_eq!(eval_bounds(&func.clone()), None);
        assert_eq!(eval_bounds(&not(func.clone())), None);
        assert_eq!(
            eval_bounds(&binary_expr(func.clone(), And, func.clone())),
            None
        );
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

    #[test]
    fn evaluate_bounds_when_then() {
        let nullable_schema =
            DFSchema::try_from(Schema::new(vec![Field::new("x", DataType::UInt8, true)]))
                .unwrap();
        let not_nullable_schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "x",
            DataType::UInt8,
            false,
        )]))
        .unwrap();

        let x = col("x");

        // CASE WHEN x IS NOT NULL OR x = 5 THEN x ELSE 0 END
        let when = binary_expr(
            is_not_null(x.clone()),
            Or,
            binary_expr(x.clone(), Eq, lit(5)),
        );

        assert_eq!(
            eval_predicate_bounds(&when, Some(&x), &nullable_schema),
            Some(false)
        );
        assert_eq!(
            eval_predicate_bounds(&when, Some(&x), &not_nullable_schema),
            Some(true)
        );
    }
}
