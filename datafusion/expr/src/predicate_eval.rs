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
use datafusion_common::{ExprSchema, ScalarValue};
use datafusion_expr_common::operator::Operator;

bitflags! {
    #[derive(PartialEq, Eq, Clone, Debug)]
    struct TriBoolSet: u8 {
        const TRUE = 0b1;
        const FALSE = 0b10;
        const UNKNOWN = 0b100;
    }
}

impl TriBoolSet {
    fn try_from(value: &ScalarValue) -> TriBoolSet {
        match value {
            ScalarValue::Null => TriBoolSet::UNKNOWN,
            ScalarValue::Boolean(b) => match b {
                Some(true) => TriBoolSet::TRUE,
                Some(false) => TriBoolSet::FALSE,
                None => TriBoolSet::UNKNOWN,
            },
            _ => {
                if let Ok(b) = value.cast_to(&DataType::Boolean) {
                    Self::try_from(&b)
                } else {
                    TriBoolSet::empty()
                }
            }
        }
    }

    /// Returns the set of possible values after applying `IS TRUE` on all
    /// values in this set
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

    /// Returns the set of possible values after applying `IS FALSE` on all
    /// values in this set
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

    /// Returns the set of possible values after applying `IS UNKNOWN` on all
    /// values in this set
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
    /// ```
    /// P       | !P
    /// --------|-------
    /// TRUE    | FALSE
    /// FALSE   | TRUE
    /// UNKNOWN | UNKNOWN
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
    /// ```
    /// P       | Q       | P AND Q
    /// --------|---------|----------
    /// TRUE    | TRUE    | TRUE
    /// TRUE    | FALSE   | FALSE
    /// FALSE   | TRUE    | FALSE
    /// FALSE   | FALSE   | FALSE
    /// FALSE   | UNKNOWN | FALSE
    /// UNKNOWN | FALSE   | FALSE
    /// TRUE    | UNKNOWN | UNKNOWN
    /// UNKNOWN | TRUE    | UNKNOWN
    /// UNKNOWN | UNKNOWN | UNKNOWN
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
    /// ```
    /// SQL three-valued logic OR truth table:
    ///
    /// P       | Q       | P OR Q
    /// --------|---------|----------
    /// FALSE   | FALSE   | FALSE
    /// TRUE    | TRUE    | TRUE
    /// TRUE    | FALSE   | TRUE
    /// FALSE   | TRUE    | TRUE
    /// TRUE    | UNKNOWN | TRUE
    /// UNKNOWN | TRUE    | TRUE
    /// FALSE   | UNKNOWN | UNKNOWN
    /// UNKNOWN | FALSE   | UNKNOWN
    /// UNKNOWN | UNKNOWN | UNKNOWN
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

/// Attempts to const evaluate a predicate using SQL three-valued logic.
///
/// Semantics of the return value:
/// - `Some(true)`      => predicate is provably truthy
/// - `Some(false)`     => predicate is provably falsy
/// - `None`            => inconclusive with available static information
///
/// The evaluation is conservative and only uses:
/// - Expression nullability from `input_schema`
/// - Simple type checks (e.g. whether an expression is Boolean)
/// - Syntactic patterns (IS NULL/IS NOT NULL/IS TRUE/IS FALSE/etc.)
/// - Three-valued boolean algebra for AND/OR/NOT
///
/// It does not evaluate user-defined functions.
pub(super) fn const_eval_predicate<F>(
    predicate: &Expr,
    evaluates_to_null: F,
    input_schema: &dyn ExprSchema,
) -> Option<bool>
where
    F: Fn(&Expr) -> Option<bool>,
{
    let evaluator = PredicateConstEvaluator {
        input_schema,
        evaluates_to_null,
    };
    let possible_results = evaluator.eval_predicate(predicate);

    if !possible_results.is_empty() {
        if possible_results == TriBoolSet::TRUE {
            // Provably true
            return Some(true);
        } else if !possible_results.contains(TriBoolSet::TRUE) {
            // Provably not true
            return Some(false);
        }
    }

    None
}

pub(super) struct PredicateConstEvaluator<'a, F> {
    input_schema: &'a dyn ExprSchema,
    evaluates_to_null: F,
}

impl<F> PredicateConstEvaluator<'_, F>
where
    F: Fn(&Expr) -> Option<bool>,
{
    /// Attempts to const evaluate a boolean predicate.
    fn eval_predicate(&self, predicate: &Expr) -> TriBoolSet {
        match predicate {
            Expr::Literal(scalar, _) => {
                // Interpret literals as boolean, coercing if necessary
                TriBoolSet::try_from(scalar)
            }
            Expr::Negative(e) => self.eval_predicate(e),
            Expr::IsNull(e) => {
                // If `e` is not nullable, then `e IS NULL` is provably false
                if let Ok(false) = e.nullable(self.input_schema) {
                    return TriBoolSet::FALSE;
                }

                match e.get_type(self.input_schema) {
                    // If `e` is a boolean expression, try to evaluate it and test for unknown
                    Ok(DataType::Boolean) => self.eval_predicate(e).is_unknown(),
                    // If `e` is not a boolean expression, check if `e` is provably null
                    Ok(_) => self.is_null(e),
                    Err(_) => TriBoolSet::empty(),
                }
            }
            Expr::IsNotNull(e) => {
                // If `e` is not nullable, then `e IS NOT NULL` is provably true
                if let Ok(false) = e.nullable(self.input_schema) {
                    return TriBoolSet::TRUE;
                }

                match e.get_type(self.input_schema) {
                    // If `e` is a boolean expression, try to evaluate it and test for not unknown
                    Ok(DataType::Boolean) => {
                        TriBoolSet::not(self.eval_predicate(e).is_unknown())
                    }
                    // If `e` is not a boolean expression, check if `e` is provably null
                    Ok(_) => TriBoolSet::not(self.is_null(e)),
                    Err(_) => TriBoolSet::empty(),
                }
            }
            Expr::IsTrue(e) => self.eval_predicate(e).is_true(),
            Expr::IsNotTrue(e) => TriBoolSet::not(self.eval_predicate(e).is_true()),
            Expr::IsFalse(e) => self.eval_predicate(e).is_false(),
            Expr::IsNotFalse(e) => TriBoolSet::not(self.eval_predicate(e).is_false()),
            Expr::IsUnknown(e) => self.eval_predicate(e).is_unknown(),
            Expr::IsNotUnknown(e) => TriBoolSet::not(self.eval_predicate(e).is_unknown()),
            Expr::Not(e) => TriBoolSet::not(self.eval_predicate(e)),
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => TriBoolSet::and(self.eval_predicate(left), self.eval_predicate(right)),
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Or,
                right,
            }) => TriBoolSet::or(self.eval_predicate(left), self.eval_predicate(right)),
            e => {
                let mut result = TriBoolSet::empty();
                let is_null = self.is_null(e);

                // If an expression is null, then it's value is UNKNOWN
                if is_null.contains(TriBoolSet::TRUE) {
                    result |= TriBoolSet::UNKNOWN
                }

                // If an expression is not null, then it's either TRUE or FALSE
                if is_null.contains(TriBoolSet::FALSE) {
                    result |= TriBoolSet::TRUE | TriBoolSet::FALSE
                }

                result
            }
        }
    }

    /// Determines if the given expression can evaluate to `NULL`.
    ///
    /// This method only returns sets containing `TRUE`, `FALSE`, or both.
    fn is_null(&self, expr: &Expr) -> TriBoolSet {
        // If `expr` is not nullable, we can be certain `expr` is not null
        if let Ok(false) = expr.nullable(self.input_schema) {
            return TriBoolSet::FALSE;
        }

        // Check if the callback can decide for us
        if let Some(is_null) = (self.evaluates_to_null)(expr) {
            return if is_null {
                TriBoolSet::TRUE
            } else {
                TriBoolSet::FALSE
            };
        }

        // `expr` is nullable, so our default answer is { TRUE, FALSE }.
        // Try to see if we can narrow it down to one of the two.
        match expr {
            Expr::Literal(s, _) => {
                if s.is_null() {
                    TriBoolSet::TRUE
                } else {
                    TriBoolSet::FALSE
                }
            }
            Expr::Alias(_)
            | Expr::Between(_)
            | Expr::BinaryExpr(_)
            | Expr::Cast(_)
            | Expr::Like(_)
            | Expr::Negative(_)
            | Expr::Not(_)
            | Expr::SimilarTo(_) => {
                // These expressions are null if any of their direct children is null
                // If any child is inconclusive, the result for this expression is also inconclusive
                let mut is_null = TriBoolSet::FALSE.clone();
                let _ = expr.apply_children(|child| {
                    let child_is_null = self.is_null(child);

                    if child_is_null.contains(TriBoolSet::TRUE) {
                        // If a child might be null, then the result may also be null
                        is_null.insert(TriBoolSet::TRUE);
                    }

                    if !child_is_null.contains(TriBoolSet::FALSE) {
                        // If the child is never not null, then the result can also never be not null
                        // and we can stop traversing the children
                        is_null.remove(TriBoolSet::FALSE);
                        Ok(TreeNodeRecursion::Stop)
                    } else {
                        Ok(TreeNodeRecursion::Continue)
                    }
                });
                is_null
            }
            _ => TriBoolSet::TRUE | TriBoolSet::FALSE,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::expr::ScalarFunction;
    use crate::predicate_eval::{const_eval_predicate, TriBoolSet};
    use crate::{
        binary_expr, col, create_udf, is_false, is_not_false, is_not_null, is_not_true,
        is_not_unknown, is_null, is_true, is_unknown, lit, not, Expr,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{DFSchema, ScalarValue};
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use datafusion_expr_common::operator::Operator::{And, Eq, Or};
    use datafusion_expr_common::signature::Volatility;
    use std::sync::Arc;

    #[test]
    fn tristate_bool_from_scalar() {
        let cases = vec![
            (ScalarValue::Null, TriBoolSet::UNKNOWN),
            (ScalarValue::Boolean(None), TriBoolSet::UNKNOWN),
            (ScalarValue::Boolean(Some(true)), TriBoolSet::TRUE),
            (ScalarValue::Boolean(Some(false)), TriBoolSet::FALSE),
            (ScalarValue::UInt8(None), TriBoolSet::UNKNOWN),
            (ScalarValue::UInt8(Some(0)), TriBoolSet::FALSE),
            (ScalarValue::UInt8(Some(1)), TriBoolSet::TRUE),
            (
                ScalarValue::Utf8(Some("abc".to_string())),
                TriBoolSet::empty(),
            ),
        ];

        for case in cases {
            assert_eq!(TriBoolSet::try_from(&case.0), case.1);
        }
    }

    #[test]
    fn tristate_bool_not() {
        let cases = vec![
            (TriBoolSet::UNKNOWN, TriBoolSet::UNKNOWN),
            (TriBoolSet::TRUE, TriBoolSet::FALSE),
            (TriBoolSet::FALSE, TriBoolSet::TRUE),
            (
                TriBoolSet::TRUE | TriBoolSet::FALSE,
                TriBoolSet::TRUE | TriBoolSet::FALSE,
            ),
            (
                TriBoolSet::TRUE | TriBoolSet::UNKNOWN,
                TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
            ),
            (
                TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE | TriBoolSet::UNKNOWN,
            ),
            (
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
            ),
        ];

        for case in cases {
            assert_eq!(TriBoolSet::not(case.0), case.1);
        }
    }

    #[test]
    fn tristate_bool_and() {
        let cases = vec![
            (
                TriBoolSet::UNKNOWN,
                TriBoolSet::UNKNOWN,
                TriBoolSet::UNKNOWN,
            ),
            (TriBoolSet::UNKNOWN, TriBoolSet::TRUE, TriBoolSet::UNKNOWN),
            (TriBoolSet::UNKNOWN, TriBoolSet::FALSE, TriBoolSet::FALSE),
            (TriBoolSet::TRUE, TriBoolSet::TRUE, TriBoolSet::TRUE),
            (TriBoolSet::TRUE, TriBoolSet::FALSE, TriBoolSet::FALSE),
            (TriBoolSet::FALSE, TriBoolSet::FALSE, TriBoolSet::FALSE),
            (
                TriBoolSet::TRUE | TriBoolSet::FALSE,
                TriBoolSet::FALSE,
                TriBoolSet::FALSE,
            ),
            (
                TriBoolSet::TRUE | TriBoolSet::FALSE,
                TriBoolSet::TRUE,
                TriBoolSet::TRUE | TriBoolSet::FALSE,
            ),
            (
                TriBoolSet::TRUE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE,
                TriBoolSet::TRUE | TriBoolSet::UNKNOWN,
            ),
            (
                TriBoolSet::TRUE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE | TriBoolSet::FALSE,
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
            ),
            (
                TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE,
                TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
            ),
            (
                TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE | TriBoolSet::FALSE,
                TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
            ),
            (
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE,
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
            ),
            (
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE | TriBoolSet::FALSE,
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
            ),
            (
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
            ),
        ];

        for case in cases {
            assert_eq!(
                TriBoolSet::and(case.0.clone(), case.1.clone()),
                case.2.clone(),
                "{:?} & {:?} = {:?}",
                case.0.clone(),
                case.1.clone(),
                case.2.clone()
            );
            assert_eq!(
                TriBoolSet::and(case.1.clone(), case.0.clone()),
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
                TriBoolSet::UNKNOWN,
                TriBoolSet::UNKNOWN,
                TriBoolSet::UNKNOWN,
            ),
            (TriBoolSet::UNKNOWN, TriBoolSet::TRUE, TriBoolSet::TRUE),
            (TriBoolSet::UNKNOWN, TriBoolSet::FALSE, TriBoolSet::UNKNOWN),
            (TriBoolSet::TRUE, TriBoolSet::TRUE, TriBoolSet::TRUE),
            (TriBoolSet::TRUE, TriBoolSet::FALSE, TriBoolSet::TRUE),
            (TriBoolSet::FALSE, TriBoolSet::FALSE, TriBoolSet::FALSE),
            (
                TriBoolSet::TRUE | TriBoolSet::FALSE,
                TriBoolSet::FALSE,
                TriBoolSet::TRUE | TriBoolSet::FALSE,
            ),
            (
                TriBoolSet::TRUE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE,
                TriBoolSet::TRUE,
            ),
            (
                TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE,
                TriBoolSet::TRUE,
            ),
            (
                TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE | TriBoolSet::FALSE,
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
            ),
            (
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE,
                TriBoolSet::TRUE,
            ),
            (
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE | TriBoolSet::FALSE,
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
            ),
            (
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
                TriBoolSet::TRUE | TriBoolSet::FALSE | TriBoolSet::UNKNOWN,
            ),
        ];

        for case in cases {
            assert_eq!(
                TriBoolSet::or(case.0.clone(), case.1.clone()),
                case.2.clone(),
                "{:?} | {:?} = {:?}",
                case.0.clone(),
                case.1.clone(),
                case.2.clone()
            );
            assert_eq!(
                TriBoolSet::or(case.1.clone(), case.0.clone()),
                case.2.clone(),
                "{:?} | {:?} = {:?}",
                case.1.clone(),
                case.0.clone(),
                case.2.clone()
            );
        }
    }

    fn const_eval(predicate: &Expr) -> Option<bool> {
        let schema = DFSchema::try_from(Schema::empty()).unwrap();
        const_eval_predicate(predicate, |_| None, &schema)
    }

    fn const_eval_with_null(
        predicate: &Expr,
        schema: &DFSchema,
        null_expr: &Expr,
    ) -> Option<bool> {
        const_eval_predicate(
            predicate,
            |e| {
                if e.eq(null_expr) {
                    Some(true)
                } else {
                    None
                }
            },
            schema,
        )
    }

    #[test]
    fn predicate_eval_literal() {
        assert_eq!(const_eval(&lit(ScalarValue::Null)), Some(false));

        assert_eq!(const_eval(&lit(false)), Some(false));
        assert_eq!(const_eval(&lit(true)), Some(true));

        assert_eq!(const_eval(&lit(0)), Some(false));
        assert_eq!(const_eval(&lit(1)), Some(true));

        assert_eq!(const_eval(&lit("foo")), None);
        assert_eq!(const_eval(&lit(ScalarValue::Utf8(None))), Some(false));
    }

    #[test]
    fn predicate_eval_and() {
        let null = lit(ScalarValue::Null);
        let zero = lit(0);
        let one = lit(1);
        let t = lit(true);
        let f = lit(false);
        let func = make_scalar_func_expr();

        assert_eq!(
            const_eval(&binary_expr(null.clone(), And, null.clone())),
            Some(false)
        );
        assert_eq!(
            const_eval(&binary_expr(null.clone(), And, one.clone())),
            Some(false)
        );
        assert_eq!(
            const_eval(&binary_expr(null.clone(), And, zero.clone())),
            Some(false)
        );

        assert_eq!(
            const_eval(&binary_expr(one.clone(), And, one.clone())),
            Some(true)
        );
        assert_eq!(
            const_eval(&binary_expr(one.clone(), And, zero.clone())),
            Some(false)
        );

        assert_eq!(
            const_eval(&binary_expr(null.clone(), And, t.clone())),
            Some(false)
        );
        assert_eq!(
            const_eval(&binary_expr(t.clone(), And, null.clone())),
            Some(false)
        );
        assert_eq!(
            const_eval(&binary_expr(null.clone(), And, f.clone())),
            Some(false)
        );
        assert_eq!(
            const_eval(&binary_expr(f.clone(), And, null.clone())),
            Some(false)
        );

        assert_eq!(
            const_eval(&binary_expr(t.clone(), And, t.clone())),
            Some(true)
        );
        assert_eq!(
            const_eval(&binary_expr(t.clone(), And, f.clone())),
            Some(false)
        );
        assert_eq!(
            const_eval(&binary_expr(f.clone(), And, t.clone())),
            Some(false)
        );
        assert_eq!(
            const_eval(&binary_expr(f.clone(), And, f.clone())),
            Some(false)
        );

        assert_eq!(const_eval(&binary_expr(t.clone(), And, func.clone())), None);
        assert_eq!(const_eval(&binary_expr(func.clone(), And, t.clone())), None);
        assert_eq!(
            const_eval(&binary_expr(f.clone(), And, func.clone())),
            Some(false)
        );
        assert_eq!(
            const_eval(&binary_expr(func.clone(), And, f.clone())),
            Some(false)
        );
        assert_eq!(
            const_eval(&binary_expr(null.clone(), And, func.clone())),
            Some(false)
        );
        assert_eq!(
            const_eval(&binary_expr(func.clone(), And, null.clone())),
            Some(false)
        );
    }

    #[test]
    fn predicate_eval_or() {
        let null = lit(ScalarValue::Null);
        let zero = lit(0);
        let one = lit(1);
        let t = lit(true);
        let f = lit(false);
        let func = make_scalar_func_expr();

        assert_eq!(
            const_eval(&binary_expr(null.clone(), Or, null.clone())),
            Some(false)
        );
        assert_eq!(
            const_eval(&binary_expr(null.clone(), Or, one.clone())),
            Some(true)
        );
        assert_eq!(
            const_eval(&binary_expr(null.clone(), Or, zero.clone())),
            Some(false)
        );

        assert_eq!(
            const_eval(&binary_expr(one.clone(), Or, one.clone())),
            Some(true)
        );
        assert_eq!(
            const_eval(&binary_expr(one.clone(), Or, zero.clone())),
            Some(true)
        );

        assert_eq!(
            const_eval(&binary_expr(null.clone(), Or, t.clone())),
            Some(true)
        );
        assert_eq!(
            const_eval(&binary_expr(t.clone(), Or, null.clone())),
            Some(true)
        );
        assert_eq!(
            const_eval(&binary_expr(null.clone(), Or, f.clone())),
            Some(false)
        );
        assert_eq!(
            const_eval(&binary_expr(f.clone(), Or, null.clone())),
            Some(false)
        );

        assert_eq!(
            const_eval(&binary_expr(t.clone(), Or, t.clone())),
            Some(true)
        );
        assert_eq!(
            const_eval(&binary_expr(t.clone(), Or, f.clone())),
            Some(true)
        );
        assert_eq!(
            const_eval(&binary_expr(f.clone(), Or, t.clone())),
            Some(true)
        );
        assert_eq!(
            const_eval(&binary_expr(f.clone(), Or, f.clone())),
            Some(false)
        );

        assert_eq!(
            const_eval(&binary_expr(t.clone(), Or, func.clone())),
            Some(true)
        );
        assert_eq!(
            const_eval(&binary_expr(func.clone(), Or, t.clone())),
            Some(true)
        );
        assert_eq!(const_eval(&binary_expr(f.clone(), Or, func.clone())), None);
        assert_eq!(const_eval(&binary_expr(func.clone(), Or, f.clone())), None);
        assert_eq!(
            const_eval(&binary_expr(null.clone(), Or, func.clone())),
            None
        );
        assert_eq!(
            const_eval(&binary_expr(func.clone(), Or, null.clone())),
            None
        );
    }

    #[test]
    fn predicate_eval_not() {
        let null = lit(ScalarValue::Null);
        let zero = lit(0);
        let one = lit(1);
        let t = lit(true);
        let f = lit(false);
        let func = make_scalar_func_expr();

        assert_eq!(const_eval(&not(null.clone())), Some(false));
        assert_eq!(const_eval(&not(one.clone())), Some(false));
        assert_eq!(const_eval(&not(zero.clone())), Some(true));

        assert_eq!(const_eval(&not(t.clone())), Some(false));
        assert_eq!(const_eval(&not(f.clone())), Some(true));

        assert_eq!(const_eval(&not(func.clone())), None);
    }

    #[test]
    fn predicate_eval_is() {
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

        assert_eq!(const_eval(&is_null(null.clone())), Some(true));
        assert_eq!(const_eval(&is_null(one.clone())), Some(false));
        assert_eq!(
            const_eval_with_null(&is_null(col.clone()), &nullable_schema, &col),
            Some(true)
        );
        assert_eq!(
            const_eval_with_null(&is_null(col.clone()), &not_nullable_schema, &col),
            Some(false)
        );

        assert_eq!(const_eval(&is_not_null(null.clone())), Some(false));
        assert_eq!(const_eval(&is_not_null(one.clone())), Some(true));
        assert_eq!(
            const_eval_with_null(&is_not_null(col.clone()), &nullable_schema, &col),
            Some(false)
        );
        assert_eq!(
            const_eval_with_null(&is_not_null(col.clone()), &not_nullable_schema, &col),
            Some(true)
        );

        assert_eq!(const_eval(&is_true(null.clone())), Some(false));
        assert_eq!(const_eval(&is_true(t.clone())), Some(true));
        assert_eq!(const_eval(&is_true(f.clone())), Some(false));
        assert_eq!(const_eval(&is_true(zero.clone())), Some(false));
        assert_eq!(const_eval(&is_true(one.clone())), Some(true));

        assert_eq!(const_eval(&is_not_true(null.clone())), Some(true));
        assert_eq!(const_eval(&is_not_true(t.clone())), Some(false));
        assert_eq!(const_eval(&is_not_true(f.clone())), Some(true));
        assert_eq!(const_eval(&is_not_true(zero.clone())), Some(true));
        assert_eq!(const_eval(&is_not_true(one.clone())), Some(false));

        assert_eq!(const_eval(&is_false(null.clone())), Some(false));
        assert_eq!(const_eval(&is_false(t.clone())), Some(false));
        assert_eq!(const_eval(&is_false(f.clone())), Some(true));
        assert_eq!(const_eval(&is_false(zero.clone())), Some(true));
        assert_eq!(const_eval(&is_false(one.clone())), Some(false));

        assert_eq!(const_eval(&is_not_false(null.clone())), Some(true));
        assert_eq!(const_eval(&is_not_false(t.clone())), Some(true));
        assert_eq!(const_eval(&is_not_false(f.clone())), Some(false));
        assert_eq!(const_eval(&is_not_false(zero.clone())), Some(false));
        assert_eq!(const_eval(&is_not_false(one.clone())), Some(true));

        assert_eq!(const_eval(&is_unknown(null.clone())), Some(true));
        assert_eq!(const_eval(&is_unknown(t.clone())), Some(false));
        assert_eq!(const_eval(&is_unknown(f.clone())), Some(false));
        assert_eq!(const_eval(&is_unknown(zero.clone())), Some(false));
        assert_eq!(const_eval(&is_unknown(one.clone())), Some(false));

        assert_eq!(const_eval(&is_not_unknown(null.clone())), Some(false));
        assert_eq!(const_eval(&is_not_unknown(t.clone())), Some(true));
        assert_eq!(const_eval(&is_not_unknown(f.clone())), Some(true));
        assert_eq!(const_eval(&is_not_unknown(zero.clone())), Some(true));
        assert_eq!(const_eval(&is_not_unknown(one.clone())), Some(true));
    }

    #[test]
    fn predicate_eval_udf() {
        let func = make_scalar_func_expr();

        assert_eq!(const_eval(&func.clone()), None);
        assert_eq!(const_eval(&not(func.clone())), None);
        assert_eq!(
            const_eval(&binary_expr(func.clone(), And, func.clone())),
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
    fn predicate_eval_when_then() {
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
            const_eval_with_null(&when, &nullable_schema, &x),
            Some(false)
        );
        assert_eq!(
            const_eval_with_null(&when, &not_nullable_schema, &x),
            Some(true)
        );
    }
}
