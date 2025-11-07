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

use crate::predicate_eval::TriStateBool::{False, True, Unknown};
use crate::{BinaryExpr, Expr, ExprSchemable};
use arrow::datatypes::DataType;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{ExprSchema, ScalarValue};
use datafusion_expr_common::operator::Operator;
use std::ops::{BitAnd, BitOr, Not};

/// Represents the possible values for SQL's three valued logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TriStateBool {
    True,
    False,
    Unknown,
}

impl From<&Option<bool>> for TriStateBool {
    fn from(value: &Option<bool>) -> Self {
        match value {
            None => Unknown,
            Some(true) => True,
            Some(false) => False,
        }
    }
}

impl TriStateBool {
    fn try_from(value: &ScalarValue) -> Option<TriStateBool> {
        match value {
            ScalarValue::Null => Some(Unknown),
            ScalarValue::Boolean(b) => Some(TriStateBool::from(b)),
            _ => Self::try_from(&value.cast_to(&DataType::Boolean).ok()?),
        }
    }

    /// [True] if self is [True], [False] otherwise.
    fn is_true(&self) -> TriStateBool {
        match self {
            True => True,
            Unknown | False => False,
        }
    }

    /// [True] if self is [False], [False] otherwise.
    fn is_false(&self) -> TriStateBool {
        match self {
            False => True,
            Unknown | True => False,
        }
    }

    /// [True] if self is [Unknown], [False] otherwise.
    fn is_unknown(&self) -> TriStateBool {
        match self {
            Unknown => True,
            True | False => False,
        }
    }
}

impl Not for TriStateBool {
    type Output = TriStateBool;

    fn not(self) -> Self::Output {
        // SQL three-valued logic NOT truth table:
        //
        // P       | !P
        // --------|-------
        // TRUE    | FALSE
        // FALSE   | TRUE
        // UNKNOWN | UNKNOWN
        match self {
            True => False,
            False => True,
            Unknown => Unknown,
        }
    }
}

impl BitAnd for TriStateBool {
    type Output = TriStateBool;

    fn bitand(self, rhs: Self) -> Self::Output {
        // SQL three-valued logic AND truth table:
        //
        // P       | Q       | P AND Q
        // --------|---------|----------
        // TRUE    | TRUE    | TRUE
        // TRUE    | FALSE   | FALSE
        // FALSE   | TRUE    | FALSE
        // FALSE   | FALSE   | FALSE
        // FALSE   | UNKNOWN | FALSE
        // UNKNOWN | FALSE   | FALSE
        // TRUE    | UNKNOWN | UNKNOWN
        // UNKNOWN | TRUE    | UNKNOWN
        // UNKNOWN | UNKNOWN | UNKNOWN
        match (self, rhs) {
            (True, True) => True,
            (False, _) | (_, False) => False,
            (Unknown, _) | (_, Unknown) => Unknown,
        }
    }
}

impl BitOr for TriStateBool {
    type Output = TriStateBool;

    fn bitor(self, rhs: Self) -> Self::Output {
        // SQL three-valued logic OR truth table:
        //
        // P       | Q       | P OR Q
        // --------|---------|----------
        // FALSE   | FALSE   | FALSE
        // TRUE    | TRUE    | TRUE
        // TRUE    | FALSE   | TRUE
        // FALSE   | TRUE    | TRUE
        // TRUE    | UNKNOWN | TRUE
        // UNKNOWN | TRUE    | TRUE
        // FALSE   | UNKNOWN | UNKNOWN
        // UNKNOWN | FALSE   | UNKNOWN
        // UNKNOWN | UNKNOWN | UNKNOWN
        match (self, rhs) {
            (False, False) => False,
            (True, _) | (_, True) => True,
            (Unknown, _) | (_, Unknown) => Unknown,
        }
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
    PredicateConstEvaluator {
        input_schema,
        evaluates_to_null,
    }
    .eval_predicate(predicate)
    .map(|b| matches!(b, True))
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
    fn eval_predicate(&self, predicate: &Expr) -> Option<TriStateBool> {
        match predicate {
            Expr::Literal(scalar, _) => {
                // Interpret literals as boolean, coercing if necessary and allowed
                TriStateBool::try_from(scalar)
            }
            Expr::Negative(e) => self.eval_predicate(e),
            Expr::IsNull(e) => {
                // If `e` is not nullable, then `e IS NULL` is provably false
                if let Ok(false) = e.nullable(self.input_schema) {
                    return Some(False);
                }

                match e.get_type(self.input_schema) {
                    // If `e` is a boolean expression, try to evaluate it and test for unknown
                    Ok(DataType::Boolean) => {
                        self.eval_predicate(e).map(|b| b.is_unknown())
                    }
                    // If `e` is not a boolean expression, check if `e` is provably null
                    Ok(_) => match self.is_null(e) {
                        // If `e` is provably null, then `e IS NULL` is provably true
                        True => Some(True),
                        // If `e` is provably not null, then `e IS NULL` is provably false
                        False => Some(False),
                        Unknown => None,
                    },
                    Err(_) => None,
                }
            }
            Expr::IsNotNull(e) => {
                // If `e` is not nullable, then `e IS NOT NULL` is provably true
                if let Ok(false) = e.nullable(self.input_schema) {
                    // If `e` is not nullable -> `e IS NOT NULL` is true
                    return Some(True);
                }

                match e.get_type(self.input_schema) {
                    // If `e` is a boolean expression, try to evaluate it and test for not unknown
                    Ok(DataType::Boolean) => {
                        self.eval_predicate(e).map(|b| !b.is_unknown())
                    }
                    // If `e` is not a boolean expression, check if `e` is provably null
                    Ok(_) => match self.is_null(e) {
                        // If `e` is provably null, then `e IS NOT NULL` is provably false
                        True => Some(False),
                        // If `e` is provably not null, then `e IS NOT NULL` is provably true
                        False => Some(True),
                        Unknown => None,
                    },
                    Err(_) => None,
                }
            }
            Expr::IsTrue(e) => self.eval_predicate(e).map(|b| b.is_true()),
            Expr::IsNotTrue(e) => self.eval_predicate(e).map(|b| !b.is_true()),
            Expr::IsFalse(e) => self.eval_predicate(e).map(|b| b.is_false()),
            Expr::IsNotFalse(e) => self.eval_predicate(e).map(|b| !b.is_false()),
            Expr::IsUnknown(e) => self.eval_predicate(e).map(|b| b.is_unknown()),
            Expr::IsNotUnknown(e) => self.eval_predicate(e).map(|b| !b.is_unknown()),
            Expr::Not(e) => self.eval_predicate(e).map(|b| !b),
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => {
                match (self.eval_predicate(left), self.eval_predicate(right)) {
                    // If either side is false, then the result is false regardless of the other side
                    (Some(False), _) | (_, Some(False)) => Some(False),
                    // If either side is inconclusive, then the result is inconclusive as well
                    (None, _) | (_, None) => None,
                    // Otherwise, defer to the tristate boolean algebra
                    (Some(l), Some(r)) => Some(l & r),
                }
            }
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Or,
                right,
            }) => {
                match (self.eval_predicate(left), self.eval_predicate(right)) {
                    // If either side is true, then the result is true regardless of the other side
                    (Some(True), _) | (_, Some(True)) => Some(True),
                    // If either side is inconclusive, then the result is inconclusive as well
                    (None, _) | (_, None) => None,
                    // Otherwise, defer to the tristate boolean algebra
                    (Some(l), Some(r)) => Some(l | r),
                }
            }
            e => match self.is_null(e) {
                // Null values coerce to unknown
                True => Some(Unknown),
                // Not null, but some unknown value -> inconclusive
                False | Unknown => None,
            },
        }
    }

    /// Determines if the given expression evaluates to `NULL`.
    fn is_null(&self, expr: &Expr) -> TriStateBool {
        match expr {
            Expr::Literal(s, _) => {
                if s.is_null() {
                    True
                } else {
                    False
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
                let mut is_null = False;
                let _ = expr.apply_children(|child| match self.is_null(child) {
                    False => Ok(TreeNodeRecursion::Continue),
                    n @ True | n @ Unknown => {
                        // If any child is null or inconclusive, this result applies to the
                        // entire expression and we can stop traversing
                        is_null = n;
                        Ok(TreeNodeRecursion::Stop)
                    }
                });
                is_null
            }
            e => {
                if let Ok(false) = e.nullable(self.input_schema) {
                    // If `expr` is not nullable, we can be certain `expr` is not null
                    False
                } else {
                    // Finally, ask the callback if it knows the nullness of `expr`
                    match (self.evaluates_to_null)(e) {
                        Some(true) => True,
                        Some(false) => False,
                        None => Unknown,
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::expr::ScalarFunction;
    use crate::predicate_eval::TriStateBool::*;
    use crate::predicate_eval::{const_eval_predicate, TriStateBool};
    use crate::{
        binary_expr, col, create_udf, is_false, is_not_false, is_not_null, is_not_true,
        is_not_unknown, is_null, is_true, is_unknown, lit, not, Expr,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{DFSchema, ScalarValue};
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use datafusion_expr_common::operator::Operator;
    use datafusion_expr_common::operator::Operator::Eq;
    use datafusion_expr_common::signature::Volatility;
    use std::sync::Arc;
    use Operator::{And, Or};

    #[test]
    fn tristate_bool_from_option() {
        assert_eq!(TriStateBool::from(&None), Unknown);
        assert_eq!(TriStateBool::from(&Some(true)), True);
        assert_eq!(TriStateBool::from(&Some(false)), False);
    }

    #[test]
    fn tristate_bool_from_scalar() {
        assert_eq!(TriStateBool::try_from(&ScalarValue::Null).unwrap(), Unknown);

        assert_eq!(
            TriStateBool::try_from(&ScalarValue::Boolean(None)).unwrap(),
            Unknown
        );
        assert_eq!(
            TriStateBool::try_from(&ScalarValue::Boolean(Some(true))).unwrap(),
            True
        );
        assert_eq!(
            TriStateBool::try_from(&ScalarValue::Boolean(Some(false))).unwrap(),
            False
        );

        assert_eq!(
            TriStateBool::try_from(&ScalarValue::UInt8(None)).unwrap(),
            Unknown
        );
        assert_eq!(
            TriStateBool::try_from(&ScalarValue::UInt8(Some(0))).unwrap(),
            False
        );
        assert_eq!(
            TriStateBool::try_from(&ScalarValue::UInt8(Some(1))).unwrap(),
            True
        );
    }

    #[test]
    fn tristate_bool_not() {
        assert_eq!(!Unknown, Unknown);
        assert_eq!(!False, True);
        assert_eq!(!True, False);
    }

    #[test]
    fn tristate_bool_and() {
        assert_eq!(Unknown & Unknown, Unknown);
        assert_eq!(Unknown & True, Unknown);
        assert_eq!(Unknown & False, False);
        assert_eq!(True & Unknown, Unknown);
        assert_eq!(True & True, True);
        assert_eq!(True & False, False);
        assert_eq!(False & Unknown, False);
        assert_eq!(False & True, False);
        assert_eq!(False & False, False);
    }

    #[test]
    fn tristate_bool_or() {
        assert_eq!(Unknown | Unknown, Unknown);
        assert_eq!(Unknown | True, True);
        assert_eq!(Unknown | False, Unknown);
        assert_eq!(True | Unknown, True);
        assert_eq!(True | True, True);
        assert_eq!(True | False, True);
        assert_eq!(False | Unknown, Unknown);
        assert_eq!(False | True, True);
        assert_eq!(False | False, False);
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
            None
        );
        assert_eq!(
            const_eval(&binary_expr(func.clone(), And, null.clone())),
            None
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
