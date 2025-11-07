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

use crate::predicate_eval::TriStateBool::{False, True, Uncertain};
use crate::{BinaryExpr, Expr, ExprSchemable};
use arrow::datatypes::DataType;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, ExprSchema, ScalarValue};
use datafusion_expr_common::operator::Operator;
use std::ops::{BitAnd, BitOr, Not};

/// Represents the possible values for SQL's three valued logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TriStateBool {
    True,
    False,
    Uncertain,
}

impl From<&Option<bool>> for TriStateBool {
    fn from(value: &Option<bool>) -> Self {
        match value {
            None => Uncertain,
            Some(true) => True,
            Some(false) => False,
        }
    }
}

impl TryFrom<&ScalarValue> for TriStateBool {
    type Error = DataFusionError;

    fn try_from(value: &ScalarValue) -> Result<Self, Self::Error> {
        match value {
            ScalarValue::Null => {
                // Literal null is equivalent to boolean uncertain
                Ok(Uncertain)
            }
            ScalarValue::Boolean(b) => Ok(match b {
                Some(true) => True,
                Some(false) => False,
                None => Uncertain,
            }),
            _ => Self::try_from(&value.cast_to(&DataType::Boolean)?),
        }
    }
}

impl TriStateBool {
    fn try_from_no_cooerce(value: &ScalarValue) -> Option<Self> {
        match value {
            ScalarValue::Null => Some(Uncertain),
            ScalarValue::Boolean(b) => Some(TriStateBool::from(b)),
            _ => None,
        }
    }

    fn is_null(&self) -> TriStateBool {
        match self {
            True | False => False,
            Uncertain => True,
        }
    }

    fn is_true(&self) -> TriStateBool {
        match self {
            True => True,
            Uncertain | False => False,
        }
    }

    fn is_false(&self) -> TriStateBool {
        match self {
            False => True,
            Uncertain | True => False,
        }
    }

    fn is_unknown(&self) -> TriStateBool {
        match self {
            Uncertain => True,
            True | False => False,
        }
    }
}

impl Not for TriStateBool {
    type Output = TriStateBool;

    fn not(self) -> Self::Output {
        match self {
            True => False,
            False => True,
            Uncertain => Uncertain,
        }
    }
}

impl BitAnd for TriStateBool {
    type Output = TriStateBool;

    fn bitand(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (False, _) | (_, False) => False,
            (Uncertain, _) | (_, Uncertain) => Uncertain,
            (True, True) => True,
        }
    }
}

impl BitOr for TriStateBool {
    type Output = TriStateBool;

    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (True, _) | (_, True) => True,
            (Uncertain, _) | (_, Uncertain) => Uncertain,
            (False, False) => False,
        }
    }
}

/// Attempts to partially constant-evaluate a predicate under SQL three-valued logic.
///
/// Semantics of the return value:
/// - `Some(true)`      => predicate is provably true
/// - `Some(false)`     => predicate is provably false
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
    .const_eval_predicate_coerced(predicate)
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
    fn const_eval_predicate_coerced(&self, predicate: &Expr) -> Option<TriStateBool> {
        match predicate {
            Expr::Literal(scalar, _) => TriStateBool::try_from(scalar).ok(),
            e => self.const_eval_predicate(e),
        }
    }

    fn const_eval_predicate(&self, predicate: &Expr) -> Option<TriStateBool> {
        match predicate {
            Expr::Literal(scalar, _) => TriStateBool::try_from_no_cooerce(scalar),
            Expr::IsNotNull(e) => {
                if let Ok(false) = e.nullable(self.input_schema) {
                    // If `e` is not nullable -> `e IS NOT NULL` is true
                    return Some(True);
                }

                match e.get_type(self.input_schema) {
                    Ok(DataType::Boolean) => {
                        self.const_eval_predicate(e).map(|b| b.is_null())
                    }
                    Ok(_) => match self.evaluates_to_null(e) {
                        True => Some(False),
                        False => Some(True),
                        Uncertain => None,
                    },
                    Err(_) => None,
                }
            }
            Expr::IsNull(e) => {
                if let Ok(false) = e.nullable(self.input_schema) {
                    // If `e` is not nullable -> `e IS NULL` is false
                    return Some(False);
                }

                match e.get_type(self.input_schema) {
                    Ok(DataType::Boolean) => {
                        self.const_eval_predicate(e).map(|b| !b.is_null())
                    }
                    Ok(_) => match self.evaluates_to_null(e) {
                        True => Some(True),
                        False => Some(False),
                        Uncertain => None,
                    },
                    Err(_) => None,
                }
            }
            Expr::IsTrue(e) => self.const_eval_predicate(e).map(|b| b.is_true()),
            Expr::IsNotTrue(e) => self.const_eval_predicate(e).map(|b| !b.is_true()),
            Expr::IsFalse(e) => self.const_eval_predicate(e).map(|b| b.is_false()),
            Expr::IsNotFalse(e) => self.const_eval_predicate(e).map(|b| !b.is_false()),
            Expr::IsUnknown(e) => self.const_eval_predicate(e).map(|b| b.is_unknown()),
            Expr::IsNotUnknown(e) => {
                self.const_eval_predicate(e).map(|b| !b.is_unknown())
            }
            Expr::Not(e) => self.const_eval_predicate(e).map(|b| !b),
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => {
                match (
                    self.const_eval_predicate(left),
                    self.const_eval_predicate(right),
                ) {
                    (Some(False), _) | (_, Some(False)) => Some(False),
                    (None, _) | (_, None) => None,
                    (Some(l), Some(r)) => Some(l & r),
                }
            }
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Or,
                right,
            }) => {
                match (
                    self.const_eval_predicate(left),
                    self.const_eval_predicate(right),
                ) {
                    (Some(True), _) | (_, Some(True)) => Some(True),
                    (None, _) | (_, None) => None,
                    (Some(l), Some(r)) => Some(l | r),
                }
            }
            e => match self.evaluates_to_null(e) {
                True => Some(Uncertain),
                _ => None,
            },
        }
    }

    /// Determines if the given expression evaluates to `NULL`.
    ///
    /// This function returns:
    /// - `True` if `expr` is provably `NULL`
    /// - `False` if `expr` is provably not `NULL`
    /// - `Uncertain` if the result is inconclusive
    fn evaluates_to_null(&self, expr: &Expr) -> TriStateBool {
        match expr {
            Expr::Literal(s, _) => {
                // Literal null is obviously null
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
                let _ =
                    expr.apply_children(|child| match self.evaluates_to_null(child) {
                        True => {
                            is_null = True;
                            Ok(TreeNodeRecursion::Stop)
                        }
                        False => Ok(TreeNodeRecursion::Continue),
                        Uncertain => {
                            is_null = Uncertain;
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
                    let evaluates_to_null = (self.evaluates_to_null)(e);
                    TriStateBool::from(&evaluates_to_null)
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
        binary_expr, create_udf, is_false, is_not_false, is_not_null, is_not_true,
        is_not_unknown, is_null, is_true, is_unknown, lit, not, Expr,
    };
    use arrow::datatypes::{DataType, Schema};
    use datafusion_common::{DFSchema, ScalarValue};
    use datafusion_expr_common::columnar_value::ColumnarValue;
    use datafusion_expr_common::operator::Operator;
    use datafusion_expr_common::signature::Volatility;
    use std::sync::Arc;
    use Operator::{And, Or};

    #[test]
    fn tristate_bool_from_option() {
        assert_eq!(TriStateBool::from(&None), Uncertain);
        assert_eq!(TriStateBool::from(&Some(true)), True);
        assert_eq!(TriStateBool::from(&Some(false)), False);
    }

    #[test]
    fn tristate_bool_from_scalar() {
        assert_eq!(
            TriStateBool::try_from(&ScalarValue::Null).unwrap(),
            Uncertain
        );

        assert_eq!(
            TriStateBool::try_from(&ScalarValue::Boolean(None)).unwrap(),
            Uncertain
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
            Uncertain
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
    fn tristate_bool_from_scalar_no_cooerce() {
        assert_eq!(
            TriStateBool::try_from_no_cooerce(&ScalarValue::Null).unwrap(),
            Uncertain
        );

        assert_eq!(
            TriStateBool::try_from_no_cooerce(&ScalarValue::Boolean(None)).unwrap(),
            Uncertain
        );
        assert_eq!(
            TriStateBool::try_from_no_cooerce(&ScalarValue::Boolean(Some(true))).unwrap(),
            True
        );
        assert_eq!(
            TriStateBool::try_from_no_cooerce(&ScalarValue::Boolean(Some(false)))
                .unwrap(),
            False
        );

        assert_eq!(
            TriStateBool::try_from_no_cooerce(&ScalarValue::UInt8(None)),
            None
        );
        assert_eq!(
            TriStateBool::try_from_no_cooerce(&ScalarValue::UInt8(Some(0))),
            None
        );
        assert_eq!(
            TriStateBool::try_from_no_cooerce(&ScalarValue::UInt8(Some(1))),
            None
        );
    }

    #[test]
    fn tristate_bool_not() {
        assert_eq!(!Uncertain, Uncertain);
        assert_eq!(!False, True);
        assert_eq!(!True, False);
    }

    #[test]
    fn tristate_bool_and() {
        assert_eq!(Uncertain & Uncertain, Uncertain);
        assert_eq!(Uncertain & True, Uncertain);
        assert_eq!(Uncertain & False, False);
        assert_eq!(True & Uncertain, Uncertain);
        assert_eq!(True & True, True);
        assert_eq!(True & False, False);
        assert_eq!(False & Uncertain, False);
        assert_eq!(False & True, False);
        assert_eq!(False & False, False);
    }

    #[test]
    fn tristate_bool_or() {
        assert_eq!(Uncertain | Uncertain, Uncertain);
        assert_eq!(Uncertain | True, True);
        assert_eq!(Uncertain | False, Uncertain);
        assert_eq!(True | Uncertain, True);
        assert_eq!(True | True, True);
        assert_eq!(True | False, True);
        assert_eq!(False | Uncertain, Uncertain);
        assert_eq!(False | True, True);
        assert_eq!(False | False, False);
    }

    fn const_eval(predicate: &Expr) -> Option<bool> {
        let schema = DFSchema::try_from(Schema::empty()).unwrap();
        const_eval_predicate(predicate, |_| None, &schema)
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
            None
        );
        assert_eq!(
            const_eval(&binary_expr(null.clone(), And, zero.clone())),
            None
        );

        assert_eq!(
            const_eval(&binary_expr(one.clone(), And, one.clone())),
            None
        );
        assert_eq!(
            const_eval(&binary_expr(one.clone(), And, zero.clone())),
            None
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
            None
        );
        assert_eq!(
            const_eval(&binary_expr(null.clone(), Or, zero.clone())),
            None
        );

        assert_eq!(const_eval(&binary_expr(one.clone(), Or, one.clone())), None);
        assert_eq!(
            const_eval(&binary_expr(one.clone(), Or, zero.clone())),
            None
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
        assert_eq!(const_eval(&not(one.clone())), None);
        assert_eq!(const_eval(&not(zero.clone())), None);

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

        assert_eq!(const_eval(&is_null(null.clone())), Some(true));
        assert_eq!(const_eval(&is_null(one.clone())), Some(false));

        assert_eq!(const_eval(&is_not_null(null.clone())), Some(false));
        assert_eq!(const_eval(&is_not_null(one.clone())), Some(true));

        assert_eq!(const_eval(&is_true(null.clone())), Some(false));
        assert_eq!(const_eval(&is_true(t.clone())), Some(true));
        assert_eq!(const_eval(&is_true(f.clone())), Some(false));
        assert_eq!(const_eval(&is_true(zero.clone())), None);
        assert_eq!(const_eval(&is_true(one.clone())), None);

        assert_eq!(const_eval(&is_not_true(null.clone())), Some(true));
        assert_eq!(const_eval(&is_not_true(t.clone())), Some(false));
        assert_eq!(const_eval(&is_not_true(f.clone())), Some(true));
        assert_eq!(const_eval(&is_not_true(zero.clone())), None);
        assert_eq!(const_eval(&is_not_true(one.clone())), None);

        assert_eq!(const_eval(&is_false(null.clone())), Some(false));
        assert_eq!(const_eval(&is_false(t.clone())), Some(false));
        assert_eq!(const_eval(&is_false(f.clone())), Some(true));
        assert_eq!(const_eval(&is_false(zero.clone())), None);
        assert_eq!(const_eval(&is_false(one.clone())), None);

        assert_eq!(const_eval(&is_not_false(null.clone())), Some(true));
        assert_eq!(const_eval(&is_not_false(t.clone())), Some(true));
        assert_eq!(const_eval(&is_not_false(f.clone())), Some(false));
        assert_eq!(const_eval(&is_not_false(zero.clone())), None);
        assert_eq!(const_eval(&is_not_false(one.clone())), None);

        assert_eq!(const_eval(&is_unknown(null.clone())), Some(true));
        assert_eq!(const_eval(&is_unknown(t.clone())), Some(false));
        assert_eq!(const_eval(&is_unknown(f.clone())), Some(false));
        assert_eq!(const_eval(&is_unknown(zero.clone())), None);
        assert_eq!(const_eval(&is_unknown(one.clone())), None);

        assert_eq!(const_eval(&is_not_unknown(null.clone())), Some(false));
        assert_eq!(const_eval(&is_not_unknown(t.clone())), Some(true));
        assert_eq!(const_eval(&is_not_unknown(f.clone())), Some(true));
        assert_eq!(const_eval(&is_not_unknown(zero.clone())), None);
        assert_eq!(const_eval(&is_not_unknown(one.clone())), None);
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
}
