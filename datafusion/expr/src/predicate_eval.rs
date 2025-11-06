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
enum TriStateBool {
    True,
    False,
    Uncertain,
}

impl From<Option<bool>> for TriStateBool {
    fn from(value: Option<bool>) -> Self {
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
    .const_eval_predicate(predicate)
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
    fn const_eval_predicate(&self, predicate: &Expr) -> Option<TriStateBool> {
        match predicate {
            Expr::Literal(scalar, _) => TriStateBool::try_from(scalar).ok(),
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
                    (self.evaluates_to_null)(e).into()
                }
            }
        }
    }
}
