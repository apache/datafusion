use crate::predicate_eval::TriStateBool::{False, True, Uncertain};
use crate::{BinaryExpr, Expr, ExprSchemable};
use arrow::datatypes::DataType;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, ExprSchema, ScalarValue};
use datafusion_expr_common::operator::Operator;

/// Represents the possible values for SQL's three valued logic.
/// `Option<bool>` is not used for this since `None` is used by
/// [const_eval_predicate] to represent inconclusive answers.
pub(super) enum TriStateBool {
    True,
    False,
    Uncertain,
}

impl TryFrom<&ScalarValue> for TriStateBool {
    type Error = DataFusionError;

    fn try_from(value: &ScalarValue) -> Result<Self, Self::Error> {
        match value {
            ScalarValue::Null => {
                // Literal null is equivalent to boolean uncertain
                Ok(Uncertain)
            },
            ScalarValue::Boolean(b) => Ok(match b {
                Some(true) => True,
                Some(false) => False,
                None => Uncertain,
            }),
            _ => Self::try_from(&value.cast_to(&DataType::Boolean)?),
        }
    }
}

/// Attempts to partially constant-evaluate a predicate under SQL three-valued logic.
///
/// Semantics of the return value:
/// - `Some(True)`      => predicate is provably true
/// - `Some(False)`     => predicate is provably false
/// - `Some(Uncertain)` => predicate is provably unknown (i.e., can only be NULL)
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
    input_schema: &dyn ExprSchema,
    evaluates_to_null: F,
) -> Option<TriStateBool>
where
    F: Fn(&Expr) -> TriStateBool,
{
    PredicateConstEvaluator {
        input_schema,
        evaluates_to_null,
    }
    .const_eval_predicate(predicate)
}

pub(super) struct PredicateConstEvaluator<'a, F> {
    input_schema: &'a dyn ExprSchema,
    evaluates_to_null: F,
}

impl<F> PredicateConstEvaluator<'_, F>
where
    F: Fn(&Expr) -> TriStateBool,
{
    fn const_eval_predicate(&self, predicate: &Expr) -> Option<TriStateBool> {
        match predicate {
            Expr::Literal(scalar, _) => TriStateBool::try_from(scalar).ok(),
            Expr::IsNotNull(e) => {
                if let Ok(false) = e.nullable(self.input_schema) {
                    // If `e` is not nullable, then `e IS NOT NULL` is always true
                    return Some(True);
                }

                match e.get_type(self.input_schema) {
                    Ok(DataType::Boolean) => match self.const_eval_predicate(e) {
                        Some(True) | Some(False) => Some(True),
                        Some(Uncertain) => Some(False),
                        None => None,
                    },
                    Ok(_) => match self.is_null(e) {
                        True => Some(False),
                        False => Some(True),
                        Uncertain => None,
                    },
                    Err(_) => None,
                }
            }
            Expr::IsNull(e) => {
                if let Ok(false) = e.nullable(self.input_schema) {
                    // If `e` is not nullable, then `e IS NULL` is always false
                    return Some(False);
                }

                match e.get_type(self.input_schema) {
                    Ok(DataType::Boolean) => match self.const_eval_predicate(e) {
                        Some(True) | Some(False) => Some(False),
                        Some(Uncertain) => Some(True),
                        None => None,
                    },
                    Ok(_) => match self.is_null(e) {
                        True => Some(True),
                        False => Some(False),
                        Uncertain => None,
                    },
                    Err(_) => None,
                }
            }
            Expr::IsTrue(e) => match self.const_eval_predicate(e) {
                Some(True) => Some(True),
                Some(_) => Some(False),
                None => None,
            },
            Expr::IsNotTrue(e) => match self.const_eval_predicate(e) {
                Some(True) => Some(False),
                Some(_) => Some(True),
                None => None,
            },
            Expr::IsFalse(e) => match self.const_eval_predicate(e) {
                Some(False) => Some(True),
                Some(_) => Some(False),
                None => None,
            },
            Expr::IsNotFalse(e) => match self.const_eval_predicate(e) {
                Some(False) => Some(False),
                Some(_) => Some(True),
                None => None,
            },
            Expr::IsUnknown(e) => match self.const_eval_predicate(e) {
                Some(Uncertain) => Some(True),
                Some(_) => Some(False),
                None => None,
            },
            Expr::IsNotUnknown(e) => match self.const_eval_predicate(e) {
                Some(Uncertain) => Some(False),
                Some(_) => Some(True),
                None => None,
            },
            Expr::Not(e) => match self.const_eval_predicate(e) {
                Some(True) => Some(False),
                Some(False) => Some(True),
                Some(Uncertain) => Some(Uncertain),
                None => None,
            },
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => {
                match (
                    self.const_eval_predicate(left),
                    self.const_eval_predicate(right),
                ) {
                    (Some(False), _) | (_, Some(False)) => Some(False),
                    (Some(True), Some(True)) => Some(True),
                    (Some(Uncertain), Some(_)) | (Some(_), Some(Uncertain)) => {
                        Some(Uncertain)
                    }
                    _ => None,
                }
            },
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::Or, right }) => {
                match (
                    self.const_eval_predicate(left),
                    self.const_eval_predicate(right),
                ) {
                    (Some(True), _) | (_, Some(True)) => Some(True),
                    (Some(False), Some(False)) => Some(False),
                    (Some(Uncertain), Some(_)) | (Some(_), Some(Uncertain)) => {
                        Some(Uncertain)
                    }
                    _ => None,
                }
            },
            e => match self.is_null(e) {
                True => Some(Uncertain),
                _ => None,
            },
        }
    }

    /// Determines if the given expression evaluates to `NULL`.
    ///
    /// This function returns:
    /// - `True` if `expr` is provably `NULL`
    /// - `False` if `expr` can provably not `NULL`
    /// - `Uncertain` if the result is inconclusive
    fn is_null(&self, expr: &Expr) -> TriStateBool {
        match expr {
            Expr::Literal(ScalarValue::Null, _) => {
                // Literal null is obviously null
                True
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
                    // Finally, ask the callback if it knows the nullability of `expr`
                    (self.evaluates_to_null)(e)
                }
            }
        }
    }
}
