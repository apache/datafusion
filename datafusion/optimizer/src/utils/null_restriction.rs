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

//! Syntactic null-restriction evaluator used by optimizer fast paths.

use std::collections::HashSet;

use datafusion_common::{Column, ScalarValue};
use datafusion_expr::{BinaryExpr, Expr, Operator};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NullSubstitutionValue {
    /// SQL NULL after substituting join columns with NULL.
    Null,
    /// Known to be non-null, but value is otherwise unknown.
    NonNull,
    /// A known boolean outcome from SQL three-valued logic.
    Boolean(bool),
}

impl NullSubstitutionValue {
    fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }
}

pub(super) fn syntactic_restrict_null_predicate(
    predicate: &Expr,
    join_cols: &HashSet<&Column>,
) -> Option<bool> {
    match syntactic_null_substitution_value(predicate, join_cols) {
        Some(NullSubstitutionValue::Boolean(true)) => Some(false),
        Some(NullSubstitutionValue::Boolean(false) | NullSubstitutionValue::Null) => {
            Some(true)
        }
        Some(NullSubstitutionValue::NonNull) | None => None,
    }
}

fn not(value: Option<NullSubstitutionValue>) -> Option<NullSubstitutionValue> {
    match value {
        Some(NullSubstitutionValue::Boolean(value)) => {
            Some(NullSubstitutionValue::Boolean(!value))
        }
        Some(NullSubstitutionValue::Null) => Some(NullSubstitutionValue::Null),
        Some(NullSubstitutionValue::NonNull) | None => None,
    }
}

fn binary_boolean_value(
    left: Option<NullSubstitutionValue>,
    right: Option<NullSubstitutionValue>,
    when_short_circuit: bool,
) -> Option<NullSubstitutionValue> {
    let short_circuit = Some(NullSubstitutionValue::Boolean(when_short_circuit));
    let identity = Some(NullSubstitutionValue::Boolean(!when_short_circuit));

    if left == short_circuit || right == short_circuit {
        return short_circuit;
    }

    match (left, right) {
        (value, other) if value == identity => other,
        (other, value) if value == identity => other,
        (Some(NullSubstitutionValue::Null), Some(NullSubstitutionValue::Null)) => {
            Some(NullSubstitutionValue::Null)
        }
        (Some(NullSubstitutionValue::NonNull), _)
        | (_, Some(NullSubstitutionValue::NonNull))
        | (None, _)
        | (_, None) => None,
        (left, right) => {
            debug_assert_eq!(left, right);
            left
        }
    }
}

fn null_check_value(
    value: Option<NullSubstitutionValue>,
    is_not_null: bool,
) -> Option<NullSubstitutionValue> {
    match value {
        Some(NullSubstitutionValue::Null) => {
            Some(NullSubstitutionValue::Boolean(!is_not_null))
        }
        Some(NullSubstitutionValue::NonNull | NullSubstitutionValue::Boolean(_)) => {
            Some(NullSubstitutionValue::Boolean(is_not_null))
        }
        None => None,
    }
}

fn null_if_contains_null(
    values: impl IntoIterator<Item = Option<NullSubstitutionValue>>,
) -> Option<NullSubstitutionValue> {
    values
        .into_iter()
        .any(|value| matches!(value, Some(NullSubstitutionValue::Null)))
        .then_some(NullSubstitutionValue::Null)
}

fn strict_null_only(
    value: Option<NullSubstitutionValue>,
) -> Option<NullSubstitutionValue> {
    value.filter(|value| value.is_null())
}

fn syntactic_null_substitution_value(
    expr: &Expr,
    join_cols: &HashSet<&Column>,
) -> Option<NullSubstitutionValue> {
    // This evaluator intentionally supports a strict subset of expressions:
    // aliases/columns/literals, boolean combinators (NOT/AND/OR), null checks
    // (IS [NOT] NULL), BETWEEN, strict-null-preserving unary operators
    // (CAST/TRY_CAST/NEGATIVE), LIKE/SIMILAR TO, and binary operators handled in
    // `syntactic_binary_value`.
    //
    // Returning `None` means "defer to the authoritative evaluator" rather than
    // "not null-restricting". Any unsupported expression variant must return
    // `None` so callers can safely fall back to full expression evaluation.
    match expr {
        Expr::Alias(alias) => {
            syntactic_null_substitution_value(alias.expr.as_ref(), join_cols)
        }
        Expr::Column(column) => join_cols
            .contains(column)
            .then_some(NullSubstitutionValue::Null),
        Expr::Literal(value, _) => Some(scalar_to_null_substitution_value(value)),
        Expr::BinaryExpr(binary_expr) => syntactic_binary_value(binary_expr, join_cols),
        Expr::Not(expr) => {
            not(syntactic_null_substitution_value(expr.as_ref(), join_cols))
        }
        Expr::IsNull(expr) => null_check_value(
            syntactic_null_substitution_value(expr.as_ref(), join_cols),
            false,
        ),
        Expr::IsNotNull(expr) => null_check_value(
            syntactic_null_substitution_value(expr.as_ref(), join_cols),
            true,
        ),
        Expr::Between(between) => null_if_contains_null([
            syntactic_null_substitution_value(between.expr.as_ref(), join_cols),
            syntactic_null_substitution_value(between.low.as_ref(), join_cols),
            syntactic_null_substitution_value(between.high.as_ref(), join_cols),
        ]),
        Expr::Cast(cast) => strict_null_only(syntactic_null_substitution_value(
            cast.expr.as_ref(),
            join_cols,
        )),
        Expr::TryCast(try_cast) => strict_null_only(syntactic_null_substitution_value(
            try_cast.expr.as_ref(),
            join_cols,
        )),
        Expr::Negative(expr) => {
            strict_null_only(syntactic_null_substitution_value(expr.as_ref(), join_cols))
        }
        Expr::Like(like) | Expr::SimilarTo(like) => null_if_contains_null([
            syntactic_null_substitution_value(like.expr.as_ref(), join_cols),
            syntactic_null_substitution_value(like.pattern.as_ref(), join_cols),
        ]),
        Expr::Exists { .. }
        | Expr::InList(_)
        | Expr::InSubquery(_)
        | Expr::SetComparison(_)
        | Expr::ScalarSubquery(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::Placeholder(_)
        | Expr::ScalarVariable(_, _)
        | Expr::Unnest(_)
        | Expr::GroupingSet(_)
        | Expr::WindowFunction(_)
        | Expr::ScalarFunction(_)
        | Expr::Case(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_) => None,
        Expr::AggregateFunction(_) => None,
        // TODO: remove the next line after `Expr::Wildcard` is removed
        #[expect(deprecated)]
        Expr::Wildcard { .. } => None,
    }
}

fn scalar_to_null_substitution_value(value: &ScalarValue) -> NullSubstitutionValue {
    match value {
        _ if value.is_null() => NullSubstitutionValue::Null,
        ScalarValue::Boolean(Some(value)) => NullSubstitutionValue::Boolean(*value),
        _ => NullSubstitutionValue::NonNull,
    }
}

fn is_strict_null_binary_op(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::Plus
            | Operator::Minus
            | Operator::Multiply
            | Operator::Divide
            | Operator::Modulo
            | Operator::RegexMatch
            | Operator::RegexIMatch
            | Operator::RegexNotMatch
            | Operator::RegexNotIMatch
            | Operator::LikeMatch
            | Operator::ILikeMatch
            | Operator::NotLikeMatch
            | Operator::NotILikeMatch
            | Operator::BitwiseAnd
            | Operator::BitwiseOr
            | Operator::BitwiseXor
            | Operator::BitwiseShiftRight
            | Operator::BitwiseShiftLeft
            | Operator::StringConcat
            | Operator::AtArrow
            | Operator::ArrowAt
            | Operator::Arrow
            | Operator::LongArrow
            | Operator::HashArrow
            | Operator::HashLongArrow
            | Operator::AtAt
            | Operator::IntegerDivide
            | Operator::HashMinus
            | Operator::AtQuestion
            | Operator::Question
            | Operator::QuestionAnd
            | Operator::QuestionPipe
            | Operator::Colon
    )
}

fn syntactic_binary_value(
    binary_expr: &BinaryExpr,
    join_cols: &HashSet<&Column>,
) -> Option<NullSubstitutionValue> {
    let left = syntactic_null_substitution_value(binary_expr.left.as_ref(), join_cols);
    let right = syntactic_null_substitution_value(binary_expr.right.as_ref(), join_cols);

    match binary_expr.op {
        Operator::And => binary_boolean_value(left, right, false),
        Operator::Or => binary_boolean_value(left, right, true),
        Operator::IsDistinctFrom | Operator::IsNotDistinctFrom => None,
        op => is_strict_null_binary_op(op)
            .then(|| null_if_contains_null([left, right]))
            .flatten(),
    }
}
