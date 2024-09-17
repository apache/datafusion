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

//! Utility functions for expression simplification

use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::{
    expr::{Between, BinaryExpr, InList},
    expr_fn::{and, bitwise_and, bitwise_or, or},
    Expr, Like, Operator,
};

pub static POWS_OF_TEN: [i128; 38] = [
    1,
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
    10000000000,
    100000000000,
    1000000000000,
    10000000000000,
    100000000000000,
    1000000000000000,
    10000000000000000,
    100000000000000000,
    1000000000000000000,
    10000000000000000000,
    100000000000000000000,
    1000000000000000000000,
    10000000000000000000000,
    100000000000000000000000,
    1000000000000000000000000,
    10000000000000000000000000,
    100000000000000000000000000,
    1000000000000000000000000000,
    10000000000000000000000000000,
    100000000000000000000000000000,
    1000000000000000000000000000000,
    10000000000000000000000000000000,
    100000000000000000000000000000000,
    1000000000000000000000000000000000,
    10000000000000000000000000000000000,
    100000000000000000000000000000000000,
    1000000000000000000000000000000000000,
    10000000000000000000000000000000000000,
];

/// returns true if `needle` is found in a chain of search_op
/// expressions. Such as: (A AND B) AND C
pub fn expr_contains(expr: &Expr, needle: &Expr, search_op: Operator) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == search_op => {
            expr_contains(left, needle, search_op)
                || expr_contains(right, needle, search_op)
        }
        _ => expr == needle,
    }
}

/// Deletes all 'needles' or remains one 'needle' that are found in a chain of xor
/// expressions. Such as: A ^ (A ^ (B ^ A))
pub fn delete_xor_in_complex_expr(expr: &Expr, needle: &Expr, is_left: bool) -> Expr {
    /// Deletes recursively 'needles' in a chain of xor expressions
    fn recursive_delete_xor_in_expr(
        expr: &Expr,
        needle: &Expr,
        xor_counter: &mut i32,
    ) -> Expr {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if *op == Operator::BitwiseXor =>
            {
                let left_expr = recursive_delete_xor_in_expr(left, needle, xor_counter);
                let right_expr = recursive_delete_xor_in_expr(right, needle, xor_counter);
                if left_expr == *needle {
                    *xor_counter += 1;
                    return right_expr;
                } else if right_expr == *needle {
                    *xor_counter += 1;
                    return left_expr;
                }

                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(left_expr),
                    *op,
                    Box::new(right_expr),
                ))
            }
            _ => expr.clone(),
        }
    }

    let mut xor_counter: i32 = 0;
    let result_expr = recursive_delete_xor_in_expr(expr, needle, &mut xor_counter);
    if result_expr == *needle {
        return needle.clone();
    } else if xor_counter % 2 == 0 {
        if is_left {
            return Expr::BinaryExpr(BinaryExpr::new(
                Box::new(needle.clone()),
                Operator::BitwiseXor,
                Box::new(result_expr),
            ));
        } else {
            return Expr::BinaryExpr(BinaryExpr::new(
                Box::new(result_expr),
                Operator::BitwiseXor,
                Box::new(needle.clone()),
            ));
        }
    }
    result_expr
}

pub fn is_zero(s: &Expr) -> bool {
    match s {
        Expr::Literal(ScalarValue::Int8(Some(0)))
        | Expr::Literal(ScalarValue::Int16(Some(0)))
        | Expr::Literal(ScalarValue::Int32(Some(0)))
        | Expr::Literal(ScalarValue::Int64(Some(0)))
        | Expr::Literal(ScalarValue::UInt8(Some(0)))
        | Expr::Literal(ScalarValue::UInt16(Some(0)))
        | Expr::Literal(ScalarValue::UInt32(Some(0)))
        | Expr::Literal(ScalarValue::UInt64(Some(0))) => true,
        Expr::Literal(ScalarValue::Float32(Some(v))) if *v == 0. => true,
        Expr::Literal(ScalarValue::Float64(Some(v))) if *v == 0. => true,
        Expr::Literal(ScalarValue::Decimal128(Some(v), _p, _s)) if *v == 0 => true,
        _ => false,
    }
}

pub fn is_one(s: &Expr) -> bool {
    match s {
        Expr::Literal(ScalarValue::Int8(Some(1)))
        | Expr::Literal(ScalarValue::Int16(Some(1)))
        | Expr::Literal(ScalarValue::Int32(Some(1)))
        | Expr::Literal(ScalarValue::Int64(Some(1)))
        | Expr::Literal(ScalarValue::UInt8(Some(1)))
        | Expr::Literal(ScalarValue::UInt16(Some(1)))
        | Expr::Literal(ScalarValue::UInt32(Some(1)))
        | Expr::Literal(ScalarValue::UInt64(Some(1))) => true,
        Expr::Literal(ScalarValue::Float32(Some(v))) if *v == 1. => true,
        Expr::Literal(ScalarValue::Float64(Some(v))) if *v == 1. => true,
        Expr::Literal(ScalarValue::Decimal128(Some(v), _p, s)) => {
            *s >= 0
                && POWS_OF_TEN
                    .get(*s as usize)
                    .map(|x| x == v)
                    .unwrap_or_default()
        }
        _ => false,
    }
}

pub fn is_true(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(ScalarValue::Boolean(Some(v))) => *v,
        _ => false,
    }
}

/// returns true if expr is a
/// `Expr::Literal(ScalarValue::Boolean(v))` , false otherwise
pub fn is_bool_lit(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(ScalarValue::Boolean(_)))
}

/// Return a literal NULL value of Boolean data type
pub fn lit_bool_null() -> Expr {
    Expr::Literal(ScalarValue::Boolean(None))
}

pub fn is_null(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(v) => v.is_null(),
        _ => false,
    }
}

pub fn is_false(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(ScalarValue::Boolean(Some(v))) => !(*v),
        _ => false,
    }
}

/// returns true if `haystack` looks like (needle OP X) or (X OP needle)
pub fn is_op_with(target_op: Operator, haystack: &Expr, needle: &Expr) -> bool {
    matches!(haystack, Expr::BinaryExpr(BinaryExpr { left, op, right }) if op == &target_op && (needle == left.as_ref() || needle == right.as_ref()))
}

/// returns true if `not_expr` is !`expr` (not)
pub fn is_not_of(not_expr: &Expr, expr: &Expr) -> bool {
    matches!(not_expr, Expr::Not(inner) if expr == inner.as_ref())
}

/// returns true if `not_expr` is !`expr` (bitwise not)
pub fn is_negative_of(not_expr: &Expr, expr: &Expr) -> bool {
    matches!(not_expr, Expr::Negative(inner) if expr == inner.as_ref())
}

/// returns the contained boolean value in `expr` as
/// `Expr::Literal(ScalarValue::Boolean(v))`.
pub fn as_bool_lit(expr: &Expr) -> Result<Option<bool>> {
    match expr {
        Expr::Literal(ScalarValue::Boolean(v)) => Ok(*v),
        _ => internal_err!("Expected boolean literal, got {expr:?}"),
    }
}

/// negate a Not clause
/// input is the clause to be negated.(args of Not clause)
/// For BinaryExpr, use the negation of op instead.
///    not ( A > B) ===> (A <= B)
/// For BoolExpr, not (A and B) ===> (not A) or (not B)
///     not (A or B) ===> (not A) and (not B)
///     not (not A) ===> A
/// For NullExpr, not (A is not null) ===> A is null
///     not (A is null) ===> A is not null
/// For InList, not (A not in (..)) ===> A in (..)
///     not (A in (..)) ===> A not in (..)
/// For Between, not (A between B and C) ===> (A not between B and C)
///     not (A not between B and C) ===> (A between B and C)
/// For others, use Not clause
pub fn negate_clause(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if let Some(negated_op) = op.negate() {
                return Expr::BinaryExpr(BinaryExpr::new(left, negated_op, right));
            }
            match op {
                // not (A and B) ===> (not A) or (not B)
                Operator::And => {
                    let left = negate_clause(*left);
                    let right = negate_clause(*right);

                    or(left, right)
                }
                // not (A or B) ===> (not A) and (not B)
                Operator::Or => {
                    let left = negate_clause(*left);
                    let right = negate_clause(*right);

                    and(left, right)
                }
                // use not clause
                _ => Expr::Not(Box::new(Expr::BinaryExpr(BinaryExpr::new(
                    left, op, right,
                )))),
            }
        }
        // not (not A) ===> A
        Expr::Not(expr) => *expr,
        // not (A is not null) ===> A is null
        Expr::IsNotNull(expr) => expr.is_null(),
        // not (A is null) ===> A is not null
        Expr::IsNull(expr) => expr.is_not_null(),
        // not (A not in (..)) ===> A in (..)
        // not (A in (..)) ===> A not in (..)
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => expr.in_list(list, !negated),
        // not (A between B and C) ===> (A not between B and C)
        // not (A not between B and C) ===> (A between B and C)
        Expr::Between(between) => Expr::Between(Between::new(
            between.expr,
            !between.negated,
            between.low,
            between.high,
        )),
        // not (A like B) ===> A not like B
        Expr::Like(like) => Expr::Like(Like::new(
            !like.negated,
            like.expr,
            like.pattern,
            like.escape_char,
            like.case_insensitive,
        )),
        // use not clause
        _ => Expr::Not(Box::new(expr)),
    }
}

/// bitwise negate a Negative clause
/// input is the clause to be bitwise negated.(args for Negative clause)
/// For BinaryExpr:
///    ~(A & B) ===> ~A | ~B
///    ~(A | B) ===> ~A & ~B
/// For Negative:
///    ~(~A) ===> A
/// For others, use Negative clause
pub fn distribute_negation(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            match op {
                // ~(A & B) ===> ~A | ~B
                Operator::BitwiseAnd => {
                    let left = distribute_negation(*left);
                    let right = distribute_negation(*right);

                    bitwise_or(left, right)
                }
                // ~(A | B) ===> ~A & ~B
                Operator::BitwiseOr => {
                    let left = distribute_negation(*left);
                    let right = distribute_negation(*right);

                    bitwise_and(left, right)
                }
                // use negative clause
                _ => Expr::Negative(Box::new(Expr::BinaryExpr(BinaryExpr::new(
                    left, op, right,
                )))),
            }
        }
        // ~(~A) ===> A
        Expr::Negative(expr) => *expr,
        // use negative clause
        _ => Expr::Negative(Box::new(expr)),
    }
}
