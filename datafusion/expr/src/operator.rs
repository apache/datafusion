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

//! Operator module contains foundational types that are used to represent operators in DataFusion.

use crate::expr_fn::binary_expr;
use crate::Expr;
use crate::Like;
use std::fmt;
use std::ops;
use std::ops::Not;

/// Operators applied to expressions
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Operator {
    /// Expressions are equal
    Eq,
    /// Expressions are not equal
    NotEq,
    /// Left side is smaller than right side
    Lt,
    /// Left side is smaller or equal to right side
    LtEq,
    /// Left side is greater than right side
    Gt,
    /// Left side is greater or equal to right side
    GtEq,
    /// Addition
    Plus,
    /// Subtraction
    Minus,
    /// Multiplication operator, like `*`
    Multiply,
    /// Division operator, like `/`
    Divide,
    /// Remainder operator, like `%`
    Modulo,
    /// Logical AND, like `&&`
    And,
    /// Logical OR, like `||`
    Or,
    /// `IS DISTINCT FROM` (see [`distinct`])
    ///
    /// [`distinct`]: arrow::compute::kernels::cmp::distinct
    IsDistinctFrom,
    /// `IS NOT DISTINCT FROM` (see [`not_distinct`])
    ///
    /// [`not_distinct`]: arrow::compute::kernels::cmp::not_distinct
    IsNotDistinctFrom,
    /// Case sensitive regex match
    RegexMatch,
    /// Case insensitive regex match
    RegexIMatch,
    /// Case sensitive regex not match
    RegexNotMatch,
    /// Case insensitive regex not match
    RegexNotIMatch,
    /// Bitwise and, like `&`
    BitwiseAnd,
    /// Bitwise or, like `|`
    BitwiseOr,
    /// Bitwise xor, such as `^` in MySQL or `#` in PostgreSQL
    BitwiseXor,
    /// Bitwise right, like `>>`
    BitwiseShiftRight,
    /// Bitwise left, like `<<`
    BitwiseShiftLeft,
    /// String concat
    StringConcat,
    /// At arrow, like `@>`
    AtArrow,
    /// Arrow at, like `<@`
    ArrowAt,
}

impl Operator {
    /// If the operator can be negated, return the negated operator
    /// otherwise return None
    pub fn negate(&self) -> Option<Operator> {
        match self {
            Operator::Eq => Some(Operator::NotEq),
            Operator::NotEq => Some(Operator::Eq),
            Operator::Lt => Some(Operator::GtEq),
            Operator::LtEq => Some(Operator::Gt),
            Operator::Gt => Some(Operator::LtEq),
            Operator::GtEq => Some(Operator::Lt),
            Operator::IsDistinctFrom => Some(Operator::IsNotDistinctFrom),
            Operator::IsNotDistinctFrom => Some(Operator::IsDistinctFrom),
            Operator::Plus
            | Operator::Minus
            | Operator::Multiply
            | Operator::Divide
            | Operator::Modulo
            | Operator::And
            | Operator::Or
            | Operator::RegexMatch
            | Operator::RegexIMatch
            | Operator::RegexNotMatch
            | Operator::RegexNotIMatch
            | Operator::BitwiseAnd
            | Operator::BitwiseOr
            | Operator::BitwiseXor
            | Operator::BitwiseShiftRight
            | Operator::BitwiseShiftLeft
            | Operator::StringConcat
            | Operator::AtArrow
            | Operator::ArrowAt => None,
        }
    }

    /// Return true if the operator is a numerical operator.
    ///
    /// For example, 'Binary(a, +, b)' would be a numerical expression.
    /// PostgresSQL concept: <https://www.postgresql.org/docs/7.0/operators2198.htm>
    pub fn is_numerical_operators(&self) -> bool {
        matches!(
            self,
            Operator::Plus
                | Operator::Minus
                | Operator::Multiply
                | Operator::Divide
                | Operator::Modulo
        )
    }

    /// Return true if the operator is a comparison operator.
    ///
    /// For example, 'Binary(a, >, b)' would be a comparison expression.
    pub fn is_comparison_operator(&self) -> bool {
        matches!(
            self,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::IsDistinctFrom
                | Operator::IsNotDistinctFrom
                | Operator::RegexMatch
                | Operator::RegexIMatch
                | Operator::RegexNotMatch
                | Operator::RegexNotIMatch
        )
    }

    /// Return true if the operator is a logic operator.
    ///
    /// For example, 'Binary(Binary(a, >, b), AND, Binary(a, <, b + 3))' would
    /// be a logical expression.
    pub fn is_logic_operator(&self) -> bool {
        matches!(self, Operator::And | Operator::Or)
    }

    /// Return the operator where swapping lhs and rhs wouldn't change the result.
    ///
    /// For example `Binary(50, >=, a)` could also be represented as `Binary(a, <=, 50)`.
    pub fn swap(&self) -> Option<Operator> {
        match self {
            Operator::Eq => Some(Operator::Eq),
            Operator::NotEq => Some(Operator::NotEq),
            Operator::Lt => Some(Operator::Gt),
            Operator::LtEq => Some(Operator::GtEq),
            Operator::Gt => Some(Operator::Lt),
            Operator::GtEq => Some(Operator::LtEq),
            Operator::AtArrow => Some(Operator::ArrowAt),
            Operator::ArrowAt => Some(Operator::AtArrow),
            Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
            | Operator::Plus
            | Operator::Minus
            | Operator::Multiply
            | Operator::Divide
            | Operator::Modulo
            | Operator::And
            | Operator::Or
            | Operator::RegexMatch
            | Operator::RegexIMatch
            | Operator::RegexNotMatch
            | Operator::RegexNotIMatch
            | Operator::BitwiseAnd
            | Operator::BitwiseOr
            | Operator::BitwiseXor
            | Operator::BitwiseShiftRight
            | Operator::BitwiseShiftLeft
            | Operator::StringConcat => None,
        }
    }

    /// Get the operator precedence
    /// use <https://www.postgresql.org/docs/7.0/operators.htm#AEN2026> as a reference
    pub fn precedence(&self) -> u8 {
        match self {
            Operator::Or => 5,
            Operator::And => 10,
            Operator::NotEq
            | Operator::Eq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => 20,
            Operator::Plus | Operator::Minus => 30,
            Operator::Multiply | Operator::Divide | Operator::Modulo => 40,
            Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
            | Operator::RegexMatch
            | Operator::RegexNotMatch
            | Operator::RegexIMatch
            | Operator::RegexNotIMatch
            | Operator::BitwiseAnd
            | Operator::BitwiseOr
            | Operator::BitwiseShiftLeft
            | Operator::BitwiseShiftRight
            | Operator::BitwiseXor
            | Operator::StringConcat
            | Operator::AtArrow
            | Operator::ArrowAt => 0,
        }
    }
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let display = match &self {
            Operator::Eq => "=",
            Operator::NotEq => "!=",
            Operator::Lt => "<",
            Operator::LtEq => "<=",
            Operator::Gt => ">",
            Operator::GtEq => ">=",
            Operator::Plus => "+",
            Operator::Minus => "-",
            Operator::Multiply => "*",
            Operator::Divide => "/",
            Operator::Modulo => "%",
            Operator::And => "AND",
            Operator::Or => "OR",
            Operator::RegexMatch => "~",
            Operator::RegexIMatch => "~*",
            Operator::RegexNotMatch => "!~",
            Operator::RegexNotIMatch => "!~*",
            Operator::IsDistinctFrom => "IS DISTINCT FROM",
            Operator::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
            Operator::BitwiseAnd => "&",
            Operator::BitwiseOr => "|",
            Operator::BitwiseXor => "BIT_XOR",
            Operator::BitwiseShiftRight => ">>",
            Operator::BitwiseShiftLeft => "<<",
            Operator::StringConcat => "||",
            Operator::AtArrow => "@>",
            Operator::ArrowAt => "<@",
        };
        write!(f, "{display}")
    }
}

/// Support `<expr> + <expr>` fluent style
impl ops::Add for Expr {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Plus, rhs)
    }
}

/// Support `<expr> - <expr>` fluent style
impl ops::Sub for Expr {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Minus, rhs)
    }
}

/// Support `<expr> * <expr>` fluent style
impl ops::Mul for Expr {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Multiply, rhs)
    }
}

/// Support `<expr> / <expr>` fluent style
impl ops::Div for Expr {
    type Output = Self;

    fn div(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Divide, rhs)
    }
}

/// Support `<expr> % <expr>` fluent style
impl ops::Rem for Expr {
    type Output = Self;

    fn rem(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Modulo, rhs)
    }
}

/// Support `<expr> & <expr>` fluent style
impl ops::BitAnd for Expr {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        binary_expr(self, Operator::BitwiseAnd, rhs)
    }
}

/// Support `<expr> | <expr>` fluent style
impl ops::BitOr for Expr {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        binary_expr(self, Operator::BitwiseOr, rhs)
    }
}

/// Support `<expr> ^ <expr>` fluent style
impl ops::BitXor for Expr {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self {
        binary_expr(self, Operator::BitwiseXor, rhs)
    }
}

/// Support `<expr> << <expr>` fluent style
impl ops::Shl for Expr {
    type Output = Self;

    fn shl(self, rhs: Self) -> Self::Output {
        binary_expr(self, Operator::BitwiseShiftLeft, rhs)
    }
}

/// Support `<expr> >> <expr>` fluent style
impl ops::Shr for Expr {
    type Output = Self;

    fn shr(self, rhs: Self) -> Self::Output {
        binary_expr(self, Operator::BitwiseShiftRight, rhs)
    }
}

/// Support `- <expr>` fluent style
impl ops::Neg for Expr {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Expr::Negative(Box::new(self))
    }
}

/// Support `NOT <expr>` fluent style
impl Not for Expr {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => Expr::Like(Like::new(
                !negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            )),
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => Expr::SimilarTo(Like::new(
                !negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            )),
            _ => Expr::Not(Box::new(self)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::lit;

    #[test]
    fn test_operators() {
        // Add
        assert_eq!(
            format!("{}", lit(1u32) + lit(2u32)),
            "UInt32(1) + UInt32(2)"
        );
        // Sub
        assert_eq!(
            format!("{}", lit(1u32) - lit(2u32)),
            "UInt32(1) - UInt32(2)"
        );
        // Mul
        assert_eq!(
            format!("{}", lit(1u32) * lit(2u32)),
            "UInt32(1) * UInt32(2)"
        );
        // Div
        assert_eq!(
            format!("{}", lit(1u32) / lit(2u32)),
            "UInt32(1) / UInt32(2)"
        );
        // Rem
        assert_eq!(
            format!("{}", lit(1u32) % lit(2u32)),
            "UInt32(1) % UInt32(2)"
        );
        // BitAnd
        assert_eq!(
            format!("{}", lit(1u32) & lit(2u32)),
            "UInt32(1) & UInt32(2)"
        );
        // BitOr
        assert_eq!(
            format!("{}", lit(1u32) | lit(2u32)),
            "UInt32(1) | UInt32(2)"
        );
        // BitXor
        assert_eq!(
            format!("{}", lit(1u32) ^ lit(2u32)),
            "UInt32(1) BIT_XOR UInt32(2)"
        );
        // Shl
        assert_eq!(
            format!("{}", lit(1u32) << lit(2u32)),
            "UInt32(1) << UInt32(2)"
        );
        // Shr
        assert_eq!(
            format!("{}", lit(1u32) >> lit(2u32)),
            "UInt32(1) >> UInt32(2)"
        );
        // Neg
        assert_eq!(format!("{}", -lit(1u32)), "(- UInt32(1))");
        // Not
        assert_eq!(format!("{}", !lit(1u32)), "NOT UInt32(1)");
    }
}
