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
use std::fmt;
use std::ops;

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
    /// Matches a wildcard pattern
    Like,
    /// Does not match a wildcard pattern
    NotLike,
    /// IS DISTINCT FROM
    IsDistinctFrom,
    /// IS NOT DISTINCT FROM
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
    /// Bitwise xor, like `#`
    BitwiseXor,
    /// Bitwise right, like `>>`
    BitwiseShiftRight,
    /// Bitwise left, like `<<`
    BitwiseShiftLeft,
    /// String concat
    StringConcat,
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
            Operator::Like => Some(Operator::NotLike),
            Operator::NotLike => Some(Operator::Like),
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
            | Operator::StringConcat => None,
        }
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
            Operator::Like
            | Operator::NotLike
            | Operator::IsDistinctFrom
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
            Operator::Like => "LIKE",
            Operator::NotLike => "NOT LIKE",
            Operator::RegexMatch => "~",
            Operator::RegexIMatch => "~*",
            Operator::RegexNotMatch => "!~",
            Operator::RegexNotIMatch => "!~*",
            Operator::IsDistinctFrom => "IS DISTINCT FROM",
            Operator::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
            Operator::BitwiseAnd => "&",
            Operator::BitwiseOr => "|",
            Operator::BitwiseXor => "#",
            Operator::BitwiseShiftRight => ">>",
            Operator::BitwiseShiftLeft => "<<",
            Operator::StringConcat => "||",
        };
        write!(f, "{}", display)
    }
}

impl ops::Add for Expr {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Plus, rhs)
    }
}

impl ops::Sub for Expr {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Minus, rhs)
    }
}

impl ops::Mul for Expr {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Multiply, rhs)
    }
}

impl ops::Div for Expr {
    type Output = Self;

    fn div(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Divide, rhs)
    }
}

impl ops::Rem for Expr {
    type Output = Self;

    fn rem(self, rhs: Self) -> Self {
        binary_expr(self, Operator::Modulo, rhs)
    }
}

#[cfg(test)]
mod tests {
    use crate::lit;

    #[test]
    fn test_operators() {
        assert_eq!(
            format!("{:?}", lit(1u32) + lit(2u32)),
            "UInt32(1) + UInt32(2)"
        );
        assert_eq!(
            format!("{:?}", lit(1u32) - lit(2u32)),
            "UInt32(1) - UInt32(2)"
        );
        assert_eq!(
            format!("{:?}", lit(1u32) * lit(2u32)),
            "UInt32(1) * UInt32(2)"
        );
        assert_eq!(
            format!("{:?}", lit(1u32) / lit(2u32)),
            "UInt32(1) / UInt32(2)"
        );
        assert_eq!(
            format!("{:?}", lit(1u32) % lit(2u32)),
            "UInt32(1) % UInt32(2)"
        );
    }
}
