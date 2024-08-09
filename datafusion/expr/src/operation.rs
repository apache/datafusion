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

//! This module contains implementations of operations (unary, binary etc.) for DataFusion expressions.

use crate::expr_fn::binary_expr;
use crate::{Expr, Like};
use datafusion_expr_common::operator::Operator;
use std::ops::{self, Not};

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
