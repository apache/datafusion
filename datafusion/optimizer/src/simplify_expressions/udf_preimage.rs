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

use datafusion_common::{Result, internal_err, tree_node::Transformed};
use datafusion_expr::{
    BinaryExpr, Expr, Operator, and, lit, or, simplify::SimplifyContext,
};
use datafusion_expr_common::interval_arithmetic::Interval;

/// Rewrites a binary expression using its "preimage"
///
/// Specifically it rewrites expressions of the form `<expr> OP x` (e.g. `<expr> =
/// x`) where `<expr>` is known to have a pre-image (aka the entire single
/// range for which it is valid)
///
/// This rewrite is described in the [ClickHouse Paper] and is particularly
/// useful for simplifying expressions `date_part` or equivalent functions. The
/// idea is that if you have an expression like `date_part(YEAR, k) = 2024` and you
/// can find a [preimage] for `date_part(YEAR, k)`, which is the range of dates
/// covering the entire year of 2024. Thus, you can rewrite the expression to `k
/// >= '2024-01-01' AND k < '2025-01-01' which is often more optimizable.
///
/// [ClickHouse Paper]:  https://www.vldb.org/pvldb/vol17/p3731-schulze.pdf
/// [preimage]: https://en.wikipedia.org/wiki/Image_(mathematics)#Inverse_image
///
pub(super) fn rewrite_with_preimage(
    _info: &SimplifyContext,
    preimage_interval: Interval,
    op: Operator,
    expr: Box<Expr>,
) -> Result<Transformed<Expr>> {
    let (lower, upper) = preimage_interval.into_bounds();
    let (lower, upper) = (lit(lower), lit(upper));

    let rewritten_expr = match op {
        // <expr> < x   ==>  <expr> < lower
        // <expr> >= x  ==>  <expr> >= lower
        Operator::Lt | Operator::GtEq => Expr::BinaryExpr(BinaryExpr {
            left: expr,
            op,
            right: Box::new(lower),
        }),
        // <expr> > x ==> <expr> >= upper
        Operator::Gt => Expr::BinaryExpr(BinaryExpr {
            left: expr,
            op: Operator::GtEq,
            right: Box::new(upper),
        }),
        // <expr> <= x ==> <expr> < upper
        Operator::LtEq => Expr::BinaryExpr(BinaryExpr {
            left: expr,
            op: Operator::Lt,
            right: Box::new(upper),
        }),
        // <expr> = x ==> (<expr> >= lower) and (<expr> < upper)
        //
        // <expr> is not distinct from x ==> (<expr> is NULL and x is NULL) or ((<expr> >= lower) and (<expr> < upper))
        // but since x is always not NULL => (<expr> >= lower) and (<expr> < upper)
        Operator::Eq | Operator::IsNotDistinctFrom => and(
            Expr::BinaryExpr(BinaryExpr {
                left: expr.clone(),
                op: Operator::GtEq,
                right: Box::new(lower),
            }),
            Expr::BinaryExpr(BinaryExpr {
                left: expr,
                op: Operator::Lt,
                right: Box::new(upper),
            }),
        ),
        // <expr> != x ==> (<expr> < lower) or (<expr> >= upper)
        Operator::NotEq => or(
            Expr::BinaryExpr(BinaryExpr {
                left: expr.clone(),
                op: Operator::Lt,
                right: Box::new(lower),
            }),
            Expr::BinaryExpr(BinaryExpr {
                left: expr,
                op: Operator::GtEq,
                right: Box::new(upper),
            }),
        ),
        // <expr> is distinct from x ==> (<expr> < lower) or (<expr> >= upper) or (<expr> is NULL and x is not NULL) or (<expr> is not NULL and x is NULL)
        // but given that x is always not NULL => (<expr> < lower) or (<expr> >= upper) or (<expr> is NULL)
        Operator::IsDistinctFrom => Expr::BinaryExpr(BinaryExpr {
            left: expr.clone(),
            op: Operator::Lt,
            right: Box::new(lower.clone()),
        })
        .or(Expr::BinaryExpr(BinaryExpr {
            left: expr.clone(),
            op: Operator::GtEq,
            right: Box::new(upper),
        }))
        .or(expr.is_null()),
        _ => return internal_err!("Expect comparison operators"),
    };
    Ok(Transformed::yes(rewritten_expr))
}
