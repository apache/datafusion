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

//! Simplification to refactor multiple aggregate functions to use the same aggregate function

use datafusion_common::HashMap;
use datafusion_expr::expr::AggregateFunctionParams;
use datafusion_expr::{BinaryExpr, Expr};
use datafusion_expr_common::operator::Operator;

/// Threshold of the number of aggregates that share similar arguments before
/// triggering rewrite.
///
/// There is a threshold because the canonical SUM rewrite described in
/// [`AggregateUDFImpl::simplify_expr_op_literal`] actually results in more
/// aggregates (2) for each original aggregate. It is important that CSE then
/// eliminate them.
///
/// [`AggregateUDFImpl::simplify_expr_op_literal`]: datafusion_expr::AggregateUDFImpl::simplify_expr_op_literal
const DUPLICATE_THRESHOLD: usize = 2;

/// Rewrites multiple aggregate expressions that have a common linear component
/// into multiple aggregate expressions that share that common component.
///
/// For example, rewrites patterns such as
/// * `SUM(x + 1), SUM(x + 2), ...`
///
/// Into
/// * `SUM(x) + 1 * COUNT(x), SUM(x) + 2 * COUNT(x), ...`
///
/// See the background [`AggregateUDFImpl::simplify_expr_op_literal`] for details.
///
/// Returns `true` if any of the arguments are rewritten (modified), `false`
/// otherwise.
///
/// ## Design goals:
/// 1. Keep the aggregate specific logic out of the optimizer (can't depend directly on SUM)
/// 2. Optimize for the case that this rewrite will not apply (it almost never does)
///
/// [`AggregateUDFImpl::simplify_expr_op_literal`]: datafusion_expr::AggregateUDFImpl::simplify_expr_op_literal
pub(super) fn rewrite_multiple_linear_aggregates(
    agg_expr: &mut [Expr],
) -> datafusion_common::Result<bool> {
    // map <expr>: count of expressions that have a common argument
    let mut common_args = HashMap::new();

    // First pass -- figure out any aggregates that can be split and have common
    // expressions.
    for agg in agg_expr.iter() {
        let Expr::AggregateFunction(agg_function) = agg else {
            continue;
        };

        let Some(arg) = candidate_linear_param(&agg_function.params) else {
            continue;
        };

        let Some(expr_literal) = ExprLiteral::try_new(arg) else {
            continue;
        };

        let counter = common_args.entry(expr_literal.expr()).or_insert(0);
        *counter += 1;
    }

    // (agg_index, new_expr)
    let mut new_aggs = vec![];

    // Second pass, actually rewrite any aggregates that have a common
    // expression and enough duplicates.
    for (idx, agg) in agg_expr.iter().enumerate() {
        let Expr::AggregateFunction(agg_function) = agg else {
            continue;
        };

        let Some(arg) = candidate_linear_param(&agg_function.params) else {
            continue;
        };

        let Some(expr_literal) = ExprLiteral::try_new(arg) else {
            continue;
        };

        // Not enough common expressions to make it worth rewriting
        if common_args.get(expr_literal.expr()).unwrap_or(&0) < &DUPLICATE_THRESHOLD {
            continue;
        }

        if let Some(new_agg_function) = agg_function.func.simplify_expr_op_literal(
            agg_function,
            expr_literal.expr(),
            expr_literal.op(),
            expr_literal.lit(),
            expr_literal.arg_is_left(),
        )? {
            new_aggs.push((idx, new_agg_function));
        }
    }

    if new_aggs.is_empty() {
        return Ok(false);
    }

    // Otherwise replace the aggregate expressions
    drop(common_args); // release borrow
    for (idx, new_agg) in new_aggs {
        let orig_name = agg_expr[idx].name_for_alias()?;
        agg_expr[idx] = new_agg.alias_if_changed(orig_name)?
    }

    Ok(true)
}

/// Returns Some(&Expr) with the single argument if this is a suitable candidate
/// for the  linear rewrite
fn candidate_linear_param(params: &AggregateFunctionParams) -> Option<&Expr> {
    // Explicitly destructure to ensure we check all relevant fields
    let AggregateFunctionParams {
        args,
        distinct,
        filter,
        order_by,
        null_treatment,
    } = params;

    // Disqualify anything "non standard"
    if *distinct
        || filter.is_some()
        || !order_by.is_empty()
        || null_treatment.is_some()
        || args.len() != 1
    {
        return None;
    }
    let arg = args.first()?;
    if arg.is_volatile() {
        return None;
    };
    Some(arg)
}

/// A view into a [`Expr::BinaryExpr`]  that is arbitrary expression and a
/// literal
///
/// This is an enum to distinguish the direction of the operator arguments
#[derive(Debug, Clone)]
pub enum ExprLiteral<'a> {
    /// if the expression is `<arg> <op> <lit>`
    ArgOpLit {
        arg: &'a Expr,
        op: Operator,
        lit: &'a Expr,
    },
    /// if the expression is `<lit> <op> <arg>`
    LitOpArg {
        lit: &'a Expr,
        op: Operator,
        arg: &'a Expr,
    },
}

impl<'a> ExprLiteral<'a> {
    /// Try and split the Expr into its parts
    fn try_new(expr: &'a Expr) -> Option<Self> {
        match expr {
            // <lit> <op> <expr>
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if matches!(left.as_ref(), Expr::Literal(..)) =>
            {
                Some(Self::LitOpArg {
                    arg: right,
                    lit: left,
                    op: *op,
                })
            }

            // <expr> + <lit>
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if matches!(right.as_ref(), Expr::Literal(..)) =>
            {
                Some(Self::ArgOpLit {
                    arg: left,
                    lit: right,
                    op: *op,
                })
            }
            _ => None,
        }
    }

    fn expr(&self) -> &'a Expr {
        match self {
            Self::ArgOpLit { arg, .. } => arg,
            Self::LitOpArg { arg, .. } => arg,
        }
    }

    fn lit(&self) -> &'a Expr {
        match self {
            Self::ArgOpLit { lit, .. } => lit,
            Self::LitOpArg { lit, .. } => lit,
        }
    }

    fn op(&self) -> Operator {
        match self {
            Self::ArgOpLit { op, .. } => *op,
            Self::LitOpArg { op, .. } => *op,
        }
    }

    fn arg_is_left(&self) -> bool {
        matches!(self, Self::ArgOpLit { .. })
    }
}
