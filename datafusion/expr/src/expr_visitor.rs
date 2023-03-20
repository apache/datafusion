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

//! Expression visitor

use crate::expr::{AggregateFunction, Cast, Sort, WindowFunction};
use crate::{
    expr::{BinaryExpr, GroupingSet, TryCast},
    Between, Expr, GetIndexedField, Like,
};
use datafusion_common::Result;

/// Controls how the visitor recursion should proceed.
pub enum Recursion<V: ExpressionVisitor> {
    /// Attempt to visit all the children, recursively, of this expression.
    Continue(V),
    /// Do not visit the children of this expression, though the walk
    /// of parents of this expression will not be affected
    Stop(V),
}

/// Implements the [visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for recursively walking [`Expr`]s.
///
/// [`ExpressionVisitor`] allows keeping the algorithms
/// separate from the code to traverse the structure of the `Expr`
/// tree and makes it easier to add new types of expressions and
/// algorithms by.
///
/// When passed to[`Expr::accept`], [`ExpressionVisitor::pre_visit`]
/// and [`ExpressionVisitor::post_visit`] are invoked recursively
/// on all nodes of an expression tree.
///
///
/// For an expression tree such as
/// ```text
/// BinaryExpr (GT)
///    left: Column("foo")
///    right: Column("bar")
/// ```
///
/// The nodes are visited using the following order
/// ```text
/// pre_visit(BinaryExpr(GT))
/// pre_visit(Column("foo"))
/// post_visit(Column("foo"))
/// pre_visit(Column("bar"))
/// post_visit(Column("bar"))
/// post_visit(BinaryExpr(GT))
/// ```
///
/// If an [`Err`] result is returned, recursion is stopped
/// immediately.
///
/// If [`Recursion::Stop`] is returned on a call to pre_visit, no
/// children of that expression are visited, nor is post_visit
/// called on that expression
///
/// # See Also:
/// * [`Expr::accept`] to drive a visitor through an [`Expr`]
/// * [inspect_expr_pre]: For visiting [`Expr`]s using functions
pub trait ExpressionVisitor<E: ExprVisitable = Expr>: Sized {
    /// Invoked before any children of `expr` are visited.
    fn pre_visit(self, expr: &E) -> Result<Recursion<Self>>
    where
        Self: ExpressionVisitor;

    /// Invoked after all children of `expr` are visited. Default
    /// implementation does nothing.
    fn post_visit(self, _expr: &E) -> Result<Self> {
        Ok(self)
    }
}

/// trait for types that can be visited by [`ExpressionVisitor`]
pub trait ExprVisitable: Sized {
    /// accept a visitor, calling `visit` on all children of this
    fn accept<V: ExpressionVisitor<Self>>(&self, visitor: V) -> Result<V>;
}

impl ExprVisitable for Expr {
    /// Performs a depth first walk of an expression and
    /// its children, see [`ExpressionVisitor`] for more details
    fn accept<V: ExpressionVisitor>(&self, visitor: V) -> Result<V> {
        let visitor = match visitor.pre_visit(self)? {
            Recursion::Continue(visitor) => visitor,
            // If the recursion should stop, do not visit children
            Recursion::Stop(visitor) => return Ok(visitor),
        };

        // recurse (and cover all expression types)
        let visitor = match self {
            Expr::Alias(expr, _)
            | Expr::Not(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::IsNull(expr)
            | Expr::Negative(expr)
            | Expr::Cast(Cast { expr, .. })
            | Expr::TryCast(TryCast { expr, .. })
            | Expr::Sort(Sort { expr, .. })
            | Expr::InSubquery { expr, .. } => expr.accept(visitor),
            Expr::GetIndexedField(GetIndexedField { expr, .. }) => expr.accept(visitor),
            Expr::GroupingSet(GroupingSet::Rollup(exprs)) => exprs
                .iter()
                .fold(Ok(visitor), |v, e| v.and_then(|v| e.accept(v))),
            Expr::GroupingSet(GroupingSet::Cube(exprs)) => exprs
                .iter()
                .fold(Ok(visitor), |v, e| v.and_then(|v| e.accept(v))),
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                lists_of_exprs.iter().fold(Ok(visitor), |v, exprs| {
                    v.and_then(|v| {
                        exprs.iter().fold(Ok(v), |v, e| v.and_then(|v| e.accept(v)))
                    })
                })
            }
            Expr::Column(_)
            // Treat OuterReferenceColumn as a leaf expression
            | Expr::OuterReferenceColumn(_, _)
            | Expr::ScalarVariable(_, _)
            | Expr::Literal(_)
            | Expr::Exists { .. }
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. }
            | Expr::Placeholder { .. } => Ok(visitor),
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                let visitor = left.accept(visitor)?;
                right.accept(visitor)
            }
            Expr::Like(Like { expr, pattern, .. }) => {
                let visitor = expr.accept(visitor)?;
                pattern.accept(visitor)
            }
            Expr::ILike(Like { expr, pattern, .. }) => {
                let visitor = expr.accept(visitor)?;
                pattern.accept(visitor)
            }
            Expr::SimilarTo(Like { expr, pattern, .. }) => {
                let visitor = expr.accept(visitor)?;
                pattern.accept(visitor)
            }
            Expr::Between(Between {
                expr, low, high, ..
            }) => {
                let visitor = expr.accept(visitor)?;
                let visitor = low.accept(visitor)?;
                high.accept(visitor)
            }
            Expr::Case(case) => {
                let visitor = if let Some(expr) = case.expr.as_ref() {
                    expr.accept(visitor)
                } else {
                    Ok(visitor)
                }?;
                let visitor = case.when_then_expr.iter().try_fold(
                    visitor,
                    |visitor, (when, then)| {
                        let visitor = when.accept(visitor)?;
                        then.accept(visitor)
                    },
                )?;
                if let Some(else_expr) = case.else_expr.as_ref() {
                    else_expr.accept(visitor)
                } else {
                    Ok(visitor)
                }
            }
            Expr::ScalarFunction { args, .. } | Expr::ScalarUDF { args, .. } => args
                .iter()
                .try_fold(visitor, |visitor, arg| arg.accept(visitor)),
            Expr::AggregateFunction(AggregateFunction { args, filter, .. })
            | Expr::AggregateUDF { args, filter, .. } => {
                if let Some(f) = filter {
                    let mut aggr_exprs = args.clone();
                    aggr_exprs.push(f.as_ref().clone());
                    aggr_exprs
                        .iter()
                        .try_fold(visitor, |visitor, arg| arg.accept(visitor))
                } else {
                    args.iter()
                        .try_fold(visitor, |visitor, arg| arg.accept(visitor))
                }
            }
            Expr::WindowFunction(WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            }) => {
                let visitor = args
                    .iter()
                    .try_fold(visitor, |visitor, arg| arg.accept(visitor))?;
                let visitor = partition_by
                    .iter()
                    .try_fold(visitor, |visitor, arg| arg.accept(visitor))?;
                let visitor = order_by
                    .iter()
                    .try_fold(visitor, |visitor, arg| arg.accept(visitor))?;
                Ok(visitor)
            }
            Expr::InList { expr, list, .. } => {
                let visitor = expr.accept(visitor)?;
                list.iter()
                    .try_fold(visitor, |visitor, arg| arg.accept(visitor))
            }
        }?;

        visitor.post_visit(self)
    }
}

struct VisitorAdapter<F, E> {
    f: F,
    // Store returned error as it my not be a DataFusionError
    err: Result<(), E>,
}

impl<F, E> ExpressionVisitor for VisitorAdapter<F, E>
where
    F: FnMut(&Expr) -> Result<(), E>,
{
    fn pre_visit(mut self, expr: &Expr) -> Result<Recursion<Self>> {
        if let Err(e) = (self.f)(expr) {
            // save the error for later (it may not be a DataFusionError
            self.err = Err(e);
            Ok(Recursion::Stop(self))
        } else {
            // keep going
            Ok(Recursion::Continue(self))
        }
    }
}

/// Recursively inspect an [`Expr`] and all its childen.
///
/// Performs a pre-visit traversal by recursively calling `f(expr)` on
/// `expr`, and then on all its children. See [`ExpressionVisitor`]
/// for more details and more options to control the walk.
pub fn inspect_expr_pre<F, E>(expr: &Expr, f: F) -> Result<(), E>
where
    F: FnMut(&Expr) -> Result<(), E>,
{
    // the visit is fallable, so unwrap here
    let adapter = expr.accept(VisitorAdapter { f, err: Ok(()) }).unwrap();
    adapter.err
}
