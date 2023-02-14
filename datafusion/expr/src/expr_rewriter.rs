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

//! Expression rewriter

use crate::expr::{
    AggregateFunction, Between, BinaryExpr, Case, Cast, GetIndexedField, GroupingSet,
    Like, Sort, TryCast, WindowFunction,
};
use crate::logical_plan::Projection;
use crate::{Expr, ExprSchemable, LogicalPlan};
use datafusion_common::Result;
use datafusion_common::{Column, DFSchema};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

mod order_by;
pub use order_by::rewrite_sort_cols_by_aggs;

/// Controls how the [ExprRewriter] recursion should proceed.
pub enum RewriteRecursion {
    /// Continue rewrite / visit this expression.
    Continue,
    /// Call [ExprRewriter::mutate()] immediately and return.
    Mutate,
    /// Do not rewrite / visit the children of this expression.
    Stop,
    /// Keep recursive but skip mutate on this expression
    Skip,
}

/// Trait for potentially recursively rewriting an [`Expr`] expression
/// tree. When passed to `Expr::rewrite`, `ExpressionVisitor::mutate` is
/// invoked recursively on all nodes of an expression tree.
///
/// Performs a depth first walk of an expression and its children
/// to rewrite an expression, consuming `self` producing a new
/// [`Expr`].
///
/// Implements a modified version of the [visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) to
/// separate algorithms from the structure of the `Expr` tree and
/// make it easier to write new, efficient expression
/// transformation algorithms.
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
/// mutate(Column("foo"))
/// pre_visit(Column("bar"))
/// mutate(Column("bar"))
/// mutate(BinaryExpr(GT))
/// ```
///
/// If an `Err` result is returned, recursion is stopped immediately
///
/// If [`false`] is returned on a call to pre_visit, no
/// children of that expression are visited, nor is mutate
/// called on that expression
///
/// # See Also:
/// * [`Expr::accept`] to drive a rewriter through an [`Expr`]
/// * [`rewrite_expr`]: For rewriting an [`Expr`] using functions
///
/// [`Expr::accept`]: crate::expr_visitor::ExprVisitable::accept
pub trait ExprRewriter<E: ExprRewritable = Expr>: Sized {
    /// Invoked before any children of `expr` are rewritten /
    /// visited. Default implementation returns `Ok(RewriteRecursion::Continue)`
    fn pre_visit(&mut self, _expr: &E) -> Result<RewriteRecursion> {
        Ok(RewriteRecursion::Continue)
    }

    /// Invoked after all children of `expr` have been mutated and
    /// returns a potentially modified expr.
    fn mutate(&mut self, expr: E) -> Result<E>;
}

/// A trait for marking types that are rewritable by [ExprRewriter]
pub trait ExprRewritable: Sized {
    /// Rewrite the expression tree using the given [ExprRewriter]
    fn rewrite<R: ExprRewriter<Self>>(self, rewriter: &mut R) -> Result<Self>;
}

impl ExprRewritable for Expr {
    /// See comments on [`ExprRewritable`] for details
    fn rewrite<R>(self, rewriter: &mut R) -> Result<Self>
    where
        R: ExprRewriter<Self>,
    {
        let need_mutate = match rewriter.pre_visit(&self)? {
            RewriteRecursion::Mutate => return rewriter.mutate(self),
            RewriteRecursion::Stop => return Ok(self),
            RewriteRecursion::Continue => true,
            RewriteRecursion::Skip => false,
        };

        // recurse into all sub expressions(and cover all expression types)
        let expr = match self {
            Expr::Alias(expr, name) => Expr::Alias(rewrite_boxed(expr, rewriter)?, name),
            Expr::Column(_) => self.clone(),
            Expr::Exists { .. } => self.clone(),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => Expr::InSubquery {
                expr: rewrite_boxed(expr, rewriter)?,
                subquery,
                negated,
            },
            Expr::ScalarSubquery(_) => self.clone(),
            Expr::ScalarVariable(ty, names) => Expr::ScalarVariable(ty, names),
            Expr::Literal(value) => Expr::Literal(value),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                Expr::BinaryExpr(BinaryExpr::new(
                    rewrite_boxed(left, rewriter)?,
                    op,
                    rewrite_boxed(right, rewriter)?,
                ))
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => Expr::Like(Like::new(
                negated,
                rewrite_boxed(expr, rewriter)?,
                rewrite_boxed(pattern, rewriter)?,
                escape_char,
            )),
            Expr::ILike(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => Expr::ILike(Like::new(
                negated,
                rewrite_boxed(expr, rewriter)?,
                rewrite_boxed(pattern, rewriter)?,
                escape_char,
            )),
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => Expr::SimilarTo(Like::new(
                negated,
                rewrite_boxed(expr, rewriter)?,
                rewrite_boxed(pattern, rewriter)?,
                escape_char,
            )),
            Expr::Not(expr) => Expr::Not(rewrite_boxed(expr, rewriter)?),
            Expr::IsNotNull(expr) => Expr::IsNotNull(rewrite_boxed(expr, rewriter)?),
            Expr::IsNull(expr) => Expr::IsNull(rewrite_boxed(expr, rewriter)?),
            Expr::IsTrue(expr) => Expr::IsTrue(rewrite_boxed(expr, rewriter)?),
            Expr::IsFalse(expr) => Expr::IsFalse(rewrite_boxed(expr, rewriter)?),
            Expr::IsUnknown(expr) => Expr::IsUnknown(rewrite_boxed(expr, rewriter)?),
            Expr::IsNotTrue(expr) => Expr::IsNotTrue(rewrite_boxed(expr, rewriter)?),
            Expr::IsNotFalse(expr) => Expr::IsNotFalse(rewrite_boxed(expr, rewriter)?),
            Expr::IsNotUnknown(expr) => {
                Expr::IsNotUnknown(rewrite_boxed(expr, rewriter)?)
            }
            Expr::Negative(expr) => Expr::Negative(rewrite_boxed(expr, rewriter)?),
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => Expr::Between(Between::new(
                rewrite_boxed(expr, rewriter)?,
                negated,
                rewrite_boxed(low, rewriter)?,
                rewrite_boxed(high, rewriter)?,
            )),
            Expr::Case(case) => {
                let expr = rewrite_option_box(case.expr, rewriter)?;
                let when_then_expr = case
                    .when_then_expr
                    .into_iter()
                    .map(|(when, then)| {
                        Ok((
                            rewrite_boxed(when, rewriter)?,
                            rewrite_boxed(then, rewriter)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let else_expr = rewrite_option_box(case.else_expr, rewriter)?;

                Expr::Case(Case::new(expr, when_then_expr, else_expr))
            }
            Expr::Cast(Cast { expr, data_type }) => {
                Expr::Cast(Cast::new(rewrite_boxed(expr, rewriter)?, data_type))
            }
            Expr::TryCast(TryCast { expr, data_type }) => {
                Expr::TryCast(TryCast::new(rewrite_boxed(expr, rewriter)?, data_type))
            }
            Expr::Sort(Sort {
                expr,
                asc,
                nulls_first,
            }) => Expr::Sort(Sort::new(rewrite_boxed(expr, rewriter)?, asc, nulls_first)),
            Expr::ScalarFunction { args, fun } => Expr::ScalarFunction {
                args: rewrite_vec(args, rewriter)?,
                fun,
            },
            Expr::ScalarUDF { args, fun } => Expr::ScalarUDF {
                args: rewrite_vec(args, rewriter)?,
                fun,
            },
            Expr::WindowFunction(WindowFunction {
                args,
                fun,
                partition_by,
                order_by,
                window_frame,
            }) => Expr::WindowFunction(WindowFunction::new(
                fun,
                rewrite_vec(args, rewriter)?,
                rewrite_vec(partition_by, rewriter)?,
                rewrite_vec(order_by, rewriter)?,
                window_frame,
            )),
            Expr::AggregateFunction(AggregateFunction {
                args,
                fun,
                distinct,
                filter,
            }) => Expr::AggregateFunction(AggregateFunction::new(
                fun,
                rewrite_vec(args, rewriter)?,
                distinct,
                filter,
            )),
            Expr::GroupingSet(grouping_set) => match grouping_set {
                GroupingSet::Rollup(exprs) => {
                    Expr::GroupingSet(GroupingSet::Rollup(rewrite_vec(exprs, rewriter)?))
                }
                GroupingSet::Cube(exprs) => {
                    Expr::GroupingSet(GroupingSet::Cube(rewrite_vec(exprs, rewriter)?))
                }
                GroupingSet::GroupingSets(lists_of_exprs) => {
                    Expr::GroupingSet(GroupingSet::GroupingSets(
                        lists_of_exprs
                            .iter()
                            .map(|exprs| rewrite_vec(exprs.clone(), rewriter))
                            .collect::<Result<Vec<_>>>()?,
                    ))
                }
            },
            Expr::AggregateUDF { args, fun, filter } => Expr::AggregateUDF {
                args: rewrite_vec(args, rewriter)?,
                fun,
                filter,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => Expr::InList {
                expr: rewrite_boxed(expr, rewriter)?,
                list: rewrite_vec(list, rewriter)?,
                negated,
            },
            Expr::Wildcard => Expr::Wildcard,
            Expr::QualifiedWildcard { qualifier } => {
                Expr::QualifiedWildcard { qualifier }
            }
            Expr::GetIndexedField(GetIndexedField { key, expr }) => {
                Expr::GetIndexedField(GetIndexedField::new(
                    rewrite_boxed(expr, rewriter)?,
                    key,
                ))
            }
            Expr::Placeholder { id, data_type } => Expr::Placeholder { id, data_type },
        };

        // now rewrite this expression itself
        if need_mutate {
            rewriter.mutate(expr)
        } else {
            Ok(expr)
        }
    }
}

#[allow(clippy::boxed_local)]
fn rewrite_boxed<R>(boxed_expr: Box<Expr>, rewriter: &mut R) -> Result<Box<Expr>>
where
    R: ExprRewriter,
{
    // TODO: It might be possible to avoid an allocation (the
    // Box::new) below by reusing the box.
    let expr: Expr = *boxed_expr;
    let rewritten_expr = expr.rewrite(rewriter)?;
    Ok(Box::new(rewritten_expr))
}

fn rewrite_option_box<R>(
    option_box: Option<Box<Expr>>,
    rewriter: &mut R,
) -> Result<Option<Box<Expr>>>
where
    R: ExprRewriter,
{
    option_box
        .map(|expr| rewrite_boxed(expr, rewriter))
        .transpose()
}

/// Rewrite a `Vec` of `Expr`s with the rewriter
fn rewrite_vec<R>(v: Vec<Expr>, rewriter: &mut R) -> Result<Vec<Expr>>
where
    R: ExprRewriter,
{
    v.into_iter().map(|expr| expr.rewrite(rewriter)).collect()
}

/// Recursively call [`Column::normalize_with_schemas`] on all [`Column`] expressions
/// in the `expr` expression tree.
pub fn normalize_col(expr: Expr, plan: &LogicalPlan) -> Result<Expr> {
    normalize_col_with_schemas(expr, &plan.all_schemas(), &plan.using_columns()?)
}

/// Recursively call [`Column::normalize_with_schemas`] on all [`Column`] expressions
/// in the `expr` expression tree.
pub fn normalize_col_with_schemas(
    expr: Expr,
    schemas: &[&Arc<DFSchema>],
    using_columns: &[HashSet<Column>],
) -> Result<Expr> {
    rewrite_expr(expr, |expr| {
        if let Expr::Column(c) = expr {
            Ok(Expr::Column(
                c.normalize_with_schemas(schemas, using_columns)?,
            ))
        } else {
            Ok(expr)
        }
    })
}

/// Recursively normalize all [`Column`] expressions in a list of expression trees
pub fn normalize_cols(
    exprs: impl IntoIterator<Item = impl Into<Expr>>,
    plan: &LogicalPlan,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .map(|e| normalize_col(e.into(), plan))
        .collect()
}

/// Recursively replace all [`Column`] expressions in a given expression tree with
/// `Column` expressions provided by the hash map argument.
pub fn replace_col(e: Expr, replace_map: &HashMap<&Column, &Column>) -> Result<Expr> {
    rewrite_expr(e, |expr| {
        if let Expr::Column(c) = &expr {
            match replace_map.get(c) {
                Some(new_c) => Ok(Expr::Column((*new_c).to_owned())),
                None => Ok(expr),
            }
        } else {
            Ok(expr)
        }
    })
}

/// Recursively 'unnormalize' (remove all qualifiers) from an
/// expression tree.
///
/// For example, if there were expressions like `foo.bar` this would
/// rewrite it to just `bar`.
pub fn unnormalize_col(expr: Expr) -> Expr {
    rewrite_expr(expr, |expr| {
        if let Expr::Column(col) = expr {
            Ok(Expr::Column(Column {
                relation: None,
                name: col.name,
            }))
        } else {
            Ok(expr)
        }
    })
    .expect("Unnormalize is infallable")
}

/// Recursively un-normalize all [`Column`] expressions in a list of expression trees
#[inline]
pub fn unnormalize_cols(exprs: impl IntoIterator<Item = Expr>) -> Vec<Expr> {
    exprs.into_iter().map(unnormalize_col).collect()
}

/// Implementation of [`ExprRewriter`] that calls a function, for use
/// with [`rewrite_expr`]
struct RewriterAdapter<F> {
    f: F,
}

impl<F> ExprRewriter for RewriterAdapter<F>
where
    F: FnMut(Expr) -> Result<Expr>,
{
    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        (self.f)(expr)
    }
}

/// Recursively rewrite an [`Expr`] via a function.
///
/// Rewrites the expression bottom up by recursively calling `f(expr)`
/// on `expr`'s children and then on `expr`. See [`ExprRewriter`]
/// for more details and more options to control the walk.
///
/// # Example:
/// ```
/// # use datafusion_expr::*;
/// # use datafusion_expr::expr_rewriter::rewrite_expr;
/// let expr = col("a") + lit(1);
///
/// // rewrite all literals to 42
/// let rewritten = rewrite_expr(expr, |e| {
///   if let Expr::Literal(_) = e {
///     Ok(lit(42))
///   } else {
///     Ok(e)
///   }
/// }).unwrap();
///
/// assert_eq!(rewritten, col("a") + lit(42));
/// ```
pub fn rewrite_expr<F>(expr: Expr, f: F) -> Result<Expr>
where
    F: FnMut(Expr) -> Result<Expr>,
{
    expr.rewrite(&mut RewriterAdapter { f })
}

/// Returns plan with expressions coerced to types compatible with
/// schema types
pub fn coerce_plan_expr_for_schema(
    plan: &LogicalPlan,
    schema: &DFSchema,
) -> Result<LogicalPlan> {
    match plan {
        // special case Projection to avoid adding multiple projections
        LogicalPlan::Projection(Projection { expr, input, .. }) => {
            let new_exprs =
                coerce_exprs_for_schema(expr.clone(), input.schema(), schema)?;
            let projection = Projection::try_new(new_exprs, input.clone())?;
            Ok(LogicalPlan::Projection(projection))
        }
        _ => {
            let exprs: Vec<Expr> = plan
                .schema()
                .fields()
                .iter()
                .map(|field| Expr::Column(field.qualified_column()))
                .collect();

            let new_exprs = coerce_exprs_for_schema(exprs, plan.schema(), schema)?;
            let add_project = new_exprs.iter().any(|expr| expr.try_into_col().is_err());
            if add_project {
                let projection = Projection::try_new(new_exprs, Arc::new(plan.clone()))?;
                Ok(LogicalPlan::Projection(projection))
            } else {
                Ok(plan.clone())
            }
        }
    }
}

fn coerce_exprs_for_schema(
    exprs: Vec<Expr>,
    src_schema: &DFSchema,
    dst_schema: &DFSchema,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .enumerate()
        .map(|(idx, expr)| {
            let new_type = dst_schema.field(idx).data_type();
            if new_type != &expr.get_type(src_schema)? {
                match expr {
                    Expr::Alias(e, alias) => {
                        Ok(e.cast_to(new_type, src_schema)?.alias(alias))
                    }
                    _ => expr.cast_to(new_type, src_schema),
                }
            } else {
                Ok(expr.clone())
            }
        })
        .collect::<Result<_>>()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{col, lit};
    use arrow::datatypes::DataType;
    use datafusion_common::{DFField, DFSchema, ScalarValue};

    #[ctor::ctor]
    fn init() {
        let _ = env_logger::try_init();
    }

    #[derive(Default)]
    struct RecordingRewriter {
        v: Vec<String>,
    }

    impl ExprRewriter for RecordingRewriter {
        fn mutate(&mut self, expr: Expr) -> Result<Expr> {
            self.v.push(format!("Mutated {expr:?}"));
            Ok(expr)
        }

        fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
            self.v.push(format!("Previsited {expr:?}"));
            Ok(RewriteRecursion::Continue)
        }
    }

    #[test]
    fn rewriter_rewrite() {
        let mut rewriter = FooBarRewriter {};

        // rewrites "foo" --> "bar"
        let rewritten = col("state").eq(lit("foo")).rewrite(&mut rewriter).unwrap();
        assert_eq!(rewritten, col("state").eq(lit("bar")));

        // doesn't rewrite
        let rewritten = col("state").eq(lit("baz")).rewrite(&mut rewriter).unwrap();
        assert_eq!(rewritten, col("state").eq(lit("baz")));
    }

    /// rewrites all "foo" string literals to "bar"
    struct FooBarRewriter {}

    impl ExprRewriter for FooBarRewriter {
        fn mutate(&mut self, expr: Expr) -> Result<Expr> {
            match expr {
                Expr::Literal(ScalarValue::Utf8(Some(utf8_val))) => {
                    let utf8_val = if utf8_val == "foo" {
                        "bar".to_string()
                    } else {
                        utf8_val
                    };
                    Ok(lit(utf8_val))
                }
                // otherwise, return the expression unchanged
                expr => Ok(expr),
            }
        }
    }

    #[test]
    fn normalize_cols() {
        let expr = col("a") + col("b") + col("c");

        // Schemas with some matching and some non matching cols
        let schema_a = make_schema_with_empty_metadata(vec![
            make_field("tableA", "a"),
            make_field("tableA", "aa"),
        ]);
        let schema_c = make_schema_with_empty_metadata(vec![
            make_field("tableC", "cc"),
            make_field("tableC", "c"),
        ]);
        let schema_b = make_schema_with_empty_metadata(vec![make_field("tableB", "b")]);
        // non matching
        let schema_f = make_schema_with_empty_metadata(vec![
            make_field("tableC", "f"),
            make_field("tableC", "ff"),
        ]);
        let schemas = vec![schema_c, schema_f, schema_b, schema_a]
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();
        let schemas = schemas.iter().collect::<Vec<_>>();

        let normalized_expr = normalize_col_with_schemas(expr, &schemas, &[]).unwrap();
        assert_eq!(
            normalized_expr,
            col("tableA.a") + col("tableB.b") + col("tableC.c")
        );
    }

    #[test]
    fn normalize_cols_priority() {
        let expr = col("a") + col("b");
        // Schemas with multiple matches for column a, first takes priority
        let schema_a = make_schema_with_empty_metadata(vec![make_field("tableA", "a")]);
        let schema_b = make_schema_with_empty_metadata(vec![make_field("tableB", "b")]);
        let schema_a2 = make_schema_with_empty_metadata(vec![make_field("tableA2", "a")]);
        let schemas = vec![schema_a2, schema_b, schema_a]
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>();
        let schemas = schemas.iter().collect::<Vec<_>>();

        let normalized_expr = normalize_col_with_schemas(expr, &schemas, &[]).unwrap();
        assert_eq!(normalized_expr, col("tableA2.a") + col("tableB.b"));
    }

    #[test]
    fn normalize_cols_non_exist() {
        // test normalizing columns when the name doesn't exist
        let expr = col("a") + col("b");
        let schema_a = make_schema_with_empty_metadata(vec![make_field("tableA", "a")]);
        let schemas = vec![schema_a].into_iter().map(Arc::new).collect::<Vec<_>>();
        let schemas = schemas.iter().collect::<Vec<_>>();

        let error = normalize_col_with_schemas(expr, &schemas, &[])
            .unwrap_err()
            .to_string();
        assert_eq!(
            error,
            "Schema error: No field named 'b'. Valid fields are 'tableA'.'a'."
        );
    }

    #[test]
    fn unnormalize_cols() {
        let expr = col("tableA.a") + col("tableB.b");
        let unnormalized_expr = unnormalize_col(expr);
        assert_eq!(unnormalized_expr, col("a") + col("b"));
    }

    fn make_schema_with_empty_metadata(fields: Vec<DFField>) -> DFSchema {
        DFSchema::new_with_metadata(fields, HashMap::new()).unwrap()
    }

    fn make_field(relation: &str, column: &str) -> DFField {
        DFField::new(Some(relation), column, DataType::Int8, false)
    }

    #[test]
    fn rewriter_visit() {
        let mut rewriter = RecordingRewriter::default();
        col("state").eq(lit("CO")).rewrite(&mut rewriter).unwrap();

        assert_eq!(
            rewriter.v,
            vec![
                "Previsited state = Utf8(\"CO\")",
                "Previsited state",
                "Mutated state",
                "Previsited Utf8(\"CO\")",
                "Mutated Utf8(\"CO\")",
                "Mutated state = Utf8(\"CO\")"
            ]
        )
    }
}
