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

//! Tree node implementation for logical expr

use crate::expr::{
    AggregateFunction, AggregateFunctionDefinition, Alias, Between, BinaryExpr, Case,
    Cast, GetIndexedField, GroupingSet, InList, InSubquery, Like, Placeholder,
    ScalarFunction, ScalarFunctionDefinition, Sort, TryCast, Unnest, WindowFunction,
};
use crate::{Expr, GetFieldAccess};

use datafusion_common::tree_node::{TreeNode, VisitRecursion};
use datafusion_common::{internal_err, Result};

impl TreeNode for Expr {
    fn apply_children<F: FnMut(&Self) -> Result<VisitRecursion>>(
        &self,
        op: &mut F,
    ) -> Result<VisitRecursion> {
        let children = match self {
            Expr::Alias(Alias{expr,..})
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
            | Expr::InSubquery(InSubquery{ expr, .. }) => vec![expr.as_ref()],
            Expr::GetIndexedField(GetIndexedField { expr, field }) => {
                let expr = expr.as_ref();
                match field {
                    GetFieldAccess::ListIndex {key} => vec![key.as_ref(), expr],
                    GetFieldAccess::ListRange {start, stop, stride} => {
                        vec![start.as_ref(), stop.as_ref(),stride.as_ref(), expr]
                    }
                    GetFieldAccess::NamedStructField { .. } => vec![expr],
                }
            }
            Expr::Unnest(Unnest { exprs }) |
            Expr::GroupingSet(GroupingSet::Rollup(exprs))
            | Expr::GroupingSet(GroupingSet::Cube(exprs)) => exprs.iter().collect(),
            Expr::ScalarFunction (ScalarFunction{ args, .. } )  => {
                args.iter().collect()
            }
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                lists_of_exprs.iter().flatten().collect()
            }
            Expr::Column(_)
            // Treat OuterReferenceColumn as a leaf expression
            | Expr::OuterReferenceColumn(_, _)
            | Expr::ScalarVariable(_, _)
            | Expr::Literal(_)
            | Expr::Exists {..}
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard {..}
            | Expr::Placeholder (_) => vec![],
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                vec![left.as_ref(), right.as_ref()]
            }
            Expr::Like(Like { expr, pattern, .. })
            | Expr::SimilarTo(Like { expr, pattern, .. }) => {
                vec![expr.as_ref(), pattern.as_ref()]
            }
            Expr::Between(Between {
                expr, low, high, ..
            }) => vec![expr.as_ref(), low.as_ref(), high.as_ref()],
            Expr::Case(case) => {
                let mut expr_vec = vec![];
                if let Some(expr) = case.expr.as_ref() {
                    expr_vec.push(expr.as_ref());
                };
                for (when, then) in case.when_then_expr.iter() {
                    expr_vec.push(when.as_ref());
                    expr_vec.push(then.as_ref());
                }
                if let Some(else_expr) = case.else_expr.as_ref() {
                    expr_vec.push(else_expr.as_ref());
                }
                expr_vec
            }
            Expr::AggregateFunction(AggregateFunction { args, filter, order_by, .. })
             => {
                let mut expr_vec = args.iter().collect::<Vec<_>>();
                if let Some(f) = filter {
                    expr_vec.push(f.as_ref());
                }
                if let Some(order_by) = order_by {
                    expr_vec.extend(order_by);
                }
                expr_vec
            }
            Expr::WindowFunction(WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            }) => {
                let mut expr_vec = args.iter().collect::<Vec<_>>();
                expr_vec.extend(partition_by);
                expr_vec.extend(order_by);
                expr_vec
            }
            Expr::InList(InList { expr, list, .. }) => {
                let mut expr_vec = vec![expr.as_ref()];
                expr_vec.extend(list);
                expr_vec
            }
        };

        for child in children {
            match op(child)? {
                VisitRecursion::Continue => {}
                VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
                VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
            }
        }

        Ok(VisitRecursion::Continue)
    }

    fn map_children<F: FnMut(Self) -> Result<Self>>(
        self,
        mut transform: F,
    ) -> Result<Self> {
        Ok(match self {
            Expr::Column(_)
            | Expr::Wildcard { .. }
            | Expr::Placeholder(Placeholder { .. })
            | Expr::OuterReferenceColumn(_, _)
            | Expr::Exists { .. }
            | Expr::ScalarSubquery(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Unnest(_)
            | Expr::Literal(_) => self,
            Expr::Alias(Alias {
                expr,
                relation,
                name,
            }) => Expr::Alias(Alias::new(transform(*expr)?, relation, name)),
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated,
            }) => Expr::InSubquery(InSubquery::new(
                transform_boxed(expr, &mut transform)?,
                subquery,
                negated,
            )),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                Expr::BinaryExpr(BinaryExpr::new(
                    transform_boxed(left, &mut transform)?,
                    op,
                    transform_boxed(right, &mut transform)?,
                ))
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => Expr::Like(Like::new(
                negated,
                transform_boxed(expr, &mut transform)?,
                transform_boxed(pattern, &mut transform)?,
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
                negated,
                transform_boxed(expr, &mut transform)?,
                transform_boxed(pattern, &mut transform)?,
                escape_char,
                case_insensitive,
            )),
            Expr::Not(expr) => Expr::Not(transform_boxed(expr, &mut transform)?),
            Expr::IsNotNull(expr) => {
                Expr::IsNotNull(transform_boxed(expr, &mut transform)?)
            }
            Expr::IsNull(expr) => Expr::IsNull(transform_boxed(expr, &mut transform)?),
            Expr::IsTrue(expr) => Expr::IsTrue(transform_boxed(expr, &mut transform)?),
            Expr::IsFalse(expr) => Expr::IsFalse(transform_boxed(expr, &mut transform)?),
            Expr::IsUnknown(expr) => {
                Expr::IsUnknown(transform_boxed(expr, &mut transform)?)
            }
            Expr::IsNotTrue(expr) => {
                Expr::IsNotTrue(transform_boxed(expr, &mut transform)?)
            }
            Expr::IsNotFalse(expr) => {
                Expr::IsNotFalse(transform_boxed(expr, &mut transform)?)
            }
            Expr::IsNotUnknown(expr) => {
                Expr::IsNotUnknown(transform_boxed(expr, &mut transform)?)
            }
            Expr::Negative(expr) => {
                Expr::Negative(transform_boxed(expr, &mut transform)?)
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => Expr::Between(Between::new(
                transform_boxed(expr, &mut transform)?,
                negated,
                transform_boxed(low, &mut transform)?,
                transform_boxed(high, &mut transform)?,
            )),
            Expr::Case(case) => {
                let expr = transform_option_box(case.expr, &mut transform)?;
                let when_then_expr = case
                    .when_then_expr
                    .into_iter()
                    .map(|(when, then)| {
                        Ok((
                            transform_boxed(when, &mut transform)?,
                            transform_boxed(then, &mut transform)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let else_expr = transform_option_box(case.else_expr, &mut transform)?;

                Expr::Case(Case::new(expr, when_then_expr, else_expr))
            }
            Expr::Cast(Cast { expr, data_type }) => {
                Expr::Cast(Cast::new(transform_boxed(expr, &mut transform)?, data_type))
            }
            Expr::TryCast(TryCast { expr, data_type }) => Expr::TryCast(TryCast::new(
                transform_boxed(expr, &mut transform)?,
                data_type,
            )),
            Expr::Sort(Sort {
                expr,
                asc,
                nulls_first,
            }) => Expr::Sort(Sort::new(
                transform_boxed(expr, &mut transform)?,
                asc,
                nulls_first,
            )),
            Expr::ScalarFunction(ScalarFunction { func_def, args }) => match func_def {
                ScalarFunctionDefinition::BuiltIn(fun) => Expr::ScalarFunction(
                    ScalarFunction::new(fun, transform_vec(args, &mut transform)?),
                ),
                ScalarFunctionDefinition::UDF(fun) => Expr::ScalarFunction(
                    ScalarFunction::new_udf(fun, transform_vec(args, &mut transform)?),
                ),
                ScalarFunctionDefinition::Name(_) => {
                    return internal_err!("Function `Expr` with name should be resolved.")
                }
            },
            Expr::WindowFunction(WindowFunction {
                args,
                fun,
                partition_by,
                order_by,
                window_frame,
                null_treatment,
            }) => Expr::WindowFunction(WindowFunction::new(
                fun,
                transform_vec(args, &mut transform)?,
                transform_vec(partition_by, &mut transform)?,
                transform_vec(order_by, &mut transform)?,
                window_frame,
                null_treatment,
            )),
            Expr::AggregateFunction(AggregateFunction {
                args,
                func_def,
                distinct,
                filter,
                order_by,
            }) => match func_def {
                AggregateFunctionDefinition::BuiltIn(fun) => {
                    Expr::AggregateFunction(AggregateFunction::new(
                        fun,
                        transform_vec(args, &mut transform)?,
                        distinct,
                        transform_option_box(filter, &mut transform)?,
                        transform_option_vec(order_by, &mut transform)?,
                    ))
                }
                AggregateFunctionDefinition::UDF(fun) => {
                    let order_by = order_by
                        .map(|order_by| transform_vec(order_by, &mut transform))
                        .transpose()?;
                    Expr::AggregateFunction(AggregateFunction::new_udf(
                        fun,
                        transform_vec(args, &mut transform)?,
                        false,
                        transform_option_box(filter, &mut transform)?,
                        transform_option_vec(order_by, &mut transform)?,
                    ))
                }
                AggregateFunctionDefinition::Name(_) => {
                    return internal_err!("Function `Expr` with name should be resolved.")
                }
            },
            Expr::GroupingSet(grouping_set) => match grouping_set {
                GroupingSet::Rollup(exprs) => Expr::GroupingSet(GroupingSet::Rollup(
                    transform_vec(exprs, &mut transform)?,
                )),
                GroupingSet::Cube(exprs) => Expr::GroupingSet(GroupingSet::Cube(
                    transform_vec(exprs, &mut transform)?,
                )),
                GroupingSet::GroupingSets(lists_of_exprs) => {
                    Expr::GroupingSet(GroupingSet::GroupingSets(
                        lists_of_exprs
                            .into_iter()
                            .map(|exprs| transform_vec(exprs, &mut transform))
                            .collect::<Result<Vec<_>>>()?,
                    ))
                }
            },
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => Expr::InList(InList::new(
                transform_boxed(expr, &mut transform)?,
                transform_vec(list, &mut transform)?,
                negated,
            )),
            Expr::GetIndexedField(GetIndexedField { expr, field }) => {
                Expr::GetIndexedField(GetIndexedField::new(
                    transform_boxed(expr, &mut transform)?,
                    field,
                ))
            }
        })
    }
}

fn transform_boxed<F: FnMut(Expr) -> Result<Expr>>(
    boxed_expr: Box<Expr>,
    transform: &mut F,
) -> Result<Box<Expr>> {
    // TODO: It might be possible to avoid an allocation (the Box::new) below by reusing the box.
    transform(*boxed_expr).map(Box::new)
}

fn transform_option_box<F: FnMut(Expr) -> Result<Expr>>(
    option_box: Option<Box<Expr>>,
    transform: &mut F,
) -> Result<Option<Box<Expr>>> {
    option_box
        .map(|expr| transform_boxed(expr, transform))
        .transpose()
}

/// &mut transform a Option<`Vec` of `Expr`s>
fn transform_option_vec<F: FnMut(Expr) -> Result<Expr>>(
    option_box: Option<Vec<Expr>>,
    transform: &mut F,
) -> Result<Option<Vec<Expr>>> {
    option_box
        .map(|exprs| transform_vec(exprs, transform))
        .transpose()
}

/// &mut transform a `Vec` of `Expr`s
fn transform_vec<F: FnMut(Expr) -> Result<Expr>>(
    v: Vec<Expr>,
    transform: &mut F,
) -> Result<Vec<Expr>> {
    v.into_iter().map(transform).collect()
}
