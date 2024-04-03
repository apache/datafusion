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

use datafusion_common::tree_node::{
    Transformed, TransformedIterator, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{handle_visit_recursion, internal_err, Result};

impl TreeNode for Expr {
    fn apply_children<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        f: &mut F,
    ) -> Result<TreeNodeRecursion> {
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

        let mut tnr = TreeNodeRecursion::Continue;
        for child in children {
            tnr = f(child)?;
            handle_visit_recursion!(tnr, DOWN);
        }

        Ok(tnr)
    }

    fn map_children<F>(self, mut f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        Ok(match self {
            Expr::Column(_)
            | Expr::Wildcard { .. }
            | Expr::Placeholder(Placeholder { .. })
            | Expr::OuterReferenceColumn(_, _)
            | Expr::Exists { .. }
            | Expr::ScalarSubquery(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Unnest(_)
            | Expr::Literal(_) => Transformed::no(self),
            Expr::Alias(Alias {
                expr,
                relation,
                name,
            }) => f(*expr)?.update_data(|e| Expr::Alias(Alias::new(e, relation, name))),
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated,
            }) => transform_box(expr, &mut f)?.update_data(|be| {
                Expr::InSubquery(InSubquery::new(be, subquery, negated))
            }),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                transform_box(left, &mut f)?
                    .update_data(|new_left| (new_left, right))
                    .try_transform_node(|(new_left, right)| {
                        Ok(transform_box(right, &mut f)?
                            .update_data(|new_right| (new_left, new_right)))
                    })?
                    .update_data(|(new_left, new_right)| {
                        Expr::BinaryExpr(BinaryExpr::new(new_left, op, new_right))
                    })
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => transform_box(expr, &mut f)?
                .update_data(|new_expr| (new_expr, pattern))
                .try_transform_node(|(new_expr, pattern)| {
                    Ok(transform_box(pattern, &mut f)?
                        .update_data(|new_pattern| (new_expr, new_pattern)))
                })?
                .update_data(|(new_expr, new_pattern)| {
                    Expr::Like(Like::new(
                        negated,
                        new_expr,
                        new_pattern,
                        escape_char,
                        case_insensitive,
                    ))
                }),
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => transform_box(expr, &mut f)?
                .update_data(|new_expr| (new_expr, pattern))
                .try_transform_node(|(new_expr, pattern)| {
                    Ok(transform_box(pattern, &mut f)?
                        .update_data(|new_pattern| (new_expr, new_pattern)))
                })?
                .update_data(|(new_expr, new_pattern)| {
                    Expr::SimilarTo(Like::new(
                        negated,
                        new_expr,
                        new_pattern,
                        escape_char,
                        case_insensitive,
                    ))
                }),
            Expr::Not(expr) => transform_box(expr, &mut f)?.update_data(Expr::Not),
            Expr::IsNotNull(expr) => {
                transform_box(expr, &mut f)?.update_data(Expr::IsNotNull)
            }
            Expr::IsNull(expr) => transform_box(expr, &mut f)?.update_data(Expr::IsNull),
            Expr::IsTrue(expr) => transform_box(expr, &mut f)?.update_data(Expr::IsTrue),
            Expr::IsFalse(expr) => {
                transform_box(expr, &mut f)?.update_data(Expr::IsFalse)
            }
            Expr::IsUnknown(expr) => {
                transform_box(expr, &mut f)?.update_data(Expr::IsUnknown)
            }
            Expr::IsNotTrue(expr) => {
                transform_box(expr, &mut f)?.update_data(Expr::IsNotTrue)
            }
            Expr::IsNotFalse(expr) => {
                transform_box(expr, &mut f)?.update_data(Expr::IsNotFalse)
            }
            Expr::IsNotUnknown(expr) => {
                transform_box(expr, &mut f)?.update_data(Expr::IsNotUnknown)
            }
            Expr::Negative(expr) => {
                transform_box(expr, &mut f)?.update_data(Expr::Negative)
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => transform_box(expr, &mut f)?
                .update_data(|new_expr| (new_expr, low, high))
                .try_transform_node(|(new_expr, low, high)| {
                    Ok(transform_box(low, &mut f)?
                        .update_data(|new_low| (new_expr, new_low, high)))
                })?
                .try_transform_node(|(new_expr, new_low, high)| {
                    Ok(transform_box(high, &mut f)?
                        .update_data(|new_high| (new_expr, new_low, new_high)))
                })?
                .update_data(|(new_expr, new_low, new_high)| {
                    Expr::Between(Between::new(new_expr, negated, new_low, new_high))
                }),
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => transform_option_box(expr, &mut f)?
                .update_data(|new_expr| (new_expr, when_then_expr, else_expr))
                .try_transform_node(|(new_expr, when_then_expr, else_expr)| {
                    Ok(when_then_expr
                        .into_iter()
                        .map_until_stop_and_collect(|(when, then)| {
                            transform_box(when, &mut f)?
                                .update_data(|new_when| (new_when, then))
                                .try_transform_node(|(new_when, then)| {
                                    Ok(transform_box(then, &mut f)?
                                        .update_data(|new_then| (new_when, new_then)))
                                })
                        })?
                        .update_data(|new_when_then_expr| {
                            (new_expr, new_when_then_expr, else_expr)
                        }))
                })?
                .try_transform_node(|(new_expr, new_when_then_expr, else_expr)| {
                    Ok(transform_option_box(else_expr, &mut f)?.update_data(
                        |new_else_expr| (new_expr, new_when_then_expr, new_else_expr),
                    ))
                })?
                .update_data(|(new_expr, new_when_then_expr, new_else_expr)| {
                    Expr::Case(Case::new(new_expr, new_when_then_expr, new_else_expr))
                }),
            Expr::Cast(Cast { expr, data_type }) => transform_box(expr, &mut f)?
                .update_data(|be| Expr::Cast(Cast::new(be, data_type))),
            Expr::TryCast(TryCast { expr, data_type }) => transform_box(expr, &mut f)?
                .update_data(|be| Expr::TryCast(TryCast::new(be, data_type))),
            Expr::Sort(Sort {
                expr,
                asc,
                nulls_first,
            }) => transform_box(expr, &mut f)?
                .update_data(|be| Expr::Sort(Sort::new(be, asc, nulls_first))),
            Expr::ScalarFunction(ScalarFunction { func_def, args }) => {
                transform_vec(args, &mut f)?.map_data(|new_args| match func_def {
                    ScalarFunctionDefinition::BuiltIn(fun) => {
                        Ok(Expr::ScalarFunction(ScalarFunction::new(fun, new_args)))
                    }
                    ScalarFunctionDefinition::UDF(fun) => {
                        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(fun, new_args)))
                    }
                    ScalarFunctionDefinition::Name(_) => {
                        internal_err!("Function `Expr` with name should be resolved.")
                    }
                })?
            }
            Expr::WindowFunction(WindowFunction {
                args,
                fun,
                partition_by,
                order_by,
                window_frame,
                null_treatment,
            }) => transform_vec(args, &mut f)?
                .update_data(|new_args| (new_args, partition_by, order_by))
                .try_transform_node(|(new_args, partition_by, order_by)| {
                    Ok(transform_vec(partition_by, &mut f)?.update_data(
                        |new_partition_by| (new_args, new_partition_by, order_by),
                    ))
                })?
                .try_transform_node(|(new_args, new_partition_by, order_by)| {
                    Ok(
                        transform_vec(order_by, &mut f)?.update_data(|new_order_by| {
                            (new_args, new_partition_by, new_order_by)
                        }),
                    )
                })?
                .update_data(|(new_args, new_partition_by, new_order_by)| {
                    Expr::WindowFunction(WindowFunction::new(
                        fun,
                        new_args,
                        new_partition_by,
                        new_order_by,
                        window_frame,
                        null_treatment,
                    ))
                }),
            Expr::AggregateFunction(AggregateFunction {
                args,
                func_def,
                distinct,
                filter,
                order_by,
                null_treatment,
            }) => transform_vec(args, &mut f)?
                .update_data(|new_args| (new_args, filter, order_by))
                .try_transform_node(|(new_args, filter, order_by)| {
                    Ok(transform_option_box(filter, &mut f)?
                        .update_data(|new_filter| (new_args, new_filter, order_by)))
                })?
                .try_transform_node(|(new_args, new_filter, order_by)| {
                    Ok(transform_option_vec(order_by, &mut f)?
                        .update_data(|new_order_by| (new_args, new_filter, new_order_by)))
                })?
                .map_data(|(new_args, new_filter, new_order_by)| match func_def {
                    AggregateFunctionDefinition::BuiltIn(fun) => {
                        Ok(Expr::AggregateFunction(AggregateFunction::new(
                            fun,
                            new_args,
                            distinct,
                            new_filter,
                            new_order_by,
                            null_treatment,
                        )))
                    }
                    AggregateFunctionDefinition::UDF(fun) => {
                        Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                            fun,
                            new_args,
                            false,
                            new_filter,
                            new_order_by,
                            null_treatment,
                        )))
                    }
                    AggregateFunctionDefinition::Name(_) => {
                        internal_err!("Function `Expr` with name should be resolved.")
                    }
                })?,
            Expr::GroupingSet(grouping_set) => match grouping_set {
                GroupingSet::Rollup(exprs) => transform_vec(exprs, &mut f)?
                    .update_data(|ve| Expr::GroupingSet(GroupingSet::Rollup(ve))),
                GroupingSet::Cube(exprs) => transform_vec(exprs, &mut f)?
                    .update_data(|ve| Expr::GroupingSet(GroupingSet::Cube(ve))),
                GroupingSet::GroupingSets(lists_of_exprs) => lists_of_exprs
                    .into_iter()
                    .map_until_stop_and_collect(|exprs| transform_vec(exprs, &mut f))?
                    .update_data(|new_lists_of_exprs| {
                        Expr::GroupingSet(GroupingSet::GroupingSets(new_lists_of_exprs))
                    }),
            },
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => transform_box(expr, &mut f)?
                .update_data(|new_expr| (new_expr, list))
                .try_transform_node(|(new_expr, list)| {
                    Ok(transform_vec(list, &mut f)?
                        .update_data(|new_list| (new_expr, new_list)))
                })?
                .update_data(|(new_expr, new_list)| {
                    Expr::InList(InList::new(new_expr, new_list, negated))
                }),
            Expr::GetIndexedField(GetIndexedField { expr, field }) => {
                transform_box(expr, &mut f)?.update_data(|be| {
                    Expr::GetIndexedField(GetIndexedField::new(be, field))
                })
            }
        })
    }
}

fn transform_box<F>(be: Box<Expr>, f: &mut F) -> Result<Transformed<Box<Expr>>>
where
    F: FnMut(Expr) -> Result<Transformed<Expr>>,
{
    Ok(f(*be)?.update_data(Box::new))
}

fn transform_option_box<F>(
    obe: Option<Box<Expr>>,
    f: &mut F,
) -> Result<Transformed<Option<Box<Expr>>>>
where
    F: FnMut(Expr) -> Result<Transformed<Expr>>,
{
    obe.map_or(Ok(Transformed::no(None)), |be| {
        Ok(transform_box(be, f)?.update_data(Some))
    })
}

/// &mut transform a Option<`Vec` of `Expr`s>
fn transform_option_vec<F>(
    ove: Option<Vec<Expr>>,
    f: &mut F,
) -> Result<Transformed<Option<Vec<Expr>>>>
where
    F: FnMut(Expr) -> Result<Transformed<Expr>>,
{
    ove.map_or(Ok(Transformed::no(None)), |ve| {
        Ok(transform_vec(ve, f)?.update_data(Some))
    })
}

/// &mut transform a `Vec` of `Expr`s
fn transform_vec<F>(ve: Vec<Expr>, f: &mut F) -> Result<Transformed<Vec<Expr>>>
where
    F: FnMut(Expr) -> Result<Transformed<Expr>>,
{
    ve.into_iter().map_until_stop_and_collect(f)
}
