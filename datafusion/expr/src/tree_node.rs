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

//! Tree node implementation for Logical Expressions

use crate::expr::{
    AggregateFunction, AggregateFunctionParams, Alias, Between, BinaryExpr, Case, Cast,
    GroupingSet, InList, InSubquery, Like, Placeholder, ScalarFunction, TryCast, Unnest,
    WindowFunction, WindowFunctionParams,
};
use crate::{Expr, ExprFunctionExt};

use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion, TreeNodeRefContainer,
};
use datafusion_common::Result;

/// Implementation of the [`TreeNode`] trait
///
/// This allows logical expressions (`Expr`) to be traversed and transformed
/// Facilitates tasks such as optimization and rewriting during query
/// planning.
impl TreeNode for Expr {
    /// Applies a function `f` to each child expression of `self`.
    ///
    /// The function `f` determines whether to continue traversing the tree or to stop.
    /// This method collects all child expressions and applies `f` to each.
    fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        match self {
            Expr::Alias(boxed_alias) => {
                boxed_alias.expr.apply_elements(f)
            }
            Expr::Unnest(Unnest { expr })
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
            | Expr::InSubquery(InSubquery { expr, .. }) => expr.apply_elements(f),
            Expr::GroupingSet(GroupingSet::Rollup(exprs))
            | Expr::GroupingSet(GroupingSet::Cube(exprs)) => exprs.apply_elements(f),
            Expr::ScalarFunction(ScalarFunction { args, .. }) => {
                args.apply_elements(f)
            }
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                lists_of_exprs.apply_elements(f)
            }
            // TODO: remove the next line after `Expr::Wildcard` is removed
            #[expect(deprecated)]
            Expr::Column(_)
            // Treat OuterReferenceColumn as a leaf expression
            | Expr::OuterReferenceColumn(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Literal(_, _)
            | Expr::Exists { .. }
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard { .. }
            | Expr::Placeholder(_) => Ok(TreeNodeRecursion::Continue),
            Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
                (left, right).apply_ref_elements(f)
            }
            Expr::Like(Like { expr, pattern, .. })
            | Expr::SimilarTo(Like { expr, pattern, .. }) => {
                (expr, pattern).apply_ref_elements(f)
            }
            Expr::Between(Between {
                              expr, low, high, ..
                          }) => (expr, low, high).apply_ref_elements(f),
            Expr::Case(Case { expr, when_then_expr, else_expr }) =>
                (expr, when_then_expr, else_expr).apply_ref_elements(f),
            Expr::AggregateFunction(AggregateFunction { params: AggregateFunctionParams { args, filter, order_by, ..}, .. }) =>
                (args, filter, order_by).apply_ref_elements(f),
            Expr::WindowFunction(window_fun) => {
                let WindowFunctionParams {
                    args,
                    partition_by,
                    order_by,
                    ..
                } = &window_fun.as_ref().params;
                (args, partition_by, order_by).apply_ref_elements(f)
            }

            Expr::InList(InList { expr, list, .. }) => {
                (expr, list).apply_ref_elements(f)
            }
        }
    }

    /// Maps each child of `self` using the provided closure `f`.
    ///
    /// The closure `f` takes ownership of an expression and returns a `Transformed` result,
    /// indicating whether the expression was transformed or left unchanged.
    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        Ok(match self {
            // TODO: remove the next line after `Expr::Wildcard` is removed
            #[expect(deprecated)]
            Expr::Column(_)
            | Expr::Wildcard { .. }
            | Expr::Placeholder(Placeholder { .. })
            | Expr::OuterReferenceColumn(_)
            | Expr::Exists { .. }
            | Expr::ScalarSubquery(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Literal(_, _) => Transformed::no(self),
            Expr::Unnest(Unnest { expr, .. }) => expr
                .map_elements(f)?
                .update_data(|expr| Expr::Unnest(Unnest { expr })),
            Expr::Alias(boxed_alias) => {
                let Alias {
                    expr,
                    relation,
                    name,
                    metadata,
                    ..
                } = *boxed_alias;

                f(*expr)?.update_data(|e| {
                    e.alias_qualified_with_metadata(relation, name, metadata)
                })
            }
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated,
            }) => expr.map_elements(f)?.update_data(|be| {
                Expr::InSubquery(InSubquery::new(be, subquery, negated))
            }),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => (left, right)
                .map_elements(f)?
                .update_data(|(new_left, new_right)| {
                    Expr::BinaryExpr(BinaryExpr::new(new_left, op, new_right))
                }),
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => {
                (expr, pattern)
                    .map_elements(f)?
                    .update_data(|(new_expr, new_pattern)| {
                        Expr::Like(Like::new(
                            negated,
                            new_expr,
                            new_pattern,
                            escape_char,
                            case_insensitive,
                        ))
                    })
            }
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => {
                (expr, pattern)
                    .map_elements(f)?
                    .update_data(|(new_expr, new_pattern)| {
                        Expr::SimilarTo(Like::new(
                            negated,
                            new_expr,
                            new_pattern,
                            escape_char,
                            case_insensitive,
                        ))
                    })
            }
            Expr::Not(expr) => expr.map_elements(f)?.update_data(Expr::Not),
            Expr::IsNotNull(expr) => expr.map_elements(f)?.update_data(Expr::IsNotNull),
            Expr::IsNull(expr) => expr.map_elements(f)?.update_data(Expr::IsNull),
            Expr::IsTrue(expr) => expr.map_elements(f)?.update_data(Expr::IsTrue),
            Expr::IsFalse(expr) => expr.map_elements(f)?.update_data(Expr::IsFalse),
            Expr::IsUnknown(expr) => expr.map_elements(f)?.update_data(Expr::IsUnknown),
            Expr::IsNotTrue(expr) => expr.map_elements(f)?.update_data(Expr::IsNotTrue),
            Expr::IsNotFalse(expr) => expr.map_elements(f)?.update_data(Expr::IsNotFalse),
            Expr::IsNotUnknown(expr) => {
                expr.map_elements(f)?.update_data(Expr::IsNotUnknown)
            }
            Expr::Negative(expr) => expr.map_elements(f)?.update_data(Expr::Negative),
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => (expr, low, high).map_elements(f)?.update_data(
                |(new_expr, new_low, new_high)| {
                    Expr::Between(Between::new(new_expr, negated, new_low, new_high))
                },
            ),
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => (expr, when_then_expr, else_expr)
                .map_elements(f)?
                .update_data(|(new_expr, new_when_then_expr, new_else_expr)| {
                    Expr::Case(Case::new(new_expr, new_when_then_expr, new_else_expr))
                }),
            Expr::Cast(Cast { expr, data_type }) => expr
                .map_elements(f)?
                .update_data(|be| Expr::Cast(Cast::new(be, data_type))),
            Expr::TryCast(TryCast { expr, data_type }) => expr
                .map_elements(f)?
                .update_data(|be| Expr::TryCast(TryCast::new(be, data_type))),
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                args.map_elements(f)?.map_data(|new_args| {
                    Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
                        func, new_args,
                    )))
                })?
            }
            Expr::WindowFunction(window_fun) => {
                let WindowFunction {
                    fun,
                    params:
                        WindowFunctionParams {
                            args,
                            partition_by,
                            order_by,
                            window_frame,
                            null_treatment,
                            distinct,
                        },
                } = *window_fun;
                (args, partition_by, order_by).map_elements(f)?.update_data(
                    |(new_args, new_partition_by, new_order_by)| {
                        if distinct {
                            return Expr::from(WindowFunction::new(fun, new_args))
                                .partition_by(new_partition_by)
                                .order_by(new_order_by)
                                .window_frame(window_frame)
                                .null_treatment(null_treatment)
                                .distinct()
                                .build()
                                .unwrap();
                        }

                        Expr::from(WindowFunction::new(fun, new_args))
                            .partition_by(new_partition_by)
                            .order_by(new_order_by)
                            .window_frame(window_frame)
                            .null_treatment(null_treatment)
                            .build()
                            .unwrap()
                    },
                )
            }
            Expr::AggregateFunction(AggregateFunction {
                func,
                params:
                    AggregateFunctionParams {
                        args,
                        distinct,
                        filter,
                        order_by,
                        null_treatment,
                    },
            }) => (args, filter, order_by).map_elements(f)?.map_data(
                |(new_args, new_filter, new_order_by)| {
                    Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                        func,
                        new_args,
                        distinct,
                        new_filter,
                        new_order_by,
                        null_treatment,
                    )))
                },
            )?,
            Expr::GroupingSet(grouping_set) => match grouping_set {
                GroupingSet::Rollup(exprs) => exprs
                    .map_elements(f)?
                    .update_data(|ve| Expr::GroupingSet(GroupingSet::Rollup(ve))),
                GroupingSet::Cube(exprs) => exprs
                    .map_elements(f)?
                    .update_data(|ve| Expr::GroupingSet(GroupingSet::Cube(ve))),
                GroupingSet::GroupingSets(lists_of_exprs) => lists_of_exprs
                    .map_elements(f)?
                    .update_data(|new_lists_of_exprs| {
                        Expr::GroupingSet(GroupingSet::GroupingSets(new_lists_of_exprs))
                    }),
            },
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => (expr, list)
                .map_elements(f)?
                .update_data(|(new_expr, new_list)| {
                    Expr::InList(InList::new(new_expr, new_list, negated))
                }),
        })
    }
}
