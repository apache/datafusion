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
    AggregateFunction, Alias, Between, BinaryExpr, Case, Cast, GroupingSet, InList,
    InSubquery, Like, Placeholder, ScalarFunction, TryCast, Unnest, WindowFunction,
};
use crate::{Expr, ExprFunctionExt};

use crate::logical_plan::tree_node::LogicalPlanStats;
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
            Expr::Alias(Alias { expr, .. }, _)
            | Expr::Unnest(Unnest { expr }, _)
            | Expr::Not(expr, _)
            | Expr::IsNotNull(expr, _)
            | Expr::IsTrue(expr, _)
            | Expr::IsFalse(expr, _)
            | Expr::IsUnknown(expr, _)
            | Expr::IsNotTrue(expr, _)
            | Expr::IsNotFalse(expr, _)
            | Expr::IsNotUnknown(expr, _)
            | Expr::IsNull(expr, _)
            | Expr::Negative(expr, _)
            | Expr::Cast(Cast { expr, .. }, _)
            | Expr::TryCast(TryCast { expr, .. }, _)
            | Expr::InSubquery(InSubquery { expr, .. }, _) => expr.apply_elements(f),
            Expr::GroupingSet(GroupingSet::Rollup(exprs), _)
            | Expr::GroupingSet(GroupingSet::Cube(exprs), _) => exprs.apply_elements(f),
            Expr::ScalarFunction(ScalarFunction { args, .. }, _) => {
                args.apply_elements(f)
            }
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs), _) => {
                lists_of_exprs.apply_elements(f)
            }
            Expr::Column(_, _)
            // Treat OuterReferenceColumn as a leaf expression
            | Expr::OuterReferenceColumn(_, _, _)
            | Expr::ScalarVariable(_, _, _)
            | Expr::Literal(_, _)
            | Expr::Exists { .. }
            | Expr::ScalarSubquery(_, _)
            | Expr::Wildcard { .. }
            | Expr::Placeholder(_, _) => Ok(TreeNodeRecursion::Continue),
            Expr::BinaryExpr(BinaryExpr { left, right, .. }, _) => {
                (left, right).apply_ref_elements(f)
            }
            Expr::Like(Like { expr, pattern, .. }, _)
            | Expr::SimilarTo(Like { expr, pattern, .. }, _) => {
                (expr, pattern).apply_ref_elements(f)
            }
            Expr::Between(Between {
                              expr, low, high, ..
                          }, _) => (expr, low, high).apply_ref_elements(f),
            Expr::Case(Case { expr, when_then_expr, else_expr }, _) =>
                (expr, when_then_expr, else_expr).apply_ref_elements(f),
            Expr::AggregateFunction(AggregateFunction { args, filter, order_by, .. }, _) =>
                (args, filter, order_by).apply_ref_elements(f),
            Expr::WindowFunction(WindowFunction {
                                     args,
                                     partition_by,
                                     order_by,
                                     ..
                                 }, _) => {
                (args, partition_by, order_by).apply_ref_elements(f)
            }
            Expr::InList(InList { expr, list, .. }, _) => {
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
            Expr::Column(_, _)
            | Expr::Wildcard { .. }
            | Expr::Placeholder(Placeholder { .. }, _)
            | Expr::OuterReferenceColumn(_, _, _)
            | Expr::Exists { .. }
            | Expr::ScalarSubquery(_, _)
            | Expr::ScalarVariable(_, _, _)
            | Expr::Literal(_, _) => Transformed::no(self),
            Expr::Unnest(Unnest { expr, .. }, _) => expr
                .map_elements(f)?
                .update_data(|expr| Expr::unnest(Unnest { expr })),
            Expr::Alias(
                Alias {
                    expr,
                    relation,
                    name,
                },
                _,
            ) => f(*expr)?.update_data(|e| e.alias_qualified(relation, name)),
            Expr::InSubquery(
                InSubquery {
                    expr,
                    subquery,
                    negated,
                },
                _,
            ) => expr.map_elements(f)?.update_data(|be| {
                Expr::in_subquery(InSubquery::new(be, subquery, negated))
            }),
            Expr::BinaryExpr(BinaryExpr { left, op, right }, _) => (left, right)
                .map_elements(f)?
                .update_data(|(new_left, new_right)| {
                    Expr::binary_expr(BinaryExpr::new(new_left, op, new_right))
                }),
            Expr::Like(
                Like {
                    negated,
                    expr,
                    pattern,
                    escape_char,
                    case_insensitive,
                },
                _,
            ) => {
                (expr, pattern)
                    .map_elements(f)?
                    .update_data(|(new_expr, new_pattern)| {
                        Expr::_like(Like::new(
                            negated,
                            new_expr,
                            new_pattern,
                            escape_char,
                            case_insensitive,
                        ))
                    })
            }
            Expr::SimilarTo(
                Like {
                    negated,
                    expr,
                    pattern,
                    escape_char,
                    case_insensitive,
                },
                _,
            ) => {
                (expr, pattern)
                    .map_elements(f)?
                    .update_data(|(new_expr, new_pattern)| {
                        Expr::similar_to(Like::new(
                            negated,
                            new_expr,
                            new_pattern,
                            escape_char,
                            case_insensitive,
                        ))
                    })
            }
            Expr::Not(expr, _) => expr.map_elements(f)?.update_data(Expr::_not),
            Expr::IsNotNull(expr, _) => {
                expr.map_elements(f)?.update_data(Expr::_is_not_null)
            }
            Expr::IsNull(expr, _) => expr.map_elements(f)?.update_data(Expr::_is_null),
            Expr::IsTrue(expr, _) => expr.map_elements(f)?.update_data(Expr::_is_true),
            Expr::IsFalse(expr, _) => expr.map_elements(f)?.update_data(Expr::_is_false),
            Expr::IsUnknown(expr, _) => {
                expr.map_elements(f)?.update_data(Expr::_is_unknown)
            }
            Expr::IsNotTrue(expr, _) => {
                expr.map_elements(f)?.update_data(Expr::_is_not_true)
            }
            Expr::IsNotFalse(expr, _) => {
                expr.map_elements(f)?.update_data(Expr::_is_not_false)
            }
            Expr::IsNotUnknown(expr, _) => {
                expr.map_elements(f)?.update_data(Expr::_is_not_unknown)
            }
            Expr::Negative(expr, _) => expr.map_elements(f)?.update_data(Expr::negative),
            Expr::Between(
                Between {
                    expr,
                    negated,
                    low,
                    high,
                },
                _,
            ) => (expr, low, high).map_elements(f)?.update_data(
                |(new_expr, new_low, new_high)| {
                    Expr::_between(Between::new(new_expr, negated, new_low, new_high))
                },
            ),
            Expr::Case(
                Case {
                    expr,
                    when_then_expr,
                    else_expr,
                },
                _,
            ) => (expr, when_then_expr, else_expr)
                .map_elements(f)?
                .update_data(|(new_expr, new_when_then_expr, new_else_expr)| {
                    Expr::case(Case::new(new_expr, new_when_then_expr, new_else_expr))
                }),
            Expr::Cast(Cast { expr, data_type }, _) => expr
                .map_elements(f)?
                .update_data(|be| Expr::cast(Cast::new(be, data_type))),
            Expr::TryCast(TryCast { expr, data_type }, _) => expr
                .map_elements(f)?
                .update_data(|be| Expr::try_cast(TryCast::new(be, data_type))),
            Expr::ScalarFunction(ScalarFunction { func, args }, _) => {
                args.map_elements(f)?.map_data(|new_args| {
                    Ok(Expr::scalar_function(ScalarFunction::new_udf(
                        func, new_args,
                    )))
                })?
            }
            Expr::WindowFunction(
                WindowFunction {
                    args,
                    fun,
                    partition_by,
                    order_by,
                    window_frame,
                    null_treatment,
                },
                _,
            ) => (args, partition_by, order_by).map_elements(f)?.update_data(
                |(new_args, new_partition_by, new_order_by)| {
                    Expr::window_function(WindowFunction::new(fun, new_args))
                        .partition_by(new_partition_by)
                        .order_by(new_order_by)
                        .window_frame(window_frame)
                        .null_treatment(null_treatment)
                        .build()
                        .unwrap()
                },
            ),
            Expr::AggregateFunction(
                AggregateFunction {
                    args,
                    func,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                },
                _,
            ) => (args, filter, order_by).map_elements(f)?.map_data(
                |(new_args, new_filter, new_order_by)| {
                    Ok(Expr::aggregate_function(AggregateFunction::new_udf(
                        func,
                        new_args,
                        distinct,
                        new_filter,
                        new_order_by,
                        null_treatment,
                    )))
                },
            )?,
            Expr::GroupingSet(grouping_set, _) => match grouping_set {
                GroupingSet::Rollup(exprs) => {
                    exprs.map_elements(f)?.update_data(GroupingSet::Rollup)
                }
                GroupingSet::Cube(exprs) => {
                    exprs.map_elements(f)?.update_data(GroupingSet::Cube)
                }
                GroupingSet::GroupingSets(lists_of_exprs) => lists_of_exprs
                    .map_elements(f)?
                    .update_data(GroupingSet::GroupingSets),
            }
            .update_data(Expr::grouping_set),
            Expr::InList(
                InList {
                    expr,
                    list,
                    negated,
                },
                _,
            ) => (expr, list)
                .map_elements(f)?
                .update_data(|(new_expr, new_list)| {
                    Expr::_in_list(InList::new(new_expr, new_list, negated))
                }),
        })
    }
}
impl Expr {
    pub fn stats(&self) -> LogicalPlanStats {
        match self {
            Expr::Alias(_, stats) => *stats,
            Expr::Column(_, stats) => *stats,
            Expr::ScalarVariable(_, _, stats) => *stats,
            Expr::Literal(_, stats) => *stats,
            Expr::BinaryExpr(_, stats) => *stats,
            Expr::Like(_, stats) => *stats,
            Expr::SimilarTo(_, stats) => *stats,
            Expr::Not(_, stats) => *stats,
            Expr::IsNotNull(_, stats) => *stats,
            Expr::IsNull(_, stats) => *stats,
            Expr::IsTrue(_, stats) => *stats,
            Expr::IsFalse(_, stats) => *stats,
            Expr::IsUnknown(_, stats) => *stats,
            Expr::IsNotTrue(_, stats) => *stats,
            Expr::IsNotFalse(_, stats) => *stats,
            Expr::IsNotUnknown(_, stats) => *stats,
            Expr::Negative(_, stats) => *stats,
            Expr::Between(_, stats) => *stats,
            Expr::Case(_, stats) => *stats,
            Expr::Cast(_, stats) => *stats,
            Expr::TryCast(_, stats) => *stats,
            Expr::ScalarFunction(_, stats) => *stats,
            Expr::AggregateFunction(_, stats) => *stats,
            Expr::WindowFunction(_, stats) => *stats,
            Expr::InList(_, stats) => *stats,
            Expr::Exists(_, stats) => *stats,
            Expr::InSubquery(_, stats) => *stats,
            Expr::ScalarSubquery(_, stats) => *stats,
            Expr::Wildcard(_, stats) => *stats,
            Expr::GroupingSet(_, stats) => *stats,
            Expr::Placeholder(_, stats) => *stats,
            Expr::OuterReferenceColumn(_, _, stats) => *stats,
            Expr::Unnest(_, stats) => *stats,
        }
    }
}
