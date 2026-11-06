// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Targeted rewrites for a subquery nested in a subquery that reads a column
//! of the query two levels up, such as `t0.g` in
//!
//! ```text
//! SELECT .. FROM t0 WHERE EXISTS (
//!     SELECT .. FROM t1 WHERE EXISTS (SELECT .. FROM t2 WHERE t2.c = t0.g))
//! ```
//!
//! [`DecorrelatePredicateSubquery`] and [`ScalarSubqueryToJoin`] decorrelate
//! a subquery into a join with the plan around it, and the correlated
//! predicates of the subquery become the join filter. A predicate that reads
//! a column two levels up cannot be evaluated there: `t0` is not an input of
//! the join of `t1` and `t2`. These rewrites run first, on the subqueries of
//! a node (the *middle* subqueries), and move such references one level
//! down, where they can, so that each subquery reads only the columns of the
//! query directly around it:
//!
//! - [`substitute_through_equalities`]: a filter of the middle subquery with
//!   a conjunct `x = t0.g` lets the subqueries in that filter read `x`
//!   instead of `t0.g`
//!   (<https://github.com/apache/datafusion/issues/15558>).
//! - [`flatten_nested_subqueries`]: in an `EXISTS` or `IN` middle subquery,
//!   whose rows count only as a set, a nested `EXISTS` or `IN` that still
//!   reads a column two levels up becomes an inner join, and the column one
//!   of the middle subquery's own outer references
//!   (<https://github.com/apache/datafusion/issues/21727>).
//!
//! Where neither applies, [`join_filters_read_outside`] and
//! [`reads_columns_of`] keep the rules from decorrelating the subqueries
//! involved: the plan keeps them, and fails to plan physically, instead of
//! reading a column where it is not in scope.
//!
//! The columns are matched by [`ColumnId`], never by name: the levels can
//! read the same table under different aliases, or under the same name.
//!
//! These are targeted fixes to the subquery rewrites that the general
//! unnesting of dependent joins
//! (<https://github.com/apache/datafusion/issues/5492>) is to replace; they
//! are kept apart from those rules and go with them.
//!
//! [`DecorrelatePredicateSubquery`]: crate::decorrelate_predicate_subquery::DecorrelatePredicateSubquery
//! [`ScalarSubqueryToJoin`]: crate::scalar_subquery_to_join::ScalarSubqueryToJoin

use std::collections::HashMap;
use std::sync::Arc;

use crate::utils::ids_builder;

use arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{ColumnId, ColumnIdGenerator, DFSchema, Result};
use datafusion_expr::expr::{Exists, InSubquery, SetComparison};
use datafusion_expr::logical_plan::{Filter, Subquery};
use datafusion_expr::utils::{conjunction, split_conjunction, split_conjunction_owned};
use datafusion_expr::{BinaryExpr, Expr, LogicalPlan, Operator};

/// Rewrites the subqueries in the expressions of `plan`, a `Filter` or a
/// `Projection` (the nodes whose subqueries the decorrelation rules
/// rewrite), so that the subqueries nested in them read fewer columns two
/// levels up (see the [module documentation](self)).
///
/// Does nothing, and walks no subquery, unless a subquery of `plan` has a
/// subquery of its own.
pub(crate) fn rewrite_nested_outer_references(
    plan: LogicalPlan,
    ids: &Arc<ColumnIdGenerator>,
) -> Result<Transformed<LogicalPlan>> {
    if !matches!(plan, LogicalPlan::Filter(_) | LogicalPlan::Projection(_))
        || !has_nested_subquery(&plan)
    {
        return Ok(Transformed::no(plan));
    }
    plan.map_expressions(|expr| {
        expr.transform_down(|expr| {
            map_subquery_expr(expr, |subquery, as_set| {
                rewrite_middle(subquery, as_set, ids)
            })
        })
    })
}

/// Rewrites the subquery of `expr` with `f`, if `expr` is a subquery
/// expression (its other parts are left to the caller's traversal). The
/// second argument of `f` tells if the value of `expr` depends only on the
/// set of rows of the subquery, not on how often each row appears: `EXISTS`
/// and `IN`, negated or not.
fn map_subquery_expr(
    expr: Expr,
    f: impl FnOnce(Subquery, bool) -> Result<Transformed<Subquery>>,
) -> Result<Transformed<Expr>> {
    Ok(match expr {
        Expr::Exists(Exists { subquery, negated }) => f(subquery, true)?
            .update_data(|subquery| Expr::Exists(Exists { subquery, negated })),
        Expr::InSubquery(InSubquery {
            expr,
            subquery,
            negated,
        }) => f(subquery, true)?.update_data(|subquery| {
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated,
            })
        }),
        Expr::SetComparison(SetComparison {
            expr,
            subquery,
            op,
            quantifier,
        }) => f(subquery, false)?.update_data(|subquery| {
            Expr::SetComparison(SetComparison {
                expr,
                subquery,
                op,
                quantifier,
            })
        }),
        Expr::ScalarSubquery(subquery) => {
            f(subquery, false)?.update_data(Expr::ScalarSubquery)
        }
        expr => Transformed::no(expr),
    })
}

/// Whether a subquery in the expressions of `plan` has a subquery in the
/// expressions of one of its nodes (the cheap check before any rewrite)
fn has_nested_subquery(plan: &LogicalPlan) -> bool {
    let mut found = false;
    // the closures never fail
    let _ = plan.apply_subqueries(|subquery| {
        found = subquery
            .exists(|node| Ok(has_subquery_expr(node)))
            .unwrap_or(false);
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
}

/// Whether an expression of `node` (not of its inputs) contains a subquery
fn has_subquery_expr(node: &LogicalPlan) -> bool {
    let mut found = false;
    // the closures never fail
    let _ = node.apply_expressions(|expr| {
        found = expr.exists(|e| Ok(is_subquery_expr(e))).unwrap_or(false);
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
}

fn is_subquery_expr(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Exists(_)
            | Expr::InSubquery(_)
            | Expr::SetComparison(_)
            | Expr::ScalarSubquery(_)
    )
}

/// Rewrites the middle subquery `subquery`, and recomputes its outer
/// references if it changed. `as_set`: whether only the set of its rows
/// matters (see [`map_subquery_expr`]), which [`flatten_nested_subqueries`]
/// needs.
fn rewrite_middle(
    mut subquery: Subquery,
    as_set: bool,
    ids: &Arc<ColumnIdGenerator>,
) -> Result<Transformed<Subquery>> {
    if !subquery
        .subquery
        .exists(|node| Ok(has_subquery_expr(node)))?
    {
        return Ok(Transformed::no(subquery));
    }
    let plan = Arc::unwrap_or_clone(Arc::clone(&subquery.subquery));
    let mut rewritten = plan.transform_down(substitute_through_equalities)?;
    if as_set {
        rewritten =
            rewritten.transform_data(|plan| flatten_nested_subqueries(plan, ids))?;
    }
    let Transformed {
        data: plan,
        transformed,
        ..
    } = rewritten;
    if !transformed {
        return Ok(Transformed::no(subquery));
    }
    // the references the middle subquery reads from outside, which the
    // rewrites may have changed
    subquery.outer_ref_columns = plan.all_out_ref_exprs();
    subquery.subquery = Arc::new(plan);
    Ok(Transformed::yes(subquery))
}

/// Where a `Filter` of a middle subquery has a top-level conjunct
/// `x = outer_ref(o)` (`x` a column of its input, `o` a column from outside
/// the middle subquery), rewrites the references to `o` in the subqueries of
/// the filter's predicate to references to `x`.
///
/// The filter keeps the rows where its predicate is TRUE, so where
/// `x = outer_ref(o)` is TRUE: `x` and `o` are equal and not NULL there, and
/// any expression of the predicate has the same value with `x` in place of
/// `o`. In the other rows the conjunct is FALSE or NULL, and the row goes
/// whatever the rest is. This holds for the conjunct at the top of the
/// predicate only (not under `OR` or `NOT`), and in the same filter only
/// (between two nodes, an outer join or an aggregate could make `x` other
/// than `o`).
///
/// The equality must say that the values are the same, not just equal
/// (`-0.0 = 0.0`): `x` and `o` have the same type, one of
/// [`equal_values_are_identical`]. `x` is not nullable where `o` is not,
/// so that the references keep the type and the nullability of `o`.
///
/// The references to `o` deeper down (a subquery in a subquery of the filter)
/// are rewritten as well: `x` is in scope there too.
fn substitute_through_equalities(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Filter(filter) = plan else {
        return Ok(Transformed::no(plan));
    };
    let schema = filter.input.schema();
    let substitutes: HashMap<ColumnId, Expr> = split_conjunction(&filter.predicate)
        .into_iter()
        .filter_map(|conjunct| outer_equality(conjunct, schema))
        .rev() // the first equality of an outer column wins
        .collect();
    if substitutes.is_empty() {
        return Ok(Transformed::no(LogicalPlan::Filter(filter)));
    }
    let input = filter.input;
    filter
        .predicate
        .transform_down(|expr| {
            map_subquery_expr(expr, |subquery, _| {
                replace_outer_references(subquery, &substitutes)
            })
        })?
        .map_data(|predicate| Filter::try_new(predicate, input).map(LogicalPlan::Filter))
}

/// If `conjunct` is `x = outer_ref(o)` or `outer_ref(o) = x`, where `x` is
/// a column of `schema` (the input of the filter) and `o` is not, both with
/// ids, and `x` can stand for `o` (see [`substitute_through_equalities`]):
/// the id of `o` and the reference to `x` that replaces it
fn outer_equality(conjunct: &Expr, schema: &DFSchema) -> Option<(ColumnId, Expr)> {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = conjunct
    else {
        return None;
    };
    let ((Expr::Column(column), Expr::OuterReferenceColumn(outer_field, outer))
    | (Expr::OuterReferenceColumn(outer_field, outer), Expr::Column(column))) =
        (left.as_ref(), right.as_ref())
    else {
        return None;
    };
    let outer_id = outer.id?;
    if schema.index_of_column_id(outer_id).is_some() {
        return None;
    }
    let field = schema.field(schema.index_of_column_id(column.id?)?);
    let same_values = field.data_type() == outer_field.data_type()
        && equal_values_are_identical(field.data_type())
        && (outer_field.is_nullable() || !field.is_nullable());
    same_values.then(|| {
        (
            outer_id,
            Expr::OuterReferenceColumn(Arc::clone(outer_field), column.clone()),
        )
    })
}

/// Whether two equal values of `data_type` are the same value, so that one
/// can stand for the other in any expression. Not for floating point
/// numbers, and not for nested or dictionary types, to be safe.
fn equal_values_are_identical(data_type: &DataType) -> bool {
    data_type.is_integer()
        || data_type.is_decimal()
        || matches!(
            data_type,
            DataType::Boolean
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::FixedSizeBinary(_)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
        )
}

/// `subquery` with its outer references to the ids of `substitutes` (in its
/// nodes, in the subqueries nested in it, and in the lists of outer
/// references of all of them) replaced
fn replace_outer_references(
    subquery: Subquery,
    substitutes: &HashMap<ColumnId, Expr>,
) -> Result<Transformed<Subquery>> {
    // the subquery as a node, so that its own list of outer references is
    // rewritten like the lists of the subqueries nested in it
    LogicalPlan::Subquery(subquery)
        .transform_up_with_subqueries(|node| match node {
            LogicalPlan::Subquery(subquery) => {
                replace_in_outer_ref_columns(subquery, substitutes)
                    .map(|t| t.update_data(LogicalPlan::Subquery))
            }
            node => node.map_expressions(|expr| {
                expr.transform(|expr| match &expr {
                    Expr::OuterReferenceColumn(_, column) => {
                        match column.id.and_then(|id| substitutes.get(&id)) {
                            Some(substitute) => Ok(Transformed::yes(substitute.clone())),
                            None => Ok(Transformed::no(expr)),
                        }
                    }
                    _ => Ok(Transformed::no(expr)),
                })
            }),
        })?
        .map_data(|plan| match plan {
            LogicalPlan::Subquery(subquery) => Ok(subquery),
            _ => unreachable!("a Subquery node is rewritten to a Subquery node"),
        })
}

/// Replaces the entries of the list of outer references of `subquery` that
/// read the ids of `substitutes`, without duplicates
fn replace_in_outer_ref_columns(
    mut subquery: Subquery,
    substitutes: &HashMap<ColumnId, Expr>,
) -> Result<Transformed<Subquery>> {
    let substitute = |expr: &Expr| match expr {
        Expr::OuterReferenceColumn(_, column) => {
            column.id.and_then(|id| substitutes.get(&id))
        }
        _ => None,
    };
    if !subquery
        .outer_ref_columns
        .iter()
        .any(|e| substitute(e).is_some())
    {
        return Ok(Transformed::no(subquery));
    }
    let mut outer_ref_columns: Vec<Expr> =
        Vec::with_capacity(subquery.outer_ref_columns.len());
    for expr in &subquery.outer_ref_columns {
        let expr = substitute(expr).unwrap_or(expr).clone();
        if !outer_ref_columns.contains(&expr) {
            outer_ref_columns.push(expr);
        }
    }
    subquery.outer_ref_columns = outer_ref_columns;
    Ok(Transformed::yes(subquery))
}

/// Flattens the subqueries that still read a column two levels up, in the
/// filters at the top of a middle subquery whose rows count only as a set
/// (an `EXISTS` or `IN`, negated or not, see [`map_subquery_expr`]).
///
/// A top-level conjunct `EXISTS (SELECT .. FROM r WHERE p)` (not negated) of
/// such a filter is TRUE for a row exactly when some row of `r` satisfies
/// `p` with it. The filter over its input `l` keeps the same rows as a filter
/// with `p` in place of the conjunct over the inner join of `l` and `r`,
/// except that a row of `l` appears once per matching row of `r`: the
/// middle subquery gets the same rows, some of them several times. Its
/// value (whether it has a row, and which values it has) is the same, as
/// long as nothing between the filter and the top of the middle subquery
/// counts the rows: only projections, filters and aliases are crossed, not
/// an aggregate, a `DISTINCT`, a window or a limit.
///
/// `v IN (SELECT e FROM r WHERE p)` as a top-level conjunct is TRUE exactly
/// when `EXISTS (SELECT .. FROM r WHERE p AND v = e)` is, so it is flattened
/// the same way. A negated `EXISTS` or `IN` is not: its value depends on all
/// the rows of `r`.
///
/// The outer references of `p` to the columns of `l` become columns of the
/// join, and the references two levels up stay outer references, now of the
/// middle subquery itself, which the decorrelation rules then pull up. `r`
/// must not read anything from outside itself (it is not in the scope of
/// `l` any more), and `p` and `e` must not be volatile.
///
/// The filter becomes a projection of the columns of `l`, by id, over the
/// filter over the join, so that the nodes above read the same columns.
fn flatten_nested_subqueries(
    plan: LogicalPlan,
    ids: &Arc<ColumnIdGenerator>,
) -> Result<Transformed<LogicalPlan>> {
    match plan {
        // the rows pass through these unchanged (the schema of the new input
        // is the schema of the old one)
        LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => {
            plan.map_children(|input| flatten_nested_subqueries(input, ids))
        }
        LogicalPlan::Filter(_) => plan
            .map_children(|input| flatten_nested_subqueries(input, ids))?
            .transform_data(|plan| flatten_filter(plan, ids)),
        plan => Ok(Transformed::no(plan)),
    }
}

/// Flattens the conjuncts of the filter `plan` that can be (see
/// [`flatten_nested_subqueries`])
fn flatten_filter(
    plan: LogicalPlan,
    ids: &Arc<ColumnIdGenerator>,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Filter(filter) = plan else {
        return Ok(Transformed::no(plan));
    };
    let input_schema = filter.input.schema();
    let mut joined = vec![];
    let mut conjuncts = vec![];
    for conjunct in split_conjunction(&filter.predicate) {
        match flatten_conjunct(conjunct, input_schema) {
            Some((plan, predicates)) => {
                joined.push(plan);
                conjuncts.extend(predicates);
            }
            None => conjuncts.push(conjunct.clone()),
        }
    }
    if joined.is_empty() {
        return Ok(Transformed::no(LogicalPlan::Filter(filter)));
    }
    let mut join = ids_builder(ids, Arc::clone(&filter.input));
    for plan in joined {
        join = join.cross_join(plan)?;
    }
    let join = join.build()?;
    // a flattened `EXISTS` without a filter leaves no conjunct
    let filtered = match conjunction(conjuncts) {
        Some(predicate) => {
            LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(join))?)
        }
        None => join,
    };
    // the columns of the filter's input, with their ids
    let columns = input_schema.columns().into_iter().map(Expr::from);
    ids_builder(ids, filtered)
        .project(columns)?
        .build()
        .map(Transformed::yes)
}

/// If `conjunct` (of a filter over `input_schema`) is an `EXISTS` or `IN`,
/// not negated, whose subquery reads a column two levels up (not a column
/// of `input_schema`), and can be flattened (see
/// [`flatten_nested_subqueries`]): the plan to join and the conjuncts that
/// replace `conjunct`
fn flatten_conjunct(
    conjunct: &Expr,
    input_schema: &DFSchema,
) -> Option<(LogicalPlan, Vec<Expr>)> {
    let (subquery, value) = match conjunct {
        Expr::Exists(Exists {
            subquery,
            negated: false,
        }) => (subquery.subquery.as_ref(), None),
        Expr::InSubquery(InSubquery {
            expr,
            subquery,
            negated: false,
        }) => (subquery.subquery.as_ref(), Some(expr.as_ref())),
        _ => return None,
    };
    let two_levels_up = subquery.all_out_ref_exprs().iter().any(|expr| {
        matches!(expr, Expr::OuterReferenceColumn(_, column)
            if column.id.is_some_and(|id| input_schema.index_of_column_id(id).is_none()))
    });
    if !two_levels_up {
        return None;
    }

    // `IN`: the value the subquery gives, `v = e`; `EXISTS`: no projection
    // matters
    let (mut plan, equality) = match value {
        Some(value) => {
            let LogicalPlan::Projection(projection) = subquery else {
                return None;
            };
            let [output] = projection.expr.as_slice() else {
                return None;
            };
            let output = output.expr.clone().unalias_nested().data;
            if output.is_volatile() || output.exists(|e| Ok(is_subquery_expr(e))).ok()? {
                return None;
            }
            (projection.input.as_ref(), Some(value.clone().eq(output)))
        }
        None => {
            let mut plan = subquery;
            while let LogicalPlan::Projection(projection) = plan {
                plan = projection.input.as_ref();
            }
            (plan, None)
        }
    };
    let mut predicates = vec![];
    if let LogicalPlan::Filter(filter) = plan {
        predicates = split_conjunction_owned(filter.predicate.clone());
        plan = filter.input.as_ref();
    }
    predicates.extend(equality);
    if predicates.iter().any(Expr::is_volatile) || reads_from_outside(plan) {
        return None;
    }

    // the outer references to the columns of the filter's input become
    // columns of the join (the expressions of nested subqueries are left:
    // their references to the filter's input stay outer references, one
    // level up, as before)
    let predicates = predicates
        .into_iter()
        .map(|predicate| {
            predicate
                .transform(|expr| match expr {
                    Expr::OuterReferenceColumn(_, column)
                        if column.id.is_some_and(|id| {
                            input_schema.index_of_column_id(id).is_some()
                        }) =>
                    {
                        Ok(Transformed::yes(Expr::Column(column)))
                    }
                    expr => Ok(Transformed::no(expr)),
                })
                .map(|t| t.data)
        })
        .collect::<Result<Vec<_>>>()
        .ok()?;
    Some((plan.clone(), predicates))
}

/// Whether `plan` or a subquery nested in it reads a column that `plan` does
/// not define (or that has no id, which is not known)
fn reads_from_outside(plan: &LogicalPlan) -> bool {
    let mut outer = vec![];
    // the closures never fail
    let _ = plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                if let Expr::OuterReferenceColumn(_, column) = expr {
                    outer.push(column.id);
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    });
    if outer.is_empty() {
        return false;
    }
    let defined = plan.defined_column_ids();
    outer
        .into_iter()
        .any(|id| id.is_none_or(|id| !defined.contains(&id)))
}

/// Whether `join_filters`, the correlated predicates pulled up from a
/// subquery (with their outer references), read a column that `outer`, the
/// plan the subquery is joined with, does not have: one two levels up from
/// the subquery, out of scope in the join filter.
///
/// The rewrites of this module remove such references where they can; the
/// decorrelation rules leave the subquery as it is where some remain (see
/// also [`reads_columns_of`]). References without an id are not
/// known, and do not count, as before ids.
pub(crate) fn join_filters_read_outside(join_filters: &[Expr], outer: &DFSchema) -> bool {
    // a schema whose fields have no ids is looked up by name (an empty one
    // has no column at all)
    let by_id =
        outer.fields().is_empty() || outer.field_ids().iter().any(Option::is_some);
    by_id
        && join_filters.iter().any(|filter| {
            filter
                .exists(|expr| {
                    Ok(matches!(expr, Expr::OuterReferenceColumn(_, column)
                        if column.id.is_some_and(|id| outer.index_of_column_id(id).is_none())))
                })
                .unwrap_or(false)
        })
}

/// Whether `decorrelated`, a subquery after its correlated predicates were
/// pulled up, still reads a column of `outer`, the plan it is joined with,
/// as an outer reference: in its own nodes (an outer reference the pull up
/// does not move, such as in a projection) or in a subquery nested in it (a
/// reference two levels up from that one). Joined, `decorrelated` is an
/// input of the join, not a subquery of `outer`, and that column is not in
/// scope in it any more. An outer reference to a column further out (when
/// `outer` is itself in a subquery) stays in scope.
///
/// References without an id are not known, and do not count, as before ids.
pub(crate) fn reads_columns_of(decorrelated: &LogicalPlan, outer: &DFSchema) -> bool {
    if !outer.field_ids().iter().any(Option::is_some) {
        return false;
    }
    let mut found = false;
    // the closures never fail
    let _ = decorrelated.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                found |= matches!(expr, Expr::OuterReferenceColumn(_, column)
                    if column.id.is_some_and(|id| outer.index_of_column_id(id).is_some()));
                Ok(if found {
                    TreeNodeRecursion::Stop
                } else {
                    TreeNodeRecursion::Continue
                })
            })
        })?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    found
}
