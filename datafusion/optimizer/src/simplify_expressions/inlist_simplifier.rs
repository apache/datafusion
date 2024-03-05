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

//! This module implements a rule that simplifies the values for `InList`s

use super::utils::{is_null, lit_bool_null};
use super::THRESHOLD_INLINE_INLIST;

use std::borrow::Cow;
use std::collections::HashSet;

use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::expr::{InList, InSubquery};
use datafusion_expr::{lit, BinaryExpr, Expr, Operator};

pub(super) struct ShortenInListSimplifier {}

impl ShortenInListSimplifier {
    pub(super) fn new() -> Self {
        Self {}
    }
}

impl TreeNodeRewriter for ShortenInListSimplifier {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        // if expr is a single column reference:
        // expr IN (A, B, ...) --> (expr = A) OR (expr = B) OR (expr = C)
        if let Expr::InList(InList {
            expr,
            list,
            negated,
        }) = expr.clone()
        {
            if !list.is_empty()
                && (
                    // For lists with only 1 value we allow more complex expressions to be simplified
                    // e.g SUBSTR(c1, 2, 3) IN ('1') -> SUBSTR(c1, 2, 3) = '1'
                    // for more than one we avoid repeating this potentially expensive
                    // expressions
                    list.len() == 1
                        || list.len() <= THRESHOLD_INLINE_INLIST
                            && expr.try_into_col().is_ok()
                )
            {
                let first_val = list[0].clone();
                if negated {
                    return Ok(Transformed::yes(list.into_iter().skip(1).fold(
                        (*expr.clone()).not_eq(first_val),
                        |acc, y| {
                            // Note that `A and B and C and D` is a left-deep tree structure
                            // as such we want to maintain this structure as much as possible
                            // to avoid reordering the expression during each optimization
                            // pass.
                            //
                            // Left-deep tree structure for `A and B and C and D`:
                            // ```
                            //        &
                            //       / \
                            //      &   D
                            //     / \
                            //    &   C
                            //   / \
                            //  A   B
                            // ```
                            //
                            // The code below maintain the left-deep tree structure.
                            acc.and((*expr.clone()).not_eq(y))
                        },
                    )));
                } else {
                    return Ok(Transformed::yes(list.into_iter().skip(1).fold(
                        (*expr.clone()).eq(first_val),
                        |acc, y| {
                            // Same reasoning as above
                            acc.or((*expr.clone()).eq(y))
                        },
                    )));
                }
            }
        }

        Ok(Transformed::no(expr))
    }
}

pub(super) struct InListSimplifier {}

impl InListSimplifier {
    pub(super) fn new() -> Self {
        Self {}
    }
}

impl TreeNodeRewriter for InListSimplifier {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if let Expr::InList(InList {
            expr,
            mut list,
            negated,
        }) = expr.clone()
        {
            // expr IN () --> false
            // expr NOT IN () --> true
            if list.is_empty() && *expr != Expr::Literal(ScalarValue::Null) {
                return Ok(Transformed::yes(lit(negated)));
            // null in (x, y, z) --> null
            // null not in (x, y, z) --> null
            } else if is_null(&expr) {
                return Ok(Transformed::yes(lit_bool_null()));
            // expr IN ((subquery)) -> expr IN (subquery), see ##5529
            } else if list.len() == 1
                && matches!(list.first(), Some(Expr::ScalarSubquery { .. }))
            {
                let Expr::ScalarSubquery(subquery) = list.remove(0) else {
                    unreachable!()
                };
                return Ok(Transformed::yes(Expr::InSubquery(InSubquery::new(
                    expr, subquery, negated,
                ))));
            }
        }
        // Combine multiple OR expressions into a single IN list expression if possible
        //
        // i.e. `a = 1 OR a = 2 OR a = 3` -> `a IN (1, 2, 3)`
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = &expr {
            if *op == Operator::Or {
                let left = as_inlist(left);
                let right = as_inlist(right);
                if let (Some(lhs), Some(rhs)) = (left, right) {
                    if lhs.expr.try_into_col().is_ok()
                        && rhs.expr.try_into_col().is_ok()
                        && lhs.expr == rhs.expr
                        && !lhs.negated
                        && !rhs.negated
                    {
                        let lhs = lhs.into_owned();
                        let rhs = rhs.into_owned();
                        let mut seen: HashSet<Expr> = HashSet::new();
                        let list = lhs
                            .list
                            .into_iter()
                            .chain(rhs.list)
                            .filter(|e| seen.insert(e.to_owned()))
                            .collect::<Vec<_>>();

                        let merged_inlist = InList {
                            expr: lhs.expr,
                            list,
                            negated: false,
                        };
                        return Ok(Transformed::yes(Expr::InList(merged_inlist)));
                    }
                }
            }
        }
        // Simplify expressions that is guaranteed to be true or false to a literal boolean expression
        //
        // Rules:
        // If both expressions are `IN` or `NOT IN`, then we can apply intersection or union on both lists
        //   Intersection:
        //     1. `a in (1,2,3) AND a in (4,5) -> a in (), which is false`
        //     2. `a in (1,2,3) AND a in (2,3,4) -> a in (2,3)`
        //     3. `a not in (1,2,3) OR a not in (3,4,5,6) -> a not in (3)`
        //   Union:
        //     4. `a not int (1,2,3) AND a not in (4,5,6) -> a not in (1,2,3,4,5,6)`
        //     # This rule is handled by `or_in_list_simplifier.rs`
        //     5. `a in (1,2,3) OR a in (4,5,6) -> a in (1,2,3,4,5,6)`
        // If one of the expressions is `IN` and another one is `NOT IN`, then we apply exception on `In` expression
        //     6. `a in (1,2,3,4) AND a not in (1,2,3,4,5) -> a in (), which is false`
        //     7. `a not in (1,2,3,4) AND a in (1,2,3,4,5) -> a = 5`
        //     8. `a in (1,2,3,4) AND a not in (5,6,7,8) -> a in (1,2,3,4)`
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr.clone() {
            match (*left, op, *right) {
                (Expr::InList(l1), Operator::And, Expr::InList(l2))
                    if l1.expr == l2.expr && !l1.negated && !l2.negated =>
                {
                    return inlist_intersection(l1, l2, false).map(Transformed::yes);
                }
                (Expr::InList(l1), Operator::And, Expr::InList(l2))
                    if l1.expr == l2.expr && l1.negated && l2.negated =>
                {
                    return inlist_union(l1, l2, true).map(Transformed::yes);
                }
                (Expr::InList(l1), Operator::And, Expr::InList(l2))
                    if l1.expr == l2.expr && !l1.negated && l2.negated =>
                {
                    return inlist_except(l1, l2).map(Transformed::yes);
                }
                (Expr::InList(l1), Operator::And, Expr::InList(l2))
                    if l1.expr == l2.expr && l1.negated && !l2.negated =>
                {
                    return inlist_except(l2, l1).map(Transformed::yes);
                }
                (Expr::InList(l1), Operator::Or, Expr::InList(l2))
                    if l1.expr == l2.expr && l1.negated && l2.negated =>
                {
                    return inlist_intersection(l1, l2, true).map(Transformed::yes);
                }
                (left, op, right) => {
                    // put the expression back together
                    return Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    })));
                }
            }
        }

        Ok(Transformed::no(expr))
    }
}

/// Try to convert an expression to an in-list expression
fn as_inlist(expr: &Expr) -> Option<Cow<InList>> {
    match expr {
        Expr::InList(inlist) => Some(Cow::Borrowed(inlist)),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(_), Expr::Literal(_)) => Some(Cow::Owned(InList {
                    expr: left.clone(),
                    list: vec![*right.clone()],
                    negated: false,
                })),
                (Expr::Literal(_), Expr::Column(_)) => Some(Cow::Owned(InList {
                    expr: right.clone(),
                    list: vec![*left.clone()],
                    negated: false,
                })),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Return the union of two inlist expressions
/// maintaining the order of the elements in the two lists
fn inlist_union(mut l1: InList, l2: InList, negated: bool) -> Result<Expr> {
    // extend the list in l1 with the elements in l2 that are not already in l1
    let l1_items: HashSet<_> = l1.list.iter().collect();

    // keep all l2 items that do not also appear in l1
    let keep_l2: Vec<_> = l2
        .list
        .into_iter()
        .filter_map(|e| if l1_items.contains(&e) { None } else { Some(e) })
        .collect();

    l1.list.extend(keep_l2);
    l1.negated = negated;
    Ok(Expr::InList(l1))
}

/// Return the intersection of two inlist expressions
/// maintaining the order of the elements in the two lists
fn inlist_intersection(mut l1: InList, l2: InList, negated: bool) -> Result<Expr> {
    let l2_items = l2.list.iter().collect::<HashSet<_>>();

    // remove all items from l1 that are not in l2
    l1.list.retain(|e| l2_items.contains(e));

    // e in () is always false
    // e not in () is always true
    if l1.list.is_empty() {
        return Ok(lit(negated));
    }
    Ok(Expr::InList(l1))
}

/// Return the all items in l1 that are not in l2
/// maintaining the order of the elements in the two lists
fn inlist_except(mut l1: InList, l2: InList) -> Result<Expr> {
    let l2_items = l2.list.iter().collect::<HashSet<_>>();

    // keep only items from l1 that are not in l2
    l1.list.retain(|e| !l2_items.contains(e));

    if l1.list.is_empty() {
        return Ok(lit(false));
    }
    Ok(Expr::InList(l1))
}
