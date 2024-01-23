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

use std::collections::HashSet;

use datafusion_common::tree_node::TreeNodeRewriter;
use datafusion_common::Result;
use datafusion_expr::expr::InList;
use datafusion_expr::{lit, BinaryExpr, Expr, Operator};

/// Simplify expressions that is guaranteed to be true or false to a literal boolean expression
///
/// Rules:
/// If both expressions are `IN` or `NOT IN`, then we can apply intersection or union on both lists
///   Intersection:
///     1. `a in (1,2,3) AND a in (4,5) -> a in (), which is false`
///     2. `a in (1,2,3) AND a in (2,3,4) -> a in (2,3)`
///     3. `a not in (1,2,3) OR a not in (3,4,5,6) -> a not in (3)`
///   Union:
///     4. `a not int (1,2,3) AND a not in (4,5,6) -> a not in (1,2,3,4,5,6)`
///     # This rule is handled by `or_in_list_simplifier.rs`
///     5. `a in (1,2,3) OR a in (4,5,6) -> a in (1,2,3,4,5,6)`
/// If one of the expressions is `IN` and another one is `NOT IN`, then we apply exception on `In` expression
///     6. `a in (1,2,3,4) AND a not in (1,2,3,4,5) -> a in (), which is false`
///     7. `a not in (1,2,3,4) AND a in (1,2,3,4,5) -> a = 5`
///     8. `a in (1,2,3,4) AND a not in (5,6,7,8) -> a in (1,2,3,4)`
pub(super) struct InListSimplifier {}

impl InListSimplifier {
    pub(super) fn new() -> Self {
        Self {}
    }
}

impl TreeNodeRewriter for InListSimplifier {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr {
            match (*left, op, *right) {
                (Expr::InList(l1), Operator::And, Expr::InList(l2)) if l1.expr == l2.expr && !l1.negated && !l2.negated =>
                    inlist_intersection(l1, l2, false),
                (Expr::InList(l1), Operator::And, Expr::InList(l2)) if l1.expr == l2.expr && l1.negated && l2.negated =>
                    inlist_union(l1, l2, true),
                (Expr::InList(l1), Operator::And, Expr::InList(l2)) if l1.expr == l2.expr && !l1.negated && l2.negated =>
                    inlist_except(l1, l2),
                (Expr::InList(l1), Operator::And, Expr::InList(l2)) if l1.expr == l2.expr && l1.negated && !l2.negated =>
                    inlist_except(l2, l1),
                (Expr::InList(l1), Operator::Or, Expr::InList(l2))  if l1.expr == l2.expr && l1.negated && l2.negated  =>
                    inlist_intersection(l1, l2, true),
               (left, op, right) => {
                   // put the expression back together
                   Ok(Expr::BinaryExpr(BinaryExpr {
                       left: Box::new(left),
                       op,
                       right: Box::new(right),
                   }))
               }
            }
        } else {
            Ok(expr)
        }
    }
}

/// Return the union of two inlist expressions
/// maintaining the order of the elements in the two lists
fn inlist_union(mut l1: InList, l2: InList, negated: bool) -> Result<Expr> {
    // extend the list in l1 with the elements in l2 that are not already in l1
    let l1_items: HashSet<_> = l1.list.iter().collect();

    // keep all l2 items that do not also appear in l1
    let keep_l2: Vec<_> = l2.list
        .into_iter()
        .filter_map(|e| {
            if l1_items.contains(&e) {
                None
            } else {
                Some(e)
            }
        }).collect();

    l1.list.extend(keep_l2.into_iter());
    l1.negated = negated;
    Ok(Expr::InList(l1))
}

/// Return the union of two inlist expressions
/// maintaining the order of the elements in the two lists
fn inlist_intersection(mut l1: InList, l2: InList, negated: bool) -> Result<Expr> {
    let l2_items = l2.list.iter().collect::<HashSet<_>>();

    // remove all items from l1 that are not in l2
    l1.list = l1.list.into_iter().filter(|e| l2_items.contains(e)).collect();

    // e in () is always false
    // e not in () is always true
    if l1.list.is_empty() {
        return Ok(lit(negated));
    }
    Ok(Expr::InList(l1))
}

fn inlist_except(l1: InList, l2: InList) -> Result<Expr> {
    let l2_set: HashSet<Expr> = l2.list.iter().cloned().collect();
    let except_list: Vec<Expr> = l1
        .list
        .iter()
        .filter(|x| !l2_set.contains(x))
        .cloned()
        .collect();
    if except_list.is_empty() {
        return Ok(lit(false));
    }
    let merged_inlist = InList {
        expr: l1.expr.clone(),
        list: except_list,
        negated: false,
    };
    Ok(Expr::InList(merged_inlist))
}
