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
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Expr> {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = &expr {
            if let (Expr::InList(l1), Operator::And, Expr::InList(l2)) =
                (left.as_ref(), op, right.as_ref())
            {
                if l1.expr == l2.expr && !l1.negated && !l2.negated {
                    return inlist_intersection(l1, l2, false);
                } else if l1.expr == l2.expr && l1.negated && l2.negated {
                    return inlist_union(l1, l2, true);
                } else if l1.expr == l2.expr && !l1.negated && l2.negated {
                    return inlist_except(l1, l2);
                } else if l1.expr == l2.expr && l1.negated && !l2.negated {
                    return inlist_except(l2, l1);
                }
            } else if let (Expr::InList(l1), Operator::Or, Expr::InList(l2)) =
                (left.as_ref(), op, right.as_ref())
            {
                if l1.expr == l2.expr && l1.negated && l2.negated {
                    return inlist_intersection(l1, l2, true);
                }
            }
        }

        Ok(expr)
    }
}

fn inlist_union(l1: &InList, l2: &InList, negated: bool) -> Result<Expr> {
    let mut seen: HashSet<Expr> = HashSet::new();
    let list = l1
        .list
        .iter()
        .chain(l2.list.iter())
        .filter(|&e| seen.insert(e.to_owned()))
        .cloned()
        .collect::<Vec<_>>();
    let merged_inlist = InList {
        expr: l1.expr.clone(),
        list,
        negated,
    };
    Ok(Expr::InList(merged_inlist))
}

fn inlist_intersection(l1: &InList, l2: &InList, negated: bool) -> Result<Expr> {
    let l1_set: HashSet<Expr> = l1.list.iter().cloned().collect();
    let intersect_list: Vec<Expr> = l2
        .list
        .iter()
        .filter(|x| l1_set.contains(x))
        .cloned()
        .collect();
    // e in () is always false
    // e not in () is always true
    if intersect_list.is_empty() {
        return Ok(lit(negated));
    }
    let merged_inlist = InList {
        expr: l1.expr.clone(),
        list: intersect_list,
        negated,
    };
    Ok(Expr::InList(merged_inlist))
}

fn inlist_except(l1: &InList, l2: &InList) -> Result<Expr> {
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
