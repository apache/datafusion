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

//! This module implements a rule that simplifies expressions that is guaranteed to be true or false at planning time

use std::collections::HashSet;

use datafusion_common::tree_node::TreeNodeRewriter;
use datafusion_common::Result;
use datafusion_expr::expr::InList;
use datafusion_expr::{lit, BinaryExpr, Expr, Operator};

/// Simplify expressions that is guaranteed to be true or false to a literal boolean expression
///
/// Rules:
/// 1. `a in (1,2,3) AND a in (4,5) -> Intersection`
// 2. `a in (1,2,3) OR a in (1,2,3) -> a in (1,2,3)`
pub(super) struct InListSimplifier {}

impl InListSimplifier {
    pub(super) fn new() -> Self {
        Self {}
    }
}

impl TreeNodeRewriter for InListSimplifier {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = &expr {
            if let (Expr::InList(l1), Operator::And, Expr::InList(l2)) =
                (left.as_ref(), op, right.as_ref())
            {
                if l1.expr == l2.expr && !l1.negated && !l2.negated {
                    let l1_set: HashSet<Expr> = l1.list.iter().cloned().collect();
                    let intersect_list: Vec<Expr> = l2
                        .list
                        .iter()
                        .filter(|x| l1_set.contains(x))
                        .cloned()
                        .collect();
                    if intersect_list.is_empty() {
                        return Ok(lit(false));
                    }
                    let merged_inlist = InList {
                        expr: l1.expr.clone(),
                        list: intersect_list,
                        negated: false,
                    };
                    return Ok(Expr::InList(merged_inlist));
                } else if l1.expr == l2.expr && l1.negated && l2.negated {
                    let l1_set: HashSet<Expr> = l1.list.iter().cloned().collect();
                    let intersect_list: Vec<Expr> = l2
                        .list
                        .iter()
                        .filter(|x| l1_set.contains(x))
                        .cloned()
                        .collect();
                    if intersect_list.is_empty() {
                        return Ok(lit(true));
                    }
                    let merged_inlist = InList {
                        expr: l1.expr.clone(),
                        list: intersect_list,
                        negated: true,
                    };
                    return Ok(Expr::InList(merged_inlist));
                }
            }
        }

        Ok(expr)
    }
}
