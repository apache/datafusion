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

//! This module implements a rule that simplifies OR expressions into IN list expressions

use std::borrow::Cow;
use std::collections::HashSet;

use datafusion_common::tree_node::TreeNodeRewriter;
use datafusion_common::Result;
use datafusion_expr::expr::InList;
use datafusion_expr::{BinaryExpr, Expr, Operator};

/// Combine multiple OR expressions into a single IN list expression if possible
///
/// i.e. `a = 1 OR a = 2 OR a = 3` -> `a IN (1, 2, 3)`
pub(super) struct OrInListSimplifier {}

impl OrInListSimplifier {
    pub(super) fn new() -> Self {
        Self {}
    }
}

impl TreeNodeRewriter for OrInListSimplifier {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
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
                        let list_set = lhs
                            .list
                            .into_iter()
                            .chain(rhs.list.into_iter())
                            .filter(|e| seen.insert(e.to_owned()))
                            .collect::<Vec<_>>();

                        let merged_inlist = InList {
                            expr: lhs.expr,
                            list: list_set,
                            negated: false,
                        };
                        return Ok(Expr::InList(merged_inlist));
                    }
                }
            }
        }

        Ok(expr)
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
