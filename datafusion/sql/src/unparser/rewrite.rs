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

use std::sync::Arc;

use datafusion_common::{
    tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRewriter},
    Result,
};
use datafusion_expr::{Expr, LogicalPlan, Sort};

/// Normalize the schema of a union plan to remove qualifiers from the schema fields.
pub(super) struct NormalizeUnionSchema {}

impl NormalizeUnionSchema {
    pub fn new() -> Self {
        Self {}
    }
}

impl TreeNodeRewriter for NormalizeUnionSchema {
    type Node = LogicalPlan;

    /// Invoked while traversing down the tree before any children are rewritten.
    /// Default implementation returns the node as is and continues recursion.
    fn f_down(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Union(mut union) => {
                let schema = match Arc::try_unwrap(union.schema) {
                    Ok(inner) => inner,
                    Err(schema) => (*schema).clone(),
                };
                let schema = schema.strip_qualifiers();

                union.schema = Arc::new(schema);
                Ok(Transformed::yes(LogicalPlan::Union(union)))
            }
            LogicalPlan::Sort(sort) => {
                if !matches!(&*sort.input, LogicalPlan::Union(_)) {
                    return Ok(Transformed::no(LogicalPlan::Sort(sort)));
                }

                let mut sort_expr_rewriter = NormalizeSortExprForUnion::new();
                let sort_exprs: Vec<Expr> = sort
                    .expr
                    .into_iter()
                    .map(|expr| expr.rewrite(&mut sort_expr_rewriter).data())
                    .collect::<Result<Vec<_>>>()?;

                Ok(Transformed::yes(LogicalPlan::Sort(Sort {
                    expr: sort_exprs,
                    input: sort.input,
                    fetch: sort.fetch,
                })))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

/// Normalize the schema of sort expressions that follow a Union to remove any column relations.
struct NormalizeSortExprForUnion {}

impl NormalizeSortExprForUnion {
    pub fn new() -> Self {
        Self {}
    }
}

impl TreeNodeRewriter for NormalizeSortExprForUnion {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match expr {
            Expr::Column(mut col) => {
                col.relation = None;
                Ok(Transformed::yes(Expr::Column(col)))
            }
            _ => Ok(Transformed::no(expr)),
        }
    }
}
