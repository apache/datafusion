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

use datafusion_common::{
    internal_err,
    tree_node::{Transformed, TreeNode},
    Result,
};
use datafusion_expr::{Aggregate, Expr, LogicalPlan};

/// Recursively searches children of [LogicalPlan] to find an Aggregate node if one exists
/// prior to encountering a Join, TableScan, or a nested subquery (derived table factor).
/// If an Aggregate node is not found prior to this or at all before reaching the end
/// of the tree, None is returned.
pub(crate) fn find_agg_node_within_select(
    plan: &LogicalPlan,
    already_projected: bool,
) -> Option<&Aggregate> {
    // Note that none of the nodes that have a corresponding agg node can have more
    // than 1 input node. E.g. Projection / Filter always have 1 input node.
    let input = plan.inputs();
    let input = if input.len() > 1 {
        return None;
    } else {
        input.first()?
    };
    if let LogicalPlan::Aggregate(agg) = input {
        Some(agg)
    } else if let LogicalPlan::TableScan(_) = input {
        None
    } else if let LogicalPlan::Projection(_) = input {
        if already_projected {
            None
        } else {
            find_agg_node_within_select(input, true)
        }
    } else {
        find_agg_node_within_select(input, already_projected)
    }
}

/// Recursively identify all Column expressions and transform them into the appropriate
/// aggregate expression contained in agg.
///
/// For example, if expr contains the column expr "COUNT(*)" it will be transformed
/// into an actual aggregate expression COUNT(*) as identified in the aggregate node.
pub(crate) fn unproject_agg_exprs(expr: &Expr, agg: &Aggregate) -> Result<Expr> {
    expr.clone()
        .transform(|sub_expr| {
            if let Expr::Column(c) = sub_expr {
                // find the column in the agg schmea
                if let Ok(n) = agg.schema.index_of_column(&c) {
                    let unprojected_expr = agg
                        .group_expr
                        .iter()
                        .chain(agg.aggr_expr.iter())
                        .nth(n)
                        .unwrap();
                    Ok(Transformed::yes(unprojected_expr.clone()))
                } else {
                    internal_err!(
                        "Tried to unproject agg expr not found in provided Aggregate!"
                    )
                }
            } else {
                Ok(Transformed::no(sub_expr))
            }
        })
        .map(|e| e.data)
}
