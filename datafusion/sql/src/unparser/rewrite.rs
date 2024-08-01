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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use datafusion_common::{
    tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeIterator},
    Result,
};
use datafusion_expr::{Expr, LogicalPlan, Projection, Sort};

/// Normalize the schema of a union plan to remove qualifiers from the schema fields and sort expressions.
///
/// DataFusion will return an error if two columns in the schema have the same name with no table qualifiers.
/// There are certain types of UNION queries that can result in having two columns with the same name, and the
/// solution was to add table qualifiers to the schema fields.
/// See <https://github.com/apache/datafusion/issues/5410> for more context on this decision.
///
/// However, this causes a problem when unparsing these queries back to SQL - as the table qualifier has
/// logically been erased and is no longer a valid reference.
///
/// The following input SQL:
/// ```sql
/// SELECT table1.foo FROM table1
/// UNION ALL
/// SELECT table2.foo FROM table2
/// ORDER BY foo
/// ```
///
/// Would be unparsed into the following invalid SQL without this transformation:
/// ```sql
/// SELECT table1.foo FROM table1
/// UNION ALL
/// SELECT table2.foo FROM table2
/// ORDER BY table1.foo
/// ```
///
/// Which would result in a SQL error, as `table1.foo` is not a valid reference in the context of the UNION.
pub(super) fn normalize_union_schema(plan: &LogicalPlan) -> Result<LogicalPlan> {
    let plan = plan.clone();

    let transformed_plan = plan.transform_up(|plan| match plan {
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
            // Only rewrite Sort expressions that have a UNION as their input
            if !matches!(&*sort.input, LogicalPlan::Union(_)) {
                return Ok(Transformed::no(LogicalPlan::Sort(sort)));
            }

            Ok(Transformed::yes(LogicalPlan::Sort(Sort {
                expr: rewrite_sort_expr_for_union(sort.expr)?,
                input: sort.input,
                fetch: sort.fetch,
            })))
        }
        _ => Ok(Transformed::no(plan)),
    });
    transformed_plan.data()
}

/// Rewrite sort expressions that have a UNION plan as their input to remove the table reference.
fn rewrite_sort_expr_for_union(exprs: Vec<Expr>) -> Result<Vec<Expr>> {
    let sort_exprs: Vec<Expr> = exprs
        .into_iter()
        .map_until_stop_and_collect(|expr| {
            expr.transform_up(|expr| {
                if let Expr::Column(mut col) = expr {
                    col.relation = None;
                    Ok(Transformed::yes(Expr::Column(col)))
                } else {
                    Ok(Transformed::no(expr))
                }
            })
        })
        .data()?;

    Ok(sort_exprs)
}

// Rewrite logic plan for query that order by columns are not in projections
// Plan before rewrite:
//
// Projection: j1.j1_string, j2.j2_string
//   Sort: j1.j1_id DESC NULLS FIRST, j2.j2_id DESC NULLS FIRST
//     Projection: j1.j1_string, j2.j2_string, j1.j1_id, j2.j2_id
//       Inner Join:  Filter: j1.j1_id = j2.j2_id
//         TableScan: j1
//         TableScan: j2
//
// Plan after rewrite
//
// Sort: j1.j1_id DESC NULLS FIRST, j2.j2_id DESC NULLS FIRST
//   Projection: j1.j1_string, j2.j2_string
//     Inner Join:  Filter: j1.j1_id = j2.j2_id
//       TableScan: j1
//       TableScan: j2
//
// This prevents the original plan generate query with derived table but missing alias.
pub(super) fn rewrite_plan_for_sort_on_non_projected_fields(
    p: &Projection,
) -> Option<LogicalPlan> {
    let LogicalPlan::Sort(sort) = p.input.as_ref() else {
        return None;
    };

    let LogicalPlan::Projection(inner_p) = sort.input.as_ref() else {
        return None;
    };

    let mut map = HashMap::new();
    let inner_exprs = inner_p
        .expr
        .iter()
        .map(|f| {
            if let Expr::Alias(alias) = f {
                let a = Expr::Column(alias.name.clone().into());
                map.insert(a.clone(), f.clone());
                a
            } else {
                f.clone()
            }
        })
        .collect::<Vec<_>>();

    let mut collects = p.expr.clone();
    for expr in &sort.expr {
        if let Expr::Sort(s) = expr {
            collects.push(s.expr.as_ref().clone());
        }
    }

    if collects.iter().collect::<HashSet<_>>()
        == inner_exprs.iter().collect::<HashSet<_>>()
    {
        let mut sort = sort.clone();
        let mut inner_p = inner_p.clone();

        let new_exprs = p
            .expr
            .iter()
            .map(|e| map.get(e).unwrap_or(e).clone())
            .collect::<Vec<_>>();

        inner_p.expr.clone_from(&new_exprs);
        sort.input = Arc::new(LogicalPlan::Projection(inner_p));

        Some(LogicalPlan::Sort(sort))
    } else {
        None
    }
}
