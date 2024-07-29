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
use sqlparser::ast::Ident;

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
                // inner expr may have different type to outer expr: e.g. a + 1 is a column of
                // string in outer, but a expr of math in inner
                map.insert(Expr::Column(f.to_string().into()), f.clone());
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

    // inner expr may have different type to outer expr: e.g. a + 1 is a column of
    // string in outer, but a expr of math in inner
    let outer_collects = collects.iter().map(Expr::to_string).collect::<HashSet<_>>();
    let inner_collects = inner_exprs
        .iter()
        .map(Expr::to_string)
        .collect::<HashSet<_>>();

    if outer_collects == inner_collects {
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

// This logic is to work out the columns and inner query for SubqueryAlias plan for both types of
// subquery
// - `(SELECT column_a as a from table) AS A`
// - `(SELECT column_a from table) AS A (a)`
//
// A roundtrip example for table alias with columns
//
// query: SELECT id FROM (SELECT j1_id from j1) AS c (id)
//
// LogicPlan:
// Projection: c.id
//   SubqueryAlias: c
//     Projection: j1.j1_id AS id
//       Projection: j1.j1_id
//         TableScan: j1
//
// Before introducing this logic, the unparsed query would be `SELECT c.id FROM (SELECT j1.j1_id AS
// id FROM (SELECT j1.j1_id FROM j1)) AS c`.
// The query is invalid as `j1.j1_id` is not a valid identifier in the derived table
// `(SELECT j1.j1_id FROM j1)`
//
// With this logic, the unparsed query will be:
// `SELECT c.id FROM (SELECT j1.j1_id FROM j1) AS c (id)`
//
// Caveat: this won't handle the case like `select * from (select 1, 2) AS a (b, c)`
// as the parser gives a wrong plan which has mismatch `Int(1)` types: Literal and
// Column in the Projections. Once the parser side is fixed, this logic should work
pub(super) fn subquery_alias_inner_query_and_columns(
    subquery_alias: &datafusion_expr::SubqueryAlias,
) -> (&LogicalPlan, Vec<Ident>) {
    let plan: &LogicalPlan = subquery_alias.input.as_ref();

    let LogicalPlan::Projection(outer_projections) = plan else {
        return (plan, vec![]);
    };

    // check if it's projection inside projection
    let Some(inner_projection) = find_projection(outer_projections.input.as_ref()) else {
        return (plan, vec![]);
    };

    let mut columns: Vec<Ident> = vec![];
    // check if the inner projection and outer projection have a matching pattern like
    //     Projection: j1.j1_id AS id
    //       Projection: j1.j1_id
    for (i, inner_expr) in inner_projection.expr.iter().enumerate() {
        let Expr::Alias(ref outer_alias) = &outer_projections.expr[i] else {
            return (plan, vec![]);
        };

        let expr = outer_alias.expr.clone();

        // inner expr may have different type to outer expr: e.g. a + 1 is a column of
        // string in outer, but a expr of math in inner
        if expr.to_string() != inner_expr.to_string() {
            return (plan, vec![]);
        };

        columns.push(outer_alias.name.as_str().into());
    }

    (outer_projections.input.as_ref(), columns)
}

fn find_projection(logical_plan: &LogicalPlan) -> Option<&Projection> {
    match logical_plan {
        LogicalPlan::Projection(p) => {
            return Some(p);
        }
        LogicalPlan::Limit(p) => find_projection(p.input.as_ref()),
        LogicalPlan::Distinct(p) => find_projection(p.input().as_ref()),
        LogicalPlan::Sort(p) => find_projection(p.input.as_ref()),

        _ => None,
    }
}
