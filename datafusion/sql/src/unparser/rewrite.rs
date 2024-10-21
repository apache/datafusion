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

use arrow_schema::Schema;
use datafusion_common::{
    tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRewriter},
    Column, Result, TableReference,
};
use datafusion_expr::{expr::Alias, tree_node::transform_sort_vec};
use datafusion_expr::{Expr, LogicalPlan, Projection, Sort, SortExpr};
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
            let schema = Arc::unwrap_or_clone(union.schema);
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
fn rewrite_sort_expr_for_union(exprs: Vec<SortExpr>) -> Result<Vec<SortExpr>> {
    let sort_exprs = transform_sort_vec(exprs, &mut |expr| {
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

/// Rewrite logic plan for query that order by columns are not in projections
/// Plan before rewrite:
///
/// Projection: j1.j1_string, j2.j2_string
///   Sort: j1.j1_id DESC NULLS FIRST, j2.j2_id DESC NULLS FIRST
///     Projection: j1.j1_string, j2.j2_string, j1.j1_id, j2.j2_id
///       Inner Join:  Filter: j1.j1_id = j2.j2_id
///         TableScan: j1
///         TableScan: j2
///
/// Plan after rewrite
///
/// Sort: j1.j1_id DESC NULLS FIRST, j2.j2_id DESC NULLS FIRST
///   Projection: j1.j1_string, j2.j2_string
///     Inner Join:  Filter: j1.j1_id = j2.j2_id
///       TableScan: j1
///       TableScan: j2
///
/// This prevents the original plan generate query with derived table but missing alias.
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
        .enumerate()
        .map(|(i, f)| match f {
            Expr::Alias(alias) => {
                let a = Expr::Column(alias.name.clone().into());
                map.insert(a.clone(), f.clone());
                a
            }
            Expr::Column(_) => {
                map.insert(
                    Expr::Column(inner_p.schema.field(i).name().into()),
                    f.clone(),
                );
                f.clone()
            }
            _ => {
                let a = Expr::Column(inner_p.schema.field(i).name().into());
                map.insert(a.clone(), f.clone());
                a
            }
        })
        .collect::<Vec<_>>();

    let mut collects = p.expr.clone();
    for sort in &sort.expr {
        collects.push(sort.expr.clone());
    }

    // Compare outer collects Expr::to_string with inner collected transformed values
    // alias -> alias column
    // column -> remain
    // others, extract schema field name
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

/// This logic is to work out the columns and inner query for SubqueryAlias plan for both types of
/// subquery
/// - `(SELECT column_a as a from table) AS A`
/// - `(SELECT column_a from table) AS A (a)`
///
/// A roundtrip example for table alias with columns
///
/// query: SELECT id FROM (SELECT j1_id from j1) AS c (id)
///
/// LogicPlan:
/// Projection: c.id
///   SubqueryAlias: c
///     Projection: j1.j1_id AS id
///       Projection: j1.j1_id
///         TableScan: j1
///
/// Before introducing this logic, the unparsed query would be `SELECT c.id FROM (SELECT j1.j1_id AS
/// id FROM (SELECT j1.j1_id FROM j1)) AS c`.
/// The query is invalid as `j1.j1_id` is not a valid identifier in the derived table
/// `(SELECT j1.j1_id FROM j1)`
///
/// With this logic, the unparsed query will be:
/// `SELECT c.id FROM (SELECT j1.j1_id FROM j1) AS c (id)`
///
/// Caveat: this won't handle the case like `select * from (select 1, 2) AS a (b, c)`
/// as the parser gives a wrong plan which has mismatch `Int(1)` types: Literal and
/// Column in the Projections. Once the parser side is fixed, this logic should work
pub(super) fn subquery_alias_inner_query_and_columns(
    subquery_alias: &datafusion_expr::SubqueryAlias,
) -> (&LogicalPlan, Vec<Ident>) {
    let plan: &LogicalPlan = subquery_alias.input.as_ref();

    let LogicalPlan::Projection(outer_projections) = plan else {
        return (plan, vec![]);
    };

    // Check if it's projection inside projection
    let Some(inner_projection) = find_projection(outer_projections.input.as_ref()) else {
        return (plan, vec![]);
    };

    let mut columns: Vec<Ident> = vec![];
    // Check if the inner projection and outer projection have a matching pattern like
    //     Projection: j1.j1_id AS id
    //       Projection: j1.j1_id
    for (i, inner_expr) in inner_projection.expr.iter().enumerate() {
        let Expr::Alias(ref outer_alias) = &outer_projections.expr[i] else {
            return (plan, vec![]);
        };

        // Inner projection schema fields store the projection name which is used in outer
        // projection expr
        let inner_expr_string = match inner_expr {
            Expr::Column(_) => inner_expr.to_string(),
            _ => inner_projection.schema.field(i).name().clone(),
        };

        if outer_alias.expr.to_string() != inner_expr_string {
            return (plan, vec![]);
        };

        columns.push(outer_alias.name.as_str().into());
    }

    (outer_projections.input.as_ref(), columns)
}

/// Injects column aliases into a subquery's logical plan. The function searches for a `Projection`
/// within the given plan, which may be wrapped by other operators (e.g., LIMIT, SORT).
/// If the top-level plan is a `Projection`, it directly injects the column aliases.
/// Otherwise, it iterates through the plan's children to locate and transform the `Projection`.
///
/// Example:
/// - `SELECT col1, col2 FROM table LIMIT 10` plan with aliases `["alias_1", "some_alias_2"]` will be transformed to
/// - `SELECT col1 AS alias_1, col2 AS some_alias_2 FROM table LIMIT 10`
pub(super) fn inject_column_aliases_into_subquery(
    plan: LogicalPlan,
    aliases: Vec<Ident>,
) -> Result<LogicalPlan> {
    match &plan {
        LogicalPlan::Projection(inner_p) => Ok(inject_column_aliases(inner_p, aliases)),
        _ => {
            // projection is wrapped by other operator (LIMIT, SORT, etc), iterate through the plan to find it
            plan.map_children(|child| {
                if let LogicalPlan::Projection(p) = &child {
                    Ok(Transformed::yes(inject_column_aliases(p, aliases.clone())))
                } else {
                    Ok(Transformed::no(child))
                }
            })
            .map(|plan| plan.data)
        }
    }
}

/// Injects column aliases into the projection of a logical plan by wrapping expressions
/// with `Expr::Alias` using the provided list of aliases.
///
/// Example:
/// - `SELECT col1, col2 FROM table` with aliases `["alias_1", "some_alias_2"]` will be transformed to
/// - `SELECT col1 AS alias_1, col2 AS some_alias_2 FROM table`
pub(super) fn inject_column_aliases(
    projection: &Projection,
    aliases: impl IntoIterator<Item = Ident>,
) -> LogicalPlan {
    let mut updated_projection = projection.clone();

    let new_exprs = updated_projection
        .expr
        .into_iter()
        .zip(aliases)
        .map(|(expr, col_alias)| {
            let relation = match &expr {
                Expr::Column(col) => col.relation.clone(),
                _ => None,
            };

            Expr::Alias(Alias {
                expr: Box::new(expr.clone()),
                relation,
                name: col_alias.value,
            })
        })
        .collect::<Vec<_>>();

    updated_projection.expr = new_exprs;

    LogicalPlan::Projection(updated_projection)
}

fn find_projection(logical_plan: &LogicalPlan) -> Option<&Projection> {
    match logical_plan {
        LogicalPlan::Projection(p) => Some(p),
        LogicalPlan::Limit(p) => find_projection(p.input.as_ref()),
        LogicalPlan::Distinct(p) => find_projection(p.input().as_ref()),
        LogicalPlan::Sort(p) => find_projection(p.input.as_ref()),
        _ => None,
    }
}

/// A `TreeNodeRewriter` implementation that rewrites `Expr::Column` expressions by
/// replacing the column's name with an alias if the column exists in the provided schema.
///
/// This is typically used to apply table aliases in query plans, ensuring that
/// the column references in the expressions use the correct table alias.
///
/// # Fields
///
/// * `table_schema`: The schema (`SchemaRef`) representing the table structure
///   from which the columns are referenced. This is used to look up columns by their names.
/// * `alias_name`: The alias (`TableReference`) that will replace the table name
///   in the column references when applicable.
pub struct TableAliasRewriter<'a> {
    pub table_schema: &'a Schema,
    pub alias_name: TableReference,
}

impl TreeNodeRewriter for TableAliasRewriter<'_> {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match expr {
            Expr::Column(column) => {
                if let Ok(field) = self.table_schema.field_with_name(&column.name) {
                    let new_column =
                        Column::new(Some(self.alias_name.clone()), field.name().clone());
                    Ok(Transformed::yes(Expr::Column(new_column)))
                } else {
                    Ok(Transformed::no(Expr::Column(column)))
                }
            }
            _ => Ok(Transformed::no(expr)),
        }
    }
}
