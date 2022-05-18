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

//! Expression utilities

use crate::expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion};
use crate::{Expr, LogicalPlan};
use datafusion_common::{Column, DFField, DFSchema, DataFusionError, Result};
use std::collections::HashSet;

/// Recursively walk a list of expression trees, collecting the unique set of columns
/// referenced in the expression
pub fn exprlist_to_columns(expr: &[Expr], accum: &mut HashSet<Column>) -> Result<()> {
    for e in expr {
        expr_to_columns(e, accum)?;
    }
    Ok(())
}

/// Recursively walk an expression tree, collecting the unique set of column names
/// referenced in the expression
struct ColumnNameVisitor<'a> {
    accum: &'a mut HashSet<Column>,
}

impl ExpressionVisitor for ColumnNameVisitor<'_> {
    fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>> {
        match expr {
            Expr::Column(qc) => {
                self.accum.insert(qc.clone());
            }
            Expr::ScalarVariable(_, var_names) => {
                self.accum.insert(Column::from_name(var_names.join(".")));
            }
            Expr::Alias(_, _)
            | Expr::Literal(_)
            | Expr::BinaryExpr { .. }
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::Negative(_)
            | Expr::Between { .. }
            | Expr::Case { .. }
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::Sort { .. }
            | Expr::ScalarFunction { .. }
            | Expr::ScalarUDF { .. }
            | Expr::WindowFunction { .. }
            | Expr::AggregateFunction { .. }
            | Expr::GroupingSet(_)
            | Expr::AggregateUDF { .. }
            | Expr::InList { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. }
            | Expr::GetIndexedField { .. } => {}
        }
        Ok(Recursion::Continue(self))
    }
}

/// Recursively walk an expression tree, collecting the unique set of columns
/// referenced in the expression
pub fn expr_to_columns(expr: &Expr, accum: &mut HashSet<Column>) -> Result<()> {
    expr.accept(ColumnNameVisitor { accum })?;
    Ok(())
}

/// Resolves an `Expr::Wildcard` to a collection of `Expr::Column`'s.
pub fn expand_wildcard(schema: &DFSchema, plan: &LogicalPlan) -> Result<Vec<Expr>> {
    let using_columns = plan.using_columns()?;
    let columns_to_skip = using_columns
        .into_iter()
        // For each USING JOIN condition, only expand to one column in projection
        .flat_map(|cols| {
            let mut cols = cols.into_iter().collect::<Vec<_>>();
            // sort join columns to make sure we consistently keep the same
            // qualified column
            cols.sort();
            cols.into_iter().skip(1)
        })
        .collect::<HashSet<_>>();

    if columns_to_skip.is_empty() {
        Ok(schema
            .fields()
            .iter()
            .map(|f| Expr::Column(f.qualified_column()))
            .collect::<Vec<Expr>>())
    } else {
        Ok(schema
            .fields()
            .iter()
            .filter_map(|f| {
                let col = f.qualified_column();
                if !columns_to_skip.contains(&col) {
                    Some(Expr::Column(col))
                } else {
                    None
                }
            })
            .collect::<Vec<Expr>>())
    }
}

/// Resolves an `Expr::Wildcard` to a collection of qualified `Expr::Column`'s.
pub fn expand_qualified_wildcard(
    qualifier: &str,
    schema: &DFSchema,
    plan: &LogicalPlan,
) -> Result<Vec<Expr>> {
    let qualified_fields: Vec<DFField> = schema
        .fields_with_qualified(qualifier)
        .into_iter()
        .cloned()
        .collect();
    if qualified_fields.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Invalid qualifier {}",
            qualifier
        )));
    }
    let qualifier_schema =
        DFSchema::new_with_metadata(qualified_fields, schema.metadata().clone())?;
    expand_wildcard(&qualifier_schema, plan)
}

type WindowSortKey = Vec<Expr>;

/// Generate a sort key for a given window expr's partition_by and order_bu expr
pub fn generate_sort_key(partition_by: &[Expr], order_by: &[Expr]) -> WindowSortKey {
    let mut sort_key = vec![];
    partition_by.iter().for_each(|e| {
        let e = e.clone().sort(true, true);
        if !sort_key.contains(&e) {
            sort_key.push(e);
        }
    });
    order_by.iter().for_each(|e| {
        if !sort_key.contains(e) {
            sort_key.push(e.clone());
        }
    });
    sort_key
}

/// group a slice of window expression expr by their order by expressions
pub fn group_window_expr_by_sort_keys(
    window_expr: &[Expr],
) -> Result<Vec<(WindowSortKey, Vec<&Expr>)>> {
    let mut result = vec![];
    window_expr.iter().try_for_each(|expr| match expr {
        Expr::WindowFunction { partition_by, order_by, .. } => {
            let sort_key = generate_sort_key(partition_by, order_by);
            if let Some((_, values)) = result.iter_mut().find(
                |group: &&mut (WindowSortKey, Vec<&Expr>)| matches!(group, (key, _) if *key == sort_key),
            ) {
                values.push(expr);
            } else {
                result.push((sort_key, vec![expr]))
            }
            Ok(())
        }
        other => Err(DataFusionError::Internal(format!(
            "Impossibly got non-window expr {:?}",
            other,
        ))),
    })?;
    Ok(result)
}

/// Collect all deeply nested `Expr::AggregateFunction` and
/// `Expr::AggregateUDF`. They are returned in order of occurrence (depth
/// first), with duplicates omitted.
pub fn find_aggregate_exprs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(
            nested_expr,
            Expr::AggregateFunction { .. } | Expr::AggregateUDF { .. }
        )
    })
}

/// Collect all deeply nested `Expr::Sort`. They are returned in order of occurrence
/// (depth first), with duplicates omitted.
pub fn find_sort_exprs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(nested_expr, Expr::Sort { .. })
    })
}

/// Collect all deeply nested `Expr::WindowFunction`. They are returned in order of occurrence
/// (depth first), with duplicates omitted.
pub fn find_window_exprs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(nested_expr, Expr::WindowFunction { .. })
    })
}

/// Search the provided `Expr`'s, and all of their nested `Expr`, for any that
/// pass the provided test. The returned `Expr`'s are deduplicated and returned
/// in order of appearance (depth first).
fn find_exprs_in_exprs<F>(exprs: &[Expr], test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    exprs
        .iter()
        .flat_map(|expr| find_exprs_in_expr(expr, test_fn))
        .fold(vec![], |mut acc, expr| {
            if !acc.contains(&expr) {
                acc.push(expr)
            }
            acc
        })
}

/// Search an `Expr`, and all of its nested `Expr`'s, for any that pass the
/// provided test. The returned `Expr`'s are deduplicated and returned in order
/// of appearance (depth first).
fn find_exprs_in_expr<F>(expr: &Expr, test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    let Finder { exprs, .. } = expr
        .accept(Finder::new(test_fn))
        // pre_visit always returns OK, so this will always too
        .expect("no way to return error during recursion");
    exprs
}

// Visitor that find expressions that match a particular predicate
struct Finder<'a, F>
where
    F: Fn(&Expr) -> bool,
{
    test_fn: &'a F,
    exprs: Vec<Expr>,
}

impl<'a, F> Finder<'a, F>
where
    F: Fn(&Expr) -> bool,
{
    /// Create a new finder with the `test_fn`
    fn new(test_fn: &'a F) -> Self {
        Self {
            test_fn,
            exprs: Vec::new(),
        }
    }
}

impl<'a, F> ExpressionVisitor for Finder<'a, F>
where
    F: Fn(&Expr) -> bool,
{
    fn pre_visit(mut self, expr: &Expr) -> Result<Recursion<Self>> {
        if (self.test_fn)(expr) {
            if !(self.exprs.contains(expr)) {
                self.exprs.push(expr.clone())
            }
            // stop recursing down this expr once we find a match
            return Ok(Recursion::Stop(self));
        }

        Ok(Recursion::Continue(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{col, AggregateFunction, WindowFunction};

    #[test]
    fn test_group_window_expr_by_sort_keys_empty_case() -> Result<()> {
        let result = group_window_expr_by_sort_keys(&[])?;
        let expected: Vec<(WindowSortKey, Vec<&Expr>)> = vec![];
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_group_window_expr_by_sort_keys_empty_window() -> Result<()> {
        let max1 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        let max2 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        let min3 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Min),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        let sum4 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Sum),
            args: vec![col("age")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        let exprs = &[max1.clone(), max2.clone(), min3.clone(), sum4.clone()];
        let result = group_window_expr_by_sort_keys(exprs)?;
        let key = vec![];
        let expected: Vec<(WindowSortKey, Vec<&Expr>)> =
            vec![(key, vec![&max1, &max2, &min3, &sum4])];
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_group_window_expr_by_sort_keys() -> Result<()> {
        let age_asc = Expr::Sort {
            expr: Box::new(col("age")),
            asc: true,
            nulls_first: true,
        };
        let name_desc = Expr::Sort {
            expr: Box::new(col("name")),
            asc: false,
            nulls_first: true,
        };
        let created_at_desc = Expr::Sort {
            expr: Box::new(col("created_at")),
            asc: false,
            nulls_first: true,
        };
        let max1 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![age_asc.clone(), name_desc.clone()],
            window_frame: None,
        };
        let max2 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
        };
        let min3 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Min),
            args: vec![col("name")],
            partition_by: vec![],
            order_by: vec![age_asc.clone(), name_desc.clone()],
            window_frame: None,
        };
        let sum4 = Expr::WindowFunction {
            fun: WindowFunction::AggregateFunction(AggregateFunction::Sum),
            args: vec![col("age")],
            partition_by: vec![],
            order_by: vec![name_desc.clone(), age_asc.clone(), created_at_desc.clone()],
            window_frame: None,
        };
        // FIXME use as_ref
        let exprs = &[max1.clone(), max2.clone(), min3.clone(), sum4.clone()];
        let result = group_window_expr_by_sort_keys(exprs)?;

        let key1 = vec![age_asc.clone(), name_desc.clone()];
        let key2 = vec![];
        let key3 = vec![name_desc, age_asc, created_at_desc];

        let expected: Vec<(WindowSortKey, Vec<&Expr>)> = vec![
            (key1, vec![&max1, &min3]),
            (key2, vec![&max2]),
            (key3, vec![&sum4]),
        ];
        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn test_find_sort_exprs() -> Result<()> {
        let exprs = &[
            Expr::WindowFunction {
                fun: WindowFunction::AggregateFunction(AggregateFunction::Max),
                args: vec![col("name")],
                partition_by: vec![],
                order_by: vec![
                    Expr::Sort {
                        expr: Box::new(col("age")),
                        asc: true,
                        nulls_first: true,
                    },
                    Expr::Sort {
                        expr: Box::new(col("name")),
                        asc: false,
                        nulls_first: true,
                    },
                ],
                window_frame: None,
            },
            Expr::WindowFunction {
                fun: WindowFunction::AggregateFunction(AggregateFunction::Sum),
                args: vec![col("age")],
                partition_by: vec![],
                order_by: vec![
                    Expr::Sort {
                        expr: Box::new(col("name")),
                        asc: false,
                        nulls_first: true,
                    },
                    Expr::Sort {
                        expr: Box::new(col("age")),
                        asc: true,
                        nulls_first: true,
                    },
                    Expr::Sort {
                        expr: Box::new(col("created_at")),
                        asc: false,
                        nulls_first: true,
                    },
                ],
                window_frame: None,
            },
        ];
        let expected = vec![
            Expr::Sort {
                expr: Box::new(col("age")),
                asc: true,
                nulls_first: true,
            },
            Expr::Sort {
                expr: Box::new(col("name")),
                asc: false,
                nulls_first: true,
            },
            Expr::Sort {
                expr: Box::new(col("created_at")),
                asc: false,
                nulls_first: true,
            },
        ];
        let result = find_sort_exprs(exprs);
        assert_eq!(expected, result);
        Ok(())
    }
}
