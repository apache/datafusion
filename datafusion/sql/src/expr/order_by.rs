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

use std::collections::HashMap;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, Result, not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, SortExpr};
use indexmap::IndexSet;
use sqlparser::ast::{
    Expr as SQLExpr, OrderByExpr, OrderByOptions, Value, ValueWithSpan,
};

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Convert sql [OrderByExpr] to `Vec<Expr>`.
    ///
    /// `input_schema` and `additional_schema` are used to resolve column references in the order-by expressions.
    /// `input_schema` is the schema of the input logical plan, typically derived from the SELECT list.
    ///
    /// Usually order-by expressions can only reference the input plan's columns.
    /// But the `SELECT ... FROM ... ORDER BY ...` syntax is a special case. Besides the input schema,
    /// it can reference an `additional_schema` derived from the `FROM` clause.
    ///
    /// If `literal_to_column` is true, treat any numeric literals (e.g. `2`) as a 1 based index into the
    /// SELECT list (e.g. `SELECT a, b FROM table ORDER BY 2`). Literals only reference the `input_schema`.
    ///
    /// If false, interpret numeric literals as constant values.
    pub(crate) fn order_by_to_sort_expr(
        &self,
        order_by_exprs: Vec<OrderByExpr>,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
        literal_to_column: bool,
        additional_schema: Option<&DFSchema>,
    ) -> Result<Vec<SortExpr>> {
        if order_by_exprs.is_empty() {
            return Ok(vec![]);
        }

        let mut combined_schema;
        let order_by_schema = match additional_schema {
            Some(schema) => {
                combined_schema = input_schema.clone();
                combined_schema.merge(schema);
                &combined_schema
            }
            None => input_schema,
        };

        let mut sort_expr_vec = Vec::with_capacity(order_by_exprs.len());

        let make_sort_expr = |expr: Expr,
                              asc: Option<bool>,
                              nulls_first: Option<bool>| {
            let asc = asc.unwrap_or(true);
            let nulls_first = nulls_first
                .unwrap_or_else(|| self.options.default_null_ordering.nulls_first(asc));
            Sort::new(expr, asc, nulls_first)
        };

        for order_by_expr in order_by_exprs {
            let OrderByExpr {
                expr,
                options: OrderByOptions { asc, nulls_first },
                with_fill,
            } = order_by_expr;

            if let Some(with_fill) = with_fill {
                return not_impl_err!("ORDER BY WITH FILL is not supported: {with_fill}");
            }

            let expr = match expr {
                SQLExpr::Value(ValueWithSpan {
                    value: Value::Number(v, _),
                    span: _,
                }) if literal_to_column => {
                    let field_index = v
                        .parse::<usize>()
                        .map_err(|err| plan_datafusion_err!("{}", err))?;

                    if field_index == 0 {
                        return plan_err!(
                            "Order by index starts at 1 for column indexes"
                        );
                    } else if input_schema.fields().len() < field_index {
                        return plan_err!(
                            "Order by column out of bounds, specified: {}, max: {}",
                            field_index,
                            input_schema.fields().len()
                        );
                    }

                    Expr::Column(Column::from(
                        input_schema.qualified_field(field_index - 1),
                    ))
                }
                e => {
                    self.sql_expr_to_logical_expr(e, order_by_schema, planner_context)?
                }
            };
            sort_expr_vec.push(make_sort_expr(expr, asc, nulls_first));
        }

        Ok(sort_expr_vec)
    }

    /// Add missing ORDER BY expressions to the SELECT list.
    ///
    /// This function handles the case where ORDER BY expressions reference columns
    /// or expressions that are not present in the SELECT list. Instead of traversing
    /// the plan tree to find projection nodes, it directly adds the missing
    /// expressions to the SELECT list.
    ///
    /// # Behavior
    ///
    /// - For aggregate functions (e.g., `SUM(x)`) and window functions, the original
    ///   expression is added to the SELECT list, and the ORDER BY expression is
    ///   replaced with a column reference to that expression's output name.
    ///
    /// - For column references that don't exist in the current schema, the column
    ///   reference itself is added to the SELECT list.
    ///
    /// - If the query uses `SELECT DISTINCT` and there are missing ORDER BY
    ///   expressions, an error is returned, as this would make the DISTINCT
    ///   operation ambiguous.
    ///
    /// - Aliases defined in the SELECT list are recognized and used to replace
    ///   the corresponding expressions in ORDER BY with column references.
    ///
    /// - When `strict` is true (e.g., when GROUP BY is present), ORDER BY
    ///   expressions must already be in the SELECT list, be an alias, or be an
    ///   aggregate/window function. Missing expressions will cause an error instead
    ///   of being added to the SELECT list. This preserves the error message
    ///   "Column in ORDER BY must be in GROUP BY" for invalid queries.
    ///
    /// # Arguments
    ///
    /// * `select_exprs` - Mutable reference to the SELECT expressions list. Missing
    ///   expressions will be added to this list (unless strict is true).
    /// * `schema` - The schema of the projected plan, used to check if column
    ///   references exist.
    /// * `distinct` - Whether the query uses `SELECT DISTINCT`. If true, missing
    ///   ORDER BY expressions will cause an error.
    /// * `strict` - Whether to strictly validate ORDER BY expressions. If true,
    ///   missing expressions will cause an error instead of being added.
    /// * `order_by` - Mutable slice of ORDER BY expressions. The expressions will
    ///   be rewritten to use column references where appropriate.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - If expressions were added to the SELECT list.
    /// * `Ok(false)` - If no expressions needed to be added.
    /// * `Err(...)` - If there's an error (e.g., DISTINCT with missing ORDER BY
    ///   expressions).
    ///
    /// # Example
    ///
    /// ```text
    /// Input:  SELECT x FROM foo ORDER BY y
    ///
    /// Before: select_exprs = [x]
    ///         order_by = [Sort { expr: Column(y), ... }]
    ///
    /// After:  select_exprs = [x, y]
    ///         order_by = [Sort { expr: Column(y), ... }]
    ///         returns Ok(true)
    /// ```
    pub(crate) fn add_missing_order_by_exprs(
        select_exprs: &mut Vec<Expr>,
        schema: &DFSchemaRef,
        distinct: bool,
        strict: bool,
        order_by: &mut [Sort],
    ) -> Result<bool> {
        add_missing_order_by_exprs_impl(select_exprs, schema, distinct, strict, order_by)
    }
}

/// Internal implementation of add_missing_order_by_exprs for testability.
fn add_missing_order_by_exprs_impl(
    select_exprs: &mut Vec<Expr>,
    schema: &DFSchemaRef,
    distinct: bool,
    strict: bool,
    order_by: &mut [Sort],
) -> Result<bool> {
    let mut missing_exprs: IndexSet<Expr> = IndexSet::new();

    let mut aliases = HashMap::new();
    for expr in select_exprs.iter() {
        if let Expr::Alias(alias) = expr {
            aliases.insert(alias.expr.clone(), alias.name.clone());
        }
    }

    let mut rewrite = |expr: Expr| {
        if select_exprs.contains(&expr) {
            return Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump));
        }
        if let Some(alias) = aliases.get(&expr) {
            return Ok(Transformed::new(
                Expr::Column(Column::new_unqualified(alias.clone())),
                false,
                TreeNodeRecursion::Jump,
            ));
        }
        match expr {
            Expr::AggregateFunction(_) | Expr::WindowFunction(_) => {
                let replaced = Expr::Column(Column::new_unqualified(
                    expr.schema_name().to_string(),
                ));
                missing_exprs.insert(expr);
                Ok(Transformed::new(replaced, true, TreeNodeRecursion::Jump))
            }
            Expr::Column(ref c) => {
                if strict {
                    // In strict mode (e.g., GROUP BY present), the column must exist in schema
                    // If it doesn't exist and isn't in select_exprs, we'll error later
                    // Don't add it to missing_exprs to preserve proper error message
                    Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump))
                } else if !schema.has_column(c) {
                    missing_exprs.insert(Expr::Column(c.clone()));
                    Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump))
                } else {
                    Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump))
                }
            }
            _ => Ok(Transformed::no(expr)),
        }
    };
    for sort in order_by.iter_mut() {
        let expr = std::mem::take(&mut sort.expr);
        sort.expr = expr.transform_down(&mut rewrite).data()?;
    }
    if !missing_exprs.is_empty() {
        if distinct {
            plan_err!(
                "For SELECT DISTINCT, ORDER BY expressions {} must appear in select list",
                missing_exprs[0]
            )
        } else {
            select_exprs.extend(missing_exprs);
            Ok(true)
        }
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use datafusion_expr::expr::Alias;

    fn create_test_schema() -> DFSchemaRef {
        let fields = vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ];
        DFSchemaRef::new(
            DFSchema::from_unqualified_fields(fields.into(), HashMap::new()).unwrap(),
        )
    }

    #[test]
    fn test_add_missing_column_not_in_select() {
        let schema = create_test_schema();
        let mut select_exprs = vec![col("a")];
        let mut order_by = vec![col("d").sort(true, false)]; // d is not in schema

        let result = add_missing_order_by_exprs_impl(
            &mut select_exprs,
            &schema,
            false,
            false,
            &mut order_by,
        );

        // d is not in schema, so it should be added
        assert!(result.unwrap());
        assert_eq!(select_exprs.len(), 2);
        assert!(select_exprs.contains(&col("a")));
        assert!(select_exprs.contains(&col("d")));
    }

    #[test]
    fn test_no_missing_column_when_already_in_select() {
        let schema = create_test_schema();
        let mut select_exprs = vec![col("a"), col("b")];
        let mut order_by = vec![col("b").sort(true, false)];

        let result = add_missing_order_by_exprs_impl(
            &mut select_exprs,
            &schema,
            false,
            false,
            &mut order_by,
        );

        assert!(!result.unwrap());
        assert_eq!(select_exprs.len(), 2);
    }

    #[test]
    fn test_alias_resolution() {
        let schema = create_test_schema();
        // SELECT a AS x, b
        let mut select_exprs = vec![
            Expr::Alias(Alias::new(col("a"), None::<&str>, "x")),
            col("b"),
        ];
        // ORDER BY a (should be resolved to alias x)
        let mut order_by = vec![col("a").sort(true, false)];

        let result = add_missing_order_by_exprs_impl(
            &mut select_exprs,
            &schema,
            false,
            false,
            &mut order_by,
        );

        // No new expressions should be added (a is resolved to alias x)
        assert!(!result.unwrap());
        // ORDER BY a should be replaced with Column(x) reference
        assert_eq!(order_by[0].expr, col("x"));
    }

    #[test]
    fn test_distinct_with_missing_column_error() {
        let schema = create_test_schema();
        // SELECT DISTINCT a
        // ORDER BY d (d is not in select, not in schema)
        let mut select_exprs = vec![col("a")];
        let mut order_by = vec![col("d").sort(true, false)];

        let result = add_missing_order_by_exprs_impl(
            &mut select_exprs,
            &schema,
            true, // distinct = true
            false,
            &mut order_by,
        );

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("SELECT DISTINCT"));
        assert!(err_msg.contains("must appear in select list"));
    }

    #[test]
    fn test_strict_mode_no_add() {
        let schema = create_test_schema();
        let mut select_exprs = vec![col("a")];
        let mut order_by = vec![col("b").sort(true, false)];

        // strict = true should NOT add missing columns
        let result = add_missing_order_by_exprs_impl(
            &mut select_exprs,
            &schema,
            false,
            true, // strict = true
            &mut order_by,
        );

        assert!(!result.unwrap());
        assert_eq!(select_exprs.len(), 1); // b was not added
    }

    #[test]
    fn test_column_in_order_by_not_in_select_or_schema() {
        let schema = create_test_schema();
        // SELECT a, b
        // ORDER BY d - d is not in schema (would come from FROM clause in real scenario)
        let mut select_exprs = vec![col("a"), col("b")];
        let mut order_by = vec![col("d").sort(true, false)];

        let result = add_missing_order_by_exprs_impl(
            &mut select_exprs,
            &schema,
            false,
            false,
            &mut order_by,
        );

        // d should be added to select_exprs
        assert!(result.unwrap());
        assert!(select_exprs.contains(&col("d")));
    }

    fn col(name: &str) -> Expr {
        Expr::Column(Column::new_unqualified(name))
    }
}
