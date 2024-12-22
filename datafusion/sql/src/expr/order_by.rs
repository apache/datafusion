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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::{
    not_impl_err, plan_datafusion_err, plan_err, Column, DFSchema, Result,
};
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, SortExpr};
use sqlparser::ast::{Expr as SQLExpr, OrderByExpr, Value};

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
        exprs: Vec<OrderByExpr>,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
        literal_to_column: bool,
        additional_schema: Option<&DFSchema>,
    ) -> Result<Vec<SortExpr>> {
        if exprs.is_empty() {
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

        let mut expr_vec = vec![];
        for e in exprs {
            let OrderByExpr {
                asc,
                expr,
                nulls_first,
                with_fill,
            } = e;

            if let Some(with_fill) = with_fill {
                return not_impl_err!("ORDER BY WITH FILL is not supported: {with_fill}");
            }

            let expr = match expr {
                SQLExpr::Value(Value::Number(v, _)) if literal_to_column => {
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
            let asc = asc.unwrap_or(true);
            expr_vec.push(Sort::new(
                expr,
                asc,
                // When asc is true, by default nulls last to be consistent with postgres
                // postgres rule: https://www.postgresql.org/docs/current/queries-order.html
                nulls_first.unwrap_or(!asc),
            ))
        }
        Ok(expr_vec)
    }
}
