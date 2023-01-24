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
use datafusion_common::{DFSchema, DataFusionError, Result};
use datafusion_expr::expr::Sort;
use datafusion_expr::Expr;
use sqlparser::ast::{Expr as SQLExpr, OrderByExpr, Value};

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// convert sql OrderByExpr to Expr::Sort
    pub(crate) fn order_by_to_sort_expr(
        &self,
        e: OrderByExpr,
        schema: &DFSchema,
    ) -> Result<Expr> {
        let OrderByExpr {
            asc,
            expr,
            nulls_first,
        } = e;

        let expr = match expr {
            SQLExpr::Value(Value::Number(v, _)) => {
                let field_index = v
                    .parse::<usize>()
                    .map_err(|err| DataFusionError::Plan(err.to_string()))?;

                if field_index == 0 {
                    return Err(DataFusionError::Plan(
                        "Order by index starts at 1 for column indexes".to_string(),
                    ));
                } else if schema.fields().len() < field_index {
                    return Err(DataFusionError::Plan(format!(
                        "Order by column out of bounds, specified: {}, max: {}",
                        field_index,
                        schema.fields().len()
                    )));
                }

                let field = schema.field(field_index - 1);
                Expr::Column(field.qualified_column())
            }
            e => self.sql_expr_to_logical_expr(e, schema, &mut PlannerContext::new())?,
        };
        Ok({
            let asc = asc.unwrap_or(true);
            Expr::Sort(Sort::new(
                Box::new(expr),
                asc,
                // when asc is true, by default nulls last to be consistent with postgres
                // postgres rule: https://www.postgresql.org/docs/current/queries-order.html
                nulls_first.unwrap_or(!asc),
            ))
        })
    }
}
