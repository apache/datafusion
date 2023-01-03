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
use datafusion_common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion_expr::{BuiltinScalarFunction, Expr};
use sqlparser::ast::Expr as SQLExpr;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(super) fn sql_substring_to_expr(
        &self,
        expr: Box<SQLExpr>,
        substring_from: Option<Box<SQLExpr>>,
        substring_for: Option<Box<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let args = match (substring_from, substring_for) {
            (Some(from_expr), Some(for_expr)) => {
                let arg =
                    self.sql_expr_to_logical_expr(*expr, schema, planner_context)?;
                let from_logic =
                    self.sql_expr_to_logical_expr(*from_expr, schema, planner_context)?;
                let for_logic =
                    self.sql_expr_to_logical_expr(*for_expr, schema, planner_context)?;
                vec![arg, from_logic, for_logic]
            }
            (Some(from_expr), None) => {
                let arg =
                    self.sql_expr_to_logical_expr(*expr, schema, planner_context)?;
                let from_logic =
                    self.sql_expr_to_logical_expr(*from_expr, schema, planner_context)?;
                vec![arg, from_logic]
            }
            (None, Some(for_expr)) => {
                let arg =
                    self.sql_expr_to_logical_expr(*expr, schema, planner_context)?;
                let from_logic = Expr::Literal(ScalarValue::Int64(Some(1)));
                let for_logic =
                    self.sql_expr_to_logical_expr(*for_expr, schema, planner_context)?;
                vec![arg, from_logic, for_logic]
            }
            (None, None) => {
                let orig_sql = SQLExpr::Substring {
                    expr,
                    substring_from: None,
                    substring_for: None,
                };

                return Err(DataFusionError::Plan(format!(
                    "Substring without for/from is not valid {orig_sql:?}"
                )));
            }
        };

        Ok(Expr::ScalarFunction {
            fun: BuiltinScalarFunction::Substr,
            args,
        })
    }
}
