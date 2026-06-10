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
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_common::{not_impl_err, plan_err};
use datafusion_expr::{Expr, planner::PlannerResult};

use sqlparser::ast::Expr as SQLExpr;

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn sql_substring_to_expr(
        &self,
        expr: Box<SQLExpr>,
        substring_from: Option<Box<SQLExpr>>,
        substring_for: Option<Box<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut substring_args = match (substring_from, substring_for) {
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
                let from_logic = Expr::Literal(ScalarValue::Int64(Some(1)), None);
                let for_logic =
                    self.sql_expr_to_logical_expr(*for_expr, schema, planner_context)?;
                vec![arg, from_logic, for_logic]
            }
            (None, None) => {
                let orig_sql = SQLExpr::Substring {
                    expr,
                    substring_from: None,
                    substring_for: None,
                    special: false,
                    shorthand: false,
                };

                return plan_err!("Substring without for/from is not valid {orig_sql:?}");
            }
        };

        // Try to plan the substring expression using one of the registered planners
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_substring(substring_args)? {
                PlannerResult::Planned(expr) => return Ok(expr),
                PlannerResult::Original(args) => {
                    substring_args = args;
                }
            }
        }

        not_impl_err!(
            "Substring could not be planned by registered expr planner. \
                        Hint: Please try with `unicode_expressions` DataFusion feature enabled"
        )
    }
}
