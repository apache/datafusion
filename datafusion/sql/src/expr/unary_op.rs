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
use datafusion_expr::{lit, Expr};
use sqlparser::ast::{Expr as SQLExpr, UnaryOperator, Value};

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(crate) fn parse_sql_unary_op(
        &self,
        op: UnaryOperator,
        expr: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        match op {
            UnaryOperator::Not => Ok(Expr::Not(Box::new(
                self.sql_expr_to_logical_expr(expr, schema, planner_context)?,
            ))),
            UnaryOperator::Plus => {
                Ok(self.sql_expr_to_logical_expr(expr, schema, planner_context)?)
            }
            UnaryOperator::Minus => {
                match expr {
                    // optimization: if it's a number literal, we apply the negative operator
                    // here directly to calculate the new literal.
                    SQLExpr::Value(Value::Number(n, _)) => match n.parse::<i64>() {
                        Ok(n) => Ok(lit(-n)),
                        Err(_) => Ok(lit(-n
                            .parse::<f64>()
                            .map_err(|_e| {
                                DataFusionError::Internal(format!(
                                    "negative operator can be only applied to integer and float operands, got: {n}"))
                            })?)),
                    },
                    // not a literal, apply negative operator on expression
                    _ => Ok(Expr::Negative(Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?))),
                }
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported SQL unary operator {op:?}"
            ))),
        }
    }
}
