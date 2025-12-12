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
use datafusion_common::{not_impl_err, plan_err, DFSchema, Diagnostic, Result};
use datafusion_expr::{
    type_coercion::{is_interval, is_timestamp},
    Expr, ExprSchemable,
};
use sqlparser::ast::{Expr as SQLExpr, UnaryOperator, Value, ValueWithSpan};

impl<S: ContextProvider> SqlToRel<'_, S> {
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
                let operand =
                    self.sql_expr_to_logical_expr(expr, schema, planner_context)?;
                let field = operand.to_field(schema)?.1;
                let data_type = field.data_type();
                if data_type.is_numeric()
                    || is_interval(data_type)
                    || is_timestamp(data_type)
                {
                    Ok(operand)
                } else {
                    let span = operand.spans().and_then(|s| s.first());
                    let mut diagnostic = Diagnostic::new_error(
                        format!("+ cannot be used with {data_type}"),
                        span,
                    );
                    diagnostic.add_note(
                        "+ can only be used with numbers, intervals, and timestamps",
                        None,
                    );
                    diagnostic
                        .add_help(format!("perhaps you need to cast {operand}"), None);
                    plan_err!("Unary operator '+' only supports numeric, interval and timestamp types"; diagnostic=diagnostic)
                }
            }
            UnaryOperator::Minus => {
                match expr {
                    // Optimization: if it's a number literal, we apply the negative operator
                    // here directly to calculate the new literal.
                    SQLExpr::Value(ValueWithSpan {
                        value: Value::Number(n, _),
                        span: _,
                    }) => self.parse_sql_number(&n, true),
                    SQLExpr::Interval(interval) => {
                        self.sql_interval_to_expr(true, interval)
                    }
                    // Not a literal, apply negative operator on expression
                    _ => Ok(Expr::Negative(Box::new(self.sql_expr_to_logical_expr(
                        expr,
                        schema,
                        planner_context,
                    )?))),
                }
            }
            _ => not_impl_err!("Unsupported SQL unary operator {op:?}"),
        }
    }
}
