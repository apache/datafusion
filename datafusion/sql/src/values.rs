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
use arrow_schema::DataType;
use datafusion_common::{DFSchema, DataFusionError, Result};
use datafusion_expr::{lit, Cast, Expr, LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{Expr as SQLExpr, Values as SQLValues};

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(super) fn sql_values_to_plan(
        &self,
        values: SQLValues,
        param_data_types: &[DataType],
    ) -> Result<LogicalPlan> {
        let SQLValues {
            explicit_row: _,
            rows,
        } = values;

        // values should not be based on any other schema
        let schema = DFSchema::empty();
        let values = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| match v {
                        SQLExpr::Value(value) => {
                            self.parse_value(value, param_data_types)
                        }
                        SQLExpr::UnaryOp { op, expr } => self.parse_sql_unary_op(
                            op,
                            *expr,
                            &schema,
                            &mut PlannerContext::new(),
                        ),
                        SQLExpr::BinaryOp { left, op, right } => self
                            .parse_sql_binary_op(
                                *left,
                                op,
                                *right,
                                &schema,
                                &mut PlannerContext::new(),
                            ),
                        SQLExpr::TypedString { data_type, value } => {
                            Ok(Expr::Cast(Cast::new(
                                Box::new(lit(value)),
                                self.convert_data_type(&data_type)?,
                            )))
                        }
                        SQLExpr::Cast { expr, data_type } => Ok(Expr::Cast(Cast::new(
                            Box::new(self.sql_expr_to_logical_expr(
                                *expr,
                                &schema,
                                &mut PlannerContext::new(),
                            )?),
                            self.convert_data_type(&data_type)?,
                        ))),
                        other => Err(DataFusionError::NotImplemented(format!(
                            "Unsupported value {other:?} in a values list expression"
                        ))),
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        LogicalPlanBuilder::values(values)?.build()
    }
}
