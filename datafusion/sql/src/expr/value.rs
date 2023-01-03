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
use datafusion_common::{parse_interval, DFSchema, DataFusionError, Result, ScalarValue};
use datafusion_expr::{lit, Expr};
use log::debug;
use sqlparser::ast::{DateTimeField, Expr as SQLExpr, Value};
use sqlparser::parser::ParserError::ParserError;
use std::collections::HashSet;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(crate) fn parse_value(
        &self,
        value: Value,
        param_data_types: &[DataType],
    ) -> Result<Expr> {
        match value {
            Value::Number(n, _) => self.parse_sql_number(&n),
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(lit(s)),
            Value::Null => Ok(Expr::Literal(ScalarValue::Null)),
            Value::Boolean(n) => Ok(lit(n)),
            Value::Placeholder(param) => {
                Self::create_placeholder_expr(param, param_data_types)
            }
            _ => Err(DataFusionError::Plan(format!(
                "Unsupported Value '{:?}'",
                value,
            ))),
        }
    }

    /// Parse number in sql string, convert to Expr::Literal
    fn parse_sql_number(&self, n: &str) -> Result<Expr> {
        if n.find('E').is_some() {
            // not implemented yet
            // https://github.com/apache/arrow-datafusion/issues/3448
            Err(DataFusionError::NotImplemented(
                "sql numeric literals in scientific notation are not supported"
                    .to_string(),
            ))
        } else if let Ok(n) = n.parse::<i64>() {
            Ok(lit(n))
        } else if self.options.parse_float_as_decimal {
            // remove leading zeroes
            let str = n.trim_start_matches('0');
            if let Some(i) = str.find('.') {
                let p = str.len() - 1;
                let s = str.len() - i - 1;
                let str = str.replace('.', "");
                let n = str.parse::<i128>().map_err(|_| {
                    DataFusionError::from(ParserError(format!(
                        "Cannot parse {str} as i128 when building decimal"
                    )))
                })?;
                Ok(Expr::Literal(ScalarValue::Decimal128(
                    Some(n),
                    p as u8,
                    s as i8,
                )))
            } else {
                let number = n.parse::<i128>().map_err(|_| {
                    DataFusionError::from(ParserError(format!(
                        "Cannot parse {n} as i128 when building decimal"
                    )))
                })?;
                Ok(Expr::Literal(ScalarValue::Decimal128(Some(number), 38, 0)))
            }
        } else {
            n.parse::<f64>().map(lit).map_err(|_| {
                DataFusionError::from(ParserError(format!("Cannot parse {n} as f64")))
            })
        }
    }

    /// Create a placeholder expression
    /// This is the same as Postgres's prepare statement syntax in which a placeholder starts with `$` sign and then
    /// number 1, 2, ... etc. For example, `$1` is the first placeholder; $2 is the second one and so on.
    fn create_placeholder_expr(
        param: String,
        param_data_types: &[DataType],
    ) -> Result<Expr> {
        // Parse the placeholder as a number because it is the only support from sqlparser and postgres
        let index = param[1..].parse::<usize>();
        let idx = match index {
            Ok(index) => index - 1,
            Err(_) => {
                return Err(DataFusionError::Internal(format!(
                    "Invalid placeholder, not a number: {param}"
                )));
            }
        };
        // Check if the placeholder is in the parameter list
        if param_data_types.len() <= idx {
            return Err(DataFusionError::Internal(format!(
                "Placehoder {param} does not exist in the parameter list: {param_data_types:?}"
            )));
        }
        // Data type of the parameter
        let param_type = param_data_types[idx].clone();
        debug!(
            "type of param {} param_data_types[idx]: {:?}",
            param, param_type
        );

        Ok(Expr::Placeholder {
            id: param,
            data_type: param_type,
        })
    }

    pub(super) fn sql_array_literal(
        &self,
        elements: Vec<SQLExpr>,
        schema: &DFSchema,
    ) -> Result<Expr> {
        let mut values = Vec::with_capacity(elements.len());

        for element in elements {
            let value = self.sql_expr_to_logical_expr(
                element,
                schema,
                &mut PlannerContext::new(),
            )?;
            match value {
                Expr::Literal(scalar) => {
                    values.push(scalar);
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Arrays with elements other than literal are not supported: {value}"
                    )));
                }
            }
        }

        let data_types: HashSet<DataType> =
            values.iter().map(|e| e.get_datatype()).collect();

        if data_types.is_empty() {
            Ok(lit(ScalarValue::new_list(None, DataType::Utf8)))
        } else if data_types.len() > 1 {
            Err(DataFusionError::NotImplemented(format!(
                "Arrays with different types are not supported: {data_types:?}",
            )))
        } else {
            let data_type = values[0].get_datatype();

            Ok(lit(ScalarValue::new_list(Some(values), data_type)))
        }
    }

    pub(super) fn sql_interval_to_expr(
        &self,
        value: SQLExpr,
        leading_field: Option<DateTimeField>,
        leading_precision: Option<u64>,
        last_field: Option<DateTimeField>,
        fractional_seconds_precision: Option<u64>,
    ) -> Result<Expr> {
        if leading_precision.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with leading_precision {leading_precision:?}"
            )));
        }

        if last_field.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with last_field {last_field:?}"
            )));
        }

        if fractional_seconds_precision.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with fractional_seconds_precision {fractional_seconds_precision:?}"
            )));
        }

        // Only handle string exprs for now
        let value = match value {
            SQLExpr::Value(
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
            ) => s,
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported interval argument. Expected string literal, got: {value:?}"
                )));
            }
        };

        let leading_field = leading_field
            .as_ref()
            .map(|dt| dt.to_string())
            .unwrap_or_else(|| "second".to_string());

        Ok(lit(parse_interval(&leading_field, &value)?))
    }
}
