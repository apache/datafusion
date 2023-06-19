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
use arrow::compute::kernels::cast_utils::parse_interval_month_day_nano;
use arrow_schema::DataType;
use datafusion_common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion_expr::expr::{BinaryExpr, Placeholder};
use datafusion_expr::{lit, Expr, Operator};
use log::debug;
use sqlparser::ast::{BinaryOperator, Expr as SQLExpr, Interval, Value};
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
                "Unsupported Value '{value:?}'",
            ))),
        }
    }

    /// Parse number in sql string, convert to Expr::Literal
    fn parse_sql_number(&self, n: &str) -> Result<Expr> {
        if let Ok(n) = n.parse::<i64>() {
            Ok(lit(n))
        } else if let Ok(n) = n.parse::<u64>() {
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
            Ok(0) => {
                return Err(DataFusionError::Plan(format!(
                    "Invalid placeholder, zero is not a valid index: {param}"
                )));
            }
            Ok(index) => index - 1,
            Err(_) => {
                return Err(DataFusionError::Plan(format!(
                    "Invalid placeholder, not a number: {param}"
                )));
            }
        };
        // Check if the placeholder is in the parameter list
        let param_type = param_data_types.get(idx);
        // Data type of the parameter
        debug!(
            "type of param {} param_data_types[idx]: {:?}",
            param, param_type
        );

        Ok(Expr::Placeholder(Placeholder::new(
            param,
            param_type.cloned(),
        )))
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

    /// Convert a SQL interval expression to a DataFusion logical plan
    /// expression
    pub(super) fn sql_interval_to_expr(
        &self,
        negative: bool,
        interval: Interval,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        if interval.leading_precision.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with leading_precision {:?}",
                interval.leading_precision,
            )));
        }

        if interval.last_field.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with last_field {:?}",
                interval.last_field,
            )));
        }

        if interval.fractional_seconds_precision.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with fractional_seconds_precision {:?}",
                interval.fractional_seconds_precision,
            )));
        }

        // Only handle string exprs for now
        let value = match *interval.value {
            SQLExpr::Value(
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
            ) => {
                if negative {
                    format!("-{s}")
                } else {
                    s
                }
            }
            // Support expressions like `interval '1 month' + date/timestamp`.
            // Such expressions are parsed like this by sqlparser-rs
            //
            // Interval
            //   BinaryOp
            //     Value(StringLiteral)
            //     Cast
            //       Value(StringLiteral)
            //
            // This code rewrites them to the following:
            //
            // BinaryOp
            //   Interval
            //     Value(StringLiteral)
            //   Cast
            //      Value(StringLiteral)
            SQLExpr::BinaryOp { left, op, right } => {
                let df_op = match op {
                    BinaryOperator::Plus => Operator::Plus,
                    BinaryOperator::Minus => Operator::Minus,
                    _ => {
                        return Err(DataFusionError::NotImplemented(format!(
                            "Unsupported interval operator: {op:?}"
                        )));
                    }
                };
                match (interval.leading_field, left.as_ref(), right.as_ref()) {
                    (_, _, SQLExpr::Value(_)) => {
                        let left_expr = self.sql_interval_to_expr(
                            negative,
                            Interval {
                                value: left,
                                leading_field: interval.leading_field,
                                leading_precision: None,
                                last_field: None,
                                fractional_seconds_precision: None,
                            },
                            schema,
                            planner_context,
                        )?;
                        let right_expr = self.sql_interval_to_expr(
                            false,
                            Interval {
                                value: right,
                                leading_field: interval.leading_field,
                                leading_precision: None,
                                last_field: None,
                                fractional_seconds_precision: None,
                            },
                            schema,
                            planner_context,
                        )?;
                        return Ok(Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(left_expr),
                            df_op,
                            Box::new(right_expr),
                        )));
                    }
                    // In this case, the left node is part of the interval
                    // expr and the right node is an independent expr.
                    //
                    // Leading field is not supported when the right operand
                    // is not a value.
                    (None, _, _) => {
                        let left_expr = self.sql_interval_to_expr(
                            negative,
                            Interval {
                                value: left,
                                leading_field: None,
                                leading_precision: None,
                                last_field: None,
                                fractional_seconds_precision: None,
                            },
                            schema,
                            planner_context,
                        )?;
                        let right_expr = self.sql_expr_to_logical_expr(
                            *right,
                            schema,
                            planner_context,
                        )?;
                        return Ok(Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(left_expr),
                            df_op,
                            Box::new(right_expr),
                        )));
                    }
                    _ => {
                        let value = SQLExpr::BinaryOp { left, op, right };
                        return Err(DataFusionError::NotImplemented(format!(
                            "Unsupported interval argument. Expected string literal, got: {value:?}"
                        )));
                    }
                }
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported interval argument. Expected string literal, got: {:?}",
                    interval.value,
                )));
            }
        };

        let value = if has_units(&value) {
            // If the interval already contains a unit
            // `INTERVAL '5 month' rather than `INTERVAL '5' month`
            // skip the other unit
            value
        } else {
            // leading_field really means the unit if specified
            // for example, "month" in  `INTERVAL '5' month`
            match interval.leading_field.as_ref() {
                Some(leading_field) => {
                    format!("{value} {leading_field}")
                }
                None => {
                    // default to seconds for the units
                    // `INTERVAL '5' is parsed as '5 seconds'
                    format!("{value} seconds")
                }
            }
        };

        let val = parse_interval_month_day_nano(&value)?;
        Ok(lit(ScalarValue::IntervalMonthDayNano(Some(val))))
    }
}

// TODO make interval parsing better in arrow-rs / expose `IntervalType`
fn has_units(val: &str) -> bool {
    val.ends_with("century")
        || val.ends_with("centuries")
        || val.ends_with("decade")
        || val.ends_with("decades")
        || val.ends_with("year")
        || val.ends_with("years")
        || val.ends_with("month")
        || val.ends_with("months")
        || val.ends_with("week")
        || val.ends_with("weeks")
        || val.ends_with("day")
        || val.ends_with("days")
        || val.ends_with("hour")
        || val.ends_with("hours")
        || val.ends_with("minute")
        || val.ends_with("minutes")
        || val.ends_with("second")
        || val.ends_with("seconds")
        || val.ends_with("millisecond")
        || val.ends_with("milliseconds")
        || val.ends_with("microsecond")
        || val.ends_with("microseconds")
        || val.ends_with("nanosecond")
        || val.ends_with("nanoseconds")
}
