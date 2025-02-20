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
use arrow::compute::kernels::cast_utils::{
    parse_interval_month_day_nano_config, IntervalParseConfig, IntervalUnit,
};
use arrow::datatypes::{
    i256, DataType, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION,
};
use bigdecimal::num_bigint::BigInt;
use bigdecimal::{BigDecimal, Signed, ToPrimitive};
use datafusion_common::{
    internal_datafusion_err, not_impl_err, plan_err, DFSchema, DataFusionError, Result,
    ScalarValue,
};
use datafusion_expr::expr::{BinaryExpr, Placeholder};
use datafusion_expr::planner::PlannerResult;
use datafusion_expr::{lit, Expr, Operator};
use log::debug;
use sqlparser::ast::{BinaryOperator, Expr as SQLExpr, Interval, UnaryOperator, Value};
use sqlparser::parser::ParserError::ParserError;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::ops::Neg;
use std::str::FromStr;

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(crate) fn parse_value(
        &self,
        value: Value,
        param_data_types: &[DataType],
    ) -> Result<Expr> {
        match value {
            Value::Number(n, _) => self.parse_sql_number(&n, false),
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(lit(s)),
            Value::Null => Ok(Expr::Literal(ScalarValue::Null)),
            Value::Boolean(n) => Ok(lit(n)),
            Value::Placeholder(param) => {
                Self::create_placeholder_expr(param, param_data_types)
            }
            Value::HexStringLiteral(s) => {
                if let Some(v) = try_decode_hex_literal(&s) {
                    Ok(lit(v))
                } else {
                    plan_err!("Invalid HexStringLiteral '{s}'")
                }
            }
            Value::DollarQuotedString(s) => Ok(lit(s.value)),
            Value::EscapedStringLiteral(s) => Ok(lit(s)),
            _ => plan_err!("Unsupported Value '{value:?}'"),
        }
    }

    /// Parse number in sql string, convert to Expr::Literal
    pub(super) fn parse_sql_number(
        &self,
        unsigned_number: &str,
        negative: bool,
    ) -> Result<Expr> {
        let signed_number: Cow<str> = if negative {
            Cow::Owned(format!("-{unsigned_number}"))
        } else {
            Cow::Borrowed(unsigned_number)
        };

        // Try to parse as i64 first, then u64 if negative is false, then decimal or f64

        if let Ok(n) = signed_number.parse::<i64>() {
            return Ok(lit(n));
        }

        if !negative {
            if let Ok(n) = unsigned_number.parse::<u64>() {
                return Ok(lit(n));
            }
        }

        if self.options.parse_float_as_decimal {
            parse_decimal(unsigned_number, negative)
        } else {
            signed_number.parse::<f64>().map(lit).map_err(|_| {
                DataFusionError::from(ParserError(format!(
                    "Cannot parse {signed_number} as f64"
                )))
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
                return plan_err!(
                    "Invalid placeholder, zero is not a valid index: {param}"
                );
            }
            Ok(index) => index - 1,
            Err(_) => {
                return if param_data_types.is_empty() {
                    Ok(Expr::Placeholder(Placeholder::new(param, None)))
                } else {
                    // when PREPARE Statement, param_data_types length is always 0
                    plan_err!("Invalid placeholder, not a number: {param}")
                };
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
        let values = elements
            .into_iter()
            .map(|element| {
                self.sql_expr_to_logical_expr(element, schema, &mut PlannerContext::new())
            })
            .collect::<Result<Vec<_>>>()?;

        self.try_plan_array_literal(values, schema)
    }

    fn try_plan_array_literal(
        &self,
        values: Vec<Expr>,
        schema: &DFSchema,
    ) -> Result<Expr> {
        let mut exprs = values;
        for planner in self.context_provider.get_expr_planners() {
            match planner.plan_array_literal(exprs, schema)? {
                PlannerResult::Planned(expr) => {
                    return Ok(expr);
                }
                PlannerResult::Original(values) => exprs = values,
            }
        }

        not_impl_err!("Could not plan array literal. Hint: Please try with `nested_expressions` DataFusion feature enabled")
    }

    /// Convert a SQL interval expression to a DataFusion logical plan
    /// expression
    #[allow(clippy::only_used_in_recursion)]
    pub(super) fn sql_interval_to_expr(
        &self,
        negative: bool,
        interval: Interval,
    ) -> Result<Expr> {
        if interval.leading_precision.is_some() {
            return not_impl_err!(
                "Unsupported Interval Expression with leading_precision {:?}",
                interval.leading_precision
            );
        }

        if interval.last_field.is_some() {
            return not_impl_err!(
                "Unsupported Interval Expression with last_field {:?}",
                interval.last_field
            );
        }

        if interval.fractional_seconds_precision.is_some() {
            return not_impl_err!(
                "Unsupported Interval Expression with fractional_seconds_precision {:?}",
                interval.fractional_seconds_precision
            );
        }

        if let SQLExpr::BinaryOp { left, op, right } = *interval.value {
            let df_op = match op {
                BinaryOperator::Plus => Operator::Plus,
                BinaryOperator::Minus => Operator::Minus,
                _ => {
                    return not_impl_err!("Unsupported interval operator: {op:?}");
                }
            };
            let left_expr = self.sql_interval_to_expr(
                negative,
                Interval {
                    value: left,
                    leading_field: interval.leading_field.clone(),
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None,
                },
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
            )?;
            return Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left_expr),
                df_op,
                Box::new(right_expr),
            )));
        }

        let value = interval_literal(*interval.value, negative)?;

        // leading_field really means the unit if specified
        // For example, "month" in  `INTERVAL '5' month`
        let value = match interval.leading_field.as_ref() {
            Some(leading_field) => format!("{value} {leading_field}"),
            None => value,
        };

        let config = IntervalParseConfig::new(IntervalUnit::Second);
        let val = parse_interval_month_day_nano_config(&value, config)?;
        Ok(lit(ScalarValue::IntervalMonthDayNano(Some(val))))
    }
}

fn interval_literal(interval_value: SQLExpr, negative: bool) -> Result<String> {
    let s = match interval_value {
        SQLExpr::Value(Value::SingleQuotedString(s) | Value::DoubleQuotedString(s)) => s,
        SQLExpr::Value(Value::Number(ref v, long)) => {
            if long {
                return not_impl_err!(
                    "Unsupported interval argument. Long number not supported: {interval_value:?}"
                );
            } else {
                v.to_string()
            }
        }
        SQLExpr::UnaryOp { op, expr } => {
            let negative = match op {
                UnaryOperator::Minus => !negative,
                UnaryOperator::Plus => negative,
                _ => {
                    return not_impl_err!(
                        "Unsupported SQL unary operator in interval {op:?}"
                    );
                }
            };
            interval_literal(*expr, negative)?
        }
        _ => {
            return not_impl_err!("Unsupported interval argument. Expected string literal or number, got: {interval_value:?}");
        }
    };
    if negative {
        Ok(format!("-{s}"))
    } else {
        Ok(s)
    }
}

/// Try to decode bytes from hex literal string.
///
/// None will be returned if the input literal is hex-invalid.
fn try_decode_hex_literal(s: &str) -> Option<Vec<u8>> {
    let hex_bytes = s.as_bytes();

    let mut decoded_bytes = Vec::with_capacity((hex_bytes.len() + 1) / 2);

    let start_idx = hex_bytes.len() % 2;
    if start_idx > 0 {
        // The first byte is formed of only one char.
        decoded_bytes.push(try_decode_hex_char(hex_bytes[0])?);
    }

    for i in (start_idx..hex_bytes.len()).step_by(2) {
        let high = try_decode_hex_char(hex_bytes[i])?;
        let low = try_decode_hex_char(hex_bytes[i + 1])?;
        decoded_bytes.push(high << 4 | low);
    }

    Some(decoded_bytes)
}

/// Try to decode a byte from a hex char.
///
/// None will be returned if the input char is hex-invalid.
const fn try_decode_hex_char(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'F' => Some(c - b'A' + 10),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'0'..=b'9' => Some(c - b'0'),
        _ => None,
    }
}

/// Returns None if the value can't be converted to i256.
/// Modified from <https://github.com/apache/arrow-rs/blob/c4dbf0d8af6ca5a19b8b2ea777da3c276807fc5e/arrow-buffer/src/bigint/mod.rs#L303>
fn bigint_to_i256(v: &BigInt) -> Option<i256> {
    let v_bytes = v.to_signed_bytes_le();
    match v_bytes.len().cmp(&32) {
        Ordering::Less => {
            let mut bytes = if v.is_negative() {
                [255_u8; 32]
            } else {
                [0; 32]
            };
            bytes[0..v_bytes.len()].copy_from_slice(&v_bytes[..v_bytes.len()]);
            Some(i256::from_le_bytes(bytes))
        }
        Ordering::Equal => Some(i256::from_le_bytes(v_bytes.try_into().unwrap())),
        Ordering::Greater => None,
    }
}

fn parse_decimal(unsigned_number: &str, negative: bool) -> Result<Expr> {
    let mut dec = BigDecimal::from_str(unsigned_number).map_err(|e| {
        DataFusionError::from(ParserError(format!(
            "Cannot parse {unsigned_number} as BigDecimal: {e}"
        )))
    })?;
    if negative {
        dec = dec.neg();
    }

    let digits = dec.digits();
    let (int_val, scale) = dec.into_bigint_and_exponent();
    if scale < i8::MIN as i64 {
        return not_impl_err!(
            "Decimal scale {} exceeds the minimum supported scale: {}",
            scale,
            i8::MIN
        );
    }
    let precision = if scale > 0 {
        // arrow-rs requires the precision to include the positive scale.
        // See <https://github.com/apache/arrow-rs/blob/123045cc766d42d1eb06ee8bb3f09e39ea995ddc/arrow-array/src/types.rs#L1230>
        std::cmp::max(digits, scale.unsigned_abs())
    } else {
        digits
    };
    if precision <= DECIMAL128_MAX_PRECISION as u64 {
        let val = int_val.to_i128().ok_or_else(|| {
            // Failures are unexpected here as we have already checked the precision
            internal_datafusion_err!(
                "Unexpected overflow when converting {} to i128",
                int_val
            )
        })?;
        Ok(Expr::Literal(ScalarValue::Decimal128(
            Some(val),
            precision as u8,
            scale as i8,
        )))
    } else if precision <= DECIMAL256_MAX_PRECISION as u64 {
        let val = bigint_to_i256(&int_val).ok_or_else(|| {
            // Failures are unexpected here as we have already checked the precision
            internal_datafusion_err!(
                "Unexpected overflow when converting {} to i256",
                int_val
            )
        })?;
        Ok(Expr::Literal(ScalarValue::Decimal256(
            Some(val),
            precision as u8,
            scale as i8,
        )))
    } else {
        not_impl_err!(
            "Decimal precision {} exceeds the maximum supported precision: {}",
            precision,
            DECIMAL256_MAX_PRECISION
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_hex_literal() {
        let cases = [
            ("", Some(vec![])),
            ("FF00", Some(vec![255, 0])),
            ("a00a", Some(vec![160, 10])),
            ("FF0", Some(vec![15, 240])),
            ("f", Some(vec![15])),
            ("FF0X", None),
            ("X0", None),
            ("XX", None),
            ("x", None),
        ];

        for (input, expect) in cases {
            let output = try_decode_hex_literal(input);
            assert_eq!(output, expect);
        }
    }

    #[test]
    fn test_bigint_to_i256() {
        let cases = [
            (BigInt::from(0), Some(i256::from(0))),
            (BigInt::from(1), Some(i256::from(1))),
            (BigInt::from(-1), Some(i256::from(-1))),
            (
                BigInt::from_str(i256::MAX.to_string().as_str()).unwrap(),
                Some(i256::MAX),
            ),
            (
                BigInt::from_str(i256::MIN.to_string().as_str()).unwrap(),
                Some(i256::MIN),
            ),
            (
                // Can't fit into i256
                BigInt::from_str((i256::MAX.to_string() + "1").as_str()).unwrap(),
                None,
            ),
        ];

        for (input, expect) in cases {
            let output = bigint_to_i256(&input);
            assert_eq!(output, expect);
        }
    }

    #[test]
    fn test_parse_decimal() {
        // Supported cases
        let cases = [
            ("0", ScalarValue::Decimal128(Some(0), 1, 0)),
            ("1", ScalarValue::Decimal128(Some(1), 1, 0)),
            ("123.45", ScalarValue::Decimal128(Some(12345), 5, 2)),
            // Digit count is less than scale
            ("0.001", ScalarValue::Decimal128(Some(1), 3, 3)),
            // Scientific notation
            ("123.456e-2", ScalarValue::Decimal128(Some(123456), 6, 5)),
            // Negative scale
            ("123456e128", ScalarValue::Decimal128(Some(123456), 6, -128)),
            // Decimal256
            (
                &("9".repeat(39) + "." + "99999"),
                ScalarValue::Decimal256(
                    Some(i256::from_string(&"9".repeat(44)).unwrap()),
                    44,
                    5,
                ),
            ),
        ];
        for (input, expect) in cases {
            let output = parse_decimal(input, true).unwrap();
            assert_eq!(output, Expr::Literal(expect.arithmetic_negate().unwrap()));

            let output = parse_decimal(input, false).unwrap();
            assert_eq!(output, Expr::Literal(expect));
        }

        // scale < i8::MIN
        assert_eq!(
            parse_decimal("1e129", false)
                .unwrap_err()
                .strip_backtrace(),
            "This feature is not implemented: Decimal scale -129 exceeds the minimum supported scale: -128"
        );

        // Unsupported precision
        assert_eq!(
            parse_decimal(&"1".repeat(77), false)
                .unwrap_err()
                .strip_backtrace(),
            "This feature is not implemented: Decimal precision 77 exceeds the maximum supported precision: 76"
        );
    }
}
