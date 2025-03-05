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

//! Unwrap casts in binary comparisons
//!
//! The functions in this module attempt to remove casts from
//! comparisons to literals ([`ScalarValue`]s) by applying the casts
//! to the literals if possible. It is inspired by the optimizer rule
//! `UnwrapCastInBinaryComparison` of Spark.
//!
//! Removing casts often improves performance because:
//! 1. The cast is done once (to the literal) rather than to every value
//! 2. Can enable other optimizations such as predicate pushdown that
//!    don't support casting
//!
//! The rule is applied to expressions of the following forms:
//!
//! 1. `cast(left_expr as data_type) comparison_op literal_expr`
//! 2. `literal_expr comparison_op cast(left_expr as data_type)`
//! 3. `cast(literal_expr) IN (expr1, expr2, ...)`
//! 4. `literal_expr IN (cast(expr1) , cast(expr2), ...)`
//!
//! If the expression matches one of the forms above, the rule will
//! ensure the value of `literal` is in range(min, max) of the
//! expr's data_type, and if the scalar is within range, the literal
//! will be casted to the data type of expr on the other side, and the
//! cast will be removed from the other side.
//!
//! # Example
//!
//! If the DataType of c1 is INT32. Given the filter
//!
//! ```text
//! cast(c1 as INT64) > INT64(10)`
//! ```
//!
//! This rule will remove the cast and rewrite the expression to:
//!
//! ```text
//! c1 > INT32(10)
//! ```
//!

use std::cmp::Ordering;

use arrow::datatypes::{
    DataType, TimeUnit, MAX_DECIMAL128_FOR_EACH_PRECISION,
    MIN_DECIMAL128_FOR_EACH_PRECISION,
};
use arrow::temporal_conversions::{MICROSECONDS, MILLISECONDS, NANOSECONDS};
use datafusion_common::{internal_err, tree_node::Transformed};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{lit, BinaryExpr};
use datafusion_expr::{simplify::SimplifyInfo, Cast, Expr, Operator, TryCast};

pub(super) fn unwrap_cast_in_comparison_for_binary<S: SimplifyInfo>(
    info: &S,
    cast_expr: Box<Expr>,
    literal: Box<Expr>,
    op: Operator,
) -> Result<Transformed<Expr>> {
    match (*cast_expr, *literal) {
        (
            Expr::TryCast(TryCast { expr, .. }) | Expr::Cast(Cast { expr, .. }),
            Expr::Literal(lit_value),
        ) => {
            let Ok(expr_type) = info.get_data_type(&expr) else {
                return internal_err!("Can't get the data type of the expr {:?}", &expr);
            };
            // if the lit_value can be casted to the type of internal_left_expr
            // we need to unwrap the cast for cast/try_cast expr, and add cast to the literal
            let Some(value) = try_cast_literal_to_type(&lit_value, &expr_type) else {
                return internal_err!(
                    "Can't cast the literal expr {:?} to type {:?}",
                    &lit_value,
                    &expr_type
                );
            };
            Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr {
                left: expr,
                op,
                right: Box::new(lit(value)),
            })))
        }
        _ => internal_err!("Expect cast expr and literal"),
    }
}

pub(super) fn is_cast_expr_and_support_unwrap_cast_in_comparison_for_binary<
    S: SimplifyInfo,
>(
    info: &S,
    expr: &Expr,
    literal: &Expr,
) -> bool {
    match (expr, literal) {
        (
            Expr::TryCast(TryCast {
                expr: left_expr, ..
            })
            | Expr::Cast(Cast {
                expr: left_expr, ..
            }),
            Expr::Literal(lit_val),
        ) => {
            let Ok(expr_type) = info.get_data_type(left_expr) else {
                return false;
            };

            let Ok(lit_type) = info.get_data_type(literal) else {
                return false;
            };

            try_cast_literal_to_type(lit_val, &expr_type).is_some()
                && is_supported_type(&expr_type)
                && is_supported_type(&lit_type)
        }
        _ => false,
    }
}

pub(super) fn is_cast_expr_and_support_unwrap_cast_in_comparison_for_inlist<
    S: SimplifyInfo,
>(
    info: &S,
    expr: &Expr,
    list: &[Expr],
) -> bool {
    let (Expr::TryCast(TryCast {
        expr: left_expr, ..
    })
    | Expr::Cast(Cast {
        expr: left_expr, ..
    })) = expr
    else {
        return false;
    };

    let Ok(expr_type) = info.get_data_type(left_expr) else {
        return false;
    };

    if !is_supported_type(&expr_type) {
        return false;
    }

    for right in list {
        let Ok(right_type) = info.get_data_type(right) else {
            return false;
        };

        if !is_supported_type(&right_type) {
            return false;
        }

        match right {
            Expr::Literal(lit_val)
                if try_cast_literal_to_type(lit_val, &expr_type).is_some() => {}
            _ => return false,
        }
    }

    true
}

/// Returns true if unwrap_cast_in_comparison supports this data type
fn is_supported_type(data_type: &DataType) -> bool {
    is_supported_numeric_type(data_type)
        || is_supported_string_type(data_type)
        || is_supported_dictionary_type(data_type)
}

/// Returns true if unwrap_cast_in_comparison support this numeric type
fn is_supported_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _)
            | DataType::Timestamp(_, _)
    )
}

/// Returns true if unwrap_cast_in_comparison supports casting this value as a string
fn is_supported_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Returns true if unwrap_cast_in_comparison supports casting this value as a dictionary
fn is_supported_dictionary_type(data_type: &DataType) -> bool {
    matches!(data_type,
                    DataType::Dictionary(_, inner) if is_supported_type(inner))
}

/// Convert a literal value from one data type to another
pub(super) fn try_cast_literal_to_type(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let lit_data_type = lit_value.data_type();
    if !is_supported_type(&lit_data_type) || !is_supported_type(target_type) {
        return None;
    }
    if lit_value.is_null() {
        // null value can be cast to any type of null value
        return ScalarValue::try_from(target_type).ok();
    }
    try_cast_numeric_literal(lit_value, target_type)
        .or_else(|| try_cast_string_literal(lit_value, target_type))
        .or_else(|| try_cast_dictionary(lit_value, target_type))
}

/// Convert a numeric value from one numeric data type to another
fn try_cast_numeric_literal(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let lit_data_type = lit_value.data_type();
    if !is_supported_numeric_type(&lit_data_type)
        || !is_supported_numeric_type(target_type)
    {
        return None;
    }

    let mul = match target_type {
        DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64 => 1_i128,
        DataType::Timestamp(_, _) => 1_i128,
        DataType::Decimal128(_, scale) => 10_i128.pow(*scale as u32),
        _ => return None,
    };
    let (target_min, target_max) = match target_type {
        DataType::UInt8 => (u8::MIN as i128, u8::MAX as i128),
        DataType::UInt16 => (u16::MIN as i128, u16::MAX as i128),
        DataType::UInt32 => (u32::MIN as i128, u32::MAX as i128),
        DataType::UInt64 => (u64::MIN as i128, u64::MAX as i128),
        DataType::Int8 => (i8::MIN as i128, i8::MAX as i128),
        DataType::Int16 => (i16::MIN as i128, i16::MAX as i128),
        DataType::Int32 => (i32::MIN as i128, i32::MAX as i128),
        DataType::Int64 => (i64::MIN as i128, i64::MAX as i128),
        DataType::Timestamp(_, _) => (i64::MIN as i128, i64::MAX as i128),
        DataType::Decimal128(precision, _) => (
            // Different precision for decimal128 can store different range of value.
            // For example, the precision is 3, the max of value is `999` and the min
            // value is `-999`
            MIN_DECIMAL128_FOR_EACH_PRECISION[*precision as usize],
            MAX_DECIMAL128_FOR_EACH_PRECISION[*precision as usize],
        ),
        _ => return None,
    };
    let lit_value_target_type = match lit_value {
        ScalarValue::Int8(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int16(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int32(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int64(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt8(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt16(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt32(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt64(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampSecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampMillisecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampMicrosecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampNanosecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::Decimal128(Some(v), _, scale) => {
            let lit_scale_mul = 10_i128.pow(*scale as u32);
            if mul >= lit_scale_mul {
                // Example:
                // lit is decimal(123,3,2)
                // target type is decimal(5,3)
                // the lit can be converted to the decimal(1230,5,3)
                (*v).checked_mul(mul / lit_scale_mul)
            } else if (*v) % (lit_scale_mul / mul) == 0 {
                // Example:
                // lit is decimal(123000,10,3)
                // target type is int32: the lit can be converted to INT32(123)
                // target type is decimal(10,2): the lit can be converted to decimal(12300,10,2)
                Some(*v / (lit_scale_mul / mul))
            } else {
                // can't convert the lit decimal to the target data type
                None
            }
        }
        _ => None,
    };

    match lit_value_target_type {
        None => None,
        Some(value) => {
            if value >= target_min && value <= target_max {
                // the value casted from lit to the target type is in the range of target type.
                // return the target type of scalar value
                let result_scalar = match target_type {
                    DataType::Int8 => ScalarValue::Int8(Some(value as i8)),
                    DataType::Int16 => ScalarValue::Int16(Some(value as i16)),
                    DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
                    DataType::Int64 => ScalarValue::Int64(Some(value as i64)),
                    DataType::UInt8 => ScalarValue::UInt8(Some(value as u8)),
                    DataType::UInt16 => ScalarValue::UInt16(Some(value as u16)),
                    DataType::UInt32 => ScalarValue::UInt32(Some(value as u32)),
                    DataType::UInt64 => ScalarValue::UInt64(Some(value as u64)),
                    DataType::Timestamp(TimeUnit::Second, tz) => {
                        let value = cast_between_timestamp(
                            &lit_data_type,
                            &DataType::Timestamp(TimeUnit::Second, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampSecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                        let value = cast_between_timestamp(
                            &lit_data_type,
                            &DataType::Timestamp(TimeUnit::Millisecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampMillisecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                        let value = cast_between_timestamp(
                            &lit_data_type,
                            &DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampMicrosecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                        let value = cast_between_timestamp(
                            &lit_data_type,
                            &DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampNanosecond(value, tz.clone())
                    }
                    DataType::Decimal128(p, s) => {
                        ScalarValue::Decimal128(Some(value), *p, *s)
                    }
                    _ => {
                        return None;
                    }
                };
                Some(result_scalar)
            } else {
                None
            }
        }
    }
}

fn try_cast_string_literal(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let string_value = lit_value.try_as_str()?.map(|s| s.to_string());
    let scalar_value = match target_type {
        DataType::Utf8 => ScalarValue::Utf8(string_value),
        DataType::LargeUtf8 => ScalarValue::LargeUtf8(string_value),
        DataType::Utf8View => ScalarValue::Utf8View(string_value),
        _ => return None,
    };
    Some(scalar_value)
}

/// Attempt to cast to/from a dictionary type by wrapping/unwrapping the dictionary
fn try_cast_dictionary(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    let lit_value_type = lit_value.data_type();
    let result_scalar = match (lit_value, target_type) {
        // Unwrap dictionary when inner type matches target type
        (ScalarValue::Dictionary(_, inner_value), _)
            if inner_value.data_type() == *target_type =>
        {
            (**inner_value).clone()
        }
        // Wrap type when target type is dictionary
        (_, DataType::Dictionary(index_type, inner_type))
            if **inner_type == lit_value_type =>
        {
            ScalarValue::Dictionary(index_type.clone(), Box::new(lit_value.clone()))
        }
        _ => {
            return None;
        }
    };
    Some(result_scalar)
}

/// Cast a timestamp value from one unit to another
fn cast_between_timestamp(from: &DataType, to: &DataType, value: i128) -> Option<i64> {
    let value = value as i64;
    let from_scale = match from {
        DataType::Timestamp(TimeUnit::Second, _) => 1,
        DataType::Timestamp(TimeUnit::Millisecond, _) => MILLISECONDS,
        DataType::Timestamp(TimeUnit::Microsecond, _) => MICROSECONDS,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => NANOSECONDS,
        _ => return Some(value),
    };

    let to_scale = match to {
        DataType::Timestamp(TimeUnit::Second, _) => 1,
        DataType::Timestamp(TimeUnit::Millisecond, _) => MILLISECONDS,
        DataType::Timestamp(TimeUnit::Microsecond, _) => MICROSECONDS,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => NANOSECONDS,
        _ => return Some(value),
    };

    match from_scale.cmp(&to_scale) {
        Ordering::Less => value.checked_mul(to_scale / from_scale),
        Ordering::Greater => Some(value / (from_scale / to_scale)),
        Ordering::Equal => Some(value),
    }
}
