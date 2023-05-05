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

//! Coercion rules for matching argument types for binary operators

use arrow::compute::can_cast_types;
use arrow::datatypes::{
    DataType, TimeUnit, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE,
};

use datafusion_common::DataFusionError;
use datafusion_common::Result;

use crate::type_coercion::{is_datetime, is_decimal, is_interval, is_numeric};
use crate::Operator;

/// Returns the result type of applying mathematics operations such as
/// `+` to arguments of `lhs_type` and `rhs_type`.
fn mathematics_temporal_result_type(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    use arrow::datatypes::IntervalUnit::*;
    use arrow::datatypes::TimeUnit::*;

    if !is_interval(lhs_type)
        && !is_interval(rhs_type)
        && !is_datetime(lhs_type)
        && !is_datetime(rhs_type)
    {
        return None;
    };

    match (lhs_type, rhs_type) {
        // datetime +/- interval
        (Interval(_), Timestamp(_, _)) => Some(rhs_type.clone()),
        (Timestamp(_, _), Interval(_)) => Some(lhs_type.clone()),
        (Interval(_), Date32) => Some(rhs_type.clone()),
        (Date32, Interval(_)) => Some(lhs_type.clone()),
        (Interval(_), Date64) => Some(rhs_type.clone()),
        (Date64, Interval(_)) => Some(lhs_type.clone()),
        // interval +/-
        (Interval(YearMonth), Interval(YearMonth)) => Some(Interval(YearMonth)),
        (Interval(DayTime), Interval(DayTime)) => Some(Interval(DayTime)),
        (Interval(_), Interval(_)) => Some(Interval(MonthDayNano)),
        // timestamp - timestamp
        (Timestamp(Second, _), Timestamp(Second, _))
        | (Timestamp(Millisecond, _), Timestamp(Millisecond, _)) => {
            Some(Interval(DayTime))
        }
        (Timestamp(Microsecond, _), Timestamp(Microsecond, _))
        | (Timestamp(Nanosecond, _), Timestamp(Nanosecond, _)) => {
            Some(Interval(MonthDayNano))
        }
        (Timestamp(_, _), Timestamp(_, _)) => None,
        // TODO: date minus date
        // date - timestamp, timestamp - date
        (Date32, Timestamp(_, _))
        | (Timestamp(_, _), Date32)
        | (Date64, Timestamp(_, _))
        | (Timestamp(_, _), Date64) => {
            // TODO: make get_result_type must after coerce type.
            // if type isn't coerced, we need get common type, and then get result type.
            let common_type = temporal_coercion(lhs_type, rhs_type);
            common_type.and_then(|t| mathematics_temporal_result_type(&t, &t))
        }
        _ => None,
    }
}

/// returns the resulting type of a binary expression evaluating the `op` with the left and right hand types
pub fn get_result_type(
    lhs_type: &DataType,
    op: &Operator,
    rhs_type: &DataType,
) -> Result<DataType> {
    if op.is_numerical_operators() && any_decimal(lhs_type, rhs_type) {
        let (coerced_lhs_type, coerced_rhs_type) =
            math_decimal_coercion(lhs_type, rhs_type);

        let lhs_type = coerced_lhs_type.unwrap_or(lhs_type.clone());
        let rhs_type = coerced_rhs_type.unwrap_or(rhs_type.clone());

        if op.is_numerical_operators() {
            if let Some(result_type) =
                decimal_op_mathematics_type(op, &lhs_type, &rhs_type)
            {
                return Ok(result_type);
            }
        }
    }
    let result = match op {
        Operator::And
        | Operator::Or
        | Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::Gt
        | Operator::GtEq
        | Operator::LtEq
        | Operator::RegexMatch
        | Operator::RegexIMatch
        | Operator::RegexNotMatch
        | Operator::RegexNotIMatch
        | Operator::IsDistinctFrom
        | Operator::IsNotDistinctFrom => Some(DataType::Boolean),
        Operator::Plus | Operator::Minus
            if is_datetime(lhs_type) && is_datetime(rhs_type)
                || (is_interval(lhs_type) && is_interval(rhs_type))
                || (is_datetime(lhs_type) && is_interval(rhs_type))
                || (is_interval(lhs_type) && is_datetime(rhs_type)) =>
        {
            mathematics_temporal_result_type(lhs_type, rhs_type)
        }
        // following same with `coerce_types`
        Operator::BitwiseAnd
        | Operator::BitwiseOr
        | Operator::BitwiseXor
        | Operator::BitwiseShiftRight
        | Operator::BitwiseShiftLeft => bitwise_coercion(lhs_type, rhs_type),
        Operator::Plus
        | Operator::Minus
        | Operator::Modulo
        | Operator::Divide
        | Operator::Multiply => mathematics_numerical_coercion(lhs_type, rhs_type),
        Operator::StringConcat => string_concat_coercion(lhs_type, rhs_type),
    };

    result.ok_or(DataFusionError::Plan(format!(
        "Unsupported argument types. Can not evaluate {lhs_type:?} {op} {rhs_type:?}"
    )))
}

/// Coercion rules for all binary operators. Returns the 'coerce_types'
/// is returns the type the arguments should be coerced to
///
/// Returns None if no suitable type can be found.
pub fn coerce_types(
    lhs_type: &DataType,
    op: &Operator,
    rhs_type: &DataType,
) -> Result<DataType> {
    // This result MUST be compatible with `binary_coerce`
    let result = match op {
        Operator::BitwiseAnd
        | Operator::BitwiseOr
        | Operator::BitwiseXor
        | Operator::BitwiseShiftRight
        | Operator::BitwiseShiftLeft => bitwise_coercion(lhs_type, rhs_type),
        Operator::And | Operator::Or => match (lhs_type, rhs_type) {
            // logical binary boolean operators can only be evaluated in bools or nulls
            (DataType::Boolean, DataType::Boolean)
            | (DataType::Null, DataType::Null)
            | (DataType::Boolean, DataType::Null)
            | (DataType::Null, DataType::Boolean) => Some(DataType::Boolean),
            _ => None,
        },
        // logical comparison operators have their own rules, and always return a boolean
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::Gt
        | Operator::GtEq
        | Operator::LtEq
        | Operator::IsDistinctFrom
        | Operator::IsNotDistinctFrom => comparison_coercion(lhs_type, rhs_type),
        Operator::Plus | Operator::Minus
            if is_interval(lhs_type) && is_interval(rhs_type) =>
        {
            temporal_coercion(lhs_type, rhs_type)
        }
        Operator::Minus if is_datetime(lhs_type) && is_datetime(rhs_type) => {
            temporal_coercion(lhs_type, rhs_type)
        }
        // for math expressions, the final value of the coercion is also the return type
        // because coercion favours higher information types
        Operator::Plus
        | Operator::Minus
        | Operator::Modulo
        | Operator::Divide
        | Operator::Multiply => mathematics_numerical_coercion(lhs_type, rhs_type),
        Operator::RegexMatch
        | Operator::RegexIMatch
        | Operator::RegexNotMatch
        | Operator::RegexNotIMatch => regex_coercion(lhs_type, rhs_type),
        // "||" operator has its own rules, and always return a string type
        Operator::StringConcat => string_concat_coercion(lhs_type, rhs_type),
    };

    // re-write the error message of failed coercions to include the operator's information
    match result {
        None => Err(DataFusionError::Plan(
            format!(
                "{lhs_type:?} {op} {rhs_type:?} can't be evaluated because there isn't a common type to coerce the types to"
            ),
        )),
        Some(t) => Ok(t)
    }
}

/// Coercion rules for mathematics operators between decimal and non-decimal types.
pub fn math_decimal_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> (Option<DataType>, Option<DataType>) {
    use arrow::datatypes::DataType::*;

    if both_decimal(lhs_type, rhs_type) {
        return (None, None);
    }

    match (lhs_type, rhs_type) {
        (Null, dec_type @ Decimal128(_, _)) => (Some(dec_type.clone()), None),
        (dec_type @ Decimal128(_, _), Null) => (None, Some(dec_type.clone())),
        (Dictionary(key_type, value_type), _) => {
            let (value_type, rhs_type) = math_decimal_coercion(value_type, rhs_type);
            let lhs_type = value_type
                .map(|value_type| Dictionary(key_type.clone(), Box::new(value_type)));
            (lhs_type, rhs_type)
        }
        (_, Dictionary(key_type, value_type)) => {
            let (lhs_type, value_type) = math_decimal_coercion(lhs_type, value_type);
            let rhs_type = value_type
                .map(|value_type| Dictionary(key_type.clone(), Box::new(value_type)));
            (lhs_type, rhs_type)
        }
        (Decimal128(_, _), Float32 | Float64) => (Some(Float64), Some(Float64)),
        (Float32 | Float64, Decimal128(_, _)) => (Some(Float64), Some(Float64)),
        (Decimal128(_, _), _) => {
            let converted_decimal_type = coerce_numeric_type_to_decimal(rhs_type);
            (None, converted_decimal_type)
        }
        (_, Decimal128(_, _)) => {
            let converted_decimal_type = coerce_numeric_type_to_decimal(lhs_type);
            (converted_decimal_type, None)
        }
        _ => (None, None),
    }
}

/// Returns the output type of applying bitwise operations such as
/// `&`, `|`, or `xor`to arguments of `lhs_type` and `rhs_type`.
pub(crate) fn bitwise_coercion(
    left_type: &DataType,
    right_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    if !both_numeric_or_null_and_numeric(left_type, right_type) {
        return None;
    }

    if left_type == right_type {
        return Some(left_type.clone());
    }

    match (left_type, right_type) {
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (Int64, _)
        | (_, Int64)
        | (UInt32, Int8)
        | (Int8, UInt32)
        | (UInt32, Int16)
        | (Int16, UInt32)
        | (UInt32, Int32)
        | (Int32, UInt32) => Some(Int64),
        (Int32, _)
        | (_, Int32)
        | (UInt16, Int16)
        | (Int16, UInt16)
        | (UInt16, Int8)
        | (Int8, UInt16) => Some(Int32),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (Int16, _) | (_, Int16) | (Int8, UInt8) | (UInt8, Int8) => Some(Int16),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

/// Returns the output type of applying comparison operations such as
/// `eq`, `not eq`, `lt`, `lteq`, `gt`, and `gteq` to arguments
/// of `lhs_type` and `rhs_type`.
pub fn comparison_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    if lhs_type == rhs_type {
        // same type => equality is possible
        return Some(lhs_type.clone());
    }
    comparison_binary_numeric_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_coercion(lhs_type, rhs_type, true))
        .or_else(|| temporal_coercion(lhs_type, rhs_type))
        .or_else(|| string_coercion(lhs_type, rhs_type))
        .or_else(|| null_coercion(lhs_type, rhs_type))
        .or_else(|| string_numeric_coercion(lhs_type, rhs_type))
}

/// Returns the output type of applying numeric operations such as `=`
/// to arguments `lhs_type` and `rhs_type` if one is numeric and one
/// is `Utf8`/`LargeUtf8`.
fn string_numeric_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Utf8, _) if DataType::is_numeric(rhs_type) => Some(Utf8),
        (LargeUtf8, _) if DataType::is_numeric(rhs_type) => Some(LargeUtf8),
        (_, Utf8) if DataType::is_numeric(lhs_type) => Some(Utf8),
        (_, LargeUtf8) if DataType::is_numeric(lhs_type) => Some(LargeUtf8),
        _ => None,
    }
}

/// Returns the output type of applying numeric operations such as `=`
/// to arguments `lhs_type` and `rhs_type` if both are numeric
fn comparison_binary_numeric_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return None;
    };

    // same type => all good
    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    // these are ordered from most informative to least informative so
    // that the coercion does not lose information via truncation
    match (lhs_type, rhs_type) {
        // support decimal data type for comparison operation
        (d1 @ Decimal128(_, _), d2 @ Decimal128(_, _)) => get_wider_decimal_type(d1, d2),
        (Decimal128(_, _), _) => get_comparison_common_decimal_type(lhs_type, rhs_type),
        (_, Decimal128(_, _)) => get_comparison_common_decimal_type(rhs_type, lhs_type),
        (Float64, _) | (_, Float64) => Some(Float64),
        (_, Float32) | (Float32, _) => Some(Float32),
        // The following match arms encode the following logic: Given the two
        // integral types, we choose the narrowest possible integral type that
        // accommodates all values of both types. Note that some information
        // loss is inevitable when we have a signed type and a `UInt64`, in
        // which case we use `Int64`;i.e. the widest signed integral type.
        (Int64, _)
        | (_, Int64)
        | (UInt64, Int8)
        | (Int8, UInt64)
        | (UInt64, Int16)
        | (Int16, UInt64)
        | (UInt64, Int32)
        | (Int32, UInt64)
        | (UInt32, Int8)
        | (Int8, UInt32)
        | (UInt32, Int16)
        | (Int16, UInt32)
        | (UInt32, Int32)
        | (Int32, UInt32) => Some(Int64),
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (Int32, _)
        | (_, Int32)
        | (UInt16, Int16)
        | (Int16, UInt16)
        | (UInt16, Int8)
        | (Int8, UInt16) => Some(Int32),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (Int16, _) | (_, Int16) | (Int8, UInt8) | (UInt8, Int8) => Some(Int16),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

/// Returns the output type of applying numeric operations such as `=`
/// to a decimal type `decimal_type` and `other_type`
fn get_comparison_common_decimal_type(
    decimal_type: &DataType,
    other_type: &DataType,
) -> Option<DataType> {
    let other_decimal_type = &match other_type {
        // This conversion rule is from spark
        // https://github.com/apache/spark/blob/1c81ad20296d34f137238dadd67cc6ae405944eb/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L127
        DataType::Int8 => DataType::Decimal128(3, 0),
        DataType::Int16 => DataType::Decimal128(5, 0),
        DataType::Int32 => DataType::Decimal128(10, 0),
        DataType::Int64 => DataType::Decimal128(20, 0),
        DataType::Float32 => DataType::Decimal128(14, 7),
        DataType::Float64 => DataType::Decimal128(30, 15),
        _ => {
            return None;
        }
    };
    match (decimal_type, &other_decimal_type) {
        (d1 @ DataType::Decimal128(_, _), d2 @ DataType::Decimal128(_, _)) => {
            get_wider_decimal_type(d1, d2)
        }
        _ => None,
    }
}

/// Returns a `DataType::Decimal128` that can store any value from either
/// `lhs_decimal_type` and `rhs_decimal_type`
///
/// The result decimal type is `(max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2))`.
fn get_wider_decimal_type(
    lhs_decimal_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    match (lhs_decimal_type, rhs_type) {
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            // max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2)
            let s = *s1.max(s2);
            let range = (*p1 as i8 - s1).max(*p2 as i8 - s2);
            Some(create_decimal_type((range + s) as u8, s))
        }
        (_, _) => None,
    }
}

/// Convert the numeric data type to the decimal data type.
/// Now, we just support the signed integer type and floating-point type.
fn coerce_numeric_type_to_decimal(numeric_type: &DataType) -> Option<DataType> {
    match numeric_type {
        DataType::Int8 => Some(DataType::Decimal128(3, 0)),
        DataType::Int16 => Some(DataType::Decimal128(5, 0)),
        DataType::Int32 => Some(DataType::Decimal128(10, 0)),
        DataType::Int64 => Some(DataType::Decimal128(20, 0)),
        // TODO if we convert the floating-point data to the decimal type, it maybe overflow.
        DataType::Float32 => Some(DataType::Decimal128(14, 7)),
        DataType::Float64 => Some(DataType::Decimal128(30, 15)),
        _ => None,
    }
}

/// Returns the output type of applying mathematics operations such as
/// `+` to arguments of `lhs_type` and `rhs_type`.
fn mathematics_numerical_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    // error on any non-numeric type
    if !both_numeric_or_null_and_numeric(lhs_type, rhs_type) {
        return None;
    };

    // same type => all good
    // TODO: remove this
    // bug: https://github.com/apache/arrow-datafusion/issues/3387
    if lhs_type == rhs_type
        && !(matches!(lhs_type, DataType::Dictionary(_, _))
            || matches!(rhs_type, DataType::Dictionary(_, _)))
    {
        return Some(lhs_type.clone());
    }

    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    match (lhs_type, rhs_type) {
        (Dictionary(_, lhs_value_type), Dictionary(_, rhs_value_type)) => {
            mathematics_numerical_coercion(lhs_value_type, rhs_value_type)
        }
        (Dictionary(key_type, value_type), _) => {
            let value_type = mathematics_numerical_coercion(value_type, rhs_type);
            value_type
                .map(|value_type| Dictionary(key_type.clone(), Box::new(value_type)))
        }
        (_, Dictionary(_, value_type)) => {
            mathematics_numerical_coercion(lhs_type, value_type)
        }
        (Float64, _) | (_, Float64) => Some(Float64),
        (_, Float32) | (Float32, _) => Some(Float32),
        (Int64, _) | (_, Int64) => Some(Int64),
        (Int32, _) | (_, Int32) => Some(Int32),
        (Int16, _) | (_, Int16) => Some(Int16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

fn create_decimal_type(precision: u8, scale: i8) -> DataType {
    DataType::Decimal128(
        DECIMAL128_MAX_PRECISION.min(precision),
        DECIMAL128_MAX_SCALE.min(scale),
    )
}

/// Returns the coerced type of applying mathematics operations on decimal types.
/// Two sides of the mathematics operation will be coerced to the same type. Note
/// that we don't coerce the decimal operands in analysis phase, but do it in the
/// execution phase because this is not idempotent.
pub fn coercion_decimal_mathematics_type(
    mathematics_op: &Operator,
    left_decimal_type: &DataType,
    right_decimal_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (left_decimal_type, right_decimal_type) {
        // The promotion rule from spark
        // https://github.com/apache/spark/blob/c20af535803a7250fef047c2bf0fe30be242369d/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/DecimalPrecision.scala#L35
        (Decimal128(_, _), Decimal128(_, _)) => match mathematics_op {
            Operator::Plus | Operator::Minus => decimal_op_mathematics_type(
                mathematics_op,
                left_decimal_type,
                right_decimal_type,
            ),
            Operator::Multiply | Operator::Divide | Operator::Modulo => {
                get_wider_decimal_type(left_decimal_type, right_decimal_type)
            }
            _ => None,
        },
        _ => None,
    }
}

/// Returns the output type of applying mathematics operations on decimal types.
/// The rule is from spark. Note that this is different to the coerced type applied
/// to two sides of the arithmetic operation.
pub fn decimal_op_mathematics_type(
    mathematics_op: &Operator,
    left_decimal_type: &DataType,
    right_decimal_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (left_decimal_type, right_decimal_type) {
        // The coercion rule from spark
        // https://github.com/apache/spark/blob/c20af535803a7250fef047c2bf0fe30be242369d/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/DecimalPrecision.scala#L35
        (Decimal128(p1, s1), Decimal128(p2, s2)) => {
            match mathematics_op {
                Operator::Plus | Operator::Minus => {
                    // max(s1, s2)
                    let result_scale = *s1.max(s2);
                    // max(s1, s2) + max(p1-s1, p2-s2) + 1
                    let result_precision =
                        result_scale + (*p1 as i8 - *s1).max(*p2 as i8 - *s2) + 1;
                    Some(create_decimal_type(result_precision as u8, result_scale))
                }
                Operator::Multiply => {
                    // s1 + s2
                    let result_scale = *s1 + *s2;
                    // p1 + p2 + 1
                    let result_precision = *p1 + *p2 + 1;
                    Some(create_decimal_type(result_precision, result_scale))
                }
                Operator::Divide => {
                    // max(6, s1 + p2 + 1)
                    let result_scale = 6.max(*s1 + *p2 as i8 + 1);
                    // p1 - s1 + s2 + max(6, s1 + p2 + 1)
                    let result_precision = result_scale + *p1 as i8 - *s1 + *s2;
                    Some(create_decimal_type(result_precision as u8, result_scale))
                }
                Operator::Modulo => {
                    // max(s1, s2)
                    let result_scale = *s1.max(s2);
                    // min(p1-s1, p2-s2) + max(s1, s2)
                    let result_precision =
                        result_scale + (*p1 as i8 - *s1).min(*p2 as i8 - *s2);
                    Some(create_decimal_type(result_precision as u8, result_scale))
                }
                _ => None,
            }
        }
        (Dictionary(_, lhs_value_type), Dictionary(_, rhs_value_type)) => {
            decimal_op_mathematics_type(
                mathematics_op,
                lhs_value_type.as_ref(),
                rhs_value_type.as_ref(),
            )
        }
        (Dictionary(key_type, value_type), _) => {
            let value_type = decimal_op_mathematics_type(
                mathematics_op,
                value_type.as_ref(),
                right_decimal_type,
            );
            value_type
                .map(|value_type| Dictionary(key_type.clone(), Box::new(value_type)))
        }
        (_, Dictionary(_, value_type)) => decimal_op_mathematics_type(
            mathematics_op,
            left_decimal_type,
            value_type.as_ref(),
        ),
        _ => None,
    }
}

/// Determine if at least of one of lhs and rhs is numeric, and the other must be NULL or numeric
fn both_numeric_or_null_and_numeric(lhs_type: &DataType, rhs_type: &DataType) -> bool {
    match (lhs_type, rhs_type) {
        (_, DataType::Null) => is_numeric(lhs_type),
        (DataType::Null, _) => is_numeric(rhs_type),
        (
            DataType::Dictionary(_, lhs_value_type),
            DataType::Dictionary(_, rhs_value_type),
        ) => is_numeric(lhs_value_type) && is_numeric(rhs_value_type),
        (DataType::Dictionary(_, value_type), _) => {
            is_numeric(value_type) && is_numeric(rhs_type)
        }
        (_, DataType::Dictionary(_, value_type)) => {
            is_numeric(lhs_type) && is_numeric(value_type)
        }
        _ => is_numeric(lhs_type) && is_numeric(rhs_type),
    }
}

/// Determine if at least of one of lhs and rhs is decimal, and the other must be NULL or decimal
fn both_decimal(lhs_type: &DataType, rhs_type: &DataType) -> bool {
    match (lhs_type, rhs_type) {
        (_, DataType::Null) => is_decimal(lhs_type),
        (DataType::Null, _) => is_decimal(rhs_type),
        (DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => true,
        (DataType::Dictionary(_, value_type), _) => {
            is_decimal(value_type) && is_decimal(rhs_type)
        }
        (_, DataType::Dictionary(_, value_type)) => {
            is_decimal(lhs_type) && is_decimal(value_type)
        }
        _ => false,
    }
}

/// Determine if at least of one of lhs and rhs is decimal
pub fn any_decimal(lhs_type: &DataType, rhs_type: &DataType) -> bool {
    match (lhs_type, rhs_type) {
        (DataType::Dictionary(_, value_type), _) => {
            is_decimal(value_type) || is_decimal(rhs_type)
        }
        (_, DataType::Dictionary(_, value_type)) => {
            is_decimal(lhs_type) || is_decimal(value_type)
        }
        (_, _) => is_decimal(lhs_type) || is_decimal(rhs_type),
    }
}

/// Coercion rules for Dictionaries: the type that both lhs and rhs
/// can be casted to for the purpose of a computation.
///
/// Not all operators support dictionaries, if `preserve_dictionaries` is true
/// dictionaries will be preserved if possible
fn dictionary_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
    preserve_dictionaries: bool,
) -> Option<DataType> {
    match (lhs_type, rhs_type) {
        (
            DataType::Dictionary(_lhs_index_type, lhs_value_type),
            DataType::Dictionary(_rhs_index_type, rhs_value_type),
        ) => comparison_coercion(lhs_value_type, rhs_value_type),
        (d @ DataType::Dictionary(_, value_type), other_type)
        | (other_type, d @ DataType::Dictionary(_, value_type))
            if preserve_dictionaries && value_type.as_ref() == other_type =>
        {
            Some(d.clone())
        }
        (DataType::Dictionary(_index_type, value_type), _) => {
            comparison_coercion(value_type, rhs_type)
        }
        (_, DataType::Dictionary(_index_type, value_type)) => {
            comparison_coercion(lhs_type, value_type)
        }
        _ => None,
    }
}

/// Coercion rules for string concat.
/// This is a union of string coercion rules and specified rules:
/// 1. At lease one side of lhs and rhs should be string type (Utf8 / LargeUtf8)
/// 2. Data type of the other side should be able to cast to string type
fn string_concat_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    string_coercion(lhs_type, rhs_type).or(match (lhs_type, rhs_type) {
        (Utf8, from_type) | (from_type, Utf8) => {
            string_concat_internal_coercion(from_type, &Utf8)
        }
        (LargeUtf8, from_type) | (from_type, LargeUtf8) => {
            string_concat_internal_coercion(from_type, &LargeUtf8)
        }
        _ => None,
    })
}

fn string_concat_internal_coercion(
    from_type: &DataType,
    to_type: &DataType,
) -> Option<DataType> {
    if can_cast_types(from_type, to_type) {
        Some(to_type.to_owned())
    } else {
        None
    }
}

/// Coercion rules for Strings: the type that both lhs and rhs can be
/// casted to for the purpose of a string computation
fn string_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Utf8, Utf8) => Some(Utf8),
        (LargeUtf8, Utf8) => Some(LargeUtf8),
        (Utf8, LargeUtf8) => Some(LargeUtf8),
        (LargeUtf8, LargeUtf8) => Some(LargeUtf8),
        _ => None,
    }
}

/// coercion rules for like operations.
/// This is a union of string coercion rules and dictionary coercion rules
pub fn like_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    string_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_coercion(lhs_type, rhs_type, false))
        .or_else(|| null_coercion(lhs_type, rhs_type))
}

/// coercion rules for regular expression comparison operations.
/// This is a union of string coercion rules and dictionary coercion rules
pub fn regex_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    string_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_coercion(lhs_type, rhs_type, false))
}

/// Checks if the TimeUnit associated with a Time32 or Time64 type is consistent,
/// as Time32 can only be used to Second and Millisecond accuracy, while Time64
/// is exclusively used to Microsecond and Nanosecond accuracy
fn is_time_with_valid_unit(datatype: DataType) -> bool {
    matches!(
        datatype,
        DataType::Time32(TimeUnit::Second)
            | DataType::Time32(TimeUnit::Millisecond)
            | DataType::Time64(TimeUnit::Microsecond)
            | DataType::Time64(TimeUnit::Nanosecond)
    )
}

/// Coercion rules for Temporal columns: the type that both lhs and rhs can be
/// casted to for the purpose of a date computation
/// For interval arithmetic, it doesn't handle datetime type +/- interval
fn temporal_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    use arrow::datatypes::IntervalUnit::*;
    match (lhs_type, rhs_type) {
        // interval +/-
        (Interval(YearMonth), Interval(YearMonth)) => Some(Interval(YearMonth)),
        (Interval(DayTime), Interval(DayTime)) => Some(Interval(DayTime)),
        (Interval(_), Interval(_)) => Some(Interval(MonthDayNano)),
        (Date64, Date32) | (Date32, Date64) => Some(Date64),
        (Utf8, Date32) | (Date32, Utf8) => Some(Date32),
        (Utf8, Date64) | (Date64, Utf8) => Some(Date64),
        (Utf8, Time32(unit)) | (Time32(unit), Utf8) => {
            match is_time_with_valid_unit(Time32(unit.clone())) {
                false => None,
                true => Some(Time32(unit.clone())),
            }
        }
        (Utf8, Time64(unit)) | (Time64(unit), Utf8) => {
            match is_time_with_valid_unit(Time64(unit.clone())) {
                false => None,
                true => Some(Time64(unit.clone())),
            }
        }
        (Timestamp(_, tz), Utf8) | (Utf8, Timestamp(_, tz)) => {
            Some(Timestamp(TimeUnit::Nanosecond, tz.clone()))
        }
        (Timestamp(_, None), Date32) | (Date32, Timestamp(_, None)) => {
            Some(Timestamp(TimeUnit::Nanosecond, None))
        }
        (Timestamp(_, _tz), Date32) | (Date32, Timestamp(_, _tz)) => {
            Some(Timestamp(TimeUnit::Nanosecond, None))
        }
        (Timestamp(lhs_unit, lhs_tz), Timestamp(rhs_unit, rhs_tz)) => {
            let tz = match (lhs_tz, rhs_tz) {
                // can't cast across timezones
                (Some(lhs_tz), Some(rhs_tz)) => {
                    if lhs_tz != rhs_tz {
                        return None;
                    } else {
                        Some(lhs_tz.clone())
                    }
                }
                (Some(lhs_tz), None) => Some(lhs_tz.clone()),
                (None, Some(rhs_tz)) => Some(rhs_tz.clone()),
                (None, None) => None,
            };

            let unit = match (lhs_unit, rhs_unit) {
                (TimeUnit::Second, TimeUnit::Millisecond) => TimeUnit::Second,
                (TimeUnit::Second, TimeUnit::Microsecond) => TimeUnit::Second,
                (TimeUnit::Second, TimeUnit::Nanosecond) => TimeUnit::Second,
                (TimeUnit::Millisecond, TimeUnit::Second) => TimeUnit::Second,
                (TimeUnit::Millisecond, TimeUnit::Microsecond) => TimeUnit::Millisecond,
                (TimeUnit::Millisecond, TimeUnit::Nanosecond) => TimeUnit::Millisecond,
                (TimeUnit::Microsecond, TimeUnit::Second) => TimeUnit::Second,
                (TimeUnit::Microsecond, TimeUnit::Millisecond) => TimeUnit::Millisecond,
                (TimeUnit::Microsecond, TimeUnit::Nanosecond) => TimeUnit::Microsecond,
                (TimeUnit::Nanosecond, TimeUnit::Second) => TimeUnit::Second,
                (TimeUnit::Nanosecond, TimeUnit::Millisecond) => TimeUnit::Millisecond,
                (TimeUnit::Nanosecond, TimeUnit::Microsecond) => TimeUnit::Microsecond,
                (l, r) => {
                    assert_eq!(l, r);
                    l.clone()
                }
            };

            Some(Timestamp(unit, tz))
        }
        _ => None,
    }
}

/// coercion rules from NULL type. Since NULL can be casted to most of types in arrow,
/// either lhs or rhs is NULL, if NULL can be casted to type of the other side, the coercion is valid.
fn null_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    match (lhs_type, rhs_type) {
        (DataType::Null, other_type) | (other_type, DataType::Null) => {
            if can_cast_types(&DataType::Null, other_type) {
                Some(other_type.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use datafusion_common::assert_contains;
    use datafusion_common::DataFusionError;
    use datafusion_common::Result;

    use crate::Operator;

    use super::*;

    #[test]
    fn test_coercion_error() -> Result<()> {
        let result_type =
            coerce_types(&DataType::Float32, &Operator::Plus, &DataType::Utf8);

        if let Err(DataFusionError::Plan(e)) = result_type {
            assert_eq!(e, "Float32 + Utf8 can't be evaluated because there isn't a common type to coerce the types to");
            Ok(())
        } else {
            Err(DataFusionError::Internal(
                "Coercion should have returned an DataFusionError::Internal".to_string(),
            ))
        }
    }

    #[test]
    fn test_decimal_binary_comparison_coercion() -> Result<()> {
        let input_decimal = DataType::Decimal128(20, 3);
        let input_types = [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(38, 10),
            DataType::Decimal128(20, 8),
            DataType::Null,
        ];
        let result_types = [
            DataType::Decimal128(20, 3),
            DataType::Decimal128(20, 3),
            DataType::Decimal128(20, 3),
            DataType::Decimal128(23, 3),
            DataType::Decimal128(24, 7),
            DataType::Decimal128(32, 15),
            DataType::Decimal128(38, 10),
            DataType::Decimal128(25, 8),
            DataType::Decimal128(20, 3),
        ];
        let comparison_op_types = [
            Operator::NotEq,
            Operator::Eq,
            Operator::Gt,
            Operator::GtEq,
            Operator::Lt,
            Operator::LtEq,
        ];
        for (i, input_type) in input_types.iter().enumerate() {
            let expect_type = &result_types[i];
            for op in comparison_op_types {
                let result_type = coerce_types(&input_decimal, &op, input_type)?;
                assert_eq!(expect_type, &result_type);
            }
        }
        // negative test
        let result_type = coerce_types(&input_decimal, &Operator::Eq, &DataType::Boolean);
        assert!(result_type.is_err());
        Ok(())
    }

    #[test]
    fn test_decimal_mathematics_op_type() {
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int8).unwrap(),
            DataType::Decimal128(3, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int16).unwrap(),
            DataType::Decimal128(5, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int32).unwrap(),
            DataType::Decimal128(10, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int64).unwrap(),
            DataType::Decimal128(20, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Float32).unwrap(),
            DataType::Decimal128(14, 7)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Float64).unwrap(),
            DataType::Decimal128(30, 15)
        );

        let op = Operator::Plus;
        let left_decimal_type = DataType::Decimal128(10, 3);
        let right_decimal_type = DataType::Decimal128(20, 4);
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal128(21, 4), result.unwrap());
        let op = Operator::Minus;
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal128(21, 4), result.unwrap());
        let op = Operator::Multiply;
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal128(20, 4), result.unwrap());
        let result =
            decimal_op_mathematics_type(&op, &left_decimal_type, &right_decimal_type);
        assert_eq!(DataType::Decimal128(31, 7), result.unwrap());
        let op = Operator::Divide;
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal128(20, 4), result.unwrap());
        let result =
            decimal_op_mathematics_type(&op, &left_decimal_type, &right_decimal_type);
        assert_eq!(DataType::Decimal128(35, 24), result.unwrap());
        let op = Operator::Modulo;
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal128(20, 4), result.unwrap());
        let result =
            decimal_op_mathematics_type(&op, &left_decimal_type, &right_decimal_type);
        assert_eq!(DataType::Decimal128(11, 4), result.unwrap());
    }

    #[test]
    fn test_dictionary_type_coercion() {
        use DataType::*;

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Int32));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, true), Some(Int32));
        assert_eq!(
            dictionary_coercion(&lhs_type, &rhs_type, false),
            Some(Int32)
        );

        // Since we can coerce values of Int16 to Utf8 can support this
        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, true), Some(Utf8));

        // Can not coerce values of Binary to int,  cannot support this
        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Binary));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, true), None);

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Utf8;
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, false), Some(Utf8));
        assert_eq!(
            dictionary_coercion(&lhs_type, &rhs_type, true),
            Some(lhs_type.clone())
        );

        let lhs_type = Utf8;
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, false), Some(Utf8));
        assert_eq!(
            dictionary_coercion(&lhs_type, &rhs_type, true),
            Some(rhs_type.clone())
        );
    }

    macro_rules! test_coercion_binary_rule {
        ($A_TYPE:expr, $B_TYPE:expr, $OP:expr, $C_TYPE:expr) => {{
            let result = coerce_types(&$A_TYPE, &$OP, &$B_TYPE)?;
            assert_eq!(result, $C_TYPE);
        }};
    }

    #[test]
    fn test_date_timestamp_arithmetic_error() -> Result<()> {
        let common_type = coerce_types(
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            &Operator::Minus,
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )?;
        assert_eq!(common_type.to_string(), "Timestamp(Millisecond, None)");

        let err = coerce_types(&DataType::Date32, &Operator::Plus, &DataType::Date64)
            .unwrap_err()
            .to_string();
        assert_contains!(&err, "Date32 + Date64 can't be evaluated because there isn't a common type to coerce the types to");

        Ok(())
    }

    #[test]
    fn test_type_coercion() -> Result<()> {
        // test like coercion rule
        let result = like_coercion(&DataType::Utf8, &DataType::Utf8);
        assert_eq!(result, Some(DataType::Utf8));

        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Date32,
            Operator::Eq,
            DataType::Date32
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Date64,
            Operator::Lt,
            DataType::Date64
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Time32(TimeUnit::Second),
            Operator::Eq,
            DataType::Time32(TimeUnit::Second)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Time32(TimeUnit::Millisecond),
            Operator::Eq,
            DataType::Time32(TimeUnit::Millisecond)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Time64(TimeUnit::Microsecond),
            Operator::Eq,
            DataType::Time64(TimeUnit::Microsecond)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Time64(TimeUnit::Nanosecond),
            Operator::Eq,
            DataType::Time64(TimeUnit::Nanosecond)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Second, None),
            Operator::Lt,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            Operator::Lt,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Operator::Lt,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            Operator::Lt,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8,
            Operator::RegexMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8,
            Operator::RegexNotMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8,
            Operator::RegexNotIMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8,
            Operator::RegexMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8,
            Operator::RegexIMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8,
            Operator::RegexNotMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
            DataType::Utf8,
            Operator::RegexNotIMatch,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Int16,
            DataType::Int64,
            Operator::BitwiseAnd,
            DataType::Int64
        );
        test_coercion_binary_rule!(
            DataType::UInt64,
            DataType::UInt64,
            Operator::BitwiseAnd,
            DataType::UInt64
        );
        test_coercion_binary_rule!(
            DataType::Int8,
            DataType::UInt32,
            Operator::BitwiseAnd,
            DataType::Int64
        );
        test_coercion_binary_rule!(
            DataType::UInt32,
            DataType::Int32,
            Operator::BitwiseAnd,
            DataType::Int64
        );
        test_coercion_binary_rule!(
            DataType::UInt16,
            DataType::Int16,
            Operator::BitwiseAnd,
            DataType::Int32
        );
        test_coercion_binary_rule!(
            DataType::UInt32,
            DataType::UInt32,
            Operator::BitwiseAnd,
            DataType::UInt32
        );
        test_coercion_binary_rule!(
            DataType::UInt16,
            DataType::UInt32,
            Operator::BitwiseAnd,
            DataType::UInt32
        );
        Ok(())
    }

    #[test]
    fn test_type_coercion_arithmetic() -> Result<()> {
        // integer
        test_coercion_binary_rule!(
            DataType::Int32,
            DataType::UInt32,
            Operator::Plus,
            DataType::Int32
        );
        test_coercion_binary_rule!(
            DataType::Int32,
            DataType::UInt16,
            Operator::Minus,
            DataType::Int32
        );
        test_coercion_binary_rule!(
            DataType::Int8,
            DataType::Int64,
            Operator::Multiply,
            DataType::Int64
        );
        // float
        test_coercion_binary_rule!(
            DataType::Float32,
            DataType::Int32,
            Operator::Plus,
            DataType::Float32
        );
        test_coercion_binary_rule!(
            DataType::Float32,
            DataType::Float64,
            Operator::Multiply,
            DataType::Float64
        );
        // TODO add other data type
        Ok(())
    }

    fn test_math_decimal_coercion_rule(
        lhs_type: DataType,
        rhs_type: DataType,
        mathematics_op: Operator,
        expected_lhs_type: Option<DataType>,
        expected_rhs_type: Option<DataType>,
        expected_coerced_type: DataType,
        expected_output_type: DataType,
    ) {
        // The coerced types for lhs and rhs, if any of them is not decimal
        let (l, r) = math_decimal_coercion(&lhs_type, &rhs_type);
        assert_eq!(l, expected_lhs_type);
        assert_eq!(r, expected_rhs_type);

        let lhs_type = l.unwrap_or(lhs_type);
        let rhs_type = r.unwrap_or(rhs_type);

        // The coerced type of decimal math expression, applied during expression evaluation
        let coerced_type =
            coercion_decimal_mathematics_type(&mathematics_op, &lhs_type, &rhs_type)
                .unwrap();
        assert_eq!(coerced_type, expected_coerced_type);

        // The output type of decimal math expression
        let output_type =
            decimal_op_mathematics_type(&mathematics_op, &lhs_type, &rhs_type).unwrap();
        assert_eq!(output_type, expected_output_type);
    }

    #[test]
    fn test_coercion_arithmetic_decimal() -> Result<()> {
        test_math_decimal_coercion_rule(
            DataType::Decimal128(10, 2),
            DataType::Decimal128(10, 2),
            Operator::Plus,
            None,
            None,
            DataType::Decimal128(11, 2),
            DataType::Decimal128(11, 2),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            Operator::Plus,
            Some(DataType::Decimal128(10, 0)),
            None,
            DataType::Decimal128(13, 2),
            DataType::Decimal128(13, 2),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            Operator::Minus,
            Some(DataType::Decimal128(10, 0)),
            None,
            DataType::Decimal128(13, 2),
            DataType::Decimal128(13, 2),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            Operator::Multiply,
            Some(DataType::Decimal128(10, 0)),
            None,
            DataType::Decimal128(12, 2),
            DataType::Decimal128(21, 2),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            Operator::Divide,
            Some(DataType::Decimal128(10, 0)),
            None,
            DataType::Decimal128(12, 2),
            DataType::Decimal128(23, 11),
        );

        test_math_decimal_coercion_rule(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            Operator::Modulo,
            Some(DataType::Decimal128(10, 0)),
            None,
            DataType::Decimal128(12, 2),
            DataType::Decimal128(10, 2),
        );

        Ok(())
    }

    #[test]
    fn test_type_coercion_compare() -> Result<()> {
        // boolean
        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Boolean,
            Operator::Eq,
            DataType::Boolean
        );
        // float
        test_coercion_binary_rule!(
            DataType::Float32,
            DataType::Int64,
            Operator::Eq,
            DataType::Float32
        );
        test_coercion_binary_rule!(
            DataType::Float32,
            DataType::Float64,
            Operator::GtEq,
            DataType::Float64
        );
        // signed integer
        test_coercion_binary_rule!(
            DataType::Int8,
            DataType::Int32,
            Operator::LtEq,
            DataType::Int32
        );
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Int32,
            Operator::LtEq,
            DataType::Int64
        );
        // unsigned integer
        test_coercion_binary_rule!(
            DataType::UInt32,
            DataType::UInt8,
            Operator::Gt,
            DataType::UInt32
        );
        // numeric/decimal
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Decimal128(10, 0),
            Operator::Eq,
            DataType::Decimal128(20, 0)
        );
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Decimal128(10, 2),
            Operator::Lt,
            DataType::Decimal128(22, 2)
        );
        test_coercion_binary_rule!(
            DataType::Float64,
            DataType::Decimal128(10, 3),
            Operator::Gt,
            DataType::Decimal128(30, 15)
        );
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Decimal128(10, 0),
            Operator::Eq,
            DataType::Decimal128(20, 0)
        );
        test_coercion_binary_rule!(
            DataType::Decimal128(14, 2),
            DataType::Decimal128(10, 3),
            Operator::GtEq,
            DataType::Decimal128(15, 3)
        );

        // TODO add other data type
        Ok(())
    }

    #[test]
    fn test_type_coercion_logical_op() -> Result<()> {
        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Boolean,
            Operator::And,
            DataType::Boolean
        );

        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Boolean,
            Operator::Or,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Null,
            Operator::And,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Null,
            Operator::Or,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Null,
            DataType::Null,
            Operator::Or,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Null,
            DataType::Null,
            Operator::And,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Null,
            DataType::Boolean,
            Operator::And,
            DataType::Boolean
        );
        test_coercion_binary_rule!(
            DataType::Null,
            DataType::Boolean,
            Operator::Or,
            DataType::Boolean
        );
        Ok(())
    }
}
