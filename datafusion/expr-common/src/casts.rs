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

//! Utilities for casting scalar literals to different data types
//!
//! This module contains functions for casting ScalarValue literals
//! to different data types, originally extracted from the optimizer's
//! unwrap_cast module to be shared between logical and physical layers.

use std::cmp::Ordering;

use arrow::datatypes::{
    DataType, MAX_DECIMAL32_FOR_EACH_PRECISION, MAX_DECIMAL64_FOR_EACH_PRECISION,
    MAX_DECIMAL128_FOR_EACH_PRECISION, MIN_DECIMAL32_FOR_EACH_PRECISION,
    MIN_DECIMAL64_FOR_EACH_PRECISION, MIN_DECIMAL128_FOR_EACH_PRECISION, TimeUnit,
};
use arrow::temporal_conversions::{MICROSECONDS, MILLISECONDS, NANOSECONDS};
use datafusion_common::ScalarValue;

/// Convert a literal value from one data type to another
pub fn try_cast_literal_to_type(
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
        .or_else(|| try_cast_binary(lit_value, target_type))
}

/// Returns true if unwrap_cast_in_comparison supports this data type
pub fn is_supported_type(data_type: &DataType) -> bool {
    is_supported_numeric_type(data_type)
        || is_supported_string_type(data_type)
        || is_supported_dictionary_type(data_type)
        || is_supported_binary_type(data_type)
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
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
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

fn is_supported_binary_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Binary | DataType::FixedSizeBinary(_))
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
        DataType::Decimal32(_, scale) => 10_i128.pow(*scale as u32),
        DataType::Decimal64(_, scale) => 10_i128.pow(*scale as u32),
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
        DataType::Decimal32(precision, _) => (
            // Different precision for decimal32 can store different range of value.
            // For example, the precision is 3, the max of value is `999` and the min
            // value is `-999`
            MIN_DECIMAL32_FOR_EACH_PRECISION[*precision as usize] as i128,
            MAX_DECIMAL32_FOR_EACH_PRECISION[*precision as usize] as i128,
        ),
        DataType::Decimal64(precision, _) => (
            // Different precision for decimal64 can store different range of value.
            // For example, the precision is 3, the max of value is `999` and the min
            // value is `-999`
            MIN_DECIMAL64_FOR_EACH_PRECISION[*precision as usize] as i128,
            MAX_DECIMAL64_FOR_EACH_PRECISION[*precision as usize] as i128,
        ),
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
        ScalarValue::Decimal32(Some(v), _, scale) => {
            let v = *v as i128;
            let lit_scale_mul = 10_i128.pow(*scale as u32);
            if mul >= lit_scale_mul {
                // Example:
                // lit is decimal(123,3,2)
                // target type is decimal(5,3)
                // the lit can be converted to the decimal(1230,5,3)
                v.checked_mul(mul / lit_scale_mul)
            } else if v % (lit_scale_mul / mul) == 0 {
                // Example:
                // lit is decimal(123000,10,3)
                // target type is int32: the lit can be converted to INT32(123)
                // target type is decimal(10,2): the lit can be converted to decimal(12300,10,2)
                Some(v / (lit_scale_mul / mul))
            } else {
                // can't convert the lit decimal to the target data type
                None
            }
        }
        ScalarValue::Decimal64(Some(v), _, scale) => {
            let v = *v as i128;
            let lit_scale_mul = 10_i128.pow(*scale as u32);
            if mul >= lit_scale_mul {
                // Example:
                // lit is decimal(123,3,2)
                // target type is decimal(5,3)
                // the lit can be converted to the decimal(1230,5,3)
                v.checked_mul(mul / lit_scale_mul)
            } else if v % (lit_scale_mul / mul) == 0 {
                // Example:
                // lit is decimal(123000,10,3)
                // target type is int32: the lit can be converted to INT32(123)
                // target type is decimal(10,2): the lit can be converted to decimal(12300,10,2)
                Some(v / (lit_scale_mul / mul))
            } else {
                // can't convert the lit decimal to the target data type
                None
            }
        }
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
                    DataType::Decimal32(p, s) => {
                        ScalarValue::Decimal32(Some(value as i32), *p, *s)
                    }
                    DataType::Decimal64(p, s) => {
                        ScalarValue::Decimal64(Some(value as i64), *p, *s)
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

fn try_cast_binary(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    match (lit_value, target_type) {
        (ScalarValue::Binary(Some(v)), DataType::FixedSizeBinary(n))
            if v.len() == *n as usize =>
        {
            Some(ScalarValue::FixedSizeBinary(*n, Some(v.clone())))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::compute::{CastOptions, cast_with_options};
    use arrow::datatypes::{Field, Fields, TimeUnit};
    use std::sync::Arc;

    #[derive(Debug, Clone)]
    enum ExpectedCast {
        /// test successfully cast value and it is as specified
        Value(ScalarValue),
        /// test returned OK, but could not cast the value
        NoValue,
    }

    /// Runs try_cast_literal_to_type with the specified inputs and
    /// ensure it computes the expected output, and ensures the
    /// casting is consistent with the Arrow kernels
    fn expect_cast(
        literal: ScalarValue,
        target_type: DataType,
        expected_result: ExpectedCast,
    ) {
        let actual_value = try_cast_literal_to_type(&literal, &target_type);

        println!("expect_cast: ");
        println!("  {literal:?} --> {target_type}");
        println!("  expected_result: {expected_result:?}");
        println!("  actual_result:   {actual_value:?}");

        match expected_result {
            ExpectedCast::Value(expected_value) => {
                let actual_value =
                    actual_value.expect("Expected cast value but got None");

                assert_eq!(actual_value, expected_value);

                // Verify that calling the arrow
                // cast kernel yields the same results
                // input array
                let literal_array = literal
                    .to_array_of_size(1)
                    .expect("Failed to convert to array of size");
                let expected_array = expected_value
                    .to_array_of_size(1)
                    .expect("Failed to convert to array of size");
                let cast_array = cast_with_options(
                    &literal_array,
                    &target_type,
                    &CastOptions::default(),
                )
                .expect("Expected to be cast array with arrow cast kernel");

                assert_eq!(
                    &expected_array, &cast_array,
                    "Result of casting {literal:?} with arrow was\n {cast_array:#?}\nbut expected\n{expected_array:#?}"
                );

                // Verify that for timestamp types the timezones are the same
                // (ScalarValue::cmp doesn't account for timezones);
                if let (
                    DataType::Timestamp(left_unit, left_tz),
                    DataType::Timestamp(right_unit, right_tz),
                ) = (actual_value.data_type(), expected_value.data_type())
                {
                    assert_eq!(left_unit, right_unit);
                    assert_eq!(left_tz, right_tz);
                }
            }
            ExpectedCast::NoValue => {
                assert!(
                    actual_value.is_none(),
                    "Expected no cast value, but got {actual_value:?}"
                );
            }
        }
    }

    #[test]
    fn test_try_cast_to_type_nulls() {
        // test that nulls can be cast to/from all integer types
        let scalars = vec![
            ScalarValue::Int8(None),
            ScalarValue::Int16(None),
            ScalarValue::Int32(None),
            ScalarValue::Int64(None),
            ScalarValue::UInt8(None),
            ScalarValue::UInt16(None),
            ScalarValue::UInt32(None),
            ScalarValue::UInt64(None),
            ScalarValue::Decimal128(None, 3, 0),
            ScalarValue::Decimal128(None, 8, 2),
            ScalarValue::Utf8(None),
            ScalarValue::LargeUtf8(None),
        ];

        for s1 in &scalars {
            for s2 in &scalars {
                let expected_value = ExpectedCast::Value(s2.clone());

                expect_cast(s1.clone(), s2.data_type(), expected_value);
            }
        }
    }

    #[test]
    fn test_try_cast_to_type_int_in_range() {
        // test values that can be cast to/from all integer types
        let scalars = vec![
            ScalarValue::Int8(Some(123)),
            ScalarValue::Int16(Some(123)),
            ScalarValue::Int32(Some(123)),
            ScalarValue::Int64(Some(123)),
            ScalarValue::UInt8(Some(123)),
            ScalarValue::UInt16(Some(123)),
            ScalarValue::UInt32(Some(123)),
            ScalarValue::UInt64(Some(123)),
            ScalarValue::Decimal128(Some(123), 3, 0),
            ScalarValue::Decimal128(Some(12300), 8, 2),
        ];

        for s1 in &scalars {
            for s2 in &scalars {
                let expected_value = ExpectedCast::Value(s2.clone());

                expect_cast(s1.clone(), s2.data_type(), expected_value);
            }
        }

        let max_i32 = ScalarValue::Int32(Some(i32::MAX));
        expect_cast(
            max_i32,
            DataType::UInt64,
            ExpectedCast::Value(ScalarValue::UInt64(Some(i32::MAX as u64))),
        );

        let min_i32 = ScalarValue::Int32(Some(i32::MIN));
        expect_cast(
            min_i32,
            DataType::Int64,
            ExpectedCast::Value(ScalarValue::Int64(Some(i32::MIN as i64))),
        );

        let max_i64 = ScalarValue::Int64(Some(i64::MAX));
        expect_cast(
            max_i64,
            DataType::UInt64,
            ExpectedCast::Value(ScalarValue::UInt64(Some(i64::MAX as u64))),
        );
    }

    #[test]
    fn test_try_cast_to_type_int_out_of_range() {
        let min_i32 = ScalarValue::Int32(Some(i32::MIN));
        let min_i64 = ScalarValue::Int64(Some(i64::MIN));
        let max_i64 = ScalarValue::Int64(Some(i64::MAX));
        let max_u64 = ScalarValue::UInt64(Some(u64::MAX));

        expect_cast(max_i64.clone(), DataType::Int8, ExpectedCast::NoValue);

        expect_cast(max_i64.clone(), DataType::Int16, ExpectedCast::NoValue);

        expect_cast(max_i64, DataType::Int32, ExpectedCast::NoValue);

        expect_cast(max_u64, DataType::Int64, ExpectedCast::NoValue);

        expect_cast(min_i64, DataType::UInt64, ExpectedCast::NoValue);

        expect_cast(min_i32, DataType::UInt64, ExpectedCast::NoValue);

        // decimal out of range
        expect_cast(
            ScalarValue::Decimal128(Some(99999999999999999999999999999999999900), 38, 0),
            DataType::Int64,
            ExpectedCast::NoValue,
        );

        expect_cast(
            ScalarValue::Decimal128(Some(-9999999999999999999999999999999999), 37, 1),
            DataType::Int64,
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_try_decimal_cast_in_range() {
        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(3, 0),
            ExpectedCast::Value(ScalarValue::Decimal128(Some(123), 3, 0)),
        );

        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(8, 0),
            ExpectedCast::Value(ScalarValue::Decimal128(Some(123), 8, 0)),
        );

        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(8, 5),
            ExpectedCast::Value(ScalarValue::Decimal128(Some(12300000), 8, 5)),
        );
    }

    #[test]
    fn test_try_decimal_cast_out_of_range() {
        // decimal would lose precision
        expect_cast(
            ScalarValue::Decimal128(Some(12345), 5, 2),
            DataType::Decimal128(3, 0),
            ExpectedCast::NoValue,
        );

        // decimal would lose precision
        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(2, 0),
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_try_cast_to_type_timestamps() {
        for time_unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            let utc = Some("+00:00".into());
            // No timezone, utc timezone
            let (lit_tz_none, lit_tz_utc) = match time_unit {
                TimeUnit::Second => (
                    ScalarValue::TimestampSecond(Some(12345), None),
                    ScalarValue::TimestampSecond(Some(12345), utc),
                ),

                TimeUnit::Millisecond => (
                    ScalarValue::TimestampMillisecond(Some(12345), None),
                    ScalarValue::TimestampMillisecond(Some(12345), utc),
                ),

                TimeUnit::Microsecond => (
                    ScalarValue::TimestampMicrosecond(Some(12345), None),
                    ScalarValue::TimestampMicrosecond(Some(12345), utc),
                ),

                TimeUnit::Nanosecond => (
                    ScalarValue::TimestampNanosecond(Some(12345), None),
                    ScalarValue::TimestampNanosecond(Some(12345), utc),
                ),
            };

            // DataFusion ignores timezones for comparisons of ScalarValue
            // so double check it here
            assert_eq!(lit_tz_none, lit_tz_utc);

            // e.g. DataType::Timestamp(_, None)
            let dt_tz_none = lit_tz_none.data_type();

            // e.g. DataType::Timestamp(_, Some(utc))
            let dt_tz_utc = lit_tz_utc.data_type();

            // None <--> None
            expect_cast(
                lit_tz_none.clone(),
                dt_tz_none.clone(),
                ExpectedCast::Value(lit_tz_none.clone()),
            );

            // None <--> Utc
            expect_cast(
                lit_tz_none.clone(),
                dt_tz_utc.clone(),
                ExpectedCast::Value(lit_tz_utc.clone()),
            );

            // Utc <--> None
            expect_cast(
                lit_tz_utc.clone(),
                dt_tz_none.clone(),
                ExpectedCast::Value(lit_tz_none.clone()),
            );

            // Utc <--> Utc
            expect_cast(
                lit_tz_utc.clone(),
                dt_tz_utc.clone(),
                ExpectedCast::Value(lit_tz_utc.clone()),
            );

            // timestamp to int64
            expect_cast(
                lit_tz_utc.clone(),
                DataType::Int64,
                ExpectedCast::Value(ScalarValue::Int64(Some(12345))),
            );

            // int64 to timestamp
            expect_cast(
                ScalarValue::Int64(Some(12345)),
                dt_tz_none.clone(),
                ExpectedCast::Value(lit_tz_none.clone()),
            );

            // int64 to timestamp
            expect_cast(
                ScalarValue::Int64(Some(12345)),
                dt_tz_utc.clone(),
                ExpectedCast::Value(lit_tz_utc.clone()),
            );

            // timestamp to string (not supported yet)
            expect_cast(
                lit_tz_utc.clone(),
                DataType::LargeUtf8,
                ExpectedCast::NoValue,
            );
        }
    }

    #[test]
    fn test_try_cast_to_type_unsupported() {
        // int64 to list
        expect_cast(
            ScalarValue::Int64(Some(12345)),
            DataType::List(Arc::new(Field::new("f", DataType::Int32, true))),
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_try_cast_literal_to_timestamp() {
        // same timestamp
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();

        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123456), None)
        );

        // TimestampNanosecond to TimestampMicrosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();

        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMicrosecond(Some(123), None)
        );

        // TimestampNanosecond to TimestampMillisecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();

        assert_eq!(new_scalar, ScalarValue::TimestampMillisecond(Some(0), None));

        // TimestampNanosecond to TimestampSecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Second, None),
        )
        .unwrap();

        assert_eq!(new_scalar, ScalarValue::TimestampSecond(Some(0), None));

        // TimestampMicrosecond to TimestampNanosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMicrosecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();

        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123000), None)
        );

        // TimestampMicrosecond to TimestampMillisecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMicrosecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();

        assert_eq!(new_scalar, ScalarValue::TimestampMillisecond(Some(0), None));

        // TimestampMicrosecond to TimestampSecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMicrosecond(Some(123456789), None),
            &DataType::Timestamp(TimeUnit::Second, None),
        )
        .unwrap();
        assert_eq!(new_scalar, ScalarValue::TimestampSecond(Some(123), None));

        // TimestampMillisecond to TimestampNanosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMillisecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123000000), None)
        );

        // TimestampMillisecond to TimestampMicrosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMillisecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMicrosecond(Some(123000), None)
        );
        // TimestampMillisecond to TimestampSecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMillisecond(Some(123456789), None),
            &DataType::Timestamp(TimeUnit::Second, None),
        )
        .unwrap();
        assert_eq!(new_scalar, ScalarValue::TimestampSecond(Some(123456), None));

        // TimestampSecond to TimestampNanosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123000000000), None)
        );

        // TimestampSecond to TimestampMicrosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMicrosecond(Some(123000000), None)
        );

        // TimestampSecond to TimestampMillisecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMillisecond(Some(123000), None)
        );

        // overflow
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(i64::MAX), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();
        assert_eq!(new_scalar, ScalarValue::TimestampMillisecond(None, None));
    }

    #[test]
    fn test_try_cast_to_string_type() {
        let scalars = vec![
            ScalarValue::from("string"),
            ScalarValue::LargeUtf8(Some("string".to_owned())),
        ];

        for s1 in &scalars {
            for s2 in &scalars {
                let expected_value = ExpectedCast::Value(s2.clone());

                expect_cast(s1.clone(), s2.data_type(), expected_value);
            }
        }
    }

    #[test]
    fn test_try_cast_to_dictionary_type() {
        fn dictionary_type(t: DataType) -> DataType {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(t))
        }
        fn dictionary_value(value: ScalarValue) -> ScalarValue {
            ScalarValue::Dictionary(Box::new(DataType::Int32), Box::new(value))
        }
        let scalars = vec![
            ScalarValue::from("string"),
            ScalarValue::LargeUtf8(Some("string".to_owned())),
        ];
        for s in &scalars {
            expect_cast(
                s.clone(),
                dictionary_type(s.data_type()),
                ExpectedCast::Value(dictionary_value(s.clone())),
            );
            expect_cast(
                dictionary_value(s.clone()),
                s.data_type(),
                ExpectedCast::Value(s.clone()),
            )
        }
    }

    #[test]
    fn test_try_cast_to_fixed_size_binary() {
        expect_cast(
            ScalarValue::Binary(Some(vec![1, 2, 3])),
            DataType::FixedSizeBinary(3),
            ExpectedCast::Value(ScalarValue::FixedSizeBinary(3, Some(vec![1, 2, 3]))),
        )
    }

    #[test]
    fn test_numeric_boundary_values() {
        // Test exact boundary values for signed integers
        expect_cast(
            ScalarValue::Int8(Some(i8::MAX)),
            DataType::UInt8,
            ExpectedCast::Value(ScalarValue::UInt8(Some(i8::MAX as u8))),
        );

        expect_cast(
            ScalarValue::Int8(Some(i8::MIN)),
            DataType::UInt8,
            ExpectedCast::NoValue,
        );

        expect_cast(
            ScalarValue::UInt8(Some(u8::MAX)),
            DataType::Int8,
            ExpectedCast::NoValue,
        );

        // Test cross-type boundary scenarios
        expect_cast(
            ScalarValue::Int32(Some(i32::MAX)),
            DataType::Int64,
            ExpectedCast::Value(ScalarValue::Int64(Some(i32::MAX as i64))),
        );

        expect_cast(
            ScalarValue::Int64(Some(i64::MIN)),
            DataType::UInt64,
            ExpectedCast::NoValue,
        );

        // Test unsigned to signed edge cases
        expect_cast(
            ScalarValue::UInt32(Some(u32::MAX)),
            DataType::Int32,
            ExpectedCast::NoValue,
        );

        expect_cast(
            ScalarValue::UInt64(Some(u64::MAX)),
            DataType::Int64,
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_decimal_precision_limits() {
        use arrow::datatypes::{
            MAX_DECIMAL128_FOR_EACH_PRECISION, MIN_DECIMAL128_FOR_EACH_PRECISION,
        };

        // Test maximum precision values
        expect_cast(
            ScalarValue::Decimal128(Some(MAX_DECIMAL128_FOR_EACH_PRECISION[3]), 3, 0),
            DataType::Decimal128(5, 0),
            ExpectedCast::Value(ScalarValue::Decimal128(
                Some(MAX_DECIMAL128_FOR_EACH_PRECISION[3]),
                5,
                0,
            )),
        );

        // Test minimum precision values
        expect_cast(
            ScalarValue::Decimal128(Some(MIN_DECIMAL128_FOR_EACH_PRECISION[3]), 3, 0),
            DataType::Decimal128(5, 0),
            ExpectedCast::Value(ScalarValue::Decimal128(
                Some(MIN_DECIMAL128_FOR_EACH_PRECISION[3]),
                5,
                0,
            )),
        );

        // Test scale increase
        expect_cast(
            ScalarValue::Decimal128(Some(123), 3, 0),
            DataType::Decimal128(5, 2),
            ExpectedCast::Value(ScalarValue::Decimal128(Some(12300), 5, 2)),
        );

        // Test precision overflow (value too large for target precision)
        expect_cast(
            ScalarValue::Decimal128(Some(MAX_DECIMAL128_FOR_EACH_PRECISION[10]), 10, 0),
            DataType::Decimal128(3, 0),
            ExpectedCast::NoValue,
        );

        // Test non-divisible decimal conversion (should fail)
        expect_cast(
            ScalarValue::Decimal128(Some(12345), 5, 3), // 12.345
            DataType::Int32,
            ExpectedCast::NoValue, // Can't convert 12.345 to integer without loss
        );

        // Test edge case: scale reduction with precision loss
        expect_cast(
            ScalarValue::Decimal128(Some(12345), 5, 2), // 123.45
            DataType::Decimal128(3, 0),                 // Can only hold up to 999
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_timestamp_overflow_scenarios() {
        // Test overflow in timestamp conversions
        let max_seconds = i64::MAX / 1_000_000_000; // Avoid overflow when converting to nanos

        // This should work - within safe range
        expect_cast(
            ScalarValue::TimestampSecond(Some(max_seconds), None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            ExpectedCast::Value(ScalarValue::TimestampNanosecond(
                Some(max_seconds * 1_000_000_000),
                None,
            )),
        );

        // Test very large nanosecond value conversion to smaller units
        expect_cast(
            ScalarValue::TimestampNanosecond(Some(i64::MAX), None),
            DataType::Timestamp(TimeUnit::Second, None),
            ExpectedCast::Value(ScalarValue::TimestampSecond(
                Some(i64::MAX / 1_000_000_000),
                None,
            )),
        );

        // Test precision loss in downscaling
        expect_cast(
            ScalarValue::TimestampNanosecond(Some(1), None),
            DataType::Timestamp(TimeUnit::Second, None),
            ExpectedCast::Value(ScalarValue::TimestampSecond(Some(0), None)),
        );

        expect_cast(
            ScalarValue::TimestampMicrosecond(Some(999), None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            ExpectedCast::Value(ScalarValue::TimestampMillisecond(Some(0), None)),
        );
    }

    #[test]
    fn test_string_view() {
        // Test Utf8View to other string types
        expect_cast(
            ScalarValue::Utf8View(Some("test".to_string())),
            DataType::Utf8,
            ExpectedCast::Value(ScalarValue::Utf8(Some("test".to_string()))),
        );

        expect_cast(
            ScalarValue::Utf8View(Some("test".to_string())),
            DataType::LargeUtf8,
            ExpectedCast::Value(ScalarValue::LargeUtf8(Some("test".to_string()))),
        );

        // Test other string types to Utf8View
        expect_cast(
            ScalarValue::Utf8(Some("hello".to_string())),
            DataType::Utf8View,
            ExpectedCast::Value(ScalarValue::Utf8View(Some("hello".to_string()))),
        );

        expect_cast(
            ScalarValue::LargeUtf8(Some("world".to_string())),
            DataType::Utf8View,
            ExpectedCast::Value(ScalarValue::Utf8View(Some("world".to_string()))),
        );

        // Test empty string
        expect_cast(
            ScalarValue::Utf8(Some("".to_string())),
            DataType::Utf8View,
            ExpectedCast::Value(ScalarValue::Utf8View(Some("".to_string()))),
        );

        // Test large string
        let large_string = "x".repeat(1000);
        expect_cast(
            ScalarValue::LargeUtf8(Some(large_string.clone())),
            DataType::Utf8View,
            ExpectedCast::Value(ScalarValue::Utf8View(Some(large_string))),
        );
    }

    #[test]
    fn test_binary_size_edge_cases() {
        // Test size mismatch - too small
        expect_cast(
            ScalarValue::Binary(Some(vec![1, 2])),
            DataType::FixedSizeBinary(3),
            ExpectedCast::NoValue,
        );

        // Test size mismatch - too large
        expect_cast(
            ScalarValue::Binary(Some(vec![1, 2, 3, 4])),
            DataType::FixedSizeBinary(3),
            ExpectedCast::NoValue,
        );

        // Test empty binary
        expect_cast(
            ScalarValue::Binary(Some(vec![])),
            DataType::FixedSizeBinary(0),
            ExpectedCast::Value(ScalarValue::FixedSizeBinary(0, Some(vec![]))),
        );

        // Test exact size match
        expect_cast(
            ScalarValue::Binary(Some(vec![1, 2, 3])),
            DataType::FixedSizeBinary(3),
            ExpectedCast::Value(ScalarValue::FixedSizeBinary(3, Some(vec![1, 2, 3]))),
        );

        // Test single byte
        expect_cast(
            ScalarValue::Binary(Some(vec![42])),
            DataType::FixedSizeBinary(1),
            ExpectedCast::Value(ScalarValue::FixedSizeBinary(1, Some(vec![42]))),
        );
    }

    #[test]
    fn test_dictionary_index_types() {
        // Test different dictionary index types
        let string_value = ScalarValue::Utf8(Some("test".to_string()));

        // Int8 index dictionary
        let dict_int8 =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        expect_cast(
            string_value.clone(),
            dict_int8,
            ExpectedCast::Value(ScalarValue::Dictionary(
                Box::new(DataType::Int8),
                Box::new(string_value.clone()),
            )),
        );

        // Int16 index dictionary
        let dict_int16 =
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8));
        expect_cast(
            string_value.clone(),
            dict_int16,
            ExpectedCast::Value(ScalarValue::Dictionary(
                Box::new(DataType::Int16),
                Box::new(string_value.clone()),
            )),
        );

        // Int64 index dictionary
        let dict_int64 =
            DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8));
        expect_cast(
            string_value.clone(),
            dict_int64,
            ExpectedCast::Value(ScalarValue::Dictionary(
                Box::new(DataType::Int64),
                Box::new(string_value.clone()),
            )),
        );

        // Test dictionary unwrapping
        let dict_value = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::LargeUtf8(Some("unwrap_test".to_string()))),
        );
        expect_cast(
            dict_value,
            DataType::LargeUtf8,
            ExpectedCast::Value(ScalarValue::LargeUtf8(Some("unwrap_test".to_string()))),
        );
    }

    #[test]
    fn test_type_support_functions() {
        // Test numeric type support
        assert!(is_supported_numeric_type(&DataType::Int8));
        assert!(is_supported_numeric_type(&DataType::UInt64));
        assert!(is_supported_numeric_type(&DataType::Decimal128(10, 2)));
        assert!(is_supported_numeric_type(&DataType::Timestamp(
            TimeUnit::Nanosecond,
            None
        )));
        assert!(!is_supported_numeric_type(&DataType::Float32));
        assert!(!is_supported_numeric_type(&DataType::Float64));

        // Test string type support
        assert!(is_supported_string_type(&DataType::Utf8));
        assert!(is_supported_string_type(&DataType::LargeUtf8));
        assert!(is_supported_string_type(&DataType::Utf8View));
        assert!(!is_supported_string_type(&DataType::Binary));

        // Test binary type support
        assert!(is_supported_binary_type(&DataType::Binary));
        assert!(is_supported_binary_type(&DataType::FixedSizeBinary(10)));
        assert!(!is_supported_binary_type(&DataType::Utf8));

        // Test dictionary type support with nested types
        assert!(is_supported_dictionary_type(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8)
        )));
        assert!(is_supported_dictionary_type(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Int64)
        )));
        assert!(!is_supported_dictionary_type(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int32,
                true
            ))))
        )));

        // Test overall type support
        assert!(is_supported_type(&DataType::Int32));
        assert!(is_supported_type(&DataType::Utf8));
        assert!(is_supported_type(&DataType::Binary));
        assert!(is_supported_type(&DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8)
        )));
        assert!(!is_supported_type(&DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true
        )))));
        assert!(!is_supported_type(&DataType::Struct(Fields::empty())));
    }

    #[test]
    fn test_error_conditions() {
        // Test unsupported source type
        expect_cast(
            ScalarValue::Float32(Some(1.5)),
            DataType::Int32,
            ExpectedCast::NoValue,
        );

        // Test unsupported target type
        expect_cast(
            ScalarValue::Int32(Some(123)),
            DataType::Float64,
            ExpectedCast::NoValue,
        );

        // Test both types unsupported
        expect_cast(
            ScalarValue::Float64(Some(1.5)),
            DataType::Float32,
            ExpectedCast::NoValue,
        );

        // Test complex unsupported types
        let list_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        expect_cast(
            ScalarValue::Int32(Some(123)),
            list_type,
            ExpectedCast::NoValue,
        );

        // Test dictionary with unsupported inner type
        let bad_dict = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int32,
                true,
            )))),
        );
        expect_cast(
            ScalarValue::Int32(Some(123)),
            bad_dict,
            ExpectedCast::NoValue,
        );
    }
}
