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

use arrow::datatypes::{Decimal128Type, DecimalType};
use bigdecimal::BigDecimal;
use half::f16;
use rust_decimal::prelude::*;

/// Represents a constant for NULL string in your database.
pub const NULL_STR: &str = "NULL";

/// Converts a bool value into a string.
///
/// # Arguments
///
/// * `value` - The bool value to convert.
pub fn bool_to_str(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

/// Converts a varchar into a string, trimming end line breaks.
/// Returns "(empty)" string for empty input.
///
/// # Arguments
///
/// * `value` - The varchar value to convert.
pub fn varchar_to_str(value: &str) -> String {
    if value.is_empty() {
        "(empty)".to_string()
    } else {
        value.trim_end_matches('\n').to_string()
    }
}

/// Converts a 16-bit floating-point number into a string.
///
/// # Arguments
///
/// * `value` - The 16-bit floating-point number to convert.
pub fn f16_to_str(value: f16) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f16::INFINITY {
        "Infinity".to_string()
    } else if value == f16::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
    }
}

/// Converts a 32-bit floating-point number into a string.
///
/// # Arguments
///
/// * `value` - The 32-bit floating-point number to convert.
pub fn f32_to_str(value: f32) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f32::INFINITY {
        "Infinity".to_string()
    } else if value == f32::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
    }
}

/// Converts a 64-bit floating-point number into a string.
///
/// # Arguments
///
/// * `value` - The 64-bit floating-point number to convert.
pub fn f64_to_str(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
    }
}

/// Converts a 128-bit integer into a string using specified precision and scale.
///
/// # Arguments
///
/// * `value` - The 128-bit integer to convert.
/// * `precision` - The number of significant digits.
/// * `scale` - The number of digits to the right of the decimal point.
pub fn i128_to_str(value: i128, precision: &u8, scale: &i8) -> String {
    big_decimal_to_str(
        BigDecimal::from_str(&Decimal128Type::format_decimal(value, *precision, *scale))
            .unwrap(),
    )
}

/// Converts a BigDecimal into a string, rounding the result to 12 decimal places.
///
/// # Arguments
///
/// * `value` - The BigDecimal value to convert.
pub fn big_decimal_to_str(value: BigDecimal) -> String {
    value.round(12).normalized().to_string()
}
