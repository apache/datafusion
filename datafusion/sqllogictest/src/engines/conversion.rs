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

use arrow::datatypes::DecimalType;
use bigdecimal::BigDecimal;
use half::f16;
use num_traits::Float;
use std::str::FromStr;

/// Represents a constant for NULL string in your database.
pub const NULL_STR: &str = "NULL";

pub(crate) fn bool_to_str(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

pub(crate) fn varchar_to_str(value: &str) -> String {
    if value.is_empty() {
        "(empty)".to_string()
    } else {
        // Escape nulls so that github renders them correctly in the webui
        value.trim_end_matches('\n').replace('\u{0000}', "\\0")
    }
}

pub(crate) fn float_to_str<T: Float + ToString>(value: T, round_digits: i64) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value == T::infinity() {
        "Infinity".to_string()
    } else if value == T::neg_infinity() {
        "-Infinity".to_string()
    } else {
        float_decimal_to_str(value, round_digits)
    }
}

pub(crate) fn f16_to_str(value: f16) -> String {
    float_to_str(value, 12)
}

pub(crate) fn f32_to_str(value: f32) -> String {
    float_to_str(value, 12)
}

pub(crate) fn f64_to_str(value: f64) -> String {
    float_to_str(value, 12)
}

pub(crate) fn spark_f64_to_str(value: f64) -> String {
    // Spark uses 15 decimal places for doubles
    float_to_str(value, 15)
}

/// Converts a float to its plain string representation, rounding to a specified number of decimal places.
fn float_decimal_to_str<T: Float + ToString>(value: T, round_digits: i64) -> String {
    let bd = BigDecimal::from_str(&value.to_string()).unwrap();
    // Round the value to limit the number of decimal places
    let value = bd.round(round_digits).normalized();
    // Format the value to a string
    value.to_plain_string()
}

/// Converts a decimal to its plain string representation, usint the given scale
pub(crate) fn arrow_decimal_to_str<T: DecimalType>(
    value: T::Native,
    scale: i8,
) -> String {
    let precision = u8::MAX; // does not matter
    T::format_decimal(value, precision, scale)
}

#[cfg(feature = "postgres")]
pub(crate) fn decimal_to_str(value: BigDecimal) -> String {
    value.to_plain_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{
        Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, i256,
    };

    #[test]
    fn test_float_decimal_to_str() {
        assert_eq!(float_decimal_to_str(0.11, 12), "0.11");
        assert_eq!(float_decimal_to_str(0.011, 12), "0.011");
        assert_eq!(float_decimal_to_str(1.1, 12), "1.1");
        assert_eq!(float_decimal_to_str(11.0, 12), "11");
        assert_eq!(float_decimal_to_str(-0.11, 12), "-0.11");
        assert_eq!(float_decimal_to_str(-0.011, 12), "-0.011");
        assert_eq!(float_decimal_to_str(-1.1, 12), "-1.1");
        assert_eq!(float_decimal_to_str(-11.0, 12), "-11");

        assert_eq!(float_decimal_to_str(-0.011, 15), "-0.011");
        assert_eq!(
            float_decimal_to_str(0.12345678901234567, 15),
            "0.123456789012346"
        );
    }

    #[test]
    fn test_arrow_decimal_to_str() {
        assert_eq!(arrow_decimal_to_str::<Decimal32Type>(12345, 2), "123.45");
        assert_eq!(arrow_decimal_to_str::<Decimal64Type>(12345, 2), "123.45");
        assert_eq!(arrow_decimal_to_str::<Decimal128Type>(12345, 2), "123.45");
        assert_eq!(
            arrow_decimal_to_str::<Decimal256Type>(i256::from(12345), 2),
            "123.45"
        );
        // Ensure it doesn't trim trailing zeros
        assert_eq!(arrow_decimal_to_str::<Decimal128Type>(12300, 2), "123.00");
        assert_eq!(arrow_decimal_to_str::<Decimal128Type>(12300, 4), "1.2300");

        // Keep full precision besides 12 decimal places
        assert_eq!(
            arrow_decimal_to_str::<Decimal128Type>(10_i128.pow(13) + 11, 13),
            "1.0000000000011"
        );

        // Zero-scale integers
        assert_eq!(
            arrow_decimal_to_str::<Decimal128Type>(
                12345678901234567890123456789012345678_i128,
                0
            ),
            "12345678901234567890123456789012345678"
        );
        // Keep full precision
        assert_eq!(
            arrow_decimal_to_str::<Decimal128Type>(
                12345678901234567890123456789012345678_i128,
                38
            ),
            "0.12345678901234567890123456789012345678"
        );
    }
}
