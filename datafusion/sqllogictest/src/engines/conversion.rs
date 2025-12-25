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

use arrow::datatypes::{Decimal128Type, Decimal256Type, DecimalType, i256};
use bigdecimal::BigDecimal;
use half::f16;
use rust_decimal::prelude::*;

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
        value.trim_end_matches('\n').replace("\u{0000}", "\\0")
    }
}

pub(crate) fn f16_to_str(value: f16) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value == f16::INFINITY {
        "Infinity".to_string()
    } else if value == f16::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap(), None)
    }
}

pub(crate) fn f32_to_str(value: f32) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value == f32::INFINITY {
        "Infinity".to_string()
    } else if value == f32::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap(), None)
    }
}

pub(crate) fn f64_to_str(value: f64) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap(), None)
    }
}

pub(crate) fn spark_f64_to_str(value: f64) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap(), Some(15))
    }
}

pub(crate) fn decimal_128_to_str(value: i128, scale: i8) -> String {
    let precision = u8::MAX; // does not matter
    big_decimal_to_str(
        BigDecimal::from_str(&Decimal128Type::format_decimal(value, precision, scale))
            .unwrap(),
        None,
    )
}

pub(crate) fn decimal_256_to_str(value: i256, scale: i8) -> String {
    let precision = u8::MAX; // does not matter
    big_decimal_to_str(
        BigDecimal::from_str(&Decimal256Type::format_decimal(value, precision, scale))
            .unwrap(),
        None,
    )
}

#[cfg(feature = "postgres")]
pub(crate) fn decimal_to_str(value: Decimal) -> String {
    big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap(), None)
}

/// Converts a `BigDecimal` to its plain string representation, optionally rounding to a specified number of decimal places.
///
/// If `round_digits` is `None`, the value is rounded to 12 decimal places by default.
#[expect(clippy::needless_pass_by_value)]
pub(crate) fn big_decimal_to_str(value: BigDecimal, round_digits: Option<i64>) -> String {
    // Round the value to limit the number of decimal places
    let value = value.round(round_digits.unwrap_or(12)).normalized();
    // Format the value to a string
    value.to_plain_string()
}

#[cfg(test)]
mod tests {
    use super::big_decimal_to_str;
    use bigdecimal::{BigDecimal, num_bigint::BigInt};

    macro_rules! assert_decimal_str_eq {
        ($integer:expr, $scale:expr, $round_digits:expr, $expected:expr) => {
            assert_eq!(
                big_decimal_to_str(
                    BigDecimal::from_bigint(BigInt::from($integer), $scale),
                    $round_digits
                ),
                $expected
            );
        };
    }

    #[test]
    fn test_big_decimal_to_str() {
        assert_decimal_str_eq!(110, 3, None, "0.11");
        assert_decimal_str_eq!(11, 3, None, "0.011");
        assert_decimal_str_eq!(11, 2, None, "0.11");
        assert_decimal_str_eq!(11, 1, None, "1.1");
        assert_decimal_str_eq!(11, 0, None, "11");
        assert_decimal_str_eq!(11, -1, None, "110");
        assert_decimal_str_eq!(0, 0, None, "0");
        assert_decimal_str_eq!(
            12345678901234567890123456789012345678_i128,
            0,
            None,
            "12345678901234567890123456789012345678"
        );
        assert_decimal_str_eq!(
            12345678901234567890123456789012345678_i128,
            38,
            None,
            "0.123456789012"
        );

        // Negative cases
        assert_decimal_str_eq!(-110, 3, None, "-0.11");
        assert_decimal_str_eq!(-11, 3, None, "-0.011");
        assert_decimal_str_eq!(-11, 2, None, "-0.11");
        assert_decimal_str_eq!(-11, 1, None, "-1.1");
        assert_decimal_str_eq!(-11, 0, None, "-11");
        assert_decimal_str_eq!(-11, -1, None, "-110");
        assert_decimal_str_eq!(
            -12345678901234567890123456789012345678_i128,
            0,
            None,
            "-12345678901234567890123456789012345678"
        );
        assert_decimal_str_eq!(
            -12345678901234567890123456789012345678_i128,
            38,
            None,
            "-0.123456789012"
        );

        // Round to 12 decimal places
        // 1.0000000000011 -> 1.000000000001
        assert_decimal_str_eq!(10_i128.pow(13) + 11, 13, None, "1.000000000001");
        assert_decimal_str_eq!(10_i128.pow(13) + 11, 13, Some(12), "1.000000000001");

        assert_decimal_str_eq!(10_i128.pow(13) + 11, 13, Some(13), "1.0000000000011");
    }
}
