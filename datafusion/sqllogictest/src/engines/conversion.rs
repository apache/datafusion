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

use arrow::datatypes::{i256, Decimal128Type, Decimal256Type, DecimalType};
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
        value.trim_end_matches('\n').to_string()
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
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
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
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
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
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
    }
}

pub(crate) fn i128_to_str(value: i128, precision: &u8, scale: &i8) -> String {
    big_decimal_to_str(
        BigDecimal::from_str(&Decimal128Type::format_decimal(value, *precision, *scale))
            .unwrap(),
    )
}

pub(crate) fn i256_to_str(value: i256, precision: &u8, scale: &i8) -> String {
    big_decimal_to_str(
        BigDecimal::from_str(&Decimal256Type::format_decimal(value, *precision, *scale))
            .unwrap(),
    )
}

#[cfg(feature = "postgres")]
pub(crate) fn decimal_to_str(value: Decimal) -> String {
    big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
}

pub(crate) fn big_decimal_to_str(value: BigDecimal) -> String {
    // Round the value to limit the number of decimal places
    let value = value.round(12).normalized();
    // Format the value to a string
    format_big_decimal(value)
}

fn format_big_decimal(value: BigDecimal) -> String {
    let (integer, scale) = value.into_bigint_and_exponent();
    let mut str = integer.to_str_radix(10);
    if scale <= 0 {
        // Append zeros to the right of the integer part
        str.extend(std::iter::repeat('0').take(scale.unsigned_abs() as usize));
        str
    } else {
        let (sign, unsigned_len, unsigned_str) = if integer.is_negative() {
            ("-", str.len() - 1, &str[1..])
        } else {
            ("", str.len(), &str[..])
        };
        let scale = scale as usize;
        if unsigned_len <= scale {
            format!("{}0.{:0>scale$}", sign, unsigned_str)
        } else {
            str.insert(str.len() - scale, '.');
            str
        }
    }
}

#[cfg(test)]
mod tests {
    use super::big_decimal_to_str;
    use bigdecimal::{num_bigint::BigInt, BigDecimal};

    macro_rules! assert_decimal_str_eq {
        ($integer:expr, $scale:expr, $expected:expr) => {
            assert_eq!(
                big_decimal_to_str(BigDecimal::from_bigint(
                    BigInt::from($integer),
                    $scale
                )),
                $expected
            );
        };
    }

    #[test]
    fn test_big_decimal_to_str() {
        assert_decimal_str_eq!(11, 3, "0.011");
        assert_decimal_str_eq!(11, 2, "0.11");
        assert_decimal_str_eq!(11, 1, "1.1");
        assert_decimal_str_eq!(11, 0, "11");
        assert_decimal_str_eq!(11, -1, "110");
        assert_decimal_str_eq!(0, 0, "0");

        // Negative cases
        assert_decimal_str_eq!(-11, 3, "-0.011");
        assert_decimal_str_eq!(-11, 2, "-0.11");
        assert_decimal_str_eq!(-11, 1, "-1.1");
        assert_decimal_str_eq!(-11, 0, "-11");
        assert_decimal_str_eq!(-11, -1, "-110");

        // Round to 12 decimal places
        // 1.0000000000011 -> 1.000000000001
        assert_decimal_str_eq!(10_i128.pow(13) + 11, 13, "1.000000000001");
    }
}
