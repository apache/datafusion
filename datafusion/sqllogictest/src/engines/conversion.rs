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
use bigdecimal::{BigDecimal, RoundingMode};
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
    float_to_str(value.into(), 4)
}

pub(crate) fn f32_to_str(value: f32) -> String {
    float_to_str(value.into(), 6)
}

pub(crate) fn f64_to_str(value: f64) -> String {
    float_to_str(value, 10)
}

fn float_to_str(value: f64, max_significant_digits: u8) -> String {
    if value.is_nan() {
        // The sign of NaN can be different depending on platform.
        // So the string representation of NaN ignores the sign.
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        let mut big_decimal = BigDecimal::from_f64(value)
            .unwrap()
            // Truncate trailing decimal zeros
            .normalized();
        let precision = big_decimal.digits();
        if precision > max_significant_digits as u64 {
            let scale = big_decimal.as_bigint_and_exponent().1;
            big_decimal = big_decimal
                .with_scale_round(
                    scale + (max_significant_digits as i64 - precision as i64),
                    RoundingMode::HalfUp,
                )
                // Truncate trailing decimal zeros
                .normalized();
        }
        big_decimal.to_plain_string()
    }
}

pub(crate) fn decimal_128_to_str(value: i128, scale: i8) -> String {
    let precision = u8::MAX; // does not matter
    BigDecimal::from_str(&Decimal128Type::format_decimal(value, precision, scale))
        .unwrap()
        .normalized() // Truncate trailing decimal zeros
        .to_plain_string()
}

pub(crate) fn decimal_256_to_str(value: i256, scale: i8) -> String {
    let precision = u8::MAX; // does not matter
    BigDecimal::from_str(&Decimal256Type::format_decimal(value, precision, scale))
        .unwrap()
        .normalized() // Truncate trailing decimal zeros
        .to_plain_string()
}

#[cfg(feature = "postgres")]
pub(crate) fn decimal_to_str(value: Decimal) -> String {
    BigDecimal::from_str(&value.to_string())
        .unwrap()
        .normalized() // Truncate trailing decimal zeros
        .to_plain_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use half::f16;

    #[test]
    fn test_f16_to_str() {
        assert_eq!(f16_to_str(f16::from_f32(0.)), "0");
        assert_eq!(f16_to_str(f16::from_f32(1.)), "1");
        assert_eq!(f16_to_str(f16::from_f32(12.345)), "12.34");
        assert_eq!(f16_to_str(f16::from_f32(-12.345)), "-12.34");
        assert_eq!(f16_to_str(f16::MAX), "65500");
        assert_eq!(f16_to_str(f16::MIN), "-65500");
        assert_eq!(f16_to_str(f16::EPSILON), "0.0009766");
        assert_eq!(f16_to_str(f16::from_f32(f32::INFINITY)), "Infinity");
        assert_eq!(f16_to_str(f16::from_f32(f32::NEG_INFINITY)), "-Infinity");
        assert_eq!(f16_to_str(f16::from_f32(f32::NAN)), "NaN");
    }

    #[test]
    fn test_f32_to_str() {
        assert_eq!(f32_to_str(0.), "0");
        assert_eq!(f32_to_str(1.), "1");
        assert_eq!(f32_to_str(12.345), "12.345");
        assert_eq!(f32_to_str(-12.345), "-12.345");
        assert_eq!(f32_to_str(0.0000012345), "0.0000012345");
        assert_eq!(f32_to_str(-0.0000012345), "-0.0000012345");
        assert_eq!(f32_to_str(12.345678), "12.3457");
        assert_eq!(f32_to_str(-12.345678), "-12.3457");
        assert_eq!(
            f32_to_str(f32::MAX),
            "340282000000000000000000000000000000000"
        );
        assert_eq!(
            f32_to_str(f32::MIN),
            "-340282000000000000000000000000000000000"
        );
        assert_eq!(f32_to_str(f32::EPSILON), "0.000000119209");
        assert_eq!(f32_to_str(f32::INFINITY), "Infinity");
        assert_eq!(f32_to_str(f32::NEG_INFINITY), "-Infinity");
        assert_eq!(f32_to_str(f32::NAN), "NaN");
    }

    #[test]
    fn test_f64_to_str() {
        assert_eq!(f64_to_str(0.), "0");
        assert_eq!(f64_to_str(1.), "1");
        assert_eq!(f64_to_str(12.345), "12.345");
        assert_eq!(f64_to_str(-12.345), "-12.345");
        assert_eq!(f64_to_str(12.345678), "12.345678");
        assert_eq!(f64_to_str(-12.345678), "-12.345678");
        assert_eq!(f64_to_str(0.00000000012345678), "0.00000000012345678");
        assert_eq!(f64_to_str(-0.00000000012345678), "-0.00000000012345678");
        assert_eq!(f64_to_str(12.34567890123456), "12.3456789");
        assert_eq!(f64_to_str(-12.34567890123456), "-12.3456789");
        assert_eq!(f64_to_str(0.99999999999999999999999), "1");
        assert_eq!(f64_to_str(0.0000000000999999999999999), "0.0000000001");
        assert_eq!(f64_to_str(f64::MAX), "179769313500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        assert_eq!(f64_to_str(f64::MIN), "-179769313500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
        assert_eq!(f64_to_str(f64::EPSILON), "0.0000000000000002220446049");
        assert_eq!(f64_to_str(f64::INFINITY), "Infinity");
        assert_eq!(f64_to_str(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(f64_to_str(f64::NAN), "NaN");
    }

    #[test]
    fn test_decimal_128_to_str() {
        assert_eq!(decimal_128_to_str(1, 0), "1");
        assert_eq!(decimal_128_to_str(1, 5), "0.00001");
        assert_eq!(decimal_128_to_str(1, 20), "0.00000000000000000001");
        assert_eq!(
            decimal_128_to_str(1, 38),
            "0.00000000000000000000000000000000000001"
        );
        assert_eq!(
            decimal_128_to_str(12345678901234567890123456789012345678, 20),
            "123456789012345678.90123456789012345678"
        );
        assert_eq!(
            decimal_128_to_str(12345678901234567890123456789012345678, 0),
            "12345678901234567890123456789012345678"
        );
    }

    #[test]
    fn test_decimal_256_to_str() {
        assert_eq!(decimal_256_to_str(i256::from_str("1").unwrap(), 0), "1");
        assert_eq!(
            decimal_256_to_str(i256::from_str("1").unwrap(), 5),
            "0.00001"
        );
        assert_eq!(
            decimal_256_to_str(i256::from_str("1").unwrap(), 20),
            "0.00000000000000000001"
        );
        assert_eq!(
            decimal_256_to_str(i256::from_str("1").unwrap(), 38),
            "0.00000000000000000000000000000000000001"
        );
        assert_eq!(
            decimal_256_to_str(
                i256::from_str("12345678901234567890123456789012345678").unwrap(),
                20
            ),
            "123456789012345678.90123456789012345678"
        );
        assert_eq!(
            decimal_256_to_str(
                i256::from_str("12345678901234567890123456789012345678").unwrap(),
                0
            ),
            "12345678901234567890123456789012345678"
        );
    }
}
