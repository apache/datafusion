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

use arrow::array::ArrowNativeTypeOp;
use arrow::error::ArrowError;
use num_traits::{CheckedMul, CheckedNeg, Signed};
use std::fmt::Display;
use std::mem::swap;
use std::ops::RemAssign;

/// A gcd helper to compute GCD using Euclidean GCD algorithm
/// on non-negative numbers (scalars and decimals)
fn gcd_helper<T>(a: T, b: T) -> Result<T, ArrowError>
where
    T: ArrowNativeTypeOp + RemAssign + CheckedNeg,
{
    debug_assert!(a >= T::ZERO);
    debug_assert!(b >= T::ZERO);
    let (mut a, mut b) = if a > b { (a, b) } else { (b, a) };

    while b != T::ZERO {
        swap(&mut a, &mut b);
        b %= a;
    }

    Ok(a)
}

/// Computes gcd of two unsigned integers using Binary GCD algorithm
/// Faster, works with integers only
pub(crate) fn unsigned_gcd(mut a: u64, mut b: u64) -> u64 {
    if a == 0 {
        return b;
    }
    if b == 0 {
        return a;
    }

    let shift = (a | b).trailing_zeros();
    a >>= a.trailing_zeros();
    loop {
        b >>= b.trailing_zeros();
        if a > b {
            swap(&mut a, &mut b);
        }
        b -= a;
        if b == 0 {
            return a << shift;
        }
    }
}

/// Computes gcd of two signed numbers (integers or decimals),
/// checking for output integer overflow
pub(crate) fn gcd_signed<T>(x: T, y: T) -> Result<T, ArrowError>
where
    T: ArrowNativeTypeOp + RemAssign + Signed + CheckedNeg,
{
    // Make absolute values, keeping type
    let a = if x.is_positive() {
        x
    } else {
        x.checked_neg()
            .ok_or_else(|| ArrowError::ComputeError("Signed integer overflow".into()))?
    };
    let b = if y.is_positive() {
        y
    } else {
        y.checked_neg()
            .ok_or_else(|| ArrowError::ComputeError("Signed integer overflow".into()))?
    };
    // Call with signed numbers
    gcd_helper(a, b)
}

/// Computes gcd of two signed integers
pub(crate) fn gcd_signed_int(x: i64, y: i64) -> Result<i64, ArrowError> {
    let a = x.unsigned_abs();
    let b = y.unsigned_abs();

    // Call with unsigned numbers
    let r = unsigned_gcd(a, b);
    // gcd(i64::MIN, i64::MIN) = u64::MIN.unsigned_abs() cannot fit into i64
    r.try_into().map_err(|_| {
        ArrowError::ComputeError(format!("Signed integer overflow in GCD({x}, {y})"))
    })
}

/// Computes lcm of two signed numbers (integers or decimals)
pub(crate) fn lcm_signed<T>(x: T, y: T) -> Result<T, ArrowError>
where
    T: ArrowNativeTypeOp + RemAssign + Signed + CheckedNeg + CheckedMul + Display,
{
    if x == T::ZERO || y == T::ZERO {
        return Ok(T::ZERO);
    }

    // Make absolute values, keeping type
    let a = if x.is_positive() {
        x
    } else {
        x.checked_neg()
            .ok_or_else(|| ArrowError::ComputeError("Signed integer overflow".into()))?
    };
    let b = if y.is_positive() {
        y
    } else {
        y.checked_neg()
            .ok_or_else(|| ArrowError::ComputeError("Signed integer overflow".into()))?
    };
    // Call with signed numbers
    let gcd = gcd_helper(a, b)?;
    // gcd is not zero since both a and b are not zero, so the division is safe.
    (a / gcd).checked_mul(&b).ok_or_else(|| {
        ArrowError::ComputeError(format!("Signed integer overflow in LCM({x}, {y})"))
    })
}

/// Computes lcm of two signed integers,
/// checking for output integer overflow
pub(crate) fn lcm_signed_int(x: i64, y: i64) -> Result<i64, ArrowError> {
    if x == 0 || y == 0 {
        return Ok(0);
    }

    let a = x.unsigned_abs();
    let b = y.unsigned_abs();

    let gcd = gcd_helper::<u64>(a, b)?;
    // gcd is not zero since both a and b are not zero, so the division is safe.
    (a / gcd)
        .checked_mul(b)
        .and_then(|v| i64::try_from(v).ok())
        .ok_or_else(|| {
            ArrowError::ComputeError(format!("Signed integer overflow in LCM({x}, {y})"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::i256;

    const GCD_COMMON_TEST_CASES: [(i64, i64, i64); 18] = [
        // Basic cases
        (48, 18, 6),
        (54, 24, 6),
        (100, 50, 50),
        (17, 19, 1),
        (21, 14, 7),
        // Edge cases with 0
        (0, 0, 0),
        (0, 5, 5),
        (10, 0, 10),
        // Same numbers
        (7, 7, 7),
        (100, 100, 100),
        // One is 1
        (1, 1, 1),
        (1, 100, 1),
        (999, 1, 1),
        // Large numbers
        (1000000, 500000, 500000),
        (123456, 789012, 12),
        (999999, 111111, 111111),
        // Powers of 2
        (64, 128, 64),
        (1024, 2048, 1024),
    ];

    const LCM_COMMON_TEST_CASES: [(i64, i64, i64); 18] = [
        // Basic cases
        (48, 18, 144),
        (54, 24, 216),
        (100, 50, 100),
        (17, 19, 323),
        (21, 14, 42),
        // Edge cases with 0
        (0, 0, 0),
        (0, 5, 0),
        (10, 0, 0),
        // Same numbers
        (7, 7, 7),
        (100, 100, 100),
        // One is 1
        (1, 1, 1),
        (1, 100, 100),
        (999, 1, 999),
        // Large numbers
        (1_000_000, 500_000, 1_000_000),
        (123_456, 789_012, 8_117_355_456),
        (999_999, 111_111, 999_999),
        // Powers of 2
        (64, 128, 128),
        (1024, 2048, 2048),
    ];

    #[test]
    fn test_gcd_i64() {
        let test_cases: Vec<(i64, i64, i64)> = [
            GCD_COMMON_TEST_CASES.into(),
            vec![
                // Max value cases
                (1, i64::MAX, 1),
                (i64::MAX, 1, 1),
                (i64::MAX, i64::MAX, i64::MAX),
            ],
        ]
        .concat();

        // Success cases
        for (a, b, expected) in test_cases {
            let actual_euclidean = gcd_signed(a, b).expect("should succeed");
            assert_eq!(
                actual_euclidean, expected,
                "gcd_signed({a}, {b}) expected {expected}, actual {actual_euclidean}"
            );
            let actual_binary: i64 =
                unsigned_gcd(a.try_into().unwrap(), b.try_into().unwrap())
                    .try_into()
                    .expect("overflow");
            assert_eq!(
                actual_binary, expected,
                "unsigned_gcd({a}, {b}) expected {expected}, actual {actual_binary}"
            );
        }
    }

    #[test]
    fn test_gcd_decimal() {
        let test_cases: Vec<(i256, i256, i256)> = [
            GCD_COMMON_TEST_CASES
                .iter()
                .map(|&(a, b, c)| (i256::from(a), i256::from(b), i256::from(c)))
                .collect(),
            vec![
                (i256::from(1), i256::MAX, i256::from(1)),
                (i256::MAX, i256::from(1), i256::from(1)),
                (i256::MAX, i256::MAX, i256::MAX),
            ],
        ]
        .concat();

        // Success cases
        for (a, b, expected) in test_cases {
            let actual = gcd_signed(a, b).expect("should succeed");
            assert_eq!(
                actual, expected,
                "euclid_gcd({a}, {b}) expected {expected}, actual {actual}"
            );
        }
    }

    #[test]
    fn test_lcm_i64() {
        let test_cases: Vec<(i64, i64, i64)> = [
            LCM_COMMON_TEST_CASES.into(),
            vec![
                // Negative inputs - LCM is always non-negative
                (-6, 4, 12),
                (-4, -6, 12),
                // Max value cases
                (1, i64::MAX, i64::MAX),
                (i64::MAX, 1, i64::MAX),
                (i64::MAX, i64::MAX, i64::MAX),
            ],
        ]
        .concat();

        for (a, b, expected) in test_cases {
            let actual = lcm_signed_int(a, b).expect("should succeed");
            assert_eq!(
                actual, expected,
                "lcm_signed_int({a}, {b}) expected {expected}, actual {actual}"
            );
        }
    }

    #[test]
    fn test_lcm_decimal() {
        let test_cases: Vec<(i256, i256, i256)> = [
            LCM_COMMON_TEST_CASES
                .iter()
                .map(|&(a, b, c)| (i256::from(a), i256::from(b), i256::from(c)))
                .collect(),
            vec![
                // Negative inputs - LCM is always non-negative
                (i256::from(-6_i64), i256::from(4_i64), i256::from(12_i64)),
                (i256::from(-4_i64), i256::from(-6_i64), i256::from(12_i64)),
                // Max value cases
                (i256::from(1_i64), i256::MAX, i256::MAX),
                (i256::MAX, i256::from(1_i64), i256::MAX),
                (i256::MAX, i256::MAX, i256::MAX),
            ],
        ]
        .concat();

        for (a, b, expected) in test_cases {
            let actual = lcm_signed(a, b).expect("should succeed");
            assert_eq!(
                actual, expected,
                "lcm_signed({a}, {b}) expected {expected}, actual {actual}"
            );
        }
    }
}
