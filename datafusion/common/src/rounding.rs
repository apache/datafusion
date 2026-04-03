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

//! Floating point rounding mode utility library
//! TODO: Remove this custom implementation and the "libc" dependency when
//!       floating-point rounding mode manipulation functions become available
//!       in Rust.

use std::ops::{Add, BitAnd, Sub};

use crate::Result;
use crate::ScalarValue;

// Define constants for ARM
#[cfg(all(target_arch = "aarch64", not(target_os = "windows")))]
const FE_UPWARD: i32 = 0x00400000;
#[cfg(all(target_arch = "aarch64", not(target_os = "windows")))]
const FE_DOWNWARD: i32 = 0x00800000;

// Define constants for x86_64
#[cfg(all(target_arch = "x86_64", not(target_os = "windows")))]
const FE_UPWARD: i32 = 0x0800;
#[cfg(all(target_arch = "x86_64", not(target_os = "windows")))]
const FE_DOWNWARD: i32 = 0x0400;

#[cfg(all(
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(target_os = "windows")
))]
extern crate libc;

#[cfg(all(
    any(target_arch = "x86_64", target_arch = "aarch64"),
    not(target_os = "windows")
))]
unsafe extern "C" {
    fn fesetround(round: i32);
    fn fegetround() -> i32;
}

/// A trait to manipulate floating-point types with bitwise operations.
/// Provides functions to convert a floating-point value to/from its bitwise
/// representation as well as utility methods to handle special values.
pub trait FloatBits {
    /// The integer type used for bitwise operations.
    type Item: Copy
        + PartialEq
        + BitAnd<Output = Self::Item>
        + Add<Output = Self::Item>
        + Sub<Output = Self::Item>;

    /// The smallest positive floating-point value representable by this type.
    const TINY_BITS: Self::Item;

    /// The smallest (in magnitude) negative floating-point value representable by this type.
    const NEG_TINY_BITS: Self::Item;

    /// A mask to clear the sign bit of the floating-point value's bitwise representation.
    const CLEAR_SIGN_MASK: Self::Item;

    /// The integer value 1, used in bitwise operations.
    const ONE: Self::Item;

    /// The integer value 0, used in bitwise operations.
    const ZERO: Self::Item;
    const NEG_ZERO: Self::Item;

    /// Converts the floating-point value to its bitwise representation.
    fn to_bits(self) -> Self::Item;

    /// Converts the bitwise representation to the corresponding floating-point value.
    fn from_bits(bits: Self::Item) -> Self;

    /// Returns true if the floating-point value is NaN (not a number).
    fn float_is_nan(self) -> bool;

    /// Returns the positive infinity value for this floating-point type.
    fn infinity() -> Self;

    /// Returns the negative infinity value for this floating-point type.
    fn neg_infinity() -> Self;
}

impl FloatBits for f32 {
    type Item = u32;
    const TINY_BITS: u32 = 0x1; // Smallest positive f32.
    const NEG_TINY_BITS: u32 = 0x8000_0001; // Smallest (in magnitude) negative f32.
    const CLEAR_SIGN_MASK: u32 = 0x7fff_ffff;
    const ONE: Self::Item = 1;
    const ZERO: Self::Item = 0;
    const NEG_ZERO: Self::Item = 0x8000_0000;

    fn to_bits(self) -> Self::Item {
        self.to_bits()
    }

    fn from_bits(bits: Self::Item) -> Self {
        f32::from_bits(bits)
    }

    fn float_is_nan(self) -> bool {
        self.is_nan()
    }

    fn infinity() -> Self {
        f32::INFINITY
    }

    fn neg_infinity() -> Self {
        f32::NEG_INFINITY
    }
}

impl FloatBits for f64 {
    type Item = u64;
    const TINY_BITS: u64 = 0x1; // Smallest positive f64.
    const NEG_TINY_BITS: u64 = 0x8000_0000_0000_0001; // Smallest (in magnitude) negative f64.
    const CLEAR_SIGN_MASK: u64 = 0x7fff_ffff_ffff_ffff;
    const ONE: Self::Item = 1;
    const ZERO: Self::Item = 0;
    const NEG_ZERO: Self::Item = 0x8000_0000_0000_0000;

    fn to_bits(self) -> Self::Item {
        self.to_bits()
    }

    fn from_bits(bits: Self::Item) -> Self {
        f64::from_bits(bits)
    }

    fn float_is_nan(self) -> bool {
        self.is_nan()
    }

    fn infinity() -> Self {
        f64::INFINITY
    }

    fn neg_infinity() -> Self {
        f64::NEG_INFINITY
    }
}

/// Returns the next representable floating-point value greater than the input value.
///
/// This function takes a floating-point value that implements the FloatBits trait,
/// calculates the next representable value greater than the input, and returns it.
///
/// If the input value is NaN or positive infinity, the function returns the input value.
///
/// # Examples
///
/// ```
/// use datafusion_common::rounding::next_up;
///
/// let f: f32 = 1.0;
/// let next_f = next_up(f);
/// assert_eq!(next_f, 1.0000001);
/// ```
pub fn next_up<F: FloatBits + Copy>(float: F) -> F {
    let bits = float.to_bits();
    if float.float_is_nan() || bits == F::infinity().to_bits() {
        return float;
    }

    let abs = bits & F::CLEAR_SIGN_MASK;
    let next_bits = if bits == F::ZERO {
        F::TINY_BITS
    } else if abs == F::ZERO {
        F::ZERO
    } else if bits == abs {
        bits + F::ONE
    } else {
        bits - F::ONE
    };
    F::from_bits(next_bits)
}

/// Returns the next representable floating-point value smaller than the input value.
///
/// This function takes a floating-point value that implements the FloatBits trait,
/// calculates the next representable value smaller than the input, and returns it.
///
/// If the input value is NaN or negative infinity, the function returns the input value.
///
/// # Examples
///
/// ```
/// use datafusion_common::rounding::next_down;
///
/// let f: f32 = 1.0;
/// let next_f = next_down(f);
/// assert_eq!(next_f, 0.99999994);
/// ```
pub fn next_down<F: FloatBits + Copy>(float: F) -> F {
    let bits = float.to_bits();
    if float.float_is_nan() || bits == F::neg_infinity().to_bits() {
        return float;
    }

    let abs = bits & F::CLEAR_SIGN_MASK;
    let next_bits = if bits == F::ZERO {
        F::NEG_ZERO
    } else if abs == F::ZERO {
        F::NEG_TINY_BITS
    } else if bits == abs {
        bits - F::ONE
    } else {
        bits + F::ONE
    };
    F::from_bits(next_bits)
}

#[cfg(any(
    not(any(target_arch = "x86_64", target_arch = "aarch64")),
    target_os = "windows"
))]
fn alter_fp_rounding_mode_conservative<const UPPER: bool, F>(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
    operation: F,
) -> Result<ScalarValue>
where
    F: FnOnce(&ScalarValue, &ScalarValue) -> Result<ScalarValue>,
{
    let mut result = operation(lhs, rhs)?;
    match &mut result {
        ScalarValue::Float64(Some(value)) => {
            if UPPER {
                *value = next_up(*value)
            } else {
                *value = next_down(*value)
            }
        }
        ScalarValue::Float32(Some(value)) => {
            if UPPER {
                *value = next_up(*value)
            } else {
                *value = next_down(*value)
            }
        }
        _ => {}
    };
    Ok(result)
}

pub fn alter_fp_rounding_mode<const UPPER: bool, F>(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
    operation: F,
) -> Result<ScalarValue>
where
    F: FnOnce(&ScalarValue, &ScalarValue) -> Result<ScalarValue>,
{
    #[cfg(all(
        any(target_arch = "x86_64", target_arch = "aarch64"),
        not(target_os = "windows")
    ))]
    unsafe {
        let current = fegetround();
        fesetround(if UPPER { FE_UPWARD } else { FE_DOWNWARD });
        let result = operation(lhs, rhs);
        fesetround(current);
        result
    }
    #[cfg(any(
        not(any(target_arch = "x86_64", target_arch = "aarch64")),
        target_os = "windows"
    ))]
    alter_fp_rounding_mode_conservative::<UPPER, _>(lhs, rhs, operation)
}

#[cfg(test)]
mod tests {
    use super::{next_down, next_up};

    #[test]
    fn test_next_down() {
        let x = 1.0f64;
        // Clamp value into range [0, 1).
        let clamped = x.clamp(0.0, next_down(1.0f64));
        assert!(clamped < 1.0);
        assert_eq!(next_up(clamped), 1.0);
    }

    #[test]
    fn test_next_up_small_positive() {
        let value: f64 = 1.0;
        let result = next_up(value);
        assert_eq!(result, 1.0000000000000002);
    }

    #[test]
    fn test_next_up_small_negative() {
        let value: f64 = -1.0;
        let result = next_up(value);
        assert_eq!(result, -0.9999999999999999);
    }

    #[test]
    fn test_next_up_pos_infinity() {
        let value: f64 = f64::INFINITY;
        let result = next_up(value);
        assert_eq!(result, f64::INFINITY);
    }

    #[test]
    fn test_next_up_nan() {
        let value: f64 = f64::NAN;
        let result = next_up(value);
        assert!(result.is_nan());
    }

    #[test]
    fn test_next_down_small_positive() {
        let value: f64 = 1.0;
        let result = next_down(value);
        assert_eq!(result, 0.9999999999999999);
    }

    #[test]
    fn test_next_down_small_negative() {
        let value: f64 = -1.0;
        let result = next_down(value);
        assert_eq!(result, -1.0000000000000002);
    }

    #[test]
    fn test_next_down_neg_infinity() {
        let value: f64 = f64::NEG_INFINITY;
        let result = next_down(value);
        assert_eq!(result, f64::NEG_INFINITY);
    }

    #[test]
    fn test_next_down_nan() {
        let value: f64 = f64::NAN;
        let result = next_down(value);
        assert!(result.is_nan());
    }

    #[test]
    fn test_next_up_small_positive_f32() {
        let value: f32 = 1.0;
        let result = next_up(value);
        assert_eq!(result, 1.0000001);
    }

    #[test]
    fn test_next_up_small_negative_f32() {
        let value: f32 = -1.0;
        let result = next_up(value);
        assert_eq!(result, -0.99999994);
    }

    #[test]
    fn test_next_up_pos_infinity_f32() {
        let value: f32 = f32::INFINITY;
        let result = next_up(value);
        assert_eq!(result, f32::INFINITY);
    }

    #[test]
    fn test_next_up_nan_f32() {
        let value: f32 = f32::NAN;
        let result = next_up(value);
        assert!(result.is_nan());
    }
    #[test]
    fn test_next_down_small_positive_f32() {
        let value: f32 = 1.0;
        let result = next_down(value);
        assert_eq!(result, 0.99999994);
    }
    #[test]
    fn test_next_down_small_negative_f32() {
        let value: f32 = -1.0;
        let result = next_down(value);
        assert_eq!(result, -1.0000001);
    }
    #[test]
    fn test_next_down_neg_infinity_f32() {
        let value: f32 = f32::NEG_INFINITY;
        let result = next_down(value);
        assert_eq!(result, f32::NEG_INFINITY);
    }
    #[test]
    fn test_next_down_nan_f32() {
        let value: f32 = f32::NAN;
        let result = next_down(value);
        assert!(result.is_nan());
    }

    #[test]
    fn test_next_up_neg_zero_f32() {
        let value: f32 = -0.0;
        let result = next_up(value);
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_next_down_zero_f32() {
        let value: f32 = 0.0;
        let result = next_down(value);
        assert_eq!(result, -0.0);
    }

    #[test]
    fn test_next_up_neg_zero_f64() {
        let value: f64 = -0.0;
        let result = next_up(value);
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_next_down_zero_f64() {
        let value: f64 = 0.0;
        let result = next_down(value);
        assert_eq!(result, -0.0);
    }
}
