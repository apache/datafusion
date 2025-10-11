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

//! Simplified SIMD overflow detection using stable Rust features
//!
//! This module provides vectorized overflow detection for batch operations
//! using simple array-based processing that can be optimized by the compiler.

use arrow::error::ArrowError;

/// Optimized batch addition with fast overflow detection - REDESIGNED
pub fn simd_checked_add_i64(left: &[i64], right: &[i64]) -> Result<Vec<i64>, ArrowError> {
    // Validate arrays have the same length (standard Arrow requirement)
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform a binary operation on arrays of different length".to_string(),
        ));
    }
    let len = left.len();
    let mut result = Vec::with_capacity(len);

    // Optimized chunk size for vectorization
    const CHUNK_SIZE: usize = 32; // Larger chunks for better throughput
    let mut i = 0;

    // Process chunks with fast overflow detection
    while i + CHUNK_SIZE <= len {
        for j in 0..CHUNK_SIZE {
            let idx = i + j;
            let a = left[idx];
            let b = right[idx];
            let sum = a.wrapping_add(b);

            // Fast overflow check using bit manipulation (same as hardware optimized)
            if ((a ^ sum) & (b ^ sum)) < 0 {
                return Err(ArrowError::ComputeError(format!(
                    "Arithmetic overflow: {a} + {b} would overflow i64 at index {idx}"
                )));
            }

            result.push(sum);
        }
        i += CHUNK_SIZE;
    }

    // Handle remaining elements with same fast detection
    for idx in i..len {
        let a = left[idx];
        let b = right[idx];
        let sum = a.wrapping_add(b);

        if ((a ^ sum) & (b ^ sum)) < 0 {
            return Err(ArrowError::ComputeError(format!(
                "Arithmetic overflow: {a} + {b} would overflow i64 at index {idx}"
            )));
        }

        result.push(sum);
    }

    Ok(result)
}

/// Optimized batch subtraction with fast overflow detection - REDESIGNED
pub fn simd_checked_sub_i64(left: &[i64], right: &[i64]) -> Result<Vec<i64>, ArrowError> {
    // Validate arrays have the same length (standard Arrow requirement)
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform a binary operation on arrays of different length".to_string(),
        ));
    }
    let len = left.len();
    let mut result = Vec::with_capacity(len);

    const CHUNK_SIZE: usize = 32;
    let mut i = 0;

    while i + CHUNK_SIZE <= len {
        for j in 0..CHUNK_SIZE {
            let idx = i + j;
            let a = left[idx];
            let b = right[idx];
            let diff = a.wrapping_sub(b);

            // Fast overflow check for subtraction
            if ((a ^ b) & (a ^ diff)) < 0 {
                return Err(ArrowError::ComputeError(format!(
                    "Arithmetic overflow: {a} - {b} would overflow i64 at index {idx}"
                )));
            }

            result.push(diff);
        }
        i += CHUNK_SIZE;
    }

    for idx in i..len {
        let a = left[idx];
        let b = right[idx];
        let diff = a.wrapping_sub(b);

        if ((a ^ b) & (a ^ diff)) < 0 {
            return Err(ArrowError::ComputeError(format!(
                "Arithmetic overflow: {a} - {b} would overflow i64 at index {idx}"
            )));
        }

        result.push(diff);
    }

    Ok(result)
}

/// Optimized batch multiplication with overflow detection
pub fn simd_checked_mul_i64(left: &[i64], right: &[i64]) -> Result<Vec<i64>, ArrowError> {
    // Validate arrays have the same length (standard Arrow requirement)
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform a binary operation on arrays of different length".to_string(),
        ));
    }
    let len = left.len();
    let mut result = Vec::with_capacity(len);

    const CHUNK_SIZE: usize = 8;
    let mut i = 0;

    while i + CHUNK_SIZE <= len {
        for j in 0..CHUNK_SIZE {
            let idx = i + j;
            match left[idx].checked_mul(right[idx]) {
                Some(val) => result.push(val),
                None => {
                    return Err(ArrowError::ComputeError(format!(
                        "Arithmetic overflow: {} * {} would overflow i64 at index {}",
                        left[idx], right[idx], idx
                    )))
                }
            }
        }
        i += CHUNK_SIZE;
    }

    for idx in i..len {
        match left[idx].checked_mul(right[idx]) {
            Some(val) => result.push(val),
            None => {
                return Err(ArrowError::ComputeError(format!(
                    "Arithmetic overflow: {} * {} would overflow i64 at index {}",
                    left[idx], right[idx], idx
                )))
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_checked_add_i64() {
        let left = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let right = vec![10, 20, 30, 40, 50, 60, 70, 80, 90];

        let result = simd_checked_add_i64(&left, &right).unwrap();
        assert_eq!(result, vec![11, 22, 33, 44, 55, 66, 77, 88, 99]);
    }

    #[test]
    fn test_simd_overflow_detection() {
        let left = vec![i64::MAX, 100];
        let right = vec![1, 200];

        // Should detect overflow in first element
        assert!(simd_checked_add_i64(&left, &right).is_err());
    }

    #[test]
    fn test_simd_checked_sub_i64() {
        let left = vec![100, 200, 300, 400];
        let right = vec![10, 20, 30, 40];

        let result = simd_checked_sub_i64(&left, &right).unwrap();
        assert_eq!(result, vec![90, 180, 270, 360]);
    }

    #[test]
    fn test_simd_checked_mul_i64() {
        let left = vec![2, 3, 4, 5];
        let right = vec![10, 20, 30, 40];

        let result = simd_checked_mul_i64(&left, &right).unwrap();
        assert_eq!(result, vec![20, 60, 120, 200]);
    }
}
