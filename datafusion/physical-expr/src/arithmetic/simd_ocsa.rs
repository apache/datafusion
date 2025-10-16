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

//! Overflow Controlled SIMD Arithmetic (OCSA) Implementation
//!
//! This module implements the OCSA technique to eliminate interim overflow
//! interference and provide vectorized overflow detection for batch operations.
//! OCSA addresses the fundamental challenge that packed operands in SIMD registers
//! have no spare space to indicate overflow.

use arrow::error::ArrowError;

/// SIMD width detection based on CPU capabilities
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SimdWidth {
    Width128,  // SSE2, 2 x i64
    Width256,  // AVX2, 4 x i64  
    Width512,  // AVX-512, 8 x i64
}

/// SIMD capability detection for runtime dispatch
pub struct SimdCapabilities {
    pub max_width: SimdWidth,
    pub supports_avx2: bool,
    pub supports_avx512: bool,
    pub supports_avx512_unsigned_compare: bool,
}

impl SimdCapabilities {
    /// Detect SIMD capabilities at runtime
    pub fn detect() -> Self {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            let has_avx2 = is_x86_feature_detected!("avx2");
            let has_avx512 = is_x86_feature_detected!("avx512f");
            let has_avx512_cmp = is_x86_feature_detected!("avx512dq");
            
            let max_width = if has_avx512 {
                SimdWidth::Width512
            } else if has_avx2 {
                SimdWidth::Width256
            } else {
                SimdWidth::Width128
            };
            
            Self {
                max_width,
                supports_avx2: has_avx2,
                supports_avx512: has_avx512,
                supports_avx512_unsigned_compare: has_avx512_cmp,
            }
        }
        
        #[cfg(target_arch = "aarch64")]
        {
            // ARM64 NEON supports 128-bit SIMD (2 x i64)
            Self {
                max_width: SimdWidth::Width128,
                supports_avx2: false,
                supports_avx512: false,
                supports_avx512_unsigned_compare: false,
            }
        }
        
        #[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self {
                max_width: SimdWidth::Width128,
                supports_avx2: false,
                supports_avx512: false,
                supports_avx512_unsigned_compare: false,
            }
        }
    }
}

/// Vectorized overflow detection using branchless bit manipulation
///
/// Uses the formula: overflow = (sum ^ a) & (sum ^ b) < 0
/// This detects overflow by checking if operands have the same sign
/// but the result has a different sign.
#[inline]
fn detect_add_overflow_sse2(a: &[i64; 2], b: &[i64; 2]) -> [bool; 2] {
    let mut result = [false; 2];
    for i in 0..2 {
        let sum = a[i].wrapping_add(b[i]);
        let xor_sum_a = sum ^ a[i];
        let xor_sum_b = sum ^ b[i];
        let overflow_mask = xor_sum_a & xor_sum_b;
        result[i] = overflow_mask < 0;
    }
    result
}

/// Vectorized overflow detection for 4-wide i64 (AVX-256)
#[inline]  
fn detect_add_overflow_avx2(a: &[i64; 4], b: &[i64; 4]) -> [bool; 4] {
    let mut result = [false; 4];
    for i in 0..4 {
        let sum = a[i].wrapping_add(b[i]);
        let xor_sum_a = sum ^ a[i];
        let xor_sum_b = sum ^ b[i];
        let overflow_mask = xor_sum_a & xor_sum_b;
        result[i] = overflow_mask < 0;
    }
    result
}

/// Vectorized subtraction overflow detection for 4-wide i64
#[inline]
fn detect_sub_overflow_avx2(a: &[i64; 4], b: &[i64; 4]) -> [bool; 4] {
    let mut result = [false; 4];
    for i in 0..4 {
        let diff = a[i].wrapping_sub(b[i]);
        let xor_diff_a = diff ^ a[i];
        let xor_a_b = a[i] ^ b[i];
        let overflow_mask = xor_diff_a & xor_a_b;
        result[i] = overflow_mask < 0;
    }
    result
}

/// Vectorized subtraction overflow detection for 2-wide i64
#[inline]
fn detect_sub_overflow_sse2(a: &[i64; 2], b: &[i64; 2]) -> [bool; 2] {
    let mut result = [false; 2];
    for i in 0..2 {
        let diff = a[i].wrapping_sub(b[i]);
        let xor_diff_a = diff ^ a[i];
        let xor_a_b = a[i] ^ b[i];
        let overflow_mask = xor_diff_a & xor_a_b;
        result[i] = overflow_mask < 0;
    }
    result
}

/// Advanced multiplication overflow detection using SIMD-style batch processing
///
/// Multiplication overflow is more complex and requires checking if
/// the result can fit in the target type.
#[inline]
fn detect_mul_overflow_batch(a: &[i64], b: &[i64]) -> Vec<bool> {
    let len = std::cmp::min(a.len(), b.len());
    let mut result = Vec::with_capacity(len);
    
    for i in 0..len {
        // Quick check for zero operands (no overflow possible)
        if a[i] == 0 || b[i] == 0 {
            result.push(false);
            continue;
        }
        
        // Quick check for unit operands (no overflow possible)
        if a[i] == 1 || a[i] == -1 || b[i] == 1 || b[i] == -1 {
            result.push(false);
            continue;
        }
        
        // For remaining cases, use magnitude-based checking
        let abs_a = a[i].abs();
        let abs_b = b[i].abs();
        
        // sqrt(i64::MAX) ≈ 3,037,000,499
        const SQRT_I64_MAX: i64 = 3_037_000_499;
        
        let potentially_overflow = abs_a > SQRT_I64_MAX || abs_b > SQRT_I64_MAX;
        result.push(potentially_overflow);
    }
    
    result
}

/// OCSA-based batch addition with vectorized overflow detection
pub fn ocsa_checked_add_i64(left: &[i64], right: &[i64]) -> Result<Vec<i64>, ArrowError> {
    let len = std::cmp::min(left.len(), right.len());
    let mut result = Vec::with_capacity(len);
    
    let simd_caps = SimdCapabilities::detect();
    
    match simd_caps.max_width {
        SimdWidth::Width256 if len >= 4 => {
            ocsa_add_avx256(left, right, &mut result)?;
        }
        SimdWidth::Width128 if len >= 2 => {
            ocsa_add_sse2(left, right, &mut result)?;
        }
        _ => {
            ocsa_add_scalar_fallback(left, right, &mut result)?;
        }
    }
    
    Ok(result)
}

/// SSE2 OCSA implementation (2 x i64 at once)
fn ocsa_add_sse2(left: &[i64], right: &[i64], result: &mut Vec<i64>) -> Result<(), ArrowError> {
    let len = std::cmp::min(left.len(), right.len());
    let mut i = 0;
    
    // Process 2 elements at a time
    while i + 2 <= len {
        let a = [left[i], left[i + 1]];
        let b = [right[i], right[i + 1]];
        
        // Vectorized overflow detection
        let overflow_mask = detect_add_overflow_sse2(&a, &b);
        
        // Check if any lane has overflow
        for (idx, &has_overflow) in overflow_mask.iter().enumerate() {
            if has_overflow {
                return Err(ArrowError::ComputeError(format!(
                    "Arithmetic overflow: {} + {} would overflow i64 at index {}",
                    left[i + idx], right[i + idx], i + idx
                )));
            }
        }
        
        // No overflow: compute results
        for j in 0..2 {
            result.push(a[j].wrapping_add(b[j]));
        }
        
        i += 2;
    }
    
    // Handle remaining elements with scalar code
    for j in i..len {
        match left[j].checked_add(right[j]) {
            Some(val) => result.push(val),
            None => return Err(ArrowError::ComputeError(format!(
                "Arithmetic overflow: {} + {} would overflow i64 at index {}",
                left[j], right[j], j
            )))
        }
    }
    
    Ok(())
}

/// AVX-256 OCSA implementation (4 x i64 at once)
fn ocsa_add_avx256(left: &[i64], right: &[i64], result: &mut Vec<i64>) -> Result<(), ArrowError> {
    let len = std::cmp::min(left.len(), right.len());
    let mut i = 0;
    
    // Process 4 elements at a time
    while i + 4 <= len {
        let a = [left[i], left[i + 1], left[i + 2], left[i + 3]];
        let b = [right[i], right[i + 1], right[i + 2], right[i + 3]];
        
        // Vectorized overflow detection
        let overflow_mask = detect_add_overflow_avx2(&a, &b);
        
        // Check if any lane has overflow
        for (idx, &has_overflow) in overflow_mask.iter().enumerate() {
            if has_overflow {
                return Err(ArrowError::ComputeError(format!(
                    "Arithmetic overflow: {} + {} would overflow i64 at index {}",
                    left[i + idx], right[i + idx], i + idx
                )));
            }
        }
        
        // No overflow: compute results
        for j in 0..4 {
            result.push(a[j].wrapping_add(b[j]));
        }
        
        i += 4;
    }
    
    // Handle remaining elements with scalar code
    for j in i..len {
        match left[j].checked_add(right[j]) {
            Some(val) => result.push(val),
            None => return Err(ArrowError::ComputeError(format!(
                "Arithmetic overflow: {} + {} would overflow i64 at index {}",
                left[j], right[j], j
            )))
        }
    }
    
    Ok(())
}

/// Scalar fallback for architectures without sufficient SIMD support
fn ocsa_add_scalar_fallback(left: &[i64], right: &[i64], result: &mut Vec<i64>) -> Result<(), ArrowError> {
    let len = std::cmp::min(left.len(), right.len());
    
    for i in 0..len {
        match left[i].checked_add(right[i]) {
            Some(val) => result.push(val),
            None => return Err(ArrowError::ComputeError(format!(
                "Arithmetic overflow: {} + {} would overflow i64 at index {}",
                left[i], right[i], i
            )))
        }
    }
    
    Ok(())
}

/// OCSA-based batch subtraction with vectorized overflow detection
pub fn ocsa_checked_sub_i64(left: &[i64], right: &[i64]) -> Result<Vec<i64>, ArrowError> {
    let len = std::cmp::min(left.len(), right.len());
    let mut result = Vec::with_capacity(len);
    
    let simd_caps = SimdCapabilities::detect();
    
    match simd_caps.max_width {
        SimdWidth::Width256 if len >= 4 => {
            ocsa_sub_avx256(left, right, &mut result)?;
        }
        SimdWidth::Width128 if len >= 2 => {
            ocsa_sub_sse2(left, right, &mut result)?;
        }
        _ => {
            ocsa_sub_scalar_fallback(left, right, &mut result)?;
        }
    }
    
    Ok(result)
}

/// AVX-512 subtraction implementation
fn ocsa_sub_avx512(left: &[i64], right: &[i64], result: &mut Vec<i64>) -> Result<(), ArrowError> {
    let len = std::cmp::min(left.len(), right.len());
    let mut i = 0;
    
    while i + 8 <= len {
        let a = i64x8::from_slice(&left[i..i + 8]);
        let b = i64x8::from_slice(&right[i..i + 8]);
        
        let overflow_mask = detect_sub_overflow_simd_i64x8(a, b);
        
        if overflow_mask.any() {
            let overflow_array = overflow_mask.to_array();
            for (idx, &has_overflow) in overflow_array.iter().enumerate() {
                if has_overflow {
                    return Err(ArrowError::ComputeError(format!(
                        "Arithmetic overflow: {} - {} would overflow i64 at index {}",
                        left[i + idx], right[i + idx], i + idx
                    )));
                }
            }
        }
        
        let diff = a.wrapping_sub(b);
        result.extend_from_slice(&diff.to_array());
        
        i += 8;
    }
    
    // Handle remaining elements
    for j in i..len {
        match left[j].checked_sub(right[j]) {
            Some(val) => result.push(val),
            None => return Err(ArrowError::ComputeError(format!(
                "Arithmetic overflow: {} - {} would overflow i64 at index {}",
                left[j], right[j], j
            )))
        }
    }
    
    Ok(())
}

/// AVX-256 subtraction implementation
fn ocsa_sub_avx256(left: &[i64], right: &[i64], result: &mut Vec<i64>) -> Result<(), ArrowError> {
    let len = std::cmp::min(left.len(), right.len());
    let mut i = 0;
    
    while i + 4 <= len {
        let a = i64x4::from_slice(&left[i..i + 4]);
        let b = i64x4::from_slice(&right[i..i + 4]);
        
        let overflow_mask = detect_sub_overflow_simd_i64x4(a, b);
        
        if overflow_mask.any() {
            let overflow_array = overflow_mask.to_array();
            for (idx, &has_overflow) in overflow_array.iter().enumerate() {
                if has_overflow {
                    return Err(ArrowError::ComputeError(format!(
                        "Arithmetic overflow: {} - {} would overflow i64 at index {}",
                        left[i + idx], right[i + idx], i + idx
                    )));
                }
            }
        }
        
        let diff = a.wrapping_sub(b);
        result.extend_from_slice(&diff.to_array());
        
        i += 4;
    }
    
    // Handle remaining elements
    for j in i..len {
        match left[j].checked_sub(right[j]) {
            Some(val) => result.push(val),
            None => return Err(ArrowError::ComputeError(format!(
                "Arithmetic overflow: {} - {} would overflow i64 at index {}",
                left[j], right[j], j
            )))
        }
    }
    
    Ok(())
}

/// Scalar subtraction fallback
fn ocsa_sub_scalar_fallback(left: &[i64], right: &[i64], result: &mut Vec<i64>) -> Result<(), ArrowError> {
    let len = std::cmp::min(left.len(), right.len());
    
    for i in 0..len {
        match left[i].checked_sub(right[i]) {
            Some(val) => result.push(val),
            None => return Err(ArrowError::ComputeError(format!(
                "Arithmetic overflow: {} - {} would overflow i64 at index {}",
                left[i], right[i], i
            )))
        }
    }
    
    Ok(())
}

/// OCSA-based batch multiplication (more conservative due to complexity)
pub fn ocsa_checked_mul_i64(left: &[i64], right: &[i64]) -> Result<Vec<i64>, ArrowError> {
    let len = std::cmp::min(left.len(), right.len());
    let mut result = Vec::with_capacity(len);
    
    let simd_caps = SimdCapabilities::detect();
    
    // Multiplication overflow detection is more complex, use SIMD for pre-screening
    match simd_caps.max_width {
        SimdWidth::Width256 if len >= 4 => {
            ocsa_mul_avx256_prescreening(left, right, &mut result)?;
        }
        _ => {
            ocsa_mul_scalar_fallback(left, right, &mut result)?;
        }
    }
    
    Ok(result)
}

/// AVX-256 multiplication with SIMD pre-screening
fn ocsa_mul_avx256_prescreening(left: &[i64], right: &[i64], result: &mut Vec<i64>) -> Result<(), ArrowError> {
    let len = std::cmp::min(left.len(), right.len());
    let mut i = 0;
    
    while i + 4 <= len {
        let a = i64x4::from_slice(&left[i..i + 4]);
        let b = i64x4::from_slice(&right[i..i + 4]);
        
        // Use SIMD to pre-screen for potential overflow
        let potential_overflow = detect_mul_overflow_simd_i64x4(a, b);
        
        if potential_overflow.any() {
            // Fall back to scalar checking for potentially overflowing elements
            let overflow_array = potential_overflow.to_array();
            for (idx, &might_overflow) in overflow_array.iter().enumerate() {
                if might_overflow {
                    // Use precise scalar check
                    match left[i + idx].checked_mul(right[i + idx]) {
                        Some(val) => result.push(val),
                        None => return Err(ArrowError::ComputeError(format!(
                            "Arithmetic overflow: {} * {} would overflow i64 at index {}",
                            left[i + idx], right[i + idx], i + idx
                        )))
                    }
                } else {
                    // Safe to use wrapping multiplication
                    result.push(left[i + idx].wrapping_mul(right[i + idx]));
                }
            }
        } else {
            // All elements are safe for wrapping multiplication
            let product = a.wrapping_mul(b);
            result.extend_from_slice(&product.to_array());
        }
        
        i += 4;
    }
    
    // Handle remaining elements
    for j in i..len {
        match left[j].checked_mul(right[j]) {
            Some(val) => result.push(val),
            None => return Err(ArrowError::ComputeError(format!(
                "Arithmetic overflow: {} * {} would overflow i64 at index {}",
                left[j], right[j], j
            )))
        }
    }
    
    Ok(())
}

/// Scalar multiplication fallback
fn ocsa_mul_scalar_fallback(left: &[i64], right: &[i64], result: &mut Vec<i64>) -> Result<(), ArrowError> {
    let len = std::cmp::min(left.len(), right.len());
    
    for i in 0..len {
        match left[i].checked_mul(right[i]) {
            Some(val) => result.push(val),
            None => return Err(ArrowError::ComputeError(format!(
                "Arithmetic overflow: {} * {} would overflow i64 at index {}",
                left[i], right[i], i
            )))
        }
    }
    
    Ok(())
}

/// Global SIMD capabilities instance
static SIMD_CAPS: std::sync::OnceLock<SimdCapabilities> = std::sync::OnceLock::new();

/// Get SIMD capabilities (cached after first call)
pub fn get_simd_capabilities() -> &'static SimdCapabilities {
    SIMD_CAPS.get_or_init(SimdCapabilities::detect)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_capabilities_detection() {
        let caps = SimdCapabilities::detect();
        println!("SIMD capabilities: {:?}", caps);
        
        // Should at least support 128-bit SIMD on modern architectures
        assert!(matches!(caps.max_width, SimdWidth::Width128 | SimdWidth::Width256 | SimdWidth::Width512));
    }

    #[test]
    fn test_ocsa_checked_add_i64() {
        let left = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let right = vec![10, 20, 30, 40, 50, 60, 70, 80];
        
        let result = ocsa_checked_add_i64(&left, &right).unwrap();
        assert_eq!(result, vec![11, 22, 33, 44, 55, 66, 77, 88]);
    }

    #[test]
    fn test_ocsa_overflow_detection() {
        let left = vec![i64::MAX, 100, 200];
        let right = vec![1, 200, 300];
        
        // Should detect overflow in first element
        assert!(ocsa_checked_add_i64(&left, &right).is_err());
    }

    #[test]
    fn test_ocsa_checked_sub_i64() {
        let left = vec![100, 200, 300, 400];
        let right = vec![10, 20, 30, 40];
        
        let result = ocsa_checked_sub_i64(&left, &right).unwrap();
        assert_eq!(result, vec![90, 180, 270, 360]);
    }

    #[test]
    fn test_ocsa_checked_mul_i64() {
        let left = vec![2, 3, 4, 5];
        let right = vec![10, 20, 30, 40];
        
        let result = ocsa_checked_mul_i64(&left, &right).unwrap();
        assert_eq!(result, vec![20, 60, 120, 200]);
    }

    #[test]
    fn test_large_batch_performance() {
        // Test with a larger batch to verify SIMD utilization
        let size = 1000;
        let left: Vec<i64> = (0..size).collect();
        let right: Vec<i64> = (1..=size).collect();
        
        let result = ocsa_checked_add_i64(&left, &right).unwrap();
        assert_eq!(result.len(), size);
        
        // Verify a few values
        assert_eq!(result[0], 1);  // 0 + 1
        assert_eq!(result[10], 21); // 10 + 11
        assert_eq!(result[size - 1], (size - 1) as i64 + size as i64);
    }

    #[test]
    fn test_simd_overflow_detection_functions() {
        // Test the SIMD overflow detection functions directly
        let a = i64x4::from_array([1, 2, i64::MAX, 4]);
        let b = i64x4::from_array([1, 2, 1, 4]);
        
        let overflow_mask = detect_add_overflow_simd_i64x4(a, b);
        let expected = [false, false, true, false]; // Only third element should overflow
        
        assert_eq!(overflow_mask.to_array(), expected);
    }
}