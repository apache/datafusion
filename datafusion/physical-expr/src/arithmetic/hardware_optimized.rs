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

//! Hardware-optimized overflow detection using CPU flags for near-zero overhead
//!
//! This module implements ultra-fast overflow detection by leveraging hardware
//! overflow flags available on modern CPUs. When supported, this provides
//! near-zero overhead overflow detection compared to software-based methods.

use arrow::error::ArrowError;
use std::arch::asm;

/// Hardware capability detection for overflow flag support
#[derive(Debug)]
pub struct HardwareCapabilities {
    pub supports_overflow_flags: bool,
    pub supports_carry_flags: bool,
    pub cpu_vendor: CpuVendor,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CpuVendor {
    Intel,
    Amd,
    Apple,
    Other,
}

impl HardwareCapabilities {
    /// Detect hardware capabilities at runtime
    pub fn detect() -> Self {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            Self {
                supports_overflow_flags: true, // x86/x64 always support overflow flags
                supports_carry_flags: true,
                cpu_vendor: detect_cpu_vendor(),
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            Self {
                supports_overflow_flags: has_arm_overflow_detection(),
                supports_carry_flags: true,
                cpu_vendor: CpuVendor::Apple, // Assume Apple Silicon for now
            }
        }

        #[cfg(not(any(
            target_arch = "x86",
            target_arch = "x86_64",
            target_arch = "aarch64"
        )))]
        {
            Self {
                supports_overflow_flags: false,
                supports_carry_flags: false,
                cpu_vendor: CpuVendor::Other,
            }
        }
    }

    /// Check if hardware-accelerated overflow detection is available
    pub fn can_use_hardware_overflow(&self) -> bool {
        self.supports_overflow_flags
    }
}

/// Hardware-accelerated overflow detection for signed 64-bit addition
///
/// Uses CPU overflow flags for near-zero overhead detection.
/// Returns `Err` if overflow occurs, `Ok(result)` otherwise.
#[inline]
pub fn checked_add_i64_hardware(a: i64, b: i64) -> Result<i64, ArrowError> {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        unsafe {
            let result: i64;
            let overflow: u8;

            asm!(
                "add {result}, {b}",      // result = a + b, sets overflow flag
                "seto {overflow:l}",      // Set overflow byte if OF=1
                result = inout(reg) a => result,  // Start with a, get result
                b = in(reg) b,
                overflow = out(reg_byte) overflow,
                options(pure, nomem, nostack)
            );

            if overflow != 0 {
                Err(ArrowError::ComputeError(format!(
                    "Arithmetic overflow: {a} + {b} would overflow i64"
                )))
            } else {
                Ok(result)
            }
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        // ARM64 overflow detection using ADDS instruction and overflow flag
        unsafe {
            let result: i64;
            let overflow_flag: u64;

            asm!(
                "adds {result}, {a}, {b}",    // Add with flags
                "cset {overflow}, vs",         // Set on overflow (V flag set)
                result = out(reg) result,
                overflow = out(reg) overflow_flag,
                a = in(reg) a,
                b = in(reg) b,
                options(pure, nomem, nostack)
            );

            if overflow_flag != 0 {
                Err(ArrowError::ComputeError(format!(
                    "Arithmetic overflow: {a} + {b} would overflow i64"
                )))
            } else {
                Ok(result)
            }
        }
    }

    #[cfg(not(any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "aarch64"
    )))]
    {
        // Fallback to software implementation for other architectures
        a.checked_add(b).ok_or_else(|| {
            ArrowError::ComputeError(format!(
                "Arithmetic overflow: {a} + {b} would overflow i64"
            ))
        })
    }
}

/// Hardware-accelerated overflow detection for signed 64-bit subtraction
#[inline]
pub fn checked_sub_i64_hardware(a: i64, b: i64) -> Result<i64, ArrowError> {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        unsafe {
            let result: i64;
            let overflow: u8;

            asm!(
                "sub {result}, {b}",      // result = a - b, sets overflow flag
                "seto {overflow:l}",      // Set overflow byte if OF=1
                result = inout(reg) a => result,
                b = in(reg) b,
                overflow = out(reg_byte) overflow,
                options(pure, nomem, nostack)
            );

            if overflow != 0 {
                Err(ArrowError::ComputeError(format!(
                    "Arithmetic overflow: {a} - {b} would overflow i64"
                )))
            } else {
                Ok(result)
            }
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        unsafe {
            let result: i64;
            let overflow_flag: u64;

            asm!(
                "subs {result}, {a}, {b}",    // Subtract with flags
                "cset {overflow}, vs",         // Set on overflow (V flag set)
                result = out(reg) result,
                overflow = out(reg) overflow_flag,
                a = in(reg) a,
                b = in(reg) b,
                options(pure, nomem, nostack)
            );

            if overflow_flag != 0 {
                Err(ArrowError::ComputeError(format!(
                    "Arithmetic overflow: {a} - {b} would overflow i64"
                )))
            } else {
                Ok(result)
            }
        }
    }

    #[cfg(not(any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "aarch64"
    )))]
    {
        a.checked_sub(b).ok_or_else(|| {
            ArrowError::ComputeError(format!(
                "Arithmetic overflow: {a} - {b} would overflow i64"
            ))
        })
    }
}

/// Hardware-accelerated overflow detection for signed 64-bit multiplication
///
/// Multiplication requires more complex overflow detection as the OF flag
/// behavior varies. We use a hybrid approach combining hardware flags
/// with efficient software checks.
#[inline]
pub fn checked_mul_i64_hardware(a: i64, b: i64) -> Result<i64, ArrowError> {
    // Quick early returns for special cases
    if a == 0 || b == 0 {
        return Ok(0);
    }
    if a == 1 {
        return Ok(b);
    }
    if b == 1 {
        return Ok(a);
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        // For multiplication, we use IMUL instruction which sets OF on overflow
        unsafe {
            let result: i64;
            let overflow: u8;

            asm!(
                "imul {result}, {b}",     // result = a * b, sets OF on overflow
                "seto {overflow:l}",      // Set overflow byte if OF=1
                result = inout(reg) a => result,
                b = in(reg) b,
                overflow = out(reg_byte) overflow,
                options(pure, nomem, nostack)
            );

            if overflow != 0 {
                Err(ArrowError::ComputeError(format!(
                    "Arithmetic overflow: {a} * {b} would overflow i64"
                )))
            } else {
                Ok(result)
            }
        }
    }

    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    {
        // Fallback for other architectures
        a.checked_mul(b).ok_or_else(|| {
            ArrowError::ComputeError(format!(
                "Arithmetic overflow: {a} * {b} would overflow i64"
            ))
        })
    }
}

/// Hardware-accelerated overflow detection for signed 32-bit addition
#[inline]
pub fn checked_add_i32_hardware(a: i32, b: i32) -> Result<i32, ArrowError> {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        unsafe {
            let result: i32;
            let overflow: u8;

            asm!(
                "add {result:e}, {b:e}",  // 32-bit add, sets overflow flag
                "seto {overflow:l}",      // Set overflow byte if OF=1
                result = inout(reg) a => result,
                b = in(reg) b,
                overflow = out(reg_byte) overflow,
                options(pure, nomem, nostack)
            );

            if overflow != 0 {
                Err(ArrowError::ComputeError(format!(
                    "Arithmetic overflow: {a} + {b} would overflow i32"
                )))
            } else {
                Ok(result)
            }
        }
    }

    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    {
        a.checked_add(b).ok_or_else(|| {
            ArrowError::ComputeError(format!(
                "Arithmetic overflow: {a} + {b} would overflow i32"
            ))
        })
    }
}

/// Batch hardware-optimized overflow detection for arrays
///
/// Processes arrays in chunks, using hardware flags for maximum efficiency.
/// Early terminates on first overflow detected.
pub fn batch_checked_add_i64_hardware(
    left: &[i64],
    right: &[i64],
) -> Result<Vec<i64>, ArrowError> {
    let len = std::cmp::min(left.len(), right.len());
    let mut result = Vec::with_capacity(len);

    // Process elements using hardware-accelerated checking
    for i in 0..len {
        match checked_add_i64_hardware(left[i], right[i]) {
            Ok(val) => result.push(val),
            Err(e) => return Err(e), // Early termination on overflow
        }
    }

    Ok(result)
}

/// Detect CPU vendor for optimization selection
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn detect_cpu_vendor() -> CpuVendor {
    use std::arch::x86_64::__cpuid;

    unsafe {
        let cpuid_result = __cpuid(0);
        let vendor_string = [cpuid_result.ebx, cpuid_result.edx, cpuid_result.ecx];

        // Check for known vendor strings
        match vendor_string {
            [0x756e6547, 0x49656e69, 0x6c65746e] => CpuVendor::Intel, // "GenuineIntel"
            [0x68747541, 0x69746e65, 0x444d4163] => CpuVendor::Amd,   // "AuthenticAMD"
            _ => CpuVendor::Other,
        }
    }
}

/// Check for ARM overflow detection capabilities
#[cfg(target_arch = "aarch64")]
fn has_arm_overflow_detection() -> bool {
    // ARM64 always supports overflow flags with ADDS/SUBS instructions
    true
}

/// Global hardware capabilities instance
static HARDWARE_CAPS: std::sync::OnceLock<HardwareCapabilities> =
    std::sync::OnceLock::new();

/// Get hardware capabilities (cached after first call)
pub fn get_hardware_capabilities() -> &'static HardwareCapabilities {
    HARDWARE_CAPS.get_or_init(HardwareCapabilities::detect)
}

/// Check if hardware-accelerated overflow detection should be used
pub fn should_use_hardware_overflow() -> bool {
    get_hardware_capabilities().can_use_hardware_overflow()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hardware_capabilities_detection() {
        let caps = HardwareCapabilities::detect();

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            assert!(caps.supports_overflow_flags);
            assert!(caps.supports_carry_flags);
        }

        println!("Hardware capabilities: {caps:?}");
    }

    #[test]
    fn test_hardware_checked_add_i64() {
        // Test normal cases
        assert!(checked_add_i64_hardware(5, 10).is_ok());
        assert!(checked_add_i64_hardware(-5, 10).is_ok());
        assert!(checked_add_i64_hardware(0, 0).is_ok());

        // Test overflow cases
        assert!(checked_add_i64_hardware(i64::MAX, 1).is_err());
        assert!(checked_add_i64_hardware(i64::MIN, -1).is_err());
    }

    #[test]
    fn test_hardware_checked_sub_i64() {
        // Test normal cases
        assert!(checked_sub_i64_hardware(10, 5).is_ok());
        assert!(checked_sub_i64_hardware(-5, 10).is_ok());

        // Test overflow cases
        assert!(checked_sub_i64_hardware(i64::MIN, 1).is_err());
        assert!(checked_sub_i64_hardware(i64::MAX, -1).is_err());
    }

    #[test]
    fn test_hardware_checked_mul_i64() {
        // Test normal cases
        assert!(checked_mul_i64_hardware(5, 10).is_ok());
        assert!(checked_mul_i64_hardware(-5, 10).is_ok());
        assert!(checked_mul_i64_hardware(0, 1000).is_ok());
        assert!(checked_mul_i64_hardware(1, 1000).is_ok());

        // Test overflow cases
        assert!(checked_mul_i64_hardware(i64::MAX, 2).is_err());
        assert!(checked_mul_i64_hardware(i64::MIN, 2).is_err());
    }

    #[test]
    fn test_batch_hardware_overflow_detection() {
        let left = vec![1, 2, 3, i64::MAX];
        let right = vec![4, 5, 6, 1];

        // Should fail on the last element due to overflow
        assert!(batch_checked_add_i64_hardware(&left, &right).is_err());

        let left_safe = vec![1, 2, 3, 100];
        let right_safe = vec![4, 5, 6, 200];

        // Should succeed for all elements
        let result = batch_checked_add_i64_hardware(&left_safe, &right_safe).unwrap();
        assert_eq!(result, vec![5, 7, 9, 300]);
    }

    #[test]
    fn test_should_use_hardware_overflow() {
        // This should return true on x86/x64 and ARM64
        let should_use = should_use_hardware_overflow();

        #[cfg(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64"))]
        assert!(should_use);

        #[cfg(not(any(
            target_arch = "x86",
            target_arch = "x86_64",
            target_arch = "aarch64"
        )))]
        assert!(!should_use);
    }
}
