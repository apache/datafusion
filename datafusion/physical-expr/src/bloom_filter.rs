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

//! Bloom filter implementation for physical expressions
//!
//! This module contains a vendored copy of the Split Block Bloom Filter (SBBF)
//! implementation from the parquet crate. This avoids circular dependencies
//! while allowing physical expressions to use bloom filters for runtime pruning.
//!
//! TODO: If this bloom filter approach is successful, extract this into a shared
//! crate (e.g., `datafusion-bloom-filter`) that both parquet and physical-expr
//! can depend on.
//!
//! The implementation below is adapted from:
//! arrow-rs/parquet/src/bloom_filter/mod.rs

use datafusion_common::{internal_err, Result};
use std::mem::size_of;
use twox_hash::XxHash64;

/// Salt values as defined in the Parquet specification
/// Although we don't *need* to follow the Parquet spec here, using the same
/// constants allows us to be compatible with Parquet bloom filters in the future
/// e.g. to do binary intersection of bloom filters.
const SALT: [u32; 8] = [
    0x47b6137b_u32,
    0x44974d91_u32,
    0x8824ad5b_u32,
    0xa2b7289d_u32,
    0x705495c7_u32,
    0x2df1424b_u32,
    0x9efc4947_u32,
    0x5c6bfb31_u32,
];

/// Minimum bitset length in bytes
const BITSET_MIN_LENGTH: usize = 32;
/// Maximum bitset length in bytes
const BITSET_MAX_LENGTH: usize = 128 * 1024 * 1024;

/// Hash seed for xxHash
const SEED: u64 = 0;

/// Each block is 256 bits, broken up into eight contiguous "words", each consisting of 32 bits.
/// Each word is thought of as an array of bits; each bit is either "set" or "not set".
#[derive(Debug, Copy, Clone)]
#[repr(transparent)]
struct Block([u32; 8]);

impl Block {
    const ZERO: Block = Block([0; 8]);

    /// Takes as its argument a single unsigned 32-bit integer and returns a block in which each
    /// word has exactly one bit set.
    fn mask(x: u32) -> Self {
        let mut result = [0_u32; 8];
        for i in 0..8 {
            // wrapping instead of checking for overflow
            let y = x.wrapping_mul(SALT[i]);
            let y = y >> 27;
            result[i] = 1 << y;
        }
        Self(result)
    }

    /// Setting every bit in the block that was also set in the result from mask
    fn insert(&mut self, hash: u32) {
        let mask = Self::mask(hash);
        for i in 0..8 {
            self[i] |= mask[i];
        }
    }

    /// Returns true when every bit that is set in the result of mask is also set in the block.
    fn check(&self, hash: u32) -> bool {
        let mask = Self::mask(hash);
        for i in 0..8 {
            if self[i] & mask[i] == 0 {
                return false;
            }
        }
        true
    }
}

impl std::ops::Index<usize> for Block {
    type Output = u32;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        self.0.index(index)
    }
}

impl std::ops::IndexMut<usize> for Block {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.0.index_mut(index)
    }
}

/// A Split Block Bloom Filter (SBBF)
///
/// This is a space-efficient probabilistic data structure used to test whether
/// an element is a member of a set. False positive matches are possible, but
/// false negatives are not.
#[derive(Debug, Clone)]
pub(crate) struct Sbbf(Vec<Block>);

impl Sbbf {
    /// Create a new Sbbf with given number of distinct values and false positive probability.
    /// Will return an error if `fpp` is greater than or equal to 1.0 or less than 0.0.
    pub fn new_with_ndv_fpp(ndv: u64, fpp: f64) -> Result<Self> {
        if !(0.0..1.0).contains(&fpp) {
            return internal_err!(
                "False positive probability must be between 0.0 and 1.0, got {fpp}"
            );
        }
        let num_bits = num_of_bits_from_ndv_fpp(ndv, fpp);
        Ok(Self::new_with_num_of_bytes(num_bits / 8))
    }

    /// Create a new Sbbf with given number of bytes, the exact number of bytes will be adjusted
    /// to the next power of two bounded by BITSET_MIN_LENGTH and BITSET_MAX_LENGTH.
    fn new_with_num_of_bytes(num_bytes: usize) -> Self {
        let num_bytes = optimal_num_of_bytes(num_bytes);
        assert_eq!(num_bytes % size_of::<Block>(), 0);
        let num_blocks = num_bytes / size_of::<Block>();
        let bitset = vec![Block::ZERO; num_blocks];
        Self(bitset)
    }

    #[inline]
    fn hash_to_block_index(&self, hash: u64) -> usize {
        // unchecked_mul is unstable, but in reality this is safe, we'd just use saturating mul
        // but it will not saturate
        (((hash >> 32).saturating_mul(self.0.len() as u64)) >> 32) as usize
    }

    /// Insert a value into the filter
    pub fn insert<T: AsBytes + ?Sized>(&mut self, value: &T) {
        self.insert_hash(hash_as_bytes(value));
    }

    /// Insert a hash into the filter
    fn insert_hash(&mut self, hash: u64) {
        let block_index = self.hash_to_block_index(hash);
        self.0[block_index].insert(hash as u32)
    }

    /// Check if a value is probably present or definitely absent in the filter
    pub fn check<T: AsBytes + ?Sized>(&self, value: &T) -> bool {
        self.check_hash(hash_as_bytes(value))
    }

    /// Check if a hash is in the filter. May return
    /// true for values that were never inserted ("false positive")
    /// but will always return false if a hash has not been inserted.
    fn check_hash(&self, hash: u64) -> bool {
        let block_index = self.hash_to_block_index(hash);
        self.0[block_index].check(hash as u32)
    }
}

/// Trait for types that can be converted to bytes for hashing
pub trait AsBytes {
    /// Return a byte slice representation of this value
    fn as_bytes(&self) -> &[u8];
}

impl AsBytes for str {
    fn as_bytes(&self) -> &[u8] {
        str::as_bytes(self)
    }
}

impl AsBytes for [u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

impl AsBytes for bool {
    fn as_bytes(&self) -> &[u8] {
        if *self {
            &[1u8]
        } else {
            &[0u8]
        }
    }
}

impl AsBytes for i32 {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const i32 as *const u8, size_of::<i32>())
        }
    }
}

impl AsBytes for i64 {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const i64 as *const u8, size_of::<i64>())
        }
    }
}

impl AsBytes for u32 {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const u32 as *const u8, size_of::<u32>())
        }
    }
}

impl AsBytes for u64 {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const u64 as *const u8, size_of::<u64>())
        }
    }
}

impl AsBytes for f32 {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const f32 as *const u8, size_of::<f32>())
        }
    }
}

impl AsBytes for f64 {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const f64 as *const u8, size_of::<f64>())
        }
    }
}

impl AsBytes for i128 {
    fn as_bytes(&self) -> &[u8] {
        // Use big-endian for i128 to match Parquet's FIXED_LEN_BYTE_ARRAY representation
        // This allows compatibility with Parquet bloom filters
        unsafe {
            std::slice::from_raw_parts(
                self as *const i128 as *const u8,
                size_of::<i128>(),
            )
        }
    }
}

impl AsBytes for [u8; 32] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

/// Hash a value using xxHash64 with seed 0
#[inline]
fn hash_as_bytes<A: AsBytes + ?Sized>(value: &A) -> u64 {
    XxHash64::oneshot(SEED, value.as_bytes())
}

/// Calculate optimal number of bytes, bounded by min/max and rounded to power of 2
#[inline]
fn optimal_num_of_bytes(num_bytes: usize) -> usize {
    let num_bytes = num_bytes.min(BITSET_MAX_LENGTH);
    let num_bytes = num_bytes.max(BITSET_MIN_LENGTH);
    num_bytes.next_power_of_two()
}

/// Calculate number of bits needed given NDV and FPP
/// Formula: m = -k * n / ln(1 - f^(1/k))
/// where k=8 (number of hash functions), n=ndv, f=fpp, m=num_bits
#[inline]
fn num_of_bits_from_ndv_fpp(ndv: u64, fpp: f64) -> usize {
    let num_bits = -8.0 * ndv as f64 / (1.0 - fpp.powf(1.0 / 8.0)).ln();
    num_bits as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_bytes() {
        assert_eq!(hash_as_bytes(""), 17241709254077376921);
    }

    #[test]
    fn test_mask_set_quick_check() {
        for i in 0..1_000 {
            let result = Block::mask(i);
            assert!(result.0.iter().all(|&x| x.is_power_of_two()));
        }
    }

    #[test]
    fn test_block_insert_and_check() {
        for i in 0..1_000 {
            let mut block = Block::ZERO;
            block.insert(i);
            assert!(block.check(i));
        }
    }

    #[test]
    fn test_sbbf_insert_and_check() {
        let mut sbbf = Sbbf(vec![Block::ZERO; 1_000]);
        for i in 0..10_000 {
            sbbf.insert(&i);
            assert!(sbbf.check(&i));
        }
    }

    #[test]
    fn test_optimal_num_of_bytes() {
        for (input, expected) in &[
            (0, 32),
            (9, 32),
            (31, 32),
            (32, 32),
            (33, 64),
            (99, 128),
            (1024, 1024),
            (999_000_000, 128 * 1024 * 1024),
        ] {
            assert_eq!(*expected, optimal_num_of_bytes(*input));
        }
    }

    #[test]
    fn test_num_of_bits_from_ndv_fpp() {
        for (fpp, ndv, num_bits) in &[
            (0.1, 10, 57),
            (0.01, 10, 96),
            (0.001, 10, 146),
            (0.1, 100, 577),
            (0.01, 100, 968),
            (0.001, 100, 1460),
        ] {
            assert_eq!(*num_bits, num_of_bits_from_ndv_fpp(*ndv, *fpp) as u64);
        }
    }
}
