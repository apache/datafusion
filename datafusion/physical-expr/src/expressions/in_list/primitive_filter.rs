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

//! Optimized primitive type filters for InList expressions
//!
//! This module provides high-performance membership testing for Arrow primitive types.

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use arrow::datatypes::ArrowPrimitiveType;
use datafusion_common::{HashSet, Result, exec_datafusion_err};

use super::result::{build_in_list_result, handle_dictionary};
use super::static_filter::StaticFilter;

// =============================================================================
// BITMAP FILTERS (O(1) lookup for u8/u16 via bit test)
// =============================================================================

/// Trait for bitmap storage (stack-allocated for u8, heap-allocated for u16).
pub(crate) trait BitmapStorage: Send + Sync {
    fn new_zeroed() -> Self;
    fn set_bit(&mut self, index: usize);
    fn get_bit(&self, index: usize) -> bool;
}

impl BitmapStorage for [u64; 4] {
    #[inline]
    fn new_zeroed() -> Self {
        [0u64; 4]
    }
    #[inline]
    fn set_bit(&mut self, index: usize) {
        self[index / 64] |= 1u64 << (index % 64);
    }
    #[inline(always)]
    fn get_bit(&self, index: usize) -> bool {
        (self[index / 64] >> (index % 64)) & 1 != 0
    }
}

impl BitmapStorage for Box<[u64; 1024]> {
    #[inline]
    fn new_zeroed() -> Self {
        Box::new([0u64; 1024])
    }
    #[inline]
    fn set_bit(&mut self, index: usize) {
        self[index / 64] |= 1u64 << (index % 64);
    }
    #[inline(always)]
    fn get_bit(&self, index: usize) -> bool {
        (self[index / 64] >> (index % 64)) & 1 != 0
    }
}

/// Configuration trait for bitmap filters.
pub(crate) trait BitmapFilterConfig: Send + Sync + 'static {
    type Native: arrow::datatypes::ArrowNativeType + Copy + Send + Sync;
    type ArrowType: ArrowPrimitiveType<Native = Self::Native>;
    type Storage: BitmapStorage;

    fn to_index(v: Self::Native) -> usize;
}

/// Config for u8 bitmap (256 bits = 32 bytes, fits in cache line).
pub(crate) enum U8Config {}
impl BitmapFilterConfig for U8Config {
    type Native = u8;
    type ArrowType = arrow::datatypes::UInt8Type;
    type Storage = [u64; 4];

    #[inline(always)]
    fn to_index(v: u8) -> usize {
        v as usize
    }
}

/// Config for u16 bitmap (65536 bits = 8 KB, fits in L1 cache).
pub(crate) enum U16Config {}
impl BitmapFilterConfig for U16Config {
    type Native = u16;
    type ArrowType = arrow::datatypes::UInt16Type;
    type Storage = Box<[u64; 1024]>;

    #[inline(always)]
    fn to_index(v: u16) -> usize {
        v as usize
    }
}

/// Bitmap filter for O(1) set membership via single bit test.
///
/// For small integer types (u8/u16), bitmap lookup outperforms both branchless
/// and hashed approaches at all list sizes.
pub(crate) struct BitmapFilter<C: BitmapFilterConfig> {
    null_count: usize,
    bits: C::Storage,
}

impl<C: BitmapFilterConfig> BitmapFilter<C> {
    pub(crate) fn try_new(in_array: &ArrayRef) -> Result<Self> {
        let prim_array =
            in_array.as_primitive_opt::<C::ArrowType>().ok_or_else(|| {
                exec_datafusion_err!("BitmapFilter: expected primitive array")
            })?;
        let mut bits = C::Storage::new_zeroed();
        for v in prim_array.iter().flatten() {
            bits.set_bit(C::to_index(v));
        }
        Ok(Self {
            null_count: prim_array.null_count(),
            bits,
        })
    }

    #[inline(always)]
    fn check(&self, needle: C::Native) -> bool {
        self.bits.get_bit(C::to_index(needle))
    }

    /// Check membership using a raw values slice (zero-copy path for type reinterpretation).
    #[inline]
    pub(crate) fn contains_slice(
        &self,
        values: &[C::Native],
        nulls: Option<&arrow::buffer::NullBuffer>,
        negated: bool,
    ) -> BooleanArray {
        build_in_list_result(values.len(), nulls, self.null_count > 0, negated, |i| {
            self.check(unsafe { *values.get_unchecked(i) })
        })
    }
}

impl<C: BitmapFilterConfig> StaticFilter for BitmapFilter<C> {
    fn null_count(&self) -> usize {
        self.null_count
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);
        let v = v.as_primitive_opt::<C::ArrowType>().ok_or_else(|| {
            exec_datafusion_err!("BitmapFilter: expected primitive array")
        })?;
        let input_values = v.values();
        Ok(build_in_list_result(
            v.len(),
            v.nulls(),
            self.null_count > 0,
            negated,
            #[inline(always)]
            |i| self.check(unsafe { *input_values.get_unchecked(i) }),
        ))
    }
}

// =============================================================================
// BRANCHLESS FILTER (Const Generic for Small Lists)
// =============================================================================

/// A branchless filter for very small IN lists (0-16 elements).
///
/// Uses const generics to unroll the membership check into a fixed-size
/// comparison chain, outperforming hash lookups for small lists due to:
/// - No branching (uses bitwise OR to combine comparisons)
/// - Better CPU pipelining
/// - No hash computation overhead
pub(crate) struct BranchlessFilter<T: ArrowPrimitiveType, const N: usize> {
    null_count: usize,
    values: [T::Native; N],
}

impl<T: ArrowPrimitiveType, const N: usize> BranchlessFilter<T, N>
where
    T::Native: Copy + PartialEq,
{
    /// Try to create a branchless filter if the array has exactly N non-null values.
    pub(crate) fn try_new(in_array: &ArrayRef) -> Option<Result<Self>> {
        let in_array = in_array.as_primitive_opt::<T>()?;
        let non_null_count = in_array.len() - in_array.null_count();
        if non_null_count != N {
            return None;
        }
        let values: Vec<_> = in_array.iter().flatten().collect();
        // Use default_value() from ArrowPrimitiveType trait instead of Default::default()
        let mut arr = [T::default_value(); N];
        arr.copy_from_slice(&values);
        Some(Ok(Self {
            null_count: in_array.null_count(),
            values: arr,
        }))
    }

    /// Branchless membership check using OR-chain.
    #[inline(always)]
    fn check(&self, needle: T::Native) -> bool {
        self.values
            .iter()
            .fold(false, |acc, &v| acc | (v == needle))
    }

    /// Check membership using a raw values slice (zero-copy path for type reinterpretation).
    #[inline]
    pub(crate) fn contains_slice(
        &self,
        values: &[T::Native],
        nulls: Option<&arrow::buffer::NullBuffer>,
        negated: bool,
    ) -> BooleanArray {
        build_in_list_result(values.len(), nulls, self.null_count > 0, negated, |i| {
            self.check(unsafe { *values.get_unchecked(i) })
        })
    }
}

impl<T: ArrowPrimitiveType, const N: usize> StaticFilter for BranchlessFilter<T, N>
where
    T::Native: Copy + PartialEq + Send + Sync,
{
    fn null_count(&self) -> usize {
        self.null_count
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);
        let v = v.as_primitive_opt::<T>().ok_or_else(|| {
            exec_datafusion_err!("Failed to downcast array to primitive type")
        })?;
        let input_values = v.values();
        Ok(build_in_list_result(
            v.len(),
            v.nulls(),
            self.null_count > 0,
            negated,
            // SAFETY: i is in bounds since we iterate 0..v.len()
            #[inline(always)]
            |i| self.check(unsafe { *input_values.get_unchecked(i) }),
        ))
    }
}

// =============================================================================
// DIRECT PROBE HASH FILTER (O(1) lookup with open addressing)
// =============================================================================

/// Load factor inverse for DirectProbeFilter hash table.
/// A value of 4 means 25% load factor (table is 4x the number of elements).
const LOAD_FACTOR_INVERSE: usize = 4;

/// Minimum table size for DirectProbeFilter.
/// Ensures reasonable performance even for very small IN lists.
const MIN_TABLE_SIZE: usize = 16;

/// Golden ratio constant for 32-bit hash mixing.
/// Derived from (2^32 / phi) where phi = (1 + sqrt(5)) / 2.
const GOLDEN_RATIO_32: u32 = 0x9e3779b9;

/// Golden ratio constant for 64-bit hash mixing.
/// Derived from (2^64 / phi) where phi = (1 + sqrt(5)) / 2.
const GOLDEN_RATIO_64: u64 = 0x9e3779b97f4a7c15;

/// Secondary mixing constant for 128-bit hashing (from SplitMix64).
/// Using a different constant for hi/lo avoids collisions when lo = hi * C.
const SPLITMIX_CONSTANT: u64 = 0xbf58476d1ce4e5b9;

/// Fast hash filter using open addressing with linear probing.
///
/// Uses a power-of-2 sized hash table for O(1) average-case lookups.
/// Optimized for the IN list use case with:
/// - Simple/fast hash function (golden ratio multiply + xor-shift)
/// - 25% load factor for minimal collisions
/// - Direct array storage for cache-friendly access
pub(crate) struct DirectProbeFilter<T: ArrowPrimitiveType>
where
    T::Native: DirectProbeHashable,
{
    null_count: usize,
    /// Hash table with open addressing. None = empty slot, Some(v) = value present
    table: Box<[Option<T::Native>]>,
    /// Mask for slot index (table.len() - 1, always power of 2 minus 1)
    mask: usize,
}

/// Trait for types that can be hashed for the direct probe filter.
///
/// Requires `Hash + Eq` for deduplication via `HashSet`, even though we use
/// a custom `probe_hash()` for the actual hash table lookups.
pub(crate) trait DirectProbeHashable:
    Copy + PartialEq + std::hash::Hash + Eq
{
    fn probe_hash(self) -> usize;
}

// Simple but fast hash - golden ratio multiply + xor-shift
impl DirectProbeHashable for i32 {
    #[inline(always)]
    fn probe_hash(self) -> usize {
        let x = self as u32;
        let x = x.wrapping_mul(GOLDEN_RATIO_32);
        (x ^ (x >> 16)) as usize
    }
}

impl DirectProbeHashable for i64 {
    #[inline(always)]
    fn probe_hash(self) -> usize {
        let x = self as u64;
        let x = x.wrapping_mul(GOLDEN_RATIO_64);
        (x ^ (x >> 32)) as usize
    }
}

impl DirectProbeHashable for u32 {
    #[inline(always)]
    fn probe_hash(self) -> usize {
        (self as i32).probe_hash()
    }
}

impl DirectProbeHashable for u64 {
    #[inline(always)]
    fn probe_hash(self) -> usize {
        (self as i64).probe_hash()
    }
}

impl DirectProbeHashable for i128 {
    #[inline(always)]
    fn probe_hash(self) -> usize {
        // Mix both halves with different constants to avoid collisions when lo = hi * C
        let lo = self as u64;
        let hi = (self >> 64) as u64;
        let x = lo.wrapping_mul(GOLDEN_RATIO_64) ^ hi.wrapping_mul(SPLITMIX_CONSTANT);
        (x ^ (x >> 32)) as usize
    }
}

impl<T: ArrowPrimitiveType> DirectProbeFilter<T>
where
    T::Native: DirectProbeHashable,
{
    pub(crate) fn try_new(in_array: &ArrayRef) -> Result<Self> {
        let arr = in_array.as_primitive_opt::<T>().ok_or_else(|| {
            exec_datafusion_err!(
                "DirectProbeFilter: expected {} array",
                std::any::type_name::<T>()
            )
        })?;

        // Collect unique values using HashSet for deduplication
        let unique_values: HashSet<_> = arr.iter().flatten().collect();

        Ok(Self::from_values_inner(
            unique_values.into_iter(),
            arr.null_count(),
        ))
    }

    /// Creates a DirectProbeFilter from an iterator of values.
    ///
    /// This is useful when building the filter from pre-processed values
    /// (e.g., masked views for Utf8View).
    pub(crate) fn from_values(values: impl Iterator<Item = T::Native>) -> Self {
        // Collect into HashSet for deduplication
        let unique_values: HashSet<_> = values.collect();
        Self::from_values_inner(unique_values.into_iter(), 0)
    }

    /// Internal constructor from deduplicated values
    fn from_values_inner(
        unique_values: impl Iterator<Item = T::Native>,
        null_count: usize,
    ) -> Self {
        let unique_values: Vec<_> = unique_values.collect();

        // Size table to ~25% load factor for fewer collisions
        let n = unique_values.len().max(1);
        let table_size = (n * LOAD_FACTOR_INVERSE)
            .next_power_of_two()
            .max(MIN_TABLE_SIZE);
        let mask = table_size - 1;

        let mut table: Box<[Option<T::Native>]> =
            vec![None; table_size].into_boxed_slice();

        // Insert all values using linear probing
        for v in unique_values {
            let mut slot = v.probe_hash() & mask;
            loop {
                if table[slot].is_none() {
                    table[slot] = Some(v);
                    break;
                }
                slot = (slot + 1) & mask;
            }
        }

        Self {
            null_count,
            table,
            mask,
        }
    }

    /// O(1) single-value lookup with linear probing.
    ///
    /// Returns true if the value is in the set.
    #[inline(always)]
    pub(crate) fn contains_single(&self, needle: T::Native) -> bool {
        let mut slot = needle.probe_hash() & self.mask;
        loop {
            // SAFETY: `slot` is always < table.len() because:
            // - `slot = hash & mask` where `mask = table.len() - 1`
            // - table size is always a power of 2
            // - `(slot + 1) & mask` wraps around within bounds
            match unsafe { self.table.get_unchecked(slot) } {
                None => return false,
                Some(v) if *v == needle => return true,
                _ => slot = (slot + 1) & self.mask,
            }
        }
    }

    /// Check membership using a raw values slice
    #[inline]
    pub(crate) fn contains_slice(
        &self,
        input: &[T::Native],
        nulls: Option<&arrow::buffer::NullBuffer>,
        negated: bool,
    ) -> BooleanArray {
        build_in_list_result(input.len(), nulls, self.null_count > 0, negated, |i| {
            // SAFETY: i is in bounds since we iterate 0..input.len()
            self.contains_single(unsafe { *input.get_unchecked(i) })
        })
    }
}

impl<T> StaticFilter for DirectProbeFilter<T>
where
    T: ArrowPrimitiveType + 'static,
    T::Native: DirectProbeHashable + Send + Sync + 'static,
{
    #[inline]
    fn null_count(&self) -> usize {
        self.null_count
    }

    #[inline]
    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);
        // Use raw buffer access for better optimization
        let data = v.to_data();
        let values: &[T::Native] = data.buffer::<T::Native>(0);
        Ok(self.contains_slice(values, v.nulls(), negated))
    }
}
