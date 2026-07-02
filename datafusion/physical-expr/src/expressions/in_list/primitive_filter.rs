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

//! Optimized primitive type filters for InList expressions.
//!
//! This module provides membership tests for Arrow primitive types.

use std::hash::Hash;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{ArrowNativeType, ArrowPrimitiveType, UInt8Type, UInt16Type};
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::{HashSet, Result, exec_datafusion_err};

use super::result::build_in_list_result;
use super::static_filter::{StaticFilter, handle_dictionary};

/// Storage for the bits used by [`BitmapFilter`].
///
/// `BitmapFilter` represents an `IN` list with one bit for each possible
/// value, so membership checks become direct bit tests. This trait lets the
/// same filter code use different storage sizes for different integer widths.
pub(super) trait BitmapStorage: Send + Sync {
    fn new_zeroed() -> Self;
    fn set_bit(&mut self, index: usize);
    fn get_bit(&self, index: usize) -> bool;
}

// `UInt8` has 256 possible values, 0 through 255. One bit per value takes
// 256 bits, which fits in four `u64` words.
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

// `UInt16` has 65,536 possible values. One bit per value takes 65,536 bits,
// which is 1,024 `u64` words, or 8 KiB. Box the array so the filter stores a
// pointer instead of carrying an 8 KiB array inline.
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

/// Arrow primitive types supported by [`BitmapFilter`].
///
/// Arrow already defines the Rust value type as `T::Native`. This trait only
/// supplies the bitmap storage size for the two integer domains that are small
/// enough to represent with one bit per possible value.
pub(super) trait BitmapFilterType:
    ArrowPrimitiveType + Send + Sync + 'static
{
    type Storage: BitmapStorage;
}

/// `UInt8` has 256 possible values, so four `u64` words cover the full domain.
impl BitmapFilterType for UInt8Type {
    type Storage = [u64; 4];
}

/// `UInt16` has 65,536 possible values, so 1,024 `u64` words cover the full
/// domain.
impl BitmapFilterType for UInt16Type {
    type Storage = Box<[u64; 1024]>;
}

/// `IN` filter backed by one bit per possible value.
///
/// Building the filter scans the non-null values in the IN-list and turns on
/// the bit selected by each value. Evaluating input values checks the same bit
/// position. Null handling and `NOT IN` inversion are handled by
/// `build_in_list_result`.
pub(super) struct BitmapFilter<T: BitmapFilterType> {
    null_count: usize,
    bits: T::Storage,
}

impl<T> BitmapFilter<T>
where
    T: BitmapFilterType,
{
    pub(super) fn try_new(in_array: &ArrayRef) -> Result<Self> {
        let prim_array = in_array.as_primitive_opt::<T>().ok_or_else(|| {
            exec_datafusion_err!("BitmapFilter: expected {} array", T::DATA_TYPE)
        })?;
        let mut bits = T::Storage::new_zeroed();
        let values = prim_array.values();
        match prim_array.nulls() {
            None => {
                for &v in values {
                    bits.set_bit(v.as_usize());
                }
            }
            Some(nulls) => {
                for i in
                    BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                {
                    bits.set_bit(values[i].as_usize());
                }
            }
        }
        Ok(Self {
            null_count: prim_array.null_count(),
            bits,
        })
    }

    #[inline(always)]
    fn check(&self, needle: T::Native) -> bool {
        self.bits.get_bit(needle.as_usize())
    }

    /// Check membership using a raw values slice (zero-copy path for type reinterpretation).
    #[inline]
    pub(super) fn contains_slice(
        &self,
        values: &[T::Native],
        nulls: Option<&NullBuffer>,
        negated: bool,
    ) -> BooleanArray {
        build_in_list_result(values.len(), nulls, self.null_count > 0, negated, |i| {
            // SAFETY: `build_in_list_result` invokes this closure for
            // indices in `0..values.len()`.
            let needle = unsafe { *values.get_unchecked(i) };
            self.check(needle)
        })
    }
}

impl<T> StaticFilter for BitmapFilter<T>
where
    T: BitmapFilterType,
{
    fn null_count(&self) -> usize {
        self.null_count
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);
        let v = v.as_primitive_opt::<T>().ok_or_else(|| {
            exec_datafusion_err!("BitmapFilter: expected {} array", T::DATA_TYPE)
        })?;
        let input_values = v.values();
        Ok(build_in_list_result(
            v.len(),
            v.nulls(),
            self.null_count > 0,
            negated,
            #[inline(always)]
            |i| {
                // SAFETY: `build_in_list_result` invokes this closure for
                // indices in `0..v.len()`, which matches `input_values.len()`.
                let needle = unsafe { *input_values.get_unchecked(i) };
                self.check(needle)
            },
        ))
    }
}

/// A branchless filter for very small fixed-width primitive IN lists.
///
/// Uses const generics to unroll the membership check into a fixed-size
/// comparison chain, outperforming hash lookups for small lists due to:
/// - No branching (uses bitwise OR to combine comparisons)
/// - Better CPU pipelining
/// - No hash computation overhead
pub(super) struct BranchlessFilter<T: ArrowPrimitiveType, const N: usize> {
    null_count: usize,
    values: [T::Native; N],
}

impl<T: ArrowPrimitiveType, const N: usize> BranchlessFilter<T, N>
where
    T::Native: Copy + PartialEq,
{
    /// Try to create a branchless filter if the array has exactly N non-null values.
    pub(super) fn try_new(in_array: &ArrayRef) -> Option<Result<Self>> {
        let in_array = in_array.as_primitive_opt::<T>()?;
        let non_null_count = in_array.len() - in_array.null_count();
        if non_null_count != N {
            return None;
        }
        // Use default_value() from ArrowPrimitiveType trait instead of Default::default()
        let mut arr = [T::default_value(); N];
        let mut i = 0;
        for value in in_array.iter().flatten() {
            arr[i] = value;
            i += 1;
        }
        debug_assert_eq!(i, N);
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
    pub(super) fn contains_slice(
        &self,
        values: &[T::Native],
        nulls: Option<&NullBuffer>,
        negated: bool,
    ) -> BooleanArray {
        build_in_list_result(values.len(), nulls, self.null_count > 0, negated, |i| {
            // SAFETY: `build_in_list_result` invokes this closure for
            // indices in `0..values.len()`.
            let needle = unsafe { *values.get_unchecked(i) };
            self.check(needle)
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
            #[inline(always)]
            |i| {
                // SAFETY: `build_in_list_result` invokes this closure for
                // indices in `0..v.len()`, which matches `input_values.len()`.
                let needle = unsafe { *input_values.get_unchecked(i) };
                self.check(needle)
            },
        ))
    }
}

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
pub(super) struct DirectProbeFilter<T: ArrowPrimitiveType>
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
pub(super) trait DirectProbeHashable: Copy + PartialEq + Hash + Eq {
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
    pub(super) fn try_new(in_array: &ArrayRef) -> Result<Self> {
        let arr = in_array.as_primitive_opt::<T>().ok_or_else(|| {
            exec_datafusion_err!(
                "DirectProbeFilter: expected {} array",
                std::any::type_name::<T>()
            )
        })?;

        // Collect unique values using HashSet for deduplication
        let unique_values: HashSet<_> = arr.iter().flatten().collect();

        Ok(Self::from_unique_values(unique_values, arr.null_count()))
    }

    fn from_unique_values(unique_values: HashSet<T::Native>, null_count: usize) -> Self {
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
    fn contains_single(&self, needle: T::Native) -> bool {
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
    pub(super) fn contains_slice(
        &self,
        input: &[T::Native],
        nulls: Option<&NullBuffer>,
        negated: bool,
    ) -> BooleanArray {
        build_in_list_result(input.len(), nulls, self.null_count > 0, negated, |i| {
            // SAFETY: `build_in_list_result` invokes this closure for
            // indices in `0..input.len()`.
            let needle = unsafe { *input.get_unchecked(i) };
            self.contains_single(needle)
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
        if v.data_type() != &T::DATA_TYPE {
            return Err(exec_datafusion_err!(
                "DirectProbeFilter: expected {} array, got {}",
                T::DATA_TYPE,
                v.data_type()
            ));
        }

        // Use raw buffer access for better optimization
        let data = v.to_data();
        let values: &[T::Native] = &data.buffer::<T::Native>(0)[..v.len()];
        Ok(self.contains_slice(values, v.nulls(), negated))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        DictionaryArray, Int8Array, UInt8Array, UInt16Array, UInt32Array,
    };
    use arrow::datatypes::UInt32Type;

    fn assert_contains(
        filter: &dyn StaticFilter,
        needles: &dyn Array,
        expected: Vec<Option<bool>>,
    ) -> Result<()> {
        assert_eq!(
            filter.contains(needles, false)?,
            BooleanArray::from(expected)
        );
        Ok(())
    }

    #[test]
    fn bitmap_filter_u8_handles_nulls() -> Result<()> {
        let haystack: ArrayRef = Arc::new(UInt8Array::from(vec![Some(1), None, Some(3)]));
        let filter = BitmapFilter::<UInt8Type>::try_new(&haystack)?;
        let needles = UInt8Array::from(vec![Some(1), Some(2), None, Some(3)]);

        assert_contains(&filter, &needles, vec![Some(true), None, None, Some(true)])?;
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );

        Ok(())
    }

    #[test]
    fn bitmap_filter_u8_handles_dictionary_needles() -> Result<()> {
        let haystack: ArrayRef = Arc::new(UInt8Array::from(vec![Some(1), None, Some(3)]));
        let filter = BitmapFilter::<UInt8Type>::try_new(&haystack)?;

        let keys = Int8Array::from(vec![Some(0), Some(1), None, Some(2)]);
        let values = Arc::new(UInt8Array::from(vec![Some(1), Some(2), Some(3)]));
        let needles = DictionaryArray::try_new(keys, values)?;

        assert_contains(&filter, &needles, vec![Some(true), None, None, Some(true)])
    }

    #[test]
    fn bitmap_filter_u16_handles_boundaries_and_nulls() -> Result<()> {
        let haystack: ArrayRef = Arc::new(UInt16Array::from(vec![
            Some(0),
            None,
            Some(1024),
            Some(u16::MAX),
        ]));
        let filter = BitmapFilter::<UInt16Type>::try_new(&haystack)?;
        let needles =
            UInt16Array::from(vec![Some(0), Some(1), Some(1024), Some(u16::MAX), None]);

        assert_contains(
            &filter,
            &needles,
            vec![Some(true), None, Some(true), Some(true), None],
        )?;
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, Some(false), Some(false), None])
        );

        Ok(())
    }

    #[test]
    fn direct_probe_filter_handles_slices_and_nulls() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            UInt32Array::from(vec![Some(999), Some(10), None, Some(20), Some(30)])
                .slice(1, 4),
        );
        let filter = DirectProbeFilter::<UInt32Type>::try_new(&haystack)?;
        let needles =
            UInt32Array::from(vec![Some(0), Some(10), Some(11), Some(30), None])
                .slice(1, 4);

        assert_contains(&filter, &needles, vec![Some(true), None, Some(true), None])?;
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, Some(false), None])
        );

        Ok(())
    }
}
