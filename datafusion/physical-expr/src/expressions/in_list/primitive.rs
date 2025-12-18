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

use std::hash::Hash;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use arrow::datatypes::ArrowPrimitiveType;
use datafusion_common::{HashSet, Result, exec_datafusion_err};

use super::array_filter::StaticFilter;
use super::result::{build_in_list_result, handle_dictionary};

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
        build_in_list_result(
            values.len(),
            nulls,
            self.null_count > 0,
            negated,
            |i| self.check(unsafe { *values.get_unchecked(i) }),
        )
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
// PRIMITIVE FILTER (Hash-based)
// =============================================================================

/// Hash-based filter for primitive types with larger IN lists.
pub(crate) struct PrimitiveFilter<T: ArrowPrimitiveType> {
    null_count: usize,
    set: HashSet<T::Native>,
}

impl<T: ArrowPrimitiveType> PrimitiveFilter<T>
where
    T::Native: Hash + Eq,
{
    pub(crate) fn try_new(in_array: &ArrayRef) -> Result<Self> {
        let arr = in_array.as_primitive_opt::<T>().ok_or_else(|| {
            exec_datafusion_err!(
                "PrimitiveFilter: expected {} array",
                std::any::type_name::<T>()
            )
        })?;
        Ok(Self {
            null_count: arr.null_count(),
            set: arr.iter().flatten().collect(),
        })
    }

    /// Check membership using a raw values slice (zero-copy path for type reinterpretation).
    #[inline]
    pub(crate) fn contains_slice(
        &self,
        values: &[T::Native],
        nulls: Option<&arrow::buffer::NullBuffer>,
        negated: bool,
    ) -> BooleanArray {
        build_in_list_result(
            values.len(),
            nulls,
            self.null_count > 0,
            negated,
            // SAFETY: i is in bounds since we iterate 0..values.len()
            |i| self.set.contains(unsafe { values.get_unchecked(i) }),
        )
    }
}

impl<T> StaticFilter for PrimitiveFilter<T>
where
    T: ArrowPrimitiveType + 'static,
    T::Native: Hash + Eq + Send + Sync + 'static,
{
    fn null_count(&self) -> usize {
        self.null_count
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);
        let v = v.as_primitive_opt::<T>().ok_or_else(|| {
            exec_datafusion_err!(
                "PrimitiveFilter: expected {} array",
                std::any::type_name::<T>()
            )
        })?;
        let values = v.values();
        Ok(build_in_list_result(
            v.len(),
            v.nulls(),
            self.null_count > 0,
            negated,
            // SAFETY: i is in bounds since we iterate 0..v.len()
            |i| self.set.contains(unsafe { values.get_unchecked(i) }),
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
        build_in_list_result(
            values.len(),
            nulls,
            self.null_count > 0,
            negated,
            |i| self.check(unsafe { *values.get_unchecked(i) }),
        )
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
