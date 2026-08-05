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

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::datatypes::*;
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::{Result, exec_datafusion_err};
use std::hash::Hash;

use super::branchless_filter::{BranchlessFilterType, BranchlessNative};
use super::frozen_set::FrozenSet;
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
/// supplies the bitmap storage size and maps values to their bit-pattern index
/// for the primitive domains that are small enough to represent with one bit
/// per possible value.
pub(super) trait BitmapFilterType:
    ArrowPrimitiveType + Send + Sync + 'static
{
    type Storage: BitmapStorage;

    /// Returns the index in the bitmap to check for this value.
    fn index(value: Self::Native) -> usize;
}

/// `Int8` has 256 possible bit patterns, so four `u64` words cover the full domain.
impl BitmapFilterType for Int8Type {
    type Storage = [u64; 4];

    #[inline(always)]
    fn index(value: Self::Native) -> usize {
        // Reinterpret the signed value's bit pattern into a bitmap index.
        value as u8 as usize
    }
}

/// `UInt8` has 256 possible values, so four `u64` words cover the full domain.
impl BitmapFilterType for UInt8Type {
    type Storage = [u64; 4];

    #[inline(always)]
    fn index(value: Self::Native) -> usize {
        value as usize
    }
}

/// `Int16` has 65,536 possible bit patterns, so 1,024 `u64` words cover the full
/// domain.
impl BitmapFilterType for Int16Type {
    type Storage = Box<[u64; 1024]>;

    #[inline(always)]
    fn index(value: Self::Native) -> usize {
        // Reinterpret the signed value's bit pattern into a bitmap index.
        value as u16 as usize
    }
}

/// `UInt16` has 65,536 possible values, so 1,024 `u64` words cover the full
/// domain.
impl BitmapFilterType for UInt16Type {
    type Storage = Box<[u64; 1024]>;

    #[inline(always)]
    fn index(value: Self::Native) -> usize {
        value as usize
    }
}

/// `Float16` has 65,536 possible bit patterns, so 1,024 `u64` words cover the
/// full domain.
impl BitmapFilterType for Float16Type {
    type Storage = Box<[u64; 1024]>;

    #[inline(always)]
    fn index(value: Self::Native) -> usize {
        value.to_bits() as usize
    }
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
                    bits.set_bit(T::index(v));
                }
            }
            Some(nulls) => {
                for i in
                    BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                {
                    bits.set_bit(T::index(values[i]));
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
        self.bits.get_bit(T::index(needle))
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

fn primitive_values<T>(array: &PrimitiveArray<T>) -> ScalarBuffer<BranchlessNative<T>>
where
    T: BranchlessFilterType,
{
    let data = array.to_data();
    ScalarBuffer::<BranchlessNative<T>>::new(
        data.buffers()[0].clone(),
        data.offset(),
        data.len(),
    )
}

/// Frozen-set filter for larger fixed-width primitive `IN` lists.
pub(super) struct PrimitiveFrozenFilter<T: BranchlessFilterType>
where
    BranchlessNative<T>: Copy + Eq + Hash,
{
    expected_data_type: DataType,
    null_count: usize,
    values: FrozenSet<BranchlessNative<T>>,
}

impl<T> PrimitiveFrozenFilter<T>
where
    T: BranchlessFilterType,
    BranchlessNative<T>: Copy + Eq + Hash,
{
    pub(super) fn try_new(in_array: &ArrayRef) -> Result<Self> {
        let in_array = in_array.as_primitive_opt::<T>().ok_or_else(|| {
            exec_datafusion_err!("PrimitiveFrozenFilter: expected {} array", T::DATA_TYPE)
        })?;

        let null_count = in_array.null_count();
        let values = primitive_values::<T>(in_array);
        let values = match in_array.nulls() {
            None => FrozenSet::try_new(values.as_ref())?,
            Some(nulls) => {
                let values =
                    BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                        .map(|i| values[i])
                        .collect::<Vec<_>>();
                FrozenSet::try_new(&values)?
            }
        };
        Ok(Self {
            expected_data_type: in_array.data_type().clone(),
            null_count,
            values,
        })
    }

    #[inline]
    fn contains_slice(
        &self,
        values: &[BranchlessNative<T>],
        nulls: Option<&NullBuffer>,
        negated: bool,
    ) -> BooleanArray {
        build_in_list_result(values.len(), nulls, self.null_count > 0, negated, |i| {
            // SAFETY: `build_in_list_result` invokes this closure for
            // indices in `0..values.len()`.
            let needle = unsafe { *values.get_unchecked(i) };
            self.values.contains(needle)
        })
    }
}

impl<T> StaticFilter for PrimitiveFrozenFilter<T>
where
    T: BranchlessFilterType,
    BranchlessNative<T>: Copy + Eq + Hash + Send + Sync,
{
    fn null_count(&self) -> usize {
        self.null_count
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        if !PrimitiveArray::<T>::is_compatible(v.data_type()) {
            return Err(exec_datafusion_err!(
                "PrimitiveFrozenFilter: expected {} array, got {}",
                self.expected_data_type,
                v.data_type()
            ));
        }

        let v = v.as_primitive_opt::<T>().ok_or_else(|| {
            exec_datafusion_err!("PrimitiveFrozenFilter: expected {} array", T::DATA_TYPE)
        })?;
        let values = primitive_values::<T>(v);
        Ok(self.contains_slice(values.as_ref(), v.nulls(), negated))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        DictionaryArray, Float16Array, Float32Array, Int8Array, Int16Array,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt8Array, UInt16Array,
        UInt32Array,
    };
    use half::f16;

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
    fn bitmap_filter_i8_handles_signed_boundaries_and_slices() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            Int8Array::from(vec![Some(99), Some(i8::MIN), None, Some(-1), Some(42)])
                .slice(1, 3),
        );
        let filter = BitmapFilter::<Int8Type>::try_new(&haystack)?;
        let needles =
            Int8Array::from(vec![Some(7), Some(i8::MIN), Some(-1), None]).slice(1, 3);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), Some(true), None])
        );
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), Some(false), None])
        );

        Ok(())
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
    fn bitmap_filter_i16_handles_signed_boundaries_and_slices() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            Int16Array::from(vec![
                Some(123),
                Some(i16::MIN),
                None,
                Some(-1),
                Some(i16::MAX),
            ])
            .slice(1, 4),
        );
        let filter = BitmapFilter::<Int16Type>::try_new(&haystack)?;
        let needles =
            Int16Array::from(vec![Some(0), Some(i16::MIN), Some(7), Some(i16::MAX)])
                .slice(1, 3);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), None, Some(true)])
        );
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, Some(false)])
        );

        Ok(())
    }

    #[test]
    fn bitmap_filter_f16_handles_bit_patterns_and_slices() -> Result<()> {
        let nan_a = f16::from_bits(0x7e01);
        let nan_b = f16::from_bits(0x7e02);
        let haystack: ArrayRef = Arc::new(
            Float16Array::from(vec![
                Some(f16::from_f32(9.0)),
                Some(f16::from_f32(1.5)),
                None,
                Some(f16::from_f32(-0.0)),
                Some(nan_a),
            ])
            .slice(1, 4),
        );
        let filter = BitmapFilter::<Float16Type>::try_new(&haystack)?;
        let needles = Float16Array::from(vec![
            Some(f16::from_f32(0.0)),
            Some(f16::from_f32(-0.0)),
            Some(nan_a),
            Some(nan_b),
            None,
        ])
        .slice(1, 4);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), Some(true), None, None])
        );
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), Some(false), None, None])
        );

        Ok(())
    }

    #[test]
    fn primitive_frozen_filter_handles_slices_and_nulls() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            UInt32Array::from(vec![
                Some(999),
                Some(10),
                None,
                Some(20),
                Some(10),
                Some(30),
            ])
            .slice(1, 5),
        );
        let filter = PrimitiveFrozenFilter::<UInt32Type>::try_new(&haystack)?;
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

    #[test]
    fn primitive_frozen_filter_floats_use_bit_equality() -> Result<()> {
        let nan_a = f32::from_bits(0x7fc0_0001);
        let nan_b = f32::from_bits(0x7fc0_0002);
        let haystack: ArrayRef =
            Arc::new(Float32Array::from(vec![Some(-0.0), Some(nan_a)]));
        let filter = PrimitiveFrozenFilter::<Float32Type>::try_new(&haystack)?;
        let needles =
            Float32Array::from(vec![Some(0.0), Some(-0.0), Some(nan_a), Some(nan_b)]);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(false), Some(true), Some(true), Some(false)])
        );

        Ok(())
    }

    #[test]
    fn primitive_frozen_filter_timestamp_uses_physical_compatibility() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![Some(1), Some(3)]).with_timezone("UTC"),
        );
        let filter =
            PrimitiveFrozenFilter::<TimestampNanosecondType>::try_new(&haystack)?;

        let different_timezone = TimestampNanosecondArray::from(vec![Some(1), Some(2)])
            .with_timezone("Europe/Paris");
        assert_contains(&filter, &different_timezone, vec![Some(true), Some(false)])?;

        let different_unit = TimestampMillisecondArray::from(vec![Some(1)]);
        let err = filter
            .contains(&different_unit, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Timestamp(ns"), "{err}");
        assert!(err.contains("Timestamp(ms"), "{err}");

        Ok(())
    }
}
