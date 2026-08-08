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

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::*;
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::{HashSet, Result, exec_datafusion_err};
use std::iter::FromIterator;

use super::integer_set::IntegerSet;
use super::result::{build_in_list_result, build_result_from_contains};
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

// Macro to generate specialized StaticFilter implementations for primitive types
macro_rules! primitive_static_filter {
    ($Name:ident, $ArrowType:ty) => {
        primitive_static_filter!(
            $Name,
            $ArrowType,
            |v| v,
            IntegerSet<<$ArrowType as ArrowPrimitiveType>::Native>,
            IntegerSet::new,
            IntegerSet::contains_values
        );
    };
    (
        $Name:ident,
        $ArrowType:ty,
        $to_set_value:expr,
        $SetType:ty,
        $new_set:expr,
        $contains_values:expr
    ) => {
        pub(super) struct $Name {
            null_count: usize,
            values: $SetType,
        }

        impl $Name {
            pub(super) fn try_new(in_array: &ArrayRef) -> Result<Self> {
                let in_array =
                    in_array.as_primitive_opt::<$ArrowType>().ok_or_else(|| {
                        exec_datafusion_err!(
                            "Failed to downcast an array to a '{}' array",
                            stringify!($ArrowType)
                        )
                    })?;

                let null_count = in_array.null_count();
                let values = ($new_set)(in_array.iter().flatten().map($to_set_value));

                Ok(Self { null_count, values })
            }
        }

        impl StaticFilter for $Name {
            fn null_count(&self) -> usize {
                self.null_count
            }

            fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
                handle_dictionary!(self, v, negated);

                let v = v.as_primitive_opt::<$ArrowType>().ok_or_else(|| {
                    exec_datafusion_err!(
                        "Failed to downcast an array to a '{}' array",
                        stringify!($ArrowType)
                    )
                })?;

                let needle_values = v.values();
                let contains = ($contains_values)(&self.values, needle_values);
                Ok(build_result_from_contains(
                    v.nulls(),
                    self.null_count > 0,
                    negated,
                    contains,
                ))
            }
        }
    };
}

primitive_static_filter!(Int32StaticFilter, Int32Type);
primitive_static_filter!(Int64StaticFilter, Int64Type);
primitive_static_filter!(UInt32StaticFilter, UInt32Type);
primitive_static_filter!(UInt64StaticFilter, UInt64Type);

#[inline]
fn float32_contains(values: &HashSet<[u8; 4]>, needles: &[f32]) -> BooleanBuffer {
    BooleanBuffer::collect_bool(needles.len(), |i| {
        values.contains(&needles[i].to_ne_bytes())
    })
}

#[inline]
fn float64_contains(values: &HashSet<[u8; 8]>, needles: &[f64]) -> BooleanBuffer {
    BooleanBuffer::collect_bool(needles.len(), |i| {
        values.contains(&needles[i].to_ne_bytes())
    })
}

primitive_static_filter!(
    Float32StaticFilter,
    Float32Type,
    f32::to_ne_bytes,
    HashSet<[u8; 4]>,
    HashSet::from_iter,
    float32_contains
);
primitive_static_filter!(
    Float64StaticFilter,
    Float64Type,
    f64::to_ne_bytes,
    HashSet<[u8; 8]>,
    HashSet::from_iter,
    float64_contains
);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        DictionaryArray, Float16Array, Float32Array, Int8Array, Int16Array, UInt8Array,
        UInt16Array, UInt32Array,
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
    fn static_filter_handles_slices_nulls_and_dictionaries() -> Result<()> {
        let mut values = (0..36).map(Some).collect::<Vec<_>>();
        values[2] = None;
        let haystack: ArrayRef = Arc::new(UInt32Array::from(values).slice(1, 34));
        let filter = UInt32StaticFilter::try_new(&haystack)?;

        let keys = Int8Array::from(vec![Some(0), Some(1), None, Some(2)]);
        let values = Arc::new(UInt32Array::from(vec![1, 35, 34]));
        let needles = DictionaryArray::try_new(keys, values)?;
        assert_contains(&filter, &needles, vec![Some(true), None, None, Some(true)])?;
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );
        Ok(())
    }

    #[test]
    fn static_filter_floats_use_bit_equality() -> Result<()> {
        let nan_a = f32::from_bits(0x7fc0_0001);
        let nan_b = f32::from_bits(0x7fc0_0002);
        let haystack: ArrayRef =
            Arc::new(Float32Array::from(vec![Some(-0.0), Some(nan_a)]));
        let filter = Float32StaticFilter::try_new(&haystack)?;
        let needles =
            Float32Array::from(vec![Some(0.0), Some(-0.0), Some(nan_a), Some(nan_b)]);

        assert_contains(
            &filter,
            &needles,
            vec![Some(false), Some(true), Some(true), Some(false)],
        )
    }
}
