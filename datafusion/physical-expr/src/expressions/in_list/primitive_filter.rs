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
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::*;
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::{HashSet, Result, exec_datafusion_err};
use std::hash::{Hash, Hasher};

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

/// Wrapper for f32 that implements Hash and Eq using bit comparison.
/// This treats NaN values as equal to each other when they have the same bit pattern.
#[derive(Clone, Copy)]
struct OrderedFloat32(f32);

impl Hash for OrderedFloat32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_ne_bytes().hash(state);
    }
}

impl PartialEq for OrderedFloat32 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat32 {}

impl From<f32> for OrderedFloat32 {
    fn from(v: f32) -> Self {
        Self(v)
    }
}

/// Wrapper for f64 that implements Hash and Eq using bit comparison.
/// This treats NaN values as equal to each other when they have the same bit pattern.
#[derive(Clone, Copy)]
struct OrderedFloat64(f64);

impl Hash for OrderedFloat64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_ne_bytes().hash(state);
    }
}

impl PartialEq for OrderedFloat64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat64 {}

impl From<f64> for OrderedFloat64 {
    fn from(v: f64) -> Self {
        Self(v)
    }
}

// Macro to generate specialized StaticFilter implementations for primitive types
macro_rules! primitive_static_filter {
    ($Name:ident, $ArrowType:ty) => {
        primitive_static_filter!(
            $Name,
            $ArrowType,
            <$ArrowType as ArrowPrimitiveType>::Native,
            |v| v
        );
    };
    ($Name:ident, $ArrowType:ty, $SetValueType:ty, $to_set_value:expr) => {
        pub(super) struct $Name {
            null_count: usize,
            values: HashSet<$SetValueType>,
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

                let mut values = HashSet::with_capacity(in_array.len());
                let null_count = in_array.null_count();

                for v in in_array.iter().flatten() {
                    values.insert(($to_set_value)(v));
                }

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

                let haystack_has_nulls = self.null_count > 0;
                let needle_values = v.values();
                let needle_nulls = v.nulls();
                let needle_has_nulls = v.null_count() > 0;

                // Truth table for `value [NOT] IN (set)` with SQL three-valued logic:
                // ("-" means the value doesn't affect the result)
                //
                // | needle_null | haystack_null | negated | in set? | result |
                // |-------------|---------------|---------|---------|--------|
                // | true        | -             | false   | -       | null   |
                // | true        | -             | true    | -       | null   |
                // | false       | true          | false   | yes     | true   |
                // | false       | true          | false   | no      | null   |
                // | false       | true          | true    | yes     | false  |
                // | false       | true          | true    | no      | null   |
                // | false       | false         | false   | yes     | true   |
                // | false       | false         | false   | no      | false  |
                // | false       | false         | true    | yes     | false  |
                // | false       | false         | true    | no      | true   |

                // Compute the "contains" result using collect_bool (fast batched approach)
                // This ignores nulls - we handle them separately
                let contains_buffer = if negated {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        !self.values.contains(&($to_set_value)(needle_values[i]))
                    })
                } else {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        self.values.contains(&($to_set_value)(needle_values[i]))
                    })
                };

                // Compute the null mask
                // Output is null when:
                // 1. needle value is null, OR
                // 2. needle value is not in set AND haystack has nulls
                let result_nulls = match (needle_has_nulls, haystack_has_nulls) {
                    (false, false) => {
                        // No nulls anywhere
                        None
                    }
                    (true, false) => {
                        // Only needle has nulls - just use needle's null mask
                        needle_nulls.cloned()
                    }
                    (false, true) => {
                        // Only haystack has nulls - result is null when value not in set
                        // Valid (not null) when original "in set" is true
                        // For NOT IN: contains_buffer = !original, so validity = !contains_buffer
                        let validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };
                        Some(NullBuffer::new(validity))
                    }
                    (true, true) => {
                        // Both have nulls - combine needle nulls with haystack-induced nulls
                        let needle_validity =
                            needle_nulls.map(|n| n.inner().clone()).unwrap_or_else(
                                || BooleanBuffer::new_set(needle_values.len()),
                            );

                        // Valid when original "in set" is true (see above)
                        let haystack_validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };

                        // Combined validity: valid only where both are valid
                        let combined_validity = &needle_validity & &haystack_validity;
                        Some(NullBuffer::new(combined_validity))
                    }
                };

                Ok(BooleanArray::new(contains_buffer, result_nulls))
            }
        }
    };
}

// Generate specialized filters for all integer primitive types
primitive_static_filter!(Int8StaticFilter, Int8Type);
primitive_static_filter!(Int16StaticFilter, Int16Type);
primitive_static_filter!(Int32StaticFilter, Int32Type);
primitive_static_filter!(Int64StaticFilter, Int64Type);
primitive_static_filter!(UInt32StaticFilter, UInt32Type);
primitive_static_filter!(UInt64StaticFilter, UInt64Type);

// Macro to generate specialized StaticFilter implementations for float types
// Floats require a wrapper type (OrderedFloat*) to implement Hash/Eq due to NaN semantics
macro_rules! float_static_filter {
    ($Name:ident, $ArrowType:ty, $OrderedType:ty) => {
        primitive_static_filter!($Name, $ArrowType, $OrderedType, <$OrderedType>::from);
    };
}

// Generate specialized filters for float types using ordered wrappers
float_static_filter!(Float32StaticFilter, Float32Type, OrderedFloat32);
float_static_filter!(Float64StaticFilter, Float64Type, OrderedFloat64);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{DictionaryArray, Int8Array, UInt8Array, UInt16Array};

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
}
