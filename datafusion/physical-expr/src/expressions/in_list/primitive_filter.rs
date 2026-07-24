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
use arrow::buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::*;
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::{HashSet, Result, exec_datafusion_err};
use std::hash::{Hash, Hasher};
use std::mem::size_of;

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

pub(super) type PrimitiveFilterNative<T> =
    <<T as PrimitiveFilterType>::CompareType as ArrowPrimitiveType>::Native;

/// Maximum list size for branchless lookup on 1-byte primitives.
///
/// Sixteen 1-byte values fit in one 128-bit SIMD vector, so this keeps the
/// branchless list small enough for a single vectorized membership check.
const BRANCHLESS_MAX_1B: usize = 16;

/// Maximum list size for branchless lookup on 2-byte primitives.
///
/// Eight 2-byte values fit in one 128-bit SIMD vector, so this keeps the
/// branchless list small enough for a single vectorized membership check.
const BRANCHLESS_MAX_2B: usize = 8;

/// Maximum list size for branchless lookup on 4-byte primitives.
///
/// Thirty-two 4-byte values keep the inline list at 128 bytes. Beyond that,
/// the comparison chain and filter footprint grow enough that the hash/generic
/// fallback is a better fit.
const BRANCHLESS_MAX_4B: usize = 32;

/// Maximum list size for branchless lookup on 8-byte primitives.
///
/// Sixteen 8-byte values use the same 128-byte inline-list budget as 4-byte
/// primitives. Larger lists are left to the hash/generic fallback.
const BRANCHLESS_MAX_8B: usize = 16;

/// Maximum list size for branchless lookup on 16-byte primitives.
///
/// These comparisons are wider, so this path is limited to four values.
/// Larger lists are left to the generic fallback.
const BRANCHLESS_MAX_16B: usize = 4;

/// Arrow primitive types supported by fixed-width primitive filters.
///
/// `T` is the logical Arrow type accepted by the filter. `CompareType` is the
/// same-width type used for the optimized lookup. Signed integers, floats, and
/// temporal values use an unsigned comparison type so they compare by their raw
/// bit pattern.
pub(super) trait PrimitiveFilterType:
    ArrowPrimitiveType + Send + Sync + 'static
{
    type CompareType: ArrowPrimitiveType + Send + Sync + 'static;

    /// Maximum number of non-null IN-list values to handle with
    /// [`BranchlessFilter`] for this primitive type.
    const BRANCHLESS_MAX_LIST_LEN: usize;
}

macro_rules! primitive_filter_type {
    ($logical:ty, $compare:ty, $max_len:expr) => {
        // These filters read the same Arrow value buffer as the comparison
        // type. That is only valid when both native types have the same width,
        // so catch any bad mapping here at compile time.
        const _: () = assert!(
            size_of::<<$logical as ArrowPrimitiveType>::Native>()
                == size_of::<<$compare as ArrowPrimitiveType>::Native>(),
            "PrimitiveFilterType::CompareType must use the same native width"
        );

        impl PrimitiveFilterType for $logical {
            type CompareType = $compare;
            const BRANCHLESS_MAX_LIST_LEN: usize = $max_len;
        }
    };
}

primitive_filter_type!(Int8Type, UInt8Type, BRANCHLESS_MAX_1B);
primitive_filter_type!(UInt8Type, UInt8Type, BRANCHLESS_MAX_1B);
primitive_filter_type!(Int16Type, UInt16Type, BRANCHLESS_MAX_2B);
primitive_filter_type!(UInt16Type, UInt16Type, BRANCHLESS_MAX_2B);
primitive_filter_type!(Float16Type, UInt16Type, BRANCHLESS_MAX_2B);

primitive_filter_type!(Int32Type, UInt32Type, BRANCHLESS_MAX_4B);
primitive_filter_type!(UInt32Type, UInt32Type, BRANCHLESS_MAX_4B);
primitive_filter_type!(Float32Type, UInt32Type, BRANCHLESS_MAX_4B);
primitive_filter_type!(Date32Type, UInt32Type, BRANCHLESS_MAX_4B);
primitive_filter_type!(Time32SecondType, UInt32Type, BRANCHLESS_MAX_4B);
primitive_filter_type!(Time32MillisecondType, UInt32Type, BRANCHLESS_MAX_4B);

primitive_filter_type!(Int64Type, UInt64Type, BRANCHLESS_MAX_8B);
primitive_filter_type!(UInt64Type, UInt64Type, BRANCHLESS_MAX_8B);
primitive_filter_type!(Float64Type, UInt64Type, BRANCHLESS_MAX_8B);
primitive_filter_type!(Date64Type, UInt64Type, BRANCHLESS_MAX_8B);
primitive_filter_type!(Time64MicrosecondType, UInt64Type, BRANCHLESS_MAX_8B);
primitive_filter_type!(Time64NanosecondType, UInt64Type, BRANCHLESS_MAX_8B);
primitive_filter_type!(TimestampSecondType, UInt64Type, BRANCHLESS_MAX_8B);
primitive_filter_type!(TimestampMillisecondType, UInt64Type, BRANCHLESS_MAX_8B);
primitive_filter_type!(TimestampMicrosecondType, UInt64Type, BRANCHLESS_MAX_8B);
primitive_filter_type!(TimestampNanosecondType, UInt64Type, BRANCHLESS_MAX_8B);
primitive_filter_type!(DurationSecondType, UInt64Type, BRANCHLESS_MAX_8B);
primitive_filter_type!(DurationMillisecondType, UInt64Type, BRANCHLESS_MAX_8B);
primitive_filter_type!(DurationMicrosecondType, UInt64Type, BRANCHLESS_MAX_8B);
primitive_filter_type!(DurationNanosecondType, UInt64Type, BRANCHLESS_MAX_8B);

primitive_filter_type!(Decimal128Type, Decimal128Type, BRANCHLESS_MAX_16B);
primitive_filter_type!(
    IntervalMonthDayNanoType,
    IntervalMonthDayNanoType,
    BRANCHLESS_MAX_16B
);

/// A branchless filter for fixed-width primitive `IN` lists up to
/// `T::BRANCHLESS_MAX_LIST_LEN` values.
///
/// The filter stores the non-null values in a fixed-size array and checks each
/// input value with a fixed comparison chain. This avoids hash table work for
/// short lists where a handful of direct comparisons is cheaper.
pub(super) struct BranchlessFilter<T: PrimitiveFilterType, const N: usize> {
    expected_data_type: DataType,
    null_count: usize,
    values: [PrimitiveFilterNative<T>; N],
}

impl<T, const N: usize> BranchlessFilter<T, N>
where
    T: PrimitiveFilterType,
    PrimitiveFilterNative<T>: Copy + PartialEq,
{
    pub(super) fn try_new(in_array: &ArrayRef) -> Result<Self> {
        let in_array = in_array.as_primitive_opt::<T>().ok_or_else(|| {
            exec_datafusion_err!("BranchlessFilter: expected {} array", T::DATA_TYPE)
        })?;
        let non_null_count = in_array.len() - in_array.null_count();
        if non_null_count != N {
            return Err(exec_datafusion_err!(
                "BranchlessFilter: expected {N} non-null values, got {non_null_count}"
            ));
        }

        let input_values = primitive_values::<T>(in_array);
        let mut values = [<T::CompareType as ArrowPrimitiveType>::default_value(); N];
        let mut i = 0;

        match in_array.nulls() {
            None => {
                for &value in input_values.iter() {
                    values[i] = value;
                    i += 1;
                }
            }
            Some(nulls) => {
                for row in
                    BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                {
                    values[i] = input_values[row];
                    i += 1;
                }
            }
        }

        debug_assert_eq!(i, N);
        Ok(Self {
            expected_data_type: in_array.data_type().clone(),
            null_count: in_array.null_count(),
            values,
        })
    }

    #[inline(always)]
    fn check(&self, needle: PrimitiveFilterNative<T>) -> bool {
        // `N` is known at compile time, and this small method is always
        // inlined. In optimized builds, the compiler can turn this into the
        // same kind of code we would write by hand:
        // `(value0 == needle) | (value1 == needle) | ...`.
        //
        // This simple shape also lets the optimizer use vector instructions
        // (SIMD) when the caller applies the filter to many input rows.
        //
        // Use `|` instead of `||` so every comparison is evaluated. With `||`,
        // the code would branch as soon as one value matched.
        self.values
            .iter()
            .fold(false, |acc, &v| acc | (v == needle))
    }

    #[inline]
    fn contains_slice(
        &self,
        values: &[PrimitiveFilterNative<T>],
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

impl<T, const N: usize> StaticFilter for BranchlessFilter<T, N>
where
    T: PrimitiveFilterType,
    PrimitiveFilterNative<T>: Copy + PartialEq + Send + Sync,
{
    fn null_count(&self) -> usize {
        self.null_count
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        // Arrow compatibility ignores timestamp timezone and decimal precision/scale
        // while still requiring the same primitive representation.
        if !PrimitiveArray::<T>::is_compatible(v.data_type()) {
            return Err(exec_datafusion_err!(
                "BranchlessFilter: expected {} array, got {}",
                self.expected_data_type,
                v.data_type()
            ));
        }

        let v = v.as_primitive_opt::<T>().ok_or_else(|| {
            exec_datafusion_err!("BranchlessFilter: expected {} array", T::DATA_TYPE)
        })?;
        let values = primitive_values::<T>(v);
        Ok(self.contains_slice(values.as_ref(), v.nulls(), negated))
    }
}

fn primitive_values<T>(
    array: &PrimitiveArray<T>,
) -> ScalarBuffer<PrimitiveFilterNative<T>>
where
    T: PrimitiveFilterType,
{
    let data = array.to_data();
    ScalarBuffer::<PrimitiveFilterNative<T>>::new(
        data.buffers()[0].clone(),
        data.offset(),
        data.len(),
    )
}

/// Frozen-set filter for larger fixed-width primitive `IN` lists.
pub(super) struct PrimitiveFrozenFilter<T: PrimitiveFilterType>
where
    PrimitiveFilterNative<T>: Copy + Eq + Hash,
{
    expected_data_type: DataType,
    null_count: usize,
    values: FrozenSet<PrimitiveFilterNative<T>>,
}

impl<T> PrimitiveFrozenFilter<T>
where
    T: PrimitiveFilterType,
    PrimitiveFilterNative<T>: Copy + Eq + Hash,
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
        values: &[PrimitiveFilterNative<T>],
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
    T: PrimitiveFilterType,
    PrimitiveFilterNative<T>: Copy + Eq + Hash + Send + Sync,
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

    use arrow::array::{
        Decimal128Array, DictionaryArray, Float16Array, Float32Array, Float64Array,
        Int8Array, Int16Array, IntervalMonthDayNanoArray, TimestampMillisecondArray,
        TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
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
    fn branchless_filter_u8_handles_nulls() -> Result<()> {
        let haystack: ArrayRef = Arc::new(UInt8Array::from(vec![Some(1), None, Some(3)]));
        let filter = BranchlessFilter::<UInt8Type, 2>::try_new(&haystack)?;
        let needles = UInt8Array::from(vec![Some(1), Some(2), None, Some(3)]);

        assert_contains(&filter, &needles, vec![Some(true), None, None, Some(true)])?;
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );

        Ok(())
    }

    #[test]
    fn branchless_filter_i8_handles_signed_boundaries_and_slices() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            Int8Array::from(vec![Some(99), Some(i8::MIN), None, Some(-1), Some(42)])
                .slice(1, 3),
        );
        let filter = BranchlessFilter::<Int8Type, 2>::try_new(&haystack)?;
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

        let wrong_type = UInt8Array::from(vec![Some(128), Some(u8::MAX)]);
        let err = filter.contains(&wrong_type, false).unwrap_err().to_string();
        assert!(err.contains("expected Int8 array, got UInt8"), "{err}");

        Ok(())
    }

    #[test]
    fn branchless_filter_f16_handles_bit_patterns_and_slices() -> Result<()> {
        let nan_a = f16::from_bits(0x7e01);
        let nan_b = f16::from_bits(0x7e02);
        let haystack: ArrayRef = Arc::new(
            Float16Array::from(vec![
                Some(f16::from_f32(9.0)),
                Some(f16::from_f32(-0.0)),
                Some(nan_a),
                None,
            ])
            .slice(1, 3),
        );
        let filter = BranchlessFilter::<Float16Type, 2>::try_new(&haystack)?;
        let needles = Float16Array::from(vec![
            Some(f16::from_f32(0.0)),
            Some(f16::from_f32(-0.0)),
            Some(nan_a),
            Some(nan_b),
            None,
        ]);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![None, Some(true), Some(true), None, None])
        );
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![None, Some(false), Some(false), None, None])
        );

        let wrong_type = UInt16Array::from(vec![Some(0x8000), Some(0x7e01)]);
        let err = filter.contains(&wrong_type, false).unwrap_err().to_string();
        assert!(err.contains("expected Float16 array, got UInt16"), "{err}");

        Ok(())
    }

    #[test]
    fn branchless_filter_floats_use_bit_equality() -> Result<()> {
        let nan_a = f32::from_bits(0x7fc0_0001);
        let nan_b = f32::from_bits(0x7fc0_0002);
        let haystack: ArrayRef =
            Arc::new(Float32Array::from(vec![Some(-0.0), Some(nan_a)]));
        let filter = BranchlessFilter::<Float32Type, 2>::try_new(&haystack)?;
        let needles =
            Float32Array::from(vec![Some(0.0), Some(-0.0), Some(nan_a), Some(nan_b)]);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(false), Some(true), Some(true), Some(false)])
        );

        let nan_a = f64::from_bits(0x7ff8_0000_0000_0001);
        let nan_b = f64::from_bits(0x7ff8_0000_0000_0002);
        let haystack: ArrayRef =
            Arc::new(Float64Array::from(vec![Some(-0.0), Some(nan_a)]));
        let filter = BranchlessFilter::<Float64Type, 2>::try_new(&haystack)?;
        let needles =
            Float64Array::from(vec![Some(0.0), Some(-0.0), Some(nan_a), Some(nan_b)]);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(false), Some(true), Some(true), Some(false)])
        );

        Ok(())
    }

    #[test]
    fn branchless_filter_timestamp_uses_physical_compatibility() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![Some(1), Some(3)]).with_timezone("UTC"),
        );
        let filter = BranchlessFilter::<TimestampNanosecondType, 2>::try_new(&haystack)?;
        let needles = TimestampNanosecondArray::from(vec![Some(1), Some(2), None])
            .with_timezone("UTC");

        assert_contains(&filter, &needles, vec![Some(true), Some(false), None])?;

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

    #[test]
    fn branchless_filter_decimal128_handles_precision_scale_and_nulls() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(12345), None, Some(-700), Some(42)])
                .with_precision_and_scale(10, 2)?,
        );
        let filter = BranchlessFilter::<Decimal128Type, 3>::try_new(&haystack)?;
        let needles =
            Decimal128Array::from(vec![Some(12345), Some(999), None, Some(-700)])
                .with_precision_and_scale(10, 2)?;

        assert_contains(&filter, &needles, vec![Some(true), None, None, Some(true)])?;
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );

        let compatible_metadata =
            Decimal128Array::from(vec![Some(12345)]).with_precision_and_scale(11, 3)?;
        assert_contains(&filter, &compatible_metadata, vec![Some(true)])?;

        Ok(())
    }

    #[test]
    fn branchless_filter_interval_month_day_nano_handles_nulls() -> Result<()> {
        let one_month = IntervalMonthDayNanoType::make_value(1, 0, 0);
        let two_days = IntervalMonthDayNanoType::make_value(0, 2, 0);
        let three_nanos = IntervalMonthDayNanoType::make_value(0, 0, 3);
        let absent = IntervalMonthDayNanoType::make_value(4, 5, 6);
        let haystack: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(one_month),
            None,
            Some(two_days),
            Some(three_nanos),
        ]));
        let filter = BranchlessFilter::<IntervalMonthDayNanoType, 3>::try_new(&haystack)?;
        let needles = IntervalMonthDayNanoArray::from(vec![
            Some(one_month),
            Some(absent),
            None,
            Some(three_nanos),
        ]);

        assert_contains(&filter, &needles, vec![Some(true), None, None, Some(true)])?;
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
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
