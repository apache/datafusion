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

use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use arrow::datatypes::*;
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::{HashSet, Result, exec_datafusion_err};

use super::branchless_filter::{BranchlessFilter, BranchlessFilterType};
use super::result::build_in_list_result;
use super::static_filter::{StaticFilter, StaticFilterRef};

/// Selects an optimized filter for a primitive representation.
///
/// Supported short lists use a branchless filter. Larger supported lists use a
/// bitmap or hash-set filter. Returns `None` for primitive types without a
/// specialized filter. Representation adapters call this after conversion and
/// use the same cutoffs and fallback policy as native primitive arrays.
pub(super) fn instantiate_primitive_filter(
    in_array: &ArrayRef,
) -> Result<Option<StaticFilterRef>> {
    if let Some(filter) = instantiate_branchless_filter(in_array)? {
        return Ok(Some(filter));
    }

    let filter: StaticFilterRef = match in_array.data_type() {
        DataType::Int8 => Arc::new(BitmapFilter::<Int8Type>::try_new(in_array)?),
        DataType::UInt8 => Arc::new(BitmapFilter::<UInt8Type>::try_new(in_array)?),
        DataType::Int16 => Arc::new(BitmapFilter::<Int16Type>::try_new(in_array)?),
        DataType::UInt16 => Arc::new(BitmapFilter::<UInt16Type>::try_new(in_array)?),
        DataType::Float16 => Arc::new(BitmapFilter::<Float16Type>::try_new(in_array)?),
        DataType::Int32 => {
            Arc::new(PrimitiveHashSetFilter::<Int32Type>::try_new(in_array)?)
        }
        DataType::Int64 => {
            Arc::new(PrimitiveHashSetFilter::<Int64Type>::try_new(in_array)?)
        }
        DataType::UInt32 => {
            Arc::new(PrimitiveHashSetFilter::<UInt32Type>::try_new(in_array)?)
        }
        DataType::UInt64 => {
            Arc::new(PrimitiveHashSetFilter::<UInt64Type>::try_new(in_array)?)
        }
        DataType::Decimal128(_, _) => {
            Arc::new(PrimitiveHashSetFilter::<Decimal128Type>::try_new(in_array)?)
        }
        // Float primitive types use ordered wrapper keys for Hash/Eq.
        DataType::Float32 => Arc::new(PrimitiveHashSetFilter::<
            Float32Type,
            OrderedFloat32,
        >::try_new(in_array)?),
        DataType::Float64 => Arc::new(PrimitiveHashSetFilter::<
            Float64Type,
            OrderedFloat64,
        >::try_new(in_array)?),
        _ => return Ok(None),
    };

    Ok(Some(filter))
}

fn instantiate_branchless_filter(in_array: &ArrayRef) -> Result<Option<StaticFilterRef>> {
    let non_null_count = in_array.len() - in_array.null_count();

    macro_rules! branchless {
        ($arrow_type:ty) => {{
            // Larger lists use the standard primitive filter. `try_new`
            // checks the limit again when called directly.
            if non_null_count > <$arrow_type as BranchlessFilterType>::MAX_LIST_LEN {
                Ok(None)
            } else {
                let filter: StaticFilterRef =
                    Arc::new(BranchlessFilter::<$arrow_type>::try_new(in_array)?);
                Ok(Some(filter))
            }
        }};
    }

    match in_array.data_type() {
        DataType::Int8 => branchless!(Int8Type),
        DataType::UInt8 => branchless!(UInt8Type),
        DataType::Int16 => branchless!(Int16Type),
        DataType::UInt16 => branchless!(UInt16Type),
        DataType::Float16 => branchless!(Float16Type),
        DataType::Int32 => branchless!(Int32Type),
        DataType::UInt32 => branchless!(UInt32Type),
        DataType::Float32 => branchless!(Float32Type),
        DataType::Date32 => branchless!(Date32Type),
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => branchless!(Time32SecondType),
            TimeUnit::Millisecond => branchless!(Time32MillisecondType),
            _ => Ok(None),
        },
        DataType::Int64 => branchless!(Int64Type),
        DataType::UInt64 => branchless!(UInt64Type),
        DataType::Float64 => branchless!(Float64Type),
        DataType::Date64 => branchless!(Date64Type),
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => branchless!(Time64MicrosecondType),
            TimeUnit::Nanosecond => branchless!(Time64NanosecondType),
            _ => Ok(None),
        },
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => branchless!(TimestampSecondType),
            TimeUnit::Millisecond => branchless!(TimestampMillisecondType),
            TimeUnit::Microsecond => branchless!(TimestampMicrosecondType),
            TimeUnit::Nanosecond => branchless!(TimestampNanosecondType),
        },
        DataType::Duration(unit) => match unit {
            TimeUnit::Second => branchless!(DurationSecondType),
            TimeUnit::Millisecond => branchless!(DurationMillisecondType),
            TimeUnit::Microsecond => branchless!(DurationMicrosecondType),
            TimeUnit::Nanosecond => branchless!(DurationNanosecondType),
        },
        DataType::Decimal128(_, _) => branchless!(Decimal128Type),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            branchless!(IntervalMonthDayNanoType)
        }
        _ => Ok(None),
    }
}

/// Storage for the bits used by [`BitmapFilter`].
///
/// `BitmapFilter` represents an `IN` list with one bit for each possible
/// value, so membership checks become direct bit tests. This trait lets the
/// same filter code use different storage sizes for different integer widths.
trait BitmapStorage: Send + Sync {
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
trait BitmapFilterType: ArrowPrimitiveType + Send + Sync + 'static {
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
struct BitmapFilter<T: BitmapFilterType> {
    null_count: usize,
    bits: T::Storage,
}

impl<T> BitmapFilter<T>
where
    T: BitmapFilterType,
{
    fn try_new(in_array: &ArrayRef) -> Result<Self> {
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

/// Hash-set membership for primitive types.
///
/// `K` defaults to the Arrow type's native value. Floats use an ordered wrapper
/// key because their native values do not implement [`Eq`] and [`Hash`].
struct PrimitiveHashSetFilter<
    T: ArrowPrimitiveType,
    K = <T as ArrowPrimitiveType>::Native,
> {
    null_count: usize,
    values: HashSet<K>,
    _marker: PhantomData<T>,
}

impl<T, K> PrimitiveHashSetFilter<T, K>
where
    T: ArrowPrimitiveType,
    T::Native: Copy,
    K: From<T::Native> + Eq + Hash,
{
    fn try_new(in_array: &ArrayRef) -> Result<Self> {
        let in_array = in_array.as_primitive_opt::<T>().ok_or_else(|| {
            exec_datafusion_err!(
                "PrimitiveHashSetFilter: expected {} array",
                T::DATA_TYPE
            )
        })?;
        let mut values = HashSet::with_capacity(in_array.len() - in_array.null_count());
        for value in in_array.iter().flatten() {
            values.insert(K::from(value));
        }

        Ok(Self {
            null_count: in_array.null_count(),
            values,
            _marker: PhantomData,
        })
    }
}

impl<T, K> StaticFilter for PrimitiveHashSetFilter<T, K>
where
    T: ArrowPrimitiveType + Send + Sync + 'static,
    T::Native: Copy + Send + Sync,
    K: From<T::Native> + Eq + Hash + Send + Sync + 'static,
{
    fn null_count(&self) -> usize {
        self.null_count
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        let v = v.as_primitive_opt::<T>().ok_or_else(|| {
            exec_datafusion_err!(
                "PrimitiveHashSetFilter: expected {} array",
                T::DATA_TYPE
            )
        })?;
        let input_values = v.values();
        Ok(build_in_list_result(
            v.len(),
            v.nulls(),
            self.null_count > 0,
            negated,
            |index| {
                let key = K::from(input_values[index]);
                self.values.contains(&key)
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        DictionaryArray, Float16Array, Float32Array, Float64Array, Int8Array, Int16Array,
        UInt8Array, UInt16Array, UInt32Array,
    };
    use half::f16;

    use super::super::dictionary_filter::DictionaryFilter;

    fn uint32_array(values: Vec<Option<u32>>) -> ArrayRef {
        Arc::new(UInt32Array::from(values))
    }

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
    fn branchless_routing_respects_max_list_len() -> Result<()> {
        let max_len = <UInt32Type as BranchlessFilterType>::MAX_LIST_LEN;

        let values = (0..max_len)
            .map(|value| Some(value as u32))
            .collect::<Vec<_>>();
        assert!(instantiate_branchless_filter(&uint32_array(values))?.is_some());

        let values = (0..=max_len)
            .map(|value| Some(value as u32))
            .collect::<Vec<_>>();
        assert!(instantiate_branchless_filter(&uint32_array(values))?.is_none());

        Ok(())
    }

    #[test]
    fn branchless_routing_handles_zero_non_null_values() -> Result<()> {
        let array = uint32_array(vec![None; 3]);

        assert!(instantiate_branchless_filter(&array)?.is_some());

        Ok(())
    }

    #[test]
    fn primitive_hash_filter_handles_float_keys() -> Result<()> {
        let nan32 = f32::NAN;
        let other_nan32 = f32::from_bits(nan32.to_bits() + 1);
        let haystack: ArrayRef = Arc::new(Float32Array::from(vec![0.0, nan32]));
        let filter =
            PrimitiveHashSetFilter::<Float32Type, OrderedFloat32>::try_new(&haystack)?;
        let needles = Float32Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(nan32),
            Some(other_nan32),
            None,
        ]);
        assert_contains(
            &filter,
            &needles,
            vec![Some(true), Some(false), Some(true), Some(false), None],
        )?;

        let nan64 = f64::NAN;
        let haystack: ArrayRef = Arc::new(Float64Array::from(vec![1.0, nan64]));
        let filter =
            PrimitiveHashSetFilter::<Float64Type, OrderedFloat64>::try_new(&haystack)?;
        let needles = Float64Array::from(vec![Some(1.0), Some(nan64), Some(2.0)]);
        assert_contains(&filter, &needles, vec![Some(true), Some(true), Some(false)])
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
        let inner: StaticFilterRef =
            Arc::new(BitmapFilter::<UInt8Type>::try_new(&haystack)?);
        let filter = DictionaryFilter::new(inner);

        let keys = Int8Array::from(vec![Some(0), Some(1), None, Some(2)]);
        let values = Arc::new(UInt8Array::from(vec![Some(1), Some(2), Some(3)]));
        let needles = DictionaryArray::try_new(keys, values)?;

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), None, None, Some(true)])
        );
        Ok(())
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
}
