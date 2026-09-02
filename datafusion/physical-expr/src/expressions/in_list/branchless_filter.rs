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

//! Fast membership tests for small, fixed-width primitive `IN` lists.
//!
//! # Why use a branchless filter?
//!
//! For a short list such as `x IN (10, 20, 30)`, it can be faster to compare
//! `x` with all three values than to build and search a hash table.
//!
//! "Branchless" means that the filter always checks every list value. It
//! combines the answers with `|`, while `||` would stop at the first match.
//! This regular sequence of comparisons is easier for the compiler and CPU to
//! optimize.
//!
//! # How does it work?
//!
//! When the filter is built, it stores the non-null list values and chooses a
//! comparison function for that list length. Only this small function is
//! specialized for each length. The rest of [`BranchlessFilter`] is shared,
//! which keeps the generated code small.
//!
//! Some Arrow types share the same in-memory representation. For example, a
//! `Float32` and a `UInt32` both use four bytes per value. The filter compares
//! those stored bits through an unsigned type of the same size, without copying
//! the value buffer. A bit pattern is simply the bytes Arrow uses to store a
//! value. Comparing it preserves details such as `0.0` versus `-0.0` and
//! different NaN values. [`BranchlessFilterType`] defines these safe,
//! same-sized mappings and checks their sizes at compile time.
//!
//! The fast path is limited to short lists:
//!
//! - 16 values for 1-byte types
//! - 8 values for 2-byte types
//! - 32 values for 4-byte types
//! - 16 values for 8-byte types
//! - 4 values for 16-byte types
//!
//! These numbers do not follow one size-based pattern. One- and two-byte
//! values have an especially efficient next step: every possible bit pattern
//! fits in a compact bitmap. For a longer list, the bitmap filter turns on one
//! bit for each listed value, then checks membership with a direct bit lookup.
//! This becomes a better fit before a 64- or 128-comparison branchless chain
//! would be useful. Wider types have too many possible values for such a
//! bitmap, so their limits are tuned separately.
//!
//! Larger lists use the standard filter strategy, including bitmap filters for
//! one- and two-byte types.
//!
//! # What about nulls?
//!
//! Null list entries are omitted from the comparison chain but counted by the
//! filter. Evaluation first records which values matched, then
//! [`build_result_from_contains`] combines it with input nulls, list nulls, and
//! `NOT IN` to produce the usual SQL null behavior.

use std::mem::size_of;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, ScalarBuffer};
use arrow::datatypes::*;
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::{Result, exec_datafusion_err, internal_datafusion_err};

use super::result::build_result_from_contains;
use super::static_filter::StaticFilter;

pub(super) type BranchlessNative<T> =
    <<T as BranchlessFilterType>::CompareType as ArrowPrimitiveType>::Native;

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

/// Arrow primitive types supported by [`BranchlessFilter`].
///
/// `T` is the logical Arrow type accepted by the filter. `CompareType` is the
/// same-width type used for the fixed comparison chain. Signed integers,
/// floats, and temporal values use an unsigned comparison type so they compare
/// by their raw bit pattern.
pub(super) trait BranchlessFilterType:
    ArrowPrimitiveType + Send + Sync + 'static
{
    type CompareType: ArrowPrimitiveType + Send + Sync + 'static;

    /// Maximum number of non-null IN-list values to handle with
    /// [`BranchlessFilter`] for this primitive type.
    const MAX_LIST_LEN: usize;
}

macro_rules! branchless_filter_type {
    ($logical:ty, $compare:ty, $max_len:expr) => {
        // The branchless filter reads the same Arrow value buffer as the
        // comparison type. That is only valid when both native types have the
        // same width, so catch any bad mapping here at compile time.
        const _: () = assert!(
            size_of::<<$logical as ArrowPrimitiveType>::Native>()
                == size_of::<<$compare as ArrowPrimitiveType>::Native>(),
            "BranchlessFilterType::CompareType must use the same native width"
        );

        impl BranchlessFilterType for $logical {
            type CompareType = $compare;
            const MAX_LIST_LEN: usize = $max_len;
        }
    };
}

branchless_filter_type!(Int8Type, UInt8Type, BRANCHLESS_MAX_1B);
branchless_filter_type!(UInt8Type, UInt8Type, BRANCHLESS_MAX_1B);
branchless_filter_type!(Int16Type, UInt16Type, BRANCHLESS_MAX_2B);
branchless_filter_type!(UInt16Type, UInt16Type, BRANCHLESS_MAX_2B);
branchless_filter_type!(Float16Type, UInt16Type, BRANCHLESS_MAX_2B);

branchless_filter_type!(Int32Type, UInt32Type, BRANCHLESS_MAX_4B);
branchless_filter_type!(UInt32Type, UInt32Type, BRANCHLESS_MAX_4B);
branchless_filter_type!(Float32Type, UInt32Type, BRANCHLESS_MAX_4B);
branchless_filter_type!(Date32Type, UInt32Type, BRANCHLESS_MAX_4B);
branchless_filter_type!(Time32SecondType, UInt32Type, BRANCHLESS_MAX_4B);
branchless_filter_type!(Time32MillisecondType, UInt32Type, BRANCHLESS_MAX_4B);

branchless_filter_type!(Int64Type, UInt64Type, BRANCHLESS_MAX_8B);
branchless_filter_type!(UInt64Type, UInt64Type, BRANCHLESS_MAX_8B);
branchless_filter_type!(Float64Type, UInt64Type, BRANCHLESS_MAX_8B);
branchless_filter_type!(Date64Type, UInt64Type, BRANCHLESS_MAX_8B);
branchless_filter_type!(Time64MicrosecondType, UInt64Type, BRANCHLESS_MAX_8B);
branchless_filter_type!(Time64NanosecondType, UInt64Type, BRANCHLESS_MAX_8B);
branchless_filter_type!(TimestampSecondType, UInt64Type, BRANCHLESS_MAX_8B);
branchless_filter_type!(TimestampMillisecondType, UInt64Type, BRANCHLESS_MAX_8B);
branchless_filter_type!(TimestampMicrosecondType, UInt64Type, BRANCHLESS_MAX_8B);
branchless_filter_type!(TimestampNanosecondType, UInt64Type, BRANCHLESS_MAX_8B);
branchless_filter_type!(DurationSecondType, UInt64Type, BRANCHLESS_MAX_8B);
branchless_filter_type!(DurationMillisecondType, UInt64Type, BRANCHLESS_MAX_8B);
branchless_filter_type!(DurationMicrosecondType, UInt64Type, BRANCHLESS_MAX_8B);
branchless_filter_type!(DurationNanosecondType, UInt64Type, BRANCHLESS_MAX_8B);

branchless_filter_type!(Decimal128Type, Decimal128Type, BRANCHLESS_MAX_16B);
branchless_filter_type!(
    IntervalMonthDayNanoType,
    IntervalMonthDayNanoType,
    BRANCHLESS_MAX_16B
);

/// Checks each input value against the `IN`-list values.
type MembershipCheck<C> = fn(in_list_values: &[C], input_values: &[C]) -> BooleanBuffer;

/// A branchless filter for fixed-width primitive `IN` lists up to
/// `T::MAX_LIST_LEN` values.
///
/// The filter stores the non-null `IN`-list values in a slice and chooses a
/// comparison function for that length. Keeping the length out of
/// `BranchlessFilter` avoids generating a full copy of the filter for every
/// supported length.
pub(super) struct BranchlessFilter<T: BranchlessFilterType> {
    expected_data_type: DataType,
    null_count: usize,
    in_list_values: Box<[BranchlessNative<T>]>,
    check_values: MembershipCheck<BranchlessNative<T>>,
}

impl<T> BranchlessFilter<T>
where
    T: BranchlessFilterType,
    BranchlessNative<T>: Copy + PartialEq,
{
    pub(super) fn try_new(in_array: &ArrayRef) -> Result<Self> {
        let in_array = in_array.as_primitive_opt::<T>().ok_or_else(|| {
            exec_datafusion_err!("BranchlessFilter: expected {} array", T::DATA_TYPE)
        })?;
        let non_null_count = in_array.len() - in_array.null_count();
        // `try_new` can be called on its own, so check the limit here too.
        if non_null_count > T::MAX_LIST_LEN {
            return Err(internal_datafusion_err!(
                "BranchlessFilter: supports at most {} non-null values, got {non_null_count}",
                T::MAX_LIST_LEN
            ));
        }

        let all_values = branchless_values::<T>(in_array);
        let mut in_list_values = Vec::with_capacity(non_null_count);

        match in_array.nulls() {
            None => {
                in_list_values.extend(all_values.iter().copied());
            }
            Some(nulls) => {
                for row in
                    BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                {
                    in_list_values.push(all_values[row]);
                }
            }
        }

        debug_assert_eq!(in_list_values.len(), non_null_count);
        let in_list_values = in_list_values.into_boxed_slice();
        let check_values = membership_check_for_len::<T>(in_list_values.len());

        Ok(Self {
            expected_data_type: in_array.data_type().clone(),
            null_count: in_array.null_count(),
            in_list_values,
            check_values,
        })
    }
}

impl<T> StaticFilter for BranchlessFilter<T>
where
    T: BranchlessFilterType,
    BranchlessNative<T>: Copy + PartialEq + Send + Sync,
{
    fn null_count(&self) -> usize {
        self.null_count
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
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
        let input_values = branchless_values::<T>(v);
        let matches =
            (self.check_values)(self.in_list_values.as_ref(), input_values.as_ref());
        Ok(build_result_from_contains(
            v.nulls(),
            self.null_count > 0,
            negated,
            matches,
        ))
    }
}

/// Picks the comparison function for `len` non-null `IN`-list values.
///
/// A length of zero is used when the list contains only nulls. The comparisons
/// return false, and the caller then applies the usual SQL null behavior.
fn membership_check_for_len<T>(len: usize) -> MembershipCheck<BranchlessNative<T>>
where
    T: BranchlessFilterType,
    BranchlessNative<T>: Copy + PartialEq,
{
    macro_rules! choose {
        ($($n:literal),* $(,)?) => {
            match len {
                $($n => check_values::<BranchlessNative<T>, $n>,)*
                _ => unreachable!("list length exceeds the configured limit"),
            }
        };
    }

    // Avoid creating checks for lengths a type does not support.
    match T::MAX_LIST_LEN {
        4 => choose!(0, 1, 2, 3, 4),
        8 => choose!(0, 1, 2, 3, 4, 5, 6, 7, 8),
        16 => choose!(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),
        32 => choose!(
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
            22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
        ),
        _ => unreachable!("list-size limits must be 4, 8, 16, or 32"),
    }
}

#[inline]
#[expect(
    clippy::needless_bitwise_bool,
    reason = "the non-short-circuiting `|` is the point of this branchless kernel"
)]
fn check_values<C, const N: usize>(
    in_list_values: &[C],
    input_values: &[C],
) -> BooleanBuffer
where
    C: Copy + PartialEq,
{
    let in_list_values: &[C; N] = in_list_values
        .try_into()
        .expect("comparison length matches IN-list values");

    BooleanBuffer::collect_bool(input_values.len(), |i| {
        // SAFETY: `collect_bool` invokes this closure for indices in
        // `0..input_values.len()`.
        let input_value = unsafe { *input_values.get_unchecked(i) };
        // `|` checks every list value; `||` would stop after the first match.
        in_list_values
            .iter()
            .fold(false, |acc, &value| acc | (value == input_value))
    })
}

fn branchless_values<T>(array: &PrimitiveArray<T>) -> ScalarBuffer<BranchlessNative<T>>
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Decimal128Array, Float16Array, Float32Array, Float64Array, Int8Array,
        IntervalMonthDayNanoArray, TimestampMillisecondArray, TimestampNanosecondArray,
        UInt8Array, UInt16Array,
    };
    use half::f16;

    use super::*;

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
    fn branchless_filter_u8_handles_nulls() -> Result<()> {
        let haystack: ArrayRef = Arc::new(UInt8Array::from(vec![Some(1), None, Some(3)]));
        let filter = BranchlessFilter::<UInt8Type>::try_new(&haystack)?;
        let needles = UInt8Array::from(vec![Some(1), Some(2), None, Some(3)]);

        assert_contains(&filter, &needles, vec![Some(true), None, None, Some(true)])?;
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );

        Ok(())
    }

    #[test]
    fn branchless_filter_all_null_list_preserves_sql_null_semantics() -> Result<()> {
        let haystack: ArrayRef = Arc::new(UInt8Array::from(vec![None, None]));
        let filter = BranchlessFilter::<UInt8Type>::try_new(&haystack)?;
        let needles = UInt8Array::from(vec![Some(1), None]);
        let expected = BooleanArray::from(vec![None, None]);

        assert_eq!(filter.contains(&needles, false)?, expected);
        assert_eq!(filter.contains(&needles, true)?, expected);

        Ok(())
    }

    #[test]
    fn branchless_filter_i8_handles_signed_boundaries_and_slices() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            Int8Array::from(vec![Some(99), Some(i8::MIN), None, Some(-1), Some(42)])
                .slice(1, 3),
        );
        let filter = BranchlessFilter::<Int8Type>::try_new(&haystack)?;
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
        let filter = BranchlessFilter::<Float16Type>::try_new(&haystack)?;
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
        let filter = BranchlessFilter::<Float32Type>::try_new(&haystack)?;
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
        let filter = BranchlessFilter::<Float64Type>::try_new(&haystack)?;
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
        let filter = BranchlessFilter::<TimestampNanosecondType>::try_new(&haystack)?;
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
        let filter = BranchlessFilter::<Decimal128Type>::try_new(&haystack)?;
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
        let filter = BranchlessFilter::<IntervalMonthDayNanoType>::try_new(&haystack)?;
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
}
