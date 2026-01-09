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

//! Type transformation utilities for InList filters.
//!
//! Some filters only depend on fixed-width value bit patterns. For those cases,
//! compatible primitive arrays can be reinterpreted to the filter's unsigned
//! storage type without copying values.

use std::hash::BuildHasher;
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{ArrowPrimitiveType, ByteViewType, Decimal128Type};
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::hash_utils::RandomState;
use datafusion_common::{Result, exec_datafusion_err};
use hashbrown::HashTable;

use super::primitive_filter::{
    BitmapFilter, BitmapFilterConfig, BranchlessFilter, DirectProbeFilter,
};
use super::result::build_in_list_result_with_null_shortcircuit;
use super::static_filter::{StaticFilter, handle_dictionary};

/// Maximum length for inline strings (≤12 bytes can be stored in 16-byte view/encoding).
/// Used by both Utf8View short string optimization and Utf8 two-stage filter.
pub(crate) const INLINE_STRING_LEN: usize = 12;

#[inline]
fn views_as_i128(views: &ScalarBuffer<u128>) -> &[i128] {
    views.inner().typed_data()
}

/// Reinterpreting filter for bitmap lookups (u8/u16).
struct ReinterpretedBitmap<C: BitmapFilterConfig> {
    inner: BitmapFilter<C>,
}

impl<C: BitmapFilterConfig> StaticFilter for ReinterpretedBitmap<C> {
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        if v.data_type().primitive_width() != Some(size_of::<C::Native>()) {
            return Err(exec_datafusion_err!(
                "BitmapFilter: expected {}-byte primitive array, got {}",
                size_of::<C::Native>(),
                v.data_type()
            ));
        }

        let data = v.to_data();
        let values: &[C::Native] = &data.buffer::<C::Native>(0)[..v.len()];

        Ok(self.inner.contains_slice(values, data.nulls(), negated))
    }
}

/// Reinterpreting filter for branchless lookups.
struct ReinterpretedBranchless<T: ArrowPrimitiveType, const N: usize> {
    inner: BranchlessFilter<T, N>,
}

impl<T, const N: usize> StaticFilter for ReinterpretedBranchless<T, N>
where
    T: ArrowPrimitiveType + 'static,
    T::Native: Copy + PartialEq + Send + Sync + 'static,
{
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        if v.data_type().primitive_width() != Some(size_of::<T::Native>()) {
            return Err(exec_datafusion_err!(
                "BranchlessFilter: expected {}-byte primitive array, got {}",
                size_of::<T::Native>(),
                v.data_type()
            ));
        }

        let data = v.to_data();
        let values: &[T::Native] = &data.buffer::<T::Native>(0)[..v.len()];

        Ok(self.inner.contains_slice(values, data.nulls(), negated))
    }
}

/// Hash filter for Utf8View short strings (≤12 bytes).
///
/// Reinterprets the views buffer directly as i128 slice.
struct Utf8ViewHashFilter {
    inner: DirectProbeFilter<Decimal128Type>,
}

impl StaticFilter for Utf8ViewHashFilter {
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        let sv = v.as_string_view();
        let values = views_as_i128(sv.views());

        Ok(self.inner.contains_slice(values, sv.nulls(), negated))
    }
}

/// Reinterprets a same-width primitive array as the target primitive type `T`.
///
/// This is a zero-copy operation: the returned array shares the original values
/// buffer and null buffer. Callers must ensure the source array and target type
/// have the same primitive width.
#[inline]
pub(crate) fn reinterpret_any_primitive_to<T: ArrowPrimitiveType>(
    array: &dyn Array,
) -> ArrayRef {
    let data = array.to_data();
    let values = data.buffers()[0].clone();
    let buffer = ScalarBuffer::<T::Native>::new(values, data.offset(), data.len());
    Arc::new(PrimitiveArray::<T>::new(buffer, array.nulls().cloned()))
}

/// Creates a bitmap filter for u8/u16 types, reinterpreting if needed.
pub(crate) fn make_bitmap_filter<C>(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>>
where
    C: BitmapFilterConfig,
{
    if in_array.data_type() == &C::ArrowType::DATA_TYPE {
        return Ok(Arc::new(BitmapFilter::<C>::try_new(in_array)?));
    }
    if in_array.data_type().primitive_width() != Some(size_of::<C::Native>()) {
        return Err(exec_datafusion_err!(
            "BitmapFilter: expected {}-byte primitive array for {} bitmap, got {}",
            size_of::<C::Native>(),
            C::DATA_TYPE_NAME,
            in_array.data_type()
        ));
    }

    let reinterpreted = reinterpret_any_primitive_to::<C::ArrowType>(in_array.as_ref());
    let inner = BitmapFilter::<C>::try_new(&reinterpreted)?;
    Ok(Arc::new(ReinterpretedBitmap { inner }))
}

/// Creates a branchless filter for primitive types.
///
/// Dispatches based on byte width and element count:
/// - 4-byte types (Int32, Float32, etc.): supports 0-32 elements
/// - 8-byte types (Int64, Float64, Timestamp, etc.): supports 0-16 elements
/// - 16-byte types (Decimal128): supports 0-4 elements
pub(crate) fn make_branchless_filter<D>(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>>
where
    D: ArrowPrimitiveType + 'static,
    D::Native: Copy + PartialEq + Send + Sync + 'static,
{
    let is_native = in_array.data_type() == &D::DATA_TYPE;
    let width = size_of::<D::Native>();
    let arr = if is_native {
        Arc::clone(in_array)
    } else {
        if in_array.data_type().primitive_width() != Some(width) {
            return Err(exec_datafusion_err!(
                "BranchlessFilter: expected {width}-byte primitive array, got {}",
                in_array.data_type()
            ));
        }
        reinterpret_any_primitive_to::<D>(in_array.as_ref())
    };
    let n = arr.len() - arr.null_count();

    // Helper to create the filter for a known size N
    #[inline]
    fn create<D: ArrowPrimitiveType + 'static, const N: usize>(
        arr: &ArrayRef,
        is_native: bool,
    ) -> Result<Arc<dyn StaticFilter + Send + Sync>>
    where
        D::Native: Copy + PartialEq + Send + Sync + 'static,
    {
        let inner = BranchlessFilter::<D, N>::try_new(arr)
            .expect("size verified")
            .expect("type verified");
        if is_native {
            Ok(Arc::new(inner))
        } else {
            Ok(Arc::new(ReinterpretedBranchless { inner }))
        }
    }

    // Match on (width, count) - shared sizes use or-patterns to avoid duplication
    match (width, n) {
        // All widths: 0-4
        (4 | 8 | 16, 0) => create::<D, 0>(&arr, is_native),
        (4 | 8 | 16, 1) => create::<D, 1>(&arr, is_native),
        (4 | 8 | 16, 2) => create::<D, 2>(&arr, is_native),
        (4 | 8 | 16, 3) => create::<D, 3>(&arr, is_native),
        (4 | 8 | 16, 4) => create::<D, 4>(&arr, is_native),
        // 4-byte and 8-byte: 5-16
        (4 | 8, 5) => create::<D, 5>(&arr, is_native),
        (4 | 8, 6) => create::<D, 6>(&arr, is_native),
        (4 | 8, 7) => create::<D, 7>(&arr, is_native),
        (4 | 8, 8) => create::<D, 8>(&arr, is_native),
        (4 | 8, 9) => create::<D, 9>(&arr, is_native),
        (4 | 8, 10) => create::<D, 10>(&arr, is_native),
        (4 | 8, 11) => create::<D, 11>(&arr, is_native),
        (4 | 8, 12) => create::<D, 12>(&arr, is_native),
        (4 | 8, 13) => create::<D, 13>(&arr, is_native),
        (4 | 8, 14) => create::<D, 14>(&arr, is_native),
        (4 | 8, 15) => create::<D, 15>(&arr, is_native),
        (4 | 8, 16) => create::<D, 16>(&arr, is_native),
        // 4-byte only: 17-32
        (4, 17) => create::<D, 17>(&arr, is_native),
        (4, 18) => create::<D, 18>(&arr, is_native),
        (4, 19) => create::<D, 19>(&arr, is_native),
        (4, 20) => create::<D, 20>(&arr, is_native),
        (4, 21) => create::<D, 21>(&arr, is_native),
        (4, 22) => create::<D, 22>(&arr, is_native),
        (4, 23) => create::<D, 23>(&arr, is_native),
        (4, 24) => create::<D, 24>(&arr, is_native),
        (4, 25) => create::<D, 25>(&arr, is_native),
        (4, 26) => create::<D, 26>(&arr, is_native),
        (4, 27) => create::<D, 27>(&arr, is_native),
        (4, 28) => create::<D, 28>(&arr, is_native),
        (4, 29) => create::<D, 29>(&arr, is_native),
        (4, 30) => create::<D, 30>(&arr, is_native),
        (4, 31) => create::<D, 31>(&arr, is_native),
        (4, 32) => create::<D, 32>(&arr, is_native),
        // Error cases
        (4, n) => datafusion_common::exec_err!(
            "Branchless filter for 4-byte types supports 0-32 elements, got {n}"
        ),
        (8, n) => datafusion_common::exec_err!(
            "Branchless filter for 8-byte types supports 0-16 elements, got {n}"
        ),
        (16, n) => datafusion_common::exec_err!(
            "Branchless filter for 16-byte types supports 0-4 elements, got {n}"
        ),
        (w, _) => datafusion_common::exec_err!(
            "Branchless filter not supported for {w}-byte types"
        ),
    }
}

// NOTE: Optimizations below assume Little Endian layout (DataFusion standard).

/// Helper to extract the length from a Utf8View u128/i128 view.
#[inline(always)]
fn view_len(view: i128) -> u32 {
    view as u32
}

/// Checks if all strings in a Utf8View array are short enough to be inline.
///
/// In Utf8View, strings ≤12 bytes are stored inline in the 16-byte view struct.
/// These can be reinterpreted as i128 for fast equality comparison.
#[inline]
pub(crate) fn utf8view_all_short_strings(array: &dyn Array) -> bool {
    let sv = array.as_string_view();
    sv.views().iter().enumerate().all(|(i, &view)| {
        !sv.is_valid(i) || view_len(view as i128) as usize <= INLINE_STRING_LEN
    })
}

/// Reinterprets a Utf8View array as Decimal128 by treating the view bytes as i128.
#[inline]
fn reinterpret_utf8view_as_decimal128(array: &dyn Array) -> ArrayRef {
    let sv = array.as_string_view();
    let buffer = ScalarBuffer::<i128>::new(sv.views().inner().clone(), 0, sv.len());
    Arc::new(PrimitiveArray::<Decimal128Type>::new(
        buffer,
        sv.nulls().cloned(),
    ))
}

/// Creates a hash filter for Utf8View arrays with short strings.
pub(crate) fn make_utf8view_hash_filter(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>> {
    let reinterpreted = reinterpret_utf8view_as_decimal128(in_array.as_ref());
    let inner = DirectProbeFilter::<Decimal128Type>::try_new(&reinterpreted)?;
    Ok(Arc::new(Utf8ViewHashFilter { inner }))
}

/// Creates a branchless filter for Utf8View arrays with short strings.
pub(crate) fn make_utf8view_branchless_filter(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>> {
    let reinterpreted = reinterpret_utf8view_as_decimal128(in_array.as_ref());

    macro_rules! try_branchless {
        ($($n:literal),*) => {
            $(if let Some(Ok(inner)) = BranchlessFilter::<Decimal128Type, $n>::try_new(&reinterpreted) {
                return Ok(Arc::new(Utf8ViewBranchless { inner }));
            })*
        };
    }
    try_branchless!(0, 1, 2, 3, 4);

    datafusion_common::exec_err!(
        "Utf8View branchless filter only supports 0-4 elements, got {}",
        in_array.len() - in_array.null_count()
    )
}

/// Branchless filter for Utf8View short strings (≤12 bytes).
struct Utf8ViewBranchless<const N: usize> {
    inner: BranchlessFilter<Decimal128Type, N>,
}

impl<const N: usize> StaticFilter for Utf8ViewBranchless<N> {
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        let sv = v.as_string_view();
        let values = views_as_i128(sv.views());

        Ok(self.inner.contains_slice(values, sv.nulls(), negated))
    }
}

/// Mask to extract len + prefix from a Utf8View view (zeroes out buffer_index and offset).
///
/// View layout (16 bytes, Little Endian):
/// - Bytes 0-3 (low): length (u32)
/// - Bytes 4-7: prefix (long strings) or inline data bytes 0-3 (short strings)
/// - Bytes 8-11: buffer_index (long) or inline data bytes 4-7 (short)
/// - Bytes 12-15 (high): offset (long) or inline data bytes 8-11 (short)
///
/// For long strings (>12 bytes), buffer_index and offset are array-specific,
/// so we mask them out, keeping only len + prefix for comparison.
const VIEW_MASK_LONG: i128 = (1_i128 << 64) - 1; // Keep low 64 bits

/// Computes the masked view for comparison.
///
/// - Short strings (≤12 bytes): returns full view (all data is inline)
/// - Long strings (>12 bytes): returns only len + prefix (masks out buffer_index/offset)
#[inline(always)]
fn masked_view(view: i128) -> i128 {
    let len = view_len(view) as usize;

    if len <= INLINE_STRING_LEN {
        view // Short string: all 16 bytes are meaningful data
    } else {
        view & VIEW_MASK_LONG // Long string: keep only len + prefix
    }
}

/// Two-stage filter for ByteView arrays (Utf8View, BinaryView) with mixed lengths.
///
/// Stage 1: Quick rejection using masked views (len + prefix as i128)
/// - Non-matches rejected without any hashing using DirectProbeFilter
/// - Short value matches (≤12 bytes) accepted immediately
///
/// Stage 2: Full verification for long value matches
/// - Only reached when masked view matches AND value is long (>12 bytes)
/// - Uses HashTable lookup with indices into haystack array
pub(crate) struct ByteViewMaskedFilter<T: ByteViewType> {
    /// The haystack array containing values to match against.
    in_array: ArrayRef,
    /// DirectProbeFilter for O(1) masked view quick rejection (faster than HashSet)
    masked_view_filter: DirectProbeFilter<Decimal128Type>,
    /// HashTable storing indices of long strings for Stage 2 verification
    long_value_table: HashTable<usize>,
    /// Random state for consistent hashing between haystack and needles
    state: RandomState,
    _phantom: PhantomData<T>,
}

impl<T: ByteViewType> ByteViewMaskedFilter<T>
where
    T::Native: PartialEq,
{
    pub(crate) fn try_new(in_array: ArrayRef) -> Result<Self> {
        let bv = in_array.as_byte_view::<T>();
        let views = views_as_i128(bv.views());

        let mut masked_views = Vec::with_capacity(in_array.len() - in_array.null_count());
        let state = RandomState::default();
        let mut long_value_table = HashTable::new();

        let mut process_idx = |idx: usize| {
            let view = views[idx];
            masked_views.push(masked_view(view));

            let len = view_len(view) as usize;
            if len > INLINE_STRING_LEN {
                // Use the same byte hash used by Stage 2 probing.
                // SAFETY: idx is valid from iterator
                let val = unsafe { bv.value_unchecked(idx) };
                let bytes: &[u8] = val.as_ref();
                let hash = state.hash_one(bytes);

                // Only insert if not already present (deduplication)
                if long_value_table
                    .find(hash, |&stored_idx| {
                        let stored: &[u8] =
                            unsafe { bv.value_unchecked(stored_idx) }.as_ref();
                        stored == bytes
                    })
                    .is_none()
                {
                    long_value_table.insert_unique(hash, idx, |&i| {
                        let stored: &[u8] = unsafe { bv.value_unchecked(i) }.as_ref();
                        state.hash_one(stored)
                    });
                }
            }
        };

        match bv.nulls() {
            Some(nulls) => {
                BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                    .for_each(&mut process_idx);
            }
            None => {
                (0..in_array.len()).for_each(&mut process_idx);
            }
        }

        // Build DirectProbeFilter from collected masked views
        let masked_view_filter =
            DirectProbeFilter::<Decimal128Type>::from_values(masked_views.into_iter());

        Ok(Self {
            in_array,
            masked_view_filter,
            long_value_table,
            state,
            _phantom: PhantomData,
        })
    }
}

impl<T: ByteViewType + 'static> StaticFilter for ByteViewMaskedFilter<T>
where
    T::Native: PartialEq,
{
    fn null_count(&self) -> usize {
        self.in_array.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        let needle_bv = v.as_byte_view::<T>();
        let needle_views = views_as_i128(needle_bv.views());
        let needle_null_count = needle_bv.null_count();
        let haystack_has_nulls = self.in_array.null_count() > 0;
        let haystack_bv = self.in_array.as_byte_view::<T>();

        // Single pass with lazy hashing - only hash long values that pass Stage 1
        // Use null shortcircuit: Stage 2 string comparison is expensive,
        // so skipping lookups for null positions is worth the branch overhead
        Ok(build_in_list_result_with_null_shortcircuit(
            v.len(),
            needle_bv.nulls(),
            needle_null_count,
            haystack_has_nulls,
            negated,
            #[inline(always)]
            |i| {
                let needle_view = needle_views[i];
                let masked = masked_view(needle_view);

                // Stage 1: Quick rejection via DirectProbeFilter (O(1) lookup)
                if !self.masked_view_filter.contains_single(masked) {
                    return false;
                }

                // Masked view found in set
                let needle_len = view_len(needle_view) as usize;

                if needle_len <= INLINE_STRING_LEN {
                    // Short value: masked view = full view, true match
                    return true;
                }

                // Stage 2: Long value - hash lazily and lookup in hash table
                // SAFETY: i is in bounds, closure only called for valid positions
                let needle_val = unsafe { needle_bv.value_unchecked(i) };
                let needle_bytes: &[u8] = needle_val.as_ref();
                let hash = self.state.hash_one(needle_bytes);

                self.long_value_table
                    .find(hash, |&idx| {
                        let haystack_val: &[u8] =
                            unsafe { haystack_bv.value_unchecked(idx) }.as_ref();
                        haystack_val == needle_bytes
                    })
                    .is_some()
            },
        ))
    }
}

/// Creates a two-stage filter for ByteView arrays (Utf8View, BinaryView).
pub(crate) fn make_byte_view_masked_filter<T: ByteViewType + 'static>(
    in_array: ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>>
where
    T::Native: PartialEq,
{
    Ok(Arc::new(ByteViewMaskedFilter::<T>::try_new(in_array)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, StringViewArray,
    };
    use arrow::datatypes::{StringViewType, UInt32Type};

    #[test]
    fn reinterpreted_bitmap_handles_signed_boundaries_and_slices() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            Int8Array::from(vec![Some(99), Some(i8::MIN), None, Some(-1), Some(42)])
                .slice(1, 3),
        );
        let filter = make_bitmap_filter::<
            super::super::primitive_filter::UInt8BitmapConfig,
        >(&haystack)?;
        let needles =
            Int8Array::from(vec![Some(7), Some(i8::MIN), Some(-1), None]).slice(1, 3);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), Some(true), None])
        );

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
        let filter = make_bitmap_filter::<
            super::super::primitive_filter::UInt16BitmapConfig,
        >(&haystack)?;
        let needles =
            Int16Array::from(vec![Some(0), Some(i16::MIN), Some(7), Some(i16::MAX)])
                .slice(1, 3);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), None, Some(true)])
        );

        Ok(())
    }

    #[test]
    fn reinterpreted_branchless_handles_slices() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            Int32Array::from(vec![Some(99), Some(-7), None, Some(42)]).slice(1, 3),
        );
        let filter = make_branchless_filter::<UInt32Type>(&haystack)?;
        let needles =
            Int32Array::from(vec![Some(0), Some(-7), Some(1), Some(42)]).slice(1, 3);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), None, Some(true)])
        );

        Ok(())
    }

    #[test]
    fn utf8view_hash_filter_handles_short_slices() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            StringViewArray::from(vec![
                Some("outside"),
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
                Some("e"),
                Some("tail"),
            ])
            .slice(1, 5),
        );
        let filter = make_utf8view_hash_filter(&haystack)?;
        let needles =
            StringViewArray::from(vec![Some("outside"), Some("b"), Some("z"), Some("e")])
                .slice(1, 3);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), Some(false), Some(true)])
        );

        Ok(())
    }

    #[test]
    fn byte_view_masked_filter_verifies_long_string_matches() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            StringViewArray::from(vec![
                Some("outside"),
                Some("abcdefghijklmn1"),
                Some("short"),
                Some("zzzzzzzzzzzzzz"),
                Some("tail"),
            ])
            .slice(1, 3),
        );
        let filter = make_byte_view_masked_filter::<StringViewType>(haystack)?;
        let needles = StringViewArray::from(vec![
            Some("outside"),
            Some("abcdefghijklmn1"),
            Some("abcdefghijklmn2"),
            Some("short"),
            Some("tail"),
        ])
        .slice(1, 3);

        assert_eq!(
            filter.contains(&needles, false)?,
            BooleanArray::from(vec![Some(true), Some(false), Some(true)])
        );

        Ok(())
    }
}
