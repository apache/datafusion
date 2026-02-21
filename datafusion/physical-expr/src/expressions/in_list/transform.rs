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

//! Type transformation utilities for InList filters
//!
//! This module provides type reinterpretation for optimizing filter dispatch.
//! For equality comparison, only the bit pattern matters, so we can:
//! - Reinterpret signed integers as unsigned (Int32 → UInt32)
//! - Reinterpret floats as unsigned integers (Float64 → UInt64)
//!
//! This allows using a single filter implementation (e.g., for UInt64) to handle
//! multiple types (Int64, Float64, Timestamp, Duration) that share the same
//! byte width, reducing code duplication.

use std::marker::PhantomData;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{ArrowPrimitiveType, ByteViewType, Decimal128Type};
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::Result;
use datafusion_common::hash_utils::with_hashes;
use hashbrown::HashTable;

use super::primitive_filter::{
    BitmapFilter, BitmapFilterConfig, BranchlessFilter, DirectProbeFilter,
};
use super::result::{build_in_list_result_with_null_shortcircuit, handle_dictionary};
use super::static_filter::StaticFilter;

/// Maximum length for inline strings (≤12 bytes can be stored in 16-byte view/encoding).
/// Used by both Utf8View short string optimization and Utf8 two-stage filter.
pub(crate) const INLINE_STRING_LEN: usize = 12;

// =============================================================================
// REINTERPRETING FILTERS (zero-copy type conversion)
// =============================================================================

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

        let data = v.to_data();
        let values: &[C::Native] = data.buffer::<C::Native>(0);

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

        let data = v.to_data();
        let values: &[T::Native] = data.buffer::<T::Native>(0);

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

        // Zero-copy: reinterpret views buffer directly as i128 slice
        let sv = v.as_string_view();
        let values: &[i128] = sv.views().inner().typed_data();

        Ok(self.inner.contains_slice(values, sv.nulls(), negated))
    }
}

/// Reinterprets any primitive-like array as the target primitive type T by extracting
/// the underlying buffer.
///
/// This is a zero-copy operation that works for all primitive types (Int*, UInt*, Float*,
/// Timestamp*, Date*, Duration*, etc.) by directly accessing the underlying buffer,
/// ignoring any metadata like timezones or precision/scale.
#[inline]
pub(crate) fn reinterpret_any_primitive_to<T: ArrowPrimitiveType>(
    array: &dyn Array,
) -> ArrayRef {
    let values = array.to_data().buffers()[0].clone();
    let buffer: ScalarBuffer<T::Native> = values.into();
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

    let reinterpreted = reinterpret_any_primitive_to::<C::ArrowType>(in_array.as_ref());
    let inner = BitmapFilter::<C>::try_new(&reinterpreted)?;
    Ok(Arc::new(ReinterpretedBitmap { inner }))
}

// =============================================================================
// BRANCHLESS FILTER CREATION (const generic dispatch)
// =============================================================================

/// Creates a branchless filter for primitive types.
///
/// Dispatches based on byte width and element count:
/// - 4-byte types (Int32, Float32, etc.): supports 0-32 elements
/// - 8-byte types (Int64, Float64, Timestamp, etc.): supports 0-16 elements
/// - 16-byte types (Decimal128): supports 0-4 elements
pub(crate) fn make_branchless_filter<D>(
    in_array: &ArrayRef,
    width: usize,
) -> Result<Arc<dyn StaticFilter + Send + Sync>>
where
    D: ArrowPrimitiveType + 'static,
    D::Native: Copy + PartialEq + Send + Sync + 'static,
{
    let is_native = in_array.data_type() == &D::DATA_TYPE;
    let arr = if is_native {
        Arc::clone(in_array)
    } else {
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

// =============================================================================
// UTF8VIEW REINTERPRETATION (short strings ≤12 bytes → Decimal128)
// =============================================================================

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
    let buffer: ScalarBuffer<i128> = sv.views().inner().clone().into();
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
        let values: &[i128] = sv.views().inner().typed_data();

        Ok(self.inner.contains_slice(values, sv.nulls(), negated))
    }
}

// =============================================================================
// UTF8VIEW TWO-STAGE FILTER (masked view pre-check + full verification)
// =============================================================================

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
        let views: &[i128] = bv.views().inner().typed_data();

        let mut masked_views = Vec::new();
        let state = RandomState::new();
        let mut long_value_table = HashTable::new();

        // Build hash table for long strings using batch hashing
        with_hashes([in_array.as_ref()], &state, |hashes| {
            let mut process_idx = |idx: usize| {
                let view = views[idx];
                masked_views.push(masked_view(view));

                // For long strings, store index in hash table
                let len = view_len(view) as usize;
                if len > INLINE_STRING_LEN {
                    let hash = hashes[idx];
                    // SAFETY: idx is valid from iterator
                    let val = unsafe { bv.value_unchecked(idx) };
                    let bytes: &[u8] = val.as_ref();

                    // Only insert if not already present (deduplication)
                    if long_value_table
                        .find(hash, |&stored_idx| {
                            let stored: &[u8] =
                                unsafe { bv.value_unchecked(stored_idx) }.as_ref();
                            stored == bytes
                        })
                        .is_none()
                    {
                        long_value_table.insert_unique(hash, idx, |&i| hashes[i]);
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
            Ok::<_, datafusion_common::DataFusionError>(())
        })?;

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
        let needle_views: &[i128] = needle_bv.views().inner().typed_data();
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

// =============================================================================
// UTF8 TWO-STAGE FILTER (length+prefix pre-check + full verification)
// =============================================================================
//
// Similar to ByteViewMaskedFilter but for regular Utf8/LargeUtf8 arrays.
// Encodes strings as i128 with length + prefix for quick rejection.
//
// Encoding (Little Endian):
// - Bytes 0-3: length (u32)
// - Bytes 4-15: data (12 bytes)
//
// This naturally distinguishes short from long strings via the length field.
// For short strings (≤12 bytes), the i128 contains all data → match is definitive.
// For long strings (>12 bytes), a match requires full string comparison.

/// Encodes a string as i128 with length + prefix.
/// Format: [len:u32][data:12 bytes] (Little Endian)
#[inline(always)]
fn encode_string_as_i128(s: &[u8]) -> i128 {
    let len = s.len();

    // Optimization: Construct the i128 directly using arithmetic and pointer copy
    // to avoid Store-to-Load Forwarding (STLF) stalls on x64 and minimize LSU pressure on ARM.
    //
    // The layout in memory must match Utf8View: [4 bytes len][12 bytes data]
    let mut val: u128 = len as u128; // Length in bytes 0-3

    // Safety: writing to the remaining bytes of an initialized u128.
    // We use a pointer copy for the string data as it is variable length (0-12 bytes).
    unsafe {
        let dst = (&mut val as *mut u128 as *mut u8).add(4);
        std::ptr::copy_nonoverlapping(s.as_ptr(), dst, len.min(INLINE_STRING_LEN));
    }

    val as i128
}

/// Two-stage filter for Utf8/LargeUtf8 arrays.
///
/// Stage 1: Quick rejection using length+prefix as i128
/// - Non-matches rejected via O(1) DirectProbeFilter lookup
/// - Short string matches (≤12 bytes) accepted immediately
///
/// Stage 2: Full verification for long string matches
/// - Only reached when encoded i128 matches AND string length >12 bytes
/// - Uses HashTable with full string comparison
pub(crate) struct Utf8TwoStageFilter<O: arrow::array::OffsetSizeTrait> {
    /// The haystack array containing values to match against
    in_array: ArrayRef,
    /// DirectProbeFilter for O(1) encoded i128 quick rejection
    encoded_filter: DirectProbeFilter<Decimal128Type>,
    /// HashTable storing indices of long strings (>12 bytes) for Stage 2
    long_string_table: HashTable<usize>,
    /// Random state for consistent hashing
    state: RandomState,
    /// Whether all haystack strings are short (≤12 bytes) - enables fast path
    all_short: bool,
    _phantom: PhantomData<O>,
}

impl<O: arrow::array::OffsetSizeTrait + 'static> Utf8TwoStageFilter<O> {
    pub(crate) fn try_new(in_array: ArrayRef) -> Result<Self> {
        use arrow::array::GenericStringArray;

        let arr = in_array
            .as_any()
            .downcast_ref::<GenericStringArray<O>>()
            .expect("Utf8TwoStageFilter requires GenericStringArray");

        let len = arr.len();
        let mut encoded_values = Vec::with_capacity(len);
        let state = RandomState::new();
        let mut long_string_table = HashTable::new();
        let mut all_short = true;

        // Build encoded values and long string table
        for i in 0..len {
            if arr.is_null(i) {
                encoded_values.push(0);
                continue;
            }

            let s = arr.value(i);
            let bytes = s.as_bytes();
            encoded_values.push(encode_string_as_i128(bytes));

            if bytes.len() > INLINE_STRING_LEN {
                all_short = false;
                // Add to long string table for Stage 2 verification (with deduplication)
                let hash = state.hash_one(bytes);
                if long_string_table
                    .find(hash, |&stored_idx| {
                        arr.value(stored_idx).as_bytes() == bytes
                    })
                    .is_none()
                {
                    long_string_table.insert_unique(hash, i, |&idx| {
                        state.hash_one(arr.value(idx).as_bytes())
                    });
                }
            }
        }

        // Build DirectProbeFilter from encoded values
        let nulls = arr
            .nulls()
            .map(|n| arrow::buffer::NullBuffer::new(n.inner().clone()));
        let encoded_array: ArrayRef = Arc::new(PrimitiveArray::<Decimal128Type>::new(
            ScalarBuffer::from(encoded_values),
            nulls,
        ));
        let encoded_filter =
            DirectProbeFilter::<Decimal128Type>::try_new(&encoded_array)?;

        Ok(Self {
            in_array,
            encoded_filter,
            long_string_table,
            state,
            all_short,
            _phantom: PhantomData,
        })
    }
}

impl<O: arrow::array::OffsetSizeTrait + 'static> StaticFilter for Utf8TwoStageFilter<O> {
    fn null_count(&self) -> usize {
        self.in_array.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        use arrow::array::GenericStringArray;

        handle_dictionary!(self, v, negated);

        let needle_arr = v
            .as_any()
            .downcast_ref::<GenericStringArray<O>>()
            .expect("needle array type mismatch in Utf8TwoStageFilter");
        let haystack_arr = self
            .in_array
            .as_any()
            .downcast_ref::<GenericStringArray<O>>()
            .expect("haystack array type mismatch in Utf8TwoStageFilter");

        let haystack_has_nulls = self.in_array.null_count() > 0;

        if self.all_short {
            // Fast path: all haystack strings are short
            // Batch-encode all needles and do bulk lookup
            let needle_encoded: Vec<i128> = (0..needle_arr.len())
                .map(|i| {
                    if needle_arr.is_null(i) {
                        0
                    } else {
                        encode_string_as_i128(needle_arr.value(i).as_bytes())
                    }
                })
                .collect();

            // For short haystack, encoded match is definitive for short needles.
            // Long needles (>12 bytes) can never match, but their encoded form
            // won't match any short haystack encoding (different length field).
            return Ok(self.encoded_filter.contains_slice(
                &needle_encoded,
                needle_arr.nulls(),
                negated,
            ));
        }

        // Two-stage path: haystack has long strings
        Ok(super::result::build_in_list_result(
            v.len(),
            needle_arr.nulls(),
            haystack_has_nulls,
            negated,
            |i| {
                // SAFETY: i is in bounds [0, v.len()), guaranteed by build_in_list_result
                let needle_bytes = unsafe { needle_arr.value_unchecked(i) }.as_bytes();
                let encoded = encode_string_as_i128(needle_bytes);

                // Stage 1: Quick rejection via encoded i128
                if !self.encoded_filter.contains_single(encoded) {
                    return false;
                }

                // Encoded match found
                let needle_len = needle_bytes.len();
                if needle_len <= INLINE_STRING_LEN {
                    // Short needle: encoded contains all data, match is definitive
                    // (If haystack had a long string with same prefix, its length
                    // field would differ, so encoded wouldn't match)
                    return true;
                }

                // Stage 2: Long needle - verify with full string comparison
                let hash = self.state.hash_one(needle_bytes);
                self.long_string_table
                    .find(hash, |&idx| {
                        // SAFETY: idx was stored in try_new from valid indices into in_array
                        unsafe { haystack_arr.value_unchecked(idx) }.as_bytes()
                            == needle_bytes
                    })
                    .is_some()
            },
        ))
    }
}

/// Creates a two-stage filter for Utf8/LargeUtf8 arrays.
/// Returns true if all non-null strings in a Utf8/LargeUtf8 array are ≤12 bytes.
/// When false, the two-stage filter's Stage 1 cannot definitively match and the
/// encoding overhead regresses performance vs the generic fallback.
pub(crate) fn utf8_all_short_strings(array: &dyn Array) -> bool {
    use arrow::array::GenericStringArray;
    use arrow::datatypes::DataType;
    match array.data_type() {
        DataType::Utf8 => utf8_all_short_strings_impl::<i32>(
            array
                .as_any()
                .downcast_ref::<GenericStringArray<i32>>()
                .unwrap(),
        ),
        DataType::LargeUtf8 => utf8_all_short_strings_impl::<i64>(
            array
                .as_any()
                .downcast_ref::<GenericStringArray<i64>>()
                .unwrap(),
        ),
        _ => false,
    }
}

fn utf8_all_short_strings_impl<O: arrow::array::OffsetSizeTrait>(
    arr: &arrow::array::GenericStringArray<O>,
) -> bool {
    (0..arr.len()).all(|i| arr.is_null(i) || arr.value(i).len() <= INLINE_STRING_LEN)
}

pub(crate) fn make_utf8_two_stage_filter(
    in_array: ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>> {
    use arrow::datatypes::DataType;
    match in_array.data_type() {
        DataType::Utf8 => Ok(Arc::new(Utf8TwoStageFilter::<i32>::try_new(in_array)?)),
        DataType::LargeUtf8 => {
            Ok(Arc::new(Utf8TwoStageFilter::<i64>::try_new(in_array)?))
        }
        dt => datafusion_common::exec_err!(
            "Unsupported data type for Utf8 two-stage filter: {dt}"
        ),
    }
}
