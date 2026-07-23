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

use std::cmp::Ordering;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{
    Array, ArrowPrimitiveType, GenericByteArray, GenericByteViewArray, OffsetSizeTrait,
    PrimitiveArray, StringViewArray, types::ByteArrayType,
};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::SortOptions;
use arrow::datatypes::ArrowNativeTypeOp;
use arrow::row::Rows;
use datafusion_execution::memory_pool::MemoryReservation;

/// A comparable collection of values for use with [`Cursor`]
///
/// This is a trait as there are several specialized implementations, such as for
/// single columns or for normalized multi column keys ([`Rows`])
pub trait CursorValues: Debug + Sync + Send {
    fn len(&self) -> usize;

    /// Returns true if `l[l_idx] == r[r_idx]`
    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool;

    /// Returns true if `row[idx] == row[idx - 1]`
    /// Given `idx` should be greater than 0
    fn eq_to_previous(cursor: &Self, idx: usize) -> bool;

    /// Returns comparison of `l[l_idx]` and `r[r_idx]`
    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering;

    /// Notifies the values that the owning [`Cursor`] moved to `offset` (always
    /// `< len()`), so caching implementations can refresh the value(s) read by
    /// the hot comparisons. Default no-op (e.g. byte/row cursors don't benefit).
    #[inline]
    fn set_offset(&mut self, offset: usize) {
        let _ = offset;
    }
}

/// A comparable cursor, used by sort operations
///
/// A `Cursor` is a pointer into a collection of rows, stored in
/// [`CursorValues`]
///
/// ```text
///
/// ┌───────────────────────┐
/// │                       │           ┌──────────────────────┐
/// │ ┌─────────┐ ┌─────┐   │    ─ ─ ─ ─│      Cursor<T>       │
/// │ │    1    │ │  A  │   │   │       └──────────────────────┘
/// │ ├─────────┤ ├─────┤   │
/// │ │    2    │ │  A  │◀─ ┼ ─ ┘          Cursor<T> tracks an
/// │ └─────────┘ └─────┘   │                offset within a
/// │     ...       ...     │                  CursorValues
/// │                       │
/// │ ┌─────────┐ ┌─────┐   │
/// │ │    3    │ │  E  │   │
/// │ └─────────┘ └─────┘   │
/// │                       │
/// │     CursorValues      │
/// └───────────────────────┘
/// ```
///
/// Store logical rows using one of several  formats, with specialized
/// implementations depending on the column types
#[derive(Debug)]
pub struct Cursor<T: CursorValues> {
    offset: usize,
    values: T,
}

impl<T: CursorValues> Cursor<T> {
    /// Create a [`Cursor`] from the given [`CursorValues`]
    pub fn new(values: T) -> Self {
        Self { offset: 0, values }
    }

    /// Returns true if there are no more rows in this cursor
    #[inline]
    pub fn is_finished(&self) -> bool {
        self.offset == self.values.len()
    }

    /// Advance the cursor, returning the previous row index
    #[inline]
    pub fn advance(&mut self) -> usize {
        let t = self.offset;
        self.offset += 1;
        // Refresh the cache for the new position. The guard keeps `set_offset`
        // in bounds; a finished cursor's stale cache is never read (it is taken
        // before the next comparison).
        if self.offset < self.values.len() {
            self.values.set_offset(self.offset);
        }
        t
    }

    pub fn is_eq_to_prev_one(&self, prev_cursor: Option<&Cursor<T>>) -> bool {
        if self.offset > 0 {
            self.is_eq_to_prev_row()
        } else if let Some(prev_cursor) = prev_cursor {
            self.is_eq_to_prev_row_in_prev_batch(prev_cursor)
        } else {
            false
        }
    }
}

impl<T: CursorValues> PartialEq for Cursor<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        T::eq(&self.values, self.offset, &other.values, other.offset)
    }
}

impl<T: CursorValues> Cursor<T> {
    fn is_eq_to_prev_row(&self) -> bool {
        T::eq_to_previous(&self.values, self.offset)
    }

    fn is_eq_to_prev_row_in_prev_batch(&self, other: &Self) -> bool {
        assert_eq!(self.offset, 0);
        T::eq(
            &self.values,
            self.offset,
            &other.values,
            other.values.len() - 1,
        )
    }
}

impl<T: CursorValues> Eq for Cursor<T> {}

impl<T: CursorValues> PartialOrd for Cursor<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: CursorValues> Ord for Cursor<T> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        T::compare(&self.values, self.offset, &other.values, other.offset)
    }
}

/// Implements [`CursorValues`] for [`Rows`]
///
/// Used for sorting when there are multiple columns in the sort key.
///
/// Caches `(ptr, len)` for the current row's serialized bytes so the merge hot
/// path compares two `&[u8]` slices directly rather than paying a
/// `Rows::row(idx)` offset lookup for each side of each compare. The pointer
/// is into `rows`'s Arc-owned buffer heap, so it stays valid even when this
/// struct is moved (e.g. written into a `Vec<Option<Cursor<..>>>` slot).
#[derive(Debug)]
pub struct RowValues {
    rows: Arc<Rows>,

    /// Number of rows — snapshot of `rows.num_rows()`. Read on every
    /// `Cursor::is_finished` / `advance` call.
    len: usize,
    /// Cached byte slice pointer for the current row.
    current_ptr: *const u8,
    /// Cached byte length for the current row.
    current_len: usize,

    /// Tracks for the memory used by in the `Rows` of this
    /// cursor. Freed on drop
    _reservation: MemoryReservation,
}

// SAFETY: `current_ptr` points into `rows`'s Arc-owned buffer heap. `Rows`
// is `Send + Sync`; the referenced bytes are read-only after construction.
unsafe impl Send for RowValues {}
unsafe impl Sync for RowValues {}

impl RowValues {
    /// Create a new [`RowValues`] from `rows` and a `reservation`
    /// that tracks its memory. There must be at least one row
    ///
    /// Panics if the reservation is not for exactly `rows.size()`
    /// bytes or if `rows` is empty.
    pub fn new(rows: Arc<Rows>, reservation: MemoryReservation) -> Self {
        assert_eq!(
            rows.size(),
            reservation.size(),
            "memory reservation mismatch"
        );
        let len = rows.num_rows();
        assert!(len > 0);
        // Extract raw ptr + length while the temporary `Row` is still alive.
        // The pointer is into `rows`'s Arc buffer heap and stays valid.
        let (current_ptr, current_len) = {
            let row = rows.row(0);
            let bytes: &[u8] = row.as_ref();
            (bytes.as_ptr(), bytes.len())
        };
        Self {
            rows,
            len,
            current_ptr,
            current_len,
            _reservation: reservation,
        }
    }

    #[inline(always)]
    fn current_slice(&self) -> &[u8] {
        // SAFETY: `set_offset` (or `new` for offset 0) populated `current_ptr`
        // / `current_len` from `rows.row(offset).as_ref()`, and the ptr is
        // into `rows`'s Arc heap that stays alive as long as `self` does.
        unsafe { std::slice::from_raw_parts(self.current_ptr, self.current_len) }
    }
}

impl CursorValues for RowValues {
    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    // No inline hint on purpose: for the heavyweight `Rows` byte comparison the
    // compiler's own choice wins — both `#[inline]` and `#[inline(never)]`
    // measurably regress the multi-column merge path.
    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool {
        // Arbitrary indices (cross-batch); can't use the cache which only
        // holds the current offset.
        l.rows.row(l_idx) == r.rows.row(r_idx)
    }

    fn eq_to_previous(cursor: &Self, idx: usize) -> bool {
        assert!(idx > 0);
        cursor.rows.row(idx) == cursor.rows.row(idx - 1)
    }

    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering {
        // Merge callers always compare at current offsets; the cache is up
        // to date. (Debug-only: verify the invariant.)
        debug_assert!(l_idx < l.len && r_idx < r.len);
        let _ = (l_idx, r_idx);
        l.current_slice().cmp(r.current_slice())
    }

    #[inline(always)]
    fn set_offset(&mut self, offset: usize) {
        // Refresh the cached byte-slice for the new row. Caller guarantees
        // `offset < len`. `Rows::row(idx).as_ref()` returns `&[u8]` into the
        // Arc-owned buffer heap, so the pointer we stow stays valid after
        // the temporary `Row` drops.
        let (ptr, len) = {
            let row = self.rows.row(offset);
            let bytes: &[u8] = row.as_ref();
            (bytes.as_ptr(), bytes.len())
        };
        self.current_ptr = ptr;
        self.current_len = len;
    }
}

/// An [`Array`] that can be converted into [`CursorValues`]
pub trait CursorArray: Array + 'static {
    type Values: CursorValues;

    fn values(&self) -> Self::Values;
}

impl<T: ArrowPrimitiveType> CursorArray for PrimitiveArray<T> {
    type Values = PrimitiveValues<T::Native>;

    fn values(&self) -> Self::Values {
        PrimitiveValues::new(self.values().clone())
    }
}

/// [`CursorValues`] for a primitive column.
///
/// Caches the value at the current (and previous) offset, refreshed once per
/// [`Cursor::advance`] via [`CursorValues::set_offset`], so the hot loser-tree
/// comparisons read a cached field instead of indexing the buffer each time.
#[derive(Debug)]
pub struct PrimitiveValues<T: ArrowNativeTypeOp> {
    values: ScalarBuffer<T>,
    /// Cached `values[offset]`.
    current: T,
    /// Cached `values[offset - 1]` (read by `eq_to_previous`, only past offset 0).
    previous: T,
    /// Current offset; used only to `debug_assert!` the cache is read in sync.
    offset: usize,
}

impl<T: ArrowNativeTypeOp> PrimitiveValues<T> {
    fn new(values: ScalarBuffer<T>) -> Self {
        // Non-empty in practice; `unwrap_or_default` just avoids a panic.
        let first = values.first().copied().unwrap_or_default();
        Self {
            values,
            current: first,
            previous: first,
            offset: 0,
        }
    }
}

impl<T: ArrowNativeTypeOp> CursorValues for PrimitiveValues<T> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline(always)]
    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool {
        // Arbitrary indices (cross-batch comparison), so index directly.
        l.values[l_idx].is_eq(r.values[r_idx])
    }

    #[inline(always)]
    fn eq_to_previous(cursor: &Self, idx: usize) -> bool {
        assert!(idx > 0);
        debug_assert_eq!(idx, cursor.offset);
        cursor.current.is_eq(cursor.previous)
    }

    #[inline(always)]
    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering {
        debug_assert_eq!(l_idx, l.offset);
        debug_assert_eq!(r_idx, r.offset);
        l.current.compare(r.current)
    }

    #[inline(always)]
    fn set_offset(&mut self, offset: usize) {
        // The caller (`Cursor::advance`) guarantees `offset < len`; inlined, that
        // guard dominates the index below so its bounds check is elided — the
        // length is checked once per row, not per comparison. The old `current`
        // is `values[offset - 1]`, so it becomes `previous`.
        self.previous = self.current;
        self.current = self.values[offset];
        self.offset = offset;
    }
}

#[derive(Debug)]
pub struct ByteArrayValues<T: OffsetSizeTrait> {
    offsets: OffsetBuffer<T>,
    values: Buffer,
    /// Cached start offset of the current row. Refreshed by
    /// [`CursorValues::set_offset`] so the hot merge compare path avoids two
    /// `OffsetBuffer::get_unchecked` loads per compare.
    current_start: usize,
    /// Cached end offset of the current row (same lifecycle as `current_start`).
    current_end: usize,
}

impl<T: OffsetSizeTrait> ByteArrayValues<T> {
    fn new(offsets: OffsetBuffer<T>, values: Buffer) -> Self {
        // Cache row 0 up-front so the first compare (before any `set_offset`)
        // reads a valid slice. If the array is empty, offsets[0] and offsets[1]
        // both equal 0.
        let start = offsets.first().map(|v| v.as_usize()).unwrap_or(0);
        let end = if offsets.len() >= 2 {
            offsets[1].as_usize()
        } else {
            start
        };
        Self {
            offsets,
            values,
            current_start: start,
            current_end: end,
        }
    }

    #[inline]
    fn value(&self, idx: usize) -> &[u8] {
        assert!(idx < self.len());
        // Safety: offsets are valid and checked bounds above
        unsafe {
            let start = self.offsets.get_unchecked(idx).as_usize();
            let end = self.offsets.get_unchecked(idx + 1).as_usize();
            self.values.get_unchecked(start..end)
        }
    }

    /// Slice for the current row (`self.current_start..self.current_end`).
    /// Cached at construction and refreshed on `set_offset`.
    #[inline(always)]
    fn current_value(&self) -> &[u8] {
        // SAFETY: `set_offset` keeps `current_start`/`current_end` within
        // bounds of `values` for a well-formed offsets buffer, and the merge
        // hot path never reads a stale cache past a cursor's length.
        unsafe {
            self.values
                .get_unchecked(self.current_start..self.current_end)
        }
    }
}

impl<T: OffsetSizeTrait> CursorValues for ByteArrayValues<T> {
    #[inline]
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    #[inline]
    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool {
        // Arbitrary indices (cross-batch comparison), so index directly.
        l.value(l_idx) == r.value(r_idx)
    }

    #[inline]
    fn eq_to_previous(cursor: &Self, idx: usize) -> bool {
        assert!(idx > 0);
        cursor.value(idx) == cursor.value(idx - 1)
    }

    #[inline(always)]
    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering {
        // For merge callers, l_idx / r_idx are guaranteed to be the current
        // cursor offsets that `set_offset` cached.
        debug_assert_eq!(
            (l.current_start, l.current_end),
            (l.offsets[l_idx].as_usize(), l.offsets[l_idx + 1].as_usize())
        );
        debug_assert_eq!(
            (r.current_start, r.current_end),
            (r.offsets[r_idx].as_usize(), r.offsets[r_idx + 1].as_usize())
        );
        l.current_value().cmp(r.current_value())
    }

    #[inline(always)]
    fn set_offset(&mut self, offset: usize) {
        // Caller (`Cursor::advance`) guarantees `offset < len()`, so both
        // `offset` and `offset + 1` are valid indices into `offsets`.
        unsafe {
            self.current_start = self.offsets.get_unchecked(offset).as_usize();
            self.current_end = self.offsets.get_unchecked(offset + 1).as_usize();
        }
    }
}

impl<T: ByteArrayType> CursorArray for GenericByteArray<T> {
    type Values = ByteArrayValues<T::Offset>;

    fn values(&self) -> Self::Values {
        ByteArrayValues::new(self.offsets().clone(), self.values().clone())
    }
}

impl CursorArray for StringViewArray {
    type Values = StringViewCursorValues;
    fn values(&self) -> Self::Values {
        StringViewCursorValues::new(self.gc())
    }
}

/// [`CursorValues`] for [`StringViewArray`] that caches the byte slice for the
/// current row.
///
/// The merge hot loop calls `compare(l, l_idx, r, r_idx)` for cursors at their
/// current offset; without a cache each compare pays a `views()` load plus a
/// `data_buffers()[buffer_id]` lookup (for non-inline views). By storing a raw
/// `(ptr, len)` for the current row we skip both.
///
/// The cached pointer is always into an Arc-owned heap allocation — either the
/// `views` scalar buffer (for inline rows, addressing bytes `4..16` of the
/// 16-byte view) or a `data_buffers[i]` (for external rows). Neither moves
/// when the enclosing [`StringViewCursorValues`] struct itself is moved (e.g.
/// when a new cursor is written into a `Vec<Option<Cursor<..>>>` slot), so the
/// cached pointer remains valid across such moves.
#[derive(Debug)]
pub struct StringViewCursorValues {
    array: StringViewArray,
    /// Snapshot of `array.views().len()` — read on every `Cursor::is_finished`
    /// / `Cursor::advance` call, so caching it locally avoids two extra field
    /// derefs through `array` per row (measurable on low-cardinality inline
    /// keys where the merge steps through millions of rows).
    len: usize,
    /// Snapshot of the `views()` scalar-buffer base pointer — the `compare`
    /// fast path indexes into it per compare, so keeping the pointer next to
    /// the cursor state avoids re-walking `array → views → ScalarBuffer` for
    /// each read.
    views_ptr: *const u128,
    /// Pointer to the bytes of the current row's value (into Arc-owned heap).
    /// Only read by the non-inline compare path.
    current_ptr: *const u8,
    /// Length in bytes of the current row's value.
    current_len: u32,
    /// `inline_key_fast` of the current row's view. Only maintained (and read
    /// by `compare`) when `all_inline` — computing it once per row here beats
    /// recomputing it per loser-tree comparison (log2(streams) per row).
    current_inline_key: u128,
    /// `true` when every row is inline (`data_buffers().is_empty()`). Selects
    /// which of the two caches above `set_offset` maintains and `compare`
    /// reads.
    all_inline: bool,
}

// SAFETY: `current_ptr` points into `array`'s Arc-owned buffer heap. Since
// `StringViewArray` is `Send + Sync`, any thread holding this struct can
// safely dereference the ptr for reads.
unsafe impl Send for StringViewCursorValues {}
unsafe impl Sync for StringViewCursorValues {}

impl StringViewCursorValues {
    fn new(array: StringViewArray) -> Self {
        let all_inline = array.data_buffers().is_empty();
        let len = array.views().len();
        let views_ptr = array.views().as_ptr();
        let mut this = Self {
            array,
            len,
            views_ptr,
            current_ptr: std::ptr::NonNull::<u8>::dangling().as_ptr(),
            current_len: 0,
            current_inline_key: 0,
            all_inline,
        };
        if len != 0 {
            this.refresh_cache(0);
        }
        this
    }

    /// Refresh the current-row cache from `views()[offset]`: the
    /// `inline_key_fast` key when `all_inline`, the `(ptr, len)` slice
    /// otherwise.
    #[inline(always)]
    fn refresh_cache(&mut self, offset: usize) {
        // SAFETY: caller guarantees `offset < len`.
        let view = unsafe { *self.views_ptr.add(offset) };
        if self.all_inline {
            self.current_inline_key = StringViewArray::inline_key_fast(view);
            return;
        }
        let len = view as u32;
        self.current_len = len;
        if len <= 12 {
            // Inline: bytes are in the view at offset [4, 4+len). Point into
            // the views buffer's Arc heap (stable across struct moves).
            let views_bytes = self.views_ptr as *const u8;
            self.current_ptr = unsafe { views_bytes.add(offset * 16 + 4) };
        } else {
            // External: extract buffer_id + offset_in_buffer from the view.
            //   bits [ 0.. 32] = length
            //   bits [32.. 64] = 4-byte prefix (unused here)
            //   bits [64.. 96] = buffer_id
            //   bits [96..128] = offset_in_buffer
            let buffer_id = (view >> 64) as u32 as usize;
            let offset_in_buffer = (view >> 96) as u32 as usize;
            let buffer = unsafe { self.array.data_buffers().get_unchecked(buffer_id) };
            self.current_ptr = unsafe { buffer.as_ptr().add(offset_in_buffer) };
        }
    }

    #[inline(always)]
    fn current_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.current_ptr, self.current_len as usize) }
    }

    /// Resolve the byte slice for an arbitrary row. Used only on the rare
    /// mixed path where one side is all-inline (so it has no slice cache) and
    /// the other is not.
    #[inline]
    fn value_slice(&self, idx: usize) -> &[u8] {
        debug_assert!(idx < self.len);
        let view = unsafe { *self.views_ptr.add(idx) };
        let len = (view as u32) as usize;
        if len <= 12 {
            let views_bytes = self.views_ptr as *const u8;
            unsafe { std::slice::from_raw_parts(views_bytes.add(idx * 16 + 4), len) }
        } else {
            let buffer_id = (view >> 64) as u32 as usize;
            let offset_in_buffer = (view >> 96) as u32 as usize;
            unsafe {
                let buffer = self.array.data_buffers().get_unchecked(buffer_id);
                std::slice::from_raw_parts(buffer.as_ptr().add(offset_in_buffer), len)
            }
        }
    }
}

impl CursorValues for StringViewCursorValues {
    #[inline(always)]
    fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool {
        // Arbitrary indices (cross-batch): read raw views rather than the
        // current-row cache.
        let l_view = unsafe { *l.views_ptr.add(l_idx) };
        let r_view = unsafe { *r.views_ptr.add(r_idx) };

        if l.all_inline && r.all_inline {
            return l_view == r_view;
        }

        let l_len = l_view as u32;
        let r_len = r_view as u32;
        if l_len != r_len {
            return false;
        }

        unsafe {
            GenericByteViewArray::compare_unchecked(&l.array, l_idx, &r.array, r_idx)
                .is_eq()
        }
    }

    #[inline(always)]
    fn eq_to_previous(cursor: &Self, idx: usize) -> bool {
        let l_view = unsafe { *cursor.views_ptr.add(idx) };
        let r_view = unsafe { *cursor.views_ptr.add(idx - 1) };
        if cursor.all_inline {
            return l_view == r_view;
        }

        let l_len = l_view as u32;
        let r_len = r_view as u32;
        if l_len != r_len {
            return false;
        }

        unsafe {
            GenericByteViewArray::compare_unchecked(
                &cursor.array,
                idx,
                &cursor.array,
                idx - 1,
            )
            .is_eq()
        }
    }

    #[inline(always)]
    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering {
        debug_assert!(l_idx < l.len && r_idx < r.len);
        // The merge hot loop always compares cursors at their current offsets
        // (the invariant behind the caches; see `Cursor::cmp`).
        //
        // All-inline vs all-inline: compare the cached `inline_key_fast` keys
        // — one u128 comparison, no memory access beyond the two cursor
        // structs. `set_offset` computed the key once per row instead of the
        // baseline's twice per comparison.
        if l.all_inline && r.all_inline {
            return l.current_inline_key.cmp(&r.current_inline_key);
        }
        // Mixed: an all-inline side maintains no slice cache; resolve on the
        // fly. Rare (only when some batches of the column have long values
        // and others don't).
        if l.all_inline || r.all_inline {
            let l_slice = if l.all_inline {
                l.value_slice(l_idx)
            } else {
                l.current_slice()
            };
            let r_slice = if r.all_inline {
                r.value_slice(r_idx)
            } else {
                r.current_slice()
            };
            return l_slice.cmp(r_slice);
        }
        // Both have data buffers: the cached `(ptr, len)` populated by
        // `set_offset` skips a `views()` load and the
        // `data_buffers()[buffer_id]` lookup per compare.
        l.current_slice().cmp(r.current_slice())
    }

    #[inline(always)]
    fn set_offset(&mut self, offset: usize) {
        self.refresh_cache(offset);
    }
}

/// A collection of sorted, nullable [`CursorValues`]
///
/// Note: comparing cursors with different `SortOptions` will yield an arbitrary ordering
#[derive(Debug)]
pub struct ArrayValues<T: CursorValues> {
    values: T,
    // If nulls first, the first non-null index
    // Otherwise, the first null index
    null_threshold: usize,
    options: SortOptions,
    /// `true` when this array has at least one null. When `false`, the hot
    /// `compare` / `eq` paths skip the two `is_null(idx)` checks entirely
    /// (sort keys are usually null-free, so this branch is common).
    has_nulls: bool,

    /// Tracks the memory used by the values array,
    /// freed on drop.
    _reservation: MemoryReservation,
}

impl<T: CursorValues> ArrayValues<T> {
    /// Create a new [`ArrayValues`] from the provided `values` sorted according
    /// to `options`.
    ///
    /// Panics if the array is empty
    pub fn new<A: CursorArray<Values = T>>(
        options: SortOptions,
        array: &A,
        reservation: MemoryReservation,
    ) -> Self {
        assert!(array.len() > 0, "Empty array passed to FieldCursor");
        let null_count = array.null_count();
        let null_threshold = match options.nulls_first {
            true => null_count,
            false => array.len() - null_count,
        };

        Self {
            values: array.values(),
            null_threshold,
            options,
            has_nulls: null_count > 0,
            _reservation: reservation,
        }
    }

    #[inline(always)]
    fn is_null(&self, idx: usize) -> bool {
        (idx < self.null_threshold) == self.options.nulls_first
    }
}

impl<T: CursorValues> CursorValues for ArrayValues<T> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline(always)]
    fn eq(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> bool {
        // Fast path: neither array has nulls → the `is_null` match below
        // would take the (false, false) arm every time.
        if !l.has_nulls && !r.has_nulls {
            return T::eq(&l.values, l_idx, &r.values, r_idx);
        }
        match (l.is_null(l_idx), r.is_null(r_idx)) {
            (true, true) => true,
            (false, false) => T::eq(&l.values, l_idx, &r.values, r_idx),
            _ => false,
        }
    }

    #[inline(always)]
    fn eq_to_previous(cursor: &Self, idx: usize) -> bool {
        assert!(idx > 0);
        if !cursor.has_nulls {
            return T::eq_to_previous(&cursor.values, idx);
        }
        match (cursor.is_null(idx), cursor.is_null(idx - 1)) {
            (true, true) => true,
            // Delegate to inner `eq_to_previous` so a caching cursor can answer
            // without indexing.
            (false, false) => T::eq_to_previous(&cursor.values, idx),
            _ => false,
        }
    }

    #[inline(always)]
    fn compare(l: &Self, l_idx: usize, r: &Self, r_idx: usize) -> Ordering {
        // Fast path: neither array has nulls → skip both is_null checks.
        if !l.has_nulls && !r.has_nulls {
            return match l.options.descending {
                true => T::compare(&r.values, r_idx, &l.values, l_idx),
                false => T::compare(&l.values, l_idx, &r.values, r_idx),
            };
        }
        match (l.is_null(l_idx), r.is_null(r_idx)) {
            (true, true) => Ordering::Equal,
            (true, false) => match l.options.nulls_first {
                true => Ordering::Less,
                false => Ordering::Greater,
            },
            (false, true) => match l.options.nulls_first {
                true => Ordering::Greater,
                false => Ordering::Less,
            },
            (false, false) => match l.options.descending {
                true => T::compare(&r.values, r_idx, &l.values, l_idx),
                false => T::compare(&l.values, l_idx, &r.values, r_idx),
            },
        }
    }

    #[inline(always)]
    fn set_offset(&mut self, offset: usize) {
        // Forward to the wrapped values (e.g. caching `PrimitiveValues`).
        self.values.set_offset(offset);
    }
}

#[cfg(test)]
mod tests {
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };

    use super::*;

    fn new_primitive(
        options: SortOptions,
        values: ScalarBuffer<i32>,
        null_count: usize,
    ) -> Cursor<ArrayValues<PrimitiveValues<i32>>> {
        let null_threshold = match options.nulls_first {
            true => null_count,
            false => values.len() - null_count,
        };

        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(10000));
        let consumer = MemoryConsumer::new("test");
        let reservation = consumer.register(&memory_pool);

        let values = ArrayValues {
            values: PrimitiveValues::new(values),
            null_threshold,
            options,
            has_nulls: null_count > 0,
            _reservation: reservation,
        };

        Cursor::new(values)
    }

    #[test]
    fn test_primitive_nulls_first() {
        let options = SortOptions {
            descending: false,
            nulls_first: true,
        };

        let buffer = ScalarBuffer::from(vec![i32::MAX, 1, 2, 3]);
        let mut a = new_primitive(options, buffer, 1);
        let buffer = ScalarBuffer::from(vec![1, 2, -2, -1, 1, 9]);
        let mut b = new_primitive(options, buffer, 2);

        // NULL == NULL
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL == NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL < -2
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 1 > -2
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 1 > -1
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 1 == 1
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // 9 > 1
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 9 > 2
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        let options = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let buffer = ScalarBuffer::from(vec![0, 1, i32::MIN, i32::MAX]);
        let mut a = new_primitive(options, buffer, 2);
        let buffer = ScalarBuffer::from(vec![-1, i32::MAX, i32::MIN]);
        let mut b = new_primitive(options, buffer, 2);

        // 0 > -1
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 0 < NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 1 < NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // NULL = NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        let options = SortOptions {
            descending: true,
            nulls_first: false,
        };

        let buffer = ScalarBuffer::from(vec![6, 1, i32::MIN, i32::MAX]);
        let mut a = new_primitive(options, buffer, 3);
        let buffer = ScalarBuffer::from(vec![67, -3, i32::MAX, i32::MIN]);
        let mut b = new_primitive(options, buffer, 2);

        // 6 > 67
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 6 < -3
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 6 < NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 6 < NULL
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // NULL == NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        let options = SortOptions {
            descending: true,
            nulls_first: true,
        };

        let buffer = ScalarBuffer::from(vec![i32::MIN, i32::MAX, 6, 3]);
        let mut a = new_primitive(options, buffer, 2);
        let buffer = ScalarBuffer::from(vec![i32::MAX, 4546, -3]);
        let mut b = new_primitive(options, buffer, 1);

        // NULL == NULL
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL == NULL
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);

        // NULL < 4546
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);

        // 6 > 4546
        a.advance();
        assert_eq!(a.cmp(&b), Ordering::Greater);

        // 6 < -3
        b.advance();
        assert_eq!(a.cmp(&b), Ordering::Less);
    }
}
