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

use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;

use datafusion_common::{Result, exec_datafusion_err, internal_err};

use arrow::array::{
    Array, ArrayAccessor, ArrayDataBuilder, ArrayRef, BinaryArray, ByteView,
    GenericStringArray, LargeStringArray, OffsetSizeTrait, StringArray, StringViewArray,
    make_view,
};
use arrow::buffer::{Buffer, MutableBuffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::DataType;

/// Builder used by `concat`/`concat_ws` to assemble a [`StringArray`] one row
/// at a time from multiple input columns.
///
/// Each row is written via repeated `write` calls (one per input fragment)
/// followed by a single `append_offset` to commit the row.  The output null
/// buffer is computed in bulk by the caller and supplied to `finish`, avoiding
/// per-row NULL handling work.
///
/// For the common "produce one `&str` per row" pattern, prefer
/// `GenericStringArrayBuilder` instead.
pub(crate) struct ConcatStringBuilder {
    offsets_buffer: MutableBuffer,
    value_buffer: MutableBuffer,
    /// If true, a safety check is required during the `finish` call
    tainted: bool,
}

impl ConcatStringBuilder {
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let capacity = item_capacity
            .checked_add(1)
            .map(|i| i.saturating_mul(size_of::<i32>()))
            .expect("capacity integer overflow");

        let mut offsets_buffer = MutableBuffer::with_capacity(capacity);
        // SAFETY: the first offset value is definitely not going to exceed the bounds.
        unsafe { offsets_buffer.push_unchecked(0_i32) };
        Self {
            offsets_buffer,
            value_buffer: MutableBuffer::with_capacity(data_capacity),
            tainted: false,
        }
    }

    pub fn write<const CHECK_VALID: bool>(
        &mut self,
        column: &ColumnarValueRef,
        i: usize,
    ) {
        match column {
            ColumnarValueRef::Scalar(s) => {
                self.value_buffer.extend_from_slice(s);
                self.tainted = true;
            }
            ColumnarValueRef::NullableArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer
                        .extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableLargeStringArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer
                        .extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableStringViewArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer
                        .extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableBinaryArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer.extend_from_slice(array.value(i));
                }
                self.tainted = true;
            }
            ColumnarValueRef::NonNullableArray(array) => {
                self.value_buffer
                    .extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableLargeStringArray(array) => {
                self.value_buffer
                    .extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableStringViewArray(array) => {
                self.value_buffer
                    .extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableBinaryArray(array) => {
                self.value_buffer.extend_from_slice(array.value(i));
                self.tainted = true;
            }
        }
    }

    pub fn append_offset(&mut self) -> Result<()> {
        let next_offset: i32 = self
            .value_buffer
            .len()
            .try_into()
            .map_err(|_| exec_datafusion_err!("byte array offset overflow"))?;
        self.offsets_buffer.push(next_offset);
        Ok(())
    }

    /// Finalize the builder into a concrete [`StringArray`].
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - the provided `null_buffer` is not the same length as the `offsets_buffer`.
    pub fn finish(self, null_buffer: Option<NullBuffer>) -> Result<StringArray> {
        let row_count = self.offsets_buffer.len() / size_of::<i32>() - 1;
        if let Some(ref null_buffer) = null_buffer
            && null_buffer.len() != row_count
        {
            return internal_err!(
                "Null buffer and offsets buffer must be the same length"
            );
        }
        let array_builder = ArrayDataBuilder::new(DataType::Utf8)
            .len(row_count)
            .add_buffer(self.offsets_buffer.into())
            .add_buffer(self.value_buffer.into())
            .nulls(null_buffer);
        if self.tainted {
            // Raw binary arrays with possible invalid utf-8 were used,
            // so let ArrayDataBuilder perform validation
            let array_data = array_builder.build()?;
            Ok(StringArray::from(array_data))
        } else {
            // SAFETY: all data that was appended was valid UTF8 and the values
            // and offsets were created correctly
            let array_data = unsafe { array_builder.build_unchecked() };
            Ok(StringArray::from(array_data))
        }
    }
}

/// Builder used by `concat`/`concat_ws` to assemble a [`StringViewArray`] one
/// row at a time from multiple input columns.
///
/// Each row is written via repeated `write` calls (one per input
/// fragment) followed by a single `append_offset` to commit the row
/// as a single string view. The output null buffer is supplied by the caller
/// at `finish` time, avoiding per-row NULL handling work.
///
/// For the common "produce one `&str` per row" pattern, prefer
/// [`StringViewArrayBuilder`] instead.
pub(crate) struct ConcatStringViewBuilder {
    views: Vec<u128>,
    data: Vec<u8>,
    block: Vec<u8>,
    /// If true, a safety check is required during the `append_offset` call
    tainted: bool,
}

impl ConcatStringViewBuilder {
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        Self {
            views: Vec::with_capacity(item_capacity),
            data: Vec::with_capacity(data_capacity),
            block: vec![],
            tainted: false,
        }
    }

    pub fn write<const CHECK_VALID: bool>(
        &mut self,
        column: &ColumnarValueRef,
        i: usize,
    ) {
        match column {
            ColumnarValueRef::Scalar(s) => {
                self.block.extend_from_slice(s);
                self.tainted = true;
            }
            ColumnarValueRef::NullableArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.block.extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableLargeStringArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.block.extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableStringViewArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.block.extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableBinaryArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.block.extend_from_slice(array.value(i));
                }
                self.tainted = true;
            }
            ColumnarValueRef::NonNullableArray(array) => {
                self.block.extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableLargeStringArray(array) => {
                self.block.extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableStringViewArray(array) => {
                self.block.extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableBinaryArray(array) => {
                self.block.extend_from_slice(array.value(i));
                self.tainted = true;
            }
        }
    }

    /// Finalizes the current row by converting the accumulated data into a
    /// StringView and appending it to the views buffer.
    pub fn append_offset(&mut self) -> Result<()> {
        if self.tainted {
            std::str::from_utf8(&self.block)
                .map_err(|_| exec_datafusion_err!("invalid UTF-8 in binary literal"))?;
        }

        let v = &self.block;
        if v.len() > 12 {
            let offset: u32 = self
                .data
                .len()
                .try_into()
                .map_err(|_| exec_datafusion_err!("byte array offset overflow"))?;
            self.data.extend_from_slice(v);
            self.views.push(make_view(v, 0, offset));
        } else {
            self.views.push(make_view(v, 0, 0));
        }

        self.block.clear();
        self.tainted = false;
        Ok(())
    }

    /// Finalize the builder into a concrete [`StringViewArray`].
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - the provided `null_buffer` length does not match the row count.
    pub fn finish(self, null_buffer: Option<NullBuffer>) -> Result<StringViewArray> {
        if let Some(ref nulls) = null_buffer
            && nulls.len() != self.views.len()
        {
            return internal_err!(
                "Null buffer length ({}) must match row count ({})",
                nulls.len(),
                self.views.len()
            );
        }

        let buffers: Vec<Buffer> = if self.data.is_empty() {
            vec![]
        } else {
            vec![Buffer::from(self.data)]
        };

        // SAFETY: views were constructed with correct lengths, offsets, and
        // prefixes. UTF-8 validity was checked in append_offset() for any row
        // where tainted data (e.g., binary literals) was appended.
        let array = unsafe {
            StringViewArray::new_unchecked(
                ScalarBuffer::from(self.views),
                buffers,
                null_buffer,
            )
        };
        Ok(array)
    }
}

/// Builder used by `concat`/`concat_ws` to assemble a [`LargeStringArray`] one
/// row at a time from multiple input columns. See [`ConcatStringBuilder`] for
/// details on the row-composition contract.
///
/// For the common "produce one `&str` per row" pattern, prefer
/// `GenericStringArrayBuilder` instead.
pub(crate) struct ConcatLargeStringBuilder {
    offsets_buffer: MutableBuffer,
    value_buffer: MutableBuffer,
    /// If true, a safety check is required during the `finish` call
    tainted: bool,
}

impl ConcatLargeStringBuilder {
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let capacity = item_capacity
            .checked_add(1)
            .map(|i| i.saturating_mul(size_of::<i64>()))
            .expect("capacity integer overflow");

        let mut offsets_buffer = MutableBuffer::with_capacity(capacity);
        // SAFETY: the first offset value is definitely not going to exceed the bounds.
        unsafe { offsets_buffer.push_unchecked(0_i64) };
        Self {
            offsets_buffer,
            value_buffer: MutableBuffer::with_capacity(data_capacity),
            tainted: false,
        }
    }

    pub fn write<const CHECK_VALID: bool>(
        &mut self,
        column: &ColumnarValueRef,
        i: usize,
    ) {
        match column {
            ColumnarValueRef::Scalar(s) => {
                self.value_buffer.extend_from_slice(s);
                self.tainted = true;
            }
            ColumnarValueRef::NullableArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer
                        .extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableLargeStringArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer
                        .extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableStringViewArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer
                        .extend_from_slice(array.value(i).as_bytes());
                }
            }
            ColumnarValueRef::NullableBinaryArray(array) => {
                if !CHECK_VALID || array.is_valid(i) {
                    self.value_buffer.extend_from_slice(array.value(i));
                }
                self.tainted = true;
            }
            ColumnarValueRef::NonNullableArray(array) => {
                self.value_buffer
                    .extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableLargeStringArray(array) => {
                self.value_buffer
                    .extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableStringViewArray(array) => {
                self.value_buffer
                    .extend_from_slice(array.value(i).as_bytes());
            }
            ColumnarValueRef::NonNullableBinaryArray(array) => {
                self.value_buffer.extend_from_slice(array.value(i));
                self.tainted = true;
            }
        }
    }

    pub fn append_offset(&mut self) -> Result<()> {
        let next_offset: i64 = self
            .value_buffer
            .len()
            .try_into()
            .map_err(|_| exec_datafusion_err!("byte array offset overflow"))?;
        self.offsets_buffer.push(next_offset);
        Ok(())
    }

    /// Finalize the builder into a concrete [`LargeStringArray`].
    ///
    /// # Errors
    ///
    /// Returns an error when:
    ///
    /// - the provided `null_buffer` is not the same length as the `offsets_buffer`.
    pub fn finish(self, null_buffer: Option<NullBuffer>) -> Result<LargeStringArray> {
        let row_count = self.offsets_buffer.len() / size_of::<i64>() - 1;
        if let Some(ref null_buffer) = null_buffer
            && null_buffer.len() != row_count
        {
            return internal_err!(
                "Null buffer and offsets buffer must be the same length"
            );
        }
        let array_builder = ArrayDataBuilder::new(DataType::LargeUtf8)
            .len(row_count)
            .add_buffer(self.offsets_buffer.into())
            .add_buffer(self.value_buffer.into())
            .nulls(null_buffer);
        if self.tainted {
            // Raw binary arrays with possible invalid utf-8 were used,
            // so let ArrayDataBuilder perform validation
            let array_data = array_builder.build()?;
            Ok(LargeStringArray::from(array_data))
        } else {
            // SAFETY: all data that was appended was valid Large UTF8 and the values
            // and offsets were created correctly
            let array_data = unsafe { array_builder.build_unchecked() };
            Ok(LargeStringArray::from(array_data))
        }
    }
}

// ----------------------------------------------------------------------------
// Bulk-nulls builders
//
// These builders are similar to Arrow's `GenericStringBuilder` and
// `StringViewBuilder` but tuned for string UDFs along two axes:
//
//   * Bulk-NULL handling. The NULL bitmap is passed to `finish()` rather than
//     maintained per-row. Many string UDFs can compute the bitmap in bulk,
//     where this is significantly more efficient.
//   * Closure-based row emission. Beyond `append_value(&str)`, the builders
//     expose `append_with` (fragments written into the builder via a
//     `StringWriter`) and `append_byte_map` (byte-to-byte mapping of an input
//     slice), letting UDFs emit a row without first assembling it in a scratch
//     `String`.
// ----------------------------------------------------------------------------

/// Builder for a [`GenericStringArray<O>`]. Instantiate with `O = i32` for
/// [`StringArray`] (Utf8) or `O = i64` for [`LargeStringArray`] (LargeUtf8).
pub(crate) struct GenericStringArrayBuilder<O: OffsetSizeTrait> {
    offsets_buffer: MutableBuffer,
    value_buffer: MutableBuffer,
    placeholder_count: usize,
    _phantom: PhantomData<O>,
}

impl<O: OffsetSizeTrait> GenericStringArrayBuilder<O> {
    pub fn with_capacity(item_capacity: usize, data_capacity: usize) -> Self {
        let capacity = item_capacity
            .checked_add(1)
            .map(|i| i.saturating_mul(size_of::<O>()))
            .expect("capacity integer overflow");

        let mut offsets_buffer = MutableBuffer::with_capacity(capacity);
        offsets_buffer.push(O::usize_as(0));
        Self {
            offsets_buffer,
            value_buffer: MutableBuffer::with_capacity(data_capacity),
            placeholder_count: 0,
            _phantom: PhantomData,
        }
    }

    /// See [`BulkNullStringArrayBuilder::append_value`].
    ///
    /// # Panics
    ///
    /// Panics if the cumulative byte length exceeds `O::MAX`.
    #[inline]
    pub fn append_value(&mut self, value: &str) {
        self.value_buffer.extend_from_slice(value.as_bytes());
        let next_offset =
            O::from_usize(self.value_buffer.len()).expect("byte array offset overflow");
        self.offsets_buffer.push(next_offset);
    }

    /// See [`BulkNullStringArrayBuilder::append_placeholder`].
    #[inline]
    pub fn append_placeholder(&mut self) {
        let next_offset =
            O::from_usize(self.value_buffer.len()).expect("byte array offset overflow");
        self.offsets_buffer.push(next_offset);
        self.placeholder_count += 1;
    }

    /// See [`BulkNullStringArrayBuilder::append_byte_map`].
    ///
    /// # Safety
    ///
    /// The bytes produced by applying `map` to each byte of `src`, in order,
    /// must form valid UTF-8.
    ///
    /// # Panics
    ///
    /// Panics if the cumulative byte length exceeds `O::MAX`.
    #[inline]
    pub unsafe fn append_byte_map<F: FnMut(u8) -> u8>(&mut self, src: &[u8], mut map: F) {
        self.value_buffer.extend(src.iter().map(|&b| map(b)));
        let next_offset =
            O::from_usize(self.value_buffer.len()).expect("byte array offset overflow");
        self.offsets_buffer.push(next_offset);
    }

    /// See [`BulkNullStringArrayBuilder::append_with`].
    ///
    /// # Panics
    ///
    /// Panics if the cumulative byte length exceeds `O::MAX`.
    #[inline]
    pub fn append_with<F>(&mut self, f: F)
    where
        F: FnOnce(&mut GenericStringWriter<'_>),
    {
        let mut writer = GenericStringWriter {
            value_buffer: &mut self.value_buffer,
        };
        f(&mut writer);
        let next_offset =
            O::from_usize(self.value_buffer.len()).expect("byte array offset overflow");
        self.offsets_buffer.push(next_offset);
    }

    /// Finalize into a [`GenericStringArray<O>`] using the caller-supplied
    /// null buffer.
    ///
    /// # Errors
    ///
    /// Returns an error when `null_buffer.len()` does not match the number of
    /// appended rows.
    pub fn finish(
        self,
        null_buffer: Option<NullBuffer>,
    ) -> Result<GenericStringArray<O>> {
        let row_count = self.offsets_buffer.len() / size_of::<O>() - 1;
        if let Some(ref n) = null_buffer
            && n.len() != row_count
        {
            return internal_err!(
                "Null buffer length ({}) must match row count ({row_count})",
                n.len()
            );
        }
        let null_count = null_buffer.as_ref().map_or(0, |n| n.null_count());
        debug_assert!(
            null_count >= self.placeholder_count,
            "{} placeholder rows but null buffer has {null_count} nulls",
            self.placeholder_count,
        );
        let array_data = ArrayDataBuilder::new(GenericStringArray::<O>::DATA_TYPE)
            .len(row_count)
            .add_buffer(self.offsets_buffer.into())
            .add_buffer(self.value_buffer.into())
            .nulls(null_buffer);
        // SAFETY: every appended value came from a `&str`, so the value
        // buffer is valid UTF-8 and offsets are monotonically non-decreasing.
        let array_data = unsafe { array_data.build_unchecked() };
        Ok(GenericStringArray::<O>::from(array_data))
    }
}

/// Starting size for the long-string data block used by `StringView`-style
/// arrays; matches Arrow's `GenericByteViewBuilder` default.
pub(crate) const STRING_VIEW_INIT_BLOCK_SIZE: u32 = 8 * 1024;
/// Maximum size each long-string data block in a `StringView`-style array
/// grows to; matches Arrow's `GenericByteViewBuilder` default.
pub(crate) const STRING_VIEW_MAX_BLOCK_SIZE: u32 = 2 * 1024 * 1024;

/// Append-only writer handed to closures passed to `append_with`.
pub(crate) trait StringWriter {
    fn write_str(&mut self, s: &str);
    fn write_char(&mut self, c: char);
}

/// [`StringWriter`] for [`GenericStringArrayBuilder`]. Writes go straight to
/// the value buffer.
pub(crate) struct GenericStringWriter<'a> {
    value_buffer: &'a mut MutableBuffer,
}

impl StringWriter for GenericStringWriter<'_> {
    #[inline(always)]
    fn write_str(&mut self, s: &str) {
        push_bytes_to_mutable_buffer(self.value_buffer, s.as_bytes());
    }

    #[inline(always)]
    fn write_char(&mut self, c: char) {
        push_char_to_mutable_buffer(self.value_buffer, c);
    }
}

/// Write `bytes` into `value_buffer`. For repeated small writes,
/// MutableBuffer::extend_from_slice can be slow (memcpy per call), so we extend
/// the buffer here directly and force inlining.
#[inline(always)]
fn push_bytes_to_mutable_buffer(value_buffer: &mut MutableBuffer, bytes: &[u8]) {
    let n = bytes.len();
    let old_len = value_buffer.len();
    value_buffer.reserve(n);

    // SAFETY: we reserved `n` bytes; the source and destination do not alias
    // because `bytes` was passed in by the caller and `value_buffer` is owned.
    unsafe {
        let dst = value_buffer.as_mut_ptr().add(old_len);
        let src = bytes.as_ptr();
        match n {
            0 => {}
            1 => std::ptr::copy_nonoverlapping(src, dst, 1),
            2 => std::ptr::copy_nonoverlapping(src, dst, 2),
            3 => std::ptr::copy_nonoverlapping(src, dst, 3),
            4 => std::ptr::copy_nonoverlapping(src, dst, 4),
            5 => std::ptr::copy_nonoverlapping(src, dst, 5),
            6 => std::ptr::copy_nonoverlapping(src, dst, 6),
            7 => std::ptr::copy_nonoverlapping(src, dst, 7),
            8 => std::ptr::copy_nonoverlapping(src, dst, 8),
            _ => std::ptr::copy_nonoverlapping(src, dst, n),
        }
        value_buffer.set_len(old_len + n);
    }
}

#[inline(always)]
fn push_char_to_mutable_buffer(value_buffer: &mut MutableBuffer, c: char) {
    let len = c.len_utf8();
    let old_len = value_buffer.len();
    value_buffer.reserve(len);

    // SAFETY: we reserved `len` bytes above, write valid UTF-8 into those
    // bytes, then update the initialized length to include them.
    unsafe {
        let dst = value_buffer.as_mut_ptr().add(old_len);
        if len == 1 {
            *dst = c as u8;
        } else {
            c.encode_utf8(std::slice::from_raw_parts_mut(dst, len));
        }
        value_buffer.set_len(old_len + len);
    }
}

/// Builder for a [`StringViewArray`].
///
/// Short strings (≤ 12 bytes) are inlined into the view itself; long strings
/// are appended into an in-progress data block. When the in-progress block
/// fills up it is flushed into `completed` and a new block — double the size
/// of the last, capped at [`STRING_VIEW_MAX_BLOCK_SIZE`] — is started.
pub(crate) struct StringViewArrayBuilder {
    views: Vec<u128>,
    in_progress: Vec<u8>,
    completed: Vec<Buffer>,
    block_size: u32,
    placeholder_count: usize,
}

impl StringViewArrayBuilder {
    pub fn with_capacity(item_capacity: usize) -> Self {
        Self {
            views: Vec::with_capacity(item_capacity),
            in_progress: Vec::new(),
            completed: Vec::new(),
            block_size: STRING_VIEW_INIT_BLOCK_SIZE,
            placeholder_count: 0,
        }
    }

    /// Doubles the block-size target and returns the new size.
    fn next_block_size(&mut self) -> u32 {
        if self.block_size < STRING_VIEW_MAX_BLOCK_SIZE {
            self.block_size = self.block_size.saturating_mul(2);
        }
        self.block_size
    }

    /// See [`BulkNullStringArrayBuilder::append_value`].
    ///
    /// # Panics
    ///
    /// Panics if the value length, the in-progress buffer offset, or the
    /// number of completed buffers exceeds `i32::MAX`. The ByteView spec
    /// uses signed 32-bit integers for these fields; exceeding `i32::MAX`
    /// would produce an array that does not round-trip through Arrow IPC
    /// (see <https://github.com/apache/arrow-rs/issues/6172>).
    #[inline]
    pub fn append_value(&mut self, value: &str) {
        let v = value.as_bytes();
        let length: u32 =
            i32::try_from(v.len()).expect("value length exceeds i32::MAX") as u32;
        if length <= 12 {
            self.views.push(make_view(v, 0, 0));
            return;
        }

        let required_cap = self.in_progress.len() + length as usize;
        if self.in_progress.capacity() < required_cap {
            self.flush_in_progress();
            let to_reserve = (length as usize).max(self.next_block_size() as usize);
            self.in_progress.reserve(to_reserve);
        }

        let offset: u32 = i32::try_from(self.in_progress.len())
            .expect("offset exceeds i32::MAX") as u32;
        self.in_progress.extend_from_slice(v);
        self.views.push(self.make_long_view(length, offset, v));
    }

    /// See [`BulkNullStringArrayBuilder::append_placeholder`].
    #[inline]
    pub fn append_placeholder(&mut self) {
        // Zero-length inline view — `length` field is 0, no buffer ref.
        self.views.push(0);
        self.placeholder_count += 1;
    }

    /// Ensure the in-progress block has room for `length` more bytes,
    /// flushing the current block and starting a new (doubled) one if not.
    /// Caller must invoke this only when no bytes of the current row are
    /// yet in `in_progress` — flushing mid-row would orphan partial data.
    #[inline]
    fn ensure_long_capacity(&mut self, length: u32) {
        let required_cap = self.in_progress.len() + length as usize;
        if self.in_progress.capacity() < required_cap {
            self.flush_in_progress();
            let to_reserve = (length as usize).max(self.next_block_size() as usize);
            self.in_progress.reserve(to_reserve);
        }
    }

    /// Encode a long-form view referencing `length` bytes already written
    /// into the in-progress block at `offset`. `prefix_bytes` is the row's
    /// data slice (or any slice starting with the row's first 4 bytes).
    ///
    /// Built inline rather than going through Arrow's `make_view`: that
    /// function is `[inline(never)]` and has to handle short strings, so
    /// building the view here ourselves is faster.
    #[inline]
    fn make_long_view(&self, length: u32, offset: u32, prefix_bytes: &[u8]) -> u128 {
        let buffer_index: u32 = i32::try_from(self.completed.len())
            .expect("buffer count exceeds i32::MAX")
            as u32;
        ByteView {
            length,
            // length > 12, so prefix_bytes has at least 4 bytes.
            prefix: u32::from_le_bytes(prefix_bytes[..4].try_into().unwrap()),
            buffer_index,
            offset,
        }
        .into()
    }

    /// See [`BulkNullStringArrayBuilder::append_byte_map`].
    ///
    /// # Safety
    ///
    /// The bytes produced by applying `map` to each byte of `src`, in order,
    /// must form valid UTF-8.
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`Self::append_value`]: if
    /// `src.len()`, the in-progress buffer offset, or the number of completed
    /// buffers exceeds `i32::MAX`.
    #[inline]
    pub unsafe fn append_byte_map<F: FnMut(u8) -> u8>(&mut self, src: &[u8], mut map: F) {
        let length: u32 =
            i32::try_from(src.len()).expect("value length exceeds i32::MAX") as u32;
        if length <= 12 {
            // `iter_mut().zip()` over a stack array is the canonical autovec
            // pattern.
            let mut bytes = [0u8; 12];
            for (d, &b) in bytes[..src.len()].iter_mut().zip(src) {
                *d = map(b);
            }
            self.views.push(make_view(&bytes[..src.len()], 0, 0));
            return;
        }

        self.ensure_long_capacity(length);

        let cursor = self.in_progress.len();
        let offset: u32 = i32::try_from(cursor).expect("offset exceeds i32::MAX") as u32;
        self.in_progress.extend(src.iter().map(|&b| map(b)));
        self.views
            .push(self.make_long_view(length, offset, &self.in_progress[cursor..]));
    }

    /// See [`BulkNullStringArrayBuilder::append_with`].
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`Self::append_value`]: if the
    /// row's byte length, the in-progress buffer offset, or the number of
    /// completed buffers exceeds `i32::MAX`.
    #[inline]
    pub fn append_with<F>(&mut self, f: F)
    where
        F: FnOnce(&mut StringViewWriter<'_>),
    {
        let mut writer = StringViewWriter {
            inline_buf: [0u8; 12],
            inline_len: 0,
            spill_cursor: None,
            builder: self,
        };
        f(&mut writer);
        // Destructure to release the borrow on `self` and pull out the
        // inline-buffer state by-value. Copy types only; the &mut self is
        // dropped here, ending the borrow.
        let StringViewWriter {
            inline_buf,
            inline_len,
            spill_cursor,
            ..
        } = writer;

        match spill_cursor {
            None => {
                self.views
                    .push(make_view(&inline_buf[..inline_len as usize], 0, 0));
            }
            Some(start) => {
                let end = self.in_progress.len();
                let length: u32 = i32::try_from(end - start)
                    .expect("value length exceeds i32::MAX")
                    as u32;
                let offset: u32 =
                    i32::try_from(start).expect("offset exceeds i32::MAX") as u32;
                self.views.push(self.make_long_view(
                    length,
                    offset,
                    &self.in_progress[start..],
                ));
            }
        }
    }

    fn flush_in_progress(&mut self) {
        if !self.in_progress.is_empty() {
            let block = std::mem::take(&mut self.in_progress);
            self.completed.push(Buffer::from_vec(block));
        }
    }

    /// Finalize into a [`StringViewArray`] using the caller-supplied null
    /// buffer.
    ///
    /// # Errors
    ///
    /// Returns an error when `null_buffer.len()` does not match the number of
    /// appended rows.
    pub fn finish(mut self, null_buffer: Option<NullBuffer>) -> Result<StringViewArray> {
        if let Some(ref n) = null_buffer
            && n.len() != self.views.len()
        {
            return internal_err!(
                "Null buffer length ({}) must match row count ({})",
                n.len(),
                self.views.len()
            );
        }
        let null_count = null_buffer.as_ref().map_or(0, |n| n.null_count());
        debug_assert!(
            null_count >= self.placeholder_count,
            "{} placeholder rows but null buffer has {null_count} nulls",
            self.placeholder_count,
        );
        self.flush_in_progress();
        // SAFETY: every long-string view references bytes we wrote ourselves
        // into `self.completed`, with prefixes derived from those same bytes.
        // Inline views were built from valid `&str`. Placeholder views are
        // zero-length with no buffer reference.
        let array = unsafe {
            StringViewArray::new_unchecked(
                ScalarBuffer::from(self.views),
                self.completed,
                null_buffer,
            )
        };
        Ok(array)
    }
}

/// [`StringWriter`] for [`StringViewArrayBuilder`].
///
/// The writer accumulates the first up-to-12 bytes of a row in a stack
/// buffer; if the row stays inline-sized, it never touches the data block.
/// On the first write that would exceed 12 bytes, the stack buffer is
/// spilled into the builder's in-progress block and subsequent writes go
/// directly there.
pub(crate) struct StringViewWriter<'a> {
    inline_buf: [u8; 12],
    inline_len: u8,
    /// `None` while the row fits inline; becomes `Some(start)` (offset of
    /// the row's first byte in `in_progress`) at first spill.
    spill_cursor: Option<usize>,
    builder: &'a mut StringViewArrayBuilder,
}

impl StringWriter for StringViewWriter<'_> {
    #[inline]
    fn write_str(&mut self, s: &str) {
        let bytes = s.as_bytes();
        if self.spill_cursor.is_some() {
            self.builder.in_progress.extend_from_slice(bytes);
            return;
        }

        let inline_len = self.inline_len as usize;
        let new_len = inline_len + bytes.len();
        if new_len <= 12 {
            self.inline_buf[inline_len..new_len].copy_from_slice(bytes);
            self.inline_len = new_len as u8;
            return;
        }

        // First spill of this row: `ensure_long_capacity` may flush the
        // current block, which is safe because no row-data for this row
        // is in it yet — the inline prefix is still in `inline_buf`.
        self.builder.ensure_long_capacity(new_len as u32);
        let cursor = self.builder.in_progress.len();
        self.builder
            .in_progress
            .extend_from_slice(&self.inline_buf[..inline_len]);
        self.builder.in_progress.extend_from_slice(bytes);
        self.spill_cursor = Some(cursor);
    }

    #[inline]
    fn write_char(&mut self, c: char) {
        let len = c.len_utf8();
        if self.spill_cursor.is_some() {
            push_char_to_vec(&mut self.builder.in_progress, c);
            return;
        }

        let inline_len = self.inline_len as usize;
        let new_len = inline_len + len;
        if new_len <= 12 {
            c.encode_utf8(&mut self.inline_buf[inline_len..new_len]);
            self.inline_len = new_len as u8;
            return;
        }

        self.builder.ensure_long_capacity(new_len as u32);
        let cursor = self.builder.in_progress.len();
        self.builder
            .in_progress
            .extend_from_slice(&self.inline_buf[..inline_len]);
        push_char_to_vec(&mut self.builder.in_progress, c);
        self.spill_cursor = Some(cursor);
    }
}

#[inline]
fn push_char_to_vec(v: &mut Vec<u8>, c: char) {
    let len = c.len_utf8();
    let old_len = v.len();
    v.reserve(len);
    // SAFETY: we reserved `len` bytes above, write valid UTF-8 into those
    // bytes, then update the initialized length to include them.
    unsafe {
        let dst = v.as_mut_ptr().add(old_len);
        if len == 1 {
            *dst = c as u8;
        } else {
            c.encode_utf8(std::slice::from_raw_parts_mut(dst, len));
        }
        v.set_len(old_len + len);
    }
}

/// Trait abstracting over the bulk-NULL string array builders.
///
/// Similar to Arrow's `StringLikeArrayBuilder`, this allows generic dispatch
/// over the three string array types (Utf8, LargeUtf8, Utf8View) when the
/// function body is uniform across them.
///
/// Three methods append a non-null row; which method to pick depends on how the
/// row is produced:
///
/// - [`append_value`](Self::append_value) pushes an already-finished `&str`.
///   Use it when the row is forwarded from an existing slice (e.g. an input
///   column) — there is nothing to elide.
/// - [`append_byte_map`](Self::append_byte_map) emits a row whose bytes are a
///   byte-to-byte mapping of an input slice. Output length is known up front
///   and the inner loop is straight-line, so this is the fastest path when the
///   shape fits.
/// - [`append_with`](Self::append_with) emits a row by feeding fragments to a
///   [`StringWriter`]. Use it when the row is computed from multiple sources or
///   when the output length is not known up front. Bytes are written directly
///   into the builder, so it is typically faster than assembling a `String` and
///   calling `append_value(&scratch)`.
///
/// For a NULL row, call [`append_placeholder`](Self::append_placeholder) to
/// advance the row count without writing into the value buffer; the caller MUST
/// clear the corresponding bit in the null buffer passed to
/// [`finish`](Self::finish).
pub(crate) trait BulkNullStringArrayBuilder {
    /// Per-builder concrete writer type, exposed as a GAT so generic callers
    /// can use the inherent (non-`dyn`) writer methods without vtable
    /// dispatch.
    type Writer<'a>: StringWriter
    where
        Self: 'a;

    /// Append `value` as the next row.
    ///
    /// # Panics
    ///
    /// Panics if the resulting array would exceed the per-implementation
    /// size limit. See the inherent method on each builder for specifics.
    fn append_value(&mut self, value: &str);

    /// Append an empty placeholder row. The corresponding slot MUST be masked
    /// as null by the null buffer passed to [`finish`](Self::finish).
    fn append_placeholder(&mut self);

    /// Append a row whose bytes are produced by `f` calling write methods on
    /// the supplied [`StringWriter`].
    ///
    /// The closure can call `write_str` or `write_char` on the supplied
    /// `StringWriter` zero or more times. Zero calls produces a row containing
    /// the empty string.
    ///
    /// # Panics
    ///
    /// See [`append_value`](Self::append_value).
    fn append_with<F>(&mut self, f: F)
    where
        F: for<'a> FnOnce(&mut Self::Writer<'a>);

    /// Append a row whose bytes are produced by mapping each byte of `src`
    /// through `map`, in order. Output length equals `src.len()`.
    ///
    /// Because the output length is known up front and the inner loop is
    /// straight-line, this is more efficient than
    /// [`append_with`](Self::append_with) for byte-to-byte mappings and
    /// autovectorizes well.
    ///
    /// # Safety
    ///
    /// The bytes produced by applying `map` to each byte of `src`, in order,
    /// must form valid UTF-8.
    ///
    /// # Panics
    ///
    /// See [`append_value`](Self::append_value).
    unsafe fn append_byte_map<F: FnMut(u8) -> u8>(&mut self, src: &[u8], map: F);

    /// Finalize into a concrete array using the caller-supplied null buffer.
    ///
    /// # Errors
    ///
    /// Returns an error when `null_buffer.len()` does not match the number
    /// of appended rows.
    fn finish(self, nulls: Option<NullBuffer>) -> Result<ArrayRef>;
}

impl<O: OffsetSizeTrait> BulkNullStringArrayBuilder for GenericStringArrayBuilder<O> {
    type Writer<'a> = GenericStringWriter<'a>;

    #[inline]
    fn append_value(&mut self, value: &str) {
        GenericStringArrayBuilder::<O>::append_value(self, value)
    }
    #[inline]
    fn append_placeholder(&mut self) {
        GenericStringArrayBuilder::<O>::append_placeholder(self)
    }
    #[inline]
    fn append_with<F>(&mut self, f: F)
    where
        F: for<'a> FnOnce(&mut Self::Writer<'a>),
    {
        GenericStringArrayBuilder::<O>::append_with(self, f)
    }
    #[inline]
    unsafe fn append_byte_map<F: FnMut(u8) -> u8>(&mut self, src: &[u8], map: F) {
        // SAFETY: contract forwarded.
        unsafe { GenericStringArrayBuilder::<O>::append_byte_map(self, src, map) }
    }
    fn finish(self, nulls: Option<NullBuffer>) -> Result<ArrayRef> {
        Ok(Arc::new(GenericStringArrayBuilder::<O>::finish(
            self, nulls,
        )?))
    }
}

impl BulkNullStringArrayBuilder for StringViewArrayBuilder {
    type Writer<'a> = StringViewWriter<'a>;

    #[inline]
    fn append_value(&mut self, value: &str) {
        StringViewArrayBuilder::append_value(self, value)
    }
    #[inline]
    fn append_placeholder(&mut self) {
        StringViewArrayBuilder::append_placeholder(self)
    }
    #[inline]
    fn append_with<F>(&mut self, f: F)
    where
        F: for<'a> FnOnce(&mut Self::Writer<'a>),
    {
        StringViewArrayBuilder::append_with(self, f)
    }
    #[inline]
    unsafe fn append_byte_map<F: FnMut(u8) -> u8>(&mut self, src: &[u8], map: F) {
        // SAFETY: contract forwarded.
        unsafe { StringViewArrayBuilder::append_byte_map(self, src, map) }
    }
    fn finish(self, nulls: Option<NullBuffer>) -> Result<ArrayRef> {
        Ok(Arc::new(StringViewArrayBuilder::finish(self, nulls)?))
    }
}

/// Append a new view to the views buffer with the given substr.
///
/// Callers are responsible for their own null tracking.
///
/// # Safety
///
/// original_view must be a valid view (the format described on
/// [`GenericByteViewArray`](arrow::array::GenericByteViewArray).
///
/// # Arguments
/// - views_buffer: The buffer to append the new view to
/// - original_view: The original view value
/// - substr: The substring to append. Must be a valid substring of the original view
/// - start_offset: The start offset of the substring in the view
///
/// LLVM is apparently overly eager to inline this function into some hot loops,
/// which bloats them and regresses performance, so we disable inlining for now.
#[inline(never)]
pub(crate) fn append_view(
    views_buffer: &mut Vec<u128>,
    original_view: &u128,
    substr: &str,
    start_offset: u32,
) {
    let substr_len = substr.len();
    let sub_view = if substr_len > 12 {
        let view = ByteView::from(*original_view);
        make_view(
            substr.as_bytes(),
            view.buffer_index,
            view.offset + start_offset,
        )
    } else {
        make_view(substr.as_bytes(), 0, 0)
    };
    views_buffer.push(sub_view);
}

#[derive(Debug)]
pub(crate) enum ColumnarValueRef<'a> {
    Scalar(&'a [u8]),
    NullableArray(&'a StringArray),
    NonNullableArray(&'a StringArray),
    NullableLargeStringArray(&'a LargeStringArray),
    NonNullableLargeStringArray(&'a LargeStringArray),
    NullableStringViewArray(&'a StringViewArray),
    NonNullableStringViewArray(&'a StringViewArray),
    NullableBinaryArray(&'a BinaryArray),
    NonNullableBinaryArray(&'a BinaryArray),
}

impl ColumnarValueRef<'_> {
    #[inline]
    pub fn is_valid(&self, i: usize) -> bool {
        match &self {
            Self::Scalar(_)
            | Self::NonNullableArray(_)
            | Self::NonNullableLargeStringArray(_)
            | Self::NonNullableStringViewArray(_)
            | Self::NonNullableBinaryArray(_) => true,
            Self::NullableArray(array) => array.is_valid(i),
            Self::NullableStringViewArray(array) => array.is_valid(i),
            Self::NullableLargeStringArray(array) => array.is_valid(i),
            Self::NullableBinaryArray(array) => array.is_valid(i),
        }
    }

    #[inline]
    pub fn nulls(&self) -> Option<NullBuffer> {
        match &self {
            Self::Scalar(_)
            | Self::NonNullableArray(_)
            | Self::NonNullableStringViewArray(_)
            | Self::NonNullableLargeStringArray(_)
            | Self::NonNullableBinaryArray(_) => None,
            Self::NullableArray(array) => array.nulls().cloned(),
            Self::NullableStringViewArray(array) => array.nulls().cloned(),
            Self::NullableLargeStringArray(array) => array.nulls().cloned(),
            Self::NullableBinaryArray(array) => array.nulls().cloned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "capacity integer overflow")]
    fn test_overflow_concat_string_builder() {
        let _builder = ConcatStringBuilder::with_capacity(usize::MAX, usize::MAX);
    }

    #[test]
    #[should_panic(expected = "capacity integer overflow")]
    fn test_overflow_concat_large_string_builder() {
        let _builder = ConcatLargeStringBuilder::with_capacity(usize::MAX, usize::MAX);
    }

    #[test]
    fn string_array_builder_empty() {
        let builder = GenericStringArrayBuilder::<i32>::with_capacity(0, 0);
        let array = builder.finish(None).unwrap();
        assert_eq!(array.len(), 0);
    }

    #[test]
    fn string_array_builder_no_nulls() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(3, 16);
        builder.append_value("foo");
        builder.append_value("");
        builder.append_value("hello world");
        let array = builder.finish(None).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), "foo");
        assert_eq!(array.value(1), "");
        assert_eq!(array.value(2), "hello world");
        assert_eq!(array.null_count(), 0);
    }

    #[test]
    fn string_array_builder_with_nulls() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(3, 8);
        builder.append_value("a");
        builder.append_placeholder();
        builder.append_value("c");
        let nulls = NullBuffer::from(vec![true, false, true]);
        let array = builder.finish(Some(nulls)).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), "a");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "c");
    }

    #[test]
    fn string_array_builder_null_buffer_length_mismatch() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(2, 4);
        builder.append_value("a");
        builder.append_value("b");
        let nulls = NullBuffer::from(vec![true, false, true]);
        assert!(builder.finish(Some(nulls)).is_err());
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "placeholder rows")]
    fn string_array_builder_placeholder_without_null_mask() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(2, 4);
        builder.append_value("a");
        builder.append_placeholder();
        // Slot 1 is a placeholder but the null buffer doesn't mark it null.
        let nulls = NullBuffer::from(vec![true, true]);
        let _ = builder.finish(Some(nulls));
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "placeholder rows")]
    fn string_array_builder_placeholder_with_none_null_buffer() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(1, 4);
        builder.append_placeholder();
        let _ = builder.finish(None);
    }

    #[test]
    fn string_array_builder_all_placeholders() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(3, 0);
        builder.append_placeholder();
        builder.append_placeholder();
        builder.append_placeholder();
        let nulls = NullBuffer::from(vec![false, false, false]);
        let array = builder.finish(Some(nulls)).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 3);
        assert!((0..3).all(|i| array.is_null(i)));
    }

    #[test]
    fn string_array_builder_append_with_basic() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(4, 32);
        builder.append_with(|w| w.write_str("hello"));
        builder.append_placeholder();
        builder.append_with(|w| {
            w.write_str("hello ");
            w.write_str("world");
        });
        builder.append_with(|_w| {});
        let nulls = NullBuffer::from(vec![true, false, true, true]);
        let array = builder.finish(Some(nulls)).unwrap();
        assert_eq!(array.len(), 4);
        assert_eq!(array.value(0), "hello");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "hello world");
        assert_eq!(array.value(3), "");
    }

    #[test]
    fn string_array_builder_append_with_chars() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(2, 16);
        builder.append_with(|w| {
            w.write_char('h');
            w.write_char('é');
            w.write_char('!');
        });
        builder.append_with(|w| w.write_char('x'));
        let array = builder.finish(None).unwrap();
        assert_eq!(array.value(0), "hé!");
        assert_eq!(array.value(1), "x");
    }

    #[test]
    fn string_array_builder_append_byte_map() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(3, 32);
        // SAFETY: ASCII inputs and ASCII outputs.
        unsafe {
            builder.append_byte_map(b"hello", |b| b.to_ascii_uppercase());
            builder.append_byte_map(b"abcab", |b| if b == b'b' { b'X' } else { b });
            builder.append_byte_map(b"", |b| b);
        }
        let array = builder.finish(None).unwrap();
        assert_eq!(array.value(0), "HELLO");
        assert_eq!(array.value(1), "aXcaX");
        assert_eq!(array.value(2), "");
    }

    #[test]
    fn large_string_array_builder_with_nulls() {
        let mut builder = GenericStringArrayBuilder::<i64>::with_capacity(3, 8);
        builder.append_value("a");
        builder.append_placeholder();
        builder.append_value("c");
        let nulls = NullBuffer::from(vec![true, false, true]);
        let array = builder.finish(Some(nulls)).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), "a");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "c");
    }

    #[test]
    fn string_view_array_builder_empty() {
        let builder = StringViewArrayBuilder::with_capacity(0);
        let array = builder.finish(None).unwrap();
        assert_eq!(array.len(), 0);
    }

    #[test]
    fn string_view_array_builder_inline_and_buffer() {
        let mut builder = StringViewArrayBuilder::with_capacity(3);
        builder.append_value("short"); // ≤ 12 bytes, inline
        builder.append_value("a string longer than twelve bytes");
        builder.append_value("");
        let array = builder.finish(None).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), "short");
        assert_eq!(array.value(1), "a string longer than twelve bytes");
        assert_eq!(array.value(2), "");
    }

    #[test]
    fn string_view_array_builder_with_nulls() {
        let mut builder = StringViewArrayBuilder::with_capacity(4);
        builder.append_value("a string longer than twelve bytes");
        builder.append_placeholder();
        builder.append_value("short");
        builder.append_placeholder();
        let nulls = NullBuffer::from(vec![true, false, true, false]);
        let array = builder.finish(Some(nulls)).unwrap();
        assert_eq!(array.len(), 4);
        assert_eq!(array.value(0), "a string longer than twelve bytes");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "short");
        assert!(array.is_null(3));
    }

    #[test]
    fn string_view_array_builder_all_placeholders() {
        let mut builder = StringViewArrayBuilder::with_capacity(3);
        builder.append_placeholder();
        builder.append_placeholder();
        builder.append_placeholder();
        let nulls = NullBuffer::from(vec![false, false, false]);
        let array = builder.finish(Some(nulls)).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 3);
        assert!((0..3).all(|i| array.is_null(i)));
    }

    #[test]
    fn string_view_array_builder_null_buffer_length_mismatch() {
        let mut builder = StringViewArrayBuilder::with_capacity(2);
        builder.append_value("a");
        builder.append_value("b");
        let nulls = NullBuffer::from(vec![true, false, true]);
        assert!(builder.finish(Some(nulls)).is_err());
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "placeholder rows")]
    fn string_view_array_builder_placeholder_without_null_mask() {
        let mut builder = StringViewArrayBuilder::with_capacity(2);
        builder.append_value("a");
        builder.append_placeholder();
        let nulls = NullBuffer::from(vec![true, true]);
        let _ = builder.finish(Some(nulls));
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "placeholder rows")]
    fn string_view_array_builder_placeholder_with_none_null_buffer() {
        let mut builder = StringViewArrayBuilder::with_capacity(1);
        builder.append_placeholder();
        let _ = builder.finish(None);
    }

    #[test]
    fn string_view_array_builder_append_with_inline() {
        // Rows that stay ≤ 12 bytes never touch the data block.
        let mut builder = StringViewArrayBuilder::with_capacity(4);
        let inputs = ["hello", "world!", "", "0123456789ab"];
        for s in &inputs {
            builder.append_with(|w| w.write_str(s));
        }
        let array = builder.finish(None).unwrap();
        assert_eq!(array.len(), inputs.len());
        for (i, s) in inputs.iter().enumerate() {
            assert_eq!(array.value(i), *s);
        }
        assert_eq!(array.data_buffers().len(), 0);
    }

    #[test]
    fn string_view_array_builder_append_byte_map() {
        let mut builder = StringViewArrayBuilder::with_capacity(4);
        // SAFETY: ASCII inputs and ASCII outputs in every call.
        unsafe {
            builder.append_byte_map(b"hello", |b| b.to_ascii_uppercase());
            builder.append_byte_map(b"a long string of 25 bytes", |b| {
                if b == b' ' { b'_' } else { b }
            });
            // 12 bytes — exactly at the inline boundary.
            builder.append_byte_map(b"abcdefghijkl", |b| b);
            builder.append_byte_map(b"", |b| b);
        }
        let array = builder.finish(None).unwrap();
        assert_eq!(array.value(0), "HELLO");
        assert_eq!(array.value(1), "a_long_string_of_25_bytes");
        assert_eq!(array.value(2), "abcdefghijkl");
        assert_eq!(array.value(3), "");
        assert_eq!(array.data_buffers().len(), 1);
        assert_eq!(array.data_buffers()[0].len(), 25);
    }

    #[test]
    fn string_view_array_builder_append_with_at_inline_boundary() {
        // Building exactly 12 bytes via several writes should still go inline.
        let mut builder = StringViewArrayBuilder::with_capacity(2);
        builder.append_with(|w| {
            w.write_str("hello");
            w.write_str(" world!");
        });
        builder.append_with(|w| {
            for _ in 0..6 {
                w.write_str("ab");
            }
        });
        let array = builder.finish(None).unwrap();
        assert_eq!(array.value(0), "hello world!");
        assert_eq!(array.value(1), "abababababab");
        assert_eq!(array.data_buffers().len(), 0);
    }

    #[test]
    fn string_view_array_builder_append_with_spill_on_overflow() {
        // 12 bytes from one write, +1 byte from another → spill at boundary.
        let mut builder = StringViewArrayBuilder::with_capacity(1);
        builder.append_with(|w| {
            w.write_str("hello world!");
            w.write_str("X");
        });
        let array = builder.finish(None).unwrap();
        assert_eq!(array.value(0), "hello world!X");
        assert_eq!(array.data_buffers().len(), 1);
        assert_eq!(array.data_buffers()[0].len(), 13);
    }

    #[test]
    fn string_view_array_builder_append_with_long_single_write() {
        // A single write larger than 12 bytes spills immediately with an
        // empty inline_buf prefix.
        let mut builder = StringViewArrayBuilder::with_capacity(1);
        builder.append_with(|w| w.write_str("a long string of 25 bytes"));
        let array = builder.finish(None).unwrap();
        assert_eq!(array.value(0), "a long string of 25 bytes");
        assert_eq!(array.data_buffers().len(), 1);
        assert_eq!(array.data_buffers()[0].len(), 25);
    }

    #[test]
    fn string_view_array_builder_append_with_many_small_writes_spilling() {
        // 30 × "ab" (60 bytes total): first 6 fit inline, remainder spills.
        let mut builder = StringViewArrayBuilder::with_capacity(1);
        builder.append_with(|w| {
            for _ in 0..30 {
                w.write_str("ab");
            }
        });
        let array = builder.finish(None).unwrap();
        assert_eq!(array.value(0), "ab".repeat(30));
        assert_eq!(array.data_buffers().len(), 1);
        assert_eq!(array.data_buffers()[0].len(), 60);
    }

    #[test]
    fn string_view_array_builder_append_with_chars() {
        // write_char with multi-byte UTF-8: row 0 stays inline (3 bytes),
        // row 1 spills (40 bytes).
        let mut builder = StringViewArrayBuilder::with_capacity(2);
        builder.append_with(|w| {
            w.write_char('é');
            w.write_char('!');
        });
        builder.append_with(|w| {
            for _ in 0..10 {
                w.write_char('🦀');
            }
        });
        let array = builder.finish(None).unwrap();
        assert_eq!(array.value(0), "é!");
        assert_eq!(array.value(1), "🦀".repeat(10));
    }

    #[test]
    fn string_view_array_builder_append_with_empty_row() {
        let mut builder = StringViewArrayBuilder::with_capacity(2);
        builder.append_with(|_w| {});
        builder.append_with(|w| w.write_str("nonempty"));
        let array = builder.finish(None).unwrap();
        assert_eq!(array.value(0), "");
        assert_eq!(array.value(1), "nonempty");
        assert_eq!(array.data_buffers().len(), 0);
    }

    #[test]
    fn string_view_array_builder_append_with_block_rotation() {
        // 40 long rows, 500 bytes each, exceeds the first doubled block
        // (~16 KiB). Forces the builder to rotate blocks between rows.
        const STR_LEN: usize = 500;
        const N: usize = 40;
        let s = "x".repeat(STR_LEN);
        let mut builder = StringViewArrayBuilder::with_capacity(N);
        for _ in 0..N {
            builder.append_with(|w| w.write_str(&s));
        }
        let array = builder.finish(None).unwrap();
        assert_eq!(array.len(), N);
        assert!(
            array.data_buffers().len() >= 2,
            "expected multiple data buffers, got {}",
            array.data_buffers().len()
        );
        let total: usize = array.data_buffers().iter().map(|b| b.len()).sum();
        assert_eq!(total, N * STR_LEN);
        for i in 0..N {
            assert_eq!(array.value(i), s);
        }
    }

    #[test]
    fn string_view_array_builder_append_with_mixed_with_placeholders() {
        let mut builder = StringViewArrayBuilder::with_capacity(4);
        builder.append_with(|w| w.write_str("hello"));
        builder.append_placeholder();
        builder.append_with(|w| w.write_str("a long string of 25 bytes"));
        builder.append_with(|_w| {});
        let nulls = NullBuffer::from(vec![true, false, true, true]);
        let array = builder.finish(Some(nulls)).unwrap();
        assert_eq!(array.value(0), "hello");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "a long string of 25 bytes");
        assert_eq!(array.value(3), "");
    }

    #[test]
    fn string_view_array_builder_flushes_full_blocks() {
        // Each value is 300 bytes. The first data block is 2 × STRING_VIEW_INIT_BLOCK_SIZE
        // = 16 KiB, so ~50 values saturate it and the rest spill into additional
        // blocks.
        let value = "x".repeat(300);
        let mut builder = StringViewArrayBuilder::with_capacity(100);
        for _ in 0..100 {
            builder.append_value(&value);
        }
        let array = builder.finish(None).unwrap();
        assert_eq!(array.len(), 100);
        assert!(
            array.data_buffers().len() > 1,
            "expected multiple data buffers, got {}",
            array.data_buffers().len()
        );
        for i in 0..100 {
            assert_eq!(array.value(i), value);
        }
    }

    /// Build an array via `BulkNullStringArrayBuilder` to verify that the
    /// trait methods produce the same result as the inherent methods.
    fn build_via_trait<B: BulkNullStringArrayBuilder>(
        mut builder: B,
        nulls: Option<NullBuffer>,
    ) -> ArrayRef {
        builder.append_value("a");
        builder.append_placeholder();
        builder.append_value("hello world!");
        builder.finish(nulls).unwrap()
    }

    #[test]
    fn bulk_null_trait_string_i32() {
        let builder = GenericStringArrayBuilder::<i32>::with_capacity(3, 16);
        let nulls = NullBuffer::from(vec![true, false, true]);
        let array = build_via_trait(builder, Some(nulls));
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), "a");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "hello world!");
    }

    #[test]
    fn bulk_null_trait_string_i64() {
        let builder = GenericStringArrayBuilder::<i64>::with_capacity(3, 16);
        let nulls = NullBuffer::from(vec![true, false, true]);
        let array = build_via_trait(builder, Some(nulls));
        let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), "a");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "hello world!");
    }

    #[test]
    fn bulk_null_trait_string_view() {
        let builder = StringViewArrayBuilder::with_capacity(3);
        let nulls = NullBuffer::from(vec![true, false, true]);
        let array = build_via_trait(builder, Some(nulls));
        let array = array.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.value(0), "a");
        assert!(array.is_null(1));
        assert_eq!(array.value(2), "hello world!");
    }

    #[test]
    fn bulk_null_trait_no_nulls() {
        let mut builder = GenericStringArrayBuilder::<i32>::with_capacity(2, 8);
        BulkNullStringArrayBuilder::append_value(&mut builder, "x");
        BulkNullStringArrayBuilder::append_value(&mut builder, "yy");
        let array = BulkNullStringArrayBuilder::finish(builder, None).unwrap();
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.value(0), "x");
        assert_eq!(array.value(1), "yy");
        assert_eq!(array.null_count(), 0);
    }

    /// Exercise `append_with` through the trait against both builder types
    /// to confirm GAT-based dispatch works.
    fn build_with_append_with<B: BulkNullStringArrayBuilder>(mut builder: B) -> ArrayRef {
        builder.append_with(|w| w.write_str("hello"));
        builder.append_with(|w| {
            w.write_str("hello ");
            w.write_str("world!");
        });
        builder.finish(None).unwrap()
    }

    #[test]
    fn bulk_null_trait_append_with_string() {
        let builder = GenericStringArrayBuilder::<i32>::with_capacity(2, 32);
        let array = build_with_append_with(builder);
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array.value(0), "hello");
        assert_eq!(array.value(1), "hello world!");
    }

    #[test]
    fn bulk_null_trait_append_with_string_view() {
        let builder = StringViewArrayBuilder::with_capacity(2);
        let array = build_with_append_with(builder);
        let array = array.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(array.value(0), "hello");
        assert_eq!(array.value(1), "hello world!");
    }
}
