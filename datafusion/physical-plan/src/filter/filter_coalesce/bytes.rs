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

use std::sync::Arc;

use arrow::{
    array::{ArrayDataBuilder, AsArray},
    compute::SlicesIterator,
    datatypes::{ByteArrayType, GenericBinaryType, GenericStringType},
};
use arrow_array::{
    Array, ArrayRef, GenericBinaryArray, GenericByteArray, GenericStringArray,
    OffsetSizeTrait,
};
use arrow_buffer::{
    bit_util, ArrowNativeType, BooleanBuffer, BooleanBufferBuilder, Buffer,
    BufferBuilder, MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer,
};
use arrow_schema::DataType;
use datafusion_common::Result;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls;
use datafusion_physical_expr::binary_map::OutputType;
use datafusion_physical_expr_common::binary_map::INITIAL_BUFFER_CAPACITY;

use super::{
    FilterCoalescer, FilterPredicate, IndexIterator, IterationStrategy,
    MaybeNullBufferBuilder,
};

pub struct ByteFilterBuilder<O>
where
    O: OffsetSizeTrait,
{
    output_type: OutputType,
    buffer: BufferBuilder<u8>,
    /// Offsets into `buffer` for each distinct value. These offsets as used
    /// directly to create the final `GenericBinaryArray`. The `i`th string is
    /// stored in the range `offsets[i]..offsets[i+1]` in `buffer`. Null values
    /// are stored as a zero length string.
    offsets: Vec<O>,
    nulls: MaybeNullBufferBuilder,
}

impl<O> ByteFilterBuilder<O>
where
    O: OffsetSizeTrait,
{
    pub fn new(output_type: OutputType) -> Self {
        Self {
            output_type,
            buffer: BufferBuilder::new(INITIAL_BUFFER_CAPACITY),
            offsets: vec![O::default()],
            nulls: MaybeNullBufferBuilder::new(),
        }
    }

    fn append_filtered_array<B>(
        &mut self,
        array: &ArrayRef,
        predicate: &FilterPredicate,
    ) -> Result<()>
    where
        B: ByteArrayType,
    {
        let arr = array.as_bytes::<B>();
        match &predicate.strategy {
            IterationStrategy::SlicesIterator => {
                for (start, end) in SlicesIterator::new(&predicate.filter) {
                    for row in start..end {
                        if arr.is_null(row) {
                            self.nulls.append(true);
                            // nulls need a zero length in the offset buffer
                            let offset = self.buffer.len();
                            self.offsets.push(O::usize_as(offset));
                        } else {
                            self.nulls.append(false);
                            self.do_append_val_inner(arr, row);
                        }
                    }
                }
            }
            IterationStrategy::Slices(slices) => {
                for (start, end) in slices {
                    for row in *start..*end {
                        if arr.is_null(row) {
                            self.nulls.append(true);
                            // nulls need a zero length in the offset buffer
                            let offset = self.buffer.len();
                            self.offsets.push(O::usize_as(offset));
                        } else {
                            self.nulls.append(false);
                            self.do_append_val_inner(arr, row);
                        }
                    }
                }
            }
            IterationStrategy::IndexIterator => {
                for row in IndexIterator::new(&predicate.filter, predicate.count) {
                    if arr.is_null(row) {
                        self.nulls.append(true);
                        // nulls need a zero length in the offset buffer
                        let offset = self.buffer.len();
                        self.offsets.push(O::usize_as(offset));
                    } else {
                        self.nulls.append(false);
                        self.do_append_val_inner(arr, row);
                    }
                }
            }
            IterationStrategy::Indices(indices) => {
                for row in indices.iter() {
                    let row = *row;
                    if arr.is_null(row) {
                        self.nulls.append(true);
                        // nulls need a zero length in the offset buffer
                        let offset = self.buffer.len();
                        self.offsets.push(O::usize_as(offset));
                    } else {
                        self.nulls.append(false);
                        self.do_append_val_inner(arr, row);
                    }
                }
            }
            IterationStrategy::None => {}
            IterationStrategy::All => {
                for row in 0..arr.len() {
                    if arr.is_null(row) {
                        self.nulls.append(true);
                        // nulls need a zero length in the offset buffer
                        let offset = self.buffer.len();
                        self.offsets.push(O::usize_as(offset));
                    } else {
                        self.nulls.append(false);
                        self.do_append_val_inner(arr, row);
                    }
                }
            }
        }

        Ok(())
    }

    fn do_append_val_inner<B>(&mut self, array: &GenericByteArray<B>, row: usize)
    where
        B: ByteArrayType,
    {
        let value: &[u8] = array.value(row).as_ref();
        self.buffer.append_slice(value);
        self.offsets.push(O::usize_as(self.buffer.len()));
    }
}

impl<O> FilterCoalescer for ByteFilterBuilder<O>
where
    O: OffsetSizeTrait,
{
    fn append_filtered_array(
        &mut self,
        array: &ArrayRef,
        predicate: &FilterPredicate,
    ) -> Result<()> {
        // Sanity array type
        match self.output_type {
            OutputType::Binary => {
                debug_assert!(matches!(
                    array.data_type(),
                    DataType::Binary | DataType::LargeBinary
                ));
                self.append_filtered_array::<GenericBinaryType<O>>(array, predicate)
            }
            OutputType::Utf8 => {
                debug_assert!(matches!(
                    array.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8
                ));
                self.append_filtered_array::<GenericStringType<O>>(array, predicate)
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
    }

    fn row_count(&self) -> usize {
        self.offsets.len() - 1
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let Self {
            output_type,
            mut buffer,
            offsets,
            nulls,
        } = *self;

        let null_buffer = nulls.build();

        // SAFETY: the offsets were constructed correctly in `insert_if_new` --
        // monotonically increasing, overflows were checked.
        let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };
        let values = buffer.finish();
        match output_type {
            OutputType::Binary => {
                // SAFETY: the offsets were constructed correctly
                Arc::new(unsafe {
                    GenericBinaryArray::new_unchecked(offsets, values, null_buffer)
                })
            }
            OutputType::Utf8 => {
                // SAFETY:
                // 1. the offsets were constructed safely
                //
                // 2. the input arrays were all the correct type and thus since
                // all the values that went in were valid (e.g. utf8) so are all
                // the values that come out
                Arc::new(unsafe {
                    GenericStringArray::new_unchecked(offsets, values, null_buffer)
                })
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
    }
}

/// [`FilterBytes`] is created from a source [`GenericByteArray`] and can be
/// used to build a new [`GenericByteArray`] by copying values from the source
///
/// TODO(raphael): Could this be used for the take kernel as well?
struct FilterBytes<'a, OffsetSize> {
    src_offsets: &'a [OffsetSize],
    src_values: &'a [u8],
    dst_offsets: MutableBuffer,
    dst_values: MutableBuffer,
    cur_offset: OffsetSize,
}

impl<'a, OffsetSize> FilterBytes<'a, OffsetSize>
where
    OffsetSize: OffsetSizeTrait,
{
    fn new<T>(capacity: usize, array: &'a GenericByteArray<T>) -> Self
    where
        T: ByteArrayType<Offset = OffsetSize>,
    {
        let num_offsets_bytes = (capacity + 1) * size_of::<OffsetSize>();
        let mut dst_offsets = MutableBuffer::new(num_offsets_bytes);
        let dst_values = MutableBuffer::new(0);
        let cur_offset = OffsetSize::from_usize(0).unwrap();
        dst_offsets.push(cur_offset);

        Self {
            src_offsets: array.value_offsets(),
            src_values: array.value_data(),
            dst_offsets,
            dst_values,
            cur_offset,
        }
    }

    /// Returns the byte offset at `idx`
    #[inline]
    fn get_value_offset(&self, idx: usize) -> usize {
        self.src_offsets[idx].as_usize()
    }

    /// Returns the start and end of the value at index `idx` along with its length
    #[inline]
    fn get_value_range(&self, idx: usize) -> (usize, usize, OffsetSize) {
        // These can only fail if `array` contains invalid data
        let start = self.get_value_offset(idx);
        let end = self.get_value_offset(idx + 1);
        let len = OffsetSize::from_usize(end - start).expect("illegal offset range");
        (start, end, len)
    }

    /// Extends the in-progress array by the indexes in the provided iterator
    fn extend_idx(&mut self, iter: impl Iterator<Item = usize>) {
        for idx in iter {
            let (start, end, len) = self.get_value_range(idx);
            self.cur_offset += len;
            self.dst_offsets.push(self.cur_offset);
            self.dst_values
                .extend_from_slice(&self.src_values[start..end]);
        }
    }

    /// Extends the in-progress array by the ranges in the provided iterator
    fn extend_slices(&mut self, iter: impl Iterator<Item = (usize, usize)>) {
        for (start, end) in iter {
            // These can only fail if `array` contains invalid data
            for idx in start..end {
                let (_, _, len) = self.get_value_range(idx);
                self.cur_offset += len;
                self.dst_offsets.push(self.cur_offset); // push_unchecked?
            }

            let value_start = self.get_value_offset(start);
            let value_end = self.get_value_offset(end);
            self.dst_values
                .extend_from_slice(&self.src_values[value_start..value_end]);
        }
    }
}

/// `filter` implementation for byte arrays
///
/// Note: NULLs with a non-zero slot length in `array` will have the corresponding
/// data copied across. This allows handling the null mask separately from the data
fn filter_bytes<T>(
    array: &GenericByteArray<T>,
    predicate: &FilterPredicate,
) -> GenericByteArray<T>
where
    T: ByteArrayType,
{
    let src_offsets = array.value_offsets();
    let src_values = array.value_data();

    let mut filter = FilterBytes::new(predicate.count, array);

    match &predicate.strategy {
        IterationStrategy::SlicesIterator => {
            filter.extend_slices(SlicesIterator::new(&predicate.filter))
        }
        IterationStrategy::Slices(slices) => filter.extend_slices(slices.iter().cloned()),
        IterationStrategy::IndexIterator => {
            filter.extend_idx(IndexIterator::new(&predicate.filter, predicate.count))
        }
        IterationStrategy::Indices(indices) => filter.extend_idx(indices.iter().cloned()),
        IterationStrategy::All | IterationStrategy::None => unreachable!(),
    }

    let mut builder = ArrayDataBuilder::new(T::DATA_TYPE)
        .len(predicate.count)
        .add_buffer(filter.dst_offsets.into())
        .add_buffer(filter.dst_values.into());

    if let Some((null_count, nulls)) = filter_null_mask(array.nulls(), predicate) {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    let data = unsafe { builder.build_unchecked() };
    GenericByteArray::from(data)
}

/// Computes a new null mask for `data` based on `predicate`
///
/// If the predicate selected no null-rows, returns `None`, otherwise returns
/// `Some((null_count, null_buffer))` where `null_count` is the number of nulls
/// in the filtered output, and `null_buffer` is the filtered null buffer
///
fn filter_null_mask(
    nulls: Option<&NullBuffer>,
    predicate: &FilterPredicate,
) -> Option<(usize, Buffer)> {
    let nulls = nulls?;
    if nulls.null_count() == 0 {
        return None;
    }

    let nulls = filter_bits(nulls.inner(), predicate);
    // The filtered `nulls` has a length of `predicate.count` bits and
    // therefore the null count is this minus the number of valid bits
    let null_count = predicate.count - nulls.count_set_bits_offset(0, predicate.count);

    if null_count == 0 {
        return None;
    }

    Some((null_count, nulls))
}

// Filter the packed bitmask `buffer`, with `predicate` starting at bit offset `offset`
fn filter_bits(buffer: &BooleanBuffer, predicate: &FilterPredicate) -> Buffer {
    let src = buffer.values();
    let offset = buffer.offset();

    match &predicate.strategy {
        IterationStrategy::IndexIterator => {
            let bits = IndexIterator::new(&predicate.filter, predicate.count)
                .map(|src_idx| bit_util::get_bit(src, src_idx + offset));

            // SAFETY: `IndexIterator` reports its size correctly
            unsafe { MutableBuffer::from_trusted_len_iter_bool(bits).into() }
        }
        IterationStrategy::Indices(indices) => {
            let bits = indices
                .iter()
                .map(|src_idx| bit_util::get_bit(src, *src_idx + offset));

            // SAFETY: `Vec::iter()` reports its size correctly
            unsafe { MutableBuffer::from_trusted_len_iter_bool(bits).into() }
        }
        IterationStrategy::SlicesIterator => {
            let mut builder =
                BooleanBufferBuilder::new(bit_util::ceil(predicate.count, 8));
            for (start, end) in SlicesIterator::new(&predicate.filter) {
                builder.append_packed_range(start + offset..end + offset, src)
            }
            builder.into()
        }
        IterationStrategy::Slices(slices) => {
            let mut builder =
                BooleanBufferBuilder::new(bit_util::ceil(predicate.count, 8));
            for (start, end) in slices {
                builder.append_packed_range(*start + offset..*end + offset, src)
            }
            builder.into()
        }
        IterationStrategy::All | IterationStrategy::None => unreachable!(),
    }
}
