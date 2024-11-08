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

use arrow::array::make_view;
use arrow::array::BufferBuilder;
use arrow::array::ByteView;
use arrow::array::GenericBinaryArray;
use arrow::array::GenericStringArray;
use arrow::array::OffsetSizeTrait;
use arrow::array::PrimitiveArray;
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray};
use arrow::buffer::OffsetBuffer;
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::ByteArrayType;
use arrow::datatypes::ByteViewType;
use arrow::datatypes::DataType;
use arrow::datatypes::GenericBinaryType;
use arrow_array::GenericByteArray;
use arrow_array::GenericByteViewArray;
use arrow_buffer::Buffer;
use datafusion_common::utils::proxy::VecAllocExt;
use itertools::izip;

use crate::aggregates::group_values::null_builder::MaybeNullBufferBuilder;
use arrow_array::types::GenericStringType;
use datafusion_physical_expr_common::binary_map::{OutputType, INITIAL_BUFFER_CAPACITY};
use std::iter;
use std::marker::PhantomData;
use std::mem::{replace, size_of};
use std::sync::Arc;
use std::vec;

const BYTE_VIEW_MAX_BLOCK_SIZE: usize = 2 * 1024 * 1024;

/// Trait for storing a single column of group values in [`GroupValuesColumn`]
///
/// Implementations of this trait store an in-progress collection of group values
/// (similar to various builders in Arrow-rs) that allow for quick comparison to
/// incoming rows.
///
/// [`GroupValuesColumn`]: crate::aggregates::group_values::GroupValuesColumn
pub trait GroupColumn: Send + Sync {
    /// Returns equal if the row stored in this builder at `lhs_row` is equal to
    /// the row in `array` at `rhs_row`
    ///
    /// Note that this comparison returns true if both elements are NULL
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool;

    /// Appends the row at `row` in `array` to this builder
    fn append_val(&mut self, array: &ArrayRef, row: usize);

    /// The vectorized version equal to
    ///
    /// When found nth row stored in this builder at `lhs_row`
    /// is equal to the row in `array` at `rhs_row`,
    /// it will record the `true` result at the corresponding
    /// position in `equal_to_results`.
    ///
    /// And if found nth result in `equal_to_results` is already
    /// `false`, the check for nth row will be skipped.
    ///
    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    );

    /// The vectorized version `append_val`
    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]);

    /// Returns the number of rows stored in this builder
    fn len(&self) -> usize;

    /// Returns the number of bytes used by this [`GroupColumn`]
    fn size(&self) -> usize;

    /// Builds a new array from all of the stored rows
    fn build(self: Box<Self>) -> ArrayRef;

    /// Builds a new array from the first `n` stored rows, shifting the
    /// remaining rows to the start of the builder
    fn take_n(&mut self, n: usize) -> ArrayRef;
}

/// An implementation of [`GroupColumn`] for primitive values
///
/// Optimized to skip null buffer construction if the input is known to be non nullable
///
/// # Template parameters
///
/// `T`: the native Rust type that stores the data
/// `NULLABLE`: if the data can contain any nulls
#[derive(Debug)]
pub struct PrimitiveGroupValueBuilder<T: ArrowPrimitiveType, const NULLABLE: bool> {
    group_values: Vec<T::Native>,
    nulls: MaybeNullBufferBuilder,
}

impl<T, const NULLABLE: bool> PrimitiveGroupValueBuilder<T, NULLABLE>
where
    T: ArrowPrimitiveType,
{
    /// Create a new `PrimitiveGroupValueBuilder`
    pub fn new() -> Self {
        Self {
            group_values: vec![],
            nulls: MaybeNullBufferBuilder::new(),
        }
    }
}

impl<T: ArrowPrimitiveType, const NULLABLE: bool> GroupColumn
    for PrimitiveGroupValueBuilder<T, NULLABLE>
{
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        // Perf: skip null check (by short circuit) if input is not nullable
        if NULLABLE {
            let exist_null = self.nulls.is_null(lhs_row);
            let input_null = array.is_null(rhs_row);
            if let Some(result) = nulls_equal_to(exist_null, input_null) {
                return result;
            }
            // Otherwise, we need to check their values
        }

        self.group_values[lhs_row] == array.as_primitive::<T>().value(rhs_row)
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) {
        // Perf: skip null check if input can't have nulls
        if NULLABLE {
            if array.is_null(row) {
                self.nulls.append(true);
                self.group_values.push(T::default_value());
            } else {
                self.nulls.append(false);
                self.group_values.push(array.as_primitive::<T>().value(row));
            }
        } else {
            self.group_values.push(array.as_primitive::<T>().value(row));
        }
    }

    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        let array = array.as_primitive::<T>();

        let iter = izip!(
            lhs_rows.iter(),
            rhs_rows.iter(),
            equal_to_results.iter_mut(),
        );

        for (&lhs_row, &rhs_row, equal_to_result) in iter {
            // Has found not equal to in previous column, don't need to check
            if !*equal_to_result {
                continue;
            }

            // Perf: skip null check (by short circuit) if input is not nullable
            if NULLABLE {
                let exist_null = self.nulls.is_null(lhs_row);
                let input_null = array.is_null(rhs_row);
                if let Some(result) = nulls_equal_to(exist_null, input_null) {
                    *equal_to_result = result;
                    continue;
                }
                // Otherwise, we need to check their values
            }

            *equal_to_result = self.group_values[lhs_row] == array.value(rhs_row);
        }
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) {
        let arr = array.as_primitive::<T>();

        let null_count = array.null_count();
        let num_rows = array.len();
        let all_null_or_non_null = if null_count == 0 {
            Some(true)
        } else if null_count == num_rows {
            Some(false)
        } else {
            None
        };

        match (NULLABLE, all_null_or_non_null) {
            (true, None) => {
                for &row in rows {
                    if array.is_null(row) {
                        self.nulls.append(true);
                        self.group_values.push(T::default_value());
                    } else {
                        self.nulls.append(false);
                        self.group_values.push(arr.value(row));
                    }
                }
            }

            (true, Some(true)) => {
                self.nulls.append_n(rows.len(), false);
                for &row in rows {
                    self.group_values.push(arr.value(row));
                }
            }

            (true, Some(false)) => {
                self.nulls.append_n(rows.len(), true);
                self.group_values
                    .extend(iter::repeat(T::default_value()).take(rows.len()));
            }

            (false, _) => {
                for &row in rows {
                    self.group_values.push(arr.value(row));
                }
            }
        }
    }

    fn len(&self) -> usize {
        self.group_values.len()
    }

    fn size(&self) -> usize {
        self.group_values.allocated_size() + self.nulls.allocated_size()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let Self {
            group_values,
            nulls,
        } = *self;

        let nulls = nulls.build();
        if !NULLABLE {
            assert!(nulls.is_none(), "unexpected nulls in non nullable input");
        }

        Arc::new(PrimitiveArray::<T>::new(
            ScalarBuffer::from(group_values),
            nulls,
        ))
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        let first_n = self.group_values.drain(0..n).collect::<Vec<_>>();

        let first_n_nulls = if NULLABLE { self.nulls.take_n(n) } else { None };

        Arc::new(PrimitiveArray::<T>::new(
            ScalarBuffer::from(first_n),
            first_n_nulls,
        ))
    }
}

/// An implementation of [`GroupColumn`] for binary and utf8 types.
///
/// Stores a collection of binary or utf8 group values in a single buffer
/// in a way that allows:
///
/// 1. Efficient comparison of incoming rows to existing rows
/// 2. Efficient construction of the final output array
pub struct ByteGroupValueBuilder<O>
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
    /// Nulls
    nulls: MaybeNullBufferBuilder,
}

impl<O> ByteGroupValueBuilder<O>
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

    fn equal_to_inner<B>(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool
    where
        B: ByteArrayType,
    {
        let array = array.as_bytes::<B>();
        self.do_equal_to_inner(lhs_row, array, rhs_row)
    }

    fn append_val_inner<B>(&mut self, array: &ArrayRef, row: usize)
    where
        B: ByteArrayType,
    {
        let arr = array.as_bytes::<B>();
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

    fn vectorized_equal_to_inner<B>(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) where
        B: ByteArrayType,
    {
        let array = array.as_bytes::<B>();

        let iter = izip!(
            lhs_rows.iter(),
            rhs_rows.iter(),
            equal_to_results.iter_mut(),
        );

        for (&lhs_row, &rhs_row, equal_to_result) in iter {
            // Has found not equal to, don't need to check
            if !*equal_to_result {
                continue;
            }

            *equal_to_result = self.do_equal_to_inner(lhs_row, array, rhs_row);
        }
    }

    fn vectorized_append_inner<B>(&mut self, array: &ArrayRef, rows: &[usize])
    where
        B: ByteArrayType,
    {
        let arr = array.as_bytes::<B>();
        let null_count = array.null_count();
        let num_rows = array.len();
        let all_null_or_non_null = if null_count == 0 {
            Some(true)
        } else if null_count == num_rows {
            Some(false)
        } else {
            None
        };

        match all_null_or_non_null {
            None => {
                for &row in rows {
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

            Some(true) => {
                self.nulls.append_n(rows.len(), false);
                for &row in rows {
                    self.do_append_val_inner(arr, row);
                }
            }

            Some(false) => {
                self.nulls.append_n(rows.len(), true);

                let new_len = self.offsets.len() + rows.len();
                let offset = self.buffer.len();
                self.offsets.resize(new_len, O::usize_as(offset));
            }
        }
    }

    fn do_equal_to_inner<B>(
        &self,
        lhs_row: usize,
        array: &GenericByteArray<B>,
        rhs_row: usize,
    ) -> bool
    where
        B: ByteArrayType,
    {
        let exist_null = self.nulls.is_null(lhs_row);
        let input_null = array.is_null(rhs_row);
        if let Some(result) = nulls_equal_to(exist_null, input_null) {
            return result;
        }
        // Otherwise, we need to check their values
        self.value(lhs_row) == (array.value(rhs_row).as_ref() as &[u8])
    }

    fn do_append_val_inner<B>(&mut self, array: &GenericByteArray<B>, row: usize)
    where
        B: ByteArrayType,
    {
        let value: &[u8] = array.value(row).as_ref();
        self.buffer.append_slice(value);
        self.offsets.push(O::usize_as(self.buffer.len()));
    }

    /// return the current value of the specified row irrespective of null
    pub fn value(&self, row: usize) -> &[u8] {
        let l = self.offsets[row].as_usize();
        let r = self.offsets[row + 1].as_usize();
        // Safety: the offsets are constructed correctly and never decrease
        unsafe { self.buffer.as_slice().get_unchecked(l..r) }
    }
}

impl<O> GroupColumn for ByteGroupValueBuilder<O>
where
    O: OffsetSizeTrait,
{
    fn equal_to(&self, lhs_row: usize, column: &ArrayRef, rhs_row: usize) -> bool {
        // Sanity array type
        match self.output_type {
            OutputType::Binary => {
                debug_assert!(matches!(
                    column.data_type(),
                    DataType::Binary | DataType::LargeBinary
                ));
                self.equal_to_inner::<GenericBinaryType<O>>(lhs_row, column, rhs_row)
            }
            OutputType::Utf8 => {
                debug_assert!(matches!(
                    column.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8
                ));
                self.equal_to_inner::<GenericStringType<O>>(lhs_row, column, rhs_row)
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
    }

    fn append_val(&mut self, column: &ArrayRef, row: usize) {
        // Sanity array type
        match self.output_type {
            OutputType::Binary => {
                debug_assert!(matches!(
                    column.data_type(),
                    DataType::Binary | DataType::LargeBinary
                ));
                self.append_val_inner::<GenericBinaryType<O>>(column, row)
            }
            OutputType::Utf8 => {
                debug_assert!(matches!(
                    column.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8
                ));
                self.append_val_inner::<GenericStringType<O>>(column, row)
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        };
    }

    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        // Sanity array type
        match self.output_type {
            OutputType::Binary => {
                debug_assert!(matches!(
                    array.data_type(),
                    DataType::Binary | DataType::LargeBinary
                ));
                self.vectorized_equal_to_inner::<GenericBinaryType<O>>(
                    lhs_rows,
                    array,
                    rhs_rows,
                    equal_to_results,
                );
            }
            OutputType::Utf8 => {
                debug_assert!(matches!(
                    array.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8
                ));
                self.vectorized_equal_to_inner::<GenericStringType<O>>(
                    lhs_rows,
                    array,
                    rhs_rows,
                    equal_to_results,
                );
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
    }

    fn vectorized_append(&mut self, column: &ArrayRef, rows: &[usize]) {
        match self.output_type {
            OutputType::Binary => {
                debug_assert!(matches!(
                    column.data_type(),
                    DataType::Binary | DataType::LargeBinary
                ));
                self.vectorized_append_inner::<GenericBinaryType<O>>(column, rows)
            }
            OutputType::Utf8 => {
                debug_assert!(matches!(
                    column.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8
                ));
                self.vectorized_append_inner::<GenericStringType<O>>(column, rows)
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        };
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn size(&self) -> usize {
        self.buffer.capacity() * size_of::<u8>()
            + self.offsets.allocated_size()
            + self.nulls.allocated_size()
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

    fn take_n(&mut self, n: usize) -> ArrayRef {
        debug_assert!(self.len() >= n);
        let null_buffer = self.nulls.take_n(n);
        let first_remaining_offset = O::as_usize(self.offsets[n]);

        // Given offests like [0, 2, 4, 5] and n = 1, we expect to get
        // offsets [0, 2, 3]. We first create two offsets for first_n as [0, 2] and the remaining as [2, 4, 5].
        // And we shift the offset starting from 0 for the remaining one, [2, 4, 5] -> [0, 2, 3].
        let mut first_n_offsets = self.offsets.drain(0..n).collect::<Vec<_>>();
        let offset_n = *self.offsets.first().unwrap();
        self.offsets
            .iter_mut()
            .for_each(|offset| *offset = offset.sub(offset_n));
        first_n_offsets.push(offset_n);

        // SAFETY: the offsets were constructed correctly in `insert_if_new` --
        // monotonically increasing, overflows were checked.
        let offsets =
            unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(first_n_offsets)) };

        let mut remaining_buffer =
            BufferBuilder::new(self.buffer.len() - first_remaining_offset);
        // TODO: Current approach copy the remaining and truncate the original one
        // Find out a way to avoid copying buffer but split the original one into two.
        remaining_buffer.append_slice(&self.buffer.as_slice()[first_remaining_offset..]);
        self.buffer.truncate(first_remaining_offset);
        let values = self.buffer.finish();
        self.buffer = remaining_buffer;

        match self.output_type {
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
                // 2. we asserted the input arrays were all the correct type and
                // thus since all the values that went in were valid (e.g. utf8)
                // so are all the values that come out
                Arc::new(unsafe {
                    GenericStringArray::new_unchecked(offsets, values, null_buffer)
                })
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
    }
}

/// An implementation of [`GroupColumn`] for binary view and utf8 view types.
///
/// Stores a collection of binary view or utf8 view group values in a buffer
/// whose structure is similar to `GenericByteViewArray`, and we can get benefits:
///
/// 1. Efficient comparison of incoming rows to existing rows
/// 2. Efficient construction of the final output array
/// 3. Efficient to perform `take_n` comparing to use `GenericByteViewBuilder`
pub struct ByteViewGroupValueBuilder<B: ByteViewType> {
    /// The views of string values
    ///
    /// If string len <= 12, the view's format will be:
    ///   string(12B) | len(4B)
    ///
    /// If string len > 12, its format will be:
    ///     offset(4B) | buffer_index(4B) | prefix(4B) | len(4B)
    views: Vec<u128>,

    /// The progressing block
    ///
    /// New values will be inserted into it until its capacity
    /// is not enough(detail can see `max_block_size`).
    in_progress: Vec<u8>,

    /// The completed blocks
    completed: Vec<Buffer>,

    /// The max size of `in_progress`
    ///
    /// `in_progress` will be flushed into `completed`, and create new `in_progress`
    /// when found its remaining capacity(`max_block_size` - `len(in_progress)`),
    /// is no enough to store the appended value.
    ///
    /// Currently it is fixed at 2MB.
    max_block_size: usize,

    /// Nulls
    nulls: MaybeNullBufferBuilder,

    /// phantom data so the type requires `<B>`
    _phantom: PhantomData<B>,
}

impl<B: ByteViewType> ByteViewGroupValueBuilder<B> {
    pub fn new() -> Self {
        Self {
            views: Vec::new(),
            in_progress: Vec::new(),
            completed: Vec::new(),
            max_block_size: BYTE_VIEW_MAX_BLOCK_SIZE,
            nulls: MaybeNullBufferBuilder::new(),
            _phantom: PhantomData {},
        }
    }

    /// Set the max block size
    fn with_max_block_size(mut self, max_block_size: usize) -> Self {
        self.max_block_size = max_block_size;
        self
    }

    fn equal_to_inner(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        let array = array.as_byte_view::<B>();
        self.do_equal_to_inner(lhs_row, array, rhs_row)
    }

    fn append_val_inner(&mut self, array: &ArrayRef, row: usize) {
        let arr = array.as_byte_view::<B>();

        // Null row case, set and return
        if arr.is_null(row) {
            self.nulls.append(true);
            self.views.push(0);
            return;
        }

        // Not null row case
        self.nulls.append(false);
        self.do_append_val_inner(arr, row);
    }

    fn vectorized_equal_to_inner(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        let array = array.as_byte_view::<B>();

        let iter = izip!(
            lhs_rows.iter(),
            rhs_rows.iter(),
            equal_to_results.iter_mut(),
        );

        for (&lhs_row, &rhs_row, equal_to_result) in iter {
            // Has found not equal to, don't need to check
            if !*equal_to_result {
                continue;
            }

            *equal_to_result = self.do_equal_to_inner(lhs_row, array, rhs_row);
        }
    }

    fn vectorized_append_inner(&mut self, array: &ArrayRef, rows: &[usize]) {
        let arr = array.as_byte_view::<B>();
        let null_count = array.null_count();
        let num_rows = array.len();
        let all_null_or_non_null = if null_count == 0 {
            Some(true)
        } else if null_count == num_rows {
            Some(false)
        } else {
            None
        };

        match all_null_or_non_null {
            None => {
                for &row in rows {
                    // Null row case, set and return
                    if arr.is_valid(row) {
                        self.nulls.append(false);
                        self.do_append_val_inner(arr, row);
                    } else {
                        self.nulls.append(true);
                        self.views.push(0);
                    }
                }
            }

            Some(true) => {
                self.nulls.append_n(rows.len(), false);
                for &row in rows {
                    self.do_append_val_inner(arr, row);
                }
            }

            Some(false) => {
                self.nulls.append_n(rows.len(), true);
                let new_len = self.views.len() + rows.len();
                self.views.resize(new_len, 0);
            }
        }
    }

    fn do_append_val_inner(&mut self, array: &GenericByteViewArray<B>, row: usize)
    where
        B: ByteViewType,
    {
        let value: &[u8] = array.value(row).as_ref();

        let value_len = value.len();
        let view = if value_len <= 12 {
            make_view(value, 0, 0)
        } else {
            // Ensure big enough block to hold the value firstly
            self.ensure_in_progress_big_enough(value_len);

            // Append value
            let buffer_index = self.completed.len();
            let offset = self.in_progress.len();
            self.in_progress.extend_from_slice(value);

            make_view(value, buffer_index as u32, offset as u32)
        };

        // Append view
        self.views.push(view);
    }

    fn ensure_in_progress_big_enough(&mut self, value_len: usize) {
        debug_assert!(value_len > 12);
        let require_cap = self.in_progress.len() + value_len;

        // If current block isn't big enough, flush it and create a new in progress block
        if require_cap > self.max_block_size {
            let flushed_block = replace(
                &mut self.in_progress,
                Vec::with_capacity(self.max_block_size),
            );
            let buffer = Buffer::from_vec(flushed_block);
            self.completed.push(buffer);
        }
    }

    fn do_equal_to_inner(
        &self,
        lhs_row: usize,
        array: &GenericByteViewArray<B>,
        rhs_row: usize,
    ) -> bool {
        // Check if nulls equal firstly
        let exist_null = self.nulls.is_null(lhs_row);
        let input_null = array.is_null(rhs_row);
        if let Some(result) = nulls_equal_to(exist_null, input_null) {
            return result;
        }

        // Otherwise, we need to check their values
        let exist_view = self.views[lhs_row];
        let exist_view_len = exist_view as u32;

        let input_view = array.views()[rhs_row];
        let input_view_len = input_view as u32;

        // The check logic
        //   - Check len equality
        //   - If inlined, check inlined value
        //   - If non-inlined, check prefix and then check value in buffer
        //     when needed
        if exist_view_len != input_view_len {
            return false;
        }

        if exist_view_len <= 12 {
            let exist_inline = unsafe {
                GenericByteViewArray::<B>::inline_value(
                    &exist_view,
                    exist_view_len as usize,
                )
            };
            let input_inline = unsafe {
                GenericByteViewArray::<B>::inline_value(
                    &input_view,
                    input_view_len as usize,
                )
            };
            exist_inline == input_inline
        } else {
            let exist_prefix =
                unsafe { GenericByteViewArray::<B>::inline_value(&exist_view, 4) };
            let input_prefix =
                unsafe { GenericByteViewArray::<B>::inline_value(&input_view, 4) };

            if exist_prefix != input_prefix {
                return false;
            }

            let exist_full = {
                let byte_view = ByteView::from(exist_view);
                self.value(
                    byte_view.buffer_index as usize,
                    byte_view.offset as usize,
                    byte_view.length as usize,
                )
            };
            let input_full: &[u8] = unsafe { array.value_unchecked(rhs_row).as_ref() };
            exist_full == input_full
        }
    }

    fn value(&self, buffer_index: usize, offset: usize, length: usize) -> &[u8] {
        debug_assert!(buffer_index <= self.completed.len());

        if buffer_index < self.completed.len() {
            let block = &self.completed[buffer_index];
            &block[offset..offset + length]
        } else {
            &self.in_progress[offset..offset + length]
        }
    }

    fn build_inner(self) -> ArrayRef {
        let Self {
            views,
            in_progress,
            mut completed,
            nulls,
            ..
        } = self;

        // Build nulls
        let null_buffer = nulls.build();

        // Build values
        // Flush `in_process` firstly
        if !in_progress.is_empty() {
            let buffer = Buffer::from(in_progress);
            completed.push(buffer);
        }

        let views = ScalarBuffer::from(views);

        // Safety:
        // * all views were correctly made
        // * (if utf8): Input was valid Utf8 so buffer contents are
        // valid utf8 as well
        unsafe {
            Arc::new(GenericByteViewArray::<B>::new_unchecked(
                views,
                completed,
                null_buffer,
            ))
        }
    }

    fn take_n_inner(&mut self, n: usize) -> ArrayRef {
        debug_assert!(self.len() >= n);

        // The `n == len` case, we need to take all
        if self.len() == n {
            let new_builder = Self::new().with_max_block_size(self.max_block_size);
            let cur_builder = replace(self, new_builder);
            return cur_builder.build_inner();
        }

        // The `n < len` case
        // Take n for nulls
        let null_buffer = self.nulls.take_n(n);

        // Take n for values:
        //   - Take first n `view`s from `views`
        //
        //   - Find the last non-inlined `view`, if all inlined,
        //     we can build array and return happily, otherwise we
        //     we need to continue to process related buffers
        //
        //   - Get the last related `buffer index`(let's name it `buffer index n`)
        //     from last non-inlined `view`
        //
        //   - Take buffers, the key is that we need to know if we need to take
        //     the whole last related buffer. The logic is a bit complex, you can
        //     detail in `take_buffers_with_whole_last`, `take_buffers_with_partial_last`
        //     and other related steps in following
        //
        //   - Shift the `buffer index` of remaining non-inlined `views`
        //
        let first_n_views = self.views.drain(0..n).collect::<Vec<_>>();

        let last_non_inlined_view = first_n_views
            .iter()
            .rev()
            .find(|view| ((**view) as u32) > 12);

        // All taken views inlined
        let Some(view) = last_non_inlined_view else {
            let views = ScalarBuffer::from(first_n_views);

            // Safety:
            // * all views were correctly made
            // * (if utf8): Input was valid Utf8 so buffer contents are
            // valid utf8 as well
            unsafe {
                return Arc::new(GenericByteViewArray::<B>::new_unchecked(
                    views,
                    Vec::new(),
                    null_buffer,
                ));
            }
        };

        // Unfortunately, some taken views non-inlined
        let view = ByteView::from(*view);
        let last_remaining_buffer_index = view.buffer_index as usize;

        // Check should we take the whole `last_remaining_buffer_index` buffer
        let take_whole_last_buffer = self.should_take_whole_buffer(
            last_remaining_buffer_index,
            (view.offset + view.length) as usize,
        );

        // Take related buffers
        let buffers = if take_whole_last_buffer {
            self.take_buffers_with_whole_last(last_remaining_buffer_index)
        } else {
            self.take_buffers_with_partial_last(
                last_remaining_buffer_index,
                (view.offset + view.length) as usize,
            )
        };

        // Shift `buffer index`s finally
        let shifts = if take_whole_last_buffer {
            last_remaining_buffer_index + 1
        } else {
            last_remaining_buffer_index
        };

        self.views.iter_mut().for_each(|view| {
            if (*view as u32) > 12 {
                let mut byte_view = ByteView::from(*view);
                byte_view.buffer_index -= shifts as u32;
                *view = byte_view.as_u128();
            }
        });

        // Build array and return
        let views = ScalarBuffer::from(first_n_views);

        // Safety:
        // * all views were correctly made
        // * (if utf8): Input was valid Utf8 so buffer contents are
        // valid utf8 as well
        unsafe {
            Arc::new(GenericByteViewArray::<B>::new_unchecked(
                views,
                buffers,
                null_buffer,
            ))
        }
    }

    fn take_buffers_with_whole_last(
        &mut self,
        last_remaining_buffer_index: usize,
    ) -> Vec<Buffer> {
        if last_remaining_buffer_index == self.completed.len() {
            self.flush_in_progress();
        }
        self.completed
            .drain(0..last_remaining_buffer_index + 1)
            .collect()
    }

    fn take_buffers_with_partial_last(
        &mut self,
        last_remaining_buffer_index: usize,
        last_take_len: usize,
    ) -> Vec<Buffer> {
        let mut take_buffers = Vec::with_capacity(last_remaining_buffer_index + 1);

        // Take `0 ~ last_remaining_buffer_index - 1` buffers
        if !self.completed.is_empty() || last_remaining_buffer_index == 0 {
            take_buffers.extend(self.completed.drain(0..last_remaining_buffer_index));
        }

        // Process the `last_remaining_buffer_index` buffers
        let last_buffer = if last_remaining_buffer_index < self.completed.len() {
            // If it is in `completed`, simply clone
            self.completed[last_remaining_buffer_index].clone()
        } else {
            // If it is `in_progress`, copied `0 ~ offset` part
            let taken_last_buffer = self.in_progress[0..last_take_len].to_vec();
            Buffer::from_vec(taken_last_buffer)
        };
        take_buffers.push(last_buffer);

        take_buffers
    }

    #[inline]
    fn should_take_whole_buffer(&self, buffer_index: usize, take_len: usize) -> bool {
        if buffer_index < self.completed.len() {
            take_len == self.completed[buffer_index].len()
        } else {
            take_len == self.in_progress.len()
        }
    }

    fn flush_in_progress(&mut self) {
        let flushed_block = replace(
            &mut self.in_progress,
            Vec::with_capacity(self.max_block_size),
        );
        let buffer = Buffer::from_vec(flushed_block);
        self.completed.push(buffer);
    }
}

impl<B: ByteViewType> GroupColumn for ByteViewGroupValueBuilder<B> {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        self.equal_to_inner(lhs_row, array, rhs_row)
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) {
        self.append_val_inner(array, row)
    }

    fn vectorized_equal_to(
        &self,
        group_indices: &[usize],
        array: &ArrayRef,
        rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        self.vectorized_equal_to_inner(group_indices, array, rows, equal_to_results);
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) {
        self.vectorized_append_inner(array, rows);
    }

    fn len(&self) -> usize {
        self.views.len()
    }

    fn size(&self) -> usize {
        let buffers_size = self
            .completed
            .iter()
            .map(|buf| buf.capacity() * size_of::<u8>())
            .sum::<usize>();

        self.nulls.allocated_size()
            + self.views.capacity() * size_of::<u128>()
            + self.in_progress.capacity() * size_of::<u8>()
            + buffers_size
            + size_of::<Self>()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        Self::build_inner(*self)
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        self.take_n_inner(n)
    }
}

/// Determines if the nullability of the existing and new input array can be used
/// to short-circuit the comparison of the two values.
///
/// Returns `Some(result)` if the result of the comparison can be determined
/// from the nullness of the two values, and `None` if the comparison must be
/// done on the values themselves.
fn nulls_equal_to(lhs_null: bool, rhs_null: bool) -> Option<bool> {
    match (lhs_null, rhs_null) {
        (true, true) => Some(true),
        (false, true) | (true, false) => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::AsArray,
        datatypes::{Int64Type, StringViewType},
    };
    use arrow_array::{Array, ArrayRef, Int64Array, StringArray, StringViewArray};
    use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
    use datafusion_physical_expr::binary_map::OutputType;

    use crate::aggregates::group_values::group_column::{
        ByteViewGroupValueBuilder, PrimitiveGroupValueBuilder,
    };

    use super::{ByteGroupValueBuilder, GroupColumn};

    // ========================================================================
    // Tests for primitive builders
    // ========================================================================
    #[test]
    fn test_nullable_primitive_equal_to() {
        let append = |builder: &mut PrimitiveGroupValueBuilder<Int64Type, true>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            for &index in append_rows {
                builder.append_val(builder_array, index);
            }
        };

        let equal_to = |builder: &PrimitiveGroupValueBuilder<Int64Type, true>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            let iter = lhs_rows.iter().zip(rhs_rows.iter());
            for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
                equal_to_results[idx] = builder.equal_to(lhs_row, input_array, rhs_row);
            }
        };

        test_nullable_primitive_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_nullable_primitive_vectorized_equal_to() {
        let append = |builder: &mut PrimitiveGroupValueBuilder<Int64Type, true>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            builder.vectorized_append(builder_array, append_rows);
        };

        let equal_to = |builder: &PrimitiveGroupValueBuilder<Int64Type, true>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            builder.vectorized_equal_to(
                lhs_rows,
                input_array,
                rhs_rows,
                equal_to_results,
            );
        };

        test_nullable_primitive_equal_to_internal(append, equal_to);
    }

    fn test_nullable_primitive_equal_to_internal<A, E>(mut append: A, mut equal_to: E)
    where
        A: FnMut(&mut PrimitiveGroupValueBuilder<Int64Type, true>, &ArrayRef, &[usize]),
        E: FnMut(
            &PrimitiveGroupValueBuilder<Int64Type, true>,
            &[usize],
            &ArrayRef,
            &[usize],
            &mut Vec<bool>,
        ),
    {
        // Will cover such cases:
        //   - exist null, input not null
        //   - exist null, input null; values not equal
        //   - exist null, input null; values equal
        //   - exist not null, input null
        //   - exist not null, input not null; values not equal
        //   - exist not null, input not null; values equal

        // Define PrimitiveGroupValueBuilder
        let mut builder = PrimitiveGroupValueBuilder::<Int64Type, true>::new();
        let builder_array = Arc::new(Int64Array::from(vec![
            None,
            None,
            None,
            Some(1),
            Some(2),
            Some(3),
        ])) as ArrayRef;
        append(&mut builder, &builder_array, &[0, 1, 2, 3, 4, 5]);

        // Define input array
        let (_nulls, values, _) =
            Int64Array::from(vec![Some(1), Some(2), None, None, Some(1), Some(3)])
                .into_parts();

        // explicitly build a boolean buffer where one of the null values also happens to match
        let mut boolean_buffer_builder = BooleanBufferBuilder::new(6);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(false); // this sets Some(2) to null above
        boolean_buffer_builder.append(false);
        boolean_buffer_builder.append(false);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(true);
        let nulls = NullBuffer::new(boolean_buffer_builder.finish());
        let input_array = Arc::new(Int64Array::new(values, Some(nulls))) as ArrayRef;

        // Check
        let mut equal_to_results = vec![true; builder.len()];
        equal_to(
            &builder,
            &[0, 1, 2, 3, 4, 5],
            &input_array,
            &[0, 1, 2, 3, 4, 5],
            &mut equal_to_results,
        );

        assert!(!equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(!equal_to_results[3]);
        assert!(!equal_to_results[4]);
        assert!(equal_to_results[5]);
    }

    #[test]
    fn test_not_nullable_primitive_equal_to() {
        let append = |builder: &mut PrimitiveGroupValueBuilder<Int64Type, false>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            for &index in append_rows {
                builder.append_val(builder_array, index);
            }
        };

        let equal_to = |builder: &PrimitiveGroupValueBuilder<Int64Type, false>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            let iter = lhs_rows.iter().zip(rhs_rows.iter());
            for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
                equal_to_results[idx] = builder.equal_to(lhs_row, input_array, rhs_row);
            }
        };

        test_not_nullable_primitive_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_not_nullable_primitive_vectorized_equal_to() {
        let append = |builder: &mut PrimitiveGroupValueBuilder<Int64Type, false>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            builder.vectorized_append(builder_array, append_rows);
        };

        let equal_to = |builder: &PrimitiveGroupValueBuilder<Int64Type, false>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            builder.vectorized_equal_to(
                lhs_rows,
                input_array,
                rhs_rows,
                equal_to_results,
            );
        };

        test_not_nullable_primitive_equal_to_internal(append, equal_to);
    }

    fn test_not_nullable_primitive_equal_to_internal<A, E>(mut append: A, mut equal_to: E)
    where
        A: FnMut(&mut PrimitiveGroupValueBuilder<Int64Type, false>, &ArrayRef, &[usize]),
        E: FnMut(
            &PrimitiveGroupValueBuilder<Int64Type, false>,
            &[usize],
            &ArrayRef,
            &[usize],
            &mut Vec<bool>,
        ),
    {
        // Will cover such cases:
        //   - values equal
        //   - values not equal

        // Define PrimitiveGroupValueBuilder
        let mut builder = PrimitiveGroupValueBuilder::<Int64Type, false>::new();
        let builder_array =
            Arc::new(Int64Array::from(vec![Some(0), Some(1)])) as ArrayRef;
        append(&mut builder, &builder_array, &[0, 1]);

        // Define input array
        let input_array = Arc::new(Int64Array::from(vec![Some(0), Some(2)])) as ArrayRef;

        // Check
        let mut equal_to_results = vec![true; builder.len()];
        equal_to(
            &builder,
            &[0, 1],
            &input_array,
            &[0, 1],
            &mut equal_to_results,
        );

        assert!(equal_to_results[0]);
        assert!(!equal_to_results[1]);
    }

    #[test]
    fn test_nullable_primitive_vectorized_operation_special_case() {
        // Test the special `all nulls` or `not nulls` input array case
        // for vectorized append and equal to

        let mut builder = PrimitiveGroupValueBuilder::<Int64Type, true>::new();

        // All nulls input array
        let all_nulls_input_array = Arc::new(Int64Array::from(vec![
            Option::<i64>::None,
            None,
            None,
            None,
            None,
        ])) as _;
        builder.vectorized_append(&all_nulls_input_array, &[0, 1, 2, 3, 4]);

        let mut equal_to_results = vec![true; all_nulls_input_array.len()];
        builder.vectorized_equal_to(
            &[0, 1, 2, 3, 4],
            &all_nulls_input_array,
            &[0, 1, 2, 3, 4],
            &mut equal_to_results,
        );

        assert!(equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(equal_to_results[3]);
        assert!(equal_to_results[4]);

        // All not nulls input array
        let all_not_nulls_input_array = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ])) as _;
        builder.vectorized_append(&all_not_nulls_input_array, &[0, 1, 2, 3, 4]);

        let mut equal_to_results = vec![true; all_not_nulls_input_array.len()];
        builder.vectorized_equal_to(
            &[5, 6, 7, 8, 9],
            &all_not_nulls_input_array,
            &[0, 1, 2, 3, 4],
            &mut equal_to_results,
        );

        assert!(equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(equal_to_results[3]);
        assert!(equal_to_results[4]);
    }

    // ========================================================================
    // Tests for byte builders
    // ========================================================================
    #[test]
    fn test_byte_take_n() {
        let mut builder = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);
        let array = Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef;
        // a, null, null
        builder.append_val(&array, 0);
        builder.append_val(&array, 1);
        builder.append_val(&array, 1);

        // (a, null) remaining: null
        let output = builder.take_n(2);
        assert_eq!(&output, &array);

        // null, a, null, a
        builder.append_val(&array, 0);
        builder.append_val(&array, 1);
        builder.append_val(&array, 0);

        // (null, a) remaining: (null, a)
        let output = builder.take_n(2);
        let array = Arc::new(StringArray::from(vec![None, Some("a")])) as ArrayRef;
        assert_eq!(&output, &array);

        let array = Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("longstringfortest"),
        ])) as ArrayRef;

        // null, a, longstringfortest, null, null
        builder.append_val(&array, 2);
        builder.append_val(&array, 1);
        builder.append_val(&array, 1);

        // (null, a, longstringfortest, null) remaining: (null)
        let output = builder.take_n(4);
        let array = Arc::new(StringArray::from(vec![
            None,
            Some("a"),
            Some("longstringfortest"),
            None,
        ])) as ArrayRef;
        assert_eq!(&output, &array);
    }

    #[test]
    fn test_byte_equal_to() {
        let append = |builder: &mut ByteGroupValueBuilder<i32>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            for &index in append_rows {
                builder.append_val(builder_array, index);
            }
        };

        let equal_to = |builder: &ByteGroupValueBuilder<i32>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            let iter = lhs_rows.iter().zip(rhs_rows.iter());
            for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
                equal_to_results[idx] = builder.equal_to(lhs_row, input_array, rhs_row);
            }
        };

        test_byte_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_byte_vectorized_equal_to() {
        let append = |builder: &mut ByteGroupValueBuilder<i32>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            builder.vectorized_append(builder_array, append_rows);
        };

        let equal_to = |builder: &ByteGroupValueBuilder<i32>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            builder.vectorized_equal_to(
                lhs_rows,
                input_array,
                rhs_rows,
                equal_to_results,
            );
        };

        test_byte_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_byte_vectorized_operation_special_case() {
        // Test the special `all nulls` or `not nulls` input array case
        // for vectorized append and equal to

        let mut builder = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);

        // All nulls input array
        let all_nulls_input_array = Arc::new(StringArray::from(vec![
            Option::<&str>::None,
            None,
            None,
            None,
            None,
        ])) as _;
        builder.vectorized_append(&all_nulls_input_array, &[0, 1, 2, 3, 4]);

        let mut equal_to_results = vec![true; all_nulls_input_array.len()];
        builder.vectorized_equal_to(
            &[0, 1, 2, 3, 4],
            &all_nulls_input_array,
            &[0, 1, 2, 3, 4],
            &mut equal_to_results,
        );

        assert!(equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(equal_to_results[3]);
        assert!(equal_to_results[4]);

        // All not nulls input array
        let all_not_nulls_input_array = Arc::new(StringArray::from(vec![
            Some("string1"),
            Some("string2"),
            Some("string3"),
            Some("string4"),
            Some("string5"),
        ])) as _;
        builder.vectorized_append(&all_not_nulls_input_array, &[0, 1, 2, 3, 4]);

        let mut equal_to_results = vec![true; all_not_nulls_input_array.len()];
        builder.vectorized_equal_to(
            &[5, 6, 7, 8, 9],
            &all_not_nulls_input_array,
            &[0, 1, 2, 3, 4],
            &mut equal_to_results,
        );

        assert!(equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(equal_to_results[3]);
        assert!(equal_to_results[4]);
    }

    fn test_byte_equal_to_internal<A, E>(mut append: A, mut equal_to: E)
    where
        A: FnMut(&mut ByteGroupValueBuilder<i32>, &ArrayRef, &[usize]),
        E: FnMut(
            &ByteGroupValueBuilder<i32>,
            &[usize],
            &ArrayRef,
            &[usize],
            &mut Vec<bool>,
        ),
    {
        // Will cover such cases:
        //   - exist null, input not null
        //   - exist null, input null; values not equal
        //   - exist null, input null; values equal
        //   - exist not null, input null
        //   - exist not null, input not null; values not equal
        //   - exist not null, input not null; values equal

        // Define PrimitiveGroupValueBuilder
        let mut builder = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);
        let builder_array = Arc::new(StringArray::from(vec![
            None,
            None,
            None,
            Some("foo"),
            Some("bar"),
            Some("baz"),
        ])) as ArrayRef;
        append(&mut builder, &builder_array, &[0, 1, 2, 3, 4, 5]);

        // Define input array
        let (offsets, buffer, _nulls) = StringArray::from(vec![
            Some("foo"),
            Some("bar"),
            None,
            None,
            Some("foo"),
            Some("baz"),
        ])
        .into_parts();

        // explicitly build a boolean buffer where one of the null values also happens to match
        let mut boolean_buffer_builder = BooleanBufferBuilder::new(6);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(false); // this sets Some("bar") to null above
        boolean_buffer_builder.append(false);
        boolean_buffer_builder.append(false);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(true);
        let nulls = NullBuffer::new(boolean_buffer_builder.finish());
        let input_array =
            Arc::new(StringArray::new(offsets, buffer, Some(nulls))) as ArrayRef;

        // Check
        let mut equal_to_results = vec![true; builder.len()];
        equal_to(
            &builder,
            &[0, 1, 2, 3, 4, 5],
            &input_array,
            &[0, 1, 2, 3, 4, 5],
            &mut equal_to_results,
        );

        assert!(!equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(!equal_to_results[3]);
        assert!(!equal_to_results[4]);
        assert!(equal_to_results[5]);
    }

    // ========================================================================
    // Tests for byte view builders
    // ========================================================================
    #[test]
    fn test_byte_view_append_val() {
        let mut builder =
            ByteViewGroupValueBuilder::<StringViewType>::new().with_max_block_size(60);
        let builder_array = StringViewArray::from(vec![
            Some("this string is quite long"), // in buffer 0
            Some("foo"),
            None,
            Some("bar"),
            Some("this string is also quite long"), // buffer 0
            Some("this string is quite long"),      // buffer 1
            Some("bar"),
        ]);
        let builder_array: ArrayRef = Arc::new(builder_array);
        for row in 0..builder_array.len() {
            builder.append_val(&builder_array, row);
        }

        let output = Box::new(builder).build();
        // should be 2 output buffers to hold all the data
        assert_eq!(output.as_string_view().data_buffers().len(), 2);
        assert_eq!(&output, &builder_array)
    }

    #[test]
    fn test_byte_view_equal_to() {
        let append = |builder: &mut ByteViewGroupValueBuilder<StringViewType>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            for &index in append_rows {
                builder.append_val(builder_array, index);
            }
        };

        let equal_to = |builder: &ByteViewGroupValueBuilder<StringViewType>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            let iter = lhs_rows.iter().zip(rhs_rows.iter());
            for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
                equal_to_results[idx] = builder.equal_to(lhs_row, input_array, rhs_row);
            }
        };

        test_byte_view_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_byte_view_vectorized_equal_to() {
        let append = |builder: &mut ByteViewGroupValueBuilder<StringViewType>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            builder.vectorized_append(builder_array, append_rows);
        };

        let equal_to = |builder: &ByteViewGroupValueBuilder<StringViewType>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            builder.vectorized_equal_to(
                lhs_rows,
                input_array,
                rhs_rows,
                equal_to_results,
            );
        };

        test_byte_view_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_byte_view_vectorized_operation_special_case() {
        // Test the special `all nulls` or `not nulls` input array case
        // for vectorized append and equal to

        let mut builder =
            ByteViewGroupValueBuilder::<StringViewType>::new().with_max_block_size(60);

        // All nulls input array
        let all_nulls_input_array = Arc::new(StringViewArray::from(vec![
            Option::<&str>::None,
            None,
            None,
            None,
            None,
        ])) as _;
        builder.vectorized_append(&all_nulls_input_array, &[0, 1, 2, 3, 4]);

        let mut equal_to_results = vec![true; all_nulls_input_array.len()];
        builder.vectorized_equal_to(
            &[0, 1, 2, 3, 4],
            &all_nulls_input_array,
            &[0, 1, 2, 3, 4],
            &mut equal_to_results,
        );

        assert!(equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(equal_to_results[3]);
        assert!(equal_to_results[4]);

        // All not nulls input array
        let all_not_nulls_input_array = Arc::new(StringViewArray::from(vec![
            Some("stringview1"),
            Some("stringview2"),
            Some("stringview3"),
            Some("stringview4"),
            Some("stringview5"),
        ])) as _;
        builder.vectorized_append(&all_not_nulls_input_array, &[0, 1, 2, 3, 4]);

        let mut equal_to_results = vec![true; all_not_nulls_input_array.len()];
        builder.vectorized_equal_to(
            &[5, 6, 7, 8, 9],
            &all_not_nulls_input_array,
            &[0, 1, 2, 3, 4],
            &mut equal_to_results,
        );

        assert!(equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(equal_to_results[3]);
        assert!(equal_to_results[4]);
    }

    fn test_byte_view_equal_to_internal<A, E>(mut append: A, mut equal_to: E)
    where
        A: FnMut(&mut ByteViewGroupValueBuilder<StringViewType>, &ArrayRef, &[usize]),
        E: FnMut(
            &ByteViewGroupValueBuilder<StringViewType>,
            &[usize],
            &ArrayRef,
            &[usize],
            &mut Vec<bool>,
        ),
    {
        // Will cover such cases:
        //   - exist null, input not null
        //   - exist null, input null; values not equal
        //   - exist null, input null; values equal
        //   - exist not null, input null
        //   - exist not null, input not null; value lens not equal
        //   - exist not null, input not null; value not equal(inlined case)
        //   - exist not null, input not null; value equal(inlined case)
        //
        //   - exist not null, input not null; value not equal
        //     (non-inlined case + prefix not equal)
        //
        //   - exist not null, input not null; value not equal
        //     (non-inlined case + value in `completed`)
        //
        //   - exist not null, input not null; value equal
        //     (non-inlined case + value in `completed`)
        //
        //   - exist not null, input not null; value not equal
        //     (non-inlined case + value in `in_progress`)
        //
        //   - exist not null, input not null; value equal
        //     (non-inlined case + value in `in_progress`)

        // Set the block size to 40 for ensuring some unlined values are in `in_progress`,
        // and some are in `completed`, so both two branches in `value` function can be covered.
        let mut builder =
            ByteViewGroupValueBuilder::<StringViewType>::new().with_max_block_size(60);
        let builder_array = Arc::new(StringViewArray::from(vec![
            None,
            None,
            None,
            Some("foo"),
            Some("bazz"),
            Some("foo"),
            Some("bar"),
            Some("I am a long string for test eq in completed"),
            Some("I am a long string for test eq in progress"),
        ])) as ArrayRef;
        append(&mut builder, &builder_array, &[0, 1, 2, 3, 4, 5, 6, 7, 8]);

        // Define input array
        let (views, buffer, _nulls) = StringViewArray::from(vec![
            Some("foo"),
            Some("bar"), // set to null
            None,
            None,
            Some("baz"),
            Some("oof"),
            Some("bar"),
            Some("i am a long string for test eq in completed"),
            Some("I am a long string for test eq in COMPLETED"),
            Some("I am a long string for test eq in completed"),
            Some("I am a long string for test eq in PROGRESS"),
            Some("I am a long string for test eq in progress"),
        ])
        .into_parts();

        // explicitly build a boolean buffer where one of the null values also happens to match
        let mut boolean_buffer_builder = BooleanBufferBuilder::new(9);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(false); // this sets Some("bar") to null above
        boolean_buffer_builder.append(false);
        boolean_buffer_builder.append(false);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(true);
        boolean_buffer_builder.append(true);
        let nulls = NullBuffer::new(boolean_buffer_builder.finish());
        let input_array =
            Arc::new(StringViewArray::new(views, buffer, Some(nulls))) as ArrayRef;

        // Check
        let mut equal_to_results = vec![true; input_array.len()];
        equal_to(
            &builder,
            &[0, 1, 2, 3, 4, 5, 6, 7, 7, 7, 8, 8],
            &input_array,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            &mut equal_to_results,
        );

        assert!(!equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(!equal_to_results[3]);
        assert!(!equal_to_results[4]);
        assert!(!equal_to_results[5]);
        assert!(equal_to_results[6]);
        assert!(!equal_to_results[7]);
        assert!(!equal_to_results[8]);
        assert!(equal_to_results[9]);
        assert!(!equal_to_results[10]);
        assert!(equal_to_results[11]);
    }

    #[test]
    fn test_byte_view_take_n() {
        // ####### Define cases and init #######

        // `take_n` is really complex, we should consider and test following situations:
        //   1. Take nulls
        //   2. Take all `inlined`s
        //   3. Take non-inlined + partial last buffer in `completed`
        //   4. Take non-inlined + whole last buffer in `completed`
        //   5. Take non-inlined + partial last `in_progress`
        //   6. Take non-inlined + whole last buffer in `in_progress`
        //   7. Take all views at once

        let mut builder =
            ByteViewGroupValueBuilder::<StringViewType>::new().with_max_block_size(60);
        let input_array = StringViewArray::from(vec![
            //  Test situation 1
            None,
            None,
            // Test situation 2 (also test take null together)
            None,
            Some("foo"),
            Some("bar"),
            // Test situation 3 (also test take null + inlined)
            None,
            Some("foo"),
            Some("this string is quite long"),
            Some("this string is also quite long"),
            // Test situation 4 (also test take null + inlined)
            None,
            Some("bar"),
            Some("this string is quite long"),
            // Test situation 5 (also test take null + inlined)
            None,
            Some("foo"),
            Some("another string that is is quite long"),
            Some("this string not so long"),
            // Test situation 6 (also test take null + inlined + insert again after taking)
            None,
            Some("bar"),
            Some("this string is quite long"),
            // Insert 4 and just take 3 to ensure it will go the path of situation 6
            None,
            // Finally, we create a new builder,  insert the whole array and then
            // take whole at once for testing situation 7
        ]);

        let input_array: ArrayRef = Arc::new(input_array);
        let first_ones_to_append = 16; // For testing situation 1~5
        let second_ones_to_append = 4; // For testing situation 6
        let final_ones_to_append = input_array.len(); // For testing situation 7

        // ####### Test situation 1~5 #######
        for row in 0..first_ones_to_append {
            builder.append_val(&input_array, row);
        }

        assert_eq!(builder.completed.len(), 2);
        assert_eq!(builder.in_progress.len(), 59);

        // Situation 1
        let taken_array = builder.take_n(2);
        assert_eq!(&taken_array, &input_array.slice(0, 2));

        // Situation 2
        let taken_array = builder.take_n(3);
        assert_eq!(&taken_array, &input_array.slice(2, 3));

        // Situation 3
        let taken_array = builder.take_n(3);
        assert_eq!(&taken_array, &input_array.slice(5, 3));

        let taken_array = builder.take_n(1);
        assert_eq!(&taken_array, &input_array.slice(8, 1));

        // Situation 4
        let taken_array = builder.take_n(3);
        assert_eq!(&taken_array, &input_array.slice(9, 3));

        // Situation 5
        let taken_array = builder.take_n(3);
        assert_eq!(&taken_array, &input_array.slice(12, 3));

        let taken_array = builder.take_n(1);
        assert_eq!(&taken_array, &input_array.slice(15, 1));

        // ####### Test situation 6 #######
        assert!(builder.completed.is_empty());
        assert!(builder.in_progress.is_empty());
        assert!(builder.views.is_empty());

        for row in first_ones_to_append..first_ones_to_append + second_ones_to_append {
            builder.append_val(&input_array, row);
        }

        assert!(builder.completed.is_empty());
        assert_eq!(builder.in_progress.len(), 25);

        let taken_array = builder.take_n(3);
        assert_eq!(&taken_array, &input_array.slice(16, 3));

        // ####### Test situation 7 #######
        // Create a new builder
        let mut builder =
            ByteViewGroupValueBuilder::<StringViewType>::new().with_max_block_size(60);

        for row in 0..final_ones_to_append {
            builder.append_val(&input_array, row);
        }

        assert_eq!(builder.completed.len(), 3);
        assert_eq!(builder.in_progress.len(), 25);

        let taken_array = builder.take_n(final_ones_to_append);
        assert_eq!(&taken_array, &input_array);
    }
}
