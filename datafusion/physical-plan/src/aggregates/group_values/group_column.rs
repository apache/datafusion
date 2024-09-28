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

use arrow::array::BooleanBufferBuilder;
use arrow::array::BufferBuilder;
use arrow::array::GenericBinaryArray;
use arrow::array::GenericStringArray;
use arrow::array::OffsetSizeTrait;
use arrow::array::PrimitiveArray;
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray};
use arrow::buffer::NullBuffer;
use arrow::buffer::OffsetBuffer;
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::ArrowNativeType;
use arrow::datatypes::ByteArrayType;
use arrow::datatypes::DataType;
use arrow::datatypes::GenericBinaryType;
use arrow::datatypes::GenericStringType;
use datafusion_common::utils::proxy::VecAllocExt;

use std::sync::Arc;
use std::vec;

use crate::aggregates::group_values::null_builder::MaybeNullBufferBuilder;
use datafusion_physical_expr_common::binary_map::{OutputType, INITIAL_BUFFER_CAPACITY};

/// Trait for group values column-wise row comparison
///
/// Implementations of this trait store a in-progress collection of group values
/// (similar to various builders in Arrow-rs) that allow for quick comparison to
/// incoming rows.
///
pub trait GroupColumn: Send + Sync {
    /// Returns equal if the row stored in this builder at `lhs_row` is equal to
    /// the row in `array` at `rhs_row`
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool;
    /// Appends the row at `row` in `array` to this builder
    fn append_val(&mut self, array: &ArrayRef, row: usize);
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

pub struct PrimitiveGroupValueBuilder<T: ArrowPrimitiveType> {
    group_values: Vec<T::Native>,
    /// If false, the input is guaranteed to have no nulls
    nullable: bool,
    /// Null state
    nulls: MaybeNullBufferBuilder,
}

impl<T> PrimitiveGroupValueBuilder<T>
where
    T: ArrowPrimitiveType,
{
    /// Create a new [`PrimitiveGroupValueBuilder`]
    ///
    /// If `nullable` is false, it means the input will never have nulls
    pub fn new(nullable: bool) -> Self {
        Self {
            group_values: vec![],
            nulls: MaybeNullBufferBuilder::new(),
            nullable,
        }
    }
}

impl<T: ArrowPrimitiveType> GroupColumn for PrimitiveGroupValueBuilder<T> {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        // fast path when input has no nulls
        if !self.nullable {
            debug_assert!(!self.nulls.has_nulls());
            return self.group_values[lhs_row]
                == array.as_primitive::<T>().value(rhs_row);
        }
        // slow path if the input could have nulls
        if self.nulls.is_null(lhs_row) || array.is_null(rhs_row) {
            false // comparing null to anything is not true
        } else {
            self.group_values[lhs_row] == array.as_primitive::<T>().value(rhs_row)
        }
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) {
        if array.is_null(row) {
            self.nulls.append(true);
            self.group_values.push(T::default_value());
        } else {
            self.nulls.append(false);
            self.group_values.push(array.as_primitive::<T>().value(row));
        }
    }

    fn len(&self) -> usize {
        self.group_values.len()
    }

    fn size(&self) -> usize {
        // BooleanBufferBuilder builder::capacity returns capacity in bits (not bytes)
        self.group_values.allocated_size() + self.nulls.allocated_size()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let Self {
            group_values,
            nulls,
            nullable: _,
        } = *self;

        Arc::new(PrimitiveArray::<T>::new(
            ScalarBuffer::from(group_values),
            nulls.build(),
        ))
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        let first_n = self.group_values.drain(0..n).collect::<Vec<_>>();
        let first_n_nulls = self.nulls.take_n(n);

        Arc::new(PrimitiveArray::<T>::new(
            ScalarBuffer::from(first_n),
            first_n_nulls,
        ))
    }
}

pub struct ByteGroupValueBuilder<O>
where
    O: OffsetSizeTrait,
{
    output_type: OutputType,
    buffer: BufferBuilder<u8>,
    /// Offsets into `buffer` for each distinct  value. These offsets as used
    /// directly to create the final `GenericBinaryArray`. The `i`th string is
    /// stored in the range `offsets[i]..offsets[i+1]` in `buffer`. Null values
    /// are stored as a zero length string.
    offsets: Vec<O>,
    /// Null indexes in offsets, if `i` is in nulls, `offsets[i]` should be equals to `offsets[i+1]`
    nulls: Vec<usize>,
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
            nulls: vec![],
        }
    }

    fn append_val_inner<B>(&mut self, array: &ArrayRef, row: usize)
    where
        B: ByteArrayType,
    {
        let arr = array.as_bytes::<B>();
        if arr.is_null(row) {
            self.nulls.push(self.len());
            // nulls need a zero length in the offset buffer
            let offset = self.buffer.len();

            self.offsets.push(O::usize_as(offset));
            return;
        }

        let value: &[u8] = arr.value(row).as_ref();
        self.buffer.append_slice(value);
        self.offsets.push(O::usize_as(self.buffer.len()));
    }

    fn equal_to_inner<B>(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool
    where
        B: ByteArrayType,
    {
        // Handle nulls
        let is_lhs_null = self.nulls.iter().any(|null_idx| *null_idx == lhs_row);
        let arr = array.as_bytes::<B>();
        if is_lhs_null {
            return arr.is_null(rhs_row);
        } else if arr.is_null(rhs_row) {
            return false;
        }

        let arr = array.as_bytes::<B>();
        let rhs_elem: &[u8] = arr.value(rhs_row).as_ref();
        let rhs_elem_len = arr.value_length(rhs_row).as_usize();
        debug_assert_eq!(rhs_elem_len, rhs_elem.len());
        let l = self.offsets[lhs_row].as_usize();
        let r = self.offsets[lhs_row + 1].as_usize();
        let existing_elem = unsafe { self.buffer.as_slice().get_unchecked(l..r) };
        rhs_elem == existing_elem
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

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn size(&self) -> usize {
        self.buffer.capacity() * std::mem::size_of::<u8>()
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

        let null_buffer = if nulls.is_empty() {
            None
        } else {
            // Only make a `NullBuffer` if there was a null value
            let num_values = offsets.len() - 1;
            let mut bool_builder = BooleanBufferBuilder::new(num_values);
            bool_builder.append_n(num_values, true);
            nulls.into_iter().for_each(|null_index| {
                bool_builder.set_bit(null_index, false);
            });
            Some(NullBuffer::from(bool_builder.finish()))
        };

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

    fn take_n(&mut self, n: usize) -> ArrayRef {
        debug_assert!(self.len() >= n);

        let null_buffer = if self.nulls.is_empty() {
            None
        } else {
            // Only make a `NullBuffer` if there was a null value
            let mut bool_builder = BooleanBufferBuilder::new(n);
            bool_builder.append_n(n, true);

            let mut new_nulls = vec![];
            self.nulls.iter().for_each(|null_index| {
                if *null_index < n {
                    bool_builder.set_bit(*null_index, false);
                } else {
                    new_nulls.push(null_index - n);
                }
            });

            self.nulls = new_nulls;
            Some(NullBuffer::from(bool_builder.finish()))
        };

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, StringArray};
    use datafusion_physical_expr::binary_map::OutputType;

    use super::{ByteGroupValueBuilder, GroupColumn};

    #[test]
    fn test_take_n() {
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
}
