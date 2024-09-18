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

use std::sync::Arc;
use std::vec;

use crate::binary_map::OutputType;
use crate::binary_map::INITIAL_BUFFER_CAPACITY;

/// Trait for group values column-wise row comparison
pub trait ArrayRowEq: Send + Sync {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool;
    fn append_val(&mut self, array: &ArrayRef, row: usize);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn build(self: Box<Self>) -> ArrayRef;
    fn take_n(&mut self, n: usize) -> ArrayRef;
}

pub struct PrimitiveGroupValueBuilder<T: ArrowPrimitiveType> {
    group_values: Vec<T::Native>,
    nulls: Vec<bool>,
    // whether the array contains at least one null, for fast non-null path
    has_null: bool,
    nullable: bool,
}

impl<T> PrimitiveGroupValueBuilder<T>
where
    T: ArrowPrimitiveType,
{
    pub fn new(nullable: bool) -> Self {
        Self {
            group_values: vec![],
            nulls: vec![],
            has_null: false,
            nullable,
        }
    }
}

impl<T: ArrowPrimitiveType> ArrayRowEq for PrimitiveGroupValueBuilder<T> {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        // non-null fast path
        if !self.nullable {
            return self.group_values[lhs_row]
                == array.as_primitive::<T>().value(rhs_row);
        }

        if !self.has_null || self.nulls[lhs_row] {
            if array.is_null(rhs_row) {
                return false;
            }

            return self.group_values[lhs_row]
                == array.as_primitive::<T>().value(rhs_row);
        }

        array.is_null(rhs_row)
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) {
        // non-null fast path
        if !self.nullable || !array.is_null(row) {
            let elem = array.as_primitive::<T>().value(row);
            self.group_values.push(elem);
            self.nulls.push(true);
        } else {
            self.group_values.push(T::default_value());
            self.nulls.push(false);
            self.has_null = true;
        }
    }

    fn len(&self) -> usize {
        self.group_values.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn build(self: Box<Self>) -> ArrayRef {
        if self.has_null {
            Arc::new(PrimitiveArray::<T>::new(
                ScalarBuffer::from(self.group_values),
                Some(NullBuffer::from(self.nulls)),
            ))
        } else {
            Arc::new(PrimitiveArray::<T>::new(
                ScalarBuffer::from(self.group_values),
                None,
            ))
        }
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        println!("go");
        if self.has_null {
            let first_n = self.group_values.drain(0..n).collect::<Vec<_>>();
            let first_n_nulls = self.nulls.drain(0..n).collect::<Vec<_>>();
            Arc::new(PrimitiveArray::<T>::new(
                ScalarBuffer::from(first_n),
                Some(NullBuffer::from(first_n_nulls)),
            ))
        } else {
            let first_n = self.group_values.drain(0..n).collect::<Vec<_>>();
            self.nulls.truncate(self.nulls.len() - n);
            Arc::new(PrimitiveArray::<T>::new(ScalarBuffer::from(first_n), None))
        }
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
    /// Null indexes in offsets
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
        assert_eq!(rhs_elem_len, rhs_elem.len());
        let l = self.offsets[lhs_row].as_usize();
        let r = self.offsets[lhs_row + 1].as_usize();
        let existing_elem = unsafe { self.buffer.as_slice().get_unchecked(l..r) };
        existing_elem.len() == rhs_elem.len() && rhs_elem == existing_elem
    }
}

impl<O> ArrayRowEq for ByteGroupValueBuilder<O>
where
    O: OffsetSizeTrait,
{
    fn equal_to(&self, lhs_row: usize, column: &ArrayRef, rhs_row: usize) -> bool {
        // Sanity array type
        match self.output_type {
            OutputType::Binary => {
                assert!(matches!(
                    column.data_type(),
                    DataType::Binary | DataType::LargeBinary
                ));
                self.equal_to_inner::<GenericBinaryType<O>>(lhs_row, column, rhs_row)
            }
            OutputType::Utf8 => {
                assert!(matches!(
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
                assert!(matches!(
                    column.data_type(),
                    DataType::Binary | DataType::LargeBinary
                ));
                self.append_val_inner::<GenericBinaryType<O>>(column, row)
            }
            OutputType::Utf8 => {
                assert!(matches!(
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

    fn is_empty(&self) -> bool {
        self.len() == 0
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
        assert!(self.len() >= n);

        let mut nulls_count = 0;
        let null_buffer = if self.nulls.is_empty() {
            None
        } else {
            // Only make a `NullBuffer` if there was a null value
            let num_values = self.offsets.len() - 1;
            let mut bool_builder = BooleanBufferBuilder::new(num_values);
            bool_builder.append_n(num_values, true);

            let nth_offset = O::as_usize(self.offsets[n]);
            // Given offsets [0, 1, 2, 2], we could know that the 3rd index is null since the offset diff is 0
            let is_nth_offset_null = O::as_usize(self.offsets[n - 1]) == nth_offset;
            let mut new_nulls = vec![];
            self.nulls.iter().for_each(|null_index| {
                if *null_index < nth_offset
                    || (*null_index == nth_offset && is_nth_offset_null)
                {
                    nulls_count += 1;
                    bool_builder.set_bit(*null_index, false);
                } else {
                    new_nulls.push(null_index - nth_offset);
                }
            });

            self.nulls = new_nulls;
            Some(NullBuffer::from(bool_builder.finish()))
        };

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

        // Consume first (n - nulls count) of elements since we don't push any value for null case.
        let r = n - nulls_count;

        let mut remaining_buffer = BufferBuilder::new(self.buffer.len() - r);
        remaining_buffer.append_slice(&self.buffer.as_slice()[r..]);
        self.buffer.truncate(r);
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
