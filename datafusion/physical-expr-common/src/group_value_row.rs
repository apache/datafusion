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
use arrow::array::GenericBinaryArray;
use arrow::array::GenericBinaryBuilder;
use arrow::array::GenericStringArray;
use arrow::array::GenericStringBuilder;
use arrow::array::OffsetSizeTrait;
use arrow::array::PrimitiveArray;
use arrow::array::PrimitiveBuilder;
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray};
use arrow::buffer::Buffer;
use arrow::buffer::NullBuffer;
use arrow::buffer::OffsetBuffer;
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::ArrowNativeType;
use arrow::datatypes::ByteArrayType;
use arrow::datatypes::DataType;
use arrow::datatypes::GenericBinaryType;
use arrow::datatypes::GenericStringType;

use std::sync::Arc;

use crate::binary_map::OutputType;
use crate::binary_map::INITIAL_BUFFER_CAPACITY;

pub trait ArrayEq: Send + Sync {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool;
    fn append_val(&mut self, array: &ArrayRef, row: usize);
    fn len(&self) -> usize;
    fn build(self: Box<Self>) -> ArrayRef;
    fn take_n(&mut self, n: usize) -> ArrayRef;
}

pub struct PrimitiveGroupValueBuilder<T: ArrowPrimitiveType>(Vec<Option<T::Native>>);
impl<T: ArrowPrimitiveType> PrimitiveGroupValueBuilder<T> {
    pub fn new() -> Self {
        Self(vec![])
    }
}

impl<T: ArrowPrimitiveType> ArrayEq for PrimitiveGroupValueBuilder<T> {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        let elem = self.0[lhs_row];
        let arr = array.as_primitive::<T>();
        let is_rhs_null = arr.is_null(rhs_row);
        if elem.is_none() && is_rhs_null {
            true
        } else if elem.is_some() && !is_rhs_null {
            elem.unwrap() == arr.value(rhs_row)
        } else {
            false
        }
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) {
        let arr = array.as_primitive::<T>();
        if arr.is_null(row) {
            self.0.push(None)
        } else {
            let elem = arr.value(row);
            self.0.push(Some(elem))
        }
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        Arc::new(PrimitiveArray::<T>::from_iter(self.0))
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        let first_n = self.0.drain(0..n).collect::<Vec<_>>();
        Arc::new(PrimitiveArray::<T>::from_iter(first_n))
    }
}

pub struct ByteGroupValueBuilderNaive<O>
where
    O: OffsetSizeTrait,
{
    output_type: OutputType,
    // buffer: BufferBuilder<u8>,
    buffer: Vec<u8>,
    /// Offsets into `buffer` for each distinct  value. These offsets as used
    /// directly to create the final `GenericBinaryArray`. The `i`th string is
    /// stored in the range `offsets[i]..offsets[i+1]` in `buffer`. Null values
    /// are stored as a zero length string.
    offsets: Vec<O>,
    /// Null indexes in offsets
    nulls: Vec<usize>,
}

impl<O> ByteGroupValueBuilderNaive<O>
where
    O: OffsetSizeTrait,
{
    pub fn new(output_type: OutputType) -> Self {
        Self {
            output_type,
            buffer: Vec::with_capacity(INITIAL_BUFFER_CAPACITY),
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
        self.buffer.extend_from_slice(value);
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

impl<O> ArrayEq for ByteGroupValueBuilderNaive<O>
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

    fn build(self: Box<Self>) -> ArrayRef {
        let Self {
            output_type,
            buffer,
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
        let values = Buffer::from_vec(buffer);

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
                let res = Arc::new(unsafe {
                    GenericStringArray::new_unchecked(offsets, values, null_buffer)
                });
                res
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        let null_buffer = if self.nulls.is_empty() {
            None
        } else {
            // Only make a `NullBuffer` if there was a null value
            let num_values = self.offsets.len() - 1;
            let mut bool_builder = BooleanBufferBuilder::new(num_values);
            bool_builder.append_n(num_values, true);
            self.nulls.iter().for_each(|null_index| {
                bool_builder.set_bit(*null_index, false);
            });
            Some(NullBuffer::from(bool_builder.finish()))
        };

        // Given offests like [0, 2, 4, 5] and n = 1, we expect to get
        // offsets [0, 2, 3]. We first create two offsets for first_n as [0, 2] and the remaining as [2, 4, 5].
        // And we shift the offset starting from 0 for the remaining one, [2, 4, 5] -> [0, 2, 3].
        let mut first_n_offsets = self.offsets.drain(0..n).collect::<Vec<_>>();
        let offset_n = self.offsets.first().unwrap().clone();
        for offset in self.offsets.iter_mut() {
            *offset = offset.sub(offset_n);
        }
        first_n_offsets.push(offset_n);

        // SAFETY: the offsets were constructed correctly in `insert_if_new` --
        // monotonically increasing, overflows were checked.
        let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(first_n_offsets)) };

        // Consume first (n - nulls count) of elements since we don't push any value for null case.
        let first_n_valid_buffer = self.buffer.drain(0..(n - self.nulls.len())).collect();
        let values = Buffer::from_vec(first_n_valid_buffer);

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
                let res = Arc::new(unsafe {
                    GenericStringArray::new_unchecked(offsets, values, null_buffer)
                });
                res
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
    }
}

pub trait ArrayRowEq: Send + Sync {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool;
    fn append_val(&mut self, array: &ArrayRef, row: usize);
    fn len(&self) -> usize;
    // take n elements as ArrayRef and adjusted the underlying buffer
    fn take_n(&mut self, n: usize) -> ArrayRef;
    fn build(&mut self) -> ArrayRef;
}

impl<T> ArrayRowEq for PrimitiveBuilder<T>
where
    T: ArrowPrimitiveType,
{
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        let arr = array.as_primitive::<T>();

        if let Some(validity_slice) = self.validity_slice() {
            let validity_slice_index = lhs_row / 8;
            let bit_map_index = lhs_row % 8;
            let is_lhs_null = ((validity_slice[validity_slice_index] >> bit_map_index) & 1) == 0;
            if is_lhs_null {
                return arr.is_null(rhs_row);
            } else if arr.is_null(rhs_row) {
                return false;
            }
        }

        let elem = self.values_slice()[lhs_row];
        elem == arr.value(rhs_row)
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) {
        let arr = array.as_primitive::<T>();
        if arr.is_null(row) {
            self.append_null();
        } else {
            let elem = arr.value(row);
            self.append_value(elem);
        }
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        todo!("")

        // let num_remaining = self.values_slice().len() - n;
        // assert!(num_remaining >= 0);

        // let mut builder = PrimitiveBuilder::<T>::new();
        // let vs = self.values_slice();
        // builder.append_slice(vs);

        // let mut values_left = vec![T::default_value(); num_remaining];
        // let mut null_buffer_left = NullBuffer::new_null(num_remaining);

        // let null_buffer_left = self.validity_slice();

        // let len = self.len();
        // let nulls = self.null_buffer_builder.finish();
        // let builder = ArrayData::builder(self.data_type.clone())
        //     .len(len)
        //     .add_buffer(self.values_builder.finish())
        //     .nulls(nulls);

        // let array_data = unsafe { builder.build_unchecked() };
        // PrimitiveArray::<T>::from(array_data)

        // let output = self.finish();

        // let mut values_buffer = MutableBuffer::new(num_remaining);
        // values_buffer.extend_from_slice(&values_left);
        // let mut null_buffer = MutableBuffer::new_null(num_remaining);
        // null_buffer.extend_from_slice(items)
        

    }

    fn len(&self) -> usize {
        self.values_slice().len()
    }

    fn build(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<O> ArrayRowEq for GenericStringBuilder<O>
where
    O: OffsetSizeTrait,
{
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        let arr = array.as_bytes::<GenericStringType<O>>();
        if let Some(validity_slice) = self.validity_slice() {
            let validity_slice_index = lhs_row / 8;
            let bit_map_index = lhs_row % 8;
            let is_lhs_null = ((validity_slice[validity_slice_index] >> bit_map_index) & 1) == 0;
            if is_lhs_null {
                return arr.is_null(rhs_row);
            } else if arr.is_null(rhs_row) {
                return false;
            }
        }

        let rhs_elem: &[u8] = arr.value(rhs_row).as_ref();
        let rhs_elem_len = arr.value_length(rhs_row).as_usize();
        assert_eq!(rhs_elem_len, rhs_elem.len());
        let l = O::as_usize(self.offsets_slice()[lhs_row]);
        let r = O::as_usize(self.offsets_slice()[lhs_row + 1]);
        let existing_elem = &self.values_slice()[l..r];
        existing_elem.len() == rhs_elem.len() && rhs_elem == existing_elem
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) {
        let arr = array.as_string::<O>();
        if arr.is_null(row) {
            self.append_null();
            return;
        }

        let value = arr.value(row);
        self.append_value(value);
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        todo!("")
    }

    fn len(&self) -> usize {
        self.offsets_slice().len() - 1
    }

    fn build(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

impl<O> ArrayRowEq for GenericBinaryBuilder<O>
where
    O: OffsetSizeTrait,
{
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        let arr = array.as_bytes::<GenericBinaryType<O>>();
        if let Some(validity_slice) = self.validity_slice() {
            let validity_slice_index = lhs_row / 8;
            let bit_map_index = lhs_row % 8;
            let is_lhs_null = ((validity_slice[validity_slice_index] >> bit_map_index) & 1) == 0;
            if is_lhs_null {
                return arr.is_null(rhs_row);
            } else if arr.is_null(rhs_row) {
                return false;
            }
        }

        let rhs_elem: &[u8] = arr.value(rhs_row).as_ref();
        let rhs_elem_len = arr.value_length(rhs_row).as_usize();
        assert_eq!(rhs_elem_len, rhs_elem.len());
        let l = O::as_usize(self.offsets_slice()[lhs_row]);
        let r = O::as_usize(self.offsets_slice()[lhs_row + 1]);
        let existing_elem = &self.values_slice()[l..r];
        existing_elem.len() == rhs_elem.len() && rhs_elem == existing_elem
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) {
        let arr = array.as_binary::<O>();
        if arr.is_null(row) {
            self.append_null();
            return;
        }

        let value: &[u8] = arr.value(row).as_ref();
        self.append_value(value);
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        todo!("")
    }

    fn len(&self) -> usize {
        self.values_slice().len()
    }

    fn build(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }
}

#[cfg(test)]
mod tests {
    use arrow::{array::{GenericStringBuilder, PrimitiveBuilder}, datatypes::Int32Type};

    #[test]
    fn te1() {
        let mut a = PrimitiveBuilder::<Int32Type>::new();
        a.append_value(1);
        a.append_value(2);
        a.append_null();
        a.append_null();
        a.append_value(2);

        let s = a.values_slice();
        let v = a.validity_slice();
        println!("s: {:?}", s);
        println!("v: {:?}", v);

    }

    #[test]
    fn test123() {
        let mut a = GenericStringBuilder::<i32>::new();
        a.append_null();
        let p = a.offsets_slice();
        println!("p: {:?}", p);
        let s = a.validity_slice();
        println!("s: {:?}", s);
        a.append_null();
        let p = a.offsets_slice();
        println!("p: {:?}", p);
        let s = a.validity_slice();
        println!("s: {:?}", s);
        a.append_value("12");
        let p = a.offsets_slice();
        println!("p: {:?}", p);
        let s = a.validity_slice();
        println!("s: {:?}", s);
        a.append_null();
        let p = a.offsets_slice();
        println!("p: {:?}", p);
        let s = a.validity_slice();
        println!("s: {:?}", s);
    }
}