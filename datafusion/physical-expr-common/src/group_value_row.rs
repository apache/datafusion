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
use arrow::buffer::NullBuffer;
use arrow::buffer::OffsetBuffer;
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::ArrowNativeType;
use arrow::datatypes::DataType;
use arrow::datatypes::GenericBinaryType;
use arrow::datatypes::GenericStringType;
use arrow::{
    array::{Array, ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray},
    datatypes::ByteArrayType,
};

use crate::binary_map::OutputType;
use crate::binary_map::INITIAL_BUFFER_CAPACITY;

use std::sync::Arc;

pub trait ArrayEq: Send + Sync {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool;
    fn append_val(&mut self, array: &ArrayRef, row: usize);
    fn len(&self) -> usize;
    fn build(self: Box<Self>) -> ArrayRef;
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
}

// pub struct StringGroupValueBuilder(Vec<Option<String>>);

// impl StringGroupValueBuilder {
//     pub fn new() -> Self {
//         Self(vec![])
//     }
// }

// impl ArrayEq for StringGroupValueBuilder {
//     fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
//         let elem = &self.0[lhs_row];
//         let arr = array.as_string::<i32>();
//         let is_rhs_null = arr.is_null(rhs_row);
//         if elem.is_none() && is_rhs_null {
//             true
//         } else if elem.is_some() && !is_rhs_null {
//             let e = elem.as_ref().unwrap();
//             e.as_str() == arr.value(rhs_row)
//         } else {
//             false
//         }
//     }

//     fn append_val(&mut self, array: &ArrayRef, row: usize) {
//         let arr = array.as_string::<i32>();
//         if arr.is_null(row) {
//             self.0.push(None)
//         } else {
//             let elem = arr.value(row);
//             self.0.push(Some(elem.to_string()))
//         }
//     }

//     fn len(&self) -> usize {
//         self.0.len()
//     }

//     fn build(self: Box<Self>) -> ArrayRef {
//         Arc::new(StringArray::from_iter(self.0))
//     }
// }

pub struct ByteGroupValueBuilderNaive<O>
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

impl<O> ByteGroupValueBuilderNaive<O>
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
                let res = Arc::new(unsafe {
                    GenericStringArray::new_unchecked(offsets, values, null_buffer)
                });
                res
            }
            _ => unreachable!("View types should use `ArrowBytesViewMap`"),
        }
    }
}

// pub struct ByteGroupValueBuilder<O>
// where
//     O: OffsetSizeTrait,
// {
//     output_type: OutputType,
//     /// Underlying hash set for each distinct value
//     map: hashbrown::raw::RawTable<EntryWithPayload<O>>,
//     /// Total size of the map in bytes
//     map_size: usize,
//     buffer: BufferBuilder<u8>,
//     /// Offsets into `buffer` for each distinct  value. These offsets as used
//     /// directly to create the final `GenericBinaryArray`. The `i`th string is
//     /// stored in the range `offsets[i]..offsets[i+1]` in `buffer`. Null values
//     /// are stored as a zero length string.
//     offsets: Vec<O>,
//     /// buffer that stores hash values (reused across batches to save allocations)
//     hashes_buffer: Vec<u64>,
//     /// Null indexes in offsets
//     nulls: Vec<usize>,
//     // Store the offset + len for group values
//     group_values_offset: Vec<Option<(usize, usize)>>,
// }

// impl<O> ByteGroupValueBuilder<O>
// where
//     O: OffsetSizeTrait,
// {
//     pub fn new(array: &ArrayRef, output_type: OutputType) -> Self {
//         let n_rows = array.len();
//         let random_state = RandomState::new();
//         let mut hashes_buffer = vec![];
//         let batch_hashes = &mut hashes_buffer;
//         batch_hashes.clear();
//         batch_hashes.resize(n_rows, 0);
//         create_hashes(&[array.to_owned()], &random_state, batch_hashes)
//             // hash is supported for all types and create_hashes only
//             // returns errors for unsupported types
//             .unwrap();

//         Self {
//             output_type,
//             map: hashbrown::raw::RawTable::with_capacity(INITIAL_MAP_CAPACITY),
//             map_size: 0,
//             buffer: BufferBuilder::new(INITIAL_BUFFER_CAPACITY),
//             offsets: vec![O::default()],
//             hashes_buffer,
//             nulls: vec![],
//             group_values_offset: vec![],
//         }
//     }

//     fn append_val_inner<B>(&mut self, array: &ArrayRef, row: usize)
//     where
//         B: ByteArrayType,
//     {
//         let arr = array.as_bytes::<B>();
//         if arr.is_null(row) {
//             self.nulls.push(self.offsets.len() - 1);
//             // nulls need a zero length in the offset buffer
//             let offset = self.buffer.len();
//             self.offsets.push(O::usize_as(offset));
//             return;
//         }

//         let hash = self.hashes_buffer[row];
//         let value: &[u8] = arr.value(row).as_ref();
//         let value_len = O::usize_as(value.len());

//         if value.len() <= SHORT_VALUE_LEN {
//             let inline = value.iter().fold(0usize, |acc, &x| acc << 8 | x as usize);
//             // is value is already present in the set?
//             let entry = self.map.get(hash, |header| {
//                 // compare value if hashes match
//                 if header.len != value_len {
//                     return false;
//                 }
//                 // value is stored inline so no need to consult buffer
//                 // (this is the "small string optimization")
//                 inline == header.offset_or_inline
//             });

//             // Put the small values into buffer and offsets so it appears
//             // the output array, but store the actual bytes inline for
//             // comparison
//             self.buffer.append_slice(value);
//             self.offsets.push(O::usize_as(self.buffer.len()));
//             if let Some(entry) = entry {
//             }
//             // if no existing entry, make a new one
//             else {
//                 // let payload = make_payload_fn(Some(value));
//                 let new_header = EntryWithPayload {
//                     hash,
//                     len: value_len,
//                     offset_or_inline: inline,
//                 };
//                 self.map.insert_accounted(
//                     new_header,
//                     |header| header.hash,
//                     &mut self.map_size,
//                 );
//             }
//         } else {
//             // Check if the value is already present in the set
//             let entry = self.map.get_mut(hash, |header| {
//                 // compare value if hashes match
//                 if header.len != value_len {
//                     return false;
//                 }
//                 // Need to compare the bytes in the buffer
//                 // SAFETY: buffer is only appended to, and we correctly inserted values and offsets
//                 let existing_value =
//                     unsafe { self.buffer.as_slice().get_unchecked(header.range()) };
//                 value == existing_value
//             });

//             let offset = self.buffer.len(); // offset of start for data
//             self.buffer.append_slice(value);
//             self.offsets.push(O::usize_as(self.buffer.len()));

//             if let Some(entry) = entry {
//             }
//             // if no existing entry, make a new header in map for equality check
//             else {
//                 let new_header = EntryWithPayload {
//                     hash,
//                     len: value_len,
//                     offset_or_inline: offset,
//                 };
//                 self.map.insert_accounted(
//                     new_header,
//                     |header| header.hash,
//                     &mut self.map_size,
//                 );
//             }
//         };
//     }

//     fn equal_to_inner<B>(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool
//     where
//         B: ByteArrayType,
//     {
//         // Handle nulls
//         let is_lhs_null = self.nulls.iter().any(|null_idx| *null_idx == lhs_row);
//         let arr = array.as_bytes::<B>();
//         if is_lhs_null {
//             return arr.is_null(rhs_row);
//         } else if arr.is_null(rhs_row) {
//             return false;
//         }

//         let hash = self.hashes_buffer[rhs_row];
//         let arr = array.as_bytes::<B>();
//         let rhs_elem: &[u8] = arr.value(rhs_row).as_ref();
//         let rhs_elem_len = O::usize_as(rhs_elem.len());
//         if rhs_elem.len() <= SHORT_VALUE_LEN {
//             let inline = rhs_elem
//                 .iter()
//                 .fold(0usize, |acc, &x| acc << 8 | x as usize);
//             // is value is already present in the set?
//             let entry = self.map.get(hash, |header| {
//                 // compare value if hashes match
//                 if header.len != rhs_elem_len {
//                     return false;
//                 }
//                 // value is stored inline so no need to consult buffer
//                 // (this is the "small string optimization")
//                 inline == header.offset_or_inline
//             });
//             entry.is_some()
//         } else {
//             // Check if the value is already present in the set
//             let entry = self.map.get(hash, |header| {
//                 // if header.hash != hash {
//                 //     return false;
//                 // }

//                 // compare value if hashes match
//                 if header.len != rhs_elem_len {
//                     return false;
//                 }
//                 // Need to compare the bytes in the buffer
//                 // SAFETY: buffer is only appended to, and we correctly inserted values and offsets
//                 let existing_elem =
//                     unsafe { self.buffer.as_slice().get_unchecked(header.range()) };
//                 rhs_elem == existing_elem
//             });
//             entry.is_some()
//         }
//     }
// }

// impl<O> ArrayEq for ByteGroupValueBuilder<O>
// where
//     O: OffsetSizeTrait,
// {
//     fn equal_to(&self, lhs_row: usize, column: &ArrayRef, rhs_row: usize) -> bool {
//         // Sanity array type
//         match self.output_type {
//             OutputType::Binary => {
//                 assert!(matches!(
//                     column.data_type(),
//                     DataType::Binary | DataType::LargeBinary
//                 ));
//                 self.equal_to_inner::<GenericBinaryType<O>>(lhs_row, column, rhs_row)
//             }
//             OutputType::Utf8 => {
//                 assert!(matches!(
//                     column.data_type(),
//                     DataType::Utf8 | DataType::LargeUtf8
//                 ));
//                 self.equal_to_inner::<GenericStringType<O>>(lhs_row, column, rhs_row)
//             }
//             _ => unreachable!("View types should use `ArrowBytesViewMap`"),
//         }
//     }

//     fn append_val(&mut self, column: &ArrayRef, row: usize) {
//         // Sanity array type
//         match self.output_type {
//             OutputType::Binary => {
//                 assert!(matches!(
//                     column.data_type(),
//                     DataType::Binary | DataType::LargeBinary
//                 ));
//                 self.append_val_inner::<GenericBinaryType<O>>(column, row)
//             }
//             OutputType::Utf8 => {
//                 assert!(matches!(
//                     column.data_type(),
//                     DataType::Utf8 | DataType::LargeUtf8
//                 ));
//                 self.append_val_inner::<GenericStringType<O>>(column, row)
//             }
//             _ => unreachable!("View types should use `ArrowBytesViewMap`"),
//         };
//     }

//     fn len(&self) -> usize {
//         self.offsets.len() - 1
//     }

//     fn build(self: Box<Self>) -> ArrayRef {
//         let Self {
//             map: _,
//             map_size: _,
//             mut buffer,
//             offsets,
//             hashes_buffer: _,
//             nulls,
//             output_type,
//             group_values_offset,
//         } = *self;

//         let null_buffer = if nulls.is_empty() {
//             None
//         } else {
//             // Only make a `NullBuffer` if there was a null value
//             let num_values = offsets.len() - 1;
//             let mut bool_builder = BooleanBufferBuilder::new(num_values);
//             bool_builder.append_n(num_values, true);
//             nulls.into_iter().for_each(|null_index| {
//                 bool_builder.set_bit(null_index, false);
//             });
//             Some(NullBuffer::from(bool_builder.finish()))
//         };

//         // let nulls = null.map(|null_index| {
//         //     let num_values = offsets.len() - 1;
//         //     single_null_buffer(num_values, null_index)
//         // });
//         // SAFETY: the offsets were constructed correctly in `insert_if_new` --
//         // monotonically increasing, overflows were checked.
//         let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };
//         let values = buffer.finish();

//         match output_type {
//             OutputType::Binary => {
//                 // SAFETY: the offsets were constructed correctly
//                 Arc::new(unsafe {
//                     GenericBinaryArray::new_unchecked(offsets, values, null_buffer)
//                 })
//             }
//             OutputType::Utf8 => {
//                 // SAFETY:
//                 // 1. the offsets were constructed safely
//                 //
//                 // 2. we asserted the input arrays were all the correct type and
//                 // thus since all the values that went in were valid (e.g. utf8)
//                 // so are all the values that come out
//                 let res = Arc::new(unsafe {
//                     GenericStringArray::new_unchecked(offsets, values, null_buffer)
//                 });
//                 res
//             }
//             _ => unreachable!("View types should use `ArrowBytesViewMap`"),
//         }
//     }
// }

// #[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
// struct EntryWithPayload<O>
// where
//     O: OffsetSizeTrait,
// {
//     /// hash of the value (stored to avoid recomputing it in hash table check)
//     hash: u64,
//     /// if len =< [`SHORT_VALUE_LEN`]: the data inlined
//     /// if len > [`SHORT_VALUE_LEN`], the offset of where the data starts
//     offset_or_inline: usize,
//     /// length of the value, in bytes (use O here so we use only i32 for
//     /// strings, rather 64 bit usize)
//     len: O,
// }

// impl<O> EntryWithPayload<O>
// where
//     O: OffsetSizeTrait,
// {
//     /// returns self.offset..self.offset + self.len
//     #[inline(always)]
//     fn range(&self) -> Range<usize> {
//         self.offset_or_inline..self.offset_or_inline + self.len.as_usize()
//     }
// }
