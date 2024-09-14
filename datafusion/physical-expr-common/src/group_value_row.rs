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

use arrow::array::GenericBinaryBuilder;
use arrow::array::GenericStringBuilder;
use arrow::array::OffsetSizeTrait;
use arrow::array::PrimitiveBuilder;
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray};
use arrow::datatypes::GenericBinaryType;
use arrow::datatypes::GenericStringType;

use std::sync::Arc;

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