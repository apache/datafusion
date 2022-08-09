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

//! A trait to define from_slice functions for arrow types
//!
//! This file essentially exists to ease the transition onto arrow2

use arrow::array::{
    ArrayData, BooleanArray, GenericBinaryArray, GenericStringArray, OffsetSizeTrait,
    PrimitiveArray,
};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use arrow::util::bit_util;

/// A trait to define from_slice functions for arrow primitive array types
pub trait FromSlice<S, E>
where
    S: AsRef<[E]>,
{
    /// convert a slice of native types into a primitive array (without nulls)
    fn from_slice(slice: S) -> Self;
}

/// default implementation for primitive array types, adapted from `From<Vec<_>>`
impl<S, T> FromSlice<S, T::Native> for PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    S: AsRef<[T::Native]>,
{
    fn from_slice(slice: S) -> Self {
        Self::from_iter_values(slice.as_ref().iter().cloned())
    }
}

/// default implementation for binary array types, adapted from `From<Vec<_>>`
impl<S, I, OffsetSize> FromSlice<S, I> for GenericBinaryArray<OffsetSize>
where
    OffsetSize: OffsetSizeTrait,
    S: AsRef<[I]>,
    I: AsRef<[u8]>,
{
    /// convert a slice of byte slices into a binary array (without nulls)
    ///
    /// implementation details: here the Self::from_vec can be called but not without another copy
    fn from_slice(slice: S) -> Self {
        let slice = slice.as_ref();
        let mut offsets = Vec::with_capacity(slice.len() + 1);
        let mut values = Vec::new();
        let mut length_so_far: OffsetSize = OffsetSize::zero();
        offsets.push(length_so_far);
        for s in slice {
            let s = s.as_ref();
            length_so_far += OffsetSize::from_usize(s.len()).unwrap();
            offsets.push(length_so_far);
            values.extend_from_slice(s);
        }
        let array_data = ArrayData::builder(Self::DATA_TYPE)
            .len(slice.len())
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_buffer(Buffer::from_slice_ref(&values));
        let array_data = unsafe { array_data.build_unchecked() };
        Self::from(array_data)
    }
}

/// default implementation for utf8 array types, adapted from `From<Vec<_>>`
impl<S, I, OffsetSize> FromSlice<S, I> for GenericStringArray<OffsetSize>
where
    OffsetSize: OffsetSizeTrait,
    S: AsRef<[I]>,
    I: AsRef<str>,
{
    fn from_slice(slice: S) -> Self {
        Self::from_iter_values(slice.as_ref().iter())
    }
}

/// default implementation for boolean array type, adapted from `From<Vec<bool>>`
impl<S> FromSlice<S, bool> for BooleanArray
where
    S: AsRef<[bool]>,
{
    fn from_slice(slice: S) -> Self {
        let slice = slice.as_ref();
        let mut mut_buf = MutableBuffer::new_null(slice.len());
        {
            let mut_slice = mut_buf.as_slice_mut();
            for (i, b) in slice.iter().enumerate() {
                if *b {
                    bit_util::set_bit(mut_slice, i);
                }
            }
        }
        let array_data = ArrayData::builder(DataType::Boolean)
            .len(slice.len())
            .add_buffer(mut_buf.into());

        let array_data = unsafe { array_data.build_unchecked() };
        Self::from(array_data)
    }
}
