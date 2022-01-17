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

use arrow::array::{ArrayData, PrimitiveArray};
use arrow::buffer::Buffer;
use arrow::datatypes::ArrowPrimitiveType;

/// A trait to define from_slice functions for arrow primitive array types
pub trait FromSlice<T>
where
    T: ArrowPrimitiveType,
{
    /// convert a slice of native types into a primitive array (without nulls)
    fn from_slice(slice: &[T::Native]) -> PrimitiveArray<T>;
}

/// default implementation for primitive types
// #[cfg(test)]
impl<T: ArrowPrimitiveType> FromSlice<T> for PrimitiveArray<T> {
    fn from_slice(slice: &[T::Native]) -> PrimitiveArray<T> {
        let array_data = ArrayData::builder(T::DATA_TYPE)
            .len(slice.len())
            .add_buffer(Buffer::from_slice_ref(&slice));
        let array_data = unsafe { array_data.build_unchecked() };
        PrimitiveArray::<T>::from(array_data)
    }
}
