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

use arrow::array::{ArrayRef, BooleanArray, Float32Array, Int32Array, StringArray};

/// Converts a vector or array into an ArrayRef.
pub trait IntoArrayRef {
    fn into_array_ref(self: Box<Self>) -> ArrayRef;
}

impl IntoArrayRef for Vec<i32> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(Int32Array::from(*self))
    }
}

impl IntoArrayRef for Vec<Option<i32>> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(Int32Array::from(*self))
    }
}

impl<const N: usize> IntoArrayRef for [i32; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(Int32Array::from(Vec::from(*self)))
    }
}

impl<const N: usize> IntoArrayRef for [Option<i32>; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(Int32Array::from(Vec::from(*self)))
    }
}

impl IntoArrayRef for Vec<f32> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(Float32Array::from(*self))
    }
}

impl IntoArrayRef for Vec<Option<f32>> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(Float32Array::from(*self))
    }
}

impl<const N: usize> IntoArrayRef for [f32; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(Float32Array::from(Vec::from(*self)))
    }
}

impl<const N: usize> IntoArrayRef for [Option<f32>; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(Float32Array::from(Vec::from(*self)))
    }
}

impl IntoArrayRef for Vec<&str> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(StringArray::from(*self))
    }
}

impl IntoArrayRef for Vec<Option<&str>> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(StringArray::from(*self))
    }
}

impl<const N: usize> IntoArrayRef for [&'static str; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(StringArray::from(Vec::from(*self)))
    }
}

impl<const N: usize> IntoArrayRef for [Option<&'static str>; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(StringArray::from(Vec::from(*self)))
    }
}

impl IntoArrayRef for Vec<String> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(StringArray::from(*self))
    }
}

impl IntoArrayRef for Vec<Option<String>> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(StringArray::from(*self))
    }
}

impl<const N: usize> IntoArrayRef for [String; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(StringArray::from(Vec::from(*self)))
    }
}

impl<const N: usize> IntoArrayRef for [Option<String>; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(StringArray::from(Vec::from(*self)))
    }
}

impl IntoArrayRef for Vec<bool> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(BooleanArray::from(*self))
    }
}

impl IntoArrayRef for Vec<Option<bool>> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(BooleanArray::from(*self))
    }
}

impl<const N: usize> IntoArrayRef for [bool; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(BooleanArray::from(Vec::from(*self)))
    }
}

impl<const N: usize> IntoArrayRef for [Option<bool>; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(BooleanArray::from(Vec::from(*self)))
    }
}
