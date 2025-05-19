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
