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

//! This module contains computation kernels that are specific to
//! datafusion and not (yet) targeted to  port upstream to arrow
use arrow::array::*;
use arrow::buffer::{MutableBuffer, NullBuffer};
use arrow::compute::kernels::bitwise::{
    bitwise_and, bitwise_and_scalar, bitwise_or, bitwise_or_scalar, bitwise_shift_left,
    bitwise_shift_left_scalar, bitwise_shift_right, bitwise_shift_right_scalar,
    bitwise_xor, bitwise_xor_scalar,
};
use arrow::datatypes::{i256, DataType};
use arrow_buffer::{ArrowNativeType, NullBufferBuilder};
use datafusion_common::plan_err;
use datafusion_common::{Result, ScalarValue};

use arrow_schema::ArrowError;
use std::sync::Arc;

/// Downcasts $LEFT and $RIGHT to $ARRAY_TYPE and then calls $KERNEL($LEFT, $RIGHT)
macro_rules! call_bitwise_kernel {
    ($LEFT:expr, $RIGHT:expr, $KERNEL:expr, $ARRAY_TYPE:ident) => {{
        let left = $LEFT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let right = $RIGHT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let result: $ARRAY_TYPE = $KERNEL(left, right)?;
        Ok(Arc::new(result))
    }};
}

/// Creates a $FUNC(left: ArrayRef, right: ArrayRef) that
/// downcasts left / right to the appropriate integral type and calls the kernel
macro_rules! create_dyn_kernel {
    ($FUNC:ident, $KERNEL:ident) => {
        pub(crate) fn $FUNC(left: ArrayRef, right: ArrayRef) -> Result<ArrayRef> {
            match &left.data_type() {
                DataType::Int8 => {
                    call_bitwise_kernel!(left, right, $KERNEL, Int8Array)
                }
                DataType::Int16 => {
                    call_bitwise_kernel!(left, right, $KERNEL, Int16Array)
                }
                DataType::Int32 => {
                    call_bitwise_kernel!(left, right, $KERNEL, Int32Array)
                }
                DataType::Int64 => {
                    call_bitwise_kernel!(left, right, $KERNEL, Int64Array)
                }
                DataType::UInt8 => {
                    call_bitwise_kernel!(left, right, $KERNEL, UInt8Array)
                }
                DataType::UInt16 => {
                    call_bitwise_kernel!(left, right, $KERNEL, UInt16Array)
                }
                DataType::UInt32 => {
                    call_bitwise_kernel!(left, right, $KERNEL, UInt32Array)
                }
                DataType::UInt64 => {
                    call_bitwise_kernel!(left, right, $KERNEL, UInt64Array)
                }
                other => plan_err!(
                    "Data type {:?} not supported for binary operation '{}' on dyn arrays",
                    other,
                    stringify!($KERNEL)
                ),
            }
        }
    };
}

create_dyn_kernel!(bitwise_or_dyn, bitwise_or);
create_dyn_kernel!(bitwise_xor_dyn, bitwise_xor);
create_dyn_kernel!(bitwise_and_dyn, bitwise_and);
create_dyn_kernel!(bitwise_shift_right_dyn, bitwise_shift_right);
create_dyn_kernel!(bitwise_shift_left_dyn, bitwise_shift_left);

/// Downcasts $LEFT as $ARRAY_TYPE and $RIGHT as TYPE and calls $KERNEL($LEFT, $RIGHT)
macro_rules! call_bitwise_scalar_kernel {
    ($LEFT:expr, $RIGHT:expr, $KERNEL:ident, $ARRAY_TYPE:ident, $TYPE:ty) => {{
        let len = $LEFT.len();
        let array = $LEFT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let scalar = $RIGHT;
        if scalar.is_null() {
            Ok(new_null_array(array.data_type(), len))
        } else {
            let scalar: $TYPE = scalar.try_into().unwrap();
            let result: $ARRAY_TYPE = $KERNEL(array, scalar).unwrap();
            Ok(Arc::new(result) as ArrayRef)
        }
    }};
}

/// Creates a $FUNC(left: ArrayRef, right: ScalarValue) that
/// downcasts left / right to the appropriate integral type and calls the kernel
macro_rules! create_dyn_scalar_kernel {
    ($FUNC:ident, $KERNEL:ident) => {
        pub(crate) fn $FUNC(array: &dyn Array, scalar: ScalarValue) -> Option<Result<ArrayRef>> {
            let result = match array.data_type() {
                DataType::Int8 => call_bitwise_scalar_kernel!(array, scalar, $KERNEL, Int8Array, i8),
                DataType::Int16 => call_bitwise_scalar_kernel!(array, scalar, $KERNEL, Int16Array, i16),
                DataType::Int32 => call_bitwise_scalar_kernel!(array, scalar, $KERNEL, Int32Array, i32),
                DataType::Int64 => call_bitwise_scalar_kernel!(array, scalar, $KERNEL, Int64Array, i64),
                DataType::UInt8 => call_bitwise_scalar_kernel!(array, scalar, $KERNEL, UInt8Array, u8),
                DataType::UInt16 => call_bitwise_scalar_kernel!(array, scalar, $KERNEL, UInt16Array, u16),
                DataType::UInt32 => call_bitwise_scalar_kernel!(array, scalar, $KERNEL, UInt32Array, u32),
                DataType::UInt64 => call_bitwise_scalar_kernel!(array, scalar, $KERNEL, UInt64Array, u64),
                other => plan_err!(
                    "Data type {:?} not supported for binary operation '{}' on dyn arrays",
                    other,
                    stringify!($KERNEL)
                ),
            };
            Some(result)
        }
    };
}

create_dyn_scalar_kernel!(bitwise_and_dyn_scalar, bitwise_and_scalar);
create_dyn_scalar_kernel!(bitwise_or_dyn_scalar, bitwise_or_scalar);
create_dyn_scalar_kernel!(bitwise_xor_dyn_scalar, bitwise_xor_scalar);
create_dyn_scalar_kernel!(bitwise_shift_right_dyn_scalar, bitwise_shift_right_scalar);
create_dyn_scalar_kernel!(bitwise_shift_left_dyn_scalar, bitwise_shift_left_scalar);

pub fn concat_elements_utf8view(
    left: &StringViewArray,
    right: &StringViewArray,
) -> std::result::Result<StringViewArray, ArrowError> {
    let capacity = left
        .data_buffers()
        .iter()
        .zip(right.data_buffers().iter())
        .map(|(b1, b2)| b1.len() + b2.len())
        .sum();
    let mut result = StringViewBuilder::with_capacity(capacity);

    // Avoid reallocations by writing to a reused buffer (note we
    // could be even more efficient r by creating the view directly
    // here and avoid the buffer but that would be more complex)
    let mut buffer = String::new();

    for (left, right) in left.iter().zip(right.iter()) {
        if let (Some(left), Some(right)) = (left, right) {
            use std::fmt::Write;
            buffer.clear();
            write!(&mut buffer, "{left}{right}")
                .expect("writing into string buffer failed");
            result.append_value(&buffer);
        } else {
            // at least one of the values is null, so the output is also null
            result.append_null()
        }
    }
    Ok(result.finish())
}

// NOCOMMIT I've just crammed these here. Gotta be a better place for them.

// NOCOMMIT steal the dispatch logic from arrow-arith-numeric if we're going to go this way.

trait ExtraFloat: Sized {
    fn div_if_greater_than_zero(self, rhs: Self) -> Option<Self>;
}

trait ExtraInt: Sized {
    fn div_if_greater_than_zero(self, rhs: Self) -> Result<Option<Self>, ArrowError>;
}

macro_rules! extra_float {
    ($t:tt) => {
        impl ExtraFloat for $t {
            #[inline]
            fn div_if_greater_than_zero(self, rhs: Self) -> Option<Self> {
                if rhs > Self::ZERO {
                    Some(self / rhs)
                } else {
                    None
                }
            }
        }
    };
}
extra_float!(f32);
extra_float!(f64);

macro_rules! extra_int {
    ($t:tt) => {
        impl ExtraInt for $t {
            #[inline]
            fn div_if_greater_than_zero(
                self,
                rhs: Self,
            ) -> Result<Option<Self>, ArrowError> {
                if rhs > Self::ZERO {
                    Ok(Some(self.checked_div(rhs).ok_or_else(|| {
                        ArrowError::ArithmeticOverflow(format!(
                            "Overflow happened on: {:?} / {:?}",
                            self, rhs
                        ))
                    })?))
                } else {
                    Ok(None)
                }
            }
        }
    };
}
extra_int!(i8);
extra_int!(i16);
extra_int!(i32);
extra_int!(i64);
extra_int!(i128);
extra_int!(i256);
extra_int!(u8);
extra_int!(u16);
extra_int!(u32);
extra_int!(u64);

// NOCOMMIT the try_op dance where we turn these into unary functions

///// Copies of arity.rs kernel dispatch methods that handle Option in results.
///// NOCOMMIT are there fast? how can we tell?
fn binary_optional<A: ArrayAccessor, B: ArrayAccessor, F, O>(
    a: A,
    b: B,
    op: F,
) -> Result<PrimitiveArray<O>, ArrowError>
where
    O: ArrowPrimitiveType,
    F: Fn(A::Item, B::Item) -> Option<O::Native>,
{
    if a.len() != b.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform a binary operation on arrays of different length".to_string(),
        ));
    }
    if a.is_empty() {
        return Ok(PrimitiveArray::from(ArrayData::new_empty(&O::DATA_TYPE)));
    }
    let len = a.len();

    if a.null_count() == 0 && b.null_count() == 0 {
        binary_optional_no_nulls(len, a, b, op)
    } else {
        todo!();
    }
}

#[inline(never)]
fn binary_optional_no_nulls<A: ArrayAccessor, B: ArrayAccessor, F, O>(
    len: usize,
    a: A,
    b: B,
    op: F,
) -> Result<PrimitiveArray<O>, ArrowError>
where
    O: ArrowPrimitiveType,
    F: Fn(A::Item, B::Item) -> Option<O::Native>,
{
    let mut nulls = NullBufferBuilder::new(len);
    let mut buffer = MutableBuffer::new(len * O::Native::get_byte_width());
    for idx in 0..len {
        if let Some(result) =
            unsafe { op(a.value_unchecked(idx), b.value_unchecked(idx)) }
        {
            unsafe {
                buffer.push_unchecked(result);
            }
            nulls.append_non_null();
        } else {
            nulls.append_null();
        }
    }
    Ok(PrimitiveArray::new(buffer.into(), nulls.finish()))
}

fn try_binary_optional<A: ArrayAccessor, B: ArrayAccessor, F, O>(
    a: A,
    b: B,
    op: F,
) -> Result<PrimitiveArray<O>, ArrowError>
where
    O: ArrowPrimitiveType,
    F: Fn(A::Item, B::Item) -> Result<Option<O::Native>, ArrowError>,
{
    if a.len() != b.len() {
        return Err(ArrowError::ComputeError(
            "Cannot perform a binary operation on arrays of different length".to_string(),
        ));
    }
    if a.is_empty() {
        return Ok(PrimitiveArray::from(ArrayData::new_empty(&O::DATA_TYPE)));
    }
    let len = a.len();

    if a.null_count() == 0 && b.null_count() == 0 {
        try_binary_optional_no_nulls(len, a, b, op)
    } else {
        todo!();
    }
}

#[inline(never)]
fn try_binary_optional_no_nulls<A: ArrayAccessor, B: ArrayAccessor, F, O>(
    len: usize,
    a: A,
    b: B,
    op: F,
) -> Result<PrimitiveArray<O>, ArrowError>
where
    O: ArrowPrimitiveType,
    F: Fn(A::Item, B::Item) -> Result<Option<O::Native>, ArrowError>,
{
    let mut nulls = NullBufferBuilder::new(len);
    let mut buffer = MutableBuffer::new(len * O::Native::get_byte_width());
    for idx in 0..len {
        if let Some(result) =
            unsafe { op(a.value_unchecked(idx), b.value_unchecked(idx))? }
        {
            unsafe {
                buffer.push_unchecked(result);
            }
            nulls.append_non_null();
        } else {
            nulls.append_null();
        }
    }
    Ok(PrimitiveArray::new(buffer.into(), nulls.finish()))
}
