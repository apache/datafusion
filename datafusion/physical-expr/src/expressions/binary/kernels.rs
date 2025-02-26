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
use arrow::compute::kernels::bitwise::{
    bitwise_and, bitwise_and_scalar, bitwise_or, bitwise_or_scalar, bitwise_shift_left,
    bitwise_shift_left_scalar, bitwise_shift_right, bitwise_shift_right_scalar,
    bitwise_xor, bitwise_xor_scalar,
};
use arrow::datatypes::DataType;
use datafusion_common::plan_err;
use datafusion_common::{Result, ScalarValue};

use arrow::error::ArrowError;
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
