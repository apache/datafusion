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
use arrow::compute::binary;
use arrow::compute::kernels::bitwise::{
    bitwise_and, bitwise_and_scalar, bitwise_or, bitwise_or_scalar, bitwise_xor,
    bitwise_xor_scalar,
};
use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::Operator;

use std::sync::Arc;

/// The binary_bitwise_array_op macro only evaluates for integer types
/// like int64, int32.
/// It is used to do bitwise operation.
macro_rules! binary_bitwise_array_op {
    ($LEFT:expr, $RIGHT:expr, $METHOD:expr, $ARRAY_TYPE:ident) => {{
        let left = $LEFT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let right = $RIGHT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let result: $ARRAY_TYPE = binary(left, right, $METHOD)?;
        Ok(Arc::new(result))
    }};
}

/// The binary_bitwise_array_op macro only evaluates for integer types
/// like int64, int32.
/// It is used to do bitwise operation on an array with a scalar.
macro_rules! binary_bitwise_array_scalar {
    ($LEFT:expr, $RIGHT:expr, $METHOD:expr, $ARRAY_TYPE:ident, $TYPE:ty) => {{
        let len = $LEFT.len();
        let array = $LEFT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let scalar = $RIGHT;
        if scalar.is_null() {
            Ok(new_null_array(array.data_type(), len))
        } else {
            let right: $TYPE = scalar.try_into().unwrap();
            let method = $METHOD(right);
            let result: $ARRAY_TYPE = array.unary(method);
            Ok(Arc::new(result) as ArrayRef)
        }
    }};
}

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
                other => Err(DataFusionError::Internal(format!(
                    "Data type {:?} not supported for binary operation '{}' on dyn arrays",
                    other,
                    stringify!($KERNEL),
                ))),
            }
        }
    };
}

create_dyn_kernel!(bitwise_or_dyn, bitwise_or);
create_dyn_kernel!(bitwise_xor_dyn, bitwise_xor);
create_dyn_kernel!(bitwise_and_dyn, bitwise_and);

// TODO: use create_dyn_kernel! when  https://github.com/apache/arrow-rs/issues/2741 is implemented
pub(crate) fn bitwise_shift_right_dyn(
    left: ArrayRef,
    right: ArrayRef,
) -> Result<ArrayRef> {
    match &left.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: i8, b: i8| a.wrapping_shr(b as u32),
                Int8Array
            )
        }
        DataType::Int16 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: i16, b: i16| a.wrapping_shr(b as u32),
                Int16Array
            )
        }
        DataType::Int32 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: i32, b: i32| a.wrapping_shr(b as u32),
                Int32Array
            )
        }
        DataType::Int64 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: i64, b: i64| a.wrapping_shr(b as u32),
                Int64Array
            )
        }
        DataType::UInt8 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: u8, b: u8| a.wrapping_shr(b as u32),
                UInt8Array
            )
        }
        DataType::UInt16 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: u16, b: u16| a.wrapping_shr(b as u32),
                UInt16Array
            )
        }
        DataType::UInt32 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: u32, b: u32| a.wrapping_shr(b),
                UInt32Array
            )
        }
        DataType::UInt64 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: u64, b: u64| a.wrapping_shr(b.try_into().unwrap()),
                UInt64Array
            )
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseShiftRight
        ))),
    }
}

// TODO: use create_dyn_kernel! when  https://github.com/apache/arrow-rs/issues/2741 is implemented
pub(crate) fn bitwise_shift_left_dyn(
    left: ArrayRef,
    right: ArrayRef,
) -> Result<ArrayRef> {
    match &left.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: i8, b: i8| a.wrapping_shl(b as u32),
                Int8Array
            )
        }
        DataType::Int16 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: i16, b: i16| a.wrapping_shl(b as u32),
                Int16Array
            )
        }
        DataType::Int32 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: i32, b: i32| a.wrapping_shl(b as u32),
                Int32Array
            )
        }
        DataType::Int64 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: i64, b: i64| a.wrapping_shl(b as u32),
                Int64Array
            )
        }
        DataType::UInt8 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: u8, b: u8| a.wrapping_shl(b as u32),
                UInt8Array
            )
        }
        DataType::UInt16 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: u16, b: u16| a.wrapping_shl(b as u32),
                UInt16Array
            )
        }
        DataType::UInt32 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: u32, b: u32| a.wrapping_shl(b),
                UInt32Array
            )
        }
        DataType::UInt64 => {
            binary_bitwise_array_op!(
                left,
                right,
                |a: u64, b: u64| a.wrapping_shr(b.try_into().unwrap()),
                UInt64Array
            )
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseShiftLeft
        ))),
    }
}

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
                other => Err(DataFusionError::Internal(format!(
                    "Data type {:?} not supported for binary operation '{}' on dyn arrays",
                    other,
                    stringify!($KERNEL),
                ))),
            };
            Some(result)
        }
    };
}

create_dyn_scalar_kernel!(bitwise_and_dyn_scalar, bitwise_and_scalar);
create_dyn_scalar_kernel!(bitwise_or_dyn_scalar, bitwise_or_scalar);
create_dyn_scalar_kernel!(bitwise_xor_dyn_scalar, bitwise_xor_scalar);

// TODO: use create_dyn_scalar_kernel! when  https://github.com/apache/arrow-rs/issues/2741 is implemented
pub(crate) fn bitwise_shift_right_dyn_scalar(
    array: &dyn Array,
    scalar: ScalarValue,
) -> Option<Result<ArrayRef>> {
    let result = match array.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i8| move |b: i8| b.wrapping_shr(a as u32),
                Int8Array,
                i8
            )
        }
        DataType::Int16 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i16| move |b: i16| b.wrapping_shr(a as u32),
                Int16Array,
                i16
            )
        }
        DataType::Int32 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i32| move |b: i32| b.wrapping_shr(a as u32),
                Int32Array,
                i32
            )
        }
        DataType::Int64 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i64| move |b: i64| b.wrapping_shr(a as u32),
                Int64Array,
                i64
            )
        }
        DataType::UInt8 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: u8| move |b: u8| b.wrapping_shr(a as u32),
                UInt8Array,
                u8
            )
        }
        DataType::UInt16 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: u16| move |b: u16| b.wrapping_shr(a as u32),
                UInt16Array,
                u16
            )
        }
        DataType::UInt32 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: u32| move |b: u32| b.wrapping_shr(a),
                UInt32Array,
                u32
            )
        }
        DataType::UInt64 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: u32| move |b: u64| b.wrapping_shr(a),
                UInt64Array,
                u32
            )
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseShiftRight
        ))),
    };
    Some(result)
}

// TODO: use create_dyn_scalar_kernel! when  https://github.com/apache/arrow-rs/issues/2741 is implemented
pub(crate) fn bitwise_shift_left_dyn_scalar(
    array: &dyn Array,
    scalar: ScalarValue,
) -> Option<Result<ArrayRef>> {
    let result = match array.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i8| move |b: i8| b.wrapping_shl(a as u32),
                Int8Array,
                i8
            )
        }
        DataType::Int16 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i16| move |b: i16| b.wrapping_shl(a as u32),
                Int16Array,
                i16
            )
        }
        DataType::Int32 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i32| move |b: i32| b.wrapping_shl(a as u32),
                Int32Array,
                i32
            )
        }
        DataType::Int64 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i64| move |b: i64| b.wrapping_shl(a as u32),
                Int64Array,
                i64
            )
        }
        DataType::UInt8 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: u8| move |b: u8| b.wrapping_shl(a as u32),
                UInt8Array,
                u8
            )
        }
        DataType::UInt16 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: u16| move |b: u16| b.wrapping_shl(a as u32),
                UInt16Array,
                u16
            )
        }
        DataType::UInt32 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: u32| move |b: u32| b.wrapping_shl(a),
                UInt32Array,
                u32
            )
        }
        DataType::UInt64 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: u32| move |b: u64| b.wrapping_shr(a),
                UInt64Array,
                u32
            )
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseShiftLeft
        ))),
    };
    Some(result)
}
