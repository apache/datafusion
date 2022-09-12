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
use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::Operator;

use std::sync::Arc;

/// The binary_bitwise_array_op macro only evaluates for integer types
/// like int64, int32.
/// It is used to do bitwise operation.
macro_rules! binary_bitwise_array_op {
    ($LEFT:expr, $RIGHT:expr, $METHOD:expr, $ARRAY_TYPE:ident) => {{
        let len = $LEFT.len();
        let left = $LEFT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let right = $RIGHT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let result = (0..len)
            .into_iter()
            .map(|i| {
                if left.is_null(i) || right.is_null(i) {
                    None
                } else {
                    Some($METHOD(left.value(i), right.value(i)))
                }
            })
            .collect::<$ARRAY_TYPE>();
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
            let result = (0..len)
                .into_iter()
                .map(|i| {
                    if array.is_null(i) {
                        None
                    } else {
                        Some($METHOD(array.value(i), right))
                    }
                })
                .collect::<$ARRAY_TYPE>();
            Ok(Arc::new(result) as ArrayRef)
        }
    }};
}

pub(crate) fn bitwise_and(left: ArrayRef, right: ArrayRef) -> Result<ArrayRef> {
    match &left.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_op!(left, right, |a, b| a & b, Int8Array)
        }
        DataType::Int16 => {
            binary_bitwise_array_op!(left, right, |a, b| a & b, Int16Array)
        }
        DataType::Int32 => {
            binary_bitwise_array_op!(left, right, |a, b| a & b, Int32Array)
        }
        DataType::Int64 => {
            binary_bitwise_array_op!(left, right, |a, b| a & b, Int64Array)
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseAnd
        ))),
    }
}

pub(crate) fn bitwise_shift_right(left: ArrayRef, right: ArrayRef) -> Result<ArrayRef> {
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
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseShiftRight
        ))),
    }
}

pub(crate) fn bitwise_shift_left(left: ArrayRef, right: ArrayRef) -> Result<ArrayRef> {
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
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseShiftLeft
        ))),
    }
}

pub(crate) fn bitwise_or(left: ArrayRef, right: ArrayRef) -> Result<ArrayRef> {
    match &left.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_op!(left, right, |a, b| a | b, Int8Array)
        }
        DataType::Int16 => {
            binary_bitwise_array_op!(left, right, |a, b| a | b, Int16Array)
        }
        DataType::Int32 => {
            binary_bitwise_array_op!(left, right, |a, b| a | b, Int32Array)
        }
        DataType::Int64 => {
            binary_bitwise_array_op!(left, right, |a, b| a | b, Int64Array)
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseOr
        ))),
    }
}

pub(crate) fn bitwise_xor(left: ArrayRef, right: ArrayRef) -> Result<ArrayRef> {
    match &left.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_op!(left, right, |a, b| a ^ b, Int8Array)
        }
        DataType::Int16 => {
            binary_bitwise_array_op!(left, right, |a, b| a ^ b, Int16Array)
        }
        DataType::Int32 => {
            binary_bitwise_array_op!(left, right, |a, b| a ^ b, Int32Array)
        }
        DataType::Int64 => {
            binary_bitwise_array_op!(left, right, |a, b| a ^ b, Int64Array)
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseXor
        ))),
    }
}

pub(crate) fn bitwise_and_scalar(
    array: &dyn Array,
    scalar: ScalarValue,
) -> Option<Result<ArrayRef>> {
    let result = match array.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_scalar!(array, scalar, |a, b| a & b, Int8Array, i8)
        }
        DataType::Int16 => {
            binary_bitwise_array_scalar!(array, scalar, |a, b| a & b, Int16Array, i16)
        }
        DataType::Int32 => {
            binary_bitwise_array_scalar!(array, scalar, |a, b| a & b, Int32Array, i32)
        }
        DataType::Int64 => {
            binary_bitwise_array_scalar!(array, scalar, |a, b| a & b, Int64Array, i64)
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseAnd
        ))),
    };
    Some(result)
}

pub(crate) fn bitwise_or_scalar(
    array: &dyn Array,
    scalar: ScalarValue,
) -> Option<Result<ArrayRef>> {
    let result = match array.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_scalar!(array, scalar, |a, b| a | b, Int8Array, i8)
        }
        DataType::Int16 => {
            binary_bitwise_array_scalar!(array, scalar, |a, b| a | b, Int16Array, i16)
        }
        DataType::Int32 => {
            binary_bitwise_array_scalar!(array, scalar, |a, b| a | b, Int32Array, i32)
        }
        DataType::Int64 => {
            binary_bitwise_array_scalar!(array, scalar, |a, b| a | b, Int64Array, i64)
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseOr
        ))),
    };
    Some(result)
}

pub(crate) fn bitwise_xor_scalar(
    array: &dyn Array,
    scalar: ScalarValue,
) -> Option<Result<ArrayRef>> {
    let result = match array.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_scalar!(array, scalar, |a, b| a ^ b, Int8Array, i8)
        }
        DataType::Int16 => {
            binary_bitwise_array_scalar!(array, scalar, |a, b| a ^ b, Int16Array, i16)
        }
        DataType::Int32 => {
            binary_bitwise_array_scalar!(array, scalar, |a, b| a ^ b, Int32Array, i32)
        }
        DataType::Int64 => {
            binary_bitwise_array_scalar!(array, scalar, |a, b| a ^ b, Int64Array, i64)
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseXor
        ))),
    };
    Some(result)
}

pub(crate) fn bitwise_shift_right_scalar(
    array: &dyn Array,
    scalar: ScalarValue,
) -> Option<Result<ArrayRef>> {
    let result = match array.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i8, b: i8| a.wrapping_shr(b as u32),
                Int8Array,
                i8
            )
        }
        DataType::Int16 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i16, b: i16| a.wrapping_shr(b as u32),
                Int16Array,
                i16
            )
        }
        DataType::Int32 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i32, b: i32| a.wrapping_shr(b as u32),
                Int32Array,
                i32
            )
        }
        DataType::Int64 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i64, b: i64| a.wrapping_shr(b as u32),
                Int64Array,
                i64
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

pub(crate) fn bitwise_shift_left_scalar(
    array: &dyn Array,
    scalar: ScalarValue,
) -> Option<Result<ArrayRef>> {
    let result = match array.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i8, b: i8| a.wrapping_shl(b as u32),
                Int8Array,
                i8
            )
        }
        DataType::Int16 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i16, b: i16| a.wrapping_shl(b as u32),
                Int16Array,
                i16
            )
        }
        DataType::Int32 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i32, b: i32| a.wrapping_shl(b as u32),
                Int32Array,
                i32
            )
        }
        DataType::Int64 => {
            binary_bitwise_array_scalar!(
                array,
                scalar,
                |a: i64, b: i64| a.wrapping_shl(b as u32),
                Int64Array,
                i64
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
