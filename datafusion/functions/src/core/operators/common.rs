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

//! Common utilities for operators.
// copy from physical-expr/src/expressions/binary/kernels.rs

use arrow::array::*;
use arrow::compute::cast;
use arrow::datatypes::*;
use datafusion_common::{Result, internal_err};

use datafusion_expr::Operator;

/// Downcasts $LEFT and $RIGHT to $ARRAY_TYPE and then calls $KERNEL($LEFT, $RIGHT)
macro_rules! call_kernel {
    ($LEFT:expr, $RIGHT:expr, $KERNEL:expr, $ARRAY_TYPE:ident) => {{
        let left = $LEFT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let right = $RIGHT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let result: $ARRAY_TYPE = $KERNEL(left, right)?;
        Ok(Arc::new(result))
    }};
}

/// Creates a $FUNC(left: ArrayRef, right: ArrayRef) that
/// downcasts left / right to the appropriate integral type and calls the kernel
macro_rules! create_left_integral_dyn_kernel {
    ($FUNC:ident, $KERNEL:ident) => {
        pub(crate) fn $FUNC(left: ArrayRef, right: ArrayRef) -> Result<ArrayRef> {
            match &left.data_type() {
                DataType::Int8 => {
                    call_kernel!(left, right, $KERNEL, Int8Array)
                }
                DataType::Int16 => {
                    call_kernel!(left, right, $KERNEL, Int16Array)
                }
                DataType::Int32 => {
                    call_kernel!(left, right, $KERNEL, Int32Array)
                }
                DataType::Int64 => {
                    call_kernel!(left, right, $KERNEL, Int64Array)
                }
                DataType::UInt8 => {
                    call_kernel!(left, right, $KERNEL, UInt8Array)
                }
                DataType::UInt16 => {
                    call_kernel!(left, right, $KERNEL, UInt16Array)
                }
                DataType::UInt32 => {
                    call_kernel!(left, right, $KERNEL, UInt32Array)
                }
                DataType::UInt64 => {
                    call_kernel!(left, right, $KERNEL, UInt64Array)
                }
                other => plan_err!(
                    "Data type {} not supported for binary operation '{}' on dyn arrays",
                    other,
                    stringify!($KERNEL)
                ),
            }
        }
    };
}

/// Downcasts $LEFT as $ARRAY_TYPE and $RIGHT as TYPE and calls $KERNEL($LEFT, $RIGHT)
macro_rules! call_scalar_kernel {
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
macro_rules! create_left_integral_dyn_scalar_kernel {
    ($FUNC:ident, $KERNEL:ident) => {
        pub(crate) fn $FUNC(
            array: &dyn Array,
            scalar: ScalarValue,
        ) -> Option<Result<ArrayRef>> {
            let result = match array.data_type() {
                DataType::Int8 => {
                    call_scalar_kernel!(array, scalar, $KERNEL, Int8Array, i8)
                }
                DataType::Int16 => {
                    call_scalar_kernel!(array, scalar, $KERNEL, Int16Array, i16)
                }
                DataType::Int32 => {
                    call_scalar_kernel!(array, scalar, $KERNEL, Int32Array, i32)
                }
                DataType::Int64 => {
                    call_scalar_kernel!(array, scalar, $KERNEL, Int64Array, i64)
                }
                DataType::UInt8 => {
                    call_scalar_kernel!(array, scalar, $KERNEL, UInt8Array, u8)
                }
                DataType::UInt16 => {
                    call_scalar_kernel!(array, scalar, $KERNEL, UInt16Array, u16)
                }
                DataType::UInt32 => {
                    call_scalar_kernel!(array, scalar, $KERNEL, UInt32Array, u32)
                }
                DataType::UInt64 => {
                    call_scalar_kernel!(array, scalar, $KERNEL, UInt64Array, u64)
                }
                other => plan_err!(
                    "Data type {} not supported for binary operation '{}' on dyn arrays",
                    other,
                    stringify!($KERNEL)
                ),
            };
            Some(result)
        }
    };
}

/// Casts dictionary array to result type for binary numerical operators. Such operators
/// between array and scalar produce a dictionary array other than primitive array of the
/// same operators between array and array. This leads to inconsistent result types causing
/// errors in the following query execution. For such operators between array and scalar,
/// we cast the dictionary array to primitive array.
pub fn to_result_type_array(
    op: &Operator,
    array: ArrayRef,
    result_type: &DataType,
) -> Result<ArrayRef> {
    if array.data_type() == result_type {
        Ok(array)
    } else if op.is_numerical_operators() {
        match array.data_type() {
            DataType::Dictionary(_, value_type) => {
                if value_type.as_ref() == result_type {
                    Ok(cast(&array, result_type)?)
                } else {
                    internal_err!(
                        "Incompatible Dictionary value type {value_type} with result type {result_type} of Binary operator {op:?}"
                    )
                }
            }
            _ => Ok(array),
        }
    } else {
        Ok(array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use datafusion_expr::Operator;
    use arrow::array::{Int8Array, Int32Array, ArrayRef, DictionaryArray};

    #[test]
    fn test_to_result_type_array() {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let keys = Int8Array::from(vec![Some(0), None, Some(2), Some(3)]);
        let dictionary =
            Arc::new(DictionaryArray::try_new(keys, values).unwrap()) as ArrayRef;

        // Casting Dictionary to Int32
        let casted = to_result_type_array(
            &Operator::Plus,
            Arc::clone(&dictionary),
            &DataType::Int32,
        )
        .unwrap();
        assert_eq!(
            &casted,
            &(Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(4)]))
                as ArrayRef)
        );

        // Array has same datatype as result type, no casting
        let casted = to_result_type_array(
            &Operator::Plus,
            Arc::clone(&dictionary),
            dictionary.data_type(),
        )
        .unwrap();
        assert_eq!(&casted, &dictionary);

        // Not numerical operator, no casting
        let casted = to_result_type_array(
            &Operator::Eq,
            Arc::clone(&dictionary),
            &DataType::Int32,
        )
        .unwrap();
        assert_eq!(&casted, &dictionary);
    }
}