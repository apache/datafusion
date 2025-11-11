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
use arrow::compute::kernels::boolean::not;
use arrow::compute::kernels::comparison::{regexp_is_match, regexp_is_match_scalar};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use datafusion_common::{internal_err, plan_err};
use datafusion_common::{Result, ScalarValue};

use std::sync::Arc;

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

create_left_integral_dyn_kernel!(bitwise_or_dyn, bitwise_or);
create_left_integral_dyn_kernel!(bitwise_xor_dyn, bitwise_xor);
create_left_integral_dyn_kernel!(bitwise_and_dyn, bitwise_and);
create_left_integral_dyn_kernel!(bitwise_shift_right_dyn, bitwise_shift_right);
create_left_integral_dyn_kernel!(bitwise_shift_left_dyn, bitwise_shift_left);

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
        pub(crate) fn $FUNC(array: &dyn Array, scalar: ScalarValue) -> Option<Result<ArrayRef>> {
            let result = match array.data_type() {
                DataType::Int8 => call_scalar_kernel!(array, scalar, $KERNEL, Int8Array, i8),
                DataType::Int16 => call_scalar_kernel!(array, scalar, $KERNEL, Int16Array, i16),
                DataType::Int32 => call_scalar_kernel!(array, scalar, $KERNEL, Int32Array, i32),
                DataType::Int64 => call_scalar_kernel!(array, scalar, $KERNEL, Int64Array, i64),
                DataType::UInt8 => call_scalar_kernel!(array, scalar, $KERNEL, UInt8Array, u8),
                DataType::UInt16 => call_scalar_kernel!(array, scalar, $KERNEL, UInt16Array, u16),
                DataType::UInt32 => call_scalar_kernel!(array, scalar, $KERNEL, UInt32Array, u32),
                DataType::UInt64 => call_scalar_kernel!(array, scalar, $KERNEL, UInt64Array, u64),
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

create_left_integral_dyn_scalar_kernel!(bitwise_and_dyn_scalar, bitwise_and_scalar);
create_left_integral_dyn_scalar_kernel!(bitwise_or_dyn_scalar, bitwise_or_scalar);
create_left_integral_dyn_scalar_kernel!(bitwise_xor_dyn_scalar, bitwise_xor_scalar);
create_left_integral_dyn_scalar_kernel!(
    bitwise_shift_right_dyn_scalar,
    bitwise_shift_right_scalar
);
create_left_integral_dyn_scalar_kernel!(
    bitwise_shift_left_dyn_scalar,
    bitwise_shift_left_scalar
);

/// Concatenates two `StringViewArray`s element-wise.  
/// If either element is `Null`, the result element is also `Null`.
///
/// # Errors
/// - Returns an error if the input arrays have different lengths.  
/// - Returns an error if any concatenated string exceeds `u32::MAX` (≈4 GB) in length.
pub fn concat_elements_utf8view(
    left: &StringViewArray,
    right: &StringViewArray,
) -> std::result::Result<StringViewArray, ArrowError> {
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(format!(
            "Arrays must have the same length: {} != {}",
            left.len(),
            right.len()
        )));
    }
    let capacity = left.len();
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
            result.try_append_value(&buffer)?;
        } else {
            // at least one of the values is null, so the output is also null
            result.append_null()
        }
    }
    Ok(result.finish())
}

/// Invoke a compute kernel on a pair of binary data arrays with flags
macro_rules! regexp_is_match_flag {
    ($LEFT:expr, $RIGHT:expr, $ARRAYTYPE:ident, $NOT:expr, $FLAG:expr) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$ARRAYTYPE>()
            .expect("failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<$ARRAYTYPE>()
            .expect("failed to downcast array");

        let flag = if $FLAG {
            Some($ARRAYTYPE::from(vec!["i"; ll.len()]))
        } else {
            None
        };
        let mut array = regexp_is_match(ll, rr, flag.as_ref())?;
        if $NOT {
            array = not(&array).unwrap();
        }
        Ok(Arc::new(array))
    }};
}

pub(crate) fn regex_match_dyn(
    left: &ArrayRef,
    right: &ArrayRef,
    not_match: bool,
    flag: bool,
) -> Result<ArrayRef> {
    match left.data_type() {
        DataType::Utf8 => {
            regexp_is_match_flag!(left, right, StringArray, not_match, flag)
        }
        DataType::Utf8View => {
            regexp_is_match_flag!(left, right, StringViewArray, not_match, flag)
        }
        DataType::LargeUtf8 => {
            regexp_is_match_flag!(left, right, LargeStringArray, not_match, flag)
        }
        other => internal_err!(
            "Data type {} not supported for regex_match_dyn on string array",
            other
        ),
    }
}

/// Invoke a compute kernel on a data array and a scalar value with flag
macro_rules! regexp_is_match_flag_scalar {
    ($LEFT:expr, $RIGHT:expr, $ARRAYTYPE:ident, $NOT:expr, $FLAG:expr) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$ARRAYTYPE>()
            .expect("failed to downcast array");

        if let Some(Some(string_value)) = $RIGHT.try_as_str() {
            let flag = $FLAG.then_some("i");
            match regexp_is_match_scalar(ll, &string_value, flag) {
                Ok(mut array) => {
                    if $NOT {
                        array = not(&array).unwrap();
                    }
                    Ok(Arc::new(array))
                }
                Err(e) => internal_err!("failed to call 'regex_match_dyn_scalar' {}", e),
            }
        } else {
            internal_err!(
                "failed to cast literal value {} for operation 'regex_match_dyn_scalar'",
                $RIGHT
            )
        }
    }};
}

pub(crate) fn regex_match_dyn_scalar(
    left: &dyn Array,
    right: &ScalarValue,
    not_match: bool,
    flag: bool,
) -> Option<Result<ArrayRef>> {
    let result: Result<ArrayRef> = match left.data_type() {
        DataType::Utf8 => {
            regexp_is_match_flag_scalar!(left, right, StringArray, not_match, flag)
        }
        DataType::Utf8View => {
            regexp_is_match_flag_scalar!(left, right, StringViewArray, not_match, flag)
        }
        DataType::LargeUtf8 => {
            regexp_is_match_flag_scalar!(left, right, LargeStringArray, not_match, flag)
        }
        DataType::Dictionary(_, _) => {
            let values = left.as_any_dictionary().values();

            match values.data_type() {
                DataType::Utf8 => regexp_is_match_flag_scalar!(values, right, StringArray, not_match, flag),
                DataType::Utf8View => regexp_is_match_flag_scalar!(values, right, StringViewArray, not_match, flag),
                DataType::LargeUtf8 => regexp_is_match_flag_scalar!(values, right, LargeStringArray, not_match, flag),
                other => internal_err!(
                    "Data type {} not supported as a dictionary value type for operation 'regex_match_dyn_scalar' on string array",
                    other
                ),
            }.map(
                // downcast_dictionary_array duplicates code per possible key type, so we aim to do all prep work before
                |evaluated_values| downcast_dictionary_array! {
                    left => {
                        let unpacked_dict = evaluated_values.take_iter(left.keys().iter().map(|opt| opt.map(|v| v as _))).collect::<BooleanArray>();
                        Arc::new(unpacked_dict) as ArrayRef
                    },
                    _ => unreachable!(),
                }
            )
        }
        other => internal_err!(
                "Data type {} not supported for operation 'regex_match_dyn_scalar' on string array",
                other
        ),
    };
    Some(result)
}
