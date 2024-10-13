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

//! [`set_nulls`], other utilities for working with nulls

use arrow::array::{
    Array, ArrayRef, ArrowNumericType, AsArray, BinaryArray, BinaryViewArray,
    BooleanArray, LargeBinaryArray, LargeStringArray, PrimitiveArray, StringArray,
    StringViewArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;
use datafusion_common::{not_impl_err, Result};
use std::sync::Arc;

/// Sets the validity mask for a `PrimitiveArray` to `nulls`
/// replacing any existing null mask
///
/// See [`set_nulls_dyn`] for a version that works with `Array`
pub fn set_nulls<T: ArrowNumericType + Send>(
    array: PrimitiveArray<T>,
    nulls: Option<NullBuffer>,
) -> PrimitiveArray<T> {
    let (dt, values, _old_nulls) = array.into_parts();
    PrimitiveArray::<T>::new(values, nulls).with_data_type(dt)
}

/// Converts a `BooleanBuffer` representing a filter to a `NullBuffer.
///
/// The `NullBuffer` is
/// * `true` (representing valid) for values that were `true` in filter
/// * `false` (representing null) for values that were `false` or `null` in filter
fn filter_to_nulls(filter: &BooleanArray) -> Option<NullBuffer> {
    let (filter_bools, filter_nulls) = filter.clone().into_parts();
    let filter_bools = NullBuffer::from(filter_bools);
    NullBuffer::union(Some(&filter_bools), filter_nulls.as_ref())
}

/// Compute an output validity mask for an array that has been filtered
///
/// This can be used to compute nulls for the output of
/// [`GroupsAccumulator::convert_to_state`], which quickly applies an optional
/// filter to the input rows by setting any filtered rows to NULL in the output.
/// Subsequent applications of  aggregate functions that ignore NULLs (most of
/// them) will thus ignore the filtered rows as well.
///
/// # Output element is `true` (and thus output is non-null)
///
/// A `true` in the output represents non null output for all values that were *both*:
///
/// * `true` in any `opt_filter` (aka values that passed the filter)
///
/// * `non null` in `input`
///
/// # Output element is `false` (and thus output is null)
///
/// A `false` in the output represents an input that was *either*:
///
/// * `null`
///
/// * filtered (aka the value was `false` or `null` in the filter)
///
/// # Example
///
/// ```text
/// ┌─────┐           ┌─────┐            ┌─────┐
/// │true │           │NULL │            │false│
/// │true │    │      │true │            │true │
/// │true │ ───┼───   │false│  ────────▶ │false│       filtered_nulls
/// │false│    │      │NULL │            │false│
/// │false│           │true │            │false│
/// └─────┘           └─────┘            └─────┘
/// array           opt_filter           output
///  .nulls()
///
/// false = NULL    true  = pass          false = NULL       Meanings
/// true  = valid   false = filter        true  = valid
///                 NULL  = filter
/// ```
///
/// [`GroupsAccumulator::convert_to_state`]: datafusion_expr_common::groups_accumulator::GroupsAccumulator
pub fn filtered_null_mask(
    opt_filter: Option<&BooleanArray>,
    input: &dyn Array,
) -> Option<NullBuffer> {
    let opt_filter = opt_filter.and_then(filter_to_nulls);
    NullBuffer::union(opt_filter.as_ref(), input.nulls())
}

/// Applies optional filter to input, returning a new array of the same type
/// with the same data, but with any values that were filtered out set to null
pub fn apply_filter_as_nulls(
    input: &dyn Array,
    opt_filter: Option<&BooleanArray>,
) -> Result<ArrayRef> {
    let nulls = filtered_null_mask(opt_filter, input);
    set_nulls_dyn(input, nulls)
}

/// Replaces the nulls in the input array with the given `NullBuffer`
///
/// TODO: replace when upstreamed in arrow-rs: <https://github.com/apache/arrow-rs/issues/6528>
pub fn set_nulls_dyn(input: &dyn Array, nulls: Option<NullBuffer>) -> Result<ArrayRef> {
    if let Some(nulls) = nulls.as_ref() {
        assert_eq!(nulls.len(), input.len());
    }

    let output: ArrayRef = match input.data_type() {
        DataType::Utf8 => {
            let input = input.as_string::<i32>();
            // safety: values / offsets came from a valid string array, so are valid utf8
            // and we checked nulls has the same length as values
            unsafe {
                Arc::new(StringArray::new_unchecked(
                    input.offsets().clone(),
                    input.values().clone(),
                    nulls,
                ))
            }
        }
        DataType::LargeUtf8 => {
            let input = input.as_string::<i64>();
            // safety: values / offsets came from a valid string array, so are valid utf8
            // and we checked nulls has the same length as values
            unsafe {
                Arc::new(LargeStringArray::new_unchecked(
                    input.offsets().clone(),
                    input.values().clone(),
                    nulls,
                ))
            }
        }
        DataType::Utf8View => {
            let input = input.as_string_view();
            // safety: values / views came from a valid string view array, so are valid utf8
            // and we checked nulls has the same length as values
            unsafe {
                Arc::new(StringViewArray::new_unchecked(
                    input.views().clone(),
                    input.data_buffers().to_vec(),
                    nulls,
                ))
            }
        }

        DataType::Binary => {
            let input = input.as_binary::<i32>();
            // safety: values / offsets came from a valid binary array
            // and we checked nulls has the same length as values
            unsafe {
                Arc::new(BinaryArray::new_unchecked(
                    input.offsets().clone(),
                    input.values().clone(),
                    nulls,
                ))
            }
        }
        DataType::LargeBinary => {
            let input = input.as_binary::<i64>();
            // safety: values / offsets came from a valid large binary array
            // and we checked nulls has the same length as values
            unsafe {
                Arc::new(LargeBinaryArray::new_unchecked(
                    input.offsets().clone(),
                    input.values().clone(),
                    nulls,
                ))
            }
        }
        DataType::BinaryView => {
            let input = input.as_binary_view();
            // safety: values / views came from a valid binary view array
            // and we checked nulls has the same length as values
            unsafe {
                Arc::new(BinaryViewArray::new_unchecked(
                    input.views().clone(),
                    input.data_buffers().to_vec(),
                    nulls,
                ))
            }
        }
        _ => {
            return not_impl_err!("Applying nulls {:?}", input.data_type());
        }
    };
    assert_eq!(input.len(), output.len());
    assert_eq!(input.data_type(), output.data_type());

    Ok(output)
}
