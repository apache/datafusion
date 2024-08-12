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

//! [`set_nulls`], and [`filtered_null_mask`], utilities for working with nulls

use arrow::array::{Array, ArrowNumericType, BooleanArray, PrimitiveArray};
use arrow::buffer::NullBuffer;

/// Sets the validity mask for a `PrimitiveArray` to `nulls`
/// replacing any existing null mask
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
