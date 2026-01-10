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

use arrow::array::Array;
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

pub(crate) enum NullMaskResolution {
    /// Return NULL as the result (e.g., scalar inputs with at least one NULL)
    ReturnNull,
    /// No null mask needed (e.g., all scalar inputs are non-NULL)
    NoMask,
    /// Null mask to apply for arrays
    Apply(NullBuffer),
}

/// Compute NULL mask for the arguments using NullBuffer::union
pub(crate) fn compute_null_mask(
    args: &[ColumnarValue],
    number_rows: usize,
) -> Result<NullMaskResolution> {
    // Check if all arguments are scalars
    let all_scalars = args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));

    if all_scalars {
        // For scalars, check if any is NULL
        for arg in args {
            if let ColumnarValue::Scalar(scalar) = arg
                && scalar.is_null()
            {
                return Ok(NullMaskResolution::ReturnNull);
            }
        }
        // No NULLs in scalars
        Ok(NullMaskResolution::NoMask)
    } else {
        // For arrays, compute NULL mask for each row using NullBuffer::union
        let array_len = args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(array) => Some(array.len()),
                _ => None,
            })
            .unwrap_or(number_rows);

        // Convert all scalars to arrays for uniform processing
        let arrays: Result<Vec<_>> = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(array) => Ok(Arc::clone(array)),
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(array_len),
            })
            .collect();
        let arrays = arrays?;

        // Use NullBuffer::union to combine all null buffers
        let combined_nulls = arrays
            .iter()
            .map(|arr| arr.nulls())
            .fold(None, |acc, nulls| NullBuffer::union(acc.as_ref(), nulls));

        match combined_nulls {
            Some(nulls) => Ok(NullMaskResolution::Apply(nulls)),
            None => Ok(NullMaskResolution::NoMask),
        }
    }
}

/// Apply NULL mask to the result using NullBuffer::union
pub(crate) fn apply_null_mask(
    result: ColumnarValue,
    null_mask: NullMaskResolution,
    return_type: &DataType,
) -> Result<ColumnarValue> {
    match (result, null_mask) {
        // Scalar with ReturnNull mask means return NULL of the correct type
        (ColumnarValue::Scalar(_), NullMaskResolution::ReturnNull) => {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from(return_type)?))
        }
        // Scalar without mask, return as-is
        (scalar @ ColumnarValue::Scalar(_), NullMaskResolution::NoMask) => Ok(scalar),
        // Array with NULL mask - use NullBuffer::union to combine nulls
        (ColumnarValue::Array(array), NullMaskResolution::Apply(null_mask)) => {
            // Combine the result's existing nulls with our computed null mask
            let combined_nulls = NullBuffer::union(array.nulls(), Some(&null_mask));

            // Create new array with combined nulls
            let new_array = array
                .into_data()
                .into_builder()
                .nulls(combined_nulls)
                .build()?;

            Ok(ColumnarValue::Array(Arc::new(arrow::array::make_array(
                new_array,
            ))))
        }
        // Array without NULL mask, return as-is
        (array @ ColumnarValue::Array(_), NullMaskResolution::NoMask) => Ok(array),
        // Edge cases that shouldn't happen in practice
        (scalar, _) => Ok(scalar),
    }
}
