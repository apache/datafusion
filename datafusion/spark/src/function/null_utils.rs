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
    /// All inputs are scalars and at least one is NULL -> return NULL
    ReturnNull,
    /// All inputs are non-NULL -> no null mask needed
    NoMask,
    /// Null mask to apply for arrays
    Apply(NullBuffer),
}

pub(crate) fn compute_null_mask(args: &[ColumnarValue]) -> NullMaskResolution {
    let mut array_len = None;
    let mut has_null_scalar = false;

    for arg in args {
        match arg {
            ColumnarValue::Array(array) => {
                array_len.get_or_insert_with(|| array.len());
            }
            ColumnarValue::Scalar(scalar) => {
                has_null_scalar |= scalar.is_null();
            }
        }
    }

    let Some(array_len) = array_len else {
        // All arguments are scalars
        return if has_null_scalar {
            NullMaskResolution::ReturnNull
        } else {
            NullMaskResolution::NoMask
        };
    };

    if has_null_scalar {
        return NullMaskResolution::Apply(NullBuffer::new_null(array_len));
    }

    let combined_nulls =
        NullBuffer::union_many(args.iter().filter_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.nulls()),
            ColumnarValue::Scalar(_) => None,
        }));

    match combined_nulls {
        Some(nulls) => NullMaskResolution::Apply(nulls),
        None => NullMaskResolution::NoMask,
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
