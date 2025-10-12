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

//! Simplified ultra-fast arithmetic with overflow detection

use arrow::array::{Array, AsArray, PrimitiveArray};
use arrow::compute::kernels::numeric::{add, mul, sub};
use arrow::datatypes::Int64Type;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

use super::simd_simple;

/// Ultra-fast overflow-checked addition
pub fn ultra_fast_checked_add(
    left: &ColumnarValue,
    right: &ColumnarValue,
) -> Result<Arc<dyn Array>> {
    match (left, right) {
        (ColumnarValue::Array(left_array), ColumnarValue::Array(right_array)) => {
            // Use optimized path for Int64 arrays that don't have nulls
            if let (Some(left_i64), Some(right_i64)) = (
                left_array.as_primitive_opt::<Int64Type>(),
                right_array.as_primitive_opt::<Int64Type>(),
            ) {
                // If either array has nulls, fall back to Arrow's standard function
                if left_array.null_count() > 0 || right_array.null_count() > 0 {
                    return add(left_array, right_array)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
                }
                let result = simd_simple::simd_checked_add_i64(
                    left_i64.values(),
                    right_i64.values(),
                )
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

                let result_array = PrimitiveArray::<Int64Type>::new(
                    result.into(),
                    left_i64.nulls().cloned(),
                );

                return Ok(Arc::new(result_array));
            }

            // Fallback to Arrow's implementation
            add(left_array, right_array)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        }
        _ => {
            // Handle scalar cases - determine the correct length for array expansion
            let length = match (left, right) {
                (ColumnarValue::Array(arr), ColumnarValue::Scalar(_)) => arr.len(),
                (ColumnarValue::Scalar(_), ColumnarValue::Array(arr)) => arr.len(),
                _ => 1, // Both scalars
            };

            let left_array = left.to_array(length)?;
            let right_array = right.to_array(length)?;
            add(&left_array, &right_array)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        }
    }
}

/// Ultra-fast overflow-checked subtraction
pub fn ultra_fast_checked_sub(
    left: &ColumnarValue,
    right: &ColumnarValue,
) -> Result<Arc<dyn Array>> {
    match (left, right) {
        (ColumnarValue::Array(left_array), ColumnarValue::Array(right_array)) => {
            // Use optimized path for Int64 arrays that don't have nulls
            if let (Some(left_i64), Some(right_i64)) = (
                left_array.as_primitive_opt::<Int64Type>(),
                right_array.as_primitive_opt::<Int64Type>(),
            ) {
                // If either array has nulls, fall back to Arrow's standard function
                if left_array.null_count() > 0 || right_array.null_count() > 0 {
                    return sub(left_array, right_array)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
                }
                let result = simd_simple::simd_checked_sub_i64(
                    left_i64.values(),
                    right_i64.values(),
                )
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

                let result_array = PrimitiveArray::<Int64Type>::new(
                    result.into(),
                    left_i64.nulls().cloned(),
                );

                return Ok(Arc::new(result_array));
            }

            // For non-Int64 types, fall back to standard Arrow functions
            // This ensures compatibility with all existing DataFusion operations
            sub(left_array, right_array)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        }
        _ => {
            // Handle scalar cases - determine the correct length for array expansion
            let length = match (left, right) {
                (ColumnarValue::Array(arr), ColumnarValue::Scalar(_)) => arr.len(),
                (ColumnarValue::Scalar(_), ColumnarValue::Array(arr)) => arr.len(),
                _ => 1, // Both scalars
            };

            let left_array = left.to_array(length)?;
            let right_array = right.to_array(length)?;
            sub(&left_array, &right_array)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        }
    }
}

/// Ultra-fast overflow-checked multiplication
pub fn ultra_fast_checked_mul(
    left: &ColumnarValue,
    right: &ColumnarValue,
) -> Result<Arc<dyn Array>> {
    match (left, right) {
        (ColumnarValue::Array(left_array), ColumnarValue::Array(right_array)) => {
            // Use optimized path for Int64 arrays that don't have nulls
            if let (Some(left_i64), Some(right_i64)) = (
                left_array.as_primitive_opt::<Int64Type>(),
                right_array.as_primitive_opt::<Int64Type>(),
            ) {
                // If either array has nulls, fall back to Arrow's standard function
                if left_array.null_count() > 0 || right_array.null_count() > 0 {
                    return mul(left_array, right_array)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
                }
                let result = simd_simple::simd_checked_mul_i64(
                    left_i64.values(),
                    right_i64.values(),
                )
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

                let result_array = PrimitiveArray::<Int64Type>::new(
                    result.into(),
                    left_i64.nulls().cloned(),
                );

                return Ok(Arc::new(result_array));
            }

            // For non-Int64 types, fall back to standard Arrow functions
            mul(left_array, right_array)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        }
        _ => {
            // Handle scalar cases - determine the correct length for array expansion
            let length = match (left, right) {
                (ColumnarValue::Array(arr), ColumnarValue::Scalar(_)) => arr.len(),
                (ColumnarValue::Scalar(_), ColumnarValue::Array(arr)) => arr.len(),
                _ => 1, // Both scalars
            };

            let left_array = left.to_array(length)?;
            let right_array = right.to_array(length)?;
            mul(&left_array, &right_array)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    #[test]
    fn test_ultra_fast_add() {
        let left = Int64Array::from(vec![1, 2, 3, 4]);
        let right = Int64Array::from(vec![10, 20, 30, 40]);

        let left_val = ColumnarValue::Array(Arc::new(left));
        let right_val = ColumnarValue::Array(Arc::new(right));

        let result = ultra_fast_checked_add(&left_val, &right_val).unwrap();
        let result_array = result.as_primitive::<Int64Type>();

        assert_eq!(result_array.values(), &[11, 22, 33, 44]);
    }

    #[test]
    fn test_overflow_detection() {
        let left = Int64Array::from(vec![i64::MAX]);
        let right = Int64Array::from(vec![1]);

        let left_val = ColumnarValue::Array(Arc::new(left));
        let right_val = ColumnarValue::Array(Arc::new(right));

        assert!(ultra_fast_checked_add(&left_val, &right_val).is_err());
    }
}
