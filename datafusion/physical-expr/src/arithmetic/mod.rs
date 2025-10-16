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

//! Ultra-low overhead arithmetic overflow detection
//!
//! This module provides advanced arithmetic overflow detection with multiple
//! optimization strategies to minimize performance overhead while maintaining
//! SQL-standard correctness.

pub mod adaptive;
pub mod benchmarks;
pub mod compile_time;
pub mod hardware_optimized;
pub mod integration_tests;
pub mod intelligent;
pub mod simd_simple;
pub mod simple;

// Re-export the main functions from the simple module
pub use simple::{
    ultra_fast_checked_add, ultra_fast_checked_mul, ultra_fast_checked_sub,
};

// Re-export intelligent functions for advanced use cases
pub use intelligent::{
    intelligent_checked_add, intelligent_checked_mul, intelligent_checked_sub,
};

// Re-export benchmarking functions
pub use benchmarks::{quick_performance_validation, run_comprehensive_benchmarks};

// Re-export integration testing functions
pub use integration_tests::quick_integration_validation;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Int64Array};
    use datafusion_expr::ColumnarValue;
    use std::sync::Arc;

    #[test]
    fn test_strategy_selection() {
        // Basic test to ensure module compiles and works
        let left = Int64Array::from(vec![1, 2, 3]);
        let right = Int64Array::from(vec![10, 20, 30]);

        let left_val = ColumnarValue::Array(Arc::new(left));
        let right_val = ColumnarValue::Array(Arc::new(right));

        let result = ultra_fast_checked_add(&left_val, &right_val).unwrap();
        let result_array = result.as_primitive::<arrow::datatypes::Int64Type>();

        assert_eq!(result_array.values(), &[11, 22, 33]);
    }

    #[test]
    fn test_ultra_fast_overflow_detection() {
        let left = Int64Array::from(vec![i64::MAX]);
        let right = Int64Array::from(vec![1]);

        let left_val = ColumnarValue::Array(Arc::new(left));
        let right_val = ColumnarValue::Array(Arc::new(right));

        // Should detect overflow
        assert!(ultra_fast_checked_add(&left_val, &right_val).is_err());
    }

    #[test]
    fn test_ultra_fast_checked_sub() {
        let left = Int64Array::from(vec![100, 200, 300]);
        let right = Int64Array::from(vec![10, 20, 30]);

        let left_val = ColumnarValue::Array(Arc::new(left));
        let right_val = ColumnarValue::Array(Arc::new(right));

        let result = ultra_fast_checked_sub(&left_val, &right_val).unwrap();
        let result_array = result.as_primitive::<arrow::datatypes::Int64Type>();

        assert_eq!(result_array.values(), &[90, 180, 270]);
    }

    #[test]
    fn test_ultra_fast_checked_mul() {
        let left = Int64Array::from(vec![2, 3, 4]);
        let right = Int64Array::from(vec![10, 20, 30]);

        let left_val = ColumnarValue::Array(Arc::new(left));
        let right_val = ColumnarValue::Array(Arc::new(right));

        let result = ultra_fast_checked_mul(&left_val, &right_val).unwrap();
        let result_array = result.as_primitive::<arrow::datatypes::Int64Type>();

        assert_eq!(result_array.values(), &[20, 60, 120]);
    }

    #[test]
    fn test_large_batch_performance() {
        let size = 2000usize;
        let left = Int64Array::from((0..size as i64).collect::<Vec<i64>>());
        let right = Int64Array::from((1..=size as i64).collect::<Vec<i64>>());

        let left_val = ColumnarValue::Array(Arc::new(left));
        let right_val = ColumnarValue::Array(Arc::new(right));

        let result = ultra_fast_checked_add(&left_val, &right_val).unwrap();
        assert_eq!(result.len(), size);

        let result_array = result.as_primitive::<arrow::datatypes::Int64Type>();
        assert_eq!(result_array.value(0), 1); // 0 + 1
        assert_eq!(result_array.value(10), 21); // 10 + 11
    }
}
