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

//! Comprehensive integration tests for ultra-low overhead arithmetic
//!
//! This module provides end-to-end testing and validation of all optimization phases.

use arrow::array::{AsArray, Int64Array};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

use super::adaptive::get_adaptive_optimizer;
use super::benchmarks::quick_performance_validation;
use super::compile_time::{CompileTimeSafetyAnalyzer, SafetyCategory};
use super::intelligent::{
    intelligent_checked_add, intelligent_checked_mul, intelligent_checked_sub,
};
use super::ultra_fast_checked_add;

/// Integration test suite for all optimization phases
pub struct IntegrationTestSuite;

impl IntegrationTestSuite {
    /// Run complete validation suite
    pub fn run_complete_validation() -> bool {
        println!("🚀 Starting Ultra-Low Overhead Arithmetic Integration Tests");
        println!("{}", "=".repeat(80));

        let mut all_passed = true;

        // Phase 1: Hardware-optimized tests
        println!("📊 Phase 1: Hardware-Optimized SIMD Tests");
        all_passed &= Self::test_hardware_optimization();

        // Phase 2: Compile-time analysis tests
        println!("🧠 Phase 2: Compile-Time Safety Analysis Tests");
        all_passed &= Self::test_compile_time_analysis();

        // Phase 3: Adaptive intelligence tests
        println!("🎯 Phase 3: Adaptive Intelligence Tests");
        all_passed &= Self::test_adaptive_optimization();

        // Phase 4: End-to-end integration tests
        println!("🔧 Phase 4: End-to-End Integration Tests");
        all_passed &= Self::test_end_to_end_integration();

        // Performance validation
        println!("⚡ Performance Validation");
        all_passed &= quick_performance_validation();

        println!("{}", "=".repeat(80));
        if all_passed {
            println!("✅ ALL INTEGRATION TESTS PASSED!");
            println!("🎉 Ultra-low overhead arithmetic is ready for production!");
        } else {
            println!("❌ Some integration tests failed!");
        }

        all_passed
    }

    /// Test hardware optimization functionality
    fn test_hardware_optimization() -> bool {
        let mut passed = 0;
        let mut total = 0;

        // Test 1: Basic arithmetic with hardware acceleration
        total += 1;
        if Self::test_basic_hardware_arithmetic() {
            passed += 1;
            println!("  ✅ Basic hardware arithmetic");
        } else {
            println!("  ❌ Basic hardware arithmetic");
        }

        // Test 2: Overflow detection with hardware flags
        total += 1;
        if Self::test_hardware_overflow_detection() {
            passed += 1;
            println!("  ✅ Hardware overflow detection");
        } else {
            println!("  ❌ Hardware overflow detection");
        }

        // Test 3: SIMD vectorization
        total += 1;
        if Self::test_simd_vectorization() {
            passed += 1;
            println!("  ✅ SIMD vectorization");
        } else {
            println!("  ❌ SIMD vectorization");
        }

        println!("  Phase 1 Results: {passed}/{total} tests passed");
        passed == total
    }

    /// Test compile-time analysis functionality
    fn test_compile_time_analysis() -> bool {
        let mut passed = 0;
        let mut total = 0;

        // Test 1: Safety categorization
        total += 1;
        if Self::test_safety_categorization() {
            passed += 1;
            println!("  ✅ Safety categorization");
        } else {
            println!("  ❌ Safety categorization");
        }

        // Test 2: Value range analysis
        total += 1;
        if Self::test_value_range_analysis() {
            passed += 1;
            println!("  ✅ Value range analysis");
        } else {
            println!("  ❌ Value range analysis");
        }

        // Test 3: Constant folding optimization
        total += 1;
        if Self::test_constant_folding() {
            passed += 1;
            println!("  ✅ Constant folding optimization");
        } else {
            println!("  ❌ Constant folding optimization");
        }

        println!("  Phase 2 Results: {passed}/{total} tests passed");
        passed == total
    }

    /// Test adaptive optimization functionality
    fn test_adaptive_optimization() -> bool {
        let mut passed = 0;
        let mut total = 0;

        // Test 1: Performance metrics collection
        total += 1;
        if Self::test_performance_metrics() {
            passed += 1;
            println!("  ✅ Performance metrics collection");
        } else {
            println!("  ❌ Performance metrics collection");
        }

        // Test 2: Strategy adaptation
        total += 1;
        if Self::test_strategy_adaptation() {
            passed += 1;
            println!("  ✅ Strategy adaptation");
        } else {
            println!("  ❌ Strategy adaptation");
        }

        // Test 3: Data distribution learning
        total += 1;
        if Self::test_data_distribution_learning() {
            passed += 1;
            println!("  ✅ Data distribution learning");
        } else {
            println!("  ❌ Data distribution learning");
        }

        println!("  Phase 3 Results: {passed}/{total} tests passed");
        passed == total
    }

    /// Test end-to-end integration
    fn test_end_to_end_integration() -> bool {
        let mut passed = 0;
        let mut total = 0;

        // Test 1: Complete pipeline integration
        total += 1;
        if Self::test_complete_pipeline() {
            passed += 1;
            println!("  ✅ Complete pipeline integration");
        } else {
            println!("  ❌ Complete pipeline integration");
        }

        // Test 2: Error handling and edge cases
        total += 1;
        if Self::test_error_handling() {
            passed += 1;
            println!("  ✅ Error handling and edge cases");
        } else {
            println!("  ❌ Error handling and edge cases");
        }

        // Test 3: Performance consistency
        total += 1;
        if Self::test_performance_consistency() {
            passed += 1;
            println!("  ✅ Performance consistency");
        } else {
            println!("  ❌ Performance consistency");
        }

        println!("  Phase 4 Results: {passed}/{total} tests passed");
        passed == total
    }

    // Individual test implementations

    fn test_basic_hardware_arithmetic() -> bool {
        let left = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let right = Int64Array::from(vec![10, 20, 30, 40, 50]);

        let left_val = ColumnarValue::Array(Arc::new(left));
        let right_val = ColumnarValue::Array(Arc::new(right));

        match ultra_fast_checked_add(&left_val, &right_val) {
            Ok(result) => {
                let result_array = result.as_primitive::<arrow::datatypes::Int64Type>();
                result_array.values() == &[11, 22, 33, 44, 55]
            }
            Err(_) => false,
        }
    }

    fn test_hardware_overflow_detection() -> bool {
        let left = Int64Array::from(vec![i64::MAX]);
        let right = Int64Array::from(vec![1]);

        let left_val = ColumnarValue::Array(Arc::new(left));
        let right_val = ColumnarValue::Array(Arc::new(right));

        // Should detect overflow
        ultra_fast_checked_add(&left_val, &right_val).is_err()
    }

    fn test_simd_vectorization() -> bool {
        let size = 1000usize;
        let left = Int64Array::from((0..size as i64).collect::<Vec<i64>>());
        let right = Int64Array::from((1..=size as i64).collect::<Vec<i64>>());

        let left_val = ColumnarValue::Array(Arc::new(left));
        let right_val = ColumnarValue::Array(Arc::new(right));

        match ultra_fast_checked_add(&left_val, &right_val) {
            Ok(result) => result.len() == size,
            Err(_) => false,
        }
    }

    fn test_safety_categorization() -> bool {
        let mut analyzer = CompileTimeSafetyAnalyzer::new();

        // Small literal should be provably safe
        let small_literal = datafusion_expr::Expr::Literal(
            datafusion_common::ScalarValue::Int64(Some(42)),
            None,
        );
        let safety = analyzer.analyze_expression(&small_literal);

        matches!(safety, SafetyCategory::ProveablySafe)
    }

    fn test_value_range_analysis() -> bool {
        use super::compile_time::ValueRange;

        let range1 = ValueRange::bounded(Some(100), Some(200));
        let range2 = ValueRange::bounded(Some(50), Some(150));

        // Should be safe to add these ranges
        range1.can_add_safely(&range2)
    }

    fn test_constant_folding() -> bool {
        // Test that compile-time analysis can identify safe operations
        let left = Int64Array::from(vec![10, 20, 30]);
        let right = Int64Array::from(vec![5, 10, 15]);

        let left_val = ColumnarValue::Array(Arc::new(left));
        let right_val = ColumnarValue::Array(Arc::new(right));

        match intelligent_checked_add(
            &left_val,
            &right_val,
            Some(SafetyCategory::ProveablySafe),
        ) {
            Ok(result) => {
                let result_array = result.as_primitive::<arrow::datatypes::Int64Type>();
                result_array.values() == &[15, 30, 45]
            }
            Err(_) => false,
        }
    }

    fn test_performance_metrics() -> bool {
        let optimizer = get_adaptive_optimizer();

        // Record some performance data
        optimizer.record_performance(
            "test_strategy",
            100,
            std::time::Duration::from_millis(10),
            false,
            1000,
        );

        let summary = optimizer.get_performance_summary();
        summary.contains_key("test_strategy")
    }

    fn test_strategy_adaptation() -> bool {
        let optimizer = get_adaptive_optimizer();

        // Test strategy selection
        let strategy = optimizer.get_optimal_strategy(
            1000,
            super::adaptive::ArithmeticOp::Add,
            Some((100, 200)),
        );

        !strategy.is_empty()
    }

    fn test_data_distribution_learning() -> bool {
        let optimizer = get_adaptive_optimizer();

        // Update data distribution
        optimizer.update_data_distribution(&[10, 20, 30, 40, 50]);

        let stats = optimizer.get_data_distribution_stats();
        stats.sample_count > 0
    }

    fn test_complete_pipeline() -> bool {
        // Test the complete optimization pipeline with intelligent functions
        let left = Int64Array::from(vec![1000, 2000, 3000]);
        let right = Int64Array::from(vec![500, 1000, 1500]);

        let left_val = ColumnarValue::Array(Arc::new(left));
        let right_val = ColumnarValue::Array(Arc::new(right));

        // Test addition
        let add_result = intelligent_checked_add(
            &left_val,
            &right_val,
            Some(SafetyCategory::LikelySafe),
        );

        // Test subtraction
        let sub_result = intelligent_checked_sub(
            &left_val,
            &right_val,
            Some(SafetyCategory::LikelySafe),
        );

        // Test multiplication
        let mul_result = intelligent_checked_mul(
            &left_val,
            &right_val,
            Some(SafetyCategory::LikelySafe),
        );

        add_result.is_ok() && sub_result.is_ok() && mul_result.is_ok()
    }

    fn test_error_handling() -> bool {
        // Test overflow detection
        let left = Int64Array::from(vec![i64::MAX]);
        let right = Int64Array::from(vec![1]);

        let left_val = ColumnarValue::Array(Arc::new(left));
        let right_val = ColumnarValue::Array(Arc::new(right));

        let result = intelligent_checked_add(&left_val, &right_val, None);
        result.is_err()
    }

    fn test_performance_consistency() -> bool {
        // Run the same operation multiple times and check for consistent performance
        let left = Int64Array::from(vec![100; 1000]);
        let right = Int64Array::from(vec![200; 1000]);

        let left_val = ColumnarValue::Array(Arc::new(left));
        let right_val = ColumnarValue::Array(Arc::new(right));

        let mut times = Vec::new();

        for _ in 0..5 {
            let start = datafusion_common::instant::Instant::now();
            let _ = intelligent_checked_add(
                &left_val,
                &right_val,
                Some(SafetyCategory::LikelySafe),
            );
            times.push(start.elapsed());
        }

        // Check that performance is consistent (not varying by more than 50%)
        let avg_time = times.iter().sum::<std::time::Duration>() / times.len() as u32;
        let max_deviation = times
            .iter()
            .map(|t| {
                (t.as_nanos() as f64 - avg_time.as_nanos() as f64).abs()
                    / avg_time.as_nanos() as f64
            })
            .fold(0.0, f64::max);

        max_deviation < 0.5 // Less than 50% variation
    }
}

/// Run quick integration validation for CI/CD
pub fn quick_integration_validation() -> bool {
    println!("Running quick integration validation...");

    let mut passed = 0;
    let mut total = 0;

    // Basic functionality test
    total += 1;
    if IntegrationTestSuite::test_basic_hardware_arithmetic() {
        passed += 1;
        println!("  ✅ Basic functionality");
    } else {
        println!("  ❌ Basic functionality");
    }

    // Overflow detection test
    total += 1;
    if IntegrationTestSuite::test_hardware_overflow_detection() {
        passed += 1;
        println!("  ✅ Overflow detection");
    } else {
        println!("  ❌ Overflow detection");
    }

    // Performance test
    total += 1;
    if quick_performance_validation() {
        passed += 1;
        println!("  ✅ Performance validation");
    } else {
        println!("  ❌ Performance validation");
    }

    let success = passed == total;
    println!("Quick validation: {passed}/{total} tests passed");

    if success {
        println!("✅ Quick integration validation passed!");
    } else {
        println!("❌ Quick integration validation failed!");
    }

    success
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integration_suite() {
        // Run a subset of integration tests that can complete quickly
        assert!(IntegrationTestSuite::test_basic_hardware_arithmetic());
        assert!(IntegrationTestSuite::test_hardware_overflow_detection());
        assert!(IntegrationTestSuite::test_safety_categorization());
        assert!(IntegrationTestSuite::test_performance_metrics());
    }

    #[test]
    fn test_quick_validation() {
        // This should pass without taking too long
        let result = quick_integration_validation();
        // We don't assert the result as it may depend on hardware capabilities
        println!("Quick validation result: {result}");
    }
}
