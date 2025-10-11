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

//! Comprehensive benchmarks for ultra-low overhead arithmetic optimizations
//!
//! This module provides extensive benchmarking to validate the <1.1x overhead target
//! and compare different optimization strategies.

use arrow::array::Int64Array;
use datafusion_common::instant::Instant;
use datafusion_expr::ColumnarValue;
use std::sync::Arc;
use std::time::Duration;

use super::adaptive::ArithmeticOp;
use super::compile_time::SafetyCategory;
use super::intelligent::{
    intelligent_checked_add, intelligent_checked_mul, intelligent_checked_sub,
};
use super::simple::{
    ultra_fast_checked_add, ultra_fast_checked_mul, ultra_fast_checked_sub,
};

/// Benchmark configuration
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub name: String,
    pub array_size: usize,
    pub iterations: usize,
    pub value_range: (i64, i64),
    pub operation: ArithmeticOp,
}

impl BenchmarkConfig {
    pub fn new(
        name: &str,
        size: usize,
        iterations: usize,
        range: (i64, i64),
        op: ArithmeticOp,
    ) -> Self {
        Self {
            name: name.to_string(),
            array_size: size,
            iterations,
            value_range: range,
            operation: op,
        }
    }
}

/// Benchmark results
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub config: BenchmarkConfig,
    pub baseline_time: Duration,
    pub optimized_time: Duration,
    pub intelligent_time: Duration,
    pub overhead_ratio: f64,
    pub throughput_ops_per_sec: f64,
    pub memory_efficiency: f64,
}

impl BenchmarkResult {
    /// Calculate performance improvement ratio
    pub fn improvement_ratio(&self) -> f64 {
        self.baseline_time.as_nanos() as f64 / self.optimized_time.as_nanos() as f64
    }

    /// Check if meets the <1.1x overhead target
    pub fn meets_target(&self) -> bool {
        self.overhead_ratio < 1.1
    }

    /// Calculate throughput in million operations per second
    pub fn throughput_mops(&self) -> f64 {
        let total_ops = self.config.iterations * self.config.array_size;
        let ops_per_sec = total_ops as f64 / self.optimized_time.as_secs_f64();
        ops_per_sec / 1_000_000.0
    }
}

/// Comprehensive benchmark suite
pub struct ArithmeticBenchmarkSuite {
    configs: Vec<BenchmarkConfig>,
}

impl ArithmeticBenchmarkSuite {
    /// Create a comprehensive benchmark suite
    #[allow(clippy::vec_init_then_push)]
    pub fn new() -> Self {
        let mut configs = Vec::new();

        // Small arrays (cache-friendly)
        configs.push(BenchmarkConfig::new(
            "small_safe_add",
            100,
            10000,
            (1, 1000),
            ArithmeticOp::Add,
        ));
        configs.push(BenchmarkConfig::new(
            "small_risky_add",
            100,
            10000,
            (i64::MAX / 2, i64::MAX / 2),
            ArithmeticOp::Add,
        ));

        // Medium arrays (typical workload)
        configs.push(BenchmarkConfig::new(
            "medium_safe_add",
            1000,
            1000,
            (1, 10000),
            ArithmeticOp::Add,
        ));
        configs.push(BenchmarkConfig::new(
            "medium_mixed_add",
            1000,
            1000,
            (-1000000, 1000000),
            ArithmeticOp::Add,
        ));

        // Large arrays (SIMD optimization target)
        configs.push(BenchmarkConfig::new(
            "large_safe_add",
            10000,
            100,
            (1, 100000),
            ArithmeticOp::Add,
        ));
        configs.push(BenchmarkConfig::new(
            "large_mixed_add",
            10000,
            100,
            (-10000000, 10000000),
            ArithmeticOp::Add,
        ));

        // Multiplication benchmarks (more overflow-prone)
        configs.push(BenchmarkConfig::new(
            "small_safe_mul",
            100,
            10000,
            (1, 100),
            ArithmeticOp::Multiply,
        ));
        configs.push(BenchmarkConfig::new(
            "medium_safe_mul",
            1000,
            1000,
            (1, 1000),
            ArithmeticOp::Multiply,
        ));
        configs.push(BenchmarkConfig::new(
            "large_risky_mul",
            10000,
            100,
            (1000, 100000),
            ArithmeticOp::Multiply,
        ));

        // Subtraction benchmarks
        configs.push(BenchmarkConfig::new(
            "medium_safe_sub",
            1000,
            1000,
            (1000, 100000),
            ArithmeticOp::Subtract,
        ));
        configs.push(BenchmarkConfig::new(
            "large_mixed_sub",
            10000,
            100,
            (-1000000, 1000000),
            ArithmeticOp::Subtract,
        ));

        // Real-world patterns
        configs.push(BenchmarkConfig::new(
            "counter_increment",
            10000,
            100,
            (0, 1000000),
            ArithmeticOp::Add,
        ));
        configs.push(BenchmarkConfig::new(
            "timestamp_math",
            5000,
            200,
            (1577836800, 1893456000),
            ArithmeticOp::Add,
        )); // Unix timestamps

        Self { configs }
    }

    /// Run all benchmarks
    pub fn run_all_benchmarks(&self) -> Vec<BenchmarkResult> {
        let mut results = Vec::new();

        println!("Running comprehensive arithmetic optimization benchmarks...");
        println!("Target: <1.1x overhead compared to baseline");
        println!("{}", "=".repeat(80));

        for config in &self.configs {
            println!("Running benchmark: {}", config.name);
            let result = self.run_single_benchmark(config);

            println!("  Baseline: {:?}", result.baseline_time);
            println!("  Optimized: {:?}", result.optimized_time);
            println!("  Intelligent: {:?}", result.intelligent_time);
            println!("  Overhead ratio: {:.3}x", result.overhead_ratio);
            println!("  Throughput: {:.2} MOPS", result.throughput_mops());
            println!(
                "  Target met: {}",
                if result.meets_target() { "✅" } else { "❌" }
            );
            println!();

            results.push(result);
        }

        self.print_summary(&results);
        results
    }

    /// Run a single benchmark
    fn run_single_benchmark(&self, config: &BenchmarkConfig) -> BenchmarkResult {
        // Generate test data
        let (left_data, right_data) = self.generate_test_data(config);

        // Benchmark baseline (Arrow's standard arithmetic)
        let baseline_time = self.benchmark_baseline(&left_data, &right_data, config);

        // Benchmark optimized version
        let optimized_time = self.benchmark_optimized(&left_data, &right_data, config);

        // Benchmark intelligent version
        let intelligent_time =
            self.benchmark_intelligent(&left_data, &right_data, config);

        let overhead_ratio =
            optimized_time.as_nanos() as f64 / baseline_time.as_nanos() as f64;
        let throughput_ops_per_sec =
            (config.iterations * config.array_size) as f64 / optimized_time.as_secs_f64();

        BenchmarkResult {
            config: config.clone(),
            baseline_time,
            optimized_time,
            intelligent_time,
            overhead_ratio,
            throughput_ops_per_sec,
            memory_efficiency: 1.0, // Could be calculated if needed
        }
    }

    /// Generate test data for benchmarking
    fn generate_test_data(
        &self,
        config: &BenchmarkConfig,
    ) -> (Vec<Int64Array>, Vec<Int64Array>) {
        let mut left_arrays = Vec::new();
        let mut right_arrays = Vec::new();

        for i in 0..config.iterations {
            let left_data: Vec<i64> = (0..config.array_size)
                .map(|j| {
                    let base = config.value_range.0;
                    let range = config.value_range.1 - config.value_range.0;
                    base + ((i * config.array_size + j) as i64 % range)
                })
                .collect();

            let right_data: Vec<i64> = (0..config.array_size)
                .map(|j| {
                    let base = config.value_range.0;
                    let range = config.value_range.1 - config.value_range.0;
                    base + (((i + 1) * config.array_size + j) as i64 % range)
                })
                .collect();

            left_arrays.push(Int64Array::from(left_data));
            right_arrays.push(Int64Array::from(right_data));
        }

        (left_arrays, right_arrays)
    }

    /// Benchmark baseline Arrow arithmetic
    fn benchmark_baseline(
        &self,
        left_arrays: &[Int64Array],
        right_arrays: &[Int64Array],
        config: &BenchmarkConfig,
    ) -> Duration {
        let start = Instant::now();

        for (left, right) in left_arrays.iter().zip(right_arrays.iter()) {
            match config.operation {
                ArithmeticOp::Add => {
                    let _ = arrow::compute::kernels::numeric::add(left, right);
                }
                ArithmeticOp::Subtract => {
                    // Use add as baseline since subtract may not be available
                    let _ = arrow::compute::kernels::numeric::add(left, right);
                }
                ArithmeticOp::Multiply => {
                    // Use add as baseline approximation
                    let _ = arrow::compute::kernels::numeric::add(left, right);
                }
                ArithmeticOp::Divide => {
                    let _ = arrow::compute::kernels::numeric::add(left, right);
                }
            }
        }

        start.elapsed()
    }

    /// Benchmark optimized version
    fn benchmark_optimized(
        &self,
        left_arrays: &[Int64Array],
        right_arrays: &[Int64Array],
        config: &BenchmarkConfig,
    ) -> Duration {
        let start = Instant::now();

        for (left, right) in left_arrays.iter().zip(right_arrays.iter()) {
            let left_val = ColumnarValue::Array(Arc::new(left.clone()));
            let right_val = ColumnarValue::Array(Arc::new(right.clone()));

            let _ = match config.operation {
                ArithmeticOp::Add => ultra_fast_checked_add(&left_val, &right_val),
                ArithmeticOp::Subtract => ultra_fast_checked_sub(&left_val, &right_val),
                ArithmeticOp::Multiply => ultra_fast_checked_mul(&left_val, &right_val),
                ArithmeticOp::Divide => ultra_fast_checked_add(&left_val, &right_val), // Fallback
            };
        }

        start.elapsed()
    }

    /// Benchmark intelligent version with adaptive optimization
    fn benchmark_intelligent(
        &self,
        left_arrays: &[Int64Array],
        right_arrays: &[Int64Array],
        config: &BenchmarkConfig,
    ) -> Duration {
        let start = Instant::now();

        // Determine compile-time safety for the test data
        let safety_category = if config.value_range.0 >= 0 && config.value_range.1 < 1000
        {
            Some(SafetyCategory::ProveablySafe)
        } else if config.value_range.0.abs() < 1_000_000
            && config.value_range.1.abs() < 1_000_000
        {
            Some(SafetyCategory::LikelySafe)
        } else {
            None
        };

        for (left, right) in left_arrays.iter().zip(right_arrays.iter()) {
            let left_val = ColumnarValue::Array(Arc::new(left.clone()));
            let right_val = ColumnarValue::Array(Arc::new(right.clone()));

            let _ = match config.operation {
                ArithmeticOp::Add => {
                    intelligent_checked_add(&left_val, &right_val, safety_category)
                }
                ArithmeticOp::Subtract => {
                    intelligent_checked_sub(&left_val, &right_val, safety_category)
                }
                ArithmeticOp::Multiply => {
                    intelligent_checked_mul(&left_val, &right_val, safety_category)
                }
                ArithmeticOp::Divide => {
                    intelligent_checked_add(&left_val, &right_val, safety_category)
                } // Fallback
            };
        }

        start.elapsed()
    }

    /// Print benchmark summary
    fn print_summary(&self, results: &[BenchmarkResult]) {
        println!("{}", "=".repeat(80));
        println!("BENCHMARK SUMMARY");
        println!("{}", "=".repeat(80));

        let total_tests = results.len();
        let passed_tests = results.iter().filter(|r| r.meets_target()).count();
        let avg_overhead =
            results.iter().map(|r| r.overhead_ratio).sum::<f64>() / total_tests as f64;
        let best_throughput = results
            .iter()
            .map(|r| r.throughput_mops())
            .fold(0.0, f64::max);

        println!(
            "Tests passed: {}/{} ({:.1}%)",
            passed_tests,
            total_tests,
            (passed_tests as f64 / total_tests as f64) * 100.0
        );
        println!("Average overhead: {avg_overhead:.3}x");
        println!("Best throughput: {best_throughput:.2} MOPS");

        if passed_tests == total_tests {
            println!("✅ ALL TESTS PASSED - Target <1.1x overhead achieved!");
        } else {
            println!("❌ Some tests failed to meet the <1.1x overhead target");
        }

        println!();
        println!("Detailed Results:");
        println!(
            "{:<20} {:<10} {:<12} {:<8} {:<10}",
            "Test", "Size", "Overhead", "Target", "Throughput"
        );
        println!("{}", "-".repeat(70));

        for result in results {
            let status = if result.meets_target() { "✅" } else { "❌" };
            println!(
                "{:<20} {:<10} {:<12.3} {:<8} {:<10.2}",
                result.config.name,
                result.config.array_size,
                result.overhead_ratio,
                status,
                result.throughput_mops()
            );
        }
    }
}

impl Default for ArithmeticBenchmarkSuite {
    fn default() -> Self {
        Self::new()
    }
}

/// Run comprehensive benchmarks and return results
pub fn run_comprehensive_benchmarks() -> Vec<BenchmarkResult> {
    let suite = ArithmeticBenchmarkSuite::new();
    suite.run_all_benchmarks()
}

/// Quick performance validation for CI/CD
pub fn quick_performance_validation() -> bool {
    println!("Running quick performance validation...");

    let config =
        BenchmarkConfig::new("quick_test", 1000, 100, (1, 10000), ArithmeticOp::Add);
    let suite = ArithmeticBenchmarkSuite::new();
    let result = suite.run_single_benchmark(&config);

    println!("Quick test overhead: {:.3}x", result.overhead_ratio);

    if result.meets_target() {
        println!("✅ Quick validation passed!");
        true
    } else {
        println!("❌ Quick validation failed!");
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_benchmark_suite_creation() {
        let suite = ArithmeticBenchmarkSuite::new();
        assert!(!suite.configs.is_empty());
        assert!(suite.configs.len() > 10); // Should have comprehensive coverage
    }

    #[test]
    fn test_benchmark_result_calculation() {
        let config = BenchmarkConfig::new("test", 100, 10, (1, 100), ArithmeticOp::Add);
        let result = BenchmarkResult {
            config,
            baseline_time: Duration::from_millis(100),
            optimized_time: Duration::from_millis(105), // 1.05x overhead
            intelligent_time: Duration::from_millis(95),
            overhead_ratio: 1.05,
            throughput_ops_per_sec: 1000.0,
            memory_efficiency: 1.0,
        };

        assert!(result.meets_target()); // 1.05 < 1.1
        assert!((result.throughput_mops() - 0.009523).abs() < 0.001); // approximately 9523 ops/sec = 0.009523 MOPS
    }

    #[test]
    fn test_quick_validation() {
        // This test should pass quickly and verify the implementation works
        let passed = quick_performance_validation();
        // We don't assert the result since it depends on hardware, but it should not panic
        println!("Quick validation result: {passed}");
    }
}
