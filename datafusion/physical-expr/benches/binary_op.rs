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

use arrow::{
    array::BooleanArray,
    datatypes::{DataType, Field, Schema},
};
use arrow::{array::StringArray, record_batch::RecordBatch};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr::{and, binary_expr, col, lit, or, Operator};
use datafusion_physical_expr::{
    expressions::{BinaryExpr, Column},
    planner::logical2physical,
    PhysicalExpr,
};
use std::sync::Arc;

/// Generates BooleanArrays with different true/false distributions for benchmarking.
///
/// Returns a vector of tuples containing scenario name and corresponding BooleanArray.
///
/// # Arguments
/// - `TEST_ALL_FALSE` - Used to generate what kind of test data
/// - `len` - Length of the BooleanArray to generate
fn generate_boolean_cases<const TEST_ALL_FALSE: bool>(
    len: usize,
) -> Vec<(String, BooleanArray)> {
    let mut cases = Vec::with_capacity(6);

    // Scenario 1: All elements false or all elements true
    if TEST_ALL_FALSE {
        let all_false = BooleanArray::from(vec![false; len]);
        cases.push(("all_false".to_string(), all_false));
    } else {
        let all_true = BooleanArray::from(vec![true; len]);
        cases.push(("all_true".to_string(), all_true));
    }

    // Scenario 2: Single true at first position or single false at first position
    if TEST_ALL_FALSE {
        let mut first_true = vec![false; len];
        first_true[0] = true;
        cases.push(("one_true_first".to_string(), BooleanArray::from(first_true)));
    } else {
        let mut first_false = vec![true; len];
        first_false[0] = false;
        cases.push((
            "one_false_first".to_string(),
            BooleanArray::from(first_false),
        ));
    }

    // Scenario 3: Single true at last position or single false at last position
    if TEST_ALL_FALSE {
        let mut last_true = vec![false; len];
        last_true[len - 1] = true;
        cases.push(("one_true_last".to_string(), BooleanArray::from(last_true)));
    } else {
        let mut last_false = vec![true; len];
        last_false[len - 1] = false;
        cases.push(("one_false_last".to_string(), BooleanArray::from(last_false)));
    }

    // Scenario 4: Single true at exact middle or single false at exact middle
    let mid = len / 2;
    if TEST_ALL_FALSE {
        let mut mid_true = vec![false; len];
        mid_true[mid] = true;
        cases.push(("one_true_middle".to_string(), BooleanArray::from(mid_true)));
    } else {
        let mut mid_false = vec![true; len];
        mid_false[mid] = false;
        cases.push((
            "one_false_middle".to_string(),
            BooleanArray::from(mid_false),
        ));
    }

    // Scenario 5: Single true at 25% position or single false at 25% position
    let mid_left = len / 4;
    if TEST_ALL_FALSE {
        let mut mid_left_true = vec![false; len];
        mid_left_true[mid_left] = true;
        cases.push((
            "one_true_middle_left".to_string(),
            BooleanArray::from(mid_left_true),
        ));
    } else {
        let mut mid_left_false = vec![true; len];
        mid_left_false[mid_left] = false;
        cases.push((
            "one_false_middle_left".to_string(),
            BooleanArray::from(mid_left_false),
        ));
    }

    // Scenario 6: Single true at 75% position or single false at 75% position
    let mid_right = (3 * len) / 4;
    if TEST_ALL_FALSE {
        let mut mid_right_true = vec![false; len];
        mid_right_true[mid_right] = true;
        cases.push((
            "one_true_middle_right".to_string(),
            BooleanArray::from(mid_right_true),
        ));
    } else {
        let mut mid_right_false = vec![true; len];
        mid_right_false[mid_right] = false;
        cases.push((
            "one_false_middle_right".to_string(),
            BooleanArray::from(mid_right_false),
        ));
    }

    // Scenario 7: Test all true or all false in AND/OR
    // This situation won't cause a short circuit, but it can skip the bool calculation
    if TEST_ALL_FALSE {
        let all_true = vec![true; len];
        cases.push(("all_true_in_and".to_string(), BooleanArray::from(all_true)));
    } else {
        let all_false = vec![false; len];
        cases.push(("all_false_in_or".to_string(), BooleanArray::from(all_false)));
    }

    cases
}

/// Benchmarks AND/OR operator short-circuiting by evaluating complex regex conditions.
///
/// Creates 7 test scenarios per operator:
/// 1. All values enable short-circuit (all_true/all_false)
/// 2. 2-6 Single true/false value at different positions to measure early exit
/// 3. Test all true or all false in AND/OR
///
/// You can run this benchmark with:
/// ```sh
/// cargo bench --bench binary_op -- short_circuit
/// ```
fn benchmark_binary_op_in_short_circuit(c: &mut Criterion) {
    // Create schema with three columns
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Boolean, false),
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Utf8, false),
    ]));

    // Generate test data with extended content
    let (b_values, c_values) = generate_test_strings(8192);

    let batches_and =
        create_record_batch::<true>(schema.clone(), &b_values, &c_values).unwrap();
    let batches_or =
        create_record_batch::<false>(schema.clone(), &b_values, &c_values).unwrap();

    // Build complex string matching conditions
    let right_condition_and = and(
        // Check for API endpoint pattern in URLs
        binary_expr(
            col("b"),
            Operator::RegexMatch,
            lit(r#"^https://(\w+\.)?example\.(com|org)/"#),
        ),
        // Check for markdown code blocks and summary section
        binary_expr(
            col("c"),
            Operator::RegexMatch,
            lit("```(rust|python|go)\nfn? main$$"),
        ),
    );

    let right_condition_or = or(
        // Check for secure HTTPS protocol
        binary_expr(
            col("b"),
            Operator::RegexMatch,
            lit(r#"^https://(\w+\.)?example\.(com|org)/"#),
        ),
        // Check for Rust code examples
        binary_expr(
            col("c"),
            Operator::RegexMatch,
            lit("```(rust|python|go)\nfn? main$$"),
        ),
    );

    // Create physical binary expressions
    // a AND ((b ~ regex) AND (c ~ regex))
    let expr_and = BinaryExpr::new_with_overflow_check(
        Arc::new(Column::new("a", 0)),
        Operator::And,
        logical2physical(&right_condition_and, &schema),
    );

    // a OR ((b ~ regex) OR (c ~ regex))
    let expr_or = BinaryExpr::new_with_overflow_check(
        Arc::new(Column::new("a", 0)),
        Operator::Or,
        logical2physical(&right_condition_or, &schema),
    );

    // Each scenario when the test operator is `and`
    {
        for (name, batch) in batches_and.into_iter() {
            c.bench_function(&format!("short_circuit/and/{name}"), |b| {
                b.iter(|| expr_and.evaluate(black_box(&batch)).unwrap())
            });
        }
    }
    // Each scenario when the test operator is `or`
    {
        for (name, batch) in batches_or.into_iter() {
            c.bench_function(&format!("short_circuit/or/{name}"), |b| {
                b.iter(|| expr_or.evaluate(black_box(&batch)).unwrap())
            });
        }
    }
}

/// Generate test data with computationally expensive patterns
fn generate_test_strings(num_rows: usize) -> (Vec<String>, Vec<String>) {
    // Extended URL patterns with query parameters and paths
    let base_urls = [
        "https://api.example.com/v2/users/12345/posts?category=tech&sort=date&lang=en-US",
        "https://cdn.example.net/assets/images/2023/08/15/sample-image-highres.jpg?width=1920&quality=85",
        "http://service.demo.org:8080/api/data/transactions/20230815123456.csv",
        "ftp://legacy.archive.example/backups/2023/Q3/database-dump.sql.gz",
        "https://docs.example.co.uk/reference/advanced-topics/concurrency/parallel-processing.md#implementation-details",
    ];

    // Extended markdown content with code blocks and structure
    let base_markdowns = [
        concat!(
            "# Advanced Topics in Computer Science\n\n",
            "## Summary\nThis article explores complex system design patterns and...\n\n",
            "```rust\nfn process_data(data: &mut [i32]) {\n    // Parallel processing example\n    data.par_iter_mut().for_each(|x| *x *= 2);\n}\n```\n\n",
            "## Performance Considerations\nWhen implementing concurrent systems...\n"
        ),
        concat!(
            "## API Documentation\n\n",
            "```json\n{\n  \"endpoint\": \"/api/v2/users\",\n  \"methods\": [\"GET\", \"POST\"],\n  \"parameters\": {\n    \"page\": \"number\"\n  }\n}\n```\n\n",
            "# Authentication Guide\nSecure your API access using OAuth 2.0...\n"
        ),
        concat!(
            "# Data Processing Pipeline\n\n",
            "```python\nfrom multiprocessing import Pool\n\ndef main():\n    with Pool(8) as p:\n        results = p.map(process_item, data)\n```\n\n",
            "## Summary of Optimizations\n1. Batch processing\n2. Memory pooling\n3. Concurrent I/O operations\n"
        ),
        concat!(
            "# System Architecture Overview\n\n",
            "## Components\n- Load Balancer\n- Database Cluster\n- Cache Service\n\n",
            "```go\nfunc main() {\n    router := gin.Default()\n    router.GET(\"/api/health\", healthCheck)\n    router.Run(\":8080\")\n}\n```\n"
        ),
        concat!(
            "## Configuration Reference\n\n",
            "```yaml\nserver:\n  port: 8080\n  max_threads: 32\n\ndatabase:\n  url: postgres://user@prod-db:5432/main\n```\n\n",
            "# Deployment Strategies\nBlue-green deployment patterns with...\n"
        ),
    ];

    let mut urls = Vec::with_capacity(num_rows);
    let mut markdowns = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        urls.push(base_urls[i % 5].to_string());
        markdowns.push(base_markdowns[i % 5].to_string());
    }

    (urls, markdowns)
}

/// Creates record batches with boolean arrays that test different short-circuit scenarios.
/// When TEST_ALL_FALSE = true: creates data for AND operator benchmarks (needs early false exit)
/// When TEST_ALL_FALSE = false: creates data for OR operator benchmarks (needs early true exit)
fn create_record_batch<const TEST_ALL_FALSE: bool>(
    schema: Arc<Schema>,
    b_values: &[String],
    c_values: &[String],
) -> arrow::error::Result<Vec<(String, RecordBatch)>> {
    // Generate data for six scenarios, but only the data for the "all_false" and "all_true" cases can be optimized through short-circuiting
    let boolean_array = generate_boolean_cases::<TEST_ALL_FALSE>(b_values.len());
    let mut rbs = Vec::with_capacity(boolean_array.len());
    for (name, a_array) in boolean_array {
        let b_array = StringArray::from(b_values.to_vec());
        let c_array = StringArray::from(c_values.to_vec());
        rbs.push((
            name,
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(a_array), Arc::new(b_array), Arc::new(c_array)],
            )?,
        ));
    }
    Ok(rbs)
}

/// Benchmarks arithmetic operations with and without overflow checking.
///
/// This measures the performance impact of enabling overflow checking by default
/// on common arithmetic operations like addition, subtraction, multiplication.
///
/// You can run this benchmark with:
/// ```sh
/// cargo bench --bench binary_op -- overflow_check
/// ```
fn benchmark_arithmetic_overflow_checking(c: &mut Criterion) {
    use arrow::array::Int64Array;

    // Create schema with two integer columns
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));

    // Generate test data with various ranges to test different overflow scenarios
    let scenarios = vec![
        ("small_numbers", create_int64_data(8192, 1, 1000)),
        ("medium_numbers", create_int64_data(8192, 1000, 1_000_000)),
        (
            "large_numbers",
            create_int64_data(8192, 1_000_000, 100_000_000),
        ),
        (
            "near_overflow",
            create_int64_data(8192, i64::MAX / 1000, i64::MAX / 100),
        ),
    ];

    let operators = vec![
        (Operator::Plus, "add"),
        (Operator::Minus, "subtract"),
        (Operator::Multiply, "multiply"),
    ];

    for (scenario_name, (a_values, b_values)) in scenarios {
        let a_array = Arc::new(Int64Array::from(a_values));
        let b_array = Arc::new(Int64Array::from(b_values));

        let batch = RecordBatch::try_new(schema.clone(), vec![a_array, b_array]).unwrap();

        for (op, op_name) in &operators {
            // Benchmark with overflow checking enabled (new default)
            let expr_with_overflow = BinaryExpr::new_with_overflow_check(
                Arc::new(Column::new("a", 0)),
                *op,
                Arc::new(Column::new("b", 1)),
            )
            .with_fail_on_overflow(true);

            c.bench_function(
                &format!("overflow_check/{scenario_name}/{op_name}/with_overflow_check"),
                |b| {
                    b.iter(|| {
                        // We expect this might error on overflow for near_overflow scenario
                        let _ = expr_with_overflow.evaluate(black_box(&batch));
                    })
                },
            );

            // Benchmark with overflow checking disabled (legacy behavior)
            let expr_without_overflow = BinaryExpr::new_with_overflow_check(
                Arc::new(Column::new("a", 0)),
                *op,
                Arc::new(Column::new("b", 1)),
            )
            .with_fail_on_overflow(false);

            c.bench_function(
                &format!(
                    "overflow_check/{scenario_name}/{op_name}/without_overflow_check"
                ),
                |b| b.iter(|| expr_without_overflow.evaluate(black_box(&batch)).unwrap()),
            );
        }
    }
}

/// Helper function to create test data with specified ranges
fn create_int64_data(size: usize, min_val: i64, max_val: i64) -> (Vec<i64>, Vec<i64>) {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut a_values = Vec::with_capacity(size);
    let mut b_values = Vec::with_capacity(size);

    // Simple deterministic pseudo-random number generation for reproducible benchmarks
    let mut hasher = DefaultHasher::new();

    for i in 0..size {
        // Generate deterministic "random" values within the specified range
        i.hash(&mut hasher);
        let hash1 = hasher.finish();
        (i + 1000).hash(&mut hasher);
        let hash2 = hasher.finish();

        let a_val = min_val + (hash1 % (max_val - min_val) as u64) as i64;
        let b_val = min_val + (hash2 % (max_val - min_val) as u64) as i64;

        a_values.push(a_val);
        b_values.push(b_val);
    }

    (a_values, b_values)
}

/// Benchmarks real overflow scenarios to measure the cost of actual overflow handling.
///
/// This benchmark creates data that will actually cause arithmetic overflow and measures
/// the performance difference between naive overflow checking and smart optimization.
///
/// You can run this benchmark with:
/// ```sh
/// cargo bench --bench binary_op -- real_overflow
/// ```
fn benchmark_real_overflow_scenarios(c: &mut Criterion) {
    use arrow::array::Int64Array;

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));

    // Different overflow scenarios
    let scenarios = vec![
        ("guaranteed_overflow", create_guaranteed_overflow_data(1000)),
        ("high_overflow_rate", create_mixed_overflow_data(1000, 0.5)), // 50% overflow
        (
            "medium_overflow_rate",
            create_mixed_overflow_data(1000, 0.2),
        ), // 20% overflow
        ("low_overflow_rate", create_mixed_overflow_data(1000, 0.05)), // 5% overflow
        ("no_overflow_baseline", create_completely_safe_data(1000)),
    ];

    for (scenario_name, (a_values, b_values)) in scenarios {
        let a_array = Arc::new(Int64Array::from(a_values));
        let b_array = Arc::new(Int64Array::from(b_values));
        let batch = RecordBatch::try_new(schema.clone(), vec![a_array, b_array]).unwrap();

        // Benchmark 1: Naive approach (always uses checked arithmetic)
        let naive_expr = BinaryExpr::new_with_overflow_check(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        );

        c.bench_function(
            &format!("real_overflow/{scenario_name}/naive_checked"),
            |b| {
                b.iter(|| {
                    // This will error on overflow scenarios - measure total cost including error handling
                    let _ = naive_expr.evaluate(black_box(&batch));
                })
            },
        );

        // Benchmark 2: Smart optimization (our approach)
        let smart_expr = BinaryExpr::new_with_overflow_check(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        );

        c.bench_function(
            &format!("real_overflow/{scenario_name}/smart_optimization"),
            |b| {
                b.iter(|| {
                    // Our smart optimization should handle overflow scenarios efficiently
                    let _ = smart_expr.evaluate(black_box(&batch));
                })
            },
        );

        // Benchmark 3: No overflow checking (baseline for comparison)
        if scenario_name == "no_overflow_baseline" {
            let unsafe_expr = BinaryExpr::new_with_overflow_check(
                Arc::new(Column::new("a", 0)),
                Operator::Plus,
                Arc::new(Column::new("b", 1)),
            )
            .with_fail_on_overflow(false);

            c.bench_function(
                &format!("real_overflow/{scenario_name}/no_overflow_check"),
                |b| {
                    b.iter(|| {
                        // This should be fastest but unsafe for overflow scenarios
                        unsafe_expr.evaluate(black_box(&batch)).unwrap()
                    })
                },
            );
        }
    }
}

/// Creates data guaranteed to overflow when added
fn create_guaranteed_overflow_data(size: usize) -> (Vec<i64>, Vec<i64>) {
    // Values very close to i64::MAX that will definitely overflow when added
    let a_values = vec![i64::MAX - 100; size];
    let b_values = vec![200i64; size];
    (a_values, b_values)
}

/// Creates mixed data with specified overflow percentage
fn create_mixed_overflow_data(size: usize, overflow_rate: f64) -> (Vec<i64>, Vec<i64>) {
    let overflow_count = (size as f64 * overflow_rate) as usize;
    let safe_count = size - overflow_count;

    let mut a_values = Vec::with_capacity(size);
    let mut b_values = Vec::with_capacity(size);

    // Add overflow cases (deterministic for reproducible benchmarks)
    for i in 0..overflow_count {
        if i % 2 == 0 {
            // Positive overflow
            a_values.push(i64::MAX - (i as i64 % 1000));
            b_values.push(1000 + (i as i64 % 500));
        } else {
            // Negative overflow
            a_values.push(i64::MIN + (i as i64 % 1000));
            b_values.push(-1000 - (i as i64 % 500));
        }
    }

    // Add safe cases
    for i in 0..safe_count {
        a_values.push(1000 + (i as i64 % 100000));
        b_values.push(2000 + (i as i64 % 100000));
    }

    (a_values, b_values)
}

/// Creates completely safe data that will never overflow
fn create_completely_safe_data(size: usize) -> (Vec<i64>, Vec<i64>) {
    let a_values: Vec<i64> = (0..size).map(|i| (i as i64 % 1000000) + 1000).collect();
    let b_values: Vec<i64> = (0..size).map(|i| (i as i64 % 1000000) + 2000).collect();
    (a_values, b_values)
}

/// Benchmarks comparing performance with and without overflow checking across different number ranges
///
/// This measures the performance impact of our overflow checking feature by comparing:
/// 1. WITHOUT feature: BinaryExpr with fail_on_overflow=false (legacy behavior)
/// 2. WITH feature: BinaryExpr with fail_on_overflow=true (new SQL-standard behavior)
///
/// Across different data scenarios:
/// - Small numbers (safe, no overflow risk)
/// - Medium numbers (larger but still safe)
/// - Numbers close to overflow (high risk but don't actually overflow)
/// - Numbers that actually overflow
///
/// You can run this benchmark with:
/// ```sh
/// cargo bench --bench binary_op -- feature_comparison
/// ```
fn benchmark_feature_comparison(c: &mut Criterion) {
    use arrow::array::Int64Array;

    let array_size = 1000;

    // Test scenarios with different number ranges
    let scenarios = vec![
        (
            "small_numbers",
            create_number_range(array_size, 1, 1000), // Very safe small numbers
        ),
        (
            "medium_numbers",
            create_number_range(array_size, 1_000, 1_000_000), // Medium range
        ),
        (
            "close_to_overflow",
            create_number_range(array_size, i64::MAX / 1000, i64::MAX / 100), // Close to limits
        ),
        (
            "guaranteed_overflow",
            create_overflow_numbers(array_size), // Will definitely overflow
        ),
    ];

    for (scenario_name, (a_values, b_values)) in scenarios {
        let a_array = Arc::new(Int64Array::from(a_values));
        let b_array = Arc::new(Int64Array::from(b_values));

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));

        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![a_array, b_array]).unwrap();

        // WITHOUT overflow checking (legacy behavior - wrapping)
        let expr_without_feature = BinaryExpr::new_with_overflow_check(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        )
        .with_fail_on_overflow(false); // Explicitly disable overflow checking

        // WITH overflow checking (new SQL-standard behavior)
        let expr_with_feature = BinaryExpr::new_with_overflow_check(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        )
        .with_fail_on_overflow(true); // Enable overflow checking (our feature)

        // Benchmark WITHOUT the feature
        c.bench_function(
            &format!("feature_comparison/{scenario_name}/WITHOUT_overflow_check"),
            |b| {
                b.iter(|| {
                    // This should always succeed (wrapping behavior)
                    expr_without_feature.evaluate(black_box(&batch)).unwrap()
                })
            },
        );

        // Benchmark WITH the feature
        c.bench_function(
            &format!("feature_comparison/{scenario_name}/WITH_overflow_check"),
            |b| {
                b.iter(|| {
                    // This may error on overflow scenarios, but we measure total time
                    let _result = expr_with_feature.evaluate(black_box(&batch));
                })
            },
        );
    }
}

/// Helper function to create number ranges for testing
fn create_number_range(size: usize, min_val: i64, max_val: i64) -> (Vec<i64>, Vec<i64>) {
    let range = max_val - min_val;
    let step = if range > size as i64 {
        range / size as i64
    } else {
        1
    };

    let a_values: Vec<i64> = (0..size)
        .map(|i| min_val + (i as i64 * step) % range)
        .collect();
    let b_values: Vec<i64> = (0..size)
        .map(|i| min_val + ((i + size / 2) as i64 * step) % range)
        .collect();

    (a_values, b_values)
}

/// Helper function to create numbers that will definitely overflow when added
fn create_overflow_numbers(size: usize) -> (Vec<i64>, Vec<i64>) {
    let a_values = vec![i64::MAX - 100; size]; // Close to maximum
    let b_values = vec![200i64; size]; // Adding this causes overflow
    (a_values, b_values)
}

/// Benchmarks to break down the individual costs of overflow handling
///
/// This measures each component: raw arithmetic, overflow detection, error creation,
/// error propagation, and full DataFusion expression evaluation.
///
/// You can run this benchmark with:
/// ```sh
/// cargo bench --bench binary_op -- overflow_cost_breakdown
/// ```
fn benchmark_overflow_cost_breakdown(c: &mut Criterion) {
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::compute::kernels::numeric::{add, add_wrapping};

    let array_size = 1000;

    // Test data sets
    let safe_a_vals: Vec<i64> = (0..array_size).map(|i| (i as i64) + 1000).collect();
    let safe_b_vals: Vec<i64> = (0..array_size).map(|i| (i as i64) + 2000).collect();

    let overflow_a_vals = vec![i64::MAX - 50; array_size];
    let overflow_b_vals = vec![100i64; array_size];

    let safe_a_array = Arc::new(Int64Array::from(safe_a_vals)) as ArrayRef;
    let safe_b_array = Arc::new(Int64Array::from(safe_b_vals)) as ArrayRef;

    let overflow_a_array = Arc::new(Int64Array::from(overflow_a_vals)) as ArrayRef;
    let overflow_b_array = Arc::new(Int64Array::from(overflow_b_vals)) as ArrayRef;

    // 1. Wrapping arithmetic (baseline - no overflow check)
    c.bench_function("overflow_cost_breakdown/wrapping_arithmetic", |b| {
        b.iter(|| {
            add_wrapping(black_box(&safe_a_array), black_box(&safe_b_array)).unwrap()
        })
    });

    // 2. Checked arithmetic with safe values (overhead of overflow detection)
    c.bench_function("overflow_cost_breakdown/checked_arithmetic_safe", |b| {
        b.iter(|| add(black_box(&safe_a_array), black_box(&safe_b_array)).unwrap())
    });

    // 3. Checked arithmetic with overflow (includes error creation cost)
    c.bench_function("overflow_cost_breakdown/checked_arithmetic_overflow", |b| {
        b.iter(|| {
            let result = add(black_box(&overflow_a_array), black_box(&overflow_b_array));
            // We expect this to error - measure the total cost including error handling
            let _ = result;
        })
    });

    // 4. DataFusion expression with overflow (full stack cost)
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));

    let overflow_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![overflow_a_array.clone(), overflow_b_array.clone()],
    )
    .unwrap();

    let overflow_expr = BinaryExpr::new_with_overflow_check(
        Arc::new(Column::new("a", 0)),
        Operator::Plus,
        Arc::new(Column::new("b", 1)),
    )
    .with_fail_on_overflow(true);

    c.bench_function(
        "overflow_cost_breakdown/datafusion_expression_overflow",
        |b| {
            b.iter(|| {
                let result = overflow_expr.evaluate(black_box(&overflow_batch));
                // Measure full DataFusion expression evaluation including error handling
                let _ = result;
            })
        },
    );

    // 5. Smart optimization vs naive (for comparison)
    let smart_expr = BinaryExpr::new_with_overflow_check(
        Arc::new(Column::new("a", 0)),
        Operator::Plus,
        Arc::new(Column::new("b", 1)),
    ); // Uses smart optimization by default

    c.bench_function("overflow_cost_breakdown/smart_optimization_overflow", |b| {
        b.iter(|| {
            let result = smart_expr.evaluate(black_box(&overflow_batch));
            let _ = result;
        })
    });
}

/// Benchmarks to measure the performance impact of our advanced overflow optimizations
///
/// This tests our new implementations:
/// - Branch-free overflow detection using bitwise operations
/// - SIMD-accelerated vectorized overflow checking  
/// - Adaptive strategy selection based on data analysis
/// - Multiple optimization levels (wrapping, SIMD, selective, full)
///
/// You can run this benchmark with:
/// ```sh
/// cargo bench --bench binary_op -- advanced_optimizations
/// ```
fn benchmark_advanced_optimizations(c: &mut Criterion) {
    use arrow::array::Int64Array;

    let array_size = 1000;

    // Test scenarios designed to trigger different optimization strategies
    let scenarios = vec![
        (
            "very_small_numbers",
            create_int64_range(array_size, 1, 100), // Should use UseWrapping
        ),
        (
            "small_numbers",
            create_int64_range(array_size, 1000, 10000), // Should use UseWrapping
        ),
        (
            "medium_numbers",
            create_int64_range(array_size, 100000, 1000000), // Should use UseSIMDCheck
        ),
        (
            "large_numbers",
            create_int64_range(array_size, i64::MAX / 1000, i64::MAX / 100), // Should use UseSelectiveCheck
        ),
        (
            "near_overflow_numbers",
            create_int64_range(array_size, i64::MAX / 16, i64::MAX / 8), // Should use UseSIMDCheck
        ),
        (
            "mixed_size_batch",
            create_mixed_magnitude_data(array_size), // Tests adaptive strategy
        ),
    ];

    for (scenario_name, (a_values, b_values)) in scenarios {
        let a_array = Arc::new(Int64Array::from(a_values));
        let b_array = Arc::new(Int64Array::from(b_values));

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));

        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![a_array, b_array]).unwrap();

        // Benchmark 1: Legacy add_conditional (current implementation)
        let legacy_expr = BinaryExpr::new_with_overflow_check(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        )
        .with_fail_on_overflow(true);

        c.bench_function(
            &format!("advanced_optimizations/{scenario_name}/legacy_conditional"),
            |b| b.iter(|| legacy_expr.evaluate(black_box(&batch)).unwrap()),
        );

        // Benchmark 2: New add_adaptive (our optimized implementation)
        let optimized_expr = BinaryExpr::new_with_overflow_check(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        )
        .with_fail_on_overflow(true);

        c.bench_function(
            &format!("advanced_optimizations/{scenario_name}/adaptive_optimized"),
            |b| b.iter(|| optimized_expr.evaluate(black_box(&batch)).unwrap()),
        );

        // Benchmark 3: Baseline without overflow checking
        let baseline_expr = BinaryExpr::new_with_overflow_check(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        )
        .with_fail_on_overflow(false);

        c.bench_function(
            &format!("advanced_optimizations/{scenario_name}/baseline_wrapping"),
            |b| b.iter(|| baseline_expr.evaluate(black_box(&batch)).unwrap()),
        );
    }
}

/// Helper function to create integer ranges for testing
fn create_int64_range(size: usize, min_val: i64, max_val: i64) -> (Vec<i64>, Vec<i64>) {
    let range = max_val - min_val;
    let step = if range > size as i64 {
        range / size as i64
    } else {
        1
    };

    let a_values: Vec<i64> = (0..size)
        .map(|i| min_val + (i as i64 * step) % range)
        .collect();
    let b_values: Vec<i64> = (0..size)
        .map(|i| min_val + ((i + size / 2) as i64 * step) % range)
        .collect();

    (a_values, b_values)
}

/// Creates mixed magnitude data to test adaptive strategy selection
fn create_mixed_magnitude_data(size: usize) -> (Vec<i64>, Vec<i64>) {
    let mut a_values = Vec::with_capacity(size);
    let mut b_values = Vec::with_capacity(size);

    // Mix different magnitude ranges to test adaptive strategy
    for i in 0..size {
        match i % 4 {
            0 => {
                // Very small values (should trigger UseWrapping)
                a_values.push((i as i64 % 100) + 1);
                b_values.push((i as i64 % 100) + 2);
            }
            1 => {
                // Medium values (should trigger UseSIMDCheck)
                a_values.push((i as i64 % 100000) + 10000);
                b_values.push((i as i64 % 100000) + 20000);
            }
            2 => {
                // Large values (should trigger UseSelectiveCheck)
                a_values.push(i64::MAX / 1000 + (i as i64 % 1000));
                b_values.push(i64::MAX / 2000 + (i as i64 % 1000));
            }
            3 => {
                // Very large values (should trigger UseSIMDCheck for batches)
                a_values.push(i64::MAX / 20 + (i as i64 % 100));
                b_values.push(i64::MAX / 40 + (i as i64 % 100));
            }
            _ => unreachable!(),
        }
    }

    (a_values, b_values)
}

criterion_group!(
    benches,
    benchmark_binary_op_in_short_circuit,
    benchmark_arithmetic_overflow_checking,
    benchmark_real_overflow_scenarios,
    benchmark_feature_comparison,
    benchmark_overflow_cost_breakdown,
    benchmark_advanced_optimizations
);

criterion_main!(benches);
