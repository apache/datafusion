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
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_expr::{Operator, and, binary_expr, col, lit, or};
use datafusion_physical_expr::{
    PhysicalExpr,
    expressions::{BinaryExpr, Column},
    planner::logical2physical,
};
use std::hint::black_box;
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
    let expr_and = BinaryExpr::new(
        Arc::new(Column::new("a", 0)),
        Operator::And,
        logical2physical(&right_condition_and, &schema),
    );

    // a OR ((b ~ regex) OR (c ~ regex))
    let expr_or = BinaryExpr::new(
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
#[expect(clippy::needless_pass_by_value)]
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

criterion_group!(benches, benchmark_binary_op_in_short_circuit);

criterion_main!(benches);
