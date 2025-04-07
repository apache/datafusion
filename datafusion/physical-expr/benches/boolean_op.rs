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
    compute::{bool_and, bool_or},
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::{Arc, LazyLock};

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

    cases
}

fn benchmark_boolean_ops(c: &mut Criterion) {
    let len = 1_000_000; // Use one million elements for clear performance differentiation
    static TEST_BOOL_COUNT: LazyLock<bool> =
        LazyLock::new(|| match std::env::var("TEST_BOOL_COUNT") {
            Ok(_) => {
                println!("TEST_BOOL_COUNT=ON");
                true
            }
            Err(_) => {
                println!("TEST_BOOL_COUNT=OFF");
                false
            }
        });

    // Determine the test function to be executed based on the ENV `TEST_BOOL_COUNT`
    fn test_func<const TEST_ALL_FALSE: bool>(array: &BooleanArray) -> bool {
        // Use false_count for all false and true_count for all true
        if *TEST_BOOL_COUNT {
            if TEST_ALL_FALSE {
                array.false_count() == array.len()
            } else {
                array.true_count() == array.len()
            }
        }
        // Use bool_or for all false and bool_and for all true
        else if TEST_ALL_FALSE {
            match bool_or(array) {
                Some(v) => !v,
                None => false,
            }
        } else {
            bool_and(array).unwrap_or(false)
        }
    }

    // Test cases for false_count and bool_or
    {
        let test_cases = generate_boolean_cases::<true>(len);
        for (scenario, array) in test_cases {
            let arr_ref = Arc::new(array);

            // Benchmark test_func across different scenarios
            c.bench_function(&scenario, |b| {
                b.iter(|| test_func::<true>(black_box(&arr_ref)))
            });
        }
    }
    // Test cases for true_count and bool_and
    {
        let test_cases = generate_boolean_cases::<false>(len);
        for (scenario, array) in test_cases {
            let arr_ref = Arc::new(array);

            // Benchmark test_func across different scenarios
            c.bench_function(&scenario, |b| {
                b.iter(|| test_func::<false>(black_box(&arr_ref)))
            });
        }
    }
}

criterion_group!(benches, benchmark_boolean_ops);
criterion_main!(benches);
