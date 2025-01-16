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

use std::process::Command;
use std::str;

use log::info;

use crate::memory_limit::memory_limit_validation::utils;

/// Runner that executes each test in a separate process with the required environment
/// variable set. Memory limit validation tasks need to measure memory resident set
/// size (RSS), so they must run in a separate process.
#[test]
fn test_runner() {
    let tests = vec![
        "memory_limit_validation_runner_works",
        "sort_no_mem_limit",
        "sort_with_mem_limit_1",
        "sort_with_mem_limit_2",
        "sort_with_mem_limit_3",
        "sort_with_mem_limit_2_cols_1",
        "sort_with_mem_limit_2_cols_2",
    ];

    let mut handles = vec![];

    // Run tests in parallel, each test in a separate process
    for test in tests {
        let test = test.to_string();
        let handle = std::thread::spawn(move || {
            let test_path = format!(
                "memory_limit::memory_limit_validation::sort_mem_validation::{}",
                test
            );
            info!("Running test: {}", test_path);

            // Run the test command
            let output = Command::new("cargo")
                .arg("test")
                .arg("--package")
                .arg("datafusion")
                .arg("--test")
                .arg("core_integration")
                .arg("--")
                .arg(&test_path)
                .arg("--exact")
                .arg("--nocapture")
                .env("DATAFUSION_TEST_MEM_LIMIT_VALIDATION", "1")
                .output()
                .expect("Failed to execute test command");

            // Convert output to strings
            let stdout = str::from_utf8(&output.stdout).unwrap_or("");
            let stderr = str::from_utf8(&output.stderr).unwrap_or("");

            info!("{}", stdout);

            // Return test result and output for error reporting
            (test, output.status, stdout.to_string(), stderr.to_string())
        });
        handles.push(handle);
    }

    // Wait for all threads to complete and check results
    for handle in handles {
        let (test, status, stdout, stderr) = handle.join().unwrap();
        assert!(
            status.success(),
            "Test '{}' failed with status: {}\nstdout:\n{}\nstderr:\n{}",
            test,
            status,
            stdout,
            stderr
        );
    }
}

// All following tests need to be run through the `test_runner()` function.
// When run directly, environment variable `DATAFUSION_TEST_MEM_LIMIT_VALIDATION`
// is not set, test will return with a no-op.
//
// If some tests consistently fail, suppress by setting a larger expected memory
// usage (e.g. 80_000_000 * 3 -> 80_000_000 * 4)
// ===========================================================================

/// Test runner itself: if memory limit violated, test should fail.
#[tokio::test]
async fn test_memory_limit_validation_runner() {
    if std::env::var("DATAFUSION_TEST_MEM_LIMIT_VALIDATION").is_err() {
        println!("Skipping test because DATAFUSION_TEST_MEM_LIMIT_VALIDATION is not set");

        return;
    }

    let result = std::panic::catch_unwind(|| {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            utils::validate_query_with_memory_limits(
                20_000_000, // set an impossible limit: query requires at least 80MB
                None,
                "select * from generate_series(1,10000000) as t1(c1) order by c1",
                "select * from generate_series(1,1000000) as t1(c1) order by c1", // Baseline query with ~10% of data
            )
            .await;
        })
    });

    assert!(
        result.is_err(),
        "Expected the query to panic due to memory limit"
    );
}

#[tokio::test]
async fn sort_no_mem_limit() {
    utils::validate_query_with_memory_limits(
        80_000_000 * 3,
        None,
        "select * from generate_series(1,10000000) as t1(c1) order by c1",
        "select * from generate_series(1,1000000) as t1(c1) order by c1", // Baseline query with ~10% of data
    )
    .await;
}

#[tokio::test]
async fn sort_with_mem_limit_1() {
    utils::validate_query_with_memory_limits(
        40_000_000 * 5,
        Some(40_000_000),
        "select * from generate_series(1,10000000) as t1(c1) order by c1",
        "select * from generate_series(1,1000000) as t1(c1) order by c1", // Baseline query with ~10% of data
    )
    .await;
}

#[tokio::test]
async fn sort_with_mem_limit_2() {
    utils::validate_query_with_memory_limits(
        80_000_000 * 3,
        Some(80_000_000),
        "select * from generate_series(1,10000000) as t1(c1) order by c1",
        "select * from generate_series(1,1000000) as t1(c1) order by c1", // Baseline query with ~10% of data
    )
    .await;
}

#[tokio::test]
async fn sort_with_mem_limit_3() {
    utils::validate_query_with_memory_limits(
        80_000_000 * 3,
        Some(80_000_000 * 10), // mem limit is large enough so that no spill happens
        "select * from generate_series(1,10000000) as t1(c1) order by c1",
        "select * from generate_series(1,1000000) as t1(c1) order by c1", // Baseline query with ~10% of data
    )
    .await;
}

#[tokio::test]
async fn sort_with_mem_limit_2_cols_1() {
    let memory_usage_in_theory = 80_000_000 * 2; // 2 columns
    let expected_max_mem_usage = memory_usage_in_theory * 4;
    utils::validate_query_with_memory_limits(
        expected_max_mem_usage,
        None,
        "select c1, c1 as c2 from generate_series(1,10000000) as t1(c1) order by c2 DESC, c1 ASC NULLS LAST",
        "select c1, c1 as c2 from generate_series(1,1000000) as t1(c1) order by c2 DESC, c1 ASC NULLS LAST", // Baseline query with ~10% of data
    )
    .await;
}

// TODO: Query fails, fix it
#[ignore]
#[tokio::test]
async fn sort_with_mem_limit_2_cols_2() {
    let memory_usage_in_theory = 80_000_000 * 2; // 2 columns
    let expected_max_mem_usage = memory_usage_in_theory * 3;
    let mem_limit = memory_usage_in_theory as f64 * 0.5;

    utils::validate_query_with_memory_limits(
        expected_max_mem_usage,
        Some(mem_limit as i64),
        "select c1, c1 as c2 from generate_series(1,10000000) as t1(c1) order by c2 DESC, c1 ASC NULLS LAST",
        "select c1, c1 as c2 from generate_series(1,1000000) as t1(c1) order by c2 DESC, c1 ASC NULLS LAST", // Baseline query with ~10% of data
    )
    .await;
}
