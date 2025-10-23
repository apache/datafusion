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

//! Memory limit validation tests for the sort queries
//!
//! These tests must run in separate processes to accurately measure memory usage.
//! This file is organized as:
//! - Test runners that spawn individual test processes
//! - Test cases that contain the actual validation logic
use log::info;
use std::sync::Once;
use std::{process::Command, str};

use crate::memory_limit::memory_limit_validation::utils;

static INIT: Once = Once::new();

// ===========================================================================
// Test runners:
// Runners are split into multiple tests to run in parallel
// ===========================================================================

#[test]
fn memory_limit_validation_runner_works_runner() {
    spawn_test_process("memory_limit_validation_runner_works");
}

#[test]
fn sort_no_mem_limit_runner() {
    spawn_test_process("sort_no_mem_limit");
}

#[test]
fn sort_with_mem_limit_1_runner() {
    spawn_test_process("sort_with_mem_limit_1");
}

#[test]
fn sort_with_mem_limit_2_runner() {
    spawn_test_process("sort_with_mem_limit_2");
}

#[test]
fn sort_with_mem_limit_3_runner() {
    spawn_test_process("sort_with_mem_limit_3");
}

#[test]
fn sort_with_mem_limit_2_cols_1_runner() {
    spawn_test_process("sort_with_mem_limit_2_cols_1");
}

#[test]
fn sort_with_mem_limit_2_cols_2_runner() {
    spawn_test_process("sort_with_mem_limit_2_cols_2");
}

/// `spawn_test_process` might trigger multiple recompilations and the test binary
/// size might grow indefinitely. This initializer ensures recompilation is only done
/// once and the target size is bounded.
///
/// TODO: This is a hack, can be cleaned up if we have a better way to let multiple
/// test cases run in different processes (instead of different threads by default)
fn init_once() {
    INIT.call_once(|| {
        let _ = Command::new("cargo")
            .arg("test")
            .arg("--no-run")
            .arg("--package")
            .arg("datafusion")
            .arg("--test")
            .arg("core_integration")
            .arg("--features")
            .arg("extended_tests")
            .env("DATAFUSION_TEST_MEM_LIMIT_VALIDATION", "1")
            .output()
            .expect("Failed to execute test command");
    });
}

/// Helper function that executes a test in a separate process with the required environment
/// variable set. Memory limit validation tasks need to measure memory resident set
/// size (RSS), so they must run in a separate process.
fn spawn_test_process(test: &str) {
    init_once();

    let test_path =
        format!("memory_limit::memory_limit_validation::sort_mem_validation::{test}");
    info!("Running test: {test_path}");

    // Run the test command
    let output = Command::new("cargo")
        .arg("test")
        .arg("--package")
        .arg("datafusion")
        .arg("--test")
        .arg("core_integration")
        .arg("--features")
        .arg("extended_tests")
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

    assert!(
        output.status.success(),
        "Test '{}' failed with status: {}\nstdout:\n{}\nstderr:\n{}",
        test,
        output.status,
        stdout,
        stderr
    );
}

// ===========================================================================
// Test cases:
// All following tests need to be run through their individual test wrapper.
// When run directly, environment variable `DATAFUSION_TEST_MEM_LIMIT_VALIDATION`
// is not set, test will return with a no-op.
//
// If some tests consistently fail, suppress by setting a larger expected memory
// usage (e.g. 80_000_000 * 3 -> 80_000_000 * 4)
// ===========================================================================

/// Test runner itself: if memory limit violated, test should fail.
#[tokio::test]
async fn memory_limit_validation_runner_works() {
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
// Issue: https://github.com/apache/datafusion/issues/14143
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
