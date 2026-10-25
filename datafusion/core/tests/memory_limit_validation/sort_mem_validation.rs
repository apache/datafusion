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

use datafusion::prelude::SessionConfig;

use crate::utils;

// ===========================================================================
// Test runners:
// Runners are split into multiple tests to run in parallel
// ===========================================================================

#[test]
fn memory_limit_validation_runner_works_runner() {
    utils::spawn_test_process(
        "sort_mem_validation",
        "memory_limit_validation_runner_works",
    );
}

#[test]
fn sort_no_mem_limit_runner() {
    utils::spawn_test_process("sort_mem_validation", "sort_no_mem_limit");
}

#[test]
fn sort_with_mem_limit_1_runner() {
    utils::spawn_test_process("sort_mem_validation", "sort_with_mem_limit_1");
}

#[test]
fn sort_with_mem_limit_2_runner() {
    utils::spawn_test_process("sort_mem_validation", "sort_with_mem_limit_2");
}

#[test]
fn sort_with_mem_limit_3_runner() {
    utils::spawn_test_process("sort_mem_validation", "sort_with_mem_limit_3");
}

#[test]
fn sort_with_mem_limit_2_cols_1_runner() {
    utils::spawn_test_process("sort_mem_validation", "sort_with_mem_limit_2_cols_1");
}

#[test]
fn sort_with_mem_limit_2_cols_2_runner() {
    utils::spawn_test_process("sort_mem_validation", "sort_with_mem_limit_2_cols_2");
}

#[test]
fn sort_strings_no_mem_limit_runner() {
    utils::spawn_test_process("sort_mem_validation", "sort_strings_no_mem_limit");
}

#[test]
fn topk_no_mem_limit_runner() {
    utils::spawn_test_process("sort_mem_validation", "topk_no_mem_limit");
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
#[ignore = "Query fails, see the issue above"]
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

/// Sorts on a column the input is not ordered by, so the sort is not removed.
#[ignore = "sorted runs wait in the merge unreserved, see https://github.com/apache/datafusion/issues/26411"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sort_strings_no_mem_limit() {
    utils::validate_query_over_rows_with_strings(
        700_000_000,
        SessionConfig::new().with_target_partitions(4),
        "SortExec",
        |t| format!("SELECT v, s FROM {t} ORDER BY s DESC"),
    )
    .await;
}

/// TopK holds 10 rows, whatever the input size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topk_no_mem_limit() {
    utils::validate_query_over_rows_with_strings(
        40_000_000,
        SessionConfig::new().with_target_partitions(4),
        "SortExec: TopK",
        |t| format!("SELECT v, s FROM {t} ORDER BY s DESC LIMIT 10"),
    )
    .await;
}
