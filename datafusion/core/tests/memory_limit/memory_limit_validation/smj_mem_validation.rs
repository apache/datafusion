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

//! Validates that the physical (RSS) memory usage of sort-merge join queries
//! stays within the configured memory limit. Tests must run in separate
//! processes for accurate measurement, so runners spawn each test case as its
//! own process.

use datafusion::prelude::SessionConfig;

use crate::memory_limit::memory_limit_validation::utils;

/// Guards against validating a hash join the planner picked instead.
const SMJ_OPERATOR_NAME: &str = "SortMergeJoinExec";

/// Force the planner to pick a sort-merge join and let it spill under a tight
/// pool: SMJ is only chosen with hash joins disabled and partitions > 1 (kept
/// at the minimum of 2 since each partition multiplies the per-sorter merge
/// reservation), and `sort_spill_reservation_bytes` is shrunk from its 10MB
/// default, which would otherwise exhaust a small pool before any data spills.
fn smj_session_config() -> SessionConfig {
    SessionConfig::new()
        .with_target_partitions(2)
        .with_sort_spill_reservation_bytes(1024 * 1024)
        .set_bool("datafusion.optimizer.prefer_hash_join", false)
}

/// Equi-join with one large right-side key group that overflows the configured
/// pools; `sum(v)` keeps the output to one row.
fn smj_sum_query(series_len: usize) -> String {
    format!(
        "SELECT sum(rr.v) FROM generate_series(0, 0) AS l(k) \
         JOIN (SELECT i % 1 AS k, i AS v FROM generate_series(1, {series_len}) AS r(i)) rr \
         ON l.k = rr.k"
    )
}

// ===========================================================================
// Test runners:
// Runners are split into multiple tests to run in parallel
// ===========================================================================

#[test]
fn smj_with_mem_limit_1_runner() {
    utils::spawn_test_process("smj_mem_validation", "smj_with_mem_limit_1");
}

#[test]
fn smj_with_mem_limit_2_runner() {
    utils::spawn_test_process("smj_mem_validation", "smj_with_mem_limit_2");
}

#[test]
fn smj_no_mem_limit_runner() {
    utils::spawn_test_process("smj_mem_validation", "smj_no_mem_limit");
}

// ===========================================================================
// Test cases:
// All following tests need to be run through their individual test wrapper.
// When run directly, environment variable `DATAFUSION_TEST_MEM_LIMIT_VALIDATION`
// is not set, test will return with a no-op.
//
// If some tests consistently fail, suppress by setting a larger expected memory
// usage (e.g. 40_000_000 * 4 -> 40_000_000 * 5)
// ===========================================================================

/// 40MB limit against one ~80MB buffered key group: the join must spill.
#[tokio::test]
async fn smj_with_mem_limit_1() {
    utils::validate_query_with_memory_limits_and_config(
        40_000_000 * 4,
        Some(40_000_000),
        &smj_sum_query(5_000_000),
        &smj_sum_query(500_000), // Baseline query with ~10% of data
        smj_session_config(),
        Some(SMJ_OPERATOR_NAME),
        Some(true),
    )
    .await;
}

/// Tighter 16MB limit forces more aggressive spilling.
#[tokio::test]
async fn smj_with_mem_limit_2() {
    utils::validate_query_with_memory_limits_and_config(
        16_000_000 * 5,
        Some(16_000_000),
        &smj_sum_query(5_000_000),
        &smj_sum_query(500_000), // Baseline query with ~10% of data
        smj_session_config(),
        Some(SMJ_OPERATOR_NAME),
        Some(true),
    )
    .await;
}

/// No memory limit: the join must not spill.
#[tokio::test]
async fn smj_no_mem_limit() {
    utils::validate_query_with_memory_limits_and_config(
        40_000_000 * 5,
        None,
        &smj_sum_query(5_000_000),
        &smj_sum_query(500_000), // Baseline query with ~10% of data
        smj_session_config(),
        Some(SMJ_OPERATOR_NAME),
        Some(false),
    )
    .await;
}
