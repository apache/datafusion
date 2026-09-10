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

//! Memory-limit validation tests for sort-merge join queries.
//!
//! These tests run in separate processes to accurately measure memory usage.

use datafusion::prelude::SessionConfig;

use crate::memory_limit::memory_limit_validation::utils;

/// Ensures the planner selected a sort-merge join.
const SMJ_OPERATOR_NAME: &str = "SortMergeJoinExec";

/// Configure a two-partition sort-merge join and reduce the sort reservation so
/// the join can spill under the tested memory limits.
fn smj_session_config() -> SessionConfig {
    SessionConfig::new()
        .with_target_partitions(2)
        .with_sort_spill_reservation_bytes(1024 * 1024)
        .set_bool("datafusion.optimizer.prefer_hash_join", false)
}

/// Build a join with one large buffered key group and scalar output.
fn smj_sum_query(series_len: usize) -> String {
    format!(
        "SELECT sum(rr.v) FROM generate_series(0, 0) AS l(k) \
         JOIN (SELECT i % 1 AS k, i AS v FROM generate_series(1, {series_len}) AS r(i)) rr \
         ON l.k = rr.k"
    )
}

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

/// Verify a 40 MB pool forces spilling within the RSS allowance.
#[tokio::test]
async fn smj_with_mem_limit_1() {
    utils::validate_query_with_memory_limits_and_config(
        40_000_000 * 4,
        Some(40_000_000),
        &smj_sum_query(5_000_000),
        &smj_sum_query(500_000),
        smj_session_config(),
        Some(SMJ_OPERATOR_NAME),
        Some(true),
    )
    .await;
}

/// Verify a 16 MB pool forces spilling. The 5M join keys (~40 MB) stay resident
/// independently of the pool limit, so this case needs a larger RSS allowance.
#[tokio::test]
async fn smj_with_mem_limit_2() {
    utils::validate_query_with_memory_limits_and_config(
        16_000_000 * 12,
        Some(16_000_000),
        &smj_sum_query(5_000_000),
        &smj_sum_query(500_000),
        smj_session_config(),
        Some(SMJ_OPERATOR_NAME),
        Some(true),
    )
    .await;
}

#[tokio::test]
async fn smj_no_mem_limit() {
    utils::validate_query_with_memory_limits_and_config(
        40_000_000 * 5,
        None,
        &smj_sum_query(5_000_000),
        &smj_sum_query(500_000),
        smj_session_config(),
        Some(SMJ_OPERATOR_NAME),
        Some(false),
    )
    .await;
}
