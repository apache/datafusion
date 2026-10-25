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

//! Memory validation for aggregates.
//!
//! These tests run in separate processes to accurately measure memory usage.

use datafusion::prelude::SessionConfig;

use crate::utils;

#[test]
fn group_by_few_groups_runner() {
    utils::spawn_test_process("aggregate_mem_validation", "group_by_few_groups");
}

#[test]
fn distinct_runner() {
    utils::spawn_test_process("aggregate_mem_validation", "distinct");
}

#[test]
fn median_runner() {
    utils::spawn_test_process("aggregate_mem_validation", "median");
}

#[test]
fn array_agg_few_groups_runner() {
    utils::spawn_test_process("aggregate_mem_validation", "array_agg_few_groups");
}

#[test]
fn array_agg_few_groups_one_partition_runner() {
    utils::spawn_test_process(
        "aggregate_mem_validation",
        "array_agg_few_groups_one_partition",
    );
}

/// Few groups: the aggregate holds almost nothing, whatever the input size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_by_few_groups() {
    utils::validate_query_over_rows_with_strings(
        20_000_000,
        SessionConfig::new().with_target_partitions(4),
        "AggregateExec",
        |t| format!("SELECT v % 10 AS k, count(*), max(s) FROM {t} GROUP BY k"),
    )
    .await;
}

/// Every row is its own group.
#[ignore = "hash table grows before it is reserved, see https://github.com/apache/datafusion/issues/25606"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct() {
    utils::validate_query_over_rows_with_strings(
        1_800_000_000,
        SessionConfig::new().with_target_partitions(4),
        "AggregateExec",
        |t| format!("SELECT DISTINCT s FROM {t}"),
    )
    .await;
}

/// The state of `median` holds every input value.
#[ignore = "partial state reaches the final aggregate unreserved, see https://github.com/apache/datafusion/issues/26705"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn median() {
    utils::validate_query_over_rows_with_strings(
        80_000_000 * 4,
        SessionConfig::new().with_target_partitions(4),
        "AggregateExec",
        |t| format!("SELECT median(v) FROM {t}"),
    )
    .await;
}

/// Few groups, each with a state that holds every input value of the group.
#[ignore = "partial state reaches the final aggregate unreserved, see https://github.com/apache/datafusion/issues/26705"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn array_agg_few_groups() {
    utils::validate_query_over_rows_with_strings(
        2_500_000_000,
        SessionConfig::new().with_target_partitions(4),
        "AggregateExec",
        |t| format!("SELECT v % 10 AS k, array_agg(s) FROM {t} GROUP BY k"),
    )
    .await;
}

/// Like `array_agg_few_groups`, in one partition, so there is a single
/// aggregate whose output is as large as its input.
#[ignore = "aggregate output is materialized unreserved, see https://github.com/apache/datafusion/issues/26701"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn array_agg_few_groups_one_partition() {
    utils::validate_query_over_rows_with_strings(
        1_200_000_000,
        SessionConfig::new().with_target_partitions(1),
        "AggregateExec",
        |t| format!("SELECT v % 10 AS k, array_agg(s) FROM {t} GROUP BY k"),
    )
    .await;
}
