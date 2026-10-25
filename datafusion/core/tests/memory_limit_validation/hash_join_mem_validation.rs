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

//! Memory validation for hash joins.
//!
//! These tests run in separate processes to accurately measure memory usage.

use datafusion::prelude::SessionConfig;

use crate::utils;

#[test]
fn hash_join_partitioned_runner() {
    utils::spawn_test_process("hash_join_mem_validation", "hash_join_partitioned");
}

#[test]
fn hash_join_collect_left_runner() {
    utils::spawn_test_process("hash_join_mem_validation", "hash_join_collect_left");
}

/// Both inputs are repartitioned on the join key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_join_partitioned() {
    utils::validate_query_over_rows_with_strings(
        80_000_000 * 5,
        SessionConfig::new().with_target_partitions(4),
        "HashJoinExec: mode=Partitioned",
        |t| format!("SELECT count(*), max(b.s) FROM {t} a JOIN {t} b ON a.v = b.v"),
    )
    .await;
}

/// Every partition probes one build side.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_join_collect_left() {
    utils::validate_query_over_rows_with_strings(
        80_000_000 * 4,
        SessionConfig::new()
            .with_target_partitions(4)
            .set_bool("datafusion.optimizer.repartition_joins", false),
        "HashJoinExec: mode=CollectLeft",
        |t| format!("SELECT count(*), max(b.s) FROM {t} a JOIN {t} b ON a.v = b.v"),
    )
    .await;
}
