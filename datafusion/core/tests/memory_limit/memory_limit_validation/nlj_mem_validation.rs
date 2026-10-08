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

//! Memory validation for the nested loop join's buffered build side.

use datafusion::prelude::SessionConfig;

use crate::memory_limit::memory_limit_validation::utils;

#[test]
fn nlj_no_mem_limit_runner() {
    utils::spawn_test_process("nlj_mem_validation", "nlj_no_mem_limit");
}

// NLJ buffers the entire build side and uses a small, constant amount of memory
// to stream the probe side.
//
// This test verifies that an NLJ without a memory limit follows this estimate.
#[tokio::test]
async fn nlj_no_mem_limit() {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(8192)
        // Keep the large input on the build side, as written in the query.
        .set_bool("datafusion.optimizer.join_reordering", false);

    utils::validate_query_with_memory_limits_and_config(
        90_000_000, // 80 MB for the build side + 10 MB of extra room.
        None,
        "SELECT count(*), sum(l.value)
         FROM generate_series(1, 10000000) AS l
         JOIN generate_series(1, 1) AS r
             ON (l.value + r.value) % 2 = 0",
        "SELECT count(*), sum(l.value)
         FROM generate_series(1, 1000000) AS l
         JOIN generate_series(1, 1) AS r
             ON (l.value + r.value) % 2 = 0",
        config,
        Some("NestedLoopJoinExec"),
        Some(false),
    )
    .await;
}
