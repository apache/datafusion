// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Memory validation for the nested loop join's buffered build side.

use datafusion::prelude::SessionConfig;

use crate::memory_limit_validation::utils;

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
