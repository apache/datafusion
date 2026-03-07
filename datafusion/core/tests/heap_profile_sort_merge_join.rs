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

//! Heap profiling test for SortMergeJoinExec.
//! Data is larger than the memory pool to exercise sort spilling
//! before the merge join.

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_execution::memory_pool::FairSpillPool;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;

const MEMORY_LIMIT: usize = 40 * 1024 * 1024; // 40MB

#[tokio::test]
async fn heap_profile_sort_merge_join() {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(MEMORY_LIMIT)))
        .build_arc()
        .unwrap();
    // Force sort-merge join: disable hash join preference and use
    // multiple partitions so CollectLeft hash join isn't chosen.
    let config = SessionConfig::new()
        .with_target_partitions(4)
        .with_sort_spill_reservation_bytes(2 * 1024 * 1024)
        .set_str("datafusion.optimizer.prefer_hash_join", "false");
    let ctx = SessionContext::new_with_config_rt(config, runtime);

    // Create tables before starting the profiler
    ctx.sql(
        "CREATE TABLE t1 AS \
         SELECT v AS id, v * 2 AS val \
         FROM generate_series(1, 1000000) AS t(v)",
    )
    .await
    .unwrap();

    ctx.sql(
        "CREATE TABLE t2 AS \
         SELECT v AS id, v * 3 AS val \
         FROM generate_series(1, 1000000) AS t(v)",
    )
    .await
    .unwrap();

    // Verify SortMergeJoin is used
    let explain = ctx
        .sql("EXPLAIN SELECT t1.id, t1.val, t2.val FROM t1 JOIN t2 ON t1.id = t2.id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plan_str = format!("{explain:?}");
    assert!(
        plan_str.contains("SortMergeJoin"),
        "Expected SortMergeJoin in plan but got: {plan_str}"
    );

    // Start profiling after table creation
    let _profiler = dhat::Profiler::builder().testing().build();

    let df = ctx
        .sql("SELECT t1.id, t1.val, t2.val FROM t1 JOIN t2 ON t1.id = t2.id")
        .await
        .unwrap();
    let _batches = df.collect().await.unwrap();

    let stats = dhat::HeapStats::get();
    let limit = (MEMORY_LIMIT as f64 * 1.1) as usize;
    println!(
        "sort_merge_join: max_bytes={}, memory_limit={}, ratio={:.2}x",
        stats.max_bytes,
        MEMORY_LIMIT,
        stats.max_bytes as f64 / MEMORY_LIMIT as f64
    );
    dhat::assert!(
        stats.max_bytes < limit,
        "Peak heap {} exceeded {}",
        stats.max_bytes,
        limit
    );
}
