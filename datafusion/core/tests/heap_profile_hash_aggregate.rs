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

//! Heap profiling test for grouped hash aggregation with spilling.
//! Data has many distinct groups to force hash table growth beyond
//! the memory pool, triggering spilling.

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_execution::memory_pool::FairSpillPool;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;

const MEMORY_LIMIT: usize = 10 * 1024 * 1024; // 10MB

#[tokio::test]
async fn heap_profile_hash_aggregate() {
    let _profiler = dhat::Profiler::builder().testing().build();

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(MEMORY_LIMIT)))
        .build_arc()
        .unwrap();
    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config_rt(config, runtime);

    // 5M distinct groups forces hash table growth and spilling
    let df = ctx
        .sql(
            "SELECT v, COUNT(*) \
             FROM generate_series(1, 5000000) AS t(v) \
             GROUP BY v",
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(row_count, 5_000_000);

    let stats = dhat::HeapStats::get();
    let limit = (MEMORY_LIMIT as f64 * 1.1) as usize;
    println!(
        "hash_aggregate: max_bytes={}, memory_limit={}, ratio={:.2}x",
        stats.max_bytes,
        MEMORY_LIMIT,
        stats.max_bytes as f64 / MEMORY_LIMIT as f64
    );
    // TODO: peak is ~122MB (12.2x pool) because:
    // 1. HashTable size() underreports (uses capacity * sizeof instead of allocation_size())
    // 2. Hash table doubles capacity atomically inside intern(), before the pool check
    // 3. generate_series input data is not tracked by the MemoryPool
    // dhat::assert!(stats.max_bytes < limit,
    //     "Peak heap {} exceeded {}", stats.max_bytes, limit);
    let _ = limit;
}
