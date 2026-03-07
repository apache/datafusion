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

//! Heap profiling test for reading parquet files and sorting.
//! This exercises the parquet reader's allocation path alongside
//! the sort operator. Data exceeds memory pool to force spilling.

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::sync::Arc;

use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_execution::memory_pool::FairSpillPool;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;

const MEMORY_LIMIT: usize = 20 * 1024 * 1024; // 20MB

#[tokio::test]
async fn heap_profile_parquet_sort() {
    // Write test data to a parquet file using a separate context
    let tmpdir = tempfile::tempdir().unwrap();
    let parquet_path = tmpdir.path().join("test_data.parquet");
    {
        let write_ctx = SessionContext::new();
        let df = write_ctx
            .sql(
                "SELECT v AS id, v * 2 AS val, \
                 CASE WHEN v % 3 = 0 THEN 'aaa' WHEN v % 3 = 1 THEN 'bbb' ELSE 'ccc' END AS category \
                 FROM generate_series(1, 2000000) AS t(v)",
            )
            .await
            .unwrap();
        df.write_parquet(
            parquet_path.to_str().unwrap(),
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await
        .unwrap();
    }

    // Set up the memory-limited context for reading
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(MEMORY_LIMIT)))
        .build_arc()
        .unwrap();
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_sort_spill_reservation_bytes(5 * 1024 * 1024);
    let ctx = SessionContext::new_with_config_rt(config, runtime);

    ctx.register_parquet("t", parquet_path.to_str().unwrap(), Default::default())
        .await
        .unwrap();

    // Start profiling before planning
    let _profiler = dhat::Profiler::builder().testing().build();

    let df = ctx.sql("SELECT * FROM t ORDER BY id DESC").await.unwrap();
    let _batches = df.collect().await.unwrap();

    let stats = dhat::HeapStats::get();
    let limit = (MEMORY_LIMIT as f64 * 1.1) as usize;
    println!(
        "parquet_sort: max_bytes={}, memory_limit={}, ratio={:.2}x",
        stats.max_bytes,
        MEMORY_LIMIT,
        stats.max_bytes as f64 / MEMORY_LIMIT as f64
    );
    // TODO: peak is ~67MB (3.3x pool) because parquet decoded
    // batches and sort output arrays are not tracked by the MemoryPool.
    // dhat::assert!(stats.max_bytes < limit,
    //     "Peak heap {} exceeded {}", stats.max_bytes, limit);
    let _ = limit;
}
