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

//! Demonstrates how to track the top memory consumers when a query
//! exceeds its memory limit.
//!
//! This example mirrors the behaviour of the `--top-memory-consumers`
//! flag in the DataFusion CLI. It constructs a session configured
//! with a small memory pool that keeps statistics about the largest
//! memory consumers. When the query runs out of memory the error
//! message will include the top consumers.
//!
//! Run it using
//!
//! ```bash
//! cargo run --example top_consumer
//! ```

use arrow::util::pretty::pretty_format_batches;
use datafusion::error::Result;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, TrackConsumersPool,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // Configure a runtime with only 10 MB of memory and track the top 2 consumers

    const MB: usize = 1024 * 1024;
    let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(
        GreedyMemoryPool::new(16 * 1024 * 1024),
        NonZeroUsize::new(2).unwrap(),
    ));

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()?;

    let ctx = SessionContext::new_with_config_rt(SessionConfig::default(), runtime);

    // Manually allocate memory and print how much was reserved
    let mut reservation = MemoryConsumer::new("manual").register(&pool);
    reservation.try_grow(15 * MB)?;
    // A query that sorts a large dataset and will exceed the memory limit
    let df = ctx
        .sql("select v % 1000 as group_key, count(*) as cnt, sum(v) as sum_v from generate_series(1,500000) as t(v) group by v % 1000 order by group_key")
        .await?;

    match df.collect().await {
        Ok(batches) => {
            // Success is unexpected, but print the results if it happens
            println!("{}", pretty_format_batches(&batches)?);
        }
        Err(e) => {
            // The error message lists the top memory consumers
            println!("{e}");
        }
    }

    Ok(())
}
