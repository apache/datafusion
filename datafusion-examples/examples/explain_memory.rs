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

use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::memory::MemTable;
use datafusion::error::Result;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryPool, TrackConsumersPool,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a data set large enough to exceed the memory pool limit
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from_iter(0..1_000_000))],
    )?;
    let table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;

    // Configure a memory pool limited to 1 KiB and track consumers
    let tracked_pool = Arc::new(TrackConsumersPool::new(
        GreedyMemoryPool::new(16 * 1024 * 1024),
        NonZeroUsize::new(5).unwrap(),
    ));
    let pool: Arc<dyn MemoryPool> = tracked_pool.clone();
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()?;
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    ctx.register_table("t", Arc::new(table))?;

    // Execute a group-by aggregation that will exceed the limit
    let df = ctx.sql("SELECT v, COUNT(*) FROM t GROUP BY v").await?;
    if let Err(e) = df.collect().await {
        println!("Query failed: {e}");
    }

    Ok(())
}
