// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License.

//! Demonstrates how to track and report memory usage of a query.
//!
//! Run with `cargo run --example memory_profiling`.

use std::sync::Arc;

use datafusion::execution::memory_tracker::{set_global_memory_tracker, MemoryTracker};
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create normal session context
    let ctx = SessionContext::new();

    // Install a tracker as global so all operators report their allocations
    let tracker = Arc::new(MemoryTracker::new());
    tracker.enable();
    set_global_memory_tracker(Some(tracker.clone()));

    let sql = "SELECT v % 100 AS group_key, COUNT(*) AS cnt, SUM(v) AS sum_v \
               FROM generate_series(1,100000) AS t(v) \
               GROUP BY group_key \
               ORDER BY group_key";

    // Execute the query; collecting results forces execution
    let df = ctx.sql(sql).await?;
    df.collect().await?;

    // Gather metrics and disable tracking
    let mut metrics: Vec<_> = tracker.metrics().into_iter().collect();
    set_global_memory_tracker(None);
    tracker.disable();

    // Print memory usage per operator
    metrics.sort_by(|a, b| a.0.cmp(&b.0));
    println!("Memory usage by operator (bytes):");
    for (op, bytes) in metrics {
        println!("{op}: {bytes}");
    }

    Ok(())
}
