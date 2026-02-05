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

//! See `main.rs` for how to run it.
//!
//! This example demonstrates how to use TrackConsumersPool for memory tracking and debugging.
//!
//! The TrackConsumersPool provides enhanced error messages that show the top memory consumers
//! when memory allocation fails, making it easier to debug memory issues in DataFusion queries.
//!
//! # Examples
//!
//! * [`automatic_usage_example`]: Shows how to use RuntimeEnvBuilder to automatically enable memory tracking

use datafusion::error::Result;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;

/// Demonstrates TrackConsumersPool for memory tracking and debugging with enhanced error messages
pub async fn mem_pool_tracking() -> Result<()> {
    println!("=== DataFusion Memory Pool Tracking Example ===\n");

    // Example 1: Automatic Usage with RuntimeEnvBuilder
    automatic_usage_example().await?;

    Ok(())
}

/// Example 1: Automatic Usage with RuntimeEnvBuilder
///
/// This shows the recommended way to use TrackConsumersPool through RuntimeEnvBuilder,
/// which automatically creates a TrackConsumersPool with sensible defaults.
async fn automatic_usage_example() -> Result<()> {
    println!("Example 1: Automatic Usage with RuntimeEnvBuilder");
    println!("------------------------------------------------");

    // Success case: Create a runtime with reasonable memory limit
    println!("Success case: Normal operation with sufficient memory");
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(5_000_000, 1.0) // 5MB, 100% utilization
        .build_arc()?;

    let config = SessionConfig::new();
    let ctx = SessionContext::new_with_config_rt(config, runtime);

    // Create a simple table for demonstration
    ctx.sql("CREATE TABLE test AS VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await?
        .collect()
        .await?;

    println!("✓ Created table with memory tracking enabled");

    // Run a simple query to show it works
    let results = ctx.sql("SELECT * FROM test").await?.collect().await?;
    println!(
        "✓ Query executed successfully. Found {} rows",
        results.len()
    );

    println!("\n{}", "-".repeat(50));

    // Error case: Create a runtime with low memory limit to trigger errors
    println!("Error case: Triggering memory limit error with detailed error messages");

    // Use a WITH query that generates data and then processes it to trigger memory usage
    match ctx.sql("
        WITH large_dataset AS (
            SELECT
                column1 as id,
                column1 * 2 as doubled,
                repeat('data_', 20) || column1 as text_field,
                column1 * column1 as squared
            FROM generate_series(1, 2000) as t(column1)
        ),
        aggregated AS (
            SELECT
                id,
                doubled,
                text_field,
                squared,
                sum(doubled) OVER (ORDER BY id ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) as running_sum
            FROM large_dataset
        )
        SELECT
            a1.id,
            a1.text_field,
            a2.text_field as text_field2,
            a1.running_sum + a2.running_sum as combined_sum
        FROM aggregated a1
        JOIN aggregated a2 ON a1.id = a2.id - 1
        ORDER BY a1.id
    ").await?.collect().await {
        Ok(results) => panic!("Should not succeed! Yet got {} batches", results.len()),
        Err(e) => {
            println!("✓ Expected memory limit error during data processing:");
            println!("Error: {e}");
            /* Example error message:
                Error: Not enough memory to continue external sort. Consider increasing the memory limit, or decreasing sort_spill_reservation_bytes
                caused by
                    Resources exhausted: Additional allocation failed with top memory consumers (across reservations) as:
                    ExternalSorterMerge[3]#112(can spill: false) consumed 10.0 MB, peak 10.0 MB,
                    ExternalSorterMerge[10]#147(can spill: false) consumed 10.0 MB, peak 10.0 MB,
                    ExternalSorter[1]#93(can spill: true) consumed 69.0 KB, peak 69.0 KB,
                    ExternalSorter[13]#155(can spill: true) consumed 67.6 KB, peak 67.6 KB,
                    ExternalSorter[8]#140(can spill: true) consumed 67.2 KB, peak 67.2 KB.
                Error: Failed to allocate additional 10.0 MB for ExternalSorterMerge[0] with 0.0 B already allocated for this reservation - 7.1 MB remain available for the total pool
             */
        }
    }

    println!("\nNote: The error message above shows which memory consumers");
    println!("were using the most memory when the limit was exceeded.");

    Ok(())
}
