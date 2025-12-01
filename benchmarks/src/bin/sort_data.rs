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

//! Sort parquet file using DataFusion's external sort

use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} <input.parquet> <output.parquet> [sort_column] [memory_limit_mb]", args[0]);
        eprintln!("Example: {} hits.parquet hits_sorted.parquet EventTime 2048", args[0]);
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = &args[2];
    let sort_column = args.get(3).map(|s| s.as_str()).unwrap_or("EventTime");
    let memory_limit_mb: usize = args.get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2048);

    println!("DataFusion Sort Utility");
    println!("=======================");
    println!("Input:  {}", input_path);
    println!("Output: {}", output_path);
    println!("Sort column: {}", sort_column);
    println!("Memory limit: {} MB", memory_limit_mb);
    println!();

    // Check if input exists
    if !Path::new(input_path).exists() {
        eprintln!("Error: Input file not found: {}", input_path);
        std::process::exit(1);
    }

    // Check if output already exists
    if Path::new(output_path).exists() {
        println!("Output file already exists: {}", output_path);
        println!("Delete it first if you want to regenerate.");
        return Ok(());
    }

    // Create session config
    let mut config = SessionConfig::new()
        .with_target_partitions(1) // Limit parallelism for local mac run
        .with_batch_size(500);

    // Set sort spill reservation to a smaller value to allow more spilling
    // Default is usually 10MB per partition, we'll reduce it for local mac run
    config = config.set_usize(
        "datafusion.execution.sort_spill_reservation_bytes",
        512 * 1024, // 512kb instead of default 10MB
    );

    // Create memory pool
    let memory_limit_bytes = memory_limit_mb * 1024 * 1024;
    let memory_pool = Arc::new(GreedyMemoryPool::new(memory_limit_bytes));

    // Create runtime environment with memory pool
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(memory_pool)
        .build_arc()?;

    // Create session state with config and runtime environment
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .build();

    let ctx = SessionContext::from(state);

    println!("Step 1: Reading input file...");
    let start = Instant::now();

    // Register the parquet file
    ctx.register_parquet("input_table", input_path, ParquetReadOptions::default())
        .await?;

    println!("  Registered in {:.2}s", start.elapsed().as_secs_f64());

    // Count rows
    println!("Step 2: Counting rows...");
    let start = Instant::now();
    let count_df = ctx.sql("SELECT COUNT(*) FROM input_table").await?;
    let count_result = count_df.collect().await?;
    let row_count: i64 = count_result[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    println!("  Total rows: {} ({:.2}s)", row_count, start.elapsed().as_secs_f64());

    // Sort the data
    println!("Step 3: Sorting by {} (this will take a while)...", sort_column);
    println!("  DataFusion will automatically spill to disk if memory limit is exceeded");
    let start = Instant::now();

    // Quote the column name to preserve case sensitivity
    let sort_query = format!("SELECT * FROM input_table ORDER BY \"{}\"", sort_column);
    let df = ctx.sql(&sort_query).await?;

    // Don't collect here - just show the plan is created
    println!("  Query planned in {:.2}s", start.elapsed().as_secs_f64());

    // Write to parquet (this will execute the sort)
    println!("Step 4: Executing sort and writing output...");
    let start = Instant::now();

    df.write_parquet(
        output_path,
        dataframe::DataFrameWriteOptions::new()
            .with_single_file_output(true),
        None,
    )
        .await?;

    let elapsed = start.elapsed().as_secs_f64();
    println!("  Sorted and written in {:.2}s", elapsed);

    // Verify the output
    println!("Step 5: Verifying output...");
    ctx.register_parquet("output_table", output_path, ParquetReadOptions::default())
        .await?;

    // Quote the column name to preserve case sensitivity
    let verify_query = format!(
        "SELECT MIN(\"{0}\") as min_time, MAX(\"{0}\") as max_time FROM output_table",
        sort_column
    );
    let verify_df = ctx.sql(&verify_query).await?;
    let verify_result = verify_df.collect().await?;

    println!("\n✓ Successfully created sorted file!");
    println!("  Location: {}", output_path);
    println!("  Row count: {}", row_count);
    println!("  Total time: {:.2}s", elapsed);

    if !verify_result.is_empty() {
        println!("  First {}: {:?}", sort_column, verify_result[0].column(0).slice(0, 1));
        println!("  Last {}:  {:?}", sort_column, verify_result[0].column(1).slice(0, 1));
    }

    Ok(())
}