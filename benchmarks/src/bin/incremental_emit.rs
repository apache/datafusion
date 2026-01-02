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

//! Benchmark comparing incremental emission vs all-at-once emission
//! for hash aggregation.
//!
//! This benchmark measures the time-to-first-row improvement from
//! the incremental drain optimization.
//!
//! Usage:
//!   cargo run --release --bin incremental_emit_bench -- --groups 1000000

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use datafusion_common::instant::Instant;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
struct BenchmarkResult {
    time_to_first_row: Duration,
    total_time: Duration,
    num_output_batches: usize,
    total_output_rows: usize,
}

async fn run_aggregation_benchmark(
    batch: RecordBatch,
    batch_size: usize,
) -> Result<BenchmarkResult> {
    let config = SessionConfig::new().with_batch_size(batch_size);
    let ctx = SessionContext::new_with_config(config);

    let schema = batch.schema();
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("bench_data", Arc::new(table))?;

    let sql = "SELECT group_key, SUM(value) as total, COUNT(*) as cnt \
               FROM bench_data \
               GROUP BY group_key";

    let df = ctx.sql(sql).await?;

    let start = Instant::now();
    let mut stream = df.execute_stream().await?;

    // Measure time to first batch
    let first_batch = stream.next().await;
    let time_to_first_row = start.elapsed();

    let mut num_output_batches = 0;
    let mut total_output_rows = 0;

    if let Some(Ok(batch)) = first_batch {
        num_output_batches += 1;
        total_output_rows += batch.num_rows();
    }

    // Consume remaining batches
    while let Some(result) = stream.next().await {
        if let Ok(batch) = result {
            num_output_batches += 1;
            total_output_rows += batch.num_rows();
        }
    }

    let total_time = start.elapsed();

    Ok(BenchmarkResult {
        time_to_first_row,
        total_time,
        num_output_batches,
        total_output_rows,
    })
}

fn create_test_data(num_groups: usize, rows_per_group: usize) -> Result<RecordBatch> {
    let total_rows = num_groups * rows_per_group;

    // Create group keys: "group_0", "group_1", ..., "group_{num_groups-1}"
    // Each group appears rows_per_group times
    let group_keys: Vec<String> = (0..total_rows)
        .map(|i| format!("group_{}", i % num_groups))
        .collect();

    // Create values: just use the row index
    let values: Vec<i64> = (0..total_rows as i64).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("group_key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(group_keys)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;

    Ok(batch)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let num_groups = args
        .iter()
        .position(|s| s == "--groups")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);

    let rows_per_group = args
        .iter()
        .position(|s| s == "--rows-per-group")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    let iterations = args
        .iter()
        .position(|s| s == "--iterations")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    println!("=== Incremental Emit Benchmark ===");
    println!("Number of groups: {num_groups}");
    println!("Rows per group: {rows_per_group}");
    println!("Total input rows: {}", num_groups * rows_per_group);
    println!("Iterations: {iterations}");
    println!();

    // Batch sizes to test
    // Note: We use num_groups as the "all-at-once" batch size to simulate the EmitTo::All behavior
    // of emitting all groups in a single batch.
    let batch_sizes = vec![
        (8192, "8192 (incremental)".to_string()),
        (32768, "32768 (larger batches)".to_string()),
        (
            num_groups,
            format!("{num_groups} (all-at-once, simulates EmitTo::All behavior)"),
        ),
    ];

    println!("Running benchmarks...");
    println!();

    for (batch_size, label) in batch_sizes {
        println!("--- Batch size: {label} ---");

        let mut first_row_times = Vec::new();
        let mut total_times = Vec::new();

        for i in 0..iterations {
            // Create fresh test data for each run
            let batch = create_test_data(num_groups, rows_per_group)?;
            let result = run_aggregation_benchmark(batch, batch_size).await?;

            println!(
                "  Iteration {}: first_row={:?}, total={:?}, batches={}, rows={}",
                i + 1,
                result.time_to_first_row,
                result.total_time,
                result.num_output_batches,
                result.total_output_rows
            );

            first_row_times.push(result.time_to_first_row);
            total_times.push(result.total_time);
        }

        let avg_first_row: Duration =
            first_row_times.iter().sum::<Duration>() / iterations as u32;
        let avg_total: Duration =
            total_times.iter().sum::<Duration>() / iterations as u32;

        println!("  Average: first_row={avg_first_row:?}, total={avg_total:?}");
        println!();
    }

    println!("=== Summary ===");
    println!("The 'time to first row' metric shows how quickly the first output");
    println!("batch is produced. With incremental emission (smaller batch sizes),");
    println!("this should be significantly faster than all-at-once emission.");

    Ok(())
}

/* Example output:

cargo run --bin incremental_emit -- --groups 1000000 --rows-per-group 5 --iterations 3

=== Incremental Emit Benchmark ===
Number of groups: 1000000
Rows per group: 5
Total input rows: 5000000
Iterations: 3

Running benchmarks...

--- Batch size: 8192 (incremental) ---
  Iteration 1: first_row=514.312458ms, total=750.121625ms, batches=128, rows=1000000
  Iteration 2: first_row=487.098583ms, total=680.311958ms, batches=128, rows=1000000
  Iteration 3: first_row=473.02925ms, total=668.469083ms, batches=128, rows=1000000
  Average: first_row=491.480097ms, total=699.634222ms

--- Batch size: 32768 (larger batches) ---
  Iteration 1: first_row=481.137417ms, total=497.485917ms, batches=32, rows=1000000
  Iteration 2: first_row=478.821ms, total=496.062959ms, batches=32, rows=1000000
  Iteration 3: first_row=524.281709ms, total=539.426584ms, batches=32, rows=1000000
  Average: first_row=494.746708ms, total=510.99182ms

--- Batch size: 1000000 (all-at-once, simulates EmitTo::All behavior) ---
  Iteration 1: first_row=1.22554625s, total=1.303929583s, batches=16, rows=1000000
  Iteration 2: first_row=1.237296333s, total=1.2897605s, batches=16, rows=1000000
  Iteration 3: first_row=1.235812417s, total=1.303563667s, batches=16, rows=1000000
  Average: first_row=1.232885s, total=1.299084583s

=== Summary ===
The 'time to first row' metric shows how quickly the first output
batch is produced. With incremental emission (smaller batch sizes),
this should be significantly faster than all-at-once emission.
*/
