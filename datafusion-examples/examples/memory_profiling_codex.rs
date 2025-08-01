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

use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion::common::Result;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;
use std::time::Instant;

/// Categorizes memory operators for better understanding
fn categorize_operator(op_name: &str) -> &'static str {
    match op_name.to_lowercase().as_str() {
        name if name.contains("scan") || name.contains("reader") => "Data Input",
        name if name.contains("aggregate") || name.contains("group") => "Aggregation",
        name if name.contains("join") || name.contains("hash") => "Join Operation",
        name if name.contains("sort") || name.contains("order") => "Sorting",
        name if name.contains("filter") || name.contains("where") => "Filtering",
        name if name.contains("project") || name.contains("select") => "Projection",
        name if name.contains("union") || name.contains("concat") => "Set Operation",
        name if name.contains("window") || name.contains("rank") => "Window Function",
        name if name.contains("limit") || name.contains("top") => "Limit/TopK",
        name if name.contains("spill") || name.contains("buffer") => "Memory Management",
        _ => "Other",
    }
}

/// Creates a large dataset with multiple columns to simulate memory-intensive operations
fn create_large_dataset(num_rows: usize) -> Result<RecordBatch> {
    let mut ids = Vec::with_capacity(num_rows);
    let mut values = Vec::with_capacity(num_rows);
    let mut categories = Vec::with_capacity(num_rows);
    let mut prices = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        ids.push(i as i64);
        values.push((i % 1000) as f64);
        categories.push(format!("category_{}", i % 100));
        prices.push((i as f64) * 1.5);
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(categories)),
            Arc::new(Float64Array::from(prices)),
        ],
    )?)
}

/// Runs a memory-intensive multi-stage query
async fn run_memory_intensive_query(ctx: &SessionContext) -> Result<()> {
    // Create a large dataset
    let batch = create_large_dataset(100_000)?;
    let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    ctx.register_table("large_table", Arc::new(provider))?;

    // Multi-stage query: aggregation, join, and window functions
    let sql = r#"
        WITH large_data AS (
            SELECT * FROM large_table
            UNION ALL
            SELECT * FROM large_table
            UNION ALL
            SELECT * FROM large_table
        ),
        aggregated AS (
            SELECT
                category,
                SUM(value) as total_value,
                AVG(price) as avg_price,
                COUNT(*) as row_count
            FROM large_data
            GROUP BY category
        ),
        ranked AS (
            SELECT
                category,
                total_value,
                avg_price,
                row_count,
                RANK() OVER (ORDER BY total_value DESC) as value_rank,
                RANK() OVER (ORDER BY avg_price DESC) as price_rank
            FROM aggregated
        ),
        with_rank_diff AS (
            SELECT
                category,
                total_value,
                avg_price,
                row_count,
                value_rank,
                price_rank,
                ABS(value_rank - price_rank) as rank_diff
            FROM ranked
        )
        SELECT
            category,
            total_value,
            avg_price,
            row_count,
            value_rank,
            price_rank,
            rank_diff
        FROM with_rank_diff
        WHERE rank_diff <= 10
        ORDER BY total_value DESC
        LIMIT 100
    "#;

    let start = Instant::now();
    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;
    let duration = start.elapsed();

    println!("Query completed in: {:?}", duration);
    println!(
        "Number of result rows: {}",
        results.iter().map(|r| r.num_rows()).sum::<usize>()
    );

    // Calculate total memory used by results
    let total_bytes: usize = results.iter().map(|r| r.get_array_memory_size()).sum();
    println!(
        "Total result memory: {:.2} MB",
        total_bytes as f64 / 1024.0 / 1024.0
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // create execution context
    let ctx = SessionContext::new();

    // enable memory profiling for the next query
    let _profile = ctx.enable_memory_profiling();

    // run a multi-stage query that joins and aggregates
    run_memory_intensive_query(&ctx).await?;

    // print memory usage collected by the profiler
    println!("\nMemory profile:");
    let memory_report = ctx.get_last_query_memory_report();

    if memory_report.is_empty() {
        println!("  No memory tracking data available");
    } else {
        // Sort operators by memory usage (descending)
        let mut operators: Vec<_> = memory_report.iter().collect();
        operators.sort_by(|a, b| b.1.cmp(a.1));

        // Find peak memory usage
        let peak_memory = operators
            .iter()
            .map(|(_, bytes)| **bytes)
            .max()
            .unwrap_or(0);
        let total_memory: usize = operators.iter().map(|(_, bytes)| **bytes).sum();

        println!(
            "  Peak memory usage: {:.2} MB",
            peak_memory as f64 / 1024.0 / 1024.0
        );
        println!(
            "  Total tracked memory: {:.2} MB",
            total_memory as f64 / 1024.0 / 1024.0
        );
        println!("\n  Memory by operator:");

        for (op, bytes) in operators {
            let percentage = if total_memory > 0 {
                (*bytes as f64 / total_memory as f64) * 100.0
            } else {
                0.0
            };

            // Categorize operators for better understanding
            let category = categorize_operator(op);
            println!(
                "    {}: {:.2} MB ({:.1}%) [{}]",
                op,
                *bytes as f64 / 1024.0 / 1024.0,
                percentage,
                category
            );
        }
    }

    Ok(())
}
