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

//! # Memory Profiling Example
//! Demonstrates memory profiling capabilities in DataFusion
//!
//! This example shows how to use `ctx.enable_memory_profiling()` to collect
//! detailed memory usage information during query execution.
//!
//! The example runs a multi-stage query and shows how to access memory
//! profiling information. Note that memory profiling is currently
//! experimental and may not capture all memory allocations.

use datafusion::prelude::*;
use datafusion::{
    arrow::{
        array::Float64Array, array::Int64Array, array::StringArray, datatypes::DataType,
        datatypes::Field, datatypes::Schema, record_batch::RecordBatch,
    },
    catalog::MemTable,
    common::Result,
};
use datafusion_common::instant::Instant;
use std::sync::Arc;
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

    println!("Query completed in: {duration:?}");
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

/// Runs the query with memory profiling disabled
async fn run_without_profiling() -> Result<()> {
    println!("=== Running WITHOUT memory profiling ===");

    let ctx = SessionContext::new();
    let start = Instant::now();
    run_memory_intensive_query(&ctx).await?;
    let total_time = start.elapsed();

    println!("Total execution time: {total_time:?}");
    println!(
        "Memory profiling enabled: {}",
        ctx.is_memory_profiling_enabled()
    );
    println!();

    Ok(())
}

/// Runs the query with memory profiling enabled
async fn run_with_profiling() -> Result<()> {
    println!("=== Running WITH memory profiling ===");

    let ctx = SessionContext::new();

    // Enable memory profiling
    let _handle = ctx.enable_memory_profiling();

    let start = Instant::now();
    run_memory_intensive_query(&ctx).await?;
    let total_time = start.elapsed();

    println!("Total execution time: {total_time:?}");
    println!(
        "Memory profiling enabled: {}",
        ctx.is_memory_profiling_enabled()
    );

    // Get memory profiling information
    let memory_report = ctx.get_last_query_memory_report();
    if !memory_report.is_empty() {
        println!("🎯 Memory profiling results collected successfully!");
        println!("Number of operators tracked: {}", memory_report.len());

        // Use enhanced memory profiling for detailed analysis
        let enhanced_report = ctx.get_enhanced_memory_report();
        enhanced_report.print_analysis();
    } else {
        println!("No memory profiling information available");
        println!("This is expected for this simple query because:");
    }

    println!();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("DataFusion Memory Profiling Example");
    println!("====================================\n");

    // Run without profiling
    run_without_profiling().await?;

    // Run with profiling
    run_with_profiling().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::assert_batches_eq;

    #[tokio::test]
    async fn test_create_large_dataset() -> Result<()> {
        let batch = create_large_dataset(100)?;
        assert_eq!(batch.num_rows(), 100);
        assert_eq!(batch.num_columns(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_memory_profiling_toggle() -> Result<()> {
        let ctx = SessionContext::new();
        assert!(!ctx.is_memory_profiling_enabled());

        let _handle = ctx.enable_memory_profiling();
        assert!(ctx.is_memory_profiling_enabled());

        Ok(())
    }
}
