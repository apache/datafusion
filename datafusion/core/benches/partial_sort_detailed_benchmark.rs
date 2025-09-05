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

//! Comprehensive benchmarks for PartialSortExec optimization
//! 
//! This benchmark tests various scenarios to understand the performance
//! characteristics of PartialSortExec vs SortExec with different:
//! - Total row counts
//! - Batch sizes
//! - Number of distinct prefix values (cardinality)
//! - Fetch values (LIMIT scenarios)
//! - Parallelism effects

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{col, SortExpr};
use datafusion::prelude::*;
use datafusion_common::Result;
use rand::Rng;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[derive(Debug, Clone)]
struct BenchmarkConfig {
    total_rows: usize,
    batch_size: usize,
    prefix_cardinality: usize,  // Number of distinct values in prefix columns
    fetch_limit: Option<usize>,  // Optional LIMIT clause
    parallelism: usize,
    description: String,
}

impl BenchmarkConfig {
    fn new(
        total_rows: usize,
        batch_size: usize,
        prefix_cardinality: usize,
        fetch_limit: Option<usize>,
        parallelism: usize,
    ) -> Self {
        let description = format!(
            "rows={}_batch={}_card={}_fetch={:?}_par={}",
            total_rows, batch_size, prefix_cardinality, fetch_limit, parallelism
        );
        Self {
            total_rows,
            batch_size,
            prefix_cardinality,
            fetch_limit,
            parallelism,
            description,
        }
    }
}

/// Creates data that is pre-sorted on columns (a, b) but not on c
/// with configurable cardinality for the prefix columns
fn create_presorted_data(config: &BenchmarkConfig) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    let mut rows_created = 0;
    let mut rng = rand::rng();
    
    while rows_created < config.total_rows {
        let batch_rows = std::cmp::min(config.batch_size, config.total_rows - rows_created);
        
        let mut a_vals = Vec::with_capacity(batch_rows);
        let mut b_vals = Vec::with_capacity(batch_rows);
        let mut c_vals = Vec::with_capacity(batch_rows);
        let mut d_vals = Vec::with_capacity(batch_rows);
        
        for i in 0..batch_rows {
            let global_idx = rows_created + i;
            // Create sorted values for (a, b) with controlled cardinality
            let a_value = (global_idx * config.prefix_cardinality / config.total_rows) as i32;
            let b_value = (global_idx % 100) as i32;
            
            a_vals.push(a_value);
            b_vals.push(b_value);
            // c is random to require actual sorting
            c_vals.push(rng.random_range(0..1000));
            // d is for testing string columns
            d_vals.push(format!("val_{}", rng.random_range(0..100)));
        }
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Utf8, false),
        ]));
        
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(a_vals)),
                Arc::new(Int32Array::from(b_vals)),
                Arc::new(Int32Array::from(c_vals)),
                Arc::new(StringArray::from(d_vals)),
            ],
        )?;
        
        batches.push(batch);
        rows_created += batch_rows;
    }
    
    Ok(batches)
}

/// Creates completely random data for baseline comparison
fn create_random_data(config: &BenchmarkConfig) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    let mut rows_created = 0;
    let mut rng = rand::rng();
    
    while rows_created < config.total_rows {
        let batch_rows = std::cmp::min(config.batch_size, config.total_rows - rows_created);
        
        let a_vals: Vec<i32> = (0..batch_rows).map(|_| rng.random_range(0..config.prefix_cardinality as i32)).collect();
        let b_vals: Vec<i32> = (0..batch_rows).map(|_| rng.random_range(0..100)).collect();
        let c_vals: Vec<i32> = (0..batch_rows).map(|_| rng.random_range(0..1000)).collect();
        let d_vals: Vec<String> = (0..batch_rows).map(|_| format!("val_{}", rng.random_range(0..100))).collect();
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Utf8, false),
        ]));
        
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(a_vals)),
                Arc::new(Int32Array::from(b_vals)),
                Arc::new(Int32Array::from(c_vals)),
                Arc::new(StringArray::from(d_vals)),
            ],
        )?;
        
        batches.push(batch);
        rows_created += batch_rows;
    }
    
    Ok(batches)
}

async fn benchmark_partial_sort(config: &BenchmarkConfig) -> Result<f64> {
    let mut ctx_config = SessionConfig::new();
    ctx_config = ctx_config.with_target_partitions(config.parallelism);
    let ctx = SessionContext::new_with_config(ctx_config);
    
    let batches = create_presorted_data(config)?;
    let schema = batches[0].schema();
    
    // Create sort expressions for (a, b) ordering
    let sort_exprs = vec![
        SortExpr::new(col("a"), true, false),
        SortExpr::new(col("b"), true, false),
    ];
    
    // Create a table with declared ordering on (a, b)
    let table = MemTable::try_new(schema, vec![batches])?
        .with_sort_order(vec![sort_exprs]);
    
    ctx.register_table("presorted_table", Arc::new(table))?;
    
    // Build query with optional LIMIT
    let query = match config.fetch_limit {
        Some(limit) => format!("SELECT * FROM presorted_table ORDER BY a, b, c LIMIT {}", limit),
        None => "SELECT * FROM presorted_table ORDER BY a, b, c".to_string(),
    };
    
    let start = std::time::Instant::now();
    let result = ctx.sql(&query).await?.collect().await?;
    let duration = start.elapsed();
    
    let total_rows: usize = result.iter().map(|batch| batch.num_rows()).sum();
    
    // Return microseconds per row
    Ok(duration.as_secs_f64() / total_rows as f64 * 1_000_000.0)
}

async fn benchmark_full_sort(config: &BenchmarkConfig) -> Result<f64> {
    let mut ctx_config = SessionConfig::new();
    ctx_config = ctx_config.with_target_partitions(config.parallelism);
    let ctx = SessionContext::new_with_config(ctx_config);
    
    let batches = create_random_data(config)?;
    let schema = batches[0].schema();
    
    // Create table without any ordering information
    let table = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("random_table", Arc::new(table))?;
    
    // Build query with optional LIMIT
    let query = match config.fetch_limit {
        Some(limit) => format!("SELECT * FROM random_table ORDER BY a, b, c LIMIT {}", limit),
        None => "SELECT * FROM random_table ORDER BY a, b, c".to_string(),
    };
    
    let start = std::time::Instant::now();
    let result = ctx.sql(&query).await?.collect().await?;
    let duration = start.elapsed();
    
    let total_rows: usize = result.iter().map(|batch| batch.num_rows()).sum();
    
    // Return microseconds per row
    Ok(duration.as_secs_f64() / total_rows as f64 * 1_000_000.0)
}

async fn verify_optimization_used(config: &BenchmarkConfig) -> Result<()> {
    let mut ctx_config = SessionConfig::new();
    ctx_config = ctx_config.with_target_partitions(config.parallelism);
    let ctx = SessionContext::new_with_config(ctx_config);
    
    let batches = create_presorted_data(config)?;
    let schema = batches[0].schema();
    
    let sort_exprs = vec![
        SortExpr::new(col("a"), true, false),
        SortExpr::new(col("b"), true, false),
    ];
    
    let table = MemTable::try_new(schema, vec![batches])?
        .with_sort_order(vec![sort_exprs]);
    
    ctx.register_table("test_table", Arc::new(table))?;
    
    let query = match config.fetch_limit {
        Some(limit) => format!("SELECT * FROM test_table ORDER BY a, b, c LIMIT {}", limit),
        None => "SELECT * FROM test_table ORDER BY a, b, c".to_string(),
    };
    
    let df = ctx.sql(&query).await?;
    let plan = df.explain(false, false)?.collect().await?;
    
    println!("\n=== Physical Plan for {} ===", config.description);
    let mut found_partial_sort = false;
    let mut found_sort = false;
    
    for batch in plan {
        if let Some(plan_column) = batch.column(1).as_any().downcast_ref::<StringArray>() {
            for row in 0..batch.num_rows() {
                let plan_line = plan_column.value(row);
                if plan_line.contains("PartialSortExec") {
                    found_partial_sort = true;
                    println!("  ✓ {}", plan_line);
                } else if plan_line.contains("SortExec") && !plan_line.contains("PartialSortExec") {
                    found_sort = true;
                    println!("  ✗ {}", plan_line);
                }
            }
        }
    }
    
    if found_partial_sort {
        println!("  ✓ PartialSortExec optimization is being used");
    } else if found_sort {
        println!("  ✗ Full SortExec is being used (no optimization)");
    }
    
    Ok(())
}

fn detailed_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    // Test configurations covering different scenarios
    let configs = vec![
        // Vary total rows (keeping other params constant)
        BenchmarkConfig::new(1_000, 1000, 10, None, 4),
        BenchmarkConfig::new(10_000, 1000, 100, None, 4),
        BenchmarkConfig::new(100_000, 1000, 1000, None, 4),
        
        // Vary batch sizes
        BenchmarkConfig::new(10_000, 100, 100, None, 4),
        BenchmarkConfig::new(10_000, 500, 100, None, 4),
        BenchmarkConfig::new(10_000, 5000, 100, None, 4),
        
        // Vary prefix cardinality (number of distinct groups)
        BenchmarkConfig::new(10_000, 1000, 10, None, 4),    // Few groups
        BenchmarkConfig::new(10_000, 1000, 100, None, 4),   // Moderate groups
        BenchmarkConfig::new(10_000, 1000, 1000, None, 4),  // Many groups
        BenchmarkConfig::new(10_000, 1000, 5000, None, 4),  // Very many groups
        
        // Test with LIMIT (fetch) - should show bigger improvements
        BenchmarkConfig::new(100_000, 1000, 100, Some(10), 4),
        BenchmarkConfig::new(100_000, 1000, 100, Some(100), 4),
        BenchmarkConfig::new(100_000, 1000, 100, Some(1000), 4),
        
        // Vary parallelism
        BenchmarkConfig::new(50_000, 1000, 100, None, 1),
        BenchmarkConfig::new(50_000, 1000, 100, None, 2),
        BenchmarkConfig::new(50_000, 1000, 100, None, 8),
    ];
    
    // First, verify that optimization is being used
    println!("\nVerifying PartialSortExec optimization usage:");
    for config in &configs[0..3] {
        rt.block_on(verify_optimization_used(config)).unwrap();
    }
    
    let mut group = c.benchmark_group("partial_sort_detailed");
    group.sample_size(10); // Reduce sample size for longer benchmarks
    
    for config in configs {
        // Benchmark PartialSortExec scenario
        group.bench_with_input(
            BenchmarkId::new("partial_sort", &config.description),
            &config,
            |b, cfg| {
                b.iter(|| {
                    rt.block_on(benchmark_partial_sort(black_box(cfg)))
                        .unwrap()
                })
            },
        );
        
        // Benchmark full SortExec scenario for comparison
        group.bench_with_input(
            BenchmarkId::new("full_sort", &config.description),
            &config,
            |b, cfg| {
                b.iter(|| {
                    rt.block_on(benchmark_full_sort(black_box(cfg)))
                        .unwrap()
                })
            },
        );
    }
    
    group.finish();
}

fn print_summary_statistics() {
    println!("\n=== Benchmark Configuration Summary ===");
    println!("This benchmark tests PartialSortExec optimization with:");
    println!("- Total rows: 1K to 100K");
    println!("- Batch sizes: 100 to 5000");
    println!("- Prefix cardinality: 10 to 5000 distinct values");
    println!("- LIMIT clauses: None, 10, 100, 1000");
    println!("- Parallelism: 1, 2, 4, 8 threads");
    println!("\nPartialSortExec should show improvements when:");
    println!("- Data has low prefix cardinality (fewer distinct groups)");
    println!("- LIMIT clauses are used (early termination)");
    println!("- Batch sizes are optimal for the workload");
}

criterion_group!(benches, detailed_benchmarks);
criterion_main!(benches);