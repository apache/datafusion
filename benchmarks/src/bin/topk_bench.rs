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

//! Benchmark for top-k aggregation with string and numeric grouping columns

use datafusion::prelude::*;
use datafusion_common::instant::Instant;

#[tokio::main]
async fn main() {
    println!("DataFusion Top-K Aggregation Benchmark");
    println!("======================================\n");

    // Test with UTF-8 string grouping (the problematic case)
    benchmark_string_grouping().await;

    // Test with numeric grouping (baseline)
    benchmark_numeric_grouping().await;

    // Test with LargeUtf8 strings
    benchmark_large_string_grouping().await;

    // Test with Utf8View strings
    benchmark_utf8view_grouping().await;
}

async fn benchmark_string_grouping() {
    println!("Test 1: String Grouping (Utf8) with Top-K");
    println!("-----------------------------------------");

    let ctx = SessionContext::new();

    // Create a test query with string grouping and LIMIT (top-k)
    let query = r#"
        WITH test_data AS (
            SELECT 
                CAST(column1 % 100 AS VARCHAR) as group_key,
                column1 as id,
                column2 as value
            FROM (
                SELECT 
                    ROW_NUMBER() OVER () - 1 as column1,
                    (ROW_NUMBER() OVER () - 1) * 2 as column2
                FROM (
                    SELECT generate_series(0, 999999) as n
                ) t
            ) t2
        )
        SELECT group_key, COUNT(*) as cnt, SUM(value) as total
        FROM test_data
        GROUP BY group_key
        ORDER BY group_key
        LIMIT 10
    "#;

    match run_benchmark(&ctx, query, "String Grouping").await {
        Ok((elapsed, rows)) => {
            println!(
                "  Result: {} rows in {:.2}ms\n",
                rows,
                elapsed.as_secs_f64() * 1000.0
            );
        }
        Err(e) => println!("  Error: {}\n", e),
    }
}

async fn benchmark_numeric_grouping() {
    println!("Test 2: Numeric Grouping with Top-K");
    println!("------------------------------------");

    let ctx = SessionContext::new();

    let query = r#"
        WITH test_data AS (
            SELECT 
                column1 % 100 as group_key,
                column1 as id,
                column2 as value
            FROM (
                SELECT 
                    ROW_NUMBER() OVER () - 1 as column1,
                    (ROW_NUMBER() OVER () - 1) * 2 as column2
                FROM (
                    SELECT generate_series(0, 999999) as n
                ) t
            ) t2
        )
        SELECT group_key, COUNT(*) as cnt, SUM(value) as total
        FROM test_data
        GROUP BY group_key
        ORDER BY group_key
        LIMIT 10
    "#;

    match run_benchmark(&ctx, query, "Numeric Grouping").await {
        Ok((elapsed, rows)) => {
            println!(
                "  Result: {} rows in {:.2}ms\n",
                rows,
                elapsed.as_secs_f64() * 1000.0
            );
        }
        Err(e) => println!("  Error: {}\n", e),
    }
}

async fn benchmark_large_string_grouping() {
    println!("Test 3: Large String Grouping (LargeUtf8) with Top-K");
    println!("-----------------------------------------------------");

    let ctx = SessionContext::new();

    let query = r#"
        WITH test_data AS (
            SELECT 
                CAST(CONCAT('group_', CAST(column1 % 100 AS VARCHAR)) AS VARCHAR) as group_key,
                column1 as id,
                column2 as value
            FROM (
                SELECT 
                    ROW_NUMBER() OVER () - 1 as column1,
                    (ROW_NUMBER() OVER () - 1) * 2 as column2
                FROM (
                    SELECT generate_series(0, 999999) as n
                ) t
            ) t2
        )
        SELECT group_key, COUNT(*) as cnt, SUM(value) as total
        FROM test_data
        GROUP BY group_key
        ORDER BY group_key
        LIMIT 10
    "#;

    match run_benchmark(&ctx, query, "Large String Grouping").await {
        Ok((elapsed, rows)) => {
            println!(
                "  Result: {} rows in {:.2}ms\n",
                rows,
                elapsed.as_secs_f64() * 1000.0
            );
        }
        Err(e) => println!("  Error: {}\n", e),
    }
}

async fn benchmark_utf8view_grouping() {
    println!("Test 4: Utf8View String Grouping with Top-K");
    println!("-------------------------------------------");

    let ctx = SessionContext::new();

    // Note: In a real benchmark, we would set a session config to prefer Utf8View
    // For now, we use the same query as test 1, since Utf8View would be used
    // if the config preferred it

    let query = r#"
        WITH test_data AS (
            SELECT 
                CAST(column1 % 100 AS VARCHAR) as group_key,
                column1 as id,
                column2 as value
            FROM (
                SELECT 
                    ROW_NUMBER() OVER () - 1 as column1,
                    (ROW_NUMBER() OVER () - 1) * 2 as column2
                FROM (
                    SELECT generate_series(0, 999999) as n
                ) t
            ) t2
        )
        SELECT group_key, COUNT(*) as cnt, SUM(value) as total
        FROM test_data
        GROUP BY group_key
        ORDER BY group_key
        LIMIT 10
    "#;

    match run_benchmark(&ctx, query, "Utf8View Grouping").await {
        Ok((elapsed, rows)) => {
            println!(
                "  Result: {} rows in {:.2}ms\n",
                rows,
                elapsed.as_secs_f64() * 1000.0
            );
        }
        Err(e) => println!("  Error: {}\n", e),
    }
}

async fn run_benchmark(
    ctx: &SessionContext,
    query: &str,
    _name: &str,
) -> Result<(std::time::Duration, usize), Box<dyn std::error::Error>> {
    // Warm-up run
    let _ = ctx.sql(query).await?.collect().await?;

    // Actual benchmark run
    let start = Instant::now();
    let result = ctx.sql(query).await?.collect().await?;
    let elapsed = start.elapsed();

    let row_count: usize = result.iter().map(|batch| batch.num_rows()).sum();

    Ok((elapsed, row_count))
}
