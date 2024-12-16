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

//! tpcds binary only entrypoint

use arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::DataFusionError;
use duckdb::Connection;
use std::fs;
use structopt::StructOpt;
use test_utils::tpcds::tpcds_schemas;

/// Global list of TPC-DS table names
static TPCDS_TABLES: &[&str] = &[
    "store_sales",
    "catalog_sales",
    "web_sales",
    "store_returns",
    "catalog_returns",
    "web_returns",
    "inventory",
    "store",
    "catalog_page",
    "web_page",
    "warehouse",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "time_dim",
];

#[cfg(all(feature = "snmalloc", feature = "mimalloc"))]
compile_error!(
    "feature \"snmalloc\" and feature \"mimalloc\" cannot be enabled at the same time"
);

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Command-line options for the TPC-DS benchmark tool
#[derive(Debug, StructOpt)]
#[structopt(name = "tpcds", about = "TPC-DS Benchmark Tool.")]
enum TpcdsOpt {
    /// Run TPC-DS queries
    Run(RunOpt),
}

/// Options for running TPC-DS queries
#[derive(Debug, StructOpt)]
pub struct RunOpt {
    /// Query number (e.g., 1 for query1.sql)
    #[structopt(short, long)]
    query: usize,

    /// Path to the data directory containing Parquet files
    #[structopt(short, long)]
    data_dir: String,
}

impl RunOpt {
    pub async fn run(&self) -> Result<()> {
        let query_number = self.query;
        let parquet_dir = &self.data_dir;

        // Load the SQL query
        let sql = load_query(query_number)?;

        println!("▶️ Running query {}", query_number);

        // Compare DuckDB and DataFusion results
        if let Err(e) = compare_duckdb_datafusion(&sql, parquet_dir).await {
            eprintln!("❌ Query {} failed: {}", query_number, e);
            return Err(e);
        }

        println!("✅ Query {} passed.", query_number);
        Ok(())
    }
}

/// Unified function to register all TPC-DS tables in DataFusion's SessionContext
async fn create_tpcds_context(parquet_dir: &str) -> Result<SessionContext> {
    let ctx = SessionContext::new();

    for table_def in tpcds_schemas() {
        let path = format!("{}/{}.parquet", parquet_dir, table_def.name);

        ctx.register_parquet(table_def.name, &path, ParquetReadOptions::default())
            .await?;
    }

    Ok(ctx)
}

/// Compare RecordBatch results from DuckDB and DataFusion
async fn compare_duckdb_datafusion(
    sql: &str,
    parquet_dir: &str,
) -> Result<(), DataFusionError> {
    // Step 1: Execute query in DuckDB (used as the expected result)
    let expected_batches = execute_duckdb_query(sql, parquet_dir)?;

    // Step 2: Execute query in DataFusion (actual result)
    let ctx = create_tpcds_context(parquet_dir).await?;
    let actual_batches = execute_datafusion_query(sql, ctx).await?;

    // Step 3: Format the batches for comparison
    let expected_output = pretty_format_batches(&expected_batches)?.to_string();
    let actual_output = pretty_format_batches(&actual_batches)?.to_string();

    if expected_output != actual_output {
        // Print detailed error information if outputs do not match
        eprintln!("❌ Query failed: Results do not match!");
        eprintln!("SQL:\n{}", sql);
        eprintln!("Expected:\n{}", expected_output);
        eprintln!("Actual:\n{}", actual_output);
        return Err(DataFusionError::Execution(
            "Results do not match!".to_string(),
        ));
    }

    println!("✅ Query succeeded: Results match!");
    Ok(())
}

/// Execute a query in DuckDB and return the results as RecordBatch
fn execute_duckdb_query(sql: &str, parquet_dir: &str) -> Result<Vec<RecordBatch>> {
    // Initialize DuckDB connection
    let conn = Connection::open_in_memory().map_err(|e| {
        DataFusionError::Execution(format!("DuckDB connection error: {}", e))
    })?;

    // Register all TPC-DS tables in DuckDB
    for table in TPCDS_TABLES {
        let path = format!("{}/{}.parquet", parquet_dir, table);
        let sql = format!(
            "CREATE TABLE {} AS SELECT * FROM read_parquet('{}')",
            table, path
        );
        println!("sql is {:?}", sql);
        conn.execute(&sql, []).map_err(|e| {
            DataFusionError::Execution(format!(
                "Error registering table '{}': {}",
                table, e
            ))
        })?;
    }

    // Execute the query
    let mut stmt = conn.prepare(sql).map_err(|e| {
        DataFusionError::Execution(format!("SQL preparation error: {}", e))
    })?;
    let batches = stmt
        .query_arrow([])
        .map_err(|e| DataFusionError::Execution(format!("Query execution error: {}", e)))?
        .collect();

    Ok(batches)
}

/// Execute a query in DataFusion and return the results as RecordBatch
async fn execute_datafusion_query(
    sql: &str,
    ctx: SessionContext,
) -> Result<Vec<RecordBatch>> {
    // Execute the query
    let df = ctx.sql(sql).await?;

    // Collect the results into RecordBatch
    df.collect().await
}

/// Load SQL query from a file
fn load_query(query_number: usize) -> Result<String> {
    let query_path = format!("datafusion/core/tests/tpc-ds/{}.sql", query_number);
    fs::read_to_string(&query_path).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to load query {}: {}",
            query_number, e
        ))
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logger
    env_logger::init();

    // Parse command-line arguments
    let opt = TpcdsOpt::from_args();
    match opt {
        TpcdsOpt::Run(opt) => opt.run().await,
    }
}
