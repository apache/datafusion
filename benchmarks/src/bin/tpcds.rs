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
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
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
/// Enum for TPC-DS command-line options
/// Includes options to run a single query or all queries
enum TpcdsOpt {
    /// Run a single TPC-DS query
    Run(RunOpt),

    /// Run all TPC-DS queries
    QueryAll(QueryAllOpt),
}

/// Options for running a single query
#[derive(Debug, StructOpt)]
pub struct RunOpt {
    /// The query number (e.g., 1 for query1.sql)
    #[structopt(short, long)]
    query: usize,

    /// The path to the data directory containing Parquet files
    #[structopt(short, long)]
    data_dir: String,
}

impl RunOpt {
    /// Executes a single query
    pub async fn run(&self) -> Result<()> {
        let query_number = self.query;
        let parquet_dir = &self.data_dir;

        // Load the SQL query
        let sql = load_query(query_number)?;

        println!("▶️ Running query {}", query_number);

        // Create a new DuckDB connection and register tables
        let conn = create_duckdb_connection(parquet_dir)?;
        let ctx = create_tpcds_context(parquet_dir).await?;

        // Compare results between DuckDB and DataFusion
        if let Err(e) = compare_duckdb_datafusion(&sql, &conn, &ctx).await {
            eprintln!("❌ Query {} failed: {}", query_number, e);
            return Err(e);
        }

        println!("✅ Query {} passed.", query_number);
        Ok(())
    }
}

/// Options for running all queries
#[derive(Debug, StructOpt)]
pub struct QueryAllOpt {
    /// The path to the data directory containing Parquet files
    #[structopt(short, long)]
    data_dir: String,
}

impl QueryAllOpt {
    /// Executes all queries sequentially
    pub async fn run(&self) -> Result<()> {
        let parquet_dir = &self.data_dir;

        println!("▶️ Running all TPC-DS queries...");

        // Create a single DuckDB connection and register tables once
        let conn = create_duckdb_connection(parquet_dir)?;
        let ctx = create_tpcds_context(parquet_dir).await?;

        // Iterate through query numbers 1 to 99 and execute each query
        for query_number in 1..=99 {
            match load_query(query_number) {
                Ok(sql) => {
                    println!("▶️ Running query {}", query_number);

                    // Compare results between DuckDB and DataFusion
                    if let Err(e) = compare_duckdb_datafusion(&sql, &conn, &ctx).await {
                        eprintln!("❌ Query {} failed: {}", query_number, e);
                        continue;
                    }

                    println!("✅ Query {} passed.", query_number);
                }
                Err(e) => {
                    eprintln!("❌ Failed to load query {}: {}", query_number, e);
                    continue;
                }
            }
        }

        println!("✅ All TPC-DS queries completed.");
        Ok(())
    }
}

/// Creates a new DuckDB connection and registers all TPC-DS tables
fn create_duckdb_connection(parquet_dir: &str) -> Result<Connection> {
    let conn = Connection::open_in_memory().map_err(|e| {
        DataFusionError::Execution(format!("DuckDB connection error: {}", e))
    })?;

    for table in TPCDS_TABLES {
        let path = format!("{}/{}.parquet", parquet_dir, table);
        let sql = format!(
            "CREATE TABLE {} AS SELECT * FROM read_parquet('{}')",
            table, path
        );
        conn.execute(&sql, []).map_err(|e| {
            DataFusionError::Execution(format!(
                "Error registering table '{}': {}",
                table, e
            ))
        })?;
    }

    println!("✅ All TPC-DS tables registered in DuckDB.");
    Ok(conn)
}

/// Registers all TPC-DS tables in DataFusion's SessionContext
async fn create_tpcds_context(parquet_dir: &str) -> Result<SessionContext> {
    let ctx = SessionContext::new();

    for table_def in tpcds_schemas() {
        let path = format!("{}/{}.parquet", parquet_dir, table_def.name);

        ctx.register_parquet(table_def.name, &path, ParquetReadOptions::default())
            .await?;
    }

    println!("✅ All TPC-DS tables registered in DataFusion.");
    Ok(ctx)
}

/// Compares results of a query between DuckDB and DataFusion
async fn compare_duckdb_datafusion(
    sql: &str,
    conn: &Connection,
    ctx: &SessionContext,
) -> Result<(), DataFusionError> {
    let expected_batches = execute_duckdb_query(sql, conn)?;
    let actual_batches = execute_datafusion_query(sql, ctx).await?;
    let expected_output = pretty_format_batches(&expected_batches)?.to_string();
    let actual_output = pretty_format_batches(&actual_batches)?.to_string();

    if expected_output != actual_output {
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

/// Executes a query in DuckDB and returns the results as RecordBatch
fn execute_duckdb_query(sql: &str, conn: &Connection) -> Result<Vec<RecordBatch>> {
    let mut stmt = conn.prepare(sql).map_err(|e| {
        DataFusionError::Execution(format!("SQL preparation error: {}", e))
    })?;
    let batches = stmt
        .query_arrow([])
        .map_err(|e| DataFusionError::Execution(format!("Query execution error: {}", e)))?
        .collect();

    Ok(batches)
}

/// Executes a query in DataFusion and returns the results as RecordBatch
async fn execute_datafusion_query(
    sql: &str,
    ctx: &SessionContext,
) -> Result<Vec<RecordBatch>> {
    let df = ctx.sql(sql).await?;
    df.collect().await
}

/// Loads the SQL file for a given query number
fn load_query(query_number: usize) -> Result<String> {
    let query_path = format!("../datafusion/core/tests/tpc-ds/{}.sql", query_number);
    fs::read_to_string(&query_path).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to load query {}: {}",
            query_number, e
        ))
    })
}

/// Main function
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // Parse command-line arguments
    let opt = TpcdsOpt::from_args();

    // Execute based on the selected option
    match opt {
        TpcdsOpt::Run(opt) => opt.run().await,
        TpcdsOpt::QueryAll(opt) => opt.run().await,
    }
}
