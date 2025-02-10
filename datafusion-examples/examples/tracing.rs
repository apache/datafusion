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

//! This example demonstrates the trace feature in DataFusion’s runtime.
//! When the `trace` feature is enabled, spawned tasks in DataFusion (such as those
//! created during repartitioning or when reading Parquet files) are instrumented
//! with the current tracing span, allowing to propagate any existing tracing context.
//!
//! In this example we create a session configured to use multiple partitions,
//! register a Parquet table (based on the `alltypes_tiny_pages_plain.parquet` file),
//! and run a query that should trigger parallel execution on multiple threads.
//! We wrap the entire query execution within a custom span and log messages.
//! By inspecting the tracing output, we should see that the tasks spawned
//! internally inherit the span context.

use arrow::util::pretty::pretty_format_batches;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::test_util::parquet_test_data;
use std::sync::Arc;
use tracing::{info, Level, instrument};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize a tracing subscriber that prints to stdout.
    tracing_subscriber::fmt()
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_max_level(Level::DEBUG)
        .init();

    log::info!("Starting example, this log is not captured by tracing");

    // execute the query within a tracing span
    let result = run_instrumented_query().await;

    info!(
        "Finished example. Check the logs above for tracing span details showing \
that tasks were spawned within the 'run_instrumented_query' span on different threads."
    );

    result
}

#[instrument(level = "info")]
async fn run_instrumented_query() -> Result<()> {
    info!("Starting query execution within the custom tracing span");

    // The default session will set the number of partitions to `std::thread::available_parallelism()`.
    let ctx = SessionContext::new();

    // Get the path to the test parquet data.
    let test_data = parquet_test_data();
    // Build listing options that pick up only the "alltypes_tiny_pages_plain.parquet" file.
    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension("alltypes_tiny_pages_plain.parquet");

    info!("Registering Parquet table 'alltypes' from {test_data} in {listing_options:?}");

    // Register a listing table using an absolute URL.
    let table_path = format!("file://{test_data}/");
    ctx.register_listing_table(
        "alltypes",
        &table_path,
        listing_options.clone(),
        None,
        None,
    )
        .await
        .expect("register_listing_table failed");

    info!("Registered Parquet table 'alltypes' from {table_path}");

    // Run a query that will trigger parallel execution on multiple threads.
    let sql = "SELECT COUNT(*), bool_col, date_string_col, string_col
                    FROM (
                      SELECT bool_col, date_string_col, string_col FROM alltypes
                      UNION ALL
                      SELECT bool_col, date_string_col, string_col FROM alltypes
                    ) AS t
                    GROUP BY bool_col, date_string_col, string_col
                    ORDER BY 1,2,3,4 DESC
                    LIMIT 5;";
    info!(%sql, "Executing SQL query");
    let df = ctx.sql(sql).await?;

    let results: Vec<RecordBatch> = df.collect().await?;
    info!("Query execution complete");

    // Print out the results and tracing output.
    datafusion::common::assert_batches_eq!(
        [
            "+----------+----------+-----------------+------------+",
            "| count(*) | bool_col | date_string_col | string_col |",
            "+----------+----------+-----------------+------------+",
            "| 2        | false    | 01/01/09        | 9          |",
            "| 2        | false    | 01/01/09        | 7          |",
            "| 2        | false    | 01/01/09        | 5          |",
            "| 2        | false    | 01/01/09        | 3          |",
            "| 2        | false    | 01/01/09        | 1          |",
            "+----------+----------+-----------------+------------+",
        ],
        &results
    );

    info!("Query results:\n{}", pretty_format_batches(&results)?);

    Ok(())
}
