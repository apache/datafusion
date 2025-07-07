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

//! # JoinSetTracer Integration Tests
//!
//! These are smoke tests that verify `JoinSetTracer` can be correctly injected into DataFusion.
//!
//! They run a SQL query that reads Parquet data and performs an aggregation,
//! which causes DataFusion to spawn multiple tasks.
//! The object store is wrapped to assert that every task can be traced back to the root.
//!
//! These tests don't cover all edge cases, but they should fail if changes to
//! DataFusion's task spawning break tracing.

mod asserting_tracer;
mod traceable_object_store;

use asserting_tracer::init_asserting_tracer;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::*;
use datafusion::test_util::parquet_test_data;
use datafusion_common::assert_contains;
use datafusion_common_runtime::SpawnedTask;
use log::info;
use object_store::local::LocalFileSystem;
use std::sync::Arc;
use traceable_object_store::traceable_object_store;
use url::Url;

/// Combined test that first verifies the query panics when no tracer is registered,
/// then initializes the tracer and confirms the query runs successfully.
///
/// Using a single test function prevents global tracer leakage between tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_tracer_injection() {
    // Without initializing the tracer, run the query.
    // Spawn the query in a separate task so we can catch its panic.
    info!("Running query without tracer");
    // The absence of the tracer should cause the task to panic inside the `TraceableObjectStore`.
    let untraced_result = SpawnedTask::spawn(run_query()).join().await;
    if let Err(e) = untraced_result {
        // Check if the error message contains the expected error.
        assert!(e.is_panic(), "Expected a panic, but got: {e:?}");
        assert_contains!(e.to_string(), "Task ID not found in spawn graph");
        info!("Caught expected panic: {e}");
    } else {
        panic!("Expected the task to panic, but it completed successfully");
    };

    // Initialize the asserting tracer and run the query.
    info!("Initializing tracer and re-running query");
    init_asserting_tracer();
    SpawnedTask::spawn(run_query()).join().await.unwrap(); // Should complete without panics or errors.
}

/// Executes a sample task-spawning SQL query using a traceable object store.
async fn run_query() {
    info!("Starting query execution");

    // Create a new session context
    let ctx = SessionContext::new();

    // Get the test data directory
    let test_data = parquet_test_data();

    // Define a Parquet file format with pruning enabled
    let file_format = ParquetFormat::default().with_enable_pruning(true);

    // Set listing options for the parquet file with a specific extension
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension("alltypes_tiny_pages_plain.parquet");

    // Wrap the local file system in a traceable object store to verify task traceability.
    let local_fs = Arc::new(LocalFileSystem::new());
    let traceable_store = traceable_object_store(local_fs);

    // Register the traceable object store with a test URL.
    let url = Url::parse("test://").unwrap();
    ctx.register_object_store(&url, traceable_store.clone());

    // Register a listing table from the test data directory.
    let table_path = format!("test://{test_data}/");
    ctx.register_listing_table("alltypes", &table_path, listing_options, None, None)
        .await
        .expect("Failed to register table");

    // Define and execute an SQL query against the registered table, which should
    // spawn multiple tasks due to the aggregation and parquet file read.
    let sql = "SELECT COUNT(*), string_col FROM alltypes GROUP BY string_col";
    let result_batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();

    info!("Query complete: {} batches returned", result_batches.len());
}
