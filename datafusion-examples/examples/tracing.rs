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

//! This example demonstrates the tracing injection feature for the DataFusion runtime.
//! Tasks spawned on new threads behave differently depending on whether a tracer is injected.
//! The log output clearly distinguishes the two cases.
//!
//! # Expected Log Output
//!
//! When **no tracer** is injected, logs from tasks running on `tokio-runtime-worker` threads
//! will _not_ include the `run_instrumented_query` span:
//!
//! ```text
//! 10:29:40.714  INFO                 main ThreadId(01) tracing: ***** RUNNING WITHOUT INJECTED TRACER *****
//! 10:29:40.714  INFO                 main ThreadId(01) run_instrumented_query: tracing: Starting query execution
//! 10:29:40.728  INFO                 main ThreadId(01) run_instrumented_query: tracing: Executing SQL query sql="SELECT COUNT(*), string_col FROM alltypes GROUP BY string_col"
//! 10:29:40.743 DEBUG                 main ThreadId(01) run_instrumented_query: datafusion_optimizer::optimizer: Optimizer took 6 ms
//! 10:29:40.759 DEBUG tokio-runtime-worker ThreadId(03) datafusion_physical_plan::aggregates::row_hash: Creating GroupedHashAggregateStream
//! 10:29:40.758 DEBUG tokio-runtime-worker ThreadId(04) datafusion_physical_plan::aggregates::row_hash: Creating GroupedHashAggregateStream
//! 10:29:40.771  INFO                 main ThreadId(01) run_instrumented_query: tracing: Query complete: 6 batches returned
//! 10:29:40.772  INFO                 main ThreadId(01) tracing: ***** WITHOUT tracer: Non-main tasks did NOT inherit the `run_instrumented_query` span *****
//! ```
//!
//! When a tracer **is** injected, tasks spawned on non‑main threads _do_ inherit the span:
//!
//! ```text
//! 10:29:40.772  INFO                 main ThreadId(01) tracing: Injecting custom tracer...
//! 10:29:40.772  INFO                 main ThreadId(01) tracing: ***** RUNNING WITH INJECTED TRACER *****
//! 10:29:40.772  INFO                 main ThreadId(01) run_instrumented_query: tracing: Starting query execution
//! 10:29:40.775  INFO                 main ThreadId(01) run_instrumented_query: tracing: Executing SQL query sql="SELECT COUNT(*), string_col FROM alltypes GROUP BY string_col"
//! 10:29:40.784 DEBUG                 main ThreadId(01) run_instrumented_query: datafusion_optimizer::optimizer: Optimizer took 7 ms
//! 10:29:40.801 DEBUG tokio-runtime-worker ThreadId(03) run_instrumented_query: datafusion_physical_plan::aggregates::row_hash: Creating GroupedHashAggregateStream
//! 10:29:40.801 DEBUG tokio-runtime-worker ThreadId(04) run_instrumented_query: datafusion_physical_plan::aggregates::row_hash: Creating GroupedHashAggregateStream
//! 10:29:40.809  INFO                 main ThreadId(01) run_instrumented_query: tracing: Query complete: 6 batches returned
//! 10:29:40.809  INFO                 main ThreadId(01) tracing: ***** WITH tracer: Non-main tasks DID inherit the `run_instrumented_query` span *****
//! ```

use datafusion::common::runtime::{set_join_set_tracer, JoinSetTracer};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::test_util::parquet_test_data;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::any::Any;
use std::sync::Arc;
use tracing::{info, instrument, Instrument, Level, Span};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing subscriber with thread info.
    tracing_subscriber::fmt()
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_max_level(Level::DEBUG)
        .init();

    // Run query WITHOUT tracer injection.
    info!("***** RUNNING WITHOUT INJECTED TRACER *****");
    run_instrumented_query().await?;
    info!("***** WITHOUT tracer: `tokio-runtime-worker` tasks did NOT inherit the `run_instrumented_query` span *****");

    // Inject custom tracer so tasks run in the current span.
    info!("Injecting custom tracer...");
    set_join_set_tracer(&SpanTracer).expect("Failed to set tracer");

    // Run query WITH tracer injection.
    info!("***** RUNNING WITH INJECTED TRACER *****");
    run_instrumented_query().await?;
    info!("***** WITH tracer: `tokio-runtime-worker` tasks DID inherit the `run_instrumented_query` span *****");

    Ok(())
}

/// A simple tracer that ensures any spawned task or blocking closure
/// inherits the current span via `in_current_span`.
struct SpanTracer;

/// Implement the `JoinSetTracer` trait so we can inject instrumentation
/// for both async futures and blocking closures.
impl JoinSetTracer for SpanTracer {
    /// Instruments a boxed future to run in the current span. The future's
    /// return type is erased to `Box<dyn Any + Send>`, which we simply
    /// run inside the `Span::current()` context.
    fn trace_future(
        &self,
        fut: BoxFuture<'static, Box<dyn Any + Send>>,
    ) -> BoxFuture<'static, Box<dyn Any + Send>> {
        fut.in_current_span().boxed()
    }

    /// Instruments a boxed blocking closure by running it inside the
    /// `Span::current()` context.
    fn trace_block(
        &self,
        f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> {
        let span = Span::current();
        Box::new(move || span.in_scope(f))
    }
}

#[instrument(level = "info")]
async fn run_instrumented_query() -> Result<()> {
    info!("Starting query execution");

    let ctx = SessionContext::new();
    let test_data = parquet_test_data();
    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension("alltypes_tiny_pages_plain.parquet");

    let table_path = format!("file://{test_data}/");
    info!("Registering table 'alltypes' from {}", table_path);
    ctx.register_listing_table("alltypes", &table_path, listing_options, None, None)
        .await
        .expect("Failed to register table");

    let sql = "SELECT COUNT(*), string_col FROM alltypes GROUP BY string_col";
    info!(sql, "Executing SQL query");
    let result = ctx.sql(sql).await?.collect().await?;
    info!("Query complete: {} batches returned", result.len());
    Ok(())
}
