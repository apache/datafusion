// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! See `main.rs` for how to run it.
//!
//! This example demonstrates the tracing injection feature for the DataFusion runtime.
//! Tasks spawned on new threads behave differently depending on whether a tracer is injected.
//!
//! Without an injected tracer, logs from tasks running on `tokio-runtime-worker`
//! threads do not include the `run_instrumented_query` span. Injecting `SpanTracer`
//! instruments spawned futures and blocking closures with the current span, so
//! their log entries include `run_instrumented_query`.

use std::any::Any;
use std::sync::Arc;

use datafusion::common::runtime::{JoinSetTracer, set_join_set_tracer};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_examples::utils::{datasets::ExampleDataset, write_csv_to_parquet};
use futures::FutureExt;
use futures::future::BoxFuture;
use tracing::{Instrument, Level, Span, info, instrument};

/// Demonstrates the tracing injection feature for the DataFusion runtime
pub async fn tracing() -> Result<()> {
    // Initialize tracing subscriber with thread info.
    tracing_subscriber::fmt()
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_max_level(Level::DEBUG)
        .init();

    // Run query WITHOUT tracer injection.
    info!("***** RUNNING WITHOUT INJECTED TRACER *****");
    run_instrumented_query().await?;
    info!(
        "***** WITHOUT tracer: `tokio-runtime-worker` tasks did NOT inherit the `run_instrumented_query` span *****"
    );

    // Inject custom tracer so tasks run in the current span.
    info!("Injecting custom tracer...");
    set_join_set_tracer(&SpanTracer).expect("Failed to set tracer");

    // Run query WITH tracer injection.
    info!("***** RUNNING WITH INJECTED TRACER *****");
    run_instrumented_query().await?;
    info!(
        "***** WITH tracer: `tokio-runtime-worker` tasks DID inherit the `run_instrumented_query` span *****"
    );

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

    // Convert the CSV input into a temporary Parquet directory for querying
    let dataset = ExampleDataset::Cars;
    let parquet_temp = write_csv_to_parquet(&ctx, &dataset.path()).await?;

    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

    info!("Registering table 'cars' from {}", parquet_temp.path_str()?);
    ctx.register_listing_table(
        "cars",
        parquet_temp.path_str()?,
        listing_options,
        None,
        None,
    )
    .await
    .expect("Failed to register table");

    let sql = "SELECT COUNT(*), car, sum(speed) FROM cars GROUP BY car";
    info!(sql, "Executing SQL query");
    let result = ctx.sql(sql).await?.collect().await?;
    info!("Query complete: {} batches returned", result.len());
    Ok(())
}
