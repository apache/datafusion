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

//! This example shows how to use separate thread pools (tokio [`Runtime`]))s to
//! run the IO and CPU intensive parts of DataFusion plans.
//!
//! # Background
//!
//! DataFusion, by default, plans and executes all operations (both CPU and IO)
//! on the same thread pool. This makes it fast and easy to get started, but
//! can cause issues when running at scale, especially when fetching and operating
//! on data directly from remote sources.
//!
//! Specifically, DataFusion plans that perform I/O, such as reading parquet files
//! directly from remote object storage (e.g. AWS S3) on the same threadpool
//! as CPU intensive work, can lead to the issues described in the
//! [Architecture section] such as throttled network bandwidth (due to congestion
//! control) and increased latencies or timeouts while processing network
//! messages.
//!
//! It is possible, but more complex, as shows in this example, to separate
//! the IO and CPU bound work on separate runtimes to avoid these issues.
use crate::thread_pools_lib::dedicated_executor::DedicatedExecutor;
use arrow::util::pretty::pretty_format_batches;
use datafusion::error::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::*;
use futures::stream::StreamExt;
use object_store::http::HttpBuilder;
use object_store::ObjectStore;
use std::sync::Arc;
use url::Url;

mod thread_pools_lib;

/// Normally, you don't need to worry about the details of the tokio runtime,
/// but for this example it is important to understand how the [`Runtime`]s work.
///
/// There is a "current" runtime that is installed in a thread local variable
/// that is used by the `tokio::spawn` function.
///
/// The `#[tokio::main]` macro  creates a [`Runtime`] and installs it as
/// as the "current" runtime in a thread local variable, on which any `async`
/// [`Future`], [`Stream]`s and [`Task]`s are run.
#[tokio::main]
async fn main() -> Result<()> {
    // The first two examples read local files, so enable the URL table feature
    // so we can treat filenames as tables in SQL.
    let ctx = SessionContext::new().enable_url_table();
    let sql = format!(
        "SELECT * FROM '{}/alltypes_plain.parquet'",
        datafusion::test_util::parquet_test_data()
    );

    // Run a query on the current runtime. Calling `await` means the future
    // (in this case the `async` function and all spawned work in DataFusion
    // plans) on the current runtime.
    same_runtime(&ctx, &sql).await?;

    // Run the same query but this time on a different runtime. Since we call
    // `await` here,  the `async` function itself runs on the current runtime,
    // but internally `different_runtime_basic` uses a  `DedicatedExecutor` to
    // run the execute the DataFusion plan on a different Runtime.
    different_runtime_basic(ctx, sql).await?;

    // Run the same query on a different runtime, including remote IO.
    //
    // This is best practice for production systems
    different_runtime_advanced().await?;

    Ok(())
}

/// Run queries directly on the current tokio `Runtime`
///
/// This is how most examples in DataFusion are written and works well for
/// development and local query processing.
async fn same_runtime(ctx: &SessionContext, sql: &str) -> Result<()> {
    // Calling .sql is an async function as it may also do network
    // I/O, for example to contact a remote catalog or do an object store LIST
    let df = ctx.sql(sql).await?;

    // While many examples call `collect` or `show()`, those methods buffers the
    // results. Internally DataFusion generates output a RecordBatch at a time

    // Calling `execute_stream` return a `SendableRecordBatchStream`. Depending
    // on the plan, this may also do network I/O, for example to begin reading a
    // parquet file from a remote object store as well. It is also possible that
    // this function call spawns tasks as well.
    let mut stream: SendableRecordBatchStream = df.execute_stream().await?;

    // Calling `next()` drives the plan, producing new `RecordBatch`es using the
    // current runtime (and typically also the current thread).
    //
    // Perhaps somewhat non obviously, calling the `next()` function can also
    // result in other tasks being spawned on the current runtime (e.g. for
    // `RepartitionExec` to read data from each of its input partitions in
    // parallel).
    //
    // Executing the plan like this results in all CPU intensive work
    // running on same Runtime, in this case whichever one ran the work
    while let Some(batch) = stream.next().await {
        println!("{}", pretty_format_batches(&[batch?]).unwrap());
    }
    Ok(())
}

/// Run queries on a **different** runtime than the current one
///
/// This is an intermediate example for explanatory purposes. Production systems
/// should follow the recommendations on  [`different_runtime_advanced`] when
/// running DataFusion queries from a network server or when processing data
/// from a remote object store.
async fn different_runtime_basic(ctx: SessionContext, sql: String) -> Result<()> {
    // Since we are already in the context of runtime (installed by
    // #[tokio::main]), First, we need a new runtime, which is managed by
    // a DedicatedExecutor (a library in this example)
    let dedicated_executor = DedicatedExecutor::builder().build();

    // Now, we run the query on the new runtime
    dedicated_executor
        .spawn(async move {
            // this closure runs on the different thread pool
            let df = ctx.sql(&sql).await?;
            let mut stream: SendableRecordBatchStream = df.execute_stream().await?;

            // Calling `next()` to drive the plan in this closure drives the
            // execution on the other thread pool
            //
            // NOTE any IO run by this plan (for example, reading from an
            // `ObjectStore`) will be done on this new thread pool as well.
            while let Some(batch) = stream.next().await {
                println!("{}", pretty_format_batches(&[batch?]).unwrap());
            }
            Ok(()) as Result<()>
        })
        // even though we are `await`ing here on the "current" pool, internally
        // the DedicatedExecutor runs the work on the separate thread pool and
        // the `await` simply notifies it when the work is done
        .await??;

    // When done with a DedicatedExecutor, it should be shut down cleanly to give
    // any outstanding tasks a chance to complete.
    dedicated_executor.join().await;

    Ok(())
}

/// Demonstrates running queries so that
/// 1. IO operations happen on the current thread pool
/// 2. CPU bound tasks happen on a different thread pool
async fn different_runtime_advanced() -> Result<()> {
    // In this example, we will query a file via https, reading
    // the data directly from the plan

    let ctx = SessionContext::new().enable_url_table();

    // setup http object store
    let base_url = Url::parse("https://github.com").unwrap();
    let http_store: Arc<dyn ObjectStore> =
        Arc::new(HttpBuilder::new().with_url(base_url.clone()).build()?);

    let dedicated_executor = DedicatedExecutor::builder().build();

    // By default, the object store will use the runtime that calls `await` for
    // IO operations. As shown above, using a DedicatedExecutor will run the
    // plan (and all its IO on the same runtime).
    //
    // To avoid this, we can wrap the object store to run on the "IO" runtime
    //
    // You can use the DedicatedExecutor::spawn_io method to run other IO
    // operations.
    //
    // Note if we don't do this the example fails with an error like
    //
    // ctx.register_object_store(&base_url, http_store);
    // A Tokio 1.x context was found, but timers are disabled. Call `enable_time` on the runtime builder to enable timers.
    let http_store = dedicated_executor.wrap_object_store_for_io(http_store);

    // Tell DataDusion to process `http://` urls with this wrapped object store
    ctx.register_object_store(&base_url, http_store);

    // Plan and begin to execute the query on the dedicated runtime
    let stream = dedicated_executor
        .spawn(async move {
            // Plan / execute the query
            let url = "https://github.com/apache/arrow-testing/raw/master/data/csv/aggregate_test_100.csv";
            let df = ctx
                .sql(&format!("SELECT c1,c2,c3 FROM '{url}' LIMIT 5"))
                .await?;

            // since we wrapped the object store, and I/O will actually happen
            // on the current runtime.
            let stream: SendableRecordBatchStream = df.execute_stream().await?;

            Ok(stream) as Result<_>
        }).await??;

    // We have now planned the query on the dedicated executor  Yay! However,
    // most applications will still drive the stream (aka call `next()` to get
    // the results) from the current runtime, for example to send results back
    // over arrow-flight.

    // However, as mentioned above, calling  `next()` drives the Stream (and any
    // work it may do) on a thread in the current (default) runtime.
    //
    // To drive the Stream on the dedicated runtime, we need to wrap it using
    // the `DedicatedExecutor::wrap_stream` stream function
    //
    // Note if you don't do this you will likely see a panic about `No IO runtime registered.`
    // because the threads in the current (main) tokio runtime have not had the IO runtime
    // installed
    let mut stream = dedicated_executor.run_cpu_sendable_record_batch_stream(stream);

    // Note you can run other streams on the DedicatedExecutor as well using the
    // same function. This is helpful for example, if you need to do non trivial
    // CPU work on the results of the stream (e.g. calling a FlightDataEncoder
    // to convert the results to flight to send it over the network),
    while let Some(batch) = stream.next().await {
        println!("{}", pretty_format_batches(&[batch?]).unwrap());
    }

    Ok(())
}
