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

//! This example shows how to use a separate thread pool (tokio [`Runtime`])) to
//! run the CPU intensive parts of DataFusion plans.
//!
//! Running DataFusion plans that perform I/O, such as reading parquet files
//! directly from remote object storage (e.g. AWS S3) without care will result
//! in running CPU intensive jobs on the same thread pool, which can lead to the
//! issues described in  the [Architecture section] such as throttled bandwidth
//! due to congestion control and increased latencies for processing network
//! messages.
use arrow::util::pretty::pretty_format_batches;
use datafusion::error::Result;
use datafusion::execution::dedicated_executor::DedicatedExecutor;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder};
use datafusion::prelude::*;
use futures::stream::StreamExt;
use object_store::http::HttpBuilder;
use object_store::ObjectStore;
use std::sync::Arc;
use url::Url;

/// Normally, you don't need to worry about the details of the tokio runtime,
/// but for this example it is important to understand how the [`Runtime`]s work.
///
/// There is a "current" runtime that is installed in a thread local variable
/// that is used by the `tokio::spawn` function.
///
/// The `#[tokio::main]` macro actually creates a [`Runtime`] and installs it as
/// as the "current" runtime (on which any `async` futures, streams and tasks
/// are run).
#[tokio::main]
async fn main() -> Result<()> {
    // The first two examples only do local file IO. Enable the  URL table so we
    // can select directly from filenames in SQL.
    let sql = format!(
        "SELECT * FROM '{}/alltypes_plain.parquet'",
        datafusion::test_util::parquet_test_data()
    );

    // Run the same query on the same runtime. Note that calling `await` here
    // will effectively run the future (in this case the `async` function) on
    // the current runtime
    same_runtime(&sql).await?;

    // Run the same query on a different runtime.
    // Run the same query on a different runtime including remote IO
    different_runtime_advanced().await?;

    Ok(())
}

/// Run queries directly on the current tokio `Runtime`
///
/// This is now most examples in DataFusion are written and works well for
/// development and local query processing.
async fn same_runtime(sql: &str) -> Result<()> {
    let ctx = SessionContext::new().enable_url_table();

    // Calling .sql is an async function as it may also do network
    // I/O, for example to contact a remote catalog or do an object store LIST
    let df = ctx.sql(sql).await?;

    // While many examples call `collect` or `show()`, those methods buffers the
    // results. internally DataFusion generates output a RecordBatch at a time

    // Calling `execute_stream` on a DataFrame returns a
    // `SendableRecordBatchStream`. Depending on the plan, this may also do
    // network I/O, for example to begin reading a parquet file from a remote
    // object store as well. It is also possible that this function call spawns
    // tasks that begin doing CPU intensive work as well
    let mut stream: SendableRecordBatchStream = df.execute_stream().await?;

    // Calling `next()` drives the plan, producing new `RecordBatch`es using the
    // current runtime (and typically also the current thread).
    //
    // Perhaps somewhat non obvious, calling the `next()` function often will
    // result in other tasks being spawned on the current runtime (e.g. for
    // `RepartitionExec` to read data from each of its input partitions in
    // parallel).
    //
    // Executing the plan like this results in all CPU intensive work
    // running on same (default) Runtime.
    while let Some(batch) = stream.next().await {
        println!("{}", pretty_format_batches(&[batch?]).unwrap());
    }
    Ok(())
}

/// Demonstrates how to run queries on a **different** runtime than the current one
///
async fn different_runtime_advanced() -> Result<()> {
    // In this example, we will configure access to a remote object store
    // over the network during the plan

    let dedicated_executor = DedicatedExecutor::builder().build();

    // setup http object store
    let base_url = Url::parse("https://github.com").unwrap();
    let http_store: Arc<dyn ObjectStore> =
        Arc::new(HttpBuilder::new().with_url(base_url.clone()).build()?);

    // By default, the object store will use the "current runtime" for IO operations
    // if we use a dedicated executor to run the plan, the eventual object store requests will also use the
    // dedicated executor's runtime
    //
    // To avoid this, we can wrap the object store to run on the "IO" runtime
    //
    // (if we don't do this the example fails with an error like
    //
    // ctx.register_object_store(&base_url, http_store);
    // A Tokio 1.x context was found, but timers are disabled. Call `enable_time` on the runtime builder to enable timers.

    //let http_store = dedicated_executor.wrap_object_store(http_store);

    // we must also register the dedicated executor with the runtime
    let runtime_env = RuntimeEnvBuilder::new()
        .with_dedicated_executor(dedicated_executor.clone())
        .build_arc()?;

    // Tell datafusion about processing http:// urls with this wrapped object store
    runtime_env.register_object_store(&base_url, http_store);

    let ctx = SessionContext::from(
        SessionStateBuilder::new()
            .with_runtime_env(runtime_env)
            .with_default_features()
            .build(),
    )
    .enable_url_table();

    // Plan (and execute) the query on the dedicated runtime
    // TODO it would be great to figure out how to run this as part of `ctx.sql`
    let stream = dedicated_executor
        .spawn_cpu(async move {
            // Plan / execute the query
            let url = "https://github.com/apache/arrow-testing/raw/master/data/csv/aggregate_test_100.csv";
            let df = ctx
                .sql(&format!("SELECT c1,c2,c3 FROM '{url}' LIMIT 5"))
                .await?;
            let stream: SendableRecordBatchStream = df.execute_stream().await?;

            Ok(stream) as Result<_>
        }).await??;

    // We have now planned the query on the dedicated runtime, Yay! but we still need to
    // drive the stream (aka call `next()` to get the results).

    // However, as mentioned above, calling  `next()` resolves the Stream (and
    // any work it may do) on a thread in the current (default) runtime.
    //
    // To drive the stream on the dedicated runtime, we need to wrap it using a
    // `DedicatedExecutor::wrap_stream` stream function
    //
    // Note if you don't do this you will likely see a panic about `No IO runtime registered.`
    // because the threads in the current (main) tokio runtime have not had the IO runtime
    // installed
    let mut stream = dedicated_executor.run_sendable_record_batch_stream(stream);

    // Note you can run other streams on the DedicatedExecutor as well using the
    // DedicatedExecutor:YYYXXX function. This is helpful for example, if you
    // need to do non trivial CPU work on the results of the stream (e.g.
    // calling a FlightDataEncoder to convert the results to flight to send it
    // over the network),

    while let Some(batch) = stream.next().await {
        println!("{}", pretty_format_batches(&[batch?]).unwrap());
    }

    Ok(())
}

// TODO add an example of a how to call IO / CPU bound work directly using DedicatedExecutor
// (e.g. to create a listing table directly)
