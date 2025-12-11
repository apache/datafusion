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

//! See `main.rs` for how to run it.
//!
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
//! Specifically, without configuration such as in this example, DataFusion
//! plans and executes everything the same thread pool (Tokio Runtime), including
//! any I/O, such as reading Parquet files from remote object storage
//! (e.g. AWS S3), catalog access, and CPU intensive work. Running this diverse
//! workload can lead to issues described in the [Architecture section] such as
//! throttled network bandwidth (due to congestion control) and increased
//! latencies or timeouts while processing network messages.
//!
//! [Architecture section]: https://docs.rs/datafusion/latest/datafusion/index.html#thread-scheduling-cpu--io-thread-pools-and-tokio-runtimes

use arrow::util::pretty::pretty_format_batches;
use datafusion::common::runtime::JoinSet;
use datafusion::error::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::*;
use futures::stream::StreamExt;
use object_store::client::SpawnedReqwestConnector;
use object_store::http::HttpBuilder;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use url::Url;

/// Normally, you don't need to worry about the details of the tokio
/// [`Runtime`], but for this example it is important to understand how the
/// [`Runtime`]s work.
///
/// Each thread has "current" runtime that is installed in a thread local
/// variable which is used by the `tokio::spawn` function.
///
/// The `#[tokio::main]` macro  creates a [`Runtime`] and installs it as
/// as the "current" runtime in a thread local variable, on which any `async`
/// [`Future`], [`Stream]`s and [`Task]`s are run.
///
/// This example uses the runtime created by [`tokio::main`] to do I/O and spawn
/// CPU intensive tasks on a separate [`Runtime`], mirroring the common pattern
/// when using Rust libraries such as `tonic`. Using a separate `Runtime` for
/// CPU bound tasks will often be simpler in larger applications, even though it
/// makes this example slightly more complex.
pub async fn thread_pools() -> Result<()> {
    // The first two examples read local files. Enabling the URL table feature
    // lets us treat filenames as tables in SQL.
    let ctx = SessionContext::new().enable_url_table();
    let sql = format!(
        "SELECT * FROM '{}/alltypes_plain.parquet'",
        datafusion::test_util::parquet_test_data()
    );

    // Run a query on the current runtime. Calling `await` means the future
    // (in this case the `async` function and all spawned work in DataFusion
    // plans) on the current runtime.
    same_runtime(&ctx, &sql).await?;

    // Run the same query but this time on a different runtime.
    //
    // Since we call `await` here, the `async` function itself runs on the
    // current runtime, but internally `different_runtime_basic` executes the
    // DataFusion plan on a different Runtime.
    different_runtime_basic(ctx, sql).await?;

    // Run the same query on a different runtime, including remote IO.
    //
    // NOTE: This is best practice for production systems
    different_runtime_advanced().await?;

    Ok(())
}

/// Run queries directly on the current tokio `Runtime`
///
/// This is how most examples in DataFusion are written and works well for
/// development, local query processing, and non latency sensitive workloads.
async fn same_runtime(ctx: &SessionContext, sql: &str) -> Result<()> {
    // Calling .sql is an async function as it may also do network
    // I/O, for example to contact a remote catalog or do an object store LIST
    let df = ctx.sql(sql).await?;

    // While many examples call `collect` or `show()`, those methods buffers the
    // results. Internally DataFusion generates output a RecordBatch at a time

    // Calling `execute_stream` return a `SendableRecordBatchStream`. Depending
    // on the plan, this may also do network I/O, for example to begin reading a
    // parquet file from a remote object store.
    let mut stream: SendableRecordBatchStream = df.execute_stream().await?;

    // `next()` drives the plan, incrementally producing new `RecordBatch`es
    // using the current runtime.
    //
    // Perhaps somewhat non obviously, calling `next()` can also result in other
    // tasks being spawned on the current runtime (e.g. for `RepartitionExec` to
    // read data from each of its input partitions in parallel).
    //
    // Executing the plan using this pattern intermixes any IO and CPU intensive
    // work on same Runtime
    while let Some(batch) = stream.next().await {
        println!("{}", pretty_format_batches(&[batch?])?);
    }
    Ok(())
}

/// Run queries on a **different** Runtime dedicated for CPU bound work
///
/// This example is suitable for running DataFusion plans against local data
/// sources (e.g. files) and returning results to an async destination, as might
/// be done to return query results to a remote client.
///
/// Production systems which also read data locally or require very low latency
/// should follow the recommendations on [`different_runtime_advanced`] when
/// processing data from a remote source such as object storage.
async fn different_runtime_basic(ctx: SessionContext, sql: String) -> Result<()> {
    // Since we are already in the context of runtime (installed by
    // #[tokio::main]), we need a new Runtime (threadpool) for CPU bound tasks
    let cpu_runtime = CpuRuntime::try_new()?;

    // Prepare a task that runs the plan on cpu_runtime and sends
    // the results back to the original runtime via a channel.
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let driver_task = async move {
        // Plan the query (which might require CPU work to evaluate statistics)
        let df = ctx.sql(&sql).await?;
        let mut stream: SendableRecordBatchStream = df.execute_stream().await?;

        // Calling `next()` to drive the plan in this task drives the
        // execution from the cpu runtime the other thread pool
        //
        // NOTE any IO run by this plan (for example, reading from an
        // `ObjectStore`) will be done on this new thread pool as well.
        while let Some(batch) = stream.next().await {
            if tx.send(batch).await.is_err() {
                // error means dropped receiver, so nothing will get results anymore
                return Ok(());
            }
        }
        Ok(()) as Result<()>
    };

    // Run the driver task on the cpu runtime. Use a JoinSet to
    // ensure the spawned task is canceled on error/drop
    let mut join_set = JoinSet::new();
    join_set.spawn_on(driver_task, cpu_runtime.handle());

    // Retrieve the results in the original (IO) runtime. This requires only
    // minimal work (pass pointers around).
    while let Some(batch) = rx.recv().await {
        println!("{}", pretty_format_batches(&[batch?])?);
    }

    // wait for completion of the driver task
    drain_join_set(join_set).await;

    Ok(())
}

/// Run CPU intensive work on a different runtime but do IO operations (object
/// store access) on the current runtime.
async fn different_runtime_advanced() -> Result<()> {
    // In this example, we will query a file via https, reading
    // the data directly from the plan

    // The current runtime (created by tokio::main) is used for IO
    //
    // Note this handle should be used for *ALL* remote IO operations in your
    // systems, including remote catalog access, which is not included in this
    // example.
    let cpu_runtime = CpuRuntime::try_new()?;
    let io_handle = Handle::current();

    let ctx = SessionContext::new();

    // By default, the HttpStore use the same runtime that calls `await` for IO
    // operations. This means that if the DataFusion plan is called from the
    // cpu_runtime,  the HttpStore IO operations will *also* run on the CPU
    // runtime, which will error.
    //
    // To avoid this, we use a `SpawnedReqwestConnector` to configure the
    // `ObjectStore` to run the HTTP requests on the IO runtime.
    let base_url = Url::parse("https://github.com").unwrap();
    let http_store = HttpBuilder::new()
        .with_url(base_url.clone())
        // Use the io_runtime to run the HTTP requests. Without this line,
        // you will see an error such as:
        // A Tokio 1.x context was found, but IO is disabled.
        .with_http_connector(SpawnedReqwestConnector::new(io_handle))
        .build()?;

    // Tell DataFusion to process `http://` urls with this wrapped object store
    ctx.register_object_store(&base_url, Arc::new(http_store));

    // As above, plan and execute the query on the cpu runtime.
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let driver_task = async move {
        // Plan / execute the query
        let url = "https://github.com/apache/arrow-testing/raw/master/data/csv/aggregate_test_100.csv";
        let df = ctx
            .sql(&format!("SELECT c1,c2,c3 FROM '{url}' LIMIT 5"))
            .await?;

        let mut stream: SendableRecordBatchStream = df.execute_stream().await?;

        // Note you can do other non trivial CPU work on the results of the
        // stream before sending it back to the original runtime. For example,
        // calling a FlightDataEncoder to convert the results to flight messages
        // to send over the network

        // send results, as above
        while let Some(batch) = stream.next().await {
            if tx.send(batch).await.is_err() {
                return Ok(());
            }
        }
        Ok(()) as Result<()>
    };

    let mut join_set = JoinSet::new();
    join_set.spawn_on(driver_task, cpu_runtime.handle());
    while let Some(batch) = rx.recv().await {
        println!("{}", pretty_format_batches(&[batch?])?);
    }

    Ok(())
}

/// Waits for all tasks in the JoinSet to complete and reports any errors that
/// occurred.
///
/// If we don't do this, any errors that occur in the task (such as IO errors)
/// are not reported.
async fn drain_join_set(mut join_set: JoinSet<Result<()>>) {
    // retrieve any errors from the tasks
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}                             // task completed successfully
            Ok(Err(e)) => eprintln!("Task failed: {e}"), // task failed
            Err(e) => eprintln!("JoinSet error: {e}"),   // JoinSet error
        }
    }
}

/// Creates a Tokio [`Runtime`] for use with CPU bound tasks
///
/// Tokio forbids dropping `Runtime`s in async contexts, so creating a separate
/// `Runtime` correctly is somewhat tricky. This structure manages the creation
/// and shutdown of a separate thread.
///
/// # Notes
/// On drop, the thread will wait for all remaining tasks to complete.
///
/// Depending on your application, more sophisticated shutdown logic may be
/// required, such as ensuring that no new tasks are added to the runtime.
///
/// # Credits
/// This code is derived from code originally written for [InfluxDB 3.0]
///
/// [InfluxDB 3.0]: https://github.com/influxdata/influxdb3_core/tree/6fcbb004232738d55655f32f4ad2385523d10696/executor
struct CpuRuntime {
    /// Handle is the tokio structure for interacting with a Runtime.
    handle: Handle,
    /// Signal to start shutting down
    notify_shutdown: Arc<Notify>,
    /// When thread is active, is Some
    thread_join_handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for CpuRuntime {
    fn drop(&mut self) {
        // Notify the thread to shutdown.
        self.notify_shutdown.notify_one();
        // In a production system you also need to ensure your code stops adding
        // new tasks to the underlying runtime after this point to allow the
        // thread to complete its work and exit cleanly.
        if let Some(thread_join_handle) = self.thread_join_handle.take() {
            // If the thread is still running, we wait for it to finish
            print!("Shutting down CPU runtime thread...");
            if let Err(e) = thread_join_handle.join() {
                eprintln!("Error joining CPU runtime thread: {e:?}",);
            } else {
                println!("CPU runtime thread shutdown successfully.");
            }
        }
    }
}

impl CpuRuntime {
    /// Create a new Tokio Runtime for CPU bound tasks
    pub fn try_new() -> Result<Self> {
        let cpu_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .build()?;
        let handle = cpu_runtime.handle().clone();
        let notify_shutdown = Arc::new(Notify::new());
        let notify_shutdown_captured = Arc::clone(&notify_shutdown);

        // The cpu_runtime runs and is dropped on a separate thread
        let thread_join_handle = std::thread::spawn(move || {
            cpu_runtime.block_on(async move {
                notify_shutdown_captured.notified().await;
            });
            // Note: cpu_runtime is dropped here, which will wait for all tasks
            // to complete
        });

        Ok(Self {
            handle,
            notify_shutdown,
            thread_join_handle: Some(thread_join_handle),
        })
    }

    /// Return a handle suitable for spawning CPU bound tasks
    ///
    /// # Notes
    ///
    /// If a task spawned on this handle attempts to do IO, it will error with a
    /// message such as:
    ///
    /// ```text
    /// A Tokio 1.x context was found, but IO is disabled.
    /// ```
    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}
