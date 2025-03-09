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

//! [DedicatedExecutor] for running CPU-bound tasks on a separate tokio runtime.

use crate::SendableRecordBatchStream;
use async_trait::async_trait;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::error::GenericError;
use datafusion_common::DataFusionError;
use futures::stream::BoxStream;
use futures::{
    future::{BoxFuture, Shared},
    Future, FutureExt, Stream, StreamExt, TryFutureExt,
};
use log::{info, warn};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, UploadPart,
};
use std::cell::RefCell;
use std::pin::Pin;
use std::sync::RwLock;
use std::task::{Context, Poll};
use std::{fmt::Display, sync::Arc, time::Duration};
use tokio::runtime::Builder;
use tokio::task::JoinHandle;
use tokio::{
    runtime::Handle,
    sync::{oneshot::error::RecvError, Notify},
    task::JoinSet,
};
use tokio_stream::wrappers::ReceiverStream;

/// Create a [`DedicatedExecutorBuilder`] from a tokio [`Builder`]
impl From<Builder> for DedicatedExecutorBuilder {
    fn from(value: Builder) -> Self {
        Self::new_from_builder(value)
    }
}

/// Manages a separate tokio [`Runtime`] (thread pool) for executing CPU bound
/// tasks such as DataFusion `ExecutionPlans`.
///
/// See [`DedicatedExecutorBuilder`] for creating a new instance.
///
/// A `DedicatedExecutor` can helps avoid issues when runnnig IO and CPU bound tasks on the
/// same thread pool by running futures (and any `tasks` that are
/// `tokio::task::spawned` by them) on a separate tokio [`Executor`].
///
/// `DedicatedExecutor`s can be `clone`ed and all clones share the same thread pool.
///
/// Since the primary use for a `DedicatedExecutor` is offloading CPU bound
/// work, IO work can not be performed on tasks launched in the Executor.
///
/// To perform IO, see:
/// - [`Self::spawn_io`]
/// - [`Self::wrap_object_store`]
///
/// When [`DedicatedExecutorBuilder::build`] is called, a reference to the
/// "current" tokio runtime will be stored and used, via [`register_io_runtime`] by all
/// threads spawned by the executor. Any I/O done by threads in this
/// [`DedicatedExecutor`] should use [`spawn_io`], which will run them on the
/// I/O runtime.
///
/// # TODO examples
///
/// # Background
///
/// Tokio has the notion of the "current" runtime, which runs the current future
/// and any tasks spawned by it. Typically, this is the runtime created by
/// `tokio::main` and is used for the main application logic and I/O handling
///
/// For CPU bound work, such as DataFusion plan execution, it is important to
/// run on a separate thread pool to avoid blocking the I/O handling for extended
/// periods of time in order to avoid long poll latencies (which decreases the
/// throughput of small requests under concurrent load).
///
/// # IO Scheduling
///
/// I/O, such as network calls, should not be performed on the runtime managed
/// by [`DedicatedExecutor`]. As tokio is a cooperative scheduler, long-running
/// CPU tasks will not be preempted and can therefore starve servicing of other
/// tasks. This manifests in long poll-latencies, where a task is ready to run
/// but isn't being scheduled to run. For CPU-bound work this isn't a problem as
/// there is no external party waiting on a response, however, for I/O tasks,
/// long poll latencies can prevent timely servicing of IO, which can have a
/// significant detrimental effect.
///
/// # Details
///
/// The worker thread priority is set to low so that such tasks do
/// not starve other more important tasks (such as answering health checks)
///
/// Follows the example from stack overflow and spawns a new
/// thread to install a Tokio runtime "context"
/// <https://stackoverflow.com/questions/62536566>
///
/// # Trouble Shooting:
///
/// ## "No IO runtime registered. Call `register_io_runtime`/`register_current_runtime_for_io` in current thread!
///
/// This means that IO was attempted on a tokio runtime that was not registered
/// for IO. One solution is to run the task using [DedicatedExecutor::spawn].
///
/// ## "Cannot drop a runtime in a context where blocking is not allowed"`
///
/// If you try to use this structure from an async context you see something like
/// thread 'test_builder_plan' panicked at 'Cannot
/// drop a runtime in a context where blocking is not allowed it means  This
/// happens when a runtime is dropped from within an asynchronous
/// context.', .../tokio-1.4.0/src/runtime/blocking/shutdown.rs:51:21
///
/// # Notes
/// This code is derived from code originally written for [InfluxDB 3.0]
///
/// [InfluxDB 3.0]: https://github.com/influxdata/influxdb3_core/tree/6fcbb004232738d55655f32f4ad2385523d10696/executor
#[derive(Clone, Debug)]
pub struct DedicatedExecutor {
    /// State for managing Tokio Runtime Handle for CPU tasks
    state: Arc<RwLock<State>>,
}

impl DedicatedExecutor {
    /// Create a new builder to crate a [`DedicatedExecutor`]
    pub fn builder() -> DedicatedExecutorBuilder {
        DedicatedExecutorBuilder::new()
    }

    /// Runs the specified [`Future`] (and any tasks it spawns) on the thread
    /// pool managed by this `DedicatedExecutor`.
    ///
    /// See The struct documentation for more details
    ///
    /// The specified task is added to the tokio executor immediately and
    /// compete for the thread pool's resources.
    ///
    /// # Behavior on `Drop`
    ///
    /// UNLIKE [`tokio::task::spawn`], the returned future is **cancelled** when
    /// it is dropped. Thus, you need ensure the returned future lives until it
    /// completes (call `await`) or you wish to cancel it.
    pub fn spawn<T>(&self, task: T) -> impl Future<Output = Result<T::Output, JobError>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let handle = {
            let state = self.state.read().expect("lock not poisoned");
            state.handle.clone()
        };

        let Some(handle) = handle else {
            return futures::future::err(JobError::WorkerGone).boxed();
        };

        // use JoinSet implement "cancel on drop"
        let mut join_set = JoinSet::new();
        join_set.spawn_on(task, &handle);
        async move {
            join_set
                .join_next()
                .await
                .expect("just spawned task")
                .map_err(|e| match e.try_into_panic() {
                    Ok(e) => {
                        let s = if let Some(s) = e.downcast_ref::<String>() {
                            s.clone()
                        } else if let Some(s) = e.downcast_ref::<&str>() {
                            s.to_string()
                        } else {
                            "unknown internal error".to_string()
                        };

                        JobError::Panic { msg: s }
                    }
                    Err(_) => JobError::WorkerGone,
                })
        }
        .boxed()
    }

    /// signals shutdown of this executor and any Clones
    pub fn shutdown(&self) {
        // hang up the channel which will cause the dedicated thread
        // to quit
        let mut state = self.state.write().expect("lock not poisoned");
        state.handle = None;
        state.start_shutdown.notify_one();
    }

    /// Stops all subsequent task executions, and waits for the worker
    /// thread to complete. Note this will shutdown all clones of this
    /// `DedicatedExecutor` as well.
    ///
    /// Only the first all to `join` will actually wait for the
    /// executing thread to complete. All other calls to join will
    /// complete immediately.
    ///
    /// # Panic / Drop
    /// [`DedicatedExecutor`] implements shutdown on [`Drop`]. You should just use this behavior and NOT call
    /// [`join`](Self::join) manually during [`Drop`] or panics because this might lead to another panic, see
    /// <https://github.com/rust-lang/futures-rs/issues/2575>.
    pub async fn join(&self) {
        self.shutdown();

        // get handle mutex is held
        let handle = {
            let state = self.state.read().expect("lock not poisoned");
            state.completed_shutdown.clone()
        };

        // wait for completion while not holding the mutex to avoid
        // deadlocks
        handle.await.expect("Thread died?")
    }

    /// Returns an [`ObjectStore`] instance that will always perform I/O work on the
    /// IO_RUNTIME.
    ///
    /// Note that this object store will only work correctly if run on this
    /// dedicated executor. If you try and use it on another executor, it will
    /// panic with "no IO runtime registered" type error.
    pub fn wrap_object_store_for_io(
        &self,
        object_store: Arc<dyn ObjectStore>,
    ) -> Arc<IoObjectStore> {
        Arc::new(IoObjectStore::new(self.clone(), object_store))
    }

    /// Runs the [`SendableRecordBatchStream`] on the CPU thread pool
    ///
    /// This is a convenience method around [`Self::run_cpu_stream`]
    pub fn run_cpu_sendable_record_batch_stream(
        &self,
        stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        let schema = stream.schema();
        let stream = self.run_cpu_stream(stream, |job_error| {
            let job_error: GenericError = Box::new(job_error);
            DataFusionError::from(job_error)
                .context("Running RecordBatchStream on DedicatedExecutor")
        });

        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    /// Runs a stream on the CPU thread pool
    ///
    /// # Note
    /// Ths stream must produce Results so that any errors on the dedicated
    /// executor (like a panic or shutdown) can be communicated back.
    ///
    /// # Arguments:
    /// - stream: the stream to run on this dedicated executor
    /// - converter: a function that converts a [`JobError`] to the error type of the stream
    pub fn run_cpu_stream<X, E, S, C>(
        &self,
        stream: S,
        converter: C,
    ) -> impl Stream<Item = Result<X, E>> + Send + 'static
    where
        X: Send + 'static,
        E: Send + 'static,
        S: Stream<Item = Result<X, E>> + Send + 'static,
        C: Fn(JobError) -> E + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        // make a copy to send any job error results back
        let error_tx = tx.clone();

        // This task will run on the CPU runtime
        let task = self.spawn(async move {
            // drive the stream forward on the CPU runtime, sending results
            // back to the original (presumably IO) runtime
            let mut stream = Box::pin(stream);
            while let Some(result) = stream.next().await {
                // try to send to the sender, if error means the
                // receiver has been closed and we terminate early
                if tx.send(result).await.is_err() {
                    return;
                }
            }
        });

        // fire up a task on the current runtime which transfers results back
        // from the CPU runtime to the calling runtime
        let mut set = JoinSet::new();
        set.spawn(async move {
            if let Err(e) = task.await {
                // error running task, try and report it back. An error sending
                // means the receiver was dropped so there is nowhere to
                // report errors. Thus ignored via ok()
                error_tx.send(Err(converter(e))).await.ok();
            }
        });

        StreamAndTask {
            inner: ReceiverStream::new(rx),
            set,
        }
    }

    /// Runs a stream on the IO thread pool
    ///
    /// Ths stream must produce Results so that any errors on the dedicated
    /// executor (like a panic or shutdown) can be communicated back.
    ///
    /// Note this has a slightly different API compared to
    /// [`Self::run_cpu_stream`] because the DedicatedExecutor  doesn't monitor
    /// the CPU thread pool (that came elsewhere)
    ///
    /// # Arguments:
    /// - stream: the stream to run on this dedicated executor
    pub fn run_io_stream<X, E, S>(
        &self,
        stream: S,
    ) -> impl Stream<Item = Result<X, E>> + Send + 'static
    where
        X: Send + 'static,
        E: Send + 'static,
        S: Stream<Item = Result<X, E>> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let mut set = JoinSet::new();
        set.spawn(Self::spawn_io_static(async move {
            // drive the stream forward on the IO runtime, sending results
            // back to the original runtime
            let mut stream = Box::pin(stream);
            while let Some(result) = stream.next().await {
                // try to send to the sender, if error means the
                // receiver has been closed and we terminate early
                if tx.send(result).await.is_err() {
                    return;
                }
            }
        }));

        StreamAndTask {
            inner: ReceiverStream::new(rx),
            set,
        }
    }

    /// Registers `handle` as the IO runtime for this thread
    ///
    /// Users should not need to call this function as it is handled by
    /// [`DedicatedExecutorBuilder`]
    ///
    /// # Notes
    ///
    /// This sets a thread-local variable
    ///
    /// See [`spawn_io`](Self::spawn_io) for more details
    pub fn register_io_runtime(handle: Option<Handle>) {
        IO_RUNTIME.set(handle)
    }

    /// Runs `fut` on IO runtime of this DedicatedExecutor
    pub async fn spawn_io<Fut>(&self, fut: Fut) -> Fut::Output
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send,
    {
        Self::spawn_io_static(fut).await
    }

    /// Runs `fut` on the runtime most recently registered by *any* DedicatedExecutor.
    ///
    /// When possible, it is preferred to use [`Self::spawn_io`]
    ///
    /// This functon is provided, similarly to tokio's [`Handle::current()`] to
    /// avoid having to thread a `DedicatedExecutor` throughout your program.
    ///
    /// # Panic
    /// Needs a IO runtime [registered](register_io_runtime).
    pub async fn spawn_io_static<Fut>(fut: Fut) -> Fut::Output
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send,
    {
        let h = IO_RUNTIME.with_borrow(|h| h.clone()).expect(
            "No IO runtime registered. If you hit this panic, it likely \
            means a DataFusion plan or other CPU bound work is running on the \
            a tokio threadpool used for IO. Try spawning the work using \
            `DedicatedExecutor::spawn` or for tests `DedicatedExecutor::register_current_runtime_for_io`",
        );
        DropGuard(h.spawn(fut)).await
    }
}

/// A wrapper around a receiver stream and task that ensures the inner
/// task is cancelled on drop
struct StreamAndTask<T> {
    inner: ReceiverStream<T>,
    /// Task which produces no output. On drop the outstanding task is cancelled
    #[expect(dead_code)]
    set: JoinSet<()>,
}

impl<T> Stream for StreamAndTask<T> {
    type Item = T;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

thread_local! {
    /// Tokio runtime `Handle` for doing network (I/O) operations, see [`spawn_io`]
    pub static IO_RUNTIME: RefCell<Option<Handle>> = const { RefCell::new(None) };
}

struct DropGuard<T>(JoinHandle<T>);
impl<T> Drop for DropGuard<T> {
    fn drop(&mut self) {
        self.0.abort()
    }
}

impl<T> Future for DropGuard<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(match std::task::ready!(self.0.poll_unpin(cx)) {
            Ok(v) => v,
            Err(e) if e.is_cancelled() => panic!("IO runtime was shut down"),
            Err(e) => std::panic::resume_unwind(e.into_panic()),
        })
    }
}

/// Runs futures (and any `tasks` that are `tokio::task::spawned` by
/// them) on a separate tokio Executor.
///
/// The state is only used by the "outer" API, not by the newly created runtime. The new runtime waits for
/// [`start_shutdown`](Self::start_shutdown) and signals the completion via
/// [`completed_shutdown`](Self::completed_shutdown) (for which is owns the sender side).
#[derive(Debug)]
struct State {
    /// Runtime handle for CPU tasks
    ///
    /// This is `None` when the executor is shutting down.
    handle: Option<Handle>,

    /// If notified, the executor tokio runtime will begin to shutdown.
    ///
    /// We could implement this by checking `handle.is_none()` in regular intervals but requires regular wake-ups and
    /// locking of the state. Just using a proper async signal is nicer.
    start_shutdown: Arc<Notify>,

    /// Receiver side indicating that shutdown is complete.
    completed_shutdown: Shared<BoxFuture<'static, Result<(), Arc<RecvError>>>>,

    /// The inner thread that can be used to join during drop.
    thread: Option<std::thread::JoinHandle<()>>,
}

/// IMPORTANT: Implement `Drop` for [`State`], NOT for [`DedicatedExecutor`],
/// because the executor can be cloned and clones share their inner state.
impl Drop for State {
    fn drop(&mut self) {
        if self.handle.is_some() {
            warn!("DedicatedExecutor dropped without calling shutdown()");
            self.handle = None;
            self.start_shutdown.notify_one();
        }

        // do NOT poll the shared future if we are panicking due to https://github.com/rust-lang/futures-rs/issues/2575
        if !std::thread::panicking()
            && self.completed_shutdown.clone().now_or_never().is_none()
        {
            warn!("DedicatedExecutor dropped without waiting for worker termination",);
        }

        // join thread but don't care about the results
        self.thread.take().expect("not dropped yet").join().ok();
    }
}

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60 * 5);

/// Potential error returned when polling [`DedicatedExecutor::spawn`].
#[derive(Debug)]
pub enum JobError {
    WorkerGone,
    Panic { msg: String },
}

impl Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobError::WorkerGone => {
                write!(f, "Worker thread gone, executor was likely shut down")
            }
            JobError::Panic { msg } => write!(f, "Panic: {}", msg),
        }
    }
}

impl std::error::Error for JobError {}

/// Builder for [`DedicatedExecutor`]
pub struct DedicatedExecutorBuilder {
    /// Name given to all execution threads. Defaults to "DedicatedExecutor"
    name: String,
    /// Builder for tokio runtime. Defaults to multi-threaded builder
    runtime_builder: Builder,
}

impl From<JobError> for DataFusionError {
    fn from(value: JobError) -> Self {
        DataFusionError::External(Box::new(value))
            .context("JobError from DedicatedExecutor")
    }
}

impl DedicatedExecutorBuilder {
    /// Create a new `DedicatedExecutorBuilder` with default values
    ///
    /// Note that by default this `DedicatedExecutor` will not be able to
    /// perform network I/O.
    pub fn new() -> Self {
        Self {
            name: String::from("DedicatedExecutor"),
            runtime_builder: Builder::new_multi_thread(),
        }
    }

    /// Create a new `DedicatedExecutorBuilder` from a pre-existing tokio
    /// runtime [`Builder`].
    ///
    /// This method permits customizing the tokio [`Executor`] used for the
    /// [`DedicatedExecutor`]
    pub fn new_from_builder(runtime_builder: Builder) -> Self {
        Self {
            name: String::from("DedicatedExecutor"),
            runtime_builder,
        }
    }

    /// Set the name of the dedicated executor (appear in the names of each thread).
    ///
    /// Defaults to "DedicatedExecutor"
    #[cfg(test)]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the number of worker threads. Defaults to the tokio default (the
    /// number of virtual CPUs)
    #[allow(dead_code)]
    pub fn with_worker_threads(mut self, num_threads: usize) -> Self {
        self.runtime_builder.worker_threads(num_threads);
        self
    }

    /// Creates a new `DedicatedExecutor` with a dedicated tokio
    /// executor that is separate from the thread pool created via
    /// `[tokio::main]` or similar.
    ///
    /// Note: If [`DedicatedExecutorBuilder::build`] is called from an existing
    /// tokio runtime, it will assume that the existing runtime should be used
    /// for I/O.
    ///
    /// See the documentation on [`DedicatedExecutor`] for more details.
    pub fn build(self) -> DedicatedExecutor {
        let Self {
            name,
            runtime_builder,
        } = self;

        let notify_shutdown = Arc::new(Notify::new());
        let notify_shutdown_captured = Arc::clone(&notify_shutdown);

        let (tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel();
        let (tx_handle, rx_handle) = std::sync::mpsc::channel();

        let io_handle = Handle::try_current().ok();
        let thread = std::thread::Builder::new()
            .name(format!("{name} driver"))
            .spawn(move || {
                // also register the IO runtime for the current thread, since it might be used as well (esp. for the
                // current thread RT)
                DedicatedExecutor::register_io_runtime(io_handle.clone());

                info!("Creating DedicatedExecutor",);

                let mut runtime_builder = runtime_builder;
                let runtime = runtime_builder
                    .on_thread_start(move || {
                        DedicatedExecutor::register_io_runtime(io_handle.clone())
                    })
                    .build()
                    .expect("Creating tokio runtime");

                runtime.block_on(async move {
                    // Enable the "notified" receiver BEFORE sending the runtime handle back to the constructor thread
                    // (i.e .the one that runs `new`) to avoid the potential (but unlikely) race that the shutdown is
                    // started right after the constructor finishes and the new runtime calls
                    // `notify_shutdown_captured.notified().await`.
                    //
                    // Tokio provides an API for that by calling `enable` on the `notified` future (this requires
                    // pinning though).
                    let shutdown = notify_shutdown_captured.notified();
                    let mut shutdown = std::pin::pin!(shutdown);
                    shutdown.as_mut().enable();

                    if tx_handle.send(Handle::current()).is_err() {
                        return;
                    }
                    shutdown.await;
                });

                runtime.shutdown_timeout(SHUTDOWN_TIMEOUT);

                // send shutdown "done" signal
                tx_shutdown.send(()).ok();
            })
            .expect("executor setup");

        let handle = rx_handle.recv().expect("driver started");

        let state = State {
            handle: Some(handle),
            start_shutdown: notify_shutdown,
            completed_shutdown: rx_shutdown.map_err(Arc::new).boxed().shared(),
            thread: Some(thread),
        };

        DedicatedExecutor {
            state: Arc::new(RwLock::new(state)),
        }
    }
}

/// Wraps an inner [`ObjectStore`] so that all underlying methods are run on
/// the Tokio Runtime dedicated to doing IO
///
/// # See Also
///
/// [`DedicatedExecutor::spawn_io`] for more details
#[derive(Debug)]
pub struct IoObjectStore {
    dedicated_executor: DedicatedExecutor,
    inner: Arc<dyn ObjectStore>,
}

impl IoObjectStore {
    pub fn new(executor: DedicatedExecutor, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            dedicated_executor: executor,
            inner: object_store,
        }
    }

    /// Wrap the result stream if necessary
    fn wrap_if_necessary(&self, payload: GetResultPayload) -> GetResultPayload {
        match payload {
            GetResultPayload::File(_, _) => payload,
            GetResultPayload::Stream(stream) => {
                let new_stream = self.dedicated_executor.run_io_stream(stream).boxed();
                GetResultPayload::Stream(new_stream)
            }
        }
    }
}

impl Display for IoObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "IoObjectStore")
    }
}

#[async_trait]
impl ObjectStore for IoObjectStore {
    /// Return GetResult for the location and options.
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let location = location.clone();
        let store = Arc::clone(&self.inner);
        let mut results = self
            .dedicated_executor
            .spawn_io(async move { store.get_opts(&location, options).await })
            .await?;

        // As the results can be a stream too, we must wrap that too
        results.payload = self.wrap_if_necessary(results.payload);
        Ok(results)
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from = from.clone();
        let to = to.clone();
        let store = Arc::clone(&self.inner);
        self.dedicated_executor
            .spawn_io(async move { store.copy(&from, &to).await })
            .await
    }

    async fn copy_if_not_exists(
        &self,
        from: &Path,
        to: &Path,
    ) -> object_store::Result<()> {
        let from = from.clone();
        let to = to.clone();
        let store = Arc::clone(&self.inner);
        self.dedicated_executor
            .spawn_io(async move { store.copy(&from, &to).await })
            .await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let location = location.clone();
        let store = Arc::clone(&self.inner);
        self.dedicated_executor
            .spawn_io(async move { store.delete(&location).await })
            .await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        // run the inner list on the dedicated executor
        //
        // This requires some fiddling as we can't pass the result of list
        // across the runtime, so start another task that drives list on the IO
        // thread and sends results back to the executor thread.
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let prefix_captured = prefix.cloned();
        let inner_captured = Arc::clone(&self.inner);
        let executor_captured = self.dedicated_executor.clone();
        let mut set = JoinSet::new();
        set.spawn(async move {
            executor_captured
                .spawn_io(async move {
                    // run and buffer on the IO stream
                    let mut list_stream = inner_captured.list(prefix_captured.as_ref());
                    while let Some(result) = list_stream.next().await {
                        // try to send to the sender, if error means the
                        // receiver has been closed and we terminate early
                        if tx.send(result).await.is_err() {
                            return;
                        }
                    }
                })
                .await;
        });

        StreamAndTask {
            inner: ReceiverStream::new(rx),
            set,
        }
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned();
        let store = Arc::clone(&self.inner);
        self.dedicated_executor
            .spawn_io(async move { store.list_with_delimiter(prefix.as_ref()).await })
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let location = location.clone();
        let store = Arc::clone(&self.inner);
        let result = self
            .dedicated_executor
            .spawn_io(async move { store.put_multipart_opts(&location, opts).await })
            .await?;

        // the resulting object has async functions too which we must wrap
        Ok(Box::new(IoMultipartUpload::new(
            self.dedicated_executor.clone(),
            result,
        )))
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let location = location.clone();
        let store = Arc::clone(&self.inner);
        self.dedicated_executor
            .spawn_io(async move { store.put_opts(&location, payload, opts).await })
            .await
    }
}

/// A wrapper around a `MultipartUpload` that runs the async functions on the
/// IO thread of the DedicatedExecutor
#[derive(Debug)]
struct IoMultipartUpload {
    dedicated_executor: DedicatedExecutor,
    /// Inner upload (needs to be an option so we can send in closure)
    inner: Option<Box<dyn MultipartUpload>>,
}
impl IoMultipartUpload {
    fn new(
        dedicated_executor: DedicatedExecutor,
        inner: Box<dyn MultipartUpload>,
    ) -> Self {
        Self {
            dedicated_executor,
            inner: Some(inner),
        }
    }
}

#[async_trait]
impl MultipartUpload for IoMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner
            .as_mut()
            .expect("paths that take put inner back")
            .put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        // because we are running the task on a different runtime, we
        // can't send references to &self. Thus take out of self.inner
        // to run on io thread
        let mut inner = self.inner.take().expect("paths that take put inner back");

        let (inner, result) = self
            .dedicated_executor
            .spawn_io(async move {
                let result = inner.as_mut().complete().await;
                (inner, result)
            })
            .await;
        self.inner = Some(inner);
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let mut inner = self.inner.take().expect("paths that take put inner back");

        let (inner, result) = self
            .dedicated_executor
            .spawn_io(async move {
                let result = inner.as_mut().abort().await;
                (inner, result)
            })
            .await;
        self.inner = Some(inner);
        result
    }
}

#[cfg(test)]
#[allow(unused_qualifications)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use std::fmt::Formatter;
    use std::{
        panic::panic_any,
        sync::{Arc, Barrier},
        time::Duration,
    };
    use tokio::{net::TcpListener, sync::Barrier as AsyncBarrier};

    /// Wait for the barrier and then return `result`
    async fn do_work(result: usize, barrier: Arc<Barrier>) -> usize {
        barrier.wait();
        result
    }

    /// Wait for the barrier and then return `result`
    async fn do_work_async(result: usize, barrier: Arc<AsyncBarrier>) -> usize {
        barrier.wait().await;
        result
    }

    /// Returns a DedicatedExecutor with a single thread and IO enabled
    fn exec() -> DedicatedExecutor {
        exec_with_threads(1)
    }

    fn exec2() -> DedicatedExecutor {
        exec_with_threads(2)
    }

    fn exec_with_threads(threads: usize) -> DedicatedExecutor {
        let mut runtime_builder = Builder::new_multi_thread();
        runtime_builder.worker_threads(threads);
        runtime_builder.enable_all();

        DedicatedExecutorBuilder::from(runtime_builder)
            .with_name("Test DedicatedExecutor")
            .build()
    }

    async fn test_io_runtime_multi_thread_impl(dedicated: DedicatedExecutor) {
        let io_runtime_id = std::thread::current().id();
        dedicated
            .spawn(async move {
                let dedicated_id = std::thread::current().id();
                let spawned = DedicatedExecutor::spawn_io_static(async move {
                    std::thread::current().id()
                })
                .await;

                assert_ne!(dedicated_id, spawned);
                assert_eq!(io_runtime_id, spawned);
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn basic() {
        let barrier = Arc::new(Barrier::new(2));

        let exec = exec();
        let dedicated_task = exec.spawn(do_work(42, Arc::clone(&barrier)));

        // Note the dedicated task will never complete if it runs on
        // the main tokio thread (as this test is not using the
        // 'multithreaded' version of the executor and the call to
        // barrier.wait actually blocks the tokio thread)
        barrier.wait();

        // should be able to get the result
        assert_eq!(dedicated_task.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    async fn basic_clone() {
        let barrier = Arc::new(Barrier::new(2));
        let exec = exec();
        // Run task on clone should work fine
        let dedicated_task = exec.clone().spawn(do_work(42, Arc::clone(&barrier)));
        barrier.wait();
        assert_eq!(dedicated_task.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    async fn drop_empty_exec() {
        exec();
    }

    #[tokio::test]
    async fn drop_clone() {
        let barrier = Arc::new(Barrier::new(2));
        let exec = exec();

        drop(exec.clone());

        let task = exec.spawn(do_work(42, Arc::clone(&barrier)));
        barrier.wait();
        assert_eq!(task.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    #[should_panic(expected = "foo")]
    async fn just_panic() {
        struct S(DedicatedExecutor);

        impl Drop for S {
            fn drop(&mut self) {
                self.0.join().now_or_never();
            }
        }

        let exec = exec();
        let _s = S(exec);

        // this must not lead to a double-panic and SIGILL
        panic!("foo")
    }

    #[tokio::test]
    async fn multi_task() {
        let barrier = Arc::new(Barrier::new(3));

        // make an executor with two threads
        let exec = exec2();
        let dedicated_task1 = exec.spawn(do_work(11, Arc::clone(&barrier)));
        let dedicated_task2 = exec.spawn(do_work(42, Arc::clone(&barrier)));

        // block main thread until completion of other two tasks
        barrier.wait();

        // should be able to get the result
        assert_eq!(dedicated_task1.await.unwrap(), 11);
        assert_eq!(dedicated_task2.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    async fn tokio_spawn() {
        let exec = exec2();

        // spawn a task that spawns to other tasks and ensure they run on the dedicated
        // executor
        let dedicated_task = exec.spawn(async move {
            // spawn separate tasks
            let t1 = tokio::task::spawn(async { 25usize });
            t1.await.unwrap()
        });

        // Validate the inner task ran to completion (aka it did not panic)
        assert_eq!(dedicated_task.await.unwrap(), 25);

        exec.join().await;
    }

    #[tokio::test]
    async fn panic_on_executor_str() {
        let exec = exec();
        let dedicated_task = exec.spawn(async move {
            if true {
                panic!("At the disco, on the dedicated task scheduler");
            } else {
                42
            }
        });

        // should not be able to get the result
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Panic: At the disco, on the dedicated task scheduler",
        );

        exec.join().await;
    }

    #[tokio::test]
    async fn panic_on_executor_string() {
        let exec = exec();
        let dedicated_task = exec.spawn(async move {
            if true {
                panic!("{} {}", 1, 2);
            } else {
                42
            }
        });

        // should not be able to get the result
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(err.to_string(), "Panic: 1 2",);

        exec.join().await;
    }

    #[tokio::test]
    async fn panic_on_executor_other() {
        let exec = exec();
        let dedicated_task = exec.spawn(async move {
            if true {
                panic_any(1)
            } else {
                42
            }
        });

        // should not be able to get the result
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(err.to_string(), "Panic: unknown internal error",);

        exec.join().await;
    }

    #[tokio::test]
    async fn executor_shutdown_while_task_running() {
        let barrier_1 = Arc::new(Barrier::new(2));
        let captured_1 = Arc::clone(&barrier_1);
        let barrier_2 = Arc::new(Barrier::new(2));
        let captured_2 = Arc::clone(&barrier_2);

        let exec = exec();
        let dedicated_task = exec.spawn(async move {
            captured_1.wait();
            do_work(42, captured_2).await
        });
        barrier_1.wait();

        exec.shutdown();
        // block main thread until completion of the outstanding task
        barrier_2.wait();

        // task should complete successfully
        assert_eq!(dedicated_task.await.unwrap(), 42);

        exec.join().await;
    }

    #[tokio::test]
    async fn executor_submit_task_after_shutdown() {
        let exec = exec();

        // Simulate trying to submit tasks once executor has shutdown
        exec.shutdown();
        let dedicated_task = exec.spawn(async { 11 });

        // task should complete, but return an error
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Worker thread gone, executor was likely shut down"
        );

        exec.join().await;
    }

    #[tokio::test]
    async fn executor_submit_task_after_clone_shutdown() {
        let exec = exec();

        // shutdown the clone (but not the exec)
        exec.clone().join().await;

        // Simulate trying to submit tasks once executor has shutdown
        let dedicated_task = exec.spawn(async { 11 });

        // task should complete, but return an error
        let err = dedicated_task.await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Worker thread gone, executor was likely shut down"
        );

        exec.join().await;
    }

    #[tokio::test]
    async fn executor_join() {
        let exec = exec();
        // test it doesn't hang
        exec.join().await;
    }

    #[tokio::test]
    async fn executor_join2() {
        let exec = exec();
        // test it doesn't hang
        exec.join().await;
        exec.join().await;
    }

    #[tokio::test]
    #[allow(clippy::redundant_clone)]
    async fn executor_clone_join() {
        let exec = exec();
        // test it doesn't hang
        exec.clone().join().await;
        exec.clone().join().await;
        exec.join().await;
    }

    #[tokio::test]
    async fn drop_receiver() {
        // create empty executor
        let exec = exec();

        // create first blocked task
        let barrier1_pre = Arc::new(AsyncBarrier::new(2));
        let barrier1_pre_captured = Arc::clone(&barrier1_pre);
        let barrier1_post = Arc::new(AsyncBarrier::new(2));
        let barrier1_post_captured = Arc::clone(&barrier1_post);
        let dedicated_task1 = exec.spawn(async move {
            barrier1_pre_captured.wait().await;
            do_work_async(11, barrier1_post_captured).await
        });
        barrier1_pre.wait().await;

        // create second blocked task
        let barrier2_pre = Arc::new(AsyncBarrier::new(2));
        let barrier2_pre_captured = Arc::clone(&barrier2_pre);
        let barrier2_post = Arc::new(AsyncBarrier::new(2));
        let barrier2_post_captured = Arc::clone(&barrier2_post);
        let dedicated_task2 = exec.spawn(async move {
            barrier2_pre_captured.wait().await;
            do_work_async(22, barrier2_post_captured).await
        });
        barrier2_pre.wait().await;

        // cancel task
        drop(dedicated_task1);

        // cancelation might take a short while
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if Arc::strong_count(&barrier1_post) == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await
            }
        })
        .await
        .unwrap();

        // unblock other task
        barrier2_post.wait().await;
        assert_eq!(dedicated_task2.await.unwrap(), 22);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if Arc::strong_count(&barrier2_post) == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await
            }
        })
        .await
        .unwrap();

        exec.join().await;
    }

    #[tokio::test]
    async fn test_io_runtime_multi_thread() {
        let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
        runtime_builder.worker_threads(1);

        let dedicated = DedicatedExecutorBuilder::from(runtime_builder)
            .with_name("Test DedicatedExecutor")
            .build();
        test_io_runtime_multi_thread_impl(dedicated).await;
    }

    #[tokio::test]
    async fn test_io_runtime_current_thread() {
        let runtime_builder = tokio::runtime::Builder::new_current_thread();

        let dedicated = DedicatedExecutorBuilder::new_from_builder(runtime_builder)
            .with_name("Test DedicatedExecutor")
            .build();
        test_io_runtime_multi_thread_impl(dedicated).await;
    }

    #[tokio::test]
    async fn test_that_default_executor_prevents_io() {
        let exec = DedicatedExecutorBuilder::new().build();

        let io_disabled = exec
            .spawn(async move {
                // the only way (I've found) to test if IO is enabled is to use it and observe if tokio panics
                TcpListener::bind("127.0.0.1:0")
                    .catch_unwind()
                    .await
                    .is_err()
            })
            .await
            .unwrap();

        assert!(io_disabled)
    }

    #[tokio::test]
    async fn test_happy_path() {
        let rt_io = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let io_thread_id = rt_io
            .spawn(async move { std::thread::current().id() })
            .await
            .unwrap();
        let parent_thread_id = std::thread::current().id();
        assert_ne!(io_thread_id, parent_thread_id);

        DedicatedExecutor::register_io_runtime(Some(rt_io.handle().clone()));

        let measured_thread_id =
            DedicatedExecutor::spawn_io_static(
                async move { std::thread::current().id() },
            )
            .await;
        assert_eq!(measured_thread_id, io_thread_id);

        rt_io.shutdown_background();
    }

    #[tokio::test]
    #[should_panic(expected = "IO runtime registered")]
    async fn test_panic_if_no_runtime_registered() {
        DedicatedExecutor::spawn_io_static(futures::future::ready(())).await;
    }

    #[tokio::test]
    #[should_panic(expected = "IO runtime was shut down")]
    async fn test_io_runtime_down() {
        let rt_io = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        DedicatedExecutor::register_io_runtime(Some(rt_io.handle().clone()));

        tokio::task::spawn_blocking(move || {
            rt_io.shutdown_timeout(Duration::from_secs(1));
        })
        .await
        .unwrap();

        DedicatedExecutor::spawn_io_static(futures::future::ready(())).await;
    }

    // -----------------------------------
    // ----- Tests for IoObjectStore ------
    // -----------------------------------

    /* ObjectStore::put_multipart() */

    #[tokio::test]
    #[should_panic(expected = "A Tokio 1.x context was found, but IO is disabled.")]
    async fn test_mock_store_put_asserts() {
        // MockObject store should error when used directly by DedicatedExecutor
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        do_put(exec, store).await
    }

    #[tokio::test]
    async fn test_io_store_put_works() {
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        let store = exec.wrap_object_store_for_io(store);
        do_put(exec, store).await
    }

    /// Runs put on MockStore with the DedicatedExecutor
    async fn do_put(exec: DedicatedExecutor, store: Arc<dyn ObjectStore>) {
        exec.spawn(async move {
            // Call put to send data to the stream, expect no error
            let payload = PutPayload::from("bar");
            store
                .put(&Path::from("another/object"), payload)
                .await
                .unwrap();
        })
        .await
        .unwrap();
    }

    /* ObjectStore::put_multipart() */

    #[tokio::test]
    #[should_panic(expected = "A Tokio 1.x context was found, but IO is disabled.")]
    async fn test_mock_store_put_multipart_asserts() {
        // MockObject store should error when used directly by DedicatedExecutor
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        do_put_multipart(exec, store).await
    }

    #[tokio::test]
    async fn test_io_store_put_multipart_works() {
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        let store = exec.wrap_object_store_for_io(store);
        do_put_multipart(exec, store).await
    }

    /// Runs put_multipart on MockStore with the DedicatedExecutor
    async fn do_put_multipart(exec: DedicatedExecutor, store: Arc<dyn ObjectStore>) {
        exec.spawn(async move {
            // Call put_multipart to send data to the stream, expect no error
            let mut res = store
                .put_multipart(&Path::from("another/object"))
                .await
                .unwrap();
            let payload = PutPayload::from("foo");
            res.put_part(payload).await.unwrap();
            res.complete().await.unwrap();

            // Also run abort put_multipart to send data to the stream, expect no error
            let mut res = store
                .put_multipart(&Path::from("another/object"))
                .await
                .unwrap();

            res.abort().await.unwrap();
        })
        .await
        .unwrap();
    }

    /* ObjectStore::get() */

    #[tokio::test]
    #[should_panic(expected = "A Tokio 1.x context was found, but IO is disabled.")]
    async fn test_mock_store_get_asserts() {
        // MockObject store should error when used directly by DedicatedExecutor
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        do_get(exec, store).await
    }

    #[tokio::test]
    async fn test_io_store_get_works() {
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        let store = exec.wrap_object_store_for_io(store);
        do_get(exec, store).await
    }

    /// Runs get on MockStore with the DedicatedExecutor
    async fn do_get(exec: DedicatedExecutor, store: Arc<dyn ObjectStore>) {
        exec.spawn(async move {
            let result = store.get(&Path::from("the/object")).await.unwrap();
            // fetch the result as a stream
            let data: Vec<_> = result
                .into_stream()
                .map(|res| res.expect("no error in stream"))
                .collect()
                .await;
            assert_eq!(data, vec![Bytes::from_static(b"the value")])
        })
        .await
        .unwrap();
    }

    /* ObjectStore::delete() */

    #[tokio::test]
    #[should_panic(expected = "A Tokio 1.x context was found, but IO is disabled.")]
    async fn test_mock_store_delete_asserts() {
        // MockObject store should error when used directly by DedicatedExecutor
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        do_delete(exec, store).await
    }

    #[tokio::test]
    async fn test_io_store_delete_works() {
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        let store = exec.wrap_object_store_for_io(store);
        do_delete(exec, store).await
    }

    /// Runs delete on MockStore with the DedicatedExecutor
    async fn do_delete(exec: DedicatedExecutor, store: Arc<dyn ObjectStore>) {
        exec.spawn(async move {
            store.delete(&Path::from("the/object")).await.unwrap();
        })
        .await
        .unwrap();
    }

    /* ObjectStore::list() */

    #[tokio::test]
    #[should_panic(expected = "A Tokio 1.x context was found, but IO is disabled.")]
    async fn test_mock_store_list_asserts() {
        // MockObject store should error when used directly by DedicatedExecutor
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        do_list(exec, store).await
    }

    #[tokio::test]
    async fn test_io_store_list_works() {
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        let store = exec.wrap_object_store_for_io(store);
        do_list(exec, store).await
    }

    /// Runs list on MockStore with the DedicatedExecutor
    async fn do_list(exec: DedicatedExecutor, store: Arc<dyn ObjectStore>) {
        exec.spawn(async move {
            // run the stream to completion
            let res: Vec<_> = store
                .list(None)
                .map(|r| r.unwrap().location.to_string())
                .collect()
                .await;
            assert_eq!(res.join(","), "the/object");
        })
        .await
        .unwrap();
    }

    /* ObjectStore::list_with_delimiter() */

    #[tokio::test]
    #[should_panic(expected = "A Tokio 1.x context was found, but IO is disabled.")]
    async fn test_mock_store_list_with_delimiter_asserts() {
        // MockObject store should error when used directly by DedicatedExecutor
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        do_list_with_delimiter(exec, store).await
    }

    #[tokio::test]
    async fn test_io_store_list_with_delimiter_works() {
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        let store = exec.wrap_object_store_for_io(store);
        do_list_with_delimiter(exec, store).await
    }

    /// Runs list_with_delimiter on MockStore with the DedicatedExecutor
    async fn do_list_with_delimiter(
        exec: DedicatedExecutor,
        store: Arc<dyn ObjectStore>,
    ) {
        exec.spawn(async move {
            // run the stream to completion
            let list_result = store.list_with_delimiter(None).await.unwrap();

            let prefixes: Vec<_> = list_result
                .common_prefixes
                .iter()
                .map(|p| p.to_string())
                .collect();

            assert_eq!(prefixes.join(","), "the");
        })
        .await
        .unwrap();
    }

    /* ObjectStore::copy() */
    #[tokio::test]
    #[should_panic(expected = "A Tokio 1.x context was found, but IO is disabled.")]
    async fn test_mock_store_copy_asserts() {
        // MockObject store should error when used directly by DedicatedExecutor
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        do_copy(exec, store).await
    }

    #[tokio::test]
    async fn test_io_store_copy_works() {
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        let store = exec.wrap_object_store_for_io(store);
        do_copy(exec, store).await
    }

    /// Runs copy on MockStore with the DedicatedExecutor
    async fn do_copy(exec: DedicatedExecutor, store: Arc<dyn ObjectStore>) {
        exec.spawn(async move {
            // run the stream to completion
            store
                .copy(&Path::from("the/object"), &Path::from("the/other_object"))
                .await
                .unwrap()
        })
        .await
        .unwrap();
    }

    /* ObjectStore::copy_if_not_exists() */

    #[tokio::test]
    #[should_panic(expected = "A Tokio 1.x context was found, but IO is disabled.")]
    async fn test_mock_store_copy_if_not_exists_asserts() {
        // MockObject store should error when used directly by DedicatedExecutor
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        do_copy_if_not_exists(exec, store).await
    }

    #[tokio::test]
    async fn test_io_store_copy_if_not_exists_works() {
        let exec = DedicatedExecutorBuilder::new().build();
        let store = MockStore::create().await;
        let store = exec.wrap_object_store_for_io(store);
        do_copy_if_not_exists(exec, store).await
    }

    /// Runs copy_if_not_exists on MockStore with the DedicatedExecutor
    async fn do_copy_if_not_exists(exec: DedicatedExecutor, store: Arc<dyn ObjectStore>) {
        exec.spawn(async move {
            // run the stream to completion
            store
                .copy_if_not_exists(
                    &Path::from("the/object"),
                    &Path::from("the/other_object"),
                )
                .await
                .unwrap()
        })
        .await
        .unwrap();
    }

    /// Mock ObjectStore that purposely does some IO that will fail on the
    /// DedicatedExecutor's CPU thread pool
    ///
    /// Objects on startup:
    /// * "the/object" --> "the value"
    #[derive(Debug)]
    struct MockStore {
        inner: Arc<dyn ObjectStore>,
    }
    impl MockStore {
        /// `create` rather than `new` as it returns an `Arc`
        async fn create() -> Arc<dyn ObjectStore> {
            let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let payload = PutPayload::from("the value");
            inner.put(&Path::from("the/object"), payload).await.unwrap();
            Arc::new(Self { inner })
        }
    }
    impl Display for MockStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockStore")
        }
    }

    #[async_trait]
    impl ObjectStore for MockStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            mock_io().await;
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            mock_io().await;
            let res = self.inner.put_multipart_opts(location, opts).await?;

            // we also need to wrap the async methods on the return
            Ok(Box::new(MockMultipartUpload::new(res)))
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            mock_io().await;
            // since the result can also have a stream inside of it, stick a mock IO there too
            let mut result = self.inner.get_opts(location, options).await?;
            result.payload = match result.payload {
                payload @ GetResultPayload::File(_, _) => payload,
                GetResultPayload::Stream(stream) => {
                    let stream = stream.then(with_mock_io).boxed();
                    GetResultPayload::Stream(stream)
                }
            };
            Ok(result)
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            mock_io().await;
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix).then(with_mock_io).boxed()
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            mock_io().await;
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            mock_io().await;
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &Path,
            to: &Path,
        ) -> object_store::Result<()> {
            mock_io().await;
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    /// Mock multipart upload that does IO as well
    #[derive(Debug)]
    struct MockMultipartUpload {
        inner: Box<dyn MultipartUpload>,
    }
    impl MockMultipartUpload {
        fn new(inner: Box<dyn MultipartUpload>) -> Self {
            Self { inner }
        }
    }

    #[async_trait]
    impl MultipartUpload for MockMultipartUpload {
        fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
            self.inner.as_mut().put_part(data)
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            mock_io().await;
            self.inner.as_mut().complete().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            mock_io().await;
            self.inner.as_mut().abort().await
        }
    }

    /// purposely do something on tokio that requires IO to be enabled
    /// in this case try and open a socket
    ///
    /// Panic's if the IO is run on the wrong thread
    async fn mock_io() {
        TcpListener::bind("127.0.0.1:0").await.unwrap();
    }

    /// Return an async closure that does some mock IO and returns the same value
    ///
    /// Meant to be used with [`StreamExt::then`]
    async fn with_mock_io<X>(x: X) -> X {
        mock_io().await;
        x
    }
}
