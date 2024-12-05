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
//!
//! Originally from [InfluxDB 3.0]
//!
//! [InfluxDB 3.0]: https://github.com/influxdata/influxdb3_core/tree/6fcbb004232738d55655f32f4ad2385523d10696/executor
use crate::cross_rt_stream::CrossRtStream;
use crate::stream::RecordBatchStreamAdapter;
use crate::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use futures::{
    future::{BoxFuture, Shared},
    Future, FutureExt, Stream, TryFutureExt,
};
use log::{info, warn};
use parking_lot::RwLock;
use std::cell::RefCell;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{fmt::Display, sync::Arc, time::Duration};
use tokio::runtime::Builder;
use tokio::task::JoinHandle;
use tokio::{
    runtime::Handle,
    sync::{oneshot::error::RecvError, Notify},
    task::JoinSet,
};

impl From<Builder> for DedicatedExecutorBuilder {
    fn from(value: Builder) -> Self {
        Self::new_from_builder(value)
    }
}

/// Manages a separate tokio [`Runtime`] (thread pool) for executing tasks such
/// as DataFusion `ExecutionPlans`.
///
/// See [`DedicatedExecutorBuilder`] for creating a new instance.
///
/// A `DedicatedExecutor` makes it easier to avoid running IO and CPU bound
/// tasks on the same threadpool by running futures (and any `tasks` that are
/// `tokio::task::spawned` by them) on a separate tokio [`Executor`].
///
/// DedicatedExecutor can be `clone`ed and all clones share the same threadpool.
///
/// TODO add note about `io_thread`
///
/// TODO: things we use in InfluxData
/// 1. Testing mode (so we can make a bunch of DedicatedExecutors) -- maybe we can wrap DedicatedExectors like IOxDedicatedExecutors
/// 2. Some sort of hook to install tokio metrics
///
/// When [`DedicatedExecutorBuilder::build`] is called, the "current" tokio
/// runtime will be maked for io, via [`register_io_runtime`] by all threads
/// spawned by the executor. Any I/O done by threads in this
/// [`DedicatedExecutor`] should use [`spawn_io`], which will run them on the I/O
/// runtime.
///
/// ## TODO examples
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
/// for IO. One solution is to run the task using [DedicatedExecutor::spawn_cpu].
///
/// ## "Cannot drop a runtime in a context where blocking is not allowed"`
///
/// If you try to use this structure from an async context you see something like
/// thread 'test_builder_plan' panicked at 'Cannot
/// drop a runtime in a context where blocking is not allowed it means  This
/// happens when a runtime is dropped from within an asynchronous
/// context.', .../tokio-1.4.0/src/runtime/blocking/shutdown.rs:51:21
///
/// TODO: make this an Arc<..> rather than an cloneable thing (to follow the smae
/// pattern as the rest of the system)
#[derive(Clone, Debug)]
pub struct DedicatedExecutor {
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
    /// # TODO: make this wait (aka so the API doesn't start a new background task or whatever)
    ///
    /// # Notes
    ///
    /// This task is run on a dedicated Tokio runtime that purposely does not have
    /// IO enabled. If your future makes any IO calls, you have to
    /// explicitly run them on DedicatedExecutor::spawn_io.
    ///
    /// If you see a message like this
    ///
    /// (Panic { msg: "A Tokio 1.x context was found, but timers are disabled. Call `enable_time` on the runtime builder to enable timers."
    ///
    /// It means some work that was meant to be done on the IO runtime was done
    /// on the CPU runtime.
    ///
    /// UNLIKE [`tokio::task::spawn`], the returned future is **cancelled** when
    /// it is dropped. Thus, you need ensure the returned future lives until it
    /// completes (call `await`) or you wish to cancel it.
    ///
    /// All spawned tasks are added to the tokio executor immediately and
    /// compete for the threadpool's resources.
    pub fn spawn_cpu<T>(
        &self,
        task: T,
    ) -> impl Future<Output = Result<T::Output, JobError>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let handle = {
            let state = self.state.read();
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
        let mut state = self.state.write();
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
            let state = self.state.read();
            state.completed_shutdown.clone()
        };

        // wait for completion while not holding the mutex to avoid
        // deadlocks
        handle.await.expect("Thread died?")
    }

    /// Returns a SendableRecordBatchStream that will run on this executor's thread pool
    pub fn run_sendable_record_batch_stream(
        &self,
        stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        let schema = stream.schema();
        let cross_rt_stream =
            CrossRtStream::new_with_df_error_stream(stream, self.clone());
        Box::pin(RecordBatchStreamAdapter::new(schema, cross_rt_stream))
    }

    /// Runs an stream that produces Results on the executor's thread pool
    ///
    /// Ths stream must produce Results so that any errors on the dedicated
    /// executor (like a panic or shutdown) can be communicated back.
    ///
    /// # Arguments:
    /// - stream: the stream to run on this dedicated executor
    /// - converter: a function that converts a [`JobError`] to the error type of the stream
    pub fn run_stream<X, E, S, C>(
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
        CrossRtStream::new_with_error_stream(stream, self.clone(), converter)
    }

    /// Registers `handle` as the IO runtime for this thread
    ///
    /// This sets a thread-local variable
    ///
    /// See [`spawn_io`](Self::spawn_io) for more details
    pub fn register_io_runtime(handle: Option<Handle>) {
        IO_RUNTIME.set(handle)
    }

    /// Registers the "current" `handle` as the IO runtime for this thread
    ///
    /// This is useful for testing purposes.
    ///
    /// # Panics if no current handle is available (aka not running in a tokio
    /// runtime)
    pub fn register_current_runtime_for_io() {
        Self::register_io_runtime(Some(Handle::current()))
    }

    /// Runs `fut` on the runtime registered by [`register_io_runtime`] if any,
    /// otherwise panics.
    ///
    /// # Panic
    /// Needs a IO runtime [registered](register_io_runtime).
    pub async fn spawn_io<Fut>(fut: Fut) -> Fut::Output
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
    /// Runtime handle.
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

/// Potential error returned when polling [`DedicatedExecutor::spawn_cpu`].
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
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the number of worker threads. Defaults to the tokio default (the
    /// number of virtual CPUs)
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

#[cfg(test)]
#[allow(unused_qualifications)]
mod tests {
    use super::*;
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
            .spawn_cpu(async move {
                let dedicated_id = std::thread::current().id();
                let spawned =
                    DedicatedExecutor::spawn_io(
                        async move { std::thread::current().id() },
                    )
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
        let dedicated_task = exec.spawn_cpu(do_work(42, Arc::clone(&barrier)));

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
        let dedicated_task = exec.clone().spawn_cpu(do_work(42, Arc::clone(&barrier)));
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

        let task = exec.spawn_cpu(do_work(42, Arc::clone(&barrier)));
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
        let dedicated_task1 = exec.spawn_cpu(do_work(11, Arc::clone(&barrier)));
        let dedicated_task2 = exec.spawn_cpu(do_work(42, Arc::clone(&barrier)));

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
        let dedicated_task = exec.spawn_cpu(async move {
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
        let dedicated_task = exec.spawn_cpu(async move {
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
        let dedicated_task = exec.spawn_cpu(async move {
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
        let dedicated_task = exec.spawn_cpu(async move {
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
        let dedicated_task = exec.spawn_cpu(async move {
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
        let dedicated_task = exec.spawn_cpu(async { 11 });

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
        let dedicated_task = exec.spawn_cpu(async { 11 });

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
        let dedicated_task1 = exec.spawn_cpu(async move {
            barrier1_pre_captured.wait().await;
            do_work_async(11, barrier1_post_captured).await
        });
        barrier1_pre.wait().await;

        // create second blocked task
        let barrier2_pre = Arc::new(AsyncBarrier::new(2));
        let barrier2_pre_captured = Arc::clone(&barrier2_pre);
        let barrier2_post = Arc::new(AsyncBarrier::new(2));
        let barrier2_post_captured = Arc::clone(&barrier2_post);
        let dedicated_task2 = exec.spawn_cpu(async move {
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
            .spawn_cpu(async move {
                // the only way (I've found) to test if IO is enabled is to use it and observer if tokio panics
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
            DedicatedExecutor::spawn_io(async move { std::thread::current().id() }).await;
        assert_eq!(measured_thread_id, io_thread_id);

        rt_io.shutdown_background();
    }

    #[tokio::test]
    #[should_panic(expected = "IO runtime registered")]
    async fn test_panic_if_no_runtime_registered() {
        DedicatedExecutor::spawn_io(futures::future::ready(())).await;
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

        DedicatedExecutor::spawn_io(futures::future::ready(())).await;
    }
}
