// A wrapper around DataFusion's `SessionContext` that provides a dedicated executor for query execution
// to avoid blocking the main tokio runtime with CPU bound work.
// Roughly vendored from https://github.com/influxdata/influxdb3_core/blob/64d98b136a3f5a70ee6bbf2d7982c4de1f1c3605/iox_query/src/exec/context.rs

use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::{RuntimeConfig, RuntimeEnv};

use futures::future::{BoxFuture, Shared};
use futures::{Future, FutureExt};
use std::sync::Arc;
use std::thread::JoinHandle as ThreadJoinHandle;
use std::time::Duration;
use std::{
    cell::OnceCell,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::Notify;
use tokio::task::{JoinError, JoinHandle as TokioJoinHandle};

#[cfg(feature = "tracing")]
use tracing::Instrument;

use crate::execution::context::SessionContext;

thread_local! {
    /// Tokio runtime `Handle` for doing network (I/O) operations, see [`spawn_io`]
    pub static IO_RUNTIME: OnceCell<Handle> = const { OnceCell::new() };
}

/// Registers `handle` as the IO runtime for this thread.
///
/// See [`spawn_io`]
fn register_io_runtime(handle: Handle) {
    IO_RUNTIME.with(|cell| {
        cell.set(handle)
            .expect("IO runtime already registered on this thread")
    });
}

/// Runs `fut` on the runtime registered by [`register_io_runtime`]
/// 
/// You should run all IO work through this function to ensure it doesn't run on
/// the CPU runtime and get blocked by CPU bound work.
///
/// # Panic
/// Needs a IO runtime [registered](register_io_runtime).
pub async fn spawn_io<Fut>(fut: Fut) -> Fut::Output
where
    Fut: Future + Send + 'static,
    Fut::Output: Send,
{
    let fut = IO_RUNTIME.with(|cell| {
        let Some(handle) = cell.get() else {
            panic!("No IO runtime registered. Call `register_io_runtime`/`register_current_runtime_for_io` in current thread!");
        };
        spawn_with_cancel_on_drop(handle, fut)
    });
    fut.await.expect("IO runtime was shut down unexpectedly")
}

/// Spawn a task onto the runtime of the provided `Handle`
///
/// Current tracing span is attached to the spawned task and will be entered
/// every time the task is polled or dropped.
fn spawn_with_cancel_on_drop<F>(
    h: &Handle,
    f: F,
) -> impl Future<Output = Result<F::Output, JoinError>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    #[cfg(feature = "tracing")]
    return DropGuard(h.spawn(f.in_current_span()));
    #[cfg(not(feature = "tracing"))]
    return DropGuard(h.spawn(f));
}

struct DropGuard<T>(TokioJoinHandle<T>);
impl<T> Drop for DropGuard<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Future for DropGuard<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

/// Configuration for a `SessionContext` that uses a dedicated runtime for query execution.
/// 
/// This ensures that CPU bound work doesn't block IO work.
pub struct CrossRtSessionConfig {
    executor: DedicatedExecutor,
    session_config: SessionConfig,
    runtime_env: Arc<RuntimeEnv>,
}

impl std::fmt::Debug for CrossRtSessionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrossRtSessionConfig")
            .field("exec", &self.executor)
            .field("session_config", &self.session_config)
            .field("runtime", &self.runtime_env)
            .finish()
    }
}

/// Sets a lower thread priority for the current thread and configures the thread to be scheduled as a batch job / non realtime.
/// This helps both IO and CPU threads by:
/// - Making sure IO threads have priority so polls and timing doesn't get messed up
/// - Making sure CPU threads don't thrash / context switch too much
#[cfg(target_os = "linux")]
pub fn set_cpu_thread_to_low_priority() {
    let thread_id = thread_priority::thread_native_id();
    thread_priority::set_thread_priority_and_policy(
        thread_id,
        // 10 is a somewhat arbitrary number, we took it from influx:
        // https://github.com/influxdata/influxdb3_core/blob/1eaa4ed5ea147bc24db98d9686e457c124dfd5b7/clap_blocks/src/tokio.rs#L262C5-L262C28
        thread_priority::ThreadPriority::Crossplatform(
            thread_priority::ThreadPriorityValue::try_from(10).unwrap(),
        ),
        // set thread scheduling policy to batch / normal to disfavor preempting them
        thread_priority::ThreadSchedulePolicy::Normal(
            thread_priority::NormalThreadSchedulePolicy::Batch,
        ),
    )
    .expect("failed to set thread priority");
}

#[cfg(not(target_os = "linux"))]
pub fn set_cpu_thread_to_low_priority() {
    // no-op on non-linux platforms
}

/// Builder for a `CrossRtSessionConfig`.
/// 
/// Allows configuring the number of worker threads, thread stack size, and memory limit.
/// 
/// It is expected that this be built on the main IO tokio runtime or that an
/// explicit `Handle` is provided.
#[derive(Debug, Clone)]
pub struct CrossRtSessionConfigBuilder {
    session_config: SessionConfig,
    io_handle: Option<Handle>,
    worker_threads: Option<usize>,
    thread_stack_size: Option<usize>,
    memory_limit: Option<usize>,
}

impl CrossRtSessionConfigBuilder {
    /// Create a new `CrossRtSessionConfigBuilder` with the provided `SessionConfig`.
    pub fn new(session_config: SessionConfig) -> Self {
        Self {
            session_config,
            io_handle: None,
            worker_threads: None,
            thread_stack_size: None,
            memory_limit: None,
        }
    }

    /// Set an explicit `Handle` for the IO runtime.
    /// 
    /// Otherwise, the current (calling) runtime will be used.
    pub fn with_io_handle(mut self, io_handle: Handle) -> Self {
        self.io_handle = Some(io_handle);
        self
    }

    /// Set the number of worker threads for the dedicated runtime.
    /// 
    /// Defaults to the number of logical CPUs.
    pub fn with_worker_threads(mut self, worker_threads: usize) -> Self {
        self.worker_threads = Some(worker_threads);
        self
    }

    /// Set the thread stack size for the dedicated runtime.
    pub fn with_thread_stack_size(mut self, thread_stack_size: usize) -> Self {
        self.thread_stack_size = Some(thread_stack_size);
        self
    }

    /// Sets a memory limit for query execution.
    pub fn with_memory_limit(mut self, memory_limit: usize) -> Self {
        self.memory_limit = Some(memory_limit);
        self
    }

    /// Build the `CrossRtSessionConfig`.
    pub fn build(self) -> CrossRtSessionConfig {
        let io_handle = self.io_handle.unwrap_or(Handle::current());
        register_io_runtime(io_handle.clone());
        let executor_io_handle = io_handle.clone();

        let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(worker_threads) = self.worker_threads {
            runtime_builder.worker_threads(worker_threads);
        }
        if let Some(thread_stack_size) = self.thread_stack_size {
            runtime_builder.thread_stack_size(thread_stack_size);
        }

        let runtime = runtime_builder
            .enable_time() // don't allow io on DataFusion CPU threads!
            .on_thread_start(move || {
                register_io_runtime(io_handle.clone());
                set_cpu_thread_to_low_priority();
            })
            .build()
            .expect("failed to build dedicated runtime");

        let executor = DedicatedExecutor::new(runtime, executor_io_handle);

        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_sign_loss,
            clippy::cast_possible_truncation
        )]
        let mut runtime_config = RuntimeConfig::new();
        if let Some(system_memory_limit_bytes) = self.memory_limit {
            runtime_config =
                runtime_config.with_memory_limit(system_memory_limit_bytes, 0.8);
        }

        let runtime_env = Arc::new(RuntimeEnv::try_new(runtime_config).unwrap());

        CrossRtSessionConfig {
            executor,
            session_config: self.session_config,
            runtime_env,
        }
    }
}

impl CrossRtSessionConfig {
    /// Create a new `SessionContext` for general query execution.
    pub fn new_context(&self) -> SessionContext {
        let session_config = self.session_config.clone();
        let runtime = self.runtime_env.clone();
        SessionContext::new_with_config_rt(session_config, runtime)
    }

    /// Spawn a future on this runtime.
    /// 
    /// This should be the entrypoint for executing queries.
    /// 
    /// In the future we may provide a more ergonomic wrapper exposing all
    /// of the methods on `SessionContext` directly so that you can do a 1:1 swap.
    /// 
    /// To ensure that IO work is moved _back_ to the IO runtime you must either:
    /// - Use [`spawn_io`] to spawn the IO work (lower level)
    /// - Use [`CrossRtObjectStore`](crate::execution::cross_rt::object_store::CrossRtObjectStore) to wrap all of
    ///   any ObjectStore you use in the query.
    pub async fn spawn<F>(&self, fut: F) -> Result<F::Output, JoinError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.executor.spawn(fut).await
    }

    /// Join the underlying executor.
    pub async fn join(&self) {
        self.executor.join().await;
    }
}

/// Dedicated runtime for query execution.
///
/// `DataFusion` plans are "CPU" bound and thus can consume tokio
/// executors threads for extended periods of time. We use a
/// dedicated tokio runtime to run them so that other requests
/// can be handled.
#[derive(Clone, Debug)]
pub struct DedicatedExecutor(Arc<DedicatedExecutorState>);

#[derive(Debug)]
struct DedicatedExecutorState {
    handle: Handle,
    start_shutdown: Arc<Notify>,
    shutdown_completed: Shared<BoxFuture<'static, Result<(), RecvError>>>,
    thread: Option<ThreadJoinHandle<()>>,
}

impl DedicatedExecutor {
    pub(super) fn new(runtime: Runtime, io_handle: Handle) -> Self {
        let handle = runtime.handle().clone();
        let start_shutdown = Arc::new(Notify::new());
        let start_shutdown_rx = start_shutdown.clone();

        // This uses a blocking channel because we want to block the main thread until the dedicated
        // executor is ready, (we don't care about doing this asynchronously because we are doing setup).
        let (rt_ready_tx, rt_ready_rx) = std::sync::mpsc::sync_channel(0);

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let shutdown_completed = shutdown_rx.boxed().shared();

        let thread = std::thread::Builder::new()
            .name("fusionfire-rt-driver".to_string())
            .spawn(move || {
                register_io_runtime(io_handle);

                runtime.block_on(async move {
                    // NB Race condition!
                    // The `.notified()` future won't receive any notifications sent before it is
                    // enabled (or polled for the first time).
                    //
                    // Therefore we do a little dance here to block `new` from returning until the
                    // call to `.enable()` has been made.
                    let mut shutdown = std::pin::pin!(start_shutdown_rx.notified());
                    shutdown.as_mut().enable();

                    rt_ready_tx.send(()).expect("failed to send ready signal");

                    shutdown.await;
                });

                runtime.shutdown_timeout(Duration::from_secs(10));
                shutdown_tx.send(()).unwrap();
            })
            .expect("failed to spawn dedicated runtime driver thread");

        rt_ready_rx
            .recv()
            .expect("rt driver thread failed to start");

        Self(Arc::new(DedicatedExecutorState {
            handle,
            start_shutdown,
            shutdown_completed,
            thread: Some(thread),
        }))
    }

    /// Spawn a future on this runtime.
    pub async fn spawn<F>(&self, fut: F) -> Result<F::Output, JoinError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        spawn_with_cancel_on_drop(&self.0.handle, fut).await
    }

    async fn join(&self) {
        self.0.shutdown();
        self.0
            .shutdown_completed
            .clone()
            .await
            .expect("failed to shutdown cpu runtime");
    }
}

impl DedicatedExecutorState {
    fn shutdown(&self) {
        // If this method is called multiple times it will essentially be a no-op
        self.start_shutdown.notify_one();
    }
}

impl Drop for DedicatedExecutorState {
    fn drop(&mut self) {
        self.shutdown();

        // This is the only time we interact with the thread
        // self.thread will always be Some(_) up to this point
        if let Some(thread) = self.thread.take() {
            if let Err(e) = thread.join() {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    "failed to join dedicated runtime thread",
                    error = format!("{e:?}")
                );
                #[cfg(not(feature = "tracing"))]
                eprintln!("failed to join dedicated runtime thread: {:?}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::RecordBatch;
    use arrow_schema::{Field, Schema};
    use datafusion_common::assert_batches_eq;
    use object_store::{path::Path, ObjectStore};
    use parquet::arrow::ArrowWriter;
    use url::Url;

    use crate::{
        execution::cross_rt::object_store::CrossRtObjectStore,
        prelude::ParquetReadOptions,
    };
    use crate::prelude::*;

    use super::*;

    #[tokio::test]
    async fn test_error_on_shutdown() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let io_handle = runtime.handle().clone();

        let executor = DedicatedExecutor::new(runtime, io_handle);

        // This should work fine
        assert!(executor.spawn(async {}).await.is_ok());

        // Shutdown the executor
        executor.join().await;

        // This should fail with a cancelled `JoinErr` after shutdown
        assert!(executor.spawn(async {}).await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn test_execute_query() {
        let crt_session_config = CrossRtSessionConfigBuilder::new(SessionConfig::new())
            .with_worker_threads(1)
            .build();

        let ctx = crt_session_config.new_context();

        let store = Arc::new(CrossRtObjectStore::new(Arc::new(
            object_store::memory::InMemory::new(),
        )));

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "a",
                arrow::datatypes::DataType::Int64,
                false,
            )])),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let mut buff = vec![];
        {
            let mut writer =
                ArrowWriter::try_new(&mut buff, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        };

        // write out batch to memory://data.parquet
        let path = Path::from("data.parquet");
        store
            .put(&path, object_store::PutPayload::from(buff))
            .await
            .unwrap();

        let batches = crt_session_config
            .spawn(async move {
                let ctx = ctx.clone();
                ctx.register_object_store(&Url::parse("memory://test").unwrap(), store);
                let opts = ParquetReadOptions::default();
                let mut df = ctx
                    .read_parquet("memory://test/data.parquet", opts)
                    .await
                    .unwrap();
                df = df.filter(col("a").gt(lit(1))).unwrap();
                let batches = df.collect().await.unwrap();
                batches
            })
            .await
            .unwrap();

        #[rustfmt::skip]
        let expected = vec![
            "+---+",
            "| a |",
            "+---+",
            "| 2 |",
            "| 3 |",
            "+---+",
        ];
        assert_batches_eq!(expected, &batches);
    }
}
