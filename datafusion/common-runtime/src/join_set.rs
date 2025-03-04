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

use futures::FutureExt;
use std::any::Any;
use std::future::Future;
use std::task::{Context, Poll};
use tokio::runtime::Handle;
use tokio::task::{AbortHandle, Id, JoinError, LocalSet};

pub mod trace_utils {
    use super::*;
    use futures::future::BoxFuture;
    use tokio::sync::OnceCell;

    /// A trait for injecting instrumentation into either asynchronous futures or
    /// blocking closures at runtime.
    pub trait JoinSetTracer: Send + Sync + 'static {
        /// Function pointer type for tracing a future.
        ///
        /// This function takes a boxed future (with its output type erased)
        /// and returns a boxed future (with its output still erased). The
        /// tracer must apply instrumentation without altering the output.
        fn trace_future(
            &self,
            fut: BoxFuture<'static, Box<dyn Any + Send>>,
        ) -> BoxFuture<'static, Box<dyn Any + Send>>;

        /// Function pointer type for tracing a blocking closure.
        ///
        /// This function takes a boxed closure (with its return type erased)
        /// and returns a boxed closure (with its return type still erased). The
        /// tracer must apply instrumentation without changing the return value.
        fn trace_block(
            &self,
            f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
        ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>;
    }

    /// A no-op tracer that does not modify or instrument any futures or closures.
    /// This is used as a fallback if no custom tracer is set.
    struct NoopTracer;

    impl JoinSetTracer for NoopTracer {
        fn trace_future(
            &self,
            fut: BoxFuture<'static, Box<dyn Any + Send>>,
        ) -> BoxFuture<'static, Box<dyn Any + Send>> {
            fut
        }

        fn trace_block(
            &self,
            f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
        ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> {
            f
        }
    }

    /// Global storage for an injected tracer. If no tracer is injected, a no-op
    /// tracer is used instead. This ensures that calls to [`trace_future`] or
    /// [`trace_block`] never panic due to missing instrumentation.
    static GLOBAL_TRACER: OnceCell<&'static dyn JoinSetTracer> = OnceCell::const_new();

    /// A no-op tracer singleton that is returned by [`get_tracer`] if no custom
    /// tracer has been registered.
    static NOOP_TRACER: NoopTracer = NoopTracer;

    /// Return the currently registered tracer, or the no-op tracer if none was
    /// registered.
    #[inline]
    fn get_tracer() -> &'static dyn JoinSetTracer {
        GLOBAL_TRACER.get().copied().unwrap_or(&NOOP_TRACER)
    }

    /// Set the custom tracer for both futures and blocking closures.
    ///
    /// This should be called once at startup. If called more than once, an
    /// `Err(())` is returned. If not called at all, a no-op tracer that does nothing
    /// is used.
    pub fn set_join_set_tracer(tracer: &'static dyn JoinSetTracer) -> Result<(), ()> {
        GLOBAL_TRACER.set(tracer).map_err(|_| ())
    }

    /// Optionally instruments a future with custom tracing.
    ///
    /// If a tracer has been injected via `set_tracer`, the future's output is
    /// boxed (erasing its type), passed to the tracer, and then downcast back
    /// to the expected type. If no tracer is set, the original future is returned.
    ///
    /// # Type Parameters
    /// * `T` - The concrete output type of the future.
    /// * `F` - The future type.
    ///
    /// # Parameters
    /// * `future` - The future to potentially instrument.
    pub fn trace_future<T, F>(future: F) -> BoxFuture<'static, T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // Erase the future’s output type first:
        let erased_future = async move {
            let result = future.await;
            Box::new(result) as Box<dyn Any + Send>
        }
        .boxed();

        // Forward through the global tracer:
        get_tracer()
            .trace_future(erased_future)
            // Downcast from `Box<dyn Any + Send>` back to `T`:
            .map(|any_box| {
                *any_box
                    .downcast::<T>()
                    .expect("Tracer must preserve the future’s output type!")
            })
            .boxed()
    }

    /// Optionally instruments a blocking closure with custom tracing.
    ///
    /// If a tracer has been injected via `set_tracer`, the closure is wrapped so that
    /// its return value is boxed (erasing its type), passed to the tracer, and then the
    /// result is downcast back to the original type. If no tracer is set, the closure is
    /// returned unmodified (except for being boxed).
    ///
    /// # Type Parameters
    /// * `T` - The concrete return type of the closure.
    /// * `F` - The closure type.
    ///
    /// # Parameters
    /// * `f` - The blocking closure to potentially instrument.
    pub fn trace_block<T, F>(f: F) -> Box<dyn FnOnce() -> T + Send>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        // Erase the closure’s return type first:
        let erased_closure = Box::new(|| {
            let result = f();
            Box::new(result) as Box<dyn Any + Send>
        });

        // Forward through the global tracer:
        let traced_closure = get_tracer().trace_block(erased_closure);

        // Downcast from `Box<dyn Any + Send>` back to `T`:
        Box::new(move || {
            let any_box = traced_closure();
            *any_box
                .downcast::<T>()
                .expect("Tracer must preserve the closure’s return type!")
        })
    }
}

/// A wrapper around Tokio's JoinSet that forwards all API calls while optionally
/// instrumenting spawned tasks and blocking closures with custom tracing behavior.
/// If no tracer is injected via `trace_utils::set_tracer`, tasks and closures are executed
/// without any instrumentation.
#[derive(Debug)]
pub struct JoinSet<T> {
    inner: tokio::task::JoinSet<T>,
}

impl<T> Default for JoinSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> JoinSet<T> {
    /// [JoinSet::new](tokio::task::JoinSet::new) - Create a new JoinSet.
    pub fn new() -> Self {
        Self {
            inner: tokio::task::JoinSet::new(),
        }
    }

    /// [JoinSet::len](tokio::task::JoinSet::len) - Return the number of tasks.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// [JoinSet::is_empty](tokio::task::JoinSet::is_empty) - Check if the JoinSet is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<T: 'static> JoinSet<T> {
    /// [JoinSet::spawn](tokio::task::JoinSet::spawn) - Spawn a new task.
    pub fn spawn<F>(&mut self, task: F) -> AbortHandle
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        self.inner.spawn(trace_utils::trace_future(task))
    }

    /// [JoinSet::spawn_on](tokio::task::JoinSet::spawn_on) - Spawn a task on a provided runtime.
    pub fn spawn_on<F>(&mut self, task: F, handle: &Handle) -> AbortHandle
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        self.inner.spawn_on(trace_utils::trace_future(task), handle)
    }

    /// [JoinSet::spawn_local](tokio::task::JoinSet::spawn_local) - Spawn a local task.
    pub fn spawn_local<F>(&mut self, task: F) -> AbortHandle
    where
        F: Future<Output = T>,
        F: 'static,
    {
        self.inner.spawn_local(task)
    }

    /// [JoinSet::spawn_local_on](tokio::task::JoinSet::spawn_local_on) - Spawn a local task on a provided LocalSet.
    pub fn spawn_local_on<F>(&mut self, task: F, local_set: &LocalSet) -> AbortHandle
    where
        F: Future<Output = T>,
        F: 'static,
    {
        self.inner.spawn_local_on(task, local_set)
    }

    /// [JoinSet::spawn_blocking](tokio::task::JoinSet::spawn_blocking) - Spawn a blocking task.
    pub fn spawn_blocking<F>(&mut self, f: F) -> AbortHandle
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send,
    {
        self.inner.spawn_blocking(trace_utils::trace_block(f))
    }

    /// [JoinSet::spawn_blocking_on](tokio::task::JoinSet::spawn_blocking_on) - Spawn a blocking task on a provided runtime.
    pub fn spawn_blocking_on<F>(&mut self, f: F, handle: &Handle) -> AbortHandle
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send,
    {
        self.inner
            .spawn_blocking_on(trace_utils::trace_block(f), handle)
    }

    /// [JoinSet::join_next](tokio::task::JoinSet::join_next) - Await the next completed task.
    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        self.inner.join_next().await
    }

    /// [JoinSet::try_join_next](tokio::task::JoinSet::try_join_next) - Try to join the next completed task.
    pub fn try_join_next(&mut self) -> Option<Result<T, JoinError>> {
        self.inner.try_join_next()
    }

    /// [JoinSet::abort_all](tokio::task::JoinSet::abort_all) - Abort all tasks.
    pub fn abort_all(&mut self) {
        self.inner.abort_all()
    }

    /// [JoinSet::detach_all](tokio::task::JoinSet::detach_all) - Detach all tasks.
    pub fn detach_all(&mut self) {
        self.inner.detach_all()
    }

    /// [JoinSet::poll_join_next](tokio::task::JoinSet::poll_join_next) - Poll for the next completed task.
    pub fn poll_join_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<T, JoinError>>> {
        self.inner.poll_join_next(cx)
    }

    /// [JoinSet::join_next_with_id](tokio::task::JoinSet::join_next_with_id) - Await the next completed task with its ID.
    pub async fn join_next_with_id(&mut self) -> Option<Result<(Id, T), JoinError>> {
        self.inner.join_next_with_id().await
    }

    /// [JoinSet::try_join_next_with_id](tokio::task::JoinSet::try_join_next_with_id) - Try to join the next completed task with its ID.
    pub fn try_join_next_with_id(&mut self) -> Option<Result<(Id, T), JoinError>> {
        self.inner.try_join_next_with_id()
    }

    /// [JoinSet::poll_join_next_with_id](tokio::task::JoinSet::poll_join_next_with_id) - Poll for the next completed task with its ID.
    pub fn poll_join_next_with_id(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(Id, T), JoinError>>> {
        self.inner.poll_join_next_with_id(cx)
    }

    /// [JoinSet::shutdown](tokio::task::JoinSet::shutdown) - Abort all tasks and wait for shutdown.
    pub async fn shutdown(&mut self) {
        self.inner.shutdown().await
    }

    /// [JoinSet::join_all](tokio::task::JoinSet::join_all) - Await all tasks.
    pub async fn join_all(self) -> Vec<T> {
        self.inner.join_all().await
    }
}
