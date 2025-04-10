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

use crate::trace_utils::{trace_spawn, trace_spawn_blocking, Spawner};
use std::future::Future;
use std::task::{Context, Poll};
use tokio::runtime::Handle;
use tokio::task::{AbortHandle, Id, JoinError, LocalSet};

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

impl<T: 'static + Send> Spawner<T> for JoinSet<T> {
    type ReturnHandle = AbortHandle;

    fn spawn_impl<F>(&mut self, future: F) -> Self::ReturnHandle
    where
        F: Future<Output = T> + Send + 'static
    {
        self.inner.spawn(future)
    }

    fn spawn_blocking_impl<F>(&mut self, f: F) -> Self::ReturnHandle
    where
        F: FnOnce() -> T + Send + 'static
    {
        self.inner.spawn_blocking(f)
    }
}

struct JoinSetWithHandle<'a, T: 'static + Send> {
    inner: &'a mut JoinSet<T>,
    handle: &'a Handle,
}

impl<T: 'static + Send> Spawner<T> for JoinSetWithHandle<'_, T> {
    type ReturnHandle = AbortHandle;

    fn spawn_impl<F>(&mut self, future: F) -> Self::ReturnHandle
    where
        F: Future<Output = T> + Send + 'static
    {
        self.inner.inner.spawn_on(future, self.handle)
    }

    fn spawn_blocking_impl<F>(&mut self, f: F) -> Self::ReturnHandle
    where
        F: FnOnce() -> T + Send + 'static
    {
        self.inner.inner.spawn_blocking_on(f, self.handle)
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
        trace_spawn(task, self)
    }

    /// [JoinSet::spawn_on](tokio::task::JoinSet::spawn_on) - Spawn a task on a provided runtime.
    pub fn spawn_on<F>(&mut self, task: F, handle: &Handle) -> AbortHandle
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        trace_spawn(task, &mut JoinSetWithHandle {
            inner: self,
            handle,
        })
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
        trace_spawn_blocking(f, self)
    }

    /// [JoinSet::spawn_blocking_on](tokio::task::JoinSet::spawn_blocking_on) - Spawn a blocking task on a provided runtime.
    pub fn spawn_blocking_on<F>(&mut self, f: F, handle: &Handle) -> AbortHandle
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send,
    {
        trace_spawn_blocking(f, &mut JoinSetWithHandle {
            inner: self,
            handle,
        })
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
