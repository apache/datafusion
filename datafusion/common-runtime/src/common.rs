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

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::task::{JoinError, JoinHandle};

use crate::trace_utils::{trace_block, trace_future};

/// Helper that  provides a simple API to spawn a single task and join it.
/// Provides guarantees of aborting on `Drop` to keep it cancel-safe.
/// Note that if the task was spawned with `spawn_blocking`, it will only be
/// aborted if it hasn't started yet.
///
/// Technically, it's just a wrapper of a `JoinHandle` overriding drop.
#[derive(Debug)]
pub struct SpawnedTask<R> {
    inner: JoinHandle<R>,
}

impl<R: 'static> SpawnedTask<R> {
    pub fn spawn<T>(task: T) -> Self
    where
        T: Future<Output = R>,
        T: Send + 'static,
        R: Send,
    {
        // Ok to use spawn here as SpawnedTask handles aborting/cancelling the task on Drop
        #[allow(clippy::disallowed_methods)]
        let inner = tokio::task::spawn(trace_future(task));
        Self { inner }
    }

    pub fn spawn_blocking<T>(task: T) -> Self
    where
        T: FnOnce() -> R,
        T: Send + 'static,
        R: Send,
    {
        // Ok to use spawn_blocking here as SpawnedTask handles aborting/cancelling the task on Drop
        #[allow(clippy::disallowed_methods)]
        let inner = tokio::task::spawn_blocking(trace_block(task));
        Self { inner }
    }

    /// Joins the task, returning the result of join (`Result<R, JoinError>`).
    /// Same as awaiting the spawned task, but left for backwards compatibility.
    pub async fn join(self) -> Result<R, JoinError> {
        self.await
    }

    /// Joins the task and unwinds the panic if it happens.
    pub async fn join_unwind(mut self) -> Result<R, JoinError> {
        self.join_unwind_mut().await
    }

    /// Joins the task using a mutable reference and unwinds the panic if it happens.
    ///
    /// This method is similar to [`join_unwind`](Self::join_unwind), but takes a mutable
    /// reference instead of consuming `self`. This allows the `SpawnedTask` to remain
    /// usable after the call.
    ///
    /// If called multiple times on the same task:
    /// - If the task is still running, it will continue waiting for completion
    /// - If the task has already completed successfully, subsequent calls will
    ///   continue to return the same `JoinError` indicating the task is finished
    /// - If the task panicked, the first call will resume the panic, and the
    ///   program will not reach subsequent calls
    pub async fn join_unwind_mut(&mut self) -> Result<R, JoinError> {
        self.await.map_err(|e| {
            // `JoinError` can be caused either by panic or cancellation. We have to handle panics:
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            } else {
                log::warn!("SpawnedTask was polled during shutdown");
                e
            }
        })
    }
}

impl<R> Future for SpawnedTask<R> {
    type Output = Result<R, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

impl<R> Drop for SpawnedTask<R> {
    fn drop(&mut self) {
        self.inner.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::future::{Pending, pending};

    use tokio::{runtime::Runtime, sync::oneshot};

    #[tokio::test]
    async fn runtime_shutdown() {
        let rt = Runtime::new().unwrap();
        #[allow(clippy::async_yields_async)]
        let task = rt
            .spawn(async {
                SpawnedTask::spawn(async {
                    let fut: Pending<()> = pending();
                    fut.await;
                    unreachable!("should never return");
                })
            })
            .await
            .unwrap();

        // caller shutdown their DF runtime (e.g. timeout, error in caller, etc)
        rt.shutdown_background();

        // race condition
        // poll occurs during shutdown (buffered stream poll calls, etc)
        assert!(matches!(
            task.join_unwind().await,
            Err(e) if e.is_cancelled()
        ));
    }

    #[tokio::test]
    #[should_panic(expected = "foo")]
    async fn panic_resume() {
        // this should panic w/o an `unwrap`
        SpawnedTask::spawn(async { panic!("foo") })
            .join_unwind()
            .await
            .ok();
    }

    #[tokio::test]
    async fn cancel_not_started_task() {
        let (sender, receiver) = oneshot::channel::<i32>();
        let task = SpawnedTask::spawn(async {
            // Shouldn't be reached.
            sender.send(42).unwrap();
        });

        drop(task);

        // If the task was cancelled, the sender was also dropped,
        // and awaiting the receiver should result in an error.
        assert!(receiver.await.is_err());
    }

    #[tokio::test]
    async fn cancel_ongoing_task() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let task = SpawnedTask::spawn(async move {
            sender.send(1).await.unwrap();
            // This line will never be reached because the channel has a buffer
            // of 1.
            sender.send(2).await.unwrap();
        });
        // Let the task start.
        assert_eq!(receiver.recv().await.unwrap(), 1);
        drop(task);

        // The sender was dropped so we receive `None`.
        assert!(receiver.recv().await.is_none());
    }
}
