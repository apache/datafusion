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

use std::future::Future;

use tokio::task::{JoinError, JoinSet};

/// Helper that  provides a simple API to spawn a single task and join it.
/// Provides guarantees of aborting on `Drop` to keep it cancel-safe.
///
/// Technically, it's just a wrapper of `JoinSet` (with size=1).
#[derive(Debug)]
pub struct SpawnedTask<R> {
    inner: JoinSet<R>,
}

impl<R: 'static> SpawnedTask<R> {
    pub fn spawn<T>(task: T) -> Self
    where
        T: Future<Output = R>,
        T: Send + 'static,
        R: Send,
    {
        let mut inner = JoinSet::new();
        inner.spawn(task);
        Self { inner }
    }

    pub fn spawn_blocking<T>(task: T) -> Self
    where
        T: FnOnce() -> R,
        T: Send + 'static,
        R: Send,
    {
        let mut inner = JoinSet::new();
        inner.spawn_blocking(task);
        Self { inner }
    }

    /// Joins the task, returning the result of join (`Result<R, JoinError>`).
    pub async fn join(mut self) -> Result<R, JoinError> {
        self.inner
            .join_next()
            .await
            .expect("`SpawnedTask` instance always contains exactly 1 task")
    }

    /// Joins the task and unwinds the panic if it happens.
    pub async fn join_unwind(self) -> Result<R, JoinError> {
        self.join().await.map_err(|e| {
            // `JoinError` can be caused either by panic or cancellation. We have to handle panics:
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            } else if e.is_cancelled() {
                log::warn!("SpawnedTask was polled during shutdown");
                e
            } else {
                unreachable!("SpawnedTask was cancelled unexpectedly");
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        future::{pending, Pending},
        sync::{Arc, Mutex},
    };

    use tokio::runtime::Runtime;

    #[tokio::test]
    async fn runtime_shutdown() {
        // capture the panic message
        let panic_msg = Arc::new(Mutex::new(None));
        let captured_panic_msg = Arc::clone(&panic_msg);
        std::panic::set_hook(Box::new(move |e| {
            let mut guard = captured_panic_msg.lock().unwrap();
            *guard = Some(e.to_string());
        }));

        for _ in 0..30 {
            let rt = Runtime::new().unwrap();
            let join = rt.spawn(async {
                let task = SpawnedTask::spawn(async {
                    let fut: Pending<()> = pending();
                    fut.await;
                    unreachable!("should never return");
                });
                let _ = task.join_unwind().await;
            });

            // caller shutdown their DF runtime (e.g. timeout, error in caller, etc)
            rt.shutdown_background();

            // race condition
            // poll occurs during shutdown (buffered stream poll calls, etc)
            let _ = join.await;
        }

        // demonstrate that we hit the unreachable code
        let maybe_panic = panic_msg.lock().unwrap().clone();
        assert_eq!(
            maybe_panic, None,
            "should not have rt thread panic, instead found {:?}",
            maybe_panic
        );
    }
}
