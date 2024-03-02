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

    pub async fn join(mut self) -> Result<R, JoinError> {
        self.inner
            .join_next()
            .await
            .expect("`SpawnedTask` instance always contains exactly 1 task")
    }
}
