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

use std::sync::Arc;

use crate::dedicated_executor::{DedicatedExecutor, JobError};
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
};

/// 'ObjectStore' that wraps an inner `ObjectStore` and wraps all the underlying
/// methods with [`DedicatedExecutor::spawn_io`] so that they are run on the Tokio Runtime
/// dedicated to doing IO.
///
///
///
#[derive(Debug)]
pub struct IoObjectStore {
    //executor: DedicatedExecutor,
    inner: Arc<dyn ObjectStore>,
}

impl IoObjectStore {
    pub fn new(_executor: DedicatedExecutor, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner: object_store,
        }
    }
}

impl std::fmt::Display for IoObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "IoObjectStore")
    }
}

fn convert_error(e: JobError) -> object_store::Error {
    object_store::Error::Generic {
        store: "IoObjectStore",
        source: Box::new(e),
    }
}

#[async_trait]
impl ObjectStore for IoObjectStore {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let location = location.clone();
        let store = Arc::clone(&self.inner);
        DedicatedExecutor::spawn_io(
            async move { store.get_opts(&location, options).await },
        )
        .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let from = from.clone();
        let to = to.clone();
        let store = Arc::clone(&self.inner);
        DedicatedExecutor::spawn_io(async move { store.copy(&from, &to).await }).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let from = from.clone();
        let to = to.clone();
        let store = Arc::clone(&self.inner);
        DedicatedExecutor::spawn_io(async move { store.copy(&from, &to).await }).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let location = location.clone();
        let store = Arc::clone(&self.inner);
        DedicatedExecutor::spawn_io(async move { store.delete(&location).await }).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        // run the inner list on the dedicated executor
        let inner_stream = self.inner.list(prefix);

        inner_stream
        //self.executor.run_stream(inner_stream, convert_error)
        //   .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = prefix.cloned();
        let store = Arc::clone(&self.inner);
        DedicatedExecutor::spawn_io(async move {
            store.list_with_delimiter(prefix.as_ref()).await
        })
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        let location = location.clone();
        let store = Arc::clone(&self.inner);
        DedicatedExecutor::spawn_io(async move {
            store.put_multipart_opts(&location, opts).await
        })
        .await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let location = location.clone();
        let store = Arc::clone(&self.inner);
        DedicatedExecutor::spawn_io(async move {
            store.put_opts(&location, payload, opts).await
        })
        .await
    }
}
