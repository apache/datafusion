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

//! Object store implementation used for testing

use crate::tracing::asserting_tracer::assert_traceability;
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

/// Returns an `ObjectStore` that asserts it can trace its calls back to the root tokio task.
pub fn traceable_object_store(
    object_store: Arc<dyn ObjectStore>,
) -> Arc<dyn ObjectStore> {
    Arc::new(TraceableObjectStore::new(object_store))
}

/// An object store that asserts it can trace all its calls back to the root tokio task.
#[derive(Debug)]
struct TraceableObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl TraceableObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl Display for TraceableObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

/// All trait methods are forwarded to the inner object store,
/// after asserting they can trace their calls back to the root tokio task.
#[async_trait::async_trait]
impl ObjectStore for TraceableObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        assert_traceability().await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        assert_traceability().await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        assert_traceability().await;
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        assert_traceability().await;
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        assert_traceability().await;
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        futures::executor::block_on(assert_traceability());
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        assert_traceability().await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        assert_traceability().await;
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &Path,
        to: &Path,
    ) -> object_store::Result<()> {
        assert_traceability().await;
        self.inner.copy_if_not_exists(from, to).await
    }
}
