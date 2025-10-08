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

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::execution::object_store::ObjectStoreRegistry;
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use url::Url;

/// Wrapped [`ObjectStore`] instances that record information for reporting on the usage of the
/// inner [`ObjectStore`]
#[derive(Debug)]
struct InstrumentedObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl InstrumentedObjectStore {
    /// Returns a new [`InstrumentedObjectStore`] that wraps the provided [`ObjectStore`]
    fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner: object_store,
        }
    }
}

impl fmt::Display for InstrumentedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Instrumented Object Store: {}", self.inner)
    }
}

#[async_trait]
impl ObjectStore for InstrumentedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner.head(location).await
    }
}

/// Provides access to [`InstrumentedObjectStore`] instances that record requests for reporting
#[derive(Debug)]
pub struct InstrumentedObjectStoreRegistry {
    inner: Arc<dyn ObjectStoreRegistry>,
}

impl InstrumentedObjectStoreRegistry {
    /// Returns a new [`InstrumentedObjectStoreRegistry`] that wraps the provided
    /// [`ObjectStoreRegistry`]
    pub fn new(registry: Arc<dyn ObjectStoreRegistry>) -> Self {
        Self { inner: registry }
    }
}

impl ObjectStoreRegistry for InstrumentedObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let instrumented = Arc::new(InstrumentedObjectStore::new(store));
        self.inner.register_store(url, instrumented)
    }

    fn get_store(&self, url: &Url) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        self.inner.get_store(url)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::object_store::DefaultObjectStoreRegistry;

    use super::*;

    #[test]
    fn instrumented_registry() {
        let reg = Arc::new(InstrumentedObjectStoreRegistry::new(Arc::new(
            DefaultObjectStoreRegistry::new(),
        )));
        let store = object_store::memory::InMemory::new();

        let url = "mem://test".parse().unwrap();
        let registered = reg.register_store(&url, Arc::new(store));
        assert!(registered.is_none());

        let fetched = reg.get_store(&url);
        assert!(fetched.is_ok())
    }
}
