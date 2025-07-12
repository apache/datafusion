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

use crate::execution::context::SessionState;
use crate::execution::session_state::SessionStateBuilder;
use crate::prelude::SessionContext;
use futures::stream::BoxStream;
use futures::FutureExt;
use object_store::{
    memory::InMemory, path::Path, Error, GetOptions, GetResult, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult,
};
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use tokio::{
    sync::Barrier,
    time::{timeout, Duration},
};
use url::Url;

/// Registers a test object store with the provided `ctx`
pub fn register_test_store(ctx: &SessionContext, files: &[(&str, u64)]) {
    let url = Url::parse("test://").unwrap();
    ctx.register_object_store(&url, make_test_store_and_state(files).0);
}

/// Create a test object store with the provided files
pub fn make_test_store_and_state(files: &[(&str, u64)]) -> (Arc<InMemory>, SessionState) {
    let memory = InMemory::new();

    for (name, size) in files {
        memory
            .put(&Path::from(*name), vec![0; *size as usize].into())
            .now_or_never()
            .unwrap()
            .unwrap();
    }

    (
        Arc::new(memory),
        SessionStateBuilder::new().with_default_features().build(),
    )
}

/// Helper method to fetch the file size and date at given path and create a `ObjectMeta`
pub fn local_unpartitioned_file(path: impl AsRef<std::path::Path>) -> ObjectMeta {
    let location = Path::from_filesystem_path(path.as_ref()).unwrap();
    let metadata = std::fs::metadata(path).expect("Local file metadata");
    ObjectMeta {
        location,
        last_modified: metadata.modified().map(chrono::DateTime::from).unwrap(),
        size: metadata.len(),
        e_tag: None,
        version: None,
    }
}

/// Blocks the object_store `head` call until `concurrency` number of calls are pending.
pub fn ensure_head_concurrency(
    object_store: Arc<dyn ObjectStore>,
    concurrency: usize,
) -> Arc<dyn ObjectStore> {
    Arc::new(BlockingObjectStore::new(object_store, concurrency))
}

/// An object store that “blocks” in its `head` call until an expected number of concurrent calls are reached.
#[derive(Debug)]
struct BlockingObjectStore {
    inner: Arc<dyn ObjectStore>,
    barrier: Arc<Barrier>,
}

impl BlockingObjectStore {
    const NAME: &'static str = "BlockingObjectStore";
    fn new(inner: Arc<dyn ObjectStore>, expected_concurrency: usize) -> Self {
        Self {
            inner,
            barrier: Arc::new(Barrier::new(expected_concurrency)),
        }
    }
}

impl Display for BlockingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

/// All trait methods are forwarded to the inner object store, except for
/// the `head` method which waits until the expected number of concurrent calls is reached.
#[async_trait::async_trait]
impl ObjectStore for BlockingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        println!(
            "{} received head call for {location}",
            BlockingObjectStore::NAME
        );
        // Wait until the expected number of concurrent calls is reached, but timeout after 1 second to avoid hanging failing tests.
        let wait_result = timeout(Duration::from_secs(1), self.barrier.wait()).await;
        match wait_result {
            Ok(_) => println!(
                "{} barrier reached for {location}",
                BlockingObjectStore::NAME
            ),
            Err(_) => {
                let error_message = format!(
                    "{} barrier wait timed out for {location}",
                    BlockingObjectStore::NAME
                );
                log::error!("{error_message}");
                return Err(Error::Generic {
                    store: BlockingObjectStore::NAME,
                    source: error_message.into(),
                });
            }
        }
        // Forward the call to the inner object store.
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &Path,
        to: &Path,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
