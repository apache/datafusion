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

//! Native storage fixtures for listing tests.
use crate::execution::{context::SessionState, session_state::SessionStateBuilder};
use crate::prelude::SessionContext;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion_storage::{
    DirectoryListing, Error, FileAccessContext, FileInfo, FileReader, ReadRange, Storage,
    StorageBinding, StorageUrl, path::Path,
};
use futures::{StreamExt, stream::BoxStream};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Barrier;

/// Register a read-only collection of zero-filled files.
pub fn register_test_store(ctx: &SessionContext, files: &[(&str, u64)]) {
    let storage = make_test_store_and_state(files).0;
    ctx.register_storage(storage.url().as_ref(), Arc::clone(storage.storage()))
        .unwrap();
}

/// Create a read-only collection and a session for listing tests.
pub fn make_test_store_and_state(
    files: &[(&str, u64)],
) -> (Arc<StorageBinding>, SessionState) {
    let files = files
        .iter()
        .map(|(path, size)| (Path::from(*path), *size))
        .collect();
    let backend = Arc::new(TestFiles { files });
    (
        Arc::new(StorageBinding::new(
            StorageUrl::parse("test://").unwrap(),
            backend,
        )),
        SessionStateBuilder::new().with_default_features().build(),
    )
}

/// Metadata of an existing local file.
pub fn local_unpartitioned_file(path: impl AsRef<std::path::Path>) -> FileInfo {
    let location = Path::from_filesystem_path(path.as_ref()).unwrap();
    let metadata = std::fs::metadata(path).expect("local file metadata");
    FileInfo {
        location,
        last_modified: metadata.modified().map(chrono::DateTime::from).unwrap(),
        size: metadata.len(),
        e_tag: None,
        version: None,
    }
}
#[derive(Debug)]
struct TestFiles {
    files: BTreeMap<Path, u64>,
}
impl TestFiles {
    fn descriptor(&self, path: &Path) -> datafusion_storage::Result<FileInfo> {
        let size = *self
            .files
            .get(path)
            .ok_or_else(|| Error::NotFound(path.to_string()))?;
        Ok(FileInfo::new(path.clone(), size))
    }
}
#[async_trait]
impl Storage for TestFiles {
    async fn open(
        &self,
        path: &Path,
        _: FileAccessContext,
    ) -> datafusion_storage::Result<Arc<dyn FileReader>> {
        self.descriptor(path)?;
        Ok(Arc::new(ZeroReader))
    }

    async fn stat(
        &self,
        path: &Path,
        _: &FileAccessContext,
    ) -> datafusion_storage::Result<FileInfo> {
        self.descriptor(path)
    }
    fn list(
        &self,
        prefix: &Path,
        _: FileAccessContext,
    ) -> BoxStream<'_, datafusion_storage::Result<FileInfo>> {
        let files = self
            .files
            .keys()
            .filter(|p| p.prefix_match(prefix).is_some())
            .map(|path| self.descriptor(path))
            .collect::<Vec<_>>();
        futures::stream::iter(files).boxed()
    }
    async fn list_with_delimiter(
        &self,
        prefix: &Path,
        _: &FileAccessContext,
    ) -> datafusion_storage::Result<DirectoryListing> {
        let mut result = DirectoryListing::default();
        let mut directories = std::collections::BTreeSet::new();
        for path in self.files.keys() {
            let Some(mut parts) = path.prefix_match(prefix) else {
                continue;
            };
            let Some(first) = parts.next() else { continue };
            if parts.next().is_some() {
                directories.insert(prefix.clone().join(first));
            } else {
                result.files.push(self.descriptor(path)?);
            }
        }
        result.directories = directories.into_iter().collect();
        Ok(result)
    }
}
#[derive(Debug)]
struct ZeroReader;
#[async_trait]
impl FileReader for ZeroReader {
    async fn read_range(&self, range: ReadRange) -> datafusion_storage::Result<Bytes> {
        let length = match range {
            ReadRange::Bounded(r) => r.end - r.start,
            ReadRange::Suffix(n) => n,
        };
        Ok(Bytes::from(vec![0; length as usize]))
    }

    async fn read_ranges(
        &self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> datafusion_storage::Result<Vec<Bytes>> {
        Ok(ranges
            .into_iter()
            .map(|r| Bytes::from(vec![0; (r.end - r.start) as usize]))
            .collect())
    }
    fn stream(
        self: Arc<Self>,
        range: Option<ReadRange>,
    ) -> BoxStream<'static, datafusion_storage::Result<Bytes>> {
        futures::stream::once(async move {
            self.read_range(range.unwrap_or(ReadRange::Bounded(0..0)))
                .await
        })
        .boxed()
    }
}

/// Delay stat calls until the expected concurrency is reached.
pub fn ensure_head_concurrency(
    inner: Arc<StorageBinding>,
    concurrency: usize,
) -> Arc<StorageBinding> {
    let url = inner.url().clone();
    let backend = Arc::new(BlockingDiscovery {
        inner,
        barrier: Barrier::new(concurrency),
    });
    Arc::new(StorageBinding::new(url, backend))
}
#[derive(Debug)]
struct BlockingDiscovery {
    inner: Arc<StorageBinding>,
    barrier: Barrier,
}
#[async_trait]
impl Storage for BlockingDiscovery {
    async fn open(
        &self,
        path: &Path,
        context: FileAccessContext,
    ) -> datafusion_storage::Result<Arc<dyn FileReader>> {
        self.inner.storage().open(path, context).await
    }

    async fn stat(
        &self,
        path: &Path,
        context: &FileAccessContext,
    ) -> datafusion_storage::Result<FileInfo> {
        tokio::time::timeout(std::time::Duration::from_secs(1), self.barrier.wait())
            .await
            .map_err(|_| {
                Error::InvalidInput("stat concurrency barrier timed out".into())
            })?;
        self.inner.storage().stat(path, context).await
    }
    fn list(
        &self,
        prefix: &Path,
        context: FileAccessContext,
    ) -> BoxStream<'_, datafusion_storage::Result<FileInfo>> {
        self.inner.storage().list(prefix, context)
    }
}
