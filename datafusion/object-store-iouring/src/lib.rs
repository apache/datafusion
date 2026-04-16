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

//! io-uring based [`ObjectStore`] implementation for DataFusion.
//!
//! Provides [`IoUringObjectStore`] which uses Linux's io_uring interface
//! for high-performance local file reads. A dedicated thread runs an
//! io_uring event loop, and read requests are dispatched via channels
//! from async [`ObjectStore`] methods.
//!
//! On non-Linux platforms, [`IoUringObjectStore`] delegates all operations
//! to [`LocalFileSystem`] without io_uring acceleration.
//!
//! # Performance
//!
//! The main benefit is **batched syscalls**: multiple byte-range reads
//! (e.g., Parquet column chunks) are submitted as a single
//! `io_uring_enter()` call instead of individual `pread()` calls.

#[cfg(target_os = "linux")]
mod uring;

use std::fmt;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
#[cfg(target_os = "linux")]
use object_store::{Attributes, GetResultPayload};
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
};

/// ObjectStore implementation that uses io_uring for local file reads on Linux.
///
/// Write, list, copy, and delete operations are delegated to [`LocalFileSystem`].
/// Read operations (`get_opts`, `get_ranges`) use a dedicated io_uring thread
/// for batched, zero-copy I/O.
///
/// # Example
///
/// ```no_run
/// use datafusion_object_store_iouring::IoUringObjectStore;
/// use object_store::ObjectStore;
///
/// let store = IoUringObjectStore::new();
/// ```
pub struct IoUringObjectStore {
    inner: Arc<LocalFileSystem>,
    root: PathBuf,
    #[cfg(target_os = "linux")]
    uring_sender: tokio::sync::mpsc::UnboundedSender<uring::IoCommand>,
}

impl IoUringObjectStore {
    /// Create a new `IoUringObjectStore` with root at `/`.
    pub fn new() -> Self {
        Self::new_with_root(PathBuf::from("/"))
    }

    /// Create a new `IoUringObjectStore` with the given root directory.
    pub fn new_with_root(root: PathBuf) -> Self {
        let inner = if root == PathBuf::from("/") {
            Arc::new(LocalFileSystem::new())
        } else {
            Arc::new(LocalFileSystem::new_with_prefix(&root).expect("valid root path"))
        };

        #[cfg(target_os = "linux")]
        {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            std::thread::Builder::new()
                .name("io-uring-worker".to_string())
                .spawn(move || uring::run_uring_loop(rx))
                .expect("failed to spawn io-uring thread");

            Self {
                inner,
                root,
                uring_sender: tx,
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            Self { inner, root }
        }
    }

    /// Resolve an object_store Path to an absolute filesystem path.
    #[cfg(target_os = "linux")]
    fn resolve_path(&self, location: &Path) -> PathBuf {
        self.root.join(location.as_ref())
    }
}

impl Default for IoUringObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for IoUringObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoUringObjectStore")
            .field("root", &self.root)
            .finish()
    }
}

impl fmt::Display for IoUringObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IoUringObjectStore({})", self.root.display())
    }
}

// ============================================================
// Linux: io_uring accelerated reads
// ============================================================

#[cfg(target_os = "linux")]
impl IoUringObjectStore {
    async fn get_opts_uring(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult> {
        // Get file metadata via the inner store
        let meta = self.inner.head(location).await?;
        let file_size = meta.size as u64;

        // Resolve the requested byte range
        let range = match &options.range {
            Some(r) => {
                r.as_range(file_size)
                    .map_err(|e| object_store::Error::Generic {
                        store: "IoUringObjectStore",
                        source: Box::new(e),
                    })?
            }
            None => 0..file_size,
        };

        if range.start == range.end {
            // Empty range — return an empty stream
            let stream = futures::stream::once(async { Ok(Bytes::new()) }).boxed();
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta,
                range: range.start as usize..range.end as usize,
                attributes: Attributes::new(),
            });
        }

        let fs_path = self.resolve_path(location);
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.uring_sender
            .send(uring::IoCommand::ReadRanges {
                path: fs_path,
                ranges: vec![range.clone()],
                response: tx,
            })
            .map_err(|_| object_store::Error::Generic {
                store: "IoUringObjectStore",
                source: "io-uring worker thread is gone".into(),
            })?;

        let mut results = rx.await.map_err(|_| object_store::Error::Generic {
            store: "IoUringObjectStore",
            source: "io-uring response channel dropped".into(),
        })??;

        let bytes = results.remove(0);
        let stream = futures::stream::once(async { Ok(bytes) }).boxed();

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range: range.start as usize..range.end as usize,
            attributes: Attributes::new(),
        })
    }

    async fn get_ranges_uring(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(vec![]);
        }

        let fs_path = self.resolve_path(location);
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.uring_sender
            .send(uring::IoCommand::ReadRanges {
                path: fs_path,
                ranges: ranges.to_vec(),
                response: tx,
            })
            .map_err(|_| object_store::Error::Generic {
                store: "IoUringObjectStore",
                source: "io-uring worker thread is gone".into(),
            })?;

        rx.await.map_err(|_| object_store::Error::Generic {
            store: "IoUringObjectStore",
            source: "io-uring response channel dropped".into(),
        })?
    }
}

// ============================================================
// ObjectStore trait implementation
// ============================================================

#[async_trait]
impl ObjectStore for IoUringObjectStore {
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
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // Head-only requests don't need io_uring
        if options.head {
            return self.inner.get_opts(location, options).await;
        }

        #[cfg(target_os = "linux")]
        {
            return self.get_opts_uring(location, options).await;
        }

        #[cfg(not(target_os = "linux"))]
        {
            self.inner.get_opts(location, options).await
        }
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>> {
        #[cfg(target_os = "linux")]
        {
            return self.get_ranges_uring(location, ranges).await;
        }

        #[cfg(not(target_os = "linux"))]
        {
            self.inner.get_ranges(location, ranges).await
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStore;

    #[tokio::test]
    async fn test_put_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let store = IoUringObjectStore::new_with_root(dir.path().to_path_buf());

        let path = Path::from("test/data.txt");
        let payload = PutPayload::from_static(b"hello io_uring");
        store.put(&path, payload).await.unwrap();

        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"hello io_uring");
    }

    #[tokio::test]
    async fn test_get_range() {
        let dir = tempfile::tempdir().unwrap();
        let store = IoUringObjectStore::new_with_root(dir.path().to_path_buf());

        let path = Path::from("test/range.txt");
        let payload = PutPayload::from_static(b"0123456789");
        store.put(&path, payload).await.unwrap();

        let bytes = store.get_range(&path, 2..5).await.unwrap();
        assert_eq!(bytes.as_ref(), b"234");
    }

    #[tokio::test]
    async fn test_get_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let store = IoUringObjectStore::new_with_root(dir.path().to_path_buf());

        let path = Path::from("test/ranges.txt");
        let payload = PutPayload::from_static(b"0123456789");
        store.put(&path, payload).await.unwrap();

        let ranges = vec![0..3, 5..8];
        let results = store.get_ranges(&path, &ranges).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref(), b"012");
        assert_eq!(results[1].as_ref(), b"567");
    }

    #[tokio::test]
    async fn test_head() {
        let dir = tempfile::tempdir().unwrap();
        let store = IoUringObjectStore::new_with_root(dir.path().to_path_buf());

        let path = Path::from("test/head.txt");
        let payload = PutPayload::from_static(b"hello");
        store.put(&path, payload).await.unwrap();

        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.size, 5);
    }

    #[tokio::test]
    async fn test_list() {
        use futures::TryStreamExt;

        let dir = tempfile::tempdir().unwrap();
        let store = IoUringObjectStore::new_with_root(dir.path().to_path_buf());

        let path1 = Path::from("prefix/a.txt");
        let path2 = Path::from("prefix/b.txt");
        store
            .put(&path1, PutPayload::from_static(b"a"))
            .await
            .unwrap();
        store
            .put(&path2, PutPayload::from_static(b"b"))
            .await
            .unwrap();

        let prefix = Path::from("prefix");
        let entries: Vec<_> = store.list(Some(&prefix)).try_collect().await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_empty_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let store = IoUringObjectStore::new_with_root(dir.path().to_path_buf());

        let path = Path::from("test/empty.txt");
        let payload = PutPayload::from_static(b"data");
        store.put(&path, payload).await.unwrap();

        let results = store.get_ranges(&path, &[]).await.unwrap();
        assert!(results.is_empty());
    }
}
