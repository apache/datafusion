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

//! Local [`ObjectStore`] that performs reads via a shared
//! [`TokioUringPool`]. Writes, list, copy, and delete delegate to
//! [`LocalFileSystem`].

#![cfg(target_os = "linux")]

use std::fmt;
use std::ops::Range;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result,
};

use super::tokio_uring_pool::TokioUringPool;

pub struct TokioUringObjectStore {
    inner: Arc<LocalFileSystem>,
    pool: Arc<TokioUringPool>,
}

impl TokioUringObjectStore {
    pub fn new(pool: Arc<TokioUringPool>) -> Self {
        Self {
            inner: Arc::new(LocalFileSystem::new()),
            pool,
        }
    }

    async fn read_ranges_uring(
        &self,
        path: PathBuf,
        ranges: Vec<Range<u64>>,
    ) -> Result<Vec<Bytes>> {
        // The future contains a `!Send` `tokio_uring::fs::File`; we build
        // it inside the worker by shipping a closure across.
        let job = self
            .pool
            .spawn(move || async move { read_ranges_inner(&path, &ranges).await });
        job.await.map_err(|e| object_store::Error::Generic {
            store: "TokioUringObjectStore",
            source: Box::new(e),
        })?
    }
}

impl fmt::Debug for TokioUringObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokioUringObjectStore")
            .field("workers", &self.pool.worker_count())
            .finish()
    }
}

impl fmt::Display for TokioUringObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TokioUringObjectStore(workers={})",
            self.pool.worker_count()
        )
    }
}

/// Open the file on the worker's runtime, then read every range
/// concurrently via `tokio_uring::spawn` on the same ring.
async fn read_ranges_inner(path: &FsPath, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
    if ranges.is_empty() {
        return Ok(Vec::new());
    }

    let file = tokio_uring::fs::File::open(path.to_path_buf())
        .await
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                object_store::Error::NotFound {
                    path: path.display().to_string(),
                    source: e.into(),
                }
            } else {
                object_store::Error::Generic {
                    store: "TokioUringObjectStore",
                    source: e.into(),
                }
            }
        })?;
    let file = Arc::new(file);

    let mut handles = Vec::with_capacity(ranges.len());
    for r in ranges {
        let file = Arc::clone(&file);
        let range = r.clone();
        handles.push(tokio_uring::spawn(
            async move { read_one(file, range).await },
        ));
    }

    let mut out = Vec::with_capacity(ranges.len());
    for h in handles {
        out.push(h.await.map_err(|e| object_store::Error::Generic {
            store: "TokioUringObjectStore",
            source: Box::new(e),
        })??);
    }

    if let Ok(file) = Arc::try_unwrap(file) {
        let _ = file.close().await;
    }

    Ok(out)
}

async fn read_one(file: Arc<tokio_uring::fs::File>, range: Range<u64>) -> Result<Bytes> {
    let total = range.end.saturating_sub(range.start) as usize;
    let mut out: Vec<u8> = Vec::with_capacity(total);

    while out.len() < total {
        let remaining = total - out.len();
        let offset = range.start + out.len() as u64;
        // tokio-uring requires owned buffers: we hand the Vec to the
        // kernel and get it back alongside the result.
        let buf = vec![0u8; remaining];
        let (res, buf) = file.read_at(buf, offset).await;
        let n = res.map_err(|e| object_store::Error::Generic {
            store: "TokioUringObjectStore",
            source: e.into(),
        })?;
        if n == 0 {
            return Err(object_store::Error::Generic {
                store: "TokioUringObjectStore",
                source: format!("unexpected EOF reading {}..{}", range.start, range.end)
                    .into(),
            });
        }
        out.extend_from_slice(&buf[..n]);
    }

    Ok(Bytes::from(out))
}

#[async_trait]
impl ObjectStore for TokioUringObjectStore {
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
        if options.head {
            return self.inner.get_opts(location, options).await;
        }

        // Delegate a HEAD through the inner store so conditional options
        // (if_match, if_modified_since, …) are honored.
        let head_opts = GetOptions {
            head: true,
            range: None,
            ..options.clone()
        };
        let meta = self.inner.get_opts(location, head_opts).await?.meta;
        let file_size = meta.size;

        let range = match &options.range {
            Some(r) => {
                r.as_range(file_size)
                    .map_err(|e| object_store::Error::Generic {
                        store: "TokioUringObjectStore",
                        source: Box::new(e),
                    })?
            }
            None => 0..file_size,
        };

        if range.start == range.end {
            let stream = futures::stream::once(async { Ok(Bytes::new()) }).boxed();
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta,
                range,
                attributes: Attributes::new(),
            });
        }

        let fs_path = self.inner.path_to_filesystem(location)?;
        let mut results = self.read_ranges_uring(fs_path, vec![range.clone()]).await?;
        let bytes = results.remove(0);
        let stream = futures::stream::once(async { Ok(bytes) }).boxed();

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range,
            attributes: Attributes::new(),
        })
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        for r in ranges {
            if r.start > r.end {
                return Err(object_store::Error::Generic {
                    store: "TokioUringObjectStore",
                    source: format!("invalid range: start {} > end {}", r.start, r.end)
                        .into(),
                });
            }
        }
        let fs_path = self.inner.path_to_filesystem(location)?;
        self.read_ranges_uring(fs_path, ranges.to_vec()).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
