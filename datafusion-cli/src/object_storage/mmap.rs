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

//! Memory-mapped [`ObjectStore`] for local files.

// ObjectStoreError is 72+ bytes, unavoidable when implementing ObjectStore
#![allow(clippy::result_large_err)]
//!
//! [`MmapObjectStore`] maps local files into memory for zero copy reads.
//! Files are mapped on first access and cached to avoid blocking I/O.
//!
//! Writes and metadata operations use [`LocalFileSystem`] and clear the cache

use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use memmap2::Mmap;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, Error as ObjectStoreError, GetOptions, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use parking_lot::RwLock;

/// An [`ObjectStore`] implementation that memory-maps local files for
/// zero-copy reads.
pub struct MmapObjectStore {
    local: LocalFileSystem,
    /// Cache of memory mapped files.
    cache: Arc<RwLock<HashMap<PathBuf, Bytes>>>,
}

impl MmapObjectStore {
    /// Creates a new `MmapObjectStore` rooted at the filesystem root (`/`)
    pub fn new() -> Self {
        Self {
            local: LocalFileSystem::new(),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Converts an `object_store::Path` to an absolute filesystem path.
    fn to_fs_path(location: &Path) -> PathBuf {
        PathBuf::from(format!("/{}", location.as_ref()))
    }

    /// Returns the memory-mapped `Bytes` for `location`.
    fn get_mmap(&self, location: &Path) -> ObjectStoreResult<Bytes> {
        let fs_path = Self::to_fs_path(location);

        {
            let cache = self.cache.read();
            if let Some(bytes) = cache.get(&fs_path) {
                return Ok(bytes.clone());
            }
        }

        let file = std::fs::File::open(&fs_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                ObjectStoreError::NotFound {
                    path: location.to_string(),
                    source: Box::new(e),
                }
            } else {
                ObjectStoreError::Generic {
                    store: "MmapObjectStore",
                    source: Box::new(e),
                }
            }
        })?;

        let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);

        let bytes = if file_len == 0 {
            Bytes::new()
        } else {
            // SAFETY: In the CLI context, files are static datasets so they won't be truncated
            let mmap = unsafe {
                Mmap::map(&file).map_err(|e| ObjectStoreError::Generic {
                    store: "MmapObjectStore",
                    source: Box::new(e),
                })?
            };
            Bytes::from_owner(mmap)
        };

        let mut cache = self.cache.write();
        Ok(cache.entry(fs_path).or_insert(bytes).clone())
    }

    /// Evicts cache entry for the location
    fn invalidate(&self, location: &Path) {
        self.cache.write().remove(&Self::to_fs_path(location));
    }

    /// Validates that range lies within [0, size)
    fn check_range(
        range: &Range<usize>,
        size: usize,
        location: &Path,
    ) -> ObjectStoreResult<()> {
        if range.end > size {
            return Err(ObjectStoreError::Generic {
                store: "MmapObjectStore",
                source: format!(
                    "requested range {}..{} out of bounds for \
                     file '{}' of size {size}",
                    range.start, range.end, location,
                )
                .into(),
            });
        }
        Ok(())
    }
}

impl Default for MmapObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for MmapObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MmapObjectStore")
    }
}

impl Debug for MmapObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapObjectStore")
            .field("local", &self.local)
            .field("cache_entries", &self.cache.read().len())
            .finish()
    }
}

#[async_trait]
impl ObjectStore for MmapObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let result = self.local.put_opts(location, payload, opts).await;
        if result.is_ok() {
            self.invalidate(location);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.invalidate(location);
        self.local.put_multipart_opts(location, opts).await
    }

    /// Handles GET requests
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let has_conditionals = options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some();

        if has_conditionals || options.head {
            return self.local.get_opts(location, options).await;
        }

        match &options.range {
            None => {
                let bytes = self.get_mmap(location)?;
                let meta = self.local.head(location).await?;
                let size = bytes.len();
                let size_u64 = size as u64;
                Ok(GetResult {
                    payload: GetResultPayload::Stream(Box::pin(stream::once(
                        async move { Ok(bytes) },
                    ))),
                    meta,
                    range: 0..size_u64,
                    attributes: Attributes::default(),
                })
            }
            Some(get_range) => {
                let bytes = self.get_mmap(location)?;
                let meta = self.local.head(location).await?;
                let size = bytes.len();

                use object_store::GetRange;
                let range: Range<usize> = match get_range {
                    GetRange::Bounded(r) => {
                        let start = usize::try_from(r.start).unwrap_or(0);
                        let end = usize::try_from(r.end).unwrap_or(size);
                        start..end
                    }
                    GetRange::Offset(o) => {
                        let start = usize::try_from(*o).unwrap_or(0);
                        start..size
                    }
                    GetRange::Suffix(n) => {
                        let n_usize = usize::try_from(*n).unwrap_or(size);
                        size.saturating_sub(n_usize)..size
                    }
                };

                Self::check_range(&range, size, location)?;
                let sliced = bytes.slice(range.clone());
                let result_range = (range.start as u64)..(range.end as u64);

                Ok(GetResult {
                    payload: GetResultPayload::Stream(Box::pin(stream::once(
                        async move { Ok(sliced) },
                    ))),
                    meta,
                    range: result_range,
                    attributes: Attributes::default(),
                })
            }
        }
    }

    /// Returns multiple byte slices
    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        let bytes = self.get_mmap(location)?;
        let size = bytes.len();
        ranges
            .iter()
            .map(|range| {
                let start = usize::try_from(range.start).unwrap_or(0);
                let end = usize::try_from(range.end).unwrap_or(size);
                let usize_range = start..end;
                Self::check_range(&usize_range, size, location)?;
                Ok(bytes.slice(usize_range))
            })
            .collect()
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        let cache = Arc::clone(&self.cache);
        let local_stream = self.local.delete_stream(locations);

        Box::pin(local_stream.map(move |result| {
            if let Ok(ref path) = result {
                cache.write().remove(&Self::to_fs_path(path));
            }
            result
        }))
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.local.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<ListResult> {
        self.local.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        let result = self.local.copy_opts(from, to, options).await;
        self.invalidate(from);
        self.invalidate(to);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Creates a temporary file with the given content and returns an
    /// `object_store::Path` that resolves to it.
    fn make_temp_file(content: &[u8]) -> (NamedTempFile, Path) {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content).unwrap();
        f.flush().unwrap();
        let abs = f.path().to_string_lossy().into_owned();
        // object_store::Path strips the leading '/'.
        let location = Path::from(abs.trim_start_matches('/'));
        (f, location)
    }

    /// Returns a fresh `MmapObjectStore` for use in tests.
    fn store() -> MmapObjectStore {
        MmapObjectStore::new()
    }

    #[tokio::test]
    async fn test_get_range() {
        let content = b"Hello, datafusion!";
        let (_tmp, location) = make_temp_file(content);
        let store = store();

        let bytes = store.get_range(&location, 0..5).await.unwrap();
        assert_eq!(bytes.as_ref(), b"Hello");

        let bytes = store.get_range(&location, 7..17).await.unwrap();
        assert_eq!(bytes.as_ref(), b"datafusion");
    }

    #[tokio::test]
    async fn test_get_ranges() {
        let content = b"Hello, datafusion!";
        let (_tmp, location) = make_temp_file(content);
        let store = store();

        let ranges = vec![0..5, 7..17];
        let results = store.get_ranges(&location, &ranges).await.unwrap();
        assert_eq!(results[0].as_ref(), b"Hello");
        assert_eq!(results[1].as_ref(), b"datafusion");
    }

    #[tokio::test]
    async fn test_get_full_file() {
        let content = b"full file contents";
        let (_tmp, location) = make_temp_file(content);
        let store = store();

        let result = store.get(&location).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), content);
    }

    #[tokio::test]
    async fn test_get_opts_no_range() {
        let content = b"options test data";
        let (_tmp, location) = make_temp_file(content);
        let store = store();

        let result = store
            .get_opts(&location, GetOptions::default())
            .await
            .unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), content);
    }

    #[tokio::test]
    async fn test_get_opts_bounded_range() {
        let content = b"ranged get test";
        let (_tmp, location) = make_temp_file(content);
        let store = store();

        let opts = GetOptions {
            range: Some(object_store::GetRange::Bounded(0..6)),
            ..Default::default()
        };
        let result = store.get_opts(&location, opts).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"ranged");
    }

    #[tokio::test]
    async fn test_head() {
        let content = b"metadata check";
        let (_tmp, location) = make_temp_file(content);
        let store = store();

        let meta = store.head(&location).await.unwrap();
        assert_eq!(meta.size, content.len() as u64);
    }

    #[tokio::test]
    async fn test_cache_hit() {
        let content = b"cache hit test";
        let (_tmp, location) = make_temp_file(content);
        let store = store();

        // First access populates the cache.
        let _b1 = store.get_range(&location, 0..5).await.unwrap();
        assert_eq!(store.cache.read().len(), 1);

        // Second access should reuse the cached entry (still 1 entry).
        let _b2 = store.get_range(&location, 5..9).await.unwrap();
        assert_eq!(store.cache.read().len(), 1);
    }

    #[tokio::test]
    async fn test_empty_file() {
        let (_tmp, location) = make_temp_file(b"");
        let store = store();

        let bytes = store.get_range(&location, 0..0).await.unwrap();
        assert!(bytes.is_empty());

        let result = store.get(&location).await.unwrap();
        assert_eq!(result.range, 0..0);
    }

    #[tokio::test]
    async fn test_out_of_bounds_range() {
        let content = b"short";
        let (_tmp, location) = make_temp_file(content);
        let store = store();

        let err = store.get_range(&location, 0..100).await.unwrap_err();
        assert!(
            err.to_string().contains("out of bounds"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_not_found() {
        let store = store();
        let location = Path::from("nonexistent/path/file.parquet");

        let err = store.get_range(&location, 0..5).await.unwrap_err();
        assert!(
            matches!(err, ObjectStoreError::NotFound { .. }),
            "expected NotFound, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_delete_invalidates_cache() {
        use futures::StreamExt;

        let content = b"cache invalidation test";
        let (_tmp, location) = make_temp_file(content);
        let store = store();

        // Populate the cache
        let _bytes = store.get_range(&location, 0..5).await.unwrap();
        assert_eq!(store.cache.read().len(), 1);

        // Delete and verify cache is invalidated
        let paths = vec![Ok(location.clone())];
        let stream = stream::iter(paths);
        let mut delete_stream = store.delete_stream(Box::pin(stream));

        // Consume the stream to trigger cache invalidation
        while let Some(result) = delete_stream.next().await {
            let _ = result;
        }

        assert_eq!(store.cache.read().len(), 0);
    }

    #[tokio::test]
    async fn test_put_invalidates_cache() {
        use object_store::PutPayload;

        let content = b"initial content";
        let (_tmp, location) = make_temp_file(content);
        let store = store();

        // Populate the cache
        let bytes = store.get_range(&location, 0..7).await.unwrap();
        assert_eq!(bytes.as_ref(), b"initial");
        assert_eq!(store.cache.read().len(), 1);

        // Overwrite the file
        let new_content = b"updated content for testing";
        let payload = PutPayload::from_bytes(new_content.as_slice().into());
        let _ = store.put(&location, payload).await;

        // Cache should be invalidated
        let cache_size_after = store.cache.read().len();
        assert_eq!(cache_size_after, 0, "put should invalidate cache entry");
    }
}
