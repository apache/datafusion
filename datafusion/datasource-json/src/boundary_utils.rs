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

use bytes::Bytes;
use datafusion_common::{DataFusionError, Result};
use object_store::{ObjectStore, path::Path};
use std::sync::Arc;

pub const DEFAULT_BOUNDARY_WINDOW: usize = 4096; // 4KB

/// Fetch bytes for [start, end) and align boundaries in memory.
///
/// Start alignment:
/// - If start == 0, use bytes as-is.
/// - Else, check byte at start-1 (included in fetch). If it is the terminator,
///   start from `start`. Otherwise scan forward in memory for the first terminator
///   and start after it. If no terminator exists in the fetched range, return None.
///
/// End alignment:
/// - If the last byte is not the terminator and end < file_size, fetch forward in
///   chunks until the terminator is found or EOF is reached.
pub async fn get_aligned_bytes(
    store: &Arc<dyn ObjectStore>,
    location: &Path,
    start: usize,
    end: usize,
    file_size: usize,
    terminator: u8,
    scan_window: usize,
) -> Result<Option<Bytes>> {
    if start >= end || start >= file_size {
        return Ok(None);
    }

    let fetch_start = start.saturating_sub(1);
    let fetch_end = std::cmp::min(end, file_size);
    let bytes = store
        .get_range(location, (fetch_start as u64)..(fetch_end as u64))
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    if bytes.is_empty() {
        return Ok(None);
    }

    let data_offset = if start == 0 {
        0
    } else if bytes[0] == terminator {
        1
    } else {
        match bytes[1..].iter().position(|&b| b == terminator) {
            Some(pos) => pos + 2,
            None => return Ok(None),
        }
    };

    if data_offset >= bytes.len() {
        return Ok(None);
    }

    let data = bytes.slice(data_offset..);

    // Fast path: if already aligned, return zero-copy
    if fetch_end >= file_size || data.last() == Some(&terminator) {
        return Ok(Some(data));
    }

    // Slow path: need to extend, preallocate capacity
    let mut buffer = Vec::with_capacity(data.len() + scan_window);
    buffer.extend_from_slice(&data);
    let mut cursor = fetch_end as u64;

    while cursor < file_size as u64 {
        let chunk_end = std::cmp::min(cursor + scan_window as u64, file_size as u64);
        let chunk = store
            .get_range(location, cursor..chunk_end)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if chunk.is_empty() {
            break;
        }

        if let Some(pos) = chunk.iter().position(|&b| b == terminator) {
            buffer.extend_from_slice(&chunk[..=pos]);
            return Ok(Some(Bytes::from(buffer)));
        }

        buffer.extend_from_slice(&chunk);
        cursor = chunk_end;
    }

    Ok(Some(Bytes::from(buffer)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use datafusion_datasource::{FileRange, PartitionedFile, calculate_range};
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::fmt;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[tokio::test]
    async fn test_get_aligned_bytes_start_at_beginning() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test.json");

        store
            .put(&path, "line1\nline2\nline3\n".into())
            .await
            .unwrap();

        let result = get_aligned_bytes(&store, &path, 0, 6, 18, b'\n', 4096)
            .await
            .unwrap();

        assert_eq!(result.unwrap().as_ref(), b"line1\n");
    }

    #[tokio::test]
    async fn test_get_aligned_bytes_start_aligned() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test.json");

        // "line1\nline2\nline3\n"
        // Position 6 is right after first \n
        store
            .put(&path, "line1\nline2\nline3\n".into())
            .await
            .unwrap();

        let result = get_aligned_bytes(&store, &path, 6, 12, 18, b'\n', 4096)
            .await
            .unwrap();

        assert_eq!(result.unwrap().as_ref(), b"line2\n");
    }

    #[tokio::test]
    async fn test_get_aligned_bytes_start_needs_alignment() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test.json");

        // "line1\nline2\nline3\n"
        // Position 8 is in the middle of "line2"
        store
            .put(&path, "line1\nline2\nline3\n".into())
            .await
            .unwrap();

        let result = get_aligned_bytes(&store, &path, 8, 18, 18, b'\n', 4096)
            .await
            .unwrap();

        assert_eq!(result.unwrap().as_ref(), b"line3\n");
    }

    #[tokio::test]
    async fn test_get_aligned_bytes_no_newline_in_range() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test.json");

        store.put(&path, "abcdefghij\n".into()).await.unwrap();

        let result = get_aligned_bytes(&store, &path, 2, 8, 11, b'\n', 4096)
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_aligned_bytes_extend_end() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test.json");

        // "line1\nline2\nline3\n"
        store
            .put(&path, "line1\nline2\nline3\n".into())
            .await
            .unwrap();

        let result = get_aligned_bytes(&store, &path, 0, 8, 18, b'\n', 2)
            .await
            .unwrap();

        assert_eq!(result.unwrap().as_ref(), b"line1\nline2\n");
    }

    #[tokio::test]
    async fn test_get_aligned_bytes_end_at_eof_without_newline() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("test.json");

        store.put(&path, "line1".into()).await.unwrap();

        let result = get_aligned_bytes(&store, &path, 0, 5, 5, b'\n', 4)
            .await
            .unwrap();

        assert_eq!(result.unwrap().as_ref(), b"line1");
    }

    #[derive(Debug)]
    struct CountingObjectStore {
        inner: Arc<dyn ObjectStore>,
        requested_bytes: AtomicU64,
        requested_calls: AtomicU64,
    }

    impl CountingObjectStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                requested_bytes: AtomicU64::new(0),
                requested_calls: AtomicU64::new(0),
            }
        }

        fn reset(&self) {
            self.requested_bytes.store(0, Ordering::Relaxed);
            self.requested_calls.store(0, Ordering::Relaxed);
        }

        fn requested_bytes(&self) -> u64 {
            self.requested_bytes.load(Ordering::Relaxed)
        }
    }

    impl fmt::Display for CountingObjectStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "CountingObjectStore({})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for CountingObjectStore {
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
            if let Some(range) = options.range.as_ref() {
                let requested = match range {
                    GetRange::Bounded(r) => r.end.saturating_sub(r.start),
                    GetRange::Offset(_) | GetRange::Suffix(_) => 0,
                };
                self.requested_bytes.fetch_add(requested, Ordering::Relaxed);
            }
            self.requested_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
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

    fn build_fixed_lines(line_len: usize, lines: usize) -> Bytes {
        let body_len = line_len.saturating_sub(1);
        let mut data = Vec::with_capacity(line_len * lines);
        for _ in 0..lines {
            data.extend(std::iter::repeat(b'a').take(body_len));
            data.push(b'\n');
        }
        Bytes::from(data)
    }

    #[tokio::test]
    async fn test_get_aligned_bytes_reduces_requested_bytes() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(CountingObjectStore::new(Arc::clone(&inner)));
        let store_dyn: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("amplification.json");

        let data = build_fixed_lines(128, 16_384);
        let file_size = data.len();
        inner.put(&path, data.into()).await.unwrap();

        let start = 1_000_003usize;
        let raw_end = start + 64_000;
        let end = (raw_end / 128).max(1) * 128;

        let object_meta = inner.head(&path).await.unwrap();
        let file = PartitionedFile {
            object_meta,
            partition_values: vec![],
            range: Some(FileRange {
                start: start as i64,
                end: end as i64,
            }),
            statistics: None,
            ordering: None,
            extensions: None,
            metadata_size_hint: None,
        };

        store.reset();
        let _ = calculate_range(&file, &store_dyn, None).await.unwrap();
        let old_bytes = store.requested_bytes();

        store.reset();
        let _ = get_aligned_bytes(
            &store_dyn,
            &path,
            start,
            end,
            file_size,
            b'\n',
            DEFAULT_BOUNDARY_WINDOW,
        )
        .await
        .unwrap();
        let new_bytes = store.requested_bytes();

        assert!(
            old_bytes >= new_bytes * 10,
            "expected old path to request significantly more bytes, old={old_bytes}, new={new_bytes}"
        );
    }
}
