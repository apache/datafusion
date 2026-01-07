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
    use object_store::memory::InMemory;

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
}
