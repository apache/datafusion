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

//! An ObjectStore wrapper that counts simulated GET requests.
//!
//! For real object stores every `get`/`get_opts` call is one GET, and a
//! `get_ranges` call becomes one GET per *coalesced* range (ranges within
//! `OBJECT_STORE_COALESCE_DEFAULT` of each other are merged into one
//! request, following `object_store::coalesce_ranges`). This wrapper models
//! that: it counts `get_opts` as one request and `get_ranges` as its merged
//! range count, regardless of how the local filesystem actually serves them.
//!
//! Counters are process-global so benchmark runners can snapshot them around
//! each query.

use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

/// Ranges within this many bytes of one another count as a single coalesced
/// GET, matching `object_store::OBJECT_STORE_COALESCE_DEFAULT`.
const COALESCE_GAP: u64 = 1024 * 1024;

/// Simulated GET requests issued so far (process-global)
static GET_REQUESTS: AtomicUsize = AtomicUsize::new(0);
/// Bytes requested by GETs so far (process-global)
static GET_BYTES: AtomicU64 = AtomicU64::new(0);

/// A snapshot of the request counters
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestCounts {
    pub get_requests: usize,
    pub get_bytes: u64,
}

impl RequestCounts {
    /// Take a snapshot of the current process-global counters
    pub fn snapshot() -> Self {
        Self {
            get_requests: GET_REQUESTS.load(Ordering::Relaxed),
            get_bytes: GET_BYTES.load(Ordering::Relaxed),
        }
    }

    /// Counts accumulated since an earlier snapshot
    pub fn since(&self, earlier: &Self) -> Self {
        Self {
            get_requests: self.get_requests - earlier.get_requests,
            get_bytes: self.get_bytes - earlier.get_bytes,
        }
    }
}

/// Number of coalesced requests `object_store` would issue for `ranges`
fn merged_request_count(ranges: &[Range<u64>]) -> usize {
    if ranges.is_empty() {
        return 0;
    }
    let mut sorted = ranges.to_vec();
    sorted.sort_unstable_by_key(|range| range.start);
    let mut count = 1;
    let mut range_end = sorted[0].end;
    for range in &sorted[1..] {
        if range
            .start
            .checked_sub(range_end)
            .map(|delta| delta <= COALESCE_GAP)
            .unwrap_or(true)
        {
            range_end = range_end.max(range.end);
        } else {
            count += 1;
            range_end = range.end;
        }
    }
    count
}

/// An ObjectStore wrapper counting simulated GET requests
#[derive(Debug)]
pub struct CountingObjectStore<T: ObjectStore> {
    inner: T,
}

impl<T: ObjectStore> CountingObjectStore<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: ObjectStore> fmt::Display for CountingObjectStore<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for CountingObjectStore<T> {
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
        GET_REQUESTS.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.get_opts(location, options).await?;
        GET_BYTES.fetch_add(result.range.end - result.range.start, Ordering::Relaxed);
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<bytes::Bytes>> {
        GET_REQUESTS.fetch_add(merged_request_count(ranges), Ordering::Relaxed);
        GET_BYTES.fetch_add(
            ranges.iter().map(|r| r.end - r.start).sum::<u64>(),
            Ordering::Relaxed,
        );
        self.inner.get_ranges(location, ranges).await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[expect(clippy::single_range_in_vec_init)]
    fn test_merged_request_count() {
        assert_eq!(merged_request_count(&[]), 0);
        assert_eq!(merged_request_count(&[0..10]), 1);
        // within 1MiB gap: one request
        assert_eq!(merged_request_count(&[0..10, 20..30]), 1);
        // over 1MiB apart: two requests
        assert_eq!(merged_request_count(&[0..10, 2_000_000..2_000_010]), 2);
        // unsorted input
        assert_eq!(merged_request_count(&[2_000_000..2_000_010, 0..10]), 2);
    }
}
