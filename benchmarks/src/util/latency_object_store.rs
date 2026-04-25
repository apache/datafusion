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

//! An ObjectStore wrapper that adds simulated S3-like latency to get and list operations.
//!
//! Cycles through a fixed latency distribution inspired by real S3 performance:
//! - P50: ~30ms
//! - P75-P90: ~100-120ms
//! - P99: ~150-200ms

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

/// GET latency distribution, inspired by S3 latencies.
/// Deterministic but shuffled to avoid artificial patterns.
/// 20 values: 11x P50 (~25-35ms), 5x P75-P90 (~70-110ms), 2x P95 (~120-150ms), 2x P99 (~180-200ms)
/// Sorted: 25,25,28,28,30,30,30,30,32,32,35, 70,85,100,100,110, 130,150, 180,200
/// P50≈32ms, P90≈110ms, P99≈200ms
const GET_LATENCIES_MS: &[u64] = &[
    30, 100, 25, 85, 32, 200, 28, 130, 35, 70, 30, 150, 30, 110, 28, 180, 32, 25, 100, 30,
];

/// LIST latency distribution, generally higher than GET.
/// 20 values: 11x P50 (~40-70ms), 5x P75-P90 (~120-180ms), 2x P95 (~200-250ms), 2x P99 (~300-400ms)
/// Sorted: 40,40,50,50,55,55,60,60,65,65,70, 120,140,160,160,180, 210,250, 300,400
/// P50≈65ms, P90≈180ms, P99≈400ms
const LIST_LATENCIES_MS: &[u64] = &[
    55, 160, 40, 140, 65, 400, 50, 210, 70, 120, 60, 250, 55, 180, 50, 300, 65, 40, 160,
    60,
];

/// An ObjectStore wrapper that injects simulated latency on get and list calls.
#[derive(Debug)]
pub struct LatencyObjectStore<T: ObjectStore> {
    inner: T,
    get_counter: AtomicUsize,
    list_counter: AtomicUsize,
}

impl<T: ObjectStore> LatencyObjectStore<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            get_counter: AtomicUsize::new(0),
            list_counter: AtomicUsize::new(0),
        }
    }

    fn next_get_latency(&self) -> Duration {
        let idx =
            self.get_counter.fetch_add(1, Ordering::Relaxed) % GET_LATENCIES_MS.len();
        Duration::from_millis(GET_LATENCIES_MS[idx])
    }

    fn next_list_latency(&self) -> Duration {
        let idx =
            self.list_counter.fetch_add(1, Ordering::Relaxed) % LIST_LATENCIES_MS.len();
        Duration::from_millis(LIST_LATENCIES_MS[idx])
    }
}

impl<T: ObjectStore> fmt::Display for LatencyObjectStore<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LatencyObjectStore({})", self.inner)
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for LatencyObjectStore<T> {
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
        tokio::time::sleep(self.next_get_latency()).await;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Vec<bytes::Bytes>> {
        tokio::time::sleep(self.next_get_latency()).await;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let latency = self.next_list_latency();
        let stream = self.inner.list(prefix);
        futures::stream::once(async move {
            tokio::time::sleep(latency).await;
            futures::stream::empty()
        })
        .flatten()
        .chain(stream)
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        tokio::time::sleep(self.next_list_latency()).await;
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
