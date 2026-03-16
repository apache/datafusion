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
/// Distribution: P50 ~30ms, P75-P90 ~100ms, P99 ~200ms.
const GET_LATENCIES_MS: &[u64] =
    &[30, 150, 28, 100, 35, 200, 30, 85, 120, 25, 110, 32, 180, 70];

/// LIST latency distribution, generally higher than GET.
/// Distribution: P50 ~55ms, P75-P90 ~150ms, P99 ~400ms.
const LIST_LATENCIES_MS: &[u64] = &[
    55, 250, 40, 160, 70, 400, 50, 140, 200, 60, 180, 65, 300, 120,
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
