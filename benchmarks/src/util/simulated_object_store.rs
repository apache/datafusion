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

//! An [`ObjectStore`] that makes a local filesystem behave like S3 or GCS.
//!
//! Each request costs:
//!
//! 1. a connection from a pool of [`MAX_CONNECTIONS`],
//! 2. a time to first byte, drawn at random from a latency distribution,
//! 3. a transfer time at [`BYTES_PER_SECOND`] for each response body.
//!
//! The store does not override [`ObjectStore::get_ranges`]. `AmazonS3` and
//! `GoogleCloudStorage` do not override it either. The trait default merges
//! ranges that are less than 1MiB apart and sends each merged range as a
//! separate GET. Thus the simulated request count is the same as on S3. A
//! wrapper that overrides `get_ranges` charges one request where S3 charges
//! several.
//!
//! The store does not model request failures, retries or a shared network
//! limit.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result,
};
use rand::Rng;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

/// GET time to first byte, inspired by S3 latencies.
/// Sorted: 25,25,28,28,30,30,30,30,32,32,35, 70,85,100,100,110, 130,150, 180,200
/// P50≈32ms, P90≈110ms, P99≈200ms
const GET_TTFB_MS: &[u64] = &[
    30, 100, 25, 85, 32, 200, 28, 130, 35, 70, 30, 150, 30, 110, 28, 180, 32, 25, 100, 30,
];

/// Time to first byte of one LIST page. It is usually higher than for GET.
/// Sorted: 40,40,50,50,55,55,60,60,65,65,70, 120,140,160,160,180, 210,250, 300,400
/// P50≈65ms, P90≈180ms, P99≈400ms
const LIST_TTFB_MS: &[u64] = &[
    55, 160, 40, 140, 65, 400, 50, 210, 70, 120, 60, 250, 55, 180, 50, 300, 65, 40, 160,
    60,
];

/// Throughput of one connection. S3 and GCS give approximately 50-100MB/s on
/// one connection, independent of the object size.
pub const BYTES_PER_SECOND: u64 = 100 * 1024 * 1024;

/// Maximum number of requests in flight.
pub const MAX_CONNECTIONS: usize = 128;

/// Maximum keys in one LIST page. S3 and GCS both use 1000.
const LIST_PAGE_SIZE: usize = 1000;

#[derive(Debug, Clone, Copy)]
struct Costs {
    get_ttfb_ms: &'static [u64],
    list_ttfb_ms: &'static [u64],
    bytes_per_second: u64,
    list_page_size: usize,
}

/// Presents `T` (usually a `LocalFileSystem`) as remote object storage. See
/// the [module docs](self).
#[derive(Debug)]
pub struct SimulatedObjectStore<T: ObjectStore> {
    inner: T,
    costs: Costs,
    connections: Arc<Semaphore>,
}

impl<T: ObjectStore> SimulatedObjectStore<T> {
    pub fn new(inner: T) -> Self {
        let costs = Costs {
            get_ttfb_ms: GET_TTFB_MS,
            list_ttfb_ms: LIST_TTFB_MS,
            bytes_per_second: BYTES_PER_SECOND,
            list_page_size: LIST_PAGE_SIZE,
        };
        Self {
            inner,
            costs,
            connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        }
    }

    /// Charges one LIST request for each page of `keys`.
    fn paged(
        &self,
        keys: BoxStream<'static, Result<ObjectMeta>>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let costs = self.costs;
        let connections = Arc::clone(&self.connections);
        futures::stream::unfold((keys, 0usize), move |(mut keys, seen)| {
            let connections = Arc::clone(&connections);
            async move {
                let key = keys.next().await;
                // Each page needs the token from the previous page, thus pages
                // are serial. An empty listing also costs one request.
                let new_page = seen % costs.list_page_size == 0;
                if new_page && (key.is_some() || seen == 0) {
                    let _permit = round_trip(&connections, costs.list_ttfb_ms).await;
                }
                Some((key?, (keys, seen + 1)))
            }
        })
        .boxed()
    }
}

/// Waits for a free connection and for the time to first byte. The request
/// keeps the connection until it drops the returned permit.
async fn round_trip(
    connections: &Arc<Semaphore>,
    ttfb_ms: &[u64],
) -> OwnedSemaphorePermit {
    let permit = Arc::clone(connections)
        .acquire_owned()
        .await
        .expect("connection pool is never closed");
    let idx = rand::rng().random_range(0..ttfb_ms.len());
    tokio::time::sleep(Duration::from_millis(ttfb_ms[idx])).await;
    permit
}

fn transfer_time(bytes: u64, bytes_per_second: u64) -> Duration {
    Duration::from_secs_f64(bytes as f64 / bytes_per_second as f64)
}

/// Charges transfer time while the caller reads the body. The connection stays
/// in use until the body is fully read or dropped.
fn throttle(
    result: GetResult,
    permit: OwnedSemaphorePermit,
    bytes_per_second: u64,
) -> GetResult {
    let meta = result.meta.clone();
    let range = result.range.clone();
    let attributes = result.attributes.clone();
    let extensions = result.extensions.clone();
    // The deadline is absolute, so time spent on the local read and timer
    // overshoot do not add to the simulated transfer time. The clock starts
    // when the caller first reads the body.
    let body = futures::stream::unfold(
        (result.into_stream(), None, 0u64, permit),
        move |(mut body, start, mut sent, permit)| async move {
            let start = start.unwrap_or_else(Instant::now);
            let chunk = body.next().await?;
            if let Ok(bytes) = &chunk {
                sent += bytes.len() as u64;
                tokio::time::sleep_until(start + transfer_time(sent, bytes_per_second))
                    .await;
            }
            Some((chunk, (body, Some(start), sent, permit)))
        },
    );
    GetResult {
        payload: GetResultPayload::Stream(body.boxed()),
        meta,
        range,
        attributes,
        extensions,
    }
}

impl<T: ObjectStore> fmt::Display for SimulatedObjectStore<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimulatedObjectStore({})", self.inner)
    }
}

/// Only reads and lists are charged. Writes go directly to `inner`, because
/// benchmarks do not measure them.
#[async_trait]
impl<T: ObjectStore> ObjectStore for SimulatedObjectStore<T> {
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
        let permit = round_trip(&self.connections, self.costs.get_ttfb_ms).await;
        let result = self.inner.get_opts(location, options).await?;
        // A HEAD result has a range that covers the full object, but the
        // caller never reads its body, so it pays no transfer time.
        Ok(throttle(result, permit, self.costs.bytes_per_second))
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.paged(self.inner.list(prefix))
    }

    // S3 and GCS override this to start the listing at `offset` on the server.
    // The trait default lists all keys and then filters them.
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.paged(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let result = self.inner.list_with_delimiter(prefix).await?;
        let entries = result.objects.len() + result.common_prefixes.len();
        let pages = entries.div_ceil(self.costs.list_page_size).max(1);
        for _ in 0..pages {
            let _permit = round_trip(&self.connections, self.costs.list_ttfb_ms).await;
        }
        Ok(result)
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
    use std::sync::Mutex;

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;

    /// Records the range of each GET that reaches the inner store. On S3, each
    /// one is a separate HTTP request.
    #[derive(Debug, Default)]
    struct Recording {
        inner: InMemory,
        gets: Mutex<Vec<std::ops::Range<u64>>>,
    }

    impl fmt::Display for Recording {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Recording")
        }
    }

    #[async_trait]
    impl ObjectStore for Recording {
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

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> Result<GetResult> {
            let result = self.inner.get_opts(location, options).await?;
            self.gets.lock().unwrap().push(result.range.clone());
            Ok(result)
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

    const TTFB_MS: u64 = 10;

    /// A store with a constant time to first byte, 1MiB/s and 3 keys per page.
    async fn store(objects: &[(&str, usize)]) -> SimulatedObjectStore<Recording> {
        let inner = Recording::default();
        for (name, len) in objects {
            let payload = PutPayload::from(vec![7u8; *len]);
            inner.put(&Path::from(*name), payload).await.unwrap();
        }
        SimulatedObjectStore {
            inner,
            costs: Costs {
                get_ttfb_ms: &[TTFB_MS],
                list_ttfb_ms: &[TTFB_MS],
                bytes_per_second: 1024 * 1024,
                list_page_size: 3,
            },
            connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        }
    }

    fn ms(elapsed: Duration) -> u64 {
        elapsed.as_millis() as u64
    }

    #[tokio::test(start_paused = true)]
    async fn distant_ranges_are_separate_requests() {
        let store = store(&[("f", 8 << 20)]).await;
        let ranges = [0..1024, (3 << 20)..(3 << 20) + 1024];

        let data = store.get_ranges(&Path::from("f"), &ranges).await.unwrap();

        assert_eq!(data, vec![Bytes::from(vec![7u8; 1024]); 2]);
        assert_eq!(store.inner.gets.lock().unwrap().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn near_ranges_are_one_request() {
        let store = store(&[("f", 8 << 20)]).await;

        store
            .get_ranges(&Path::from("f"), &[0..1024, 2048..4096])
            .await
            .unwrap();

        assert_eq!(*store.inner.gets.lock().unwrap(), vec![0..4096]);
    }

    #[tokio::test(start_paused = true)]
    async fn get_costs_ttfb_plus_transfer() {
        let store = store(&[("f", 1 << 20)]).await;

        let start = Instant::now();
        store
            .get_range(&Path::from("f"), 0..256 * 1024)
            .await
            .unwrap();

        // 10ms TTFB plus 256KiB at 1MiB/s.
        assert_eq!(ms(start.elapsed()), TTFB_MS + 250);
    }

    #[tokio::test(start_paused = true)]
    async fn head_does_not_pay_for_a_body() {
        let store = store(&[("f", 1 << 20)]).await;

        let start = Instant::now();
        store.head(&Path::from("f")).await.unwrap();

        assert_eq!(ms(start.elapsed()), TTFB_MS);
    }

    #[tokio::test(start_paused = true)]
    async fn list_costs_one_request_per_page() {
        for (keys, pages) in [(0, 1), (1, 1), (3, 1), (4, 2), (6, 2), (7, 3)] {
            let names: Vec<_> = (0..keys).map(|i| format!("k{i}")).collect();
            let objects: Vec<_> = names.iter().map(|n| (n.as_str(), 1)).collect();
            let store = store(&objects).await;

            let start = Instant::now();
            let listed: Vec<_> = store.list(None).collect().await;

            assert_eq!(listed.len(), keys);
            assert_eq!(ms(start.elapsed()), pages * TTFB_MS, "{keys} keys");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn list_with_offset_skips_earlier_pages() {
        let names: Vec<_> = (0..9).map(|i| format!("k{i}")).collect();
        let objects: Vec<_> = names.iter().map(|n| (n.as_str(), 1)).collect();
        let store = store(&objects).await;

        let start = Instant::now();
        let listed: Vec<_> = store
            .list_with_offset(None, &Path::from("k5"))
            .collect()
            .await;

        // k6, k7 and k8 fit in one page.
        assert_eq!(listed.len(), 3);
        assert_eq!(ms(start.elapsed()), TTFB_MS);
    }

    /// A Parquet file with 8 columns. Random `i64` values do not compress, so
    /// each column chunk is larger than 1MiB and is not merged with the next.
    fn wide_parquet() -> Bytes {
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use rand::{SeedableRng, rngs::StdRng};

        let mut rng = StdRng::seed_from_u64(42);
        let fields: Vec<_> = (0..8)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<ArrayRef> = (0..8)
            .map(|_| {
                let values: Vec<i64> = (0..200_000).map(|_| rng.random()).collect();
                Arc::new(Int64Array::from(values)) as ArrayRef
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buf)
    }

    /// Returns the number of GET requests that the scan in `sql` sends.
    async fn gets_for_query(sql: &str) -> usize {
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::prelude::{ParquetReadOptions, SessionContext};

        let store = Arc::new(store(&[]).await);
        store
            .put(&Path::from("t.parquet"), wide_parquet().into())
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let url = ObjectStoreUrl::parse("mem://").unwrap();
        ctx.register_object_store(url.as_ref(), Arc::clone(&store) as _);
        ctx.register_parquet("t", "mem:///t.parquet", ParquetReadOptions::default())
            .await
            .unwrap();
        // Do not count the reads of schema inference.
        store.inner.gets.lock().unwrap().clear();
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
        store.inner.gets.lock().unwrap().len()
    }

    #[tokio::test(start_paused = true)]
    async fn distant_column_chunks_cost_more_requests() {
        // DataFusion reads both column chunks with one `get_ranges` call. On
        // S3 that is two requests, because the chunks are far apart.
        let one = gets_for_query("SELECT sum(c0) FROM t").await;
        let two = gets_for_query("SELECT sum(c0), sum(c7) FROM t").await;

        assert_eq!(two, one + 1);
    }
}
