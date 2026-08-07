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

//! An [`ObjectStore`] that makes a local filesystem behave like remote object
//! storage (S3, GCS), so that benchmarks can be run against realistic IO
//! without a network.
//!
//! # Why not just sleep before each call
//!
//! What matters is the *request pattern*, not only the wall clock. A store that
//! sleeps once per [`ObjectStore`] method call charges one round trip where the
//! real store pays several, and it is blind to exactly the thing an IO
//! optimisation changes.
//!
//! The concrete case is [`ObjectStore::get_ranges`]. Neither `AmazonS3` nor
//! `GoogleCloudStorage` implements it; they inherit the trait default, which
//! merges ranges less than
//! [`OBJECT_STORE_COALESCE_DEFAULT`](object_store::OBJECT_STORE_COALESCE_DEFAULT)
//! bytes apart and then issues the merged chunks as up to 10 *concurrent* GETs.
//! A vectored read of a row group whose column chunks sit more than 1MiB apart
//! is therefore many round trips against S3, and how many is data dependent.
//!
//! So this store deliberately implements **only** the HTTP-shaped primitives a
//! real remote store implements, and leaves `get_ranges` — along with
//! [`ObjectStore::list_with_offset`] and [`ObjectStore::rename_opts`] — to the
//! trait default. Everything arrow-rs does above the wire (coalescing, fan-out,
//! pagination) then runs against the simulator unmodified.
//!
//! Note this is the opposite of the advice in [`ObjectStore`]'s "Wrappers"
//! section, which tells wrappers to implement every method so they do not lose
//! the wrapped store's overrides. That advice is for observability wrappers.
//! Here the wrapped store's overrides are the problem: `LocalFileSystem`
//! implements `get_ranges` as a sequence of positional reads with no coalescing
//! at all, which is precisely the behaviour we need to shed. Do not add
//! `#[deny(clippy::missing_trait_methods)]` to the impl below.
//!
//! # What a simulated request costs
//!
//! 1. waiting for a free connection, bounded by
//!    [`SimulatedStoreConfig::max_concurrent_requests`],
//! 2. time to first byte, drawn from a latency distribution (see [`latency`]),
//! 3. transfer time at
//!    [`SimulatedStoreConfig::connection_bytes_per_second`], charged as the
//!    response body is consumed.
//!
//! Step 3 is what makes coalescing decisions meaningful: merging across a gap
//! trades bytes for round trips, and without a per-byte cost a simulator says
//! that trade is always free.
//!
//! # What is deliberately not modelled
//!
//! arrow-rs never splits one large range into several smaller concurrent
//! requests, so neither does this store: a 200MB coalesced range is one GET on
//! one connection, and is bandwidth bound. Request failures, retries and
//! per-prefix throttling are also not modelled.

mod latency;

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
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub use latency::{GET_TTFB_MS, LIST_TTFB_MS, LatencySampler};

/// The connection pool is owned by the store and never closed.
const POOL_OPEN: &str = "connection pool is never closed";

/// How the simulated remote store behaves.
#[derive(Debug, Clone)]
pub struct SimulatedStoreConfig {
    /// Time-to-first-byte distribution for GET, in milliseconds.
    pub get_ttfb_ms: &'static [u64],
    /// Time-to-first-byte distribution for a single LIST page, in milliseconds.
    pub list_ttfb_ms: &'static [u64],
    /// Sustained throughput of a *single* connection.
    ///
    /// S3 and GCS deliver roughly 50-100MB/s on one connection no matter how
    /// large the object is, which is why one huge coalesced range is slow even
    /// though it is only one round trip.
    pub connection_bytes_per_second: u64,
    /// Maximum number of requests in flight, i.e. the client connection pool.
    pub max_concurrent_requests: usize,
    /// Keys returned per LIST page. Both S3 and GCS default to 1000.
    pub list_page_size: usize,
    /// Transfer time is accumulated and only slept once it exceeds this, so
    /// that small body chunks do not become a storm of sub-millisecond timer
    /// waits that the runtime cannot honour accurately anyway.
    pub min_transfer_sleep: Duration,
    /// Reads no larger than this are served by one blocking read of the local
    /// file rather than a chunked stream. Above it the body is streamed, so a
    /// whole-object GET of a multi-gigabyte CSV is not buffered in memory.
    pub max_eager_read_bytes: u64,
}

impl Default for SimulatedStoreConfig {
    fn default() -> Self {
        Self {
            get_ttfb_ms: GET_TTFB_MS,
            list_ttfb_ms: LIST_TTFB_MS,
            connection_bytes_per_second: 100 * 1024 * 1024,
            max_concurrent_requests: 128,
            list_page_size: 1000,
            min_transfer_sleep: Duration::from_micros(500),
            max_eager_read_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Presents `T` — in practice a `LocalFileSystem` — as if it were remote object
/// storage. See the [module docs](self).
#[derive(Debug)]
pub struct SimulatedObjectStore<T: ObjectStore> {
    inner: T,
    config: SimulatedStoreConfig,
    connections: Arc<Semaphore>,
    get_ttfb: Arc<LatencySampler>,
    list_ttfb: Arc<LatencySampler>,
}

impl<T: ObjectStore> SimulatedObjectStore<T> {
    pub fn new(inner: T) -> Self {
        Self::with_config(inner, SimulatedStoreConfig::default())
    }

    pub fn with_config(inner: T, config: SimulatedStoreConfig) -> Self {
        Self {
            inner,
            connections: Arc::new(Semaphore::new(config.max_concurrent_requests)),
            get_ttfb: Arc::new(LatencySampler::new(config.get_ttfb_ms)),
            list_ttfb: Arc::new(LatencySampler::new(config.list_ttfb_ms)),
            config,
        }
    }

    pub fn config(&self) -> &SimulatedStoreConfig {
        &self.config
    }

    /// Take a connection from the pool, waiting if all of them are busy.
    async fn connection(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.connections)
            .acquire_owned()
            .await
            .expect(POOL_OPEN)
    }

    /// Wrap a response so that its body costs transfer time, holding the
    /// connection until the body has been consumed.
    async fn simulate_body(
        &self,
        result: GetResult,
        permit: OwnedSemaphorePermit,
    ) -> Result<GetResult> {
        let rate = self.config.connection_bytes_per_second;
        let len = result.range.end - result.range.start;

        if len <= self.config.max_eager_read_bytes {
            // Every read the Parquet reader makes is bounded, and its caller
            // collects the body in full, so charging the whole transfer up
            // front is exact. Doing it this way also keeps `LocalFileSystem`'s
            // single-blocking-read fast path instead of paying a
            // `spawn_blocking` per 8KiB chunk.
            let meta = result.meta.clone();
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let bytes = result.bytes().await?;
            tokio::time::sleep(transfer_time(bytes.len() as u64, rate)).await;
            drop(permit);
            return Ok(GetResult {
                payload: GetResultPayload::Stream(
                    futures::stream::once(async move { Ok(bytes) }).boxed(),
                ),
                meta,
                range,
                attributes,
            });
        }

        Ok(self.throttle_body(result, permit))
    }

    /// Charge transfer time incrementally as the body is consumed, so that a
    /// large or unbounded read is neither buffered in memory nor able to hide
    /// its time-to-first-byte behind its throughput.
    fn throttle_body(
        &self,
        result: GetResult,
        permit: OwnedSemaphorePermit,
    ) -> GetResult {
        let rate = self.config.connection_bytes_per_second;
        let min_sleep = self.config.min_transfer_sleep;
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();

        let payload = futures::stream::unfold(
            (result.into_stream(), Duration::ZERO, permit),
            move |(mut body, mut owed, permit)| async move {
                let item = body.next().await?;
                if let Ok(bytes) = &item {
                    owed += transfer_time(bytes.len() as u64, rate);
                    if owed >= min_sleep {
                        tokio::time::sleep(owed).await;
                        owed = Duration::ZERO;
                    }
                }
                // `permit` rides along so the connection stays checked out
                // until the body is fully read or the stream is dropped.
                Some((item, (body, owed, permit)))
            },
        )
        .boxed();

        GetResult {
            payload: GetResultPayload::Stream(payload),
            meta,
            range,
            attributes,
        }
    }
}

fn transfer_time(bytes: u64, bytes_per_second: u64) -> Duration {
    Duration::from_secs_f64(bytes as f64 / bytes_per_second as f64)
}

impl<T: ObjectStore> fmt::Display for SimulatedObjectStore<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimulatedObjectStore({})", self.inner)
    }
}

// NOTE: `get_ranges`, `list_with_offset` and `rename_opts` are intentionally
// left to the `ObjectStore` defaults, exactly as the real remote stores leave
// them. See the module docs before adding any of them here.
#[async_trait]
impl<T: ObjectStore> ObjectStore for SimulatedObjectStore<T> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let _permit = self.connection().await;
        let bytes = payload.content_length() as u64;
        let cost = self.get_ttfb.sample()
            + transfer_time(bytes, self.config.connection_bytes_per_second);
        tokio::time::sleep(cost).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        // Only the "create upload" request is charged; the parts go through the
        // returned `MultipartUpload`, which is not wrapped. Benchmarks read far
        // more than they write, so this has not been worth the extra machinery.
        let _permit = self.connection().await;
        tokio::time::sleep(self.get_ttfb.sample()).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // A HEAD moves no bytes, but the `GetResult` it returns still reports a
        // range spanning the whole object. Charging that range as a transfer
        // would make every metadata probe cost as much as downloading the file.
        let head = options.head;
        let permit = self.connection().await;
        tokio::time::sleep(self.get_ttfb.sample()).await;
        let result = self.inner.get_opts(location, options).await?;
        if head {
            return Ok(result);
        }
        self.simulate_body(result, permit).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let ttfb = Arc::clone(&self.get_ttfb);
        let connections = Arc::clone(&self.connections);
        let locations = locations
            .then(move |location| {
                let ttfb = Arc::clone(&ttfb);
                let connections = Arc::clone(&connections);
                async move {
                    let _permit = connections.acquire_owned().await.expect(POOL_OPEN);
                    tokio::time::sleep(ttfb.sample()).await;
                    location
                }
            })
            .boxed();
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let inner = self.inner.list(prefix);
        let page_size = self.config.list_page_size;
        let ttfb = Arc::clone(&self.list_ttfb);
        let connections = Arc::clone(&self.connections);

        futures::stream::unfold((inner, 0usize), move |(mut inner, seen)| {
            let ttfb = Arc::clone(&ttfb);
            let connections = Arc::clone(&connections);
            async move {
                // S3 and GCS cap a listing response at `page_size` keys, and
                // each page needs the continuation token from the page before
                // it. Pages are therefore serial round trips, not a fan-out:
                // listing 5000 files costs five times the latency of listing
                // 500, which is why partition discovery hurts on remote stores.
                if seen % page_size == 0 {
                    let _permit = connections.acquire_owned().await.expect(POOL_OPEN);
                    tokio::time::sleep(ttfb.sample()).await;
                }
                let item = inner.next().await?;
                Some((item, (inner, seen + 1)))
            }
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        // The real stores paginate this internally and hand back the assembled
        // result, so the caller makes one call but pays for every page. The
        // page count is only known after listing, so the cost is charged
        // afterwards; the total is what matters.
        let result = self.inner.list_with_delimiter(prefix).await?;
        let entries = result.objects.len() + result.common_prefixes.len();
        let pages = entries.div_ceil(self.config.list_page_size).max(1);
        for _ in 0..pages {
            let _permit = self.connection().await;
            tokio::time::sleep(self.list_ttfb.sample()).await;
        }
        Ok(result)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<()> {
        // Server side copy: one round trip, no bytes over the wire.
        let _permit = self.connection().await;
        tokio::time::sleep(self.get_ttfb.sample()).await;
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use datafusion_common::instant::Instant;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;

    /// Records the range of every `get_opts` reaching the inner store, which is
    /// what a real store would turn into one HTTP request each.
    #[derive(Debug, Default)]
    struct RequestLog {
        gets: AtomicUsize,
        ranges: Mutex<Vec<(u64, u64)>>,
    }

    #[derive(Debug)]
    struct Recording {
        inner: InMemory,
        log: Arc<RequestLog>,
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
            self.log.gets.fetch_add(1, Ordering::Relaxed);
            let result = self.inner.get_opts(location, options).await?;
            self.log
                .ranges
                .lock()
                .unwrap()
                .push((result.range.start, result.range.end));
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

    /// A config with no latency and effectively infinite bandwidth, so tests
    /// assert on request *shape* without waiting.
    fn instant() -> SimulatedStoreConfig {
        SimulatedStoreConfig {
            get_ttfb_ms: &[0],
            list_ttfb_ms: &[0],
            connection_bytes_per_second: u64::MAX,
            ..Default::default()
        }
    }

    async fn store_with(
        config: SimulatedStoreConfig,
        len: usize,
    ) -> (SimulatedObjectStore<Recording>, Arc<RequestLog>, Path) {
        let log = Arc::new(RequestLog::default());
        let inner = Recording {
            inner: InMemory::new(),
            log: Arc::clone(&log),
        };
        let path = Path::from("data.parquet");
        inner
            .put(&path, PutPayload::from(vec![7u8; len]))
            .await
            .unwrap();
        (SimulatedObjectStore::with_config(inner, config), log, path)
    }

    #[tokio::test]
    async fn distant_ranges_become_separate_requests() {
        // Ranges more than OBJECT_STORE_COALESCE_DEFAULT (1MiB) apart are
        // separate GETs against S3. A store that overrode `get_ranges` would
        // report one.
        let (store, log, path) = store_with(instant(), 8 * 1024 * 1024).await;
        let ranges = [0..1024, 3 * 1024 * 1024..3 * 1024 * 1024 + 1024];

        let data = store.get_ranges(&path, &ranges).await.unwrap();

        assert_eq!(data.len(), 2);
        assert_eq!(log.gets.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn nearby_ranges_are_coalesced_into_one_request() {
        // Under 1MiB apart, so arrow-rs merges them and reads the gap too.
        let (store, log, path) = store_with(instant(), 8 * 1024 * 1024).await;
        let ranges = [0..1024, 2048..4096];

        store.get_ranges(&path, &ranges).await.unwrap();

        assert_eq!(log.gets.load(Ordering::Relaxed), 1);
        assert_eq!(log.ranges.lock().unwrap().as_slice(), &[(0, 4096)]);
    }

    /// A Parquet file whose column chunks are far enough apart that arrow-rs
    /// will not coalesce them: random `i64`s do not compress, so each chunk is
    /// well over the 1MiB coalescing threshold.
    fn wide_parquet(rows: usize, cols: usize) -> Bytes {
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use rand::{Rng, SeedableRng, rngs::StdRng};

        let mut rng = StdRng::seed_from_u64(42);
        let schema = Arc::new(Schema::new(
            (0..cols)
                .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        ));
        let arrays: Vec<ArrayRef> = (0..cols)
            .map(|_| {
                let values: Vec<i64> = (0..rows).map(|_| rng.random()).collect();
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

    /// Run `sql` against a Parquet file served by the simulated store, and
    /// return how many requests reached the wire during execution.
    async fn requests_for_query(sql: &str) -> usize {
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::prelude::{ParquetReadOptions, SessionContext};

        let log = Arc::new(RequestLog::default());
        let recording = Recording {
            inner: InMemory::new(),
            log: Arc::clone(&log),
        };
        recording
            .put(
                &Path::from("t.parquet"),
                PutPayload::from(wide_parquet(200_000, 8)),
            )
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let url = ObjectStoreUrl::parse("mem://").unwrap();
        ctx.register_object_store(
            url.as_ref(),
            Arc::new(SimulatedObjectStore::with_config(recording, instant())),
        );
        ctx.register_parquet("t", "mem:///t.parquet", ParquetReadOptions::default())
            .await
            .unwrap();

        // Registration infers the schema, which is IO of its own. Only count
        // what the scan itself costs.
        log.gets.store(0, Ordering::Relaxed);
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
        log.gets.load(Ordering::Relaxed)
    }

    #[tokio::test]
    async fn projecting_more_columns_costs_more_requests() {
        // The whole point of the module: DataFusion asks for both column chunks
        // in a single `get_byte_ranges` call, and the store must turn that into
        // as many requests as S3 would. A store that overrode `get_ranges` and
        // slept once would report the same count for both queries.
        let one = requests_for_query("SELECT sum(c0) FROM t").await;
        let two = requests_for_query("SELECT sum(c0), sum(c7) FROM t").await;

        assert!(
            two > one,
            "reading two distant column chunks ({two} requests) should cost more \
             than reading one ({one} requests)"
        );
    }

    #[tokio::test]
    async fn returned_ranges_are_correct() {
        let (store, _log, path) = store_with(instant(), 4096).await;
        let ranges = [10..20, 100..108, 3000..3001];

        let data = store.get_ranges(&path, &ranges).await.unwrap();

        assert_eq!(
            data,
            vec![
                Bytes::from(vec![7u8; 10]),
                Bytes::from(vec![7u8; 8]),
                Bytes::from(vec![7u8; 1])
            ]
        );
    }

    #[tokio::test]
    async fn head_requests_do_not_pay_for_a_body() {
        let config = SimulatedStoreConfig {
            get_ttfb_ms: &[0],
            list_ttfb_ms: &[0],
            // 1KiB/s, so charging the 1MiB object as a transfer would take
            // roughly a quarter of an hour.
            connection_bytes_per_second: 1024,
            ..Default::default()
        };
        let (store, _log, path) = store_with(config, 1024 * 1024).await;

        let start = Instant::now();
        store.head(&path).await.unwrap();

        assert!(
            start.elapsed() < Duration::from_secs(1),
            "a HEAD moves no bytes and must not be charged as a transfer"
        );
    }

    #[tokio::test]
    async fn transfer_time_scales_with_bytes() {
        let config = SimulatedStoreConfig {
            get_ttfb_ms: &[0],
            list_ttfb_ms: &[0],
            // 1MiB/s, so a 256KiB read should take about a quarter second.
            connection_bytes_per_second: 1024 * 1024,
            ..Default::default()
        };
        let (store, _log, path) = store_with(config, 1024 * 1024).await;

        let start = Instant::now();
        store.get_range(&path, 0..256 * 1024).await.unwrap();
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(200),
            "expected a bandwidth cost, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn list_is_charged_per_page() {
        let config = SimulatedStoreConfig {
            get_ttfb_ms: &[0],
            list_ttfb_ms: &[20],
            list_page_size: 3,
            connection_bytes_per_second: u64::MAX,
            ..Default::default()
        };
        let log = Arc::new(RequestLog::default());
        let inner = Recording {
            inner: InMemory::new(),
            log,
        };
        for i in 0..7 {
            inner
                .put(&Path::from(format!("part-{i}")), PutPayload::from("x"))
                .await
                .unwrap();
        }
        let store = SimulatedObjectStore::with_config(inner, config);

        let start = Instant::now();
        let listed: Vec<_> = store.list(None).collect().await;
        let elapsed = start.elapsed();

        assert_eq!(listed.len(), 7);
        // 7 keys at 3 per page is 3 serial pages, so at least 60ms.
        assert!(
            elapsed >= Duration::from_millis(60),
            "expected three serial LIST pages, took {elapsed:?}"
        );
    }
}
