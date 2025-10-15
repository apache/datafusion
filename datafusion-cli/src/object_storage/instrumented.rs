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

use std::{
    cmp, fmt,
    ops::AddAssign,
    str::FromStr,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::Utc;
use datafusion::{
    common::{instant::Instant, HashMap},
    error::DataFusionError,
    execution::object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
};
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use parking_lot::{Mutex, RwLock};
use url::Url;

/// The profiling mode to use for an [`InstrumentedObjectStore`] instance. Collecting profiling
/// data will have a small negative impact on both CPU and memory usage. Default is `Disabled`
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum InstrumentedObjectStoreMode {
    /// Disable collection of profiling data
    #[default]
    Disabled,
    /// Enable collection of profiling data
    Enabled,
}

impl fmt::Display for InstrumentedObjectStoreMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FromStr for InstrumentedObjectStoreMode {
    type Err = DataFusionError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disabled" => Ok(Self::Disabled),
            "enabled" => Ok(Self::Enabled),
            _ => Err(DataFusionError::Execution(format!("Unrecognized mode {s}"))),
        }
    }
}

impl From<u8> for InstrumentedObjectStoreMode {
    fn from(value: u8) -> Self {
        match value {
            1 => InstrumentedObjectStoreMode::Enabled,
            _ => InstrumentedObjectStoreMode::Disabled,
        }
    }
}

/// Wrapped [`ObjectStore`] instances that record information for reporting on the usage of the
/// inner [`ObjectStore`]
#[derive(Debug)]
pub struct InstrumentedObjectStore {
    inner: Arc<dyn ObjectStore>,
    instrument_mode: AtomicU8,
    requests: Mutex<Vec<RequestDetails>>,
}

impl InstrumentedObjectStore {
    /// Returns a new [`InstrumentedObjectStore`] that wraps the provided [`ObjectStore`]
    fn new(object_store: Arc<dyn ObjectStore>, instrument_mode: AtomicU8) -> Self {
        Self {
            inner: object_store,
            instrument_mode,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn set_instrument_mode(&self, mode: InstrumentedObjectStoreMode) {
        self.instrument_mode.store(mode as u8, Ordering::Relaxed)
    }

    /// Returns all [`RequestDetails`] accumulated in this [`InstrumentedObjectStore`] and clears
    /// the stored requests
    pub fn take_requests(&self) -> Vec<RequestDetails> {
        let mut req = self.requests.lock();

        req.drain(..).collect()
    }

    async fn instrumented_get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult> {
        let timestamp = Utc::now();
        let range = options.range.clone();

        let start = Instant::now();
        let ret = self.inner.get_opts(location, options).await?;
        let elapsed = start.elapsed();

        self.requests.lock().push(RequestDetails {
            op: Operation::Get,
            path: location.clone(),
            timestamp,
            duration: Some(elapsed),
            size: Some((ret.range.end - ret.range.start) as usize),
            range,
            extra_display: None,
        });

        Ok(ret)
    }
}

impl fmt::Display for InstrumentedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode: InstrumentedObjectStoreMode =
            self.instrument_mode.load(Ordering::Relaxed).into();
        write!(
            f,
            "Instrumented Object Store: instrument_mode: {mode}, inner: {}",
            self.inner
        )
    }
}

#[async_trait]
impl ObjectStore for InstrumentedObjectStore {
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
        if self.instrument_mode.load(Ordering::Relaxed)
            != InstrumentedObjectStoreMode::Disabled as u8
        {
            return self.instrumented_get_opts(location, options).await;
        }

        self.inner.get_opts(location, options).await
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

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner.head(location).await
    }
}

/// Object store operation types tracked by [`InstrumentedObjectStore`]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Operation {
    _Copy,
    _Delete,
    Get,
    _Head,
    _List,
    _Put,
}

/// Holds profiling details about individual requests made through an [`InstrumentedObjectStore`]
#[derive(Debug)]
pub struct RequestDetails {
    op: Operation,
    path: Path,
    timestamp: chrono::DateTime<Utc>,
    duration: Option<Duration>,
    size: Option<usize>,
    range: Option<GetRange>,
    extra_display: Option<String>,
}

impl fmt::Display for RequestDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut output_parts = vec![format!(
            "{} operation={:?}",
            self.timestamp.to_rfc3339(),
            self.op
        )];

        if let Some(d) = self.duration {
            output_parts.push(format!("duration={:.6}s", d.as_secs_f32()));
        }
        if let Some(s) = self.size {
            output_parts.push(format!("size={s}"));
        }
        if let Some(r) = &self.range {
            output_parts.push(format!("range: {r}"));
        }
        output_parts.push(format!("path={}", self.path));

        if let Some(ed) = &self.extra_display {
            output_parts.push(ed.clone());
        }

        write!(f, "{}", output_parts.join(" "))
    }
}

/// Summary statistics for an [`InstrumentedObjectStore`]'s [`RequestDetails`]
#[derive(Default)]
pub struct RequestSummary {
    count: usize,
    duration_stats: Option<Stats<Duration>>,
    size_stats: Option<Stats<usize>>,
}

impl RequestSummary {
    /// Generates a set of [RequestSummaries](RequestSummary) from the input [`RequestDetails`]
    /// grouped by the input's [`Operation`]
    pub fn summarize_by_operation(
        requests: &[RequestDetails],
    ) -> HashMap<Operation, Self> {
        let mut summaries: HashMap<Operation, Self> = HashMap::new();
        for rd in requests {
            match summaries.get_mut(&rd.op) {
                Some(rs) => rs.push(rd),
                None => {
                    let mut rs = RequestSummary::default();
                    rs.push(rd);
                    summaries.insert(rd.op, rs);
                }
            }
        }

        summaries
    }

    fn push(&mut self, request: &RequestDetails) {
        self.count += 1;
        if let Some(dur) = request.duration {
            self.duration_stats.get_or_insert_default().push(dur)
        }
        if let Some(size) = request.size {
            self.size_stats.get_or_insert_default().push(size)
        }
    }
}

impl fmt::Display for RequestSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "count: {}", self.count)?;

        if let Some(dur_stats) = &self.duration_stats {
            writeln!(f, "duration min: {:.6}s", dur_stats.min.as_secs_f32())?;
            writeln!(f, "duration max: {:.6}s", dur_stats.max.as_secs_f32())?;
            let avg = dur_stats.sum.as_secs_f32() / (self.count as f32);
            writeln!(f, "duration avg: {:.6}s", avg)?;
        }

        if let Some(size_stats) = &self.size_stats {
            writeln!(f, "size min: {} B", size_stats.min)?;
            writeln!(f, "size max: {} B", size_stats.max)?;
            let avg = size_stats.sum / self.count;
            writeln!(f, "size avg: {} B", avg)?;
            writeln!(f, "size sum: {} B", size_stats.sum)?;
        }

        Ok(())
    }
}

struct Stats<T: Copy + Ord + AddAssign<T>> {
    min: T,
    max: T,
    sum: T,
}

impl<T: Copy + Ord + AddAssign<T>> Stats<T> {
    fn push(&mut self, val: T) {
        self.min = cmp::min(val, self.min);
        self.max = cmp::max(val, self.max);
        self.sum += val;
    }
}

impl Default for Stats<Duration> {
    fn default() -> Self {
        Self {
            min: Duration::MAX,
            max: Duration::ZERO,
            sum: Duration::ZERO,
        }
    }
}

impl Default for Stats<usize> {
    fn default() -> Self {
        Self {
            min: usize::MAX,
            max: usize::MIN,
            sum: 0,
        }
    }
}

/// Provides access to [`InstrumentedObjectStore`] instances that record requests for reporting
#[derive(Debug)]
pub struct InstrumentedObjectStoreRegistry {
    inner: Arc<dyn ObjectStoreRegistry>,
    instrument_mode: AtomicU8,
    stores: RwLock<Vec<Arc<InstrumentedObjectStore>>>,
}

impl Default for InstrumentedObjectStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl InstrumentedObjectStoreRegistry {
    /// Returns a new [`InstrumentedObjectStoreRegistry`] that wraps the provided
    /// [`ObjectStoreRegistry`]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DefaultObjectStoreRegistry::new()),
            instrument_mode: AtomicU8::new(InstrumentedObjectStoreMode::default() as u8),
            stores: RwLock::new(Vec::new()),
        }
    }

    pub fn with_profile_mode(self, mode: InstrumentedObjectStoreMode) -> Self {
        self.instrument_mode.store(mode as u8, Ordering::Relaxed);
        self
    }

    /// Provides access to all of the [`InstrumentedObjectStore`]s managed by this
    /// [`InstrumentedObjectStoreRegistry`]
    pub fn stores(&self) -> Vec<Arc<InstrumentedObjectStore>> {
        self.stores.read().clone()
    }

    /// Returns the current [`InstrumentedObjectStoreMode`] for this
    /// [`InstrumentedObjectStoreRegistry`]
    pub fn instrument_mode(&self) -> InstrumentedObjectStoreMode {
        self.instrument_mode.load(Ordering::Relaxed).into()
    }

    /// Sets the [`InstrumentedObjectStoreMode`] for this [`InstrumentedObjectStoreRegistry`]
    pub fn set_instrument_mode(&self, mode: InstrumentedObjectStoreMode) {
        self.instrument_mode.store(mode as u8, Ordering::Relaxed);
        for s in self.stores.read().iter() {
            s.set_instrument_mode(mode)
        }
    }
}

impl ObjectStoreRegistry for InstrumentedObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let mode = self.instrument_mode.load(Ordering::Relaxed);
        let instrumented =
            Arc::new(InstrumentedObjectStore::new(store, AtomicU8::new(mode)));
        self.stores.write().push(Arc::clone(&instrumented));
        self.inner.register_store(url, instrumented)
    }

    fn get_store(&self, url: &Url) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        self.inner.get_store(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instrumented_mode() {
        assert!(matches!(
            InstrumentedObjectStoreMode::default(),
            InstrumentedObjectStoreMode::Disabled
        ));

        assert!(matches!(
            "dIsABleD".parse().unwrap(),
            InstrumentedObjectStoreMode::Disabled
        ));
        assert!(matches!(
            "EnABlEd".parse().unwrap(),
            InstrumentedObjectStoreMode::Enabled
        ));
        assert!("does_not_exist"
            .parse::<InstrumentedObjectStoreMode>()
            .is_err());

        assert!(matches!(0.into(), InstrumentedObjectStoreMode::Disabled));
        assert!(matches!(1.into(), InstrumentedObjectStoreMode::Enabled));
        assert!(matches!(2.into(), InstrumentedObjectStoreMode::Disabled));
    }

    #[test]
    fn instrumented_registry() {
        let mut reg = InstrumentedObjectStoreRegistry::new();
        assert!(reg.stores().is_empty());
        assert_eq!(
            reg.instrument_mode(),
            InstrumentedObjectStoreMode::default()
        );

        reg = reg.with_profile_mode(InstrumentedObjectStoreMode::Enabled);
        assert_eq!(reg.instrument_mode(), InstrumentedObjectStoreMode::Enabled);

        let store = object_store::memory::InMemory::new();
        let url = "mem://test".parse().unwrap();
        let registered = reg.register_store(&url, Arc::new(store));
        assert!(registered.is_none());

        let fetched = reg.get_store(&url);
        assert!(fetched.is_ok());
        assert_eq!(reg.stores().len(), 1);
    }

    #[tokio::test]
    async fn instrumented_store() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let mode = AtomicU8::new(InstrumentedObjectStoreMode::default() as u8);
        let instrumented = InstrumentedObjectStore::new(store, mode);

        // Load the test store with some data we can read
        let path = Path::from("test/data");
        let payload = PutPayload::from_static(b"test_data");
        instrumented.put(&path, payload).await.unwrap();

        // By default no requests should be instrumented/stored
        assert!(instrumented.requests.lock().is_empty());
        let _ = instrumented.get(&path).await.unwrap();
        assert!(instrumented.requests.lock().is_empty());

        instrumented.set_instrument_mode(InstrumentedObjectStoreMode::Enabled);
        assert!(instrumented.requests.lock().is_empty());
        let _ = instrumented.get(&path).await.unwrap();
        assert_eq!(instrumented.requests.lock().len(), 1);

        let mut requests = instrumented.take_requests();
        assert_eq!(requests.len(), 1);
        assert!(instrumented.requests.lock().is_empty());

        let request = requests.pop().unwrap();
        assert_eq!(request.op, Operation::Get);
        assert_eq!(request.path, path);
        assert!(request.duration.is_some());
        assert_eq!(request.size, Some(9));
        assert_eq!(request.range, None);
        assert!(request.extra_display.is_none());
    }

    #[test]
    fn request_details() {
        let rd = RequestDetails {
            op: Operation::Get,
            path: Path::from("test"),
            timestamp: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            duration: Some(Duration::new(5, 0)),
            size: Some(10),
            range: Some((..10).into()),
            extra_display: Some(String::from("extra info")),
        };

        assert_eq!(
            format!("{rd}"),
            "1970-01-01T00:00:00+00:00 operation=Get duration=5.000000s size=10 range: bytes=0-9 path=test extra info"
        );
    }

    #[test]
    fn request_summary() {
        // Test empty request list
        let mut requests = Vec::new();
        let summaries = RequestSummary::summarize_by_operation(&requests);
        assert!(summaries.is_empty());

        requests.push(RequestDetails {
            op: Operation::Get,
            path: Path::from("test1"),
            timestamp: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            duration: Some(Duration::from_secs(5)),
            size: Some(100),
            range: None,
            extra_display: None,
        });

        let summaries = RequestSummary::summarize_by_operation(&requests);
        assert_eq!(summaries.len(), 1);

        let summary = summaries.get(&Operation::Get).unwrap();
        assert_eq!(summary.count, 1);
        assert_eq!(
            summary.duration_stats.as_ref().unwrap().min,
            Duration::from_secs(5)
        );
        assert_eq!(
            summary.duration_stats.as_ref().unwrap().max,
            Duration::from_secs(5)
        );
        assert_eq!(
            summary.duration_stats.as_ref().unwrap().sum,
            Duration::from_secs(5)
        );
        assert_eq!(summary.size_stats.as_ref().unwrap().min, 100);
        assert_eq!(summary.size_stats.as_ref().unwrap().max, 100);
        assert_eq!(summary.size_stats.as_ref().unwrap().sum, 100);

        // Add more Get requests to test aggregation
        requests.push(RequestDetails {
            op: Operation::Get,
            path: Path::from("test2"),
            timestamp: chrono::DateTime::from_timestamp(1, 0).unwrap(),
            duration: Some(Duration::from_secs(8)),
            size: Some(150),
            range: None,
            extra_display: None,
        });
        requests.push(RequestDetails {
            op: Operation::Get,
            path: Path::from("test3"),
            timestamp: chrono::DateTime::from_timestamp(2, 0).unwrap(),
            duration: Some(Duration::from_secs(2)),
            size: Some(50),
            range: None,
            extra_display: None,
        });

        let summaries = RequestSummary::summarize_by_operation(&requests);
        assert_eq!(summaries.len(), 1);

        let summary = summaries.get(&Operation::Get).unwrap();
        assert_eq!(summary.count, 3);
        assert_eq!(
            summary.duration_stats.as_ref().unwrap().min,
            Duration::from_secs(2)
        );
        assert_eq!(
            summary.duration_stats.as_ref().unwrap().max,
            Duration::from_secs(8)
        );
        assert_eq!(
            summary.duration_stats.as_ref().unwrap().sum,
            Duration::from_secs(15)
        );
        assert_eq!(summary.size_stats.as_ref().unwrap().min, 50);
        assert_eq!(summary.size_stats.as_ref().unwrap().max, 150);
        assert_eq!(summary.size_stats.as_ref().unwrap().sum, 300);

        // Add Put requests to test grouping
        requests.push(RequestDetails {
            op: Operation::_Put,
            path: Path::from("test4"),
            timestamp: chrono::DateTime::from_timestamp(3, 0).unwrap(),
            duration: Some(Duration::from_millis(200)),
            size: Some(75),
            range: None,
            extra_display: None,
        });

        let summaries = RequestSummary::summarize_by_operation(&requests);
        assert_eq!(summaries.len(), 2);

        let get_summary = summaries.get(&Operation::Get).unwrap();
        assert_eq!(get_summary.count, 3);

        let put_summary = summaries.get(&Operation::_Put).unwrap();
        assert_eq!(put_summary.count, 1);
        assert_eq!(
            put_summary.duration_stats.as_ref().unwrap().min,
            Duration::from_millis(200)
        );
        assert_eq!(put_summary.size_stats.as_ref().unwrap().sum, 75);

        // Test request with only duration (no size)
        let only_duration = vec![RequestDetails {
            op: Operation::Get,
            path: Path::from("test1"),
            timestamp: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            duration: Some(Duration::from_secs(3)),
            size: None,
            range: None,
            extra_display: None,
        }];
        let summaries = RequestSummary::summarize_by_operation(&only_duration);
        let summary = summaries.get(&Operation::Get).unwrap();
        assert_eq!(summary.count, 1);
        assert!(summary.duration_stats.is_some());
        assert!(summary.size_stats.is_none());

        // Test request with only size (no duration)
        let only_size = vec![RequestDetails {
            op: Operation::Get,
            path: Path::from("test1"),
            timestamp: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            duration: None,
            size: Some(200),
            range: None,
            extra_display: None,
        }];
        let summaries = RequestSummary::summarize_by_operation(&only_size);
        let summary = summaries.get(&Operation::Get).unwrap();
        assert_eq!(summary.count, 1);
        assert!(summary.duration_stats.is_none());
        assert!(summary.size_stats.is_some());
        assert_eq!(summary.size_stats.as_ref().unwrap().sum, 200);

        // Test request with neither duration nor size
        let no_stats = vec![RequestDetails {
            op: Operation::Get,
            path: Path::from("test1"),
            timestamp: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            duration: None,
            size: None,
            range: None,
            extra_display: None,
        }];
        let summaries = RequestSummary::summarize_by_operation(&no_stats);
        let summary = summaries.get(&Operation::Get).unwrap();
        assert_eq!(summary.count, 1);
        assert!(summary.duration_stats.is_none());
        assert!(summary.size_stats.is_none());
    }
}
