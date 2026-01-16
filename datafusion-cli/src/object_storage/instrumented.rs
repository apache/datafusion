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
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use chrono::Utc;
use datafusion::{
    common::{HashMap, instant::Instant},
    error::DataFusionError,
    execution::object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
};
use futures::stream::{BoxStream, Stream};
use object_store::{
    GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    path::Path,
};
use parking_lot::{Mutex, RwLock};
use url::Url;

/// A stream wrapper that measures the time until the first response(item or end of stream) is yielded
struct TimeToFirstItemStream<S> {
    inner: S,
    start: Instant,
    request_index: usize,
    requests: Arc<Mutex<Vec<RequestDetails>>>,
    first_item_yielded: bool,
}

impl<S> TimeToFirstItemStream<S> {
    fn new(
        inner: S,
        start: Instant,
        request_index: usize,
        requests: Arc<Mutex<Vec<RequestDetails>>>,
    ) -> Self {
        Self {
            inner,
            start,
            request_index,
            requests,
            first_item_yielded: false,
        }
    }
}

impl<S> Stream for TimeToFirstItemStream<S>
where
    S: Stream<Item = Result<ObjectMeta>> + Unpin,
{
    type Item = Result<ObjectMeta>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let poll_result = std::pin::Pin::new(&mut self.inner).poll_next(cx);

        if !self.first_item_yielded && poll_result.is_ready() {
            self.first_item_yielded = true;
            let elapsed = self.start.elapsed();

            let mut requests = self.requests.lock();
            if let Some(request) = requests.get_mut(self.request_index) {
                request.duration = Some(elapsed);
            }
        }

        poll_result
    }
}

/// The profiling mode to use for an [`InstrumentedObjectStore`] instance. Collecting profiling
/// data will have a small negative impact on both CPU and memory usage. Default is `Disabled`
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum InstrumentedObjectStoreMode {
    /// Disable collection of profiling data
    #[default]
    Disabled,
    /// Enable collection of profiling data and output a summary
    Summary,
    /// Enable collection of profiling data and output a summary and all details
    Trace,
}

impl fmt::Display for InstrumentedObjectStoreMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl FromStr for InstrumentedObjectStoreMode {
    type Err = DataFusionError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("disabled") {
            Ok(Self::Disabled)
        } else if s.eq_ignore_ascii_case("summary") {
            Ok(Self::Summary)
        } else if s.eq_ignore_ascii_case("trace") {
            Ok(Self::Trace)
        } else {
            Err(DataFusionError::Execution(format!("Unrecognized mode {s}")))
        }
    }
}

impl From<u8> for InstrumentedObjectStoreMode {
    fn from(value: u8) -> Self {
        match value {
            1 => InstrumentedObjectStoreMode::Summary,
            2 => InstrumentedObjectStoreMode::Trace,
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
    requests: Arc<Mutex<Vec<RequestDetails>>>,
}

impl InstrumentedObjectStore {
    /// Returns a new [`InstrumentedObjectStore`] that wraps the provided [`ObjectStore`]
    fn new(object_store: Arc<dyn ObjectStore>, instrument_mode: AtomicU8) -> Self {
        Self {
            inner: object_store,
            instrument_mode,
            requests: Arc::new(Mutex::new(Vec::new())),
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

    fn enabled(&self) -> bool {
        self.instrument_mode.load(Ordering::Relaxed)
            != InstrumentedObjectStoreMode::Disabled as u8
    }

    async fn instrumented_put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let timestamp = Utc::now();
        let start = Instant::now();
        let size = payload.content_length();
        let ret = self.inner.put_opts(location, payload, opts).await?;
        let elapsed = start.elapsed();

        self.requests.lock().push(RequestDetails {
            op: Operation::Put,
            path: location.clone(),
            timestamp,
            duration: Some(elapsed),
            size: Some(size),
            range: None,
            extra_display: None,
        });

        Ok(ret)
    }

    async fn instrumented_put_multipart(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let timestamp = Utc::now();
        let start = Instant::now();
        let ret = self.inner.put_multipart_opts(location, opts).await?;
        let elapsed = start.elapsed();

        self.requests.lock().push(RequestDetails {
            op: Operation::Put,
            path: location.clone(),
            timestamp,
            duration: Some(elapsed),
            size: None,
            range: None,
            extra_display: None,
        });

        Ok(ret)
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

    async fn instrumented_delete(&self, location: &Path) -> Result<()> {
        let timestamp = Utc::now();
        let start = Instant::now();
        self.inner.delete(location).await?;
        let elapsed = start.elapsed();

        self.requests.lock().push(RequestDetails {
            op: Operation::Delete,
            path: location.clone(),
            timestamp,
            duration: Some(elapsed),
            size: None,
            range: None,
            extra_display: None,
        });

        Ok(())
    }

    fn instrumented_list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let timestamp = Utc::now();
        let start = Instant::now();
        let inner_stream = self.inner.list(prefix);

        let request_index = {
            let mut requests = self.requests.lock();
            requests.push(RequestDetails {
                op: Operation::List,
                path: prefix.cloned().unwrap_or_else(|| Path::from("")),
                timestamp,
                duration: None,
                size: None,
                range: None,
                extra_display: None,
            });
            requests.len() - 1
        };

        let wrapped_stream = TimeToFirstItemStream::new(
            inner_stream,
            start,
            request_index,
            Arc::clone(&self.requests),
        );

        Box::pin(wrapped_stream)
    }

    async fn instrumented_list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult> {
        let timestamp = Utc::now();
        let start = Instant::now();
        let ret = self.inner.list_with_delimiter(prefix).await?;
        let elapsed = start.elapsed();

        self.requests.lock().push(RequestDetails {
            op: Operation::List,
            path: prefix.cloned().unwrap_or_else(|| Path::from("")),
            timestamp,
            duration: Some(elapsed),
            size: None,
            range: None,
            extra_display: None,
        });

        Ok(ret)
    }

    async fn instrumented_copy(&self, from: &Path, to: &Path) -> Result<()> {
        let timestamp = Utc::now();
        let start = Instant::now();
        self.inner.copy(from, to).await?;
        let elapsed = start.elapsed();

        self.requests.lock().push(RequestDetails {
            op: Operation::Copy,
            path: from.clone(),
            timestamp,
            duration: Some(elapsed),
            size: None,
            range: None,
            extra_display: Some(format!("copy_to: {to}")),
        });

        Ok(())
    }

    async fn instrumented_copy_if_not_exists(
        &self,
        from: &Path,
        to: &Path,
    ) -> Result<()> {
        let timestamp = Utc::now();
        let start = Instant::now();
        self.inner.copy_if_not_exists(from, to).await?;
        let elapsed = start.elapsed();

        self.requests.lock().push(RequestDetails {
            op: Operation::Copy,
            path: from.clone(),
            timestamp,
            duration: Some(elapsed),
            size: None,
            range: None,
            extra_display: Some(format!("copy_to: {to}")),
        });

        Ok(())
    }

    async fn instrumented_head(&self, location: &Path) -> Result<ObjectMeta> {
        let timestamp = Utc::now();
        let start = Instant::now();
        let ret = self.inner.head(location).await?;
        let elapsed = start.elapsed();

        self.requests.lock().push(RequestDetails {
            op: Operation::Head,
            path: location.clone(),
            timestamp,
            duration: Some(elapsed),
            size: None,
            range: None,
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
        if self.enabled() {
            return self.instrumented_put_opts(location, payload, opts).await;
        }

        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        if self.enabled() {
            return self.instrumented_put_multipart(location, opts).await;
        }

        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if self.enabled() {
            return self.instrumented_get_opts(location, options).await;
        }

        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        if self.enabled() {
            return self.instrumented_delete(location).await;
        }

        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        if self.enabled() {
            return self.instrumented_list(prefix);
        }

        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        if self.enabled() {
            return self.instrumented_list_with_delimiter(prefix).await;
        }

        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        if self.enabled() {
            return self.instrumented_copy(from, to).await;
        }

        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        if self.enabled() {
            return self.instrumented_copy_if_not_exists(from, to).await;
        }

        self.inner.copy_if_not_exists(from, to).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        if self.enabled() {
            return self.instrumented_head(location).await;
        }

        self.inner.head(location).await
    }
}

/// Object store operation types tracked by [`InstrumentedObjectStore`]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Operation {
    Copy,
    Delete,
    Get,
    Head,
    List,
    Put,
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
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

/// Summary statistics for all requests recorded in an [`InstrumentedObjectStore`]
#[derive(Default)]
pub struct RequestSummaries {
    summaries: Vec<RequestSummary>,
}

/// Display the summary as a table
impl fmt::Display for RequestSummaries {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Don't expect an error, but avoid panicking if it happens
        match pretty_format_batches(&[self.to_batch()]) {
            Err(e) => {
                write!(f, "Error formatting summary: {e}")
            }
            Ok(displayable) => {
                write!(f, "{displayable}")
            }
        }
    }
}

impl RequestSummaries {
    /// Summarizes input [`RequestDetails`]
    pub fn new(requests: &[RequestDetails]) -> Self {
        let mut summaries: HashMap<Operation, RequestSummary> = HashMap::new();
        for rd in requests {
            match summaries.get_mut(&rd.op) {
                Some(rs) => rs.push(rd),
                None => {
                    let mut rs = RequestSummary::new(rd.op);
                    rs.push(rd);
                    summaries.insert(rd.op, rs);
                }
            }
        }
        // Convert to a Vec with consistent ordering
        let mut summaries: Vec<RequestSummary> = summaries.into_values().collect();
        summaries.sort_by_key(|s| s.operation);
        Self { summaries }
    }

    /// Convert the summaries into a `RecordBatch` for display
    ///
    /// Results in a table like:
    /// ```text
    /// +-----------+----------+-----------+-----------+-----------+-----------+-----------+
    /// | Operation | Metric   | min       | max       | avg       | sum       | count     |
    /// +-----------+----------+-----------+-----------+-----------+-----------+-----------+
    /// | Get       | duration | 5.000000s | 5.000000s | 5.000000s |           | 1         |
    /// | Get       | size     | 100 B     | 100 B     | 100 B     | 100 B     | 1         |
    /// +-----------+----------+-----------+-----------+-----------+-----------+-----------+
    /// ```
    pub fn to_batch(&self) -> RecordBatch {
        let operations: StringArray = self
            .iter()
            .flat_map(|s| std::iter::repeat_n(Some(s.operation.to_string()), 2))
            .collect();
        let metrics: StringArray = self
            .iter()
            .flat_map(|_s| [Some("duration"), Some("size")])
            .collect();
        let mins: StringArray = self
            .stats_iter()
            .flat_map(|(duration_stats, size_stats)| {
                let dur_min =
                    duration_stats.map(|d| format!("{:.6}s", d.min.as_secs_f32()));
                let size_min = size_stats.map(|s| format!("{} B", s.min));
                [dur_min, size_min]
            })
            .collect();
        let maxs: StringArray = self
            .stats_iter()
            .flat_map(|(duration_stats, size_stats)| {
                let dur_max =
                    duration_stats.map(|d| format!("{:.6}s", d.max.as_secs_f32()));
                let size_max = size_stats.map(|s| format!("{} B", s.max));
                [dur_max, size_max]
            })
            .collect();
        let avgs: StringArray = self
            .iter()
            .flat_map(|s| {
                let count = s.count as f32;
                let duration_stats = s.duration_stats.as_ref();
                let size_stats = s.size_stats.as_ref();
                let dur_avg = duration_stats.map(|d| {
                    let avg = d.sum.as_secs_f32() / count;
                    format!("{avg:.6}s")
                });
                let size_avg = size_stats.map(|s| {
                    let avg = s.sum as f32 / count;
                    format!("{avg} B")
                });
                [dur_avg, size_avg]
            })
            .collect();
        let sums: StringArray = self
            .stats_iter()
            .flat_map(|(duration_stats, size_stats)| {
                // Omit a sum stat for duration in the initial
                // implementation because it can be a bit misleading (at least
                // at first glance). For example, particularly large queries the
                // sum of the durations was often larger than the total time of
                // the query itself, can be confusing without additional
                // explanation (e.g. that the sum is of individual requests,
                // which may be concurrent).
                let dur_sum =
                    duration_stats.map(|d| format!("{:.6}s", d.sum.as_secs_f32()));
                let size_sum = size_stats.map(|s| format!("{} B", s.sum));
                [dur_sum, size_sum]
            })
            .collect();
        let counts: StringArray = self
            .iter()
            .flat_map(|s| {
                let count = s.count.to_string();
                [Some(count.clone()), Some(count)]
            })
            .collect();

        RecordBatch::try_from_iter(vec![
            ("Operation", Arc::new(operations) as ArrayRef),
            ("Metric", Arc::new(metrics) as ArrayRef),
            ("min", Arc::new(mins) as ArrayRef),
            ("max", Arc::new(maxs) as ArrayRef),
            ("avg", Arc::new(avgs) as ArrayRef),
            ("sum", Arc::new(sums) as ArrayRef),
            ("count", Arc::new(counts) as ArrayRef),
        ])
        .expect("Created the batch correctly")
    }

    /// Return an iterator over the summaries
    fn iter(&self) -> impl Iterator<Item = &RequestSummary> {
        self.summaries.iter()
    }

    /// Return an iterator over (duration_stats, size_stats) tuples
    /// for each summary
    fn stats_iter(
        &self,
    ) -> impl Iterator<Item = (Option<&Stats<Duration>>, Option<&Stats<usize>>)> {
        self.summaries
            .iter()
            .map(|s| (s.duration_stats.as_ref(), s.size_stats.as_ref()))
    }
}

/// Summary statistics for a particular type of [`Operation`] (e.g. `GET` or `PUT`)
/// in an [`InstrumentedObjectStore`]'s [`RequestDetails`]
pub struct RequestSummary {
    operation: Operation,
    count: usize,
    duration_stats: Option<Stats<Duration>>,
    size_stats: Option<Stats<usize>>,
}

impl RequestSummary {
    fn new(operation: Operation) -> Self {
        Self {
            operation,
            count: 0,
            duration_stats: None,
            size_stats: None,
        }
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

    fn deregister_store(
        &self,
        url: &Url,
    ) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        self.inner.deregister_store(url)
    }

    fn get_store(&self, url: &Url) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        self.inner.get_store(url)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use object_store::WriteMultipart;

    use super::*;
    use insta::assert_snapshot;

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
            "SUmMaRy".parse().unwrap(),
            InstrumentedObjectStoreMode::Summary
        ));
        assert!(matches!(
            "TRaCe".parse().unwrap(),
            InstrumentedObjectStoreMode::Trace
        ));
        assert!(
            "does_not_exist"
                .parse::<InstrumentedObjectStoreMode>()
                .is_err()
        );

        assert!(matches!(0.into(), InstrumentedObjectStoreMode::Disabled));
        assert!(matches!(1.into(), InstrumentedObjectStoreMode::Summary));
        assert!(matches!(2.into(), InstrumentedObjectStoreMode::Trace));
        assert!(matches!(3.into(), InstrumentedObjectStoreMode::Disabled));
    }

    #[test]
    fn instrumented_registry() {
        let mut reg = InstrumentedObjectStoreRegistry::new();
        assert!(reg.stores().is_empty());
        assert_eq!(
            reg.instrument_mode(),
            InstrumentedObjectStoreMode::default()
        );

        reg = reg.with_profile_mode(InstrumentedObjectStoreMode::Trace);
        assert_eq!(reg.instrument_mode(), InstrumentedObjectStoreMode::Trace);

        let store = object_store::memory::InMemory::new();
        let url = "mem://test".parse().unwrap();
        let registered = reg.register_store(&url, Arc::new(store));
        assert!(registered.is_none());

        let fetched = reg.get_store(&url);
        assert!(fetched.is_ok());
        assert_eq!(reg.stores().len(), 1);
    }

    // Returns an `InstrumentedObjectStore` with some data loaded for testing and the path to
    // access the data
    async fn setup_test_store() -> (InstrumentedObjectStore, Path) {
        let store = Arc::new(object_store::memory::InMemory::new());
        let mode = AtomicU8::new(InstrumentedObjectStoreMode::default() as u8);
        let instrumented = InstrumentedObjectStore::new(store, mode);

        // Load the test store with some data we can read
        let path = Path::from("test/data");
        let payload = PutPayload::from_static(b"test_data");
        instrumented.put(&path, payload).await.unwrap();

        (instrumented, path)
    }

    #[tokio::test]
    async fn instrumented_store_get() {
        let (instrumented, path) = setup_test_store().await;

        // By default no requests should be instrumented/stored
        assert!(instrumented.requests.lock().is_empty());
        let _ = instrumented.get(&path).await.unwrap();
        assert!(instrumented.requests.lock().is_empty());

        instrumented.set_instrument_mode(InstrumentedObjectStoreMode::Trace);
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

    #[tokio::test]
    async fn instrumented_store_delete() {
        let (instrumented, path) = setup_test_store().await;

        // By default no requests should be instrumented/stored
        assert!(instrumented.requests.lock().is_empty());
        instrumented.delete(&path).await.unwrap();
        assert!(instrumented.requests.lock().is_empty());

        // We need a new store so we have data to delete again
        let (instrumented, path) = setup_test_store().await;
        instrumented.set_instrument_mode(InstrumentedObjectStoreMode::Trace);
        assert!(instrumented.requests.lock().is_empty());
        instrumented.delete(&path).await.unwrap();
        assert_eq!(instrumented.requests.lock().len(), 1);

        let mut requests = instrumented.take_requests();
        assert_eq!(requests.len(), 1);
        assert!(instrumented.requests.lock().is_empty());

        let request = requests.pop().unwrap();
        assert_eq!(request.op, Operation::Delete);
        assert_eq!(request.path, path);
        assert!(request.duration.is_some());
        assert!(request.size.is_none());
        assert!(request.range.is_none());
        assert!(request.extra_display.is_none());
    }

    #[tokio::test]
    async fn instrumented_store_list() {
        let (instrumented, path) = setup_test_store().await;

        // By default no requests should be instrumented/stored
        assert!(instrumented.requests.lock().is_empty());
        let _ = instrumented.list(Some(&path));
        assert!(instrumented.requests.lock().is_empty());

        instrumented.set_instrument_mode(InstrumentedObjectStoreMode::Trace);
        assert!(instrumented.requests.lock().is_empty());
        let mut stream = instrumented.list(Some(&path));
        // Consume at least one item from the stream to trigger duration measurement
        let _ = stream.next().await;
        assert_eq!(instrumented.requests.lock().len(), 1);

        let request = instrumented.take_requests().pop().unwrap();
        assert_eq!(request.op, Operation::List);
        assert_eq!(request.path, path);
        assert!(request.duration.is_some());
        assert!(request.size.is_none());
        assert!(request.range.is_none());
        assert!(request.extra_display.is_none());
    }

    #[tokio::test]
    async fn instrumented_store_list_with_delimiter() {
        let (instrumented, path) = setup_test_store().await;

        // By default no requests should be instrumented/stored
        assert!(instrumented.requests.lock().is_empty());
        let _ = instrumented.list_with_delimiter(Some(&path)).await.unwrap();
        assert!(instrumented.requests.lock().is_empty());

        instrumented.set_instrument_mode(InstrumentedObjectStoreMode::Trace);
        assert!(instrumented.requests.lock().is_empty());
        let _ = instrumented.list_with_delimiter(Some(&path)).await.unwrap();
        assert_eq!(instrumented.requests.lock().len(), 1);

        let request = instrumented.take_requests().pop().unwrap();
        assert_eq!(request.op, Operation::List);
        assert_eq!(request.path, path);
        assert!(request.duration.is_some());
        assert!(request.size.is_none());
        assert!(request.range.is_none());
        assert!(request.extra_display.is_none());
    }

    #[tokio::test]
    async fn instrumented_store_put_opts() {
        // The `setup_test_store()` method comes with data already `put` into it, so we'll setup
        // manually for this test
        let store = Arc::new(object_store::memory::InMemory::new());
        let mode = AtomicU8::new(InstrumentedObjectStoreMode::default() as u8);
        let instrumented = InstrumentedObjectStore::new(store, mode);

        let path = Path::from("test/data");
        let payload = PutPayload::from_static(b"test_data");
        let size = payload.content_length();

        // By default no requests should be instrumented/stored
        assert!(instrumented.requests.lock().is_empty());
        instrumented.put(&path, payload.clone()).await.unwrap();
        assert!(instrumented.requests.lock().is_empty());

        instrumented.set_instrument_mode(InstrumentedObjectStoreMode::Trace);
        assert!(instrumented.requests.lock().is_empty());
        instrumented.put(&path, payload).await.unwrap();
        assert_eq!(instrumented.requests.lock().len(), 1);

        let request = instrumented.take_requests().pop().unwrap();
        assert_eq!(request.op, Operation::Put);
        assert_eq!(request.path, path);
        assert!(request.duration.is_some());
        assert_eq!(request.size.unwrap(), size);
        assert!(request.range.is_none());
        assert!(request.extra_display.is_none());
    }

    #[tokio::test]
    async fn instrumented_store_put_multipart() {
        // The `setup_test_store()` method comes with data already `put` into it, so we'll setup
        // manually for this test
        let store = Arc::new(object_store::memory::InMemory::new());
        let mode = AtomicU8::new(InstrumentedObjectStoreMode::default() as u8);
        let instrumented = InstrumentedObjectStore::new(store, mode);

        let path = Path::from("test/data");

        // By default no requests should be instrumented/stored
        assert!(instrumented.requests.lock().is_empty());
        let mp = instrumented.put_multipart(&path).await.unwrap();
        let mut write = WriteMultipart::new(mp);
        write.write(b"test_data");
        write.finish().await.unwrap();
        assert!(instrumented.requests.lock().is_empty());

        instrumented.set_instrument_mode(InstrumentedObjectStoreMode::Trace);
        assert!(instrumented.requests.lock().is_empty());
        let mp = instrumented.put_multipart(&path).await.unwrap();
        let mut write = WriteMultipart::new(mp);
        write.write(b"test_data");
        write.finish().await.unwrap();
        assert_eq!(instrumented.requests.lock().len(), 1);

        let request = instrumented.take_requests().pop().unwrap();
        assert_eq!(request.op, Operation::Put);
        assert_eq!(request.path, path);
        assert!(request.duration.is_some());
        assert!(request.size.is_none());
        assert!(request.range.is_none());
        assert!(request.extra_display.is_none());
    }

    #[tokio::test]
    async fn instrumented_store_copy() {
        let (instrumented, path) = setup_test_store().await;
        let copy_to = Path::from("test/copied");

        // By default no requests should be instrumented/stored
        assert!(instrumented.requests.lock().is_empty());
        instrumented.copy(&path, &copy_to).await.unwrap();
        assert!(instrumented.requests.lock().is_empty());

        instrumented.set_instrument_mode(InstrumentedObjectStoreMode::Trace);
        assert!(instrumented.requests.lock().is_empty());
        instrumented.copy(&path, &copy_to).await.unwrap();
        assert_eq!(instrumented.requests.lock().len(), 1);

        let mut requests = instrumented.take_requests();
        assert_eq!(requests.len(), 1);
        assert!(instrumented.requests.lock().is_empty());

        let request = requests.pop().unwrap();
        assert_eq!(request.op, Operation::Copy);
        assert_eq!(request.path, path);
        assert!(request.duration.is_some());
        assert!(request.size.is_none());
        assert!(request.range.is_none());
        assert_eq!(
            request.extra_display.unwrap(),
            format!("copy_to: {copy_to}")
        );
    }

    #[tokio::test]
    async fn instrumented_store_copy_if_not_exists() {
        let (instrumented, path) = setup_test_store().await;
        let mut copy_to = Path::from("test/copied");

        // By default no requests should be instrumented/stored
        assert!(instrumented.requests.lock().is_empty());
        instrumented
            .copy_if_not_exists(&path, &copy_to)
            .await
            .unwrap();
        assert!(instrumented.requests.lock().is_empty());

        // Use a new destination since the previous one already exists
        copy_to = Path::from("test/copied_again");
        instrumented.set_instrument_mode(InstrumentedObjectStoreMode::Trace);
        assert!(instrumented.requests.lock().is_empty());
        instrumented
            .copy_if_not_exists(&path, &copy_to)
            .await
            .unwrap();
        assert_eq!(instrumented.requests.lock().len(), 1);

        let mut requests = instrumented.take_requests();
        assert_eq!(requests.len(), 1);
        assert!(instrumented.requests.lock().is_empty());

        let request = requests.pop().unwrap();
        assert_eq!(request.op, Operation::Copy);
        assert_eq!(request.path, path);
        assert!(request.duration.is_some());
        assert!(request.size.is_none());
        assert!(request.range.is_none());
        assert_eq!(
            request.extra_display.unwrap(),
            format!("copy_to: {copy_to}")
        );
    }

    #[tokio::test]
    async fn instrumented_store_head() {
        let (instrumented, path) = setup_test_store().await;

        // By default no requests should be instrumented/stored
        assert!(instrumented.requests.lock().is_empty());
        let _ = instrumented.head(&path).await.unwrap();
        assert!(instrumented.requests.lock().is_empty());

        instrumented.set_instrument_mode(InstrumentedObjectStoreMode::Trace);
        assert!(instrumented.requests.lock().is_empty());
        let _ = instrumented.head(&path).await.unwrap();
        assert_eq!(instrumented.requests.lock().len(), 1);

        let mut requests = instrumented.take_requests();
        assert_eq!(requests.len(), 1);
        assert!(instrumented.requests.lock().is_empty());

        let request = requests.pop().unwrap();
        assert_eq!(request.op, Operation::Head);
        assert_eq!(request.path, path);
        assert!(request.duration.is_some());
        assert!(request.size.is_none());
        assert!(request.range.is_none());
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
        assert_snapshot!(RequestSummaries::new(&requests), @r"
        +-----------+--------+-----+-----+-----+-----+-------+
        | Operation | Metric | min | max | avg | sum | count |
        +-----------+--------+-----+-----+-----+-----+-------+
        +-----------+--------+-----+-----+-----+-----+-------+
        ");

        requests.push(RequestDetails {
            op: Operation::Get,
            path: Path::from("test1"),
            timestamp: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            duration: Some(Duration::from_secs(5)),
            size: Some(100),
            range: None,
            extra_display: None,
        });

        assert_snapshot!(RequestSummaries::new(&requests), @r"
        +-----------+----------+-----------+-----------+-----------+-----------+-------+
        | Operation | Metric   | min       | max       | avg       | sum       | count |
        +-----------+----------+-----------+-----------+-----------+-----------+-------+
        | Get       | duration | 5.000000s | 5.000000s | 5.000000s | 5.000000s | 1     |
        | Get       | size     | 100 B     | 100 B     | 100 B     | 100 B     | 1     |
        +-----------+----------+-----------+-----------+-----------+-----------+-------+
        ");

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
        assert_snapshot!(RequestSummaries::new(&requests), @r"
        +-----------+----------+-----------+-----------+-----------+------------+-------+
        | Operation | Metric   | min       | max       | avg       | sum        | count |
        +-----------+----------+-----------+-----------+-----------+------------+-------+
        | Get       | duration | 2.000000s | 8.000000s | 5.000000s | 15.000000s | 3     |
        | Get       | size     | 50 B      | 150 B     | 100 B     | 300 B      | 3     |
        +-----------+----------+-----------+-----------+-----------+------------+-------+
        ");

        // Add Put requests to test grouping
        requests.push(RequestDetails {
            op: Operation::Put,
            path: Path::from("test4"),
            timestamp: chrono::DateTime::from_timestamp(3, 0).unwrap(),
            duration: Some(Duration::from_millis(200)),
            size: Some(75),
            range: None,
            extra_display: None,
        });

        assert_snapshot!(RequestSummaries::new(&requests), @r"
        +-----------+----------+-----------+-----------+-----------+------------+-------+
        | Operation | Metric   | min       | max       | avg       | sum        | count |
        +-----------+----------+-----------+-----------+-----------+------------+-------+
        | Get       | duration | 2.000000s | 8.000000s | 5.000000s | 15.000000s | 3     |
        | Get       | size     | 50 B      | 150 B     | 100 B     | 300 B      | 3     |
        | Put       | duration | 0.200000s | 0.200000s | 0.200000s | 0.200000s  | 1     |
        | Put       | size     | 75 B      | 75 B      | 75 B      | 75 B       | 1     |
        +-----------+----------+-----------+-----------+-----------+------------+-------+
        ");
    }

    #[test]
    fn request_summary_only_duration() {
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
        assert_snapshot!(RequestSummaries::new(&only_duration), @r"
        +-----------+----------+-----------+-----------+-----------+-----------+-------+
        | Operation | Metric   | min       | max       | avg       | sum       | count |
        +-----------+----------+-----------+-----------+-----------+-----------+-------+
        | Get       | duration | 3.000000s | 3.000000s | 3.000000s | 3.000000s | 1     |
        | Get       | size     |           |           |           |           | 1     |
        +-----------+----------+-----------+-----------+-----------+-----------+-------+
        ");
    }

    #[test]
    fn request_summary_only_size() {
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
        assert_snapshot!(RequestSummaries::new(&only_size), @r"
        +-----------+----------+-------+-------+-------+-------+-------+
        | Operation | Metric   | min   | max   | avg   | sum   | count |
        +-----------+----------+-------+-------+-------+-------+-------+
        | Get       | duration |       |       |       |       | 1     |
        | Get       | size     | 200 B | 200 B | 200 B | 200 B | 1     |
        +-----------+----------+-------+-------+-------+-------+-------+
        ");
    }

    #[test]
    fn request_summary_neither_duration_or_size() {
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
        assert_snapshot!(RequestSummaries::new(&no_stats), @r"
        +-----------+----------+-----+-----+-----+-----+-------+
        | Operation | Metric   | min | max | avg | sum | count |
        +-----------+----------+-----+-----+-----+-----+-------+
        | Get       | duration |     |     |     |     | 1     |
        | Get       | size     |     |     |     |     | 1     |
        +-----------+----------+-----+-----+-----+-----+-------+
        ");
    }
}
