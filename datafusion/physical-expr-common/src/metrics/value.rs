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

//! Value representation of metrics

use super::CustomMetricValue;
use chrono::{DateTime, Utc};
use datafusion_common::{
    human_readable_count, human_readable_duration, human_readable_size, instant::Instant,
};
use parking_lot::Mutex;
use std::{
    borrow::{Borrow, Cow},
    fmt::{Debug, Display},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

/// A counter to record things such as number of input or output rows
///
/// Note `clone`ing counters update the same underlying metrics
#[derive(Debug, Clone)]
pub struct Count {
    /// value of the metric counter
    value: Arc<AtomicUsize>,
}

impl PartialEq for Count {
    fn eq(&self, other: &Self) -> bool {
        self.value().eq(&other.value())
    }
}

impl Display for Count {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", human_readable_count(self.value()))
    }
}

impl Default for Count {
    fn default() -> Self {
        Self::new()
    }
}

impl Count {
    /// create a new counter
    pub fn new() -> Self {
        Self {
            value: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Add `n` to the metric's value
    pub fn add(&self, n: usize) {
        // relaxed ordering for operations on `value` poses no issues
        // we're purely using atomic ops with no associated memory ops
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Get the current value
    pub fn value(&self) -> usize {
        self.value.load(Ordering::Relaxed)
    }
}

/// A gauge is the simplest metrics type. It just returns a value.
/// For example, you can easily expose current memory consumption with a gauge.
///
/// Note `clone`ing gauge update the same underlying metrics
#[derive(Debug, Clone)]
pub struct Gauge {
    /// value of the metric gauge
    value: Arc<AtomicUsize>,
}

impl PartialEq for Gauge {
    fn eq(&self, other: &Self) -> bool {
        self.value().eq(&other.value())
    }
}

impl Display for Gauge {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}

impl Default for Gauge {
    fn default() -> Self {
        Self::new()
    }
}

impl Gauge {
    /// create a new gauge
    pub fn new() -> Self {
        Self {
            value: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Add `n` to the metric's value
    pub fn add(&self, n: usize) {
        // relaxed ordering for operations on `value` poses no issues
        // we're purely using atomic ops with no associated memory ops
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Sub `n` from the metric's value
    pub fn sub(&self, n: usize) {
        // relaxed ordering for operations on `value` poses no issues
        // we're purely using atomic ops with no associated memory ops
        self.value.fetch_sub(n, Ordering::Relaxed);
    }

    /// Set metric's value to maximum of `n` and current value
    pub fn set_max(&self, n: usize) {
        self.value.fetch_max(n, Ordering::Relaxed);
    }

    /// Set the metric's value to `n` and return the previous value
    pub fn set(&self, n: usize) -> usize {
        // relaxed ordering for operations on `value` poses no issues
        // we're purely using atomic ops with no associated memory ops
        self.value.swap(n, Ordering::Relaxed)
    }

    /// Get the current value
    pub fn value(&self) -> usize {
        self.value.load(Ordering::Relaxed)
    }
}

/// Measure a potentially non contiguous duration of time
#[derive(Debug, Clone)]
pub struct Time {
    /// elapsed time, in nanoseconds
    nanos: Arc<AtomicUsize>,
}

impl Default for Time {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for Time {
    fn eq(&self, other: &Self) -> bool {
        self.value().eq(&other.value())
    }
}

impl Display for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", human_readable_duration(self.value() as u64))
    }
}

impl Time {
    /// Create a new [`Time`] wrapper suitable for recording elapsed
    /// times for operations.
    pub fn new() -> Self {
        Self {
            nanos: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Add elapsed nanoseconds since `start`to self
    pub fn add_elapsed(&self, start: Instant) {
        self.add_duration(start.elapsed());
    }

    /// Add duration of time to self
    ///
    /// Note: this will always increment the recorded time by at least 1 nanosecond
    /// to distinguish between the scenario of no values recorded, in which
    /// case the value will be 0, and no measurable amount of time having passed,
    /// in which case the value will be small but not 0.
    ///
    /// This is based on the assumption that the timing logic in most cases is likely
    /// to take at least a nanosecond, and so this is reasonable mechanism to avoid
    /// ambiguity, especially on systems with low-resolution monotonic clocks
    pub fn add_duration(&self, duration: Duration) {
        let more_nanos = duration.as_nanos() as usize;
        self.nanos.fetch_add(more_nanos.max(1), Ordering::Relaxed);
    }

    /// Add the number of nanoseconds of other `Time` to self
    pub fn add(&self, other: &Time) {
        self.add_duration(Duration::from_nanos(other.value() as u64))
    }

    /// return a scoped guard that adds the amount of time elapsed
    /// between its creation and its drop or call to `stop` to the
    /// underlying metric.
    pub fn timer(&self) -> ScopedTimerGuard<'_> {
        ScopedTimerGuard {
            inner: self,
            start: Some(Instant::now()),
        }
    }

    /// Get the number of nanoseconds record by this Time metric
    pub fn value(&self) -> usize {
        self.nanos.load(Ordering::Relaxed)
    }

    /// Return a scoped guard that adds the amount of time elapsed between the
    /// given instant and its drop (or the call to `stop`) to the underlying metric
    pub fn timer_with(&self, now: Instant) -> ScopedTimerGuard<'_> {
        ScopedTimerGuard {
            inner: self,
            start: Some(now),
        }
    }
}

/// Stores a single timestamp, stored as the number of nanoseconds
/// elapsed from Jan 1, 1970 UTC
#[derive(Debug, Clone)]
pub struct Timestamp {
    /// Time thing started
    timestamp: Arc<Mutex<Option<DateTime<Utc>>>>,
}

impl Default for Timestamp {
    fn default() -> Self {
        Self::new()
    }
}

impl Timestamp {
    /// Create a new timestamp and sets its value to 0
    pub fn new() -> Self {
        Self {
            timestamp: Arc::new(Mutex::new(None)),
        }
    }

    /// Sets the timestamps value to the current time
    pub fn record(&self) {
        self.set(Utc::now())
    }

    /// Sets the timestamps value to a specified time
    pub fn set(&self, now: DateTime<Utc>) {
        *self.timestamp.lock() = Some(now);
    }

    /// return the timestamps value at the last time `record()` was
    /// called.
    ///
    /// Returns `None` if `record()` has not been called
    pub fn value(&self) -> Option<DateTime<Utc>> {
        *self.timestamp.lock()
    }

    /// sets the value of this timestamp to the minimum of this and other
    pub fn update_to_min(&self, other: &Timestamp) {
        let min = match (self.value(), other.value()) {
            (None, None) => None,
            (Some(v), None) => Some(v),
            (None, Some(v)) => Some(v),
            (Some(v1), Some(v2)) => Some(if v1 < v2 { v1 } else { v2 }),
        };

        *self.timestamp.lock() = min;
    }

    /// sets the value of this timestamp to the maximum of this and other
    pub fn update_to_max(&self, other: &Timestamp) {
        let max = match (self.value(), other.value()) {
            (None, None) => None,
            (Some(v), None) => Some(v),
            (None, Some(v)) => Some(v),
            (Some(v1), Some(v2)) => Some(if v1 < v2 { v2 } else { v1 }),
        };

        *self.timestamp.lock() = max;
    }
}

impl PartialEq for Timestamp {
    fn eq(&self, other: &Self) -> bool {
        self.value().eq(&other.value())
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.value() {
            None => write!(f, "NONE"),
            Some(v) => {
                write!(f, "{v}")
            }
        }
    }
}

/// RAAI structure that adds all time between its construction and
/// destruction to the CPU time or the first call to `stop` whichever
/// comes first
pub struct ScopedTimerGuard<'a> {
    inner: &'a Time,
    start: Option<Instant>,
}

impl ScopedTimerGuard<'_> {
    /// Stop the timer timing and record the time taken
    pub fn stop(&mut self) {
        if let Some(start) = self.start.take() {
            self.inner.add_elapsed(start)
        }
    }

    /// Restarts the timer recording from the current time
    pub fn restart(&mut self) {
        self.start = Some(Instant::now())
    }

    /// Stop the timer, record the time taken and consume self
    pub fn done(mut self) {
        self.stop()
    }

    /// Stop the timer timing and record the time taken since the given endpoint.
    pub fn stop_with(&mut self, end_time: Instant) {
        if let Some(start) = self.start.take() {
            let elapsed = end_time - start;
            self.inner.add_duration(elapsed)
        }
    }

    /// Stop the timer, record the time taken since `end_time` endpoint, and
    /// consume self.
    pub fn done_with(mut self, end_time: Instant) {
        self.stop_with(end_time)
    }
}

impl Drop for ScopedTimerGuard<'_> {
    fn drop(&mut self) {
        self.stop()
    }
}

/// Counters tracking pruning metrics
///
/// For example, a file scanner initially is planned to scan 10 files, but skipped
/// 8 of them using statistics, the pruning metrics would look like: 10 total -> 2 matched
///
/// Note `clone`ing update the same underlying metrics
#[derive(Debug, Clone)]
pub struct PruningMetrics {
    pruned: Arc<AtomicUsize>,
    matched: Arc<AtomicUsize>,
    fully_matched: Arc<AtomicUsize>,
}

impl Display for PruningMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let matched = self.matched.load(Ordering::Relaxed);
        let total = self.pruned.load(Ordering::Relaxed) + matched;
        let fully_matched = self.fully_matched.load(Ordering::Relaxed);

        if fully_matched != 0 {
            write!(
                f,
                "{} total → {} matched -> {} fully matched",
                human_readable_count(total),
                human_readable_count(matched),
                human_readable_count(fully_matched)
            )
        } else {
            write!(
                f,
                "{} total → {} matched",
                human_readable_count(total),
                human_readable_count(matched)
            )
        }
    }
}

impl Default for PruningMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PruningMetrics {
    /// create a new PruningMetrics
    pub fn new() -> Self {
        Self {
            pruned: Arc::new(AtomicUsize::new(0)),
            matched: Arc::new(AtomicUsize::new(0)),
            fully_matched: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Add `n` to the metric's pruned value
    pub fn add_pruned(&self, n: usize) {
        // relaxed ordering for operations on `value` poses no issues
        // we're purely using atomic ops with no associated memory ops
        self.pruned.fetch_add(n, Ordering::Relaxed);
    }

    /// Add `n` to the metric's matched value
    pub fn add_matched(&self, n: usize) {
        // relaxed ordering for operations on `value` poses no issues
        // we're purely using atomic ops with no associated memory ops
        self.matched.fetch_add(n, Ordering::Relaxed);
    }

    /// Add `n` to the metric's fully matched value
    pub fn add_fully_matched(&self, n: usize) {
        // relaxed ordering for operations on `value` poses no issues
        // we're purely using atomic ops with no associated memory ops
        self.fully_matched.fetch_add(n, Ordering::Relaxed);
    }

    /// Subtract `n` to the metric's matched value.
    pub fn subtract_matched(&self, n: usize) {
        // relaxed ordering for operations on `value` poses no issues
        // we're purely using atomic ops with no associated memory ops
        self.matched.fetch_sub(n, Ordering::Relaxed);
    }

    /// Number of items pruned
    pub fn pruned(&self) -> usize {
        self.pruned.load(Ordering::Relaxed)
    }

    /// Number of items matched (not pruned)
    pub fn matched(&self) -> usize {
        self.matched.load(Ordering::Relaxed)
    }

    /// Number of items fully matched
    pub fn fully_matched(&self) -> usize {
        self.fully_matched.load(Ordering::Relaxed)
    }
}

/// Counters tracking ratio metrics (e.g. matched vs total)
///
/// The counters are thread-safe and shared across clones.
#[derive(Debug, Clone, Default)]
pub struct RatioMetrics {
    part: Arc<AtomicUsize>,
    total: Arc<AtomicUsize>,
    merge_strategy: RatioMergeStrategy,
}

#[derive(Debug, Clone, Default)]
pub enum RatioMergeStrategy {
    #[default]
    AddPartAddTotal,
    AddPartSetTotal,
    SetPartAddTotal,
}

impl RatioMetrics {
    /// Create a new [`RatioMetrics`]
    pub fn new() -> Self {
        Self {
            part: Arc::new(AtomicUsize::new(0)),
            total: Arc::new(AtomicUsize::new(0)),
            merge_strategy: RatioMergeStrategy::AddPartAddTotal,
        }
    }

    pub fn with_merge_strategy(mut self, merge_strategy: RatioMergeStrategy) -> Self {
        self.merge_strategy = merge_strategy;
        self
    }

    /// Add `n` to the numerator (`part`) value
    pub fn add_part(&self, n: usize) {
        self.part.fetch_add(n, Ordering::Relaxed);
    }

    /// Add `n` to the denominator (`total`) value
    pub fn add_total(&self, n: usize) {
        self.total.fetch_add(n, Ordering::Relaxed);
    }

    /// Set the numerator (`part`) value to `n`, overwriting any existing value
    pub fn set_part(&self, n: usize) {
        self.part.store(n, Ordering::Relaxed);
    }

    /// Set the denominator (`total`) value to `n`, overwriting any existing value
    pub fn set_total(&self, n: usize) {
        self.total.store(n, Ordering::Relaxed);
    }

    /// Merge the value from `other` into `self`
    pub fn merge(&self, other: &Self) {
        match self.merge_strategy {
            RatioMergeStrategy::AddPartAddTotal => {
                self.add_part(other.part());
                self.add_total(other.total());
            }
            RatioMergeStrategy::AddPartSetTotal => {
                self.add_part(other.part());
                self.set_total(other.total());
            }
            RatioMergeStrategy::SetPartAddTotal => {
                self.set_part(other.part());
                self.add_total(other.total());
            }
        }
    }

    /// Return the numerator (`part`) value
    pub fn part(&self) -> usize {
        self.part.load(Ordering::Relaxed)
    }

    /// Return the denominator (`total`) value
    pub fn total(&self) -> usize {
        self.total.load(Ordering::Relaxed)
    }
}

impl PartialEq for RatioMetrics {
    fn eq(&self, other: &Self) -> bool {
        self.part() == other.part() && self.total() == other.total()
    }
}

/// Format a float number with `digits` most significant numbers.
///
/// fmt_significant(12.5) -> "12"
/// fmt_significant(0.0543) -> "0.054"
/// fmt_significant(0.000123) -> "0.00012"
fn fmt_significant(mut x: f64, digits: usize) -> String {
    if x == 0.0 {
        return "0".to_string();
    }

    let exp = x.abs().log10().floor(); // exponent of first significant digit
    let scale = 10f64.powf(-(exp - (digits as f64 - 1.0)));
    x = (x * scale).round() / scale; // round to N significant digits
    format!("{x}")
}

impl Display for RatioMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let part = self.part();
        let total = self.total();

        if total == 0 {
            if part == 0 {
                write!(f, "N/A (0/0)")
            } else {
                write!(f, "N/A ({}/0)", human_readable_count(part))
            }
        } else {
            let percentage = (part as f64 / total as f64) * 100.0;

            write!(
                f,
                "{}% ({}/{})",
                fmt_significant(percentage, 2),
                human_readable_count(part),
                human_readable_count(total)
            )
        }
    }
}

/// Possible values for a [super::Metric].
///
/// Among other differences, the metric types have different ways to
/// logically interpret their underlying values and some metrics are
/// so common they are given special treatment.
#[derive(Debug, Clone)]
pub enum MetricValue {
    /// Number of output rows produced: "output_rows" metric
    OutputRows(Count),
    /// Elapsed Compute Time: the wall clock time spent in "cpu
    /// intensive" work.
    ///
    /// This measurement represents, roughly:
    /// ```
    /// use std::time::Instant;
    /// let start = Instant::now();
    /// // ...CPU intensive work here...
    /// let elapsed_compute = (Instant::now() - start).as_nanos();
    /// ```
    ///
    /// Note 1: Does *not* include time other operators spend
    /// computing input.
    ///
    /// Note 2: *Does* includes time when the thread could have made
    /// progress but the OS did not schedule it (e.g. due to CPU
    /// contention), thus making this value different than the
    /// classical definition of "cpu_time", which is the time reported
    /// from `clock_gettime(CLOCK_THREAD_CPUTIME_ID, ..)`.
    ElapsedCompute(Time),
    /// Number of spills produced: "spill_count" metric
    SpillCount(Count),
    /// Total size of spilled bytes produced: "spilled_bytes" metric
    SpilledBytes(Count),
    /// Total size of output bytes produced: "output_bytes" metric
    OutputBytes(Count),
    /// Total number of output batches produced: "output_batches" metric
    OutputBatches(Count),
    /// Total size of spilled rows produced: "spilled_rows" metric
    SpilledRows(Count),
    /// Current memory used
    CurrentMemoryUsage(Gauge),
    /// Operator defined count.
    Count {
        /// The provided name of this metric
        name: Cow<'static, str>,
        /// The value of the metric
        count: Count,
    },
    /// Operator defined gauge.
    Gauge {
        /// The provided name of this metric
        name: Cow<'static, str>,
        /// The value of the metric
        gauge: Gauge,
    },
    /// Operator defined time
    Time {
        /// The provided name of this metric
        name: Cow<'static, str>,
        /// The value of the metric
        time: Time,
    },
    /// The time at which execution started
    StartTimestamp(Timestamp),
    /// The time at which execution ended
    EndTimestamp(Timestamp),
    /// Metrics related to scan pruning
    PruningMetrics {
        name: Cow<'static, str>,
        pruning_metrics: PruningMetrics,
    },
    /// Metrics that should be displayed as ratio like (42%)
    Ratio {
        name: Cow<'static, str>,
        ratio_metrics: RatioMetrics,
    },
    Custom {
        /// The provided name of this metric
        name: Cow<'static, str>,
        /// A custom implementation of the metric value.
        value: Arc<dyn CustomMetricValue>,
    },
}

// Manually implement PartialEq for `MetricValue` because it contains CustomMetricValue in its
// definition which is a dyn trait. This wouldn't allow us to just derive PartialEq.
impl PartialEq for MetricValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (MetricValue::OutputRows(count), MetricValue::OutputRows(other)) => {
                count == other
            }
            (MetricValue::ElapsedCompute(time), MetricValue::ElapsedCompute(other)) => {
                time == other
            }
            (MetricValue::SpillCount(count), MetricValue::SpillCount(other)) => {
                count == other
            }
            (MetricValue::SpilledBytes(count), MetricValue::SpilledBytes(other)) => {
                count == other
            }
            (MetricValue::OutputBytes(count), MetricValue::OutputBytes(other)) => {
                count == other
            }
            (MetricValue::OutputBatches(count), MetricValue::OutputBatches(other)) => {
                count == other
            }
            (MetricValue::SpilledRows(count), MetricValue::SpilledRows(other)) => {
                count == other
            }
            (
                MetricValue::CurrentMemoryUsage(gauge),
                MetricValue::CurrentMemoryUsage(other),
            ) => gauge == other,
            (
                MetricValue::Count { name, count },
                MetricValue::Count {
                    name: other_name,
                    count: other_count,
                },
            ) => name == other_name && count == other_count,
            (
                MetricValue::Gauge { name, gauge },
                MetricValue::Gauge {
                    name: other_name,
                    gauge: other_gauge,
                },
            ) => name == other_name && gauge == other_gauge,
            (
                MetricValue::Time { name, time },
                MetricValue::Time {
                    name: other_name,
                    time: other_time,
                },
            ) => name == other_name && time == other_time,

            (
                MetricValue::StartTimestamp(timestamp),
                MetricValue::StartTimestamp(other),
            ) => timestamp == other,
            (MetricValue::EndTimestamp(timestamp), MetricValue::EndTimestamp(other)) => {
                timestamp == other
            }
            (
                MetricValue::PruningMetrics {
                    name,
                    pruning_metrics,
                },
                MetricValue::PruningMetrics {
                    name: other_name,
                    pruning_metrics: other_pruning_metrics,
                },
            ) => {
                name == other_name
                    && pruning_metrics.pruned() == other_pruning_metrics.pruned()
                    && pruning_metrics.matched() == other_pruning_metrics.matched()
            }
            (
                MetricValue::Ratio {
                    name,
                    ratio_metrics,
                },
                MetricValue::Ratio {
                    name: other_name,
                    ratio_metrics: other_ratio_metrics,
                },
            ) => name == other_name && ratio_metrics == other_ratio_metrics,
            (
                MetricValue::Custom { name, value },
                MetricValue::Custom {
                    name: other_name,
                    value: other_value,
                },
            ) => name == other_name && value.is_eq(other_value),
            // Default case when the two sides do not have the same type.
            _ => false,
        }
    }
}

impl MetricValue {
    /// Return the name of this SQL metric
    pub fn name(&self) -> &str {
        match self {
            Self::OutputRows(_) => "output_rows",
            Self::SpillCount(_) => "spill_count",
            Self::SpilledBytes(_) => "spilled_bytes",
            Self::OutputBytes(_) => "output_bytes",
            Self::OutputBatches(_) => "output_batches",
            Self::SpilledRows(_) => "spilled_rows",
            Self::CurrentMemoryUsage(_) => "mem_used",
            Self::ElapsedCompute(_) => "elapsed_compute",
            Self::Count { name, .. } => name.borrow(),
            Self::Gauge { name, .. } => name.borrow(),
            Self::Time { name, .. } => name.borrow(),
            Self::StartTimestamp(_) => "start_timestamp",
            Self::EndTimestamp(_) => "end_timestamp",
            Self::PruningMetrics { name, .. } => name.borrow(),
            Self::Ratio { name, .. } => name.borrow(),
            Self::Custom { name, .. } => name.borrow(),
        }
    }

    /// Return the value of the metric as a usize value, used to aggregate metric
    /// value across partitions.
    pub fn as_usize(&self) -> usize {
        match self {
            Self::OutputRows(count) => count.value(),
            Self::SpillCount(count) => count.value(),
            Self::SpilledBytes(bytes) => bytes.value(),
            Self::OutputBytes(bytes) => bytes.value(),
            Self::OutputBatches(count) => count.value(),
            Self::SpilledRows(count) => count.value(),
            Self::CurrentMemoryUsage(used) => used.value(),
            Self::ElapsedCompute(time) => time.value(),
            Self::Count { count, .. } => count.value(),
            Self::Gauge { gauge, .. } => gauge.value(),
            Self::Time { time, .. } => time.value(),
            Self::StartTimestamp(timestamp) => timestamp
                .value()
                .and_then(|ts| ts.timestamp_nanos_opt())
                .map(|nanos| nanos as usize)
                .unwrap_or(0),
            Self::EndTimestamp(timestamp) => timestamp
                .value()
                .and_then(|ts| ts.timestamp_nanos_opt())
                .map(|nanos| nanos as usize)
                .unwrap_or(0),
            // This function is a utility for aggregating metrics, for complex metric
            // like `PruningMetrics`, this function is not supposed to get called.
            // Metrics aggregation for them are implemented inside `MetricsSet` directly.
            Self::PruningMetrics { .. } => 0,
            // Should not be used. See comments in `PruningMetrics` for details.
            Self::Ratio { .. } => 0,
            Self::Custom { value, .. } => value.as_usize(),
        }
    }

    /// create a new MetricValue with the same type as `self` suitable
    /// for accumulating
    pub fn new_empty(&self) -> Self {
        match self {
            Self::OutputRows(_) => Self::OutputRows(Count::new()),
            Self::SpillCount(_) => Self::SpillCount(Count::new()),
            Self::SpilledBytes(_) => Self::SpilledBytes(Count::new()),
            Self::OutputBytes(_) => Self::OutputBytes(Count::new()),
            Self::OutputBatches(_) => Self::OutputBatches(Count::new()),
            Self::SpilledRows(_) => Self::SpilledRows(Count::new()),
            Self::CurrentMemoryUsage(_) => Self::CurrentMemoryUsage(Gauge::new()),
            Self::ElapsedCompute(_) => Self::ElapsedCompute(Time::new()),
            Self::Count { name, .. } => Self::Count {
                name: name.clone(),
                count: Count::new(),
            },
            Self::Gauge { name, .. } => Self::Gauge {
                name: name.clone(),
                gauge: Gauge::new(),
            },
            Self::Time { name, .. } => Self::Time {
                name: name.clone(),
                time: Time::new(),
            },
            Self::StartTimestamp(_) => Self::StartTimestamp(Timestamp::new()),
            Self::EndTimestamp(_) => Self::EndTimestamp(Timestamp::new()),
            Self::PruningMetrics { name, .. } => Self::PruningMetrics {
                name: name.clone(),
                pruning_metrics: PruningMetrics::new(),
            },
            Self::Ratio {
                name,
                ratio_metrics,
            } => {
                let merge_strategy = ratio_metrics.merge_strategy.clone();
                Self::Ratio {
                    name: name.clone(),
                    ratio_metrics: RatioMetrics::new()
                        .with_merge_strategy(merge_strategy),
                }
            }
            Self::Custom { name, value } => Self::Custom {
                name: name.clone(),
                value: value.new_empty(),
            },
        }
    }

    /// Aggregates the value of other to `self`. panic's if the types
    /// are mismatched or aggregating does not make sense for this
    /// value
    ///
    /// Note this is purposely marked `mut` (even though atomics are
    /// used) so Rust's type system can be used to ensure the
    /// appropriate API access. `MetricValues` should be modified
    /// using the original [`Count`] or [`Time`] they were created
    /// from.
    pub fn aggregate(&mut self, other: &Self) {
        match (self, other) {
            (Self::OutputRows(count), Self::OutputRows(other_count))
            | (Self::SpillCount(count), Self::SpillCount(other_count))
            | (Self::SpilledBytes(count), Self::SpilledBytes(other_count))
            | (Self::OutputBytes(count), Self::OutputBytes(other_count))
            | (Self::OutputBatches(count), Self::OutputBatches(other_count))
            | (Self::SpilledRows(count), Self::SpilledRows(other_count))
            | (
                Self::Count { count, .. },
                Self::Count {
                    count: other_count, ..
                },
            ) => count.add(other_count.value()),
            (Self::CurrentMemoryUsage(gauge), Self::CurrentMemoryUsage(other_gauge))
            | (
                Self::Gauge { gauge, .. },
                Self::Gauge {
                    gauge: other_gauge, ..
                },
            ) => gauge.add(other_gauge.value()),
            (Self::ElapsedCompute(time), Self::ElapsedCompute(other_time))
            | (
                Self::Time { time, .. },
                Self::Time {
                    time: other_time, ..
                },
            ) => time.add(other_time),
            // timestamps are aggregated by min/max
            (Self::StartTimestamp(timestamp), Self::StartTimestamp(other_timestamp)) => {
                timestamp.update_to_min(other_timestamp);
            }
            // timestamps are aggregated by min/max
            (Self::EndTimestamp(timestamp), Self::EndTimestamp(other_timestamp)) => {
                timestamp.update_to_max(other_timestamp);
            }
            (
                Self::PruningMetrics {
                    pruning_metrics, ..
                },
                Self::PruningMetrics {
                    pruning_metrics: other_pruning_metrics,
                    ..
                },
            ) => {
                let pruned = other_pruning_metrics.pruned.load(Ordering::Relaxed);
                let matched = other_pruning_metrics.matched.load(Ordering::Relaxed);
                let fully_matched =
                    other_pruning_metrics.fully_matched.load(Ordering::Relaxed);
                pruning_metrics.add_pruned(pruned);
                pruning_metrics.add_matched(matched);
                pruning_metrics.add_fully_matched(fully_matched);
            }
            (
                Self::Ratio { ratio_metrics, .. },
                Self::Ratio {
                    ratio_metrics: other_ratio_metrics,
                    ..
                },
            ) => {
                ratio_metrics.merge(other_ratio_metrics);
            }
            (
                Self::Custom { value, .. },
                Self::Custom {
                    value: other_value, ..
                },
            ) => {
                value.aggregate(Arc::clone(other_value));
            }
            m @ (_, _) => {
                panic!(
                    "Mismatched metric types. Can not aggregate {:?} with value {:?}",
                    m.0, m.1
                )
            }
        }
    }

    /// Returns a number by which to sort metrics by display. Lower
    /// numbers are "more useful" (and displayed first)
    pub fn display_sort_key(&self) -> u8 {
        match self {
            // `BaselineMetrics` that is common for most operators
            Self::OutputRows(_) => 0,
            Self::ElapsedCompute(_) => 1,
            Self::OutputBytes(_) => 2,
            Self::OutputBatches(_) => 3,
            // Other metrics
            Self::PruningMetrics { name, .. } => match name.as_ref() {
                // The following metrics belong to `DataSourceExec` with a Parquet data source.
                // They are displayed in a specific order that reflects the actual pruning process,
                // from coarse-grained to fine-grained pruning levels.
                //
                // You may update these metrics as long as their relative order remains unchanged.
                //
                // Reference PR: <https://github.com/apache/datafusion/pull/18379>
                "files_ranges_pruned_statistics" => 4,
                "row_groups_pruned_statistics" => 5,
                "row_groups_pruned_bloom_filter" => 6,
                "page_index_rows_pruned" => 7,
                _ => 8,
            },
            Self::SpillCount(_) => 9,
            Self::SpilledBytes(_) => 10,
            Self::SpilledRows(_) => 11,
            Self::CurrentMemoryUsage(_) => 12,
            Self::Count { .. } => 13,
            Self::Gauge { .. } => 14,
            Self::Time { .. } => 15,
            Self::Ratio { .. } => 16,
            Self::StartTimestamp(_) => 17, // show timestamps last
            Self::EndTimestamp(_) => 18,
            Self::Custom { .. } => 19,
        }
    }

    /// returns true if this metric has a timestamp value
    pub fn is_timestamp(&self) -> bool {
        matches!(self, Self::StartTimestamp(_) | Self::EndTimestamp(_))
    }
}

impl Display for MetricValue {
    /// Prints the value of this metric
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::OutputRows(count)
            | Self::OutputBatches(count)
            | Self::SpillCount(count)
            | Self::SpilledRows(count)
            | Self::Count { count, .. } => {
                write!(f, "{count}")
            }
            Self::SpilledBytes(count) | Self::OutputBytes(count) => {
                let readable_count = human_readable_size(count.value());
                write!(f, "{readable_count}")
            }
            Self::CurrentMemoryUsage(gauge) => {
                // CurrentMemoryUsage is in bytes, format like SpilledBytes
                let readable_size = human_readable_size(gauge.value());
                write!(f, "{readable_size}")
            }
            Self::Gauge { gauge, .. } => {
                // Generic gauge metrics - format with human-readable count
                write!(f, "{}", human_readable_count(gauge.value()))
            }
            Self::ElapsedCompute(time) | Self::Time { time, .. } => {
                // distinguish between no time recorded and very small
                // amount of time recorded
                if time.value() > 0 {
                    write!(f, "{time}")
                } else {
                    write!(f, "NOT RECORDED")
                }
            }
            Self::StartTimestamp(timestamp) | Self::EndTimestamp(timestamp) => {
                write!(f, "{timestamp}")
            }
            Self::PruningMetrics {
                pruning_metrics, ..
            } => {
                write!(f, "{pruning_metrics}")
            }
            Self::Ratio { ratio_metrics, .. } => write!(f, "{ratio_metrics}"),
            Self::Custom { name, value } => {
                write!(f, "name:{name} {value}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use chrono::TimeZone;
    use datafusion_common::units::MB;

    use super::*;

    #[derive(Debug, Default)]
    pub struct CustomCounter {
        count: AtomicUsize,
    }

    impl Display for CustomCounter {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "count: {}", self.count.load(Ordering::Relaxed))
        }
    }

    impl CustomMetricValue for CustomCounter {
        fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
            Arc::new(CustomCounter::default())
        }

        fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
            let other = other.as_any().downcast_ref::<Self>().unwrap();
            self.count
                .fetch_add(other.count.load(Ordering::Relaxed), Ordering::Relaxed);
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
            let Some(other) = other.as_any().downcast_ref::<Self>() else {
                return false;
            };

            self.count.load(Ordering::Relaxed) == other.count.load(Ordering::Relaxed)
        }
    }

    fn new_custom_counter(name: &'static str, value: usize) -> MetricValue {
        let custom_counter = CustomCounter::default();
        custom_counter.count.fetch_add(value, Ordering::Relaxed);

        MetricValue::Custom {
            name: Cow::Borrowed(name),
            value: Arc::new(custom_counter),
        }
    }

    #[test]
    fn test_custom_metric_with_mismatching_names() {
        let mut custom_val = new_custom_counter("Hi", 1);
        let other_custom_val = new_custom_counter("Hello", 1);

        // Not equal since the name differs.
        assert!(other_custom_val != custom_val);

        // Should work even though the name differs
        custom_val.aggregate(&other_custom_val);

        let expected_val = new_custom_counter("Hi", 2);
        assert!(expected_val == custom_val);
    }

    #[test]
    fn test_custom_metric() {
        let mut custom_val = new_custom_counter("hi", 11);
        let other_custom_val = new_custom_counter("hi", 20);

        custom_val.aggregate(&other_custom_val);

        assert!(custom_val != other_custom_val);

        if let MetricValue::Custom { value, .. } = custom_val {
            let counter = value
                .as_any()
                .downcast_ref::<CustomCounter>()
                .expect("Expected CustomCounter");
            assert_eq!(counter.count.load(Ordering::Relaxed), 31);
        } else {
            panic!("Unexpected value");
        }
    }

    #[test]
    fn test_display_output_rows() {
        let count = Count::new();
        let values = vec![
            MetricValue::OutputRows(count.clone()),
            MetricValue::Count {
                name: "my_counter".into(),
                count: count.clone(),
            },
        ];

        for value in &values {
            assert_eq!("0", value.to_string(), "value {value:?}");
        }

        count.add(42);
        for value in &values {
            assert_eq!("42", value.to_string(), "value {value:?}");
        }
    }

    #[test]
    fn test_display_spilled_bytes() {
        let count = Count::new();
        let spilled_byte = MetricValue::SpilledBytes(count.clone());

        assert_eq!("0.0 B", spilled_byte.to_string());

        count.add((100 * MB) as usize);
        assert_eq!("100.0 MB", spilled_byte.to_string());

        count.add((0.5 * MB as f64) as usize);
        assert_eq!("100.5 MB", spilled_byte.to_string());
    }

    #[test]
    fn test_display_time() {
        let time = Time::new();
        let values = vec![
            MetricValue::ElapsedCompute(time.clone()),
            MetricValue::Time {
                name: "my_time".into(),
                time: time.clone(),
            },
        ];

        // if time is not set, it should not be reported as zero
        for value in &values {
            assert_eq!("NOT RECORDED", value.to_string(), "value {value:?}");
        }

        time.add_duration(Duration::from_nanos(1042));
        for value in &values {
            assert_eq!("1.04µs", value.to_string(), "value {value:?}");
        }
    }

    #[test]
    fn test_display_ratio() {
        let ratio_metrics = RatioMetrics::new();
        let ratio = MetricValue::Ratio {
            name: Cow::Borrowed("ratio_metric"),
            ratio_metrics: ratio_metrics.clone(),
        };

        assert_eq!("N/A (0/0)", ratio.to_string());

        ratio_metrics.add_part(10);
        assert_eq!("N/A (10/0)", ratio.to_string());

        ratio_metrics.add_total(40);
        assert_eq!("25% (10/40)", ratio.to_string());

        let tiny_ratio_metrics = RatioMetrics::new();
        let tiny_ratio = MetricValue::Ratio {
            name: Cow::Borrowed("tiny_ratio_metric"),
            ratio_metrics: tiny_ratio_metrics.clone(),
        };
        tiny_ratio_metrics.add_part(1);
        tiny_ratio_metrics.add_total(3000);
        assert_eq!("0.033% (1/3.00 K)", tiny_ratio.to_string());
    }

    #[test]
    fn test_ratio_set_methods() {
        let ratio_metrics = RatioMetrics::new();

        // Ensure set methods don't increment
        ratio_metrics.set_part(10);
        ratio_metrics.set_part(10);
        ratio_metrics.set_total(40);
        ratio_metrics.set_total(40);
        assert_eq!("25% (10/40)", ratio_metrics.to_string());

        let ratio_metrics = RatioMetrics::new();

        // Calling set should change the value
        ratio_metrics.set_part(10);
        ratio_metrics.set_part(30);
        ratio_metrics.set_total(40);
        ratio_metrics.set_total(50);
        assert_eq!("60% (30/50)", ratio_metrics.to_string());
    }

    #[test]
    fn test_ratio_merge_strategy() {
        // Test AddPartSetTotal strategy
        let ratio_metrics1 =
            RatioMetrics::new().with_merge_strategy(RatioMergeStrategy::AddPartSetTotal);

        ratio_metrics1.set_part(10);
        ratio_metrics1.set_total(40);
        assert_eq!("25% (10/40)", ratio_metrics1.to_string());
        let ratio_metrics2 =
            RatioMetrics::new().with_merge_strategy(RatioMergeStrategy::AddPartSetTotal);
        ratio_metrics2.set_part(20);
        ratio_metrics2.set_total(40);
        assert_eq!("50% (20/40)", ratio_metrics2.to_string());

        ratio_metrics1.merge(&ratio_metrics2);
        assert_eq!("75% (30/40)", ratio_metrics1.to_string());

        // Test SetPartAddTotal strategy
        let ratio_metrics1 =
            RatioMetrics::new().with_merge_strategy(RatioMergeStrategy::SetPartAddTotal);
        ratio_metrics1.set_part(20);
        ratio_metrics1.set_total(50);
        let ratio_metrics2 = RatioMetrics::new();
        ratio_metrics2.set_part(20);
        ratio_metrics2.set_total(50);
        ratio_metrics1.merge(&ratio_metrics2);
        assert_eq!("20% (20/100)", ratio_metrics1.to_string());

        // Test AddPartAddTotal strategy (default)
        let ratio_metrics1 = RatioMetrics::new();
        ratio_metrics1.set_part(20);
        ratio_metrics1.set_total(50);
        let ratio_metrics2 = RatioMetrics::new();
        ratio_metrics2.set_part(20);
        ratio_metrics2.set_total(50);
        ratio_metrics1.merge(&ratio_metrics2);
        assert_eq!("40% (40/100)", ratio_metrics1.to_string());
    }

    #[test]
    fn test_display_timestamp() {
        let timestamp = Timestamp::new();
        let values = vec![
            MetricValue::StartTimestamp(timestamp.clone()),
            MetricValue::EndTimestamp(timestamp.clone()),
        ];

        // if time is not set, it should not be reported as zero
        for value in &values {
            assert_eq!("NONE", value.to_string(), "value {value:?}");
        }

        timestamp.set(Utc.timestamp_nanos(1431648000000000));
        for value in &values {
            assert_eq!(
                "1970-01-17 13:40:48 UTC",
                value.to_string(),
                "value {value:?}"
            );
        }
    }

    #[test]
    fn test_timer_with_custom_instant() {
        let time = Time::new();
        let start_time = Instant::now();

        // Sleep a bit to ensure some time passes
        std::thread::sleep(Duration::from_millis(1));

        // Create timer with the earlier start time
        let mut timer = time.timer_with(start_time);

        // Sleep a bit more
        std::thread::sleep(Duration::from_millis(1));

        // Stop the timer
        timer.stop();

        // The recorded time should be at least 20ms (both sleeps)
        assert!(
            time.value() >= 2_000_000,
            "Expected at least 2ms, got {} ns",
            time.value()
        );
    }

    #[test]
    fn test_stop_with_custom_endpoint() {
        let time = Time::new();
        let start = Instant::now();
        let mut timer = time.timer_with(start);

        // Simulate exactly 10ms passing
        let end = start + Duration::from_millis(10);

        // Stop with custom endpoint
        timer.stop_with(end);

        // Should record exactly 10ms (10_000_000 nanoseconds)
        // Allow for small variations due to timer resolution
        let recorded = time.value();
        assert!(
            (10_000_000..=10_100_000).contains(&recorded),
            "Expected ~10ms, got {recorded} ns"
        );

        // Calling stop_with again should not add more time
        timer.stop_with(end);
        assert_eq!(
            recorded,
            time.value(),
            "Time should not change after second stop"
        );
    }

    #[test]
    fn test_done_with_custom_endpoint() {
        let time = Time::new();
        let start = Instant::now();

        // Create a new scope for the timer
        {
            let timer = time.timer_with(start);

            // Simulate 50ms passing
            let end = start + Duration::from_millis(5);

            // Call done_with to stop and consume the timer
            timer.done_with(end);

            // Timer is consumed, can't use it anymore
        }

        // Should record exactly 5ms
        let recorded = time.value();
        assert!(
            (5_000_000..=5_100_000).contains(&recorded),
            "Expected ~5ms, got {recorded} ns",
        );

        // Test that done_with prevents drop from recording time again
        {
            let timer2 = time.timer_with(start);
            let end2 = start + Duration::from_millis(5);
            timer2.done_with(end2);
            // drop happens here but should not record additional time
        }

        // Should have added only 5ms more
        let new_recorded = time.value();
        assert!(
            (10_000_000..=10_100_000).contains(&new_recorded),
            "Expected ~10ms total, got {new_recorded} ns",
        );
    }

    #[test]
    fn test_human_readable_metric_formatting() {
        // Test Count formatting with various sizes
        let small_count = Count::new();
        small_count.add(42);
        assert_eq!(
            MetricValue::OutputRows(small_count.clone()).to_string(),
            "42"
        );

        let thousand_count = Count::new();
        thousand_count.add(10_100);
        assert_eq!(
            MetricValue::OutputRows(thousand_count.clone()).to_string(),
            "10.10 K"
        );

        let million_count = Count::new();
        million_count.add(1_532_000);
        assert_eq!(
            MetricValue::SpilledRows(million_count.clone()).to_string(),
            "1.53 M"
        );

        let billion_count = Count::new();
        billion_count.add(2_500_000_000);
        assert_eq!(
            MetricValue::OutputBatches(billion_count.clone()).to_string(),
            "2.50 B"
        );

        // Test Time formatting with various durations
        let micros_time = Time::new();
        micros_time.add_duration(Duration::from_nanos(1_234));
        assert_eq!(
            MetricValue::ElapsedCompute(micros_time.clone()).to_string(),
            "1.23µs"
        );

        let millis_time = Time::new();
        millis_time.add_duration(Duration::from_nanos(11_295_377));
        assert_eq!(
            MetricValue::ElapsedCompute(millis_time.clone()).to_string(),
            "11.30ms"
        );

        let seconds_time = Time::new();
        seconds_time.add_duration(Duration::from_nanos(1_234_567_890));
        assert_eq!(
            MetricValue::ElapsedCompute(seconds_time.clone()).to_string(),
            "1.23s"
        );

        // Test CurrentMemoryUsage formatting (should use size, not count)
        let mem_gauge = Gauge::new();
        mem_gauge.add(100 * MB as usize);
        assert_eq!(
            MetricValue::CurrentMemoryUsage(mem_gauge.clone()).to_string(),
            "100.0 MB"
        );

        // Test custom Gauge formatting (should use count)
        let custom_gauge = Gauge::new();
        custom_gauge.add(50_000);
        assert_eq!(
            MetricValue::Gauge {
                name: "custom".into(),
                gauge: custom_gauge.clone()
            }
            .to_string(),
            "50.00 K"
        );

        // Test PruningMetrics formatting
        let pruning = PruningMetrics::new();
        pruning.add_matched(500_000);
        pruning.add_pruned(500_000);
        assert_eq!(
            MetricValue::PruningMetrics {
                name: "test_pruning".into(),
                pruning_metrics: pruning.clone()
            }
            .to_string(),
            "1.00 M total → 500.0 K matched"
        );

        // Test RatioMetrics formatting
        let ratio = RatioMetrics::new();
        ratio.add_part(250_000);
        ratio.add_total(1_000_000);
        assert_eq!(
            MetricValue::Ratio {
                name: "test_ratio".into(),
                ratio_metrics: ratio.clone()
            }
            .to_string(),
            "25% (250.0 K/1.00 M)"
        );
    }
}
