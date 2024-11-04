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

use std::{
    borrow::{Borrow, Cow},
    fmt::Display,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use datafusion_common::instant::Instant;
use parking_lot::Mutex;

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
        write!(f, "{}", self.value())
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
        let duration = Duration::from_nanos(self.value() as u64);
        write!(f, "{duration:?}")
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

impl<'a> ScopedTimerGuard<'a> {
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
}

impl<'a> Drop for ScopedTimerGuard<'a> {
    fn drop(&mut self) {
        self.stop()
    }
}

/// Possible values for a [super::Metric].
///
/// Among other differences, the metric types have different ways to
/// logically interpret their underlying values and some metrics are
/// so common they are given special treatment.
#[derive(Debug, Clone, PartialEq)]
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
}

impl MetricValue {
    /// Return the name of this SQL metric
    pub fn name(&self) -> &str {
        match self {
            Self::OutputRows(_) => "output_rows",
            Self::SpillCount(_) => "spill_count",
            Self::SpilledBytes(_) => "spilled_bytes",
            Self::SpilledRows(_) => "spilled_rows",
            Self::CurrentMemoryUsage(_) => "mem_used",
            Self::ElapsedCompute(_) => "elapsed_compute",
            Self::Count { name, .. } => name.borrow(),
            Self::Gauge { name, .. } => name.borrow(),
            Self::Time { name, .. } => name.borrow(),
            Self::StartTimestamp(_) => "start_timestamp",
            Self::EndTimestamp(_) => "end_timestamp",
        }
    }

    /// Return the value of the metric as a usize value
    pub fn as_usize(&self) -> usize {
        match self {
            Self::OutputRows(count) => count.value(),
            Self::SpillCount(count) => count.value(),
            Self::SpilledBytes(bytes) => bytes.value(),
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
        }
    }

    /// create a new MetricValue with the same type as `self` suitable
    /// for accumulating
    pub fn new_empty(&self) -> Self {
        match self {
            Self::OutputRows(_) => Self::OutputRows(Count::new()),
            Self::SpillCount(_) => Self::SpillCount(Count::new()),
            Self::SpilledBytes(_) => Self::SpilledBytes(Count::new()),
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
            Self::OutputRows(_) => 0,     // show first
            Self::ElapsedCompute(_) => 1, // show second
            Self::SpillCount(_) => 2,
            Self::SpilledBytes(_) => 3,
            Self::SpilledRows(_) => 4,
            Self::CurrentMemoryUsage(_) => 5,
            Self::Count { .. } => 6,
            Self::Gauge { .. } => 7,
            Self::Time { .. } => 8,
            Self::StartTimestamp(_) => 9, // show timestamps last
            Self::EndTimestamp(_) => 10,
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
            | Self::SpillCount(count)
            | Self::SpilledBytes(count)
            | Self::SpilledRows(count)
            | Self::Count { count, .. } => {
                write!(f, "{count}")
            }
            Self::CurrentMemoryUsage(gauge) | Self::Gauge { gauge, .. } => {
                write!(f, "{gauge}")
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
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

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
            assert_eq!("1.042µs", value.to_string(), "value {value:?}");
        }
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
}
