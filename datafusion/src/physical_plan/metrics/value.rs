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
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

/// A counter to record things such as number of input or output rows
///
/// Note `clone`ing counters update the same underlying metrics
#[derive(Debug, Clone)]
pub struct Count {
    /// value of the metric counter
    value: std::sync::Arc<AtomicUsize>,
}

impl PartialEq for Count {
    fn eq(&self, other: &Self) -> bool {
        self.value().eq(&other.value())
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

/// Measure a potentially non contiguous duration of time
#[derive(Debug, Clone)]
pub struct Time {
    /// elapsed time, in nanoseconds
    nanos: Arc<AtomicUsize>,
}

impl PartialEq for Time {
    fn eq(&self, other: &Self) -> bool {
        self.value().eq(&other.value())
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
    pub fn add_duration(&self, duration: Duration) {
        let more_nanos = duration.as_nanos() as usize;
        self.nanos.fetch_add(more_nanos, Ordering::Relaxed);
    }

    /// Add the number of nanoseconds of other `Time` to self
    pub fn add(&self, other: &Time) {
        self.nanos.fetch_add(other.value(), Ordering::Relaxed);
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

/// Possible values for a metric.
///
/// Among other differences, the metric types have different ways to
/// logically interpret their underlying values and some metrics are
/// so common they are given special treatment.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    /// Number of output rows produced: "output_rows" metric
    OutputRows(Count),
    /// CPU time: the "cpu_time" metric
    CPUTime(Time),
    /// Operator defined count.
    Count {
        /// The provided name of this metric
        name: Cow<'static, str>,
        /// The value of the metric
        count: Count,
    },
    /// Operator defined time
    Time {
        /// The provided name of this metric
        name: Cow<'static, str>,
        /// The value of the metric
        time: Time,
    },
    // TODO timestamp, etc
    // https://github.com/apache/arrow-datafusion/issues/866
}

impl MetricValue {
    /// Return the name of this SQL metric
    pub fn name(&self) -> &str {
        match self {
            Self::OutputRows(_) => "output_rows",
            Self::CPUTime(_) => "cpu_time",
            Self::Count { name, .. } => name.borrow(),
            Self::Time { name, .. } => name.borrow(),
        }
    }

    /// Return the value of the metric as a usize value
    pub fn as_usize(&self) -> usize {
        match self {
            Self::OutputRows(count) => count.value(),
            Self::CPUTime(time) => time.value(),
            Self::Count { count, .. } => count.value(),
            Self::Time { time, .. } => time.value(),
        }
    }

    /// create a new MetricValue with the same type as `self` suitable
    /// for accumulating
    pub fn new_empty(&self) -> Self {
        match self {
            Self::OutputRows(_) => Self::OutputRows(Count::new()),
            Self::CPUTime(_) => Self::CPUTime(Time::new()),
            Self::Count { name, .. } => Self::Count {
                name: name.clone(),
                count: Count::new(),
            },
            Self::Time { name, .. } => Self::Time {
                name: name.clone(),
                time: Time::new(),
            },
        }
    }

    /// Add the value of other to `self`. panic's if the type is mismatched or
    /// aggregating does not make sense for this value
    ///
    /// Note this is purposely marked `mut` (even though atomics are
    /// used) so Rust's type system can be used to ensure the
    /// appropriate API access. `MetricValues` should be modified
    /// using the original [`Count`] or [`Time`] they were created
    /// from.
    pub fn add(&mut self, other: &Self) {
        match (self, other) {
            (Self::OutputRows(count), Self::OutputRows(other_count))
            | (
                Self::Count { count, .. },
                Self::Count {
                    count: other_count, ..
                },
            ) => count.add(other_count.value()),
            (Self::CPUTime(time), Self::CPUTime(other_time))
            | (
                Self::Time { time, .. },
                Self::Time {
                    time: other_time, ..
                },
            ) => time.add(other_time),
            m @ (_, _) => {
                panic!(
                    "Mismatched metric types. Can not aggregate {:?} with value {:?}",
                    m.0, m.1
                )
            }
        }
    }
}

impl std::fmt::Display for MetricValue {
    /// Prints the value of this metric
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutputRows(count) | Self::Count { count, .. } => {
                write!(f, "{}", count.value())
            }
            Self::CPUTime(time) | Self::Time { time, .. } => {
                let duration = std::time::Duration::from_nanos(time.value() as u64);
                write!(f, "{:?}", duration)
            }
        }
    }
}
