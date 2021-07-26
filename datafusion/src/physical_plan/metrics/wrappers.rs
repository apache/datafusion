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

//!  wrappers for `SQLMetrics` for more conveniently recording execution metrics

use std::{sync::Arc, time::Instant};

use super::SQLMetric;

// pub trait MetricSource {
//     /// Return the underlying metric
//     pub fn metric(&self) -> Arc<SQLMetric>;
// }

/// a SQLMetric wrapper for a counter (number of input or output rows)
///
/// Note `clone` counters update the same underlying metrics
#[derive(Debug, Clone)]
pub struct Count {
    inner: Arc<SQLMetric>,
}

impl Count {
    /// create a new counter wrapper around this metric
    pub fn new(inner: Arc<SQLMetric>) -> Self {
        Self { inner }
    }

    /// Add `n` to the counter's value
    pub fn add(&self, n: usize) {
        self.inner.add(n)
    }
}

/// a SQLMetric wrapper for CPU timing information
#[derive(Debug, Clone)]
pub struct Time {
    inner: Arc<SQLMetric>,
}

impl Time {
    /// Create a new [`Time`] wrapper suitable for recording elapsed
    /// times for operations.
    pub fn new(inner: Arc<SQLMetric>) -> Self {
        Self { inner }
    }

    /// Add elapsed nanoseconds since `start`to self
    pub fn add_elapsed(&self, start: Instant) {
        let nanos = start.elapsed().as_nanos() as usize;
        self.inner.add(nanos)
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
