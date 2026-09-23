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

//! Logs the drift between what [`MemoryPool`]s have reserved and what the
//! process has actually allocated.
//!
//! [`MemoryPool`] accounting is voluntary: only allocations that an operator
//! explicitly reserves are counted. Anything else (in-flight batches, kernel
//! scratch space, untracked buffers) is invisible to the pool, so a process
//! can run out of memory while its pool reports plenty of headroom. See
//! <https://github.com/apache/datafusion/issues/25650>.
//!
//! This module only observes. Nothing here changes how memory is granted or
//! limited; it logs the gap so that operators which under-report can be found.
//!
//! DataFusion is a library and does not choose the global allocator, so the
//! allocated byte count is supplied by the caller, e.g. from a counting
//! [`GlobalAlloc`](std::alloc::GlobalAlloc) wrapper or allocator statistics.

use std::{
    fmt::{Debug, Display, Formatter},
    sync::{
        Arc,
        atomic::{AtomicIsize, AtomicUsize, Ordering},
    },
};

use datafusion_common::{Result, human_readable_size};
use parking_lot::Mutex;

use super::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation};

/// Returns the number of bytes currently allocated by the process.
pub type AllocatedBytesFn = Arc<dyn Fn() -> usize + Send + Sync>;

/// Default rise in drift, in bytes, needed before another line is logged.
pub const DEFAULT_DRIFT_LOG_THRESHOLD: usize = 64 * 1024 * 1024;

/// Compares allocated bytes against the total reserved by every
/// [`DriftLoggingPool`] that reports to it.
///
/// A single tracker can be shared by many pools, e.g. one pool per
/// `SessionContext` in a process running several at once. The reserved total
/// is then summed across all of them, which is what has to be compared with a
/// process-wide allocated byte count.
///
/// Drift is `allocated - reserved`. A line is logged at `info` level each time
/// drift rises by at least the log threshold, naming the pool and consumer
/// whose reservation change triggered the check.
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use datafusion_execution::memory_pool::{
/// #     MemoryConsumer, MemoryDriftTracker, MemoryPool, DriftLoggingPool, UnboundedMemoryPool,
/// # };
/// // A real caller would read a counting allocator or allocator stats here.
/// let tracker = Arc::new(MemoryDriftTracker::new(Arc::new(|| 10_000)));
/// let pool: Arc<dyn MemoryPool> = Arc::new(DriftLoggingPool::new(
///     Arc::new(UnboundedMemoryPool::default()),
///     Arc::clone(&tracker),
///     "example",
/// ));
///
/// let reservation = MemoryConsumer::new("op").register(&pool);
/// reservation.grow(4_000);
///
/// assert_eq!(tracker.reserved(), 4_000);
/// assert_eq!(tracker.peak_drift().unwrap().drift, 6_000);
/// ```
pub struct MemoryDriftTracker {
    allocated: AllocatedBytesFn,
    log_threshold: usize,
    /// Total reserved across every pool reporting to this tracker.
    reserved: AtomicUsize,
    /// Positive drift at the time of the last logged line.
    last_logged: AtomicIsize,
    /// Largest drift seen.
    peak_drift: AtomicIsize,
    /// Where the largest drift was seen.
    peak: Mutex<Option<DriftSample>>,
}

/// One observation of drift, recorded by [`MemoryDriftTracker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriftSample {
    /// Label of the [`DriftLoggingPool`] that made the observation.
    pub pool: String,
    /// Consumer whose reservation change triggered the observation.
    pub consumer: String,
    /// Bytes reserved across all pools reporting to the tracker.
    pub reserved: usize,
    /// Bytes allocated, as reported by the tracker's [`AllocatedBytesFn`].
    pub allocated: usize,
    /// `allocated - reserved`.
    pub drift: isize,
}

impl Display for DriftSample {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let drift = if self.drift < 0 {
            format!("-{}", human_readable_size(self.drift.unsigned_abs()))
        } else {
            human_readable_size(self.drift as usize)
        };
        write!(
            f,
            "drift={drift} allocated={} reserved={} pool={} consumer={}",
            human_readable_size(self.allocated),
            human_readable_size(self.reserved),
            self.pool,
            self.consumer,
        )
    }
}

impl MemoryDriftTracker {
    /// Create a tracker that reads allocated bytes from `allocated`, logging
    /// every [`DEFAULT_DRIFT_LOG_THRESHOLD`] of drift.
    pub fn new(allocated: AllocatedBytesFn) -> Self {
        Self {
            allocated,
            log_threshold: DEFAULT_DRIFT_LOG_THRESHOLD,
            reserved: AtomicUsize::new(0),
            last_logged: AtomicIsize::new(0),
            peak_drift: AtomicIsize::new(isize::MIN),
            peak: Mutex::new(None),
        }
    }

    /// Log a line each time drift rises by `log_threshold` bytes.
    pub fn with_log_threshold(mut self, log_threshold: usize) -> Self {
        self.log_threshold = log_threshold;
        self
    }

    /// Bytes currently reserved across all pools reporting to this tracker.
    pub fn reserved(&self) -> usize {
        self.reserved.load(Ordering::Relaxed)
    }

    /// The largest drift seen so far, if any reservation has been made.
    pub fn peak_drift(&self) -> Option<DriftSample> {
        self.peak.lock().clone()
    }

    fn grew(&self, pool: &str, consumer: &str, additional: usize) {
        let reserved =
            self.reserved.fetch_add(additional, Ordering::Relaxed) + additional;
        self.observe(pool, consumer, reserved);
    }

    fn shrank(&self, pool: &str, consumer: &str, shrink: usize) {
        let reserved = self.reserved.fetch_sub(shrink, Ordering::Relaxed) - shrink;
        self.observe(pool, consumer, reserved);
    }

    fn observe(&self, pool: &str, consumer: &str, reserved: usize) {
        let allocated = (self.allocated)();
        let drift = allocated as isize - reserved as isize;

        let sample = || DriftSample {
            pool: pool.to_string(),
            consumer: consumer.to_string(),
            reserved,
            allocated,
            drift,
        };

        // Lock-free check first so the lock is only taken for a new peak.
        if self.peak_drift.fetch_max(drift, Ordering::Relaxed) < drift {
            let mut peak = self.peak.lock();
            if peak.as_ref().is_none_or(|p| drift > p.drift) {
                *peak = Some(sample());
            }
        }

        // Only rising positive drift is logged: that is untracked memory, which
        // is what leads to OOM kills. Negative drift (e.g. an operator
        // reserving ahead of allocating) counts as zero. Falling drift quietly
        // lowers the baseline so the next rise is seen.
        let untracked = drift.max(0);
        let last = self.last_logged.load(Ordering::Relaxed);
        let rose = untracked >= last.saturating_add(self.log_threshold as isize);
        if !rose && untracked >= last {
            return;
        }
        let updated = self
            .last_logged
            .compare_exchange(last, untracked, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok();
        if updated && rose {
            log::info!("memory drift: {}", sample());
        }
    }
}

impl Debug for MemoryDriftTracker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryDriftTracker")
            .field("log_threshold", &self.log_threshold)
            .field("reserved", &self.reserved())
            .field("peak", &self.peak_drift())
            .finish()
    }
}

/// Wraps a [`MemoryPool`], reporting every reservation change to a
/// [`MemoryDriftTracker`].
///
/// Every method delegates to the wrapped pool, so wrapping does not change how
/// memory is granted, limited, or reported. As with other wrappers,
/// downcasting the pool finds this wrapper rather than the pool it wraps.
pub struct DriftLoggingPool {
    inner: Arc<dyn MemoryPool>,
    tracker: Arc<MemoryDriftTracker>,
    label: String,
}

impl DriftLoggingPool {
    /// Wrap `inner`, reporting to `tracker`. `label` identifies this pool in
    /// log lines, which matters when several pools share one tracker.
    ///
    /// `inner` is expected to be empty: anything reserved before wrapping is
    /// not counted.
    pub fn new(
        inner: Arc<dyn MemoryPool>,
        tracker: Arc<MemoryDriftTracker>,
        label: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            tracker,
            label: label.into(),
        }
    }

    /// The tracker this pool reports to.
    pub fn tracker(&self) -> &Arc<MemoryDriftTracker> {
        &self.tracker
    }
}

impl Debug for DriftLoggingPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriftLoggingPool")
            .field("inner", &self.inner)
            .field("label", &self.label)
            .finish()
    }
}

impl Display for DriftLoggingPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Keep output identical to running without the wrapper.
        Display::fmt(&self.inner, f)
    }
}

impl MemoryPool for DriftLoggingPool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.tracker
            .grew(&self.label, reservation.consumer().name(), additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.tracker
            .shrank(&self.label, reservation.consumer().name(), shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.tracker
            .grew(&self.label, reservation.consumer().name(), additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[cfg(test)]
mod tests {
    use crate::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};

    use super::*;

    /// A tracker whose allocated byte count is set by the test.
    fn tracker() -> (Arc<AtomicUsize>, Arc<MemoryDriftTracker>) {
        let allocated = Arc::new(AtomicUsize::new(0));
        let source = Arc::clone(&allocated);
        let tracker = Arc::new(MemoryDriftTracker::new(Arc::new(move || {
            source.load(Ordering::Relaxed)
        })));
        (allocated, tracker)
    }

    fn pool(tracker: &Arc<MemoryDriftTracker>, label: &str) -> Arc<dyn MemoryPool> {
        Arc::new(DriftLoggingPool::new(
            Arc::new(UnboundedMemoryPool::default()),
            Arc::clone(tracker),
            label,
        ))
    }

    #[test]
    fn records_peak_drift_and_where_it_happened() {
        let (allocated, tracker) = tracker();
        let pool = pool(&tracker, "q1");

        let a = MemoryConsumer::new("a").register(&pool);
        let b = MemoryConsumer::new("b").register(&pool);

        allocated.store(1000, Ordering::Relaxed);
        a.grow(800);
        allocated.store(5000, Ordering::Relaxed);
        b.grow(200);
        allocated.store(1500, Ordering::Relaxed);
        a.shrink(800);

        assert_eq!(tracker.reserved(), 200);
        assert_eq!(
            tracker.peak_drift(),
            Some(DriftSample {
                pool: "q1".to_string(),
                consumer: "b".to_string(),
                reserved: 1000,
                allocated: 5000,
                drift: 4000,
            })
        );
    }

    #[test]
    fn sums_reservations_across_pools_sharing_a_tracker() {
        let (allocated, tracker) = tracker();
        let one = pool(&tracker, "one");
        let two = pool(&tracker, "two");

        allocated.store(1000, Ordering::Relaxed);
        let a = MemoryConsumer::new("a").register(&one);
        let b = MemoryConsumer::new("b").register(&two);
        a.grow(300);
        b.grow(400);

        // Drift is measured against the sum, so the second pool's growth
        // shrinks it rather than starting from zero.
        assert_eq!(tracker.reserved(), 700);
        let peak = tracker.peak_drift().unwrap();
        assert_eq!(peak.pool, "one");
        assert_eq!(peak.drift, 700);

        drop(a);
        drop(b);
        assert_eq!(tracker.reserved(), 0);
    }

    #[test]
    fn failed_growth_is_not_counted() {
        let (_allocated, tracker) = tracker();
        let pool: Arc<dyn MemoryPool> = Arc::new(DriftLoggingPool::new(
            Arc::new(GreedyMemoryPool::new(1024)),
            Arc::clone(&tracker),
            "limited",
        ));

        let reservation = MemoryConsumer::new("a").register(&pool);
        reservation.try_grow(600).unwrap();
        reservation.try_grow(600).unwrap_err();

        assert_eq!(tracker.reserved(), 600);
    }

    #[test]
    fn drift_can_be_negative() {
        let (_allocated, tracker) = tracker();
        let pool = pool(&tracker, "over-reserved");

        let reservation = MemoryConsumer::new("a").register(&pool);
        reservation.grow(2048);

        let peak = tracker.peak_drift().unwrap();
        assert_eq!(peak.drift, -2048);
        assert_eq!(
            peak.to_string(),
            "drift=-2.0 KB allocated=0.0 B reserved=2.0 KB pool=over-reserved consumer=a"
        );
    }

    #[test]
    fn delegates_to_the_wrapped_pool() {
        let (_allocated, tracker) = tracker();
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
        let wrapped = DriftLoggingPool::new(Arc::clone(&inner), tracker, "x");

        assert_eq!(wrapped.name(), inner.name());
        assert_eq!(wrapped.to_string(), inner.to_string());
        assert!(matches!(wrapped.memory_limit(), MemoryLimit::Finite(4096)));
    }
}
