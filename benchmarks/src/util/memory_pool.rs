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

//! Records the peak [`MemoryPool`] reservation reached during a benchmark.
//!
//! DataFusion's [`MemoryPool`] deliberately accounts for only the "large"
//! allocations that scale with input size; intermediate batches flowing between
//! operators are assumed to be small and are left untracked. The [`MemoryPool`]
//! documentation therefore advises reserving "some overhead (e.g. 10%)" on top
//! of the configured limit.
//!
//! Nothing reports what that overhead actually is, because the peak reservation
//! itself is never recorded — [`MemoryPool::reserved`] is a live value that has
//! usually fallen back to zero by the time a query finishes. This module records
//! the high-water mark so benchmarks can emit it alongside the peak RSS that
//! [`print_memory_stats`] already prints, making the gap between the two
//! measurable.
//!
//! This is measurement only: nothing here enforces a relationship between the
//! two numbers.
//!
//! [`print_memory_stats`]: super::print_memory_stats

use std::{
    fmt::{Debug, Display, Formatter},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion_common::Result;

/// High-water mark since the last [`reset_peak_pool_reserved`].
static PEAK_RESERVED: AtomicUsize = AtomicUsize::new(0);

/// High-water mark since the process started. Never reset.
static MAX_RESERVED: AtomicUsize = AtomicUsize::new(0);

/// Whether a [`PeakRecordingPool`] has ever been constructed, used to
/// distinguish "no pool was recording" from "the pool peaked at zero bytes".
static RECORDING: AtomicBool = AtomicBool::new(false);

/// Peak [`MemoryPool`] reservation, in bytes, since the last call to
/// [`reset_peak_pool_reserved`].
///
/// Returns `None` if no [`PeakRecordingPool`] has been installed, which is the
/// case whenever a benchmark runs without a memory limit.
pub fn peak_pool_reserved() -> Option<usize> {
    RECORDING
        .load(Ordering::Relaxed)
        .then(|| PEAK_RESERVED.load(Ordering::Relaxed))
}

/// Peak [`MemoryPool`] reservation, in bytes, since the process started.
///
/// Unlike [`peak_pool_reserved`] this is never reset, so it reports the peak
/// across every query in a run. Returns `None` if no [`PeakRecordingPool`] has
/// been installed.
pub fn max_pool_reserved() -> Option<usize> {
    RECORDING
        .load(Ordering::Relaxed)
        .then(|| MAX_RESERVED.load(Ordering::Relaxed))
}

/// Reset the value returned by [`peak_pool_reserved`], so the next reading
/// covers only what follows.
///
/// [`BenchmarkRun::start_new_case`] calls this, giving each benchmark query its
/// own reading.
///
/// [`BenchmarkRun::start_new_case`]: super::BenchmarkRun::start_new_case
pub fn reset_peak_pool_reserved() {
    PEAK_RESERVED.store(0, Ordering::Relaxed);
}

/// Wraps a [`MemoryPool`], recording the high-water mark of
/// [`MemoryPool::reserved`] as reservations come and go.
///
/// Every method delegates to the wrapped pool, so wrapping does not change how
/// memory is granted, limited, or reported. Peaks are published to the
/// process-wide counters read by [`peak_pool_reserved`] and
/// [`max_pool_reserved`] rather than held per instance, so callers can read
/// them without threading a handle through the benchmark. The benchmarks run
/// one query at a time, so a process-wide counter attributes cleanly.
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
/// # use datafusion_benchmarks::util::{
/// #     PeakRecordingPool, peak_pool_reserved, reset_peak_pool_reserved,
/// # };
/// let pool: Arc<dyn MemoryPool> =
///     Arc::new(PeakRecordingPool::new(Arc::new(GreedyMemoryPool::new(1024))));
/// reset_peak_pool_reserved();
///
/// let reservation = MemoryConsumer::new("example").register(&pool);
/// reservation.try_grow(512)?;
/// reservation.shrink(512);
///
/// // The pool is back to empty, but the high-water mark is retained.
/// assert_eq!(pool.reserved(), 0);
/// assert_eq!(peak_pool_reserved(), Some(512));
/// # Ok::<(), datafusion_common::DataFusionError>(())
/// ```
pub struct PeakRecordingPool {
    inner: Arc<dyn MemoryPool>,
}

impl PeakRecordingPool {
    /// Wrap `inner`, recording its peak reservation from here on.
    pub fn new(inner: Arc<dyn MemoryPool>) -> Self {
        RECORDING.store(true, Ordering::Relaxed);
        Self { inner }
    }

    /// The wrapped pool.
    pub fn inner(&self) -> &Arc<dyn MemoryPool> {
        &self.inner
    }

    /// Publish the pool's current reservation to both high-water marks.
    ///
    /// Called after any operation that can raise `reserved()`. Reading the
    /// total rather than accumulating deltas keeps this correct when the
    /// wrapped pool declines or adjusts a request.
    fn record(&self) {
        let reserved = self.inner.reserved();
        PEAK_RESERVED.fetch_max(reserved, Ordering::Relaxed);
        MAX_RESERVED.fetch_max(reserved, Ordering::Relaxed);
    }
}

impl Debug for PeakRecordingPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeakRecordingPool")
            .field("inner", &self.inner)
            .field("peak", &PEAK_RESERVED.load(Ordering::Relaxed))
            .finish()
    }
}

impl Display for PeakRecordingPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Deferring to the wrapped pool keeps `SHOW ALL`-style output and error
        // messages identical to running without the wrapper.
        Display::fmt(&self.inner, f)
    }
}

impl MemoryPool for PeakRecordingPool {
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
        self.record();
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.record();
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
    use std::sync::{Mutex, MutexGuard};

    use datafusion::execution::memory_pool::GreedyMemoryPool;

    use super::*;

    /// The high-water marks are process-wide, so these tests would otherwise
    /// clobber each other when the test harness runs them in parallel.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    /// Take the lock and hand back a freshly reset pool. The guard is returned
    /// so it stays held for the body of the test.
    fn pool(limit: usize) -> (Arc<dyn MemoryPool>, MutexGuard<'static, ()>) {
        let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let pool: Arc<dyn MemoryPool> = Arc::new(PeakRecordingPool::new(Arc::new(
            GreedyMemoryPool::new(limit),
        )));
        reset_peak_pool_reserved();
        (pool, guard)
    }

    #[test]
    fn records_high_water_mark_across_reservations() {
        let (pool, _guard) = pool(1024);

        let a = MemoryConsumer::new("a").register(&pool);
        let b = MemoryConsumer::new("b").register(&pool);

        a.try_grow(300).unwrap();
        b.try_grow(400).unwrap();
        // Peak of the sum, not the largest single reservation.
        assert_eq!(peak_pool_reserved(), Some(700));

        a.shrink(300);
        b.try_grow(100).unwrap();

        // Falling back below the peak leaves it untouched, and the later growth
        // does not reach it.
        assert_eq!(pool.reserved(), 500);
        assert_eq!(peak_pool_reserved(), Some(700));
    }

    #[test]
    fn failed_growth_does_not_move_the_peak() {
        let (pool, _guard) = pool(1024);

        let reservation = MemoryConsumer::new("a").register(&pool);
        reservation.try_grow(600).unwrap();
        reservation
            .try_grow(600)
            .expect_err("should exceed the 1024 byte pool");

        assert_eq!(peak_pool_reserved(), Some(600));
    }

    #[test]
    fn reset_clears_the_window_but_not_the_run_maximum() {
        let (pool, _guard) = pool(1024);

        let reservation = MemoryConsumer::new("a").register(&pool);
        reservation.try_grow(800).unwrap();
        reservation.shrink(800);

        reset_peak_pool_reserved();
        assert_eq!(peak_pool_reserved(), Some(0));
        assert!(max_pool_reserved().unwrap() >= 800);

        reservation.try_grow(100).unwrap();
        assert_eq!(peak_pool_reserved(), Some(100));
    }

    #[test]
    fn delegates_limit_and_name_to_the_wrapped_pool() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
        let wrapped = PeakRecordingPool::new(Arc::clone(&inner));

        assert_eq!(wrapped.name(), inner.name());
        assert_eq!(wrapped.to_string(), inner.to_string());
        assert!(matches!(wrapped.memory_limit(), MemoryLimit::Finite(4096)));
    }
}
