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
//! `print_memory_stats` already prints, making the gap between the two
//! measurable.
//!
//! This is measurement only: nothing here enforces a relationship between the
//! two numbers.
//!
//! What lands in the peak is whatever the pool accounts for, so this follows
//! the accounting rather than fixing it in place. Arrow-side reservations made
//! through `ArrowMemoryPool` are included, because that adapter grows a
//! DataFusion reservation against the pool it wraps; nothing claims buffers
//! today, but the peak picks it up when something does.

use std::{
    fmt::{Debug, Display, Formatter},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation};
use datafusion_common::Result;

/// Wraps a [`MemoryPool`], recording the high-water mark of
/// [`MemoryPool::reserved`] as reservations come and go.
///
/// Every method delegates to the wrapped pool, so wrapping does not change how
/// memory is granted, limited, or reported. The one thing it does change is
/// downcasting: `rt.memory_pool.downcast_ref::<FairSpillPool>()` now finds this
/// wrapper instead of the pool it wraps. Nothing in the benchmarks relies on
/// that, and [`Self::from_pool`] uses the same mechanism to find the recorder.
///
/// Both high-water marks are held per instance, so a benchmark that builds a
/// fresh runtime per query gets a reading scoped to that query without any
/// coordination.
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool, PeakRecordingPool};
/// let recording = Arc::new(PeakRecordingPool::new(Arc::new(GreedyMemoryPool::new(1024))));
/// let pool: Arc<dyn MemoryPool> = Arc::clone(&recording) as _;
///
/// let reservation = MemoryConsumer::new("example").register(&pool);
/// reservation.try_grow(512)?;
/// reservation.shrink(512);
///
/// // The pool is back to empty, but the high-water mark is retained.
/// assert_eq!(pool.reserved(), 0);
/// assert_eq!(recording.peak_reserved(), 512);
///
/// // The recorder can also be recovered from the pool it was installed as.
/// assert_eq!(PeakRecordingPool::from_pool(&*pool).unwrap().peak_reserved(), 512);
/// # Ok::<(), datafusion_common::DataFusionError>(())
/// ```
pub struct PeakRecordingPool {
    inner: Arc<dyn MemoryPool>,
    /// Running total of everything granted through this wrapper, kept so the
    /// peak can be maintained without asking `inner` for its total.
    reserved: AtomicUsize,
    /// High-water mark since the last [`PeakRecordingPool::reset_peak`].
    peak: AtomicUsize,
    /// High-water mark since this pool was created. Never reset.
    max: AtomicUsize,
}

impl PeakRecordingPool {
    /// Wrap `inner`, recording its peak reservation from here on.
    ///
    /// `inner` is expected to be empty: the running total starts at zero, so
    /// anything reserved before wrapping is not counted.
    pub fn new(inner: Arc<dyn MemoryPool>) -> Self {
        Self {
            inner,
            reserved: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            max: AtomicUsize::new(0),
        }
    }

    /// The recorder installed as `pool`, if there is one.
    ///
    /// Returns `None` whenever a benchmark runs without a memory limit, since
    /// `CommonOpt::runtime_env_builder` only installs the wrapper alongside a
    /// pool it has a limit for.
    pub fn from_pool(pool: &dyn MemoryPool) -> Option<&Self> {
        pool.downcast_ref::<Self>()
    }

    /// Peak reservation, in bytes, since the last [`Self::reset_peak`].
    pub fn peak_reserved(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Peak reservation, in bytes, since this pool was created.
    ///
    /// Unlike [`Self::peak_reserved`] this is never reset, so it reports the
    /// peak across every query that shared this pool.
    pub fn max_reserved(&self) -> usize {
        self.max.load(Ordering::Relaxed)
    }

    /// Reset the value returned by [`Self::peak_reserved`] to what is reserved
    /// right now, so the next reading covers only what follows.
    ///
    /// `BenchmarkRun::start_new_case` calls this, giving each benchmark query
    /// its own reading. Anything still held when a query starts — data the
    /// benchmark loaded up front, say — stays in the reading, since the query
    /// runs with those bytes reserved.
    pub fn reset_peak(&self) {
        self.peak
            .store(self.reserved.load(Ordering::Relaxed), Ordering::Relaxed);
    }

    /// Add `additional` granted bytes to the running total and publish it to
    /// both high-water marks.
    ///
    /// Accumulating deltas rather than reading [`MemoryPool::reserved`] keeps
    /// the wrapped pool's own bookkeeping off this path: `FairSpillPool` takes
    /// its state lock to answer `reserved()`, which would double the lock
    /// traffic of every accounted allocation in the benchmark being measured.
    /// The total stays exact because the trait grants exactly what is asked
    /// for — `grow` is infallible and `try_grow` either grants `additional` or
    /// returns an error, leaving the reservation untouched.
    fn record(&self, additional: usize) {
        let reserved =
            self.reserved.fetch_add(additional, Ordering::Relaxed) + additional;
        self.peak.fetch_max(reserved, Ordering::Relaxed);
        self.max.fetch_max(reserved, Ordering::Relaxed);
    }
}

impl Debug for PeakRecordingPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeakRecordingPool")
            .field("inner", &self.inner)
            .field("peak", &self.peak_reserved())
            .field("max", &self.max_reserved())
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
        self.record(additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.reserved.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.record(additional);
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
    use crate::memory_pool::GreedyMemoryPool;

    use super::*;

    /// A recording pool over a `GreedyMemoryPool`, returned both as the
    /// recorder (to read the marks) and as the pool reservations register with.
    fn pool(limit: usize) -> (Arc<PeakRecordingPool>, Arc<dyn MemoryPool>) {
        let recording = Arc::new(PeakRecordingPool::new(Arc::new(
            GreedyMemoryPool::new(limit),
        )));
        let pool = Arc::clone(&recording) as Arc<dyn MemoryPool>;
        (recording, pool)
    }

    #[test]
    fn records_high_water_mark_across_reservations() {
        let (recording, pool) = pool(1024);

        let a = MemoryConsumer::new("a").register(&pool);
        let b = MemoryConsumer::new("b").register(&pool);

        a.try_grow(300).unwrap();
        b.try_grow(400).unwrap();
        // Peak of the sum, not the largest single reservation.
        assert_eq!(recording.peak_reserved(), 700);

        a.shrink(300);
        b.try_grow(100).unwrap();

        // Falling back below the peak leaves it untouched, and the later growth
        // does not reach it.
        assert_eq!(pool.reserved(), 500);
        assert_eq!(recording.peak_reserved(), 700);
    }

    #[test]
    fn failed_growth_does_not_move_the_peak() {
        let (recording, pool) = pool(1024);

        let reservation = MemoryConsumer::new("a").register(&pool);
        reservation.try_grow(600).unwrap();
        reservation
            .try_grow(600)
            .expect_err("should exceed the 1024 byte pool");

        assert_eq!(recording.peak_reserved(), 600);
    }

    #[test]
    fn reset_clears_the_window_but_not_the_run_maximum() {
        let (recording, pool) = pool(1024);

        let reservation = MemoryConsumer::new("a").register(&pool);
        reservation.try_grow(800).unwrap();
        reservation.shrink(800);

        recording.reset_peak();
        assert_eq!(recording.peak_reserved(), 0);
        assert_eq!(recording.max_reserved(), 800);

        reservation.try_grow(100).unwrap();
        assert_eq!(recording.peak_reserved(), 100);
        assert_eq!(recording.max_reserved(), 800);
    }

    #[test]
    fn reset_keeps_what_is_still_reserved() {
        let (recording, pool) = pool(1024);

        // Something a benchmark loaded up front and holds across queries.
        let held = MemoryConsumer::new("held").register(&pool);
        held.try_grow(300).unwrap();

        recording.reset_peak();
        assert_eq!(recording.peak_reserved(), 300);

        let query = MemoryConsumer::new("query").register(&pool);
        query.try_grow(200).unwrap();
        assert_eq!(recording.peak_reserved(), 500);
    }

    #[test]
    fn marks_are_per_instance() {
        let (one, one_pool) = pool(1024);
        let (two, _two_pool) = pool(1024);

        MemoryConsumer::new("a")
            .register(&one_pool)
            .try_grow(512)
            .unwrap();

        assert_eq!(one.peak_reserved(), 512);
        assert_eq!(two.peak_reserved(), 0);
    }

    #[test]
    fn is_recoverable_from_the_pool_it_is_installed_as() {
        let (recording, pool) = pool(1024);

        MemoryConsumer::new("a")
            .register(&pool)
            .try_grow(512)
            .unwrap();

        let found = PeakRecordingPool::from_pool(&*pool).expect("recorder installed");
        assert_eq!(found.peak_reserved(), recording.peak_reserved());

        // A pool with no recorder in front of it reports nothing.
        let plain: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        assert!(PeakRecordingPool::from_pool(&*plain).is_none());
    }

    #[test]
    fn delegates_limit_and_name_to_the_wrapped_pool() {
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
        let wrapped = PeakRecordingPool::new(Arc::clone(&inner));

        assert_eq!(wrapped.name(), inner.name());
        assert_eq!(wrapped.to_string(), inner.to_string());
        assert!(matches!(wrapped.memory_limit(), MemoryLimit::Finite(4096)));
    }

    /// Arrow-side reservations reach the recorder too.
    ///
    /// [`ArrowMemoryPool`] implements Arrow's `MemoryPool` by growing a
    /// DataFusion [`MemoryReservation`] against the pool it wraps, so a buffer
    /// claimed through it lands in `grow` here. Nothing in DataFusion claims
    /// buffers yet (see apache/datafusion#22898), but when something does, the
    /// bytes show up in this peak without further changes — as long as the
    /// adapter is built from the `RuntimeEnv`'s pool, which is the wrapped one.
    /// This test pins that.
    ///
    /// Only compiled with `--features arrow_buffer_pool`, since that's what
    /// gates `crate::memory_pool::arrow` and `arrow_buffer::MemoryPool` in the
    /// first place; not part of this crate's default feature set.
    #[cfg(feature = "arrow_buffer_pool")]
    #[test]
    fn records_reservations_arriving_through_the_arrow_adapter() {
        use crate::memory_pool::arrow::ArrowMemoryPool;
        use arrow_buffer::MemoryPool as ArrowMemoryPoolTrait;

        let (recording, pool) = pool(4096);

        let arrow_pool =
            ArrowMemoryPool::new(Arc::clone(&pool), MemoryConsumer::new("arrow"));
        let reservation = arrow_pool.reserve(1024);

        // The Arrow-side reservation is visible as DataFusion pool usage...
        assert_eq!(pool.reserved(), 1024);
        assert_eq!(recording.peak_reserved(), 1024);

        // ...and dropping it releases the bytes while the peak is retained.
        drop(reservation);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(recording.peak_reserved(), 1024);
    }
}
