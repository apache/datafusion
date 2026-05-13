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

use crate::memory_pool::reclaimer::reclaimer_state;
use crate::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReclaimer, MemoryReservation,
    human_readable_size,
};
use datafusion_common::HashMap;
use datafusion_common::{DataFusionError, Result, resources_datafusion_err};
use log::debug;
use parking_lot::{Mutex, RwLock};
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicU8, AtomicUsize, Ordering},
};

/// How long [`TrackConsumersPool::try_grow_async`] waits for an
/// in-flight sibling to finish reclaiming before retrying. Kept short
/// so we don't stall the requestor longer than the typical reclaim
/// (mpsc send + spill commit).
const RECLAIM_RETRY_SLEEP: Duration = Duration::from_millis(50);

/// Maximum number of times [`TrackConsumersPool::try_grow_async`]
/// retries the candidate walk while siblings are still in-flight.
/// Bounds the total wait at `MAX_RECLAIM_RETRIES * RECLAIM_RETRY_SLEEP`
/// so a livelock surfaces as OOM rather than a hang.
const MAX_RECLAIM_RETRIES: usize = 3;

/// A [`MemoryPool`] that enforces no limit
#[derive(Debug, Default)]
pub struct UnboundedMemoryPool {
    used: AtomicUsize,
}

impl MemoryPool for UnboundedMemoryPool {
    fn name(&self) -> &str {
        "unbounded"
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.used.fetch_add(additional, Ordering::Relaxed);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.used.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.grow(reservation, additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Infinite
    }
}

impl Display for UnboundedMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let used = self.used.load(Ordering::Relaxed);
        write!(f, "{}(used: {})", &self.name(), human_readable_size(used))
    }
}

/// A [`MemoryPool`] that implements a greedy first-come first-serve limit.
///
/// This pool works well for queries that do not need to spill or have
/// a single spillable operator. See [`FairSpillPool`] if there are
/// multiple spillable operators that all will spill.
#[derive(Debug)]
pub struct GreedyMemoryPool {
    pool_size: usize,
    used: AtomicUsize,
}

impl GreedyMemoryPool {
    /// Create a new pool that can allocate up to `pool_size` bytes
    pub fn new(pool_size: usize) -> Self {
        debug!("Created new GreedyMemoryPool(pool_size={pool_size})");
        Self {
            pool_size,
            used: AtomicUsize::new(0),
        }
    }
}

impl MemoryPool for GreedyMemoryPool {
    fn name(&self) -> &str {
        "greedy"
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.used.fetch_add(additional, Ordering::Relaxed);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.used.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                let new_used = used + additional;
                (new_used <= self.pool_size).then_some(new_used)
            })
            .map_err(|used| {
                insufficient_capacity_err(
                    reservation,
                    additional,
                    self.pool_size.saturating_sub(used),
                    self,
                )
            })?;
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.pool_size)
    }
}

impl Display for GreedyMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let used = self.used.load(Ordering::Relaxed);
        write!(
            f,
            "{}(used: {}, pool_size: {})",
            &self.name(),
            human_readable_size(used),
            human_readable_size(self.pool_size)
        )
    }
}

/// A [`MemoryPool`] that prevents spillable reservations from using more than
/// an even fraction of the available memory sans any unspillable reservations
/// (i.e. `(pool_size - unspillable_memory) / num_spillable_reservations`)
///
/// This pool works best when you know beforehand the query has
/// multiple spillable operators that will likely all need to
/// spill. Sometimes it will cause spills even when there was
/// sufficient memory (reserved for other operators) to avoid doing
/// so.
///
/// ```text
///    ┌───────────────────────z──────────────────────z───────────────┐
///    │                       z                      z               │
///    │                       z                      z               │
///    │       Spillable       z       Unspillable    z     Free      │
///    │        Memory         z        Memory        z    Memory     │
///    │                       z                      z               │
///    │                       z                      z               │
///    └───────────────────────z──────────────────────z───────────────┘
/// ```
///
/// Unspillable memory is allocated in a first-come, first-serve fashion
#[derive(Debug)]
pub struct FairSpillPool {
    /// The total memory limit
    pool_size: usize,

    state: Mutex<FairSpillPoolState>,
}

#[derive(Debug)]
struct FairSpillPoolState {
    /// The number of consumers that can spill
    num_spill: usize,

    /// The total amount of memory reserved that can be spilled
    spillable: usize,

    /// The total amount of memory reserved by consumers that cannot spill
    unspillable: usize,
}

impl FairSpillPool {
    /// Allocate up to `limit` bytes
    pub fn new(pool_size: usize) -> Self {
        debug!("Created new FairSpillPool(pool_size={pool_size})");
        Self {
            pool_size,
            state: Mutex::new(FairSpillPoolState {
                num_spill: 0,
                spillable: 0,
                unspillable: 0,
            }),
        }
    }
}

impl MemoryPool for FairSpillPool {
    fn name(&self) -> &str {
        "fair"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        if consumer.can_spill {
            self.state.lock().num_spill += 1;
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        if consumer.can_spill {
            let mut state = self.state.lock();
            state.num_spill = state.num_spill.checked_sub(1).unwrap();
        }
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock();
        match reservation.registration.consumer.can_spill {
            true => state.spillable += additional,
            false => state.unspillable += additional,
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.state.lock();
        match reservation.registration.consumer.can_spill {
            true => state.spillable -= shrink,
            false => state.unspillable -= shrink,
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.state.lock();

        match reservation.registration.consumer.can_spill {
            true => {
                // The total amount of memory available to spilling consumers
                let spill_available = self.pool_size.saturating_sub(state.unspillable);

                // No spiller may use more than their fraction of the memory available
                let available = spill_available
                    .checked_div(state.num_spill)
                    .unwrap_or(spill_available);

                if reservation.size() + additional > available {
                    return Err(insufficient_capacity_err(
                        reservation,
                        additional,
                        available,
                        self,
                    ));
                }
                state.spillable += additional;
            }
            false => {
                let available = self
                    .pool_size
                    .saturating_sub(state.unspillable + state.spillable);

                if available < additional {
                    return Err(insufficient_capacity_err(
                        reservation,
                        additional,
                        available,
                        self,
                    ));
                }
                state.unspillable += additional;
            }
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        let state = self.state.lock();
        state.spillable + state.unspillable
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.pool_size)
    }
}

impl Display for FairSpillPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}(pool_size: {})",
            &self.name(),
            human_readable_size(self.pool_size),
        )
    }
}

/// Constructs a resources error based upon the individual [`MemoryReservation`].
///
/// The error references the `bytes already allocated` for the reservation,
/// and not the total within the collective [`MemoryPool`],
/// nor the total across multiple reservations with the same [`MemoryConsumer`].
#[inline(always)]
fn insufficient_capacity_err(
    reservation: &MemoryReservation,
    additional: usize,
    available: usize,
    pool: &impl MemoryPool,
) -> DataFusionError {
    resources_datafusion_err!(
        "Failed to allocate additional {} for {} with {} already allocated for this reservation - {} remain available for the total memory pool: {}",
        human_readable_size(additional),
        reservation.registration.consumer.name,
        human_readable_size(reservation.size()),
        human_readable_size(available),
        pool
    )
}

#[derive(Debug)]
struct TrackedConsumer {
    name: String,
    can_spill: bool,
    reserved: AtomicUsize,
    peak: AtomicUsize,
    reclaimer: Option<Arc<dyn MemoryReclaimer>>,
    /// Tri-state eligibility flag for [`reclaimer`], encoded per
    /// [`reclaimer_state`]. The pool flips `AVAILABLE` ↔ `IN_FLIGHT`
    /// for dedup; the reclaimer's owner may sticky-set `DISABLED` once
    /// it can no longer free memory. Shared `Arc` so the reclaimer
    /// side and the pool see the same cell. `None` reclaimer ⇒ flag
    /// is unused but still allocated.
    reclaimer_state: Arc<AtomicU8>,
}

/// RAII guard for the [`IN_FLIGHT`] slot of a [`TrackedConsumer`]'s
/// `reclaimer_state` flag. `Drop` only restores `AVAILABLE` if the
/// state is still `IN_FLIGHT` — leaves a sticky `DISABLED` alone.
///
/// [`IN_FLIGHT`]: reclaimer_state::IN_FLIGHT
struct ReclaimerStateGuard {
    flag: Arc<AtomicU8>,
}

impl Drop for ReclaimerStateGuard {
    fn drop(&mut self) {
        let _ = self.flag.compare_exchange(
            reclaimer_state::IN_FLIGHT,
            reclaimer_state::AVAILABLE,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }
}

impl ReclaimerStateGuard {
    /// Try to transition the flag from `AVAILABLE` to `IN_FLIGHT`.
    /// Fails on contention or on a sticky `DISABLED`.
    fn try_acquire(flag: &Arc<AtomicU8>) -> Option<Self> {
        flag.compare_exchange(
            reclaimer_state::AVAILABLE,
            reclaimer_state::IN_FLIGHT,
            Ordering::AcqRel,
            Ordering::Relaxed,
        )
        .ok()
        .map(|_| Self {
            flag: Arc::clone(flag),
        })
    }
}

impl TrackedConsumer {
    /// Shorthand to return the currently reserved value
    fn reserved(&self) -> usize {
        self.reserved.load(Ordering::Relaxed)
    }

    /// Return the peak value
    fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Grows the tracked consumer's reserved size,
    /// should be called after the pool has successfully performed the grow().
    ///
    /// Uses the value `reserved` definitely held immediately after this
    /// thread's `fetch_add` as the peak candidate, then bumps `peak` via a
    /// monotone-max CAS loop. This avoids the race in the previous
    /// `peak.fetch_max(self.reserved())` form, where a concurrent `shrink`
    /// between the load of `reserved` and the max-write to `peak` could
    /// record a peak below the true high-water mark.
    fn grow(&self, additional: usize) {
        let prev = self.reserved.fetch_add(additional, Ordering::Relaxed);
        let new = prev + additional;

        let mut peak = self.peak.load(Ordering::Relaxed);
        while peak < new {
            match self.peak.compare_exchange_weak(
                peak,
                new,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }
    }

    /// Reduce the tracked consumer's reserved size,
    /// should be called after the pool has successfully performed the shrink().
    fn shrink(&self, shrink: usize) {
        self.reserved.fetch_sub(shrink, Ordering::Relaxed);
    }
}

/// A point-in-time snapshot of a tracked memory consumer's state.
///
/// Returned by [`TrackConsumersPool::metrics()`].
#[derive(Debug, Clone)]
pub struct MemoryConsumerMetrics {
    /// The name of the memory consumer
    pub name: String,
    /// Whether this consumer can spill to disk
    pub can_spill: bool,
    /// The number of bytes currently reserved by this consumer
    pub reserved: usize,
    /// The peak number of bytes reserved by this consumer
    pub peak: usize,
}

impl From<&TrackedConsumer> for MemoryConsumerMetrics {
    fn from(tracked: &TrackedConsumer) -> Self {
        Self {
            name: tracked.name.clone(),
            can_spill: tracked.can_spill,
            reserved: tracked.reserved(),
            peak: tracked.peak(),
        }
    }
}

/// A [`MemoryPool`] that tracks the consumers that have
/// reserved memory within the inner memory pool.
///
/// By tracking memory reservations more carefully this pool
/// can provide better error messages on the largest memory users
/// when memory allocation fails.
///
/// Tracking is per hashed [`MemoryConsumer`], not per [`MemoryReservation`].
/// The same consumer can have multiple reservations.
///
/// # Automatic Usage via [`RuntimeEnvBuilder`]
///
/// The easiest way to use `TrackConsumersPool` is via
/// [`RuntimeEnvBuilder::with_memory_limit()`].
///
/// [`RuntimeEnvBuilder`]: crate::runtime_env::RuntimeEnvBuilder
/// [`RuntimeEnvBuilder::with_memory_limit()`]: crate::runtime_env::RuntimeEnvBuilder::with_memory_limit
///
/// # Usage Examples
///
/// For more examples of using `TrackConsumersPool`, see the [memory_pool_tracking.rs] example
///
/// [memory_pool_tracking.rs]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/execution_monitoring/memory_pool_tracking.rs
/// [memory_pool_execution_plan.rs]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/execution_monitoring/memory_pool_execution_plan.rs
#[derive(Debug)]
pub struct TrackConsumersPool<I> {
    /// The wrapped memory pool that actually handles reservation logic
    inner: I,
    /// The amount of consumers to report(ordered top to bottom by reservation size)
    top: NonZeroUsize,
    /// Cap on the number of reclaim candidates considered per
    /// [`try_grow_async`] call. Bounds reclaim work when many consumers
    /// are registered. Defaults to 4; override with
    /// [`Self::with_reclaim_candidate_limit`].
    reclaim_candidate_limit: NonZeroUsize,
    /// Maps consumer_id --> TrackedConsumer.
    ///
    /// Protected by an [`RwLock`] rather than a [`Mutex`]: registration
    /// (insert) and unregistration (remove) take the write lock; grow,
    /// shrink, try_grow, metrics, and report_top take the read lock and run
    /// concurrently. The per-consumer [`AtomicUsize`] fields are mutated
    /// under the shared read lock — see [`TrackedConsumer::grow`].
    tracked_consumers: RwLock<HashMap<usize, TrackedConsumer>>,
}

impl<I: MemoryPool> Display for TrackConsumersPool<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}(inner_pool: {}, num_of_top_consumers: {})",
            &self.name(),
            &self.inner,
            &self.top,
        )
    }
}

impl<I: MemoryPool> TrackConsumersPool<I> {
    /// Creates a new [`TrackConsumersPool`].
    ///
    /// # Arguments
    /// * `inner` - The underlying memory pool that handles actual memory allocation
    /// * `top` - The number of top memory consumers to include in error messages
    ///
    /// # Note
    /// In most cases, you should use [`RuntimeEnvBuilder::with_memory_limit()`](crate::runtime_env::RuntimeEnvBuilder::with_memory_limit)
    /// instead of creating this pool manually, as it automatically sets up tracking with
    /// sensible defaults (top 5 consumers).
    ///
    /// # Example
    ///
    /// ```rust
    /// use datafusion_execution::memory_pool::{
    ///     FairSpillPool, GreedyMemoryPool, TrackConsumersPool,
    /// };
    /// use std::num::NonZeroUsize;
    ///
    /// // Create with a greedy pool backend, reporting top 3 consumers in error messages
    /// let tracked_greedy = TrackConsumersPool::new(
    ///     GreedyMemoryPool::new(1024 * 1024), // 1MB limit
    ///     NonZeroUsize::new(3).unwrap(),
    /// );
    ///
    /// // Create with a fair spill pool backend, reporting top 5 consumers in error messages
    /// let tracked_fair = TrackConsumersPool::new(
    ///     FairSpillPool::new(2 * 1024 * 1024), // 2MB limit
    ///     NonZeroUsize::new(5).unwrap(),
    /// );
    /// ```
    ///
    /// # Impact on Error Messages
    ///
    /// The `top` determines how many Top K [`MemoryConsumer`]s to include
    /// in the reported [`DataFusionError::ResourcesExhausted`].
    pub fn new(inner: I, top: NonZeroUsize) -> Self {
        Self {
            inner,
            top,
            reclaim_candidate_limit: NonZeroUsize::new(4).unwrap(),
            tracked_consumers: Default::default(),
        }
    }

    /// Override the cap on reclaim candidates considered per
    /// [`try_grow_async`] call (default `4`).
    pub fn with_reclaim_candidate_limit(mut self, n: NonZeroUsize) -> Self {
        self.reclaim_candidate_limit = n;
        self
    }

    /// Returns a reference to the wrapped inner [`MemoryPool`].
    pub fn inner(&self) -> &I {
        &self.inner
    }

    /// Returns a snapshot of all currently tracked consumers.
    pub fn metrics(&self) -> Vec<MemoryConsumerMetrics> {
        self.tracked_consumers
            .read()
            .values()
            .map(Into::into)
            .collect()
    }

    /// Returns a formatted string with the top memory consumers.
    pub fn report_top(&self, top: usize) -> String {
        let mut consumers = self
            .tracked_consumers
            .read()
            .iter()
            .map(|(consumer_id, tracked_consumer)| {
                (
                    (
                        *consumer_id,
                        tracked_consumer.name.to_owned(),
                        tracked_consumer.can_spill,
                        tracked_consumer.peak(),
                    ),
                    tracked_consumer.reserved(),
                )
            })
            .collect::<Vec<_>>();
        consumers.sort_by(|a, b| b.1.cmp(&a.1)); // inverse ordering

        consumers[0..std::cmp::min(top, consumers.len())]
            .iter()
            .map(|((id, name, can_spill, peak), size)| {
                format!(
                    "  {name}#{id}(can spill: {can_spill}) consumed {}, peak {}",
                    human_readable_size(*size),
                    human_readable_size(*peak),
                )
            })
            .collect::<Vec<_>>()
            .join(",\n")
            + "."
    }
}

impl<I: MemoryPool> MemoryPool for TrackConsumersPool<I> {
    fn name(&self) -> &str {
        "track_consumers"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);

        let reclaimer = consumer.reclaimer().cloned();
        // Reuse the reclaimer's own flag when it provides one — that
        // way the reclaimer side can sticky-set `DISABLED` and the
        // pool sees it on the next filter pass. Otherwise allocate a
        // fresh `AVAILABLE` flag for in-flight dedup only.
        let state = reclaimer
            .as_ref()
            .and_then(|r| r.reclaimer_state())
            .unwrap_or_else(|| Arc::new(AtomicU8::new(reclaimer_state::AVAILABLE)));

        let mut guard = self.tracked_consumers.write();
        let existing = guard.insert(
            consumer.id(),
            TrackedConsumer {
                name: consumer.name().to_string(),
                can_spill: consumer.can_spill(),
                reserved: Default::default(),
                peak: Default::default(),
                reclaimer,
                reclaimer_state: state,
            },
        );

        debug_assert!(
            existing.is_none(),
            "Registered was called twice on the same consumer"
        );
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
        self.tracked_consumers.write().remove(&consumer.id());
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        if let Some(tracked) = self
            .tracked_consumers
            .read()
            .get(&reservation.consumer().id())
        {
            tracked.grow(additional);
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        if let Some(tracked) = self
            .tracked_consumers
            .read()
            .get(&reservation.consumer().id())
        {
            tracked.shrink(shrink);
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner
            .try_grow(reservation, additional)
            .map_err(|e| match e {
                DataFusionError::ResourcesExhausted(e) => {
                    // wrap OOM message in top consumers
                    DataFusionError::ResourcesExhausted(
                        provide_top_memory_consumers_to_error_msg(
                            &reservation.consumer().name,
                            &e,
                            &self.report_top(self.top.into()),
                        ),
                    )
                }
                _ => e,
            })?;

        if let Some(tracked) = self
            .tracked_consumers
            .read()
            .get(&reservation.consumer().id())
        {
            tracked.grow(additional);
        }
        Ok(())
    }

    fn try_grow_async<'a>(
        &'a self,
        reservation: &'a MemoryReservation,
        additional: usize,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            // Fast path.
            let initial_err = match self.try_grow(reservation, additional) {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };

            // Mark the requestor as IN_FLIGHT for the duration of this
            // walk. Without this, a victim's reclaim handler that
            // recursively triggers `pool.reclaim` (e.g. a merge stream
            // started inside an `ExternalSorter` spill) could pick the
            // requestor as its own victim, send it a reclaim oneshot,
            // and deadlock — the requestor is blocked here at
            // `reclaimer.reclaim().await` and can't drain its own
            // reclaim channel. Sticky-disabled or already-in-flight
            // requestors aren't acquired; the walk proceeds without
            // protection (the candidate filter still rejects the
            // requestor by id).
            let requestor_id = reservation.consumer().id();
            let (_self_guard, requestor_has_reclaimer) = {
                let guard = self.tracked_consumers.read();
                let tc = guard.get(&requestor_id);
                let has_reclaimer = tc.map(|tc| tc.reclaimer.is_some()).unwrap_or(false);
                let g = tc
                    .and_then(|tc| ReclaimerStateGuard::try_acquire(&tc.reclaimer_state));
                (g, has_reclaimer)
            };

            let mut retries: usize = 0;
            loop {
                // Snapshot reclaimers. When the requestor has its own reclaimer,
                // only consumers strictly larger than the requestor are
                // eligible: smaller-or-equal siblings would free less than the
                // requestor itself can, so the requestor should self-spill
                // instead. This size-ordering also breaks mutual-reclaim
                // cycles among reclaimable consumers (A targets B while B
                // targets A) — at most one side of any pair can hold strictly
                // more memory, so the other side has no candidates and
                // surfaces an error for the caller's self-spill fallback.
                //
                // When the requestor has no reclaimer it cannot self-spill,
                // so this filter is skipped — any positive sibling is a
                // valid victim. No cycle is possible through a no-reclaimer
                // requestor: it can never be selected as a candidate (the
                // `reclaimer.as_ref()?` guard below rejects it), so no other
                // walk can target it.
                //
                // Filter out anyone whose `reclaimer_state` flag is not
                // `AVAILABLE` (in-flight or sticky-disabled). Also count
                // IN_FLIGHT siblings so we know whether to wait briefly for
                // them to finish before giving up. Drop the read guard
                // before awaiting any reclaim.
                let requestor_reserved = {
                    let guard = self.tracked_consumers.read();
                    guard
                        .get(&requestor_id)
                        .map(|tc| tc.reserved())
                        .unwrap_or(0)
                };
                let mut in_flight_seen: usize = 0;
                let mut candidates: Vec<(
                    usize,
                    Arc<dyn MemoryReclaimer>,
                    Arc<AtomicU8>,
                )> = {
                    let guard = self.tracked_consumers.read();
                    guard
                        .iter()
                        .filter_map(|(cid, tc)| {
                            if *cid == requestor_id {
                                return None;
                            }
                            // Track in-flight siblings (any size) so we can
                            // decide whether a retry has any chance of helping.
                            let state = tc.reclaimer_state.load(Ordering::Acquire);
                            if state == reclaimer_state::IN_FLIGHT {
                                in_flight_seen += 1;
                            }
                            let reclaimer = tc.reclaimer.as_ref()?;
                            if requestor_has_reclaimer
                                && tc.reserved() <= requestor_reserved
                            {
                                return None;
                            }
                            if state != reclaimer_state::AVAILABLE {
                                return None;
                            }
                            Some((
                                tc.reserved(),
                                Arc::clone(reclaimer),
                                Arc::clone(&tc.reclaimer_state),
                            ))
                        })
                        .collect()
                };
                // Order: priority desc, then reservation size desc.
                candidates.sort_by(|(lr, l, _), (rr, r, _)| {
                    r.priority().cmp(&l.priority()).then_with(|| rr.cmp(lr))
                });
                // Cap reclaim work — only consider the top-ranked candidates.
                candidates.truncate(self.reclaim_candidate_limit.get());

                // For each candidate: try to claim its in-flight slot
                // (skip on contention or sticky-disabled so we work on a
                // different victim rather than serializing behind a
                // sibling's reclaim); re-check `try_grow` before reclaiming
                // in case a sibling already freed enough; reclaim; retry
                // `try_grow`. The retry path goes through `self.try_grow`,
                // which already updates the tracked consumer's atomic
                // reservation — no manual accounting needed here.
                for (_, reclaimer, flag) in candidates {
                    let _g = match ReclaimerStateGuard::try_acquire(&flag) {
                        Some(g) => g,
                        None => continue,
                    };
                    if self.try_grow(reservation, additional).is_ok() {
                        return Ok(());
                    }
                    if let Err(e) = reclaimer.reclaim(additional).await {
                        debug!("memory reclaimer returned error: {e}");
                        continue;
                    }
                    if self.try_grow(reservation, additional).is_ok() {
                        return Ok(());
                    }
                }

                // Walk produced nothing usable. If other consumers are
                // currently reclaiming for someone else, their freed bytes
                // may land in the pool shortly — wait briefly and retry
                // before falling through to OOM. Bounded so we don't stall
                // forever on a livelock.
                if in_flight_seen > 0 && retries < MAX_RECLAIM_RETRIES {
                    retries += 1;
                    tokio::time::sleep(RECLAIM_RETRY_SLEEP).await;
                    // Quick fast-path retry: an in-flight sibling may have
                    // freed bytes during the sleep.
                    if self.try_grow(reservation, additional).is_ok() {
                        return Ok(());
                    }
                    continue;
                }
                break;
            }

            // Fall through to the inner pool's own reclaim path, if any.
            // The default impl just re-runs `inner.try_grow`, which
            // bypasses `TrackConsumersPool::try_grow`, so the
            // consumer-side update is still required.
            self.inner
                .try_grow_async(reservation, additional)
                .await
                .map_err(|_| initial_err)?;
            if let Some(tracked) = self
                .tracked_consumers
                .read()
                .get(&reservation.consumer().id())
            {
                tracked.grow(additional);
            }
            Ok(())
        })
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

fn provide_top_memory_consumers_to_error_msg(
    consumer_name: &str,
    error_msg: &str,
    top_consumers: &str,
) -> String {
    format!(
        "Additional allocation failed for {consumer_name} with top memory consumers (across reservations) as:\n{top_consumers}\nError: {error_msg}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::{Settings, allow_duplicates, assert_snapshot, with_settings};
    use std::sync::Arc;

    fn make_settings() -> Settings {
        let mut settings = Settings::clone_current();
        settings.add_filter(
            r"([^\s]+)\#\d+\(can spill: (true|false)\)",
            "$1#[ID](can spill: $2)",
        );
        settings
    }

    #[test]
    fn test_fair() {
        let pool = Arc::new(FairSpillPool::new(100)) as _;

        let r1 = MemoryConsumer::new("unspillable").register(&pool);
        // Can grow beyond capacity of pool
        r1.grow(2000);
        assert_eq!(pool.reserved(), 2000);

        let r2 = MemoryConsumer::new("r2")
            .with_can_spill(true)
            .register(&pool);
        // Can grow beyond capacity of pool
        r2.grow(2000);

        assert_eq!(pool.reserved(), 4000);

        let err = r2.try_grow(1).unwrap_err().strip_backtrace();
        assert_snapshot!(err, @"Resources exhausted: Failed to allocate additional 1.0 B for r2 with 2000.0 B already allocated for this reservation - 0.0 B remain available for the total memory pool: fair(pool_size: 100.0 B)");

        let err = r2.try_grow(1).unwrap_err().strip_backtrace();
        assert_snapshot!(err, @"Resources exhausted: Failed to allocate additional 1.0 B for r2 with 2000.0 B already allocated for this reservation - 0.0 B remain available for the total memory pool: fair(pool_size: 100.0 B)");

        r1.shrink(1990);
        r2.shrink(2000);

        assert_eq!(pool.reserved(), 10);

        r1.try_grow(10).unwrap();
        assert_eq!(pool.reserved(), 20);

        // Can grow r2 to 80 as only spilling consumer
        r2.try_grow(80).unwrap();
        assert_eq!(pool.reserved(), 100);

        r2.shrink(70);

        assert_eq!(r1.size(), 20);
        assert_eq!(r2.size(), 10);
        assert_eq!(pool.reserved(), 30);

        let r3 = MemoryConsumer::new("r3")
            .with_can_spill(true)
            .register(&pool);

        let err = r3.try_grow(70).unwrap_err().strip_backtrace();
        assert_snapshot!(err, @"Resources exhausted: Failed to allocate additional 70.0 B for r3 with 0.0 B already allocated for this reservation - 40.0 B remain available for the total memory pool: fair(pool_size: 100.0 B)");

        //Shrinking r2 to zero doesn't allow a3 to allocate more than 45
        r2.free();
        let err = r3.try_grow(70).unwrap_err().strip_backtrace();
        assert_snapshot!(err, @"Resources exhausted: Failed to allocate additional 70.0 B for r3 with 0.0 B already allocated for this reservation - 40.0 B remain available for the total memory pool: fair(pool_size: 100.0 B)");

        // But dropping r2 does
        drop(r2);
        assert_eq!(pool.reserved(), 20);
        r3.try_grow(80).unwrap();

        assert_eq!(pool.reserved(), 100);
        r1.free();
        assert_eq!(pool.reserved(), 80);

        let r4 = MemoryConsumer::new("s4").register(&pool);
        let err = r4.try_grow(30).unwrap_err().strip_backtrace();
        assert_snapshot!(err, @"Resources exhausted: Failed to allocate additional 30.0 B for s4 with 0.0 B already allocated for this reservation - 20.0 B remain available for the total memory pool: fair(pool_size: 100.0 B)");
    }

    #[test]
    fn test_tracked_consumers_pool() {
        let setting = make_settings();
        let _bound = setting.bind_to_scope();
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(100),
            NonZeroUsize::new(3).unwrap(),
        ));

        // Test: use all the different interfaces to change reservation size

        // set r1=50, using grow and shrink
        let r1 = MemoryConsumer::new("r1").register(&pool);
        r1.grow(50);
        r1.grow(20);
        r1.shrink(20);

        // set r2=15 using try_grow
        let r2 = MemoryConsumer::new("r2").register(&pool);
        r2.try_grow(15)
            .expect("should succeed in memory allotment for r2");

        // set r3=20 using try_resize
        let r3 = MemoryConsumer::new("r3").register(&pool);
        r3.try_resize(25)
            .expect("should succeed in memory allotment for r3");
        r3.try_resize(20)
            .expect("should succeed in memory allotment for r3");

        // set r4=10
        // this should not be reported in top 3
        let r4 = MemoryConsumer::new("r4").register(&pool);
        r4.grow(10);

        // Test: reports if new reservation causes error
        // using the previously set sizes for other consumers
        let r5 = MemoryConsumer::new("r5").register(&pool);
        let res = r5.try_grow(150);
        assert!(res.is_err());
        let error = res.unwrap_err().strip_backtrace();
        assert_snapshot!(error, @r"
        Resources exhausted: Additional allocation failed for r5 with top memory consumers (across reservations) as:
          r1#[ID](can spill: false) consumed 50.0 B, peak 70.0 B,
          r3#[ID](can spill: false) consumed 20.0 B, peak 25.0 B,
          r2#[ID](can spill: false) consumed 15.0 B, peak 15.0 B.
        Error: Failed to allocate additional 150.0 B for r5 with 0.0 B already allocated for this reservation - 5.0 B remain available for the total memory pool: greedy(used: 95.0 B, pool_size: 100.0 B)
        ");
    }

    #[test]
    fn test_tracked_consumers_pool_register() {
        let setting = make_settings();
        let _bound = setting.bind_to_scope();
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(100),
            NonZeroUsize::new(3).unwrap(),
        ));

        let same_name = "foo";

        // Test: see error message when no consumers recorded yet
        let r0 = MemoryConsumer::new(same_name).register(&pool);
        let res = r0.try_grow(150);
        assert!(res.is_err());
        let error = res.unwrap_err().strip_backtrace();
        assert_snapshot!(error, @r"
        Resources exhausted: Additional allocation failed for foo with top memory consumers (across reservations) as:
          foo#[ID](can spill: false) consumed 0.0 B, peak 0.0 B.
        Error: Failed to allocate additional 150.0 B for foo with 0.0 B already allocated for this reservation - 100.0 B remain available for the total memory pool: greedy(used: 0.0 B, pool_size: 100.0 B)
        ");

        // API: multiple registrations using the same hashed consumer,
        // will be recognized *differently* in the TrackConsumersPool.

        r0.grow(10); // make r0=10, pool available=90
        let new_consumer_same_name = MemoryConsumer::new(same_name);
        let r1 = new_consumer_same_name.register(&pool);
        // TODO: the insufficient_capacity_err() message is per reservation, not per consumer.
        // a followup PR will clarify this message "0 bytes already allocated for this reservation"
        let res = r1.try_grow(150);
        assert!(res.is_err());
        let error = res.unwrap_err().strip_backtrace();
        assert_snapshot!(error, @r"
        Resources exhausted: Additional allocation failed for foo with top memory consumers (across reservations) as:
          foo#[ID](can spill: false) consumed 10.0 B, peak 10.0 B,
          foo#[ID](can spill: false) consumed 0.0 B, peak 0.0 B.
        Error: Failed to allocate additional 150.0 B for foo with 0.0 B already allocated for this reservation - 90.0 B remain available for the total memory pool: greedy(used: 10.0 B, pool_size: 100.0 B)
        ");

        // Test: will accumulate size changes per consumer, not per reservation
        r1.grow(20);

        let res = r1.try_grow(150);
        assert!(res.is_err());
        let error = res.unwrap_err().strip_backtrace();
        assert_snapshot!(error, @r"
        Resources exhausted: Additional allocation failed for foo with top memory consumers (across reservations) as:
          foo#[ID](can spill: false) consumed 20.0 B, peak 20.0 B,
          foo#[ID](can spill: false) consumed 10.0 B, peak 10.0 B.
        Error: Failed to allocate additional 150.0 B for foo with 20.0 B already allocated for this reservation - 70.0 B remain available for the total memory pool: greedy(used: 30.0 B, pool_size: 100.0 B)
        ");

        // Test: different hashed consumer, (even with the same name),
        // will be recognized as different in the TrackConsumersPool
        let consumer_with_same_name_but_different_hash =
            MemoryConsumer::new(same_name).with_can_spill(true);
        let r2 = consumer_with_same_name_but_different_hash.register(&pool);
        let res = r2.try_grow(150);
        assert!(res.is_err());
        let error = res.unwrap_err().strip_backtrace();
        assert_snapshot!(error, @r"
        Resources exhausted: Additional allocation failed for foo with top memory consumers (across reservations) as:
          foo#[ID](can spill: false) consumed 20.0 B, peak 20.0 B,
          foo#[ID](can spill: false) consumed 10.0 B, peak 10.0 B,
          foo#[ID](can spill: true) consumed 0.0 B, peak 0.0 B.
        Error: Failed to allocate additional 150.0 B for foo with 0.0 B already allocated for this reservation - 70.0 B remain available for the total memory pool: greedy(used: 30.0 B, pool_size: 100.0 B)
        ");
    }

    #[test]
    fn test_tracked_consumers_pool_deregister() {
        fn test_per_pool_type<P: MemoryPool + 'static>(pool: Arc<TrackConsumersPool<P>>) {
            // `snapshot_suffix` ties each insta snapshot to this pool's inner backend; filters
            // normalize inner pool `Display` so fair vs greedy share the same `@` reference text.
            with_settings!({
                snapshot_suffix => pool.inner().name().to_string(),
                filters => vec![
                    (
                        r"([^\s]+)\#\d+\(can spill: (true|false)\)",
                        "$1#[ID](can spill: $2)",
                    ),
                    (
                        r"for the total memory pool: [^\n]+",
                        "for the total memory pool: [INNER_POOL]",
                    ),
                ],
            }, {
                let memory_pool: Arc<dyn MemoryPool> = Arc::<TrackConsumersPool<P>>::clone(&pool);
                let r0 = MemoryConsumer::new("r0").register(&memory_pool);
                r0.grow(10);
                let r1 = MemoryConsumer::new("r1").register(&memory_pool);
                r1.grow(20);

                // Baseline: see the 2 memory consumers
                let error = r0.try_grow(150).unwrap_err().strip_backtrace();
                assert_snapshot!(error, @r"
                Resources exhausted: Additional allocation failed for r0 with top memory consumers (across reservations) as:
                  r1#[ID](can spill: false) consumed 20.0 B, peak 20.0 B,
                  r0#[ID](can spill: false) consumed 10.0 B, peak 10.0 B.
                Error: Failed to allocate additional 150.0 B for r0 with 10.0 B already allocated for this reservation - 70.0 B remain available for the total memory pool: [INNER_POOL]
                ");

                // Test: unregister one — only the remaining consumer should be listed
                drop(r1);
                let error = r0.try_grow(150).unwrap_err().strip_backtrace();
                assert_snapshot!(error, @r"
                Resources exhausted: Additional allocation failed for r0 with top memory consumers (across reservations) as:
                  r0#[ID](can spill: false) consumed 10.0 B, peak 10.0 B.
                Error: Failed to allocate additional 150.0 B for r0 with 10.0 B already allocated for this reservation - 90.0 B remain available for the total memory pool: [INNER_POOL]
                ");

                // Test: actual message we see is the `available is 70`. When it should be `available is 90`.
                // This is because the pool.shrink() does not automatically occur within the inner_pool.deregister().
                let error = r0.try_grow(150).unwrap_err().strip_backtrace();
                assert_snapshot!(error, @r"
                Resources exhausted: Additional allocation failed for r0 with top memory consumers (across reservations) as:
                  r0#[ID](can spill: false) consumed 10.0 B, peak 10.0 B.
                Error: Failed to allocate additional 150.0 B for r0 with 10.0 B already allocated for this reservation - 90.0 B remain available for the total memory pool: [INNER_POOL]
                ");

                // Test: the registration needs to free itself (or be dropped),
                // for the proper error message
                let error = r0.try_grow(150).unwrap_err().strip_backtrace();
                assert_snapshot!(error, @r"
                Resources exhausted: Additional allocation failed for r0 with top memory consumers (across reservations) as:
                  r0#[ID](can spill: false) consumed 10.0 B, peak 10.0 B.
                Error: Failed to allocate additional 150.0 B for r0 with 10.0 B already allocated for this reservation - 90.0 B remain available for the total memory pool: [INNER_POOL]
                ");
                }
            );
        }

        allow_duplicates! {
            let tracked_spill_pool = Arc::new(TrackConsumersPool::new(
                FairSpillPool::new(100),
                NonZeroUsize::new(3).unwrap(),
            ));
            test_per_pool_type(tracked_spill_pool);

            let tracked_greedy_pool = Arc::new(TrackConsumersPool::new(
                GreedyMemoryPool::new(100),
                NonZeroUsize::new(3).unwrap(),
            ));
            test_per_pool_type(tracked_greedy_pool);
        }
    }

    #[test]
    fn test_track_consumers_pool_metrics() {
        let track_consumers_pool = Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(1000),
            NonZeroUsize::new(3).unwrap(),
        ));
        let memory_pool: Arc<dyn MemoryPool> = Arc::clone(&track_consumers_pool) as _;

        // Empty pool has no metrics
        assert!(track_consumers_pool.metrics().is_empty());

        // Register consumers with different spill settings
        let r1 = MemoryConsumer::new("spilling")
            .with_can_spill(true)
            .register(&memory_pool);
        let r2 = MemoryConsumer::new("non-spilling").register(&memory_pool);

        // Grow r1 in two steps to verify peak tracking
        r1.grow(100);
        r1.grow(50);
        r1.shrink(50); // reserved=100, peak=150

        r2.grow(200); // reserved=200, peak=200

        let mut metrics = track_consumers_pool.metrics();
        metrics.sort_by_key(|m| m.name.clone());

        assert_eq!(metrics.len(), 2);

        let m_non = &metrics[0];
        assert_eq!(m_non.name, "non-spilling");
        assert!(!m_non.can_spill);
        assert_eq!(m_non.reserved, 200);
        assert_eq!(m_non.peak, 200);

        let m_spill = &metrics[1];
        assert_eq!(m_spill.name, "spilling");
        assert!(m_spill.can_spill);
        assert_eq!(m_spill.reserved, 100);
        assert_eq!(m_spill.peak, 150);

        // Unregistered consumers are removed from metrics
        drop(r2);
        let metrics = track_consumers_pool.metrics();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name, "spilling");
    }

    #[test]
    fn test_tracked_consumers_pool_use_beyond_errors() {
        let setting = make_settings();
        let _bound = setting.bind_to_scope();
        let upcasted: Arc<dyn std::any::Any + Send + Sync> =
            Arc::new(TrackConsumersPool::new(
                GreedyMemoryPool::new(100),
                NonZeroUsize::new(3).unwrap(),
            ));
        let pool: Arc<dyn MemoryPool> = Arc::clone(&upcasted)
            .downcast::<TrackConsumersPool<GreedyMemoryPool>>()
            .unwrap();
        // set r1=20
        let r1 = MemoryConsumer::new("r1").register(&pool);
        r1.grow(20);
        // set r2=15
        let r2 = MemoryConsumer::new("r2").register(&pool);
        r2.grow(15);
        // set r3=45
        let r3 = MemoryConsumer::new("r3").register(&pool);
        r3.grow(45);

        let downcasted = upcasted
            .downcast::<TrackConsumersPool<GreedyMemoryPool>>()
            .unwrap();

        // Test: can get runtime metrics, even without an error thrown
        let res = downcasted.report_top(2);
        assert_snapshot!(res, @r"
        r3#[ID](can spill: false) consumed 45.0 B, peak 45.0 B,
        r1#[ID](can spill: false) consumed 20.0 B, peak 20.0 B.
        ");
    }

    #[test]
    fn test_memory_pool_display_fmt() {
        let top = NonZeroUsize::new(5).unwrap();

        // UnboundedMemoryPool Display with default allocation: 0.0B
        let unbounded = UnboundedMemoryPool::default();
        assert_eq!(
            unbounded.to_string(),
            "unbounded(used: 0.0 B)",
            "UnboundedMemoryPool Display"
        );

        // UnboundedMemoryPool Display with reservations
        let unbounded_arc: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let r = MemoryConsumer::new("u").register(&unbounded_arc);
        r.grow(2048);
        assert_eq!(
            unbounded_arc.as_ref().to_string(),
            "unbounded(used: 2.0 KB)",
            "UnboundedMemoryPool Display with reservations"
        );

        // GreedyMemoryPool Display with default allocation: 100.0B
        let greedy = GreedyMemoryPool::new(100);
        assert_eq!(
            greedy.to_string(),
            "greedy(used: 0.0 B, pool_size: 100.0 B)",
            "GreedyMemoryPool Display"
        );

        // GreedyMemoryPool Display with reservations
        let greedy_arc: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(100));
        let r = MemoryConsumer::new("g").register(&greedy_arc);
        r.grow(50);
        assert_eq!(
            greedy_arc.as_ref().to_string(),
            "greedy(used: 50.0 B, pool_size: 100.0 B)",
            "GreedyMemoryPool Display with reservations"
        );

        // FairSpillPool Display with default allocation: 4.0KB and without reservations
        let fair = FairSpillPool::new(4096);
        assert_eq!(
            fair.to_string(),
            "fair(pool_size: 4.0 KB)",
            "FairSpillPool Display"
        );

        // TrackConsumersPool<GreedyMemoryPool> Display with default allocation: 128.0B and without reservations
        let tracked_greedy = TrackConsumersPool::new(GreedyMemoryPool::new(128), top);
        assert_eq!(
            tracked_greedy.to_string(),
            "track_consumers(inner_pool: greedy(used: 0.0 B, pool_size: 128.0 B), num_of_top_consumers: 5)",
            "TrackConsumersPool<GreedyMemoryPool> Display"
        );

        // TrackConsumersPool<FairSpillPool> Display with default allocation: 256.0B and without reservations
        let tracked_fair = TrackConsumersPool::new(FairSpillPool::new(256), top);
        assert_eq!(
            tracked_fair.to_string(),
            "track_consumers(inner_pool: fair(pool_size: 256.0 B), num_of_top_consumers: 5)",
            "TrackConsumersPool<FairSpillPool> Display"
        );

        // TrackConsumersPool<UnboundedMemoryPool> Display without reservations
        let tracked_unbounded =
            TrackConsumersPool::new(UnboundedMemoryPool::default(), top);
        assert_eq!(
            tracked_unbounded.to_string(),
            "track_consumers(inner_pool: unbounded(used: 0.0 B), num_of_top_consumers: 5)",
            "TrackConsumersPool<UnboundedMemoryPool> Display"
        );
    }

    /// N threads each call `grow(STEP)` then `shrink(STEP)` once on the same
    /// consumer. Final `reserved == 0`. Peak hit at least once and at most
    /// `THREADS * STEP` — validates that `fetch_add` on `reserved` is correct
    /// under concurrent readers of the `RwLock`-protected map.
    #[test]
    fn test_tracked_consumer_concurrent_grow() {
        const THREADS: usize = 16;
        const STEP: usize = 7;

        let tracked = Arc::new(TrackConsumersPool::new(
            UnboundedMemoryPool::default(),
            NonZeroUsize::new(5).unwrap(),
        ));
        let tracked_clone = Arc::clone(&tracked);
        let pool: Arc<dyn MemoryPool> = tracked_clone;
        let r = Arc::new(MemoryConsumer::new("c").register(&pool));

        std::thread::scope(|s| {
            for _ in 0..THREADS {
                let r = Arc::clone(&r);
                s.spawn(move || {
                    let local = r.new_empty();
                    local.grow(STEP);
                    local.shrink(STEP);
                });
            }
        });

        let metrics = tracked.metrics();
        let entry = metrics.iter().find(|m| m.name == "c").unwrap();
        assert_eq!(entry.reserved, 0);
        assert!(entry.peak >= STEP);
        assert!(entry.peak <= THREADS * STEP);
    }

    /// N threads run interleaved `grow`/`shrink` pairs on the same consumer.
    /// Final `reserved` must be 0; `peak` must be at least `STEP` (any grow
    /// records its own bump) and at most `THREADS * STEP`. Validates the
    /// monotone-max CAS on `peak`, fixing today's `fetch_max(self.reserved())`
    /// race where an intervening shrink could drop `reserved` below the value
    /// used to bump `peak`.
    #[test]
    fn test_tracked_consumer_concurrent_peak_monotone() {
        const THREADS: usize = 16;
        const ITERS: usize = 10_000;
        const STEP: usize = 3;

        let tracked = Arc::new(TrackConsumersPool::new(
            UnboundedMemoryPool::default(),
            NonZeroUsize::new(5).unwrap(),
        ));
        let tracked_clone = Arc::clone(&tracked);
        let pool: Arc<dyn MemoryPool> = tracked_clone;
        let r = Arc::new(MemoryConsumer::new("c").register(&pool));

        std::thread::scope(|s| {
            for _ in 0..THREADS {
                let r = Arc::clone(&r);
                s.spawn(move || {
                    let local = r.new_empty();
                    for _ in 0..ITERS {
                        local.grow(STEP);
                        local.shrink(STEP);
                    }
                });
            }
        });

        let entry = tracked
            .metrics()
            .into_iter()
            .find(|m| m.name == "c")
            .unwrap();
        assert_eq!(entry.reserved, 0, "all grows undone by shrinks");
        assert!(entry.peak >= STEP);
        assert!(entry.peak <= THREADS * STEP);
    }

    /// One thread loops register/unregister, another loops grow/shrink on a
    /// stable consumer. Verifies no panics or deadlocks across the `RwLock`
    /// boundary, and that the stable consumer's accounting is preserved
    /// when a writer briefly takes the exclusive lock.
    #[test]
    fn test_tracked_consumers_pool_register_grow_concurrent() {
        const ITERS: usize = 1_000;

        let tracked = Arc::new(TrackConsumersPool::new(
            UnboundedMemoryPool::default(),
            NonZeroUsize::new(5).unwrap(),
        ));
        let tracked_clone = Arc::clone(&tracked);
        let pool: Arc<dyn MemoryPool> = tracked_clone;

        let r = Arc::new(MemoryConsumer::new("stable").register(&pool));

        std::thread::scope(|s| {
            let pool_w = Arc::clone(&pool);
            s.spawn(move || {
                for i in 0..ITERS {
                    let _churn =
                        MemoryConsumer::new(format!("churn-{i}")).register(&pool_w);
                }
            });

            let r_inner = Arc::clone(&r);
            s.spawn(move || {
                let local = r_inner.new_empty();
                for _ in 0..ITERS {
                    local.grow(5);
                    local.shrink(5);
                }
            });
        });

        let metrics = tracked.metrics();
        let stable = metrics.iter().find(|m| m.name == "stable").unwrap();
        assert_eq!(stable.reserved, 0);
        assert!(stable.peak >= 5);
        assert!(metrics.iter().all(|m| !m.name.starts_with("churn-")));
        drop(r);
    }
}
