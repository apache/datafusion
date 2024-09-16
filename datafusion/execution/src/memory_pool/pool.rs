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

use crate::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_common::{resources_datafusion_err, DataFusionError, Result};
use hashbrown::HashMap;
use log::debug;
use parking_lot::Mutex;
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

/// A [`MemoryPool`] that enforces no limit
#[derive(Debug, Default)]
pub struct UnboundedMemoryPool {
    used: AtomicUsize,
}

impl MemoryPool for UnboundedMemoryPool {
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
    /// Allocate up to `limit` bytes
    pub fn new(pool_size: usize) -> Self {
        debug!("Created new GreedyMemoryPool(pool_size={pool_size})");
        Self {
            pool_size,
            used: AtomicUsize::new(0),
        }
    }
}

impl MemoryPool for GreedyMemoryPool {
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
                )
            })?;
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Ordering::Relaxed)
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

                if reservation.size + additional > available {
                    return Err(insufficient_capacity_err(
                        reservation,
                        additional,
                        available,
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
) -> DataFusionError {
    resources_datafusion_err!("Failed to allocate additional {} bytes for {} with {} bytes already allocated for this reservation - {} bytes remain available for the total pool", additional, reservation.registration.consumer.name, reservation.size, available)
}

/// A [`MemoryPool`] that tracks the consumers that have
/// reserved memory within the inner memory pool.
///
/// By tracking memory reservations more carefully this pool
/// can provide better error messages on the largest memory users
///
/// Tracking is per hashed [`MemoryConsumer`], not per [`MemoryReservation`].
/// The same consumer can have multiple reservations.
#[derive(Debug)]
pub struct TrackConsumersPool<I> {
    inner: I,
    top: NonZeroUsize,
    tracked_consumers: Mutex<HashMap<MemoryConsumer, AtomicU64>>,
}

impl<I: MemoryPool> TrackConsumersPool<I> {
    /// Creates a new [`TrackConsumersPool`].
    ///
    /// The `top` determines how many Top K [`MemoryConsumer`]s to include
    /// in the reported [`DataFusionError::ResourcesExhausted`].
    pub fn new(inner: I, top: NonZeroUsize) -> Self {
        Self {
            inner,
            top,
            tracked_consumers: Default::default(),
        }
    }

    /// Determine if there are multiple [`MemoryConsumer`]s registered
    /// which have the same name.
    ///
    /// This is very tied to the implementation of the memory consumer.
    fn has_multiple_consumers(&self, name: &String) -> bool {
        let consumer = MemoryConsumer::new(name);
        let consumer_with_spill = consumer.clone().with_can_spill(true);
        let guard = self.tracked_consumers.lock();
        guard.contains_key(&consumer) && guard.contains_key(&consumer_with_spill)
    }

    /// The top consumers in a report string.
    pub fn report_top(&self, top: usize) -> String {
        let mut consumers = self
            .tracked_consumers
            .lock()
            .iter()
            .map(|(consumer, reserved)| {
                (
                    (consumer.name().to_owned(), consumer.can_spill()),
                    reserved.load(Ordering::Acquire),
                )
            })
            .collect::<Vec<_>>();
        consumers.sort_by(|a, b| b.1.cmp(&a.1)); // inverse ordering

        consumers[0..std::cmp::min(top, consumers.len())]
            .iter()
            .map(|((name, can_spill), size)| {
                if self.has_multiple_consumers(name) {
                    format!("{name}(can_spill={}) consumed {:?} bytes", can_spill, size)
                } else {
                    format!("{name} consumed {:?} bytes", size)
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl<I: MemoryPool> MemoryPool for TrackConsumersPool<I> {
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);

        let mut guard = self.tracked_consumers.lock();
        if let Some(already_reserved) = guard.insert(consumer.clone(), Default::default())
        {
            guard.entry_ref(consumer).and_modify(|bytes| {
                bytes.fetch_add(
                    already_reserved.load(Ordering::Acquire),
                    Ordering::AcqRel,
                );
            });
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
        self.tracked_consumers.lock().remove(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.tracked_consumers
            .lock()
            .entry_ref(reservation.consumer())
            .and_modify(|bytes| {
                bytes.fetch_add(additional as u64, Ordering::AcqRel);
            });
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.tracked_consumers
            .lock()
            .entry_ref(reservation.consumer())
            .and_modify(|bytes| {
                bytes.fetch_sub(shrink as u64, Ordering::AcqRel);
            });
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner
            .try_grow(reservation, additional)
            .map_err(|e| match e {
                DataFusionError::ResourcesExhausted(e) => {
                    // wrap OOM message in top consumers
                    DataFusionError::ResourcesExhausted(
                        provide_top_memory_consumers_to_error_msg(
                            e,
                            self.report_top(self.top.into()),
                        ),
                    )
                }
                _ => e,
            })?;

        self.tracked_consumers
            .lock()
            .entry_ref(reservation.consumer())
            .and_modify(|bytes| {
                bytes.fetch_add(additional as u64, Ordering::AcqRel);
            });
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

fn provide_top_memory_consumers_to_error_msg(
    error_msg: String,
    top_consumers: String,
) -> String {
    format!("Additional allocation failed with top memory consumers (across reservations) as: {}. Error: {}", top_consumers, error_msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_fair() {
        let pool = Arc::new(FairSpillPool::new(100)) as _;

        let mut r1 = MemoryConsumer::new("unspillable").register(&pool);
        // Can grow beyond capacity of pool
        r1.grow(2000);
        assert_eq!(pool.reserved(), 2000);

        let mut r2 = MemoryConsumer::new("r2")
            .with_can_spill(true)
            .register(&pool);
        // Can grow beyond capacity of pool
        r2.grow(2000);

        assert_eq!(pool.reserved(), 4000);

        let err = r2.try_grow(1).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 1 bytes for r2 with 2000 bytes already allocated for this reservation - 0 bytes remain available for the total pool");

        let err = r2.try_grow(1).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 1 bytes for r2 with 2000 bytes already allocated for this reservation - 0 bytes remain available for the total pool");

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

        let mut r3 = MemoryConsumer::new("r3")
            .with_can_spill(true)
            .register(&pool);

        let err = r3.try_grow(70).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 70 bytes for r3 with 0 bytes already allocated for this reservation - 40 bytes remain available for the total pool");

        //Shrinking r2 to zero doesn't allow a3 to allocate more than 45
        r2.free();
        let err = r3.try_grow(70).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 70 bytes for r3 with 0 bytes already allocated for this reservation - 40 bytes remain available for the total pool");

        // But dropping r2 does
        drop(r2);
        assert_eq!(pool.reserved(), 20);
        r3.try_grow(80).unwrap();

        assert_eq!(pool.reserved(), 100);
        r1.free();
        assert_eq!(pool.reserved(), 80);

        let mut r4 = MemoryConsumer::new("s4").register(&pool);
        let err = r4.try_grow(30).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 30 bytes for s4 with 0 bytes already allocated for this reservation - 20 bytes remain available for the total pool");
    }

    #[test]
    fn test_tracked_consumers_pool() {
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(100),
            NonZeroUsize::new(3).unwrap(),
        ));

        // Test: use all the different interfaces to change reservation size

        // set r1=50, using grow and shrink
        let mut r1 = MemoryConsumer::new("r1").register(&pool);
        r1.grow(70);
        r1.shrink(20);

        // set r2=15 using try_grow
        let mut r2 = MemoryConsumer::new("r2").register(&pool);
        r2.try_grow(15)
            .expect("should succeed in memory allotment for r2");

        // set r3=20 using try_resize
        let mut r3 = MemoryConsumer::new("r3").register(&pool);
        r3.try_resize(25)
            .expect("should succeed in memory allotment for r3");
        r3.try_resize(20)
            .expect("should succeed in memory allotment for r3");

        // set r4=10
        // this should not be reported in top 3
        let mut r4 = MemoryConsumer::new("r4").register(&pool);
        r4.grow(10);

        // Test: reports if new reservation causes error
        // using the previously set sizes for other consumers
        let mut r5 = MemoryConsumer::new("r5").register(&pool);
        let expected = "Additional allocation failed with top memory consumers (across reservations) as: r1 consumed 50 bytes, r3 consumed 20 bytes, r2 consumed 15 bytes. Error: Failed to allocate additional 150 bytes for r5 with 0 bytes already allocated for this reservation - 5 bytes remain available for the total pool";
        let res = r5.try_grow(150);
        assert!(
            matches!(
                &res,
                Err(DataFusionError::ResourcesExhausted(ref e)) if e.to_string().contains(expected)
            ),
            "should provide list of top memory consumers, instead found {:?}",
            res
        );
    }

    #[test]
    fn test_tracked_consumers_pool_register() {
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(100),
            NonZeroUsize::new(3).unwrap(),
        ));

        let same_name = "foo";

        // Test: see error message when no consumers recorded yet
        let mut r0 = MemoryConsumer::new(same_name).register(&pool);
        let expected = "Additional allocation failed with top memory consumers (across reservations) as: foo consumed 0 bytes. Error: Failed to allocate additional 150 bytes for foo with 0 bytes already allocated for this reservation - 100 bytes remain available for the total pool";
        let res = r0.try_grow(150);
        assert!(
            matches!(
                &res,
                Err(DataFusionError::ResourcesExhausted(ref e)) if e.to_string().contains(expected)
            ),
            "should provide proper error when no reservations have been made yet, instead found {:?}", res
        );

        // API: multiple registrations using the same hashed consumer,
        // will be recognized as the same in the TrackConsumersPool.

        // Test: will be the same per Top Consumers reported.
        r0.grow(10); // make r0=10, pool available=90
        let new_consumer_same_name = MemoryConsumer::new(same_name);
        let mut r1 = new_consumer_same_name.register(&pool);
        // TODO: the insufficient_capacity_err() message is per reservation, not per consumer.
        // a followup PR will clarify this message "0 bytes already allocated for this reservation"
        let expected = "Additional allocation failed with top memory consumers (across reservations) as: foo consumed 10 bytes. Error: Failed to allocate additional 150 bytes for foo with 0 bytes already allocated for this reservation - 90 bytes remain available for the total pool";
        let res = r1.try_grow(150);
        assert!(
            matches!(
                &res,
                Err(DataFusionError::ResourcesExhausted(ref e)) if e.to_string().contains(expected)
            ),
            "should provide proper error with same hashed consumer (a single foo=10 bytes, available=90), instead found {:?}", res
        );

        // Test: will accumulate size changes per consumer, not per reservation
        r1.grow(20);
        let expected = "Additional allocation failed with top memory consumers (across reservations) as: foo consumed 30 bytes. Error: Failed to allocate additional 150 bytes for foo with 20 bytes already allocated for this reservation - 70 bytes remain available for the total pool";
        let res = r1.try_grow(150);
        assert!(
            matches!(
                &res,
                Err(DataFusionError::ResourcesExhausted(ref e)) if e.to_string().contains(expected)
            ),
            "should provide proper error with same hashed consumer (a single foo=30 bytes, available=70), instead found {:?}", res
        );

        // Test: different hashed consumer, (even with the same name),
        // will be recognized as different in the TrackConsumersPool
        let consumer_with_same_name_but_different_hash =
            MemoryConsumer::new(same_name).with_can_spill(true);
        let mut r2 = consumer_with_same_name_but_different_hash.register(&pool);
        let expected = "Additional allocation failed with top memory consumers (across reservations) as: foo(can_spill=false) consumed 30 bytes, foo(can_spill=true) consumed 0 bytes. Error: Failed to allocate additional 150 bytes for foo with 0 bytes already allocated for this reservation - 70 bytes remain available for the total pool";
        let res = r2.try_grow(150);
        assert!(
            matches!(
                &res,
                Err(DataFusionError::ResourcesExhausted(ref e)) if e.to_string().contains(expected)
            ),
            "should provide proper error with different hashed consumer (foo(can_spill=false)=30 bytes and foo(can_spill=true)=0 bytes, available=70), instead found {:?}", res
        );
    }

    #[test]
    fn test_tracked_consumers_pool_deregister() {
        fn test_per_pool_type(pool: Arc<dyn MemoryPool>) {
            // Baseline: see the 2 memory consumers
            let mut r0 = MemoryConsumer::new("r0").register(&pool);
            r0.grow(10);
            let r1_consumer = MemoryConsumer::new("r1");
            let mut r1 = r1_consumer.clone().register(&pool);
            r1.grow(20);
            let expected = "Additional allocation failed with top memory consumers (across reservations) as: r1 consumed 20 bytes, r0 consumed 10 bytes. Error: Failed to allocate additional 150 bytes for r0 with 10 bytes already allocated for this reservation - 70 bytes remain available for the total pool";
            let res = r0.try_grow(150);
            assert!(
                matches!(
                    &res,
                    Err(DataFusionError::ResourcesExhausted(ref e)) if e.to_string().contains(expected)
                ),
                "should provide proper error with both consumers, instead found {:?}",
                res
            );

            // Test: unregister one
            // only the remaining one should be listed
            pool.unregister(&r1_consumer);
            let expected_consumers = "Additional allocation failed with top memory consumers (across reservations) as: r0 consumed 10 bytes";
            let res = r0.try_grow(150);
            assert!(
                matches!(
                    &res,
                    Err(DataFusionError::ResourcesExhausted(ref e)) if e.to_string().contains(expected_consumers)
                ),
                "should provide proper error with only 1 consumer left registered, instead found {:?}", res
            );

            // Test: actual message we see is the `available is 70`. When it should be `available is 90`.
            // This is because the pool.shrink() does not automatically occur within the inner_pool.deregister().
            let expected_70_available = "Failed to allocate additional 150 bytes for r0 with 10 bytes already allocated for this reservation - 70 bytes remain available for the total pool";
            let res = r0.try_grow(150);
            assert!(
                matches!(
                    &res,
                    Err(DataFusionError::ResourcesExhausted(ref e)) if e.to_string().contains(expected_70_available)
                ),
                "should find that the inner pool will still count all bytes for the deregistered consumer until the reservation is dropped, instead found {:?}", res
            );

            // Test: the registration needs to free itself (or be dropped),
            // for the proper error message
            r1.free();
            let expected_90_available = "Failed to allocate additional 150 bytes for r0 with 10 bytes already allocated for this reservation - 90 bytes remain available for the total pool";
            let res = r0.try_grow(150);
            assert!(
                matches!(
                    &res,
                    Err(DataFusionError::ResourcesExhausted(ref e)) if e.to_string().contains(expected_90_available)
                ),
                "should correctly account the total bytes after reservation is free, instead found {:?}", res
            );
        }

        let tracked_spill_pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(
            FairSpillPool::new(100),
            NonZeroUsize::new(3).unwrap(),
        ));
        test_per_pool_type(tracked_spill_pool);

        let tracked_greedy_pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(100),
            NonZeroUsize::new(3).unwrap(),
        ));
        test_per_pool_type(tracked_greedy_pool);
    }

    #[test]
    fn test_tracked_consumers_pool_use_beyond_errors() {
        let upcasted: Arc<dyn std::any::Any + Send + Sync> =
            Arc::new(TrackConsumersPool::new(
                GreedyMemoryPool::new(100),
                NonZeroUsize::new(3).unwrap(),
            ));
        let pool: Arc<dyn MemoryPool> = Arc::clone(&upcasted)
            .downcast::<TrackConsumersPool<GreedyMemoryPool>>()
            .unwrap();
        // set r1=20
        let mut r1 = MemoryConsumer::new("r1").register(&pool);
        r1.grow(20);
        // set r2=15
        let mut r2 = MemoryConsumer::new("r2").register(&pool);
        r2.grow(15);
        // set r3=45
        let mut r3 = MemoryConsumer::new("r3").register(&pool);
        r3.grow(45);

        let downcasted = upcasted
            .downcast::<TrackConsumersPool<GreedyMemoryPool>>()
            .unwrap();

        // Test: can get runtime metrics, even without an error thrown
        let expected = "r3 consumed 45 bytes, r1 consumed 20 bytes";
        let res = downcasted.report_top(2);
        assert_eq!(
            res, expected,
            "should provide list of top memory consumers, instead found {:?}",
            res
        );
    }
}
