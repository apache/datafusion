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
/// The error references the total bytes affiliated with the reservation's
/// [`MemoryConsumer`], and not the total within the collective [`MemoryPool`].
#[inline(always)]
fn insufficient_capacity_err(
    reservation: &MemoryReservation,
    additional: usize,
    available: usize,
) -> DataFusionError {
    resources_datafusion_err!("Failed to allocate additional {} bytes for {} with {} bytes already allocated - maximum available is {}", additional, reservation.registration.consumer.name, reservation.size, available)
}

/// A [`MemoryPool`] that tracks the consumers that have
/// reserved memory within the inner memory pool.
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

    /// The top consumers in a report string.
    fn report_top(&self) -> String {
        let mut consumers = self
            .tracked_consumers
            .lock()
            .iter()
            .map(|(consumer, reserved)| {
                (consumer.name().to_owned(), reserved.load(Ordering::Acquire))
            })
            .collect::<Vec<_>>();
        consumers.sort_by(|a, b| b.1.cmp(&a.1)); // inverse ordering

        format!(
            "The top memory consumers (across reservations) are: {}",
            consumers[0..std::cmp::min(self.top.into(), consumers.len())]
                .iter()
                .map(|(name, size)| format!("{name} consumed {:?} bytes", size))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl<I: MemoryPool> MemoryPool for TrackConsumersPool<I> {
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
        self.tracked_consumers
            .lock()
            .insert_unique_unchecked(consumer.clone(), Default::default());
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
        self.inner.try_grow(reservation, additional).map_err(|e| {
            match e.find_root() {
                DataFusionError::ResourcesExhausted(e) => {
                    DataFusionError::ResourcesExhausted(
                        e.to_owned() + ". " + &self.report_top(),
                    )
                }
                _ => e,
            }
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
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 1 bytes for r2 with 2000 bytes already allocated - maximum available is 0");

        let err = r2.try_grow(1).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 1 bytes for r2 with 2000 bytes already allocated - maximum available is 0");

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
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 70 bytes for r3 with 0 bytes already allocated - maximum available is 40");

        //Shrinking r2 to zero doesn't allow a3 to allocate more than 45
        r2.free();
        let err = r3.try_grow(70).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 70 bytes for r3 with 0 bytes already allocated - maximum available is 40");

        // But dropping r2 does
        drop(r2);
        assert_eq!(pool.reserved(), 20);
        r3.try_grow(80).unwrap();

        assert_eq!(pool.reserved(), 100);
        r1.free();
        assert_eq!(pool.reserved(), 80);

        let mut r4 = MemoryConsumer::new("s4").register(&pool);
        let err = r4.try_grow(30).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 30 bytes for s4 with 0 bytes already allocated - maximum available is 20");
    }

    #[test]
    fn test_tracked_consumers_pool() {
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(100),
            NonZeroUsize::new(3).unwrap(),
        ));

        // Test: see error message when no consumers recorded yet
        let mut r0 = MemoryConsumer::new("r0").register(&pool);
        let expected = "Failed to allocate additional 150 bytes for r0 with 0 bytes already allocated - maximum available is 100. The top memory consumers (across reservations) are: r0 consumed 0 bytes";
        assert!(
            matches!(
                r0.try_grow(150),
                Err(DataFusionError::ResourcesExhausted(e)) if e.to_string().contains(expected)
            ),
            "should error when no other consumers are reported"
        );

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
        // using the previous set size for other consumers
        let mut r5 = MemoryConsumer::new("r5").register(&pool);
        let expected = "Failed to allocate additional 150 bytes for r5 with 0 bytes already allocated - maximum available is 5. The top memory consumers (across reservations) are: r1 consumed 50 bytes, r3 consumed 20 bytes, r2 consumed 15 bytes";
        assert!(matches!(
            r5.try_grow(150),
            Err(DataFusionError::ResourcesExhausted(e)) if e.to_string().contains(expected)
        ));
    }
}
