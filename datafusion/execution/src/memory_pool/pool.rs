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
use datafusion_common::{DataFusionError, Result};
use hashbrown::HashMap;
use log::debug;
use parking_lot::Mutex;
use std::{
    fmt::Display,
    sync::atomic::{AtomicUsize, Ordering},
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
    state: Mutex<GreedyMemoryPoolState>,
}

#[derive(Debug)]
struct GreedyMemoryPoolState {
    pool_members: HashMap<String, usize>,
}

impl GreedyMemoryPool {
    /// Allocate up to `limit` bytes
    pub fn new(pool_size: usize) -> Self {
        debug!("Created new GreedyMemoryPool(pool_size={pool_size})");
        Self {
            pool_size,
            state: Mutex::new(GreedyMemoryPoolState {
                pool_members: HashMap::new(),
            }),
        }
    }
}

impl MemoryPool for GreedyMemoryPool {
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock();
        let used = state
            .pool_members
            .entry_ref(reservation.consumer().name())
            .or_insert(0);
        *used = used.saturating_add(additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.state.lock();
        let used = state
            .pool_members
            .entry_ref(reservation.consumer().name())
            .or_insert(0);
        *used = used.saturating_sub(shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.state.lock();
        let used: usize = state.pool_members.values().sum();
        if used.saturating_add(additional) > self.pool_size {
            // dropping the mutex so that the display trait method does not deadlock
            drop(state);

            debug!("Pool Exhausted while trying to allocate {additional} bytes for {}:\n{self}", reservation.registration.consumer.name());
            return Err(insufficient_capacity_err(
                reservation,
                additional,
                self.pool_size.saturating_sub(used),
            ));
        }
        let entry = state
            .pool_members
            .entry_ref(reservation.consumer().name())
            .or_insert(0);
        *entry = entry.saturating_add(additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        let state = self.state.lock();
        state.pool_members.values().sum()
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        let mut state = self.state.lock();
        state.pool_members.remove(consumer.name());
    }
}

impl Display for GreedyMemoryPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        let used: usize = state.pool_members.values().sum();
        let free = self.pool_size.saturating_sub(used);

        let mut allocations = state.pool_members.iter().collect::<Vec<_>>();
        allocations.sort_by(|(_, a), (_, b)| a.cmp(b).reverse());

        let allocation_report = allocations
            .iter()
            .fold("".to_string(), |acc, (member, bytes)| {
                format!("{acc}\t{bytes}: {member}\n")
            });

        write!(
            f,
            "GreedyPool {} allocations, {} used, {} free, {} capacity\n{}",
            state.pool_members.len(),
            used,
            free,
            self.pool_size,
            allocation_report
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
struct FairSpillPoolMember {
    used: usize,
    can_spill: bool,
}

#[derive(Debug)]
struct FairSpillPoolState {
    pool_members: HashMap<String, FairSpillPoolMember>,
}

impl FairSpillPool {
    /// Allocate up to `limit` bytes
    pub fn new(pool_size: usize) -> Self {
        debug!("Created new FairSpillPool(pool_size={pool_size})");
        Self {
            pool_size,
            state: Mutex::new(FairSpillPoolState {
                pool_members: HashMap::new(),
            }),
        }
    }
}

impl MemoryPool for FairSpillPool {
    fn register(&self, consumer: &MemoryConsumer) {
        let mut state = self.state.lock();
        state.pool_members.insert(
            consumer.name().into(),
            FairSpillPoolMember {
                used: 0,
                can_spill: consumer.can_spill,
            },
        );
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        let mut state = self.state.lock();
        state.pool_members.remove(consumer.name());
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock();
        let member_state = state
            .pool_members
            .entry_ref(reservation.consumer().name())
            .or_insert_with(|| FairSpillPoolMember {
                used: 0,
                can_spill: reservation.registration.consumer.can_spill,
            });

        member_state.used = member_state.used.saturating_add(additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.state.lock();
        let member_state = state
            .pool_members
            .entry_ref(reservation.consumer().name())
            .or_insert_with(|| FairSpillPoolMember {
                used: 0,
                can_spill: reservation.registration.consumer.can_spill,
            });

        member_state.used = member_state.used.saturating_sub(shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.state.lock();

        match reservation.registration.consumer.can_spill {
            true => {
                let unspillable: usize = state
                    .pool_members
                    .values()
                    .filter_map(|e| (!e.can_spill).then_some(e.used))
                    .sum();

                // The total amount of memory available to spilling consumers
                let spill_available = self.pool_size.saturating_sub(unspillable);

                let num_spill =
                    state.pool_members.values().filter(|e| e.can_spill).count();

                // No spiller may use more than their fraction of the memory available
                let available = spill_available
                    .checked_div(num_spill)
                    .unwrap_or(spill_available);

                if reservation.size + additional > available {
                    // dropping the mutex so that the display trait method does not deadlock
                    drop(state);

                    debug!("Pool Exhausted while trying to allocate {additional} bytes for {}:\n{self}", reservation.registration.consumer.name());
                    return Err(insufficient_capacity_err(
                        reservation,
                        additional,
                        available,
                    ));
                }

                let entry = state
                    .pool_members
                    .entry_ref(reservation.consumer().name())
                    .or_insert_with(|| FairSpillPoolMember {
                        used: 0,
                        can_spill: reservation.registration.consumer.can_spill,
                    });

                entry.used = entry.used.saturating_add(additional);
            }
            false => {
                let total_used: usize = state.pool_members.values().map(|e| e.used).sum();
                let available = self.pool_size.saturating_sub(total_used);

                if available < additional {
                    drop(state);

                    debug!("Pool Exhausted while trying to allocate {additional} bytes for {}:\n{self}", reservation.registration.consumer.name());
                    return Err(insufficient_capacity_err(
                        reservation,
                        additional,
                        available,
                    ));
                }

                let entry = state
                    .pool_members
                    .entry_ref(reservation.consumer().name())
                    .or_insert_with(|| FairSpillPoolMember {
                        used: 0,
                        can_spill: reservation.registration.consumer.can_spill,
                    });

                entry.used = entry.used.saturating_add(additional);
            }
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        let state = self.state.lock();
        state
            .pool_members
            .values()
            .map(|FairSpillPoolMember { used, .. }| used)
            .sum()
    }
}

impl Display for FairSpillPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        let mut num_spill = 0;
        let mut unspillable_memory = 0;
        let mut spillable_memory = 0;

        for member in state.pool_members.values() {
            match member.can_spill {
                true => {
                    num_spill += 1;
                    spillable_memory += member.used
                }
                false => unspillable_memory += member.used,
            }
        }
        let free = self
            .pool_size
            .saturating_sub(unspillable_memory + spillable_memory);

        let mut allocations = state.pool_members.iter().collect::<Vec<_>>();
        allocations.sort_by(|(_, a), (_, b)| a.used.cmp(&b.used).reverse());

        let allocation_report = allocations.iter().fold(
            "".to_string(),
            |acc, (member, FairSpillPoolMember { can_spill, used })| {
                let can_spill = if *can_spill {
                    "spillable"
                } else {
                    "unspillable"
                };
                format!("{acc}\t{used}: [{can_spill}] {member}\n")
            },
        );

        write!(
            f,
            "FairSpillPool {} allocations, {} spillable used, {} total spillable, {} unspillable used, {} free, {} capacity\n{}",
            state.pool_members.len(),
            spillable_memory,
            num_spill,
            unspillable_memory,
            free,
            self.pool_size,
            allocation_report
        )
    }
}

fn insufficient_capacity_err(
    reservation: &MemoryReservation,
    additional: usize,
    available: usize,
) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!("Failed to allocate additional {} bytes for {} with {} bytes already allocated - maximum available is {}", additional, reservation.registration.consumer.name, reservation.size, available))
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
    fn test_greedy() {
        let pool = Arc::new(GreedyMemoryPool::new(100)) as _;
        let mut r1 = MemoryConsumer::new("r1").register(&pool);

        // Can grow beyond capacity of pool
        r1.grow(2000);
        assert_eq!(pool.reserved(), 2000);

        let mut r2 = MemoryConsumer::new("r2")
            .with_can_spill(true)
            .register(&pool);
        // Can grow beyond capacity of pool
        r2.grow(2000);

        let err = r1.try_grow(1).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 1 bytes for r1 with 2000 bytes already allocated - maximum available is 0");

        let err = r2.try_grow(1).unwrap_err().strip_backtrace();
        assert_eq!(err, "Resources exhausted: Failed to allocate additional 1 bytes for r2 with 2000 bytes already allocated - maximum available is 0");

        r1.shrink(1990);
        r2.shrink(2000);

        assert_eq!(pool.reserved(), 10);

        r1.try_grow(10).unwrap();
        assert_eq!(pool.reserved(), 20);
    }
}
