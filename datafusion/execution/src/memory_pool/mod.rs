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

//! Manages all available memory during query execution

use datafusion_common::Result;
use std::{cmp::Ordering, sync::Arc};

mod pool;
pub mod proxy;

pub use pool::*;

/// The pool of memory on which [`MemoryReservation`]s record their
/// memory reservations.
///
/// DataFusion is a streaming query engine, processing most queries
/// without buffering the entire input. However, certain operations
/// such as sorting and grouping/joining with a large number of
/// distinct groups/keys, can require buffering intermediate results
/// and for large datasets this can require large amounts of memory.
///
/// In order to avoid allocating memory until the OS or the container
/// system kills the process, DataFusion operators only allocate
/// memory they are able to reserve from the configured
/// [`MemoryPool`]. Once the memory tracked by the pool is exhausted,
/// operators must either free memory by spilling to local disk or
/// error.
///
/// A `MemoryPool` can be shared by concurrently executing plans in
/// the same process to control memory usage in a multi-tenant system.
///
/// The following memory pool implementations are available:
///
/// * [`UnboundedMemoryPool`](pool::UnboundedMemoryPool)
/// * [`GreedyMemoryPool`](pool::GreedyMemoryPool)
/// * [`FairSpillPool`](pool::FairSpillPool)
pub trait MemoryPool: Send + Sync + std::fmt::Debug {
    /// Registers a new [`MemoryConsumer`]
    ///
    /// Note: Subsequent calls to [`Self::grow`] must be made to reserve memory
    fn register(&self, _consumer: &MemoryConsumer) {}

    /// Records the destruction of a [`MemoryReservation`] with [`MemoryConsumer`]
    ///
    /// Note: Prior calls to [`Self::shrink`] must be made to free any reserved memory
    fn unregister(&self, _consumer: &MemoryConsumer) {}

    /// Infallibly grow the provided `reservation` by `additional` bytes
    ///
    /// This must always succeed
    fn grow(&self, reservation: &MemoryReservation, additional: usize);

    /// Infallibly shrink the provided `reservation` by `shrink` bytes
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize);

    /// Attempt to grow the provided `reservation` by `additional` bytes
    ///
    /// On error the `allocation` will not be increased in size
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()>;

    /// Return the total amount of memory reserved
    fn reserved(&self) -> usize;
}

/// A memory consumer that can be tracked by [`MemoryReservation`] in a [`MemoryPool`]
#[derive(Debug)]
pub struct MemoryConsumer {
    name: String,
    can_spill: bool,
}

impl MemoryConsumer {
    /// Create a new empty [`MemoryConsumer`] that can be grown using [`MemoryReservation`]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            can_spill: false,
        }
    }

    /// Set whether this allocation can be spilled to disk
    pub fn with_can_spill(self, can_spill: bool) -> Self {
        Self { can_spill, ..self }
    }

    /// Returns true if this allocation can spill to disk
    pub fn can_spill(&self) -> bool {
        self.can_spill
    }

    /// Returns the name associated with this allocation
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Registers this [`MemoryConsumer`] with the provided [`MemoryPool`] returning
    /// a [`MemoryReservation`] that can be used to grow or shrink the memory reservation
    pub fn register(self, pool: &Arc<dyn MemoryPool>) -> MemoryReservation {
        pool.register(&self);
        MemoryReservation {
            consumer: self,
            size: 0,
            policy: Arc::clone(pool),
        }
    }
}

/// A [`MemoryReservation`] tracks a reservation of memory in a [`MemoryPool`]
/// that is freed back to the pool on drop
#[derive(Debug)]
pub struct MemoryReservation {
    consumer: MemoryConsumer,
    size: usize,
    policy: Arc<dyn MemoryPool>,
}

impl MemoryReservation {
    /// Returns the size of this reservation in bytes
    pub fn size(&self) -> usize {
        self.size
    }

    /// Frees all bytes from this reservation returning the number of bytes freed
    pub fn free(&mut self) -> usize {
        let size = self.size;
        if size != 0 {
            self.shrink(size)
        }
        size
    }

    /// Frees `capacity` bytes from this reservation
    ///
    /// # Panics
    ///
    /// Panics if `capacity` exceeds [`Self::size`]
    pub fn shrink(&mut self, capacity: usize) {
        let new_size = self.size.checked_sub(capacity).unwrap();
        self.policy.shrink(self, capacity);
        self.size = new_size
    }

    /// Sets the size of this reservation to `capacity`
    pub fn resize(&mut self, capacity: usize) {
        match capacity.cmp(&self.size) {
            Ordering::Greater => self.grow(capacity - self.size),
            Ordering::Less => self.shrink(self.size - capacity),
            _ => {}
        }
    }

    /// Try to set the size of this reservation to `capacity`
    pub fn try_resize(&mut self, capacity: usize) -> Result<()> {
        match capacity.cmp(&self.size) {
            Ordering::Greater => self.try_grow(capacity - self.size)?,
            Ordering::Less => self.shrink(self.size - capacity),
            _ => {}
        };
        Ok(())
    }

    /// Increase the size of this reservation by `capacity` bytes
    pub fn grow(&mut self, capacity: usize) {
        self.policy.grow(self, capacity);
        self.size += capacity;
    }

    /// Try to increase the size of this reservation by `capacity` bytes
    pub fn try_grow(&mut self, capacity: usize) -> Result<()> {
        self.policy.try_grow(self, capacity)?;
        self.size += capacity;
        Ok(())
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.free();
        self.policy.unregister(&self.consumer);
    }
}

const TB: u64 = 1 << 40;
const GB: u64 = 1 << 30;
const MB: u64 = 1 << 20;
const KB: u64 = 1 << 10;

/// Present size in human readable form
pub fn human_readable_size(size: usize) -> String {
    let size = size as u64;
    let (value, unit) = {
        if size >= 2 * TB {
            (size as f64 / TB as f64, "TB")
        } else if size >= 2 * GB {
            (size as f64 / GB as f64, "GB")
        } else if size >= 2 * MB {
            (size as f64 / MB as f64, "MB")
        } else if size >= 2 * KB {
            (size as f64 / KB as f64, "KB")
        } else {
            (size as f64, "B")
        }
    };
    format!("{value:.1} {unit}")
}

/// Tracks the change in memory to avoid overflow. Typically, this
/// is isued like the following
///
/// 1. Call `delta.dec(sized_thing.size())`
///
/// 2. potentially change size of `sized_thing`
///
/// 3. Call `delta.inc(size_thing.size())`
#[derive(Debug, Default)]
pub struct MemoryDelta {
    decrease: usize,
    increase: usize,
}

impl MemoryDelta {
    pub fn new() -> Self {
        Default::default()
    }

    /// record size being 'decremented'. This is used for to record the
    /// initial size of some allocation prior to hange
    pub fn dec(&mut self, sz: usize) {
        self.decrease += sz;
    }

    /// record size being 'incremented'. This is used for to record
    /// the final size of some object.
    pub fn inc(&mut self, sz: usize) {
        self.increase += sz;
    }

    /// Adjusts the reservation with the delta used / freed
    pub fn update(self, reservation: &mut MemoryReservation) -> Result<()> {
        let Self { decrease, increase } = self;
        match increase.cmp(&decrease) {
            Ordering::Less => reservation.shrink(decrease - increase),
            Ordering::Equal => {}
            Ordering::Greater => reservation.try_grow(increase - decrease)?,
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_pool_underflow() {
        let pool = Arc::new(GreedyMemoryPool::new(50)) as _;
        let mut a1 = MemoryConsumer::new("a1").register(&pool);
        assert_eq!(pool.reserved(), 0);

        a1.grow(100);
        assert_eq!(pool.reserved(), 100);

        assert_eq!(a1.free(), 100);
        assert_eq!(pool.reserved(), 0);

        a1.try_grow(100).unwrap_err();
        assert_eq!(pool.reserved(), 0);

        a1.try_grow(30).unwrap();
        assert_eq!(pool.reserved(), 30);

        let mut a2 = MemoryConsumer::new("a2").register(&pool);
        a2.try_grow(25).unwrap_err();
        assert_eq!(pool.reserved(), 30);

        drop(a1);
        assert_eq!(pool.reserved(), 0);

        a2.try_grow(25).unwrap();
        assert_eq!(pool.reserved(), 25);
    }
}
