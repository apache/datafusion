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

//! Shares reserved workspace across merge reservations and temporary loans.

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use datafusion_common::{Result, resources_err};
use parking_lot::Mutex;

use super::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation};

/// Shares reserved merge workspace across child reservations and temporary loans.
///
/// This pool charges a single reservation to its parent [`MemoryPool`]. After
/// acquiring workspace through a child [`MemoryReservation`], call [`Self::retain`]
/// to keep that capacity available even when children release their reservations.
/// Children and [`WorkspaceLoan`]s share this capacity; only usage above the
/// existing parent reservation requires additional parent memory. Call
/// [`Self::release_unused`] when no further merge needs the idle workspace.
/// Live children and loans keep the pool and their reserved memory alive.
///
/// All parent accounting and allocation policy uses the [`MemoryConsumer`]
/// supplied to [`Self::new`]. Child consumers are not registered with the parent,
/// so their names and [`MemoryConsumer::can_spill`] flags do not affect the
/// parent's accounting or fair shares. Consumers that need a separate parent
/// allocation policy should register directly with the parent. They can borrow
/// already-acquired workspace with [`Self::borrow`] and request any additional
/// capacity through their own reservation.
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use datafusion_common::Result;
/// # use datafusion_execution::memory_pool::{
/// #     GreedyMemoryPool, MemoryConsumer, MemoryPool, MergeMemoryPool,
/// # };
/// # fn main() -> Result<()> {
/// let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(100));
/// let workspace = Arc::new(MergeMemoryPool::new(
///     Arc::clone(&parent),
///     MemoryConsumer::new("merge workspace"),
/// ));
/// let pool: Arc<dyn MemoryPool> = Arc::clone(&workspace) as _;
/// let reservation = MemoryConsumer::new("merge children").register(&pool);
/// reservation.try_grow(60)?;
/// workspace.retain(60);
/// reservation.free();
///
/// // Another operator can use only the space outside the retained workspace.
/// let contender = MemoryConsumer::new("contender").register(&parent);
/// contender.try_grow(40)?;
/// assert_eq!(parent.reserved(), 100);
///
/// // Children and loans reuse that workspace without another parent grant.
/// let cursor = reservation.new_empty();
/// cursor.try_grow(20)?;
/// let loan = workspace.borrow(60);
/// assert_eq!(loan.size(), 40);
/// workspace.release_unused();
/// assert_eq!(parent.reserved(), 100); // Both the cursor and loan remain charged.
/// drop(loan);
/// assert_eq!(parent.reserved(), 60); // Only the cursor and contender remain.
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct MergeMemoryPool {
    parent: Arc<dyn MemoryPool>,
    state: Mutex<MergeMemoryState>,
}

#[derive(Debug)]
struct MergeMemoryState {
    /// The only reservation charged to the execution pool.
    reservation: MemoryReservation,
    /// Total usage of all reservations and workspace loans in this pool,
    /// including siblings created with `new_empty` or `split`.
    used: usize,
    /// Workspace already acquired for the next merge. It is reusable by any
    /// child reservation, but unavailable to other execution-pool consumers.
    retained: usize,
}

/// A temporary claim on workspace already reserved by a [`MergeMemoryPool`].
///
/// Created by [`MergeMemoryPool::borrow`]. The loan shares capacity with child
/// reservations and keeps the pool alive until it is dropped. Shrinking or
/// dropping the loan returns its bytes to the pool; the parent reservation is
/// reduced only when those bytes are no longer retained as workspace.
#[derive(Debug)]
pub struct WorkspaceLoan {
    pool: Arc<MergeMemoryPool>,
    size: usize,
}

impl WorkspaceLoan {
    /// Returns the number of bytes currently held by this loan.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns `size` bytes to the pool.
    ///
    /// # Panics
    ///
    /// Panics if `size` exceeds [`Self::size`].
    pub fn shrink(&mut self, size: usize) {
        self.size = self
            .size
            .checked_sub(size)
            .expect("workspace loan underflow");
        if size != 0 {
            self.pool.release(size);
        }
    }
}

impl Drop for WorkspaceLoan {
    fn drop(&mut self) {
        self.shrink(self.size);
    }
}

impl MergeMemoryPool {
    /// Creates a pool whose memory is charged to `consumer` in `parent`.
    ///
    /// Registers the consumer without reserving any memory. The consumer's
    /// allocation policy applies to the combined usage of all children and loans.
    pub fn new(parent: Arc<dyn MemoryPool>, consumer: MemoryConsumer) -> Self {
        let reservation = consumer.register(&parent);
        Self {
            parent,
            state: Mutex::new(MergeMemoryState {
                reservation,
                used: 0,
                retained: 0,
            }),
        }
    }

    /// Sets the amount of acquired workspace to retain when children release it.
    ///
    /// The workspace must first be acquired through a child reservation. This
    /// method sets the retained floor without changing the parent reservation.
    /// Use [`Self::release_unused`] to stop retaining idle workspace immediately.
    ///
    /// # Panics
    ///
    /// Panics if `size` exceeds this pool's current reservation in the parent,
    /// as reported by its [`MemoryPool::reserved`] method.
    pub fn retain(&self, size: usize) {
        let mut state = self.state.lock();
        assert!(size <= state.reservation.size());
        state.retained = size;
    }

    /// Lends up to `size` unused bytes without acquiring more parent memory.
    ///
    /// Returns a smaller loan if less workspace is available, including an empty
    /// loan if all capacity is in use. The loan is counted alongside child
    /// reservations so they cannot spend the same credit. Dropping it returns
    /// any outstanding bytes.
    pub fn borrow(self: &Arc<Self>, size: usize) -> WorkspaceLoan {
        let mut state = self.state.lock();
        let size = size.min(state.reservation.size() - state.used);
        state.used += size;
        WorkspaceLoan {
            pool: Arc::clone(self),
            size,
        }
    }

    /// Stops retaining idle workspace and returns unused capacity to the parent.
    ///
    /// Live child reservations and loans remain charged. Their future releases
    /// also return memory to the parent, unless [`Self::retain`] is called again.
    pub fn release_unused(&self) {
        let mut state = self.state.lock();
        state.retained = 0;
        state.reservation.resize(state.used);
    }

    fn release(&self, size: usize) {
        let mut state = self.state.lock();
        state.used = state.used.checked_sub(size).expect("memory underflow");
        state.reservation.resize(state.used.max(state.retained));
    }
}

impl Display for MergeMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MergeMemoryPool")
    }
}

impl MemoryPool for MergeMemoryPool {
    fn name(&self) -> &str {
        "MergeMemoryPool"
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock();
        let used = state.used.checked_add(additional).expect("memory overflow");
        if used > state.reservation.size() {
            state.reservation.resize(used);
        }
        state.used = used;
    }

    fn try_grow(
        &self,
        _reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<()> {
        let mut state = self.state.lock();
        let Some(used) = state.used.checked_add(additional) else {
            return resources_err!("Sort merge memory reservation overflow");
        };
        if used > state.reservation.size() {
            state.reservation.try_resize(used)?;
        }
        state.used = used;
        Ok(())
    }

    fn shrink(&self, _reservation: &MemoryReservation, subtractive: usize) {
        self.release(subtractive);
    }

    fn reserved(&self) -> usize {
        self.state.lock().reservation.size()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.parent.memory_limit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::GreedyMemoryPool;

    fn reservation(
        parent: &Arc<dyn MemoryPool>,
    ) -> (Arc<MergeMemoryPool>, MemoryReservation) {
        let pool = Arc::new(MergeMemoryPool::new(
            Arc::clone(parent),
            MemoryConsumer::new("merge workspace"),
        ));
        let reservation = MemoryConsumer::new("merge children")
            .register(&(Arc::clone(&pool) as Arc<dyn MemoryPool>));
        (pool, reservation)
    }

    #[test]
    fn reuses_workspace_across_child_reservations() -> Result<()> {
        let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(100));
        let (pool, reservation) = reservation(&parent);
        reservation.try_grow(60)?;
        pool.retain(60);
        reservation.free();

        let contender = MemoryConsumer::new("contender").register(&parent);
        contender.try_grow(40)?;
        assert_eq!(parent.reserved(), 100);

        let cursor = reservation.new_empty();
        let rows = reservation.new_empty();
        cursor.try_grow(20)?;
        rows.try_grow(40)?;
        assert!(rows.try_grow(1).is_err());
        assert_eq!(rows.size(), 40);
        assert_eq!(parent.reserved(), 100);

        drop(cursor);
        drop(rows);
        assert_eq!(pool.reserved(), 60);
        reservation.try_grow(60)?; // Workspace for the next spill needs no new grant.
        assert_eq!(parent.reserved(), 100);

        drop(reservation);
        drop(pool);
        assert_eq!(parent.reserved(), 40);
        drop(contender);
        assert_eq!(parent.reserved(), 0);
        Ok(())
    }

    #[test]
    fn child_keeps_workspace_alive_after_sorter_drops() -> Result<()> {
        let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(100));
        let (pool, mut reservation) = reservation(&parent);
        reservation.try_grow(60)?;
        pool.retain(60);
        let child = reservation.take();
        let sibling = child.split(20);
        drop(reservation);
        drop(pool);
        assert_eq!(parent.reserved(), 60);

        child.free();
        assert_eq!(parent.reserved(), 60);
        child.try_grow(40)?;
        drop(child);
        assert_eq!(parent.reserved(), 60);
        drop(sibling);
        assert_eq!(parent.reserved(), 0);
        Ok(())
    }

    #[test]
    fn releases_excess_capacity_but_retains_workspace() -> Result<()> {
        let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(100));
        let (pool, reservation) = reservation(&parent);
        reservation.try_grow(40)?;
        pool.retain(40);
        reservation.try_grow(50)?;
        assert_eq!(parent.reserved(), 90);
        reservation.shrink(60);
        assert_eq!(parent.reserved(), 40);
        let loan = pool.borrow(10);
        assert_eq!(loan.size(), 10);
        pool.release_unused();
        assert_eq!(parent.reserved(), 40); // Live loans remain charged.
        drop(loan);
        assert_eq!(parent.reserved(), 30);
        drop(reservation);
        assert_eq!(parent.reserved(), 0);
        Ok(())
    }

    #[test]
    fn does_not_reserve_workspace_implicitly() -> Result<()> {
        let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8));
        let (pool, reservation) = reservation(&parent);
        reservation.grow(0);
        reservation.try_grow(0)?;
        assert_eq!(parent.reserved(), 0);
        reservation.try_grow(8)?;
        assert!(reservation.try_grow(1).is_err());
        assert!(reservation.try_grow(usize::MAX).is_err());
        assert_eq!(reservation.size(), 8);
        assert_eq!(pool.reserved(), 8);
        reservation.free();
        assert_eq!(parent.reserved(), 0);
        Ok(())
    }

    #[test]
    fn delegates_infallible_growth_to_parent() {
        let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8));
        let (pool, reservation) = reservation(&parent);
        reservation.grow(16);
        assert_eq!(parent.reserved(), 16);
        assert_eq!(pool.reserved(), 16);
        drop(reservation);
        assert_eq!(parent.reserved(), 0);
    }

    #[test]
    fn workspace_loans_share_credit_with_cursors() -> Result<()> {
        let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(100));
        let (pool, reservation) = reservation(&parent);
        reservation.try_grow(60)?;
        pool.retain(60);
        reservation.free();
        let contender = MemoryConsumer::new("contender").register(&parent);
        contender.try_grow(40)?;
        let cursor = reservation.new_empty();
        cursor.try_grow(20)?;

        // Both loans stay alive until after both requests complete. Together they
        // may claim only the 40 bytes not already assigned to the cursor.
        let gate = std::sync::Barrier::new(2);
        let (mut first, second) = std::thread::scope(|scope| {
            let first = scope.spawn(|| {
                gate.wait();
                pool.borrow(30)
            });
            let second = scope.spawn(|| {
                gate.wait();
                pool.borrow(30)
            });
            (first.join().unwrap(), second.join().unwrap())
        });
        assert_eq!(first.size() + second.size(), 40);
        assert!(matches!((first.size(), second.size()), (30, 10) | (10, 30)));
        assert_eq!(pool.borrow(1).size(), 0);
        assert_eq!(parent.reserved(), 100);

        first.shrink(5);
        cursor.try_grow(5)?;
        assert_eq!(first.size() + second.size() + cursor.size(), 60);
        assert_eq!(pool.borrow(1).size(), 0);
        assert!(cursor.try_grow(1).is_err());
        assert_eq!(cursor.size(), 25);
        assert_eq!(parent.reserved(), 100);

        let returned = first.size();
        drop(first);
        let replacement = pool.borrow(usize::MAX);
        assert_eq!(replacement.size(), returned);
        assert_eq!(parent.reserved(), 100);

        // The final loan owns the workspace even after the sorter and its child
        // reservations are gone; dropping it releases the parent allocation once.
        drop(reservation);
        drop(pool);
        drop(cursor);
        drop(second);
        assert_eq!(parent.reserved(), 100);
        drop(replacement);
        assert_eq!(parent.reserved(), 40);
        drop(contender);
        assert_eq!(parent.reserved(), 0);
        Ok(())
    }

    #[test]
    fn workspace_loan_preserves_ordinary_consumer_fair_share() -> Result<()> {
        use crate::memory_pool::{FairSpillPool, TrackConsumersPool};
        use std::num::NonZeroUsize;

        let tracked = Arc::new(TrackConsumersPool::new(
            FairSpillPool::new(100),
            NonZeroUsize::new(3).unwrap(),
        ));
        let parent: Arc<dyn MemoryPool> = Arc::clone(&tracked) as Arc<dyn MemoryPool>;
        let (pool, reservation) = reservation(&parent);
        reservation.try_grow(20)?;
        pool.retain(20);
        reservation.free();
        let ordinary = MemoryConsumer::new("ordinary sort")
            .with_can_spill(true)
            .register(&parent);
        let contender = MemoryConsumer::new("other spillable sort")
            .with_can_spill(true)
            .register(&parent);
        ordinary.try_grow(40)?;
        assert_eq!(parent.reserved(), 60);

        let snapshot = || {
            let mut metrics = tracked
                .metrics()
                .into_iter()
                .map(|m| (m.name, m.can_spill, m.reserved, m.peak))
                .collect::<Vec<_>>();
            metrics.sort_unstable();
            metrics
        };
        let before = snapshot();
        assert_eq!(before.len(), 3);

        // Although 40 bytes remain globally free, the ordinary consumer's share
        // is (100 - 20) / 2 = 40. Only the acquired 20 bytes may be borrowed;
        // the remaining 5 must be denied under the original consumer's policy.
        let result: Result<()> = (|| {
            let sorted_size = 65;
            let loan = pool.borrow(sorted_size - ordinary.size());
            assert_eq!(loan.size(), 20);
            ordinary.try_resize(sorted_size - loan.size())?;
            Ok(())
        })();
        assert!(result.is_err());
        assert_eq!(ordinary.size(), 40);
        assert_eq!(parent.reserved(), 60);
        assert_eq!(snapshot(), before);

        // Failed resize returned its loan, so a smaller expansion can now use
        // existing credit without a new grant or a different parent consumer.
        let loan = pool.borrow(55 - ordinary.size());
        assert_eq!(loan.size(), 15);
        ordinary.try_resize(55 - loan.size())?;
        assert_eq!(ordinary.size(), 40);
        assert_eq!(parent.reserved(), 60);
        assert_eq!(snapshot(), before);

        drop(loan);
        drop(ordinary);
        drop(contender);
        drop(reservation);
        drop(pool);
        assert_eq!(parent.reserved(), 0);
        assert!(tracked.metrics().is_empty());
        Ok(())
    }

    #[test]
    fn failed_sorted_resize_returns_workspace_loan() -> Result<()> {
        let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(100));
        let (pool, reservation) = reservation(&parent);
        assert_eq!(pool.borrow(usize::MAX).size(), 0);
        assert_eq!(parent.reserved(), 0);
        reservation.try_grow(60)?;
        pool.retain(60);
        reservation.free();
        let ordinary = MemoryConsumer::new("ordinary sort")
            .with_can_spill(true)
            .register(&parent);
        ordinary.try_grow(40)?;

        let result: Result<()> = (|| {
            let sorted_size = 120;
            let loan = pool.borrow(sorted_size - ordinary.size());
            assert_eq!(loan.size(), 60);
            ordinary.try_resize(sorted_size - loan.size())?;
            Ok(())
        })();
        assert!(result.is_err());
        assert_eq!(ordinary.size(), 40);
        assert_eq!(parent.reserved(), 100);
        let restored = pool.borrow(usize::MAX);
        assert_eq!(restored.size(), 60);
        assert_eq!(parent.reserved(), 100);

        drop(restored);
        drop(ordinary);
        drop(reservation);
        drop(pool);
        assert_eq!(parent.reserved(), 0);
        Ok(())
    }
}
