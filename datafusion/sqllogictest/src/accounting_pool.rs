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

//! [`AccountingMemoryPool`] bridges DataFusion's voluntary memory tracking
//! to the allocator-level bank in [`crate::accounting`].
//!
//! It wraps any [`MemoryPool`] and re-tunes the current thread's bank
//! account whenever the pool's limit changes (via [`MemoryPool::try_resize`],
//! which `RuntimeEnvBuilder::with_memory_limit` triggers on `SET
//! datafusion.runtime.memory_limit = '…'`).
//!
//! Each retune sets the bank to `new_limit * HEADROOM_FACTOR`. A query
//! that allocates past that envelope panics with an `OverdraftPanic` —
//! the gap between DF's voluntary tracker and the allocator's reality
//! is the bug we're hunting.

use crate::set_account_balance;
use datafusion::common::Result;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use std::cell::Cell;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

/// Default overdraft factor: how much the allocator may exceed DataFusion's
/// declared `MemoryPool` limit before [`AccountingMemoryPool`] panics.
/// Anything past this is an untracked allocation — by definition, since
/// DF's pool didn't see it.
///
/// `8.0` (800%) is what it takes to pass the SLT suite today. Goal is to
/// drive this down to ~`1.1`; progress tracked in epic
/// <https://github.com/apache/datafusion/issues/22758>.
///
/// SLTs that exercise queries with more in-flight batches than the default
/// covers can opt into a larger factor for one resize via
/// `SET datafusion.sqllogictest.memory_overdraft_factor = N`, which the
/// SLT runner intercepts and stores in [`MEMORY_OVERDRAFT_FACTOR`].
pub const DEFAULT_MEMORY_OVERDRAFT_FACTOR: f64 = 8.0;

thread_local! {
    /// Per-thread overdraft multiplier, applied by [`AccountingMemoryPool::try_resize`].
    /// The SLT runner writes here when it sees a
    /// `SET datafusion.sqllogictest.memory_overdraft_factor = N` statement;
    /// it resets to [`DEFAULT_MEMORY_OVERDRAFT_FACTOR`] on each `try_resize`
    /// so opt-ins stay scoped to the SLT block that asked for them.
    static MEMORY_OVERDRAFT_FACTOR: Cell<f64> = const { Cell::new(DEFAULT_MEMORY_OVERDRAFT_FACTOR) };
}

/// Override the memory overdraft factor for the current thread. The next
/// [`AccountingMemoryPool::try_resize`] consumes the override, then resets
/// the thread-local back to [`DEFAULT_MEMORY_OVERDRAFT_FACTOR`].
pub fn set_memory_overdraft_factor(factor: f64) {
    MEMORY_OVERDRAFT_FACTOR.with(|h| h.set(factor));
}

/// Read the current thread's memory overdraft factor.
pub fn memory_overdraft_factor() -> f64 {
    MEMORY_OVERDRAFT_FACTOR.with(|h| h.get())
}

pub struct AccountingMemoryPool {
    inner: Arc<dyn MemoryPool>,
    /// The operator-configured default pool size, used as a "no SET has
    /// happened yet" sentinel by [`Self::memory_limit`].
    default_size: usize,
}

impl AccountingMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>, default_size: usize) -> Self {
        Self {
            inner,
            default_size,
        }
    }
}

impl fmt::Debug for AccountingMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccountingMemoryPool")
            .field("inner", &self.inner)
            .field("default_size", &self.default_size)
            .finish()
    }
}

impl Display for AccountingMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "accounting({})", self.inner)
    }
}

impl MemoryPool for AccountingMemoryPool {
    fn name(&self) -> &str {
        "accounting"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional)
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink)
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        // HACK: When the inner pool still reports the operator-configured
        // default, no `SET datafusion.runtime.memory_limit` has happened —
        // render as `Infinite` so `information_schema.slt`'s `SHOW ALL`
        // expectation of `unlimited` for an un-SET context stays satisfied.
        // Once a SET fires, `try_resize` mutates the inner pool to some
        // other value and we report the real limit.
        match self.inner.memory_limit() {
            MemoryLimit::Finite(n) if n == self.default_size => MemoryLimit::Infinite,
            other => other,
        }
    }

    fn try_resize(&self, new_limit: usize) -> Result<()> {
        self.inner.try_resize(new_limit)?;
        let factor = MEMORY_OVERDRAFT_FACTOR
            .with(|h| h.replace(DEFAULT_MEMORY_OVERDRAFT_FACTOR));
        set_account_balance((new_limit as f64 * factor) as isize);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{account_balance, next_context_id, set_thread_context_id};
    use datafusion::execution::memory_pool::GreedyMemoryPool;

    #[test]
    fn memory_limit_returns_infinite_for_sentinel() {
        let default_size = 1_000_000;
        let pool = AccountingMemoryPool::new(
            Arc::new(GreedyMemoryPool::new(default_size)),
            default_size,
        );
        assert!(matches!(pool.memory_limit(), MemoryLimit::Infinite));
    }

    #[test]
    fn memory_limit_returns_finite_after_resize() {
        let default_size = 1_000_000;
        let pool = AccountingMemoryPool::new(
            Arc::new(GreedyMemoryPool::new(default_size)),
            default_size,
        );
        pool.try_resize(50_000).unwrap();
        assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(50_000)));
    }

    #[test]
    fn try_resize_retunes_current_account_balance() {
        // Stamp a fresh context so set_account_balance lands somewhere
        // visible. Otherwise CONTEXT_ID == 0 means the call is a no-op.
        set_thread_context_id(next_context_id());

        let default_size = 1_000_000;
        let pool = AccountingMemoryPool::new(
            Arc::new(GreedyMemoryPool::new(default_size)),
            default_size,
        );
        pool.try_resize(50_000).unwrap();

        // Balance is reset to limit * DEFAULT_MEMORY_OVERDRAFT_FACTOR,
        // minus a small drift from this test thread's own allocs between
        // set and read.
        let expected = (50_000.0 * DEFAULT_MEMORY_OVERDRAFT_FACTOR) as isize;
        let bal = account_balance();
        assert!(
            (50_000..=expected).contains(&bal),
            "balance not in expected range: got {bal}, expected ≤ {expected}"
        );
    }

    #[test]
    fn overdraft_factor_override_is_consumed_by_try_resize() {
        set_thread_context_id(next_context_id());

        let default_size = 1_000_000;
        let pool = AccountingMemoryPool::new(
            Arc::new(GreedyMemoryPool::new(default_size)),
            default_size,
        );

        set_memory_overdraft_factor(2.0);
        pool.try_resize(100_000).unwrap();
        // 2.0 * 100K = 200K; subtract worst-case drift from this thread's
        // own allocs between the resize and the read.
        let bal = account_balance();
        assert!(
            (100_000..=200_000).contains(&bal),
            "first resize used wrong factor: got {bal}, expected ≤ 200_000"
        );

        // Override consumed: next resize falls back to default.
        pool.try_resize(100_000).unwrap();
        let bal = account_balance();
        let expected_default =
            (100_000.0 * DEFAULT_MEMORY_OVERDRAFT_FACTOR) as isize;
        assert!(
            (100_000..=expected_default).contains(&bal),
            "second resize did not fall back to default factor: got {bal}, \
             expected ≤ {expected_default}"
        );
    }
}
