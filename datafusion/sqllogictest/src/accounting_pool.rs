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
//! Each retune sets the bank to `new_limit * 1.10` — a fixed 10% headroom
//! over what DataFusion thinks it's allowed to use, so a query that
//! actually allocates >10% beyond its declared limit panics with an
//! `OverdraftPanic`. That gap *is* the bug we're hunting: DataFusion's
//! voluntary tracker saying one thing while the allocator says another.

use crate::set_account_balance;
use datafusion::common::Result;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

/// 10% headroom over the pool's declared limit. If actual allocations exceed
/// this, DataFusion's voluntary tracking is lying.
const HEADROOM_FACTOR: f64 = 1.10;

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
        set_account_balance((new_limit as f64 * HEADROOM_FACTOR) as isize);
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

        // 50_000 * 1.10 = 55_000. The balance is reset to this minus any
        // drift from allocations in this test thread between try_resize and
        // the read; tolerate a small window.
        let bal = account_balance();
        assert!(
            (50_000..=55_000).contains(&bal),
            "balance not in expected range: got {bal}"
        );
    }
}
