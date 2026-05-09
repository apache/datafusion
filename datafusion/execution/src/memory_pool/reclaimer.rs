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

//! Operator hook used by a [`MemoryPool`] to free memory before an
//! allocation fails.
//!
//! [`MemoryPool`]: super::MemoryPool

use datafusion_common::Result;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;

/// Encoded values stored in the [`reclaimer_state`] tri-state.
///
/// [`reclaimer_state`]: MemoryReclaimer::reclaimer_state
pub mod reclaimer_state {
    /// Reclaimer is idle and may be selected as a victim.
    pub const AVAILABLE: u8 = 0;
    /// A pool task is currently driving `reclaim` on this reclaimer.
    pub const IN_FLIGHT: u8 = 1;
    /// Reclaimer has been retired (e.g. operator entered a phase where
    /// it can no longer free memory). Sticky — never returns to
    /// `AVAILABLE`.
    pub const DISABLED: u8 = 2;
}

/// Hook attached to a [`MemoryConsumer`] via
/// [`MemoryConsumer::with_reclaimer`]. On
/// [`MemoryPool::try_grow_async`] failure the pool walks registered
/// reclaimers in descending [`Self::priority`] and asks each to free bytes.
///
/// Implementations MUST:
///
/// - Spill data **before** shrinking the reservation, so reported bytes
///   are recoverable.
/// - Release bytes via [`MemoryReservation::shrink`] /
///   [`MemoryReservation::free`] and return at most `target`.
/// - Not call `try_grow*` on the pool — risks reentrancy/deadlock.
/// - Not capture `Arc<MemoryReservation>` / `Arc<MemoryConsumer>`
///   (creates a cycle that blocks `unregister`); use channels or `Weak`.
///
/// [`MemoryConsumer`]: super::MemoryConsumer
/// [`MemoryConsumer::with_reclaimer`]: super::MemoryConsumer::with_reclaimer
/// [`MemoryPool::try_grow_async`]: super::MemoryPool::try_grow_async
/// [`MemoryReservation::shrink`]: super::MemoryReservation::shrink
/// [`MemoryReservation::free`]: super::MemoryReservation::free
#[async_trait::async_trait]
pub trait MemoryReclaimer: Send + Sync + Debug {
    /// Upper bound on bytes this reclaimer can free. `None` = unknown.
    fn reclaimable_bytes(&self) -> Option<usize> {
        None
    }

    /// Free up to `target` bytes; return the amount actually released.
    /// See trait-level contract.
    async fn reclaim(&self, target: usize) -> Result<usize>;

    /// Higher priorities reclaim first. Negative = last resort.
    fn priority(&self) -> i32 {
        0
    }

    /// Optional shared tri-state flag controlling whether the pool
    /// currently considers this reclaimer eligible. Values are defined
    /// in [`reclaimer_state`]. Returning `Some(arc)` lets the
    /// reclaimer's owner flip itself to `DISABLED` once it can no
    /// longer free memory (e.g., on entering a merge phase), which
    /// the pool observes immediately. Returning `None` lets the pool
    /// allocate its own private flag — used only for in-flight dedup.
    fn reclaimer_state(&self) -> Option<Arc<AtomicU8>> {
        None
    }
}
