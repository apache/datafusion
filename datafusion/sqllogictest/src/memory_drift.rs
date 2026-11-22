// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Logs drift between `MemoryPool` reservations and actual allocations while
//! running sqllogictests. See <https://github.com/apache/datafusion/issues/25650>.
//!
//! Test files run concurrently in one process, so allocations cannot be split
//! per file. Instead every file's pool reports to one process-wide
//! [`MemoryDriftTracker`], which compares the sum of all reservations with the
//! bytes counted by [`CountingAllocator`].
//!
//! [`CountingAllocator`]: datafusion::execution::memory_pool::counting_allocator::CountingAllocator
//!
//! Files that `SET datafusion.runtime.memory_limit` replace their pool; the
//! runner wraps the replacement too (see [`rewrap_replaced_pool`]).
//!
//! This only logs. It never fails a test.

use std::sync::{Arc, OnceLock};

use datafusion::execution::SessionStateBuilder;
use datafusion::execution::memory_pool::counting_allocator::allocated_bytes;
use datafusion::execution::memory_pool::{
    MemoryDriftTracker, MemoryPool, PeakRecordingPool,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionContext;

static TRACKER: OnceLock<Arc<MemoryDriftTracker>> = OnceLock::new();

/// Wrap the memory pool of every test file created from now on, logging each
/// rise in drift of `log_threshold` bytes. Only meaningful if
/// [`CountingAllocator`] is the global allocator.
///
/// [`CountingAllocator`]: datafusion::execution::memory_pool::counting_allocator::CountingAllocator
pub fn enable_memory_drift_logging(log_threshold: usize) {
    TRACKER.get_or_init(|| {
        Arc::new(
            MemoryDriftTracker::new(Arc::new(allocated_bytes))
                .with_log_threshold(log_threshold),
        )
    });
}

/// The process-wide tracker, if [`enable_memory_drift_logging`] was called.
pub fn memory_drift_tracker() -> Option<&'static Arc<MemoryDriftTracker>> {
    TRACKER.get()
}

/// Wrap `pool` so its reservations are reported to the process-wide tracker,
/// or return it unchanged if drift logging is not enabled.
pub fn wrap_pool(pool: Arc<dyn MemoryPool>, label: &str) -> Arc<dyn MemoryPool> {
    match memory_drift_tracker() {
        Some(tracker) => Arc::new(
            PeakRecordingPool::new(pool).with_drift_tracker(Arc::clone(tracker), label),
        ),
        None => pool,
    }
}

/// Wrap the memory pool of `ctx` again if a statement replaced it, e.g.
/// `SET datafusion.runtime.memory_limit`, so its reservations keep being
/// counted. Does nothing if drift logging is not enabled.
pub(crate) fn rewrap_replaced_pool(ctx: &SessionContext, label: &str) {
    if memory_drift_tracker().is_none() {
        return;
    }
    let state = ctx.state_ref();
    let mut state = state.write();
    let runtime = state.runtime_env();
    if runtime.memory_pool.is::<PeakRecordingPool>() {
        return;
    }
    let pool = wrap_pool(Arc::clone(&runtime.memory_pool), label);
    let runtime = RuntimeEnvBuilder::from_runtime_env(runtime)
        .with_memory_pool(pool)
        .build_arc()
        .expect("rebuilding an existing runtime succeeds");
    *state = SessionStateBuilder::from(state.clone())
        .with_runtime_env(runtime)
        .build();
}
