// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Logs drift between `MemoryPool` reservations and actual allocations while
//! running sqllogictests. See <https://github.com/apache/datafusion/issues/25650>.
//!
//! Each test file runs in a [`MemoryTrackedFuture`] charged to its own
//! [`MemoryAccount`], from the per-thread count kept by [`CountingAllocator`].
//! [`MemoryAccountTracer`] charges the tasks a file spawns to the same account,
//! so files running concurrently are measured apart. Each file's pool reports
//! to its own [`MemoryDriftTracker`], comparing its reservations with its
//! account.
//!
//! Files that `SET datafusion.runtime.memory_limit` replace their pool; the
//! runner wraps the replacement too (see [`rewrap_replaced_pool`]).
//!
//! This only logs. It never fails a test.
//!
//! [`MemoryTrackedFuture`]: datafusion::common::runtime::MemoryTrackedFuture
//! [`MemoryAccount`]: datafusion::common::runtime::MemoryAccount

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::{Arc, Mutex, OnceLock},
};

use datafusion::common::runtime::{
    MemoryAccountTracer, current_memory_account, set_join_set_tracer,
    set_thread_allocated_bytes,
};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::memory_pool::{
    DriftSample, MemoryDriftTracker, MemoryPool, PeakRecordingPool,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionContext;

/// Set once drift logging is enabled.
static LOG_THRESHOLD: OnceLock<usize> = OnceLock::new();
/// Every test file's tracker, to report the peak at the end of the run.
static TRACKERS: Mutex<Vec<Arc<MemoryDriftTracker>>> = Mutex::new(Vec::new());

/// A [`GlobalAlloc`] that counts the bytes each thread allocates minus the
/// bytes it frees, delegating the allocation itself to [`System`].
///
/// Counts requested sizes, so allocator overhead and memory retained by the
/// allocator are not included.
pub struct CountingAllocator;

thread_local! {
    // `const` with no destructor, so accessing it never allocates.
    static THREAD_ALLOCATED: Cell<u64> = const { Cell::new(0) };
}

fn count(delta: isize) {
    // `try_with` because this can run while the thread is being torn down.
    let _ = THREAD_ALLOCATED
        .try_with(|allocated| allocated.set(allocated.get().wrapping_add(delta as u64)));
}

fn thread_allocated_bytes() -> u64 {
    THREAD_ALLOCATED.try_with(Cell::get).unwrap_or(0)
}

// SAFETY: every call is forwarded unchanged to `System`, which upholds the
// `GlobalAlloc` contract. Counting has no effect on the returned memory.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            count(layout.size() as isize);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            count(layout.size() as isize);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        count(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            count(new_size as isize - layout.size() as isize);
        }
        new_ptr
    }
}

/// Charge each test file run from now on to its own account, logging each
/// rise in a file's drift of `log_threshold` bytes. Only meaningful if
/// [`CountingAllocator`] is the global allocator.
pub fn enable_memory_drift_logging(log_threshold: usize) {
    if LOG_THRESHOLD.set(log_threshold).is_err() {
        return;
    }
    set_thread_allocated_bytes(thread_allocated_bytes)
        .expect("only sqllogictests sets the thread allocated bytes counter");
    set_join_set_tracer(&MemoryAccountTracer)
        .expect("only sqllogictests sets the JoinSet tracer");
}

/// The largest drift any test file reached, if drift logging is enabled.
pub fn peak_memory_drift() -> Option<DriftSample> {
    TRACKERS
        .lock()
        .unwrap()
        .iter()
        .filter_map(|tracker| tracker.peak_drift())
        .max_by_key(|sample| sample.drift)
}

/// Wrap `pool` so its reservations are compared with the account of the test
/// file creating it. Returns it unchanged if drift logging is not enabled or
/// no file is being charged.
pub fn wrap_pool(pool: Arc<dyn MemoryPool>, label: &str) -> Arc<dyn MemoryPool> {
    let (Some(&log_threshold), Some(account)) =
        (LOG_THRESHOLD.get(), current_memory_account())
    else {
        return pool;
    };
    let tracker = Arc::new(
        MemoryDriftTracker::new(Arc::new(move || account.bytes().max(0) as usize))
            .with_log_threshold(log_threshold),
    );
    TRACKERS.lock().unwrap().push(Arc::clone(&tracker));
    Arc::new(PeakRecordingPool::new(pool).with_drift_tracker(tracker, label))
}

/// Wrap the memory pool of `ctx` again if a statement replaced it, e.g.
/// `SET datafusion.runtime.memory_limit`, so its reservations keep being
/// counted. Does nothing if drift logging is not enabled.
pub(crate) fn rewrap_replaced_pool(ctx: &SessionContext, label: &str) {
    if LOG_THRESHOLD.get().is_none() {
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
