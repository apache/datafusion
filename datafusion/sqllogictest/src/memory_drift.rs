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

//! Logs drift between `MemoryPool` reservations and actual allocations while
//! running sqllogictests. See <https://github.com/apache/datafusion/issues/25650>.
//!
//! Test files run concurrently in one process, so allocations cannot be split
//! per file. Instead every file's pool reports to one process-wide
//! [`MemoryDriftTracker`], which compares the sum of all reservations with the
//! bytes counted by [`CountingAllocator`].
//!
//! Files that `SET datafusion.runtime.memory_limit` replace their pool; the
//! runner wraps the replacement too (see [`rewrap_replaced_pool`]).
//!
//! This only logs. It never fails a test.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicIsize, Ordering},
    },
};

use datafusion::execution::SessionStateBuilder;
use datafusion::execution::memory_pool::{
    MemoryDriftTracker, MemoryPool, PeakRecordingPool,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionContext;

static ALLOCATED: AtomicIsize = AtomicIsize::new(0);
static TRACKER: OnceLock<Arc<MemoryDriftTracker>> = OnceLock::new();

/// A [`GlobalAlloc`] that counts the bytes currently allocated through it,
/// delegating the allocation itself to [`System`].
///
/// Counts requested sizes, so allocator overhead and memory retained by the
/// allocator are not included. Counting starts with the process, so memory
/// freed later was always counted when it was allocated.
pub struct CountingAllocator;

/// Per-thread count is flushed to [`ALLOCATED`] once it moves this far, so
/// threads do not contend on one atomic for every allocation. The global count
/// is therefore off by up to `FLUSH_BYTES` per live thread. Tokio threads
/// flush the rest when they stop (see [`flush_thread_allocations`]); the
/// unflushed count of any other thread that exits is lost.
const FLUSH_BYTES: isize = 256 * 1024;

thread_local! {
    // `const` with no destructor, so accessing it never allocates.
    static UNFLUSHED: Cell<isize> = const { Cell::new(0) };
}

fn count(delta: isize) {
    // `try_with` because this can run while the thread is being torn down.
    let _ = UNFLUSHED.try_with(|unflushed| {
        let pending = unflushed.get() + delta;
        if pending.abs() >= FLUSH_BYTES {
            ALLOCATED.fetch_add(pending, Ordering::Relaxed);
            unflushed.set(0);
        } else {
            unflushed.set(pending);
        }
    });
}

/// Flush this thread's unflushed count into the global count.
///
/// Registered as the Tokio runtime's `on_thread_stop` hook, which runs just
/// before a worker or blocking thread exits, outside the allocator.
pub fn flush_thread_allocations() {
    let _ = UNFLUSHED.try_with(|unflushed| {
        ALLOCATED.fetch_add(unflushed.replace(0), Ordering::Relaxed);
    });
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

/// Bytes currently allocated through [`CountingAllocator`], clamped at zero
/// because counts lost with exiting threads can bias it either way.
fn allocated_bytes() -> usize {
    ALLOCATED.load(Ordering::Relaxed).max(0) as usize
}

/// Wrap the memory pool of every test file created from now on, logging each
/// rise in drift of `log_threshold` bytes. Only meaningful if
/// [`CountingAllocator`] is the global allocator.
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
