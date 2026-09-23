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
//! Files that `SET datafusion.runtime.memory_limit` replace their pool, so
//! their reservations after that point are not included in the total.
//!
//! This only logs. It never fails a test.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicIsize, Ordering},
    },
};

use datafusion::execution::memory_pool::{
    DriftLoggingPool, MemoryDriftTracker, MemoryPool,
};

static ALLOCATED: AtomicIsize = AtomicIsize::new(0);
static COUNTING: AtomicBool = AtomicBool::new(false);
static TRACKER: OnceLock<Arc<MemoryDriftTracker>> = OnceLock::new();

/// A [`GlobalAlloc`] that counts the bytes currently allocated through it,
/// delegating the allocation itself to `A`.
///
/// Counts requested sizes, so allocator overhead and memory retained by the
/// allocator are not included. Counting is off until
/// [`enable_memory_drift_logging`] is called.
pub struct CountingAllocator<A = System> {
    inner: A,
}

impl<A> CountingAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

/// Per-thread count is flushed to [`ALLOCATED`] once it moves this far, so
/// threads do not contend on one atomic for every allocation. The global count
/// is therefore accurate to within `threads * FLUSH_BYTES`.
const FLUSH_BYTES: isize = 256 * 1024;

thread_local! {
    // `const` with no destructor, so accessing it never allocates.
    static UNFLUSHED: Cell<isize> = const { Cell::new(0) };
}

fn count(delta: isize) {
    if !COUNTING.load(Ordering::Relaxed) {
        return;
    }
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

// SAFETY: every call is forwarded unchanged to `A`, which upholds the
// `GlobalAlloc` contract. Counting has no effect on the returned memory.
unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc(layout) };
        if !ptr.is_null() {
            count(layout.size() as isize);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        if !ptr.is_null() {
            count(layout.size() as isize);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.inner.dealloc(ptr, layout) };
        count(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            count(new_size as isize - layout.size() as isize);
        }
        new_ptr
    }
}

/// Bytes currently allocated through [`CountingAllocator`] since counting was
/// enabled.
///
/// Memory allocated before counting started and freed afterwards is
/// subtracted, so the count is clamped at zero.
pub fn allocated_bytes() -> usize {
    ALLOCATED.load(Ordering::Relaxed).max(0) as usize
}

/// Start counting allocations and wrap the memory pool of every test file
/// created from now on. Only has an effect if [`CountingAllocator`] is the
/// global allocator.
pub fn enable_memory_drift_logging() {
    COUNTING.store(true, Ordering::Relaxed);
    TRACKER.get_or_init(|| Arc::new(MemoryDriftTracker::new(Arc::new(allocated_bytes))));
}

/// The process-wide tracker, if [`enable_memory_drift_logging`] was called.
pub fn memory_drift_tracker() -> Option<&'static Arc<MemoryDriftTracker>> {
    TRACKER.get()
}

/// Wrap `pool` so its reservations are reported to the process-wide tracker,
/// or return it unchanged if drift logging is not enabled.
pub fn wrap_pool(pool: Arc<dyn MemoryPool>, label: &str) -> Arc<dyn MemoryPool> {
    match memory_drift_tracker() {
        Some(tracker) => {
            Arc::new(DriftLoggingPool::new(pool, Arc::clone(tracker), label))
        }
        None => pool,
    }
}
