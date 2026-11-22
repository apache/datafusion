// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! A counting [`GlobalAlloc`] that tests and test runners install to compare
//! [`MemoryPool`](super::MemoryPool) reservations with real allocations.
//!
//! # Public Only for Internal Use:
//!
//! This is not a public API and is for internal use only; see [API policy] for details.
//!
//! [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::atomic::{AtomicIsize, Ordering},
};

static ALLOCATED: AtomicIsize = AtomicIsize::new(0);

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
    // Exact count of this thread, for [`ThreadPeak`]. A block that another
    // thread frees makes it drift, so only differences are meaningful.
    static THREAD_LIVE: Cell<isize> = const { Cell::new(0) };
    static THREAD_PEAK: Cell<isize> = const { Cell::new(0) };
}

/// Raises this thread's peak to cover `extra` bytes above the live count.
fn raise_thread_peak(extra: isize) {
    let _ = THREAD_LIVE.try_with(|live| {
        let _ = THREAD_PEAK.try_with(|peak| peak.set(peak.get().max(live.get() + extra)));
    });
}

fn count(delta: isize) {
    let _ = THREAD_LIVE.try_with(|live| live.set(live.get() + delta));
    raise_thread_peak(0);
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
            // Count the peak as a copy: the new block is live before the old
            // block is freed.
            raise_thread_peak(new_size as isize);
            count(new_size as isize - layout.size() as isize);
        }
        new_ptr
    }
}

/// Bytes currently allocated through [`CountingAllocator`], clamped at zero
/// because counts lost with exiting threads can bias it either way.
pub fn allocated_bytes() -> usize {
    ALLOCATED.load(Ordering::Relaxed).max(0) as usize
}

/// Measures the peak bytes that the current thread allocates through
/// [`CountingAllocator`] from [`Self::start`] on.
///
/// The count is per thread, so tests that run in parallel do not mix. It
/// counts only the calling thread, so the measured code must run on it, for
/// example on a current-thread Tokio runtime.
///
/// # Public Only for Internal Use:
///
/// This is not a public API and is for internal use only; see [API policy] for details.
///
/// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
#[derive(Debug)]
pub struct ThreadPeak {
    start: isize,
}

impl ThreadPeak {
    /// Resets this thread's peak to its live count and starts measuring.
    pub fn start() -> Self {
        let start = THREAD_LIVE.with(Cell::get);
        THREAD_PEAK.with(|peak| peak.set(start));
        Self { start }
    }

    /// Returns the peak bytes above the live count at [`Self::start`].
    pub fn peak_bytes(&self) -> usize {
        (THREAD_PEAK.with(Cell::get) - self.start).max(0) as usize
    }
}
