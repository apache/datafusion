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

//! A global allocator that counts the bytes currently allocated, so that tests
//! can compare them with what memory pools have reserved.

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
///
/// Install it with `#[global_allocator]` and read the count with
/// [`allocated_bytes`].
pub struct CountingAllocator;

/// Per-thread count is flushed to [`ALLOCATED`] once it moves this far, so
/// threads do not contend on one atomic for every allocation. The global count
/// is therefore off by up to `FLUSH_BYTES` per live thread. A Tokio runtime
/// that registers [`flush_thread_allocations`] as its `on_thread_stop` hook
/// flushes the rest when its threads stop; the unflushed count of any other
/// thread that exits is lost.
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
/// Register it as a Tokio runtime's `on_thread_stop` hook, which runs just
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
pub fn allocated_bytes() -> usize {
    ALLOCATED.load(Ordering::Relaxed).max(0) as usize
}
