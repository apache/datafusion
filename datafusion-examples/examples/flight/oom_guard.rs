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

//! `OomGuard` — `GlobalAlloc` wrapper that converts a process-wide OOM
//! into a per-query panic *before* the kernel SIGKILLs the process.
//!
//! Motivation: DataFusion's [`MemoryPool`] is voluntary — operators
//! that don't call `try_grow` (e.g. in-flight batches, `arrow-row`
//! decode buffers, Arrow kernel scratch space) allocate freely past
//! the declared budget. On a process with a hard memory ceiling
//! (k8s pod cgroup, ulimit, etc.) the result is an OS-level kill,
//! which takes down every concurrent query running on the process.
//!
//! `OomGuard` solves this at the allocator layer:
//!
//! - A single global `AtomicIsize BALANCE` represents bytes of headroom
//!   remaining before a threshold. Every thread's allocations debit it
//!   and frees credit it.
//! - A periodic poll overwrites `BALANCE` with `threshold - resident`,
//!   resyncing the bank to ground truth (read from `/proc/self/statm`)
//!   so per-alloc drift can't accumulate forever.
//! - When the bank goes negative *on a stamped thread* (typically a
//!   query worker), `alloc` returns NULL. The `alloc_error_hook` then
//!   originates a `panic_any(OomGuardPanic)` from a clean safe-Rust
//!   frame outside the allocator, which unwinds only the offending
//!   query task. The rest of the process survives.
//!
//! Only threads that have called [`stamp_current_thread`] can panic —
//! the poll task, gRPC server task, etc. are exempt.
//!
//! The wrapper costs one relaxed atomic load per allocation when
//! [`disarm`]ed (which is the default). [`arm`] flips the gate.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::time::Duration;

/// Per-thread drift settles into the global bank when `|drift|` crosses this.
/// 64 KB keeps per-thread error tight (≤1 MB on a 16-core box) while still
/// amortizing the atomic op across thousands of allocations.
const SETTLE_THRESHOLD: isize = 64 * 1024;

/// Runtime kill-switch. When `false`, every `track()` call short-circuits on
/// a single relaxed atomic load — same cost as no wrapper at all on every
/// path through every allocator op. Stays `false` until [`arm`] is called.
static ARMED: AtomicBool = AtomicBool::new(false);

/// Headroom remaining (bytes) before a stamped thread will panic.
/// `isize::MAX` until the first poll runs — the guard is effectively off
/// until then.
static BALANCE: AtomicIsize = AtomicIsize::new(isize::MAX);

thread_local! {
    static LOCAL_DRIFT: Cell<isize> = const { Cell::new(0) };
    static STAMPED: Cell<bool> = const { Cell::new(false) };
    /// Set by `track()` just before signalling NULL-PTR back to the
    /// `GlobalAlloc` caller. Read-and-cleared by `take_kill_pending()`
    /// from inside the `alloc_error_hook` so the hook knows whether
    /// to `panic_any(OomGuardPanic)` (our kill) or fall back to
    /// `process::abort()` (a genuine allocator OOM the runtime asked
    /// the hook to handle).
    static KILL_PENDING: Cell<bool> = const { Cell::new(false) };
}

/// Stamp the current thread as a query worker. Only stamped threads panic
/// on overdraft; everything else (poll task, gRPC server, control plane)
/// is exempt. Intended for `on_thread_start` of a per-query / per-task
/// tokio runtime.
///
/// No effect until [`arm`] flips the runtime gate.
pub fn stamp_current_thread() {
    STAMPED.with(|stamped| stamped.set(true));
}

/// Arm the guard. Until this is called, every `track()` short-circuits on a
/// single relaxed atomic load and the wrapper costs effectively nothing.
pub fn arm() {
    ARMED.store(true, Ordering::Relaxed);
}

/// Disarm the guard. `track()` reverts to the no-op fast path.
#[allow(dead_code)] // public API; unused by the flight example itself.
pub fn disarm() {
    ARMED.store(false, Ordering::Relaxed);
}

/// Whether the guard is currently armed.
#[allow(dead_code)] // public API; unused by the flight example itself.
pub fn is_armed() -> bool {
    ARMED.load(Ordering::Relaxed)
}

/// Overwrite the bank with `value`. The poll task uses this to resync from
/// real RSS each tick.
pub fn set_balance(value: isize) {
    BALANCE.store(value, Ordering::Relaxed);
}

/// Current bank balance — bytes remaining before overdraft. Negative means
/// the next stamped allocation will panic.
pub fn balance() -> isize {
    BALANCE.load(Ordering::Relaxed)
}

/// Payload attached to overdraft panics. Fired from the `alloc_error_hook`
/// installed by the binary, which is the only safe seam to originate the
/// panic from — calling `panic_any` from inside the `unsafe impl GlobalAlloc`
/// corrupts the unwind ABI.
#[derive(Debug, Clone)]
pub struct OomGuardPanic {
    /// Balance at the moment the panic fired (negative — that's the point).
    pub balance: isize,
}

/// Read-and-clear the current thread's `KILL_PENDING` flag. Called by the
/// `alloc_error_hook` to distinguish OomGuard-induced NULL returns (we
/// want `panic_any`) from genuine allocator OOM (the hook should fall
/// back to `process::abort()`).
#[must_use]
pub fn take_kill_pending() -> bool {
    KILL_PENDING.with(|f| f.replace(false))
}

/// Returns `true` when the caller's `GlobalAlloc` op should fail with
/// `NULL_PTR` instead of returning a real pointer. The caller is
/// responsible for refunding the bank if it takes the abort path.
#[inline(always)]
#[must_use]
fn track(delta: isize) -> bool {
    // Fast path when disarmed: one relaxed load and we're out. Compiles to
    // a single `mov` on x86 with no fence. This is the path every alloc in
    // every binary takes by default; performance must be indistinguishable
    // from the inner allocator.
    if !ARMED.load(Ordering::Relaxed) {
        return false;
    }
    LOCAL_DRIFT
        .try_with(|drift_cell| {
            let drift = drift_cell.get() + delta;
            // Hot path: drift fits — accumulate locally and bail.
            if -SETTLE_THRESHOLD < drift && drift < SETTLE_THRESHOLD {
                drift_cell.set(drift);
                return false;
            }
            let new_balance = BALANCE
                .fetch_add(drift, Ordering::Relaxed)
                .wrapping_add(drift);
            drift_cell.set(0);
            // Credits never fire the kill — they happen during Drop chains
            // in unwind, where aborting an alloc inside a Drop invites UB.
            if delta >= 0 {
                return false;
            }
            if new_balance >= 0 {
                return false;
            }
            // Returning null to a Drop body inside an ongoing unwind is
            // unsafe — caller may not be coded to handle the failure path.
            if std::thread::panicking() {
                return false;
            }
            // Unstamped threads (poll task, control plane) still debit the
            // bank above so its value stays honest, but they're exempt from
            // the kill.
            if !STAMPED.with(|stamped| stamped.get()) {
                return false;
            }
            // Flag the upcoming `alloc_error_hook` invocation as ours so
            // it fires `panic_any` instead of falling back to abort.
            KILL_PENDING.with(|f| f.set(true));
            true
        })
        .unwrap_or(false)
}

/// Read the process's resident set size (anon + file pages) in bytes,
/// straight from `/proc/self/statm`. Avoids any allocator-specific
/// dependency (jemalloc, mimalloc, etc.) — the cost is a small file
/// read on each poll tick.
fn read_resident_bytes() -> Option<u64> {
    let content = std::fs::read_to_string("/proc/self/statm").ok()?;
    let mut parts = content.split_ascii_whitespace();
    // Fields: size resident shared text lib data dt (all in pages)
    let _size = parts.next()?;
    let resident_pages: u64 = parts.next()?.parse().ok()?;
    // Page size — `sysconf(_SC_PAGESIZE)` would be nicer but adds a
    // libc dep; assume 4 KiB on the platforms this example runs on.
    Some(resident_pages * 4096)
}

/// Read the process's cgroup memory limit, if any. Returns `None` for
/// "no limit set" (cgroup v2 reports `max`; cgroup v1 reports a sentinel
/// value near `u64::MAX`).
pub fn cgroup_memory_max() -> Option<u64> {
    // cgroup v2 (modern Linux distros, k8s nodes).
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let trimmed = content.trim();
        if trimmed != "max" {
            if let Ok(value) = trimmed.parse::<u64>() {
                return Some(value);
            }
        }
    }
    // cgroup v1 fallback.
    if let Ok(content) =
        std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    {
        if let Ok(value) = content.trim().parse::<u64>() {
            // Kernels report a near-u64::MAX sentinel for "no limit".
            if value < u64::MAX / 2 {
                return Some(value);
            }
        }
    }
    None
}

/// Spawn a tokio task that periodically resyncs the bank from real RSS.
/// Between ticks, per-alloc tracking drifts could accumulate; this task
/// keeps it in line with the kernel's ground truth.
pub fn spawn_balance_poll(threshold_bytes: usize, interval: Duration) {
    log::info!(
        "OomGuard: starting balance poll (threshold={} bytes, interval={:?})",
        threshold_bytes,
        interval
    );

    tokio::spawn(async move {
        let mut timer =
            tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        // If tokio is busy and we miss a tick, don't hammer /proc with a
        // backlog — just skip to the next.
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            if let Some(resident) = read_resident_bytes() {
                let new_balance =
                    (threshold_bytes as isize).saturating_sub(resident as isize);
                set_balance(new_balance);
            }
        }
    });
}

/// `GlobalAlloc` wrapper. Forwards every op unchanged to `inner`; the
/// accounting is a thread-local update + amortized atomic settle.
pub struct OomGuard<A: GlobalAlloc> {
    inner: A,
}

impl<A: GlobalAlloc> OomGuard<A> {
    /// Wrap `inner`. `const` so it can be assigned to a `#[global_allocator]` static.
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for OomGuard<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let delta = -(layout.size() as isize);
        if track(delta) {
            // Caller will get null, call `handle_alloc_error`, and abort cleanly.
            // Refund the accounting since no allocation actually happened.
            let _ = track(-delta);
            return std::ptr::null_mut();
        }
        // SAFETY: layout is forwarded unchanged.
        let ptr = unsafe { self.inner.alloc(layout) };
        if ptr.is_null() {
            let _ = track(-delta);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: caller upholds GlobalAlloc invariants; forwarded unchanged.
        unsafe { self.inner.dealloc(ptr, layout) };
        // Credit only; `track()` short-circuits on `delta >= 0` so this can
        // never request an abort.
        let _ = track(layout.size() as isize);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let delta = -(layout.size() as isize);
        if track(delta) {
            let _ = track(-delta);
            return std::ptr::null_mut();
        }
        // SAFETY: layout is forwarded unchanged.
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        if ptr.is_null() {
            let _ = track(-delta);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let delta = layout.size() as isize - new_size as isize;
        if track(delta) {
            // Returning null is the documented way to signal realloc failure;
            // the caller's old `ptr` remains valid (we never called
            // `inner.realloc`), so no double-free risk.
            let _ = track(-delta);
            return std::ptr::null_mut();
        }
        // SAFETY: caller upholds GlobalAlloc invariants; forwarded unchanged.
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };
        if new_ptr.is_null() {
            let _ = track(-delta);
        }
        new_ptr
    }
}
