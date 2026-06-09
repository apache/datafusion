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
//! - A dedicated OS thread polls jemalloc's `stats.resident` at a fixed
//!   interval (see [`spawn_balance_poll`]) and writes
//!   `BALANCE = threshold - resident`. Per-allocation tracking refines
//!   the bank between ticks; the poll's job is to inject the
//!   allocator's slab-overhead delta into the bank before per-alloc
//!   tracking would discover it.
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

use tikv_jemalloc_ctl::{epoch, stats};

/// Per-thread drift settles into the global bank when `|drift|` crosses this.
/// 64 KB keeps per-thread error tight (≤1 MB on a 16-core box) while still
/// amortizing the atomic op across thousands of allocations.
const SETTLE_THRESHOLD: isize = 64 * 1024;

/// Runtime kill-switch. When `false`, every `track()` call short-circuits on
/// a single relaxed atomic load — same cost as no wrapper at all on every
/// path through every allocator op. Stays `false` until [`arm`] is called.
static ARMED: AtomicBool = AtomicBool::new(false);

/// Headroom remaining (bytes) before a stamped thread will panic.
/// `isize::MAX` until [`seed_balance`] runs — the guard is effectively
/// off until then.
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
/// on overdraft; everything else (i.e. gRPC server)
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

/// Overwrite the bank with `value`. Only `seed_balance` calls this.
fn set_balance(value: isize) {
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
            // Credits never fire the kill
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
            // Unstamped threads still debit the
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

/// Read the process's cgroup memory limit, if any. Returns `None` for
/// "no limit set" (cgroup v2 reports `max`; cgroup v1 reports a sentinel
/// value near `u64::MAX`).
pub fn cgroup_memory_max() -> Option<u64> {
    fn parse_v2(content: &str) -> Option<u64> {
        let trimmed = content.trim();
        if trimmed == "max" {
            None
        } else {
            trimmed.parse::<u64>().ok()
        }
    }

    // (1) Namespaced cgroup v2 mount.
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        if let Some(value) = parse_v2(&content) {
            return Some(value);
        }
    }
    // (2) Full-hierarchy cgroup v2: discover our own path. `/proc/self/cgroup`
    //     on v2 looks like `0::/user.slice/.../foo.service`.
    if let Ok(proc_cgroup) = std::fs::read_to_string("/proc/self/cgroup") {
        for line in proc_cgroup.lines() {
            if let Some(rest) = line.strip_prefix("0::") {
                let path = format!("/sys/fs/cgroup{}/memory.max", rest.trim_end());
                if let Ok(content) = std::fs::read_to_string(&path) {
                    if let Some(value) = parse_v2(&content) {
                        return Some(value);
                    }
                }
            }
        }
    }
    // (3) cgroup v1 fallback.
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

/// Spawn a dedicated OS thread that periodically resyncs `BALANCE` from
/// jemalloc's `stats.resident`. Sets `BALANCE = threshold - resident` on
/// every tick.
///
/// Performs an initial synchronous read before returning so the guard
/// is fully armed by the time the caller continues.
pub fn spawn_balance_poll(threshold_bytes: usize, interval: Duration) {
    let resident_mib = match stats::resident::mib() {
        Ok(m) => m,
        Err(e) => {
            log::error!(
                "OomGuard: cannot resolve jemalloc resident MIB ({e}); guard disarmed"
            );
            return;
        }
    };
    let epoch_mib = match epoch::mib() {
        Ok(m) => m,
        Err(e) => {
            log::error!(
                "OomGuard: cannot resolve jemalloc epoch MIB ({e}); guard disarmed"
            );
            return;
        }
    };

    // Initial sync so the bank starts in a meaningful state.
    let _ = epoch_mib.advance();
    if let Ok(resident) = resident_mib.read() {
        let initial = (threshold_bytes as isize).saturating_sub(resident as isize);
        set_balance(initial);
    }

    log::info!(
        "OomGuard: starting balance poll (threshold={threshold_bytes} bytes, interval={interval:?}, initial_balance={} bytes)",
        balance(),
    );

    std::thread::Builder::new()
        .name("oom-guard-poll".into())
        .spawn(move || {
            loop {
                std::thread::sleep(interval);
                let _ = epoch_mib.advance();
                if let Ok(resident) = resident_mib.read() {
                    let new_balance =
                        (threshold_bytes as isize).saturating_sub(resident as isize);
                    BALANCE.store(new_balance, Ordering::Relaxed);
                }
            }
        })
        .expect("failed to spawn oom-guard-poll thread");
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
