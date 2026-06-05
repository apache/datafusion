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

//! Allocator-driven memory accounting with per-context budgets.
//!
//! The bank ([`ACCOUNTS`]) holds one [`AtomicIsize`] account per stamped
//! `CONTEXT_ID`, each tracking its own remaining budget. Allocations debit
//! the current thread's account, deallocations credit it; below zero is an
//! overdraft. Threads with `CONTEXT_ID == 0` (main, the outer orchestration
//! runtime, blocking-pool hosts) are untracked and skip the hot path.
//!
//! Per-alloc bookkeeping accumulates in a thread-local `LOCAL_BALANCE`
//! drift counter; it settles into the account once `|drift|` crosses
//! [`SETTLE_THRESHOLD`] (64 KB), amortizing the `RwLock` read + atomic
//! op across thousands of allocations.
//!
//! [`account_balance`] reads the current thread's account; it lags reality
//! by up to one threshold's worth of un-settled drift per thread.
//!
//! # Enforcement
//!
//! An allocation that drives the bank negative on a stamped thread
//! (`CONTEXT_ID != 0`) panics with [`OverdraftPanic`] on the polling thread.
//! Drop-chain credits during unwind never re-panic — `track` only fires on
//! debits (`delta < 0`). Unstamped threads are silently skipped.
//!
//! Compiled in only when the `memory-accounting` feature is on.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::sync::{OnceLock, RwLock};

/// Net byte change at which a thread flushes its local count into the bank.
/// 64 KB chosen to keep per-thread drift tight (≤1 MB on a 16-core box) while
/// still settling rarely enough to make the bank's atomic op amortized-free.
const SETTLE_THRESHOLD: isize = 64 * 1024;

/// The bank: every account, keyed by context-id, valued by remaining budget.
/// Debits on alloc, credits on free, negative = overdraft. ctx-id 0 never
/// gets an entry — that's the "untracked thread" marker.
static ACCOUNTS: OnceLock<RwLock<HashMap<usize, AtomicIsize>>> = OnceLock::new();

/// Starting budget for any new account, set by [`set_default_budget`] and
/// inherited by per-file SLT contexts spawned after.
static DEFAULT_BUDGET: AtomicIsize = AtomicIsize::new(0);

fn accounts() -> &'static RwLock<HashMap<usize, AtomicIsize>> {
    ACCOUNTS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Run `f` against the current thread's account balance, or return `None`
/// if there isn't one — silently skipping the update is fine on the alloc
/// hot path.
fn with_current_balance<R>(op: impl FnOnce(&AtomicIsize) -> R) -> Option<R> {
    let ctx_id = CONTEXT_ID.with(|ctx| ctx.get());
    if ctx_id == 0 {
        return None;
    }
    // PERF: acquires an `RwLock` read on every settle. If it ever shows up
    // hot, stash a `&'static AtomicIsize` in a thread-local (set in
    // `set_thread_context_id`, backed by `Box::leak`) and skip the lookup.
    let accounts_lock = ACCOUNTS.get()?;
    let accounts = accounts_lock.read().ok()?;
    accounts.get(&ctx_id).map(op)
}

thread_local! {
    static LOCAL_BALANCE: Cell<isize> = const { Cell::new(0) };

    /// Account-id stamped onto worker threads via [`set_thread_context_id`].
    /// Zero = untracked thread; nothing to track, nothing to enforce.
    static CONTEXT_ID: Cell<usize> = const { Cell::new(0) };
}

/// Monotonic source of fresh context-ids. Starts at 1; the zero value is
/// reserved for "no per-file runtime" so callers can distinguish.
static CONTEXT_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Returns a fresh, never-before-used context-id. Call once per file in the
/// SLT binary and pass the result into the per-file runtime's
/// `on_thread_start` callback so every worker thread of that runtime shares
/// the same id.
pub fn next_context_id() -> usize {
    CONTEXT_ID_COUNTER.fetch_add(1, Ordering::Relaxed) + 1
}

/// Stamp the current thread with `id`. Intended for `on_thread_start`.
/// Creates the account if it doesn't already exist.
pub fn set_thread_context_id(id: usize) {
    if id == 0 {
        CONTEXT_ID.with(|ctx| ctx.set(0));
        return;
    }
    // Insert under the write lock *before* stamping the thread. A HashMap
    // resize allocates → recurses through `track` → `with_current_account`,
    // which sees `CONTEXT_ID == 0` and bails out instead of trying to
    // read-lock the map we're holding for writing on the same thread.
    {
        let accounts_lock = accounts();
        let mut accounts = accounts_lock
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        accounts
            .entry(id)
            .or_insert_with(|| AtomicIsize::new(DEFAULT_BUDGET.load(Ordering::Relaxed)));
    }
    CONTEXT_ID.with(|ctx| ctx.set(id));
}

/// Current thread's context-id, or 0 if none has been set.
pub fn current_context_id() -> usize {
    CONTEXT_ID.with(|ctx| ctx.get())
}

/// Payload attached to allocator-induced panics. Catch with:
///
/// ```ignore
/// match std::panic::catch_unwind(|| { /* ... */ }) {
///     Err(e) if e.is::<OverdraftPanic>() => { /* it was an overdraft */ }
///     ...
/// }
/// ```
#[derive(Debug, Clone)]
pub struct OverdraftPanic {
    /// Account balance at the moment the panic fired (negative — that's the point).
    pub account_balance: isize,
}

/// Set the default budget new accounts will be created with. Existing
/// accounts are untouched.
pub fn set_default_budget(value: isize) {
    DEFAULT_BUDGET.store(value, Ordering::Relaxed);
}

/// Current default budget — what a fresh account starts at and what
/// [`reset_account_to_default`] restores to.
pub fn default_budget() -> isize {
    DEFAULT_BUDGET.load(Ordering::Relaxed)
}

/// Restore the current thread's account to [`default_budget`]. Used by the
/// SLT runner after catching an [`OverdraftPanic`] so the next statement
/// starts clean — otherwise the bank stays negative and every subsequent
/// allocation refires, which is unsafe (allocator hooks must not panic
/// repeatedly within a single thread).
pub fn reset_account_to_default() {
    set_account_balance(default_budget());
}

/// Set the current thread's account balance to `value`. No-op on untracked
/// threads (`CONTEXT_ID == 0`).
pub fn set_account_balance(value: isize) {
    let _ = with_current_balance(|bal| bal.store(value, Ordering::Relaxed));
}

/// Cross-module config for DataFusion's voluntary `MemoryPool` limit, set
/// from the SLT binary's CLI and read by test_context when building each
/// per-file `RuntimeEnv`. Zero means "use the default `UnboundedMemoryPool`".
static MEMORY_TRACKER_LIMIT: AtomicUsize = AtomicUsize::new(0);

/// Set the size (in bytes) the per-file `MemoryPool` should be built with.
/// Zero (the default) leaves the existing `UnboundedMemoryPool` behavior.
pub fn set_memory_tracker_limit(bytes: usize) {
    MEMORY_TRACKER_LIMIT.store(bytes, Ordering::Relaxed);
}

/// Current `MemoryPool` limit configured via [`set_memory_tracker_limit`].
pub fn memory_tracker_limit() -> usize {
    MEMORY_TRACKER_LIMIT.load(Ordering::Relaxed)
}

/// Current account balance. Negative = overdraft. `0` if untracked.
pub fn account_balance() -> isize {
    with_current_balance(|bal| bal.load(Ordering::Relaxed)).unwrap_or(0)
}

/// Current thread's local balance — not yet reflected in the global bank.
/// Always in `(-SETTLE_THRESHOLD, +SETTLE_THRESHOLD)`. Sign matches the bank:
/// negative on a thread that's net-allocated, positive on one that's net-freed.
pub fn local_balance() -> isize {
    LOCAL_BALANCE.with(|loc_bal| loc_bal.get())
}

/// Force the current thread to flush its local count into its context bank.
/// No-op on untracked threads (`CONTEXT_ID == 0`).
pub fn settle_thread_local() {
    if CONTEXT_ID.with(|ctx| ctx.get()) == 0 {
        return;
    }
    let _ = LOCAL_BALANCE.try_with(|loc_bal| {
        let drift = loc_bal.replace(0);
        if drift != 0 {
            let _ = with_current_balance(|bal| bal.fetch_add(drift, Ordering::Relaxed));
        }
    });
}

/// Record a delta into the current thread's account: settle local drift into
/// the bank when it crosses `±SETTLE_THRESHOLD`, fire the kill panic on a
/// debit that leaves the account negative.
#[inline(always)]
fn track(delta: isize) {
    if CONTEXT_ID.with(|ctx| ctx.get()) == 0 {
        return;
    }
    let _ = LOCAL_BALANCE.try_with(|loc_bal| {
        let drift = loc_bal.get() + delta;
        // 99% case: drift fits — accumulate locally and bail.
        if -SETTLE_THRESHOLD < drift && drift < SETTLE_THRESHOLD {
            loc_bal.set(drift);
            return;
        }
        // Drop the read lock *before* maybe_kill — the panic allocates,
        // recurses through track, and would self-deadlock on std::sync::RwLock.
        let new_bal = with_current_balance(|bal| {
            bal.fetch_add(drift, Ordering::Relaxed).wrapping_add(drift)
        });
        loc_bal.set(0);
        // Only debits fire the kill — credits run inside Drop chains during
        // unwinding, where a panic would double-fault and abort the process.
        if delta >= 0 {
            return;
        }
        let Some(new_bal) = new_bal else { return };
        if new_bal >= 0 {
            return;
        }
        // Skip if we're already unwinding — `panic_any` boxes the payload,
        // which allocates, which re-enters `track`; without this gate the
        // second debit would fire a nested panic and abort the process.
        if std::thread::panicking() {
            return;
        }
        std::panic::panic_any(OverdraftPanic {
            account_balance: new_bal,
        });
    });
}

/// `GlobalAlloc` wrapper that counts bytes against a thread-local + global bank.
///
/// Forwards every operation unchanged to the inner allocator; the bookkeeping
/// is a thread-local update on the fast path plus an amortized atomic settle.
pub struct AccountingAllocator<A: GlobalAlloc = System> {
    inner: A,
}

impl<A: GlobalAlloc> AccountingAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

impl AccountingAllocator<System> {
    /// Convenience constructor for the typical `System`-backed case.
    pub const fn system() -> Self {
        Self { inner: System }
    }
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for AccountingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // Account BEFORE the inner alloc. If we panicked AFTER `inner.alloc`
        // succeeded, the bytes are physically allocated but no caller ever
        // sees the pointer → unwind leaks the very bytes that pushed us
        // over the budget — the opposite of what the kill panic is for.
        let delta = -(layout.size() as isize);
        track(delta);
        // SAFETY: layout is forwarded unchanged.
        let ptr = unsafe { self.inner.alloc(layout) };
        if ptr.is_null() {
            // Allocator refused — refund so the bank matches reality.
            track(-delta);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: caller upholds GlobalAlloc invariants; we forward unchanged.
        unsafe { self.inner.dealloc(ptr, layout) };
        // Credit only; `track()` short-circuits on `delta >= 0` and never
        // panics, so ordering relative to `inner.dealloc` doesn't matter.
        track(layout.size() as isize);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // Same panic-then-leak hazard as `alloc`; account first.
        let delta = -(layout.size() as isize);
        track(delta);
        // SAFETY: layout is forwarded unchanged.
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        if ptr.is_null() {
            track(-delta);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Account BEFORE the inner realloc so a kill panic doesn't strand the
        // caller with a freed `ptr`. `inner.realloc` frees `ptr` on success;
        // if we panicked after that, the caller's `Vec`-or-similar would
        // still hold the old pointer and double-free on unwind (glibc
        // "double free or corruption (out)" + SIGABRT).
        let delta = layout.size() as isize - new_size as isize;
        track(delta);
        // SAFETY: caller upholds GlobalAlloc invariants; we forward unchanged.
        let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };
        if new_ptr.is_null() {
            // Allocator refused — refund so the bank matches reality.
            track(-delta);
        }
        new_ptr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[global_allocator]
    static GLOBAL: AccountingAllocator = AccountingAllocator::system();

    /// Each test runs on its own thread (cargo-test parallelism) and stamps a
    /// fresh context-id, so per-context isolation makes them naturally
    /// independent — no shared mutex required.
    fn enter_fresh_context() {
        set_thread_context_id(next_context_id());
    }

    #[test]
    fn alloc_debits_and_free_credits_account() {
        enter_fresh_context();
        // Bump budget well above the alloc + this thread's own background
        // drift so the test's own activity can't accidentally overdraw.
        set_account_balance(10_000_000);
        settle_thread_local();
        let before = account_balance();

        let buf: Vec<u8> = vec![0u8; 8192];
        settle_thread_local();
        let mid = account_balance();
        // Alloc debited the account → mid should be at least 8192 below before.
        assert!(
            before - mid >= 8192,
            "alloc didn't debit: before={before} mid={mid}"
        );

        drop(buf);
        settle_thread_local();
        let after = account_balance();
        // Free credited the account → after should be at least 8192 above mid.
        assert!(
            after - mid >= 8192,
            "free didn't credit: mid={mid} after={after}"
        );
    }

    #[test]
    fn set_account_balance_sticks() {
        enter_fresh_context();
        set_account_balance(1_000_000);
        // Balance drifts a little from this thread's own allocator activity
        // between the set and the read, so we expect at-or-below the set value.
        let bal = account_balance();
        assert!(
            (900_000..=1_000_000).contains(&bal),
            "set_account_balance didn't stick: bal={bal}"
        );
    }

    #[test]
    fn overdraft_on_stamped_thread_panics() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        enter_fresh_context();
        set_account_balance(1024);

        let result = catch_unwind(AssertUnwindSafe(|| {
            // Alloc large enough to cross SETTLE_THRESHOLD in one shot — the
            // settle drives the bank negative on a stamped thread, which now
            // unconditionally panics.
            let _buf: Vec<u8> = vec![0u8; SETTLE_THRESHOLD as usize + 4096];
            unreachable!("alloc should have panicked");
        }));

        let payload = result.expect_err("alloc should have panicked");
        let overdraft = payload
            .downcast_ref::<OverdraftPanic>()
            .expect("panic payload should be OverdraftPanic");
        assert!(
            overdraft.account_balance < 0,
            "payload should report negative balance; got {}",
            overdraft.account_balance
        );
    }

    #[test]
    fn threshold_settlement_flushes_to_account() {
        enter_fresh_context();
        // Bump budget — the settle on threshold crossing now panics on
        // a stamped thread if it goes negative. We just want to observe the
        // flush mechanism here, not the kill.
        set_account_balance(10_000_000);
        settle_thread_local();
        let before = account_balance();

        let buf: Vec<u8> = vec![0u8; SETTLE_THRESHOLD as usize + 1024];
        // Crossing the threshold auto-settles; account balance should have
        // dropped by at least SETTLE_THRESHOLD without us calling
        // settle_thread_local.
        let after_alloc = account_balance();
        assert!(
            before - after_alloc >= SETTLE_THRESHOLD,
            "balance didn't auto-settle on threshold crossing: \
             before={before} after_alloc={after_alloc}"
        );
        drop(buf);
    }
}
