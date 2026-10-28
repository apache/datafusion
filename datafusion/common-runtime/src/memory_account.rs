// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Charges the memory allocated while a future runs to a [`MemoryAccount`].
//!
//! DataFusion does not choose the global allocator, so the caller supplies a
//! per-thread count of bytes allocated minus bytes freed (see
//! [`set_thread_allocated_bytes`]), e.g. from jemalloc's `thread.allocatedp`
//! and `thread.deallocatedp`. Each poll of a [`MemoryTrackedFuture`] charges
//! its account with how far that count moved during the poll.
//!
//! [`MemoryAccountTracer`] carries the account into tasks spawned through
//! [`SpawnedTask`](crate::SpawnedTask) and [`JoinSet`](crate::JoinSet).
//! Tasks spawned with `tokio::spawn` directly and threads DataFusion does not
//! start are not charged.
//!
//! A free is charged to whichever account the freeing thread is charging, so
//! accounts that hand memory to each other are only exact in total.

use std::any::Any;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use futures::FutureExt;
use futures::future::BoxFuture;

use crate::JoinSetTracer;

/// Returns the bytes this thread has allocated minus the bytes it has freed,
/// since the thread started. Wrapping is expected, since threads free memory
/// other threads allocated. Called twice per poll, so it must be cheap.
pub type ThreadAllocatedBytesFn = fn() -> u64;

static THREAD_ALLOCATED_BYTES: OnceLock<ThreadAllocatedBytesFn> = OnceLock::new();

/// Set the per-thread counter that [`MemoryAccount`]s are charged from.
///
/// Until this is called, nothing is charged. Returns `Err` with the given
/// function if a counter was already set.
pub fn set_thread_allocated_bytes(
    thread_allocated_bytes: ThreadAllocatedBytesFn,
) -> Result<(), ThreadAllocatedBytesFn> {
    THREAD_ALLOCATED_BYTES.set(thread_allocated_bytes)
}

fn thread_allocated_bytes() -> Option<u64> {
    THREAD_ALLOCATED_BYTES.get().map(|f| f())
}

/// Net bytes allocated while charged to one owner, e.g. a query.
#[derive(Debug, Default)]
pub struct MemoryAccount {
    bytes: AtomicI64,
}

impl MemoryAccount {
    /// Bytes allocated minus bytes freed while charged to this account,
    /// including the current poll if this thread is charging it.
    pub fn bytes(&self) -> i64 {
        let settled = self.bytes.load(Ordering::Relaxed);
        let unsettled = CHARGING.with_borrow(|charging| match charging {
            Some(charging) if std::ptr::eq(Arc::as_ptr(&charging.account), self) => {
                thread_allocated_bytes()
                    .map_or(0, |now| now.wrapping_sub(charging.since) as i64)
            }
            _ => 0,
        });
        settled + unsettled
    }

    /// Add `bytes` allocated where no account was charged, e.g. on a thread
    /// another library started, and handed to this account's owner.
    pub fn charge(&self, bytes: i64) {
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

struct Charging {
    account: Arc<MemoryAccount>,
    /// [`thread_allocated_bytes`] when this thread started charging `account`.
    since: u64,
}

thread_local! {
    static CHARGING: RefCell<Option<Charging>> = const { RefCell::new(None) };
}

/// Make `next` the account this thread charges, settling the bytes allocated
/// since the last switch on the previous account, which is returned.
fn switch_account(next: Option<Arc<MemoryAccount>>) -> Option<Arc<MemoryAccount>> {
    let now = thread_allocated_bytes()?;
    let previous = CHARGING.with_borrow_mut(|charging| {
        std::mem::replace(
            charging,
            next.map(|account| Charging {
                account,
                since: now,
            }),
        )
    })?;
    previous
        .account
        .bytes
        .fetch_add(now.wrapping_sub(previous.since) as i64, Ordering::Relaxed);
    Some(previous.account)
}

/// The account this thread is charging, if any.
pub fn current_memory_account() -> Option<Arc<MemoryAccount>> {
    CHARGING.with_borrow(|charging| {
        charging
            .as_ref()
            .map(|charging| Arc::clone(&charging.account))
    })
}

/// Switches back to the previous account on drop, including when a poll panics.
struct RestoreAccount(Option<Arc<MemoryAccount>>);

impl RestoreAccount {
    fn charge(account: Arc<MemoryAccount>) -> Self {
        Self(switch_account(Some(account)))
    }
}

impl Drop for RestoreAccount {
    fn drop(&mut self) {
        switch_account(self.0.take());
    }
}

pin_project_lite::pin_project! {
    /// A future whose polls are charged to a [`MemoryAccount`].
    pub struct MemoryTrackedFuture<F> {
        account: Arc<MemoryAccount>,
        #[pin]
        future: F,
    }
}

impl<F> MemoryTrackedFuture<F> {
    /// Charge every poll of `future` to `account`.
    pub fn new(account: Arc<MemoryAccount>, future: F) -> Self {
        Self { account, future }
    }
}

impl<F: Future> Future for MemoryTrackedFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        let _restore = RestoreAccount::charge(Arc::clone(this.account));
        this.future.poll(cx)
    }
}

/// A [`JoinSetTracer`] that charges spawned tasks and blocking closures to the
/// account of the thread that spawned them.
///
/// Install it with [`set_join_set_tracer`](crate::set_join_set_tracer), or
/// call it from your own tracer.
#[derive(Debug, Default)]
pub struct MemoryAccountTracer;

impl JoinSetTracer for MemoryAccountTracer {
    fn trace_future(
        &self,
        fut: BoxFuture<'static, Box<dyn Any + Send>>,
    ) -> BoxFuture<'static, Box<dyn Any + Send>> {
        match current_memory_account() {
            Some(account) => MemoryTrackedFuture::new(account, fut).boxed(),
            None => fut,
        }
    }

    fn trace_block(
        &self,
        f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> {
        match current_memory_account() {
            Some(account) => Box::new(move || {
                let _restore = RestoreAccount::charge(account);
                f()
            }),
            None => f,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    thread_local! {
        static ALLOCATED: Cell<u64> = const { Cell::new(0) };
    }

    fn allocated() -> u64 {
        ALLOCATED.get()
    }

    fn allocate(bytes: u64) {
        ALLOCATED.set(ALLOCATED.get().wrapping_add(bytes));
    }

    fn free(bytes: u64) {
        ALLOCATED.set(ALLOCATED.get().wrapping_sub(bytes));
    }

    fn account() -> Arc<MemoryAccount> {
        let _ = set_thread_allocated_bytes(allocated);
        Arc::new(MemoryAccount::default())
    }

    #[tokio::test]
    async fn charges_only_polls_of_the_tracked_future() {
        let account = account();
        allocate(1000);
        MemoryTrackedFuture::new(Arc::clone(&account), async {
            allocate(100);
            tokio::task::yield_now().await;
            allocate(50);
            free(30);
        })
        .await;
        allocate(1000);

        assert_eq!(account.bytes(), 120);
    }

    #[tokio::test]
    async fn nested_futures_charge_their_own_accounts() {
        let outer = account();
        let inner = account();
        MemoryTrackedFuture::new(Arc::clone(&outer), async {
            allocate(10);
            MemoryTrackedFuture::new(Arc::clone(&inner), async {
                allocate(100);
                tokio::task::yield_now().await;
                allocate(100);
            })
            .await;
            allocate(10);
        })
        .await;

        assert_eq!(outer.bytes(), 20);
        assert_eq!(inner.bytes(), 200);
    }

    #[tokio::test]
    async fn bytes_include_the_current_poll() {
        let account = account();
        let during_poll = MemoryTrackedFuture::new(Arc::clone(&account), {
            let account = Arc::clone(&account);
            async move {
                allocate(64);
                account.bytes()
            }
        })
        .await;

        assert_eq!(during_poll, 64);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    // Spawn without `SpawnedTask`, which would apply the global tracer.
    #[expect(clippy::disallowed_methods)]
    async fn tracer_charges_spawned_work_to_the_spawner() {
        let account = account();
        MemoryTrackedFuture::new(Arc::clone(&account), async {
            let task = MemoryAccountTracer.trace_future(Box::pin(async {
                allocate(70);
                Box::new(()) as Box<dyn Any + Send>
            }));
            let block = MemoryAccountTracer.trace_block(Box::new(|| {
                allocate(30);
                Box::new(()) as Box<dyn Any + Send>
            }));
            tokio::spawn(task).await.unwrap();
            tokio::task::spawn_blocking(block).await.unwrap();
        })
        .await;

        assert_eq!(account.bytes(), 100);
    }

    #[tokio::test]
    // Spawn without `SpawnedTask`, which would apply the global tracer.
    #[expect(clippy::disallowed_methods)]
    async fn tracer_leaves_untracked_work_alone() {
        let _ = account();
        assert!(current_memory_account().is_none());
        let task = MemoryAccountTracer.trace_future(Box::pin(async {
            Box::new(current_memory_account().is_none()) as Box<dyn Any + Send>
        }));
        let untracked = tokio::spawn(task).await.unwrap();

        assert!(*untracked.downcast::<bool>().unwrap());
    }
}
