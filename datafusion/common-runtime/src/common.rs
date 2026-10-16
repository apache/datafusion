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

use std::{
    any::Any,
    fmt,
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, PoisonError, TryLockError},
    task::{Context, Poll, ready},
};

use futures::future::BoxFuture;
use tokio::runtime::Handle;
use tokio::task::{JoinError, JoinHandle};

use crate::trace_utils::{trace_block, trace_future};

/// Helper that  provides a simple API to spawn a single task and join it.
/// Provides guarantees of aborting on `Drop` to keep it cancel-safe.
/// Note that if the task was spawned with `spawn_blocking`, it will only be
/// aborted if it hasn't started yet.
///
/// Technically, it's just a wrapper of a `JoinHandle` overriding drop, which
/// for tasks spawned with [`Self::spawn_reclaimable`] also drops the future.
#[derive(Debug)]
pub struct SpawnedTask<R> {
    inner: JoinHandle<R>,
    /// Set for tasks spawned with [`Self::spawn_reclaimable`]
    reclaim: Option<ReclaimHandle<R>>,
}

impl<R: 'static> SpawnedTask<R> {
    pub fn spawn<T>(task: T) -> Self
    where
        T: Future<Output = R>,
        T: Send + 'static,
        R: Send,
    {
        // Ok to use spawn here as SpawnedTask handles aborting/cancelling the task on Drop
        #[expect(clippy::disallowed_methods)]
        let inner = tokio::task::spawn(trace_future(task));
        Self {
            inner,
            reclaim: None,
        }
    }

    /// Like [`Self::spawn`], but dropping the handle also drops the task's
    /// future right away, on the dropping thread, so everything the task owns
    /// is released by the time `drop` returns. A task that a worker is polling
    /// at that moment is aborted as usual, and its future is dropped when that
    /// poll returns.
    ///
    /// The future's destructor then runs inside the task's runtime but outside
    /// the task itself, where [`tokio::task::try_id`] returns `None` or the ID
    /// of the task that dropped the handle. Use this for futures whose
    /// destructors do not depend on the task they run in, such as tasks that
    /// drive a plan's streams. A [`JoinSetTracer`](crate::JoinSetTracer) wraps
    /// the task, not the future, so the tracer is still dropped inside it.
    pub fn spawn_reclaimable<T>(task: T) -> Self
    where
        T: Future<Output = R>,
        T: Send + 'static,
        R: Send,
    {
        let slot = TaskSlot::new(Box::pin(task));
        // Ok to use spawn here as SpawnedTask handles aborting/cancelling the task on Drop
        #[expect(clippy::disallowed_methods)]
        let inner = tokio::task::spawn(trace_future(SlotFuture(Arc::clone(&slot))));
        let reclaim = Some(ReclaimHandle::new(slot, Handle::current()));
        Self { inner, reclaim }
    }

    pub fn spawn_blocking<T>(task: T) -> Self
    where
        T: FnOnce() -> R,
        T: Send + 'static,
        R: Send,
    {
        // Ok to use spawn_blocking here as SpawnedTask handles aborting/cancelling the task on Drop
        #[expect(clippy::disallowed_methods)]
        let inner = tokio::task::spawn_blocking(trace_block(task));
        Self {
            inner,
            reclaim: None,
        }
    }

    /// Joins the task, returning the result of join (`Result<R, JoinError>`).
    /// Same as awaiting the spawned task, but left for backwards compatibility.
    pub async fn join(self) -> Result<R, JoinError> {
        self.await
    }

    /// Joins the task and unwinds the panic if it happens.
    pub async fn join_unwind(mut self) -> Result<R, JoinError> {
        self.join_unwind_mut().await
    }

    /// Joins the task using a mutable reference and unwinds the panic if it happens.
    ///
    /// This method is similar to [`join_unwind`](Self::join_unwind), but takes a mutable
    /// reference instead of consuming `self`. This allows the `SpawnedTask` to remain
    /// usable after the call.
    ///
    /// If called multiple times on the same task:
    /// - If the task is still running, it will continue waiting for completion
    /// - If the task has already completed successfully, subsequent calls will
    ///   continue to return the same `JoinError` indicating the task is finished
    /// - If the task panicked, the first call will resume the panic, and the
    ///   program will not reach subsequent calls
    pub async fn join_unwind_mut(&mut self) -> Result<R, JoinError> {
        self.await.map_err(|e| {
            // `JoinError` can be caused either by panic or cancellation. We have to handle panics:
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            } else {
                log::warn!("SpawnedTask was polled during shutdown");
                e
            }
        })
    }
}

impl<R> Future for SpawnedTask<R> {
    type Output = Result<R, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

impl<R> Drop for SpawnedTask<R> {
    fn drop(&mut self) {
        // Before aborting, so a worker cannot pick the cancelled task up and drop the future itself
        if let Some(reclaim) = &self.reclaim {
            reclaim.reclaim();
        }
        self.inner.abort();
    }
}

/// The future of a reclaimable task, shared by the task and its handle so
/// that whichever is done with it first drops it
pub(crate) struct TaskSlot<R>(Mutex<SlotState<R>>);

struct SlotState<R> {
    future: Option<BoxFuture<'static, R>>,
    /// Panic from the future's destructor when its handle dropped it, raised
    /// again when tokio drops the task so that joining reports it as tokio would
    destructor_panic: Option<Box<dyn Any + Send>>,
}

impl<R> TaskSlot<R> {
    pub(crate) fn new(future: BoxFuture<'static, R>) -> Arc<Self> {
        Arc::new(Self(Mutex::new(SlotState {
            future: Some(future),
            destructor_panic: None,
        })))
    }

    fn lock(&self) -> MutexGuard<'_, SlotState<R>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn try_lock(&self) -> Option<MutexGuard<'_, SlotState<R>>> {
        match self.0.try_lock() {
            Ok(state) => Some(state),
            Err(TryLockError::Poisoned(state)) => Some(state.into_inner()),
            Err(TryLockError::WouldBlock) => None,
        }
    }
}

impl<R> fmt::Debug for TaskSlot<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskSlot").finish_non_exhaustive()
    }
}

/// The future tokio runs for a task with a [`TaskSlot`]
pub(crate) struct SlotFuture<R>(pub(crate) Arc<TaskSlot<R>>);

impl<R> Future for SlotFuture<R> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<R> {
        // The handle is taking the future, and aborts the task right after
        let Some(mut state) = self.0.try_lock() else {
            return Poll::Pending;
        };
        // Reclaimed, and the task aborted
        let Some(future) = state.future.as_mut() else {
            return Poll::Pending;
        };
        let output = ready!(future.as_mut().poll(cx));
        let finished = state.future.take();
        drop(state);
        drop(finished);
        Poll::Ready(output)
    }
}

impl<R> Drop for SlotFuture<R> {
    fn drop(&mut self) {
        // The handle locks the slot only to take the future or record its
        // destructor's panic, never while that destructor runs
        let mut state = self.0.lock();
        let future = state.future.take();
        let destructor_panic = state.destructor_panic.take();
        drop(state);
        drop(future);
        if let Some(panic) = destructor_panic
            && !std::thread::panicking()
        {
            std::panic::resume_unwind(panic);
        }
    }
}

/// The handle side of a [`TaskSlot`]
#[derive(Debug)]
pub(crate) struct ReclaimHandle<R> {
    slot: Arc<TaskSlot<R>>,
    runtime: Handle,
}

impl<R> ReclaimHandle<R> {
    pub(crate) fn new(slot: Arc<TaskSlot<R>>, runtime: Handle) -> Self {
        Self { slot, runtime }
    }

    /// Drops the task's future on this thread, unless a worker is polling it,
    /// in which case the task drops it once aborted and that poll returns.
    pub(crate) fn reclaim(&self) {
        // A panicking destructor would abort the process while this thread unwinds
        if std::thread::panicking() {
            return;
        }
        let Some(future) = self
            .slot
            .try_lock()
            .and_then(|mut state| state.future.take())
        else {
            return;
        };
        // Tokio drops a cancelled task's future inside its runtime, and some
        // destructors need it (timers, spawning)
        let _runtime = self.runtime.enter();
        // Recorded before the caller aborts the task, whose drop raises it again
        if let Err(panic) =
            std::panic::catch_unwind(AssertUnwindSafe(move || drop(future)))
        {
            self.slot.lock().destructor_panic = Some(panic);
        }
    }

    /// Waits until no worker is polling the task
    #[cfg(test)]
    pub(crate) fn wait_until_idle(&self) {
        while self.slot.0.try_lock().is_err() {
            std::thread::yield_now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::future::{Pending, pending};

    use tokio::{runtime::Runtime, sync::oneshot};

    #[tokio::test]
    async fn runtime_shutdown() {
        let rt = Runtime::new().unwrap();
        #[expect(clippy::async_yields_async)]
        let task = rt
            .spawn(async {
                SpawnedTask::spawn(async {
                    let fut: Pending<()> = pending();
                    fut.await;
                    unreachable!("should never return");
                })
            })
            .await
            .unwrap();

        // caller shutdown their DF runtime (e.g. timeout, error in caller, etc)
        rt.shutdown_background();

        // race condition
        // poll occurs during shutdown (buffered stream poll calls, etc)
        assert!(matches!(
            task.join_unwind().await,
            Err(e) if e.is_cancelled()
        ));
    }

    #[tokio::test]
    #[should_panic(expected = "foo")]
    async fn panic_resume() {
        // this should panic w/o an `unwrap`
        SpawnedTask::spawn(async { panic!("foo") })
            .join_unwind()
            .await
            .ok();
    }

    #[tokio::test]
    async fn cancel_not_started_task() {
        let (sender, receiver) = oneshot::channel::<i32>();
        let task = SpawnedTask::spawn(async {
            // Shouldn't be reached.
            sender.send(42).unwrap();
        });

        drop(task);

        // If the task was cancelled, the sender was also dropped,
        // and awaiting the receiver should result in an error.
        assert!(receiver.await.is_err());
    }

    #[tokio::test]
    async fn cancel_ongoing_task() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let task = SpawnedTask::spawn(async move {
            sender.send(1).await.unwrap();
            // This line will never be reached because the channel has a buffer
            // of 1.
            sender.send(2).await.unwrap();
        });
        // Let the task start.
        assert_eq!(receiver.recv().await.unwrap(), 1);
        drop(task);

        // The sender was dropped so we receive `None`.
        assert!(receiver.recv().await.is_none());
    }

    fn wait_until_idle<R>(task: &SpawnedTask<R>) {
        task.reclaim.as_ref().unwrap().wait_until_idle();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drop_releases_idle_task_in_place() {
        let owned = Arc::new(());
        let released = Arc::downgrade(&owned);
        let (started_tx, started_rx) = oneshot::channel();
        let task = SpawnedTask::spawn_reclaimable(async move {
            started_tx.send(()).unwrap();
            pending::<()>().await;
            drop(owned);
        });
        started_rx.await.unwrap();
        wait_until_idle(&task);

        drop(task);
        assert_eq!(released.strong_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drop_during_poll_releases_after_the_poll() {
        let owned = Arc::new(());
        let released = Arc::downgrade(&owned);
        let (polling_tx, polling_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();
        let task = SpawnedTask::spawn_reclaimable(async move {
            polling_tx.send(()).unwrap();
            // Keeps the worker inside this poll until the test resumes it
            resume_rx.recv().unwrap();
            pending::<()>().await;
            drop(owned);
        });
        polling_rx.recv().unwrap();

        drop(task);
        assert_eq!(released.strong_count(), 1);

        resume_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while released.strong_count() > 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    type DropContext = (std::thread::ThreadId, bool, Option<tokio::task::Id>);

    struct RecordDropContext(std::sync::mpsc::Sender<DropContext>);

    impl Drop for RecordDropContext {
        fn drop(&mut self) {
            let in_runtime = Handle::try_current().is_ok();
            let context = (
                std::thread::current().id(),
                in_runtime,
                tokio::task::try_id(),
            );
            self.0.send(context).unwrap();
        }
    }

    #[tokio::test]
    async fn spawn_drops_its_future_inside_the_task() {
        let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
        let record = RecordDropContext(dropped_tx);
        let (id_tx, id_rx) = oneshot::channel();
        let task = SpawnedTask::spawn(async move {
            let _record = record;
            id_tx.send(tokio::task::id()).unwrap();
            pending::<()>().await;
        });
        let id = id_rx.await.unwrap();

        drop(task);
        tokio::task::yield_now().await;
        assert_eq!(dropped_rx.recv().unwrap().2, Some(id));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reclaimed_future_drops_in_the_runtime_outside_its_task() {
        let (dropped_tx, dropped_rx) = std::sync::mpsc::channel();
        let record = RecordDropContext(dropped_tx);
        let (started_tx, started_rx) = oneshot::channel();
        let task = SpawnedTask::spawn_reclaimable(async move {
            let _record = record;
            started_tx.send(()).unwrap();
            pending::<()>().await;
        });
        started_rx.await.unwrap();
        wait_until_idle(&task);

        let dropper = std::thread::spawn(move || {
            drop(task);
            std::thread::current().id()
        })
        .join()
        .unwrap();
        assert_eq!(dropped_rx.recv().unwrap(), (dropper, true, None));
    }

    #[tokio::test]
    async fn drop_contains_destructor_panics() {
        struct PanicOnDrop;
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("destructor panic");
            }
        }
        let task = SpawnedTask::spawn_reclaimable(async move {
            let _panics = PanicOnDrop;
            pending::<()>().await;
        });
        // Lets the task start and park
        tokio::task::yield_now().await;

        drop(task);
    }
}
