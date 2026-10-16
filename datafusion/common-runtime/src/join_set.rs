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

use crate::common::{ReclaimHandle, SlotFuture, TaskSlot};
use crate::trace_utils::{trace_block, trace_future};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use tokio::runtime::Handle;
use tokio::task::{AbortHandle, Id, JoinError, LocalSet};

/// A wrapper around [Tokio's `JoinSet`] that forwards all API calls while optionally
/// instrumenting spawned tasks and blocking closures with custom tracing behavior.
/// If no tracer is injected via [`set_join_set_tracer`], tasks and closures are executed
/// without any instrumentation.
///
/// Tasks spawned with `spawn_reclaimable` or `spawn_reclaimable_on` behave like
/// those of [`SpawnedTask::spawn_reclaimable`]: dropping the set, or calling
/// `abort_all` or `shutdown`, drops their futures right away unless a worker is
/// polling them.
///
/// [Tokio's `JoinSet`]: tokio::task::JoinSet
/// [`set_join_set_tracer`]: crate::trace_utils::set_join_set_tracer
/// [`SpawnedTask::spawn_reclaimable`]: crate::SpawnedTask::spawn_reclaimable
#[derive(Debug)]
pub struct JoinSet<T> {
    inner: tokio::task::JoinSet<T>,
    /// Reclaimable tasks that have not been joined yet
    reclaim: HashMap<Id, ReclaimHandle<T>>,
}

impl<T> Default for JoinSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> JoinSet<T> {
    /// [JoinSet::new](tokio::task::JoinSet::new) - Create a new JoinSet.
    pub fn new() -> Self {
        Self {
            inner: tokio::task::JoinSet::new(),
            reclaim: HashMap::new(),
        }
    }

    /// [JoinSet::len](tokio::task::JoinSet::len) - Return the number of tasks.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// [JoinSet::is_empty](tokio::task::JoinSet::is_empty) - Check if the JoinSet is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn reclaim_all(&mut self) {
        for (_, task) in self.reclaim.drain() {
            task.reclaim();
        }
    }

    fn forget<R>(&mut self, joined: Option<&Result<(Id, R), JoinError>>) {
        match joined {
            Some(Ok((id, _))) => self.reclaim.remove(id),
            Some(Err(e)) => self.reclaim.remove(&e.id()),
            None => None,
        };
    }
}

impl<T> Drop for JoinSet<T> {
    fn drop(&mut self) {
        // `inner` aborts the tasks after this
        self.reclaim_all();
    }
}

impl<T: 'static> JoinSet<T> {
    /// [JoinSet::spawn](tokio::task::JoinSet::spawn) - Spawn a new task.
    pub fn spawn<F>(&mut self, task: F) -> AbortHandle
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        self.inner.spawn(trace_future(task))
    }

    /// [JoinSet::spawn_on](tokio::task::JoinSet::spawn_on) - Spawn a task on a provided runtime.
    pub fn spawn_on<F>(&mut self, task: F, handle: &Handle) -> AbortHandle
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        self.inner.spawn_on(trace_future(task), handle)
    }

    /// Like [`Self::spawn`], with the drop behavior of
    /// [`SpawnedTask::spawn_reclaimable`](crate::SpawnedTask::spawn_reclaimable).
    ///
    /// Returns the task's ID rather than an [`AbortHandle`]: such a task is
    /// cancelled only through the set, which drops its future before aborting
    /// it, so nothing can observe it finish while its destructor still runs.
    /// The runtime shutting down still cancels it independently: if that
    /// happens while [`Self::abort_all`] is dropping the future, joining the
    /// task afterwards reports it as cancelled even if its destructor panicked.
    pub fn spawn_reclaimable<F>(&mut self, task: F) -> Id
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        self.spawn_reclaimable_on(task, &Handle::current())
    }

    /// Like [`Self::spawn_on`], with the drop behavior of
    /// [`SpawnedTask::spawn_reclaimable`](crate::SpawnedTask::spawn_reclaimable),
    /// and returning the task's ID as [`Self::spawn_reclaimable`] does.
    pub fn spawn_reclaimable_on<F>(&mut self, task: F, handle: &Handle) -> Id
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        let slot = TaskSlot::new(Box::pin(task));
        let id = self
            .inner
            .spawn_on(trace_future(SlotFuture(Arc::clone(&slot))), handle)
            .id();
        self.reclaim
            .insert(id, ReclaimHandle::new(slot, handle.clone()));
        id
    }

    /// [JoinSet::spawn_local](tokio::task::JoinSet::spawn_local) - Spawn a local task.
    pub fn spawn_local<F>(&mut self, task: F) -> AbortHandle
    where
        F: Future<Output = T>,
        F: 'static,
    {
        self.inner.spawn_local(task)
    }

    /// [JoinSet::spawn_local_on](tokio::task::JoinSet::spawn_local_on) - Spawn a local task on a provided LocalSet.
    pub fn spawn_local_on<F>(&mut self, task: F, local_set: &LocalSet) -> AbortHandle
    where
        F: Future<Output = T>,
        F: 'static,
    {
        self.inner.spawn_local_on(task, local_set)
    }

    /// [JoinSet::spawn_blocking](tokio::task::JoinSet::spawn_blocking) - Spawn a blocking task.
    pub fn spawn_blocking<F>(&mut self, f: F) -> AbortHandle
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send,
    {
        self.inner.spawn_blocking(trace_block(f))
    }

    /// [JoinSet::spawn_blocking_on](tokio::task::JoinSet::spawn_blocking_on) - Spawn a blocking task on a provided runtime.
    pub fn spawn_blocking_on<F>(&mut self, f: F, handle: &Handle) -> AbortHandle
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send,
    {
        self.inner.spawn_blocking_on(trace_block(f), handle)
    }

    /// [JoinSet::join_next](tokio::task::JoinSet::join_next) - Await the next completed task.
    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        let joined = self.inner.join_next_with_id().await;
        self.forget(joined.as_ref());
        joined.map(|res| res.map(|(_, output)| output))
    }

    /// [JoinSet::try_join_next](tokio::task::JoinSet::try_join_next) - Try to join the next completed task.
    pub fn try_join_next(&mut self) -> Option<Result<T, JoinError>> {
        let joined = self.inner.try_join_next_with_id();
        self.forget(joined.as_ref());
        joined.map(|res| res.map(|(_, output)| output))
    }

    /// [JoinSet::abort_all](tokio::task::JoinSet::abort_all) - Abort all tasks.
    pub fn abort_all(&mut self) {
        self.reclaim_all();
        self.inner.abort_all()
    }

    /// [JoinSet::detach_all](tokio::task::JoinSet::detach_all) - Detach all tasks.
    pub fn detach_all(&mut self) {
        self.reclaim.clear();
        self.inner.detach_all()
    }

    /// [JoinSet::poll_join_next](tokio::task::JoinSet::poll_join_next) - Poll for the next completed task.
    pub fn poll_join_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<T, JoinError>>> {
        let joined = ready!(self.inner.poll_join_next_with_id(cx));
        self.forget(joined.as_ref());
        Poll::Ready(joined.map(|res| res.map(|(_, output)| output)))
    }

    /// [JoinSet::join_next_with_id](tokio::task::JoinSet::join_next_with_id) - Await the next completed task with its ID.
    pub async fn join_next_with_id(&mut self) -> Option<Result<(Id, T), JoinError>> {
        let joined = self.inner.join_next_with_id().await;
        self.forget(joined.as_ref());
        joined
    }

    /// [JoinSet::try_join_next_with_id](tokio::task::JoinSet::try_join_next_with_id) - Try to join the next completed task with its ID.
    pub fn try_join_next_with_id(&mut self) -> Option<Result<(Id, T), JoinError>> {
        let joined = self.inner.try_join_next_with_id();
        self.forget(joined.as_ref());
        joined
    }

    /// [JoinSet::poll_join_next_with_id](tokio::task::JoinSet::poll_join_next_with_id) - Poll for the next completed task with its ID.
    pub fn poll_join_next_with_id(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(Id, T), JoinError>>> {
        let joined = ready!(self.inner.poll_join_next_with_id(cx));
        self.forget(joined.as_ref());
        Poll::Ready(joined)
    }

    /// [JoinSet::shutdown](tokio::task::JoinSet::shutdown) - Abort all tasks and wait for shutdown.
    pub async fn shutdown(&mut self) {
        self.reclaim_all();
        self.inner.shutdown().await
    }

    /// [JoinSet::join_all](tokio::task::JoinSet::join_all) - Await all tasks.
    pub async fn join_all(mut self) -> Vec<T> {
        // Tokio's loop, run on this set so that dropping the future reclaims before aborting
        let mut output = Vec::with_capacity(self.len());
        while let Some(res) = self.join_next().await {
            match res {
                Ok(t) => output.push(t),
                Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
                Err(err) => panic!("{err}"),
            }
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::future::pending;

    use tokio::sync::oneshot;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drop_releases_idle_tasks_in_place() {
        let owned = Arc::new(());
        let released = Arc::downgrade(&owned);
        let mut set = JoinSet::new();
        let mut started = vec![];
        for _ in 0..4 {
            let owned = Arc::clone(&owned);
            let (started_tx, started_rx) = oneshot::channel();
            started.push(started_rx);
            set.spawn_reclaimable(async move {
                started_tx.send(()).unwrap();
                pending::<()>().await;
                drop(owned);
            });
        }
        drop(owned);
        for started_rx in started {
            started_rx.await.unwrap();
        }
        set.reclaim
            .values()
            .for_each(ReclaimHandle::wait_until_idle);

        drop(set);
        assert_eq!(released.strong_count(), 0);
    }

    #[tokio::test]
    async fn joined_tasks_are_forgotten() {
        let mut set = JoinSet::new();
        for i in 0..3 {
            set.spawn_reclaimable(async move { i });
        }
        let mut outputs = vec![];
        while let Some(output) = set.join_next().await {
            outputs.push(output.unwrap());
        }
        outputs.sort_unstable();
        assert_eq!(outputs, vec![0, 1, 2]);
        assert!(set.reclaim.is_empty());
    }

    #[tokio::test]
    async fn detached_tasks_keep_running() {
        let mut set = JoinSet::new();
        let (resume_tx, resume_rx) = oneshot::channel::<()>();
        let (done_tx, done_rx) = oneshot::channel();
        set.spawn_reclaimable(async move {
            resume_rx.await.unwrap();
            done_tx.send(()).unwrap();
        });
        set.detach_all();
        drop(set);

        resume_tx.send(()).unwrap();
        done_rx.await.unwrap();
    }

    #[tokio::test]
    async fn abort_all_reports_a_panic_from_a_reclaimed_destructor() {
        struct PanicOnDrop;
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("destructor panic");
            }
        }
        let mut set = JoinSet::new();
        set.spawn_reclaimable(async move {
            let _panics = PanicOnDrop;
            pending::<()>().await;
        });
        // Lets the task start and park
        tokio::task::yield_now().await;

        set.abort_all();
        assert!(set.join_next().await.unwrap().unwrap_err().is_panic());
    }

    #[test]
    fn runtime_shutdown_does_not_wait_for_a_reclaiming_destructor() {
        use std::sync::mpsc::{Receiver, Sender, channel};
        use std::time::Duration;

        struct BlockInDrop {
            entered: Sender<()>,
            resume: Receiver<()>,
        }
        impl Drop for BlockInDrop {
            fn drop(&mut self) {
                self.entered.send(()).unwrap();
                let _ = self.resume.recv();
            }
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let (entered_tx, entered_rx) = channel();
        let (resume_tx, resume_rx) = channel();
        let (started_tx, started_rx) = channel();
        let guard = BlockInDrop {
            entered: entered_tx,
            resume: resume_rx,
        };
        let mut set = JoinSet::new();
        set.spawn_reclaimable_on(
            async move {
                started_tx.send(()).unwrap();
                pending::<()>().await;
                drop(guard);
            },
            runtime.handle(),
        );
        started_rx.recv().unwrap();
        set.reclaim
            .values()
            .for_each(ReclaimHandle::wait_until_idle);

        // The set's drop runs the destructor on this thread until the test resumes it
        let dropper = std::thread::spawn(move || drop(set));
        entered_rx.recv().unwrap();
        let (shut_down_tx, shut_down_rx) = channel();
        std::thread::spawn(move || {
            runtime.shutdown_timeout(Duration::from_secs(10));
            shut_down_tx.send(()).unwrap();
        });
        let shut_down = shut_down_rx.recv_timeout(Duration::from_secs(5)).is_ok();
        resume_tx.send(()).unwrap();
        dropper.join().unwrap();
        assert!(
            shut_down,
            "shutdown waited for a destructor on another thread"
        );
    }

    #[test]
    fn dropping_join_all_reclaims_before_aborting() {
        use std::sync::mpsc::{Receiver, Sender, channel};
        use std::time::Duration;

        struct BlockInDrop {
            entered: Sender<()>,
            resume: Receiver<()>,
        }
        impl Drop for BlockInDrop {
            fn drop(&mut self) {
                self.entered.send(()).unwrap();
                let _ = self.resume.recv();
            }
        }

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let (entered_tx, entered_rx) = channel();
        let (resume_tx, resume_rx) = channel();
        let (started_tx, started_rx) = channel();
        let guard = BlockInDrop {
            entered: entered_tx,
            resume: resume_rx,
        };
        let mut set = JoinSet::new();
        let first_started = started_tx.clone();
        set.spawn_reclaimable_on(
            async move {
                first_started.send(()).unwrap();
                pending::<()>().await;
                drop(guard);
            },
            runtime.handle(),
        );
        // Enough other tasks that aborting them all keeps the worker busy while
        // an aborting drop would still be reclaiming
        for _ in 0..10_000 {
            let started = started_tx.clone();
            set.spawn_reclaimable_on(
                async move {
                    started.send(()).unwrap();
                    pending::<()>().await;
                },
                runtime.handle(),
            );
        }
        for _ in 0..10_001 {
            started_rx.recv().unwrap();
        }
        // The only worker has finished every first poll once it runs this
        runtime.block_on(runtime.spawn(async {})).unwrap();

        let (dropped_tx, dropped_rx) = channel();
        let dropper = std::thread::spawn(move || {
            let mut joined = Box::pin(set.join_all());
            let mut cx = Context::from_waker(std::task::Waker::noop());
            assert!(joined.as_mut().poll(&mut cx).is_pending());
            drop(joined);
            dropped_tx.send(()).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let returned_early = dropped_rx.recv_timeout(Duration::from_millis(250)).is_ok();
        resume_tx.send(()).unwrap();
        dropper.join().unwrap();
        assert!(
            !returned_early,
            "dropping join_all left a destructor running"
        );
    }
}
