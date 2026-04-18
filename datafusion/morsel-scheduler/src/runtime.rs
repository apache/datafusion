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

//! Thread-per-core [`WorkerPool`] with crossbeam-deque work-stealing.
//!
//! Each worker owns one OS thread hosting a dedicated `current_thread`
//! Tokio runtime and a [`tokio::task::LocalSet`]. Work is submitted as
//! a `Send` closure; when it runs, the closure constructs the
//! (possibly `!Send`) future **on the worker thread**, which is then
//! `spawn_local`-ed onto the worker's `LocalSet`. The future never
//! migrates after that point.
//!
//! # Scheduling
//!
//! Queues are the Tokio / Chase-Lev layout:
//!
//! * A **shared [`Injector`]** receives all external submissions
//!   (wait-free MPSC push).
//! * Each worker owns a local **[`Worker<BoxedJob>`]** (Chase-Lev
//!   deque; owner push/pop are wait-free) plus a **[`Stealer`]** that
//!   peer workers can use to pull batches.
//!
//! An idle worker's scan order is: own deque → shared injector →
//! peer stealers → park on [`Notify`]. `spawn_on(worker_id, …)` is a
//! **hint**: the submission lands on the injector but the target's
//! notify is nudged first, so a free target tends to pick up its own
//! job. A saturated target doesn't starve peers: any idle worker can
//! steal from the injector.
use std::cell::Cell;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use datafusion_common::error::{DataFusionError, Result};
use tokio::runtime::Builder;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::LocalSet;

/// A job queued onto a worker: a `Send` closure that, when invoked on
/// the worker thread, spawns the real (possibly `!Send`) future onto
/// the worker's `LocalSet`.
type BoxedJob = Box<dyn FnOnce(&LocalSet) + Send + 'static>;

/// State shared by the pool handle and every worker thread.
struct Shared {
    /// Unique pool id, used by [`WorkerPool::current_worker`] to reject
    /// matches when multiple pools coexist in the same process.
    id: usize,
    /// Global submission queue — any thread may push, any worker may
    /// steal a batch. Wait-free.
    injector: Injector<BoxedJob>,
    /// One stealer per worker, used by peers when their own deque is
    /// empty.
    stealers: Vec<Stealer<BoxedJob>>,
    /// Per-worker wake-up (nudged by submissions whose `worker_id`
    /// hint targets this worker).
    notifies: Vec<Notify>,
    /// Wakes some sleeping worker whenever *any* queue grows — pulls
    /// idle workers into stealing mode.
    global_notify: Notify,
    /// Round-robin cursor for [`WorkerPool::spawn_any`].
    next: AtomicUsize,
}

/// Source of fresh pool ids.
static NEXT_POOL_ID: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// `Some((pool_id, worker_id))` on threads that host a pool
    /// worker's reactor; `None` elsewhere. Set once by
    /// [`run_worker`] before entering its scheduling loop.
    static CURRENT_WORKER: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}

/// Pool of thread-per-core `current_thread` Tokio runtimes with
/// crossbeam-deque work-stealing.
pub struct WorkerPool {
    shared: Arc<Shared>,
    /// One sender per worker. Dropping the pool drops these; each
    /// worker's receiver then sees `None` in its `select!` and exits.
    _shutdown: Vec<mpsc::UnboundedSender<()>>,
}

impl WorkerPool {
    /// Spawn `workers` OS threads, each hosting a dedicated
    /// `current_thread` Tokio runtime and [`LocalSet`].
    pub fn new(workers: usize) -> Result<Arc<Self>> {
        if workers == 0 {
            return Err(DataFusionError::Configuration(
                "WorkerPool requires at least one worker".to_string(),
            ));
        }

        // Build each worker's local deque up front so we can hand the
        // stealers to `Shared` before the worker threads start.
        let mut locals: Vec<Option<Worker<BoxedJob>>> = Vec::with_capacity(workers);
        let mut stealers = Vec::with_capacity(workers);
        let mut notifies = Vec::with_capacity(workers);
        for _ in 0..workers {
            let w = Worker::<BoxedJob>::new_fifo();
            stealers.push(w.stealer());
            locals.push(Some(w));
            notifies.push(Notify::new());
        }

        let shared = Arc::new(Shared {
            id: NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed),
            injector: Injector::new(),
            stealers,
            notifies,
            global_notify: Notify::new(),
            next: AtomicUsize::new(0),
        });

        let mut shutdown = Vec::with_capacity(workers);
        for (i, slot) in locals.iter_mut().enumerate() {
            let (sd_tx, sd_rx) = mpsc::unbounded_channel::<()>();
            let shared = Arc::clone(&shared);
            let local = slot.take().expect("local worker deque missing");
            std::thread::Builder::new()
                .name(format!("morsel-worker-{i}"))
                .spawn(move || {
                    let rt = match Builder::new_current_thread().enable_all().build() {
                        Ok(rt) => rt,
                        Err(e) => {
                            log::error!("worker {i}: failed to build runtime: {e}");
                            return;
                        }
                    };
                    let lset = LocalSet::new();
                    rt.block_on(
                        lset.run_until(run_worker(i, shared, local, sd_rx, &lset)),
                    );
                })
                .map_err(|e| {
                    DataFusionError::External(
                        format!("failed to spawn morsel worker: {e}").into(),
                    )
                })?;
            shutdown.push(sd_tx);
        }

        Ok(Arc::new(Self {
            shared,
            _shutdown: shutdown,
        }))
    }

    /// Number of worker runtimes in the pool.
    pub fn worker_count(&self) -> usize {
        self.shared.stealers.len()
    }

    /// Round-robin pick of the next worker id. The returned id is not
    /// reserved — subsequent calls will keep rotating regardless of
    /// whether the caller actually uses it.
    pub fn next_worker(&self) -> usize {
        self.shared.next.fetch_add(1, Ordering::Relaxed) % self.worker_count()
    }

    /// If the current OS thread is running inside **this** pool's
    /// worker loop, returns that worker's id; otherwise returns `None`.
    pub fn current_worker(&self) -> Option<usize> {
        let pool_id = self.shared.id;
        CURRENT_WORKER.with(|cell| {
            cell.get()
                .and_then(|(pid, wid)| (pid == pool_id).then_some(wid))
        })
    }

    /// Submit a job with affinity to `worker_id`.
    ///
    /// The job lands on the shared injector; the target's notify is
    /// pinged first so an idle target tends to pick up its own work,
    /// while a saturated target doesn't starve peers.
    pub fn spawn_on<R, F, O>(
        self: &Arc<Self>,
        worker_id: usize,
        make_future: R,
    ) -> impl Future<Output = Result<O>> + Send + 'static
    where
        R: FnOnce() -> F + Send + 'static,
        F: Future<Output = O> + 'static,
        O: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<O>();
        let idx = worker_id % self.worker_count();
        let job: BoxedJob = Box::new(move |local: &LocalSet| {
            local.spawn_local(async move {
                let out = make_future().await;
                let _ = tx.send(out);
            });
        });

        self.shared.injector.push(job);
        self.shared.notifies[idx].notify_one();
        self.shared.global_notify.notify_one();

        async move {
            rx.await.map_err(|_| {
                DataFusionError::External(
                    "morsel worker job was cancelled before completing".into(),
                )
            })
        }
    }

    /// Submit a job with no affinity; equivalent to
    /// [`Self::spawn_on`] with a round-robin hint.
    pub fn spawn_any<R, F, O>(
        self: &Arc<Self>,
        make_future: R,
    ) -> impl Future<Output = Result<O>> + Send + 'static
    where
        R: FnOnce() -> F + Send + 'static,
        F: Future<Output = O> + 'static,
        O: Send + 'static,
    {
        let worker_id = self.next_worker();
        self.spawn_on(worker_id, make_future)
    }
}

/// Main loop for a worker.
async fn run_worker(
    my_id: usize,
    shared: Arc<Shared>,
    local: Worker<BoxedJob>,
    mut sd_rx: mpsc::UnboundedReceiver<()>,
    lset: &LocalSet,
) {
    // Identify this OS thread so any task on `lset` can see its host.
    CURRENT_WORKER.with(|cell| cell.set(Some((shared.id, my_id))));

    loop {
        if let Some(job) = find_job(&local, &shared, my_id) {
            job(lset);
            continue;
        }

        // No work anywhere. Sleep until a submission, a cross-worker
        // nudge, or pool shutdown.
        tokio::select! {
            biased;
            _ = sd_rx.recv() => return,
            _ = shared.notifies[my_id].notified() => {}
            _ = shared.global_notify.notified() => {}
        }
    }
}

/// Owner pops own first, then steals a batch from the global injector,
/// then from peer workers. Returns `None` if every queue is empty.
#[inline]
fn find_job(local: &Worker<BoxedJob>, shared: &Shared, my_id: usize) -> Option<BoxedJob> {
    // Fast path: own deque.
    if let Some(job) = local.pop() {
        return Some(job);
    }
    // Try the global injector, grabbing a batch so subsequent pops are
    // cheap.
    loop {
        match shared.injector.steal_batch_and_pop(local) {
            Steal::Success(job) => return Some(job),
            Steal::Empty => break,
            Steal::Retry => continue,
        }
    }
    // Try peer workers' deques.
    let n = shared.stealers.len();
    for offset in 1..n {
        let victim = (my_id + offset) % n;
        loop {
            match shared.stealers[victim].steal_batch_and_pop(local) {
                Steal::Success(job) => return Some(job),
                Steal::Empty => break,
                Steal::Retry => continue,
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::instant::Instant;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::Duration;

    #[test]
    fn spawn_on_runs_future_on_a_worker() {
        // Stealing means we can't guarantee *which* worker ran the
        // closure, only that it ran on some worker — so the !Send
        // future lived on that worker's LocalSet and never crossed
        // threads.
        let pool = WorkerPool::new(4).unwrap();
        let rt = Builder::new_current_thread().enable_all().build().unwrap();

        rt.block_on(async {
            let out = pool
                .spawn_on(2, || async {
                    let local = Rc::new(Cell::new(0u32));
                    local.set(42);
                    local.get()
                })
                .await
                .unwrap();
            assert_eq!(out, 42);
        });
    }

    #[test]
    fn spawn_any_round_robins() {
        let pool = WorkerPool::new(3).unwrap();
        let rt = Builder::new_current_thread().enable_all().build().unwrap();

        rt.block_on(async {
            for _ in 0..10 {
                let v = pool.spawn_any(|| async { 7u32 }).await.unwrap();
                assert_eq!(v, 7);
            }
        });
    }

    #[test]
    fn current_worker_reports_self_on_a_worker_thread() {
        let pool = WorkerPool::new(3).unwrap();
        let rt = Builder::new_current_thread().enable_all().build().unwrap();

        rt.block_on(async {
            assert_eq!(pool.current_worker(), None);

            let observed = pool
                .spawn_on(1, {
                    let pool = Arc::clone(&pool);
                    move || async move { pool.current_worker() }
                })
                .await
                .unwrap();
            let id = observed.expect("expected to run on a worker of this pool");
            assert!(id < pool.worker_count());
        });
    }

    #[test]
    fn current_worker_returns_none_for_a_different_pool() {
        let pool_a = WorkerPool::new(2).unwrap();
        let pool_b = WorkerPool::new(2).unwrap();
        let rt = Builder::new_current_thread().enable_all().build().unwrap();

        rt.block_on(async {
            let out = pool_a
                .spawn_on(0, {
                    let pool_b = Arc::clone(&pool_b);
                    move || async move { pool_b.current_worker() }
                })
                .await
                .unwrap();
            assert_eq!(out, None);
        });
    }

    #[test]
    fn idle_workers_steal_from_saturated_target() {
        // Pin eight 50ms sleeping jobs all to worker 0. With a pool of
        // four workers, stealing should fan them out so wall time is
        // well below the 400ms sequential lower bound.
        let pool = WorkerPool::new(4).unwrap();
        let rt = Builder::new_current_thread().enable_all().build().unwrap();

        rt.block_on(async {
            let mut handles = Vec::new();
            for _ in 0..8 {
                handles.push(pool.spawn_on(0, || async {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    1u32
                }));
            }
            let start = Instant::now();
            for h in handles {
                assert_eq!(h.await.unwrap(), 1);
            }
            let elapsed = start.elapsed();
            assert!(
                elapsed < Duration::from_millis(300),
                "expected stealing to parallelize work pinned to worker 0, \
                 but wall time was {elapsed:?}"
            );
        });
    }
}
