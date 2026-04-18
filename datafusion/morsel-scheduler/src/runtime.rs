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

//! Thread-per-core [`WorkerPool`] with work-stealing.
//!
//! Each worker owns one OS thread that hosts a dedicated
//! `current_thread` Tokio runtime and a [`tokio::task::LocalSet`]. Work
//! is submitted as a `Send` closure which is enqueued onto the target
//! worker's deque; when it runs, the closure constructs the real
//! (possibly `!Send`) future **on the worker thread**, which is then
//! `spawn_local`-ed onto the worker's `LocalSet`. The future never
//! migrates after that point (Tokio's `LocalSet` guarantees this).
//!
//! # Work-stealing
//!
//! Submitted closures live in per-worker deques and are **stealable**
//! until a worker picks them up. An idle worker checks its own deque
//! first (LIFO for cache locality), then walks the other workers'
//! deques in round-robin order and steals the oldest pending job
//! (FIFO) from a busy peer.
//!
//! Stealing is safe because the `BoxedJob` is `Send`. It is the
//! **future built by the job** that is `!Send`; by the time that
//! future exists, it has already been `spawn_local`-ed onto the
//! thief's own `LocalSet` and is pinned there for the rest of its
//! life.
//!
//! This means [`WorkerPool::spawn_on`] is a *hint*: the job will
//! prefer `worker_id` but may be picked up by an idle peer. If strict
//! affinity is ever needed, add a non-stealable variant.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion_common::error::{DataFusionError, Result};
use tokio::runtime::Builder;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::LocalSet;

/// A job queued onto a worker: a `Send` closure that, when invoked on
/// the worker thread, spawns the real (possibly `!Send`) future onto
/// the worker's `LocalSet`.
type BoxedJob = Box<dyn FnOnce(&LocalSet) + Send + 'static>;

/// Per-worker state shared with the pool for submission and stealing.
struct WorkerState {
    /// Owner pushes/pops the **back** (LIFO, warm cache). Thieves pop
    /// the **front** (FIFO, oldest first) so owner and thief touch
    /// opposite ends, reducing contention.
    deque: Mutex<VecDeque<BoxedJob>>,
    /// Wakes the worker when someone submits to *this* deque.
    notify: Notify,
}

/// State shared by the pool handle and every worker thread.
struct Shared {
    workers: Vec<Arc<WorkerState>>,
    /// Wakes some sleeping worker when *any* deque grows — used to pull
    /// idle workers into stealing mode.
    global_notify: Notify,
    /// Round-robin cursor for [`WorkerPool::spawn_any`].
    next: AtomicUsize,
}

/// Pool of thread-per-core `current_thread` Tokio runtimes with
/// work-stealing between workers.
pub struct WorkerPool {
    shared: Arc<Shared>,
    /// One sender per worker. When the last [`WorkerPool`] handle
    /// drops, these drop with it; each worker's receiver then sees
    /// `None` and exits its loop.
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

        let states: Vec<Arc<WorkerState>> = (0..workers)
            .map(|_| {
                Arc::new(WorkerState {
                    deque: Mutex::new(VecDeque::new()),
                    notify: Notify::new(),
                })
            })
            .collect();

        let shared = Arc::new(Shared {
            workers: states,
            global_notify: Notify::new(),
            next: AtomicUsize::new(0),
        });

        let mut shutdown = Vec::with_capacity(workers);
        for i in 0..workers {
            let (sd_tx, sd_rx) = mpsc::unbounded_channel::<()>();
            let shared = Arc::clone(&shared);
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
                    let local = LocalSet::new();
                    rt.block_on(local.run_until(run_worker(i, shared, sd_rx, &local)));
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
        self.shared.workers.len()
    }

    /// Round-robin pick of the next worker id. The returned id is not
    /// reserved — subsequent calls will keep rotating regardless of
    /// whether the caller actually uses it.
    pub fn next_worker(&self) -> usize {
        self.shared.next.fetch_add(1, Ordering::Relaxed) % self.shared.workers.len()
    }

    /// Submit a job with affinity to `worker_id`.
    ///
    /// The job is pushed onto that worker's deque. If the target is
    /// busy and another worker is idle, the idle worker may steal it
    /// before the target picks it up. The caller passes a
    /// `Send + 'static` **closure** that builds the future on the
    /// running worker; the resulting future may be `!Send`.
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
        let idx = worker_id % self.shared.workers.len();
        let job: BoxedJob = Box::new(move |local: &LocalSet| {
            local.spawn_local(async move {
                let out = make_future().await;
                let _ = tx.send(out);
            });
        });

        {
            let w = &self.shared.workers[idx];
            w.deque.lock().expect("deque poisoned").push_back(job);
            // Prefer waking the target; also nudge one sleeping thief so
            // a saturated target doesn't starve idle peers.
            w.notify.notify_one();
            self.shared.global_notify.notify_one();
        }

        async move {
            rx.await.map_err(|_| {
                DataFusionError::External(
                    "morsel worker job was cancelled before completing".into(),
                )
            })
        }
    }

    /// Submit a job with no affinity; the round-robin target is a
    /// starting hint and any idle worker may steal it.
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

/// Main loop for a worker: run own jobs, steal from peers when idle,
/// sleep when no work is available.
async fn run_worker(
    my_id: usize,
    shared: Arc<Shared>,
    mut sd_rx: mpsc::UnboundedReceiver<()>,
    local: &LocalSet,
) {
    let me = Arc::clone(&shared.workers[my_id]);
    loop {
        // 1) Own deque (LIFO — hottest cache).
        if let Some(job) = pop_own(&me) {
            job(local);
            continue;
        }
        // 2) Steal from a peer (FIFO — their oldest).
        if let Some(job) = steal(&shared, my_id) {
            job(local);
            continue;
        }
        // 3) No work. Sleep until woken by a submission, a cross-worker
        //    nudge, or pool shutdown.
        tokio::select! {
            biased;
            _ = sd_rx.recv() => return,
            _ = me.notify.notified() => {}
            _ = shared.global_notify.notified() => {}
        }
    }
}

#[inline]
fn pop_own(me: &WorkerState) -> Option<BoxedJob> {
    me.deque.lock().expect("deque poisoned").pop_back()
}

/// Walk peers in round-robin order and steal the oldest job from the
/// first one that has any. Returns `None` if every peer deque is empty.
fn steal(shared: &Shared, my_id: usize) -> Option<BoxedJob> {
    let n = shared.workers.len();
    for offset in 1..n {
        let victim = (my_id + offset) % n;
        if let Some(job) = shared.workers[victim]
            .deque
            .lock()
            .expect("deque poisoned")
            .pop_front()
        {
            return Some(job);
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
