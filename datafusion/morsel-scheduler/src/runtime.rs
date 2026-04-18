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

//! Thread-per-core [`WorkerPool`] built on plain Tokio.
//!
//! Each worker owns one OS thread that hosts a dedicated
//! `current_thread` Tokio runtime and a [`tokio::task::LocalSet`]. Work
//! is submitted as a `Send` closure which is delivered to a worker's
//! inbox over an unbounded channel; the closure constructs the real
//! (possibly `!Send`) future **on the worker thread**, so the future
//! is `spawn_local`-ed onto the worker's local set and never migrates.
//!
//! The pool exposes two entry points:
//!
//! * [`WorkerPool::spawn_on`] — run a future on a specific worker id.
//!   Used when the caller has already decided which core should own
//!   the work (e.g. "partition `p` of this pipeline runs on worker
//!   `p % N`").
//! * [`WorkerPool::spawn_any`] — round-robin across workers. Used for
//!   one-off work that has no partition affinity.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion_common::error::{DataFusionError, Result};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, oneshot};
use tokio::task::LocalSet;

/// A job queued onto a worker: a `Send` closure that, when invoked on
/// the worker thread, spawns the real (possibly `!Send`) future onto
/// the worker's `LocalSet`.
type BoxedJob = Box<dyn FnOnce(&LocalSet) + Send + 'static>;

/// Pool of thread-per-core `current_thread` Tokio runtimes.
pub struct WorkerPool {
    workers: Vec<mpsc::UnboundedSender<BoxedJob>>,
    next: AtomicUsize,
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

        let mut senders = Vec::with_capacity(workers);
        for i in 0..workers {
            let (tx, rx) = mpsc::unbounded_channel::<BoxedJob>();
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
                    rt.block_on(local.run_until(run_worker(rx, &local)));
                })
                .map_err(|e| {
                    DataFusionError::External(
                        format!("failed to spawn morsel worker: {e}").into(),
                    )
                })?;
            senders.push(tx);
        }

        Ok(Arc::new(Self {
            workers: senders,
            next: AtomicUsize::new(0),
        }))
    }

    /// Number of worker runtimes in the pool.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Round-robin pick of the next worker id. The returned id is not
    /// reserved — subsequent calls will keep rotating regardless of
    /// whether the caller actually uses it.
    pub fn next_worker(&self) -> usize {
        self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len()
    }

    /// Run a future on the `worker_id`-th worker.
    ///
    /// The caller passes a `Send + 'static` **closure** that builds the
    /// future. The closure is shipped to the worker, invoked there,
    /// and the resulting future is `spawn_local`-ed onto the worker's
    /// [`LocalSet`]. The future itself may be `!Send`.
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
        let idx = worker_id % self.workers.len();
        let send_result = self.workers[idx].send(Box::new(move |local: &LocalSet| {
            local.spawn_local(async move {
                let out = make_future().await;
                let _ = tx.send(out);
            });
        }));

        async move {
            send_result.map_err(|_| {
                DataFusionError::External("morsel worker thread is gone".into())
            })?;
            rx.await.map_err(|_| {
                DataFusionError::External(
                    "morsel worker job was cancelled before completing".into(),
                )
            })
        }
    }

    /// Run a future on the next worker chosen round-robin.
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

async fn run_worker(mut rx: mpsc::UnboundedReceiver<BoxedJob>, local: &LocalSet) {
    while let Some(job) = rx.recv().await {
        job(local);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    #[test]
    fn spawn_on_runs_future_on_designated_worker() {
        let pool = WorkerPool::new(4).unwrap();
        let rt = Builder::new_current_thread().enable_all().build().unwrap();

        rt.block_on(async {
            // Build a !Send future (Rc<Cell<_>>) on the worker to prove
            // the closure is evaluated there and the future never
            // crosses threads.
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
}
