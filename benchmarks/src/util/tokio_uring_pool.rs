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

//! Pool of dedicated `tokio-uring` runtimes.
//!
//! Each pool worker owns an OS thread that runs its own
//! [`tokio_uring::start`] runtime (a current-thread Tokio reactor with
//! an `io_uring` driver). The pool exposes a single
//! [`TokioUringPool::spawn`] entry point that dispatches an arbitrary
//! `Send + 'static` future round-robin to a worker.
//!
//! This primitive powers two things in the benchmark:
//!
//! * The [`TokioUringObjectStore`](crate::util::tokio_uring_store::TokioUringObjectStore)
//!   uses it to run its `get_ranges` reads.
//! * The ClickBench runner uses it to execute per-partition
//!   [`SendableRecordBatchStream`](datafusion::physical_plan::SendableRecordBatchStream)
//!   jobs, so plan execution itself happens on the `io_uring` reactors
//!   — giving genuine I/O + compute parallelism across `N` threads.

#![cfg(target_os = "linux")]

use std::cell::Cell;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::error::{DataFusionError, Result};
use tokio::sync::{mpsc, oneshot};

/// A job queued onto a worker: a `Send` closure that, when invoked on
/// the worker thread, spawns the real (possibly `!Send`) future onto
/// the local `tokio-uring` runtime.
type BoxedJob = Box<dyn FnOnce() + Send + 'static>;

/// Pool of dedicated `tokio-uring` runtimes.
pub struct TokioUringPool {
    workers: Vec<mpsc::UnboundedSender<BoxedJob>>,
    next: AtomicUsize,
}

impl TokioUringPool {
    /// Spawn `workers` OS threads, each hosting its own
    /// [`tokio_uring::start`] runtime.
    pub fn new(workers: usize) -> Result<Arc<Self>> {
        assert!(workers > 0, "TokioUringPool requires at least one worker");

        let mut senders = Vec::with_capacity(workers);
        for i in 0..workers {
            let (tx, rx) = mpsc::unbounded_channel::<BoxedJob>();
            std::thread::Builder::new()
                .name(format!("tokio-uring-{i}"))
                .spawn(move || {
                    tokio_uring::start(run_worker(rx));
                })
                .map_err(|e| {
                    DataFusionError::External(
                        format!("failed to spawn tokio-uring worker: {e}").into(),
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

    /// Run a future on the next worker (round-robin).
    ///
    /// The caller passes a `Send + 'static` **closure** that builds the
    /// future. The closure is shipped to the worker, invoked there, and
    /// the resulting future is spawned onto that worker's `tokio-uring`
    /// runtime. The future itself may be `!Send` (e.g. hold a
    /// [`tokio_uring::fs::File`]) — it never crosses threads because it
    /// is constructed on the worker.
    ///
    /// The returned future resolves to the future's output (wrapped in
    /// `Result` to surface dead-worker / cancellation errors).
    pub fn spawn<R, F, O>(
        self: &Arc<Self>,
        make_future: R,
    ) -> impl Future<Output = Result<O>> + Send + 'static
    where
        R: FnOnce() -> F + Send + 'static,
        F: Future<Output = O> + 'static,
        O: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<O>();
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();

        let send_result = self.workers[idx].send(Box::new(move || {
            tokio_uring::spawn(async move {
                let out = make_future().await;
                let _ = tx.send(out);
            });
        }));

        async move {
            send_result.map_err(|_| {
                DataFusionError::External("tokio-uring worker thread is gone".into())
            })?;
            rx.await.map_err(|_| {
                DataFusionError::External(
                    "tokio-uring job was cancelled before completing".into(),
                )
            })
        }
    }
}

thread_local! {
    /// `true` while the current thread is running a pool worker's
    /// [`tokio_uring::start`] reactor. Consumers (e.g. the object
    /// store) use this to stay **local** — spawning on the current
    /// ring instead of hopping to a round-robin pool worker.
    static IN_WORKER: Cell<bool> = const { Cell::new(false) };
}

/// `true` if the current thread is inside a pool worker's reactor.
pub fn in_worker() -> bool {
    IN_WORKER.with(|flag| flag.get())
}

async fn run_worker(mut rx: mpsc::UnboundedReceiver<BoxedJob>) {
    IN_WORKER.with(|flag| flag.set(true));
    while let Some(job) = rx.recv().await {
        job();
    }
}
