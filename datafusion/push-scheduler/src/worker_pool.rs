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

//! Crossbeam-deque work-stealing pool for push-based scheduler tasks.
//!
//! Each worker is an OS thread that:
//! * enters a **shared** `tokio::runtime::Handle` (so `tokio::spawn` inside
//!   wrapped pull-based operators goes to a real multi-thread runtime), and
//! * runs a tight blocking loop that pops [`BoxedJob`]s from the work-stealing
//!   queue and executes them synchronously.
//!
//! # Queues
//!
//! * A shared [`Injector`] receives all external submissions.
//! * Each worker owns:
//!   * a stealable [`Worker<BoxedJob>`] (LIFO) for `spawn_local` pushes,
//!   * a thread-local `VecDeque<BoxedJob>` FIFO side-queue (non-stealable)
//!     for `spawn_local_fifo` — used by `Task::do_work` to reschedule
//!     itself after routing a batch, preserving PR #2226's intent of
//!     prioritising freshly woken work.
//!
//! Search order on each worker iteration: local FIFO → own LIFO deque →
//! shared injector (batch-steal) → peer stealers (round-robin). Idle
//! workers park on a `Condvar` keyed by a single atomic wake counter;
//! submissions bump that counter to unblock one parker.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use datafusion_common::error::{DataFusionError, Result};
use tokio::runtime::Handle;

/// A job queued onto a worker: a `Send` closure that, when invoked on the
/// worker thread, does any work it needs (including synchronously driving
/// one step of a [`Task`](crate::task::Task)).
pub(crate) type BoxedJob = Box<dyn FnOnce() + Send + 'static>;

/// State shared by the pool handle and every worker thread.
struct Shared {
    /// Unique pool id, used by [`current_worker`] to reject matches when
    /// multiple pools coexist in the same process.
    id: usize,
    /// Global submission queue — any thread may push, any worker may steal
    /// a batch. Wait-free.
    injector: Injector<BoxedJob>,
    /// One stealer per worker, used by peers when their own deque is empty.
    stealers: Vec<Stealer<BoxedJob>>,
    /// Parker state. Workers wait on `wake_cvar` while the pool is idle;
    /// submissions bump `wake_count` and notify one waiter.
    wake_count: AtomicUsize,
    wake_mutex: StdMutex<()>,
    wake_cvar: Condvar,
    /// Shut down flag — set on drop. Workers exit on the next wake.
    shutdown: AtomicBool,
    /// Tokio runtime the workers attach to (so `tokio::spawn` inside
    /// wrapped pipelines targets a real async runtime).
    handle: Handle,
}

static NEXT_POOL_ID: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// `Some((pool_id, worker_id))` on threads that host a worker loop;
    /// `None` elsewhere.
    static CURRENT_WORKER: Cell<Option<(usize, usize)>> = const { Cell::new(None) };

    /// FIFO side-queue used by [`spawn_local_fifo`]. Only the owner touches
    /// this — no synchronisation needed.
    static FIFO_QUEUE: RefCell<VecDeque<BoxedJob>> = RefCell::new(VecDeque::new());

    /// Raw pointer to the owning worker's LIFO deque. Set by `run_worker`
    /// before entering the loop and cleared on exit. Safe to dereference
    /// from any code running inside the worker's job execution.
    static OWNER_DEQUE: Cell<Option<*const Worker<BoxedJob>>> = const { Cell::new(None) };

    /// Handle to the pool that owns the current worker.
    static CURRENT_SHARED: RefCell<Option<Arc<Shared>>> = const { RefCell::new(None) };
}

/// Pool of OS threads with crossbeam-deque work-stealing, entered into
/// a shared tokio runtime handle.
pub struct WorkerPool {
    shared: Arc<Shared>,
    /// Join handles for the worker OS threads. Kept so [`Drop`] can cleanly
    /// wait for workers to finish.
    handles: StdMutex<Option<Vec<JoinHandle<()>>>>,
}

impl WorkerPool {
    /// Spawn `workers` OS threads attached to the given tokio runtime
    /// handle. `handle` is used by the workers to drive async futures
    /// spawned by wrapped pipelines (e.g. `tokio::spawn` inside
    /// `ExecutionPlan::execute` internals).
    pub fn new(workers: usize, handle: Handle) -> Result<Arc<Self>> {
        if workers == 0 {
            return Err(DataFusionError::Configuration(
                "WorkerPool requires at least one worker".to_string(),
            ));
        }

        let mut locals: Vec<Option<Worker<BoxedJob>>> = Vec::with_capacity(workers);
        let mut stealers = Vec::with_capacity(workers);
        for _ in 0..workers {
            let w = Worker::<BoxedJob>::new_lifo();
            stealers.push(w.stealer());
            locals.push(Some(w));
        }

        let shared = Arc::new(Shared {
            id: NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed),
            injector: Injector::new(),
            stealers,
            wake_count: AtomicUsize::new(0),
            wake_mutex: StdMutex::new(()),
            wake_cvar: Condvar::new(),
            shutdown: AtomicBool::new(false),
            handle,
        });

        let mut handles = Vec::with_capacity(workers);
        for (i, slot) in locals.iter_mut().enumerate() {
            let shared_clone = Arc::clone(&shared);
            let local = slot.take().expect("local worker deque missing");
            let th = std::thread::Builder::new()
                .name(format!("push-sched-worker-{i}"))
                .spawn(move || {
                    run_worker(i, &shared_clone, &local);
                    // `local` lives until here, dropped after the worker
                    // loop exits so any in-flight stealers see an empty
                    // deque.
                    drop(local);
                })
                .map_err(|e| {
                    DataFusionError::External(
                        format!("failed to spawn push-scheduler worker: {e}").into(),
                    )
                })?;
            handles.push(th);
        }

        Ok(Arc::new(Self {
            shared,
            handles: StdMutex::new(Some(handles)),
        }))
    }

    pub fn worker_count(&self) -> usize {
        self.shared.stealers.len()
    }

    pub fn id(&self) -> usize {
        self.shared.id
    }

    /// Submit a job to the shared injector.
    pub fn spawn(&self, job: BoxedJob) {
        self.shared.injector.push(job);
        self.shared.wake_one();
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::SeqCst);
        self.shared.wake_all();
        if let Some(handles) = self.handles.lock().unwrap().take() {
            for h in handles {
                let _ = h.join();
            }
        }
    }
}

impl Shared {
    /// Bump the wake counter and notify one parker.
    fn wake_one(&self) {
        self.wake_count.fetch_add(1, Ordering::SeqCst);
        // Holding the mutex while notifying is the standard Condvar
        // pattern — prevents a lost wake-up when a parker is in between
        // checking the counter and calling wait().
        let _guard = self.wake_mutex.lock().unwrap();
        self.wake_cvar.notify_one();
    }

    fn wake_all(&self) {
        self.wake_count.fetch_add(1, Ordering::SeqCst);
        let _guard = self.wake_mutex.lock().unwrap();
        self.wake_cvar.notify_all();
    }
}

fn run_worker(my_id: usize, shared: &Arc<Shared>, local: &Worker<BoxedJob>) {
    // Attach this thread to the shared tokio runtime so `tokio::spawn`
    // calls inside pipeline futures target a real runtime.
    let _enter = shared.handle.enter();

    CURRENT_WORKER.with(|cell| cell.set(Some((shared.id, my_id))));
    OWNER_DEQUE.with(|cell| cell.set(Some(local as *const _)));
    CURRENT_SHARED.with(|cell| *cell.borrow_mut() = Some(Arc::clone(shared)));

    let mut observed_wake = shared.wake_count.load(Ordering::SeqCst);
    loop {
        if shared.shutdown.load(Ordering::SeqCst) {
            break;
        }
        if let Some(job) = find_job(local, shared, my_id) {
            job();
            continue;
        }

        // No work. Park on the wake counter.
        let current = shared.wake_count.load(Ordering::SeqCst);
        if current != observed_wake {
            // A wake has happened since we last looked — recheck queues.
            observed_wake = current;
            continue;
        }
        let mut guard = shared.wake_mutex.lock().unwrap();
        while shared.wake_count.load(Ordering::SeqCst) == observed_wake
            && !shared.shutdown.load(Ordering::SeqCst)
        {
            // Brief timeout prevents any pathological wake-up loss from
            // wedging the pool forever.
            let (g, _timeout) = shared
                .wake_cvar
                .wait_timeout(guard, Duration::from_millis(50))
                .unwrap();
            guard = g;
        }
        observed_wake = shared.wake_count.load(Ordering::SeqCst);
    }

    FIFO_QUEUE.with(|q| q.borrow_mut().clear());
    OWNER_DEQUE.with(|cell| cell.set(None));
    CURRENT_WORKER.with(|cell| cell.set(None));
    CURRENT_SHARED.with(|cell| *cell.borrow_mut() = None);
}

#[inline]
fn find_job(local: &Worker<BoxedJob>, shared: &Shared, my_id: usize) -> Option<BoxedJob> {
    if let Some(job) = FIFO_QUEUE.with(|q| q.borrow_mut().pop_front()) {
        return Some(job);
    }
    if let Some(job) = local.pop() {
        return Some(job);
    }
    loop {
        match shared.injector.steal_batch_and_pop(local) {
            Steal::Success(job) => return Some(job),
            Steal::Empty => break,
            Steal::Retry => continue,
        }
    }
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

// ---------------------------------------------------------------------------
// Thread-local helpers used by `scheduler::spawn_local{_fifo}`.
// ---------------------------------------------------------------------------

pub fn is_worker() -> bool {
    CURRENT_WORKER.with(|cell| cell.get().is_some())
}

pub fn current_worker() -> Option<(usize, usize)> {
    CURRENT_WORKER.with(|cell| cell.get())
}

/// Push a job onto the current worker's LIFO deque. Falls back to the
/// shared injector if called off a worker thread or if no pool context
/// is attached to this thread.
pub(crate) fn spawn_local(job: BoxedJob) {
    let ptr = OWNER_DEQUE.with(|cell| cell.get());
    let shared = CURRENT_SHARED.with(|cell| cell.borrow().clone());

    match ptr {
        Some(ptr) => {
            // SAFETY: the worker thread sets this pointer to its own local
            // deque before entering the loop and clears it on exit; any
            // code running on this thread sees a valid pointer.
            let worker: &Worker<BoxedJob> = unsafe { &*ptr };
            worker.push(job);
            // Don't wake peers — preserves cache locality. Owner picks
            // this up on its next iteration (LIFO).
            let _ = shared;
        }
        None => match shared {
            Some(shared) => {
                shared.injector.push(job);
                shared.wake_one();
            }
            None => {
                drop(job);
                log::error!("spawn_local called outside a pool worker; job dropped");
            }
        },
    }
}

/// Push a job onto the current worker's FIFO side-queue (non-stealable).
/// Falls back to [`spawn_local`] when called off a worker thread.
pub(crate) fn spawn_local_fifo(job: BoxedJob) {
    let on_worker = OWNER_DEQUE.with(|c| c.get().is_some());
    if on_worker {
        FIFO_QUEUE.with(|q| q.borrow_mut().push_back(job));
    } else {
        spawn_local(job);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::instant::Instant;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::oneshot;

    #[test]
    fn spawn_runs_on_a_worker() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = WorkerPool::new(3, Handle::current()).unwrap();
            let (tx, rx) = oneshot::channel();
            pool.spawn(Box::new(move || {
                let _ = tx.send(is_worker());
            }));
            assert!(rx.await.unwrap());
        });
    }

    #[test]
    fn many_jobs_run_in_parallel() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = WorkerPool::new(4, Handle::current()).unwrap();
            let counter = Arc::new(AtomicUsize::new(0));
            let start = Instant::now();
            let mut rxs = Vec::new();
            for _ in 0..16 {
                let (tx, rx) = oneshot::channel();
                let c = Arc::clone(&counter);
                pool.spawn(Box::new(move || {
                    std::thread::sleep(Duration::from_millis(50));
                    c.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.send(());
                }));
                rxs.push(rx);
            }
            for rx in rxs {
                rx.await.unwrap();
            }
            let elapsed = start.elapsed();
            assert_eq!(counter.load(Ordering::Relaxed), 16);
            assert!(
                elapsed < Duration::from_millis(400),
                "expected 16 x 50ms jobs to fan across 4 workers; got {elapsed:?}"
            );
        });
    }

    #[test]
    fn spawn_local_fifo_inside_worker_uses_side_queue() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let pool = WorkerPool::new(1, Handle::current()).unwrap();
            let (tx, rx) = oneshot::channel();
            pool.spawn(Box::new(move || {
                spawn_local_fifo(Box::new(move || {
                    let _ = tx.send(is_worker());
                }));
            }));
            assert!(rx.await.unwrap());
        });
    }
}
