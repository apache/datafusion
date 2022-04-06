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

//Inspire by https://thenewstack.io/using-rustlangs-async-tokio-runtime-for-cpu-bound-tasks/

//! This module contains a dedicated thread pool for running "cpu
//! intensive" workloads as query plans

use log::warn;
use parking_lot::Mutex;
use std::{pin::Pin, sync::Arc};
use tokio::sync::oneshot::Receiver;

use futures::Future;

/// The type of thing that the dedicated executor runs
type Task = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Runs futures (and any `tasks` that are `tokio::task::spawned` by
/// them) on a separate tokio runtime, like separate CPU-bound (execute a datafusion plan) tasks
/// from IO-bound tasks(heartbeats). Get more from the above blog.
#[derive(Clone)]
pub struct DedicatedExecutor {
    state: Arc<Mutex<State>>,
}

/// Runs futures (and any `tasks` that are `tokio::task::spawned` by
/// them) on a separate tokio Executor
struct State {
    /// The number of threads in this pool
    num_threads: usize,

    /// The name of the threads for this executor
    thread_name: String,

    /// Channel for requests -- the dedicated executor takes requests
    /// from here and runs them.
    requests: Option<std::sync::mpsc::Sender<Task>>,

    /// The thread that is doing the work
    thread: Option<std::thread::JoinHandle<()>>,
}

/// The default worker priority (value passed to `libc::setpriority`);
const WORKER_PRIORITY: i32 = 10;

impl std::fmt::Debug for DedicatedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();

        let mut d = f.debug_struct("DedicatedExecutor");

        d.field("num_threads", &state.num_threads)
            .field("thread_name", &state.thread_name);

        if state.requests.is_some() {
            d.field("requests", &"Some(...)")
        } else {
            d.field("requests", &"None")
        };

        if state.thread.is_some() {
            d.field("thread", &"Some(...)")
        } else {
            d.field("thread", &"None")
        };

        d.finish()
    }
}

impl DedicatedExecutor {
    /// https://stackoverflow.com/questions/62536566
    /// Creates a new `DedicatedExecutor` with a dedicated tokio
    /// runtime that is separate from the `[tokio::main]` threadpool.
    ///
    /// The worker thread priority is set to low so that such tasks do
    /// not starve other more important tasks (such as answering health checks)
    ///
    pub fn new(thread_name: impl Into<String>, num_threads: usize) -> Self {
        let thread_name = thread_name.into();
        let name_copy = thread_name.to_string();

        let (tx, rx) = std::sync::mpsc::channel();

        // Cannot create a separated tokio runtime in another tokio runtime,
        // So use std::thread to spawn a thread
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name(&name_copy)
                .worker_threads(num_threads)
                .on_thread_start(move || set_current_thread_priority(WORKER_PRIORITY))
                .build()
                .expect("Creating tokio runtime");

            // By entering the context, all calls to `tokio::spawn` go
            // to this executor
            let _guard = runtime.enter();

            while let Ok(request) = rx.recv() {
                // TODO feedback request status
                tokio::task::spawn(request);
            }
        });

        let state = State {
            num_threads,
            thread_name,
            requests: Some(tx),
            thread: Some(thread),
        };

        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    /// Runs the specified Future (and any tasks it spawns) on the
    /// `DedicatedExecutor`.
    ///
    /// Currently all tasks are added to the tokio executor
    /// immediately and compete for the threadpool's resources.
    pub fn spawn<T>(&self, task: T) -> Receiver<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();

        // create a execution plan to spawn
        let job = Box::pin(async move {
            let task_output = task.await;
            if tx.send(task_output).is_err() {
                warn!("Spawned task output ignored: receiver dropped");
            }
        });

        let mut state = self.state.lock();

        if let Some(requests) = &mut state.requests {
            // would fail if someone has started shutdown
            requests.send(job).ok();
        } else {
            warn!("tried to schedule task on an executor that was shutdown");
        }

        rx
    }

    /// signals shutdown of this executor and any Clones
    #[allow(dead_code)]
    pub fn shutdown(&self) {
        // hang up the channel which will cause the dedicated thread
        // to quit
        let mut state = self.state.lock();
        // remaining job will still running
        state.requests = None;
    }

    /// Stops all subsequent task executions, and waits for the worker
    /// thread to complete. Note this will shutdown all clones of this
    /// `DedicatedExecutor` as well.
    ///
    /// Only the first one to `join` will actually wait for the
    /// executing thread to complete. All other calls to join will
    /// complete immediately.
    #[allow(dead_code)]
    pub fn join(&self) {
        self.shutdown();

        // take the thread out when mutex is held
        let thread = {
            let mut state = self.state.lock();
            state.thread.take()
        };

        // wait for completion while not holding the mutex to avoid
        // deadlocks
        if let Some(thread) = thread {
            thread.join().ok();
        }
    }
}

#[cfg(unix)]
fn set_current_thread_priority(prio: i32) {
    unsafe { libc::setpriority(0, 0, prio) };
}

#[cfg(not(unix))]
fn set_current_thread_priority(_prio: i32) {
    warn!("Setting worker thread priority not supported on this platform");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[cfg(unix)]
    fn get_current_thread_priority() -> i32 {
        // on linux setpriority sets the current thread's priority
        // (as opposed to the current process).
        unsafe { libc::getpriority(0, 0) }
    }

    #[cfg(not(unix))]
    fn get_current_thread_priority() -> i32 {
        WORKER_PRIORITY
    }

    #[tokio::test]
    async fn basic_test_in_diff_thread() {
        let barrier = Arc::new(Barrier::new(2));

        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);
        let dedicated_task = exec.spawn(do_work(42, Arc::clone(&barrier)));

        // Note the dedicated task will never complete if it runs on
        // the main tokio thread
        //#[tokio::test] will only create one thread, if we running use tokio spwan
        // after call do_work with barrier.wait() the only thread will be blocked and never finished
        barrier.wait();

        // should be able to get the result
        assert_eq!(dedicated_task.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn basic_clone() {
        let barrier = Arc::new(Barrier::new(2));
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);
        let dedicated_task = exec.clone().spawn(do_work(42, Arc::clone(&barrier)));
        barrier.wait();
        assert_eq!(dedicated_task.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn multi_task() {
        let barrier = Arc::new(Barrier::new(3));

        // make an executor with two threads
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 2);
        let dedicated_task1 = exec.spawn(do_work(11, Arc::clone(&barrier)));
        let dedicated_task2 = exec.spawn(do_work(42, Arc::clone(&barrier)));

        // block main thread until completion of other two tasks
        barrier.wait();

        // should be able to get the result
        assert_eq!(dedicated_task1.await.unwrap(), 11);
        assert_eq!(dedicated_task2.await.unwrap(), 42);

        exec.join();
    }

    #[tokio::test]
    async fn worker_priority() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 2);

        let dedicated_task = exec.spawn(async move { get_current_thread_priority() });

        assert_eq!(dedicated_task.await.unwrap(), WORKER_PRIORITY);
    }

    #[tokio::test]
    async fn tokio_spawn() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 2);

        // spawn a task that spawns to other tasks and ensure they run on the dedicated
        // executor
        let dedicated_task = exec.spawn(async move {
            // spawn separate tasks
            let t1 = tokio::task::spawn(async {
                assert_eq!(
                    std::thread::current().name(),
                    Some("Test DedicatedExecutor")
                );
                25usize
            });
            t1.await.unwrap()
        });

        assert_eq!(dedicated_task.await.unwrap(), 25);
    }

    #[tokio::test]
    async fn panic_on_executor() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);
        let dedicated_task = exec.spawn(async move {
            panic!("At the disco, on the dedicated task scheduler");
        });

        // should not be able to get the result
        dedicated_task.await.unwrap_err();
    }

    #[tokio::test]
    #[ignore]
    // related https://github.com/apache/arrow-datafusion/issues/2140
    async fn executor_shutdown_while_task_running() {
        let barrier = Arc::new(Barrier::new(2));

        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);
        let dedicated_task = exec.spawn(do_work(42, Arc::clone(&barrier)));

        exec.shutdown();
        // block main thread until completion of the outstanding task
        barrier.wait();

        // task should complete successfully
        assert_eq!(dedicated_task.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn executor_submit_task_after_shutdown() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);

        // Simulate trying to submit tasks once executor has shutdown
        exec.shutdown();
        let dedicated_task = exec.spawn(async { 11 });

        // task should complete, but return an error
        dedicated_task.await.unwrap_err();
    }

    #[tokio::test]
    async fn executor_submit_task_after_clone_shutdown() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);

        // shutdown the clone (but not the exec)
        exec.clone().join();

        // Simulate trying to submit tasks once executor has shutdown
        let dedicated_task = exec.spawn(async { 11 });

        // task should complete, but return an error
        dedicated_task.await.unwrap_err();
    }

    #[tokio::test]
    async fn executor_join() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);
        // test it doesn't hang
        exec.join()
    }

    #[tokio::test]
    #[allow(clippy::redundant_clone)]
    async fn executor_clone_join() {
        let exec = DedicatedExecutor::new("Test DedicatedExecutor", 1);
        // test not hang
        exec.clone().join();
        exec.clone().join();
        exec.join();
    }

    /// Wait for the barrier and then return `result`
    async fn do_work(result: usize, barrier: Arc<Barrier>) -> usize {
        barrier.wait();
        result
    }
}
