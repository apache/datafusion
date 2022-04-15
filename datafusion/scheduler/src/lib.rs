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

use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt};
use log::{debug, error};

use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;

use crate::query::Query;
use crate::task::{spawn_query, Task};

use rayon::{ThreadPool, ThreadPoolBuilder};

mod pipeline;
mod query;
mod task;

/// Builder for a [`Scheduler`]
#[derive(Debug)]
pub struct SchedulerBuilder {
    inner: ThreadPoolBuilder,
}

impl SchedulerBuilder {
    /// Create a new [`SchedulerConfig`] with the provided number of threads
    pub fn new(num_threads: usize) -> Self {
        let builder = ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .panic_handler(|p| error!("{}", format_worker_panic(p)))
            .thread_name(|idx| format!("df-worker-{}", idx));

        Self { inner: builder }
    }

    /// Registers a custom panic handler
    ///
    /// Used by tests
    #[allow(dead_code)]
    fn panic_handler<H>(self, panic_handler: H) -> Self
    where
        H: Fn(Box<dyn std::any::Any + Send>) + Send + Sync + 'static,
    {
        Self {
            inner: self.inner.panic_handler(panic_handler),
        }
    }

    /// Build a new [`Scheduler`]
    fn build(self) -> Scheduler {
        Scheduler {
            pool: Arc::new(self.inner.build().unwrap()),
        }
    }
}

/// A [`Scheduler`] maintains a pool of dedicated worker threads on which
/// query execution can be scheduled. This is based on the idea of [Morsel-Driven Parallelism]
/// and is designed to decouple the execution parallelism from the parallelism expressed in
/// the physical plan as partitions.
///
/// # Implementation
///
/// When provided with an [`ExecutionPlan`] the [`Scheduler`] first breaks it up into smaller
/// chunks called pipelines. Each pipeline may consist of one or more nodes from the
/// [`ExecutionPlan`] tree.
///
/// The scheduler then maintains a list of pending [`Task`], that identify a partition within
/// a particular pipeline that may be able to make progress on some "morsel" of data. These
/// [`Task`] are then scheduled on the worker pool, with a preference for scheduling work
/// on a given "morsel" on the same thread that produced it.
///
/// # Rayon
///
/// Under-the-hood these [`Task`] are scheduled by [rayon], which is a lightweight, work-stealing
/// scheduler optimised for CPU-bound workloads. Pipelines may exploit this fact, and use [rayon]'s
/// structured concurrency primitives to express additional parallelism that may be exploited
/// if there are idle threads available at runtime
///
/// # Shutdown
///
/// TBC
///
/// [Morsel-Driven Parallelism]: https://db.in.tum.de/~leis/papers/morsels.pdf
/// [rayon]: https://docs.rs/rayon/latest/rayon/
///
pub struct Scheduler {
    pool: Arc<ThreadPool>,
}

impl Scheduler {
    /// Create a new [`Scheduler`] with `num_threads` new threads in a dedicated thread pool
    pub fn new(num_threads: usize) -> Self {
        SchedulerBuilder::new(num_threads).build()
    }

    /// Schedule the provided [`ExecutionPlan`] on this [`Scheduler`].
    ///
    /// Returns a [`BoxStream`] that can be used to receive results as they are produced
    pub fn schedule(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, ArrowResult<RecordBatch>>> {
        let (query, receiver) = Query::new(plan, context, self.spawner())?;
        spawn_query(Arc::new(query));
        Ok(receiver.boxed())
    }

    fn spawner(&self) -> Spawner {
        Spawner {
            pool: self.pool.clone(),
        }
    }
}

/// Formats a panic message for a worker
fn format_worker_panic(panic: Box<dyn std::any::Any + Send>) -> String {
    let maybe_idx = rayon::current_thread_index();
    let worker: &dyn std::fmt::Display = match &maybe_idx {
        Some(idx) => idx,
        None => &"UNKNOWN",
    };

    let message = if let Some(msg) = panic.downcast_ref::<&str>() {
        *msg
    } else if let Some(msg) = panic.downcast_ref::<String>() {
        msg.as_str()
    } else {
        "UNKNOWN"
    };

    format!("worker {} panicked with: {}", worker, message)
}

/// Returns `true` if the current thread is a worker thread
fn is_worker() -> bool {
    rayon::current_thread_index().is_some()
}

/// Spawn a [`Task`] onto the local workers thread pool
///
/// There is no guaranteed order of execution, as workers may steal at any time. However,
/// `spawn_local` will append to the front of the current worker's queue, workers pop tasks from
/// the front of their queue, and steal tasks from the back of other workers queues
///
/// The effect is that tasks spawned using `spawn_local` will typically be prioritised in
/// a LIFO order, however, this should not be relied upon
fn spawn_local(task: Task) {
    // Verify is a worker thread to avoid creating a global pool
    assert!(is_worker(), "must be called from a worker");
    rayon::spawn(|| task.do_work())
}

/// Spawn a [`Task`] onto the local workers thread pool with fifo ordering
///
/// There is no guaranteed order of execution, as workers may steal at any time. However,
/// `spawn_local_fifo` will append to the back of the current worker's queue, workers pop tasks
/// from the front of their queue, and steal tasks from the back of other workers queues
///
/// The effect is that tasks spawned using `spawn_local_fifo` will typically be prioritised
/// in a FIFO order, however, this should not be relied upon
fn spawn_local_fifo(task: Task) {
    // Verify is a worker thread to avoid creating a global pool
    assert!(is_worker(), "must be called from a worker");
    rayon::spawn_fifo(|| task.do_work())
}

#[derive(Debug, Clone)]
pub struct Spawner {
    pool: Arc<ThreadPool>,
}

impl Spawner {
    pub fn spawn(&self, task: Task) {
        debug!("Spawning {:?} to any worker", task);
        self.pool.spawn(move || task.do_work());
    }
}

#[cfg(test)]
mod tests {
    use arrow::util::pretty::pretty_format_batches;
    use crossbeam_utils::sync::WaitGroup;
    use std::ops::Range;
    use std::panic::panic_any;

    use futures::TryStreamExt;
    use log::info;
    use parking_lot::Mutex;
    use rand::distributions::uniform::SampleUniform;
    use rand::{thread_rng, Rng};

    use datafusion::arrow::array::{ArrayRef, PrimitiveArray};
    use datafusion::arrow::datatypes::{ArrowPrimitiveType, Float64Type, Int32Type};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::*;

    fn generate_primitive<T, R>(
        rng: &mut R,
        len: usize,
        valid_percent: f64,
        range: Range<T::Native>,
    ) -> ArrayRef
    where
        T: ArrowPrimitiveType,
        T::Native: SampleUniform,
        R: Rng,
    {
        Arc::new(PrimitiveArray::<T>::from_iter((0..len).map(|_| {
            rng.gen_bool(valid_percent)
                .then(|| rng.gen_range(range.clone()))
        })))
    }

    fn generate_batch<R: Rng>(
        rng: &mut R,
        row_count: usize,
        id_offset: i32,
    ) -> RecordBatch {
        let id_range = id_offset..(row_count as i32 + id_offset);
        let a = generate_primitive::<Int32Type, _>(rng, row_count, 0.5, 0..1000);
        let b = generate_primitive::<Float64Type, _>(rng, row_count, 0.5, 0. ..1000.);
        let id = PrimitiveArray::<Int32Type>::from_iter_values(id_range);

        RecordBatch::try_from_iter_with_nullable([
            ("a", a, true),
            ("b", b, true),
            ("id", Arc::new(id), false),
        ])
        .unwrap()
    }

    fn make_batches() -> Vec<Vec<RecordBatch>> {
        let mut rng = thread_rng();

        let batches_per_partition = 20;
        let rows_per_batch = 100;
        let num_partitions = 2;

        let mut id_offset = 0;

        (0..num_partitions)
            .map(|_| {
                (0..batches_per_partition)
                    .map(|_| {
                        let batch = generate_batch(&mut rng, rows_per_batch, id_offset);
                        id_offset += rows_per_batch as i32;
                        batch
                    })
                    .collect()
            })
            .collect()
    }

    fn make_provider() -> Arc<dyn TableProvider> {
        let batches = make_batches();
        let schema = batches.first().unwrap().first().unwrap().schema();
        Arc::new(MemTable::try_new(schema, make_batches()).unwrap())
    }

    fn init_logging() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn test_simple() {
        init_logging();

        let scheduler = Scheduler::new(4);

        let config = SessionConfig::new().with_target_partitions(4);
        let context = SessionContext::with_config(config);

        context.register_table("table1", make_provider()).unwrap();
        context.register_table("table2", make_provider()).unwrap();

        let queries = [
            "select * from table1 order by id",
            "select * from table1 where table1.a > 100 order by id",
            "select distinct a from table1 where table1.b > 100 order by a",
            "select * from table1 join table2 on table1.id = table2.id order by table1.id",
            "select id from table1 union all select id from table2 order by id",
            "select id from table1 union all select id from table2 where a > 100 order by id",
            "select id, b from (select id, b from table1 union all select id, b from table2 where a > 100 order by id) as t where b > 10 order by id, b",
            "select id, MIN(b), MAX(b), AVG(b) from table1 group by id order by id",
            "select count(*) from table1 where table1.a > 4",
        ];

        for sql in queries {
            let task = context.task_ctx();

            let query = context.sql(sql).await.unwrap();

            let plan = query.create_physical_plan().await.unwrap();

            info!("Plan: {}", displayable(plan.as_ref()).indent());

            let stream = scheduler.schedule(plan, task).unwrap();
            let scheduled: Vec<_> = stream.try_collect().await.unwrap();
            let expected = query.collect().await.unwrap();

            let total_expected = expected.iter().map(|x| x.num_rows()).sum::<usize>();
            let total_scheduled = scheduled.iter().map(|x| x.num_rows()).sum::<usize>();
            assert_eq!(total_expected, total_scheduled);

            info!("Query \"{}\" produced {} rows", sql, total_expected);

            let expected = pretty_format_batches(&expected).unwrap().to_string();
            let scheduled = pretty_format_batches(&scheduled).unwrap().to_string();

            assert_eq!(
                expected, scheduled,
                "\n\nexpected:\n\n{}\nactual:\n\n{}\n\n",
                expected, scheduled
            );
        }
    }

    /// Combines a function with a [`WaitGroup`]
    fn wait_fn(wait: &WaitGroup, f: impl FnOnce()) -> impl FnOnce() {
        let captured = wait.clone();
        move || {
            f();
            std::mem::drop(captured);
        }
    }

    #[test]
    fn test_panic() {
        init_logging();

        let do_test = |scheduler: Scheduler| {
            let wait = WaitGroup::new();
            scheduler.pool.spawn(wait_fn(&wait, || panic!("test")));
            scheduler.pool.spawn(wait_fn(&wait, || panic!("{}", 1)));
            scheduler.pool.spawn(wait_fn(&wait, || panic_any(21)));

            wait.wait();
        };

        // The default panic handler should log panics and not abort the process
        do_test(Scheduler::new(1));

        // Override panic handler and capture panics to test formatting
        let buffer = Arc::new(Mutex::new(vec![]));
        let captured = buffer.clone();
        let scheduler = SchedulerBuilder::new(1)
            .panic_handler(move |panic| captured.lock().push(format_worker_panic(panic)))
            .build();

        do_test(scheduler);

        // Sort as order not guaranteed
        let mut buffer = buffer.lock();
        buffer.sort_unstable();

        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer[0], "worker 0 panicked with: 1");
        assert_eq!(buffer[1], "worker 0 panicked with: UNKNOWN");
        assert_eq!(buffer[2], "worker 0 panicked with: test");
    }
}
