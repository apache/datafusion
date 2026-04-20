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

//! Public [`Scheduler`] entry point and task-submission primitives used by
//! [`task`](crate::task).

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::ExecutionPlan;
use tokio::runtime::Handle;

use crate::plan::{PipelinePlan, PipelinePlanner};
use crate::task::{ExecutionResults, Task, spawn_plan};
use crate::worker_pool::{self, WorkerPool};

/// Handle for submitting [`Task`]s to the worker pool. Cheap to clone.
#[derive(Clone)]
pub struct Spawner {
    pool: Arc<WorkerPool>,
}

impl std::fmt::Debug for Spawner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spawner")
            .field("workers", &self.pool.worker_count())
            .finish()
    }
}

impl Spawner {
    pub(crate) fn new(pool: Arc<WorkerPool>) -> Self {
        Self { pool }
    }

    /// Submit a [`Task`] from outside a worker thread. Goes through the
    /// shared injector; any idle worker may pick it up.
    pub fn spawn(&self, task: Task) {
        self.pool.spawn(Box::new(move || task.do_work()));
    }

    /// Pool id — matches the id stored in each worker's thread-local.
    pub fn pool_id(&self) -> usize {
        self.pool.id()
    }
}

/// Push the task onto the current worker's LIFO deque. Used by the task
/// waker when woken from inside a worker thread.
pub(crate) fn spawn_local(task: Task) {
    worker_pool::spawn_local(Box::new(move || task.do_work()));
}

/// Push the task onto the current worker's FIFO side-queue. Used by
/// [`Task::do_work`] to reschedule itself *after* routing an output batch.
pub(crate) fn spawn_local_fifo(task: Task) {
    worker_pool::spawn_local_fifo(Box::new(move || task.do_work()));
}

/// Re-export for convenience.
pub use crate::worker_pool::is_worker;

/// Returns `true` if `plan`'s tree contains any operator the planner
/// would cut into a separate pipeline. Mirrors the predicates in
/// [`PipelinePlanner::visit_operator`](crate::plan::PipelinePlanner).
fn has_cut(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.downcast_ref::<RepartitionExec>().is_some()
        || plan.downcast_ref::<CoalescePartitionsExec>().is_some()
    {
        return true;
    }
    plan.children().iter().any(|c| has_cut(c))
}

/// Public, high-level scheduler handle.
///
/// Owns a [`WorkerPool`]. Queries are submitted via [`Scheduler::schedule`]
/// (from a raw `ExecutionPlan`) or [`Scheduler::schedule_plan`] (from a
/// pre-built [`PipelinePlan`]).
pub struct Scheduler {
    pool: Arc<WorkerPool>,
}

impl Scheduler {
    /// Build a scheduler backed by `worker_threads` OS threads, attached
    /// to the **current** tokio runtime. MUST be called from within a
    /// tokio runtime context — otherwise use [`Scheduler::with_handle`].
    pub fn new(worker_threads: usize) -> Result<Self> {
        let handle = Handle::current();
        Self::with_handle(worker_threads, handle)
    }

    /// Build a scheduler backed by `worker_threads` OS threads, attached
    /// to the given tokio runtime handle.
    pub fn with_handle(worker_threads: usize, handle: Handle) -> Result<Self> {
        let pool = WorkerPool::new(worker_threads, handle)?;
        Ok(Self { pool })
    }

    /// Number of worker threads backing this scheduler.
    pub fn worker_count(&self) -> usize {
        self.pool.worker_count()
    }

    /// Compile and schedule an [`ExecutionPlan`].
    ///
    /// If the plan contains no breakers (`RepartitionExec` or
    /// `CoalescePartitionsExec`) the scheduler short-circuits and
    /// returns the raw `plan.execute(p)` streams unchanged — no task
    /// queue, no channels, no worker dispatch. This avoids pure-overhead
    /// runs on simple scan/filter/project queries where cutting buys
    /// nothing.
    pub fn schedule(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<ExecutionResults> {
        if !has_cut(&plan) {
            let schema = plan.schema();
            return Ok(ExecutionResults::direct(schema, plan, context));
        }
        let pipeline_plan = PipelinePlanner::new(plan, context).build()?;
        Ok(self.schedule_plan(pipeline_plan))
    }

    /// Schedule a pre-built [`PipelinePlan`].
    pub fn schedule_plan(&self, plan: PipelinePlan) -> ExecutionResults {
        let spawner = Spawner::new(Arc::clone(&self.pool));
        spawn_plan(plan, spawner)
    }

    /// Handle to the scheduler's spawner — useful for tests that want to
    /// submit raw [`Task`]s.
    pub fn spawner(&self) -> Spawner {
        Spawner::new(Arc::clone(&self.pool))
    }
}
