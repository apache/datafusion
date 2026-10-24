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

//! Running the plans under test: on which runtime and task context, with a
//! timeout, and with panics caught.

use std::any::Any;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use datafusion_common::instant::Instant;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::MemoryPool;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_plan::execution_plan::reset_plan_states;
use datafusion_physical_plan::{ExecutionPlan, collect_partitioned};
use futures::{FutureExt, StreamExt};

use crate::physical_plan::NodeOutput;

/// A current-thread runtime for executing plans from synchronous code. With
/// one thread, execution is deterministic, although some operators produce
/// output that depends on how tasks interleave.
pub(crate) fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

/// A default task context with the session batch size set to `batch_size`
pub(crate) fn task_context(batch_size: usize) -> Arc<TaskContext> {
    let config = SessionConfig::new().with_batch_size(batch_size);
    Arc::new(TaskContext::default().with_session_config(config))
}

/// A default task context whose memory pool is `pool`
pub(crate) fn task_context_with_pool(
    pool: Arc<dyn MemoryPool>,
) -> Result<Arc<TaskContext>> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()?;
    Ok(Arc::new(TaskContext::default().with_runtime(runtime)))
}

/// Why a run of a plan did not produce its output
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunError {
    /// The plan returned an error, or could not be built
    Failed(String),
    /// The plan panicked
    Panicked(String),
    /// The plan did not finish within this time
    TimedOut(Duration),
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunError::Failed(error) => write!(f, "{error}"),
            RunError::Panicked(message) => write!(f, "panicked: {message}"),
            RunError::TimedOut(timeout) => write!(f, "did not finish within {timeout:?}"),
        }
    }
}

/// Execute every partition of `plan` concurrently and collect the output.
/// Errors, panics and running longer than `timeout` are returned as a
/// [`RunError`].
pub(crate) async fn collect_output(
    plan: Arc<dyn ExecutionPlan>,
    task_context: Arc<TaskContext>,
    timeout: Duration,
) -> Result<NodeOutput, RunError> {
    let future = AssertUnwindSafe(collect_partitioned(plan, task_context)).catch_unwind();
    match tokio::time::timeout(timeout, future).await {
        Ok(Ok(Ok(partitions))) => Ok(NodeOutput::new(partitions)),
        Ok(Ok(Err(e))) => Err(RunError::Failed(e.strip_backtrace())),
        Ok(Err(panic)) => Err(RunError::Panicked(panic_message(&panic))),
        Err(_) => Err(RunError::TimedOut(timeout)),
    }
}

/// Execute partition `partition_count` of a fresh copy of `node`, which does
/// not exist, and poll the stream it returns once, catching panics. Returns
/// `None` if the node returned an error, from `execute` or as the first item
/// of the stream, or if the copy could not be made (the normal execution then
/// fails too, which `execution_succeeds` reports), and otherwise what the
/// node did instead, such as `panicked: ...`.
pub(crate) async fn execute_invalid_partition(
    node: &Arc<dyn ExecutionPlan>,
    timeout: Duration,
) -> Option<String> {
    let node = reset_plan_states(Arc::clone(node)).ok()?;
    let partition = node.properties().output_partitioning().partition_count();
    let task_context = Arc::new(TaskContext::default());
    let execute = std::panic::catch_unwind(AssertUnwindSafe(|| {
        node.execute(partition, task_context)
    }));
    let mut stream = match execute {
        Ok(Ok(stream)) => stream,
        Ok(Err(_)) => return None,
        Err(panic) => return Some(format!("panicked: {}", panic_message(&panic))),
    };
    let first = AssertUnwindSafe(stream.next()).catch_unwind();
    match tokio::time::timeout(timeout, first).await {
        Ok(Ok(Some(Err(_)))) => None,
        Ok(Ok(Some(Ok(batch)))) => Some(format!(
            "returned a stream that produced a batch of {} rows",
            batch.num_rows()
        )),
        Ok(Ok(None)) => Some("returned a stream that ended without an error".into()),
        Ok(Err(panic)) => Some(format!(
            "returned a stream that panicked when polled: {}",
            panic_message(&panic)
        )),
        Err(_) => Some(format!(
            "returned a stream that produced nothing within {timeout:?}"
        )),
    }
}

pub(crate) fn panic_message(panic: &Box<dyn Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// Wait until `released` returns true, for example once streams or memory a
/// node no longer needs are released, or until `timeout` elapses.
///
/// Such resources are released either synchronously, when what owns them is
/// dropped, or later by code running in a task, such as a spawned task that
/// notices that its receiver was dropped, or an aborted task that the runtime
/// drops. Once no task is alive on the current Tokio runtime, nothing is left
/// that could release them, so the wait ends early instead of taking the
/// whole timeout. Tokio drops a task's future before it stops counting the
/// task as alive. Work on threads outside the runtime, including
/// `spawn_blocking` tasks, which Tokio does not count, is not waited for once
/// no task is alive.
pub(crate) async fn wait_for_release(released: impl Fn() -> bool, timeout: Duration) {
    let runtime = tokio::runtime::Handle::current();
    let start = Instant::now();
    while !released()
        && start.elapsed() < timeout
        && runtime.metrics().num_alive_tasks() > 0
    {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
