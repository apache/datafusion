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

use std::any::Any;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};

use datafusion_common::{HashMap, HashSet};
use datafusion_common_runtime::{JoinSetTracer, set_join_set_tracer};
use futures::future::BoxFuture;
use tokio::sync::{Mutex, MutexGuard};

/// Initializes the global join set tracer with the asserting tracer.
/// Call this function before spawning any tasks that should be traced.
pub fn init_asserting_tracer() {
    set_join_set_tracer(ASSERTING_TRACER.deref())
        .expect("Failed to initialize asserting tracer");
}

/// Verifies that the current task has a traceable ancestry back to "root".
///
/// The function performs a breadth-first search (BFS) in the global spawn graph:
/// - It starts at the current task and follows parent links.
/// - If it reaches the "root" task, the ancestry is valid.
/// - If a task is missing from the graph, it panics.
///
/// Note: Tokio task IDs are unique only while a task is active.
/// Once a task completes, its ID may be reused.
pub async fn assert_traceability() {
    // Acquire the spawn graph lock.
    let spawn_graph = acquire_spawn_graph().await;

    // Start BFS with the current task.
    let mut tasks_to_check = VecDeque::from(vec![current_task()]);

    while let Some(task_id) = tasks_to_check.pop_front() {
        if task_id == "root" {
            // Ancestry reached the root.
            continue;
        }
        // Obtain parent tasks, panicking if the task is not present.
        let parents = spawn_graph
            .get(&task_id)
            .expect("Task ID not found in spawn graph");
        // Queue each parent for checking.
        for parent in parents {
            tasks_to_check.push_back(parent.clone());
        }
    }
}

/// Tracer that maintains a graph of task ancestry for tracing purposes.
///
/// For each task, it records a set of parent task IDs to ensure that every
/// asynchronous task can be traced back to "root".
struct AssertingTracer {
    /// An asynchronous map from task IDs to their parent task IDs.
    spawn_graph: Arc<Mutex<HashMap<String, HashSet<String>>>>,
}

/// Lazily initialized global instance of `AssertingTracer`.
static ASSERTING_TRACER: LazyLock<AssertingTracer> = LazyLock::new(AssertingTracer::new);

impl AssertingTracer {
    /// Creates a new `AssertingTracer` with an empty spawn graph.
    fn new() -> Self {
        Self {
            spawn_graph: Arc::default(),
        }
    }
}

/// Returns the current task's ID as a string, or "root" if unavailable.
///
/// Tokio guarantees task IDs are unique only among active tasks,
/// so completed tasks may have their IDs reused.
fn current_task() -> String {
    tokio::task::try_id()
        .map(|id| format!("{id}"))
        .unwrap_or_else(|| "root".to_string())
}

/// Asynchronously locks and returns the spawn graph.
///
/// The returned guard allows inspection or modification of task ancestry.
async fn acquire_spawn_graph<'a>() -> MutexGuard<'a, HashMap<String, HashSet<String>>> {
    ASSERTING_TRACER.spawn_graph.lock().await
}

/// Registers the current task as a child of `parent_id` in the spawn graph.
async fn register_task(parent_id: String) {
    acquire_spawn_graph()
        .await
        .entry(current_task())
        .or_insert_with(HashSet::new)
        .insert(parent_id);
}

impl JoinSetTracer for AssertingTracer {
    /// Wraps an asynchronous future to record its parent task before execution.
    fn trace_future(
        &self,
        fut: BoxFuture<'static, Box<dyn Any + Send>>,
    ) -> BoxFuture<'static, Box<dyn Any + Send>> {
        // Capture the parent task ID.
        let parent_id = current_task();
        Box::pin(async move {
            // Register the parent-child relationship.
            register_task(parent_id).await;
            // Execute the wrapped future.
            fut.await
        })
    }

    /// Wraps a blocking closure to record its parent task before execution.
    fn trace_block(
        &self,
        f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> {
        let parent_id = current_task();
        Box::new(move || {
            // Synchronously record the task relationship.
            futures::executor::block_on(register_task(parent_id));
            f()
        })
    }
}
