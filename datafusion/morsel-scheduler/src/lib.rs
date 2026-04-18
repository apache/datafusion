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

//! Thread-per-core morsel-driven scheduler prototype for DataFusion.
//!
//! This crate is an experimental runtime that executes a DataFusion
//! [`ExecutionPlan`](datafusion_physical_plan::ExecutionPlan) on top of
//! a pool of pinned **per-core** Tokio `current_thread` runtimes
//! instead of the default multi-threaded runtime.
//!
//! # Architecture
//!
//! The scheduler has three layers:
//!
//! * [`WorkerPool`] — `N` OS threads, each owning a
//!   `tokio::runtime::Runtime` built with `Builder::new_current_thread`.
//!   Work is shipped to a worker as a `Send` closure that constructs
//!   the real (possibly `!Send`) future **on the worker**, so the
//!   future never migrates across threads.
//!
//! * **Pipelines** — a [`Pipeline`](pipeline::Pipeline) is a subgraph
//!   of an `ExecutionPlan` with no internal *pipeline breaker*. The
//!   [`planner`] cuts the plan at breakers (currently
//!   [`CoalescePartitionsExec`](datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec)
//!   and
//!   [`SortPreservingMergeExec`](datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec))
//!   and rewires upstream/downstream halves through
//!   [`InboxSourceExec`](inbox::InboxSourceExec) leaves fed by per-core
//!   mpsc channels.
//!
//! * **Execution** — [`execute`] runs each pipeline partition on its
//!   assigned worker, streaming morsels into the inboxes for the next
//!   pipeline. The top pipeline's output is bridged back to the caller
//!   as a normal `SendableRecordBatchStream`.
//!
//! # Scope
//!
//! * Cuts at `CoalescePartitionsExec`, `SortPreservingMergeExec`,
//!   `RepartitionExec`, and `SortExec`. For `RepartitionExec` the
//!   input partitions become their own upstream pipeline and the
//!   repartition shuffle reads from inboxes — so scans and filters
//!   pipeline fully in parallel while only the (cheap) hashing /
//!   round-robin logic runs inside the repartition's fetcher task.
//! * [`WorkerDispatchExec`](dispatch::WorkerDispatchExec) wraps each
//!   pipeline's leaves so that any remaining `RepartitionExec` (or
//!   other fan-in operator) inside a pipeline also sees its partition
//!   scans distributed across workers.
//! * `HashJoinExec` build-side and `NestedLoopJoinExec` are not yet
//!   treated as explicit cut points — they remain internal to a
//!   pipeline.
//!
//! # Entry point
//!
//! ```ignore
//! use std::sync::Arc;
//! use datafusion_morsel_scheduler::{WorkerPool, execute};
//!
//! # async fn run(plan: std::sync::Arc<dyn datafusion_physical_plan::ExecutionPlan>,
//! #              task_ctx: std::sync::Arc<datafusion_execution::TaskContext>)
//! #     -> datafusion_common::Result<()> {
//! let pool = WorkerPool::new(num_cpus::get())?;
//! let mut stream = execute(plan, task_ctx, &pool)?;
//! while let Some(batch) = futures::StreamExt::next(&mut stream).await {
//!     let _ = batch?;
//! }
//! # Ok(()) }
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(clippy::clone_on_ref_ptr)]

pub mod dispatch;
pub mod executor;
pub mod inbox;
pub mod pipeline;
pub mod planner;
pub mod runtime;

pub use dispatch::WorkerDispatchExec;
pub use executor::execute;
pub use runtime::WorkerPool;
