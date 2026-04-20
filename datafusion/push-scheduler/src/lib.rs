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

//! Push-based, morsel-driven scheduler for Apache DataFusion.
//!
//! This crate is a port of the experimental scheduler from
//! [apache/datafusion#2226](https://github.com/apache/datafusion/pull/2226)
//! onto plain `tokio` + `crossbeam-deque` (the original used `rayon`).
//!
//! # Architecture
//!
//! * A [`PipelinePlanner`] compiles an
//!   [`ExecutionPlan`](datafusion_physical_plan::ExecutionPlan) into a flat
//!   [`PipelinePlan`] — a vector of [`RoutablePipeline`]s plus their
//!   [`OutputLink`]s. Linear chains of pull-based operators are grouped
//!   into an [`ExecutionPipeline`](pipelines::execution::ExecutionPipeline);
//!   `RepartitionExec`, `CoalescePartitionsExec`, `SortExec`, and partial
//!   `AggregateExec` become dedicated breaker pipelines.
//!
//! * A [`Scheduler`] owns a pool of worker OS threads, each running a tokio
//!   `current_thread` runtime + `LocalSet` and pulling tasks from a
//!   `crossbeam_deque` work-stealing queue. Idle workers steal from peers;
//!   external submissions go through a shared `Injector`.
//!
//! * A `Task = (pipeline_idx, partition)` is scheduled per output partition.
//!   Each worker iteration calls
//!   [`Pipeline::poll_partition`](pipeline::Pipeline::poll_partition) on the
//!   task's pipeline and routes the result to the downstream pipeline's
//!   `push` / `close`. `Poll::Pending` parks the task on a
//!   [`ArcWake`](futures::task::ArcWake) that re-enqueues it when the
//!   underlying future wakes.
//!
//! # Entry point
//!
//! ```ignore
//! use std::sync::Arc;
//! use datafusion_push_scheduler::Scheduler;
//!
//! # async fn run(plan: std::sync::Arc<dyn datafusion_physical_plan::ExecutionPlan>,
//! #              ctx: std::sync::Arc<datafusion_execution::TaskContext>)
//! #   -> datafusion_common::Result<()> {
//! let scheduler = Scheduler::new(num_cpus::get());
//! let results = scheduler.schedule(plan, ctx)?;
//! let mut stream = results.stream();
//! while let Some(batch) = futures::StreamExt::next(&mut stream).await {
//!     let _ = batch?;
//! }
//! # Ok(()) }
//! ```

#![deny(clippy::clone_on_ref_ptr)]

pub mod pipeline;
pub mod pipelines;
pub mod plan;
pub mod scheduler;
pub mod task;
pub mod worker_pool;

pub use pipeline::Pipeline;
pub use pipelines::execution::ExecutionPipeline;
pub use pipelines::repartition::RepartitionPipeline;
pub use plan::{OutputLink, PipelinePlan, PipelinePlanner, RoutablePipeline};
pub use scheduler::Scheduler;
pub use task::{ExecutionResults, spawn_plan};
