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

//! [`Pipeline`] — a breaker-free subgraph of an `ExecutionPlan`.
//!
//! A pipeline is the unit the scheduler dispatches: each of its output
//! partitions becomes a task pinned to a worker. Leaves of a pipeline
//! are either true data sources ([`DataSourceExec`] et al.) or
//! [`InboxSourceExec`](crate::inbox::InboxSourceExec) stubs that read
//! morsels from an upstream pipeline.

use std::sync::Arc;

use datafusion_physical_plan::ExecutionPlan;

use crate::inbox::InboxSender;

/// One unit of scheduling. Represents a breaker-free
/// [`ExecutionPlan`] subgraph. Its output partitions are dispatched
/// across [`crate::runtime::WorkerPool`] workers.
pub struct Pipeline {
    /// The (possibly rewritten) plan to execute.
    pub plan: Arc<dyn ExecutionPlan>,
    /// If `Some`, each output partition's morsels are forwarded into
    /// the matching [`InboxSender`] (feeding a downstream pipeline).
    /// If `None`, this is the final pipeline and its output flows back
    /// to the caller.
    pub output_senders: Option<Vec<InboxSender>>,
}

impl Pipeline {
    pub fn partition_count(&self) -> usize {
        self.plan.properties().partitioning.partition_count()
    }
}

/// The set of pipelines produced by cutting a plan at every breaker.
///
/// The entry at `final_pipeline` is the one whose output is returned
/// to the caller; its `output_senders` is `None`. All other pipelines
/// feed inboxes consumed by pipelines closer to the root.
pub struct PipelineGraph {
    pub pipelines: Vec<Pipeline>,
    pub final_pipeline: usize,
}
