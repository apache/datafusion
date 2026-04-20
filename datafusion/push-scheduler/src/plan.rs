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

//! Planner that lowers an [`ExecutionPlan`] into a flat [`PipelinePlan`].
//!
//! v1 lowers the entire `ExecutionPlan` tree as a single
//! [`ExecutionPipeline`], so the scheduler executes via the default
//! pull-based path wrapped in a push-scheduler task. Breaker cuts
//! ([`RepartitionPipeline`](crate::pipelines::repartition::RepartitionPipeline),
//! [`SortPipeline`](crate::pipelines::sort::SortPipeline)) are
//! implemented but not yet wired in — PR apache/datafusion#2226 relies
//! on an "Inbox" mechanism to splice the scheduler's push output into
//! the wrapped subtree's leaf; porting that is tracked as the next step.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::pipeline::Pipeline;
use crate::pipelines::execution::ExecutionPipeline;

/// Points a pipeline's output at a specific input of another pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputLink {
    /// Index of the consuming pipeline in [`PipelinePlan::pipelines`].
    pub pipeline: usize,
    /// Which input (child) of the consuming pipeline to push into.
    pub child: usize,
}

/// A [`Pipeline`] paired with an [`OutputLink`] describing where to send
/// its output. `output == None` means the pipeline feeds the query's final
/// result stream.
pub struct RoutablePipeline {
    pub pipeline: Box<dyn Pipeline>,
    pub output: Option<OutputLink>,
}

impl std::fmt::Debug for RoutablePipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutablePipeline")
            .field("pipeline", &self.pipeline)
            .field("output", &self.output)
            .finish()
    }
}

/// Scheduler-facing representation of a compiled query: a flat list of
/// pipelines with routing info.
#[derive(Debug)]
pub struct PipelinePlan {
    pub schema: SchemaRef,
    pub output_partitions: usize,
    pub pipelines: Vec<RoutablePipeline>,
}

/// Lowers an [`ExecutionPlan`] into a [`PipelinePlan`].
pub struct PipelinePlanner {
    task_context: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
}

impl PipelinePlanner {
    pub fn new(plan: Arc<dyn ExecutionPlan>, task_context: Arc<TaskContext>) -> Self {
        Self { plan, task_context }
    }

    /// Wrap the entire `ExecutionPlan` tree as a single
    /// [`ExecutionPipeline`].
    pub fn build(self) -> Result<PipelinePlan> {
        let schema = self.plan.schema();
        let output_partitions = self.plan.output_partitioning().partition_count();
        let pipeline = ExecutionPipeline::new(self.plan, self.task_context)?;
        Ok(PipelinePlan {
            schema,
            output_partitions,
            pipelines: vec![RoutablePipeline {
                pipeline: Box::new(pipeline),
                output: None,
            }],
        })
    }
}
