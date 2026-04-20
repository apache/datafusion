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
//! Follows PR apache/datafusion#2226 `plan.rs`: a depth-first walk that
//! groups runs of pull-based operators into a single
//! [`ExecutionPipeline`](crate::pipelines::execution::ExecutionPipeline)
//! and cuts at:
//!
//! * [`RepartitionExec`] and [`CoalescePartitionsExec`] →
//!   [`RepartitionPipeline`](crate::pipelines::repartition::RepartitionPipeline),
//! * [`SortExec`] when not a merge-from-multiple-partitions →
//!   [`SortPipeline`](crate::pipelines::sort::SortPipeline).
//!
//! At each cut the surrounding `ExecutionPipeline` is rewritten so the
//! breaker's position in the plan tree becomes an
//! [`InboxExec`](crate::pipelines::inbox::InboxExec). The scheduler
//! pushes the breaker's output into those inboxes; the wrapped subtree
//! pulls from them as usual.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::Partitioning;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::pipeline::Pipeline;
use crate::pipelines::execution::ExecutionPipeline;
use crate::pipelines::repartition::RepartitionPipeline;
use crate::pipelines::sort::SortPipeline;

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

/// Accumulator for an `ExecutionPipeline` under construction.
struct OperatorGroup {
    /// Where the eventual ExecutionPipeline's output should be routed.
    output: Option<OutputLink>,
    /// Topmost operator in the group (the one that will be wrapped).
    root: Arc<dyn ExecutionPlan>,
    /// Number of levels we have descended past `root` into its
    /// single-child chain. When flushed, the breaker (or multi-child
    /// node) at `depth` levels below `root` has its children replaced
    /// with [`InboxExec`](crate::pipelines::inbox::InboxExec).
    depth: usize,
}

/// Lowers an [`ExecutionPlan`] into a [`PipelinePlan`].
pub struct PipelinePlanner {
    task_context: Arc<TaskContext>,
    schema: SchemaRef,
    output_partitions: usize,
    completed: Vec<RoutablePipeline>,
    /// DFS stack of nodes still to visit, paired with the output link
    /// their eventual pipeline should route to.
    to_visit: Vec<(Arc<dyn ExecutionPlan>, Option<OutputLink>)>,
    /// Current in-progress `ExecutionPipeline` group, if any.
    execution_operators: Option<OperatorGroup>,
}

impl PipelinePlanner {
    pub fn new(plan: Arc<dyn ExecutionPlan>, task_context: Arc<TaskContext>) -> Self {
        let schema = plan.schema();
        let output_partitions = plan.output_partitioning().partition_count();
        Self {
            completed: vec![],
            to_visit: vec![(plan, None)],
            task_context,
            execution_operators: None,
            schema,
            output_partitions,
        }
    }

    /// Flush the in-progress group into a new `ExecutionPipeline` (with
    /// Inbox rewiring at `group.depth`) and return its index in
    /// `completed`.
    fn flush_exec(&mut self) -> Result<usize> {
        let group = self.execution_operators.take().unwrap();
        let node_idx = self.completed.len();
        let pipeline = ExecutionPipeline::with_depth(
            group.root,
            Arc::clone(&self.task_context),
            Some(group.depth),
        )?;
        self.completed.push(RoutablePipeline {
            pipeline: Box::new(pipeline),
            output: group.output,
        });
        Ok(node_idx)
    }

    fn visit_exec(
        &mut self,
        plan: &Arc<dyn ExecutionPlan>,
        parent: Option<OutputLink>,
    ) -> Result<()> {
        let children = plan.children();

        match self.execution_operators.as_mut() {
            Some(group) => {
                debug_assert_eq!(
                    parent, group.output,
                    "PipelinePlanner: operator group's output link diverged"
                );
                group.depth += 1;
            }
            None => {
                self.execution_operators = Some(OperatorGroup {
                    output: parent,
                    root: Arc::clone(plan),
                    depth: 0,
                });
            }
        }

        match children.len() {
            1 => {
                // Continue DFS into the single child, keeping the same
                // parent output link — the child joins the current group.
                self.to_visit.push((Arc::clone(children[0]), parent));
            }
            _ => {
                // Leaf (0 children) or multi-child node — flush the
                // group here. Multi-child nodes have each child wired
                // into its own pipeline feeding this one's inboxes.
                let node = self.flush_exec()?;
                self.enqueue_children(
                    children.into_iter().cloned().collect::<Vec<_>>(),
                    node,
                );
            }
        }

        Ok(())
    }

    fn enqueue_children(
        &mut self,
        children: Vec<Arc<dyn ExecutionPlan>>,
        parent_node_idx: usize,
    ) {
        for (child_idx, child) in children.into_iter().enumerate() {
            self.to_visit.push((
                child,
                Some(OutputLink {
                    pipeline: parent_node_idx,
                    child: child_idx,
                }),
            ));
        }
    }

    /// Push a new `RoutablePipeline` and enqueue its children.
    fn push_pipeline(
        &mut self,
        node: RoutablePipeline,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) {
        let node_idx = self.completed.len();
        self.completed.push(node);
        self.enqueue_children(children, node_idx);
    }

    /// Flush any in-progress group, then push `pipeline` as a new
    /// breaker. The breaker's `parent` routing link is rewritten to
    /// point at the flushed group's pipeline (child 0 — single-child
    /// groups only) when a group was in progress.
    fn push_breaker(
        &mut self,
        pipeline: Box<dyn Pipeline>,
        parent: Option<OutputLink>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<()> {
        let parent = match &self.execution_operators {
            Some(group) => {
                debug_assert_eq!(
                    group.output, parent,
                    "PipelinePlanner: breaker's parent diverged from group's output"
                );
                Some(OutputLink {
                    pipeline: self.flush_exec()?,
                    child: 0,
                })
            }
            None => parent,
        };
        self.push_pipeline(
            RoutablePipeline {
                pipeline,
                output: parent,
            },
            children,
        );
        Ok(())
    }

    fn visit_operator(
        &mut self,
        plan: &Arc<dyn ExecutionPlan>,
        parent: Option<OutputLink>,
    ) -> Result<()> {
        if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
            let input_partitioning = repartition.input().output_partitioning().clone();
            let output_partitioning = repartition.partitioning().clone();
            let pipeline = Box::new(RepartitionPipeline::try_new(
                &input_partitioning,
                &output_partitioning,
            )?);
            self.push_breaker(
                pipeline,
                parent,
                plan.children().into_iter().cloned().collect(),
            )
        } else if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
            let input_partitioning = coalesce.input().output_partitioning().clone();
            let pipeline = Box::new(RepartitionPipeline::try_new(
                &input_partitioning,
                &Partitioning::RoundRobinBatch(1),
            )?);
            self.push_breaker(
                pipeline,
                parent,
                plan.children().into_iter().cloned().collect(),
            )
        } else if let Some(sort) = plan.downcast_ref::<SortExec>() {
            // Merging-sort (single-output) with multiple input partitions
            // is a gather + sort — keep it pull-based via ExecutionPipeline
            // so the merge logic in SortExec stays in charge.
            let input = sort.input();
            let input_parts = input.output_partitioning().partition_count();
            let preserve = sort.preserve_partitioning();
            if preserve || input_parts == 1 {
                let pipeline = Box::new(SortPipeline::try_new(
                    sort.expr().clone(),
                    sort.fetch(),
                    input.schema(),
                    input_parts,
                    &self.task_context,
                )?);
                self.push_breaker(
                    pipeline,
                    parent,
                    plan.children().into_iter().cloned().collect(),
                )
            } else {
                self.visit_exec(plan, parent)
            }
        } else {
            self.visit_exec(plan, parent)
        }
    }

    /// Run the DFS walk and produce the final [`PipelinePlan`].
    pub fn build(mut self) -> Result<PipelinePlan> {
        while let Some((plan, parent)) = self.to_visit.pop() {
            self.visit_operator(&plan, parent)?;
        }
        if self.execution_operators.is_some() {
            self.flush_exec()?;
        }
        Ok(PipelinePlan {
            schema: self.schema,
            output_partitions: self.output_partitions,
            pipelines: self.completed,
        })
    }
}
