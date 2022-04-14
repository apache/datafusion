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

use futures::channel::mpsc;
use log::debug;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};

use crate::pipeline::{
    execution::ExecutionPipeline, repartition::RepartitionPipeline, Pipeline,
};
use crate::{ArrowResult, Spawner};

/// Identifies the [`Pipeline`] within the [`Query`] to route output to
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OutputLink {
    /// The index of the [`Pipeline`] in [`Query`] to route output to
    pub pipeline: usize,

    /// The child of the [`Pipeline`] to route output to
    pub child: usize,
}

/// Combines a [`Pipeline`] with an [`OutputLink`] identifying where to send its output
#[derive(Debug)]
pub struct RoutablePipeline {
    /// The pipeline that produces data
    pub pipeline: Box<dyn Pipeline>,

    /// Where to send output the output of `pipeline`
    ///
    /// If `None`, the output should be sent to the query output
    pub output: Option<OutputLink>,
}

/// [`Query`] is the scheduler's representation of the [`ExecutionPlan`] passed to
/// [`super::Scheduler::schedule`]. It combines the list of [Pipeline`] with the information
/// necessary to route output from one stage to the next
#[derive(Debug)]
pub struct Query {
    /// Spawner for this query
    spawner: Spawner,

    /// List of pipelines that belong to this query, pipelines are addressed
    /// based on their index within this list
    pipelines: Vec<RoutablePipeline>,

    /// The output stream for this query's execution
    output: mpsc::UnboundedSender<ArrowResult<RecordBatch>>,
}

impl Drop for Query {
    fn drop(&mut self) {
        debug!("Query finished");
    }
}

impl Query {
    /// Creates a new [`Query`] from the provided [`ExecutionPlan`], returning
    /// an [`mpsc::UnboundedReceiver`] that can be used to receive the results
    /// of this query's execution
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        task_context: Arc<TaskContext>,
        spawner: Spawner,
    ) -> Result<(Query, mpsc::UnboundedReceiver<ArrowResult<RecordBatch>>)> {
        QueryBuilder::new(plan, task_context).build(spawner)
    }

    /// Returns a list of this queries [`QueryPipeline`]
    pub fn pipelines(&self) -> &[RoutablePipeline] {
        &self.pipelines
    }

    /// Returns `true` if this query has been dropped, specifically if the
    /// stream returned by [`super::Scheduler::schedule`] has been dropped
    pub fn is_cancelled(&self) -> bool {
        self.output.is_closed()
    }

    /// Sends `output` to this query's output stream
    pub fn send_query_output(&self, output: ArrowResult<RecordBatch>) {
        let _ = self.output.unbounded_send(output);
    }

    /// Returns the [`Spawner`] associated with this [`Query`]
    pub fn spawner(&self) -> &Spawner {
        &self.spawner
    }
}

/// When converting [`ExecutionPlan`] to [`Pipeline`] we may wish to group
/// together multiple [`ExecutionPlan`], [`ExecGroup`] stores this state
struct ExecGroup {
    /// Where to route the output of the eventual [`Pipeline`]
    output: Option<OutputLink>,

    /// The [`ExecutionPlan`] from which to start recursing
    root: Arc<dyn ExecutionPlan>,

    /// The number of times to recurse into the [`ExecutionPlan`]'s children
    depth: usize,
}

/// A utility struct to assist converting from [`ExecutionPlan`] to [`Query`]
///
/// The [`ExecutionPlan`] is visited in a depth-first fashion, gradually building
/// up the [`RoutablePipeline`] for the [`Query`]. As nodes are visited depth-first,
/// a node is visited only after its parent has been.
struct QueryBuilder {
    task_context: Arc<TaskContext>,
    /// The current list of completed pipelines
    in_progress: Vec<RoutablePipeline>,

    /// A list of [`ExecutionPlan`] still to visit, along with
    /// where they should route their output
    to_visit: Vec<(Arc<dyn ExecutionPlan>, Option<OutputLink>)>,

    /// Stores one or more [`ExecutionPlan`] to combine together into
    /// a single [`ExecutionPipeline`]
    exec_buffer: Option<ExecGroup>,
}

impl QueryBuilder {
    fn new(plan: Arc<dyn ExecutionPlan>, task_context: Arc<TaskContext>) -> Self {
        Self {
            in_progress: vec![],
            to_visit: vec![(plan, None)],
            task_context,
            exec_buffer: None,
        }
    }

    /// Flush the current group of [`ExecutionPlan`] stored in `exec_buffer`
    /// into a single [`ExecutionPipeline]
    fn flush_exec(&mut self) -> Result<usize> {
        let group = self.exec_buffer.take().unwrap();
        let node_idx = self.in_progress.len();
        self.in_progress.push(RoutablePipeline {
            pipeline: Box::new(ExecutionPipeline::new(
                group.root,
                self.task_context.clone(),
                group.depth,
            )?),
            output: group.output,
        });
        Ok(node_idx)
    }

    /// Visit a non-special cased [`ExecutionPlan`]
    fn visit_exec(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
        parent: Option<OutputLink>,
    ) -> Result<()> {
        let children = plan.children();

        // Add the node to the current group of execution plan to be combined
        // into a single [`ExecutionPipeline`].
        //
        // TODO: More sophisticated policy, just because we can combine them doesn't mean we should
        match self.exec_buffer.as_mut() {
            Some(buffer) => {
                assert_eq!(parent, buffer.output, "QueryBuilder out of sync");
                buffer.depth += 1;
            }
            None => {
                self.exec_buffer = Some(ExecGroup {
                    output: parent,
                    root: plan,
                    depth: 0,
                })
            }
        }

        match children.len() {
            1 => {
                // Enqueue the children with the parent of the `ExecGroup`
                self.to_visit
                    .push((children.into_iter().next().unwrap(), parent))
            }
            _ => {
                // We can only recursively group through nodes with a single child, therefore
                // if this node has multiple children, we now need to flush the buffer and
                // enqueue its children with this new pipeline as its parent
                let node = self.flush_exec()?;
                self.enqueue_children(children, node);
            }
        }

        Ok(())
    }

    /// Add the given list of children to the stack of [`ExecutionPlan`] to visit
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
            ))
        }
    }

    /// Push a new [`RoutablePipeline`] and enqueue its children to be visited
    fn push_pipeline(
        &mut self,
        node: RoutablePipeline,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) {
        let node_idx = self.in_progress.len();
        self.in_progress.push(node);
        self.enqueue_children(children, node_idx)
    }

    /// Push a new [`RepartitionPipeline`] first flushing any buffered [`ExecGroup`]
    fn push_repartition(
        &mut self,
        input: Partitioning,
        output: Partitioning,
        parent: Option<OutputLink>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<()> {
        let parent = match &self.exec_buffer {
            Some(buffer) => {
                assert_eq!(buffer.output, parent, "QueryBuilder out of sync");
                Some(OutputLink {
                    pipeline: self.flush_exec()?,
                    child: 0, // Must be the only child
                })
            }
            None => parent,
        };

        let node = Box::new(RepartitionPipeline::new(input, output));
        self.push_pipeline(
            RoutablePipeline {
                pipeline: node,
                output: parent,
            },
            children,
        );
        Ok(())
    }

    /// Visit an [`ExecutionPlan`] node and add it to the [`Query`] being built
    fn visit_node(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
        parent: Option<OutputLink>,
    ) -> Result<()> {
        if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
            self.push_repartition(
                repartition.input().output_partitioning(),
                repartition.output_partitioning(),
                parent,
                repartition.children(),
            )
        } else if let Some(coalesce) =
            plan.as_any().downcast_ref::<CoalescePartitionsExec>()
        {
            self.push_repartition(
                coalesce.input().output_partitioning(),
                Partitioning::RoundRobinBatch(1),
                parent,
                coalesce.children(),
            )
        } else {
            self.visit_exec(plan, parent)
        }
    }

    /// Build a [`Query`] from the [`ExecutionPlan`] provided to [`QueryBuilder::new`]
    ///
    /// This will group all [`ExecutionPlan`] possible into a single [`ExecutionPipeline`], only
    /// creating new pipelines when:
    ///
    /// - encountering an [`ExecutionPlan`] with multiple children
    /// - encountering a repartitioning [`ExecutionPlan`]
    ///
    /// This latter case is because currently the repartitioning operators in DataFusion are
    /// coupled with the non-scheduler-based parallelism story
    ///
    /// The above logic is liable to change, is considered an implementation detail of the
    /// scheduler, and should not be relied upon by operators
    ///
    fn build(
        mut self,
        spawner: Spawner,
    ) -> Result<(Query, mpsc::UnboundedReceiver<ArrowResult<RecordBatch>>)> {
        // We do a depth-first scan of the operator tree, extracting a list of [`QueryNode`]
        while let Some((plan, parent)) = self.to_visit.pop() {
            self.visit_node(plan, parent)?;
        }

        if self.exec_buffer.is_some() {
            self.flush_exec()?;
        }

        let (sender, receiver) = mpsc::unbounded();
        Ok((
            Query {
                spawner,
                pipelines: self.in_progress,
                output: sender,
            },
            receiver,
        ))
    }
}
