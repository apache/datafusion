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

//! [`ExecutionPipeline`] — adapter that wraps a subtree of pull-based
//! [`ExecutionPlan`]s and exposes it as a push-based [`Pipeline`].
//!
//! If `depth` is `Some(n)` the wrapped plan is rewritten so that the
//! operator `n` levels below the root has each of its children replaced
//! by an [`InboxExec`](super::inbox::InboxExec). The scheduler then
//! pushes batches from the upstream breaker into those inboxes via
//! [`Pipeline::push`], and the wrapped subtree's ordinary
//! `plan.execute(partition)` call pulls from them as if the breaker's
//! output had arrived pull-based.
//!
//! When `depth` is `None` (or the wrapped plan is a true leaf at
//! `depth=0`), no rewiring is performed and the pipeline is a leaf
//! adapter — `push` / `close` are unreachable.

use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::error::DataFusionError;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use futures::StreamExt;
use parking_lot::Mutex;

use super::inbox::{InboxExec, InboxSendGroup};
use crate::pipeline::Pipeline;

pub struct ExecutionPipeline {
    plan: Arc<dyn ExecutionPlan>,
    task_context: Arc<TaskContext>,
    output_partitions: usize,
    /// One slot per output partition. Lazily populated on first
    /// `poll_partition`. Wrapped in `Mutex` because `poll_partition`
    /// takes `&self`.
    streams: Vec<Mutex<PartitionState>>,

    /// Inbox senders, one group per original child of the rewiring
    /// point. Empty when no rewiring was performed (true leaf adapter).
    inboxes: Vec<InboxSendGroup>,
}

enum PartitionState {
    /// Not yet fetched `plan.execute(p)`.
    Fresh,
    /// Stream is live.
    Running(SendableRecordBatchStream),
    /// Stream reached EOS or errored — emit `Ready(None)` forever.
    Done,
}

impl std::fmt::Debug for ExecutionPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionPipeline")
            .field("plan", &self.plan)
            .field("output_partitions", &self.output_partitions)
            .field("inbox_children", &self.inboxes.len())
            .finish()
    }
}

impl ExecutionPipeline {
    /// Wrap `plan` with no rewiring. Use for true-leaf ExecutionPipelines
    /// (whole plan, or below a breaker-free subtree) where no breaker
    /// output needs to be injected.
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        task_context: Arc<TaskContext>,
    ) -> Result<Self> {
        Self::with_depth(plan, task_context, None)
    }

    /// Wrap `plan`, optionally rewriting the operator `depth` levels
    /// below the root so that each of its children is replaced with an
    /// [`InboxExec`]. Used by [`PipelinePlanner`](crate::plan::PipelinePlanner)
    /// when the group sits above a breaker cut.
    pub fn with_depth(
        plan: Arc<dyn ExecutionPlan>,
        task_context: Arc<TaskContext>,
        depth: Option<usize>,
    ) -> Result<Self> {
        let (plan, inboxes) = match depth {
            Some(d) => rewrite_with_inboxes(plan, d)?,
            None => (plan, Vec::new()),
        };
        let output_partitions = plan.output_partitioning().partition_count();
        let streams = (0..output_partitions)
            .map(|_| Mutex::new(PartitionState::Fresh))
            .collect();
        Ok(Self {
            plan,
            task_context,
            output_partitions,
            streams,
            inboxes,
        })
    }

    /// Number of `(child_idx, partition)` input slots this pipeline
    /// expects from the scheduler — 0 for leaf adapters, `Σ partitions`
    /// for Inbox-rewired pipelines.
    pub fn input_children(&self) -> usize {
        self.inboxes.len()
    }
}

/// Rewrite `plan` so that the operator `depth` levels below its root
/// has every child replaced with an [`InboxExec`]. Recurses through
/// single-child operators; panics if a non-single-child node is
/// encountered before `depth == 0` (the planner guarantees this invariant).
fn rewrite_with_inboxes(
    plan: Arc<dyn ExecutionPlan>,
    depth: usize,
) -> Result<(Arc<dyn ExecutionPlan>, Vec<InboxSendGroup>)> {
    if depth == 0 {
        let children = plan.children();
        if children.is_empty() {
            // True leaf at depth 0 — no breaker below; nothing to rewire.
            return Ok((plan, Vec::new()));
        }
        let mut new_children = Vec::with_capacity(children.len());
        let mut groups = Vec::with_capacity(children.len());
        for child in children {
            let schema = child.schema();
            let props = Arc::clone(child.properties());
            let (inbox, group) = InboxExec::new(schema, props);
            new_children.push(inbox as Arc<dyn ExecutionPlan>);
            groups.push(group);
        }
        let new_plan = plan.with_new_children(new_children)?;
        return Ok((new_plan, groups));
    }

    let children = plan.children();
    if children.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "rewrite_with_inboxes: depth={depth} but node has {} children \
             (planner should have flushed at a multi-child node)",
            children.len()
        )));
    }
    let child = Arc::clone(children[0]);
    let (new_child, groups) = rewrite_with_inboxes(child, depth - 1)?;
    let new_plan = plan.with_new_children(vec![new_child])?;
    Ok((new_plan, groups))
}

impl Pipeline for ExecutionPipeline {
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()> {
        if child >= self.inboxes.len() {
            return Err(DataFusionError::Internal(format!(
                "ExecutionPipeline::push for child {child} but only {} inboxes exist",
                self.inboxes.len()
            )));
        }
        self.inboxes[child].push(partition, input)
    }

    fn close(&self, child: usize, partition: usize) {
        if let Some(group) = self.inboxes.get(child) {
            group.close(partition);
        } else {
            log::error!(
                "ExecutionPipeline::close for child {child} but only {} inboxes exist",
                self.inboxes.len()
            );
        }
    }

    fn output_partitions(&self) -> usize {
        self.output_partitions
    }

    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let mut state = self.streams[partition].lock();
        loop {
            match &mut *state {
                PartitionState::Fresh => {
                    match self.plan.execute(partition, Arc::clone(&self.task_context)) {
                        Ok(stream) => *state = PartitionState::Running(stream),
                        Err(e) => {
                            *state = PartitionState::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                PartitionState::Running(stream) => {
                    return match stream.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),
                        Poll::Ready(Some(Err(e))) => {
                            *state = PartitionState::Done;
                            Poll::Ready(Some(Err(e)))
                        }
                        Poll::Ready(None) => {
                            *state = PartitionState::Done;
                            Poll::Ready(None)
                        }
                        Poll::Pending => Poll::Pending,
                    };
                }
                PartitionState::Done => return Poll::Ready(None),
            }
        }
    }
}
