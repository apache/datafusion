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
//! [`ExecutionPlan`]s and exposes it as a push-based [`Pipeline`] leaf.
//!
//! From the scheduler's POV the pipeline has **zero** input children
//! (everything was absorbed into the wrapped plan); `push` / `close` are
//! unreachable. `poll_partition(p)` lazily calls `plan.execute(p, ctx)` on
//! first use and forwards to the returned stream's `poll_next` thereafter.

use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::error::DataFusionError;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use futures::StreamExt;
use parking_lot::Mutex;

use crate::pipeline::Pipeline;

pub struct ExecutionPipeline {
    plan: Arc<dyn ExecutionPlan>,
    task_context: Arc<TaskContext>,
    output_partitions: usize,
    /// One slot per output partition. Lazily populated on first
    /// `poll_partition`. Wrapped in a `Mutex` because `poll_partition`
    /// takes `&self`.
    streams: Vec<Mutex<PartitionState>>,
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
            .finish()
    }
}

impl ExecutionPipeline {
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        task_context: Arc<TaskContext>,
    ) -> Result<Self> {
        let output_partitions = plan.output_partitioning().partition_count();
        let streams = (0..output_partitions)
            .map(|_| Mutex::new(PartitionState::Fresh))
            .collect();
        Ok(Self {
            plan,
            task_context,
            output_partitions,
            streams,
        })
    }
}

impl Pipeline for ExecutionPipeline {
    fn push(&self, _input: RecordBatch, _child: usize, _partition: usize) -> Result<()> {
        Err(DataFusionError::Internal(
            "ExecutionPipeline::push called — this pipeline is a leaf adapter \
             and should not receive input from the scheduler"
                .to_string(),
        ))
    }

    fn close(&self, _child: usize, _partition: usize) {
        // Unreachable in correct use; a stray close is harmless, just log.
        log::error!("ExecutionPipeline::close called — this pipeline is a leaf adapter");
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
