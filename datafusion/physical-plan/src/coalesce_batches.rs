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

//! [`CoalesceBatchesExec`] combines small batches into larger batches.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use super::{DisplayAs, ExecutionPlanProperties, PlanProperties, Statistics};
use crate::{
    DisplayFormatType, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::TaskContext;

use crate::coalesce::{BatchCoalescer, CoalescerState};
use futures::ready;
use futures::stream::{Stream, StreamExt};

/// `CoalesceBatchesExec` combines small batches into larger batches for more
/// efficient vectorized processing by later operators.
///
/// The operator buffers batches until it collects `target_batch_size` rows and
/// then emits a single concatenated batch. When only a limited number of rows
/// are necessary (specified by the `fetch` parameter), the operator will stop
/// buffering and returns the final batch once the number of collected rows
/// reaches the `fetch` value.
///
/// See [`BatchCoalescer`] for more information
#[derive(Debug)]
pub struct CoalesceBatchesExec {
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Minimum number of rows for coalesces batches
    target_batch_size: usize,
    /// Maximum number of rows to fetch, `None` means fetching all rows
    fetch: Option<usize>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
}

impl CoalesceBatchesExec {
    /// Create a new CoalesceBatchesExec
    pub fn new(input: Arc<dyn ExecutionPlan>, target_batch_size: usize) -> Self {
        let cache = Self::compute_properties(&input);
        Self {
            input,
            target_batch_size,
            fetch: None,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }

    /// Update fetch with the argument
    pub fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Minimum number of rows for coalesces batches
    pub fn target_batch_size(&self) -> usize {
        self.target_batch_size
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        // The coalesce batches operator does not make any changes to the
        // partitioning of its input.
        PlanProperties::new(
            input.equivalence_properties().clone(), // Equivalence Properties
            input.output_partitioning().clone(),    // Output Partitioning
            input.execution_mode(),                 // Execution Mode
        )
    }
}

impl DisplayAs for CoalesceBatchesExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "CoalesceBatchesExec: target_batch_size={}",
                    self.target_batch_size,
                )?;
                if let Some(fetch) = self.fetch {
                    write!(f, ", fetch={fetch}")?;
                };

                Ok(())
            }
        }
    }
}

impl ExecutionPlan for CoalesceBatchesExec {
    fn name(&self) -> &'static str {
        "CoalesceBatchesExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            CoalesceBatchesExec::new(Arc::clone(&children[0]), self.target_batch_size)
                .with_fetch(self.fetch),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(CoalesceBatchesStream {
            input: self.input.execute(partition, context)?,
            coalescer: BatchCoalescer::new(
                self.input.schema(),
                self.target_batch_size,
                self.fetch,
            ),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            // Start by pulling data
            inner_state: CoalesceBatchesStreamState::Pull,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Statistics::with_fetch(self.input.statistics()?, self.schema(), self.fetch, 0, 1)
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(CoalesceBatchesExec {
            input: Arc::clone(&self.input),
            target_batch_size: self.target_batch_size,
            fetch: limit,
            metrics: self.metrics.clone(),
            cache: self.cache.clone(),
        }))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

/// Stream for [`CoalesceBatchesExec`]. See [`CoalesceBatchesExec`] for more details.
struct CoalesceBatchesStream {
    /// The input plan
    input: SendableRecordBatchStream,
    /// Buffer for combining batches
    coalescer: BatchCoalescer,
    /// Execution metrics
    baseline_metrics: BaselineMetrics,
    /// The current inner state of the stream. This state dictates the current
    /// action or operation to be performed in the streaming process.
    inner_state: CoalesceBatchesStreamState,
}

impl Stream for CoalesceBatchesStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // we can't predict the size of incoming batches so re-use the size hint from the input
        self.input.size_hint()
    }
}

/// Enumeration of possible states for `CoalesceBatchesStream`.
/// It represents different stages in the lifecycle of a stream of record batches.
///
/// An example of state transition:
/// Notation:
/// `[3000]`: A batch with size 3000
/// `{[2000], [3000]}`: `CoalesceBatchStream`'s internal buffer with 2 batches buffered
/// Input of `CoalesceBatchStream` will generate three batches `[2000], [3000], [4000]`
/// The coalescing procedure will go through the following steps with 4096 coalescing threshold:
/// 1. Read the first batch and get it buffered.
/// - initial state: `Pull`
/// - initial buffer: `{}`
/// - updated buffer: `{[2000]}`
/// - next state: `Pull`
/// 2. Read the second batch, the coalescing target is reached since 2000 + 3000 > 4096
/// - initial state: `Pull`
/// - initial buffer: `{[2000]}`
/// - updated buffer: `{[2000], [3000]}`
/// - next state: `ReturnBuffer`
/// 4. Two batches in the batch get merged and consumed by the upstream operator.
/// - initial state: `ReturnBuffer`
/// - initial buffer: `{[2000], [3000]}`
/// - updated buffer: `{}`
/// - next state: `Pull`
/// 5. Read the third input batch.
/// - initial state: `Pull`
/// - initial buffer: `{}`
/// - updated buffer: `{[4000]}`
/// - next state: `Pull`
/// 5. The input is ended now. Jump to exhaustion state preparing the finalized data.
/// - initial state: `Pull`
/// - initial buffer: `{[4000]}`
/// - updated buffer: `{[4000]}`
/// - next state: `Exhausted`
#[derive(Debug, Clone, Eq, PartialEq)]
enum CoalesceBatchesStreamState {
    /// State to pull a new batch from the input stream.
    Pull,
    /// State to return a buffered batch.
    ReturnBuffer,
    /// State indicating that the stream is exhausted.
    Exhausted,
}

impl CoalesceBatchesStream {
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let cloned_time = self.baseline_metrics.elapsed_compute().clone();
        loop {
            match &self.inner_state {
                CoalesceBatchesStreamState::Pull => {
                    // Attempt to pull the next batch from the input stream.
                    let input_batch = ready!(self.input.poll_next_unpin(cx));
                    // Start timing the operation. The timer records time upon being dropped.
                    let _timer = cloned_time.timer();

                    match input_batch {
                        Some(Ok(batch)) => match self.coalescer.push_batch(batch) {
                            CoalescerState::Continue => {}
                            CoalescerState::LimitReached => {
                                self.inner_state = CoalesceBatchesStreamState::Exhausted;
                            }
                            CoalescerState::TargetReached => {
                                self.inner_state =
                                    CoalesceBatchesStreamState::ReturnBuffer;
                            }
                        },
                        None => {
                            // End of input stream, but buffered batches might still be present.
                            self.inner_state = CoalesceBatchesStreamState::Exhausted;
                        }
                        other => return Poll::Ready(other),
                    }
                }
                CoalesceBatchesStreamState::ReturnBuffer => {
                    // Combine buffered batches into one batch and return it.
                    let batch = self.coalescer.finish_batch()?;
                    // Set to pull state for the next iteration.
                    self.inner_state = CoalesceBatchesStreamState::Pull;
                    return Poll::Ready(Some(Ok(batch)));
                }
                CoalesceBatchesStreamState::Exhausted => {
                    // Handle the end of the input stream.
                    return if self.coalescer.is_empty() {
                        // If buffer is empty, return None indicating the stream is fully consumed.
                        Poll::Ready(None)
                    } else {
                        // If the buffer still contains batches, prepare to return them.
                        let batch = self.coalescer.finish_batch()?;
                        Poll::Ready(Some(Ok(batch)))
                    };
                }
            }
        }
    }
}

impl RecordBatchStream for CoalesceBatchesStream {
    fn schema(&self) -> SchemaRef {
        self.coalescer.schema()
    }
}
