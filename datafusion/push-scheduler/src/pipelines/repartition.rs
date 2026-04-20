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

//! [`RepartitionPipeline`] — N-input / M-output breaker implementing
//! hash or round-robin repartitioning. Reused for `CoalescePartitionsExec`
//! by constructing with `RoundRobinBatch(1)` output.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use arrow::record_batch::RecordBatch;
use datafusion_common::error::DataFusionError;
use datafusion_common::{Result, not_impl_err};
use datafusion_physical_expr::Partitioning;
use datafusion_physical_plan::metrics::Time;
use datafusion_physical_plan::repartition::BatchPartitioner;
use parking_lot::Mutex;

use crate::pipeline::Pipeline;

pub struct RepartitionPipeline {
    /// Number of input partitions ( = number of children = 1, with N
    /// partitions on that single child).
    input_partitions: usize,
    /// Number of output partitions.
    output_partitions: usize,

    /// One partitioner per input partition. Protected by `Mutex` because
    /// the scheduler's Task owns the input and we only need interior
    /// mutability for the partitioner's internal buffers.
    partitioners: Vec<Mutex<BatchPartitioner>>,

    /// Shared state for each output partition.
    outputs: Vec<Mutex<OutputBuffer>>,

    /// Number of input partitions that have not yet sent `close`. When it
    /// drops to zero every output flips to "closed" and emits `None`.
    open_inputs: AtomicUsize,
}

impl std::fmt::Debug for RepartitionPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepartitionPipeline")
            .field("input_partitions", &self.input_partitions)
            .field("output_partitions", &self.output_partitions)
            .finish()
    }
}

struct OutputBuffer {
    queue: VecDeque<RecordBatch>,
    waker: Option<Waker>,
    closed: bool,
}

impl OutputBuffer {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            waker: None,
            closed: false,
        }
    }
}

impl RepartitionPipeline {
    pub fn try_new(input: &Partitioning, output: &Partitioning) -> Result<Self> {
        let input_partitions = input.partition_count();
        let output_partitions = output.partition_count();
        if input_partitions == 0 {
            return Err(DataFusionError::Internal(
                "RepartitionPipeline requires at least one input partition".to_string(),
            ));
        }
        if output_partitions == 0 {
            return Err(DataFusionError::Internal(
                "RepartitionPipeline requires at least one output partition".to_string(),
            ));
        }

        // Refuse partitioning schemes BatchPartitioner can't handle.
        match output {
            Partitioning::Hash(..) | Partitioning::RoundRobinBatch(_) => {}
            other => {
                return not_impl_err!(
                    "RepartitionPipeline does not support output partitioning {other:?}"
                );
            }
        }

        let mut partitioners = Vec::with_capacity(input_partitions);
        for i in 0..input_partitions {
            let p = BatchPartitioner::try_new(
                output.clone(),
                Time::default(),
                i,
                input_partitions,
            )?;
            partitioners.push(Mutex::new(p));
        }

        let outputs = (0..output_partitions)
            .map(|_| Mutex::new(OutputBuffer::new()))
            .collect();

        Ok(Self {
            input_partitions,
            output_partitions,
            partitioners,
            outputs,
            open_inputs: AtomicUsize::new(input_partitions),
        })
    }
}

impl Pipeline for RepartitionPipeline {
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()> {
        debug_assert_eq!(child, 0, "RepartitionPipeline has a single input child");
        debug_assert!(
            partition < self.input_partitions,
            "input partition out of range"
        );

        // Partition the batch — need to be careful with the partitioner
        // lock: BatchPartitioner::partition_iter borrows `&mut self`, so
        // we collect results into a Vec before pushing into outputs.
        let outputs_to_push: Vec<(usize, RecordBatch)> = {
            let mut p = self.partitioners[partition].lock();
            let iter = p.partition_iter(input)?;
            iter.collect::<Result<Vec<_>>>()?
        };

        for (out_p, batch) in outputs_to_push {
            // Only wake the downstream task on the empty→non-empty
            // transition. The Task's wake_count coalesces redundant wakes
            // anyway, but skipping them here saves the atomic op cost on
            // the hot push path.
            let waker = {
                let mut o = self.outputs[out_p].lock();
                let was_empty = o.queue.is_empty();
                o.queue.push_back(batch);
                if was_empty { o.waker.take() } else { None }
            };
            if let Some(w) = waker {
                w.wake();
            }
        }

        Ok(())
    }

    fn close(&self, child: usize, partition: usize) {
        debug_assert_eq!(child, 0, "RepartitionPipeline has a single input child");
        debug_assert!(partition < self.input_partitions);

        let prev = self.open_inputs.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            // Last input closed — close every output and wake pending
            // pollers.
            for out in &self.outputs {
                let waker = {
                    let mut o = out.lock();
                    o.closed = true;
                    o.waker.take()
                };
                if let Some(w) = waker {
                    w.wake();
                }
            }
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
        debug_assert!(partition < self.output_partitions);
        let mut o = self.outputs[partition].lock();
        if let Some(batch) = o.queue.pop_front() {
            return Poll::Ready(Some(Ok(batch)));
        }
        if o.closed {
            return Poll::Ready(None);
        }
        // Stash the waker; replace any prior one since the latest call's
        // context represents the current task execution.
        o.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
