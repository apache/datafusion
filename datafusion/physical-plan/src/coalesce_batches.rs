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
use std::task::{ready, Context, Poll};

use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use super::{DisplayAs, ExecutionPlanProperties, PlanProperties, Statistics};
use crate::{
    DisplayFormatType, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};

use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::TaskContext;

use futures::stream::{Stream, StreamExt};
use log::trace;

/// `CoalesceBatchesExec` combines small batches into larger batches for more
/// efficient use of vectorized processing by upstream operators.
///
/// Generally speaking, larger RecordBatches are more efficient to process than
/// smaller record batches (until the CPU cache is exceeded) because there is
/// fixed processing overhead per batch. This code concatenates multiple small
/// record batches into larger ones to amortize this overhead.
///
/// ```text
/// ┌────────────────────┐
/// │    RecordBatch     │
/// │   num_rows = 23    │
/// └────────────────────┘                 ┌────────────────────┐
///                                        │                    │
/// ┌────────────────────┐     Coalesce    │                    │
/// │                    │      Batches    │                    │
/// │    RecordBatch     │                 │                    │
/// │   num_rows = 50    │  ─ ─ ─ ─ ─ ─ ▶  │                    │
/// │                    │                 │    RecordBatch     │
/// │                    │                 │   num_rows = 106   │
/// └────────────────────┘                 │                    │
///                                        │                    │
/// ┌────────────────────┐                 │                    │
/// │                    │                 │                    │
/// │    RecordBatch     │                 │                    │
/// │   num_rows = 33    │                 └────────────────────┘
/// │                    │
/// └────────────────────┘
/// ```
#[derive(Debug)]
pub struct CoalesceBatchesExec {
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Minimum number of rows for coalesces batches
    target_batch_size: usize,
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
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
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
                    self.target_batch_size
                )
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
        Ok(Arc::new(CoalesceBatchesExec::new(
            Arc::clone(&children[0]),
            self.target_batch_size,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(CoalesceBatchesStream {
            input: self.input.execute(partition, context)?,
            coalescer: BatchCoalescer::new(self.input.schema(), self.target_batch_size),
            is_closed: false,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }
}

/// Stream for [`CoalesceBatchesExec`]. See [`CoalesceBatchesExec`] for more details.
struct CoalesceBatchesStream {
    /// The input plan
    input: SendableRecordBatchStream,
    /// Buffer for combining batches
    coalescer: BatchCoalescer,
    /// Whether the stream has finished returning all of its data or not
    is_closed: bool,
    /// Execution metrics
    baseline_metrics: BaselineMetrics,
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

impl CoalesceBatchesStream {
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        // Get a clone (uses same underlying atomic) as self gets borrowed below
        let cloned_time = self.baseline_metrics.elapsed_compute().clone();

        if self.is_closed {
            return Poll::Ready(None);
        }
        loop {
            let input_batch = self.input.poll_next_unpin(cx);
            // records time on drop
            let _timer = cloned_time.timer();
            match ready!(input_batch) {
                Some(result) => {
                    let Ok(input_batch) = result else {
                        return Poll::Ready(Some(result)); // pass back error
                    };
                    // Buffer the batch and either get more input if not enough
                    // rows yet or output
                    match self.coalescer.push_batch(input_batch) {
                        Ok(None) => continue,
                        res => return Poll::Ready(res.transpose()),
                    }
                }
                None => {
                    self.is_closed = true;
                    // we have reached the end of the input stream but there could still
                    // be buffered batches
                    return match self.coalescer.finish() {
                        Ok(None) => Poll::Ready(None),
                        res => Poll::Ready(res.transpose()),
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

/// Concatenates an array of `RecordBatch` into one batch
pub fn concat_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    row_count: usize,
) -> ArrowResult<RecordBatch> {
    trace!(
        "Combined {} batches containing {} rows",
        batches.len(),
        row_count
    );
    arrow::compute::concat_batches(schema, batches)
}

/// Concatenate multiple record batches into larger batches
///
/// See [`CoalesceBatchesExec`] for more details.
///
/// Notes:
///
/// 1. The output rows is the same order as the input rows
///
/// 2. The output is a sequence of batches, with all but the last being at least
/// `target_batch_size` rows.
///
/// 3. Eventually this may also be able to handle other optimizations such as a
/// combined filter/coalesce operation.
#[derive(Debug)]
struct BatchCoalescer {
    /// The input schema
    schema: SchemaRef,
    /// Minimum number of rows for coalesces batches
    target_batch_size: usize,
    /// Buffered batches
    buffer: Vec<RecordBatch>,
    /// Buffered row count
    buffered_rows: usize,
}

impl BatchCoalescer {
    /// Create a new BatchCoalescer that produces batches of at least `target_batch_size` rows
    fn new(schema: SchemaRef, target_batch_size: usize) -> Self {
        Self {
            schema,
            target_batch_size,
            buffer: vec![],
            buffered_rows: 0,
        }
    }

    /// Return the schema of the output batches
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Add a batch to the coalescer, returning a batch if the target batch size is reached
    fn push_batch(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        if batch.num_rows() >= self.target_batch_size && self.buffer.is_empty() {
            return Ok(Some(batch));
        }
        // discard empty batches
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        // add to the buffered batches
        self.buffered_rows += batch.num_rows();
        self.buffer.push(batch);
        // check to see if we have enough batches yet
        let batch = if self.buffered_rows >= self.target_batch_size {
            // combine the batches and return
            let batch = concat_batches(&self.schema, &self.buffer, self.buffered_rows)?;
            // reset buffer state
            self.buffer.clear();
            self.buffered_rows = 0;
            // return batch
            Some(batch)
        } else {
            None
        };
        Ok(batch)
    }

    /// Finish the coalescing process, returning all buffered data as a final,
    /// single batch, if any
    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.buffer.is_empty() {
            Ok(None)
        } else {
            // combine the batches and return
            let batch = concat_batches(&self.schema, &self.buffer, self.buffered_rows)?;
            // reset buffer state
            self.buffer.clear();
            self.buffered_rows = 0;
            // return batch
            Ok(Some(batch))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::UInt32Array;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concat_batches() -> Result<()> {
        let Scenario { schema, batch } = uint32_scenario();

        // input is 10 batches x 8 rows (80 rows)
        let input = std::iter::repeat(batch).take(10);

        // expected output is batches of at least 20 rows (except for the final batch)
        let batches = do_coalesce_batches(&schema, input, 21);
        assert_eq!(4, batches.len());
        assert_eq!(24, batches[0].num_rows());
        assert_eq!(24, batches[1].num_rows());
        assert_eq!(24, batches[2].num_rows());
        assert_eq!(8, batches[3].num_rows());

        Ok(())
    }

    // Coalesce the batches with a BatchCoalescer  function with the given input
    // and target batch size returning the resulting batches
    fn do_coalesce_batches(
        schema: &SchemaRef,
        input: impl IntoIterator<Item = RecordBatch>,
        target_batch_size: usize,
    ) -> Vec<RecordBatch> {
        // create physical plan
        let mut coalescer = BatchCoalescer::new(Arc::clone(schema), target_batch_size);
        let mut output_batches: Vec<_> = input
            .into_iter()
            .filter_map(|batch| coalescer.push_batch(batch).unwrap())
            .collect();
        if let Some(batch) = coalescer.finish().unwrap() {
            output_batches.push(batch);
        }
        output_batches
    }

    /// Test scenario
    #[derive(Debug)]
    struct Scenario {
        schema: Arc<Schema>,
        batch: RecordBatch,
    }

    /// a batch of 8 rows of UInt32
    fn uint32_scenario() -> Scenario {
        let schema =
            Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))],
        )
        .unwrap();

        Scenario { schema, batch }
    }
}
