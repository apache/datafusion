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
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::ready;
use futures::stream::{Stream, StreamExt};

use super::hash_table::{AggregateHashTable, ExecutionState, PartialFinal};
use crate::aggregates::{AggregateExec, AggregateMode};
use crate::metrics::{BaselineMetrics, RecordOutput, SpillMetrics};
use crate::stream::EmptyRecordBatchStream;
use crate::{InputOrderMode, RecordBatchStream, SendableRecordBatchStream};

/// `AggregateMode::FinalPartitioned`.
///
/// Input: partial state, such as `sum(x), count(x)` for `avg(x)`.
/// Output: final values, such as `avg(x)`.
pub(crate) struct PartialFinalHashAggregateStream {
    /// Output schema: group columns followed by final aggregate value columns.
    schema: SchemaRef,

    /// Input batches containing partial aggregate state rows.
    input: SendableRecordBatchStream,

    /// Controls whether the stream is reading input, emitting output, or done.
    exec_state: ExecutionState,

    /// Hash table and accumulator state for all groups seen so far.
    hash_table: AggregateHashTable<PartialFinal>,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,
}

impl PartialFinalHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            AggregateMode::Final | AggregateMode::FinalPartitioned
        ));
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);

        let hash_table = AggregateHashTable::<PartialFinal>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;

        let reservation =
            MemoryConsumer::new(format!("PartialFinalHashAggregateStream[{partition}]"))
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            exec_state: ExecutionState::ReadingInput,
            hash_table,
            baseline_metrics,
            reservation,
        })
    }
}

impl Stream for PartialFinalHashAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        loop {
            match &self.exec_state {
                ExecutionState::ReadingInput => {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            let timer = elapsed_compute.timer();
                            let result = self.hash_table.aggregate_batch(&batch);
                            timer.done();

                            if let Err(e) = result {
                                return Poll::Ready(Some(Err(e)));
                            }

                            if let Err(e) =
                                self.reservation.try_resize(self.hash_table.memory_size())
                            {
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                        Some(Err(e)) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            let input_schema = self.input.schema();
                            self.input =
                                Box::pin(EmptyRecordBatchStream::new(input_schema));

                            self.exec_state = ExecutionState::ProducingOutput;
                        }
                    }
                }

                ExecutionState::ProducingOutput => {
                    let timer = elapsed_compute.timer();
                    let result = self.hash_table.next_output_batch();
                    timer.done();

                    match result {
                        Ok(Some(batch)) => {
                            let _ = self
                                .reservation
                                .try_resize(self.hash_table.memory_size());
                            debug_assert!(batch.num_rows() > 0);
                            return Poll::Ready(Some(Ok(
                                batch.record_output(&self.baseline_metrics)
                            )));
                        }
                        Ok(None) => {
                            let _ = self
                                .reservation
                                .try_resize(self.hash_table.memory_size());
                            self.exec_state = ExecutionState::Done;
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }

                ExecutionState::Done => {
                    self.hash_table.clear();
                    let _ = self.reservation.try_resize(self.hash_table.memory_size());
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for PartialFinalHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
