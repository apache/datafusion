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

//! Grouped hash aggregation for simple multi-stage aggregation paths.
//!
//! This module handles the basic grouped two-stage paths:
//!
//! ```text
//! input rows -> GROUP BY hash table -> accumulator state rows
//! state rows -> GROUP BY hash table -> final aggregate rows
//! ```
//!
//! `AggregateExec` keeps finite-memory, ordered, limit, grouping-set,
//! `partial state -> partial state`, and single-stage aggregation on
//! `GroupedHashAggregateStream` for now.

use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::ready;
use futures::stream::{Stream, StreamExt};

use self::hash_table::{AggregateHashTable, ExecutionState, RawPartial};
use super::{AggregateExec, AggregateMode};
use crate::metrics::{BaselineMetrics, RecordOutput, SpillMetrics};
use crate::stream::EmptyRecordBatchStream;
use crate::{InputOrderMode, RecordBatchStream, SendableRecordBatchStream};

mod hash_table;
mod partial_final;

pub(crate) use hash_table::can_use_blocked_hash_aggregate;
pub(crate) use partial_final::PartialFinalHashAggregateStream;

/// Hash aggregate stream for grouped `AggregateMode::Partial`.
///
/// Input: raw rows
/// Output: partial state (e.g. for avg(x), it's sum(x), count(x))
pub(crate) struct RawPartialHashAggregateStream {
    // ========================================================================
    // PROPERTIES:
    // Initialized once for this input partition.
    // ========================================================================
    /// Output schema: group columns followed by partial aggregate state columns.
    schema: SchemaRef,

    /// Input batches containing raw rows, not partial aggregate state.
    input: SendableRecordBatchStream,

    // ========================================================================
    // STATE FLAGS:
    // Control whether the stream is reading input, emitting state, or done.
    // ========================================================================
    exec_state: ExecutionState,

    // ========================================================================
    // STATE BUFFERS:
    //
    // Hold intermediate groups and aggregate state while reading input.
    // Example: `SELECT z, COUNT(x), SUM(y) FROM t GROUP BY z` stores each distinct
    // `z` in `group_values` and keeps one partial-state accumulator for `COUNT(x)`
    // and one for `SUM(y)`.
    // ========================================================================
    /// Hash table and accumulator state for all groups seen so far.
    hash_table: AggregateHashTable<RawPartial>,

    // ========================================================================
    // EXECUTION RESOURCES:
    // Metrics and memory accounting for this stream.
    // ========================================================================
    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,
}

impl RawPartialHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert_eq!(agg.mode, AggregateMode::Partial);
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);

        let hash_table = AggregateHashTable::<RawPartial>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;

        let reservation =
            MemoryConsumer::new(format!("RawPartialHashAggregateStream[{partition}]"))
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

impl Stream for RawPartialHashAggregateStream {
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

                            // TODO: impl memory-limited aggr, when OOM directly send
                            // partial state to final aggregate stage
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

impl RecordBatchStream for RawPartialHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
