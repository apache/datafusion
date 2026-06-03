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
use futures::stream::{Stream, StreamExt};

use super::AggregateExec;
use super::hash_table::{AggregateHashTable, InitialPartial, PartialFinal};
use crate::metrics::{BaselineMetrics, MetricBuilder, RecordOutput, SpillMetrics};
use crate::stream::EmptyRecordBatchStream;
use crate::{InputOrderMode, RecordBatchStream, SendableRecordBatchStream, metrics};

/// Hash aggregation uses a 2-stage (partial and final) hash aggregation, this stream
/// is for the partial stage.
///
/// # Example
///
/// select k, avg(v) from t group by k;
///
/// ## Plan
/// AggregateExec(stage=final)
/// -- RepartitionExec(hash(k))
/// ---- AggregateExec(stage=partial)
///
/// ## Partial Stage Behavior
/// Input: raw rows
/// Output: partial states for all groups (e.g. for avg(x), it's sum(x), count(x))
///
/// ## Final Stage Behavior
/// Input: partial states
/// Output: results for all groups (e.g. for avg(x), it's avg(x) calculated from the state)
pub(crate) struct InitialPartialHashAggregateStream {
    /// Output schema: group columns followed by partial aggregate state columns.
    schema: SchemaRef,

    /// Input batches containing raw rows, not partial aggregate state.
    input: SendableRecordBatchStream,

    /// Hash table state for this aggregate stream.
    hash_table: AggregateHashTable<InitialPartial>,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Tracks partial aggregation row reduction, matching `GroupedHashAggregateStream`.
    reduction_factor: metrics::RatioMetrics,
}

/// Hash aggregation uses a 2-stage (partial and final) hash aggregation, this stream
/// is for the final stage.
///
/// See [`InitialPartialHashAggregateStream`] for details.
pub(crate) struct PartialFinalHashAggregateStream {
    /// Output schema: group columns followed by final aggregate value columns.
    schema: SchemaRef,

    /// Input batches containing partial aggregate state rows.
    input: SendableRecordBatchStream,

    /// Hash table state for this aggregate stream.
    hash_table: AggregateHashTable<PartialFinal>,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,
}

impl InitialPartialHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert_eq!(agg.mode, super::AggregateMode::Partial);
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);
        let reduction_factor = MetricBuilder::new(&agg.metrics)
            .with_type(metrics::MetricType::Summary)
            .ratio_metrics("reduction_factor", partition);

        let hash_table = AggregateHashTable::<InitialPartial>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;

        let reservation = MemoryConsumer::new(format!(
            "InitialPartialHashAggregateStream[{partition}]"
        ))
        .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            hash_table,
            baseline_metrics,
            reservation,
            reduction_factor,
        })
    }
}

impl Stream for InitialPartialHashAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        loop {
            if self.hash_table.is_done() {
                let _ = self.reservation.try_resize(0);
                return Poll::Ready(None);
            } else if self.hash_table.is_building() {
                match self.input.poll_next_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Ok(batch))) => {
                        let timer = elapsed_compute.timer();
                        self.reduction_factor.add_total(batch.num_rows());
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
                    Poll::Ready(Some(Err(e))) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        let input_schema = self.input.schema();
                        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));

                        let timer = elapsed_compute.timer();
                        let result = self.hash_table.start_output();
                        timer.done();

                        if let Err(e) = result {
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
            } else {
                let timer = elapsed_compute.timer();
                let result = self.hash_table.next_output_batch();
                timer.done();

                match result {
                    Ok(Some(batch)) => {
                        let _ =
                            self.reservation.try_resize(self.hash_table.memory_size());
                        self.reduction_factor.add_part(batch.num_rows());
                        debug_assert!(batch.num_rows() > 0);
                        return Poll::Ready(Some(Ok(
                            batch.record_output(&self.baseline_metrics)
                        )));
                    }
                    Ok(None) => {
                        let _ = self.reservation.try_resize(0);
                        return Poll::Ready(None);
                    }
                    Err(e) => return Poll::Ready(Some(Err(e))),
                }
            }
        }
    }
}

impl RecordBatchStream for InitialPartialHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl PartialFinalHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            super::AggregateMode::Final | super::AggregateMode::FinalPartitioned
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
            if self.hash_table.is_done() {
                let _ = self.reservation.try_resize(0);
                return Poll::Ready(None);
            } else if self.hash_table.is_building() {
                match self.input.poll_next_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Ok(batch))) => {
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
                    Poll::Ready(Some(Err(e))) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        let input_schema = self.input.schema();
                        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));

                        let timer = elapsed_compute.timer();
                        let result = self.hash_table.start_output();
                        timer.done();

                        if let Err(e) = result {
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
            } else {
                let timer = elapsed_compute.timer();
                let result = self.hash_table.next_output_batch();
                timer.done();

                match result {
                    Ok(Some(batch)) => {
                        let _ =
                            self.reservation.try_resize(self.hash_table.memory_size());
                        debug_assert!(batch.num_rows() > 0);
                        return Poll::Ready(Some(Ok(
                            batch.record_output(&self.baseline_metrics)
                        )));
                    }
                    Ok(None) => {
                        let _ = self.reservation.try_resize(0);
                        return Poll::Ready(None);
                    }
                    Err(e) => return Poll::Ready(Some(Err(e))),
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
