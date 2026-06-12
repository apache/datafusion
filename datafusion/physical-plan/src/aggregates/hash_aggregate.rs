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

//! 2-stage hash aggregation stream implementation.
//!
//! See comments in [`PartialHashAggregateStream`] and [`FinalHashAggregateStream`]
//! for details.
//!
//! Note these streams are an incremental migration of the existing
//! [`crate::aggregates::row_hash::GroupedHashAggregateStream`].
//!
//! See issue for details: <https://github.com/apache/datafusion/issues/22710>

use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::stream::{Stream, StreamExt};

use super::AggregateExec;
use super::hash_table::{AggregateHashTable, Final, Partial};
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
///
/// # Optimization: DISTINCT LIMIT Soft Limit
///
/// This optimization applies to both [`PartialHashAggregateStream`] and [`FinalHashAggregateStream`]
///
/// Unordered distinct queries such as:
///
/// ```sql
/// SELECT DISTINCT x FROM t LIMIT 10;
/// ```
///
/// are optimized into a two-stage aggregate like:
///
/// ```txt
/// LimitExec, limit=10
/// --AggregateExec(Final), group_by=[x], aggr=[], soft_limit=10
/// ---- RepartitionExec, partitioning=hash(x)
/// ------ AggregateExec(Partial), group_by=[x], aggr=[], soft_limit=10
/// -------- Scan(t)
/// ```
///
/// After each input batch, the stream checks whether the soft limit has been
/// reached. If so, it emits the accumulated groups and stops reading input.
///
/// This operator does not guarantee an exact limit because a single batch can
/// cross the threshold. The downstream limit operator enforces the exact result
/// size.
pub(crate) struct PartialHashAggregateStream {
    /// Output schema: group columns followed by partial aggregate state columns.
    schema: SchemaRef,

    /// Input batches containing raw rows, not partial aggregate state.
    input: SendableRecordBatchStream,

    /// Hash table state for this aggregate stream.
    hash_table: AggregateHashTable<Partial>,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Tracks partial aggregation row reduction, matching `GroupedHashAggregateStream`.
    reduction_factor: metrics::RatioMetrics,

    /// Optional soft limit on the number of groups to accumulate before output.
    ///
    /// Invariant: when this is `Some(..)`, the accumulators inside `hash_table` must
    /// be empty. See struct comments for details.
    group_values_soft_limit: Option<usize>,
}

/// Hash aggregation uses a 2-stage (partial and final) hash aggregation, this stream
/// is for the final stage.
///
/// See [`PartialHashAggregateStream`] for details.
pub(crate) struct FinalHashAggregateStream {
    /// Output schema: group columns followed by final aggregate value columns.
    schema: SchemaRef,

    /// Input batches containing partial aggregate state rows.
    input: SendableRecordBatchStream,

    /// Hash table state for this aggregate stream.
    hash_table: AggregateHashTable<Final>,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,

    /// See comments for the same variable in [`PartialHashAggregateStream`]
    group_values_soft_limit: Option<usize>,
}

impl PartialHashAggregateStream {
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

        let hash_table = AggregateHashTable::<Partial>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;

        let reservation =
            MemoryConsumer::new(format!("PartialHashAggregateStream[{partition}]"))
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            hash_table,
            baseline_metrics,
            reservation,
            reduction_factor,
            group_values_soft_limit: agg.limit_options().map(|config| config.limit()),
        })
    }

    /// See comments in [`Self::group_values_soft_limit`] for details.
    fn hit_soft_group_limit(&self) -> bool {
        self.group_values_soft_limit
            .is_some_and(|limit| limit <= self.hash_table.building_group_count())
    }

    fn start_output(&mut self) -> Result<()> {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
        self.hash_table.start_output()
    }
}

impl Stream for PartialHashAggregateStream {
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

                        if self.hit_soft_group_limit() {
                            let timer = elapsed_compute.timer();
                            let result = self.start_output();
                            timer.done();

                            if let Err(e) = result {
                                return Poll::Ready(Some(Err(e)));
                            }

                            continue;
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
                        let timer = elapsed_compute.timer();
                        let result = self.start_output();
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

impl RecordBatchStream for PartialHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl FinalHashAggregateStream {
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

        let hash_table = AggregateHashTable::<Final>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;

        let reservation =
            MemoryConsumer::new(format!("FinalHashAggregateStream[{partition}]"))
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            hash_table,
            baseline_metrics,
            reservation,
            group_values_soft_limit: agg.limit_options().map(|config| config.limit()),
        })
    }

    /// See comments in [`Self::group_values_soft_limit`] for details.
    fn hit_soft_group_limit(&self) -> bool {
        self.group_values_soft_limit
            .is_some_and(|limit| limit <= self.hash_table.building_group_count())
    }

    fn start_output(&mut self) -> Result<()> {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
        self.hash_table.start_output()
    }
}

impl Stream for FinalHashAggregateStream {
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

                        if self.hit_soft_group_limit() {
                            let timer = elapsed_compute.timer();
                            let result = self.start_output();
                            timer.done();

                            if let Err(e) = result {
                                return Poll::Ready(Some(Err(e)));
                            }

                            continue;
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
                        let timer = elapsed_compute.timer();
                        let result = self.start_output();
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

impl RecordBatchStream for FinalHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
