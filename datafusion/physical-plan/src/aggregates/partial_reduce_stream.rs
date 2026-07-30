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

//! Partial-reduce hash aggregation stream implementation.
//!
//! This stream is part of the incremental migration from
//! [`crate::aggregates::grouped_hash_stream::GroupedHashAggregateStream`].
//!
//! See issue for details: <https://github.com/apache/datafusion/issues/22710>

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{TaskContext, TryEmitter, async_try_stream};
use futures::stream::{Stream, StreamExt};

use super::AggregateExec;
use super::aggregate_hash_table::{AggregateHashTable, PartialReduceMarker};
use crate::metrics::{BaselineMetrics, SpillMetrics};
use crate::stream::{EmptyRecordBatchStream, ObservedStream, RecordBatchStreamAdapter};
use crate::{InputOrderMode, SendableRecordBatchStream};

/// Hash aggregation can combine multiple partial stages before final
/// evaluation. This stream implements the partial-reduce stage.
///
/// # Example
///
/// SELECT k, AVG(v) FROM t GROUP BY k;
///
/// ## Plan
/// AggregateExec(stage=final)
/// -- RepartitionExec(hash(k))
/// ---- AggregateExec(stage=partial_reduce)
/// ------ RepartitionExec(hash(k))
/// -------- AggregateExec(stage=partial)
///
/// Note: the example plan is only intended to demonstrate this stream's semantics;
/// the default DataFusion SQL planner does not produce plans in this shape.
///
/// This stream implements the middle partial-reduce aggregation in the plan above.
///
/// The motivation is to reduce shuffling traffic in a distributed setting. See
/// <https://github.com/datafusion-contrib/datafusion-distributed/issues/360>
///
/// ## Partial-Reduce Stage Behavior
/// Input: partial aggregate state rows
/// Output: merged partial aggregate state rows
///
/// This stage is useful for tree-reduce plans. It consumes the same schema as
/// a final aggregate stage, but emits the same schema as a partial aggregate
/// stage.
pub(crate) struct PartialReduceHashAggregateStream {
    /// Output schema: group columns followed by partial aggregate state columns.
    schema: SchemaRef,

    /// Input batches containing partial aggregate state rows.
    input: SendableRecordBatchStream,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,

    /// The hash table owns the lower-level state for emitting output batches.
    ///
    /// This is option since it will be taken on [`Self::create_stream`] to control the memory
    hash_table: Option<AggregateHashTable<PartialReduceMarker>>,
}

impl PartialReduceHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert_eq!(agg.mode, super::AggregateMode::PartialReduce);
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);

        let hash_table = AggregateHashTable::<PartialReduceMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;

        let reservation =
            MemoryConsumer::new(format!("PartialReduceHashAggregateStream[{partition}]"))
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            baseline_metrics,
            reservation,
            hash_table: Some(hash_table),
        })
    }

    pub(crate) fn into_stream(self) -> SendableRecordBatchStream {
        let schema_clone = Arc::clone(&self.schema);

        let cloned_metrics = self.baseline_metrics.clone();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema_clone,
            self.create_stream(),
        ));

        Box::pin(ObservedStream::new(stream, cloned_metrics, None))
    }

    /// Entry point for the partial-reduce hash aggregate state machine.
    ///
    /// See comments in [`PartialReduceHashAggregateStream`] for high-level ideas.
    ///
    /// State transition graph:
    ///
    /// ```text
    /// (start)
    ///   -> ReadingInput
    ///      The stream starts by polling partial-state input and merging those
    ///      states into the partial-reduce hash table.
    ///
    /// ReadingInput
    ///   -> ReadingInput
    ///      Aggregate one partial-state input batch, update the inner aggregate
    ///      hash table, and continue with the next input batch.
    ///
    ///   -> ProducingOutput
    ///      Input was exhausted. Move to the next state to start outputting
    ///      merged partial aggregate states.
    ///
    /// ProducingOutput
    ///   -> ProducingOutput
    ///      One merged partial-state output batch was yielded; repeat to
    ///      continue producing output incrementally.
    ///
    ///   -> Done
    ///      All merged partial-state output was emitted.
    ///
    /// Done
    ///   -> (end)
    /// ```
    fn create_stream(mut self) -> impl Stream<Item = Result<RecordBatch>> {
        async_try_stream(|emitter| async move {
            let mut hash_table = self
                .hash_table
                .take()
                .expect("hash table should not be None");

            self.consume_input(&mut hash_table).await?;
            self.produce_output(hash_table, emitter).await?;

            Ok(())
        })
    }

    fn start_output(
        &mut self,
        hash_table: &mut AggregateHashTable<PartialReduceMarker>,
    ) -> Result<()> {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
        hash_table.start_output()
    }

    /// Aggregate partial state batches into the hash table.
    ///
    /// See comments at [`Self::create_stream`] for details.
    async fn consume_input(
        &mut self,
        hash_table: &mut AggregateHashTable<PartialReduceMarker>,
    ) -> Result<()> {
        debug_assert!(hash_table.is_building());

        // Get a new input batch, aggregate it in the hash table
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        while let Some(batch) = self.input.next().await.transpose()? {
            let _timer = elapsed_compute.timer();
            hash_table.aggregate_batch(&batch)?;

            self.reservation.try_resize(hash_table.memory_size())?;
        }

        // Input ends, move to output state
        let _timer = elapsed_compute.timer();
        self.start_output(hash_table)?;

        Ok(())
    }

    /// Emit merged partial aggregate state batches.
    ///
    /// See comments at [`Self::create_stream`] for details.
    async fn produce_output(
        &mut self,
        mut hash_table: AggregateHashTable<PartialReduceMarker>,
        mut emitter: TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        debug_assert!(!hash_table.is_building());
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let mut timer = elapsed_compute.timer();

        while let Some(batch) = hash_table.next_output_batch()? {
            let _ = self.reservation.try_resize(hash_table.memory_size());
            debug_assert!(batch.num_rows() > 0);

            if hash_table.is_done() {
                drop(hash_table);
                self.reservation.try_resize(0)?;
                timer.done();
                emitter.emit(batch).await;

                return Ok(());
            }

            timer.done();
            emitter.emit(batch).await;
            timer = elapsed_compute.timer();
        }

        Ok(())
    }
}
