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

//! Single-stage hash aggregation stream implementation.
//!
//! See comments in [`SingleHashAggregateStream`] for details.

use std::mem::size_of;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, assert_ne_or_internal_err};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{TaskContext, TryEmitter, async_try_stream};
use futures::stream::{Stream, StreamExt};

use super::aggregate_hash_table::{
    AggregateHashTable, OrderedAggregateTableMetrics, SingleMarker,
};
use super::spill::AggregateSpill;
use super::{AggregateExec, create_schema};
use crate::aggregates::AggregateMode;
use crate::metrics::{BaselineMetrics, RecordOutput, SpillMetrics};
use crate::stream::{EmptyRecordBatchStream, RecordBatchStreamAdapter};
use crate::{InputOrderMode, SendableRecordBatchStream};

/// Hash aggregation can run the full logical aggregation in one operator. This
/// stream implements the single stage for grouped hash aggregation.
///
/// This aggregation variant is useful when:
/// - There is only one partition (config `target_partitions` is set to 1)
/// - When input is already partitioned (`t` is backed by Parquet files, that is range/hash
///   partitioned on the group keys), the single aggregation mode is the most efficient
///   approach to use.
///
/// # Example
///
/// SELECT k, AVG(v) FROM t GROUP BY k;
///
/// ## Plan
/// AggregateExec(stage=single)
/// -- DataSourceExec(t)
///
/// ## Single Stage Behavior
/// Input: raw rows
/// Output: final aggregate values for all groups (for example, `AVG(x)`)
///
/// This stream implements the complete aggregation without a partial/final
/// split. It consumes raw input rows and emits final aggregate values.
///
/// # Grouping Sets
///
/// `GROUPING SETS`, `CUBE` and `ROLLUP` are expanded while consuming raw input:
/// every grouping set of an input batch is evaluated and interned into the same
/// hash table, the same way [`super::hash_stream::PartialHashAggregateStream`]
/// does it. When spilling, the expanded keys are sorted and replayed as a plain
/// group by.
///
/// # Spilling
///
/// During aggregation, group keys and states accumulate. If memory usage exceeds
/// the budget, spilling is triggered as follows:
/// 1. After aggregating a new input batch, if the memory reservation exceeds its
///    limit, spill all accumulated groups and states.
///    - Sort all groups by the group keys before spilling.
/// 2. Repeat until the input is exhausted.
/// 3. Perform a sort-preserving merge of all spill files and feed the merged output
///    into an ordered streaming aggregation, which ensures bounded memory usage and
///    evaluates the final result.
///    - [`OrderedFinalAggregateStream`](super::ordered_final_stream::OrderedFinalAggregateStream) is reused for the streaming aggregation.
///
/// # Optimization: DISTINCT LIMIT Soft Limit
///
/// When the input has only one partition or the input is already partitioned,
/// unordered distinct queries such as:
///
/// ```sql
/// SELECT DISTINCT x FROM t LIMIT 10;
/// ```
///
/// are optimized into a single-stage aggregate like:
///
/// ```txt
/// LimitExec, limit=10
/// --AggregateExec(Single), group_by=[x], aggr=[], soft_limit=10
/// ---- Scan(t)
/// ```
///
/// After each input batch, the stream checks whether the soft limit has been
/// reached. If so, it emits the accumulated groups and stops reading input.
///
/// This early termination is skipped after spilling has occurred to keep the
/// spill and replay path simple. In that case, the stream consumes the remaining
/// input and merges all spill runs before producing output.
///
/// This operator does not guarantee an exact limit because a single batch can
/// cross the threshold. The downstream limit operator enforces the exact result
/// size.
pub(crate) struct SingleHashAggregateStream {
    /// Output schema: group columns followed by final aggregate value columns.
    schema: SchemaRef,

    /// Input batches containing raw rows, not partial aggregate state.
    input: SendableRecordBatchStream,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys, accumulators, and spill sorting.
    reservation: MemoryReservation,

    /// See the "Optimization: DISTINCT LIMIT Soft Limit" section in
    /// [`SingleHashAggregateStream`] for details.
    group_values_soft_limit: Option<usize>,

    /// The hash table owns the lower-level state for emitting output batches.
    ///
    /// This is taken out when the stream starts executing.
    hash_table: Option<AggregateHashTable<SingleMarker>>,

    /// `None` if spilling is not supported by the configured `DiskManager`.
    spill_context: Option<Box<AggregateSpill>>,
}

impl SingleHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            AggregateMode::Single | AggregateMode::SinglePartitioned
        ));
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let input_schema = input.schema();
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);
        let spill_metrics = SpillMetrics::new(&agg.metrics, partition);
        let state_schema = Arc::new(create_schema(
            input_schema.as_ref(),
            agg.group_by(),
            agg.aggr_expr(),
            AggregateMode::Partial,
        )?);

        let hash_table = AggregateHashTable::<SingleMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            Arc::clone(&state_schema),
            batch_size,
        )?;

        let can_spill = context.runtime_env().disk_manager.tmp_files_enabled();
        let spill_context = if can_spill {
            Some(Box::new(AggregateSpill::try_new(
                "SingleHashAggregateSpill",
                agg,
                context,
                partition,
                batch_size,
                &InputOrderMode::Linear,
                &state_schema,
                spill_metrics,
            )?))
        } else {
            None
        };

        let reservation =
            MemoryConsumer::new(format!("SingleHashAggregateStream[{partition}]"))
                .with_can_spill(can_spill)
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            baseline_metrics,
            reservation,
            group_values_soft_limit: agg.limit_options().map(|config| config.limit()),
            hash_table: Some(hash_table),
            spill_context,
        })
    }

    pub(crate) fn into_stream(self) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);

        Box::pin(RecordBatchStreamAdapter::new(schema, self.create_stream()))
    }

    /// Entry point for the single hash aggregate flow
    ///
    /// See comments in [`SingleHashAggregateStream`] for high-level ideas.
    fn create_stream(mut self) -> impl Stream<Item = Result<RecordBatch>> {
        async_try_stream(|emitter| async move {
            let mut hash_table = self
                .hash_table
                .take()
                .expect("hash_table should not be None");

            let mut spill_context = self.spill_context.take();

            self.consume_input(&mut hash_table, &mut spill_context)
                .await?;
            self.close_input();

            match spill_context.filter(|s| s.has_spills()) {
                // - If spilled before, perform merging spill runs
                Some(spill_context) => {
                    self.produce_output_from_spills(hash_table, spill_context, emitter)
                        .await?
                }
                // Either all the input fit in memory or hit soft group limit with no spilling
                None => self.produce_output_from_memory(hash_table, emitter).await?,
            }

            Ok(())
        })
    }

    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
    }

    /// See comments in [`Self::group_values_soft_limit`] for details.
    fn hit_soft_group_limit(
        &self,
        hash_table: &AggregateHashTable<SingleMarker>,
    ) -> bool {
        self.group_values_soft_limit
            .is_some_and(|limit| limit <= hash_table.building_group_count())
    }

    /// Reserve memory for the current aggregate table.
    fn reservation_size_for_table(
        hash_table: &AggregateHashTable<SingleMarker>,
        spill_context: Option<&AggregateSpill>,
    ) -> usize {
        let table_size = hash_table.memory_size();
        if spill_context.is_some() {
            // Count extra space needed for in-memory sorting and spilling. Only
            // count memory for indices, the payload will be materialize incrementally
            // in smaller chunks.
            table_size.saturating_add(
                hash_table
                    .building_group_count()
                    .saturating_mul(size_of::<u32>()),
            )
        } else {
            table_size
        }
    }

    /// Read input stream, if no memory, then spill and continue reading - aggregate raw input batches into the hash table.
    ///
    /// Spilling: The table cannot reserve enough memory.
    ///           Move all current states into one fully group-key-sorted spill run.
    async fn consume_input(
        &mut self,
        hash_table: &mut AggregateHashTable<SingleMarker>,
        spill_context: &mut Option<Box<AggregateSpill>>,
    ) -> Result<()> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        while let Some(batch) = self.input.next().await.transpose()? {
            let _timer = elapsed_compute.timer();
            hash_table.aggregate_batch(&batch)?;

            // Soft group limits are usually small and rarely coincide with
            // spilling. Once spilling has occurred, skip this optimization to
            // make the internal logic simpler.
            let spilled = spill_context
                .as_ref()
                .is_some_and(|context| context.has_spills());
            if self.hit_soft_group_limit(hash_table) && !spilled {
                break;
            }

            // Check memory reservation, and potentially spill.
            let resize_result =
                self.reservation
                    .try_resize(Self::reservation_size_for_table(
                        hash_table,
                        spill_context.as_deref(),
                    ));

            match resize_result {
                Ok(()) => {}

                // The table cannot reserve enough memory.
                // Move all current states into one fully group-key-sorted spill run.
                Err(e @ DataFusionError::ResourcesExhausted(_)) => {
                    // OOM and don't support spilling from configuration
                    let spill_context = spill_context.as_mut().ok_or_else(|| e.context(
                        "Single hash aggregate cannot spill because temporary files are not enabled in the DiskManager",
                    ))?;

                    // Sanity check: impossible to OOM when there is no group aggregated.
                    assert_ne_or_internal_err!(
                        hash_table.building_group_count(),
                        0,
                        "Single hash aggregate ran out of memory with no aggregated groups"
                    );

                    // Sorts and spills one complete in-memory state run
                    let result = hash_table
                        .take_state_batch()
                        .and_then(|batch| spill_context.sort_and_spill(batch));

                    // Spilling shrinks the aggregate table and releases its accumulated
                    // memory. Update the reservation accordingly.
                    self.reservation
                        .try_resize(hash_table.memory_size())
                        .map_err(|e| {
                            e.context(
                                "Decreasing allocation after spilling should succeed",
                            )
                        })?;

                    result?;

                    // One sorted run was written; resume reading the original input.
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    /// Produce output from spills
    /// 1. Spill in progress in-memory hash table
    /// 2. Switch to ordered final stream
    /// 3. passthrough stream output
    async fn produce_output_from_spills(
        &mut self,
        mut hash_table: AggregateHashTable<SingleMarker>,
        mut spill_context: Box<AggregateSpill>,
        mut emitter: TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();

        // Input was exhausted after spilling. Spill the last in-memory run
        hash_table
            .take_state_batch()
            .and_then(|batch| spill_context.sort_and_spill(batch))?;

        // Construct the ordered input used to merge all spill files.
        let mut output_stream =
            self.switch_to_ordered_final_stream(hash_table, spill_context)?;

        timer.done();

        // Forwards output from the fully ordered stream that consumes the merged
        // spill runs.
        //
        // Not wrapping in a timer and not record output batches since this is now `merge_stream` responsibility
        // we just pass through
        while let Some(batch) = output_stream.next().await.transpose()? {
            emitter.emit(batch).await;
        }

        Ok(())
    }

    /// 1. Constructs a globally ordered input stream by applying a sort-preserving
    ///    merge to all spills.
    /// 2. Constructs a replay stream: an ordered final aggregate stream over the
    ///    fully ordered input constructed from the spills.
    ///
    /// Returns the replay stream
    fn switch_to_ordered_final_stream(
        &mut self,
        hash_table: AggregateHashTable<SingleMarker>,
        spill_context: Box<AggregateSpill>,
    ) -> Result<SendableRecordBatchStream> {
        let metrics = OrderedAggregateTableMetrics::from_hash_table(&hash_table);
        drop(hash_table);
        self.reservation.try_resize(0)?;
        spill_context.into_replay_stream(
            &self.baseline_metrics,
            metrics,
            self.reservation.new_empty(),
        )
    }

    /// Emit final aggregate value batches:
    /// Input was exhausted without spilling, or the soft group limit was reached.
    async fn produce_output_from_memory(
        &mut self,
        mut hash_table: AggregateHashTable<SingleMarker>,
        mut emitter: TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        let mut timer = elapsed_compute.timer();
        hash_table.start_output()?;

        loop {
            let Some(batch) = hash_table.next_output_batch()? else {
                // Only reachable when the table held no groups at all: a
                // non-empty table always reports its last batch together with
                // the `Done` state, which the `try_resize` below already zeroes.
                self.reservation.try_resize(0)?;
                return Ok(());
            };

            debug_assert!(batch.num_rows() > 0);

            // The table hands over its groups as they are materialized and
            // reports a size of 0 once it reaches `Done`, so this releases the
            // reservation before the final batch goes downstream.
            self.reservation.try_resize(hash_table.memory_size())?;

            timer.done();
            emitter
                .emit(batch.record_output(&self.baseline_metrics))
                .await;
            timer = elapsed_compute.timer();
        }
    }
}
