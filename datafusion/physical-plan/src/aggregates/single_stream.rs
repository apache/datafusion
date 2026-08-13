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
//! This stream is part of the incremental migration from
//! [`crate::aggregates::grouped_hash_stream::GroupedHashAggregateStream`].
//!
//! See issue for details: <https://github.com/apache/datafusion/issues/22710>

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{TaskContext, TryEmitter, async_try_stream};
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::stream::{Stream, StreamExt};

use super::aggregate_hash_table::{AggregateHashTable, SingleMarker};
use super::group_values::GroupByMetrics;
use super::ordered_final_stream::OrderedFinalAggregateStream;
use super::{AggregateExec, create_schema};
use crate::aggregates::AggregateMode;
use crate::metrics::{BaselineMetrics, SpillMetrics};
use crate::sorts::IncrementalSortIterator;
use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use crate::spill::spill_manager::SpillManager;
use crate::stream::{EmptyRecordBatchStream, ObservedStream, RecordBatchStreamAdapter};
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
///    - [`OrderedFinalAggregateStream`] is reused for the streaming aggregation.
pub(crate) struct SingleHashAggregateStream {
    /// Output schema: group columns followed by final aggregate value columns.
    schema: SchemaRef,

    /// Input batches containing raw rows, not partial aggregate state.
    input: SendableRecordBatchStream,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys, accumulators, and spill sorting.
    reservation: MemoryReservation,

    /// The hash table owns the lower-level state for emitting output batches.
    ///
    /// This is optional so `create_stream` can take ownership and control when
    /// the table's memory is released.
    hash_table: Option<AggregateHashTable<SingleMarker>>,

    /// Spill configuration when temporary files are enabled.
    spill_context: Option<Box<SingleSpillContext>>,
}

/// Spill configuration and accumulated runs for single hash aggregation.
///
/// Each spill event drains all currently buffered groups, sorts their intermediate
/// states by the full group key, and writes them to one spill file. All files are
/// merged and replayed after the original input ends.
struct SingleSpillContext {
    /// Aggregate configuration used to construct the final replay stream.
    ///
    /// Spilled rows already contain evaluated group keys and intermediate
    /// aggregate states. Replay must therefore use final aggregation semantics
    /// and column-based group expressions rather than evaluating the raw input
    /// expressions a second time. After the spill files are merged into ordered
    /// input, this configuration is used to construct an
    /// [`OrderedFinalAggregateStream`], and perform the final evaluation step.
    final_agg: AggregateExec,
    /// Task context.
    context: Arc<TaskContext>,
    /// Original partition index.
    partition: usize,
    /// Target batch size from configuration.
    batch_size: usize,
    /// Full group-key ordering kept by every spill file and the merged input.
    spill_expr: LexOrdering,
    /// Spill I/O and metrics manager.
    spill_manager: SpillManager,
    /// Spill runs waiting to be merged, they're all sorted by full group-by keys.
    spills: Vec<SortedSpillFile>,
}

impl SingleSpillContext {
    fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        batch_size: usize,
        spill_schema: &SchemaRef,
        spill_metrics: SpillMetrics,
    ) -> Result<Self> {
        let group_schema = agg.group_by.group_schema(&agg.input().schema())?;
        let output_ordering = agg.cache.output_ordering();
        let spill_sort_exprs =
            group_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(idx, field)| {
                    let output_expr = Column::new(field.name(), idx);
                    let sort_options = output_ordering
                        .and_then(|ordering| ordering.get_sort_options(&output_expr))
                        .unwrap_or_default();
                    PhysicalSortExpr::new(Arc::new(output_expr), sort_options)
                });
        let Some(spill_expr) = LexOrdering::new(spill_sort_exprs) else {
            return internal_err!("Single hash aggregate spill expression is empty");
        };

        let spill_manager = SpillManager::new(
            context.runtime_env(),
            spill_metrics,
            Arc::clone(spill_schema),
        )
        .with_compression_type(context.session_config().spill_compression());

        // See `SingleSpillContext::final_agg` comments for `final_agg`'s usage
        let mut final_agg = agg.clone();
        final_agg.mode = match agg.mode {
            AggregateMode::Single => AggregateMode::Final,
            AggregateMode::SinglePartitioned => AggregateMode::FinalPartitioned,
            mode => {
                return internal_err!(
                    "Single hash aggregate spill cannot replay aggregate mode {mode:?}"
                );
            }
        };
        final_agg.group_by = Arc::new(agg.group_by.as_final());
        final_agg.input_order_mode = InputOrderMode::Sorted;

        Ok(Self {
            final_agg,
            context: Arc::clone(context),
            partition,
            batch_size,
            spill_expr,
            spill_manager,
            spills: vec![],
        })
    }

    fn has_spills(&self) -> bool {
        !self.spills.is_empty()
    }

    /// Sorts and spills the aggregated groups. Memory reservation should be updated
    /// by the caller.
    ///
    /// Individual spill files are ordered by the `group by` keys.
    ///
    /// See [`SingleHashAggregateStream`] for spilling details.
    fn spill_table(
        &mut self,
        hash_table: &mut AggregateHashTable<SingleMarker>,
    ) -> Result<()> {
        let Some(batch) = hash_table.take_state_batch()? else {
            return Ok(());
        };

        let sorted_iter =
            IncrementalSortIterator::new(batch, self.spill_expr.clone(), self.batch_size);
        let spill_file = self
            .spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                sorted_iter,
                "SingleHashAggregateSpill",
            )?;

        let Some((file, max_record_batch_memory)) = spill_file else {
            return internal_err!("Single hash aggregation produced an empty spill");
        };

        self.spills.push(SortedSpillFile {
            file,
            max_record_batch_memory,
        });

        Ok(())
    }

    /// Merges every sorted run, and do the aggregate evaluation with
    /// [`OrderedFinalAggregateStream`]
    fn into_replay_stream(
        self,
        baseline_metrics: &BaselineMetrics,
        group_by_metrics: GroupByMetrics,
        reservation: MemoryReservation,
    ) -> Result<SendableRecordBatchStream> {
        let Self {
            final_agg,
            context,
            partition,
            batch_size,
            spill_expr,
            spill_manager,
            spills,
        } = self;

        let spill_schema = Arc::clone(spill_manager.schema());
        // The merge and replay table are two components of the same aggregate
        // operator. Keep them under one consumer registration so a fair memory
        // pool does not divide this operator's quota between its own phases.
        let merge_reservation = reservation.new_empty();
        let merged = StreamingMergeBuilder::new()
            .with_schema(spill_schema)
            .with_spill_manager(spill_manager)
            .with_sorted_spill_files(spills)
            .with_expressions(&spill_expr)
            .with_metrics(baseline_metrics.intermediate())
            .with_batch_size(batch_size)
            .with_reservation(merge_reservation)
            .build()?;
        let replay = OrderedFinalAggregateStream::new_with_input_and_metrics(
            &final_agg,
            &context,
            partition,
            merged,
            &InputOrderMode::Sorted,
            baseline_metrics.intermediate(),
            group_by_metrics,
            None,
            reservation,
        )?;
        Ok(Box::pin(replay))
    }
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
            &agg.group_by,
            &agg.aggr_expr,
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
            Some(Box::new(SingleSpillContext::new(
                agg,
                context,
                partition,
                batch_size,
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
            hash_table: Some(hash_table),
            spill_context,
        })
    }

    pub(crate) fn into_stream(self) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let baseline_metrics = self.baseline_metrics.clone();
        let stream =
            Box::pin(RecordBatchStreamAdapter::new(schema, self.create_stream()));

        Box::pin(ObservedStream::new(stream, baseline_metrics, None))
    }

    /// State transitions are implemented using the generator pattern; see the
    /// comments in [`async_try_stream`].
    ///
    /// Conceptually, the stream reads input and spills whenever its reservation
    /// is exhausted. It then either drains the in-memory table directly or merges
    /// and replays the spilled runs through an ordered final aggregation.
    fn create_stream(mut self) -> impl Stream<Item = Result<RecordBatch>> {
        async_try_stream(|mut emitter| async move {
            let mut hash_table = self
                .hash_table
                .take()
                .expect("SingleHashAggregateStream hash table should not be None");
            let mut spill_context = self.spill_context.take();

            self.handle_reading_input(&mut hash_table, &mut spill_context)
                .await?;
            self.close_input();

            match spill_context {
                Some(spill_context) if spill_context.has_spills() => {
                    self.handle_spilled_output(hash_table, spill_context, &mut emitter)
                        .await?;
                }
                _ => {
                    let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                    let timer = elapsed_compute.timer();
                    let result = hash_table.start_output();
                    timer.done();
                    result?;

                    self.handle_producing_output(hash_table, &mut emitter)
                        .await?;
                }
            }

            Ok(())
        })
    }

    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
    }

    /// Reserve memory for the current aggregate table.
    fn reservation_size_for_table(
        hash_table: &AggregateHashTable<SingleMarker>,
        spill_context: Option<&SingleSpillContext>,
    ) -> usize {
        let table_size = hash_table.memory_size();
        if spill_context.is_some() {
            // See `SingleHashAggregateStream` comments for how this is estimated.
            table_size.saturating_add(
                hash_table
                    .building_group_count()
                    .saturating_mul(size_of::<u32>()),
            )
        } else {
            table_size
        }
    }

    /// Consumes raw input batches and updates the single-stage hash table.
    ///
    /// When the table exceeds its reservation, all accumulated groups are
    /// spilled before input processing resumes.
    async fn handle_reading_input(
        &mut self,
        hash_table: &mut AggregateHashTable<SingleMarker>,
        spill_context: &mut Option<Box<SingleSpillContext>>,
    ) -> Result<()> {
        debug_assert!(hash_table.is_building());
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        while let Some(batch) = self.input.next().await.transpose()? {
            let timer = elapsed_compute.timer();
            hash_table.aggregate_batch(&batch)?;
            timer.done();

            let timer = elapsed_compute.timer();
            let resize_result =
                self.reservation
                    .try_resize(Self::reservation_size_for_table(
                        hash_table,
                        spill_context.as_deref(),
                    ));
            timer.done();

            match resize_result {
                Ok(()) => {}
                Err(e @ DataFusionError::ResourcesExhausted(_)) => {
                    let Some(spill_context) = spill_context.as_deref_mut() else {
                        return Err(e.context(
                            "Single hash aggregate cannot spill because temporary files are not enabled in the DiskManager",
                        ));
                    };
                    if hash_table.building_group_count() == 0 {
                        return internal_err!(
                            "Single hash aggregate ran out of memory with no aggregated groups"
                        );
                    }
                    self.handle_spilling(hash_table, spill_context)?;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    /// Sorts and spills one complete in-memory state run, then resumes input.
    fn handle_spilling(
        &mut self,
        hash_table: &mut AggregateHashTable<SingleMarker>,
        spill_context: &mut SingleSpillContext,
    ) -> Result<()> {
        // Sanity check: it is impossible to OOM when the table is empty.
        if hash_table.building_group_count() == 0 {
            return internal_err!(
                "Single hash aggregation entered Spilling with an empty table"
            );
        }

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let mut result = spill_context.spill_table(hash_table);

        // Spilling shrinks the aggregate table and releases its accumulated
        // memory. Update the reservation accordingly.
        if let Err(e) = self.reservation.try_resize(hash_table.memory_size()) {
            result =
                Err(e.context("Decreasing allocation after spilling should succeed"));
        }

        timer.done();
        result
    }

    /// Spills the final in-memory run, merges all runs, and emits the replayed
    /// final aggregate output.
    async fn handle_spilled_output(
        &mut self,
        mut hash_table: AggregateHashTable<SingleMarker>,
        mut spill_context: Box<SingleSpillContext>,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();

        spill_context.spill_table(&mut hash_table)?;
        let group_by_metrics = hash_table.group_by_metrics().clone();
        drop(hash_table);
        self.reservation.try_resize(0)?;

        let replay_reservation = self.reservation.new_empty();
        let mut replay = (*spill_context).into_replay_stream(
            &self.baseline_metrics,
            group_by_metrics,
            replay_reservation,
        )?;
        timer.done();

        while let Some(batch) = replay.next().await.transpose()? {
            emitter.emit(batch).await;
        }

        Ok(())
    }

    /// Emits final aggregate value batches after input is exhausted.
    async fn handle_producing_output(
        &mut self,
        mut hash_table: AggregateHashTable<SingleMarker>,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        debug_assert!(!hash_table.is_building());
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        loop {
            let timer = elapsed_compute.timer();
            let batch = hash_table.next_output_batch();
            timer.done();

            let Some(batch) = batch? else {
                drop(hash_table);
                self.reservation.try_resize(0)?;
                return Ok(());
            };
            debug_assert!(batch.num_rows() > 0);

            if hash_table.is_done() {
                drop(hash_table);
                self.reservation.try_resize(0)?;
                emitter.emit(batch).await;
                return Ok(());
            }

            self.reservation.try_resize(hash_table.memory_size())?;
            emitter.emit(batch).await;
        }
    }
}
