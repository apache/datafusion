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

//! Final aggregate stream for ordered partial-state input.

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

use super::AggregateExec;
use super::aggregate_hash_table::{FinalMarker, OrderedAggregateTable};
use super::group_values::GroupByMetrics;
use crate::aggregates::AggregateMode;
use crate::metrics::{BaselineMetrics, SpillMetrics};
use crate::sorts::IncrementalSortIterator;
use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use crate::spill::spill_manager::SpillManager;
use crate::stream::{EmptyRecordBatchStream, ObservedStream, RecordBatchStreamAdapter};
use crate::{InputOrderMode, SendableRecordBatchStream};

/// Final aggregate stream for `InputOrderMode::Sorted` and
/// `InputOrderMode::PartiallySorted`.
///
/// See comments at [`super::ordered_partial_stream::OrderedPartialAggregateStream`] for details.
///
/// # Spilling
///
/// This section is only for implementation notes, for background, see [`super::ordered_partial_stream::OrderedPartialAggregateStream`]
///
/// For partially sorted input, spilling works as follows:
///
/// - Reserve the table footprint plus one `u32` sort index per buffered group. The
///   extra index array is used in later sorting before spilling.
/// - On memory pressure, materialize all group states into one batch.
/// - Use [`IncrementalSortIterator`] to compute the full-batch index, then
///   materialize and write one sorted `batch_size` slice at a time. The original
///   batch and full index remain live until the run is written.
/// - After input ends, merge the sorted runs and replay them through a fully
///   ordered final aggregate stream.
pub(crate) struct OrderedFinalAggregateStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    reservation: MemoryReservation,
    baseline_metrics: BaselineMetrics,

    /// Will be taken on [`Self::aggregate`], we just keep it in the start until creating the stream itself
    table: Option<OrderedAggregateTable<FinalMarker>>,
    spill_context: Option<Box<OrderedFinalSpillContext>>,
}

/// Spill configuration and accumulated runs for partially ordered final
/// aggregation.
///
/// Each spill event drains all currently buffered groups, sorts their intermediate
/// states by the full group key, and writes them to one spill file. All files are
/// merged and replayed after the original input ends.
struct OrderedFinalSpillContext {
    /// Aggregate configuration
    agg: AggregateExec,
    /// Task context
    context: Arc<TaskContext>,
    /// Original partition index
    partition: usize,
    /// Target batch size from configuration
    batch_size: usize,
    /// Full group-key ordering, such ordering with be kept in: a) individual spill
    /// files, b) order after final merging and streaming aggregate
    spill_expr: LexOrdering,
    /// Spill I/O and metrics manager.
    spill_manager: SpillManager,
    /// Fully sorted spill runs waiting to be merged.
    spills: Vec<SortedSpillFile>,
}

impl OrderedFinalSpillContext {
    fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        batch_size: usize,
        input_order_mode: &InputOrderMode,
        spill_schema: &SchemaRef,
        spill_metrics: SpillMetrics,
    ) -> Result<Self> {
        let group_schema = agg.group_by.group_schema(spill_schema)?;
        let output_ordering = agg.cache.output_ordering();
        let InputOrderMode::PartiallySorted(order_indices) = input_order_mode else {
            return internal_err!("Ordered final spill requires partially ordered input");
        };
        let spill_indices = order_indices.iter().copied().chain(
            (0..group_schema.fields().len()).filter(|idx| !order_indices.contains(idx)),
        );
        let spill_sort_exprs = spill_indices.map(|idx| {
            let field = group_schema.field(idx);
            let output_expr = Column::new(field.name(), idx);
            let sort_options = output_ordering
                .and_then(|ordering| ordering.get_sort_options(&output_expr))
                .unwrap_or_default();
            PhysicalSortExpr::new(Arc::new(output_expr), sort_options)
        });
        let Some(spill_expr) = LexOrdering::new(spill_sort_exprs) else {
            return internal_err!("Ordered final spill expression is empty");
        };

        let spill_manager = SpillManager::new(
            context.runtime_env(),
            spill_metrics,
            Arc::clone(spill_schema),
        )
        .with_compression_type(context.session_config().spill_compression());

        Ok(Self {
            agg: agg.clone(),
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
    /// See [`OrderedFinalAggregateStream`] for spilling details.
    fn spill_table(
        &mut self,
        table: &mut OrderedAggregateTable<FinalMarker>,
    ) -> Result<()> {
        let Some(batch) = table.take_state_batch()? else {
            return Ok(());
        };

        let sorted_iter =
            IncrementalSortIterator::new(batch, self.spill_expr.clone(), self.batch_size);
        let spill_file = self
            .spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                sorted_iter,
                "OrderedFinalAggregateSpill",
            )?;

        let Some((file, max_record_batch_memory)) = spill_file else {
            return internal_err!("Ordered final aggregation produced an empty spill");
        };

        self.spills.push(SortedSpillFile {
            file,
            max_record_batch_memory,
        });

        Ok(())
    }

    /// Merges every sorted run and finalizes it through the fully ordered path.
    fn into_replay_stream(
        self,
        baseline_metrics: &BaselineMetrics,
        group_by_metrics: GroupByMetrics,
        reservation: MemoryReservation,
    ) -> Result<SendableRecordBatchStream> {
        let Self {
            agg,
            context,
            partition,
            batch_size,
            spill_expr,
            spill_manager,
            spills,
        } = self;

        let spill_schema = Arc::clone(spill_manager.schema());
        let merged = StreamingMergeBuilder::new()
            .with_schema(spill_schema)
            .with_spill_manager(spill_manager)
            .with_sorted_spill_files(spills)
            .with_expressions(&spill_expr)
            .with_metrics(baseline_metrics.intermediate())
            .with_batch_size(batch_size)
            .with_reservation(reservation)
            .build()?;

        Ok(OrderedFinalAggregateStream::new_with_input_and_metrics(
            &agg,
            &context,
            partition,
            merged,
            &InputOrderMode::Sorted,
            baseline_metrics.clone(),
            group_by_metrics,
            None,
        )?
        .into_stream())
    }
}

impl OrderedFinalAggregateStream {
    pub(crate) fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            AggregateMode::Final | AggregateMode::FinalPartitioned
        ));
        debug_assert_ne!(agg.input_order_mode, InputOrderMode::Linear);

        let input = agg.input.execute(partition, Arc::clone(context))?;
        Self::new_with_input(agg, context, partition, input, &agg.input_order_mode)
    }

    pub(in crate::aggregates) fn new_with_input(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        input: SendableRecordBatchStream,
        input_order_mode: &InputOrderMode,
    ) -> Result<Self> {
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);
        let group_by_metrics = GroupByMetrics::new(&agg.metrics, partition);
        let spill_metrics = SpillMetrics::new(&agg.metrics, partition);
        Self::new_with_input_and_metrics(
            agg,
            context,
            partition,
            input,
            input_order_mode,
            baseline_metrics,
            group_by_metrics,
            Some(spill_metrics),
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "keeps replay metric reuse explicit"
    )]
    fn new_with_input_and_metrics(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        input: SendableRecordBatchStream,
        input_order_mode: &InputOrderMode,
        baseline_metrics: BaselineMetrics,
        group_by_metrics: GroupByMetrics,
        spill_metrics: Option<SpillMetrics>,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            AggregateMode::Final | AggregateMode::FinalPartitioned
        ));
        debug_assert_ne!(*input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input_schema = input.schema();
        let batch_size = context.session_config().batch_size();

        let can_spill = matches!(input_order_mode, InputOrderMode::PartiallySorted(_))
            && context.runtime_env().disk_manager.tmp_files_enabled();
        let spill_context = if can_spill {
            let Some(spill_metrics) = spill_metrics else {
                return internal_err!("Spillable ordered final stream requires metrics");
            };
            Some(Box::new(OrderedFinalSpillContext::new(
                agg,
                context,
                partition,
                batch_size,
                input_order_mode,
                &input_schema,
                spill_metrics,
            )?))
        } else {
            None
        };

        let table = OrderedAggregateTable::<FinalMarker>::new_with_input_order(
            agg,
            &input_schema,
            Arc::clone(&schema),
            batch_size,
            input_order_mode,
            group_by_metrics,
        )?;

        let reservation =
            MemoryConsumer::new(format!("OrderedFinalAggregateStream[{partition}]"))
                .with_can_spill(can_spill)
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            reservation,
            baseline_metrics,
            table: Some(table),
            spill_context,
        })
    }

    pub(crate) fn into_stream(self) -> SendableRecordBatchStream {
        let schema_clone = Arc::clone(&self.schema);

        let cloned_metrics = self.baseline_metrics.clone();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema_clone,
            self.aggregate(),
        ));

        Box::pin(ObservedStream::new(stream, cloned_metrics, None))
    }

    /// Entry point for the ordered final aggregate stream
    ///
    /// See comments in [`OrderedFinalAggregateStream`] for high-level ideas.
    ///
    /// State transition graph:
    ///
    /// ```text
    /// (start)
    ///   -> ReadingInput
    ///      The stream starts by polling ordered partial-state input and merging
    ///      those states into the ordered final aggregate table.
    ///
    /// ReadingInput
    ///   -> ReadingInput
    ///      Merge one input batch. If it fits in memory, optionally yield groups
    ///      proven complete by the input ordering, then read the next batch.
    ///   -> Spilling
    ///      The table cannot reserve enough memory. Move all current states into
    ///      one fully group-key-sorted spill run.
    ///   -> ProducingOutput
    ///      Input was exhausted without spilling. Mark every remaining group as
    ///      complete and produce its final result.
    ///   -> PreparingMergeInput
    ///      Input was exhausted after spilling. Spill the last in-memory run and
    ///      construct the ordered input used to merge all spill files.
    ///
    /// Spilling
    ///   -> ReadingInput
    ///      One sorted run was written; resume reading the original input.
    ///
    /// PreparingMergeInput
    ///   Spill the final in-memory run and build the input ordered replay stream.
    ///   -> MergingSpills
    ///      The final run was spilled and the ordered replay stream was built.
    ///
    /// MergingSpills
    ///   Aggregate the merged spill runs and emit final results.
    ///   -> MergingSpills
    ///      Forward one result batch from the fully ordered replay stream that
    ///      consumes the sort-preserving merge.
    ///   -> Done
    ///      The merged spill input was fully aggregated.
    ///
    /// ProducingOutput
    ///   -> ProducingOutput
    ///      One remaining final aggregate batch was yielded; repeat to continue
    ///      draining the table.
    ///   -> Done
    ///      All remaining groups were emitted.
    ///
    /// Done
    ///   -> (end)
    /// ```
    fn aggregate(mut self) -> impl Stream<Item = Result<RecordBatch>> {
        async_try_stream(|mut emitter| async move {
            // Take the table on init since we want to control when the table is dropped and free memory.
            let mut table = self.table.take().unwrap();

            let spilled = self.consume_input(&mut table, &mut emitter).await?;

            if spilled {
                let mut merging_spills_stream = self.prepare_merge_spills(table)?;

                // Forwards output from the fully ordered stream that consumes the merged
                // spill runs.
                //
                // See comments at `create_stream()` for details.
                while let Some(batch) = merging_spills_stream.next().await.transpose()? {
                    emitter.emit(batch).await;
                }

                // Make sure empty
                self.reservation.try_resize(0)?;
            } else {
                table.input_done();
                self.produce_output(table, &mut emitter).await?;
            };

            Ok(())
        })
    }

    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
    }

    /// Reserve memory for the current aggregate table.
    fn reservation_size_for_table(
        table: &OrderedAggregateTable<FinalMarker>,
        spill_context: Option<&OrderedFinalSpillContext>,
    ) -> usize {
        let table_size = table.memory_size();
        if spill_context.is_some() {
            // See `OrderedFinalAggregateStream` comments for how is it estimated
            table_size.saturating_add(table.num_groups().saturating_mul(size_of::<u32>()))
        } else {
            table_size
        }
    }

    /// Consumes one ordered partial-state input batch, then immediately emits
    /// finalized groups if the ordering proves any group is ready.
    ///
    /// See comments at [`Self::aggregate`] for details.
    ///
    /// Returns whether there are any spill files
    async fn consume_input(
        &mut self,
        table: &mut OrderedAggregateTable<FinalMarker>,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<bool> {
        while let Some(batch) = self.input.next().await.transpose()? {
            let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
            let timer = elapsed_compute.timer();
            table.aggregate_batch(&batch)?;

            // Check memory reservation, and potentially spill.
            let resize_result =
                self.reservation
                    .try_resize(Self::reservation_size_for_table(
                        table,
                        self.spill_context.as_deref(),
                    ));

            match resize_result {
                Ok(()) => {}
                Err(e @ DataFusionError::ResourcesExhausted(_)) => {
                    let Some(mut spill_context) = self.spill_context.take() else {
                        return Err(e);
                    };
                    if table.is_empty() {
                        return Err(e);
                    }

                    timer.done();
                    self.handle_spilling(table, &mut spill_context)?;

                    self.spill_context = Some(spill_context);

                    continue;
                }
                Err(e) => return Err(e),
            }

            let result = if self
                .spill_context
                .as_ref()
                .is_some_and(|spill_context| spill_context.has_spills())
            {
                // Once one incomplete run is spilled, every remaining state
                // must participate in replay so no group is finalized twice.
                None
            } else {
                table.next_output_batch()?
            };

            // Some finalized groups can be emitted. Yield them, then
            // continue aggregating input in the current state.
            let Some(batch) = result else {
                // Can't do early emit, continue aggregating.
                continue;
            };

            self.reservation
                .try_resize(Self::reservation_size_for_table(
                    table,
                    self.spill_context.as_deref(),
                ))?;

            drop(timer);
            emitter.emit(batch).await;
        }

        self.close_input();

        Ok(self.spill_context.as_ref().is_some_and(|c| c.has_spills()))
    }

    /// Sorts and spills one complete in-memory state run, then resumes input.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_spilling(
        &mut self,
        table: &mut OrderedAggregateTable<FinalMarker>,
        spill_context: &mut Box<OrderedFinalSpillContext>,
    ) -> Result<()> {
        // Sanity check: it's impossible to OOM when the table is empty
        if table.is_empty() {
            return internal_err!(
                "Ordered final aggregation entered Spilling with an empty table"
            );
        }

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();
        let result = spill_context.spill_table(table);

        // Spilling shrinks the aggregate table and releases its accumulated
        // memory. Update the reservation accordingly.
        if let Err(e) = self.reservation.try_resize(table.memory_size()) {
            return Err(e.context("Decreasing allocation after spilling should succeed"));
        }

        result?;

        // Finished spilling the aggregate table, continue aggregating from input
        Ok(())
    }

    /// 1. Spills the last in-memory run.
    /// 2. Constructs a globally ordered input stream by applying a sort-preserving
    ///    merge to all spills.
    /// 3. Constructs a replay stream: an ordered aggregate stream over the fully
    ///    ordered input constructed from the spills.
    ///
    /// See comments at [`Self::aggregate`] for details.
    ///
    /// Returns the merged spill stream
    fn prepare_merge_spills(
        &mut self,
        mut table: OrderedAggregateTable<FinalMarker>,
    ) -> Result<SendableRecordBatchStream> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        let mut spill_context = self
            .spill_context
            .take()
            .expect("must have spill context when merging input");
        spill_context.spill_table(&mut table)?;
        let group_by_metrics = table.group_by_metrics();
        drop(table);
        self.reservation.try_resize(0)?;
        (*spill_context).into_replay_stream(
            &self.baseline_metrics,
            group_by_metrics,
            self.reservation.new_empty(),
        )
    }

    /// Emits all batches after input is exhausted.
    ///
    /// `table.input_done()` has already made every remaining group safe to emit,
    /// so this state keeps draining until the table is empty.
    ///
    /// See comments at [`Self::aggregate`] for details.
    async fn produce_output(
        &mut self,
        mut table: OrderedAggregateTable<FinalMarker>,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let mut timer = elapsed_compute.timer();

        while let Some(batch) = table.next_output_batch()? {
            if table.is_empty() {
                drop(table);
                self.reservation.try_resize(0)?;

                drop(timer);
                emitter.emit(batch).await;

                return Ok(());
            }

            self.reservation.try_resize(table.memory_size())?;

            timer.done();
            emitter.emit(batch).await;
            timer = elapsed_compute.timer();
        }

        Ok(())
    }
}
