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

use std::ops::ControlFlow;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::stream::{Stream, StreamExt};

use super::AggregateExec;
use super::aggregate_hash_table::{FinalMarker, OrderedAggregateTable};
use super::group_values::GroupByMetrics;
use crate::aggregates::AggregateMode;
use crate::metrics::{BaselineMetrics, RecordOutput, SpillMetrics};
use crate::sorts::IncrementalSortIterator;
use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use crate::spill::spill_manager::SpillManager;
use crate::stream::EmptyRecordBatchStream;
use crate::{InputOrderMode, RecordBatchStream, SendableRecordBatchStream};

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
    state: Option<OrderedFinalAggregateState>,
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

/// See comments at `poll_next()` for details.
enum OrderedFinalAggregateState {
    ReadingInput {
        table: OrderedAggregateTable<FinalMarker>,
        spill_context: Option<Box<OrderedFinalSpillContext>>,
    },
    Spilling {
        table: OrderedAggregateTable<FinalMarker>,
        spill_context: Box<OrderedFinalSpillContext>,
    },
    ProducingOutput {
        table: OrderedAggregateTable<FinalMarker>,
    },
    PreparingMergeInput {
        table: OrderedAggregateTable<FinalMarker>,
        spill_context: Box<OrderedFinalSpillContext>,
    },
    MergingSpills {
        stream: SendableRecordBatchStream,
    },
    Done,
}

type OrderedFinalAggregatePoll = Poll<Option<Result<RecordBatch>>>;
type OrderedFinalAggregateStateTransition = ControlFlow<
    (OrderedFinalAggregatePoll, OrderedFinalAggregateState),
    OrderedFinalAggregateState,
>;

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
        let replay = OrderedFinalAggregateStream::new_with_input_and_metrics(
            &agg,
            &context,
            partition,
            merged,
            &InputOrderMode::Sorted,
            baseline_metrics.clone(),
            group_by_metrics,
            None,
        )?;
        Ok(Box::pin(replay))
    }
}

impl OrderedFinalAggregateStream {
    pub fn new(
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
            state: Some(OrderedFinalAggregateState::ReadingInput {
                table,
                spill_context,
            }),
        })
    }

    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
    }

    fn break_with_internal_err(message: &str) -> OrderedFinalAggregateStateTransition {
        ControlFlow::Break((
            Poll::Ready(Some(internal_err!("{message}"))),
            OrderedFinalAggregateState::Done,
        ))
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
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_reading_input(
        &mut self,
        cx: &mut Context<'_>,
        original_state: OrderedFinalAggregateState,
    ) -> OrderedFinalAggregateStateTransition {
        let OrderedFinalAggregateState::ReadingInput {
            mut table,
            spill_context,
        } = original_state
        else {
            return Self::break_with_internal_err(
                "Ordered final aggregate stream expected ReadingInput state",
            );
        };

        match self.input.poll_next_unpin(cx) {
            Poll::Pending => ControlFlow::Break((
                Poll::Pending,
                OrderedFinalAggregateState::ReadingInput {
                    table,
                    spill_context,
                },
            )),
            Poll::Ready(Some(Ok(batch))) => {
                let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                let timer = elapsed_compute.timer();
                let result = table.aggregate_batch(&batch);
                timer.done();

                if let Err(e) = result {
                    return ControlFlow::Break((
                        Poll::Ready(Some(Err(e))),
                        OrderedFinalAggregateState::ReadingInput {
                            table,
                            spill_context,
                        },
                    ));
                }

                // Check memory reservation, and potentially spill.
                let timer = elapsed_compute.timer();
                let resize_result =
                    self.reservation
                        .try_resize(Self::reservation_size_for_table(
                            &table,
                            spill_context.as_deref(),
                        ));
                timer.done();
                match resize_result {
                    Ok(()) => {}
                    Err(e @ DataFusionError::ResourcesExhausted(_)) => {
                        let Some(spill_context) = spill_context else {
                            return ControlFlow::Break((
                                Poll::Ready(Some(Err(e))),
                                OrderedFinalAggregateState::Done,
                            ));
                        };
                        if table.is_empty() {
                            return ControlFlow::Break((
                                Poll::Ready(Some(Err(e))),
                                OrderedFinalAggregateState::Done,
                            ));
                        }
                        return ControlFlow::Continue(
                            OrderedFinalAggregateState::Spilling {
                                table,
                                spill_context,
                            },
                        );
                    }
                    Err(e) => {
                        return ControlFlow::Break((
                            Poll::Ready(Some(Err(e))),
                            OrderedFinalAggregateState::Done,
                        ));
                    }
                }

                let result = if spill_context
                    .as_ref()
                    .is_some_and(|spill_context| spill_context.has_spills())
                {
                    // Once one incomplete run is spilled, every remaining state
                    // must participate in replay so no group is finalized twice.
                    Ok(None)
                } else {
                    let timer = elapsed_compute.timer();
                    let result = table.next_output_batch();
                    timer.done();
                    result
                };

                match result {
                    // Some finalized groups can be emitted. Yield them, then
                    // continue aggregating input in the current state.
                    Ok(Some(batch)) => {
                        if let Err(e) =
                            self.reservation
                                .try_resize(Self::reservation_size_for_table(
                                    &table,
                                    spill_context.as_deref(),
                                ))
                        {
                            return ControlFlow::Break((
                                Poll::Ready(Some(Err(e))),
                                OrderedFinalAggregateState::Done,
                            ));
                        }
                        let next_state = OrderedFinalAggregateState::ReadingInput {
                            table,
                            spill_context,
                        };

                        ControlFlow::Break((
                            Poll::Ready(Some(Ok(
                                batch.record_output(&self.baseline_metrics)
                            ))),
                            next_state,
                        ))
                    }
                    // Can't do early emit, continue aggregating.
                    Ok(None) => {
                        ControlFlow::Continue(OrderedFinalAggregateState::ReadingInput {
                            table,
                            spill_context,
                        })
                    }
                    Err(e) => ControlFlow::Break((
                        Poll::Ready(Some(Err(e))),
                        OrderedFinalAggregateState::ReadingInput {
                            table,
                            spill_context,
                        },
                    )),
                }
            }
            Poll::Ready(Some(Err(e))) => ControlFlow::Break((
                Poll::Ready(Some(Err(e))),
                OrderedFinalAggregateState::ReadingInput {
                    table,
                    spill_context,
                },
            )),
            Poll::Ready(None) => {
                self.close_input();
                match spill_context {
                    Some(spill_context) if spill_context.has_spills() => {
                        ControlFlow::Continue(
                            OrderedFinalAggregateState::PreparingMergeInput {
                                table,
                                spill_context,
                            },
                        )
                    }
                    _ => {
                        table.input_done();
                        ControlFlow::Continue(
                            OrderedFinalAggregateState::ProducingOutput { table },
                        )
                    }
                }
            }
        }
    }

    /// Sorts and spills one complete in-memory state run, then resumes input.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_spilling(
        &mut self,
        original_state: OrderedFinalAggregateState,
    ) -> OrderedFinalAggregateStateTransition {
        let OrderedFinalAggregateState::Spilling {
            mut table,
            mut spill_context,
        } = original_state
        else {
            return Self::break_with_internal_err(
                "Ordered final aggregate stream expected Spilling state",
            );
        };

        // Sanity check: it's impossible to OOM when the table is empty
        if table.is_empty() {
            return ControlFlow::Break((
                Poll::Ready(Some(internal_err!(
                    "Ordered final aggregation entered Spilling with an empty table"
                ))),
                OrderedFinalAggregateState::Done,
            ));
        }

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let mut result = spill_context.spill_table(&mut table);

        // Spilling shrinks the aggregate table and releases its accumulated
        // memory. Update the reservation accordingly.
        if let Err(e) = self.reservation.try_resize(table.memory_size()) {
            result =
                Err(e.context("Decreasing allocation after spilling should succeed"));
        }

        timer.done();

        match result {
            // Finished spilling the aggregate table, continue aggregating from input
            Ok(()) => ControlFlow::Continue(OrderedFinalAggregateState::ReadingInput {
                table,
                spill_context: Some(spill_context),
            }),
            Err(e) => ControlFlow::Break((
                Poll::Ready(Some(Err(e))),
                OrderedFinalAggregateState::Done,
            )),
        }
    }

    /// 1. Spills the last in-memory run.
    /// 2. Constructs a globally ordered input stream by applying a sort-preserving
    ///    merge to all spills.
    /// 3. Constructs a replay stream: an ordered aggregate stream over the fully
    ///    ordered input constructed from the spills.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_preparing_merge_input(
        &mut self,
        original_state: OrderedFinalAggregateState,
    ) -> OrderedFinalAggregateStateTransition {
        let OrderedFinalAggregateState::PreparingMergeInput {
            mut table,
            mut spill_context,
        } = original_state
        else {
            return Self::break_with_internal_err(
                "Ordered final aggregate stream expected PreparingMergeInput state",
            );
        };

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let replay = match spill_context.spill_table(&mut table) {
            Ok(()) => {
                let group_by_metrics = table.group_by_metrics();
                drop(table);
                match self.reservation.try_resize(0) {
                    Ok(()) => (*spill_context).into_replay_stream(
                        &self.baseline_metrics,
                        group_by_metrics,
                        self.reservation.new_empty(),
                    ),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        };
        timer.done();

        match replay {
            Ok(stream) => {
                ControlFlow::Continue(OrderedFinalAggregateState::MergingSpills {
                    stream,
                })
            }
            Err(e) => ControlFlow::Break((
                Poll::Ready(Some(Err(e))),
                OrderedFinalAggregateState::Done,
            )),
        }
    }

    /// Forwards output from the fully ordered stream that consumes the merged
    /// spill runs.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_merging_spills(
        &mut self,
        cx: &mut Context<'_>,
        original_state: OrderedFinalAggregateState,
    ) -> OrderedFinalAggregateStateTransition {
        let OrderedFinalAggregateState::MergingSpills { mut stream } = original_state
        else {
            return Self::break_with_internal_err(
                "Ordered final aggregate stream expected MergingSpills state",
            );
        };

        match stream.poll_next_unpin(cx) {
            Poll::Pending => ControlFlow::Break((
                Poll::Pending,
                OrderedFinalAggregateState::MergingSpills { stream },
            )),
            Poll::Ready(Some(Ok(batch))) => ControlFlow::Break((
                Poll::Ready(Some(Ok(batch))),
                OrderedFinalAggregateState::MergingSpills { stream },
            )),
            Poll::Ready(Some(Err(e))) => ControlFlow::Break((
                Poll::Ready(Some(Err(e))),
                OrderedFinalAggregateState::Done,
            )),
            Poll::Ready(None) => ControlFlow::Continue(OrderedFinalAggregateState::Done),
        }
    }

    /// Emits one batch after input is exhausted.
    ///
    /// `table.input_done()` has already made every remaining group safe to emit,
    /// so this state keeps draining until the table is empty.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_producing_output(
        &mut self,
        original_state: OrderedFinalAggregateState,
    ) -> OrderedFinalAggregateStateTransition {
        let OrderedFinalAggregateState::ProducingOutput { table } = original_state else {
            return Self::break_with_internal_err(
                "Ordered final aggregate stream expected ProducingOutput state",
            );
        };

        let mut table = table;
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let result = table.next_output_batch();
        timer.done();

        match result {
            Ok(Some(batch)) => {
                let next_state = if table.is_empty() {
                    drop(table);
                    if let Err(e) = self.reservation.try_resize(0) {
                        return ControlFlow::Break((
                            Poll::Ready(Some(Err(e))),
                            OrderedFinalAggregateState::Done,
                        ));
                    }
                    OrderedFinalAggregateState::Done
                } else {
                    if let Err(e) = self.reservation.try_resize(table.memory_size()) {
                        return ControlFlow::Break((
                            Poll::Ready(Some(Err(e))),
                            OrderedFinalAggregateState::ProducingOutput { table },
                        ));
                    }
                    OrderedFinalAggregateState::ProducingOutput { table }
                };

                ControlFlow::Break((
                    Poll::Ready(Some(Ok(batch.record_output(&self.baseline_metrics)))),
                    next_state,
                ))
            }
            Err(e) => ControlFlow::Break((
                Poll::Ready(Some(Err(e))),
                OrderedFinalAggregateState::ProducingOutput { table },
            )),
            Ok(None) => {
                drop(table);
                let next_state = OrderedFinalAggregateState::Done;
                if let Err(e) = self.reservation.try_resize(0) {
                    return ControlFlow::Break((Poll::Ready(Some(Err(e))), next_state));
                }
                ControlFlow::Continue(next_state)
            }
        }
    }
}

impl Stream for OrderedFinalAggregateStream {
    type Item = Result<RecordBatch>;

    /// Entry point for the ordered final aggregate state machine.
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
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let cur_state = self
                .state
                .take()
                .expect("OrderedFinalAggregateStream state should not be None");

            let next_state = match cur_state {
                state @ OrderedFinalAggregateState::ReadingInput { .. } => {
                    self.handle_reading_input(cx, state)
                }
                state @ OrderedFinalAggregateState::Spilling { .. } => {
                    self.handle_spilling(state)
                }
                state @ OrderedFinalAggregateState::PreparingMergeInput { .. } => {
                    self.handle_preparing_merge_input(state)
                }
                state @ OrderedFinalAggregateState::MergingSpills { .. } => {
                    self.handle_merging_spills(cx, state)
                }
                state @ OrderedFinalAggregateState::ProducingOutput { .. } => {
                    self.handle_producing_output(state)
                }
                state @ OrderedFinalAggregateState::Done => {
                    let _ = self.reservation.try_resize(0);
                    self.state = Some(state);
                    return Poll::Ready(None);
                }
            };

            match next_state {
                ControlFlow::Continue(next_state) => {
                    self.state = Some(next_state);
                    continue;
                }
                ControlFlow::Break((Poll::Ready(Some(Err(e))), next_state)) => {
                    // Errors are terminal: discard all operator state and release
                    // its upstream input and memory reservation before returning.
                    drop(next_state);
                    self.close_input();
                    self.reservation.free();
                    self.state = Some(OrderedFinalAggregateState::Done);
                    return Poll::Ready(Some(Err(e)));
                }
                ControlFlow::Break((poll, next_state)) => {
                    self.state = Some(next_state);
                    return poll;
                }
            }
        }
    }
}

impl RecordBatchStream for OrderedFinalAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
