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

//! Single-stage aggregate stream for ordered raw input.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::stream::{Stream, StreamExt};

use super::aggregate_hash_table::{
    OrderedAggregateTable, OrderedAggregateTableMetrics, SingleMarker,
};
use super::ordered_final_stream::OrderedFinalAggregateStream;
use super::{AggregateExec, create_schema};
use crate::aggregates::AggregateMode;
use crate::metrics::{BaselineMetrics, RecordOutput, SpillMetrics};
use crate::sorts::IncrementalSortIterator;
use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use crate::spill::spill_manager::SpillManager;
use crate::stream::EmptyRecordBatchStream;
use crate::{InputOrderMode, RecordBatchStream, SendableRecordBatchStream};

/// Single aggregate stream for `InputOrderMode::Sorted` and
/// `InputOrderMode::PartiallySorted`.
///
/// # Example
///
/// SELECT k, AVG(v) FROM t GROUP BY k;
///
/// If the input is ordered by `k`, and there are existing key partitioning on group
/// by keys, the single mode aggregation with ordering optimization can be used:
///
/// ## Plan
/// AggregateExec(stage=single, ordered)
/// -- DataSourceExec(t)
///
/// ## Single Stage Behavior
/// Input: raw rows
/// Output: final results for all groups (for example, `AVG(x)`)
///
/// # Order-based Optimization
///
/// For the aggregation work, the hash aggregation implementation is reused.
///
/// After each input batch, check whether any groups can be emitted eagerly to
/// improve memory efficiency. For example, if the last group key seen is
/// `k = 100`, it is safe to emit all groups with keys less than 100 because the
/// input is ordered.
///
/// # Memory Pressure and Spilling
///
/// ## Fully ordered case
///
/// If the input is ordered by every group key, for example:
///
/// - Input order: `a, b`
/// - `GROUP BY`: `a, b`
///
/// Completed groups can be emitted as soon as the next group is observed. Thus,
/// only the current group remains active after completed groups are emitted, and
/// memory usage does not grow with the total number of groups.
///
/// If a memory reservation nevertheless fails, the stream returns the error
/// directly, indicating an unexpected behavior.
///
/// ## Partially ordered case
///
/// If the input is ordered by only a subset of the group keys, for example:
///
/// - Input order: `a`
/// - `GROUP BY`: `a, b`
///
/// If one `a` value contains many distinct `b` values, the table may accumulate
/// enough groups to exceed the memory limit.
///
/// On reservation failure, the stream sorts the current intermediate states by
/// the complete group key and spills them as one run. After the input ends, it
/// spills any remaining states, performs a sort-preserving merge of all runs,
/// and feeds the merged input into a fully ordered final aggregate stream.
pub(crate) struct OrderedSingleAggregateStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    reservation: MemoryReservation,
    baseline_metrics: BaselineMetrics,
    state: Option<OrderedSingleAggregateState>,
}

/// Spill configuration and accumulated runs for partially ordered single
/// aggregation.
///
/// Each spill event drains all currently buffered groups, sorts their intermediate
/// states by the full group key, and writes them to one spill file. All files are
/// merged and replayed after the original input ends.
struct OrderedSingleSpillContext {
    /// Aggregate configuration used to construct the final replay stream.
    final_agg: AggregateExec,
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
enum OrderedSingleAggregateState {
    ReadingInput {
        table: OrderedAggregateTable<SingleMarker>,
        /// None if either
        /// - Disk Manager doesn't enable temporary file creation
        /// - The group keys are fully ordered, it's expected to use bounded memory
        spill_context: Option<Box<OrderedSingleSpillContext>>,
    },
    Spilling {
        table: OrderedAggregateTable<SingleMarker>,
        spill_context: Box<OrderedSingleSpillContext>,
    },
    ProducingOutput {
        table: OrderedAggregateTable<SingleMarker>,
    },
    PreparingMergeInput {
        table: OrderedAggregateTable<SingleMarker>,
        spill_context: Box<OrderedSingleSpillContext>,
    },
    MergingSpills {
        stream: SendableRecordBatchStream,
    },
    Done,
    /// Sentinel state to use when returning error from any other states, because:
    /// - It explicitly releases state-owned resources immediately
    /// - More defensive against accidentally resuming execution after error
    Error,
}

type OrderedSingleAggregatePoll = Poll<Option<Result<RecordBatch>>>;
type OrderedSingleAggregateStateTransition = ControlFlow<
    (OrderedSingleAggregatePoll, OrderedSingleAggregateState),
    OrderedSingleAggregateState,
>;

impl OrderedSingleSpillContext {
    fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        batch_size: usize,
        input_order_mode: &InputOrderMode,
        spill_schema: &SchemaRef,
        spill_metrics: SpillMetrics,
    ) -> Result<Self> {
        let group_schema = agg.group_by.group_schema(&agg.input().schema())?;
        let output_ordering = agg.cache.output_ordering();
        let InputOrderMode::PartiallySorted(order_indices) = input_order_mode else {
            return internal_err!(
                "Ordered single spill requires partially ordered input"
            );
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
            return internal_err!("Ordered single spill expression is empty");
        };

        let spill_manager = SpillManager::new(
            context.runtime_env(),
            spill_metrics,
            Arc::clone(spill_schema),
        )
        .with_compression_type(context.session_config().spill_compression());

        // Spilled rows contain group keys and intermediate states. Replay must
        // merge those states and evaluate the final aggregate values.
        let mut final_agg = agg.clone();
        final_agg.mode = match agg.mode {
            AggregateMode::Single => AggregateMode::Final,
            AggregateMode::SinglePartitioned => AggregateMode::FinalPartitioned,
            mode => {
                return internal_err!(
                    "Ordered single aggregate spill cannot replay aggregate mode {mode:?}"
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
    /// See [`OrderedSingleAggregateStream`] for spilling details.
    fn spill_table(
        &mut self,
        table: &mut OrderedAggregateTable<SingleMarker>,
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
                "OrderedSingleAggregateSpill",
            )?;

        let Some((file, max_record_batch_memory)) = spill_file else {
            return internal_err!("Ordered single aggregation produced an empty spill");
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
        metrics: OrderedAggregateTableMetrics,
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
            baseline_metrics.clone(),
            metrics,
            None,
            reservation,
        )?;
        Ok(Box::pin(replay))
    }
}

impl OrderedSingleAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            AggregateMode::Single | AggregateMode::SinglePartitioned
        ));
        debug_assert_ne!(agg.input_order_mode, InputOrderMode::Linear);

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

        let table = OrderedAggregateTable::<SingleMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            Arc::clone(&state_schema),
            batch_size,
        )?;

        let can_spill =
            matches!(agg.input_order_mode, InputOrderMode::PartiallySorted(_))
                && context.runtime_env().disk_manager.tmp_files_enabled();
        let spill_context = if can_spill {
            Some(Box::new(OrderedSingleSpillContext::new(
                agg,
                context,
                partition,
                batch_size,
                &agg.input_order_mode,
                &state_schema,
                spill_metrics,
            )?))
        } else {
            None
        };

        let reservation =
            MemoryConsumer::new(format!("OrderedSingleAggregateStream[{partition}]"))
                .with_can_spill(can_spill)
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            reservation,
            baseline_metrics,
            state: Some(OrderedSingleAggregateState::ReadingInput {
                table,
                spill_context,
            }),
        })
    }

    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
    }

    fn break_with_err(error: DataFusionError) -> OrderedSingleAggregateStateTransition {
        ControlFlow::Break((
            Poll::Ready(Some(Err(error))),
            OrderedSingleAggregateState::Error,
        ))
    }

    fn break_with_internal_err(message: &str) -> OrderedSingleAggregateStateTransition {
        Self::break_with_err(internal_datafusion_err!("{message}"))
    }

    /// Reserve memory for the current aggregate table.
    fn reservation_size_for_table(
        table: &OrderedAggregateTable<SingleMarker>,
        spill_context: Option<&OrderedSingleSpillContext>,
    ) -> usize {
        let table_size = table.memory_size();
        if spill_context.is_some() {
            // See `OrderedSingleAggregateStream` comments for how is it estimated
            table_size.saturating_add(table.num_groups().saturating_mul(size_of::<u32>()))
        } else {
            table_size
        }
    }

    /// Consumes one ordered raw input batch, then immediately emits
    /// finalized groups if the ordering proves any group is ready.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_reading_input(
        &mut self,
        cx: &mut Context<'_>,
        original_state: OrderedSingleAggregateState,
    ) -> OrderedSingleAggregateStateTransition {
        let OrderedSingleAggregateState::ReadingInput {
            mut table,
            spill_context,
        } = original_state
        else {
            return Self::break_with_internal_err(
                "Ordered single aggregate stream expected ReadingInput state",
            );
        };

        match self.input.poll_next_unpin(cx) {
            Poll::Pending => ControlFlow::Break((
                Poll::Pending,
                OrderedSingleAggregateState::ReadingInput {
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
                    return Self::break_with_err(e);
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
                            // `None` means spilling is not supported, see comments
                            // at `OrderedSingleAggregateState` for details.
                            return Self::break_with_err(e);
                        };
                        if table.is_empty() {
                            return Self::break_with_internal_err(
                                "Ordered single aggregate ran out of memory with no aggregated groups",
                            );
                        }
                        return ControlFlow::Continue(
                            OrderedSingleAggregateState::Spilling {
                                table,
                                spill_context,
                            },
                        );
                    }
                    Err(e) => {
                        return Self::break_with_err(e);
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
                            return Self::break_with_err(e);
                        }
                        let next_state = OrderedSingleAggregateState::ReadingInput {
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
                        ControlFlow::Continue(OrderedSingleAggregateState::ReadingInput {
                            table,
                            spill_context,
                        })
                    }
                    Err(e) => Self::break_with_err(e),
                }
            }
            Poll::Ready(Some(Err(e))) => Self::break_with_err(e),
            Poll::Ready(None) => {
                self.close_input();
                match spill_context {
                    Some(spill_context) if spill_context.has_spills() => {
                        ControlFlow::Continue(
                            OrderedSingleAggregateState::PreparingMergeInput {
                                table,
                                spill_context,
                            },
                        )
                    }
                    _ => {
                        table.input_done();
                        ControlFlow::Continue(
                            OrderedSingleAggregateState::ProducingOutput { table },
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
        original_state: OrderedSingleAggregateState,
    ) -> OrderedSingleAggregateStateTransition {
        let OrderedSingleAggregateState::Spilling {
            mut table,
            mut spill_context,
        } = original_state
        else {
            return Self::break_with_internal_err(
                "Ordered single aggregate stream expected Spilling state",
            );
        };

        // Sanity check: it's impossible to OOM when the table is empty
        if table.is_empty() {
            return Self::break_with_internal_err(
                "Ordered single aggregation entered Spilling with an empty table",
            );
        }

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let mut result = spill_context.spill_table(&mut table);

        // Spilling shrinks the aggregate table and releases its accumulated
        // memory. Update the reservation accordingly.
        if let Err(e) = self.reservation.try_resize(table.memory_size()) {
            result = Err(e);
        }

        timer.done();

        match result {
            // Finished spilling the aggregate table, continue aggregating from input
            Ok(()) => ControlFlow::Continue(OrderedSingleAggregateState::ReadingInput {
                table,
                spill_context: Some(spill_context),
            }),
            Err(e) => Self::break_with_err(e),
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
        original_state: OrderedSingleAggregateState,
    ) -> OrderedSingleAggregateStateTransition {
        let OrderedSingleAggregateState::PreparingMergeInput {
            mut table,
            mut spill_context,
        } = original_state
        else {
            return Self::break_with_internal_err(
                "Ordered single aggregate stream expected PreparingMergeInput state",
            );
        };

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let replay = match spill_context.spill_table(&mut table) {
            Ok(()) => {
                let metrics = table.metrics();
                drop(table);
                match self.reservation.try_resize(0) {
                    Ok(()) => (*spill_context).into_replay_stream(
                        &self.baseline_metrics,
                        metrics,
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
                ControlFlow::Continue(OrderedSingleAggregateState::MergingSpills {
                    stream,
                })
            }
            Err(e) => Self::break_with_err(e),
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
        original_state: OrderedSingleAggregateState,
    ) -> OrderedSingleAggregateStateTransition {
        let OrderedSingleAggregateState::MergingSpills { mut stream } = original_state
        else {
            return Self::break_with_internal_err(
                "Ordered single aggregate stream expected MergingSpills state",
            );
        };

        match stream.poll_next_unpin(cx) {
            Poll::Pending => ControlFlow::Break((
                Poll::Pending,
                OrderedSingleAggregateState::MergingSpills { stream },
            )),
            Poll::Ready(Some(Ok(batch))) => ControlFlow::Break((
                Poll::Ready(Some(Ok(batch))),
                OrderedSingleAggregateState::MergingSpills { stream },
            )),
            Poll::Ready(Some(Err(e))) => Self::break_with_err(e),
            Poll::Ready(None) => ControlFlow::Continue(OrderedSingleAggregateState::Done),
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
        original_state: OrderedSingleAggregateState,
    ) -> OrderedSingleAggregateStateTransition {
        let OrderedSingleAggregateState::ProducingOutput { table } = original_state
        else {
            return Self::break_with_internal_err(
                "Ordered single aggregate stream expected ProducingOutput state",
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
                        return Self::break_with_err(e);
                    }
                    OrderedSingleAggregateState::Done
                } else {
                    if let Err(e) = self.reservation.try_resize(table.memory_size()) {
                        return Self::break_with_err(e);
                    }
                    OrderedSingleAggregateState::ProducingOutput { table }
                };

                ControlFlow::Break((
                    Poll::Ready(Some(Ok(batch.record_output(&self.baseline_metrics)))),
                    next_state,
                ))
            }
            Err(e) => Self::break_with_err(e),
            Ok(None) => {
                drop(table);
                let next_state = OrderedSingleAggregateState::Done;
                if let Err(e) = self.reservation.try_resize(0) {
                    return Self::break_with_err(e);
                }
                ControlFlow::Continue(next_state)
            }
        }
    }
}

impl Stream for OrderedSingleAggregateStream {
    type Item = Result<RecordBatch>;

    /// Entry point for the ordered single aggregate state machine.
    ///
    /// See comments in [`OrderedSingleAggregateStream`] for high-level ideas.
    ///
    /// State transition graph:
    ///
    /// ```text
    /// (start)
    ///   -> ReadingInput
    ///      The stream starts by polling ordered raw input and updating the
    ///      ordered single aggregate table.
    ///
    /// ReadingInput
    ///   -> ReadingInput
    ///      Aggregate one input batch. If it fits in memory, optionally yield
    ///      groups proven complete by the input ordering, then read the next batch.
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
    /// Any active state
    ///   -> Error
    ///      An error drops state-owned resources before it is returned.
    ///
    /// Error
    ///   -> (end)
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
                .expect("OrderedSingleAggregateStream state should not be None");

            let next_state = match cur_state {
                state @ OrderedSingleAggregateState::ReadingInput { .. } => {
                    self.handle_reading_input(cx, state)
                }
                state @ OrderedSingleAggregateState::Spilling { .. } => {
                    self.handle_spilling(state)
                }
                state @ OrderedSingleAggregateState::PreparingMergeInput { .. } => {
                    self.handle_preparing_merge_input(state)
                }
                state @ OrderedSingleAggregateState::MergingSpills { .. } => {
                    self.handle_merging_spills(cx, state)
                }
                state @ OrderedSingleAggregateState::ProducingOutput { .. } => {
                    self.handle_producing_output(state)
                }
                state @ OrderedSingleAggregateState::Error => {
                    self.close_input();
                    self.reservation.free();
                    self.state = Some(state);
                    return Poll::Ready(None);
                }
                state @ OrderedSingleAggregateState::Done => {
                    let _ = self.reservation.try_resize(0);
                    self.state = Some(state);
                    return Poll::Ready(None);
                }
            };

            match next_state {
                ControlFlow::Continue(next_state) => {
                    self.state = Some(next_state);
                }
                ControlFlow::Break((Poll::Ready(Some(Err(e))), next_state)) => {
                    debug_assert!(matches!(
                        next_state,
                        OrderedSingleAggregateState::Error
                    ));

                    // The handler has already discarded its state-owned resources.
                    // Release the remaining stream-owned resources before returning.
                    self.close_input();
                    self.reservation.free();
                    self.state = Some(OrderedSingleAggregateState::Error);
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

impl RecordBatchStream for OrderedSingleAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
