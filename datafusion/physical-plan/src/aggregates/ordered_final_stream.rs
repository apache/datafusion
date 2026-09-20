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
use futures::stream::{Stream, StreamExt};

use super::AggregateExec;
use super::aggregate_hash_table::{
    FinalMarker, OrderedAggregateTable, OrderedAggregateTableMetrics,
};
use super::spill::AggregateSpill;
use crate::aggregates::AggregateMode;
use crate::metrics::{BaselineMetrics, RecordOutput, SpillMetrics};
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

/// See comments at `poll_next()` for details.
enum OrderedFinalAggregateState {
    ReadingInput {
        table: OrderedAggregateTable<FinalMarker>,
        /// None if either
        /// - Disk Manager doesn't enable temporary file creation
        /// - The group keys are fully ordered, it's expected to use bounded memory
        spill_context: Option<Box<AggregateSpill>>,
    },
    Spilling {
        table: OrderedAggregateTable<FinalMarker>,
        spill_context: Box<AggregateSpill>,
    },
    ProducingOutput {
        table: OrderedAggregateTable<FinalMarker>,
    },
    PreparingMergeInput {
        table: OrderedAggregateTable<FinalMarker>,
        spill_context: Box<AggregateSpill>,
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
        let metrics = OrderedAggregateTableMetrics::new(agg, partition);
        let spill_metrics = SpillMetrics::new(&agg.metrics, partition);
        let reservation =
            MemoryConsumer::new(format!("OrderedFinalAggregateStream[{partition}]"))
                // HACK: Technically, fully ordered aggregate is a non-spillable
                // consumer, since it uses bounded memory. There is a known race
                // condition bug, and we set it to spillable to let it have larger
                // memory budget to suppress the bug.
                // Bug issue: https://github.com/apache/datafusion/issues/17334
                .with_can_spill(true)
                .register(context.memory_pool());
        Self::new_with_input_and_metrics(
            agg,
            context,
            partition,
            input,
            input_order_mode,
            baseline_metrics,
            metrics,
            Some(spill_metrics),
            reservation,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "keeps replay metric reuse explicit"
    )]
    /// Builds the stream with the reservation of its logical aggregate operator.
    /// Replay callers pass a sibling of the reservation used by the merge input,
    /// keeping both components under one memory-consumer registration.
    pub(in crate::aggregates) fn new_with_input_and_metrics(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        input: SendableRecordBatchStream,
        input_order_mode: &InputOrderMode,
        baseline_metrics: BaselineMetrics,
        metrics: OrderedAggregateTableMetrics,
        spill_metrics: Option<SpillMetrics>,
        reservation: MemoryReservation,
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
            Some(Box::new(AggregateSpill::try_new(
                "OrderedFinalAggregateSpill",
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
            metrics,
        )?;
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
        spill_context: Option<&AggregateSpill>,
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
                            // `None` means spilling is not supported, see comments
                            // at `OrderedFinalAggregateState` for details.
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
        let mut result = table
            .take_state_batch()
            .and_then(|batch| spill_context.spill(batch));

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
        let replay = match table
            .take_state_batch()
            .and_then(|batch| spill_context.spill(batch))
        {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExecutionPlan;
    use crate::aggregates::PhysicalGroupBy;
    use crate::common::collect;
    use crate::stream::RecordBatchStreamAdapter;
    use crate::test::TestMemoryExec;
    use arrow::array::{Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_functions_aggregate::{min_max::min_udaf, sum::sum_udaf};
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr_common::sort_expr::LexOrdering;
    use futures::FutureExt;
    use futures::channel::mpsc;
    use std::collections::BTreeMap;

    #[derive(Clone, Copy)]
    enum Finish {
        Collect,
        DropDuringMerge,
        DropDuringReplay,
        InputError,
    }

    #[tokio::test]
    async fn spill_replay_with_another_ordered_partition() -> Result<()> {
        for input_batches in [28, 36, 55, 63] {
            run_shared_pool_case(input_batches, 600 * 1024, Finish::Collect).await?;
        }
        // The same input also produces the reference results without spilling.
        run_shared_pool_case(63, 10 * 1024 * 1024, Finish::Collect).await
    }

    #[tokio::test]
    async fn spill_replay_releases_memory_on_drop() -> Result<()> {
        run_shared_pool_case(36, 600 * 1024, Finish::DropDuringMerge).await?;
        run_shared_pool_case(36, 600 * 1024, Finish::DropDuringReplay).await
    }

    #[tokio::test]
    async fn ordered_spill_releases_memory_on_input_error() -> Result<()> {
        run_shared_pool_case(36, 600 * 1024, Finish::InputError).await
    }

    async fn run_shared_pool_case(
        input_batches: i64,
        limit: usize,
        finish: Finish,
    ) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("s", DataType::Utf8View, false),
        ]));
        let groups = PhysicalGroupBy::new_single(vec![
            (col("a", &schema)?, "a".into()),
            (col("b", &schema)?, "b".into()),
        ]);
        let expressions = vec![
            Arc::new(
                AggregateExprBuilder::new(sum_udaf(), vec![col("v", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("sum")
                    .build()?,
            ),
            Arc::new(
                AggregateExprBuilder::new(min_udaf(), vec![col("s", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("min")
                    .build()?,
            ),
        ];
        let empty = TestMemoryExec::try_new_exec(&[vec![]], Arc::clone(&schema), None)?;
        let partial = AggregateExec::try_new(
            AggregateMode::Partial,
            groups.clone(),
            expressions.clone(),
            vec![None; 2],
            empty,
            Arc::clone(&schema),
        )?;
        let partial_schema = partial.schema();
        let ordering =
            LexOrdering::new([PhysicalSortExpr::new_default(col("a", &partial_schema)?)])
                .unwrap();
        let input = TestMemoryExec::try_new(
            &[vec![], vec![]],
            Arc::clone(&partial_schema),
            None,
        )?
        .try_with_sort_information(vec![ordering])?;
        let aggregate = AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            groups.as_final(),
            expressions,
            vec![None; 2],
            Arc::new(TestMemoryExec::update_cache(&Arc::new(input))),
            schema,
        )?;
        assert_eq!(
            aggregate.input_order_mode(),
            &InputOrderMode::PartiallySorted(vec![0])
        );

        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(
                    SessionConfig::new().with_batch_size(128).set_bool(
                        "datafusion.execution.enable_migration_aggregate",
                        true,
                    ),
                )
                .with_runtime(
                    RuntimeEnvBuilder::new()
                        .with_max_spill_merge_fan_in(2)
                        .with_memory_pool(Arc::clone(&pool))
                        .build_arc()?,
                ),
        );
        let mut streams = vec![];
        let mut senders = vec![];
        for partition in 0..2 {
            let (sender, receiver) = mpsc::unbounded();
            let input = Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&partial_schema),
                receiver,
            ));
            let stream = OrderedFinalAggregateStream::new_with_input(
                &aggregate,
                &context,
                partition,
                input,
                aggregate.input_order_mode(),
            )?;
            senders.push(sender);
            streams.push(stream);
        }
        let mut expected = BTreeMap::new();
        let mut make_batch = |partition: i64, start: i64| {
            for value in start..start + 128 {
                let entry = expected
                    .entry((
                        partition,
                        if partition == 0 && value % 128 == 0 {
                            0
                        } else {
                            value
                        },
                    ))
                    .or_insert_with(|| (0, (value % 2).to_string()));
                entry.0 += value * 2;
            }
            RecordBatch::try_new(
                Arc::clone(&partial_schema),
                vec![
                    Arc::new(Int64Array::from(vec![partition; 128])),
                    Arc::new(Int64Array::from_iter_values((start..start + 128).map(
                        |value| {
                            if partition == 0 && value % 128 == 0 {
                                0
                            } else {
                                value
                            }
                        },
                    ))),
                    Arc::new(Int64Array::from_iter_values(
                        (start..start + 128).map(|v| v * 2),
                    )),
                    Arc::new(StringViewArray::from_iter_values(
                        (start..start + 128).map(|v| if v % 2 == 0 { "0" } else { "1" }),
                    )),
                ],
            )
            .unwrap()
        };
        // Keep partition 1's incomplete ordered run live while partition 0 spills
        // and replays. Channel inputs return Pending after each supplied batch,
        // making this interleaving independent of task scheduling.
        for batch in 0..55 {
            senders[1]
                .unbounded_send(Ok(make_batch(1, batch * 128)))
                .unwrap();
            assert!(streams[1].next().now_or_never().is_none());
        }
        let held = streams[1].reservation.size();
        assert!(held > 500 * 1024);
        for batch in 0..input_batches {
            // Repeated keys cross spill runs, so replay must merge their sums.
            senders[0]
                .unbounded_send(Ok(make_batch(0, batch * 128)))
                .unwrap();
            assert!(streams[0].next().now_or_never().is_none());
        }
        if limit == 600 * 1024 {
            assert!(aggregate.metrics().unwrap().spill_count().unwrap() > 0);
        }
        let mut first = streams.remove(0);
        match finish {
            Finish::Collect => {
                senders[0].close_channel();
                let mut output = collect(Box::pin(first)).await?;
                assert_eq!(pool.reserved(), held);
                senders[1].close_channel();
                output.extend(collect(Box::pin(streams.remove(0))).await?);
                let mut actual = BTreeMap::new();
                for batch in output {
                    let a = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let b = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let sum = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let min = batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .unwrap();
                    for row in 0..batch.num_rows() {
                        assert!(
                            actual
                                .insert(
                                    (a.value(row), b.value(row)),
                                    (sum.value(row), min.value(row).to_string())
                                )
                                .is_none()
                        );
                    }
                }
                assert_eq!(actual, expected);
                assert_eq!(
                    aggregate.metrics().unwrap().spill_count().unwrap() > 0,
                    limit == 600 * 1024
                );
            }
            Finish::DropDuringMerge => {
                senders[0].close_channel();
                let _ = first.next().now_or_never();
                assert!(matches!(
                    first.state.as_ref(),
                    Some(OrderedFinalAggregateState::MergingSpills { .. })
                ));
                drop(first);
            }
            Finish::DropDuringReplay => {
                senders[0].close_channel();
                first.next().await.unwrap()?;
                assert!(pool.reserved() > held);
                drop(first);
                assert_eq!(pool.reserved(), held);
            }
            Finish::InputError => {
                senders[0]
                    .unbounded_send(datafusion_common::exec_err!(
                        "injected input failure"
                    ))
                    .unwrap();
                let error = first.next().await.unwrap().unwrap_err();
                assert!(error.to_string().contains("injected input failure"));
                assert_eq!(pool.reserved(), held);
                drop(first);
            }
        }
        drop(streams);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }
}
