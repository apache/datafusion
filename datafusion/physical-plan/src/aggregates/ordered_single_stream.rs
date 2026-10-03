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
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::stream::{Stream, StreamExt};

use super::aggregate_hash_table::{OrderedAggregateTable, SingleMarker};
use super::spill::AggregateSpill;
use super::{AggregateExec, create_schema};
use crate::aggregates::AggregateMode;
use crate::metrics::{BaselineMetrics, RecordOutput, SpillMetrics};
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
/// input is ordered. Materialize that entire completed prefix once, then emit
/// slices of it before reading more input. See
/// [`OrderedPartialAggregateStream::into_stream`] for why this avoids
/// repeatedly removing small batches of groups from the table.
///
/// [`OrderedPartialAggregateStream::into_stream`]: super::ordered_partial_stream::OrderedPartialAggregateStream::into_stream
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
    batch_size: usize,
    state: Option<OrderedSingleAggregateState>,
}

/// See comments at `poll_next()` for details.
enum OrderedSingleAggregateState {
    ReadingInput {
        table: OrderedAggregateTable<SingleMarker>,
        /// None if either
        /// - Disk Manager doesn't enable temporary file creation
        /// - The group keys are fully ordered, it's expected to use bounded memory
        spill_context: Option<Box<AggregateSpill>>,
    },
    Spilling {
        table: OrderedAggregateTable<SingleMarker>,
        spill_context: Box<AggregateSpill>,
    },
    /// Emits one materialized batch in `batch_size` slices, then continues
    /// with `next_state` (`ReadingInput`, or `Done` after input is exhausted).
    Outputting {
        batch: RecordBatch,
        /// Reserved memory of `batch`, released when handing off the last slice.
        batch_memory: usize,
        next_state: Box<OrderedSingleAggregateState>,
    },
    PreparingMergeInput {
        table: OrderedAggregateTable<SingleMarker>,
        spill_context: Box<AggregateSpill>,
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
            agg.group_by(),
            agg.aggr_expr(),
            AggregateMode::Partial,
        )?);

        let table = OrderedAggregateTable::<SingleMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            Arc::clone(&state_schema),
        )?;

        let can_spill =
            matches!(agg.input_order_mode, InputOrderMode::PartiallySorted(_))
                && context.runtime_env().disk_manager.tmp_files_enabled();
        let spill_context = if can_spill {
            Some(Box::new(AggregateSpill::try_new(
                "OrderedSingleAggregateSpill",
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

        // Reserve memory for the initial hash table. that we hold for the lifetime of the stream.
        reservation.try_grow(table.memory_size())?;

        Ok(Self {
            schema,
            input,
            reservation,
            baseline_metrics,
            batch_size,
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
        spill_context: Option<&AggregateSpill>,
    ) -> usize {
        let table_size = table.memory_size();
        if spill_context.is_some() {
            // See `OrderedSingleAggregateStream` comments for how is it estimated
            table_size.saturating_add(table.num_groups().saturating_mul(size_of::<u32>()))
        } else {
            table_size
        }
    }

    /// Reserves `batch` on top of `table_memory` and moves to `Outputting`,
    /// which emits it in `batch_size` slices before continuing with
    /// `next_state`. If the batch cannot be reserved, hands it off whole.
    fn start_outputting(
        &mut self,
        batch: RecordBatch,
        table_memory: usize,
        next_state: OrderedSingleAggregateState,
    ) -> OrderedSingleAggregateStateTransition {
        let batch_memory = batch.get_array_memory_size();
        match self.reservation.try_resize(table_memory + batch_memory) {
            Ok(()) => ControlFlow::Continue(OrderedSingleAggregateState::Outputting {
                batch,
                batch_memory,
                next_state: Box::new(next_state),
            }),
            Err(DataFusionError::ResourcesExhausted(_)) => {
                // Only the retained table needs to remain reserved.
                if let Err(e) = self.reservation.try_resize(table_memory) {
                    return Self::break_with_err(e);
                }
                ControlFlow::Break((
                    Poll::Ready(Some(Ok(batch.record_output(&self.baseline_metrics)))),
                    next_state,
                ))
            }
            Err(e) => Self::break_with_err(e),
        }
    }

    /// Consumes one ordered raw input batch, then materializes all finalized
    /// groups if the ordering proves any group is ready.
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
                    let result = table.take_completed_result_batch();
                    timer.done();
                    result
                };

                match result {
                    // Some finalized groups can be emitted. Yield them in
                    // slices, then continue aggregating input.
                    Ok(Some(batch)) => {
                        let table_memory = Self::reservation_size_for_table(
                            &table,
                            spill_context.as_deref(),
                        );
                        self.start_outputting(
                            batch,
                            table_memory,
                            OrderedSingleAggregateState::ReadingInput {
                                table,
                                spill_context,
                            },
                        )
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
                        let elapsed_compute =
                            self.baseline_metrics.elapsed_compute().clone();
                        let timer = elapsed_compute.timer();
                        let result = table.take_completed_result_batch();
                        drop(table);
                        timer.done();
                        match result {
                            Ok(Some(batch)) => self.start_outputting(
                                batch,
                                0,
                                OrderedSingleAggregateState::Done,
                            ),
                            Ok(None) => {
                                ControlFlow::Continue(OrderedSingleAggregateState::Done)
                            }
                            Err(e) => Self::break_with_err(e),
                        }
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
        let mut result = table
            .take_state_batch()
            .and_then(|batch| spill_context.sort_and_spill(batch));

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
        let replay = match table
            .take_state_batch()
            .and_then(|batch| spill_context.sort_and_spill(batch))
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

    /// Emits the next `batch_size` slice of a materialized batch.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_outputting(
        &mut self,
        original_state: OrderedSingleAggregateState,
    ) -> OrderedSingleAggregateStateTransition {
        let OrderedSingleAggregateState::Outputting {
            batch,
            batch_memory,
            next_state,
        } = original_state
        else {
            return Self::break_with_internal_err(
                "Ordered single aggregate stream expected Outputting state",
            );
        };

        if batch.num_rows() > self.batch_size {
            let output = batch.slice(0, self.batch_size);
            let batch = batch.slice(self.batch_size, batch.num_rows() - self.batch_size);
            return ControlFlow::Break((
                Poll::Ready(Some(Ok(output.record_output(&self.baseline_metrics)))),
                OrderedSingleAggregateState::Outputting {
                    batch,
                    batch_memory,
                    next_state,
                },
            ));
        }

        // The final slice transfers ownership of the buffers to the consumer.
        if let Err(e) = self.reservation.try_shrink(batch_memory) {
            return Self::break_with_err(e);
        }
        ControlFlow::Break((
            Poll::Ready(Some(Ok(batch.record_output(&self.baseline_metrics)))),
            *next_state,
        ))
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
    ///      Aggregate one input batch. If it fits in memory and no groups are
    ///      complete, read the next batch.
    ///   -> Spilling
    ///      The table cannot reserve enough memory. Move all current states into
    ///      one fully group-key-sorted spill run.
    ///   -> Outputting
    ///      Either the input ordering proves some groups complete, or input was
    ///      exhausted without spilling and every remaining group is complete.
    ///      Materialize all of them once into one batch. If the batch cannot be
    ///      reserved, yield it whole and go to the state after `Outputting`.
    ///   -> Done
    ///      Input was exhausted without spilling and no groups remain.
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
    /// Outputting
    ///   Yield one `batch_size` slice of the materialized batch, keeping the
    ///   batch reserved until its last slice is handed off.
    ///   -> Outputting
    ///      More slices remain.
    ///   -> ReadingInput
    ///      The batch was fully emitted; resume aggregating input.
    ///   -> Done
    ///      The batch was fully emitted after input was exhausted.
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
                state @ OrderedSingleAggregateState::Outputting { .. } => {
                    self.handle_outputting(state)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExecutionPlan;
    use crate::aggregates::PhysicalGroupBy;
    use crate::stream::RecordBatchStreamAdapter;
    use crate::test::TestMemoryExec;
    use arrow::array::{AsArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryPool, PeakRecordingPool,
    };
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr_common::sort_expr::LexOrdering;
    use futures::channel::mpsc;
    use futures::{FutureExt, TryStreamExt};
    use std::collections::BTreeMap;

    const BATCH_SIZE: usize = 4;
    /// Groups completed at once by each `a` boundary, many more than `BATCH_SIZE`.
    const GROUPS_PER_KEY: i64 = 10 * BATCH_SIZE as i64 + 3;

    type InputSender = mpsc::UnboundedSender<Result<RecordBatch>>;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    /// `SELECT a, b, SUM(v) GROUP BY a, b` over input ordered by `a`, with the
    /// input fed through the returned channel.
    fn new_stream(
        pool: Arc<dyn MemoryPool>,
    ) -> Result<(InputSender, OrderedSingleAggregateStream, AggregateExec)> {
        let schema = schema();
        let input = TestMemoryExec::try_new(&[vec![]], Arc::clone(&schema), None)?
            .try_with_sort_information(vec![
                LexOrdering::new([PhysicalSortExpr::new_default(col("a", &schema)?)])
                    .unwrap(),
            ])?;
        let aggregate = AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![
                (col("a", &schema)?, "a".into()),
                (col("b", &schema)?, "b".into()),
            ]),
            vec![Arc::new(
                AggregateExprBuilder::new(sum_udaf(), vec![col("v", &schema)?])
                    .schema(Arc::clone(&schema))
                    .alias("sum")
                    .build()?,
            )],
            vec![None],
            Arc::new(TestMemoryExec::update_cache(&Arc::new(input))),
            Arc::clone(&schema),
        )?;
        assert_eq!(
            aggregate.input_order_mode(),
            &InputOrderMode::PartiallySorted(vec![0])
        );
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(
                    SessionConfig::new().with_batch_size(BATCH_SIZE).set_bool(
                        "datafusion.execution.enable_migration_aggregate",
                        true,
                    ),
                )
                .with_runtime(
                    RuntimeEnvBuilder::new()
                        .with_memory_pool(pool)
                        .build_arc()?,
                ),
        );
        let mut stream = OrderedSingleAggregateStream::new(&aggregate, &context, 0)?;
        let (sender, receiver) = mpsc::unbounded();
        stream.input = Box::pin(RecordBatchStreamAdapter::new(schema, receiver));
        Ok((sender, stream, aggregate))
    }

    /// Rows `(a, b, v = b)` for every `b` in `0..GROUPS_PER_KEY`.
    fn input_batch(a: i64) -> RecordBatch {
        let num_rows = GROUPS_PER_KEY as usize;
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![a; num_rows])),
                Arc::new(Int64Array::from_iter_values(0..GROUPS_PER_KEY)),
                Arc::new(Int64Array::from_iter_values(0..GROUPS_PER_KEY)),
            ],
        )
        .unwrap()
    }

    /// Sends two input batches for each `a` in `keys`, so every group sums to
    /// `2 * b`.
    fn send_input(sender: &InputSender, keys: std::ops::Range<i64>) {
        for a in keys {
            for _ in 0..2 {
                sender.unbounded_send(Ok(input_batch(a))).unwrap();
            }
        }
    }

    /// Checks that every group of `keys` is emitted exactly once with the
    /// expected sum.
    fn assert_output(output: &[RecordBatch], keys: std::ops::Range<i64>) {
        let mut actual = BTreeMap::new();
        for batch in output {
            let [a, b, sum] =
                [0, 1, 2].map(|i| batch.column(i).as_primitive::<Int64Type>());
            for row in 0..batch.num_rows() {
                let previous =
                    actual.insert((a.value(row), b.value(row)), sum.value(row));
                assert!(previous.is_none(), "duplicate group");
            }
        }
        let expected = keys
            .flat_map(|a| (0..GROUPS_PER_KEY).map(move |b| ((a, b), 2 * b)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual, expected);
    }

    /// One ordered key boundary completes many more groups than `batch_size`.
    /// They are materialized once and emitted in `batch_size` slices before
    /// more input is read.
    #[tokio::test]
    async fn completed_groups_are_emitted_in_batch_size_slices() -> Result<()> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(usize::MAX));
        let (sender, mut stream, _) = new_stream(Arc::clone(&pool))?;

        // No group is complete until a larger `a` arrives.
        send_input(&sender, 0..1);
        assert!(stream.next().now_or_never().is_none());

        // `a = 1` completes every `a = 0` group. All of them are emitted
        // without reading more input.
        send_input(&sender, 1..2);
        let mut output = vec![];
        while let Some(batch) = stream.next().now_or_never() {
            output.push(batch.expect("stream ended before input was exhausted")?);
        }
        assert_output(&output, 0..1);

        // End of input completes the `a = 1` groups.
        sender.close_channel();
        output.extend(stream.by_ref().try_collect::<Vec<_>>().await?);
        assert_output(&output, 0..2);
        assert!(output.iter().all(|batch| batch.num_rows() <= BATCH_SIZE));
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// If completed groups cannot be reserved while they are sliced, they are
    /// handed off as one batch instead of failing the query.
    #[tokio::test]
    async fn completed_groups_are_handed_off_whole_under_memory_pressure() -> Result<()> {
        let run = |pool: Arc<dyn MemoryPool>| async move {
            let (sender, stream, aggregate) = new_stream(Arc::clone(&pool))?;
            send_input(&sender, 0..3);
            sender.close_channel();
            let output = stream.try_collect::<Vec<_>>().await?;
            assert_output(&output, 0..3);
            assert_eq!(aggregate.metrics().unwrap().spill_count(), Some(0));
            assert_eq!(pool.reserved(), 0);
            Ok::<_, DataFusionError>(output)
        };

        // Without a limit, the peak reservation holds the table plus one
        // batch of completed groups.
        let pool = Arc::new(PeakRecordingPool::new(Arc::new(GreedyMemoryPool::new(
            usize::MAX,
        ))));
        let output = run(Arc::clone(&pool) as _).await?;
        assert!(output.iter().all(|batch| batch.num_rows() <= BATCH_SIZE));

        // One byte less fits the table but not that batch.
        let limit = pool.peak_reserved() - 1;
        let output = run(Arc::new(GreedyMemoryPool::new(limit))).await?;
        assert!(output.iter().any(|batch| batch.num_rows() > BATCH_SIZE));
        Ok(())
    }
}
