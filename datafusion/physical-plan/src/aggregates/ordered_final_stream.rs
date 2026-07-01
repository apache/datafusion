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
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::stream::{Stream, StreamExt};

use super::AggregateExec;
use super::aggregate_hash_table::{OrderedAggregateTable, OrderedFinalMarker};
use crate::aggregates::AggregateMode;
use crate::metrics::{BaselineMetrics, RecordOutput, SpillMetrics};
use crate::stream::EmptyRecordBatchStream;
use crate::{InputOrderMode, RecordBatchStream, SendableRecordBatchStream};

/// Final aggregate stream for `InputOrderMode::Sorted` and
/// `InputOrderMode::PartiallySorted`.
///
/// See comments at [`super::ordered_partial_stream`] for details.
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
        table: OrderedAggregateTable<OrderedFinalMarker>,
    },
    DrainingFinal {
        table: OrderedAggregateTable<OrderedFinalMarker>,
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
        debug_assert!(matches!(
            agg.mode,
            AggregateMode::Final | AggregateMode::FinalPartitioned
        ));
        debug_assert_ne!(*input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input_schema = input.schema();
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);

        let table = OrderedAggregateTable::<OrderedFinalMarker>::new_with_input_order(
            agg,
            partition,
            &input_schema,
            Arc::clone(&schema),
            batch_size,
            input_order_mode,
        )?;
        let reservation =
            MemoryConsumer::new(format!("OrderedFinalAggregateStream[{partition}]"))
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            reservation,
            baseline_metrics,
            state: Some(OrderedFinalAggregateState::ReadingInput { table }),
        })
    }

    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
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
        let OrderedFinalAggregateState::ReadingInput { mut table } = original_state
        else {
            unreachable!("expected reading input state")
        };

        match self.input.poll_next_unpin(cx) {
            Poll::Pending => ControlFlow::Break((
                Poll::Pending,
                OrderedFinalAggregateState::ReadingInput { table },
            )),
            Poll::Ready(Some(Ok(batch))) => {
                let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                let timer = elapsed_compute.timer();
                let result = table.aggregate_batch(&batch);
                timer.done();

                if let Err(e) = result {
                    return ControlFlow::Break((
                        Poll::Ready(Some(Err(e))),
                        OrderedFinalAggregateState::ReadingInput { table },
                    ));
                }

                let timer = elapsed_compute.timer();
                let result = table.next_output_batch();
                timer.done();

                match result {
                    // Some finalized groups can be emitted. Yield them, then
                    // continue aggregating input in the current state.
                    Ok(Some(batch)) => {
                        let next_state =
                            OrderedFinalAggregateState::ReadingInput { table };
                        self.resize_reservation_for_state(&next_state);

                        ControlFlow::Break((
                            Poll::Ready(Some(Ok(
                                batch.record_output(&self.baseline_metrics)
                            ))),
                            next_state,
                        ))
                    }
                    Ok(None) => {
                        // Ordered variant doesn't support memory-limited
                        // execution, so it errors when memory reservation fails.
                        if let Err(e) = self.reservation.try_resize(table.memory_size()) {
                            return ControlFlow::Break((
                                Poll::Ready(Some(Err(e))),
                                OrderedFinalAggregateState::ReadingInput { table },
                            ));
                        }

                        // Can't do early emit, continue aggregating.
                        ControlFlow::Continue(OrderedFinalAggregateState::ReadingInput {
                            table,
                        })
                    }
                    Err(e) => ControlFlow::Break((
                        Poll::Ready(Some(Err(e))),
                        OrderedFinalAggregateState::ReadingInput { table },
                    )),
                }
            }
            Poll::Ready(Some(Err(e))) => ControlFlow::Break((
                Poll::Ready(Some(Err(e))),
                OrderedFinalAggregateState::ReadingInput { table },
            )),
            Poll::Ready(None) => {
                self.close_input();
                table.input_done();
                ControlFlow::Continue(OrderedFinalAggregateState::DrainingFinal { table })
            }
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
    fn handle_draining_final(
        &mut self,
        original_state: OrderedFinalAggregateState,
    ) -> OrderedFinalAggregateStateTransition {
        let OrderedFinalAggregateState::DrainingFinal { table } = original_state else {
            unreachable!("expected draining final state")
        };

        let mut table = table;
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let result = table.next_output_batch();
        timer.done();

        match result {
            Ok(Some(batch)) => {
                let next_state = if table.is_empty() {
                    OrderedFinalAggregateState::Done
                } else {
                    OrderedFinalAggregateState::DrainingFinal { table }
                };
                self.resize_reservation_for_state(&next_state);

                ControlFlow::Break((
                    Poll::Ready(Some(Ok(batch.record_output(&self.baseline_metrics)))),
                    next_state,
                ))
            }
            Err(e) => ControlFlow::Break((
                Poll::Ready(Some(Err(e))),
                OrderedFinalAggregateState::DrainingFinal { table },
            )),
            Ok(None) => {
                let next_state = OrderedFinalAggregateState::Done;
                self.resize_reservation_for_state(&next_state);
                ControlFlow::Continue(next_state)
            }
        }
    }

    fn resize_reservation_for_state(&mut self, state: &OrderedFinalAggregateState) {
        let new_size = match state {
            OrderedFinalAggregateState::ReadingInput { table }
            | OrderedFinalAggregateState::DrainingFinal { table } => table.memory_size(),
            OrderedFinalAggregateState::Done => 0,
        };
        let _ = self.reservation.try_resize(new_size);
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
    ///      Merge one input batch. If the ordering proves some groups are
    ///      complete, yield one final aggregate batch immediately, then continue
    ///      reading input. Otherwise continue directly with the next input batch.
    ///   -> DrainingFinal
    ///      Input was exhausted. Mark the table input as done so every remaining
    ///      group is safe to emit.
    ///
    /// DrainingFinal
    ///   -> DrainingFinal
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
                state @ OrderedFinalAggregateState::DrainingFinal { .. } => {
                    self.handle_draining_final(state)
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
