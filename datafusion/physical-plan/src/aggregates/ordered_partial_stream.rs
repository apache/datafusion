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

//! Partial aggregate stream for ordered group input.

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
use super::aggregate_hash_table::{OrderedAggregateTable, OrderedPartialMarker};
use crate::aggregates::AggregateMode;
use crate::metrics::{BaselineMetrics, MetricBuilder, RecordOutput, SpillMetrics};
use crate::stream::EmptyRecordBatchStream;
use crate::{InputOrderMode, RecordBatchStream, SendableRecordBatchStream, metrics};

/// Partial aggregate stream for `InputOrderMode::Sorted` and
/// `InputOrderMode::PartiallySorted`.
///
/// # Example
///
/// SELECT k, AVG(v) FROM t GROUP BY k;
///
/// If the input is ordered by `k`, the aggregate can use ordered partial and
/// final stages:
///
/// ## Plan
/// AggregateExec(stage=final, ordered)
/// -- RepartitionExec(hash(k), preserves_order=true)
/// ---- AggregateExec(stage=partial, ordered)
///
/// ## Partial Stage Behavior
/// Input: raw rows
/// Output: partial states for all groups (for example, `AVG(x)` emits `SUM(x)`
/// and `COUNT(x)`)
///
/// ## Final Stage Behavior
/// Input: partial states
/// Output: results for all groups (for example, `AVG(x)` calculated from the
/// state)
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
/// ## Implementation Note
///
/// This is intentionally kept simple and closely maps to
/// `GroupedHashAggregateStream` to finish the refactor sooner.
///
/// See issue for details: <https://github.com/apache/datafusion/issues/22710>
///
/// More applicable optimizations are left to future work.
pub(crate) struct OrderedPartialAggregateStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    reservation: MemoryReservation,
    baseline_metrics: BaselineMetrics,
    reduction_factor: metrics::RatioMetrics,
    state: Option<OrderedPartialAggregateState>,
}

/// See comments at `poll_next()` for details.
enum OrderedPartialAggregateState {
    ReadingInput {
        table: OrderedAggregateTable<OrderedPartialMarker>,
    },
    DrainingFinal {
        table: OrderedAggregateTable<OrderedPartialMarker>,
    },
    Done,
}

type OrderedPartialAggregatePoll = Poll<Option<Result<RecordBatch>>>;
type OrderedPartialAggregateStateTransition = ControlFlow<
    (OrderedPartialAggregatePoll, OrderedPartialAggregateState),
    OrderedPartialAggregateState,
>;

impl OrderedPartialAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert_eq!(agg.mode, AggregateMode::Partial);
        debug_assert_ne!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);
        let reduction_factor = MetricBuilder::new(&agg.metrics)
            .with_type(metrics::MetricType::Summary)
            .ratio_metrics("reduction_factor", partition);

        let table = OrderedAggregateTable::<OrderedPartialMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;
        let reservation =
            MemoryConsumer::new(format!("OrderedPartialAggregateStream[{partition}]"))
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            reservation,
            baseline_metrics,
            reduction_factor,
            state: Some(OrderedPartialAggregateState::ReadingInput { table }),
        })
    }

    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
    }

    /// Consumes one ordered input batch, then immediately emits completed groups
    /// if the ordering proves any group is ready.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_reading_input(
        &mut self,
        cx: &mut Context<'_>,
        original_state: OrderedPartialAggregateState,
    ) -> OrderedPartialAggregateStateTransition {
        let OrderedPartialAggregateState::ReadingInput { mut table } = original_state
        else {
            unreachable!("expected reading input state")
        };

        match self.input.poll_next_unpin(cx) {
            Poll::Pending => ControlFlow::Break((
                Poll::Pending,
                OrderedPartialAggregateState::ReadingInput { table },
            )),
            Poll::Ready(Some(Ok(batch))) => {
                let input_rows = batch.num_rows();
                self.reduction_factor.add_total(input_rows);

                let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                let timer = elapsed_compute.timer();
                let result = table.aggregate_batch(&batch);
                timer.done();

                if let Err(e) = result {
                    return ControlFlow::Break((
                        Poll::Ready(Some(Err(e))),
                        OrderedPartialAggregateState::ReadingInput { table },
                    ));
                }

                let timer = elapsed_compute.timer();
                let result = table.next_output_batch();
                timer.done();

                match result {
                    // There is some previous group results can be emitted: emit
                    // them, and next continuing aggreagting input (loop in the
                    // current state)
                    Ok(Some(batch)) => {
                        self.reduction_factor.add_part(batch.num_rows());
                        let next_state =
                            OrderedPartialAggregateState::ReadingInput { table };
                        self.resize_reservation_for_state(&next_state);

                        ControlFlow::Break((
                            Poll::Ready(Some(Ok(
                                batch.record_output(&self.baseline_metrics)
                            ))),
                            next_state,
                        ))
                    }
                    Ok(None) => {
                        // Ordered variant don't support memory-limited execution,
                        // it have to error when OOM
                        if let Err(e) = self.reservation.try_resize(table.memory_size()) {
                            return ControlFlow::Break((
                                Poll::Ready(Some(Err(e))),
                                OrderedPartialAggregateState::ReadingInput { table },
                            ));
                        }

                        // Can't do early emit, continue aggregating.
                        ControlFlow::Continue(
                            OrderedPartialAggregateState::ReadingInput { table },
                        )
                    }
                    Err(e) => ControlFlow::Break((
                        Poll::Ready(Some(Err(e))),
                        OrderedPartialAggregateState::ReadingInput { table },
                    )),
                }
            }
            Poll::Ready(Some(Err(e))) => ControlFlow::Break((
                Poll::Ready(Some(Err(e))),
                OrderedPartialAggregateState::ReadingInput { table },
            )),
            // Input has exhausted, move to the final draining stage.
            Poll::Ready(None) => {
                self.close_input();
                table.input_done();
                ControlFlow::Continue(OrderedPartialAggregateState::DrainingFinal {
                    table,
                })
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
        original_state: OrderedPartialAggregateState,
    ) -> OrderedPartialAggregateStateTransition {
        let OrderedPartialAggregateState::DrainingFinal { table } = original_state else {
            unreachable!("expected draining final state")
        };

        let mut table = table;
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let result = table.next_output_batch();
        timer.done();

        match result {
            Ok(Some(batch)) => {
                self.reduction_factor.add_part(batch.num_rows());
                let next_state = if table.is_empty() {
                    OrderedPartialAggregateState::Done
                } else {
                    OrderedPartialAggregateState::DrainingFinal { table }
                };
                self.resize_reservation_for_state(&next_state);

                ControlFlow::Break((
                    Poll::Ready(Some(Ok(batch.record_output(&self.baseline_metrics)))),
                    next_state,
                ))
            }
            Err(e) => ControlFlow::Break((
                Poll::Ready(Some(Err(e))),
                OrderedPartialAggregateState::DrainingFinal { table },
            )),
            Ok(None) => {
                let next_state = OrderedPartialAggregateState::Done;
                self.resize_reservation_for_state(&next_state);
                ControlFlow::Continue(next_state)
            }
        }
    }

    fn resize_reservation_for_state(&mut self, state: &OrderedPartialAggregateState) {
        let new_size = match state {
            OrderedPartialAggregateState::ReadingInput { table }
            | OrderedPartialAggregateState::DrainingFinal { table } => {
                table.memory_size()
            }
            OrderedPartialAggregateState::Done => 0,
        };
        let _ = self.reservation.try_resize(new_size);
    }
}

impl Stream for OrderedPartialAggregateStream {
    type Item = Result<RecordBatch>;

    /// Entry point for the ordered partial aggregate state machine.
    ///
    /// See comments in [`OrderedPartialAggregateStream`] for high-level ideas.
    ///
    /// State transition graph:
    ///
    /// ```text
    /// (start)
    ///   -> ReadingInput
    ///      The stream starts by polling ordered input and aggregating batches
    ///      into the ordered partial aggregate table.
    ///
    /// ReadingInput
    ///   -> ReadingInput
    ///      Aggregate one input batch. If the ordering proves some groups are
    ///      complete, yield one partial-state batch immediately, then continue
    ///      reading input. Otherwise continue directly with the next input batch.
    ///   -> DrainingFinal
    ///      Input was exhausted. Mark the table input as done so every remaining
    ///      group is safe to emit.
    ///
    /// DrainingFinal
    ///   -> DrainingFinal
    ///      One remaining partial-state batch was yielded; repeat to continue
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
                .expect("OrderedPartialAggregateStream state should not be None");

            let next_state = match cur_state {
                state @ OrderedPartialAggregateState::ReadingInput { .. } => {
                    self.handle_reading_input(cx, state)
                }
                state @ OrderedPartialAggregateState::DrainingFinal { .. } => {
                    self.handle_draining_final(state)
                }
                state @ OrderedPartialAggregateState::Done => {
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

impl RecordBatchStream for OrderedPartialAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
