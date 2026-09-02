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

use std::ops::ControlFlow;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::stream::{Stream, StreamExt};

use super::AggregateExec;
use super::aggregate_hash_table::{AggregateHashTable, PartialReduceMarker};
use crate::metrics::{BaselineMetrics, RecordOutput, SpillMetrics};
use crate::stream::EmptyRecordBatchStream;
use crate::{InputOrderMode, RecordBatchStream, SendableRecordBatchStream};

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
///
/// # Memory Management
///
/// If the memory reservation cannot grow after aggregating an input batch, all
/// accumulated partial states are emitted immediately, and the remaining input
/// is aggregated with an empty table. This repeats until the input ends.
///
/// See [`crate::aggregates::AggregateMode::PartialReduce`] for why it's allowed
/// to emit the same group multiple times.
pub(crate) struct PartialReduceHashAggregateStream {
    /// Output schema: group columns followed by partial aggregate state columns.
    schema: SchemaRef,

    /// Input batches containing partial aggregate state rows.
    input: SendableRecordBatchStream,

    /// Target output batch size from configuration.
    batch_size: usize,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,

    /// Tracks the high-level stream lifecycle. The hash table owns the lower-level
    /// state for emitting output batches.
    state: Option<PartialReduceHashAggregateState>,
}

/// States for partial-reduce hash aggregation processing.
// The typestate pattern mirrors the final stream and keeps the input/output
// semantics explicit for this mode.
enum PartialReduceHashAggregateState {
    ReadingInput {
        hash_table: AggregateHashTable<PartialReduceMarker>,
    },
    /// A fully materialized partial-state batch being emitted incrementally
    /// because the table ran out of memory while reading input.
    EmittingOnMemoryPressure {
        hash_table: AggregateHashTable<PartialReduceMarker>,
        // After each incremental emitting step, `remaining_groups` is updated
        // with batch slicing.
        remaining_groups: RecordBatch,
    },
    ProducingOutput {
        hash_table: AggregateHashTable<PartialReduceMarker>,
    },
    Done,
    /// Sentinel state to use when returning error from any other states, because:
    /// - It explicitly releases state-owned resources immediately
    /// - More defensive against accidentally resuming execution after error
    Error,
}

type PartialReduceHashAggregatePoll = Poll<Option<Result<RecordBatch>>>;
type PartialReduceHashAggregateStateTransition = ControlFlow<
    (
        PartialReduceHashAggregatePoll,
        PartialReduceHashAggregateState,
    ),
    PartialReduceHashAggregateState,
>;

impl PartialReduceHashAggregateState {
    fn hash_table(&self) -> &AggregateHashTable<PartialReduceMarker> {
        match self {
            Self::ReadingInput { hash_table }
            | Self::EmittingOnMemoryPressure { hash_table, .. }
            | Self::ProducingOutput { hash_table } => hash_table,
            Self::Done | Self::Error => {
                unreachable!("Done and Error states do not hold a hash table")
            }
        }
    }

    fn hash_table_mut(&mut self) -> &mut AggregateHashTable<PartialReduceMarker> {
        match self {
            Self::ReadingInput { hash_table }
            | Self::EmittingOnMemoryPressure { hash_table, .. }
            | Self::ProducingOutput { hash_table } => hash_table,
            Self::Done | Self::Error => {
                unreachable!("Done and Error states do not hold a hash table")
            }
        }
    }

    fn into_hash_table(self) -> AggregateHashTable<PartialReduceMarker> {
        match self {
            Self::ReadingInput { hash_table }
            | Self::EmittingOnMemoryPressure { hash_table, .. }
            | Self::ProducingOutput { hash_table } => hash_table,
            Self::Done | Self::Error => {
                unreachable!("Done and Error states do not hold a hash table")
            }
        }
    }

    fn into_producing_output(self) -> Self {
        Self::ProducingOutput {
            hash_table: self.into_hash_table(),
        }
    }

    fn into_done(self) -> Self {
        Self::Done
    }
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
            batch_size,
            baseline_metrics,
            reservation,
            state: Some(PartialReduceHashAggregateState::ReadingInput { hash_table }),
        })
    }

    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
    }

    fn break_with_err(
        error: DataFusionError,
    ) -> PartialReduceHashAggregateStateTransition {
        ControlFlow::Break((
            Poll::Ready(Some(Err(error))),
            PartialReduceHashAggregateState::Error,
        ))
    }

    /// Handle ReadingInput state - aggregate partial state batches into the hash table.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_reading_input(
        &mut self,
        cx: &mut Context<'_>,
        mut original_state: PartialReduceHashAggregateState,
    ) -> PartialReduceHashAggregateStateTransition {
        debug_assert!(matches!(
            &original_state,
            PartialReduceHashAggregateState::ReadingInput { .. }
        ));
        debug_assert!(original_state.hash_table().is_building());

        match self.input.poll_next_unpin(cx) {
            Poll::Pending => ControlFlow::Break((Poll::Pending, original_state)),
            // Get a new input batch, aggregate it in the hash table
            Poll::Ready(Some(Ok(batch))) => {
                let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                let timer = elapsed_compute.timer();
                let result = original_state.hash_table_mut().aggregate_batch(&batch);
                timer.done();

                if let Err(e) = result {
                    return Self::break_with_err(e);
                }

                // Update the memory reservation. If OOM, do early emit.
                self.resize_or_emit_early(original_state)
            }
            Poll::Ready(Some(Err(e))) => Self::break_with_err(e),
            // Input ends, move to output state
            Poll::Ready(None) => {
                self.close_input();
                let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                let timer = elapsed_compute.timer();
                let result = original_state.hash_table_mut().start_output();
                timer.done();

                match result {
                    Ok(()) => {
                        ControlFlow::Continue(original_state.into_producing_output())
                    }
                    Err(e) => Self::break_with_err(e),
                }
            }
        }
    }

    /// Update the memory reservation. If the reservation succeeds, continue reading
    /// input. If OOM, clear the aggregated states in the hash table, and early emit
    /// them immediately.
    ///
    /// Returns the next state; the caller finishes the intended task based on it.
    ///
    /// The reservation is left at its pre-emission size while the states are being
    /// emitted, because the cleared states are still held in memory as
    /// `remaining_groups`. The reservation will be reset after exiting the
    /// `EmittingOnMemoryPressure` state.
    ///
    /// # Implementation Note
    /// All accumulated states are materialized at once, and then sliced into
    /// `batch_size` output batches. Emit them incrementally after blocked state
    /// management is ready.
    ///
    /// Issue: <https://github.com/apache/datafusion/issues/7065>
    fn resize_or_emit_early(
        &mut self,
        mut original_state: PartialReduceHashAggregateState,
    ) -> PartialReduceHashAggregateStateTransition {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer(); // Stop on drop
        let resize_result = self
            .reservation
            .try_resize(original_state.hash_table().memory_size());

        let oom = match resize_result {
            Ok(()) => return ControlFlow::Continue(original_state),
            Err(e @ DataFusionError::ResourcesExhausted(_)) => e,
            Err(e) => return Self::break_with_err(e),
        };

        let state_batch_result = original_state.hash_table_mut().take_state_batch();

        match state_batch_result {
            Ok(Some(remaining_groups)) => ControlFlow::Continue(
                PartialReduceHashAggregateState::EmittingOnMemoryPressure {
                    hash_table: original_state.into_hash_table(),
                    remaining_groups,
                },
            ),
            // No accumulated group to emit, so early emission cannot release any
            // memory: report the original error.
            Ok(None) => Self::break_with_err(oom),
            Err(e) => Self::break_with_err(e),
        }
    }

    /// Handle EmittingOnMemoryPressure state - emit a materialized partial-state
    /// batch in `batch_size`(from configuration) slices. After all slices are
    /// emitted, update the memory reservation and resume reading input.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_emitting_on_memory_pressure(
        &mut self,
        original_state: PartialReduceHashAggregateState,
    ) -> PartialReduceHashAggregateStateTransition {
        let PartialReduceHashAggregateState::EmittingOnMemoryPressure {
            hash_table,
            remaining_groups: batch,
        } = original_state
        else {
            unreachable!("expected the EmittingOnMemoryPressure state")
        };

        let (output_batch, next_state) = if batch.num_rows() <= self.batch_size {
            // Go back to `ReadingInput`
            (
                batch,
                PartialReduceHashAggregateState::ReadingInput { hash_table },
            )
        } else {
            // More batches to output, continue in the current state.
            let remaining =
                batch.slice(self.batch_size, batch.num_rows() - self.batch_size);
            let output = batch.slice(0, self.batch_size);
            (
                output,
                PartialReduceHashAggregateState::EmittingOnMemoryPressure {
                    hash_table,
                    remaining_groups: remaining,
                },
            )
        };

        debug_assert!(output_batch.num_rows() > 0);
        ControlFlow::Break((
            Poll::Ready(Some(Ok(output_batch.record_output(&self.baseline_metrics)))),
            next_state,
        ))
    }

    /// Handle ProducingOutput state - emit merged partial aggregate state batches.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_producing_output(
        &mut self,
        mut original_state: PartialReduceHashAggregateState,
    ) -> PartialReduceHashAggregateStateTransition {
        debug_assert!(matches!(
            &original_state,
            PartialReduceHashAggregateState::ProducingOutput { .. }
        ));
        debug_assert!(!original_state.hash_table().is_building());

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let result = original_state.hash_table_mut().next_output_batch();
        timer.done();

        match result {
            Ok(Some(batch)) => {
                // The output is already materialized, so a failed resize cannot
                // be acted on: keep the reservation as is and finish the output.
                let _ = self
                    .reservation
                    .try_resize(original_state.hash_table().memory_size());
                debug_assert!(batch.num_rows() > 0);
                let next_state = if original_state.hash_table().is_done() {
                    original_state.into_done()
                } else {
                    original_state
                };

                ControlFlow::Break((
                    Poll::Ready(Some(Ok(batch.record_output(&self.baseline_metrics)))),
                    next_state,
                ))
            }
            Ok(None) => {
                let _ = self.reservation.try_resize(0);
                ControlFlow::Continue(original_state.into_done())
            }
            Err(e) => Self::break_with_err(e),
        }
    }
}

impl Stream for PartialReduceHashAggregateStream {
    type Item = Result<RecordBatch>;

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
    ///   -> EmittingOnMemoryPressure
    ///      The table cannot reserve enough memory. Materialize all accumulated
    ///      partial states and begin emitting them incrementally.
    ///
    ///   -> ProducingOutput
    ///      Input was exhausted. Move to the next state to start outputting
    ///      merged partial aggregate states.
    ///
    /// EmittingOnMemoryPressure
    ///   -> EmittingOnMemoryPressure
    ///      One batch-sized slice was yielded; repeat until all materialized
    ///      partial states are emitted.
    ///
    ///   -> ReadingInput
    ///      The materialized states were emitted; continue with the empty table.
    ///
    /// ProducingOutput
    ///   -> ProducingOutput
    ///      One merged partial-state output batch was yielded; repeat to
    ///      continue producing output incrementally.
    ///
    ///   -> Done
    ///      All merged partial-state output was emitted.
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
                .expect("PartialReduceHashAggregateStream state should not be None");

            let next_state = match cur_state {
                state @ PartialReduceHashAggregateState::ReadingInput { .. } => {
                    self.handle_reading_input(cx, state)
                }
                state @ PartialReduceHashAggregateState::EmittingOnMemoryPressure {
                    ..
                } => self.handle_emitting_on_memory_pressure(state),
                state @ PartialReduceHashAggregateState::ProducingOutput { .. } => {
                    self.handle_producing_output(state)
                }
                state @ PartialReduceHashAggregateState::Error => {
                    self.close_input();
                    self.reservation.free();
                    self.state = Some(state);
                    return Poll::Ready(None);
                }
                state @ PartialReduceHashAggregateState::Done => {
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
                        PartialReduceHashAggregateState::Error
                    ));

                    // The handler has already discarded its state-owned resources.
                    // Release the remaining stream-owned resources before returning.
                    self.close_input();
                    self.reservation.free();
                    self.state = Some(PartialReduceHashAggregateState::Error);
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

impl RecordBatchStream for PartialReduceHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
