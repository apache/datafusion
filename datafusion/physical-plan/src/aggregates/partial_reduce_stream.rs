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
use datafusion_common::Result;
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
pub(crate) struct PartialReduceHashAggregateStream {
    /// Output schema: group columns followed by partial aggregate state columns.
    schema: SchemaRef,

    /// Input batches containing partial aggregate state rows.
    input: SendableRecordBatchStream,

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
    ProducingOutput {
        hash_table: AggregateHashTable<PartialReduceMarker>,
    },
    Done,
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
            Self::ReadingInput { hash_table } | Self::ProducingOutput { hash_table } => {
                hash_table
            }
            Self::Done => unreachable!("Done state does not hold a hash table"),
        }
    }

    fn hash_table_mut(&mut self) -> &mut AggregateHashTable<PartialReduceMarker> {
        match self {
            Self::ReadingInput { hash_table } | Self::ProducingOutput { hash_table } => {
                hash_table
            }
            Self::Done => unreachable!("Done state does not hold a hash table"),
        }
    }

    fn into_hash_table(self) -> AggregateHashTable<PartialReduceMarker> {
        match self {
            Self::ReadingInput { hash_table } | Self::ProducingOutput { hash_table } => {
                hash_table
            }
            Self::Done => unreachable!("Done state does not hold a hash table"),
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
            baseline_metrics,
            reservation,
            state: Some(PartialReduceHashAggregateState::ReadingInput { hash_table }),
        })
    }

    fn start_output(
        &mut self,
        hash_table: &mut AggregateHashTable<PartialReduceMarker>,
    ) -> Result<()> {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
        hash_table.start_output()
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
                    return ControlFlow::Break((
                        Poll::Ready(Some(Err(e))),
                        original_state,
                    ));
                }

                if let Err(e) = self
                    .reservation
                    .try_resize(original_state.hash_table().memory_size())
                {
                    return ControlFlow::Break((
                        Poll::Ready(Some(Err(e))),
                        original_state,
                    ));
                }

                ControlFlow::Continue(original_state)
            }
            Poll::Ready(Some(Err(e))) => {
                ControlFlow::Break((Poll::Ready(Some(Err(e))), original_state))
            }
            // Input ends, move to output state
            Poll::Ready(None) => {
                let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                let timer = elapsed_compute.timer();
                let result = self.start_output(original_state.hash_table_mut());
                timer.done();

                match result {
                    Ok(()) => {
                        ControlFlow::Continue(original_state.into_producing_output())
                    }
                    Err(e) => {
                        ControlFlow::Break((Poll::Ready(Some(Err(e))), original_state))
                    }
                }
            }
        }
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
            Err(e) => ControlFlow::Break((Poll::Ready(Some(Err(e))), original_state)),
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
    ///   -> ProducingOutput
    ///      Input was exhausted. Move to the next state to start outputting
    ///      merged partial aggregate states.
    ///
    /// ProducingOutput
    ///   -> ProducingOutput
    ///      One merged partial-state output batch was yielded; repeat to
    ///      continue producing output incrementally.
    ///
    ///   -> Done
    ///      All merged partial-state output was emitted.
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
                state @ PartialReduceHashAggregateState::ProducingOutput { .. } => {
                    self.handle_producing_output(state)
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

impl RecordBatchStream for PartialReduceHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
