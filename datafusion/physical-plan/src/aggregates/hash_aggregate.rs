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

//! 2-stage hash aggregation stream implementation.
//!
//! See comments in [`PartialHashAggregateStream`] and [`FinalHashAggregateStream`]
//! for details.
//!
//! Note these streams are an incremental migration of the existing
//! [`crate::aggregates::row_hash::GroupedHashAggregateStream`].
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
use super::hash_table::{AggregateHashTable, Final, Partial, PartialSkip};
use super::utils::SkipAggregationProbe;
use crate::metrics::{
    BaselineMetrics, MetricBuilder, MetricCategory, RecordOutput, SpillMetrics,
};
use crate::stream::EmptyRecordBatchStream;
use crate::{InputOrderMode, RecordBatchStream, SendableRecordBatchStream, metrics};

/// Hash aggregation uses a 2-stage (partial and final) hash aggregation, this stream
/// is for the partial stage.
///
/// # Example
///
/// select k, avg(v) from t group by k;
///
/// ## Plan
/// AggregateExec(stage=final)
/// -- RepartitionExec(hash(k))
/// ---- AggregateExec(stage=partial)
///
/// ## Partial Stage Behavior
/// Input: raw rows
/// Output: partial states for all groups (e.g. for avg(x), it's sum(x), count(x))
///
/// ## Final Stage Behavior
/// Input: partial states
/// Output: results for all groups (e.g. for avg(x), it's avg(x) calculated from the state)
///
/// # Optimization: DISTINCT LIMIT Soft Limit
///
/// This optimization applies to both [`PartialHashAggregateStream`] and [`FinalHashAggregateStream`]
///
/// Unordered distinct queries such as:
///
/// ```sql
/// SELECT DISTINCT x FROM t LIMIT 10;
/// ```
///
/// are optimized into a two-stage aggregate like:
///
/// ```txt
/// LimitExec, limit=10
/// --AggregateExec(Final), group_by=[x], aggr=[], soft_limit=10
/// ---- RepartitionExec, partitioning=hash(x)
/// ------ AggregateExec(Partial), group_by=[x], aggr=[], soft_limit=10
/// -------- Scan(t)
/// ```
///
/// After each input batch, the stream checks whether the soft limit has been
/// reached. If so, it emits the accumulated groups and stops reading input.
///
/// This operator does not guarantee an exact limit because a single batch can
/// cross the threshold. The downstream limit operator enforces the exact result
/// size.
///
/// # Optimization: Partial Aggregation Skip
///
/// Partial aggregation can be counterproductive for high-cardinality inputs,
/// where most rows create distinct groups. The stream probes the ratio of
/// accumulated groups to input rows while it is still aggregating. If the ratio
/// crosses the configured threshold and all aggregate accumulators can convert
/// raw inputs directly to partial state, the stream emits any already
/// accumulated groups, then switches to a skip state. In that state, each
/// remaining input batch is converted directly to partial aggregate state rows
/// without inserting the rows into the grouped hash table.
pub(crate) struct PartialHashAggregateStream {
    /// Output schema: group columns followed by partial aggregate state columns.
    schema: SchemaRef,

    /// Input batches containing raw rows, not partial aggregate state.
    input: SendableRecordBatchStream,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Tracks partial aggregation row reduction, matching `GroupedHashAggregateStream`.
    reduction_factor: metrics::RatioMetrics,

    /// Tracks whether partial aggregation should switch to direct state conversion.
    skip_aggregation_probe: Option<SkipAggregationProbe>,

    /// Optional soft limit on the number of groups to accumulate before output.
    ///
    /// Invariant: when this is `Some(..)`, the accumulators inside `hash_table` must
    /// be empty. See struct comments for details.
    group_values_soft_limit: Option<usize>,

    /// Tracks the high-level stream lifecycle. The hash table owns the lower-level
    /// state for materializing and slicing output batches.
    state: Option<PartialHashAggregateState>,
}

/// States for partial hash aggregation processing.
enum PartialHashAggregateState {
    ReadingInput {
        hash_table: AggregateHashTable<Partial>,
    },
    ProducingOutput {
        hash_table: AggregateHashTable<Partial>,
        /// If `None`, partial skip was never triggered and this state will
        /// finish in `Done`. If `Some`, partial skip has triggered and the
        /// stream will move to `SkippingAggregation` after these accumulated
        /// groups are emitted.
        skip_hash_table: Option<AggregateHashTable<PartialSkip>>,
    },
    SkippingAggregation {
        hash_table: AggregateHashTable<PartialSkip>,
    },
    Done,
}

type PartialHashAggregatePoll = Poll<Option<Result<RecordBatch>>>;
type PartialHashAggregateStateTransition = ControlFlow<
    (PartialHashAggregatePoll, PartialHashAggregateState),
    PartialHashAggregateState,
>;

impl PartialHashAggregateState {
    fn hash_table(&self) -> &AggregateHashTable<Partial> {
        match self {
            Self::ReadingInput { hash_table }
            | Self::ProducingOutput { hash_table, .. } => hash_table,
            Self::SkippingAggregation { .. } | Self::Done => {
                unreachable!("state does not hold a partial hash table")
            }
        }
    }

    fn hash_table_mut(&mut self) -> &mut AggregateHashTable<Partial> {
        match self {
            Self::ReadingInput { hash_table }
            | Self::ProducingOutput { hash_table, .. } => hash_table,
            Self::SkippingAggregation { .. } | Self::Done => {
                unreachable!("state does not hold a partial hash table")
            }
        }
    }
}

/// Hash aggregation uses a 2-stage (partial and final) hash aggregation, this stream
/// is for the final stage.
///
/// See [`PartialHashAggregateStream`] for details.
pub(crate) struct FinalHashAggregateStream {
    /// Output schema: group columns followed by final aggregate value columns.
    schema: SchemaRef,

    /// Input batches containing partial aggregate state rows.
    input: SendableRecordBatchStream,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,

    /// See comments for the same variable in [`PartialHashAggregateStream`]
    group_values_soft_limit: Option<usize>,

    /// Tracks the high-level stream lifecycle. The hash table owns the lower-level
    /// state for materializing and slicing output batches.
    state: Option<FinalHashAggregateState>,
}

/// States for final hash aggregation processing.
// Typestate pattern is used, in case the inner logic become more complex in the future.
enum FinalHashAggregateState {
    ReadingInput {
        hash_table: AggregateHashTable<Final>,
    },
    ProducingOutput {
        hash_table: AggregateHashTable<Final>,
    },
    Done,
}

type FinalHashAggregatePoll = Poll<Option<Result<RecordBatch>>>;
type FinalHashAggregateStateTransition = ControlFlow<
    (FinalHashAggregatePoll, FinalHashAggregateState),
    FinalHashAggregateState,
>;

impl FinalHashAggregateState {
    fn hash_table(&self) -> &AggregateHashTable<Final> {
        match self {
            Self::ReadingInput { hash_table } | Self::ProducingOutput { hash_table } => {
                hash_table
            }
            Self::Done => unreachable!("Done state does not hold a hash table"),
        }
    }

    fn hash_table_mut(&mut self) -> &mut AggregateHashTable<Final> {
        match self {
            Self::ReadingInput { hash_table } | Self::ProducingOutput { hash_table } => {
                hash_table
            }
            Self::Done => unreachable!("Done state does not hold a hash table"),
        }
    }

    fn into_hash_table(self) -> AggregateHashTable<Final> {
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

impl PartialHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert_eq!(agg.mode, super::AggregateMode::Partial);
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);
        let reduction_factor = MetricBuilder::new(&agg.metrics)
            .with_type(metrics::MetricType::Summary)
            .ratio_metrics("reduction_factor", partition);

        let hash_table = AggregateHashTable::<Partial>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;
        let can_skip_aggregation =
            agg.group_by.is_single() && hash_table.can_skip_aggregation();
        let skip_aggregation_probe = if can_skip_aggregation {
            let options = &context.session_config().options().execution;
            let probe_ratio_threshold =
                options.skip_partial_aggregation_probe_ratio_threshold;
            // A threshold >= 1.0 means the ratio (num_groups / input_rows) can
            // never exceed it, so the feature is effectively disabled.
            if probe_ratio_threshold >= 1.0 {
                None
            } else {
                let skipped_aggregation_rows = MetricBuilder::new(&agg.metrics)
                    .with_category(MetricCategory::Rows)
                    .counter("skipped_aggregation_rows", partition);
                Some(SkipAggregationProbe::new(
                    options.skip_partial_aggregation_probe_rows_threshold,
                    probe_ratio_threshold,
                    skipped_aggregation_rows,
                ))
            }
        } else {
            None
        };

        let reservation =
            MemoryConsumer::new(format!("PartialHashAggregateStream[{partition}]"))
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            baseline_metrics,
            reservation,
            reduction_factor,
            skip_aggregation_probe,
            group_values_soft_limit: agg.limit_options().map(|config| config.limit()),
            state: Some(PartialHashAggregateState::ReadingInput { hash_table }),
        })
    }

    /// See comments in [`Self::group_values_soft_limit`] for details.
    fn hit_soft_group_limit(&self, hash_table: &AggregateHashTable<Partial>) -> bool {
        self.group_values_soft_limit
            .is_some_and(|limit| limit <= hash_table.building_group_count())
    }

    /// Updates skip aggregation probe state.
    fn update_skip_aggregation_probe(&mut self, input_rows: usize, num_groups: usize) {
        if let Some(probe) = self.skip_aggregation_probe.as_mut() {
            probe.update_state(input_rows, num_groups);
        }
    }

    /// Returns true if the aggregation probe indicates that aggregation
    /// should be skipped.
    fn should_skip_aggregation(&self) -> bool {
        self.skip_aggregation_probe
            .as_ref()
            .is_some_and(|probe| probe.should_skip())
    }

    fn start_output(
        &mut self,
        hash_table: &mut AggregateHashTable<Partial>,
        close_input: bool,
    ) -> Result<()> {
        if close_input {
            let input_schema = self.input.schema();
            self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
        }
        hash_table.start_output()
    }

    /// Handle ReadingInput state - aggregate input batches into the hash table.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_reading_input(
        &mut self,
        cx: &mut Context<'_>,
        mut original_state: PartialHashAggregateState,
    ) -> PartialHashAggregateStateTransition {
        debug_assert!(matches!(
            &original_state,
            PartialHashAggregateState::ReadingInput { .. }
        ));
        debug_assert!(original_state.hash_table().is_building());

        match self.input.poll_next_unpin(cx) {
            Poll::Pending => ControlFlow::Break((Poll::Pending, original_state)),
            Poll::Ready(Some(Ok(batch))) => {
                let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                let timer = elapsed_compute.timer();
                let input_rows = batch.num_rows();
                self.reduction_factor.add_total(input_rows);
                let result = original_state.hash_table_mut().aggregate_batch(&batch);
                timer.done();

                if let Err(e) = result {
                    return ControlFlow::Break((
                        Poll::Ready(Some(Err(e))),
                        original_state,
                    ));
                }

                if self.hit_soft_group_limit(original_state.hash_table()) {
                    let timer = elapsed_compute.timer();
                    let result = self.start_output(original_state.hash_table_mut(), true);
                    timer.done();

                    if let Err(e) = result {
                        return ControlFlow::Break((
                            Poll::Ready(Some(Err(e))),
                            original_state,
                        ));
                    }

                    let PartialHashAggregateState::ReadingInput { hash_table } =
                        original_state
                    else {
                        unreachable!("expected reading input state")
                    };
                    return ControlFlow::Continue(
                        PartialHashAggregateState::ProducingOutput {
                            hash_table,
                            skip_hash_table: None,
                        },
                    );
                }

                self.update_skip_aggregation_probe(
                    input_rows,
                    original_state.hash_table().building_group_count(),
                );

                // True branch: a decision has been made to skip partial aggregation.
                if self.should_skip_aggregation() {
                    let timer = elapsed_compute.timer();
                    let result = match original_state.hash_table().partial_skip_table() {
                        Ok(skip_hash_table) => self
                            .start_output(original_state.hash_table_mut(), false)
                            .map(|()| skip_hash_table),
                        Err(e) => Err(e),
                    };
                    timer.done();

                    match result {
                        Ok(skip_hash_table) => {
                            let PartialHashAggregateState::ReadingInput { hash_table } =
                                original_state
                            else {
                                unreachable!("expected reading input state")
                            };

                            // Move to `ProdicingOutput` state first, its `skip_hash_table`
                            // field will ensure it will go to the next skip-partial-aggr
                            // stage, after the accumulated batches has been output.
                            return ControlFlow::Continue(
                                PartialHashAggregateState::ProducingOutput {
                                    hash_table,
                                    skip_hash_table: Some(skip_hash_table),
                                },
                            );
                        }
                        Err(e) => {
                            return ControlFlow::Break((
                                Poll::Ready(Some(Err(e))),
                                original_state,
                            ));
                        }
                    }
                }

                // TODO: impl memory-limited aggr, when OOM directly send
                // partial state to final aggregate stage
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
            Poll::Ready(None) => {
                let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                let timer = elapsed_compute.timer();
                let result = self.start_output(original_state.hash_table_mut(), true);
                timer.done();

                match result {
                    Ok(()) => {
                        let PartialHashAggregateState::ReadingInput { hash_table } =
                            original_state
                        else {
                            unreachable!("expected reading input state")
                        };
                        ControlFlow::Continue(
                            PartialHashAggregateState::ProducingOutput {
                                hash_table,
                                skip_hash_table: None,
                            },
                        )
                    }
                    Err(e) => {
                        ControlFlow::Break((Poll::Ready(Some(Err(e))), original_state))
                    }
                }
            }
        }
    }

    /// Handle ProducingOutput state - emit partial aggregate state batches.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_producing_output(
        &mut self,
        mut original_state: PartialHashAggregateState,
    ) -> PartialHashAggregateStateTransition {
        debug_assert!(matches!(
            &original_state,
            PartialHashAggregateState::ProducingOutput { .. }
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
                self.reduction_factor.add_part(batch.num_rows());
                debug_assert!(batch.num_rows() > 0);
                let next_state = if original_state.hash_table().is_done() {
                    match original_state {
                        PartialHashAggregateState::ProducingOutput {
                            skip_hash_table: Some(hash_table),
                            ..
                        } => {
                            PartialHashAggregateState::SkippingAggregation { hash_table }
                        }
                        PartialHashAggregateState::ProducingOutput {
                            skip_hash_table: None,
                            ..
                        } => PartialHashAggregateState::Done,
                        _ => unreachable!("expected producing output state"),
                    }
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
                // If in the previous `Aggregating` stage it has decided to skip partial
                // aggregation, go the `SkipAggregation` stage; otherwise finish.
                let next_state = match original_state {
                    PartialHashAggregateState::ProducingOutput {
                        skip_hash_table: Some(hash_table),
                        ..
                    } => PartialHashAggregateState::SkippingAggregation { hash_table },
                    PartialHashAggregateState::ProducingOutput {
                        skip_hash_table: None,
                        ..
                    } => PartialHashAggregateState::Done,
                    _ => unreachable!("expected producing output state"),
                };
                ControlFlow::Continue(next_state)
            }
            Err(e) => ControlFlow::Break((Poll::Ready(Some(Err(e))), original_state)),
        }
    }

    /// Handle SkippingAggregation state - convert raw input directly to partial states.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_skipping_aggregation(
        &mut self,
        cx: &mut Context<'_>,
        mut original_state: PartialHashAggregateState,
    ) -> PartialHashAggregateStateTransition {
        debug_assert!(matches!(
            &original_state,
            PartialHashAggregateState::SkippingAggregation { .. }
        ));

        match self.input.poll_next_unpin(cx) {
            Poll::Pending => ControlFlow::Break((Poll::Pending, original_state)),
            Poll::Ready(Some(Ok(batch))) => {
                if let Some(probe) = self.skip_aggregation_probe.as_mut() {
                    probe.record_skipped(&batch);
                }

                let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                let timer = elapsed_compute.timer();
                let result = match &mut original_state {
                    PartialHashAggregateState::SkippingAggregation { hash_table } => {
                        hash_table.convert_batch_to_state(&batch)
                    }
                    _ => unreachable!("expected skipping aggregation state"),
                };
                timer.done();

                match result {
                    Ok(batch) => ControlFlow::Break((
                        Poll::Ready(Some(
                            Ok(batch.record_output(&self.baseline_metrics)),
                        )),
                        original_state,
                    )),
                    Err(e) => {
                        ControlFlow::Break((Poll::Ready(Some(Err(e))), original_state))
                    }
                }
            }
            Poll::Ready(Some(Err(e))) => {
                ControlFlow::Break((Poll::Ready(Some(Err(e))), original_state))
            }
            Poll::Ready(None) => {
                let input_schema = self.input.schema();
                self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
                ControlFlow::Continue(PartialHashAggregateState::Done)
            }
        }
    }
}

impl Stream for PartialHashAggregateStream {
    type Item = Result<RecordBatch>;

    /// Entry point for the partial hash aggregate state machine.
    ///
    /// See comments in [`PartialHashAggregateStream`] for high-level ideas.
    ///
    /// ============================
    /// State transition graph:
    /// ============================
    ///
    /// (start) --> ReadingInput
    /// ----------------------------
    /// ReadingInput -> ReadingInput (after aggregating an input batch)
    /// ReadingInput -> ProducingOutput(skip=None) (input exhausted or soft
    /// limit reached)
    /// ReadingInput -> ProducingOutput(skip=Some) (partial skip triggered)
    ///
    /// ProducingOutput(skip=None) -> ProducingOutput(skip=None)
    /// (after yielding one accumulated output batch)
    /// ProducingOutput(skip=None) -> Done (all accumulated output emitted)
    ///
    /// ProducingOutput(skip=Some) -> ProducingOutput(skip=Some)
    /// (after yielding one accumulated output batch)
    /// ProducingOutput(skip=Some) -> SkippingAggregation (all accumulated output
    /// emitted)
    ///
    /// SkippingAggregation -> SkippingAggregation (after yielding one
    /// `convert_to_state` batch)
    /// SkippingAggregation -> Done (input exhausted)
    /// ----------------------------
    /// Done -> (end)
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let cur_state = self
                .state
                .take()
                .expect("PartialHashAggregateStream state should not be None");

            let next_state = match cur_state {
                state @ PartialHashAggregateState::ReadingInput { .. } => {
                    self.handle_reading_input(cx, state)
                }
                state @ PartialHashAggregateState::ProducingOutput { .. } => {
                    self.handle_producing_output(state)
                }
                state @ PartialHashAggregateState::SkippingAggregation { .. } => {
                    self.handle_skipping_aggregation(cx, state)
                }
                state @ PartialHashAggregateState::Done => {
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

impl RecordBatchStream for PartialHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl FinalHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            super::AggregateMode::Final | super::AggregateMode::FinalPartitioned
        ));
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);

        let hash_table = AggregateHashTable::<Final>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;

        let reservation =
            MemoryConsumer::new(format!("FinalHashAggregateStream[{partition}]"))
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            baseline_metrics,
            reservation,
            group_values_soft_limit: agg.limit_options().map(|config| config.limit()),
            state: Some(FinalHashAggregateState::ReadingInput { hash_table }),
        })
    }

    /// See comments in [`Self::group_values_soft_limit`] for details.
    fn hit_soft_group_limit(&self, hash_table: &AggregateHashTable<Final>) -> bool {
        self.group_values_soft_limit
            .is_some_and(|limit| limit <= hash_table.building_group_count())
    }

    fn start_output(&mut self, hash_table: &mut AggregateHashTable<Final>) -> Result<()> {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
        hash_table.start_output()
    }

    /// Handle ReadingInput state - aggregate partial state batches into the hash table.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_reading_input(
        &mut self,
        cx: &mut Context<'_>,
        mut original_state: FinalHashAggregateState,
    ) -> FinalHashAggregateStateTransition {
        debug_assert!(matches!(
            &original_state,
            FinalHashAggregateState::ReadingInput { .. }
        ));
        debug_assert!(original_state.hash_table().is_building());

        match self.input.poll_next_unpin(cx) {
            Poll::Pending => ControlFlow::Break((Poll::Pending, original_state)),
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

                if self.hit_soft_group_limit(original_state.hash_table()) {
                    let timer = elapsed_compute.timer();
                    let result = self.start_output(original_state.hash_table_mut());
                    timer.done();

                    if let Err(e) = result {
                        return ControlFlow::Break((
                            Poll::Ready(Some(Err(e))),
                            original_state,
                        ));
                    }

                    return ControlFlow::Continue(original_state.into_producing_output());
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

    /// Handle ProducingOutput state - emit final aggregate value batches.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_producing_output(
        &mut self,
        mut original_state: FinalHashAggregateState,
    ) -> FinalHashAggregateStateTransition {
        debug_assert!(matches!(
            &original_state,
            FinalHashAggregateState::ProducingOutput { .. }
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

impl Stream for FinalHashAggregateStream {
    type Item = Result<RecordBatch>;

    /// Entry point for the final hash aggregate state machine.
    ///
    /// See comments in [`FinalHashAggregateStream`] for high-level ideas.
    ///
    /// ============================
    /// State transition graph:
    /// ============================
    ///
    /// (start) --> ReadingInput
    /// ----------------------------
    /// ReadingInput -> ReadingInput (after aggregating one partial-state input
    /// batch)
    /// ReadingInput -> ProducingOutput (input exhausted or soft limit reached)
    ///
    /// ProducingOutput -> ProducingOutput (after yielding one final output batch)
    /// ProducingOutput -> Done (all final output emitted)
    /// ----------------------------
    /// Done -> (end)
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let cur_state = self
                .state
                .take()
                .expect("FinalHashAggregateStream state should not be None");

            let next_state = match cur_state {
                state @ FinalHashAggregateState::ReadingInput { .. } => {
                    self.handle_reading_input(cx, state)
                }
                state @ FinalHashAggregateState::ProducingOutput { .. } => {
                    self.handle_producing_output(state)
                }
                state @ FinalHashAggregateState::Done => {
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

impl RecordBatchStream for FinalHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
