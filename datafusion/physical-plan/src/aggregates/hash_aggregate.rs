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

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::Array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_uint32_array;
use datafusion_common::utils::memory::get_record_batch_memory_size;
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::stream::{Stream, StreamExt};

use super::aggregate_hash_table::{
    AggregateHashTable, FinalMarker, PartialMarker, PartialSkipMarker,
};
use super::skip_partial::SkipAggregationProbe;
use super::{AggregateExec, strip_subpartition_column, subpartition_column_index};
use crate::coalesce::{LimitedBatchCoalescer, PushBatchStatus};
use crate::metrics::{
    BaselineMetrics, MetricBuilder, MetricCategory, RecordOutput, SpillMetrics,
};
use crate::stream::EmptyRecordBatchStream;
use crate::{InputOrderMode, RecordBatchStream, SendableRecordBatchStream, metrics};

struct CoalescedPartitionStream {
    schema: SchemaRef,
    coalescer: LimitedBatchCoalescer,
}

impl Stream for CoalescedPartitionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().coalescer.next_completed_batch().map(Ok))
    }
}

impl RecordBatchStream for CoalescedPartitionStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

struct FinalPartitionRunState {
    coalescers: BTreeMap<usize, LimitedBatchCoalescer>,
    coalescer_sizes: BTreeMap<usize, usize>,
    total_coalescer_size: usize,
    replaying_coalescer_size: usize,
    replaying_partition_id: Option<usize>,
    is_draining: bool,
    schema: Option<SchemaRef>,
    target_batch_size: usize,
}

impl FinalPartitionRunState {
    fn new(target_batch_size: usize) -> Self {
        Self {
            coalescers: BTreeMap::new(),
            coalescer_sizes: BTreeMap::new(),
            total_coalescer_size: 0,
            replaying_coalescer_size: 0,
            replaying_partition_id: None,
            is_draining: false,
            schema: None,
            target_batch_size,
        }
    }

    fn total_size(&self) -> usize {
        self.total_coalescer_size + self.replaying_coalescer_size
    }

    fn has_buffered_partitions(&self) -> bool {
        !self.coalescers.is_empty()
    }

    fn output_schema(&mut self, schema: SchemaRef) -> Result<()> {
        match &self.schema {
            Some(existing) if !Arc::ptr_eq(existing, &schema) && existing != &schema => {
                internal_err!(
                    "final partitioned aggregation received inconsistent buffered schemas"
                )
            }
            Some(_) => Ok(()),
            None => {
                self.schema = Some(schema);
                Ok(())
            }
        }
    }

    fn stage_batch(
        &mut self,
        batch: RecordBatch,
        partition_id: usize,
        batch_size: usize,
    ) -> Result<()> {
        self.output_schema(batch.schema())?;
        let coalescer = self.coalescers.entry(partition_id).or_insert_with(|| {
            LimitedBatchCoalescer::new(batch.schema(), self.target_batch_size, None)
                .with_biggest_coalesce_batch_size(None)
        });
        let push_status = coalescer.push_batch(batch)?;
        debug_assert_eq!(push_status, PushBatchStatus::Continue);
        *self.coalescer_sizes.entry(partition_id).or_default() += batch_size;
        self.total_coalescer_size += batch_size;
        Ok(())
    }

    fn begin_replay(&mut self) -> Result<()> {
        self.is_draining = true;
        for coalescer in self.coalescers.values_mut() {
            coalescer.finish()?;
        }
        Ok(())
    }

    fn finish_replaying_run(&mut self) {
        self.replaying_coalescer_size = 0;
        self.replaying_partition_id = None;
    }

    fn next_partition_id(&self) -> Option<usize> {
        self.coalescers
            .first_key_value()
            .map(|(partition_id, _)| *partition_id)
    }

    fn take_run(&mut self, partition_id: usize) -> Result<LimitedBatchCoalescer> {
        let coalescer = self.coalescers.remove(&partition_id).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Missing coalescer for final aggregate partition {partition_id}"
            ))
        })?;
        let coalescer_size =
            self.coalescer_sizes.remove(&partition_id).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Missing coalescer size for final aggregate partition {partition_id}"
                ))
            })?;
        self.total_coalescer_size -= coalescer_size;
        self.replaying_coalescer_size = coalescer_size;
        self.replaying_partition_id = Some(partition_id);
        Ok(coalescer)
    }
}

/// Hash aggregation is implemented in two stages: partial and final. This
/// stream implements the partial stage.
///
/// # Example
///
/// SELECT k, AVG(v) FROM t GROUP BY k;
///
/// ## Plan
/// AggregateExec(stage=final)
/// -- RepartitionExec(hash(k))
/// ---- AggregateExec(stage=partial)
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
/// # Optimization: DISTINCT LIMIT Soft Limit
///
/// This optimization applies to both [`PartialHashAggregateStream`] and
/// [`FinalHashAggregateStream`].
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
    /// state for emitting output batches.
    state: Option<PartialHashAggregateState>,
}

/// States for partial hash aggregation processing.
enum PartialHashAggregateState {
    ReadingInput {
        hash_table: AggregateHashTable<PartialMarker>,
    },
    ProducingOutput {
        hash_table: AggregateHashTable<PartialMarker>,
        /// If `None`, partial skip was never triggered and this state will
        /// finish in `Done`. If `Some`, partial skip has triggered and the
        /// stream will move to `SkippingAggregation` after these accumulated
        /// groups are emitted.
        skip_hash_table: Option<AggregateHashTable<PartialSkipMarker>>,
    },
    SkippingAggregation {
        hash_table: AggregateHashTable<PartialSkipMarker>,
    },
    Done,
}

type PartialHashAggregatePoll = Poll<Option<Result<RecordBatch>>>;
type PartialHashAggregateStateTransition = ControlFlow<
    (PartialHashAggregatePoll, PartialHashAggregateState),
    PartialHashAggregateState,
>;

impl PartialHashAggregateState {
    fn hash_table(&self) -> &AggregateHashTable<PartialMarker> {
        match self {
            Self::ReadingInput { hash_table }
            | Self::ProducingOutput { hash_table, .. } => hash_table,
            Self::SkippingAggregation { .. } | Self::Done => {
                unreachable!("state does not hold a partial hash table")
            }
        }
    }

    fn hash_table_mut(&mut self) -> &mut AggregateHashTable<PartialMarker> {
        match self {
            Self::ReadingInput { hash_table }
            | Self::ProducingOutput { hash_table, .. } => hash_table,
            Self::SkippingAggregation { .. } | Self::Done => {
                unreachable!("state does not hold a partial hash table")
            }
        }
    }
}

/// Hash aggregation is implemented in two stages: partial and final. This
/// stream implements the final stage.
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

    /// Buffered final-partitioned runs replayed one aggregate partition at a time.
    partition_run_state: Option<FinalPartitionRunState>,

    /// See comments for the same variable in [`PartialHashAggregateStream`].
    group_values_soft_limit: Option<usize>,

    /// Tracks the high-level stream lifecycle. The hash table owns the lower-level
    /// state for emitting output batches.
    state: Option<FinalHashAggregateState>,
}

/// States for final hash aggregation processing.
// The typestate pattern is used in case the inner logic becomes more complex in
// the future.
enum FinalHashAggregateState {
    ReadingInput {
        hash_table: AggregateHashTable<FinalMarker>,
    },
    ProducingOutput {
        hash_table: AggregateHashTable<FinalMarker>,
    },
    Done,
}

type FinalHashAggregatePoll = Poll<Option<Result<RecordBatch>>>;
type FinalHashAggregateStateTransition = ControlFlow<
    (FinalHashAggregatePoll, FinalHashAggregateState),
    FinalHashAggregateState,
>;

impl FinalHashAggregateState {
    fn hash_table(&self) -> &AggregateHashTable<FinalMarker> {
        match self {
            Self::ReadingInput { hash_table } | Self::ProducingOutput { hash_table } => {
                hash_table
            }
            Self::Done => unreachable!("Done state does not hold a hash table"),
        }
    }

    fn hash_table_mut(&mut self) -> &mut AggregateHashTable<FinalMarker> {
        match self {
            Self::ReadingInput { hash_table } | Self::ProducingOutput { hash_table } => {
                hash_table
            }
            Self::Done => unreachable!("Done state does not hold a hash table"),
        }
    }

    fn into_hash_table(self) -> AggregateHashTable<FinalMarker> {
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

        let hash_table = AggregateHashTable::<PartialMarker>::new(
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
    fn hit_soft_group_limit(
        &self,
        hash_table: &AggregateHashTable<PartialMarker>,
    ) -> bool {
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
        hash_table: &mut AggregateHashTable<PartialMarker>,
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
    /// See comments at `poll_next()` for details.
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

                            // Move to `ProducingOutput` first. Its `skip_hash_table`
                            // field moves the stream to skip-partial aggregation after
                            // the accumulated batches have been output.
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
    /// See comments at `poll_next()` for details.
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
                // If the previous `Aggregating` stage decided to skip partial
                // aggregation, go to the `SkippingAggregation` stage; otherwise finish.
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
    /// See comments at `poll_next()` for details.
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
    /// State transition graph:
    ///
    /// ```text
    /// (start)
    ///   -> ReadingInput
    ///      The stream starts by polling input and aggregating batches into the
    ///      in-memory hash table.
    ///
    /// ReadingInput
    ///   -> ReadingInput
    ///      Aggregate one batch, update the inner aggregate hash table, and
    ///      continue with the next input batch.
    ///   -> ProducingOutput(skip=None)
    ///      Input was exhausted, or the soft group limit was reached. Move to
    ///      the next state to start outputting.
    ///   -> ProducingOutput(skip=Some)
    ///      Partial skip aggregation was triggered. First move to the
    ///      `ProducingOutput` state to drain the accumulated state, then move to
    ///      the `SkippingAggregation` state to convert input directly to partial
    ///      state without aggregation.
    ///
    /// ProducingOutput(skip=None)
    ///   -> ProducingOutput(skip=None)
    ///      One accumulated output batch was yielded, repeat to continue producing
    ///      output incrementally.
    ///   -> Done
    ///      All accumulated output was emitted.
    ///
    /// ProducingOutput(skip=Some)
    ///   -> ProducingOutput(skip=Some)
    ///      One accumulated output batch was yielded, repeat to continue producing
    ///      output incrementally.
    ///   -> SkippingAggregation
    ///      All accumulated output was emitted. Continue by converting raw
    ///      input batches directly to partial aggregate state.
    ///
    /// SkippingAggregation
    ///   -> SkippingAggregation
    ///      One `convert_to_state` batch was yielded; repeat to continue
    ///      processing.
    ///   -> Done
    ///      Input was exhausted.
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

        let hash_table = AggregateHashTable::<FinalMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;

        let reservation =
            MemoryConsumer::new(format!("FinalHashAggregateStream[{partition}]"))
                .register(context.memory_pool());
        let uses_partition_runs = agg.mode == super::AggregateMode::FinalPartitioned;

        Ok(Self {
            schema,
            input,
            baseline_metrics,
            reservation,
            partition_run_state: uses_partition_runs
                .then(|| FinalPartitionRunState::new(batch_size)),
            group_values_soft_limit: agg.limit_options().map(|config| config.limit()),
            state: Some(FinalHashAggregateState::ReadingInput { hash_table }),
        })
    }

    /// See comments in [`Self::group_values_soft_limit`] for details.
    fn hit_soft_group_limit(&self, hash_table: &AggregateHashTable<FinalMarker>) -> bool {
        if self
            .partition_run_state
            .as_ref()
            .is_some_and(|state| state.is_draining || state.has_buffered_partitions())
        {
            return false;
        }

        self.group_values_soft_limit
            .is_some_and(|limit| limit <= hash_table.building_group_count())
    }

    fn should_buffer_partition_runs(&self) -> bool {
        self.partition_run_state
            .as_ref()
            .is_some_and(|state| !state.is_draining)
    }

    fn stage_partition_runs(&mut self, batch: &RecordBatch) -> Result<Option<usize>> {
        let Some(subpartition_idx) = subpartition_column_index(batch.schema_ref()) else {
            if self
                .partition_run_state
                .as_ref()
                .is_some_and(|state| state.has_buffered_partitions())
            {
                return internal_err!(
                    "missing hash aggregate subpartition column after buffered runs have started"
                );
            }
            return Ok(None);
        };

        let subpartitions = as_uint32_array(batch.column(subpartition_idx).as_ref())?;
        if subpartitions.null_count() != 0 {
            return internal_err!(
                "Hash aggregate subpartition column must not contain nulls"
            );
        }

        let output_batch = strip_subpartition_column(batch, subpartition_idx)?;
        let input_batch_size = get_record_batch_memory_size(&output_batch);
        let num_rows = output_batch.num_rows();
        let mut accounted_memory = 0;
        let mut runs = Vec::new();
        let mut values = subpartitions.values().iter().copied().enumerate();
        if let Some((run_start, first_partition)) = values.next() {
            let mut current_partition = first_partition as usize;
            let mut current_start = run_start;
            let mut current_len = 1;
            for (row_idx, relative_partition) in values {
                let relative_partition = relative_partition as usize;
                if relative_partition == current_partition {
                    current_len += 1;
                } else {
                    runs.push((current_partition, current_start, current_len));
                    current_partition = relative_partition;
                    current_start = row_idx;
                    current_len = 1;
                }
            }
            runs.push((current_partition, current_start, current_len));
        }

        for (run_idx, (relative_partition, offset, len)) in
            runs.iter().copied().enumerate()
        {
            let run_size = if run_idx + 1 == runs.len() {
                input_batch_size - accounted_memory
            } else {
                input_batch_size
                    .saturating_mul(len)
                    .checked_div(num_rows)
                    .unwrap_or(0)
            };
            accounted_memory += run_size;
            if let Some(state) = self.partition_run_state.as_mut() {
                state.stage_batch(
                    output_batch.slice(offset, len),
                    relative_partition,
                    run_size,
                )?;
            }
        }
        Ok(Some(input_batch_size))
    }

    fn reserve_staged_partition_runs(
        &mut self,
        batch_memory: usize,
        hash_table: &AggregateHashTable<FinalMarker>,
    ) -> Result<()> {
        if batch_memory == 0 {
            return Ok(());
        }

        let total_buffered_size = self
            .partition_run_state
            .as_ref()
            .map(FinalPartitionRunState::total_size)
            .unwrap_or(0);

        if total_buffered_size == batch_memory {
            return self.update_memory_reservation(Some(hash_table));
        }

        self.reservation.try_grow(batch_memory)?;
        Ok(())
    }

    fn begin_partition_run_replay(
        &mut self,
        hash_table: AggregateHashTable<FinalMarker>,
    ) -> Result<FinalHashAggregateState> {
        if let Some(state) = self.partition_run_state.as_mut() {
            state.begin_replay()?;
        }

        self.load_next_partition_run(hash_table)
    }

    fn finish_partition_run_replay(
        &mut self,
        mut hash_table: AggregateHashTable<FinalMarker>,
    ) -> Result<FinalHashAggregateState> {
        self.start_output(&mut hash_table)?;

        if let Some(state) = self.partition_run_state.as_mut() {
            state.finish_replaying_run();
        }

        self.update_memory_reservation(Some(&hash_table))?;
        Ok(FinalHashAggregateState::ProducingOutput { hash_table })
    }

    fn next_state_after_partition_output(
        &mut self,
        hash_table: AggregateHashTable<FinalMarker>,
    ) -> Result<FinalHashAggregateState> {
        if !self
            .partition_run_state
            .as_ref()
            .is_some_and(|state| state.is_draining)
        {
            return Ok(FinalHashAggregateState::Done);
        }

        self.load_next_partition_run(hash_table)
    }

    fn load_next_partition_run(
        &mut self,
        hash_table: AggregateHashTable<FinalMarker>,
    ) -> Result<FinalHashAggregateState> {
        let next_partition_id = self
            .partition_run_state
            .as_ref()
            .and_then(FinalPartitionRunState::next_partition_id);

        if let Some(partition_id) = next_partition_id {
            let coalescer = self
                .partition_run_state
                .as_mut()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "missing partition run state while replaying final aggregate runs"
                            .to_string(),
                    )
                })?
                .take_run(partition_id)?;
            let schema = coalescer.schema();
            self.input = Box::pin(CoalescedPartitionStream { schema, coalescer });
            self.update_memory_reservation(Some(&hash_table))?;
            Ok(FinalHashAggregateState::ReadingInput { hash_table })
        } else {
            let input_schema = self.input.schema();
            self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
            self.update_memory_reservation(None)?;
            Ok(FinalHashAggregateState::Done)
        }
    }

    fn update_memory_reservation(
        &mut self,
        hash_table: Option<&AggregateHashTable<FinalMarker>>,
    ) -> Result<()> {
        let hash_table_size = hash_table
            .map(AggregateHashTable::memory_size)
            .or_else(|| {
                self.state
                    .as_ref()
                    .map(|state| state.hash_table().memory_size())
            })
            .unwrap_or(0);
        let partition_runs_size = self
            .partition_run_state
            .as_ref()
            .map(FinalPartitionRunState::total_size)
            .unwrap_or(0);
        self.reservation
            .try_resize(hash_table_size + partition_runs_size)
    }

    fn start_output(
        &mut self,
        hash_table: &mut AggregateHashTable<FinalMarker>,
    ) -> Result<()> {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
        hash_table.start_output()
    }

    // FinalPartitioned inputs arrive with an internal subpartition column. The
    // rows can contain multiple relative partitions in one output stream.
    //
    // Original input stream:
    //
    //   batch A rows: [ p0 ][ p1 ]       hidden column: [p0, ..., p1, ...]
    //   batch B rows: [ p0 ][ p1 ]       hidden column: [p0, ..., p1, ...]
    //
    // Copy rows into one coalescer per relative aggregate partition while
    // reading the original stream:
    //
    //   FinalPartitionRunState.coalescers
    //   +----+-----------------------------+
    //   | p0 | coalescer(A.p0, B.p0, ...)  |
    //   | p1 | coalescer(A.p1, B.p1, ...)  |
    //   +----+-----------------------------+
    //
    // Replay after the original stream ends:
    //
    //   p0 coalescer batches -> fresh final hash table -> output p0 groups
    //   p1 coalescer batches -> fresh final hash table -> output p1 groups
    //
    // This keeps equal group keys from different relative aggregate partitions from
    // being merged together, while still merging the same group key within one
    // relative aggregate partition.
    //
    // State transitions:
    //
    //   Reading original input
    //     + hidden column    -> buffer rows, keep ReadingInput
    //     + no hidden column -> normal aggregate_batch path
    //     + input done  -> begin_partition_run_replay(...)
    //
    //   Reading CoalescedPartitionStream(pN)
    //     + input done  -> finish_partition_run_replay(...) -> ProducingOutput(pN)
    //
    //   ProducingOutput(pN)
    //     + output done -> load_next_partition_run(fresh table) or Done
    //
    /// Handle ReadingInput state - aggregate partial state batches into the hash table.
    ///
    /// See comments at `poll_next()` for details.
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

                if self.should_buffer_partition_runs() {
                    if original_state.hash_table().building_group_count() != 0 {
                        timer.done();
                        return ControlFlow::Break((
                            Poll::Ready(Some(internal_err!(
                                "cannot switch final partitioned aggregation to buffered runs after groups have already been accumulated"
                            ))),
                            original_state,
                        ));
                    }

                    match self.stage_partition_runs(&batch) {
                        Ok(Some(batch_memory)) => {
                            let result = self.reserve_staged_partition_runs(
                                batch_memory,
                                original_state.hash_table(),
                            );
                            timer.done();
                            if let Err(e) = result {
                                return ControlFlow::Break((
                                    Poll::Ready(Some(Err(e))),
                                    original_state,
                                ));
                            }
                            return ControlFlow::Continue(original_state);
                        }
                        Ok(None) => {}
                        Err(e) => {
                            timer.done();
                            return ControlFlow::Break((
                                Poll::Ready(Some(Err(e))),
                                original_state,
                            ));
                        }
                    }
                }

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

                if let Err(e) =
                    self.update_memory_reservation(Some(original_state.hash_table()))
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
                let result = match self.partition_run_state.as_ref() {
                    Some(state) if state.is_draining => {
                        self.finish_partition_run_replay(original_state.into_hash_table())
                    }
                    Some(state) if state.has_buffered_partitions() => {
                        self.begin_partition_run_replay(original_state.into_hash_table())
                    }
                    _ => self
                        .start_output(original_state.hash_table_mut())
                        .map(|()| original_state.into_producing_output()),
                };
                timer.done();

                match result {
                    Ok(next_state) => ControlFlow::Continue(next_state),
                    Err(e) => ControlFlow::Break((
                        Poll::Ready(Some(Err(e))),
                        FinalHashAggregateState::Done,
                    )),
                }
            }
        }
    }

    /// Handle ProducingOutput state - emit final aggregate value batches.
    ///
    /// See comments at `poll_next()` for details.
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
                debug_assert!(batch.num_rows() > 0);
                let next_state = if original_state.hash_table().is_done()
                    || original_state.hash_table().is_building()
                {
                    match self.next_state_after_partition_output(
                        original_state.into_hash_table(),
                    ) {
                        Ok(next_state) => next_state,
                        Err(e) => {
                            return ControlFlow::Break((
                                Poll::Ready(Some(Err(e))),
                                FinalHashAggregateState::Done,
                            ));
                        }
                    }
                } else {
                    let _ =
                        self.update_memory_reservation(Some(original_state.hash_table()));
                    original_state
                };

                ControlFlow::Break((
                    Poll::Ready(Some(Ok(batch.record_output(&self.baseline_metrics)))),
                    next_state,
                ))
            }
            Ok(None) => {
                let _ = self.reservation.try_resize(0);
                ControlFlow::Continue(FinalHashAggregateState::Done)
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
    /// State transition graph:
    ///
    /// ```text
    /// (start)
    ///   -> ReadingInput
    ///      The stream starts by polling partial-state input and aggregating
    ///      those states into the final hash table.
    ///
    /// ReadingInput
    ///   -> ReadingInput
    ///      Aggregate one partial-state input batch, update the inner aggregate
    ///      hash table, and continue with the next input batch.
    ///
    ///   -> ProducingOutput
    ///      Input was exhausted, or the soft group limit was reached. Move to
    ///      the next state to start outputting final aggregate values.
    ///
    /// ProducingOutput
    ///   -> ProducingOutput
    ///      One final output batch was yielded; repeat to continue producing
    ///      output incrementally.
    ///
    ///   -> Done
    ///      All final output was emitted.
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::aggregates::{
        AggregateMode, PartitionRun, PhysicalGroupBy, append_subpartition_column,
        subpartition_schema,
    };
    use crate::execution_plan::ExecutionPlan;
    use crate::test::TestMemoryExec;

    use arrow::array::{ArrayRef, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;
    use futures::StreamExt;

    // Covers final partitioned hash aggregation replaying coalesced partition
    // runs one relative aggregate partition at a time.
    // Example: interleaved runs for partitions 0 and 1 are aggregated separately.
    #[tokio::test]
    async fn test_final_hash_stream_replays_partition_runs() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let make_batch = |groups: Vec<i32>, values: Vec<i64>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(groups)) as ArrayRef,
                    Arc::new(Int64Array::from(values)) as ArrayRef,
                ],
            )
        };

        let input_batches = vec![
            append_subpartition_column(
                &make_batch(vec![1, 2, 1], vec![2, 5, 11])?,
                &[PartitionRun::new(0, 2)?, PartitionRun::new(1, 1)?],
            )?,
            append_subpartition_column(
                &make_batch(vec![1, 3, 1, 3], vec![3, 7, 1, 9])?,
                &[PartitionRun::new(0, 2)?, PartitionRun::new(1, 2)?],
            )?,
        ];

        let input_schema = subpartition_schema(&schema);
        let input = TestMemoryExec::try_new_exec(&[input_batches], input_schema, None)?;
        let input =
            Arc::new(TestMemoryExec::update_cache(&input)) as Arc<dyn ExecutionPlan>;

        let group_by = PhysicalGroupBy::new_single(vec![(
            col("group_col", &schema)?,
            "group_col".to_string(),
        )]);
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(sum_udaf(), vec![col("value", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("SUM(value)")
                .build()?,
        )];
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            group_by,
            aggr_expr,
            vec![None],
            input,
            Arc::clone(&schema),
        )?;

        let task_ctx = Arc::new(TaskContext::default());
        let mut stream = FinalHashAggregateStream::new(&aggregate_exec, &task_ctx, 0)?;
        let mut actual = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let groups = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("group column should be Int32");
            let sums = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("sum column should be Int64");
            for row in 0..batch.num_rows() {
                actual.push((groups.value(row), sums.value(row)));
            }
        }
        actual.sort_unstable();

        assert_eq!(actual, vec![(1, 5), (1, 12), (2, 5), (3, 7), (3, 9)]);
        assert!(
            stream
                .partition_run_state
                .as_ref()
                .is_some_and(
                    |state| !state.has_buffered_partitions() && state.is_draining
                )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_hash_stream_double_emission_race_condition_bug() -> Result<()> {
        // Fix for https://github.com/apache/datafusion/issues/18701
        // This test specifically proves that we have fixed double emission race condition
        // where emit_early_if_necessary() and switch_to_skip_aggregation()
        // both emit in the same loop iteration, causing data loss

        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value_col", DataType::Int64, false),
        ]));

        // Create data that will trigger BOTH conditions in the same iteration:
        // 1. More groups than batch_size (triggers early emission when memory pressure hits)
        // 2. High cardinality ratio (triggers skip aggregation)
        let batch_size = 1024; // We'll set this in session config
        let num_groups = batch_size + 100; // Slightly more than batch_size (1124 groups)

        // Create exactly 1 row per group = 100% cardinality ratio
        let group_ids: Vec<i32> = (0..num_groups as i32).collect();
        let values: Vec<i64> = vec![1; num_groups];

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        let input_partitions = vec![vec![batch]];

        // Create constrained memory to trigger early emission but not completely fail
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(1024, 1.0) // small enough to start but will trigger pressure
            .build_arc()?;

        let mut task_ctx = TaskContext::default().with_runtime(runtime);

        // Configure to trigger BOTH conditions:
        // 1. Low probe threshold (triggers skip probe after few rows)
        // 2. Low ratio threshold (triggers skip aggregation immediately)
        // 3. Set batch_size to 1024 so our 1124 groups will trigger early emission
        // This creates the race condition where both emit paths are triggered
        let mut session_config = task_ctx.session_config().clone();
        session_config = session_config.set(
            "datafusion.execution.batch_size",
            &datafusion_common::ScalarValue::UInt64(Some(1024)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            &datafusion_common::ScalarValue::UInt64(Some(50)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &datafusion_common::ScalarValue::Float64(Some(0.8)),
        );
        task_ctx = task_ctx.with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);

        // Create aggregate: COUNT(*) GROUP BY group_col
        let group_expr = vec![(col("group_col", &schema)?, "group_col".to_string())];
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_value")
                .build()?,
        )];

        let exec = TestMemoryExec::try_new(&input_partitions, Arc::clone(&schema), None)?;
        let exec = Arc::new(TestMemoryExec::update_cache(&Arc::new(exec)));

        // Use Partial mode where the race condition occurs
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(group_expr),
            aggr_expr,
            vec![None],
            exec,
            Arc::clone(&schema),
        )?;

        // Execute and collect results
        let mut stream =
            PartialHashAggregateStream::new(&aggregate_exec, &Arc::clone(&task_ctx), 0)?;
        let mut results = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result?;
            results.push(batch);
        }

        // Count total groups emitted
        let mut total_output_groups = 0;
        for batch in &results {
            total_output_groups += batch.num_rows();
        }

        assert_eq!(
            total_output_groups, num_groups,
            "Unexpected number of groups",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_hash_stream_skip_aggregation_probe_not_locked_until_skip()
    -> Result<()> {
        // Test that the probe is not locked until we actually decide to skip.
        // This allows us to continue evaluating the skip condition across multiple batches.
        //
        // Scenario:
        // - Batch 1: Hits rows threshold but NOT ratio threshold (low cardinality) -> don't skip
        // - Batch 2: Now hits ratio threshold (high cardinality) -> skip
        //
        // Without the fix, the probe would be locked after batch 1, preventing the skip
        // decision from being made on batch 2.

        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value_col", DataType::Int32, false),
        ]));

        // Configure thresholds:
        // - probe_rows_threshold: 100 rows
        // - probe_ratio_threshold: 0.8 (80%)
        let probe_rows_threshold = 100;
        let probe_ratio_threshold = 0.8;

        // Batch 1: 100 rows with only 10 unique groups
        // Ratio: 10/100 = 0.1 (10%) < 0.8 -> should NOT skip
        // This will hit the rows threshold but not the ratio threshold
        let batch1_rows = 100;
        let batch1_groups = 10;
        let mut group_ids_batch1 = Vec::new();
        for i in 0..batch1_rows {
            group_ids_batch1.push((i % batch1_groups) as i32);
        }
        let values_batch1: Vec<i32> = vec![1; batch1_rows];

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch1)),
                Arc::new(Int32Array::from(values_batch1)),
            ],
        )?;

        // Batch 2: 360 rows with 360 unique NEW groups (starting from group 10)
        // After batch 2, total: 460 rows, 370 groups
        // Ratio: 370/460 is about 0.804 (80.4%) > 0.8 -> SHOULD decide to skip
        let batch2_rows = 360;
        let batch2_groups = 360;
        let group_ids_batch2: Vec<i32> = (batch1_groups..(batch1_groups + batch2_groups))
            .map(|x| x as i32)
            .collect();
        let values_batch2: Vec<i32> = vec![1; batch2_rows];

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch2)),
                Arc::new(Int32Array::from(values_batch2)),
            ],
        )?;

        // Batch 3: This batch should be skipped since we decided to skip after batch 2
        // 100 rows with 100 unique groups (continuing from where batch 2 left off)
        let batch3_rows = 100;
        let batch3_groups = 100;
        let batch3_start_group = batch1_groups + batch2_groups;
        let group_ids_batch3: Vec<i32> = (batch3_start_group
            ..(batch3_start_group + batch3_groups))
            .map(|x| x as i32)
            .collect();
        let values_batch3: Vec<i32> = vec![1; batch3_rows];

        let batch3 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch3)),
                Arc::new(Int32Array::from(values_batch3)),
            ],
        )?;

        let input_partitions = vec![vec![batch1, batch2, batch3]];

        let runtime = RuntimeEnvBuilder::default().build_arc()?;
        let mut task_ctx = TaskContext::default().with_runtime(runtime);

        // Configure skip aggregation settings
        let mut session_config = task_ctx.session_config().clone();
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            &datafusion_common::ScalarValue::UInt64(Some(probe_rows_threshold)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &datafusion_common::ScalarValue::Float64(Some(probe_ratio_threshold)),
        );
        task_ctx = task_ctx.with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);

        // Create aggregate: COUNT(*) GROUP BY group_col
        let group_expr = vec![(col("group_col", &schema)?, "group_col".to_string())];
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_value")
                .build()?,
        )];

        let exec = TestMemoryExec::try_new(&input_partitions, Arc::clone(&schema), None)?;
        let exec = Arc::new(TestMemoryExec::update_cache(&Arc::new(exec)));

        // Use Partial mode
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(group_expr),
            aggr_expr,
            vec![None],
            exec,
            Arc::clone(&schema),
        )?;

        // Execute and collect results
        let mut stream =
            PartialHashAggregateStream::new(&aggregate_exec, &Arc::clone(&task_ctx), 0)?;
        let mut results = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result?;
            results.push(batch);
        }

        // Check that skip aggregation actually happened.
        // The key metric is skipped_aggregation_rows.
        let metrics = aggregate_exec.metrics().unwrap();
        let skipped_rows = metrics
            .sum_by_name("skipped_aggregation_rows")
            .map(|m| m.as_usize())
            .unwrap_or(0);

        // We expect batch 3's rows to be skipped (100 rows)
        assert_eq!(
            skipped_rows, batch3_rows,
            "Expected batch 3's rows ({batch3_rows}) to be skipped",
        );

        Ok(())
    }
}
