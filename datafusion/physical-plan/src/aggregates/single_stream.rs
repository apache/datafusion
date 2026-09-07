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

//! Single-stage hash aggregation stream implementation.
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
use datafusion_common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::stream::{Stream, StreamExt};

use super::aggregate_hash_table::{
    AggregateHashTable, OrderedAggregateTableMetrics, SingleMarker,
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

/// Hash aggregation can run the full logical aggregation in one operator. This
/// stream implements the single stage for grouped hash aggregation.
///
/// This aggregation variant is useful when:
/// - There is only one partition (config `target_partitions` is set to 1)
/// - When input is already partitioned (`t` is backed by Parquet files, that is range/hash
///   partitioned on the group keys), the single aggregation mode is the most efficient
///   approach to use.
///
/// # Example
///
/// SELECT k, AVG(v) FROM t GROUP BY k;
///
/// ## Plan
/// AggregateExec(stage=single)
/// -- DataSourceExec(t)
///
/// ## Single Stage Behavior
/// Input: raw rows
/// Output: final aggregate values for all groups (for example, `AVG(x)`)
///
/// This stream implements the complete aggregation without a partial/final
/// split. It consumes raw input rows and emits final aggregate values.
///
/// # Grouping Sets
///
/// `GROUPING SETS`, `CUBE` and `ROLLUP` are expanded while consuming raw input:
/// every grouping set of an input batch is evaluated and interned into the same
/// hash table, the same way [`super::hash_stream::PartialHashAggregateStream`]
/// does it. When spilling, the expanded keys are sorted and replayed as a plain
/// group by.
///
/// # Spilling
///
/// During aggregation, group keys and states accumulate. If memory usage exceeds
/// the budget, spilling is triggered as follows:
/// 1. After aggregating a new input batch, if the memory reservation exceeds its
///    limit, spill all accumulated groups and states.
///    - Sort all groups by the group keys before spilling.
/// 2. Repeat until the input is exhausted.
/// 3. Perform a sort-preserving merge of all spill files and feed the merged output
///    into an ordered streaming aggregation, which ensures bounded memory usage and
///    evaluates the final result.
///    - [`OrderedFinalAggregateStream`] is reused for the streaming aggregation.
pub(crate) struct SingleHashAggregateStream {
    /// Output schema: group columns followed by final aggregate value columns.
    schema: SchemaRef,

    /// Input batches containing raw rows, not partial aggregate state.
    input: SendableRecordBatchStream,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys, accumulators, and spill sorting.
    reservation: MemoryReservation,

    /// Tracks the high-level stream lifecycle. The hash table owns the lower-level
    /// state for emitting output batches.
    state: Option<SingleHashAggregateState>,
}

/// Spill configuration and accumulated runs for single hash aggregation.
///
/// Each spill event drains all currently buffered groups, sorts their intermediate
/// states by the full group key, and writes them to one spill file. All files are
/// merged and replayed after the original input ends.
struct SingleSpillContext {
    /// Aggregate configuration used to construct the final replay stream.
    ///
    /// Spilled rows already contain evaluated group keys and intermediate
    /// aggregate states. Replay must therefore use final aggregation semantics
    /// and column-based group expressions rather than evaluating the raw input
    /// expressions a second time. After the spill files are merged into ordered
    /// input, this configuration is used to construct an
    /// [`OrderedFinalAggregateStream`], and perform the final evaluation step.
    final_agg: AggregateExec,
    /// Task context.
    context: Arc<TaskContext>,
    /// Original partition index.
    partition: usize,
    /// Target batch size from configuration.
    batch_size: usize,
    /// Full group-key ordering kept by every spill file and the merged input.
    spill_expr: LexOrdering,
    /// Spill I/O and metrics manager.
    spill_manager: SpillManager,
    /// Spill runs waiting to be merged, they're all sorted by full group-by keys.
    spills: Vec<SortedSpillFile>,
}

/// See comments at `poll_next()` for details.
enum SingleHashAggregateState {
    ReadingInput {
        hash_table: AggregateHashTable<SingleMarker>,
        spill_context: Option<Box<SingleSpillContext>>,
    },
    Spilling {
        hash_table: AggregateHashTable<SingleMarker>,
        spill_context: Box<SingleSpillContext>,
    },
    ProducingOutput {
        hash_table: AggregateHashTable<SingleMarker>,
    },
    PreparingMergeInput {
        hash_table: AggregateHashTable<SingleMarker>,
        spill_context: Box<SingleSpillContext>,
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

type SingleHashAggregatePoll = Poll<Option<Result<RecordBatch>>>;
type SingleHashAggregateStateTransition = ControlFlow<
    (SingleHashAggregatePoll, SingleHashAggregateState),
    SingleHashAggregateState,
>;

impl SingleSpillContext {
    fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        batch_size: usize,
        spill_schema: &SchemaRef,
        spill_metrics: SpillMetrics,
    ) -> Result<Self> {
        let group_schema = agg.group_by.group_schema(&agg.input().schema())?;
        let output_ordering = agg.cache.output_ordering();
        let spill_sort_exprs =
            group_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(idx, field)| {
                    let output_expr = Column::new(field.name(), idx);
                    let sort_options = output_ordering
                        .and_then(|ordering| ordering.get_sort_options(&output_expr))
                        .unwrap_or_default();
                    PhysicalSortExpr::new(Arc::new(output_expr), sort_options)
                });
        let Some(spill_expr) = LexOrdering::new(spill_sort_exprs) else {
            return internal_err!("Single hash aggregate spill expression is empty");
        };

        let spill_manager = SpillManager::new(
            context.runtime_env(),
            spill_metrics,
            Arc::clone(spill_schema),
        )
        .with_compression_type(context.session_config().spill_compression());

        // See `SingleSpillContext::final_agg` comments for `final_agg`'s usage
        let mut final_agg = agg.clone();
        final_agg.mode = match agg.mode {
            AggregateMode::Single => AggregateMode::Final,
            AggregateMode::SinglePartitioned => AggregateMode::FinalPartitioned,
            mode => {
                return internal_err!(
                    "Single hash aggregate spill cannot replay aggregate mode {mode:?}"
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
    /// See [`SingleHashAggregateStream`] for spilling details.
    fn spill_table(
        &mut self,
        hash_table: &mut AggregateHashTable<SingleMarker>,
    ) -> Result<()> {
        let Some(batch) = hash_table.take_state_batch()? else {
            return Ok(());
        };

        let sorted_iter =
            IncrementalSortIterator::new(batch, self.spill_expr.clone(), self.batch_size);
        let spill_file = self
            .spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                sorted_iter,
                "SingleHashAggregateSpill",
            )?;

        let Some((file, max_record_batch_memory)) = spill_file else {
            return internal_err!("Single hash aggregation produced an empty spill");
        };

        self.spills.push(SortedSpillFile {
            file,
            max_record_batch_memory,
        });

        Ok(())
    }

    /// Merges every sorted run, and do the aggregate evaluation with
    /// [`OrderedFinalAggregateStream`]
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

impl SingleHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert!(matches!(
            agg.mode,
            AggregateMode::Single | AggregateMode::SinglePartitioned
        ));
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

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

        let hash_table = AggregateHashTable::<SingleMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            Arc::clone(&state_schema),
            batch_size,
        )?;

        let can_spill = context.runtime_env().disk_manager.tmp_files_enabled();
        let spill_context = if can_spill {
            Some(Box::new(SingleSpillContext::new(
                agg,
                context,
                partition,
                batch_size,
                &state_schema,
                spill_metrics,
            )?))
        } else {
            None
        };

        let reservation =
            MemoryConsumer::new(format!("SingleHashAggregateStream[{partition}]"))
                .with_can_spill(can_spill)
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            baseline_metrics,
            reservation,
            state: Some(SingleHashAggregateState::ReadingInput {
                hash_table,
                spill_context,
            }),
        })
    }

    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
    }

    fn break_with_err(error: DataFusionError) -> SingleHashAggregateStateTransition {
        ControlFlow::Break((
            Poll::Ready(Some(Err(error))),
            SingleHashAggregateState::Error,
        ))
    }

    fn break_with_internal_err(message: &str) -> SingleHashAggregateStateTransition {
        Self::break_with_err(internal_datafusion_err!("{message}"))
    }

    /// Reserve memory for the current aggregate table.
    fn reservation_size_for_table(
        hash_table: &AggregateHashTable<SingleMarker>,
        spill_context: Option<&SingleSpillContext>,
    ) -> usize {
        let table_size = hash_table.memory_size();
        if spill_context.is_some() {
            // See `SingleHashAggregateStream` comments for how this is estimated.
            table_size.saturating_add(
                hash_table
                    .building_group_count()
                    .saturating_mul(size_of::<u32>()),
            )
        } else {
            table_size
        }
    }

    /// Consumes one raw input batch and updates the single-stage hash table.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_reading_input(
        &mut self,
        cx: &mut Context<'_>,
        original_state: SingleHashAggregateState,
    ) -> SingleHashAggregateStateTransition {
        let SingleHashAggregateState::ReadingInput {
            mut hash_table,
            spill_context,
        } = original_state
        else {
            return Self::break_with_internal_err(
                "Single hash aggregate stream expected ReadingInput state",
            );
        };

        match self.input.poll_next_unpin(cx) {
            Poll::Pending => ControlFlow::Break((
                Poll::Pending,
                SingleHashAggregateState::ReadingInput {
                    hash_table,
                    spill_context,
                },
            )),
            Poll::Ready(Some(Ok(batch))) => {
                let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
                let timer = elapsed_compute.timer();
                let result = hash_table.aggregate_batch(&batch);
                timer.done();

                if let Err(e) = result {
                    return Self::break_with_err(e);
                }

                // Check memory reservation, and potentially spill.
                let timer = elapsed_compute.timer();
                let resize_result =
                    self.reservation
                        .try_resize(Self::reservation_size_for_table(
                            &hash_table,
                            spill_context.as_deref(),
                        ));
                timer.done();
                match resize_result {
                    Ok(()) => {}
                    Err(e @ DataFusionError::ResourcesExhausted(_)) => {
                        let Some(spill_context) = spill_context else {
                            return Self::break_with_err(e.context(
                                "Single hash aggregate cannot spill because temporary files are not enabled in the DiskManager",
                            ));
                        };
                        if hash_table.building_group_count() == 0 {
                            return Self::break_with_internal_err(
                                "Single hash aggregate ran out of memory with no aggregated groups",
                            );
                        }
                        return ControlFlow::Continue(
                            SingleHashAggregateState::Spilling {
                                hash_table,
                                spill_context,
                            },
                        );
                    }
                    Err(e) => {
                        return Self::break_with_err(e);
                    }
                }

                ControlFlow::Continue(SingleHashAggregateState::ReadingInput {
                    hash_table,
                    spill_context,
                })
            }
            Poll::Ready(Some(Err(e))) => Self::break_with_err(e),
            Poll::Ready(None) => {
                self.close_input();
                match spill_context {
                    Some(spill_context) if spill_context.has_spills() => {
                        ControlFlow::Continue(
                            SingleHashAggregateState::PreparingMergeInput {
                                hash_table,
                                spill_context,
                            },
                        )
                    }
                    _ => {
                        let elapsed_compute =
                            self.baseline_metrics.elapsed_compute().clone();
                        let timer = elapsed_compute.timer();
                        let result = hash_table.start_output();
                        timer.done();

                        match result {
                            Ok(()) => ControlFlow::Continue(
                                SingleHashAggregateState::ProducingOutput { hash_table },
                            ),
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
        original_state: SingleHashAggregateState,
    ) -> SingleHashAggregateStateTransition {
        let SingleHashAggregateState::Spilling {
            mut hash_table,
            mut spill_context,
        } = original_state
        else {
            return Self::break_with_internal_err(
                "Single hash aggregate stream expected Spilling state",
            );
        };

        // Sanity check: it is impossible to OOM when the table is empty.
        if hash_table.building_group_count() == 0 {
            return Self::break_with_internal_err(
                "Single hash aggregation entered Spilling with an empty table",
            );
        }

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let mut result = spill_context.spill_table(&mut hash_table);

        // Spilling shrinks the aggregate table and releases its accumulated
        // memory. Update the reservation accordingly.
        if let Err(e) = self.reservation.try_resize(hash_table.memory_size()) {
            result =
                Err(e.context("Decreasing allocation after spilling should succeed"));
        }

        timer.done();

        match result {
            // Finished spilling the aggregate table, continue aggregating from input.
            Ok(()) => ControlFlow::Continue(SingleHashAggregateState::ReadingInput {
                hash_table,
                spill_context: Some(spill_context),
            }),
            Err(e) => Self::break_with_err(e),
        }
    }

    /// 1. Spills the last in-memory run.
    /// 2. Constructs a globally ordered input stream by applying a sort-preserving
    ///    merge to all spills.
    /// 3. Constructs a replay stream: an ordered final aggregate stream over the
    ///    fully ordered input constructed from the spills.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_preparing_merge_input(
        &mut self,
        original_state: SingleHashAggregateState,
    ) -> SingleHashAggregateStateTransition {
        let SingleHashAggregateState::PreparingMergeInput {
            mut hash_table,
            mut spill_context,
        } = original_state
        else {
            return Self::break_with_internal_err(
                "Single hash aggregate stream expected PreparingMergeInput state",
            );
        };

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let replay = match spill_context.spill_table(&mut hash_table) {
            Ok(()) => {
                let metrics = OrderedAggregateTableMetrics::from_hash_table(&hash_table);
                drop(hash_table);
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
                ControlFlow::Continue(SingleHashAggregateState::MergingSpills { stream })
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
        original_state: SingleHashAggregateState,
    ) -> SingleHashAggregateStateTransition {
        let SingleHashAggregateState::MergingSpills { mut stream } = original_state
        else {
            return Self::break_with_internal_err(
                "Single hash aggregate stream expected MergingSpills state",
            );
        };

        match stream.poll_next_unpin(cx) {
            Poll::Pending => ControlFlow::Break((
                Poll::Pending,
                SingleHashAggregateState::MergingSpills { stream },
            )),
            Poll::Ready(Some(Ok(batch))) => ControlFlow::Break((
                Poll::Ready(Some(Ok(batch))),
                SingleHashAggregateState::MergingSpills { stream },
            )),
            Poll::Ready(Some(Err(e))) => Self::break_with_err(e),
            Poll::Ready(None) => ControlFlow::Continue(SingleHashAggregateState::Done),
        }
    }

    /// Emits one batch after input is exhausted.
    ///
    /// See comments at `poll_next()` for details.
    ///
    /// Returns the next operator state with control flow decision.
    fn handle_producing_output(
        &mut self,
        original_state: SingleHashAggregateState,
    ) -> SingleHashAggregateStateTransition {
        let SingleHashAggregateState::ProducingOutput { mut hash_table } = original_state
        else {
            return Self::break_with_internal_err(
                "Single hash aggregate stream expected ProducingOutput state",
            );
        };

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let result = hash_table.next_output_batch();
        timer.done();

        match result {
            Ok(Some(batch)) => {
                let next_state = if hash_table.is_done() {
                    drop(hash_table);
                    if let Err(e) = self.reservation.try_resize(0) {
                        return Self::break_with_err(e);
                    }
                    SingleHashAggregateState::Done
                } else {
                    if let Err(e) = self.reservation.try_resize(hash_table.memory_size())
                    {
                        return Self::break_with_err(e);
                    }
                    SingleHashAggregateState::ProducingOutput { hash_table }
                };

                ControlFlow::Break((
                    Poll::Ready(Some(Ok(batch.record_output(&self.baseline_metrics)))),
                    next_state,
                ))
            }
            Err(e) => Self::break_with_err(e),
            Ok(None) => {
                drop(hash_table);
                let next_state = SingleHashAggregateState::Done;
                if let Err(e) = self.reservation.try_resize(0) {
                    return Self::break_with_err(e);
                }
                ControlFlow::Continue(next_state)
            }
        }
    }
}

impl Stream for SingleHashAggregateStream {
    type Item = Result<RecordBatch>;

    /// Entry point for the single hash aggregate state machine.
    ///
    /// See comments in [`SingleHashAggregateStream`] for high-level ideas.
    ///
    /// State transition graph:
    ///
    /// ```text
    /// (start)
    ///   -> ReadingInput
    ///      The stream starts by polling raw input rows and aggregating those
    ///      rows into the single-stage hash table.
    ///
    /// ReadingInput
    ///   -> ReadingInput
    ///      Aggregate one raw input batch. If it fits in memory, continue with
    ///      the next input batch.
    ///   -> Spilling
    ///      The table cannot reserve enough memory. Move all current states into
    ///      one fully group-key-sorted spill run.
    ///   -> ProducingOutput
    ///      Input was exhausted without spilling. Start outputting final values.
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
    ///      One final output batch was yielded; repeat to continue producing
    ///      output incrementally.
    ///   -> Done
    ///      All final output was emitted.
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
                .expect("SingleHashAggregateStream state should not be None");

            let next_state = match cur_state {
                state @ SingleHashAggregateState::ReadingInput { .. } => {
                    self.handle_reading_input(cx, state)
                }
                state @ SingleHashAggregateState::Spilling { .. } => {
                    self.handle_spilling(state)
                }
                state @ SingleHashAggregateState::PreparingMergeInput { .. } => {
                    self.handle_preparing_merge_input(state)
                }
                state @ SingleHashAggregateState::MergingSpills { .. } => {
                    self.handle_merging_spills(cx, state)
                }
                state @ SingleHashAggregateState::ProducingOutput { .. } => {
                    self.handle_producing_output(state)
                }
                state @ SingleHashAggregateState::Error => {
                    self.close_input();
                    self.reservation.free();
                    self.state = Some(state);
                    return Poll::Ready(None);
                }
                state @ SingleHashAggregateState::Done => {
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
                    debug_assert!(matches!(next_state, SingleHashAggregateState::Error));

                    // The handler has already discarded its state-owned resources.
                    // Release the remaining stream-owned resources before returning.
                    self.close_input();
                    self.reservation.free();
                    self.state = Some(SingleHashAggregateState::Error);
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

impl RecordBatchStream for SingleHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
