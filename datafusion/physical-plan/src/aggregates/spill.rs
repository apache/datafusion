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

//! Spill and replay support shared by the grouped aggregation streams.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, internal_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::LexOrdering;

use super::aggregate_hash_table::OrderedAggregateTableMetrics;
use super::ordered_final_stream::OrderedFinalAggregateStream;
use super::{AggregateExec, AggregateMode};
use crate::metrics::{BaselineMetrics, SpillMetrics};
use crate::sorts::IncrementalSortIterator;
use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use crate::spill::spill_manager::SpillManager;
use crate::{InputOrderMode, SendableRecordBatchStream};

/// Spill configuration and accumulated runs of one grouped aggregation stream.
///
/// Every aggregation stream that spills does so the same way. Each spill event
/// drains all currently buffered groups as intermediate state (see
/// `take_state_batch` on the aggregate tables), sorts them by the full group
/// key, and writes them to one spill file. After the original input ends, all
/// files are merged and replayed through an [`OrderedFinalAggregateStream`],
/// which merges the states and evaluates the final aggregate values.
pub(super) struct AggregateSpill {
    /// Aggregate configuration used to construct the replay stream.
    ///
    /// Spilled rows already contain evaluated group keys and intermediate
    /// aggregate states. Replay must therefore use final aggregation semantics
    /// and column-based group expressions rather than evaluating the raw input
    /// expressions a second time, so single-stage aggregates are rewritten to
    /// their final counterpart here.
    ///
    /// # Example walkthrough
    ///
    /// This example walks through two key APIs of [`AggregateSpill`]:
    /// - [`AggregateSpill::sort_and_spill`]
    /// - [`AggregateSpill::into_replay_stream`]
    ///
    /// ```txt
    /// SELECT k, SUM(v) FROM t GROUP BY k
    ///
    /// --------------------
    /// Step 1: OOM round 1
    /// --------------------
    ///
    /// First OOM: sort by k and write spill file 1 using `AggregateSpill::sort_and_spill`.
    ///
    /// Buffered batch        Spill file 1 (sorted)
    /// k  partial_sum        k  partial_sum
    /// 1            3        1            3
    /// 3            4   ->   2            5
    /// 2            5        3            4
    ///
    /// --------------------
    /// Step 2: OOM round 2
    /// --------------------
    /// After more input, a second OOM occurs: sort and spill similarly.
    ///
    /// Buffered batch        Spill file 2 (sorted)
    /// k  partial_sum        k  partial_sum
    /// 3            6   ->   1            2
    /// 1            2        3            6
    ///
    /// ------------------------------------------
    /// Step 3: Global sort and final aggregation
    /// ------------------------------------------
    /// 1. Construct a globally sorted aggregate stream via `SortPreservingMergeStream`
    ///    using the two previously sorted spill files.
    /// 2. Build a final aggregation stream:
    ///     - The input is the SPM stream.
    ///     - It reuses `OrderedFinalAggregateStream` for processing.
    ///     - It returns the final aggregation result directly.
    ///
    /// SPM output            Final aggregate output
    /// k  partial_sum        k  SUM(v)
    /// 1            3        1       5
    /// 1            2   ->   2       5
    /// 2            5        3      10
    /// 3            4
    /// 3            6
    /// ```
    replay_agg: AggregateExec,
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
    /// Spill runs waiting to be merged, all sorted by `spill_expr`.
    spills: Vec<SortedSpillFile>,
    /// Describes this stream's spill requests, and prefixes its internal errors.
    label: &'static str,
}

impl AggregateSpill {
    /// Creates the spill context of a stream, whose spill requests are described
    /// as `label`.
    ///
    /// `input_order_mode` is the order of the stream's input: spill files are
    /// sorted by the already ordered group columns first, followed by the
    /// remaining ones, so that replay keeps the ordering the stream promised.
    /// Fully sorted input aggregates in bounded memory and never spills.
    ///
    /// `spill_schema` is the schema of the intermediate state batches.
    #[expect(clippy::too_many_arguments)]
    pub(super) fn try_new(
        label: &'static str,
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        batch_size: usize,
        input_order_mode: &InputOrderMode,
        spill_schema: &SchemaRef,
        spill_metrics: SpillMetrics,
    ) -> Result<Self> {
        let mut replay_agg = agg.clone();
        replay_agg.input_order_mode = InputOrderMode::Sorted;
        let group_schema = match agg.mode {
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                agg.group_by().group_schema(spill_schema)?
            }
            AggregateMode::Single | AggregateMode::SinglePartitioned => {
                replay_agg.mode = if agg.mode == AggregateMode::Single {
                    AggregateMode::Final
                } else {
                    AggregateMode::FinalPartitioned
                };
                *replay_agg.group_by_mut() = Arc::new(agg.group_by().as_final());
                agg.group_by().group_schema(&agg.input().schema())?
            }
            mode => {
                return internal_err!("{label}: cannot replay aggregate mode {mode:?}");
            }
        };

        let num_group_columns = group_schema.fields().len();
        let ordered_indices: &[usize] = match input_order_mode {
            InputOrderMode::Linear => &[],
            InputOrderMode::PartiallySorted(ordered_indices) => ordered_indices,
            InputOrderMode::Sorted => {
                return internal_err!("{label}: fully ordered input does not spill");
            }
        };
        let spill_indices = ordered_indices
            .iter()
            .copied()
            .chain((0..num_group_columns).filter(|idx| !ordered_indices.contains(idx)));
        let output_ordering = agg.cache.output_ordering();
        let spill_sort_exprs = spill_indices.map(|idx| {
            let output_expr = Column::new(group_schema.field(idx).name(), idx);
            let sort_options = output_ordering
                .and_then(|ordering| ordering.get_sort_options(&output_expr))
                .unwrap_or_default();
            PhysicalSortExpr::new(Arc::new(output_expr), sort_options)
        });
        let Some(spill_expr) = LexOrdering::new(spill_sort_exprs) else {
            return internal_err!("{label}: spill expression is empty");
        };

        let spill_manager = SpillManager::new(
            context.runtime_env(),
            spill_metrics,
            Arc::clone(spill_schema),
        )
        .with_compression_type(context.session_config().spill_compression());

        Ok(Self {
            replay_agg,
            context: Arc::clone(context),
            partition,
            batch_size,
            spill_expr,
            spill_manager,
            spills: vec![],
            label,
        })
    }

    pub(super) fn has_spills(&self) -> bool {
        !self.spills.is_empty()
    }

    /// Sorts `state_batch`, the intermediate state of all currently buffered
    /// groups (`None` if there are no groups), and writes it as one spill file.
    /// Memory reservation should be updated by the caller.
    pub(super) fn sort_and_spill(
        &mut self,
        state_batch: Option<RecordBatch>,
    ) -> Result<()> {
        let Some(state_batch) = state_batch else {
            return Ok(());
        };

        let sorted_iter = IncrementalSortIterator::new(
            state_batch,
            self.spill_expr.clone(),
            self.batch_size,
        );
        let spill_file = self
            .spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                sorted_iter,
                self.label,
            )?;

        let Some((file, max_record_batch_memory)) = spill_file else {
            return internal_err!("{}: produced an empty spill", self.label);
        };

        self.spills.push(SortedSpillFile {
            file,
            max_record_batch_memory,
        });

        Ok(())
    }

    /// Merges every sorted run, and does the aggregate evaluation with
    /// [`OrderedFinalAggregateStream`].
    pub(super) fn into_replay_stream(
        self,
        baseline_metrics: &BaselineMetrics,
        metrics: OrderedAggregateTableMetrics,
        reservation: MemoryReservation,
    ) -> Result<SendableRecordBatchStream> {
        let Self {
            replay_agg,
            context,
            partition,
            batch_size,
            spill_expr,
            spill_manager,
            spills,
            label: _,
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
            .with_replay_headroom()
            .build()?;
        let replay = OrderedFinalAggregateStream::new_with_input_and_metrics(
            &replay_agg,
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
