// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Spill and replay support shared by the grouped aggregation streams.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, internal_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryLimit, MemoryReservation};
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::StreamExt;

use super::aggregate_hash_table::ClusteredAggregateTableMetrics;
use super::clustered_final_stream::ClusteredFinalAggregateStream;
use super::order::GroupClusteringMode;
use super::{AggregateExec, AggregateMode};
use crate::SendableRecordBatchStream;
use crate::metrics::{BaselineMetrics, SpillMetrics};
use crate::sorts::IncrementalSortIterator;
use crate::sorts::sort::spill_max_batch_bytes;
use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use crate::spill::gc_nested_arrays;
use crate::spill::in_progress_spill_file::WrittenBatchStats;
use crate::spill::spill_manager::{GetSlicedSize, SpillManager};
use crate::stream::RecordBatchStreamAdapter;

/// Target size in bytes of the batches that spilling writes and replay
/// merges. A batch can be larger when it holds a larger group.
///
/// Replay holds the groups of the batch it merges in memory, and merging
/// several spill files reserves memory for a few batches of each. A limit of
/// `batch_size` rows alone does not bound the size of a batch: a few groups
/// with large state, such as `array_agg` over a low-cardinality key, would fit
/// in a single batch.
const SPILL_BATCH_TARGET_BYTES: usize = 1024 * 1024;

/// Spill configuration and accumulated runs of one grouped aggregation stream.
///
/// Every aggregation stream that spills does so the same way. Each spill event
/// drains all currently buffered groups as intermediate state (see
/// `take_state_batch` on the aggregate tables), sorts them by the full group
/// key, and writes them to one spill file. After the original input ends, all
/// files are merged and replayed through a [`ClusteredFinalAggregateStream`],
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
    ///     - It reuses `ClusteredFinalAggregateStream` for processing.
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
    /// Minimum, across original runs, of each run's largest batch row count.
    min_spill_batch_rows: usize,
    /// Describes this stream's spill requests, and prefixes its internal errors.
    label: &'static str,
}

impl AggregateSpill {
    /// Creates the spill context of a stream, whose spill requests are described
    /// as `label`.
    ///
    /// `group_clustering_mode` determines which group columns are already
    /// contiguous. Spill files are sorted by those columns first, followed by
    /// the remaining ones. Existing output sort options are retained so replay
    /// preserves any advertised ordering as well as group clustering.
    /// Full group clustering aggregates in bounded memory and never spills.
    ///
    /// `spill_schema` is the schema of the intermediate state batches.
    #[expect(clippy::too_many_arguments)]
    pub(super) fn try_new(
        label: &'static str,
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        batch_size: usize,
        group_clustering_mode: &GroupClusteringMode,
        spill_schema: &SchemaRef,
        spill_metrics: SpillMetrics,
    ) -> Result<Self> {
        let mut replay_agg = agg.clone();
        replay_agg.group_clustering_mode = GroupClusteringMode::Full;
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
        let contiguous_indices: &[usize] = match group_clustering_mode {
            GroupClusteringMode::None => &[],
            GroupClusteringMode::Partial(contiguous_indices) => contiguous_indices,
            GroupClusteringMode::Full => {
                return internal_err!("{label}: fully contiguous groups do not spill");
            }
        };
        let spill_indices = contiguous_indices.iter().copied().chain(
            (0..num_group_columns).filter(|idx| !contiguous_indices.contains(idx)),
        );
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
        .with_compression_type(context.session_config().spill_compression())
        .with_max_batch_bytes(Some(configured_spill_max_batch_bytes(context)));

        Ok(Self {
            replay_agg,
            context: Arc::clone(context),
            partition,
            batch_size,
            spill_expr,
            spill_manager,
            spills: vec![],
            min_spill_batch_rows: batch_size,
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
        if self.spills.is_empty() {
            self.spill_manager = bound_spill_batches_by_first_spill(
                &self.spill_manager,
                &state_batch,
                memory_pool_limit(&self.context),
            )?;
        }

        let sorted_iter = IncrementalSortIterator::new(
            state_batch,
            self.spill_expr.clone(),
            self.batch_size,
        )
        .with_max_batch_bytes(SPILL_BATCH_TARGET_BYTES);
        let mut in_progress_file =
            self.spill_manager.create_in_progress_file(self.label)?;
        let mut written = WrittenBatchStats::default();
        for batch in sorted_iter {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let stats = in_progress_file.append_batch_with_stats(&batch)?;
            written.memory_size = written.memory_size.max(stats.memory_size);
            written.rows = written.rows.max(stats.rows);
        }
        let Some(file) = in_progress_file.finish()? else {
            return internal_err!("{}: produced an empty spill", self.label);
        };
        let (max_record_batch_memory, max_batch_rows) =
            (written.memory_size, written.rows);

        self.spills.push(SortedSpillFile {
            file,
            max_record_batch_memory,
        });
        self.min_spill_batch_rows = self.min_spill_batch_rows.min(max_batch_rows);

        Ok(())
    }

    /// Writes already evaluated output batches to one spill file and returns a
    /// stream that reads them back.
    pub(super) fn spill_output(
        &self,
        schema: SchemaRef,
        batches: impl Iterator<Item = Result<RecordBatch>>,
    ) -> Result<SendableRecordBatchStream> {
        let spill_manager = SpillManager::new(
            self.context.runtime_env(),
            self.spill_manager.metrics.clone(),
            schema,
        )
        .with_compression_type(self.spill_manager.compression);
        let spill_file = spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(batches, self.label)?;
        let Some((file, max_record_batch_memory)) = spill_file else {
            return internal_err!("{}: produced an empty output spill", self.label);
        };
        spill_manager.read_spill_as_stream(file, Some(max_record_batch_memory))
    }

    /// Merges every sorted run, and does the aggregate evaluation with
    /// [`ClusteredFinalAggregateStream`].
    pub(super) fn into_replay_stream(
        self,
        baseline_metrics: &BaselineMetrics,
        metrics: ClusteredAggregateTableMetrics,
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
            min_spill_batch_rows,
            label: _,
        } = self;

        let spill_schema = Arc::clone(spill_manager.schema());
        let max_batch_bytes = spill_manager.max_batch_bytes();
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
            .with_max_batch_bytes(max_batch_bytes)
            .with_reservation(merge_reservation)
            .with_replay_headroom()
            .with_intermediate_merge_sizing(Some(min_spill_batch_rows))
            .build()?;
        // A merged batch references the whole data buffers of the spilled
        // batches its rows come from, and can hold all their dictionary
        // values. Replay keeps the list states of a group, such as those of
        // `array_agg`, until the group completes, so compact them to hold only
        // the bytes of their own rows.
        let merged: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            merged.schema(),
            merged.map(|batch| gc_nested_arrays(&batch?)),
        ));
        let replay = ClusteredFinalAggregateStream::new_with_input_and_metrics(
            &replay_agg,
            &context,
            partition,
            merged,
            &GroupClusteringMode::Full,
            baseline_metrics.clone(),
            metrics,
            None,
            reservation,
        )?;
        Ok(replay.into_stream())
    }
}

/// The bound on an aggregate's spill batches before its first spill: the 1 MiB target
/// of the replay, or less when `sort_spill_reservation_bytes` fits only smaller batches.
pub(super) fn configured_spill_max_batch_bytes(context: &TaskContext) -> usize {
    let reservation = context
        .session_config()
        .options()
        .execution
        .sort_spill_reservation_bytes;
    SPILL_BATCH_TARGET_BYTES.min(spill_max_batch_bytes(reservation))
}

/// The memory limit of the pool of `context`, if it has one. See
/// [`bound_spill_batches_by_first_spill`].
pub(super) fn memory_pool_limit(context: &TaskContext) -> Option<usize> {
    match context.memory_pool().memory_limit() {
        MemoryLimit::Finite(limit) => Some(limit),
        MemoryLimit::Infinite | MemoryLimit::Unknown => None,
    }
}

/// Lowers the bound on the batches of an aggregate's sorted spill runs (see
/// [`SpillManager::with_max_batch_bytes`]) to fit the state of its first spill.
///
/// Unlike the sort, the merge of an aggregate's runs holds no workspace of its
/// own. The batches are bounded so that the smallest merge, plus as much again
/// for the replay, fits in half of what the aggregate held when memory first ran
/// out. The other half is slack for the replay's group table, and lets a merge
/// read more than the smallest number of runs at once.
///
/// The aggregate checks its memory after it aggregates an input batch, so the first
/// spill holds at least one input batch of state, which can be larger than the pool.
/// What it held is therefore capped by `memory_limit`, the limit of the pool.
/// Later spills, such as the remainder spilled at the end of the input, can
/// hold much less, so they do not lower the bound.
///
/// The partitions of a query share the pool, and each derives its bound from its own
/// first spill. Their bounds together can exceed the pool.
pub(super) fn bound_spill_batches_by_first_spill(
    spill_manager: &SpillManager,
    first_spill: &RecordBatch,
    memory_limit: Option<usize>,
) -> Result<SpillManager> {
    let held = first_spill
        .get_sliced_size()?
        .min(memory_limit.unwrap_or(usize::MAX));
    // `spill_max_batch_bytes(h)` fits the smallest merge in `h`.
    let bound = spill_max_batch_bytes(held / 4)
        .min(spill_manager.max_batch_bytes().unwrap_or(usize::MAX));
    Ok(spill_manager.clone().with_max_batch_bytes(Some(bound)))
}
