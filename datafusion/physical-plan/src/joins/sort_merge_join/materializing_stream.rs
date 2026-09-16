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

//! Sort-Merge Join execution
//!
//! This module implements the Sort-Merge Join operator as an async
//! generator running a merge scan: it drives two sorted input streams (the
//! *streamed* side and the *buffered* side), compares join keys, and
//! produces joined `RecordBatch`es.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::mem::size_of;
use std::ops::Range;
use std::sync::Arc;

use crate::joins::sort_merge_join::filter::{
    FilterMetadata, filter_record_batch_by_join_type, get_corrected_filter_mask,
    get_filter_columns, needs_deferred_filtering,
};
use crate::joins::sort_merge_join::metrics::SortMergeJoinMetrics;
use crate::joins::utils::{JoinFilter, JoinKeyComparator};
use crate::metrics::Time;
use crate::spill::spill_manager::SpillManager;
use crate::stream::{EmptyRecordBatchStream, ObservedStream, RecordBatchStreamAdapter};
use crate::{PhysicalExpr, SendableRecordBatchStream};

use arrow::array::{types::UInt64Type, *};
use arrow::compute::{
    self, BatchCoalescer, SortOptions, concat_batches, filter_record_batch, interleave,
    take_arrays,
};
use arrow::datatypes::SchemaRef;
use datafusion_common::cast::as_uint64_array;
use datafusion_common::instant::Instant;
use datafusion_common::{
    DataFusionError, JoinType, NullEquality, Result, exec_err, internal_err,
};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::{SpillFile, TryEmitter, async_try_stream};
use datafusion_physical_expr_common::physical_expr::PhysicalExprRef;

use futures::StreamExt;

/// Represents a chunk of joined data from streamed and buffered side
pub(super) struct StreamedJoinedChunk {
    /// Index of batch in buffered_data
    buffered_batch_idx: Option<usize>,
    /// Array builder for streamed indices
    streamed_indices: UInt64Builder,
    /// Array builder for buffered indices
    /// This could contain nulls if the join is null-joined
    buffered_indices: UInt64Builder,
}

/// Represents a record batch from streamed input.
///
/// Also stores information of matching rows from buffered batches.
pub(super) struct StreamedBatch {
    /// The streamed record batch
    pub batch: RecordBatch,
    /// The index of row in the streamed batch to compare with buffered batches
    pub idx: usize,
    /// The join key arrays of streamed batch which are used to compare with buffered batches
    /// and to produce output. They are produced by evaluating `on` expressions.
    pub join_arrays: Vec<ArrayRef>,
    /// Chunks of indices from buffered side (may be nulls) joined to streamed
    pub output_indices: Vec<StreamedJoinedChunk>,
    /// Total number of output rows across all chunks in `output_indices`
    pub num_output_rows: usize,
    /// Index of currently scanned batch from buffered data
    pub buffered_batch_idx: Option<usize>,
}

impl StreamedBatch {
    fn try_new(batch: RecordBatch, on_column: &[Arc<dyn PhysicalExpr>]) -> Result<Self> {
        let join_arrays = join_arrays(&batch, on_column)?;
        Ok(StreamedBatch {
            batch,
            idx: 0,
            join_arrays,
            output_indices: vec![],
            num_output_rows: 0,
            buffered_batch_idx: None,
        })
    }

    fn new_empty(schema: SchemaRef) -> Self {
        StreamedBatch {
            batch: RecordBatch::new_empty(schema),
            idx: 0,
            join_arrays: vec![],
            output_indices: vec![],
            num_output_rows: 0,
            buffered_batch_idx: None,
        }
    }

    /// Number of unfrozen output pairs in this streamed batch
    fn num_output_rows(&self) -> usize {
        self.num_output_rows
    }

    /// Appends new pair consisting of current streamed index and `buffered_idx`
    /// index of buffered batch with `buffered_batch_idx` index.
    fn append_output_pair(
        &mut self,
        buffered_batch_idx: Option<usize>,
        buffered_idx: Option<usize>,
        batch_size: usize,
    ) {
        // If no current chunk exists or current chunk is not for current buffered batch,
        // create a new chunk
        if self.output_indices.is_empty() || self.buffered_batch_idx != buffered_batch_idx
        {
            // Compute capacity only when creating a new chunk (infrequent operation).
            // The capacity is the remaining space to reach batch_size.
            // This should always be >= 1 since we only call this when num_output_rows < batch_size.
            debug_assert!(
                batch_size > self.num_output_rows,
                "batch_size ({batch_size}) must be > num_output_rows ({})",
                self.num_output_rows
            );
            let capacity = batch_size - self.num_output_rows;
            self.output_indices.push(StreamedJoinedChunk {
                buffered_batch_idx,
                streamed_indices: UInt64Builder::with_capacity(capacity),
                buffered_indices: UInt64Builder::with_capacity(capacity),
            });
            self.buffered_batch_idx = buffered_batch_idx;
        }
        let current_chunk = self.output_indices.last_mut().unwrap();

        // Append index of streamed batch and index of buffered batch into current chunk
        current_chunk.streamed_indices.append_value(self.idx as u64);
        if let Some(idx) = buffered_idx {
            current_chunk.buffered_indices.append_value(idx as u64);
        } else {
            current_chunk.buffered_indices.append_null();
        }
        self.num_output_rows += 1;
    }
}

/// Per-row filter outcome tracking for full outer joins.
///
/// In a full outer join with a filter, buffered rows that match on join
/// keys but fail every filter evaluation must be emitted with NULLs on
/// the streamed side. Three states are needed because a simple boolean
/// cannot distinguish "never matched" (handled by [`BufferedBatch::null_joined`])
/// from "matched but all filters failed" (must be emitted as null-joined).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FilterState {
    /// Row never appeared in a matched pair.
    Unvisited = 0,
    /// Row matched streamed rows, but all filter evaluations failed.
    AllFailed = 1,
    /// Row matched and at least one filter evaluation passed.
    SomePassed = 2,
}

/// A buffered batch that contains contiguous rows with same join key
///
/// `BufferedBatch` can exist as either an in-memory `RecordBatch` or a `SpillFile`.
#[derive(Debug)]
pub(super) struct BufferedBatch {
    /// Represents in memory or spilled record batch
    pub batch: BufferedBatchState,
    /// The range in which the rows share the same join key
    pub range: Range<usize>,
    /// Array refs of the join key
    pub join_arrays: Vec<ArrayRef>,
    /// Buffered joined index (null joining buffered)
    pub null_joined: Vec<usize>,
    /// Size estimation used for reserving / releasing memory
    pub size_estimation: usize,
    /// Memory footprint of `join_arrays` cached at construction time.
    /// Used during spill to track the residual memory that remains after
    /// the main batch is written to disk.
    pub join_arrays_mem: usize,
    /// Actual amount tracked in the memory reservation for this batch.
    ///
    /// - `InMemory`: equals `size_estimation` (full batch + join_arrays + metadata)
    /// - `Spilled`: equals `join_arrays_mem` (join key arrays stay in memory)
    ///
    /// Invariant: `free_reservation()` shrinks by exactly this amount, so we never
    /// shrink by more than we grew.
    pub reserved_amount: usize,
    /// Tracks filter outcomes for buffered rows in full outer joins.
    /// Indexed by absolute row position within the batch. See [`FilterState`].
    pub join_filter_status: Vec<FilterState>,
    /// Current buffered batch number of rows. Equal to batch.num_rows()
    /// but if batch is spilled to disk this property is preferable
    /// and less expensive
    pub num_rows: usize,
}

impl BufferedBatch {
    fn try_new(
        batch: RecordBatch,
        range: Range<usize>,
        on_column: &[PhysicalExprRef],
    ) -> Result<Self> {
        let join_arrays = join_arrays(&batch, on_column)?;

        // Estimation is calculated as
        //   inner batch size
        // + join keys size
        // + worst case null_joined (as vector capacity * element size)
        // + Range size
        // + size of this estimation
        let join_arrays_mem: usize = join_arrays
            .iter()
            .map(|arr| arr.get_array_memory_size())
            .sum();

        let size_estimation = batch.get_array_memory_size()
            + join_arrays_mem
            + batch.num_rows().next_power_of_two() * size_of::<usize>()
            + size_of::<Range<usize>>()
            + size_of::<usize>();

        let num_rows = batch.num_rows();
        Ok(BufferedBatch {
            batch: BufferedBatchState::InMemory(batch),
            range,
            join_arrays,
            null_joined: vec![],
            size_estimation,
            join_arrays_mem,
            reserved_amount: 0,
            join_filter_status: vec![FilterState::Unvisited; num_rows],
            num_rows,
        })
    }
}

// TODO: Spill join arrays (https://github.com/apache/datafusion/pull/17429)
// Used to represent whether the buffered data is currently in memory or written to disk
pub(super) enum BufferedBatchState {
    // In memory record batch
    InMemory(RecordBatch),
    // Spilled temp file
    Spilled(Arc<dyn SpillFile>),
}

impl Debug for BufferedBatchState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InMemory(batch) => f.debug_tuple("InMemory").field(batch).finish(),
            Self::Spilled(_) => {
                write!(f, "Spilled(Custom_Backend)")
            }
        }
    }
}
/// Sort-Merge join stream for Inner/Left/Right/Full joins.
///
/// Named "materializing" because it builds explicit `(streamed, buffered)` row
/// pairs in [`JoinedRecordBatches`] to produce output columns from both sides
/// of the join.
pub(super) struct MaterializingSortMergeJoinStream {
    // ========================================================================
    // PROPERTIES:
    // These fields are initialized at the start and remain constant throughout
    // the execution.
    // ========================================================================
    /// Output schema
    pub schema: SchemaRef,
    /// Defines the null equality for the join.
    pub null_equality: NullEquality,
    /// Sort options of join columns used to sort streamed and buffered data stream
    pub sort_options: Vec<SortOptions>,
    /// optional join filter
    pub filter: Option<JoinFilter>,
    /// How the join is performed
    pub join_type: JoinType,
    /// Cached `needs_deferred_filtering(filter, join_type)` — both inputs
    /// are fixed at construction time.
    pub deferred_filtering: bool,
    /// Target output batch size
    pub batch_size: usize,

    // ========================================================================
    // STREAMED FIELDS:
    // These fields manage the properties and state of the streamed input.
    // ========================================================================
    /// Input schema of streamed
    pub streamed_schema: SchemaRef,
    /// Streamed data stream
    pub streamed: SendableRecordBatchStream,
    /// Current processing record batch of streamed
    pub streamed_batch: StreamedBatch,
    /// True once the streamed input has no more rows
    pub streamed_exhausted: bool,
    /// Join key columns of streamed
    pub on_streamed: Vec<PhysicalExprRef>,

    // ========================================================================
    // BUFFERED FIELDS:
    // These fields manage the properties and state of the buffered input.
    // ========================================================================
    /// Input schema of buffered
    pub buffered_schema: SchemaRef,
    /// Buffered data stream
    pub buffered: SendableRecordBatchStream,
    /// Current buffered data
    pub buffered_data: BufferedData,
    /// Has any streamed row matched the current buffered key group?
    /// (FULL join: an unmatched group is emitted null-joined when passed.)
    pub buffered_group_matched: bool,
    /// True once the buffered input has no more rows and no group remains
    pub buffered_exhausted: bool,
    /// Join key columns of buffered
    pub on_buffered: Vec<PhysicalExprRef>,

    // ========================================================================
    // MERGE JOIN STATES:
    // These fields track the execution state of merge join and are updated
    // during the execution.
    // ========================================================================
    /// Staging output array builders
    pub joined_record_batches: JoinedRecordBatches,
    /// Output buffer. Currently used by filtering as it requires double buffering
    /// to avoid small/empty batches. Non-filtered joins output directly from
    /// `joined_record_batches.joined_batches`
    pub output: BatchCoalescer,
    /// Manages the process of spilling and reading back intermediate data
    pub spill_manager: SpillManager,

    /// Tracks the number of batches currently spilled
    pub spilled_batch_count: usize,

    /// Time spent doing the join's own work (including spill write and
    /// read-back). The clock is stopped while awaiting the child inputs or
    /// the consumer taking an emitted batch — see [`Self::stop_join_time`].
    pub join_time: Time,
    /// Start of the currently running `join_time` span; `None` while the
    /// clock is stopped.
    pub join_time_start: Option<Instant>,

    // ========================================================================
    // CACHED COMPARATORS:
    // Pre-built comparators to avoid per-row type dispatch in hot loops.
    // ========================================================================
    /// Comparator for streamed vs buffered head batch key comparison
    pub streamed_buffered_cmp: Option<JoinKeyComparator>,
    /// Comparator for buffered head vs tail batch equality check
    pub buffered_equality_cmp: Option<JoinKeyComparator>,

    // ========================================================================
    // EXECUTION RESOURCES:
    // Fields related to managing execution resources and monitoring performance.
    // ========================================================================
    /// Metrics
    pub join_metrics: SortMergeJoinMetrics,
    /// Memory reservation
    pub reservation: MemoryReservation,
    /// Runtime env
    pub runtime_env: Arc<RuntimeEnv>,
    /// A unique id per streamed batch, tagging deferred-filter metadata so
    /// `get_corrected_filter_mask` can group output rows by input batch.
    pub streamed_batch_counter: usize,
}

/// Staging area for joined data before output
///
/// Accumulates joined rows until either:
/// - Target batch size reached (for efficiency)
/// - Stream exhausted (flush remaining data)
pub(super) struct JoinedRecordBatches {
    /// Joined batches. Each batch is already joined columns from left and right sources
    pub(super) joined_batches: BatchCoalescer,
    /// Filter metadata for deferred filtering
    pub(super) filter_metadata: FilterMetadata,
}

impl JoinedRecordBatches {
    /// Concatenates all accumulated batches into a single RecordBatch
    ///
    /// Must drain ALL batches from BatchCoalescer for filtered joins to ensure
    /// metadata alignment when applying get_corrected_filter_mask().
    pub(super) fn concat_batches(&mut self, schema: &SchemaRef) -> Result<RecordBatch> {
        self.joined_batches.finish_buffered_batch()?;

        let mut all_batches = vec![];
        while let Some(batch) = self.joined_batches.next_completed_batch() {
            all_batches.push(batch);
        }

        match all_batches.as_slice() {
            [] => unreachable!("concat_batches called with empty BatchCoalescer"),
            [single_batch] => Ok(single_batch.clone()),
            multiple_batches => Ok(concat_batches(schema, multiple_batches)?),
        }
    }

    /// Clears batches without touching metadata (for early return when no filtering needed)
    fn clear_batches(&mut self, schema: &SchemaRef, batch_size: usize) {
        self.joined_batches = new_output_coalescer(Arc::clone(schema), batch_size);
    }

    /// Asserts that if batches is empty, metadata is also empty
    #[inline]
    fn debug_assert_empty_consistency(&self) {
        if self.joined_batches.is_empty() {
            debug_assert_eq!(
                self.filter_metadata.filter_mask.len(),
                0,
                "filter_mask should be empty when batches is empty"
            );
            debug_assert_eq!(
                self.filter_metadata.row_indices.len(),
                0,
                "row_indices should be empty when batches is empty"
            );
            debug_assert_eq!(
                self.filter_metadata.batch_ids.len(),
                0,
                "batch_ids should be empty when batches is empty"
            );
        }
    }

    /// Pushes a batch with null metadata (rows that need no filter correction)
    ///
    /// Used for: (1) Full join buffered rows with no streamed match, and
    /// (2) outer join streamed rows with no buffered match. These rows are
    /// already in final form but must flow through the deferred filtering
    /// pipeline to preserve output ordering. Null metadata causes
    /// get_corrected_filter_mask() to pass them through unchanged.
    ///
    /// Maintains invariant: N rows → N metadata entries (nulls)
    fn push_batch_with_null_metadata(&mut self, batch: RecordBatch, join_type: JoinType) {
        debug_assert!(
            matches!(join_type, JoinType::Left | JoinType::Right | JoinType::Full),
            "push_batch_with_null_metadata should only be called for deferred-filtered joins"
        );

        let num_rows = batch.num_rows();

        self.filter_metadata.append_nulls(num_rows);

        self.filter_metadata.debug_assert_metadata_aligned();
        self.joined_batches
            .push_batch(batch)
            .expect("Failed to push batch to BatchCoalescer");
    }

    /// Pushes a batch with filter metadata (filtered outer joins)
    ///
    /// Deferred filtering: An input row may join with multiple buffered rows, but we
    /// don't know yet if all matches failed the filter. We track metadata so
    /// `get_corrected_filter_mask()` can later group by input row and decide:
    /// - If any match passed: emit passing rows
    /// - If all matches failed: emit null-joined row
    ///
    /// Maintains invariant: N rows → N metadata entries
    fn push_batch_with_filter_metadata(
        &mut self,
        batch: RecordBatch,
        row_indices: &UInt64Array,
        filter_mask: &BooleanArray,
        streamed_batch_id: usize,
        join_type: JoinType,
    ) {
        debug_assert!(
            matches!(join_type, JoinType::Left | JoinType::Right | JoinType::Full),
            "push_batch_with_filter_metadata should only be called for outer joins that need deferred filtering"
        );

        debug_assert_eq!(
            row_indices.len(),
            filter_mask.len(),
            "row_indices and filter_mask must have same length"
        );

        self.filter_metadata.append_filter_metadata(
            row_indices,
            filter_mask,
            streamed_batch_id,
        );

        self.filter_metadata.debug_assert_metadata_aligned();
        self.joined_batches
            .push_batch(batch)
            .expect("Failed to push batch to BatchCoalescer");
    }

    /// Pushes a batch without metadata (non-filtered joins)
    ///
    /// No deferred filtering needed. Either every join match is output (Inner),
    /// or null-joined rows are handled separately. No need to track which input
    /// row produced which output row.
    fn push_batch_without_metadata(&mut self, batch: RecordBatch) {
        self.joined_batches
            .push_batch(batch)
            .expect("Failed to push batch to BatchCoalescer");
    }

    fn clear(&mut self, schema: &SchemaRef, batch_size: usize) {
        self.joined_batches = new_output_coalescer(Arc::clone(schema), batch_size);
        self.filter_metadata = FilterMetadata::new();
        self.debug_assert_empty_consistency();
    }
}

impl MaterializingSortMergeJoinStream {
    #[expect(clippy::too_many_arguments)]
    pub fn try_new(
        schema: SchemaRef,
        sort_options: Vec<SortOptions>,
        null_equality: NullEquality,
        streamed: SendableRecordBatchStream,
        buffered: SendableRecordBatchStream,
        on_streamed: Vec<Arc<dyn PhysicalExpr>>,
        on_buffered: Vec<Arc<dyn PhysicalExpr>>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        batch_size: usize,
        join_metrics: SortMergeJoinMetrics,
        reservation: MemoryReservation,
        spill_manager: SpillManager,
        runtime_env: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        let streamed_schema = streamed.schema();
        let buffered_schema = buffered.schema();
        debug_assert!(
            matches!(
                join_type,
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
            ),
            "MaterializingSortMergeJoinStream does not handle {join_type:?}; \
             semi/anti/mark joins use BitwiseSortMergeJoinStream"
        );
        let join_time = join_metrics.join_time();
        let mut this = Self {
            sort_options,
            null_equality,
            schema: Arc::clone(&schema),
            streamed_schema: Arc::clone(&streamed_schema),
            buffered_schema,
            streamed,
            buffered,
            streamed_batch: StreamedBatch::new_empty(streamed_schema),
            buffered_data: BufferedData::default(),
            buffered_group_matched: false,
            streamed_exhausted: false,
            buffered_exhausted: false,
            on_streamed,
            on_buffered,
            deferred_filtering: needs_deferred_filtering(filter.as_ref(), join_type),
            filter,
            joined_record_batches: JoinedRecordBatches {
                joined_batches: new_output_coalescer(Arc::clone(&schema), batch_size),
                filter_metadata: FilterMetadata::new(),
            },
            output: new_output_coalescer(schema, batch_size),
            batch_size,
            join_type,
            join_metrics,
            reservation,
            runtime_env,
            spill_manager,
            spilled_batch_count: 0,
            join_time,
            join_time_start: None,
            streamed_buffered_cmp: None,
            buffered_equality_cmp: None,
            streamed_batch_counter: 0,
        };

        let schema = Arc::clone(&this.schema);
        let baseline_metrics = this.join_metrics.baseline_metrics();

        let stream = async_try_stream(|mut emitter| async move {
            this.start_join_time();
            let result = this.join(&mut emitter).await;
            this.stop_join_time();
            result
        });
        // ObservedStream records the baseline metrics (output rows/batches,
        // end time).
        Ok(Box::pin(ObservedStream::new(
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
            baseline_metrics,
            None,
        )))
    }

    /// Main loop: the textbook sort-merge join.
    ///
    /// Both inputs arrive sorted on the join keys. The streamed side is
    /// consumed one row at a time; the buffered side one key *group* (all
    /// contiguous rows sharing a key) at a time
    async fn join(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        // 1. Load the first streamed row and the first buffered key group.
        self.load_next_streamed_batch().await?;
        self.advance_buffered_group().await?;

        // 2. Merge-scan while either input still has rows.
        while !(self.streamed_exhausted && self.buffered_exhausted) {
            // Flush the deferred-filtering pipeline once a full batch of
            // rows accumulated (filtered outer joins output through it).
            if self.deferred_filtering
                && self.deferred_rows_accumulated() >= self.batch_size
            {
                self.emit_deferred_output(emitter).await?;
            }

            // 3. Compare the join keys at both cursors. An exhausted side
            //    compares as the larger one, so the other side keeps
            //    draining through its own arm.
            match self.compare_streamed_buffered()? {
                // 3a. The streamed row can never match: null-join it (outer
                //     joins emit it; inner joins drop it), then advance.
                Ordering::Less => {
                    self.null_join_streamed_row();
                    if self.num_unfrozen_pairs() >= self.batch_size {
                        self.freeze_and_emit(emitter).await?;
                    }
                    if !self.try_advance_streamed_row() {
                        self.load_next_streamed_batch().await?;
                    }
                }
                // 3b. The buffered group can never match again: null-join
                //     it if nothing matched it (FULL join), then advance to
                //     the next key group.
                Ordering::Greater => {
                    self.null_join_buffered_group();
                    if !self.try_advance_buffered_group()? {
                        self.advance_buffered_group().await?;
                    }
                }
                // 3c. Match: pair the streamed row with the whole group —
                //     materializing ("freezing") mid-scan whenever a full
                //     batch of pairs accumulates — then advance streamed.
                //     The group stays for the next streamed row.
                Ordering::Equal => {
                    while !self.pair_streamed_row_with_group() {
                        self.freeze_and_emit(emitter).await?;
                    }
                    if !self.try_advance_streamed_row() {
                        self.load_next_streamed_batch().await?;
                    }
                }
            }

            // 4. Emit completed output batches (filtered joins emit
            //    through the deferred-filtering pipeline above instead).
            if !self.deferred_filtering
                && self
                    .joined_record_batches
                    .joined_batches
                    .has_completed_batch()
            {
                self.emit_completed_joined_batches(emitter).await;
            }
        }

        // 5. Flush everything that remains.
        self.on_children_exhausted(emitter).await
    }

    /// `Equal`: pair the current streamed row with every row of the
    /// buffered key group, and mark the group as matched.
    ///
    /// Returns false when a full batch of pairs has accumulated (the scan
    /// may or may not be complete): the caller must materialize
    /// (`freeze_and_emit`) and call again, which resumes the scan where it
    /// paused. Returns true when the group scan is complete and there is
    /// room for more pairs.
    fn pair_streamed_row_with_group(&mut self) -> bool {
        while !self.buffered_data.scanning_finished()
            && self.num_unfrozen_pairs() < self.batch_size
        {
            let scanning_idx = self.buffered_data.scanning_idx();
            self.streamed_batch.append_output_pair(
                Some(self.buffered_data.scanning_batch_idx),
                Some(scanning_idx),
                self.batch_size,
            );
            self.buffered_data.scanning_advance();
        }
        if self.num_unfrozen_pairs() >= self.batch_size {
            return false;
        }

        self.buffered_group_matched = true;
        self.buffered_data.scanning_reset();
        true
    }

    /// `Less` (outer joins): no buffered row matches the current streamed
    /// row — emit it joined to NULLs. Inner joins emit nothing.
    fn null_join_streamed_row(&mut self) {
        if matches!(
            self.join_type,
            JoinType::Left | JoinType::Right | JoinType::Full
        ) {
            let scanning_batch_idx = if self.buffered_data.scanning_finished() {
                None
            } else {
                Some(self.buffered_data.scanning_batch_idx)
            };
            self.streamed_batch.append_output_pair(
                scanning_batch_idx,
                None,
                self.batch_size,
            );
        }
        self.buffered_data.scanning_reset();
    }

    /// `Greater` (FULL join): the buffered group can never match a streamed
    /// row anymore — if nothing matched it, mark all its rows for
    /// null-joined output (produced when the group's batches are dequeued).
    fn null_join_buffered_group(&mut self) {
        if self.join_type == JoinType::Full && !self.buffered_group_matched {
            while !self.buffered_data.scanning_finished() {
                let scanning_idx = self.buffered_data.scanning_idx();
                self.buffered_data
                    .scanning_batch_mut()
                    .null_joined
                    .push(scanning_idx);
                self.buffered_data.scanning_advance();
            }
        }
        self.buffered_data.scanning_reset();
    }

    /// Start (resume) the `join_time` clock.
    fn start_join_time(&mut self) {
        debug_assert!(self.join_time_start.is_none(), "join_time already running");
        self.join_time_start = Some(Instant::now());
    }

    /// Stop (pause) the `join_time` clock, accumulating the elapsed span.
    ///
    /// Called around awaits whose duration is not the join's own work: the
    /// child input streams' `next()` and `emitter.emit()` (where the
    /// consumer processes the batch). The join's own spill write and
    /// read-back are NOT excluded — that time is join work.
    fn stop_join_time(&mut self) {
        if let Some(start) = self.join_time_start.take() {
            self.join_time.add_elapsed(start);
        }
    }

    /// Number of rows currently waiting in the deferred-filtering pipeline.
    ///
    /// Typically bounded to ~2*batch_size: one batch_size worth from
    /// freeze_dequeuing_buffered() (when an input batch is fully consumed),
    /// plus up to batch_size pairs accumulating toward the next freeze. A
    /// single streamed row matching a very large key group can exceed that
    /// (its pairs freeze into the pipeline before the gate runs again — same
    /// as the pre-generator design). This does not reintroduce the unbounded
    /// buffering fixed by PR #20482; `on_children_exhausted` flushes the
    /// remainder.
    fn deferred_rows_accumulated(&self) -> usize {
        self.num_unfrozen_pairs()
            + self.joined_record_batches.filter_metadata.filter_mask.len()
    }

    /// Run the deferred-filtering pipeline over everything accumulated so
    /// far and emit its completed output, if any. Clears the accumulation
    /// it processed.
    ///
    /// The caller gates this on `deferred_rows_accumulated() >= batch_size`:
    /// running the pipeline per row instead (concat + correct_mask +
    /// filter_by_type) would dominate runtime for unique keys.
    async fn emit_deferred_output(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        // Ensure required spilled batches are restored to memory before
        // processing, as this path invokes freeze_all().
        self.restore_spilled_batches_for_freeze().await?;
        self.stage_filtered_output()?;
        self.emit_completed_output(emitter).await;
        Ok(())
    }

    /// Emit every completed batch of the deferred-filtering output buffer.
    ///
    /// All deferred-filtered output must leave through this single buffer:
    /// emitting a batch around it would reorder it ahead of rows still
    /// buffered here, breaking the streamed-side ordering the operator
    /// advertises via `maintains_input_order`.
    async fn emit_completed_output(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) {
        while let Some(record_batch) = self.output.next_completed_batch() {
            // While the emitted batch is in the consumer's hands the join
            // isn't doing any work.
            self.stop_join_time();
            emitter.emit(record_batch).await;
            self.start_join_time();
        }
    }

    /// Restore every spilled buffered batch that the next freeze needs.
    async fn restore_spilled_batches_for_freeze(&mut self) -> Result<()> {
        let needed = self.get_required_batch_indices(self.buffered_data.batches.len());
        self.restore_spilled_batches(&needed).await
    }

    /// Emit all completed joined batches to the stream consumer.
    async fn emit_completed_joined_batches(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) {
        while let Some(record_batch) = self
            .joined_record_batches
            .joined_batches
            .next_completed_batch()
        {
            // While the emitted batch is in the consumer's hands the join
            // isn't doing any work.
            self.stop_join_time();
            emitter.emit(record_batch).await;
            self.start_join_time();
        }
    }

    /// Flush everything that remains once both inputs are exhausted.
    async fn on_children_exhausted(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        // Freeze the remaining pairs, restoring any spilled batches needed.
        self.restore_spilled_batches_for_freeze().await?;
        self.freeze_all()?;

        // Verify metadata alignment before final output
        self.joined_record_batches
            .filter_metadata
            .debug_assert_metadata_aligned();

        if self.deferred_filtering {
            // Filtered joins must concat and filter ALL remaining data at
            // once. The result is staged in `output` rather than emitted
            // directly: `output` may still hold rows from earlier flushes,
            // and those precede these on the streamed side.
            if !self.joined_record_batches.joined_batches.is_empty() {
                let record_batch = self.filter_joined_batch()?;
                self.output
                    .push_batch(record_batch)
                    .expect("Failed to push output batch");
            }
        } else if !self.joined_record_batches.joined_batches.is_empty() {
            // For non-filtered joins, finish buffered data first, then emit
            // every completed batch.
            self.joined_record_batches
                .joined_batches
                .finish_buffered_batch()?;
            self.emit_completed_joined_batches(emitter).await;
        }

        // Drain the double-buffering coalescer used by filtered joins.
        if !self.output.is_empty() {
            self.output.finish_buffered_batch()?;
            self.emit_completed_output(emitter).await;
        }

        Ok(())
    }

    /// Build a comparator for streamed vs buffered head batch keys.
    fn rebuild_streamed_buffered_cmp(&mut self) -> Result<()> {
        if self.streamed_batch.join_arrays.is_empty()
            || !self.buffered_data.has_buffered_rows()
        {
            self.streamed_buffered_cmp = None;
            return Ok(());
        }
        self.streamed_buffered_cmp = Some(JoinKeyComparator::new(
            &self.streamed_batch.join_arrays,
            &self.buffered_data.head_batch().join_arrays,
            &self.sort_options,
            self.null_equality,
        )?);
        Ok(())
    }

    /// Build a comparator for buffered head vs tail batch equality.
    fn rebuild_buffered_equality_cmp(&mut self) -> Result<()> {
        if self.buffered_data.batches.is_empty() {
            self.buffered_equality_cmp = None;
            return Ok(());
        }
        self.buffered_equality_cmp = Some(JoinKeyComparator::new(
            &self.buffered_data.head_batch().join_arrays,
            &self.buffered_data.tail_batch().join_arrays,
            &self.sort_options,
            // is_join_arrays_equal treats both-null as equal
            NullEquality::NullEqualsNull,
        )?);
        Ok(())
    }

    /// Number of unfrozen output pairs (used to decide when to freeze + output)
    fn num_unfrozen_pairs(&self) -> usize {
        self.streamed_batch.num_output_rows()
    }

    /// Process accumulated batches for filtered joins.
    ///
    /// Freezes unfrozen pairs, applies deferred filtering and stages the
    /// result in [`Self::output`]. Completed batches are emitted separately
    /// by [`Self::emit_completed_output`].
    fn stage_filtered_output(&mut self) -> Result<()> {
        self.freeze_all()?;

        self.joined_record_batches
            .filter_metadata
            .debug_assert_metadata_aligned();

        if !self.joined_record_batches.joined_batches.is_empty() {
            let out_filtered_batch = self.filter_joined_batch()?;
            self.output
                .push_batch(out_filtered_batch)
                .expect("Failed to push output batch");
        }

        Ok(())
    }

    /// Identifies which buffered batches are needed for the upcoming freeze operation
    fn get_required_batch_indices(&self, buffered_freeze_count: usize) -> Vec<usize> {
        let mut needed = vec![];
        // Avoid scanning if no spilled batches exist
        if self.spilled_batch_count == 0 {
            return needed;
        }
        // We need all batches that matched with streamed rows
        for chunk in &self.streamed_batch.output_indices {
            if let Some(idx) = chunk.buffered_batch_idx {
                needed.push(idx);
            }
        }

        // Full Joins need to emit null-joined rows, so we need batches up to freeze_count
        if self.join_type == JoinType::Full {
            needed.extend(0..buffered_freeze_count);
        }

        needed.sort_unstable();
        needed.dedup();
        needed
    }

    /// Asynchronously reads spilled batches back into memory.
    /// Only processes the required indices to avoid OOMs.
    async fn restore_spilled_batches(
        &mut self,
        required_indices: &[usize],
    ) -> Result<()> {
        for &idx in required_indices {
            // Guard against indices that might be out of bounds if the queue was cleared
            if idx >= self.buffered_data.batches.len() {
                continue;
            }

            let bb = &mut self.buffered_data.batches[idx];

            if let BufferedBatchState::Spilled(spill_file) = &bb.batch {
                let mut spill_stream = self
                    .spill_manager
                    .read_spill_as_stream(Arc::clone(spill_file), None)?;

                match spill_stream.next().await.transpose()? {
                    Some(batch) => {
                        // Transition the batch back to InMemory
                        bb.batch = BufferedBatchState::InMemory(batch);
                        self.spilled_batch_count -= 1;
                        // The batch is back in memory, so we must account for its size.
                        let newly_allocated =
                            bb.size_estimation.saturating_sub(bb.reserved_amount);
                        self.reservation.grow(newly_allocated);
                        bb.reserved_amount = bb.size_estimation;

                        self.join_metrics
                            .peak_mem_used()
                            .set_max(self.reservation.size());
                    }
                    None => {
                        return internal_err!("Spill file was empty");
                    }
                }
            }
        }

        Ok(())
    }

    /// Sync fast path of advancing the streamed cursor: move to the next row
    /// of the current batch. Returns false at the batch boundary, where the
    /// caller must load the next batch via
    /// [`Self::load_next_streamed_batch`].
    fn try_advance_streamed_row(&mut self) -> bool {
        if self.streamed_batch.idx + 1 < self.streamed_batch.batch.num_rows() {
            self.streamed_batch.idx += 1;
            return true;
        }
        false
    }

    /// Load the next streamed batch (freezing the finished one) and point
    /// the streamed cursor at its first row. Sets `streamed_exhausted` when
    /// the streamed input has no more rows.
    async fn load_next_streamed_batch(&mut self) -> Result<()> {
        loop {
            // Loading a new streamed batch freezes the current one, which
            // materializes buffered columns — restore any spilled buffered
            // batches it needs first.
            self.restore_spilled_batches_for_freeze().await?;

            // The child's execution time is its own, not join_time.
            self.stop_join_time();
            let item = self.streamed.next().await.transpose();
            self.start_join_time();
            match item? {
                None => {
                    // Release the streamed input pipeline's resources.
                    let streamed_schema = self.streamed.schema();
                    self.streamed =
                        Box::pin(EmptyRecordBatchStream::new(streamed_schema));
                    self.streamed_exhausted = true;
                    return Ok(());
                }
                Some(batch) => {
                    if batch.num_rows() > 0 {
                        self.freeze_streamed()?;
                        self.join_metrics.input_batches().add(1);
                        self.join_metrics.input_rows().add(batch.num_rows());
                        self.streamed_batch =
                            StreamedBatch::try_new(batch, &self.on_streamed)?;
                        self.rebuild_streamed_buffered_cmp()?;
                        // Every incoming streamed batch gets a unique id.
                        self.streamed_batch_counter += 1;
                        return Ok(());
                    }
                }
            }
        }
    }

    fn free_reservation(&mut self, buffered_batch: &BufferedBatch) {
        if buffered_batch.reserved_amount > 0 {
            self.reservation.shrink(buffered_batch.reserved_amount);
        }
    }

    fn allocate_reservation(&mut self, mut buffered_batch: BufferedBatch) -> Result<()> {
        match self.reservation.try_grow(buffered_batch.size_estimation) {
            Ok(_) => {
                buffered_batch.reserved_amount = buffered_batch.size_estimation;
                self.join_metrics
                    .peak_mem_used()
                    .set_max(self.reservation.size());
                Ok(())
            }
            Err(_) if self.runtime_env.disk_manager.tmp_files_enabled() => {
                // Spill buffered batch to disk

                match buffered_batch.batch {
                    BufferedBatchState::InMemory(batch) => {
                        let spill_file = self
                            .spill_manager
                            .spill_record_batch_and_finish(
                                &[batch],
                                "sort_merge_join_buffered_spill",
                            )?
                            .unwrap(); // Operation only return None if no batches are spilled, here we ensure that at least one batch is spilled

                        buffered_batch.batch = BufferedBatchState::Spilled(spill_file);
                        self.spilled_batch_count += 1;

                        // Join key arrays remain in memory after the batch is
                        // spilled — the comparator needs them for key boundary
                        // detection. Force-grow the reservation so the pool
                        // reflects actual memory usage even if this pushes
                        // pool.reserved() above the configured limit. This is
                        // safe because the memory is physically consumed and
                        // not tracking it would let other operators over-allocate
                        // against a stale pool view.
                        let join_arrays_mem = buffered_batch.join_arrays_mem;
                        self.reservation.grow(join_arrays_mem);
                        buffered_batch.reserved_amount = join_arrays_mem;
                        self.join_metrics
                            .peak_mem_used()
                            .set_max(self.reservation.size());

                        Ok(())
                    }
                    _ => internal_err!("Buffered batch has empty body"),
                }
            }
            Err(e) => exec_err!("{}. Disk spilling disabled.", e.message()),
        }?;

        self.buffered_data.batches.push_back(buffered_batch);
        Ok(())
    }

    /// Sync fast path of [`Self::advance_buffered_group`]: when the next
    /// group starts in the single remaining buffered batch and provably ends
    /// within it (the common case — a group only reaches a batch boundary
    /// once per batch), advance entirely synchronously. Returns false —
    /// leaving all state unchanged — when the async path must run instead.
    fn try_advance_buffered_group(&mut self) -> Result<bool> {
        if self.buffered_data.batches.len() != 1 {
            return Ok(false);
        }
        let head_batch = self.buffered_data.head_batch();
        if head_batch.range.end == head_batch.num_rows {
            // Fully consumed — needs dequeuing (and loading the next batch).
            return Ok(false);
        }

        if self.buffered_equality_cmp.is_none() {
            self.rebuild_buffered_equality_cmp()?;
        }
        let cmp = self.buffered_equality_cmp.as_ref().unwrap();

        // Scan the next group's extent before committing any state, so a
        // bail-out (the group may span into the next batch) leaves
        // everything untouched for the async path.
        let batch = self.buffered_data.head_batch();
        let group_start = batch.range.end;
        let mut group_end = group_start + 1;
        while group_end < batch.num_rows && cmp.is_equal(group_start, group_end) {
            group_end += 1;
        }
        if group_end == batch.num_rows {
            return Ok(false);
        }

        let batch = self.buffered_data.tail_batch_mut();
        batch.range.start = group_start;
        batch.range.end = group_end;
        self.buffered_group_matched = false;
        Ok(true)
    }

    /// Advance the buffered side to the next key group: dequeue batches
    /// fully consumed by the previous group, then collect all contiguous
    /// rows sharing the next join key (the group may span multiple buffered
    /// batches). Sets `buffered_exhausted` when no group remains.
    async fn advance_buffered_group(&mut self) -> Result<()> {
        self.buffered_group_matched = false;
        self.dequeue_consumed_buffered_batches().await?;

        if self.buffered_data.batches.is_empty() {
            // Load the batch holding the first row of the next group.
            if !self.load_next_buffered_batch().await? {
                self.buffered_exhausted = true;
                return Ok(());
            }
        } else {
            // Seed the next group at the first unconsumed row of the
            // remaining batch.
            let tail_batch = self.buffered_data.tail_batch_mut();
            tail_batch.range.start = tail_batch.range.end;
            tail_batch.range.end += 1;
        }

        self.extend_buffered_group().await
    }

    /// Dequeue buffered batches fully consumed by the previous group,
    /// producing their pending output (e.g. Full-join null-joined rows).
    async fn dequeue_consumed_buffered_batches(&mut self) -> Result<()> {
        let mut head_changed = false;
        while !self.buffered_data.batches.is_empty() {
            let head_batch = self.buffered_data.head_batch();
            if head_batch.range.end != head_batch.num_rows {
                // The next group starts within the head batch: streamed rows
                // will be joined with the head batch in the next step.
                break;
            }
            // load the spilled head batch before dequeuing
            let needed = self.get_required_batch_indices(1);
            self.restore_spilled_batches(&needed).await?;

            self.freeze_dequeuing_buffered()?;
            if let Some(mut buffered_batch) = self.buffered_data.batches.pop_front() {
                self.produce_buffered_not_matched(&mut buffered_batch)?;
                self.free_reservation(&buffered_batch);
                if matches!(buffered_batch.batch, BufferedBatchState::Spilled(_)) {
                    self.spilled_batch_count -= 1;
                }
                head_changed = true;
            }
        }
        if head_changed {
            self.streamed_buffered_cmp = None;
            self.buffered_equality_cmp = None;
        }
        Ok(())
    }

    /// Load the next non-empty buffered batch and seed a new group with its
    /// first row. Returns false when the buffered input is exhausted.
    async fn load_next_buffered_batch(&mut self) -> Result<bool> {
        loop {
            // The child's execution time is its own, not join_time.
            self.stop_join_time();
            let item = self.buffered.next().await.transpose();
            self.start_join_time();
            match item? {
                None => {
                    // Release the buffered input pipeline's resources.
                    let buffered_schema = self.buffered.schema();
                    self.buffered =
                        Box::pin(EmptyRecordBatchStream::new(buffered_schema));
                    return Ok(false);
                }
                Some(batch) => {
                    self.join_metrics.input_batches().add(1);
                    self.join_metrics.input_rows().add(batch.num_rows());

                    if batch.num_rows() > 0 {
                        let buffered_batch =
                            BufferedBatch::try_new(batch, 0..1, &self.on_buffered)?;
                        self.allocate_reservation(buffered_batch)?;
                        self.streamed_buffered_cmp = None;
                        return Ok(true);
                    }
                }
            }
        }
    }

    /// Extend the current group with every following row that shares its
    /// key, loading more buffered batches as needed.
    async fn extend_buffered_group(&mut self) -> Result<()> {
        loop {
            if self.buffered_data.tail_batch().range.end
                < self.buffered_data.tail_batch().num_rows
            {
                if self.buffered_equality_cmp.is_none() {
                    self.rebuild_buffered_equality_cmp()?;
                }
                while self.buffered_data.tail_batch().range.end
                    < self.buffered_data.tail_batch().num_rows
                {
                    if self.buffered_equality_cmp.as_ref().unwrap().is_equal(
                        self.buffered_data.head_batch().range.start,
                        self.buffered_data.tail_batch().range.end,
                    ) {
                        self.buffered_data.tail_batch_mut().range.end += 1;
                    } else {
                        // Group complete within the current batch.
                        return Ok(());
                    }
                }
            } else {
                // The child's execution time is its own, not join_time.
                self.stop_join_time();
                let item = self.buffered.next().await.transpose();
                self.start_join_time();
                match item? {
                    None => {
                        // Group complete; the input is done but the group is
                        // still valid — `buffered_exhausted` is only set once
                        // it has been fully consumed and dequeued.
                        // Release the buffered input pipeline's resources.
                        let buffered_schema = self.buffered.schema();
                        self.buffered =
                            Box::pin(EmptyRecordBatchStream::new(buffered_schema));
                        return Ok(());
                    }
                    Some(batch) => {
                        // Polling batches coming concurrently as multiple partitions
                        self.join_metrics.input_batches().add(1);
                        self.join_metrics.input_rows().add(batch.num_rows());
                        if batch.num_rows() > 0 {
                            let buffered_batch =
                                BufferedBatch::try_new(batch, 0..0, &self.on_buffered)?;
                            self.allocate_reservation(buffered_batch)?;
                            self.buffered_equality_cmp = None;
                        }
                    }
                }
            }
        }
    }

    /// Get comparison result of streamed row and buffered batches
    fn compare_streamed_buffered(&mut self) -> Result<Ordering> {
        if self.streamed_exhausted {
            return Ok(Ordering::Greater);
        }
        if !self.buffered_data.has_buffered_rows() {
            return Ok(Ordering::Less);
        }

        if self.streamed_buffered_cmp.is_none() {
            self.rebuild_streamed_buffered_cmp()?;
        }
        Ok(self.streamed_buffered_cmp.as_ref().unwrap().compare(
            self.streamed_batch.idx,
            self.buffered_data.head_batch().range.start,
        ))
    }

    /// Materialize ("freeze") the accumulated pairs — restoring any spilled
    /// batches they reference first — and emit completed output batches
    /// (filtered joins emit through the deferred-filtering gate instead).
    async fn freeze_and_emit(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        self.restore_spilled_batches_for_freeze().await?;
        self.freeze_all()?;

        if !self.deferred_filtering
            && self
                .joined_record_batches
                .joined_batches
                .has_completed_batch()
        {
            self.emit_completed_joined_batches(emitter).await;
        }
        Ok(())
    }

    fn freeze_all(&mut self) -> Result<()> {
        self.freeze_buffered(self.buffered_data.batches.len())?;
        self.freeze_streamed()?;

        // After freezing, metadata should be aligned
        self.joined_record_batches
            .filter_metadata
            .debug_assert_metadata_aligned();

        Ok(())
    }

    // Produces and stages record batches to ensure dequeued buffered batch
    // no longer needed:
    //   1. freezes all indices joined to streamed side
    //   2. freezes NULLs joined to dequeued buffered batch to "release" it
    fn freeze_dequeuing_buffered(&mut self) -> Result<()> {
        self.freeze_streamed()?;
        // Only freeze and produce the first batch in buffered_data as the batch is fully processed
        self.freeze_buffered(1)?;

        // After freezing, metadata should be aligned
        self.joined_record_batches
            .filter_metadata
            .debug_assert_metadata_aligned();

        Ok(())
    }

    // Produces and stages record batch from buffered indices with corresponding
    // NULLs on streamed side.
    //
    // Applicable only in case of Full join.
    //
    fn freeze_buffered(&mut self, batch_count: usize) -> Result<()> {
        if self.join_type != JoinType::Full {
            return Ok(());
        }
        for buffered_batch in self.buffered_data.batches.range_mut(..batch_count) {
            let buffered_indices = UInt64Array::from_iter_values(
                buffered_batch.null_joined.iter().map(|&index| index as u64),
            );
            if let Some(record_batch) = produce_buffered_null_batch(
                &self.schema,
                &self.streamed_schema,
                &buffered_indices,
                buffered_batch,
            )? {
                self.joined_record_batches
                    .push_batch_with_null_metadata(record_batch, self.join_type);
            }
            buffered_batch.null_joined.clear();
        }
        Ok(())
    }

    fn produce_buffered_not_matched(
        &mut self,
        buffered_batch: &mut BufferedBatch,
    ) -> Result<()> {
        if self.join_type != JoinType::Full {
            return Ok(());
        }

        // Collect buffered rows that matched on join keys but had every
        // filter evaluation fail — these must be emitted with NULLs on
        // the streamed side to satisfy full outer join semantics.
        let not_matched_buffered_indices = buffered_batch
            .join_filter_status
            .iter()
            .enumerate()
            .filter_map(|(i, state)| {
                matches!(state, FilterState::AllFailed).then_some(i as u64)
            })
            .collect::<Vec<_>>();

        let buffered_indices =
            UInt64Array::from_iter_values(not_matched_buffered_indices.iter().copied());

        if let Some(record_batch) = produce_buffered_null_batch(
            &self.schema,
            &self.streamed_schema,
            &buffered_indices,
            buffered_batch,
        )? {
            self.joined_record_batches
                .push_batch_with_null_metadata(record_batch, self.join_type);
        }
        buffered_batch
            .join_filter_status
            .fill(FilterState::Unvisited);

        Ok(())
    }

    // Produces and stages record batch for all output indices found
    // for current streamed batch and clears staged output indices.
    //
    // Null-joined chunks (no buffered match) are pushed immediately.
    // Matched chunks are collected and processed together in
    // freeze_streamed_matched() to amortize filter evaluation overhead.
    fn freeze_streamed(&mut self) -> Result<()> {
        let mut matched_chunks: Vec<(usize, UInt64Array, UInt64Array)> = Vec::new();
        let mut total_matched_rows: usize = 0;

        for chunk in self.streamed_batch.output_indices.iter_mut() {
            let left_indices = chunk.streamed_indices.finish();
            if left_indices.is_empty() {
                continue;
            }
            let right_indices: UInt64Array = chunk.buffered_indices.finish();

            if chunk.buffered_batch_idx.is_none() {
                let left_columns =
                    materialize_left_columns(&self.streamed_batch.batch, &left_indices)?;
                let right_columns =
                    create_unmatched_columns(&self.buffered_schema, left_indices.len());

                let columns = if self.join_type != JoinType::Right {
                    [left_columns, right_columns].concat()
                } else {
                    [right_columns, left_columns].concat()
                };
                let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;

                // Null-joined rows (no buffered match) need no filter correction,
                // but must flow through the same pipeline as matched rows to
                // preserve output ordering. Use null metadata as a sentinel so
                // get_corrected_filter_mask() passes them through unchanged.
                if self.deferred_filtering {
                    self.joined_record_batches
                        .push_batch_with_null_metadata(batch, self.join_type);
                } else {
                    self.joined_record_batches
                        .push_batch_without_metadata(batch);
                }
                continue;
            }

            total_matched_rows += left_indices.len();
            matched_chunks.push((
                chunk.buffered_batch_idx.unwrap(),
                left_indices,
                right_indices,
            ));
        }

        if !matched_chunks.is_empty() {
            self.freeze_streamed_matched(&matched_chunks, total_matched_rows)?;
        }

        self.streamed_batch.output_indices.clear();
        self.streamed_batch.num_output_rows = 0;
        Ok(())
    }

    /// Materializes columns, evaluates the join filter, and pushes output
    /// for all matched chunks in a single batch. This avoids per-chunk
    /// RecordBatch construction and filter evaluation, which dominates
    /// cost when keys are near-unique (1 row per chunk).
    fn freeze_streamed_matched(
        &mut self,
        matched_chunks: &[(usize, UInt64Array, UInt64Array)],
        total_matched_rows: usize,
    ) -> Result<()> {
        debug_assert!(
            !matched_chunks.is_empty(),
            "caller guards this with an is_empty check before calling"
        );
        debug_assert!(
            matched_chunks.iter().all(|(idx, left, right)| {
                left.len() == right.len() && *idx < self.buffered_data.batches.len()
            }),
            "left/right indices are built in pairs from the same streamed×buffered cross, \
             and batch_idx comes from iterating buffered_data.batches"
        );
        debug_assert_eq!(
            matched_chunks
                .iter()
                .map(|(_, l, _)| l.len())
                .sum::<usize>(),
            total_matched_rows,
            "total_matched_rows is accumulated from the same chunks in freeze_streamed"
        );

        let combined_left_indices = if matched_chunks.len() == 1 {
            matched_chunks[0].1.clone()
        } else {
            let refs: Vec<&dyn Array> =
                matched_chunks.iter().map(|c| &c.1 as &dyn Array).collect();
            as_uint64_array(&compute::concat(&refs)?)?.clone()
        };

        let left_columns =
            materialize_left_columns(&self.streamed_batch.batch, &combined_left_indices)?;

        let right_columns =
            self.materialize_right_columns(matched_chunks, total_matched_rows)?;

        let filter_columns = if self.join_type == JoinType::Right {
            get_filter_columns(self.filter.as_ref(), &right_columns, &left_columns)
        } else {
            get_filter_columns(self.filter.as_ref(), &left_columns, &right_columns)
        };

        let columns = if self.join_type != JoinType::Right {
            [left_columns, right_columns].concat()
        } else {
            [right_columns, left_columns].concat()
        };
        let output_batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;

        if !filter_columns.is_empty() {
            if let Some(f) = &self.filter {
                let filter_batch =
                    RecordBatch::try_new(Arc::clone(f.schema()), filter_columns)?;
                let filter_result = f
                    .expression()
                    .evaluate(&filter_batch)?
                    .into_array(filter_batch.num_rows())?;

                let filter_result_mask =
                    datafusion_common::cast::as_boolean_array(&filter_result)?;

                // Convert NULL filter results to false — NULL means "not satisfied"
                // per SQL semantics, same as Left/Right outer joins.
                let mask = if filter_result_mask.null_count() > 0 {
                    compute::prep_null_mask_filter(filter_result_mask)
                } else {
                    filter_result_mask.clone()
                };

                if self.deferred_filtering {
                    self.joined_record_batches.push_batch_with_filter_metadata(
                        output_batch,
                        &combined_left_indices,
                        &mask,
                        self.streamed_batch_counter,
                        self.join_type,
                    );
                } else {
                    let filtered_batch = filter_record_batch(&output_batch, &mask)?;
                    self.joined_record_batches
                        .push_batch_without_metadata(filtered_batch);
                }

                // Track which buffered rows had all filter matches fail,
                // so full join can emit them as null-joined later.
                if self.join_type == JoinType::Full {
                    let mut offset = 0usize;
                    for (batch_idx, _left, right) in matched_chunks {
                        let chunk_len = right.len();
                        let buffered_batch = &mut self.buffered_data.batches[*batch_idx];

                        for i in 0..chunk_len {
                            if right.is_null(i) {
                                continue;
                            }
                            let idx = right.value(i) as usize;
                            match buffered_batch.join_filter_status[idx] {
                                FilterState::SomePassed => {}
                                _ if mask.value(offset + i) => {
                                    buffered_batch.join_filter_status[idx] =
                                        FilterState::SomePassed;
                                }
                                _ => {
                                    buffered_batch.join_filter_status[idx] =
                                        FilterState::AllFailed;
                                }
                            }
                        }
                        offset += chunk_len;
                    }
                    debug_assert_eq!(
                        offset, total_matched_rows,
                        "offset must advance through every chunk exactly once"
                    );
                }
            }
        } else {
            self.joined_record_batches
                .push_batch_without_metadata(output_batch);
        }

        Ok(())
    }

    /// Materializes right-side columns across all matched chunks.
    ///
    /// When chunks reference a single buffered batch, indices are concatenated
    /// for a single fetch. When multiple batches are involved, `interleave`
    /// gathers columns across sources. A null-row sentinel at source index 0
    /// handles null right indices (unmatched streamed rows).
    fn materialize_right_columns(
        &self,
        matched_chunks: &[(usize, UInt64Array, UInt64Array)],
        total_matched_rows: usize,
    ) -> Result<Vec<ArrayRef>> {
        let first_batch_idx = matched_chunks[0].0;
        let single_source = matched_chunks.iter().all(|c| c.0 == first_batch_idx);

        if single_source {
            let combined_right_indices = if matched_chunks.len() == 1 {
                matched_chunks[0].2.clone()
            } else {
                let refs: Vec<&dyn Array> =
                    matched_chunks.iter().map(|c| &c.2 as &dyn Array).collect();
                as_uint64_array(&compute::concat(&refs)?)?.clone()
            };

            return fetch_right_columns_by_idxs(
                &self.buffered_data,
                first_batch_idx,
                &combined_right_indices,
            );
        }

        // Multiple source batches: map each buffered_batch_idx to a
        // contiguous source index. A null sentinel array is prepended as
        // source 0 only when some right index is actually null (an
        // unmatched streamed row inside an otherwise matched chunk);
        // `interleave` walks a null buffer for *every* output row as soon as
        // any input is nullable, so an always-present sentinel would tax the
        // common all-matched case.
        let needs_null_sentinel = matched_chunks
            .iter()
            .any(|(_, _, right)| right.null_count() > 0);
        let source_offset = usize::from(needs_null_sentinel);

        // Map each distinct `buffered_batch_idx` to a contiguous source
        // index for `interleave`. The keys are not opaque: they are
        // positions in `self.buffered_data.batches`, so the key space is
        // dense and bounded by the deque length. A direct-addressed table
        // over `min..=max` resolves every chunk in O(1), with no hashing and
        // no key comparison.
        //
        // The keys a freeze sees are usually a contiguous run, since
        // `scanning_advance` walks the deque in order. The exception is a
        // freeze that straddles a `scanning_reset`: its window wraps (the
        // tail of one streamed row's pass, then the head of the next) and
        // leaves a gap, so the table is sized by the whole group rather than
        // by the sources present. That costs O(group) for O(batch_size) of
        // work -- but only once per pass, against the O(group) of useful
        // work the rest of the pass does, so it stays O(1) amortized per
        // pair. Measured over a 524288-batch group at `batch_size` 8192,
        // a full pass costs 1.17 ms here against 11.25 ms for the hashmap.
        //
        // A linear `position()` scan over `source_batches` is not enough
        // here, even though a freeze holds at most `batch_size` pairs.
        // `pair_streamed_row_with_group` restarts the buffered scan at batch
        // 0 for *every* streamed row of the key group (`scanning_reset`), so
        // the chunk sequence cycles `0,1,..,S-1,0,1,..` and the chunk count
        // is not bounded by the distinct-source count `S`. The scan is then
        // O(chunks * S), and nothing bounds `S`: `SortMergeJoinExec` accepts
        // arbitrary `ExecutionPlan` children, so one emitting tiny batches
        // pushes `S` towards `batch_size`.
        //
        // Measured over 8192 rows in 2048 chunks, against a
        // `HashMap<usize, usize>` built in one pass and read back in a
        // second:
        //
        //   distinct sources |  hashmap  |  linear scan  |  direct table
        //   -----------------+-----------+---------------+--------------
        //                  4 |   19.7 us |       4.5 us  |      4.8 us
        //                 32 |   20.7 us |      13.0 us  |      5.0 us
        //                128 |   23.5 us |      42.7 us  |      5.1 us
        //               1024 |   48.1 us |     281.7 us  |      5.8 us
        //               8192 |  293.3 us |    8347.6 us  |     16.9 us
        //
        // The last row is the degenerate shape a one-row-per-batch child
        // produces: 8192 chunks of a single row each, all from distinct
        // buffered batches. 8.3 ms of index construction, in one freeze.
        //
        // The table ties the scan where the scan is at its best (a handful
        // of sources): both stay in L1 and neither hashes, whereas
        // `std::collections::HashMap` uses SipHash-1-3 and pays several ns
        // of serial latency before each probe begins. Unlike the scan, it
        // stays flat. `source_batches` has to be built regardless
        // (`source_data` is gathered from it), so the table is the only
        // added state, and it is transient: sized to the span this freeze
        // touches rather than held across freezes.
        let (min_batch_idx, max_batch_idx) = matched_chunks
            .iter()
            .fold((usize::MAX, 0usize), |(lo, hi), (batch_idx, _, _)| {
                (lo.min(*batch_idx), hi.max(*batch_idx))
            });
        // Every key indexes the live buffered deque -- this is what keeps
        // the key space dense, and what makes `source_data` below safe.
        debug_assert!(
            max_batch_idx < self.buffered_data.batches.len(),
            "buffered batch index {max_batch_idx} outside the buffered deque"
        );
        // Sentinel for "no source index assigned to this buffered batch yet".
        const UNSEEN: usize = usize::MAX;
        let mut source_of_batch = vec![UNSEEN; max_batch_idx - min_batch_idx + 1];
        let mut source_batches: Vec<usize> = Vec::new();
        let mut interleave_indices: Vec<(usize, usize)> =
            Vec::with_capacity(total_matched_rows);
        for (batch_idx, _, right) in matched_chunks {
            let slot = &mut source_of_batch[batch_idx - min_batch_idx];
            if *slot == UNSEEN {
                *slot = source_batches.len();
                source_batches.push(*batch_idx);
            }
            let source = *slot + source_offset;
            if right.null_count() == 0 {
                // Hot path: no per-row null check, and `values()` avoids
                // the bounds check `value(i)` would repeat.
                interleave_indices
                    .extend(right.values().iter().map(|&idx| (source, idx as usize)));
            } else {
                for i in 0..right.len() {
                    if right.is_null(i) {
                        interleave_indices.push((0, 0));
                    } else {
                        interleave_indices.push((source, right.value(i) as usize));
                    }
                }
            }
        }

        let num_right_cols = self.buffered_schema.fields().len();

        // Read each source batch once (spilled batches require disk I/O).
        let source_data: Vec<&RecordBatch> = source_batches
            .iter()
            .map(|&idx| match &self.buffered_data.batches[idx].batch {
                BufferedBatchState::InMemory(batch) => Ok(batch),
                BufferedBatchState::Spilled(_) => internal_err!(
                    "Buffered batch should have been unspilled before fetching columns"
                ),
            })
            .collect::<Result<_>>()?;

        // One single-row null array per column, built up front so the
        // per-column `source_arrays` can borrow them.
        let null_arrays: Vec<ArrayRef> = if needs_null_sentinel {
            self.buffered_schema
                .fields()
                .iter()
                .map(|f| new_null_array(f.data_type(), 1))
                .collect()
        } else {
            vec![]
        };

        let mut source_arrays: Vec<&dyn Array> =
            Vec::with_capacity(source_data.len() + source_offset);
        let mut right_columns = Vec::with_capacity(num_right_cols);
        for col_idx in 0..num_right_cols {
            source_arrays.clear();
            source_arrays.extend(null_arrays.get(col_idx).map(|a| a.as_ref()));
            source_arrays.extend(source_data.iter().map(|d| d.column(col_idx).as_ref()));

            right_columns.push(interleave(&source_arrays, &interleave_indices)?);
        }

        Ok(right_columns)
    }

    fn filter_joined_batch(&mut self) -> Result<RecordBatch> {
        // Metadata should be aligned before processing
        self.joined_record_batches
            .filter_metadata
            .debug_assert_metadata_aligned();

        let record_batch = self.joined_record_batches.concat_batches(&self.schema)?;
        let (mut out_indices, mut out_mask, mut batch_ids) =
            self.joined_record_batches.filter_metadata.finish_metadata();
        let default_batch_ids = vec![0; record_batch.num_rows()];

        // If only nulls come in and indices sizes doesn't match with expected record batch count
        // generate missing indices
        // Happens for null joined batches for Full Join
        if out_indices.null_count() == out_indices.len()
            && out_indices.len() != record_batch.num_rows()
        {
            out_mask = BooleanArray::from(vec![None; record_batch.num_rows()]);
            out_indices = UInt64Array::from(vec![None; record_batch.num_rows()]);
            batch_ids = &default_batch_ids;
        }

        // After potential reconstruction, metadata should align with batch row count
        debug_assert_eq!(
            out_indices.len(),
            record_batch.num_rows(),
            "out_indices length should match record_batch row count"
        );
        debug_assert_eq!(
            out_mask.len(),
            record_batch.num_rows(),
            "out_mask length should match record_batch row count (unless empty)"
        );
        debug_assert_eq!(
            batch_ids.len(),
            record_batch.num_rows(),
            "batch_ids length should match record_batch row count"
        );

        if out_mask.is_empty() {
            self.joined_record_batches
                .clear_batches(&self.schema, self.batch_size);
            return Ok(record_batch);
        }

        // Validate inputs to get_corrected_filter_mask
        debug_assert_eq!(
            out_indices.len(),
            out_mask.len(),
            "out_indices and out_mask must have same length for get_corrected_filter_mask"
        );
        debug_assert_eq!(
            batch_ids.len(),
            out_mask.len(),
            "batch_ids and out_mask must have same length for get_corrected_filter_mask"
        );

        let maybe_corrected_mask = get_corrected_filter_mask(
            self.join_type,
            &out_indices,
            batch_ids,
            &out_mask,
            record_batch.num_rows(),
        );

        let corrected_mask = if let Some(ref filtered_join_mask) = maybe_corrected_mask {
            filtered_join_mask
        } else {
            &out_mask
        };

        self.filter_record_batch_by_join_type(&record_batch, corrected_mask)
    }

    fn filter_record_batch_by_join_type(
        &mut self,
        record_batch: &RecordBatch,
        corrected_mask: &BooleanArray,
    ) -> Result<RecordBatch> {
        let filtered_record_batch = filter_record_batch_by_join_type(
            record_batch,
            corrected_mask,
            self.join_type,
            &self.schema,
            &self.buffered_schema,
        )?;

        self.joined_record_batches
            .clear(&self.schema, self.batch_size);

        Ok(filtered_record_batch)
    }
}

/// Materialize left (streamed) columns using slice or take.
fn materialize_left_columns(
    batch: &RecordBatch,
    indices: &UInt64Array,
) -> Result<Vec<ArrayRef>> {
    if let Some(range) = is_contiguous_range(indices) {
        Ok(batch.slice(range.start, range.len()).columns().to_vec())
    } else {
        Ok(take_arrays(batch.columns(), indices, None)?)
    }
}

fn create_unmatched_columns(schema: &SchemaRef, size: usize) -> Vec<ArrayRef> {
    schema
        .fields()
        .iter()
        .map(|f| new_null_array(f.data_type(), size))
        .collect::<Vec<_>>()
}

fn produce_buffered_null_batch(
    schema: &SchemaRef,
    streamed_schema: &SchemaRef,
    buffered_indices: &PrimitiveArray<UInt64Type>,
    buffered_batch: &BufferedBatch,
) -> Result<Option<RecordBatch>> {
    if buffered_indices.is_empty() {
        return Ok(None);
    }

    // Take buffered (right) columns
    let right_columns =
        fetch_right_columns_from_batch_by_idxs(buffered_batch, buffered_indices)?;

    // Create null streamed (left) columns
    let mut left_columns = streamed_schema
        .fields()
        .iter()
        .map(|f| new_null_array(f.data_type(), buffered_indices.len()))
        .collect::<Vec<_>>();

    left_columns.extend(right_columns);

    Ok(Some(RecordBatch::try_new(
        Arc::clone(schema),
        left_columns,
    )?))
}

/// Checks if a `UInt64Array` contains a contiguous ascending range (e.g. \[3,4,5,6\]).
/// Returns `Some(start..start+len)` if so, `None` otherwise.
/// This allows replacing an O(n) `take` with an O(1) `slice`.
#[inline]
fn is_contiguous_range(indices: &UInt64Array) -> Option<Range<usize>> {
    if indices.is_empty() || indices.null_count() > 0 {
        return None;
    }
    let values = indices.values();
    let start = values[0];
    let len = values.len() as u64;
    // Quick rejection: if last element doesn't match expected, not contiguous
    if values[values.len() - 1] != start + len - 1 {
        return None;
    }
    // Verify every element is sequential (handles duplicates and gaps)
    for i in 1..values.len() {
        if values[i] != start + i as u64 {
            return None;
        }
    }
    Some(start as usize..(start + len) as usize)
}

/// Get `buffered_indices` rows for `buffered_data[buffered_batch_idx]` by specific column indices
#[inline(always)]
fn fetch_right_columns_by_idxs(
    buffered_data: &BufferedData,
    buffered_batch_idx: usize,
    buffered_indices: &UInt64Array,
) -> Result<Vec<ArrayRef>> {
    fetch_right_columns_from_batch_by_idxs(
        &buffered_data.batches[buffered_batch_idx],
        buffered_indices,
    )
}

#[inline(always)]
fn fetch_right_columns_from_batch_by_idxs(
    buffered_batch: &BufferedBatch,
    buffered_indices: &UInt64Array,
) -> Result<Vec<ArrayRef>> {
    match &buffered_batch.batch {
        BufferedBatchState::InMemory(batch) => {
            if let Some(range) = is_contiguous_range(buffered_indices) {
                Ok(batch.slice(range.start, range.len()).columns().to_vec())
            } else {
                Ok(take_arrays(batch.columns(), buffered_indices, None)?)
            }
        }
        BufferedBatchState::Spilled(_) => {
            internal_err!(
                "Buffered batch should have been unspilled before fetching columns"
            )
        }
    }
}

/// Buffered data contains all buffered batches with one unique join key
#[derive(Debug, Default)]
pub(super) struct BufferedData {
    /// Buffered batches with the same key
    pub batches: VecDeque<BufferedBatch>,
    /// current scanning batch index used by the group-scan phase
    pub scanning_batch_idx: usize,
    /// current scanning offset used by the group-scan phase
    pub scanning_offset: usize,
}

impl BufferedData {
    pub fn head_batch(&self) -> &BufferedBatch {
        self.batches.front().unwrap()
    }

    pub fn tail_batch(&self) -> &BufferedBatch {
        self.batches.back().unwrap()
    }

    pub fn tail_batch_mut(&mut self) -> &mut BufferedBatch {
        self.batches.back_mut().unwrap()
    }

    pub fn has_buffered_rows(&self) -> bool {
        self.batches.iter().any(|batch| !batch.range.is_empty())
    }

    pub fn scanning_reset(&mut self) {
        self.scanning_batch_idx = 0;
        self.scanning_offset = 0;
    }

    pub fn scanning_advance(&mut self) {
        self.scanning_offset += 1;
        while !self.scanning_finished() && self.scanning_batch_finished() {
            self.scanning_batch_idx += 1;
            self.scanning_offset = 0;
        }
    }

    pub fn scanning_batch(&self) -> &BufferedBatch {
        &self.batches[self.scanning_batch_idx]
    }

    pub fn scanning_batch_mut(&mut self) -> &mut BufferedBatch {
        &mut self.batches[self.scanning_batch_idx]
    }

    pub fn scanning_idx(&self) -> usize {
        self.scanning_batch().range.start + self.scanning_offset
    }

    pub fn scanning_batch_finished(&self) -> bool {
        self.scanning_offset == self.scanning_batch().range.len()
    }

    pub fn scanning_finished(&self) -> bool {
        self.scanning_batch_idx == self.batches.len()
    }
}

/// Build the `BatchCoalescer` used for staging join output.
///
/// `biggest_coalesce_batch_size` lets batches larger than half the target
/// pass through without being copied into the coalescer's buffer.
fn new_output_coalescer(schema: SchemaRef, batch_size: usize) -> BatchCoalescer {
    BatchCoalescer::new(schema, batch_size)
        .with_biggest_coalesce_batch_size(Some(batch_size / 2))
}

/// Evaluate the join key expressions against `batch`.
fn join_arrays(
    batch: &RecordBatch,
    on_column: &[PhysicalExprRef],
) -> Result<Vec<ArrayRef>> {
    let num_rows = batch.num_rows();
    on_column
        .iter()
        .map(|c| c.evaluate(batch)?.into_array(num_rows))
        .collect()
}
