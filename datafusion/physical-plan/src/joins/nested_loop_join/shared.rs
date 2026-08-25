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

//! Shared types and utilities for nested loop join streams.

use std::ops::BitOr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::joins::SharedBitmapBuilder;
use crate::joins::utils::{
    BuildProbeJoinMetrics, ColumnIndex, JoinFilter, OnceAsync, OnceFut,
};
use crate::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricType, RatioMetrics, SpillMetrics,
};
use crate::{ExecutionPlan, SendableRecordBatchStream};

use arrow::array::{
    Array, BooleanArray, BooleanBufferBuilder, RecordBatchOptions, UInt32Array,
    UInt64Array, new_null_array,
};
use arrow::buffer::BooleanBuffer;
use arrow::compute::{concat_batches, filter, filter_record_batch, not, take};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_schema::DataType;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{
    DataFusionError, JoinSide, Result, ScalarValue, internal_datafusion_err, internal_err,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{SpillFile, TaskContext};
use datafusion_expr::JoinType;

use futures::TryStreamExt;
use parking_lot::Mutex;

use crate::spill::replayable_spill_input::ReplayableStreamSource;
use crate::spill::spill_manager::SpillManager;

/// Left (build-side) data
pub(super) struct JoinLeftData {
    /// Build-side data collected to single batch
    pub(super) batch: RecordBatch,
    /// Shared bitmap builder for visited left indices
    bitmap: SharedBitmapBuilder,
    /// Counter of running probe-threads, potentially able to update `bitmap`
    probe_threads_counter: AtomicUsize,
    /// Memory reservation for tracking batch and bitmap
    /// Cleared on `JoinLeftData` drop
    /// reservation is cleared on Drop
    #[expect(dead_code)]
    reservation: MemoryReservation,
}

impl JoinLeftData {
    pub(super) fn new(
        batch: RecordBatch,
        bitmap: SharedBitmapBuilder,
        probe_threads_counter: AtomicUsize,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            batch,
            bitmap,
            probe_threads_counter,
            reservation,
        }
    }

    pub(super) fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub(super) fn bitmap(&self) -> &SharedBitmapBuilder {
        &self.bitmap
    }

    /// Decrements counter of running threads, and returns `true`
    /// if caller is the last running thread
    pub(super) fn report_probe_completed(&self) -> bool {
        self.probe_threads_counter.fetch_sub(1, Ordering::Relaxed) == 1
    }

    /// Slice the left batch and bitmap to range `[start_idx, end_idx)`.
    /// The range uses bit indices, not byte indices.
    pub(super) fn slice_batch_and_bitmap(
        &self,
        start_idx: usize,
        end_idx: usize,
    ) -> (RecordBatch, BooleanArray) {
        let left_batch_sliced = self.batch.slice(start_idx, end_idx - start_idx);

        // Can this be more efficient?
        let mut bitmap_sliced = BooleanBufferBuilder::new(end_idx - start_idx);
        bitmap_sliced.append_n(end_idx - start_idx, false);
        let bitmap = self.bitmap().lock();
        for i in start_idx..end_idx {
            assert!(
                i - start_idx < bitmap_sliced.capacity(),
                "DBG: {start_idx}, {end_idx}"
            );
            bitmap_sliced.set_bit(i - start_idx, bitmap.get_bit(i));
        }
        let bitmap_sliced = BooleanArray::new(bitmap_sliced.finish(), None);

        (left_batch_sliced, bitmap_sliced)
    }
}

/// Asynchronously collect input into a single batch, and creates `JoinLeftData` from it
pub(super) async fn collect_left_input(
    stream: SendableRecordBatchStream,
    join_metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
    with_visited_left_side: bool,
    probe_threads_count: usize,
) -> Result<JoinLeftData> {
    let schema = stream.schema();

    // Load all batches and count the rows
    let (batches, metrics, reservation) = stream
        .try_fold(
            (Vec::new(), join_metrics, reservation),
            |(mut batches, metrics, reservation), batch| async {
                let batch_size = batch.get_array_memory_size();
                // Reserve memory for incoming batch
                reservation.try_grow(batch_size)?;
                // Update metrics
                metrics.build_mem_used.add(batch_size);
                metrics.build_input_batches.add(1);
                metrics.build_input_rows.add(batch.num_rows());
                // Push batch to output
                batches.push(batch);
                Ok((batches, metrics, reservation))
            },
        )
        .await?;

    let merged_batch = concat_batches(&schema, &batches)?;

    // Reserve memory for visited_left_side bitmap if required by join type
    let visited_left_side = if with_visited_left_side {
        let n_rows = merged_batch.num_rows();
        let buffer_size = n_rows.div_ceil(8);
        reservation.try_grow(buffer_size)?;
        metrics.build_mem_used.add(buffer_size);

        let mut buffer = BooleanBufferBuilder::new(n_rows);
        buffer.append_n(n_rows, false);
        buffer
    } else {
        BooleanBufferBuilder::new(0)
    };

    Ok(JoinLeftData::new(
        merged_batch,
        Mutex::new(visited_left_side),
        AtomicUsize::new(probe_threads_count),
        reservation,
    ))
}

/// Shared data for the left-side spill fallback.
///
/// When the in-memory `OnceFut` path fails with OOM, the first partition
/// spills the entire left side to disk. This struct holds the spill file
/// reference so other partitions can read from the same file.
pub(super) struct LeftSpillData {
    /// SpillManager used to read the spill file (has the left schema)
    spill_manager: SpillManager,
    /// The spill file containing all left-side batches
    spill_file: Arc<dyn SpillFile>,
    /// Left-side schema
    schema: SchemaRef,
}

/// Tracks the state of the memory-limited spill fallback for NLJ.
///
/// The NLJ always starts with the standard OnceFut path. If the in-memory
/// load fails with OOM and conditions allow, the operator falls back to a
/// multi-pass strategy where left data is loaded in chunks and the right
/// side is spilled to disk.
pub(super) enum SpillState {
    /// Fallback is not possible (e.g., join type requires global right bitmap,
    /// or disk manager is disabled). OOM errors will propagate as-is.
    Disabled,

    /// Fallback is possible but not yet triggered. The operator is still
    /// attempting the standard OnceFut path. Holds the context needed to
    /// initiate fallback if OOM occurs.
    Pending {
        /// Left child plan for re-execution
        left_plan: Arc<dyn ExecutionPlan>,
        /// TaskContext for re-execution and SpillManager creation
        task_context: Arc<TaskContext>,
        /// Shared OnceAsync for left-side spill data. The first partition
        /// to initiate fallback spills the left side; others share the file.
        left_spill_data: Arc<OnceAsync<LeftSpillData>>,
    },

    /// Fallback has been triggered. Left data is being loaded in chunks
    /// and the right side is spilled to disk for re-scanning.
    Active(Box<SpillStateActive>),
}

impl SpillState {
    /// Returns true if the stream is operating in memory-limited mode.
    pub(super) fn is_active(&self) -> bool {
        matches!(self, SpillState::Active(_))
    }

    /// Check if the stream can fall back to memory-limited mode on this error.
    pub(super) fn can_fallback_to_spill(&self, error: &DataFusionError) -> bool {
        matches!(self, SpillState::Pending { .. })
            && matches!(error.find_root(), DataFusionError::ResourcesExhausted(_))
    }
}

/// State for active memory-limited spill execution.
/// Boxed inside [`SpillState::Active`] to reduce enum size.
pub(super) struct SpillStateActive {
    /// Shared future for left-side spill data. All partitions wait on
    /// the same future — the first to poll triggers the actual spill.
    pub(super) left_spill_fut: OnceFut<LeftSpillData>,
    /// Left input stream for incremental chunk reading (from spill file).
    /// None until `left_spill_fut` resolves.
    pub(super) left_stream: Option<SendableRecordBatchStream>,
    /// Left-side schema (set once `left_spill_fut` resolves)
    left_schema: Option<SchemaRef>,
    /// Memory reservation for left-side buffering
    pub(super) reservation: MemoryReservation,
    /// Accumulated left batches for the current chunk
    pub(super) pending_batches: Vec<RecordBatch>,
    /// Right input that spills on the first pass and replays from spill later.
    right_input: ReplayableStreamSource,
    /// Per-batch accumulated right bitmaps across all left chunks.
    /// Index = right batch sequence number (0-based, non-empty batches only).
    /// Only populated for joins that track right-side matches
    /// (`should_track_unmatched_right` in the materializing stream, or a
    /// right-side SAM join).
    global_right_bitmaps: Vec<BooleanBuffer>,
    /// Separate reservation for `global_right_bitmaps`. These buffers live
    /// for the full operator lifetime (not per-chunk), so they must be
    /// tracked separately from `reservation`, which gets `resize(0)`-ed
    /// between chunks.
    global_right_bitmaps_reservation: MemoryReservation,
    /// Current right batch sequence index within the current pass.
    right_batch_index: usize,
}

impl SpillStateActive {
    /// Open the shared left spill file as a stream and retain its schema.
    pub(super) fn set_left_spill_data(
        &mut self,
        spill_data: &LeftSpillData,
    ) -> Result<()> {
        self.left_stream = Some(
            spill_data
                .spill_manager
                .read_spill_as_stream(Arc::clone(&spill_data.spill_file), None)?,
        );
        self.left_schema = Some(Arc::clone(&spill_data.schema));
        Ok(())
    }

    /// Open a new right-side pass and reset its batch sequence index.
    pub(super) fn open_right_pass(&mut self) -> Result<SendableRecordBatchStream> {
        self.right_batch_index = 0;
        self.right_input.open_pass()
    }

    /// Merge a per-pass right bitmap into the global accumulator at the
    /// given batch index, growing the dedicated reservation when seeing
    /// a batch index for the first time.
    ///
    /// On first encounter of `idx`, the bitmap is stored as-is and its
    /// size is reserved. On subsequent encounters (later left chunk
    /// passes over the same right batch), the existing entry is OR-merged
    /// with `values`. Because `bitor` produces a buffer of the same bit
    /// length, the reservation does not need to be adjusted on merge.
    fn merge_current_right_bitmap(&mut self, idx: usize, values: BooleanBuffer) {
        if idx >= self.global_right_bitmaps.len() {
            // First encounter of this right batch — account memory and store.
            // The bitmap has one bit per right row, so for very large right
            // inputs the accumulated size can be non-negligible (e.g.,
            // 1M rows ≈ 125 KB per batch).
            // Use infallible `grow` because we must accept the bitmap to
            // preserve correctness — the fallback path has no other recourse.
            let bytes = values.len().div_ceil(8);
            self.global_right_bitmaps_reservation.grow(bytes);
            self.global_right_bitmaps.push(values);
        } else {
            // Subsequent left chunk pass — OR merge. Same bit length, so
            // no reservation adjustment is needed.
            self.global_right_bitmaps[idx] =
                self.global_right_bitmaps[idx].bitor(&values);
        }
    }

    /// Merge a completed right-batch bitmap into the global accumulator and
    /// advance the right batch index.
    pub(super) fn handoff_completed_right_bitmap(&mut self, bitmap: BooleanArray) {
        let (values, _nulls) = bitmap.into_parts();
        let idx = self.right_batch_index;
        self.merge_current_right_bitmap(idx, values);
        self.right_batch_index += 1;
    }

    /// Look up the global right bitmap for the given batch index, returning an
    /// all-unset bitmap if the batch was never seen.
    fn global_right_bitmap_for_batch(&self, idx: usize, num_rows: usize) -> BooleanArray {
        // Build BooleanArray from the global bitmap.
        if idx < self.global_right_bitmaps.len() {
            BooleanArray::new(self.global_right_bitmaps[idx].clone(), None)
        } else {
            // Batch never seen — treat all rows as unmatched
            BooleanArray::new(BooleanBuffer::new_unset(num_rows), None)
        }
    }

    /// Return the global bitmap for the next right batch and advance its index.
    pub(super) fn next_global_right_bitmap(&mut self, num_rows: usize) -> BooleanArray {
        let idx = self.right_batch_index;
        self.right_batch_index += 1;
        self.global_right_bitmap_for_batch(idx, num_rows)
    }
}

/// Outcome of buffering one left batch during memory-limited chunk loading.
pub(super) enum LeftBufferBatchDecision {
    /// Batch buffered; continue polling the left stream.
    Continue,
    /// Memory limit reached with existing data; the current chunk is full.
    ChunkFull,
}

/// Buffer one left batch into the current chunk, updating build metrics.
pub(super) fn buffer_left_batch_in_chunk(
    reservation: &mut MemoryReservation,
    pending_batches: &mut Vec<RecordBatch>,
    join_metrics: &BuildProbeJoinMetrics,
    batch: RecordBatch,
) -> LeftBufferBatchDecision {
    let batch_rows = batch.num_rows();
    let batch_size = batch.get_array_memory_size();
    let can_grow = reservation.try_grow(batch_size).is_ok();

    if !can_grow && !pending_batches.is_empty() {
        // Memory limit reached and we already have data.
        // Push this batch into pending (it's already in memory)
        // and stop buffering for this chunk.
        pending_batches.push(batch);
        LeftBufferBatchDecision::ChunkFull
    } else {
        if !can_grow {
            // No pending batches yet — we must accept this batch
            // to make progress, even if it exceeds the budget.
            reservation.grow(batch_size);
        }

        join_metrics.build_mem_used.add(batch_size);
        join_metrics.build_input_batches.add(1);
        join_metrics.build_input_rows.add(batch_rows);
        pending_batches.push(batch);
        LeftBufferBatchDecision::Continue
    }
}

/// A finalized left chunk ready for probing, plus a fresh right-side pass.
pub(super) struct FinalizedLeftChunk {
    pub(super) left_data: JoinLeftData,
    pub(super) right_pass: SendableRecordBatchStream,
}

/// Concatenate pending left batches, allocate visited bitmap if needed, and
/// open a new right-side pass for probing.
pub(super) fn finalize_buffered_left_chunk(
    active: &mut SpillStateActive,
    join_metrics: &BuildProbeJoinMetrics,
    track_left_matches: bool,
) -> Result<FinalizedLeftChunk> {
    let merged_batch = concat_batches(
        active
            .left_schema
            .as_ref()
            .expect("left_schema must be set"),
        &active.pending_batches,
    )?;
    active.pending_batches.clear();

    // Build visited bitmap if needed for this join type
    let n_rows = merged_batch.num_rows();
    let visited_left_side = if track_left_matches {
        let buffer_size = n_rows.div_ceil(8);
        // Use infallible grow for bitmap — it's small
        active.reservation.grow(buffer_size);
        join_metrics.build_mem_used.add(buffer_size);
        let mut buffer = BooleanBufferBuilder::new(n_rows);
        buffer.append_n(n_rows, false);
        buffer
    } else {
        BooleanBufferBuilder::new(0)
    };

    // Create an empty reservation for JoinLeftData's RAII field.
    // The actual memory tracking is managed by the Active state's reservation.
    let dummy_reservation = active.reservation.new_empty();

    let left_data = JoinLeftData::new(
        merged_batch,
        Mutex::new(visited_left_side),
        // In memory-limited mode, only 1 probe thread per chunk
        AtomicUsize::new(1),
        dummy_reservation,
    );

    let right_pass = active.open_right_pass()?;

    Ok(FinalizedLeftChunk {
        left_data,
        right_pass,
    })
}

/// Switch from the standard OnceFut path to memory-limited mode.
///
/// Uses the shared `left_spill_data` OnceAsync so that only the first
/// partition to reach this point re-executes the left child and spills
/// it to disk. Other partitions share the same spill file.
pub(super) fn initiate_spill_fallback(
    spill_state: &mut SpillState,
    metrics: &NestedLoopJoinMetrics,
    right_data: &mut Option<SendableRecordBatchStream>,
) -> Result<()> {
    // Take ownership of Pending state
    let SpillState::Pending {
        left_plan,
        task_context: context,
        left_spill_data,
    } = std::mem::replace(spill_state, SpillState::Disabled)
    else {
        return internal_err!("initiate_fallback called in non-Pending spill state");
    };

    // Use OnceAsync to ensure only the first partition spills the left
    // side. Other partitions will get the same OnceFut that resolves
    // to the shared spill file.
    let left_spill_fut = left_spill_data.try_once(|| {
        let plan = Arc::clone(&left_plan);
        let ctx = Arc::clone(&context);
        let spill_metrics = metrics.spill_metrics.clone();
        Ok(async move {
            let mut stream = plan.execute(0, Arc::clone(&ctx))?;
            let schema = stream.schema();
            let left_spill_manager =
                SpillManager::new(ctx.runtime_env(), spill_metrics, Arc::clone(&schema))
                    .with_compression_type(ctx.session_config().spill_compression());

            let result = left_spill_manager
                .spill_record_batch_stream_and_return_max_batch_memory(
                    &mut stream,
                    "NestedLoopJoin left spill",
                )
                .await?;

            match result {
                Some((file, _max_batch_memory)) => Ok(LeftSpillData {
                    spill_manager: left_spill_manager,
                    spill_file: file,
                    schema,
                }),
                None => internal_err!("Left side produced no data to spill"),
            }
        })
    })?;

    // Create reservation with can_spill for fair memory allocation
    let reservation = MemoryConsumer::new("NestedLoopJoinLoad[fallback]".to_string())
        .with_can_spill(true)
        .register(context.memory_pool());

    // Separate reservation for the global right bitmaps. These buffers
    // persist across all left chunks, whereas `reservation` is reset
    // between chunks via `resize(0)`.
    let global_right_bitmaps_reservation =
        MemoryConsumer::new("NestedLoopJoinGlobalRightBitmaps".to_string())
            .register(context.memory_pool());

    // Create SpillManager for right-side spilling
    let right_schema = right_data
        .as_ref()
        .expect("right_data must be present before fallback")
        .schema();
    let right_data_stream = right_data
        .take()
        .expect("right_data must be present before fallback");
    let right_spill_manager = SpillManager::new(
        context.runtime_env(),
        metrics.spill_metrics.clone(),
        right_schema,
    )
    .with_compression_type(context.session_config().spill_compression());

    *spill_state = SpillState::Active(Box::new(SpillStateActive {
        left_spill_fut,
        left_stream: None,
        left_schema: None,
        reservation,
        pending_batches: Vec::new(),
        right_input: ReplayableStreamSource::new(
            right_data_stream,
            right_spill_manager,
            "NestedLoopJoin right spill",
        ),
        global_right_bitmaps: Vec::new(),
        global_right_bitmaps_reservation,
        right_batch_index: 0,
    }));

    Ok(())
}

pub(super) struct NestedLoopJoinMetrics {
    /// Join execution metrics
    pub(super) join_metrics: BuildProbeJoinMetrics,
    /// Selectivity of the join: output_rows / (left_rows * right_rows)
    pub(super) selectivity: RatioMetrics,
    /// Spill metrics for memory-limited execution
    pub(super) spill_metrics: SpillMetrics,
}

impl NestedLoopJoinMetrics {
    pub(super) fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            join_metrics: BuildProbeJoinMetrics::new(partition, metrics),
            selectivity: MetricBuilder::new(metrics)
                .with_type(MetricType::Summary)
                .ratio_metrics("selectivity", partition),
            spill_metrics: SpillMetrics::new(metrics, partition),
        }
    }
}

// ==== Utilities ====

/// Apply the join filter between:
/// (l_index th row in left buffer) x (right batch)
/// Returns a bitmap, with successfully joined indices set to true
pub(super) fn apply_filter_to_row_join_batch(
    left_batch: &RecordBatch,
    l_index: usize,
    right_batch: &RecordBatch,
    filter: &JoinFilter,
) -> Result<BooleanArray> {
    debug_assert!(left_batch.num_rows() != 0 && right_batch.num_rows() != 0);

    let intermediate_batch = if filter.schema.fields().is_empty() {
        // If filter is constant (e.g. literal `true`), empty batch can be used
        // in the later filter step.
        create_record_batch_with_empty_schema(
            Arc::new((*filter.schema).clone()),
            right_batch.num_rows(),
        )?
    } else {
        build_row_join_batch(
            &filter.schema,
            left_batch,
            l_index,
            right_batch,
            None,
            &filter.column_indices,
            JoinSide::Left,
        )?
        .ok_or_else(|| internal_datafusion_err!("This function assume input batch is not empty, so the intermediate batch can't be empty too"))?
    };

    let filter_result = filter
        .expression()
        .evaluate(&intermediate_batch)?
        .into_array(intermediate_batch.num_rows())?;
    let filter_arr = as_boolean_array(&filter_result)?;

    // Convert boolean array with potential nulls into a unified mask bitmap
    let bitmap_combined = boolean_mask_from_filter(filter_arr);

    Ok(bitmap_combined)
}

/// Update SAM match bitmaps after probing one left row against a right batch.
pub(super) fn update_sam_matched_bitmaps(
    left_data: &JoinLeftData,
    l_index: usize,
    r_matched_bitmap: &BooleanArray,
    track_left_matches: bool,
    right_batch_matched: &mut Option<BooleanArray>,
) -> Result<()> {
    if track_left_matches && r_matched_bitmap.has_true() {
        let mut bitmap = left_data.bitmap().lock();
        bitmap.set_bit(l_index, true);
    }

    if right_batch_matched.is_some() {
        let taken =
            std::mem::take(right_batch_matched).expect("right bitmap must be present");
        let (buf, nulls) = taken.into_parts();
        debug_assert!(nulls.is_none());
        let updated = buf.bitor(r_matched_bitmap.values());
        *right_batch_matched = Some(BooleanArray::new(updated, None));
    }

    Ok(())
}

/// Probe `[l_start_index, l_start_index + l_row_count)` against `right_batch`,
/// updating SAM bitmaps only (no row-pair materialization).
pub(super) fn probe_sam_left_range(
    left_data: &JoinLeftData,
    right_batch: &RecordBatch,
    l_start_index: usize,
    l_row_count: usize,
    join_filter: Option<&JoinFilter>,
    track_left_matches: bool,
    right_batch_matched: &mut Option<BooleanArray>,
) -> Result<()> {
    let right_rows = right_batch.num_rows();
    let total_rows = l_row_count * right_rows;

    let left_indices: UInt32Array = UInt32Array::from_iter_values(
        (0..l_row_count)
            .flat_map(|i| std::iter::repeat_n((l_start_index + i) as u32, right_rows)),
    );
    let right_indices: UInt32Array = UInt32Array::from_iter_values(
        (0..l_row_count).flat_map(|_| 0..right_rows as u32),
    );

    debug_assert!(
        left_indices.len() == right_indices.len() && right_indices.len() == total_rows,
        "The length of cartesian product should be (left_size * right_size)",
    );

    let bitmap_combined = if let Some(filter) = join_filter {
        let intermediate_batch = if filter.schema.fields().is_empty() {
            create_record_batch_with_empty_schema(
                Arc::new((*filter.schema).clone()),
                total_rows,
            )?
        } else {
            let mut filter_columns: Vec<Arc<dyn Array>> =
                Vec::with_capacity(filter.column_indices().len());
            for column_index in filter.column_indices() {
                let array = if column_index.side == JoinSide::Left {
                    let col = left_data.batch().column(column_index.index);
                    take(col.as_ref(), &left_indices, None)?
                } else {
                    let col = right_batch.column(column_index.index);
                    take(col.as_ref(), &right_indices, None)?
                };
                filter_columns.push(array);
            }

            RecordBatch::try_new(Arc::new((*filter.schema).clone()), filter_columns)?
        };

        let filter_result = filter
            .expression()
            .evaluate(&intermediate_batch)?
            .into_array(intermediate_batch.num_rows())?;
        let filter_arr = as_boolean_array(&filter_result)?;
        boolean_mask_from_filter(filter_arr)
    } else {
        BooleanArray::from(vec![true; total_rows])
    };

    let mut left_bitmap = if track_left_matches {
        Some(left_data.bitmap().lock())
    } else {
        None
    };

    let mut local_right_bitmap = if right_batch_matched.is_some() {
        let mut current_right_batch_bitmap = BooleanBufferBuilder::new(right_rows);
        current_right_batch_bitmap.append_n(right_rows, false);
        Some(current_right_batch_bitmap)
    } else {
        None
    };

    for (i, is_matched) in bitmap_combined.iter().enumerate() {
        let is_matched = is_matched.ok_or_else(|| {
            internal_datafusion_err!("Must be Some after the previous combining step")
        })?;

        let l_index = l_start_index + i / right_rows;
        let r_index = i % right_rows;

        if let Some(bitmap) = left_bitmap.as_mut()
            && is_matched
        {
            bitmap.set_bit(l_index, true);
        }

        if let Some(bitmap) = local_right_bitmap.as_mut()
            && is_matched
        {
            bitmap.set_bit(r_index, true);
        }
    }

    if let Some(_right_bitmap) = right_batch_matched.as_mut() {
        let global_right_bitmap =
            std::mem::take(right_batch_matched).expect("right bitmap must be present");
        let (buf, nulls) = global_right_bitmap.into_parts();
        debug_assert!(nulls.is_none());
        let current_right_bitmap = local_right_bitmap
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "Should be Some if the current join type requires right bitmap"
                )
            })?
            .finish();
        let updated_global_right_bitmap = buf.bitor(&current_right_bitmap);
        *right_batch_matched = Some(BooleanArray::new(updated_global_right_bitmap, None));
    }

    Ok(())
}

/// Convert a boolean filter array into a unified mask bitmap.
///
/// Caution: The filter result is NOT a bitmap; it contains true/false/null values.
/// For example, `1 < NULL` evaluates to NULL. Therefore, we must combine (AND)
/// the boolean array with its null bitmap to construct a unified bitmap.
#[inline]
pub(super) fn boolean_mask_from_filter(filter_arr: &BooleanArray) -> BooleanArray {
    let (values, nulls) = filter_arr.clone().into_parts();
    match nulls {
        Some(nulls) => BooleanArray::new(nulls.inner() & &values, None),
        None => BooleanArray::new(values, None),
    }
}

/// This function performs the following steps:
/// 1. Apply filter to probe-side batch
/// 2. Broadcast the left row (build_side_batch\[build_side_index\]) to the
///    filtered probe-side batch
/// 3. Concat them together according to `col_indices`, and return the result
///    (None if the result is empty)
///
/// Example:
/// build_side_batch:
/// a
/// ----
/// 1
/// 2
/// 3
///
/// # 0 index element in the build_side_batch (that is `1`) will be used
/// build_side_index: 0
///
/// probe_side_batch:
/// b
/// ----
/// 10
/// 20
/// 30
/// 40
///
/// # After applying it, only index 1 and 3 elements in probe_side_batch will be
/// # kept
/// probe_side_filter:
/// false
/// true
/// false
/// true
///
///
/// # Projections to the build/probe side batch, to construct the output batch
/// col_indices:
/// [(left, 0), (right, 0)]
///
/// build_side: left
///
/// ====
/// Result batch:
/// a b
/// ----
/// 1 20
/// 1 40
pub(super) fn build_row_join_batch(
    output_schema: &Schema,
    build_side_batch: &RecordBatch,
    build_side_index: usize,
    probe_side_batch: &RecordBatch,
    probe_side_filter: Option<BooleanArray>,
    // See `NestedLoopJoinStream::column_indices` or
    // `SemiAntiMarkNestedLoopJoinStream::column_indices` for more detail
    col_indices: &[ColumnIndex],
    // If the build side is left or right, used to interpret the side information
    // in `col_indices`
    build_side: JoinSide,
) -> Result<Option<RecordBatch>> {
    debug_assert_ne!(build_side, JoinSide::None);

    // TODO(perf): since the output might be projection of right batch, this
    // filtering step is more efficient to be done inside the column_index loop
    let filtered_probe_batch = if let Some(filter) = probe_side_filter {
        &filter_record_batch(probe_side_batch, &filter)?
    } else {
        probe_side_batch
    };

    if filtered_probe_batch.num_rows() == 0 {
        return Ok(None);
    }

    // Edge case: downstream operator does not require any columns from this NLJ,
    // so allow an empty projection.
    // Example:
    //  SELECT DISTINCT 32 AS col2
    //  FROM tab0 AS cor0
    //  LEFT OUTER JOIN tab2 AS cor1
    //  ON ( NULL ) IS NULL;
    if output_schema.fields.is_empty() {
        return Ok(Some(create_record_batch_with_empty_schema(
            Arc::new(output_schema.clone()),
            filtered_probe_batch.num_rows(),
        )?));
    }

    let mut columns: Vec<Arc<dyn Array>> =
        Vec::with_capacity(output_schema.fields().len());

    for column_index in col_indices {
        let array = if column_index.side == build_side {
            // Broadcast the single build-side row to match the filtered
            // probe-side batch length
            let original_left_array = build_side_batch.column(column_index.index);

            // Use `arrow::compute::take` directly for `List(Utf8View)` rather
            // than going through `ScalarValue::to_array_of_size()`, which
            // avoids some intermediate allocations.
            //
            // In other cases, `to_array_of_size()` is faster.
            match original_left_array.data_type() {
                DataType::List(field) | DataType::LargeList(field)
                    if field.data_type() == &DataType::Utf8View =>
                {
                    let indices_iter = std::iter::repeat_n(
                        build_side_index as u64,
                        filtered_probe_batch.num_rows(),
                    );
                    let indices_array = UInt64Array::from_iter_values(indices_iter);
                    take(original_left_array.as_ref(), &indices_array, None)?
                }
                _ => {
                    let scalar_value = ScalarValue::try_from_array(
                        original_left_array.as_ref(),
                        build_side_index,
                    )?;
                    scalar_value.to_array_of_size(filtered_probe_batch.num_rows())?
                }
            }
        } else {
            // Take the filtered probe-side column using compute::take
            Arc::clone(filtered_probe_batch.column(column_index.index))
        };

        columns.push(array);
    }

    Ok(Some(RecordBatch::try_new(
        Arc::new(output_schema.clone()),
        columns,
    )?))
}

/// Special case for `PlaceHolderRowExec`
/// Minimal example:  SELECT 1 WHERE EXISTS (SELECT 1);
//
/// # Return
/// If Some, that's the result batch
/// If None, it's not for this special case. Continue execution.
pub(super) fn build_unmatched_batch_empty_schema(
    output_schema: &SchemaRef,
    batch_bitmap: &BooleanArray,
    // For left/right/full joins, it needs to fill nulls for another side
    join_type: JoinType,
) -> Result<Option<RecordBatch>> {
    let result_size = match join_type {
        JoinType::Left
        | JoinType::Right
        | JoinType::Full
        | JoinType::LeftAnti
        | JoinType::RightAnti => batch_bitmap.false_count(),
        JoinType::LeftSemi | JoinType::RightSemi => batch_bitmap.true_count(),
        JoinType::LeftMark | JoinType::RightMark => batch_bitmap.len(),
        _ => unreachable!(),
    };

    if output_schema.fields().is_empty() {
        Ok(Some(create_record_batch_with_empty_schema(
            Arc::clone(output_schema),
            result_size,
        )?))
    } else {
        Ok(None)
    }
}

/// Creates an empty RecordBatch with a specific row count.
/// This is useful for cases where we need a batch with the correct schema and row count
/// but no actual data columns (e.g., for constant filters).
pub(super) fn create_record_batch_with_empty_schema(
    schema: SchemaRef,
    row_count: usize,
) -> Result<RecordBatch> {
    let options = RecordBatchOptions::new()
        .with_match_field_names(true)
        .with_row_count(Some(row_count));

    RecordBatch::try_new_with_options(schema, vec![], &options).map_err(|e| {
        internal_datafusion_err!("Failed to create empty record batch: {}", e)
    })
}

/// # Example:
/// batch:
/// a
/// ----
/// 1
/// 2
/// 3
///
/// batch_bitmap:
/// ----
/// false
/// true
/// false
///
/// another_side_schema:
/// [(b, bool), (c, int32)]
///
/// join_type: JoinType::Left
///
/// col_indices: ...(please refer to the stream's `column_indices` field)
///
/// batch_side: right
///
/// # Walkthrough:
///
/// This executor is performing a right join, and the currently processed right
/// batch is as above. After joining it with all buffered left rows, the joined
/// entries are marked by the `batch_bitmap`.
/// This method will keep the unmatched indices on the batch side (right), and pad
/// the left side with nulls. The result would be:
///
/// b          c           a
/// ------------------------
/// Null(bool) Null(Int32) 1
/// Null(bool) Null(Int32) 3
pub(super) fn build_unmatched_batch(
    output_schema: &SchemaRef,
    batch: &RecordBatch,
    batch_bitmap: BooleanArray,
    // For left/right/full joins, it needs to fill nulls for another side
    another_side_schema: &SchemaRef,
    col_indices: &[ColumnIndex],
    join_type: JoinType,
    batch_side: JoinSide,
) -> Result<Option<RecordBatch>> {
    // Should not call it for inner joins
    debug_assert_ne!(join_type, JoinType::Inner);
    debug_assert_ne!(batch_side, JoinSide::None);

    // Handle special case (see function comment)
    if let Some(batch) =
        build_unmatched_batch_empty_schema(output_schema, &batch_bitmap, join_type)?
    {
        return Ok(Some(batch));
    }

    match join_type {
        JoinType::Full | JoinType::Right | JoinType::Left => {
            if join_type == JoinType::Right {
                debug_assert_eq!(batch_side, JoinSide::Right);
            }
            if join_type == JoinType::Left {
                debug_assert_eq!(batch_side, JoinSide::Left);
            }

            // 1. Filter the batch with *flipped* bitmap
            // 2. Fill left side with nulls
            let flipped_bitmap = not(&batch_bitmap)?;

            // create a record batch, with left_schema, of only one row of all nulls
            let left_null_columns: Vec<Arc<dyn Array>> = another_side_schema
                .fields()
                .iter()
                .map(|field| new_null_array(field.data_type(), 1))
                .collect();

            // Hack: If the left schema is not nullable, the full join result
            // might contain null, this is only a temporary batch to construct
            // such full join result.
            let nullable_left_schema = Arc::new(Schema::new(
                another_side_schema
                    .fields()
                    .iter()
                    .map(|field| (**field).clone().with_nullable(true))
                    .collect::<Vec<_>>(),
            ));
            let left_null_batch = if nullable_left_schema.fields.is_empty() {
                // Left input can be an empty relation, in this case left relation
                // won't be used to construct the result batch (i.e. not in `col_indices`)
                create_record_batch_with_empty_schema(nullable_left_schema, 0)?
            } else {
                RecordBatch::try_new(nullable_left_schema, left_null_columns)?
            };

            debug_assert_ne!(batch_side, JoinSide::None);
            let opposite_side = batch_side.negate();

            build_row_join_batch(
                output_schema,
                &left_null_batch,
                0,
                batch,
                Some(flipped_bitmap),
                col_indices,
                opposite_side,
            )
        }
        JoinType::RightSemi
        | JoinType::RightAnti
        | JoinType::LeftSemi
        | JoinType::LeftAnti => {
            if matches!(join_type, JoinType::RightSemi | JoinType::RightAnti) {
                debug_assert_eq!(batch_side, JoinSide::Right);
            }
            if matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                debug_assert_eq!(batch_side, JoinSide::Left);
            }

            let bitmap = if matches!(join_type, JoinType::LeftSemi | JoinType::RightSemi)
            {
                batch_bitmap.clone()
            } else {
                not(&batch_bitmap)?
            };

            if !bitmap.has_true() {
                return Ok(None);
            }

            let mut columns: Vec<Arc<dyn Array>> =
                Vec::with_capacity(output_schema.fields().len());

            for column_index in col_indices {
                debug_assert_eq!(column_index.side, batch_side);

                let col = batch.column(column_index.index);
                let filtered_col = filter(col, &bitmap)?;

                columns.push(filtered_col);
            }

            Ok(Some(RecordBatch::try_new(
                Arc::clone(output_schema),
                columns,
            )?))
        }
        JoinType::RightMark | JoinType::LeftMark => {
            if join_type == JoinType::RightMark {
                debug_assert_eq!(batch_side, JoinSide::Right);
            }
            if join_type == JoinType::LeftMark {
                debug_assert_eq!(batch_side, JoinSide::Left);
            }

            let mut columns: Vec<Arc<dyn Array>> =
                Vec::with_capacity(output_schema.fields().len());

            // Hack to deal with the borrow checker
            let mut right_batch_bitmap_opt = Some(batch_bitmap);

            for column_index in col_indices {
                if column_index.side == batch_side {
                    let col = batch.column(column_index.index);

                    columns.push(Arc::clone(col));
                } else if column_index.side == JoinSide::None {
                    let right_batch_bitmap = std::mem::take(&mut right_batch_bitmap_opt);
                    match right_batch_bitmap {
                        Some(right_batch_bitmap) => {
                            columns.push(Arc::new(right_batch_bitmap))
                        }
                        None => unreachable!("Should only be one mark column"),
                    }
                } else {
                    return internal_err!(
                        "Not possible to have this join side for RightMark join"
                    );
                }
            }

            Ok(Some(RecordBatch::try_new(
                Arc::clone(output_schema),
                columns,
            )?))
        }
        _ => internal_err!(
            "If batch is at right side, this function must be handling Full/Right/RightSemi/RightAnti/RightMark joins"
        ),
    }
}

/// Build the final right-side result batch from the global bitmap accumulated
/// across all left chunks.
pub(super) fn build_global_right_result_batch(
    active: &mut SpillStateActive,
    output_schema: &SchemaRef,
    right_batch: &RecordBatch,
    col_indices: &[ColumnIndex],
    join_type: JoinType,
) -> Result<Option<RecordBatch>> {
    let bitmap = active.next_global_right_bitmap(right_batch.num_rows());
    let left_schema = Arc::clone(
        active
            .left_schema
            .as_ref()
            .expect("left_schema must be set"),
    );

    build_unmatched_batch(
        output_schema,
        right_batch,
        bitmap,
        &left_schema,
        col_indices,
        join_type,
        JoinSide::Right,
    )
}
