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
//! This module implements the runtime state machine for the Sort-Merge Join
//! operator. It drives two sorted input streams (the *streamed* side and the
//! *buffered* side), compares join keys, and produces joined `RecordBatch`es.

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::BufReader;
use std::mem::size_of;
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::joins::sort_merge_join::metrics::SortMergeJoinMetrics;
use crate::joins::utils::{compare_join_arrays, JoinFilter};
use crate::metrics::RecordOutput;
use crate::spill::spill_manager::SpillManager;
use crate::{PhysicalExpr, RecordBatchStream, SendableRecordBatchStream};

use arrow::array::{types::UInt64Type, *};
use arrow::compute::{
    self, concat_batches, filter_record_batch, is_not_null, take, SortOptions,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use datafusion_common::config::SpillCompression;
use datafusion_common::{
    exec_err, internal_err, not_impl_err, DataFusionError, HashSet, JoinSide, JoinType,
    NullEquality, Result,
};
use datafusion_execution::disk_manager::RefCountedTempFile;
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_physical_expr_common::physical_expr::PhysicalExprRef;

use futures::{Stream, StreamExt};

/// State of SMJ stream
#[derive(Debug, PartialEq, Eq)]
pub(super) enum SortMergeJoinState {
    /// Init joining with a new streamed row or a new buffered batches
    Init,
    /// Polling one streamed row or one buffered batch, or both
    Polling,
    /// Joining polled data and making output
    JoinOutput,
    /// No more output
    Exhausted,
}

/// State of streamed data stream
#[derive(Debug, PartialEq, Eq)]
pub(super) enum StreamedState {
    /// Init polling
    Init,
    /// Polling one streamed row
    Polling,
    /// Ready to produce one streamed row
    Ready,
    /// No more streamed row
    Exhausted,
}

/// State of buffered data stream
#[derive(Debug, PartialEq, Eq)]
pub(super) enum BufferedState {
    /// Init polling
    Init,
    /// Polling first row in the next batch
    PollingFirst,
    /// Polling rest rows in the next batch
    PollingRest,
    /// Ready to produce one batch
    Ready,
    /// No more buffered batches
    Exhausted,
}

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
    /// Index of currently scanned batch from buffered data
    pub buffered_batch_idx: Option<usize>,
    /// Indices that found a match for the given join filter
    /// Used for semi joins to keep track the streaming index which got a join filter match
    /// and already emitted to the output.
    pub join_filter_matched_idxs: HashSet<u64>,
}

impl StreamedBatch {
    fn new(batch: RecordBatch, on_column: &[Arc<dyn PhysicalExpr>]) -> Self {
        let join_arrays = join_arrays(&batch, on_column);
        StreamedBatch {
            batch,
            idx: 0,
            join_arrays,
            output_indices: vec![],
            buffered_batch_idx: None,
            join_filter_matched_idxs: HashSet::new(),
        }
    }

    fn new_empty(schema: SchemaRef) -> Self {
        StreamedBatch {
            batch: RecordBatch::new_empty(schema),
            idx: 0,
            join_arrays: vec![],
            output_indices: vec![],
            buffered_batch_idx: None,
            join_filter_matched_idxs: HashSet::new(),
        }
    }

    /// Appends new pair consisting of current streamed index and `buffered_idx`
    /// index of buffered batch with `buffered_batch_idx` index.
    fn append_output_pair(
        &mut self,
        buffered_batch_idx: Option<usize>,
        buffered_idx: Option<usize>,
    ) {
        // If no current chunk exists or current chunk is not for current buffered batch,
        // create a new chunk
        if self.output_indices.is_empty() || self.buffered_batch_idx != buffered_batch_idx
        {
            self.output_indices.push(StreamedJoinedChunk {
                buffered_batch_idx,
                streamed_indices: UInt64Builder::with_capacity(1),
                buffered_indices: UInt64Builder::with_capacity(1),
            });
            self.buffered_batch_idx = buffered_batch_idx;
        };
        let current_chunk = self.output_indices.last_mut().unwrap();

        // Append index of streamed batch and index of buffered batch into current chunk
        current_chunk.streamed_indices.append_value(self.idx as u64);
        if let Some(idx) = buffered_idx {
            current_chunk.buffered_indices.append_value(idx as u64);
        } else {
            current_chunk.buffered_indices.append_null();
        }
    }
}

/// A buffered batch that contains contiguous rows with same join key
///
/// `BufferedBatch` can exist as either an in-memory `RecordBatch` or a `RefCountedTempFile` on disk.
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
    /// The indices of buffered batch that the join filter doesn't satisfy.
    /// This is a map between right row index and a boolean value indicating whether all joined row
    /// of the right row does not satisfy the filter .
    /// When dequeuing the buffered batch, we need to produce null joined rows for these indices.
    pub join_filter_not_matched_map: HashMap<u64, bool>,
    /// Current buffered batch number of rows. Equal to batch.num_rows()
    /// but if batch is spilled to disk this property is preferable
    /// and less expensive
    pub num_rows: usize,
}

impl BufferedBatch {
    fn new(
        batch: RecordBatch,
        range: Range<usize>,
        on_column: &[PhysicalExprRef],
    ) -> Self {
        let join_arrays = join_arrays(&batch, on_column);

        // Estimation is calculated as
        //   inner batch size
        // + join keys size
        // + worst case null_joined (as vector capacity * element size)
        // + Range size
        // + size of this estimation
        let size_estimation = batch.get_array_memory_size()
            + join_arrays
                .iter()
                .map(|arr| arr.get_array_memory_size())
                .sum::<usize>()
            + batch.num_rows().next_power_of_two() * size_of::<usize>()
            + size_of::<Range<usize>>()
            + size_of::<usize>();

        let num_rows = batch.num_rows();
        BufferedBatch {
            batch: BufferedBatchState::InMemory(batch),
            range,
            join_arrays,
            null_joined: vec![],
            size_estimation,
            join_filter_not_matched_map: HashMap::new(),
            num_rows,
        }
    }
}

// TODO: Spill join arrays (https://github.com/apache/datafusion/pull/17429)
// Used to represent whether the buffered data is currently in memory or written to disk
#[derive(Debug)]
pub(super) enum BufferedBatchState {
    // In memory record batch
    InMemory(RecordBatch),
    // Spilled temp file
    Spilled(RefCountedTempFile),
}

/// Sort-Merge join stream that consumes streamed and buffered data streams
/// and produces joined output stream.
pub(super) struct SortMergeJoinStream {
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
    /// (used in outer join) Is current streamed row joined at least once?
    pub streamed_joined: bool,
    /// State of streamed
    pub streamed_state: StreamedState,
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
    /// (used in outer join) Is current buffered batches joined at least once?
    pub buffered_joined: bool,
    /// State of buffered
    pub buffered_state: BufferedState,
    /// Join key columns of buffered
    pub on_buffered: Vec<PhysicalExprRef>,

    // ========================================================================
    // MERGE JOIN STATES:
    // These fields track the execution state of merge join and are updated
    // during the execution.
    // ========================================================================
    /// Current state of the stream
    pub state: SortMergeJoinState,
    /// Staging output array builders
    pub staging_output_record_batches: JoinedRecordBatches,
    /// Output buffer. Currently used by filtering as it requires double buffering
    /// to avoid small/empty batches. Non-filtered join outputs directly from `staging_output_record_batches.batches`
    pub output: RecordBatch,
    /// Staging output size, including output batches and staging joined results.
    /// Increased when we put rows into buffer and decreased after we actually output batches.
    /// Used to trigger output when sufficient rows are ready
    pub output_size: usize,
    /// The comparison result of current streamed row and buffered batches
    pub current_ordering: Ordering,
    /// Manages the process of spilling and reading back intermediate data
    pub spill_manager: SpillManager,

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
    /// A unique number for each batch
    pub streamed_batch_counter: AtomicUsize,
}

/// Joined batches with attached join filter information
pub(super) struct JoinedRecordBatches {
    /// Joined batches. Each batch is already joined columns from left and right sources
    pub batches: Vec<RecordBatch>,
    /// Filter match mask for each row(matched/non-matched)
    pub filter_mask: BooleanBuilder,
    /// Left row indices to glue together rows in `batches` and `filter_mask`
    pub row_indices: UInt64Builder,
    /// Which unique batch id the row belongs to
    /// It is necessary to differentiate rows that are distributed the way when they point to the same
    /// row index but in not the same batches
    pub batch_ids: Vec<usize>,
}

impl JoinedRecordBatches {
    fn clear(&mut self) {
        self.batches.clear();
        self.batch_ids.clear();
        self.filter_mask = BooleanBuilder::new();
        self.row_indices = UInt64Builder::new();
    }
}
impl RecordBatchStream for SortMergeJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// True if next index refers to either:
/// - another batch id
/// - another row index within same batch id
/// - end of row indices
#[inline(always)]
fn last_index_for_row(
    row_index: usize,
    indices: &UInt64Array,
    batch_ids: &[usize],
    indices_len: usize,
) -> bool {
    row_index == indices_len - 1
        || batch_ids[row_index] != batch_ids[row_index + 1]
        || indices.value(row_index) != indices.value(row_index + 1)
}

// Returns a corrected boolean bitmask for the given join type
// Values in the corrected bitmask can be: true, false, null
// `true` - the row found its match and sent to the output
// `null` - the row ignored, no output
// `false` - the row sent as NULL joined row
pub(super) fn get_corrected_filter_mask(
    join_type: JoinType,
    row_indices: &UInt64Array,
    batch_ids: &[usize],
    filter_mask: &BooleanArray,
    expected_size: usize,
) -> Option<BooleanArray> {
    let row_indices_length = row_indices.len();
    let mut corrected_mask: BooleanBuilder =
        BooleanBuilder::with_capacity(row_indices_length);
    let mut seen_true = false;

    match join_type {
        JoinType::Left | JoinType::Right => {
            for i in 0..row_indices_length {
                let last_index =
                    last_index_for_row(i, row_indices, batch_ids, row_indices_length);
                if filter_mask.value(i) {
                    seen_true = true;
                    corrected_mask.append_value(true);
                } else if seen_true || !filter_mask.value(i) && !last_index {
                    corrected_mask.append_null(); // to be ignored and not set to output
                } else {
                    corrected_mask.append_value(false); // to be converted to null joined row
                }

                if last_index {
                    seen_true = false;
                }
            }

            // Generate null joined rows for records which have no matching join key
            corrected_mask.append_n(expected_size - corrected_mask.len(), false);
            Some(corrected_mask.finish())
        }
        JoinType::LeftMark | JoinType::RightMark => {
            for i in 0..row_indices_length {
                let last_index =
                    last_index_for_row(i, row_indices, batch_ids, row_indices_length);
                if filter_mask.value(i) && !seen_true {
                    seen_true = true;
                    corrected_mask.append_value(true);
                } else if seen_true || !filter_mask.value(i) && !last_index {
                    corrected_mask.append_null(); // to be ignored and not set to output
                } else {
                    corrected_mask.append_value(false); // to be converted to null joined row
                }

                if last_index {
                    seen_true = false;
                }
            }

            // Generate null joined rows for records which have no matching join key
            corrected_mask.append_n(expected_size - corrected_mask.len(), false);
            Some(corrected_mask.finish())
        }
        JoinType::LeftSemi | JoinType::RightSemi => {
            for i in 0..row_indices_length {
                let last_index =
                    last_index_for_row(i, row_indices, batch_ids, row_indices_length);
                if filter_mask.value(i) && !seen_true {
                    seen_true = true;
                    corrected_mask.append_value(true);
                } else {
                    corrected_mask.append_null(); // to be ignored and not set to output
                }

                if last_index {
                    seen_true = false;
                }
            }

            Some(corrected_mask.finish())
        }
        JoinType::LeftAnti | JoinType::RightAnti => {
            for i in 0..row_indices_length {
                let last_index =
                    last_index_for_row(i, row_indices, batch_ids, row_indices_length);

                if filter_mask.value(i) {
                    seen_true = true;
                }

                if last_index {
                    if !seen_true {
                        corrected_mask.append_value(true);
                    } else {
                        corrected_mask.append_null();
                    }

                    seen_true = false;
                } else {
                    corrected_mask.append_null();
                }
            }
            // Generate null joined rows for records which have no matching join key,
            // for LeftAnti non-matched considered as true
            corrected_mask.append_n(expected_size - corrected_mask.len(), true);
            Some(corrected_mask.finish())
        }
        JoinType::Full => {
            let mut mask: Vec<Option<bool>> = vec![Some(true); row_indices_length];
            let mut last_true_idx = 0;
            let mut first_row_idx = 0;
            let mut seen_false = false;

            for i in 0..row_indices_length {
                let last_index =
                    last_index_for_row(i, row_indices, batch_ids, row_indices_length);
                let val = filter_mask.value(i);
                let is_null = filter_mask.is_null(i);

                if val {
                    // memoize the first seen matched row
                    if !seen_true {
                        last_true_idx = i;
                    }
                    seen_true = true;
                }

                if is_null || val {
                    mask[i] = Some(true);
                } else if !is_null && !val && (seen_true || seen_false) {
                    mask[i] = None;
                } else {
                    mask[i] = Some(false);
                }

                if !is_null && !val {
                    seen_false = true;
                }

                if last_index {
                    // If the left row seen as true its needed to output it once
                    // To do that we mark all other matches for same row as null to avoid the output
                    if seen_true {
                        #[expect(clippy::needless_range_loop)]
                        for j in first_row_idx..last_true_idx {
                            mask[j] = None;
                        }
                    }

                    seen_true = false;
                    seen_false = false;
                    last_true_idx = 0;
                    first_row_idx = i + 1;
                }
            }

            Some(BooleanArray::from(mask))
        }
        // Only outer joins needs to keep track of processed rows and apply corrected filter mask
        _ => None,
    }
}

impl Stream for SortMergeJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let join_time = self.join_metrics.join_time().clone();
        let _timer = join_time.timer();
        loop {
            match &self.state {
                SortMergeJoinState::Init => {
                    let streamed_exhausted =
                        self.streamed_state == StreamedState::Exhausted;
                    let buffered_exhausted =
                        self.buffered_state == BufferedState::Exhausted;
                    self.state = if streamed_exhausted && buffered_exhausted {
                        SortMergeJoinState::Exhausted
                    } else {
                        match self.current_ordering {
                            Ordering::Less | Ordering::Equal => {
                                if !streamed_exhausted {
                                    if self.filter.is_some()
                                        && matches!(
                                            self.join_type,
                                            JoinType::Left
                                                | JoinType::LeftSemi
                                                | JoinType::LeftMark
                                                | JoinType::Right
                                                | JoinType::RightSemi
                                                | JoinType::RightMark
                                                | JoinType::LeftAnti
                                                | JoinType::RightAnti
                                                | JoinType::Full
                                        )
                                    {
                                        self.freeze_all()?;

                                        // If join is filtered and there is joined tuples waiting
                                        // to be filtered
                                        if !self
                                            .staging_output_record_batches
                                            .batches
                                            .is_empty()
                                        {
                                            // Apply filter on joined tuples and get filtered batch
                                            let out_filtered_batch =
                                                self.filter_joined_batch()?;

                                            // Append filtered batch to the output buffer
                                            self.output = concat_batches(
                                                &self.schema(),
                                                [&self.output, &out_filtered_batch],
                                            )?;

                                            // Send to output if the output buffer surpassed the `batch_size`
                                            if self.output.num_rows() >= self.batch_size {
                                                let record_batch = std::mem::replace(
                                                    &mut self.output,
                                                    RecordBatch::new_empty(
                                                        out_filtered_batch.schema(),
                                                    ),
                                                );
                                                return Poll::Ready(Some(Ok(
                                                    record_batch,
                                                )));
                                            }
                                        }
                                    }

                                    self.streamed_joined = false;
                                    self.streamed_state = StreamedState::Init;
                                }
                            }
                            Ordering::Greater => {
                                if !buffered_exhausted {
                                    self.buffered_joined = false;
                                    self.buffered_state = BufferedState::Init;
                                }
                            }
                        }
                        SortMergeJoinState::Polling
                    };
                }
                SortMergeJoinState::Polling => {
                    if ![StreamedState::Exhausted, StreamedState::Ready]
                        .contains(&self.streamed_state)
                    {
                        match self.poll_streamed_row(cx)? {
                            Poll::Ready(_) => {}
                            Poll::Pending => return Poll::Pending,
                        }
                    }

                    if ![BufferedState::Exhausted, BufferedState::Ready]
                        .contains(&self.buffered_state)
                    {
                        match self.poll_buffered_batches(cx)? {
                            Poll::Ready(_) => {}
                            Poll::Pending => return Poll::Pending,
                        }
                    }
                    let streamed_exhausted =
                        self.streamed_state == StreamedState::Exhausted;
                    let buffered_exhausted =
                        self.buffered_state == BufferedState::Exhausted;
                    if streamed_exhausted && buffered_exhausted {
                        self.state = SortMergeJoinState::Exhausted;
                        continue;
                    }
                    self.current_ordering = self.compare_streamed_buffered()?;
                    self.state = SortMergeJoinState::JoinOutput;
                }
                SortMergeJoinState::JoinOutput => {
                    self.join_partial()?;

                    if self.output_size < self.batch_size {
                        if self.buffered_data.scanning_finished() {
                            self.buffered_data.scanning_reset();
                            self.state = SortMergeJoinState::Init;
                        }
                    } else {
                        self.freeze_all()?;
                        if !self.staging_output_record_batches.batches.is_empty() {
                            let record_batch = self.output_record_batch_and_reset()?;
                            // For non-filtered join output whenever the target output batch size
                            // is hit. For filtered join its needed to output on later phase
                            // because target output batch size can be hit in the middle of
                            // filtering causing the filtering to be incomplete and causing
                            // correctness issues
                            if self.filter.is_some()
                                && matches!(
                                    self.join_type,
                                    JoinType::Left
                                        | JoinType::LeftSemi
                                        | JoinType::Right
                                        | JoinType::RightSemi
                                        | JoinType::LeftAnti
                                        | JoinType::RightAnti
                                        | JoinType::LeftMark
                                        | JoinType::RightMark
                                        | JoinType::Full
                                )
                            {
                                continue;
                            }

                            return Poll::Ready(Some(Ok(record_batch)));
                        }
                        return Poll::Pending;
                    }
                }
                SortMergeJoinState::Exhausted => {
                    self.freeze_all()?;

                    // if there is still something not processed
                    if !self.staging_output_record_batches.batches.is_empty() {
                        if self.filter.is_some()
                            && matches!(
                                self.join_type,
                                JoinType::Left
                                    | JoinType::LeftSemi
                                    | JoinType::Right
                                    | JoinType::RightSemi
                                    | JoinType::LeftAnti
                                    | JoinType::RightAnti
                                    | JoinType::Full
                                    | JoinType::LeftMark
                                    | JoinType::RightMark
                            )
                        {
                            let record_batch = self.filter_joined_batch()?;
                            return Poll::Ready(Some(Ok(record_batch)));
                        } else {
                            let record_batch = self.output_record_batch_and_reset()?;
                            return Poll::Ready(Some(Ok(record_batch)));
                        }
                    } else if self.output.num_rows() > 0 {
                        // if processed but still not outputted because it didn't hit batch size before
                        let schema = self.output.schema();
                        let record_batch = std::mem::replace(
                            &mut self.output,
                            RecordBatch::new_empty(schema),
                        );
                        return Poll::Ready(Some(Ok(record_batch)));
                    } else {
                        return Poll::Ready(None);
                    }
                }
            }
        }
    }
}

impl SortMergeJoinStream {
    #[expect(clippy::too_many_arguments)]
    pub fn try_new(
        // Configured via `datafusion.execution.spill_compression`.
        spill_compression: SpillCompression,
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
        runtime_env: Arc<RuntimeEnv>,
    ) -> Result<Self> {
        let streamed_schema = streamed.schema();
        let buffered_schema = buffered.schema();
        let spill_manager = SpillManager::new(
            Arc::clone(&runtime_env),
            join_metrics.spill_metrics().clone(),
            Arc::clone(&buffered_schema),
        )
        .with_compression_type(spill_compression);
        Ok(Self {
            state: SortMergeJoinState::Init,
            sort_options,
            null_equality,
            schema: Arc::clone(&schema),
            streamed_schema: Arc::clone(&streamed_schema),
            buffered_schema,
            streamed,
            buffered,
            streamed_batch: StreamedBatch::new_empty(streamed_schema),
            buffered_data: BufferedData::default(),
            streamed_joined: false,
            buffered_joined: false,
            streamed_state: StreamedState::Init,
            buffered_state: BufferedState::Init,
            current_ordering: Ordering::Equal,
            on_streamed,
            on_buffered,
            filter,
            staging_output_record_batches: JoinedRecordBatches {
                batches: vec![],
                filter_mask: BooleanBuilder::new(),
                row_indices: UInt64Builder::new(),
                batch_ids: vec![],
            },
            output: RecordBatch::new_empty(schema),
            output_size: 0,
            batch_size,
            join_type,
            join_metrics,
            reservation,
            runtime_env,
            spill_manager,
            streamed_batch_counter: AtomicUsize::new(0),
        })
    }

    /// Poll next streamed row
    fn poll_streamed_row(&mut self, cx: &mut Context) -> Poll<Option<Result<()>>> {
        loop {
            match &self.streamed_state {
                StreamedState::Init => {
                    if self.streamed_batch.idx + 1 < self.streamed_batch.batch.num_rows()
                    {
                        self.streamed_batch.idx += 1;
                        self.streamed_state = StreamedState::Ready;
                        return Poll::Ready(Some(Ok(())));
                    } else {
                        self.streamed_state = StreamedState::Polling;
                    }
                }
                StreamedState::Polling => match self.streamed.poll_next_unpin(cx)? {
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                    Poll::Ready(None) => {
                        self.streamed_state = StreamedState::Exhausted;
                    }
                    Poll::Ready(Some(batch)) => {
                        if batch.num_rows() > 0 {
                            self.freeze_streamed()?;
                            self.join_metrics.input_batches().add(1);
                            self.join_metrics.input_rows().add(batch.num_rows());
                            self.streamed_batch =
                                StreamedBatch::new(batch, &self.on_streamed);
                            // Every incoming streaming batch should have its unique id
                            // Check `JoinedRecordBatches.self.streamed_batch_counter` documentation
                            self.streamed_batch_counter
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            self.streamed_state = StreamedState::Ready;
                        }
                    }
                },
                StreamedState::Ready => {
                    return Poll::Ready(Some(Ok(())));
                }
                StreamedState::Exhausted => {
                    return Poll::Ready(None);
                }
            }
        }
    }

    fn free_reservation(&mut self, buffered_batch: &BufferedBatch) -> Result<()> {
        // Shrink memory usage for in-memory batches only
        if let BufferedBatchState::InMemory(_) = buffered_batch.batch {
            self.reservation
                .try_shrink(buffered_batch.size_estimation)?;
        }
        Ok(())
    }

    fn allocate_reservation(&mut self, mut buffered_batch: BufferedBatch) -> Result<()> {
        match self.reservation.try_grow(buffered_batch.size_estimation) {
            Ok(_) => {
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

    /// Poll next buffered batches
    fn poll_buffered_batches(&mut self, cx: &mut Context) -> Poll<Option<Result<()>>> {
        loop {
            match &self.buffered_state {
                BufferedState::Init => {
                    // pop previous buffered batches
                    while !self.buffered_data.batches.is_empty() {
                        let head_batch = self.buffered_data.head_batch();
                        // If the head batch is fully processed, dequeue it and produce output of it.
                        if head_batch.range.end == head_batch.num_rows {
                            self.freeze_dequeuing_buffered()?;
                            if let Some(mut buffered_batch) =
                                self.buffered_data.batches.pop_front()
                            {
                                self.produce_buffered_not_matched(&mut buffered_batch)?;
                                self.free_reservation(&buffered_batch)?;
                            }
                        } else {
                            // If the head batch is not fully processed, break the loop.
                            // Streamed batch will be joined with the head batch in the next step.
                            break;
                        }
                    }
                    if self.buffered_data.batches.is_empty() {
                        self.buffered_state = BufferedState::PollingFirst;
                    } else {
                        let tail_batch = self.buffered_data.tail_batch_mut();
                        tail_batch.range.start = tail_batch.range.end;
                        tail_batch.range.end += 1;
                        self.buffered_state = BufferedState::PollingRest;
                    }
                }
                BufferedState::PollingFirst => match self.buffered.poll_next_unpin(cx)? {
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                    Poll::Ready(None) => {
                        self.buffered_state = BufferedState::Exhausted;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Some(batch)) => {
                        self.join_metrics.input_batches().add(1);
                        self.join_metrics.input_rows().add(batch.num_rows());

                        if batch.num_rows() > 0 {
                            let buffered_batch =
                                BufferedBatch::new(batch, 0..1, &self.on_buffered);

                            self.allocate_reservation(buffered_batch)?;
                            self.buffered_state = BufferedState::PollingRest;
                        }
                    }
                },
                BufferedState::PollingRest => {
                    if self.buffered_data.tail_batch().range.end
                        < self.buffered_data.tail_batch().num_rows
                    {
                        while self.buffered_data.tail_batch().range.end
                            < self.buffered_data.tail_batch().num_rows
                        {
                            if is_join_arrays_equal(
                                &self.buffered_data.head_batch().join_arrays,
                                self.buffered_data.head_batch().range.start,
                                &self.buffered_data.tail_batch().join_arrays,
                                self.buffered_data.tail_batch().range.end,
                            )? {
                                self.buffered_data.tail_batch_mut().range.end += 1;
                            } else {
                                self.buffered_state = BufferedState::Ready;
                                return Poll::Ready(Some(Ok(())));
                            }
                        }
                    } else {
                        match self.buffered.poll_next_unpin(cx)? {
                            Poll::Pending => {
                                return Poll::Pending;
                            }
                            Poll::Ready(None) => {
                                self.buffered_state = BufferedState::Ready;
                            }
                            Poll::Ready(Some(batch)) => {
                                // Polling batches coming concurrently as multiple partitions
                                self.join_metrics.input_batches().add(1);
                                self.join_metrics.input_rows().add(batch.num_rows());
                                if batch.num_rows() > 0 {
                                    let buffered_batch = BufferedBatch::new(
                                        batch,
                                        0..0,
                                        &self.on_buffered,
                                    );
                                    self.allocate_reservation(buffered_batch)?;
                                }
                            }
                        }
                    }
                }
                BufferedState::Ready => {
                    return Poll::Ready(Some(Ok(())));
                }
                BufferedState::Exhausted => {
                    return Poll::Ready(None);
                }
            }
        }
    }

    /// Get comparison result of streamed row and buffered batches
    fn compare_streamed_buffered(&self) -> Result<Ordering> {
        if self.streamed_state == StreamedState::Exhausted {
            return Ok(Ordering::Greater);
        }
        if !self.buffered_data.has_buffered_rows() {
            return Ok(Ordering::Less);
        }

        compare_join_arrays(
            &self.streamed_batch.join_arrays,
            self.streamed_batch.idx,
            &self.buffered_data.head_batch().join_arrays,
            self.buffered_data.head_batch().range.start,
            &self.sort_options,
            self.null_equality,
        )
    }

    /// Produce join and fill output buffer until reaching target batch size
    /// or the join is finished
    fn join_partial(&mut self) -> Result<()> {
        // Whether to join streamed rows
        let mut join_streamed = false;
        // Whether to join buffered rows
        let mut join_buffered = false;
        // For Mark join we store a dummy id to indicate the row has a match
        let mut mark_row_as_match = false;

        // determine whether we need to join streamed/buffered rows
        match self.current_ordering {
            Ordering::Less => {
                if matches!(
                    self.join_type,
                    JoinType::Left
                        | JoinType::Right
                        | JoinType::Full
                        | JoinType::LeftAnti
                        | JoinType::RightAnti
                        | JoinType::LeftMark
                        | JoinType::RightMark
                ) {
                    join_streamed = !self.streamed_joined;
                }
            }
            Ordering::Equal => {
                if matches!(
                    self.join_type,
                    JoinType::LeftSemi
                        | JoinType::LeftMark
                        | JoinType::RightSemi
                        | JoinType::RightMark
                ) {
                    mark_row_as_match = matches!(
                        self.join_type,
                        JoinType::LeftMark | JoinType::RightMark
                    );
                    // if the join filter is specified then its needed to output the streamed index
                    // only if it has not been emitted before
                    // the `join_filter_matched_idxs` keeps track on if streamed index has a successful
                    // filter match and prevents the same index to go into output more than once
                    if self.filter.is_some() {
                        join_streamed = !self
                            .streamed_batch
                            .join_filter_matched_idxs
                            .contains(&(self.streamed_batch.idx as u64))
                            && !self.streamed_joined;
                        // if the join filter specified there can be references to buffered columns
                        // so buffered columns are needed to access them
                        join_buffered = join_streamed;
                    } else {
                        join_streamed = !self.streamed_joined;
                    }
                }
                if matches!(
                    self.join_type,
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
                ) {
                    join_streamed = true;
                    join_buffered = true;
                };

                if matches!(self.join_type, JoinType::LeftAnti | JoinType::RightAnti)
                    && self.filter.is_some()
                {
                    join_streamed = !self.streamed_joined;
                    join_buffered = join_streamed;
                }
            }
            Ordering::Greater => {
                if matches!(self.join_type, JoinType::Full) {
                    join_buffered = !self.buffered_joined;
                };
            }
        }
        if !join_streamed && !join_buffered {
            // no joined data
            self.buffered_data.scanning_finish();
            return Ok(());
        }

        if join_buffered {
            // joining streamed/nulls and buffered
            while !self.buffered_data.scanning_finished()
                && self.output_size < self.batch_size
            {
                let scanning_idx = self.buffered_data.scanning_idx();
                if join_streamed {
                    // Join streamed row and buffered row
                    self.streamed_batch.append_output_pair(
                        Some(self.buffered_data.scanning_batch_idx),
                        Some(scanning_idx),
                    );
                } else {
                    // Join nulls and buffered row for FULL join
                    self.buffered_data
                        .scanning_batch_mut()
                        .null_joined
                        .push(scanning_idx);
                }
                self.output_size += 1;
                self.buffered_data.scanning_advance();

                if self.buffered_data.scanning_finished() {
                    self.streamed_joined = join_streamed;
                    self.buffered_joined = true;
                }
            }
        } else {
            // joining streamed and nulls
            let scanning_batch_idx = if self.buffered_data.scanning_finished() {
                None
            } else {
                Some(self.buffered_data.scanning_batch_idx)
            };
            // For Mark join we store a dummy id to indicate the row has a match
            let scanning_idx = mark_row_as_match.then_some(0);

            self.streamed_batch
                .append_output_pair(scanning_batch_idx, scanning_idx);
            self.output_size += 1;
            self.buffered_data.scanning_finish();
            self.streamed_joined = true;
        }
        Ok(())
    }

    fn freeze_all(&mut self) -> Result<()> {
        self.freeze_buffered(self.buffered_data.batches.len())?;
        self.freeze_streamed()?;
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
        Ok(())
    }

    // Produces and stages record batch from buffered indices with corresponding
    // NULLs on streamed side.
    //
    // Applicable only in case of Full join.
    //
    fn freeze_buffered(&mut self, batch_count: usize) -> Result<()> {
        if !matches!(self.join_type, JoinType::Full) {
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
                let num_rows = record_batch.num_rows();
                self.staging_output_record_batches
                    .filter_mask
                    .append_nulls(num_rows);
                self.staging_output_record_batches
                    .row_indices
                    .append_nulls(num_rows);
                self.staging_output_record_batches.batch_ids.resize(
                    self.staging_output_record_batches.batch_ids.len() + num_rows,
                    0,
                );

                self.staging_output_record_batches
                    .batches
                    .push(record_batch);
            }
            buffered_batch.null_joined.clear();
        }
        Ok(())
    }

    fn produce_buffered_not_matched(
        &mut self,
        buffered_batch: &mut BufferedBatch,
    ) -> Result<()> {
        if !matches!(self.join_type, JoinType::Full) {
            return Ok(());
        }

        // For buffered row which is joined with streamed side rows but all joined rows
        // don't satisfy the join filter
        let not_matched_buffered_indices = buffered_batch
            .join_filter_not_matched_map
            .iter()
            .filter_map(|(idx, failed)| if *failed { Some(*idx) } else { None })
            .collect::<Vec<_>>();

        let buffered_indices =
            UInt64Array::from_iter_values(not_matched_buffered_indices.iter().copied());

        if let Some(record_batch) = produce_buffered_null_batch(
            &self.schema,
            &self.streamed_schema,
            &buffered_indices,
            buffered_batch,
        )? {
            let num_rows = record_batch.num_rows();

            self.staging_output_record_batches
                .filter_mask
                .append_nulls(num_rows);
            self.staging_output_record_batches
                .row_indices
                .append_nulls(num_rows);
            self.staging_output_record_batches.batch_ids.resize(
                self.staging_output_record_batches.batch_ids.len() + num_rows,
                0,
            );
            self.staging_output_record_batches
                .batches
                .push(record_batch);
        }
        buffered_batch.join_filter_not_matched_map.clear();

        Ok(())
    }

    // Produces and stages record batch for all output indices found
    // for current streamed batch and clears staged output indices.
    fn freeze_streamed(&mut self) -> Result<()> {
        for chunk in self.streamed_batch.output_indices.iter_mut() {
            // The row indices of joined streamed batch
            let left_indices = chunk.streamed_indices.finish();

            if left_indices.is_empty() {
                continue;
            }

            let mut left_columns = self
                .streamed_batch
                .batch
                .columns()
                .iter()
                .map(|column| take(column, &left_indices, None))
                .collect::<Result<Vec<_>, ArrowError>>()?;

            // The row indices of joined buffered batch
            let right_indices: UInt64Array = chunk.buffered_indices.finish();
            let mut right_columns =
                if matches!(self.join_type, JoinType::LeftMark | JoinType::RightMark) {
                    vec![Arc::new(is_not_null(&right_indices)?) as ArrayRef]
                } else if matches!(
                    self.join_type,
                    JoinType::LeftSemi
                        | JoinType::LeftAnti
                        | JoinType::RightAnti
                        | JoinType::RightSemi
                ) {
                    vec![]
                } else if let Some(buffered_idx) = chunk.buffered_batch_idx {
                    fetch_right_columns_by_idxs(
                        &self.buffered_data,
                        buffered_idx,
                        &right_indices,
                    )?
                } else {
                    // If buffered batch none, meaning it is null joined batch.
                    // We need to create null arrays for buffered columns to join with streamed rows.
                    create_unmatched_columns(
                        self.join_type,
                        &self.buffered_schema,
                        right_indices.len(),
                    )
                };

            // Prepare the columns we apply join filter on later.
            // Only for joined rows between streamed and buffered.
            let filter_columns = if chunk.buffered_batch_idx.is_some() {
                if !matches!(self.join_type, JoinType::Right) {
                    if matches!(
                        self.join_type,
                        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark
                    ) {
                        let right_cols = fetch_right_columns_by_idxs(
                            &self.buffered_data,
                            chunk.buffered_batch_idx.unwrap(),
                            &right_indices,
                        )?;

                        get_filter_column(&self.filter, &left_columns, &right_cols)
                    } else if matches!(
                        self.join_type,
                        JoinType::RightAnti | JoinType::RightSemi | JoinType::RightMark
                    ) {
                        let right_cols = fetch_right_columns_by_idxs(
                            &self.buffered_data,
                            chunk.buffered_batch_idx.unwrap(),
                            &right_indices,
                        )?;

                        get_filter_column(&self.filter, &right_cols, &left_columns)
                    } else {
                        get_filter_column(&self.filter, &left_columns, &right_columns)
                    }
                } else {
                    get_filter_column(&self.filter, &right_columns, &left_columns)
                }
            } else {
                // This chunk is totally for null joined rows (outer join), we don't need to apply join filter.
                // Any join filter applied only on either streamed or buffered side will be pushed already.
                vec![]
            };

            let columns = if !matches!(self.join_type, JoinType::Right) {
                left_columns.extend(right_columns);
                left_columns
            } else {
                right_columns.extend(left_columns);
                right_columns
            };

            let output_batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
            // Apply join filter if any
            if !filter_columns.is_empty() {
                if let Some(f) = &self.filter {
                    // Construct batch with only filter columns
                    let filter_batch =
                        RecordBatch::try_new(Arc::clone(f.schema()), filter_columns)?;

                    let filter_result = f
                        .expression()
                        .evaluate(&filter_batch)?
                        .into_array(filter_batch.num_rows())?;

                    // The boolean selection mask of the join filter result
                    let pre_mask =
                        datafusion_common::cast::as_boolean_array(&filter_result)?;

                    // If there are nulls in join filter result, exclude them from selecting
                    // the rows to output.
                    let mask = if pre_mask.null_count() > 0 {
                        compute::prep_null_mask_filter(
                            datafusion_common::cast::as_boolean_array(&filter_result)?,
                        )
                    } else {
                        pre_mask.clone()
                    };

                    // Push the filtered batch which contains rows passing join filter to the output
                    if matches!(
                        self.join_type,
                        JoinType::Left
                            | JoinType::LeftSemi
                            | JoinType::Right
                            | JoinType::RightSemi
                            | JoinType::LeftAnti
                            | JoinType::RightAnti
                            | JoinType::LeftMark
                            | JoinType::RightMark
                            | JoinType::Full
                    ) {
                        self.staging_output_record_batches
                            .batches
                            .push(output_batch);
                    } else {
                        let filtered_batch = filter_record_batch(&output_batch, &mask)?;
                        self.staging_output_record_batches
                            .batches
                            .push(filtered_batch);
                    }

                    if !matches!(self.join_type, JoinType::Full) {
                        self.staging_output_record_batches.filter_mask.extend(&mask);
                    } else {
                        self.staging_output_record_batches
                            .filter_mask
                            .extend(pre_mask);
                    }
                    self.staging_output_record_batches
                        .row_indices
                        .extend(&left_indices);
                    self.staging_output_record_batches.batch_ids.resize(
                        self.staging_output_record_batches.batch_ids.len()
                            + left_indices.len(),
                        self.streamed_batch_counter.load(Relaxed),
                    );

                    // For outer joins, we need to push the null joined rows to the output if
                    // all joined rows are failed on the join filter.
                    // I.e., if all rows joined from a streamed row are failed with the join filter,
                    // we need to join it with nulls as buffered side.
                    if matches!(self.join_type, JoinType::Full) {
                        let buffered_batch = &mut self.buffered_data.batches
                            [chunk.buffered_batch_idx.unwrap()];

                        for i in 0..pre_mask.len() {
                            // If the buffered row is not joined with streamed side,
                            // skip it.
                            if right_indices.is_null(i) {
                                continue;
                            }

                            let buffered_index = right_indices.value(i);

                            buffered_batch.join_filter_not_matched_map.insert(
                                buffered_index,
                                *buffered_batch
                                    .join_filter_not_matched_map
                                    .get(&buffered_index)
                                    .unwrap_or(&true)
                                    && !pre_mask.value(i),
                            );
                        }
                    }
                } else {
                    self.staging_output_record_batches
                        .batches
                        .push(output_batch);
                }
            } else {
                self.staging_output_record_batches
                    .batches
                    .push(output_batch);
            }
        }

        self.streamed_batch.output_indices.clear();

        Ok(())
    }

    fn output_record_batch_and_reset(&mut self) -> Result<RecordBatch> {
        let record_batch =
            concat_batches(&self.schema, &self.staging_output_record_batches.batches)?;
        (&record_batch).record_output(&self.join_metrics.baseline_metrics());
        // If join filter exists, `self.output_size` is not accurate as we don't know the exact
        // number of rows in the output record batch. If streamed row joined with buffered rows,
        // once join filter is applied, the number of output rows may be more than 1.
        // If `record_batch` is empty, we should reset `self.output_size` to 0. It could be happened
        // when the join filter is applied and all rows are filtered out.
        if record_batch.num_rows() == 0 || record_batch.num_rows() > self.output_size {
            self.output_size = 0;
        } else {
            self.output_size -= record_batch.num_rows();
        }

        if !(self.filter.is_some()
            && matches!(
                self.join_type,
                JoinType::Left
                    | JoinType::LeftSemi
                    | JoinType::Right
                    | JoinType::RightSemi
                    | JoinType::LeftAnti
                    | JoinType::RightAnti
                    | JoinType::LeftMark
                    | JoinType::RightMark
                    | JoinType::Full
            ))
        {
            self.staging_output_record_batches.batches.clear();
        }

        Ok(record_batch)
    }

    fn filter_joined_batch(&mut self) -> Result<RecordBatch> {
        let record_batch =
            concat_batches(&self.schema, &self.staging_output_record_batches.batches)?;
        let mut out_indices = self.staging_output_record_batches.row_indices.finish();
        let mut out_mask = self.staging_output_record_batches.filter_mask.finish();
        let mut batch_ids = &self.staging_output_record_batches.batch_ids;
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

        if out_mask.is_empty() {
            self.staging_output_record_batches.batches.clear();
            return Ok(record_batch);
        }

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
        let mut filtered_record_batch =
            filter_record_batch(record_batch, corrected_mask)?;
        let left_columns_length = self.streamed_schema.fields.len();
        let right_columns_length = self.buffered_schema.fields.len();

        if matches!(
            self.join_type,
            JoinType::Left | JoinType::LeftMark | JoinType::Right | JoinType::RightMark
        ) {
            let null_mask = compute::not(corrected_mask)?;
            let null_joined_batch = filter_record_batch(record_batch, &null_mask)?;

            let mut right_columns = create_unmatched_columns(
                self.join_type,
                &self.buffered_schema,
                null_joined_batch.num_rows(),
            );

            let columns = match self.join_type {
                JoinType::Right => {
                    // The first columns are the right columns.
                    let left_columns = null_joined_batch
                        .columns()
                        .iter()
                        .skip(right_columns_length)
                        .cloned()
                        .collect::<Vec<_>>();

                    right_columns.extend(left_columns);
                    right_columns
                }
                JoinType::Left | JoinType::LeftMark | JoinType::RightMark => {
                    // The first columns are the left columns.
                    let mut left_columns = null_joined_batch
                        .columns()
                        .iter()
                        .take(left_columns_length)
                        .cloned()
                        .collect::<Vec<_>>();

                    left_columns.extend(right_columns);
                    left_columns
                }
                _ => exec_err!("Did not expect join type {}", self.join_type)?,
            };

            // Push the streamed/buffered batch joined nulls to the output
            let null_joined_streamed_batch =
                RecordBatch::try_new(Arc::clone(&self.schema), columns)?;

            filtered_record_batch = concat_batches(
                &self.schema,
                &[filtered_record_batch, null_joined_streamed_batch],
            )?;
        } else if matches!(self.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
            let output_column_indices = (0..left_columns_length).collect::<Vec<_>>();
            filtered_record_batch =
                filtered_record_batch.project(&output_column_indices)?;
        } else if matches!(self.join_type, JoinType::RightAnti | JoinType::RightSemi) {
            let output_column_indices = (0..right_columns_length).collect::<Vec<_>>();
            filtered_record_batch =
                filtered_record_batch.project(&output_column_indices)?;
        } else if matches!(self.join_type, JoinType::Full)
            && corrected_mask.false_count() > 0
        {
            // Find rows which joined by key but Filter predicate evaluated as false
            let joined_filter_not_matched_mask = compute::not(corrected_mask)?;
            let joined_filter_not_matched_batch =
                filter_record_batch(record_batch, &joined_filter_not_matched_mask)?;

            // Add left unmatched rows adding the right side as nulls
            let right_null_columns = self
                .buffered_schema
                .fields()
                .iter()
                .map(|f| {
                    new_null_array(
                        f.data_type(),
                        joined_filter_not_matched_batch.num_rows(),
                    )
                })
                .collect::<Vec<_>>();

            let mut result_joined = joined_filter_not_matched_batch
                .columns()
                .iter()
                .take(left_columns_length)
                .cloned()
                .collect::<Vec<_>>();

            result_joined.extend(right_null_columns);

            let left_null_joined_batch =
                RecordBatch::try_new(Arc::clone(&self.schema), result_joined)?;

            // Add right unmatched rows adding the left side as nulls
            let mut result_joined = self
                .streamed_schema
                .fields()
                .iter()
                .map(|f| {
                    new_null_array(
                        f.data_type(),
                        joined_filter_not_matched_batch.num_rows(),
                    )
                })
                .collect::<Vec<_>>();

            let right_data = joined_filter_not_matched_batch
                .columns()
                .iter()
                .skip(left_columns_length)
                .cloned()
                .collect::<Vec<_>>();

            result_joined.extend(right_data);

            filtered_record_batch = concat_batches(
                &self.schema,
                &[filtered_record_batch, left_null_joined_batch],
            )?;
        }

        self.staging_output_record_batches.clear();

        Ok(filtered_record_batch)
    }
}

fn create_unmatched_columns(
    join_type: JoinType,
    schema: &SchemaRef,
    size: usize,
) -> Vec<ArrayRef> {
    if matches!(join_type, JoinType::LeftMark | JoinType::RightMark) {
        vec![Arc::new(BooleanArray::from(vec![false; size])) as ArrayRef]
    } else {
        schema
            .fields()
            .iter()
            .map(|f| new_null_array(f.data_type(), size))
            .collect::<Vec<_>>()
    }
}

/// Gets the arrays which join filters are applied on.
fn get_filter_column(
    join_filter: &Option<JoinFilter>,
    streamed_columns: &[ArrayRef],
    buffered_columns: &[ArrayRef],
) -> Vec<ArrayRef> {
    let mut filter_columns = vec![];

    if let Some(f) = join_filter {
        let left_columns = f
            .column_indices()
            .iter()
            .filter(|col_index| col_index.side == JoinSide::Left)
            .map(|i| Arc::clone(&streamed_columns[i.index]))
            .collect::<Vec<_>>();

        let right_columns = f
            .column_indices()
            .iter()
            .filter(|col_index| col_index.side == JoinSide::Right)
            .map(|i| Arc::clone(&buffered_columns[i.index]))
            .collect::<Vec<_>>();

        filter_columns.extend(left_columns);
        filter_columns.extend(right_columns);
    }

    filter_columns
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
        // In memory batch
        BufferedBatchState::InMemory(batch) => Ok(batch
            .columns()
            .iter()
            .map(|column| take(column, &buffered_indices, None))
            .collect::<Result<Vec<_>, ArrowError>>()
            .map_err(Into::<DataFusionError>::into)?),
        // If the batch was spilled to disk, less likely
        BufferedBatchState::Spilled(spill_file) => {
            let mut buffered_cols: Vec<ArrayRef> =
                Vec::with_capacity(buffered_indices.len());

            let file = BufReader::new(File::open(spill_file.path())?);
            let reader = StreamReader::try_new(file, None)?;

            for batch in reader {
                batch?.columns().iter().for_each(|column| {
                    buffered_cols.extend(take(column, &buffered_indices, None))
                });
            }

            Ok(buffered_cols)
        }
    }
}

/// Buffered data contains all buffered batches with one unique join key
#[derive(Debug, Default)]
pub(super) struct BufferedData {
    /// Buffered batches with the same key
    pub batches: VecDeque<BufferedBatch>,
    /// current scanning batch index used in join_partial()
    pub scanning_batch_idx: usize,
    /// current scanning offset used in join_partial()
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

    pub fn scanning_finish(&mut self) {
        self.scanning_batch_idx = self.batches.len();
        self.scanning_offset = 0;
    }
}

/// Get join array refs of given batch and join columns
fn join_arrays(batch: &RecordBatch, on_column: &[PhysicalExprRef]) -> Vec<ArrayRef> {
    on_column
        .iter()
        .map(|c| {
            let num_rows = batch.num_rows();
            let c = c.evaluate(batch).unwrap();
            c.into_array(num_rows).unwrap()
        })
        .collect()
}

/// A faster version of compare_join_arrays() that only output whether
/// the given two rows are equal
fn is_join_arrays_equal(
    left_arrays: &[ArrayRef],
    left: usize,
    right_arrays: &[ArrayRef],
    right: usize,
) -> Result<bool> {
    let mut is_equal = true;
    for (left_array, right_array) in left_arrays.iter().zip(right_arrays) {
        macro_rules! compare_value {
            ($T:ty) => {{
                match (left_array.is_null(left), right_array.is_null(right)) {
                    (false, false) => {
                        let left_array =
                            left_array.as_any().downcast_ref::<$T>().unwrap();
                        let right_array =
                            right_array.as_any().downcast_ref::<$T>().unwrap();
                        if left_array.value(left) != right_array.value(right) {
                            is_equal = false;
                        }
                    }
                    (true, false) => is_equal = false,
                    (false, true) => is_equal = false,
                    _ => {}
                }
            }};
        }

        match left_array.data_type() {
            DataType::Null => {}
            DataType::Boolean => compare_value!(BooleanArray),
            DataType::Int8 => compare_value!(Int8Array),
            DataType::Int16 => compare_value!(Int16Array),
            DataType::Int32 => compare_value!(Int32Array),
            DataType::Int64 => compare_value!(Int64Array),
            DataType::UInt8 => compare_value!(UInt8Array),
            DataType::UInt16 => compare_value!(UInt16Array),
            DataType::UInt32 => compare_value!(UInt32Array),
            DataType::UInt64 => compare_value!(UInt64Array),
            DataType::Float32 => compare_value!(Float32Array),
            DataType::Float64 => compare_value!(Float64Array),
            DataType::Utf8 => compare_value!(StringArray),
            DataType::Utf8View => compare_value!(StringViewArray),
            DataType::LargeUtf8 => compare_value!(LargeStringArray),
            DataType::Binary => compare_value!(BinaryArray),
            DataType::BinaryView => compare_value!(BinaryViewArray),
            DataType::FixedSizeBinary(_) => compare_value!(FixedSizeBinaryArray),
            DataType::LargeBinary => compare_value!(LargeBinaryArray),
            DataType::Decimal32(..) => compare_value!(Decimal32Array),
            DataType::Decimal64(..) => compare_value!(Decimal64Array),
            DataType::Decimal128(..) => compare_value!(Decimal128Array),
            DataType::Decimal256(..) => compare_value!(Decimal256Array),
            DataType::Timestamp(time_unit, None) => match time_unit {
                TimeUnit::Second => compare_value!(TimestampSecondArray),
                TimeUnit::Millisecond => compare_value!(TimestampMillisecondArray),
                TimeUnit::Microsecond => compare_value!(TimestampMicrosecondArray),
                TimeUnit::Nanosecond => compare_value!(TimestampNanosecondArray),
            },
            DataType::Date32 => compare_value!(Date32Array),
            DataType::Date64 => compare_value!(Date64Array),
            dt => {
                return not_impl_err!(
                    "Unsupported data type in sort merge join comparator: {}",
                    dt
                );
            }
        }
        if !is_equal {
            return Ok(false);
        }
    }
    Ok(true)
}
