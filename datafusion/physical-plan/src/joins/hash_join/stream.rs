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

//! Stream implementation for Hash Join
//!
//! This module implements [`HashJoinStream`], the streaming engine for
//! [`super::HashJoinExec`]. See comments in [`HashJoinStream`] for more details.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Poll;

use crate::coalesce::{LimitedBatchCoalescer, PushBatchStatus};
use crate::joins::Map;
use crate::joins::MapOffset;
use crate::joins::PartitionMode;
use crate::joins::hash_join::exec::JoinLeftData;
use crate::joins::hash_join::shared_bounds::{
    PartitionBounds, PartitionBuildData, SharedBuildAccumulator,
};
use crate::joins::utils::{
    OnceFut, equal_rows_arr, equal_rows_mask, get_anti_indices,
    get_final_indices_from_shared_bitmap, is_contiguous_range,
};
use crate::stream::EmptyRecordBatchStream;
use crate::{
    RecordBatchStream, SendableRecordBatchStream, handle_state,
    hash_utils::create_hashes,
    joins::utils::{
        BuildProbeJoinMetrics, ColumnIndex, ExistenceProbe, JoinFilter, JoinHashMapType,
        StatefulStreamResult, adjust_indices_by_join_type, apply_join_filter_to_indices,
        build_batch_empty_build_side, build_batch_from_indices,
        need_produce_result_in_final,
    },
};

use arrow::array::{
    Array, ArrayRef, BooleanArray, UInt32Array, UInt64Array, downcast_array,
};
use arrow::compute::FilterBuilder;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{
    JoinSide, JoinType, NullEquality, Result, internal_datafusion_err, internal_err,
};
use datafusion_physical_expr::PhysicalExprRef;

use datafusion_common::hash_utils::RandomState;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use futures::{Stream, StreamExt, ready};

/// Represents build-side of hash join.
pub(super) enum BuildSide {
    /// Indicates that build-side not collected yet
    Initial(BuildSideInitialState),
    /// Indicates that build-side data has been collected
    Ready(BuildSideReadyState),
}

/// Container for BuildSide::Initial related data
pub(super) struct BuildSideInitialState {
    /// Future for building hash table from build-side input
    pub(super) left_fut: OnceFut<JoinLeftData>,
}

/// Container for BuildSide::Ready related data
pub(super) struct BuildSideReadyState {
    /// Collected build-side data
    left_data: Arc<JoinLeftData>,
}

impl BuildSide {
    /// Tries to extract BuildSideInitialState from BuildSide enum.
    /// Returns an error if state is not Initial.
    fn try_as_initial_mut(&mut self) -> Result<&mut BuildSideInitialState> {
        match self {
            BuildSide::Initial(state) => Ok(state),
            _ => internal_err!("Expected build side in initial state"),
        }
    }

    /// Tries to extract BuildSideReadyState from BuildSide enum.
    /// Returns an error if state is not Ready.
    fn try_as_ready(&self) -> Result<&BuildSideReadyState> {
        match self {
            BuildSide::Ready(state) => Ok(state),
            _ => internal_err!("Expected build side in ready state"),
        }
    }
}

/// Represents state of HashJoinStream
///
/// Expected state transitions performed by HashJoinStream are:
///
/// ```text
///
///       WaitBuildSide
///             │
///             ▼
///  ┌─► FetchProbeBatch ───► ExhaustedProbeSide ───► Completed
///  │          │
///  │          ▼
///  └─ ProcessProbeBatch
/// ```
#[derive(Debug, Clone)]
pub(super) enum HashJoinStreamState {
    /// Initial state for HashJoinStream indicating that build-side data not collected yet
    WaitBuildSide,
    /// Waiting for bounds to be reported by all partitions
    WaitPartitionBoundsReport,
    /// Indicates that build-side has been collected, and stream is ready for fetching probe-side
    FetchProbeBatch,
    /// Indicates that non-empty batch has been fetched from probe-side, and is ready to be processed
    ProcessProbeBatch(ProcessProbeBatchState),
    /// Indicates that probe-side has been fully processed
    ExhaustedProbeSide,
    /// Indicates that HashJoinStream execution is completed
    Completed,
}

impl HashJoinStreamState {
    /// Tries to extract ProcessProbeBatchState from HashJoinStreamState enum.
    /// Returns an error if state is not ProcessProbeBatchState.
    fn try_as_process_probe_batch_mut(&mut self) -> Result<&mut ProcessProbeBatchState> {
        match self {
            HashJoinStreamState::ProcessProbeBatch(state) => Ok(state),
            _ => internal_err!("Expected hash join stream in ProcessProbeBatch state"),
        }
    }
}

/// Container for HashJoinStreamState::ProcessProbeBatch related data
#[derive(Debug, Clone)]
pub(super) struct ProcessProbeBatchState {
    /// Current probe-side batch
    batch: RecordBatch,
    /// Probe-side on expressions values
    values: Vec<ArrayRef>,
    /// Starting offset for JoinHashMap lookups
    offset: MapOffset,
    /// Max joined probe-side index from current batch
    joined_probe_idx: Option<usize>,
}

impl ProcessProbeBatchState {
    fn advance(&mut self, offset: MapOffset, joined_probe_idx: Option<usize>) {
        self.offset = offset;
        if joined_probe_idx.is_some() {
            self.joined_probe_idx = joined_probe_idx;
        }
    }
}

/// Lifecycle of this partition's build-data report to the shared coordinator.
///
/// `Scheduled` means the reporting `OnceFut` has been constructed but is lazy:
/// the coordinator has not necessarily observed the report. Only `Delivered`
/// guarantees the coordinator saw it, so `Drop` must still cancel a `Scheduled`
/// partition — otherwise sibling partitions can wait forever for a report that
/// never runs.
#[derive(Debug, PartialEq, Eq)]
enum BuildReportState {
    NotReported,
    Scheduled,
    Delivered,
    Canceled,
    Finalized,
}

/// Owns the stream-side lifecycle for one partition's build-data report.
struct BuildReportHandle {
    partition: usize,
    mode: PartitionMode,
    build_accumulator: Option<Arc<SharedBuildAccumulator>>,
    waiter: Option<OnceFut<()>>,
    state: BuildReportState,
}

impl BuildReportHandle {
    fn new(
        partition: usize,
        mode: PartitionMode,
        build_accumulator: Option<Arc<SharedBuildAccumulator>>,
    ) -> Self {
        Self {
            partition,
            mode,
            build_accumulator,
            waiter: None,
            state: BuildReportState::NotReported,
        }
    }

    fn has_accumulator(&self) -> bool {
        self.build_accumulator.is_some()
    }

    fn schedule(&mut self, build_data: PartitionBuildData) {
        let Some(build_accumulator) = &self.build_accumulator else {
            // Defensive no-op terminal state; current callers avoid scheduling
            // unless an accumulator is present.
            self.finalize();
            return;
        };

        debug_assert!(matches!(self.state, BuildReportState::NotReported));
        let acc = Arc::clone(build_accumulator);
        self.waiter = Some(OnceFut::new(async move {
            acc.report_build_data(build_data).await
        }));
        self.state = BuildReportState::Scheduled;
    }

    fn poll_delivery(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<()>> {
        if let Some(ref mut fut) = self.waiter {
            ready!(fut.get_shared(cx))?;
            if !matches!(self.state, BuildReportState::Delivered) {
                debug_assert!(matches!(self.state, BuildReportState::Scheduled));
                self.state = BuildReportState::Delivered;
            }
        }
        Poll::Ready(Ok(()))
    }

    fn cancel_pending(&mut self) {
        if matches!(
            self.state,
            BuildReportState::Delivered
                | BuildReportState::Canceled
                | BuildReportState::Finalized
        ) {
            return;
        }

        if self.mode == PartitionMode::Partitioned
            && let Some(build_accumulator) = &self.build_accumulator
        {
            build_accumulator.report_canceled_partition(self.partition);
            self.state = BuildReportState::Canceled;
        } else {
            self.finalize();
        }
    }

    fn finalize(&mut self) {
        self.state = BuildReportState::Finalized;
    }

    #[cfg(test)]
    fn state(&self) -> &BuildReportState {
        &self.state
    }
}

impl Drop for BuildReportHandle {
    fn drop(&mut self) {
        self.cancel_pending();
    }
}

/// [`Stream`] for [`super::HashJoinExec`] that does the actual join.
///
/// This stream:
///
/// - Collecting the build side (left input) into a hash map
/// - Iterating over the probe side (right input) in streaming fashion
/// - Looking up matches against the hash table and applying join filters
/// - Producing joined [`RecordBatch`]es incrementally
/// - Emitting unmatched rows for outer/semi/anti joins in the final stage
pub(super) struct HashJoinStream {
    /// Partition identifier for debugging and determinism
    partition: usize,
    /// Input schema
    schema: Arc<Schema>,
    /// equijoin columns from the right (probe side)
    on_right: Vec<PhysicalExprRef>,
    /// optional join filter
    filter: Option<JoinFilter>,
    /// type of the join (left, right, semi, etc)
    join_type: JoinType,
    /// right (probe) input
    right: SendableRecordBatchStream,
    /// Random state used for hashing initialization
    random_state: RandomState,
    /// Metrics
    join_metrics: BuildProbeJoinMetrics,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// Defines the null equality for the join.
    null_equality: NullEquality,
    /// State of the stream
    state: HashJoinStreamState,
    /// Build side
    build_side: BuildSide,
    /// Maximum output batch size
    batch_size: usize,
    /// Scratch space for computing hashes
    hashes_buffer: Vec<u64>,
    /// Reusable scratch space for probe-side lookups
    probe_scratch: ProbeScratch,
    /// Specifies whether the right side has an ordering to potentially preserve
    right_side_ordered: bool,
    /// Owns this partition's build-data report lifecycle.
    build_report: BuildReportHandle,
    /// Partitioning mode to use
    mode: PartitionMode,
    /// Output buffer for coalescing small batches into larger ones with optional fetch limit.
    /// Uses `LimitedBatchCoalescer` to efficiently combine batches and absorb limit with 'fetch'
    output_buffer: LimitedBatchCoalescer,
    /// Whether this is a null-aware anti join
    null_aware: bool,
}

impl RecordBatchStream for HashJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Executes lookups by hash against JoinHashMap and resolves potential
/// hash collisions.
/// Returns build/probe indices satisfying the equality condition, along with
/// (optional) starting point for next iteration.
///
/// # Example
///
/// For `LEFT.b1 = RIGHT.b2`:
/// LEFT (build) Table:
/// ```text
///  a1  b1  c1
///  1   1   10
///  3   3   30
///  5   5   50
///  7   7   70
///  9   8   90
///  11  8   110
///  13   10  130
/// ```
///
/// RIGHT (probe) Table:
/// ```text
///  a2   b2  c2
///  2    2   20
///  4    4   40
///  6    6   60
///  8    8   80
/// 10   10  100
/// 12   10  120
/// ```
///
/// The result is
/// ```text
/// "+----+----+-----+----+----+-----+",
/// "| a1 | b1 | c1  | a2 | b2 | c2  |",
/// "+----+----+-----+----+----+-----+",
/// "| 9  | 8  | 90  | 8  | 8  | 80  |",
/// "| 11 | 8  | 110 | 8  | 8  | 80  |",
/// "| 13 | 10 | 130 | 10 | 10 | 100 |",
/// "| 13 | 10 | 130 | 12 | 10 | 120 |",
/// "+----+----+-----+----+----+-----+"
/// ```
///
/// And the result of build and probe indices are:
/// ```text
/// Build indices: 4, 5, 6, 6
/// Probe indices: 3, 3, 4, 5
/// ```
/// Reusable scratch buffers for probe-side lookups, allocated once per stream
/// and recycled across probe chunks.
///
/// `probe_indices` and `build_indices` are shared by the pair-producing and
/// existence-probe paths; the candidate-window buffers belong to the
/// existence probe's chain walk and stay empty on the pair-producing path.
struct ProbeScratch {
    /// Probe-side row indices produced by map lookups
    probe_indices: Vec<u32>,
    /// Build-side row indices produced by map lookups
    build_indices: Vec<u64>,
    /// Original candidate position of each entry in the current window
    candidate_positions: Vec<u32>,
    /// Original candidate position of each entry in the next window
    next_candidate_positions: Vec<u32>,
    /// Per-candidate match verdicts, indexed by original candidate position
    matched_candidates: Vec<bool>,
}

impl ProbeScratch {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            probe_indices: Vec::with_capacity(capacity),
            build_indices: Vec::with_capacity(capacity),
            candidate_positions: Vec::new(),
            next_candidate_positions: Vec::new(),
            matched_candidates: Vec::new(),
        }
    }
}

#[expect(clippy::too_many_arguments)]
pub(super) fn lookup_join_hashmap(
    build_hashmap: &dyn JoinHashMapType,
    build_side_values: &[ArrayRef],
    probe_side_values: &[ArrayRef],
    null_equality: NullEquality,
    hashes_buffer: &[u64],
    limit: usize,
    offset: MapOffset,
    probe_indices_buffer: &mut Vec<u32>,
    build_indices_buffer: &mut Vec<u64>,
) -> Result<(UInt64Array, UInt32Array, Option<MapOffset>)> {
    let next_offset = build_hashmap.get_matched_indices_with_limit_offset(
        hashes_buffer,
        limit,
        offset,
        probe_indices_buffer,
        build_indices_buffer,
    );

    let build_indices_unfiltered: UInt64Array =
        std::mem::take(build_indices_buffer).into();
    let probe_indices_unfiltered: UInt32Array =
        std::mem::take(probe_indices_buffer).into();

    // TODO: optimize equal_rows_arr to avoid allocation of intermediate arrays
    // https://github.com/apache/datafusion/issues/12131
    let (build_indices, probe_indices) = equal_rows_arr(
        &build_indices_unfiltered,
        &probe_indices_unfiltered,
        build_side_values,
        probe_side_values,
        null_equality,
    )?;

    // Reclaim buffers
    *build_indices_buffer = build_indices_unfiltered.into_parts().1.into();
    *probe_indices_buffer = probe_indices_unfiltered.into_parts().1.into();

    Ok((build_indices, probe_indices, next_offset))
}

/// Returns the sorted, deduplicated indices of the probe rows in
/// `probe_range` that have at least one key-equal row on the build side.
///
/// This is the chained-hash-map implementation of the existence probe used by
/// `HashJoinStream::try_existence_probe_batch`; the dense-key `ArrayMap`
/// strategy provides an equivalent inherent method of the same name.
///
/// # Algorithm
///
/// Walking each probe row's hash chain to completion would do work
/// proportional to fanout, and comparing keys row-by-row during the walk
/// would lose vectorization. Instead, all chains are walked in lockstep, one
/// link per round, comparing keys for a whole round with one vectorized
/// [`equal_rows_mask`] call:
///
/// 1. Gather the initial candidate window: each probe row's first chain
///    entry.
/// 2. Compare build/probe join keys for the entire window at once.
/// 3. Window entries whose keys matched are finished; the rest advance to
///    their next chain entry, dropping out when their chain is exhausted.
///    Repeat from step 2 until the window is empty.
///
/// A hash chain groups build rows with identical hash values: absent collisions
/// a chain holds duplicates of a single key, and a probe row either matches its
/// first candidate or will match none of them. In practice, nearly every row
/// will resolve in the first round; later rounds handle hash collisions.
///
/// `scratch` is reused across calls: its index vectors back the per-round
/// index arrays and are returned (emptied) on exit, and its candidate-window
/// buffers retain their allocations across rounds and chunks.
fn get_probe_indices_with_any_match(
    existence_probe: &dyn ExistenceProbe,
    build_side_values: &[ArrayRef],
    probe_side_values: &[ArrayRef],
    null_equality: NullEquality,
    hashes_buffer: &[u64],
    probe_range: Range<usize>,
    scratch: &mut ProbeScratch,
) -> Result<UInt32Array> {
    // Gather the initial candidates: a (build index, probe index) pair for
    // each probe row in `probe_range` whose hash is present in the map.
    existence_probe.get_probe_first_candidates(
        hashes_buffer,
        probe_range,
        &mut scratch.probe_indices,
        &mut scratch.build_indices,
    );

    let mut current_build_indices: UInt64Array =
        std::mem::take(&mut scratch.build_indices).into();
    let initial_probe_indices: UInt32Array =
        std::mem::take(&mut scratch.probe_indices).into();

    let equal_mask = equal_rows_mask(
        &current_build_indices,
        &initial_probe_indices,
        build_side_values,
        probe_side_values,
        null_equality,
    )?;

    // The equality check passed for every candidate, so the matched set is all
    // of `initial_probe_indices`: return it as-is instead of filtering by an
    // all-true mask.
    if equal_mask.null_count() == 0 && equal_mask.true_count() == equal_mask.len() {
        scratch.build_indices = current_build_indices.into_parts().1.into();
        let capacity = initial_probe_indices.len();
        scratch.probe_indices = Vec::with_capacity(capacity);
        return Ok(initial_probe_indices);
    }

    // Unique hashes mean every chain has length 1, so the chain walk below
    // is unnecessary.
    if existence_probe.has_unique_hashes() {
        let filter_builder = FilterBuilder::new(&equal_mask).optimize().build();
        let matched_probe_indices = filter_builder.filter(&initial_probe_indices)?;

        scratch.build_indices = current_build_indices.into_parts().1.into();
        scratch.probe_indices = initial_probe_indices.into_parts().1.into();

        return Ok(downcast_array(matched_probe_indices.as_ref()));
    }

    /// Marks the candidates of the current window whose join keys compared equal.
    fn mark_matched(equal_mask: &BooleanArray, positions: &[u32], matched: &mut [bool]) {
        if equal_mask.null_count() == 0 && equal_mask.true_count() == equal_mask.len() {
            for &position in positions {
                matched[position as usize] = true;
            }
        } else {
            for (i, &position) in positions.iter().enumerate() {
                if !equal_mask.is_null(i) && equal_mask.value(i) {
                    matched[position as usize] = true;
                }
            }
        }
    }

    // General case: walk the chains round by round. `matched_candidates[i]`
    // records the verdict for the i-th initial candidate (one per probe
    // row); because the window shrinks as rows resolve, each window entry
    // carries its original candidate position in `candidate_positions`
    // (initially the identity mapping) so later rounds can record verdicts
    // in the right slot.
    let candidate_count = initial_probe_indices.len();
    scratch.candidate_positions.clear();
    scratch
        .candidate_positions
        .extend(0..candidate_count as u32);
    scratch.matched_candidates.clear();
    scratch.matched_candidates.resize(candidate_count, false);
    // Cheap Arc clone; reassigned (releasing the alias) on the first chain advance.
    let mut current_probe_indices = initial_probe_indices.clone();
    let mut equal_mask = equal_mask;

    loop {
        mark_matched(
            &equal_mask,
            &scratch.candidate_positions,
            &mut scratch.matched_candidates,
        );

        // Advance each unmatched window entry one link down its hash chain;
        // matched and chain-exhausted entries drop out of the window.
        existence_probe.get_next_probe_candidates(
            current_build_indices.values().as_ref(),
            current_probe_indices.values().as_ref(),
            &scratch.candidate_positions,
            &scratch.matched_candidates,
            &mut scratch.next_candidate_positions,
            &mut scratch.build_indices,
            &mut scratch.probe_indices,
        );

        if scratch.build_indices.is_empty() {
            break;
        }

        std::mem::swap(
            &mut scratch.candidate_positions,
            &mut scratch.next_candidate_positions,
        );
        current_build_indices = std::mem::take(&mut scratch.build_indices).into();
        current_probe_indices = std::mem::take(&mut scratch.probe_indices).into();

        equal_mask = equal_rows_mask(
            &current_build_indices,
            &current_probe_indices,
            build_side_values,
            probe_side_values,
            null_equality,
        )?;
    }

    // Release the potential alias of `initial_probe_indices` so the buffer
    // recycling below stays zero-copy.
    drop(current_probe_indices);

    // Collect the probe index of every candidate position that matched in
    // some round; ascending position order keeps the result sorted.
    scratch.probe_indices.clear();
    for (position, &is_matched) in scratch.matched_candidates.iter().enumerate() {
        if is_matched {
            scratch
                .probe_indices
                .push(initial_probe_indices.value(position));
        }
    }

    scratch.build_indices = current_build_indices.into_parts().1.into();
    let matched_probe_indices = std::mem::take(&mut scratch.probe_indices).into();
    scratch.probe_indices = initial_probe_indices.into_parts().1.into();

    Ok(matched_probe_indices)
}

/// Counts the number of distinct elements in the input array.
///
/// The input array must be sorted (e.g., `[0, 1, 1, 2, 2, ...]`) and contain no null values.
#[inline]
fn count_distinct_sorted_indices(indices: &UInt32Array) -> usize {
    if indices.is_empty() {
        return 0;
    }

    debug_assert!(indices.null_count() == 0);

    let values_buf = indices.values();
    let values = values_buf.as_ref();
    let mut iter = values.iter();
    let Some(&first) = iter.next() else {
        return 0;
    };

    let mut count = 1usize;
    let mut last = first;
    for &value in iter {
        if value != last {
            last = value;
            count += 1;
        }
    }
    count
}

/// Builds an output batch by slicing `probe_batch` instead of `take`-ing it,
/// when `probe_indices` form a contiguous ascending run and every output
/// column is sourced from the probe side.
///
/// Returns `Ok(None)` when the fast path does not apply and the caller should
/// fall back to [`build_batch_from_indices`].
fn try_build_contiguous_probe_output(
    schema: &Arc<Schema>,
    column_indices: &[ColumnIndex],
    probe_batch: &RecordBatch,
    probe_indices: &UInt32Array,
) -> Result<Option<RecordBatch>> {
    if schema.fields().is_empty() {
        return Ok(None);
    }

    let Some(range) = is_contiguous_range(probe_indices) else {
        return Ok(None);
    };

    let probe_slice = probe_batch.slice(range.start, range.len());
    let mut columns = Vec::with_capacity(column_indices.len());
    for column_index in column_indices {
        if column_index.side != JoinSide::Right {
            return Ok(None);
        }
        columns.push(Arc::clone(probe_slice.column(column_index.index)));
    }

    Ok(Some(RecordBatch::try_new(Arc::clone(schema), columns)?))
}

impl HashJoinStream {
    #[expect(clippy::too_many_arguments)]
    pub(super) fn new(
        partition: usize,
        schema: Arc<Schema>,
        on_right: Vec<PhysicalExprRef>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        right: SendableRecordBatchStream,
        random_state: RandomState,
        join_metrics: BuildProbeJoinMetrics,
        column_indices: Vec<ColumnIndex>,
        null_equality: NullEquality,
        state: HashJoinStreamState,
        build_side: BuildSide,
        batch_size: usize,
        hashes_buffer: Vec<u64>,
        right_side_ordered: bool,
        build_accumulator: Option<Arc<SharedBuildAccumulator>>,
        mode: PartitionMode,
        null_aware: bool,
        fetch: Option<usize>,
    ) -> Self {
        // Create output buffer with coalescing and optional fetch limit.
        let output_buffer =
            LimitedBatchCoalescer::new(Arc::clone(&schema), batch_size, fetch);

        Self {
            partition,
            schema,
            on_right,
            filter,
            join_type,
            right,
            random_state,
            join_metrics,
            column_indices,
            null_equality,
            state,
            build_side,
            batch_size,
            hashes_buffer,
            probe_scratch: ProbeScratch::with_capacity(batch_size),
            right_side_ordered,
            build_report: BuildReportHandle::new(partition, mode, build_accumulator),
            mode,
            output_buffer,
            null_aware,
        }
    }

    /// Try the existence-probe fast path for a probe-batch chunk.
    ///
    /// RightSemi and RightAnti joins only need to know whether each probe row
    /// has at least one equal build row. For those joins, when there is no
    /// join filter, the build side is non-empty, and the build-side map
    /// supports [`ExistenceProbe`], this path scans a range of probe rows and
    /// emits each probe row at most once, stopping each row's hash-chain
    /// search at its first key-equal match.
    ///
    /// Returns false when the path is ineligible and the caller should fall
    /// back to the general pair-producing path.
    fn try_existence_probe_batch(&mut self) -> Result<bool> {
        let state = self.state.try_as_process_probe_batch_mut()?;
        let build_side = self.build_side.try_as_ready()?;
        let probe_batch_num_rows = state.batch.num_rows();

        // An empty build side falls through to the general path, which emits
        // whole probe batches at once via `build_batch_empty_build_side`
        // instead of probing the empty map row by row.
        if !matches!(self.join_type, JoinType::RightSemi | JoinType::RightAnti)
            || self.filter.is_some()
            || build_side.left_data.map().is_empty()
        {
            return Ok(false);
        }

        let timer = self.join_metrics.join_time.timer();

        let probe_range_start = state.offset.0;
        let probe_range_end =
            (probe_range_start + self.batch_size).min(probe_batch_num_rows);
        let probe_range = probe_range_start..probe_range_end;

        let matched_probe_indices = match build_side.left_data.map() {
            Map::HashMap(map) => {
                let Some(existence_probe) = map.existence_probe() else {
                    timer.done();
                    return Ok(false);
                };
                get_probe_indices_with_any_match(
                    existence_probe,
                    build_side.left_data.values(),
                    &state.values,
                    self.null_equality,
                    &self.hashes_buffer,
                    probe_range.clone(),
                    &mut self.probe_scratch,
                )?
            }
            Map::ArrayMap(array_map) => {
                array_map.get_probe_indices_with_any_match(
                    &state.values,
                    probe_range.clone(),
                    &mut self.probe_scratch.probe_indices,
                )?;
                UInt32Array::from(std::mem::take(&mut self.probe_scratch.probe_indices))
            }
        };
        let matched_probe_count = matched_probe_indices.len();

        // The existence path stops after the first equal build row for each
        // probe row, so exact fanout cannot be determined.  Leave avg_fanout as
        // N/A for this path.
        self.join_metrics
            .probe_hit_rate
            .add_total(probe_range.len());
        self.join_metrics
            .probe_hit_rate
            .add_part(matched_probe_count);

        let right_indices = match self.join_type {
            JoinType::RightSemi => matched_probe_indices.clone(),
            JoinType::RightAnti => {
                get_anti_indices(probe_range.clone(), &matched_probe_indices)
            }
            _ => {
                unreachable!("existence probe only runs for right semi/anti")
            }
        };

        let push_status = if right_indices.is_empty() {
            self.probe_scratch.probe_indices =
                matched_probe_indices.into_parts().1.into();
            PushBatchStatus::Continue
        } else {
            let batch = if let Some(batch) = try_build_contiguous_probe_output(
                &self.schema,
                &self.column_indices,
                &state.batch,
                &right_indices,
            )? {
                batch
            } else {
                let left_indices = UInt64Array::from(Vec::<u64>::new());
                build_batch_from_indices(
                    &self.schema,
                    build_side.left_data.batch(),
                    &state.batch,
                    &left_indices,
                    &right_indices,
                    &self.column_indices,
                    JoinSide::Left,
                    self.join_type,
                )?
            };

            let push_status = self.output_buffer.push_batch(batch)?;
            self.probe_scratch.probe_indices = right_indices.into_parts().1.into();
            push_status
        };

        timer.done();

        if probe_range_end == probe_batch_num_rows {
            self.state = HashJoinStreamState::FetchProbeBatch;
        } else {
            state.advance((probe_range_end, None), None);
        }

        if push_status == PushBatchStatus::LimitReached {
            self.output_buffer.finish()?;
            self.state = HashJoinStreamState::Completed;
        }

        Ok(true)
    }

    /// Returns the next state after the build side has been fully collected
    /// and any required build-side coordination has completed.
    fn state_after_build_ready(
        join_type: JoinType,
        left_data: &JoinLeftData,
    ) -> HashJoinStreamState {
        if left_data.map().is_empty()
            && join_type.empty_build_side_produces_empty_result()
        {
            HashJoinStreamState::Completed
        } else {
            HashJoinStreamState::FetchProbeBatch
        }
    }

    /// Transitions state after build-side data has been collected, automatically
    /// reporting build data to the accumulator when one is present.
    ///
    /// If a `build_accumulator` is configured, this method constructs the
    /// appropriate [`PartitionBuildData`], schedules the reporting future, and
    /// returns [`HashJoinStreamState::WaitPartitionBoundsReport`]. Otherwise it
    /// delegates to [`Self::state_after_build_ready`].
    fn transition_after_build_collected(
        &mut self,
        left_data: &Arc<JoinLeftData>,
    ) -> HashJoinStreamState {
        if !self.build_report.has_accumulator() {
            return Self::state_after_build_ready(self.join_type, left_data.as_ref());
        }

        let pushdown = left_data.membership().clone();
        let bounds = left_data
            .bounds
            .clone()
            .unwrap_or_else(|| PartitionBounds::new(vec![]));

        let build_data = match self.mode {
            PartitionMode::Partitioned => PartitionBuildData::Partitioned {
                partition_id: self.partition,
                pushdown,
                bounds,
            },
            PartitionMode::CollectLeft => {
                PartitionBuildData::CollectLeft { pushdown, bounds }
            }
            PartitionMode::Auto => unreachable!(
                "PartitionMode::Auto should not be present at execution time. This is a bug in DataFusion, please report it!"
            ),
        };

        self.build_report.schedule(build_data);
        HashJoinStreamState::WaitPartitionBoundsReport
    }

    /// Separate implementation function that unpins the [`HashJoinStream`] so
    /// that partial borrows work correctly
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            // First, check if we have any completed batches ready to emit
            if let Some(batch) = self.output_buffer.next_completed_batch() {
                return self
                    .join_metrics
                    .baseline
                    .record_poll(Poll::Ready(Some(Ok(batch))));
            }

            // Check if the coalescer has finished (limit reached and flushed)
            if self.output_buffer.is_finished() {
                return Poll::Ready(None);
            }

            return match self.state {
                HashJoinStreamState::WaitBuildSide => {
                    handle_state!(ready!(self.collect_build_side(cx)))
                }
                HashJoinStreamState::WaitPartitionBoundsReport => {
                    handle_state!(ready!(self.wait_for_partition_bounds_report(cx)))
                }
                HashJoinStreamState::FetchProbeBatch => {
                    handle_state!(ready!(self.fetch_probe_batch(cx)))
                }
                HashJoinStreamState::ProcessProbeBatch(_) => {
                    handle_state!(self.process_probe_batch())
                }
                HashJoinStreamState::ExhaustedProbeSide => {
                    handle_state!(self.process_unmatched_build_batch())
                }
                HashJoinStreamState::Completed if !self.output_buffer.is_empty() => {
                    // Flush any remaining buffered data
                    self.output_buffer.finish()?;
                    // Continue loop to emit the flushed batch
                    continue;
                }
                HashJoinStreamState::Completed => Poll::Ready(None),
            };
        }
    }

    /// Optional step to wait until build-side information (hash maps or bounds) has been reported by all partitions.
    /// This state is only entered if a build accumulator is present.
    ///
    /// ## Why wait?
    ///
    /// The dynamic filter is only built once all partitions have reported their information (hash maps or bounds).
    /// If we do not wait here, the probe-side scan may start before the filter is ready.
    /// This can lead to the probe-side scan missing the opportunity to apply the filter
    /// and skip reading unnecessary data.
    fn wait_for_partition_bounds_report(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        ready!(self.build_report.poll_delivery(cx))?;
        let build_side = self.build_side.try_as_ready()?;
        self.state =
            Self::state_after_build_ready(self.join_type, build_side.left_data.as_ref());
        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    /// Collects build-side data by polling `OnceFut` future from initialized build-side
    ///
    /// Updates build-side to `Ready`, and state to `FetchProbeSide`
    fn collect_build_side(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        let build_timer = self.join_metrics.build_time.timer();
        // build hash table from left (build) side, if not yet done
        let left_data = ready!(
            self.build_side
                .try_as_initial_mut()?
                .left_fut
                .get_shared(cx)
        )?;
        build_timer.done();

        // Note: For null-aware anti join, we need to check the probe side (right) for NULLs,
        // not the build side (left). The probe-side NULL check happens during process_probe_batch.
        // The probe_side_has_null flag will be set there if any probe batch contains NULL.

        self.state = self.transition_after_build_collected(&left_data);

        self.build_side = BuildSide::Ready(BuildSideReadyState { left_data });
        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    /// Fetches next batch from probe-side
    ///
    /// If non-empty batch has been fetched, updates state to `ProcessProbeBatchState`,
    /// otherwise updates state to `ExhaustedProbeSide`
    fn fetch_probe_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        match ready!(self.right.poll_next_unpin(cx)) {
            None => {
                // Release the probe-side input pipeline's resources. The schema
                // is preserved so callers that still query `self.right.schema()`
                // (e.g. for unmatched-build emission) keep working.
                let right_schema = self.right.schema();
                self.right = Box::pin(EmptyRecordBatchStream::new(right_schema));
                self.state = HashJoinStreamState::ExhaustedProbeSide;
            }
            Some(Ok(batch)) => {
                // Precalculate hash values for fetched batch
                let keys_values = evaluate_expressions_to_arrays(&self.on_right, &batch)?;

                if let Map::HashMap(_) = self.build_side.try_as_ready()?.left_data.map() {
                    self.hashes_buffer.clear();
                    self.hashes_buffer.resize(batch.num_rows(), 0);
                    create_hashes(
                        &keys_values,
                        &self.random_state,
                        &mut self.hashes_buffer,
                    )?;
                }

                self.join_metrics.input_batches.add(1);
                self.join_metrics.input_rows.add(batch.num_rows());

                self.state =
                    HashJoinStreamState::ProcessProbeBatch(ProcessProbeBatchState {
                        batch,
                        values: keys_values,
                        offset: (0, None),
                        joined_probe_idx: None,
                    });
            }
            Some(Err(err)) => return Poll::Ready(Err(err)),
        };

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    /// Joins current probe batch with build-side data and produces batch with matched output
    ///
    /// Updates state to `FetchProbeBatch`
    fn process_probe_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        if self.try_existence_probe_batch()? {
            return Ok(StatefulStreamResult::Continue);
        }

        let state = self.state.try_as_process_probe_batch_mut()?;
        let build_side = self.build_side.try_as_ready()?;
        let is_empty = build_side.left_data.map().is_empty();

        self.join_metrics
            .probe_hit_rate
            .add_total(state.batch.num_rows());

        let timer = self.join_metrics.join_time.timer();

        // Null-aware anti join semantics:
        // For LeftAnti: output LEFT (build) rows where LEFT.key NOT IN RIGHT.key
        // 1. If RIGHT (probe) contains NULL in any batch, no LEFT rows should be output
        // 2. LEFT rows with NULL keys should not be output (handled in final stage)
        if self.null_aware {
            // Mark that we've seen a probe batch with actual rows (probe side is non-empty)
            // Only set this if batch has rows - empty batches don't count
            // Use shared atomic state so all partitions can see this global information
            if state.batch.num_rows() > 0 {
                build_side
                    .left_data
                    .probe_side_non_empty
                    .store(true, Ordering::Relaxed);
            }

            // Check if probe side (RIGHT) contains NULL
            // Since null_aware validation ensures single column join, we only check the first column
            let probe_key_column = &state.values[0];
            if probe_key_column.null_count() > 0 {
                // Found NULL in probe side - set shared flag to prevent any output
                build_side
                    .left_data
                    .probe_side_has_null
                    .store(true, Ordering::Relaxed);
            }

            // If probe side has NULL (detected in this or any other partition), return empty result
            if build_side
                .left_data
                .probe_side_has_null
                .load(Ordering::Relaxed)
            {
                timer.done();
                self.state = HashJoinStreamState::FetchProbeBatch;
                return Ok(StatefulStreamResult::Continue);
            }
        }

        if is_empty {
            // Invariant: state_after_build_ready should have already completed
            // join types whose result is fixed to empty when the build side is empty.
            debug_assert!(!self.join_type.empty_build_side_produces_empty_result());
            let result = build_batch_empty_build_side(
                &self.schema,
                build_side.left_data.batch(),
                &state.batch,
                &self.column_indices,
                self.join_type,
            )?;
            timer.done();
            self.output_buffer.push_batch(result)?;
            self.state = HashJoinStreamState::FetchProbeBatch;

            return Ok(StatefulStreamResult::Continue);
        }

        // Find build/probe row pairs with equal join keys, bounded by
        // `batch_size`. `next_offset` identifies where to resume if the
        // candidate scan needs another output batch.
        let (left_indices, right_indices, next_offset) = match build_side.left_data.map()
        {
            Map::HashMap(map) => lookup_join_hashmap(
                map.as_ref(),
                build_side.left_data.values(),
                &state.values,
                self.null_equality,
                &self.hashes_buffer,
                self.batch_size,
                state.offset,
                &mut self.probe_scratch.probe_indices,
                &mut self.probe_scratch.build_indices,
            )?,
            Map::ArrayMap(array_map) => {
                let next_offset = array_map.get_matched_indices_with_limit_offset(
                    &state.values,
                    self.batch_size,
                    state.offset,
                    &mut self.probe_scratch.probe_indices,
                    &mut self.probe_scratch.build_indices,
                )?;
                (
                    UInt64Array::from(self.probe_scratch.build_indices.clone()),
                    UInt32Array::from(self.probe_scratch.probe_indices.clone()),
                    next_offset,
                )
            }
        };

        let distinct_right_indices_count = count_distinct_sorted_indices(&right_indices);

        self.join_metrics
            .probe_hit_rate
            .add_part(distinct_right_indices_count);

        self.join_metrics.avg_fanout.add_part(left_indices.len());

        self.join_metrics
            .avg_fanout
            .add_total(distinct_right_indices_count);

        // apply join filter, if any
        let (left_indices, right_indices) = if let Some(filter) = &self.filter {
            apply_join_filter_to_indices(
                build_side.left_data.batch(),
                &state.batch,
                left_indices,
                right_indices,
                filter,
                JoinSide::Left,
                None,
                self.join_type,
            )?
        } else {
            (left_indices, right_indices)
        };

        // mark joined left-side indices as visited, if required by join type
        if need_produce_result_in_final(self.join_type) {
            let mut bitmap = build_side.left_data.visited_indices_bitmap().lock();
            left_indices.iter().flatten().for_each(|x| {
                bitmap.set_bit(x as usize, true);
            });
        }

        // The goals of index alignment for different join types are:
        //
        // 1) Right & FullJoin -- to append all missing probe-side indices between
        //    previous (excluding) and current joined indices.
        // 2) SemiJoin -- deduplicate probe indices in range between previous
        //    (excluding) and current joined indices.
        // 3) AntiJoin -- return only missing indices in range between
        //    previous and current joined indices.
        //    Inclusion/exclusion of the indices themselves don't matter
        //
        // As a summary -- alignment range can be produced based only on
        // joined (matched with filters applied) probe side indices, excluding starting one
        // (left from previous iteration).

        // if any rows have been joined -- get last joined probe-side (right) row
        // it's important that index counts as "joined" after hash collisions checks
        // and join filters applied.
        let last_joined_right_idx = match right_indices.len() {
            0 => None,
            n => Some(right_indices.value(n - 1) as usize),
        };

        // Calculate range and perform alignment.
        // In case probe batch has been processed -- align all remaining rows.
        let index_alignment_range_start = state.joined_probe_idx.map_or(0, |v| v + 1);
        let index_alignment_range_end = if next_offset.is_none() {
            state.batch.num_rows()
        } else {
            last_joined_right_idx.map_or(0, |v| v + 1)
        };

        let (left_indices, right_indices) = adjust_indices_by_join_type(
            left_indices,
            right_indices,
            index_alignment_range_start..index_alignment_range_end,
            self.join_type,
            self.right_side_ordered,
        )?;

        // Build output batch and push to coalescer
        let (build_batch, probe_batch, join_side) =
            if self.join_type == JoinType::RightMark {
                (&state.batch, build_side.left_data.batch(), JoinSide::Right)
            } else {
                (build_side.left_data.batch(), &state.batch, JoinSide::Left)
            };

        let batch = build_batch_from_indices(
            &self.schema,
            build_batch,
            probe_batch,
            &left_indices,
            &right_indices,
            &self.column_indices,
            join_side,
            self.join_type,
        )?;

        let push_status = self.output_buffer.push_batch(batch)?;

        timer.done();

        // If limit reached, finish and move to Completed state
        if push_status == PushBatchStatus::LimitReached {
            self.output_buffer.finish()?;
            self.state = HashJoinStreamState::Completed;
            return Ok(StatefulStreamResult::Continue);
        }

        if next_offset.is_none() {
            self.state = HashJoinStreamState::FetchProbeBatch;
        } else {
            state.advance(
                next_offset
                    .ok_or_else(|| internal_datafusion_err!("unexpected None offset"))?,
                last_joined_right_idx,
            )
        };

        Ok(StatefulStreamResult::Continue)
    }

    /// Processes unmatched build-side rows for certain join types and produces output batch
    ///
    /// Updates state to `Completed`
    fn process_unmatched_build_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        let timer = self.join_metrics.join_time.timer();

        if !need_produce_result_in_final(self.join_type) {
            self.state = HashJoinStreamState::Completed;
            return Ok(StatefulStreamResult::Continue);
        }

        let build_side = self.build_side.try_as_ready()?;

        // For null-aware anti join, if probe side had NULL, no rows should be output
        // Check shared atomic state to get global knowledge across all partitions
        if self.null_aware
            && build_side
                .left_data
                .probe_side_has_null
                .load(Ordering::Relaxed)
        {
            timer.done();
            self.state = HashJoinStreamState::Completed;
            return Ok(StatefulStreamResult::Continue);
        }
        if !build_side.left_data.report_probe_completed() {
            self.state = HashJoinStreamState::Completed;
            return Ok(StatefulStreamResult::Continue);
        }

        // use the global left bitmap to produce the left indices and right indices
        let (mut left_side, mut right_side) = get_final_indices_from_shared_bitmap(
            build_side.left_data.visited_indices_bitmap(),
            self.join_type,
            true,
        );

        // For null-aware anti join, filter out LEFT rows with NULL in join keys
        // BUT only if the probe side (RIGHT) was non-empty. If probe side is empty,
        // NULL NOT IN (empty) = TRUE, so NULL rows should be returned.
        // Use shared atomic state to get global knowledge across all partitions
        if self.null_aware
            && self.join_type == JoinType::LeftAnti
            && build_side
                .left_data
                .probe_side_non_empty
                .load(Ordering::Relaxed)
        {
            // Since null_aware validation ensures single column join, we only check the first column
            let build_key_column = &build_side.left_data.values()[0];

            // Filter out indices where the key is NULL
            let filtered_indices: Vec<u64> = left_side
                .iter()
                .filter_map(|idx| {
                    let idx_usize = idx.unwrap() as usize;
                    if build_key_column.is_null(idx_usize) {
                        None // Skip rows with NULL keys
                    } else {
                        Some(idx.unwrap())
                    }
                })
                .collect();

            left_side = UInt64Array::from(filtered_indices);

            // Update right_side to match the new length
            let mut builder = arrow::array::UInt32Builder::with_capacity(left_side.len());
            builder.append_nulls(left_side.len());
            right_side = builder.finish();
        }

        self.join_metrics.input_batches.add(1);
        self.join_metrics.input_rows.add(left_side.len());

        timer.done();

        self.state = HashJoinStreamState::Completed;

        // Push final unmatched indices to output buffer
        if !left_side.is_empty() {
            let empty_right_batch = RecordBatch::new_empty(self.right.schema());
            let batch = build_batch_from_indices(
                &self.schema,
                build_side.left_data.batch(),
                &empty_right_batch,
                &left_side,
                &right_side,
                &self.column_indices,
                JoinSide::Left,
                self.join_type,
            )?;
            let push_status = self.output_buffer.push_batch(batch)?;

            // If limit reached, finish the coalescer
            if push_status == PushBatchStatus::LimitReached {
                self.output_buffer.finish()?;
            }
        }

        Ok(StatefulStreamResult::Continue)
    }
}

impl Stream for HashJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::joins::hash_join::shared_bounds::{
        PushdownStrategy, completed_partitions_for_test,
        make_partitioned_accumulator_for_test,
    };

    fn empty_build_data(partition_id: usize) -> PartitionBuildData {
        PartitionBuildData::Partitioned {
            partition_id,
            pushdown: PushdownStrategy::Empty,
            bounds: PartitionBounds::new(vec![]),
        }
    }

    fn partitioned_handle(acc: &Arc<SharedBuildAccumulator>) -> BuildReportHandle {
        BuildReportHandle::new(0, PartitionMode::Partitioned, Some(Arc::clone(acc)))
    }

    #[test]
    fn build_report_handle_cancels_scheduled_partition_on_drop() {
        let acc = Arc::new(make_partitioned_accumulator_for_test(2));

        {
            let mut handle = partitioned_handle(&acc);
            handle.schedule(empty_build_data(0));
            assert_eq!(handle.state(), &BuildReportState::Scheduled);
        }

        assert_eq!(completed_partitions_for_test(&acc), 1);
    }

    #[test]
    fn build_report_handle_does_not_cancel_delivered_partition_on_drop() {
        let acc = Arc::new(make_partitioned_accumulator_for_test(1));

        {
            let mut handle = partitioned_handle(&acc);
            handle.schedule(empty_build_data(0));
            let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
            assert!(matches!(handle.poll_delivery(&mut cx), Poll::Ready(Ok(()))));
            assert_eq!(handle.state(), &BuildReportState::Delivered);
        }

        assert_eq!(completed_partitions_for_test(&acc), 1);
    }

    #[test]
    fn build_report_handle_cancel_pending_is_idempotent() {
        let acc = Arc::new(make_partitioned_accumulator_for_test(2));
        let mut handle = partitioned_handle(&acc);
        handle.schedule(empty_build_data(0));

        handle.cancel_pending();
        handle.cancel_pending();

        assert_eq!(handle.state(), &BuildReportState::Canceled);
        assert_eq!(completed_partitions_for_test(&acc), 1);
    }

    #[test]
    fn build_report_handle_no_accumulator_finalizes() {
        let mut handle = BuildReportHandle::new(0, PartitionMode::Partitioned, None);

        handle.schedule(empty_build_data(0));
        handle.cancel_pending();

        assert_eq!(handle.state(), &BuildReportState::Finalized);
    }
}
