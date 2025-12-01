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

use std::sync::Arc;
use std::task::Poll;

use crate::joins::hash_join::exec::JoinLeftData;
use crate::joins::hash_join::shared_bounds::{
    PartitionBuildDataReport, SharedBuildAccumulator,
};
use crate::joins::utils::{
    equal_rows_arr, get_final_indices_from_shared_bitmap, OnceFut,
};
use crate::joins::PartitionMode;
use crate::{
    handle_state,
    hash_utils::create_hashes,
    joins::join_hash_map::JoinHashMapOffset,
    joins::utils::{
        adjust_indices_by_join_type, apply_join_filter_to_indices,
        build_batch_empty_build_side, build_batch_from_indices,
        need_produce_result_in_final, BuildProbeJoinMetrics, ColumnIndex, JoinFilter,
        JoinHashMapType, StatefulStreamResult,
    },
    RecordBatchStream, SendableRecordBatchStream,
};

use arrow::array::{Array, ArrayRef, UInt32Array, UInt64Array};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{
    internal_datafusion_err, internal_err, JoinSide, JoinType, NullEquality, Result,
};
use datafusion_physical_expr::PhysicalExprRef;

use ahash::RandomState;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use futures::{ready, Stream, StreamExt};

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

    /// Tries to extract BuildSideReadyState from BuildSide enum.
    /// Returns an error if state is not Ready.
    fn try_as_ready_mut(&mut self) -> Result<&mut BuildSideReadyState> {
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
    offset: JoinHashMapOffset,
    /// Max joined probe-side index from current batch
    joined_probe_idx: Option<usize>,
}

impl ProcessProbeBatchState {
    fn advance(&mut self, offset: JoinHashMapOffset, joined_probe_idx: Option<usize>) {
        self.offset = offset;
        if joined_probe_idx.is_some() {
            self.joined_probe_idx = joined_probe_idx;
        }
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
    /// Specifies whether the right side has an ordering to potentially preserve
    right_side_ordered: bool,
    /// Shared build accumulator for coordinating dynamic filter updates (collects hash maps and/or bounds, optional)
    build_accumulator: Option<Arc<SharedBuildAccumulator>>,
    /// Optional future to signal when build information has been reported by all partitions
    /// and the dynamic filter has been updated
    build_waiter: Option<OnceFut<()>>,

    /// Partitioning mode to use
    mode: PartitionMode,
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
pub(super) fn lookup_join_hashmap(
    build_hashmap: &dyn JoinHashMapType,
    build_side_values: &[ArrayRef],
    probe_side_values: &[ArrayRef],
    null_equality: NullEquality,
    hashes_buffer: &[u64],
    limit: usize,
    offset: JoinHashMapOffset,
) -> Result<(UInt64Array, UInt32Array, Option<JoinHashMapOffset>)> {
    let (probe_indices, build_indices, next_offset) =
        build_hashmap.get_matched_indices_with_limit_offset(hashes_buffer, limit, offset);

    let build_indices: UInt64Array = build_indices.into();
    let probe_indices: UInt32Array = probe_indices.into();

    let (build_indices, probe_indices) = equal_rows_arr(
        &build_indices,
        &probe_indices,
        build_side_values,
        probe_side_values,
        null_equality,
    )?;

    Ok((build_indices, probe_indices, next_offset))
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
    ) -> Self {
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
            right_side_ordered,
            build_accumulator,
            build_waiter: None,
            mode,
        }
    }

    /// Separate implementation function that unpins the [`HashJoinStream`] so
    /// that partial borrows work correctly
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
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
                    let poll = handle_state!(self.process_probe_batch());
                    self.join_metrics.baseline.record_poll(poll)
                }
                HashJoinStreamState::ExhaustedProbeSide => {
                    let poll = handle_state!(self.process_unmatched_build_batch());
                    self.join_metrics.baseline.record_poll(poll)
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
        if let Some(ref mut fut) = self.build_waiter {
            ready!(fut.get_shared(cx))?;
        }
        self.state = HashJoinStreamState::FetchProbeBatch;
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
        let left_data = ready!(self
            .build_side
            .try_as_initial_mut()?
            .left_fut
            .get_shared(cx))?;
        build_timer.done();

        // Handle dynamic filter build-side information accumulation
        //
        // Dynamic filter coordination between partitions:
        // Report hash maps (Partitioned mode) or bounds (CollectLeft mode) to the accumulator
        // which will handle synchronization and filter updates
        if let Some(ref build_accumulator) = self.build_accumulator {
            let build_accumulator = Arc::clone(build_accumulator);

            let left_side_partition_id = match self.mode {
                PartitionMode::Partitioned => self.partition,
                PartitionMode::CollectLeft => 0,
                PartitionMode::Auto => unreachable!("PartitionMode::Auto should not be present at execution time. This is a bug in DataFusion, please report it!"),
            };

            let build_data = match self.mode {
                PartitionMode::Partitioned => PartitionBuildDataReport::Partitioned {
                    partition_id: left_side_partition_id,
                    bounds: left_data.bounds.clone(),
                },
                PartitionMode::CollectLeft => PartitionBuildDataReport::CollectLeft {
                    bounds: left_data.bounds.clone(),
                },
                PartitionMode::Auto => unreachable!(
                    "PartitionMode::Auto should not be present at execution time"
                ),
            };
            self.build_waiter = Some(OnceFut::new(async move {
                build_accumulator.report_build_data(build_data).await
            }));
            self.state = HashJoinStreamState::WaitPartitionBoundsReport;
        } else {
            self.state = HashJoinStreamState::FetchProbeBatch;
        }

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
                self.state = HashJoinStreamState::ExhaustedProbeSide;
            }
            Some(Ok(batch)) => {
                // Precalculate hash values for fetched batch
                let keys_values = evaluate_expressions_to_arrays(&self.on_right, &batch)?;

                self.hashes_buffer.clear();
                self.hashes_buffer.resize(batch.num_rows(), 0);
                create_hashes(&keys_values, &self.random_state, &mut self.hashes_buffer)?;

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
        let state = self.state.try_as_process_probe_batch_mut()?;
        let build_side = self.build_side.try_as_ready_mut()?;

        self.join_metrics
            .probe_hit_rate
            .add_total(state.batch.num_rows());

        let timer = self.join_metrics.join_time.timer();

        // if the left side is empty, we can skip the (potentially expensive) join operation
        if build_side.left_data.hash_map.is_empty() && self.filter.is_none() {
            let result = build_batch_empty_build_side(
                &self.schema,
                build_side.left_data.batch(),
                &state.batch,
                &self.column_indices,
                self.join_type,
            )?;
            timer.done();

            self.state = HashJoinStreamState::FetchProbeBatch;

            return Ok(StatefulStreamResult::Ready(Some(result)));
        }

        // get the matched by join keys indices
        let (left_indices, right_indices, next_offset) = lookup_join_hashmap(
            build_side.left_data.hash_map(),
            build_side.left_data.values(),
            &state.values,
            self.null_equality,
            &self.hashes_buffer,
            self.batch_size,
            state.offset,
        )?;

        let distinct_right_indices_count = count_distinct_sorted_indices(&right_indices);

        self.join_metrics
            .probe_hit_rate
            .add_part(distinct_right_indices_count);

        self.join_metrics.avg_fanout.add_part(left_indices.len());

        self.join_metrics
            .avg_fanout
            .add_total(distinct_right_indices_count);

        // apply join filter if exists
        let (left_indices, right_indices) = if let Some(filter) = &self.filter {
            apply_join_filter_to_indices(
                build_side.left_data.batch(),
                &state.batch,
                left_indices,
                right_indices,
                filter,
                JoinSide::Left,
                None,
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

        let result = if self.join_type == JoinType::RightMark {
            build_batch_from_indices(
                &self.schema,
                &state.batch,
                build_side.left_data.batch(),
                &left_indices,
                &right_indices,
                &self.column_indices,
                JoinSide::Right,
            )?
        } else {
            build_batch_from_indices(
                &self.schema,
                build_side.left_data.batch(),
                &state.batch,
                &left_indices,
                &right_indices,
                &self.column_indices,
                JoinSide::Left,
            )?
        };

        timer.done();

        if next_offset.is_none() {
            self.state = HashJoinStreamState::FetchProbeBatch;
        } else {
            state.advance(
                next_offset
                    .ok_or_else(|| internal_datafusion_err!("unexpected None offset"))?,
                last_joined_right_idx,
            )
        };

        Ok(StatefulStreamResult::Ready(Some(result)))
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
        if !build_side.left_data.report_probe_completed() {
            self.state = HashJoinStreamState::Completed;
            return Ok(StatefulStreamResult::Continue);
        }

        // use the global left bitmap to produce the left indices and right indices
        let (left_side, right_side) = get_final_indices_from_shared_bitmap(
            build_side.left_data.visited_indices_bitmap(),
            self.join_type,
            true,
        );
        let empty_right_batch = RecordBatch::new_empty(self.right.schema());
        // use the left and right indices to produce the batch result
        let result = build_batch_from_indices(
            &self.schema,
            build_side.left_data.batch(),
            &empty_right_batch,
            &left_side,
            &right_side,
            &self.column_indices,
            JoinSide::Left,
        );

        if let Ok(ref batch) = result {
            self.join_metrics.input_batches.add(1);
            self.join_metrics.input_rows.add(batch.num_rows());
        }
        timer.done();

        self.state = HashJoinStreamState::Completed;

        Ok(StatefulStreamResult::Ready(Some(result?)))
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
