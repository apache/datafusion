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
use std::sync::atomic::Ordering;
use std::task::Poll;

use crate::joins::Map;
use crate::joins::MapOffset;
use crate::joins::PartitionMode;
use crate::joins::hash_join::exec::JoinLeftData;
use crate::joins::hash_join::shared_bounds::{
    PartitionBounds, PartitionBuildData, SharedBuildAccumulator,
};
use crate::joins::utils::{
    OnceFut, equal_rows_arr, get_final_indices_from_shared_bitmap,
};
use crate::{
    RecordBatchStream, SendableRecordBatchStream, handle_state,
    hash_utils::create_hashes,
    joins::utils::{
        BuildProbeJoinMetrics, ColumnIndex, JoinFilter, JoinHashMapType,
        StatefulStreamResult, apply_join_filter_to_indices, build_batch_from_indices,
        need_produce_result_in_final,
    },
};

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, UInt32Array, UInt64Array, new_null_array,
};
use arrow::compute::{BatchCoalescer, take};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{JoinSide, JoinType, NullEquality, Result, internal_err};
use datafusion_physical_expr::PhysicalExprRef;

use ahash::RandomState;
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
    /// Futures for building hash tables from build-side input partitions
    pub(super) left_futs: Vec<OnceFut<JoinLeftData>>,
}

/// Container for BuildSide::Ready related data
pub(super) struct BuildSideReadyState {
    /// Collected build-side data for each partition
    left_data: Vec<Arc<JoinLeftData>>,
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
    /// Cached hashes for the probe batch
    hashes: Vec<u64>,
    /// Indices of probe rows sharded by build partition
    partition_indices: Option<Vec<Vec<u32>>>,
    /// Current build partition index we are processing
    current_partition_idx: usize,
    /// Starting offset for JoinHashMap lookups
    offset: MapOffset,
    /// Max joined probe-side index from current batch
    joined_probe_idx: Option<usize>,
    /// Track which probe rows have matched any build partition
    probe_matched: Vec<bool>,
}

impl ProcessProbeBatchState {}

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
    /// Scratch space for probe indices during hash lookup
    probe_indices_buffer: Vec<u32>,
    /// Scratch space for build indices during hash lookup
    build_indices_buffer: Vec<u64>,
    /// Scratch space for hashes during join
    hashes_buffer: Vec<u64>,
    /// Shared build accumulator for coordinating dynamic filter updates (collects hash maps and/or bounds, optional)
    build_accumulator: Option<Arc<SharedBuildAccumulator>>,
    /// Optional future to signal when build information has been reported by all partitions
    /// and the dynamic filter has been updated
    build_waiter: Option<OnceFut<()>>,
    /// Partition index of this stream
    partition: usize,
    /// Whether the probe side is already partitioned
    probe_side_partitioned: bool,
    /// Partitioning mode to use
    mode: PartitionMode,
    /// Output buffer for coalescing small batches into larger ones.
    /// Uses `BatchCoalescer` from arrow to efficiently combine batches.
    /// When batches are already close to target size, they bypass coalescing.
    output_buffer: Box<BatchCoalescer>,
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
#[expect(clippy::too_many_arguments)]
pub(super) fn lookup_join_hashmap(
    build_hashmap: &dyn JoinHashMapType,
    build_side_values: &[ArrayRef],
    probe_side_values: &[ArrayRef],
    null_equality: NullEquality,
    hashes_buffer: &[u64],
    indices: Option<&[u32]>,
    limit: usize,
    offset: MapOffset,
    probe_indices_buffer: &mut Vec<u32>,
    build_indices_buffer: &mut Vec<u64>,
) -> Result<(UInt64Array, UInt32Array, Option<MapOffset>)> {
    let next_offset = build_hashmap.get_matched_indices_with_limit_offset(
        hashes_buffer,
        indices,
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

/// Counts the number of distinct elements in the input array.
///
/// The input array must be sorted (e.g., `[0, 1, 1, 2, 2, ...]`) and contain no null values.
impl HashJoinStream {
    #[expect(clippy::too_many_arguments)]
    pub(super) fn new(
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
        build_accumulator: Option<Arc<SharedBuildAccumulator>>,
        partition: usize,
        probe_side_partitioned: bool,
        mode: PartitionMode,
        null_aware: bool,
    ) -> Self {
        // Create output buffer with coalescing.
        // Use biggest_coalesce_batch_size to bypass coalescing for batches
        // that are already close to target size (within 50%).
        let output_buffer = Box::new(
            BatchCoalescer::new(Arc::clone(&schema), batch_size)
                .with_biggest_coalesce_batch_size(Some(batch_size / 2)),
        );

        Self {
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
            hashes_buffer: Vec::with_capacity(batch_size),
            probe_indices_buffer: Vec::with_capacity(batch_size),
            build_indices_buffer: Vec::with_capacity(batch_size),
            build_accumulator,
            build_waiter: None,
            partition,
            probe_side_partitioned,
            mode,
            output_buffer,
            null_aware,
        }
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
                    self.output_buffer.finish_buffered_batch()?;
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
        let initial_state = self.build_side.try_as_initial_mut()?;
        let mut left_data = Vec::with_capacity(initial_state.left_futs.len());
        for fut in &mut initial_state.left_futs {
            left_data.push(ready!(fut.get_shared(cx))?);
        }
        build_timer.done();

        // Handle dynamic filter build-side information accumulation
        if let Some(ref build_accumulator) = self.build_accumulator {
            let build_accumulator = Arc::clone(build_accumulator);
            let mut report_futs = Vec::new();
            for (i, data) in left_data.iter().enumerate() {
                let left_side_partition_id = match self.mode {
                    PartitionMode::Partitioned => i,
                    PartitionMode::CollectLeft => 0,
                    _ => unreachable!(),
                };

                let pushdown = data.membership().clone();
                let build_data = match self.mode {
                    PartitionMode::Partitioned => PartitionBuildData::Partitioned {
                        partition_id: left_side_partition_id,
                        pushdown,
                        bounds: data
                            .bounds
                            .clone()
                            .unwrap_or_else(|| PartitionBounds::new(vec![])),
                    },
                    PartitionMode::CollectLeft => PartitionBuildData::CollectLeft {
                        pushdown,
                        bounds: data
                            .bounds
                            .clone()
                            .unwrap_or_else(|| PartitionBounds::new(vec![])),
                    },
                    _ => unreachable!(),
                };

                let build_accumulator_clone = Arc::clone(&build_accumulator);
                report_futs.push(async move {
                    build_accumulator_clone.report_build_data(build_data).await
                });
            }
            self.build_waiter = Some(OnceFut::new(async move {
                for fut in report_futs {
                    fut.await?;
                }
                Ok(())
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

                let build_side = self.build_side.try_as_ready()?;
                let num_parts = build_side.left_data.len();

                self.hashes_buffer.clear();
                self.hashes_buffer.resize(batch.num_rows(), 0);
                create_hashes(&keys_values, &self.random_state, &mut self.hashes_buffer)?;
                let hashes = self.hashes_buffer.clone();

                let partition_indices = if num_parts > 1 && !self.probe_side_partitioned {
                    let mut indices =
                        vec![Vec::with_capacity(batch.num_rows() / num_parts); num_parts];
                    for (i, hash) in self.hashes_buffer.iter().enumerate() {
                        let p = (hash % num_parts as u64) as usize;
                        indices[p].push(i as u32);
                    }
                    Some(indices)
                } else {
                    None
                };

                let current_partition_idx = if self.probe_side_partitioned {
                    self.partition
                } else {
                    0
                };

                self.join_metrics.input_batches.add(1);
                self.join_metrics.input_rows.add(batch.num_rows());

                let num_rows = batch.num_rows();
                self.state =
                    HashJoinStreamState::ProcessProbeBatch(ProcessProbeBatchState {
                        batch,
                        values: keys_values,
                        hashes,
                        partition_indices,
                        current_partition_idx,
                        offset: (0, None),
                        joined_probe_idx: None,
                        probe_matched: vec![false; num_rows],
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

        // Null-aware anti join semantics
        if self.null_aware {
            if state.batch.num_rows() > 0 {
                build_side.left_data[0]
                    .probe_side_non_empty
                    .store(true, Ordering::Relaxed);
            }

            let probe_key_column = &state.values[0];
            if probe_key_column.null_count() > 0 {
                build_side.left_data[0]
                    .probe_side_has_null
                    .store(true, Ordering::Relaxed);
            }

            if build_side.left_data[0]
                .probe_side_has_null
                .load(Ordering::Relaxed)
            {
                timer.done();
                self.state = HashJoinStreamState::FetchProbeBatch;
                return Ok(StatefulStreamResult::Continue);
            }
        }

        let num_parts = build_side.left_data.len();

        while state.current_partition_idx < num_parts {
            let partition_idx = state.current_partition_idx;
            let left_data = &build_side.left_data[partition_idx];
            let partition_indices = state
                .partition_indices
                .as_ref()
                .map(|v| v[partition_idx].as_slice());

            let (left_indices, right_indices, next_offset) = match left_data.map() {
                Map::HashMap(map) => lookup_join_hashmap(
                    map.as_ref(),
                    left_data.values(),
                    &state.values,
                    self.null_equality,
                    &state.hashes,
                    partition_indices,
                    self.batch_size,
                    state.offset,
                    &mut self.probe_indices_buffer,
                    &mut self.build_indices_buffer,
                )?,
                Map::ArrayMap(array_map) => {
                    let next_offset = array_map.get_matched_indices_with_limit_offset(
                        &state.values,
                        partition_indices,
                        self.batch_size,
                        state.offset,
                        &mut self.probe_indices_buffer,
                        &mut self.build_indices_buffer,
                    )?;
                    (
                        UInt64Array::from(self.build_indices_buffer.clone()),
                        UInt32Array::from(self.probe_indices_buffer.clone()),
                        next_offset,
                    )
                }
            };

            if !left_indices.is_empty() {
                // Apply join filter if exists
                let (left_indices, right_indices) = if let Some(filter) = &self.filter {
                    apply_join_filter_to_indices(
                        left_data.batch(),
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

                // Mark probe rows as matched (AFTER FILTER)
                if matches!(
                    self.join_type,
                    JoinType::Right
                        | JoinType::Full
                        | JoinType::RightSemi
                        | JoinType::RightAnti
                        | JoinType::RightMark
                ) {
                    right_indices.values().iter().for_each(|&r_idx| {
                        state.probe_matched[r_idx as usize] = true;
                    });
                }

                // Mark build visited
                if need_produce_result_in_final(self.join_type) {
                    let mut bitmap = left_data.visited_indices_bitmap().lock();
                    left_indices.iter().flatten().for_each(|x| {
                        bitmap.set_bit(x as usize, true);
                    });
                }

                // Output matches for join types that produce them during join
                match self.join_type {
                    JoinType::Inner
                    | JoinType::Left
                    | JoinType::Right
                    | JoinType::Full => {
                        let batch = build_batch_from_indices(
                            &self.schema,
                            left_data.batch(),
                            &state.batch,
                            &left_indices,
                            &right_indices,
                            &self.column_indices,
                            JoinSide::Left,
                        )?;
                        self.output_buffer.push_batch(batch)?;
                        // If we didn't finish all matches, save state and return Continue to check output buffer
                        if next_offset.is_some() {
                            self.state = HashJoinStreamState::ProcessProbeBatch(
                                ProcessProbeBatchState {
                                    batch: state.batch.clone(),
                                    values: state.values.clone(),
                                    hashes: state.hashes.clone(),
                                    partition_indices: state.partition_indices.clone(),
                                    current_partition_idx: partition_idx,
                                    offset: next_offset.unwrap_or((0, None)),
                                    joined_probe_idx: state.joined_probe_idx,
                                    probe_matched: state.probe_matched.clone(),
                                },
                            );
                            return Ok(StatefulStreamResult::Continue);
                        }
                    }
                    _ => {}
                }
            }

            if let Some(offset) = next_offset {
                state.offset = offset;
                return Ok(StatefulStreamResult::Continue);
            }

            if self.probe_side_partitioned {
                state.current_partition_idx = num_parts;
            } else {
                state.current_partition_idx += 1;
            }
            state.offset = (0, None);
        }

        // After all partitions, handle probe-side outer/semi/anti joins
        match self.join_type {
            JoinType::Right
            | JoinType::Full
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::RightMark => {
                let mut r_indices = Vec::with_capacity(state.batch.num_rows());
                for (i, &matched) in state.probe_matched.iter().enumerate() {
                    match self.join_type {
                        JoinType::Right | JoinType::Full | JoinType::RightAnti => {
                            if !matched {
                                r_indices.push(i as u32);
                            }
                        }
                        JoinType::RightSemi => {
                            if matched {
                                r_indices.push(i as u32);
                            }
                        }
                        JoinType::RightMark => {
                            r_indices.push(i as u32);
                        }
                        _ => {}
                    }
                }

                if !r_indices.is_empty() {
                    let r_indices = UInt32Array::from(r_indices);
                    let batch = if self.join_type == JoinType::RightMark {
                        // Produce RightMark batch with mark column
                        let mut columns = Vec::with_capacity(self.column_indices.len());
                        for column_index in &self.column_indices {
                            let array = match column_index.side {
                                JoinSide::Right => take(
                                    state.batch.column(column_index.index),
                                    &r_indices,
                                    None,
                                )?,
                                JoinSide::Left => {
                                    // Should not happen for RightMark output columns?
                                    // Actually RightMark only produces Right side and Mark.
                                    new_null_array(
                                        build_side.left_data[0]
                                            .batch()
                                            .column(column_index.index)
                                            .data_type(),
                                        r_indices.len(),
                                    )
                                }
                                JoinSide::None => {
                                    let mut mark_builder =
                                        BooleanBuilder::with_capacity(r_indices.len());
                                    for i in 0..r_indices.len() {
                                        let matched = state.probe_matched
                                            [r_indices.value(i) as usize];
                                        mark_builder.append_value(matched);
                                    }
                                    Arc::new(mark_builder.finish()) as ArrayRef
                                }
                            };
                            columns.push(array);
                        }
                        RecordBatch::try_new(Arc::clone(&self.schema), columns)?
                    } else {
                        let l_indices = UInt64Array::from(vec![None; r_indices.len()]);
                        build_batch_from_indices(
                            &self.schema,
                            build_side.left_data[0].batch(),
                            &state.batch,
                            &l_indices,
                            &r_indices,
                            &self.column_indices,
                            JoinSide::Left,
                        )?
                    };
                    self.output_buffer.push_batch(batch)?;
                }
            }
            _ => {}
        }

        timer.done();
        self.state = HashJoinStreamState::FetchProbeBatch;
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
        if self.null_aware
            && build_side.left_data[0]
                .probe_side_has_null
                .load(Ordering::Relaxed)
        {
            timer.done();
            self.state = HashJoinStreamState::Completed;
            return Ok(StatefulStreamResult::Continue);
        }

        let mut produced_any = false;
        for left_data in &build_side.left_data {
            if !left_data.report_probe_completed() {
                continue;
            }

            produced_any = true;
            // use the global left bitmap to produce the left indices and right indices
            let (mut left_side, right_side) = get_final_indices_from_shared_bitmap(
                left_data.visited_indices_bitmap(),
                self.join_type,
                true,
            );

            if self.null_aware
                && self.join_type == JoinType::LeftAnti
                && build_side.left_data[0]
                    .probe_side_non_empty
                    .load(Ordering::Relaxed)
            {
                let build_key_column = &left_data.values()[0];
                let filtered_indices: Vec<u64> = left_side
                    .iter()
                    .filter_map(|idx| {
                        let idx_usize = idx? as usize;
                        if build_key_column.is_null(idx_usize) {
                            None
                        } else {
                            Some(idx?)
                        }
                    })
                    .collect();
                left_side = UInt64Array::from(filtered_indices);
            }

            if !left_side.is_empty() {
                let batch = build_batch_from_indices(
                    &self.schema,
                    left_data.batch(),
                    &RecordBatch::new_empty(self.right.schema()),
                    &left_side,
                    &right_side,
                    &self.column_indices,
                    JoinSide::Left,
                )?;
                self.output_buffer.push_batch(batch)?;
            }
        }

        if !produced_any {
            // Not the last stream or no partitions needed reporting?
            // Actually, if we are in this state, it's the end of probe side.
        }

        timer.done();
        self.state = HashJoinStreamState::Completed;
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
