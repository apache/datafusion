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

use std::future::poll_fn;
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
    OnceFut, equal_rows_arr, get_final_indices_from_shared_bitmap, matchable_join_keys,
};
use crate::stream::{EmptyRecordBatchStream, ObservedStream, RecordBatchStreamAdapter};
use crate::{
    SendableRecordBatchStream,
    hash_utils::create_hashes,
    joins::utils::{
        BuildProbeJoinMetrics, ColumnIndex, JoinFilter, JoinHashMapType,
        adjust_indices_by_join_type, apply_join_filter_to_indices,
        build_batch_empty_build_side, build_batch_from_indices,
        need_produce_result_in_final,
    },
};

use arrow::array::{Array, ArrayRef, UInt32Array, UInt64Array};
use arrow::buffer::NullBuffer;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, JoinSide, JoinType, NullEquality, Result};
use datafusion_execution::{TryEmitter, async_try_stream};
use datafusion_physical_expr::PhysicalExprRef;

use datafusion_common::hash_utils::RandomState;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use futures::{StreamExt, ready};

/// Cursor over one probe-side batch: key values, hash context, and the
/// chunk offset for `batch_size`-limited hash-map lookups.
#[derive(Debug, Clone)]
struct ProcessProbeBatchState {
    /// Current probe-side batch
    batch: RecordBatch,
    /// Probe-side on expressions values
    values: Vec<ArrayRef>,
    /// Combined validity of the probe-side key columns, set when NULL keys
    /// exist and cannot match (`NullEquality::NullEqualsNothing`); NULL rows
    /// are skipped during JoinHashMap lookups
    valid_keys: Option<NullBuffer>,
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

    /// Waits until the scheduled report (if any) has been delivered to the
    /// coordinator.
    async fn delivered(&mut self) -> Result<()> {
        poll_fn(|cx| self.poll_delivery(cx)).await
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
    /// Future producing the shared build-side data (hash table etc.)
    left_fut: OnceFut<JoinLeftData>,
    /// Maximum output batch size
    batch_size: usize,
    /// Scratch space for computing hashes
    hashes_buffer: Vec<u64>,
    /// Scratch space for probe indices during hash lookup
    probe_indices_buffer: Vec<u32>,
    /// Scratch space for build indices during hash lookup
    build_indices_buffer: Vec<u64>,
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
    valid_keys: Option<&NullBuffer>,
    limit: usize,
    offset: MapOffset,
    probe_indices_buffer: &mut Vec<u32>,
    build_indices_buffer: &mut Vec<u64>,
) -> Result<(UInt64Array, UInt32Array, Option<MapOffset>)> {
    let next_offset = build_hashmap.get_matched_indices_with_limit_offset(
        hashes_buffer,
        valid_keys,
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
        left_fut: OnceFut<JoinLeftData>,
        batch_size: usize,
        hashes_buffer: Vec<u64>,
        right_side_ordered: bool,
        build_accumulator: Option<Arc<SharedBuildAccumulator>>,
        mode: PartitionMode,
        null_aware: bool,
        fetch: Option<usize>,
    ) -> SendableRecordBatchStream {
        // Create output buffer with coalescing and optional fetch limit.
        let output_buffer =
            LimitedBatchCoalescer::new(Arc::clone(&schema), batch_size, fetch);

        let this = Self {
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
            left_fut,
            batch_size,
            hashes_buffer,
            probe_indices_buffer: Vec::with_capacity(batch_size),
            build_indices_buffer: Vec::with_capacity(batch_size),
            right_side_ordered,
            build_report: BuildReportHandle::new(partition, mode, build_accumulator),
            mode,
            output_buffer,
            null_aware,
        };

        let schema = Arc::clone(&this.schema);
        let baseline_metrics = this.join_metrics.baseline.clone();
        let stream =
            async_try_stream(|mut emitter| async move { this.join(&mut emitter).await });
        // ObservedStream records the baseline metrics (output rows/batches,
        // end time).
        Box::pin(ObservedStream::new(
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)),
            baseline_metrics,
            None,
        ))
    }

    /// Returns true when an empty build side already determines an empty
    /// result, so the probe side does not need to be scanned at all.
    fn empty_build_short_circuit(join_type: JoinType, left_data: &JoinLeftData) -> bool {
        let build_empty = !left_data.has_build_rows();
        // The map can be empty even when the build side has rows: under
        // `NullEqualsNothing`, build rows with a NULL join key are omitted. For
        // join types whose every output row requires a build match, that still
        // guarantees an empty result, so we can skip scanning the probe side.
        let map_empty = !left_data.has_matchable_build_rows();

        (build_empty && join_type.empty_build_side_produces_empty_result())
            || (map_empty && join_type.empty_map_produces_empty_result())
    }

    /// Main loop: the textbook hash join.
    ///
    /// ```text
    /// 1. build: collect the build side into a hash table
    ///    (with dynamic-filter coordination, also report this partition's
    ///    build data and wait until every partition has reported)
    /// 2. probe: for each probe-side batch, look up matches against the
    ///    hash table in batch_size chunks, apply the join filter, and emit
    ///    the joined rows
    /// 3. final: emit the unmatched build-side rows (outer/anti/mark joins)
    /// ```
    async fn join(
        mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        // 1. Build the hash table from the build side.
        let left_data = self.collect_build_side().await?;

        // Report this partition's build data (bounds/membership) and wait
        // until every partition has reported, so the probe side's dynamic
        // filter is complete before its scan starts.
        if self.build_report.has_accumulator() {
            self.schedule_build_report(&left_data);
            self.build_report.delivered().await?;
        }

        if !Self::empty_build_short_circuit(self.join_type, &left_data) {
            // 2. Probe: join each probe-side batch against the hash table,
            // in batch_size chunks.
            while let Some(batch) = self.fetch_probe_batch().await? {
                let mut probe = self.prepare_probe_batch(batch, &left_data)?;
                loop {
                    let batch_done = self.process_probe_chunk(&mut probe, &left_data)?;
                    // Sync guard: only enter the async emit helper when a
                    // completed batch is actually ready.
                    if self.output_buffer.has_completed_batch() {
                        self.emit_completed_batches(emitter).await;
                    }
                    if self.output_buffer.is_finished() {
                        // Fetch limit reached.
                        return Ok(());
                    }
                    if batch_done {
                        break;
                    }
                }
            }

            // 3. Emit the unmatched build-side rows.
            self.process_unmatched_build_batch(&left_data)?;
            if self.output_buffer.has_completed_batch() {
                self.emit_completed_batches(emitter).await;
            }
            if self.output_buffer.is_finished() {
                return Ok(());
            }
        }

        // Flush the remaining buffered output.
        self.output_buffer.finish()?;
        self.emit_completed_batches(emitter).await;
        Ok(())
    }

    /// Emit all completed output batches to the stream consumer.
    async fn emit_completed_batches(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) {
        while let Some(batch) = self.output_buffer.next_completed_batch() {
            emitter.emit(batch).await;
        }
    }

    /// Collects build-side data by awaiting the shared build future.
    async fn collect_build_side(&mut self) -> Result<Arc<JoinLeftData>> {
        // The timer guard is scoped to each poll, so `build_time` counts
        // this stream's polling work only — matching the previous
        // poll-based accounting — not the wall time spent waiting on the
        // shared build task.
        poll_fn(|cx| {
            let _build_timer = self.join_metrics.build_time.timer();
            self.left_fut.get_shared(cx)
        })
        .await
    }

    /// Constructs this partition's [`PartitionBuildData`] and schedules its
    /// report to the shared accumulator.
    fn schedule_build_report(&mut self, left_data: &Arc<JoinLeftData>) {
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
    }

    /// Fetches the next probe-side batch. Returns None — releasing the
    /// probe pipeline's resources — when the probe side is exhausted.
    async fn fetch_probe_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self.right.next().await.transpose()? {
            None => {
                // Release the probe-side input pipeline's resources. The schema
                // is preserved so callers that still query `self.right.schema()`
                // (e.g. for unmatched-build emission) keep working.
                let right_schema = self.right.schema();
                self.right = Box::pin(EmptyRecordBatchStream::new(right_schema));
                Ok(None)
            }
            Some(batch) => Ok(Some(batch)),
        }
    }

    /// Evaluates the join keys (and their hashes) of a fetched probe batch,
    /// producing the cursor the chunked lookups iterate with.
    fn prepare_probe_batch(
        &mut self,
        batch: RecordBatch,
        left_data: &JoinLeftData,
    ) -> Result<ProcessProbeBatchState> {
        // Precalculate hash values for fetched batch
        let keys_values = evaluate_expressions_to_arrays(&self.on_right, &batch)?;

        let valid_keys = if let Map::HashMap(_) = left_data.map() {
            self.hashes_buffer.clear();
            self.hashes_buffer.resize(batch.num_rows(), 0);
            create_hashes(&keys_values, &self.random_state, &mut self.hashes_buffer)?;
            matchable_join_keys(&keys_values, self.null_equality)
        } else {
            None
        };

        self.join_metrics.input_batches.add(1);
        self.join_metrics.input_rows.add(batch.num_rows());

        Ok(ProcessProbeBatchState {
            batch,
            values: keys_values,
            valid_keys,
            offset: (0, None),
            joined_probe_idx: None,
        })
    }

    /// Joins one `batch_size` chunk of the current probe batch against the
    /// hash table and pushes the joined rows to the output buffer.
    ///
    /// Returns true when the probe batch is fully processed (the caller
    /// fetches the next one), false when more chunks remain.
    fn process_probe_chunk(
        &mut self,
        probe: &mut ProcessProbeBatchState,
        left_data: &JoinLeftData,
    ) -> Result<bool> {
        self.join_metrics
            .probe_hit_rate
            .add_total(probe.batch.num_rows());

        let timer = self.join_metrics.join_time.timer();

        // Null-aware anti join semantics:
        // For LeftAnti: output LEFT (build) rows where LEFT.key NOT IN RIGHT.key
        // 1. If RIGHT (probe) contains NULL in any batch, no LEFT rows should be output
        // 2. LEFT rows with NULL keys should not be output (handled in final stage)
        if self.null_aware {
            // Mark that we've seen a probe batch with actual rows (probe side is non-empty)
            // Only set this if batch has rows - empty batches don't count
            // Use shared atomic state so all partitions can see this global information
            if probe.batch.num_rows() > 0 {
                left_data
                    .probe_side_non_empty
                    .store(true, Ordering::Relaxed);
            }

            // Check if probe side (RIGHT) contains NULL
            // Since null_aware validation ensures single column join, we only check the first column
            let probe_key_column = &probe.values[0];
            if probe_key_column.null_count() > 0 {
                // Found NULL in probe side - set shared flag to prevent any output
                left_data.probe_side_has_null.store(true, Ordering::Relaxed);
            }

            // If probe side has NULL (detected in this or any other partition), return empty result
            if left_data.probe_side_has_null.load(Ordering::Relaxed) {
                timer.done();
                return Ok(true);
            }
        }

        let is_empty = !left_data.has_matchable_build_rows();

        if is_empty {
            let result = build_batch_empty_build_side(
                &self.schema,
                left_data.batch(),
                &probe.batch,
                &self.column_indices,
                self.join_type,
            )?;
            timer.done();
            self.output_buffer.push_batch(result)?;

            return Ok(true);
        }

        // get the matched by join keys indices
        let (left_indices, right_indices, next_offset) = match left_data.map() {
            Map::HashMap(map) => lookup_join_hashmap(
                map.as_ref(),
                left_data.values(),
                &probe.values,
                self.null_equality,
                &self.hashes_buffer,
                probe.valid_keys.as_ref(),
                self.batch_size,
                probe.offset,
                &mut self.probe_indices_buffer,
                &mut self.build_indices_buffer,
            )?,
            Map::ArrayMap(array_map) => {
                let next_offset = array_map.get_matched_indices_with_limit_offset(
                    &probe.values,
                    self.batch_size,
                    probe.offset,
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
                left_data.batch(),
                &probe.batch,
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
            let mut bitmap = left_data.visited_indices_bitmap().lock();
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
        let index_alignment_range_start = probe.joined_probe_idx.map_or(0, |v| v + 1);
        let index_alignment_range_end = if next_offset.is_none() {
            probe.batch.num_rows()
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
                (&probe.batch, left_data.batch(), JoinSide::Right)
            } else {
                (left_data.batch(), &probe.batch, JoinSide::Left)
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

        // If the fetch limit was reached, finish the output buffer; the
        // caller observes `is_finished` and stops.
        if push_status == PushBatchStatus::LimitReached {
            self.output_buffer.finish()?;
            return Ok(true);
        }

        match next_offset {
            None => Ok(true),
            Some(next_offset) => {
                probe.advance(next_offset, last_joined_right_idx);
                Ok(false)
            }
        }
    }

    /// Emits unmatched build-side rows for join types that need them
    /// (left/full outer, semi, anti, mark joins), once every partition
    /// sharing the build side has finished probing.
    fn process_unmatched_build_batch(&mut self, left_data: &JoinLeftData) -> Result<()> {
        let timer = self.join_metrics.join_time.timer();

        if !need_produce_result_in_final(self.join_type) {
            return Ok(());
        }

        // For null-aware anti join, if probe side had NULL, no rows should be output
        // Check shared atomic state to get global knowledge across all partitions
        if self.null_aware && left_data.probe_side_has_null.load(Ordering::Relaxed) {
            timer.done();
            return Ok(());
        }
        if !left_data.report_probe_completed() {
            return Ok(());
        }

        // use the global left bitmap to produce the left indices and right indices
        let (mut left_side, mut right_side) = get_final_indices_from_shared_bitmap(
            left_data.visited_indices_bitmap(),
            self.join_type,
            true,
        );

        // For null-aware anti join, filter out LEFT rows with NULL in join keys
        // BUT only if the probe side (RIGHT) was non-empty. If probe side is empty,
        // NULL NOT IN (empty) = TRUE, so NULL rows should be returned.
        // Use shared atomic state to get global knowledge across all partitions
        if self.null_aware
            && self.join_type == JoinType::LeftAnti
            && left_data.probe_side_non_empty.load(Ordering::Relaxed)
        {
            // Since null_aware validation ensures single column join, we only check the first column
            let build_key_column = &left_data.values()[0];

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

        // Push final unmatched indices to output buffer
        if !left_side.is_empty() {
            let empty_right_batch = RecordBatch::new_empty(self.right.schema());
            let batch = build_batch_from_indices(
                &self.schema,
                left_data.batch(),
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

        Ok(())
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
