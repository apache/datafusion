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

//! Sort-merge join stream specialized for semi/anti/mark joins.
//!
//! Instantiated by [`SortMergeJoinExec`](crate::joins::sort_merge_join::SortMergeJoinExec)
//! when the join type is `LeftSemi`, `LeftAnti`, `RightSemi`, `RightAnti`,
//! `LeftMark`, or `RightMark`.
//!
//! # Motivation
//!
//! The general-purpose `MaterializingSortMergeJoinStream`
//! handles semi/anti joins by materializing `(outer, inner)` row pairs,
//! applying a filter, then using a "corrected filter mask" to deduplicate.
//! Semi/anti joins only need a boolean per outer row (does a match exist?),
//! not pairs. The pair-based approach incurs unnecessary memory allocation
//! and intermediate batches.
//!
//! This stream instead tracks matches with a per-outer-batch bitset,
//! avoiding all pair materialization.
//!
//! # "Outer Side" vs "Inner Side"
//!
//! For `Left*` join types, left is outer and right is inner.
//! For `Right*` join types, right is outer and left is inner.
//! The output schema always equals the outer side's schema (for semi/anti)
//! or the outer side's schema plus a boolean mark column (for mark joins).
//!
//! # Algorithm
//!
//! Both inputs must be sorted by the join keys. The stream performs a merge
//! scan across the two sorted inputs:
//!
//! ```text
//!   outer cursor ──►  [1, 2, 2, 3, 5, 5, 7]
//!   inner cursor ──►  [2, 2, 4, 5, 6, 7, 7]
//!                       ▲
//!                   compare keys at cursors
//! ```
//!
//! At each step, the keys at the outer and inner cursors are compared:
//!
//! - **outer < inner**: Skip the outer key group (no match exists).
//! - **outer > inner**: Skip the inner key group.
//! - **outer == inner**: Process the match (see below).
//!
//! Key groups are contiguous runs of equal keys within one side. The scan
//! advances past entire groups at each step.
//!
//! ## Processing a key match
//!
//! **Without filter**: All outer rows in the key group are marked as matched.
//!
//! **With filter**: The inner key group is buffered (may span multiple inner
//! batches). For each buffered inner row, the filter is evaluated against the
//! outer key group as a batch. Results are OR'd into the matched bitset. A
//! short-circuit exits early when all outer rows in the group are matched.
//!
//! ```text
//!   matched bitset:  [0, 0, 1, 0, 0, ...]
//!                     ▲── one bit per outer row ──▲
//!
//!   On emit:
//!     Semi  → filter_record_batch(outer_batch, &matched)
//!     Anti  → filter_record_batch(outer_batch, &NOT(matched))
//!     Mark  → outer_batch + matched as boolean column
//! ```
//!
//! ## Batch boundaries
//!
//! Key groups can span batch boundaries on either side. The stream handles
//! this by detecting when a group extends to the end of a batch, loading the
//! next batch, and continuing if the key matches. The generator-based stream
//! suspends in place at `await` points, so no explicit re-entry state is
//! needed.
//!
//! # Memory
//!
//! Memory usage is bounded and independent of total input size:
//! - One outer batch at a time (not tracked by reservation — single batch,
//!   cannot be spilled since it's needed for filter evaluation)
//! - One inner batch at a time (streaming)
//! - `matched` bitset: one bit per outer row, re-allocated per batch
//! - Inner key group buffer: only for filtered joins, one key group at a time.
//!   Tracked via `MemoryReservation`; spilled to disk when the memory pool
//!   limit is exceeded.
//! - `BatchCoalescer`: output buffering to target batch size
//!
//! # Degenerate cases
//!
//! **Highly skewed key (filtered joins only):** When a filter is present,
//! the inner key group is buffered so each inner row can be evaluated
//! against the outer group. If one join key has N inner rows, all N rows
//! are held in memory simultaneously (or spilled to disk if the memory
//! pool limit is reached). With uniform key distribution this is small
//! (inner_rows / num_distinct_keys), but a single hot key can buffer
//! arbitrarily many rows. The no-filter path does not buffer inner
//! rows — it only advances the cursor — so it is unaffected.
//!
//! **Scalar broadcast during filter evaluation:** Each inner row is
//! broadcast to match the outer group length for filter evaluation,
//! allocating one array per inner row × filter column. This is inherent
//! to the `PhysicalExpr::evaluate(RecordBatch)` API, which does not
//! support scalar inputs directly. The total work is
//! O(inner_group × outer_group) per key, but with much lower constant
//! factor than the pair-materialization approach.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::EmptyRecordBatchStream;
use crate::joins::utils::{JoinFilter, JoinKeyComparator, compare_join_arrays};
use crate::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, RecordOutput,
};
use crate::spill::spill_manager::SpillManager;
use crate::stream::RecordBatchStreamAdapter;
use arrow::array::{Array, ArrayRef, BooleanArray, BooleanBufferBuilder, RecordBatch};
use arrow::compute::{BatchCoalescer, SortOptions, filter_record_batch, not};
use arrow::datatypes::SchemaRef;
use arrow::util::bit_chunk_iterator::UnalignedBitChunk;
use arrow::util::bit_util::apply_bitwise_binary_op;
use datafusion_common::{
    DataFusionError, JoinSide, JoinType, NullEquality, Result, ScalarValue, internal_err,
};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_execution::{
    SendableRecordBatchStream, SpillFile, TryEmitter, async_try_stream,
};
use datafusion_physical_expr_common::physical_expr::PhysicalExprRef;

use futures::StreamExt;

/// Evaluates join key expressions against a batch, returning one array per key.
fn evaluate_join_keys(
    batch: &RecordBatch,
    on: &[PhysicalExprRef],
) -> Result<Vec<ArrayRef>> {
    on.iter()
        .map(|expr| {
            let num_rows = batch.num_rows();
            let val = expr.evaluate(batch)?;
            val.into_array(num_rows)
        })
        .collect()
}

/// Find the first index in `key_arrays` starting from `from` where the key
/// differs from the key at `from`. Uses a pre-built `JoinKeyComparator` for
/// zero-alloc ordinal comparison without per-row type dispatch.
///
/// Optimized for join workloads: checks adjacent and boundary keys before
/// falling back to binary search, since most key groups are small (often 1).
fn find_key_group_end(cmp: &JoinKeyComparator, from: usize, len: usize) -> usize {
    let next = from + 1;
    if next >= len {
        return len;
    }

    // Fast path: single-row group (common with unique keys).
    if cmp.compare(from, next) != Ordering::Equal {
        return next;
    }

    // Check if the entire remaining batch shares this key.
    let last = len - 1;
    if cmp.compare(from, last) == Ordering::Equal {
        return len;
    }

    // Binary search the interior: key at `next` matches, key at `last` doesn't.
    let mut lo = next + 1;
    let mut hi = last;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if cmp.compare(from, mid) == Ordering::Equal {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Sort-Merge join stream for Semi/Anti/Mark joins.
///
/// Named "bitwise" because it tracks outer-row matches via a per-batch
/// boolean bitset (`BooleanBufferBuilder`) rather than materializing
/// `(outer, inner)` row pairs. Filter results are OR'd into the bitset
/// in `u64` chunks, and emitting applies the bitset directly.
pub(crate) struct BitwiseSortMergeJoinStream {
    join_type: JoinType,

    // Input streams — in the nested-loop model that sort-merge join
    // implements, "outer" is the driving loop and "inner" is probed for
    // matches. The existing MaterializingSortMergeJoinStream calls these "streamed"
    // and "buffered" respectively. For Left* joins, outer=left; for
    // Right* joins, outer=right. Output schema equals the outer side.
    outer: SendableRecordBatchStream,
    inner: SendableRecordBatchStream,

    // Current batches and cursor positions within them
    outer_batch: Option<RecordBatch>,
    /// Row index into `outer_batch` — the next unprocessed outer row.
    outer_offset: usize,
    outer_key_arrays: Vec<ArrayRef>,
    inner_batch: Option<RecordBatch>,
    /// Row index into `inner_batch` — the next unprocessed inner row.
    inner_offset: usize,
    inner_key_arrays: Vec<ArrayRef>,

    // Per-outer-batch match tracking, reused across batches.
    // Bit-packed (not Vec<bool>) so that:
    //  - emit: finish() yields a BooleanBuffer directly (no packing iteration)
    //  - OR: apply_bitwise_binary_op ORs filter results in u64 chunks
    //  - count: UnalignedBitChunk::count_ones uses popcnt
    matched: BooleanBufferBuilder,

    // Inner key group buffer: all inner rows sharing the current join key.
    // Only populated when a filter is present. Unbounded — a single key
    // with many inner rows will buffer them all. See "Degenerate cases"
    // in exec.rs. Spilled to disk when memory reservation fails.
    inner_key_buffer: Vec<RecordBatch>,
    inner_key_spill: Option<Arc<dyn SpillFile>>,

    // Join ON expressions, evaluated against each new batch to produce
    // the key arrays used for sorted key comparisons.
    on_outer: Vec<PhysicalExprRef>,
    on_inner: Vec<PhysicalExprRef>,
    filter: Option<JoinFilter>,
    sort_options: Vec<SortOptions>,
    null_equality: NullEquality,
    // Decomposed from JoinType: when RightSemi/RightAnti, outer=right,
    // inner=left, so we swap sides when building the filter batch.
    outer_is_left: bool,

    // Output
    coalescer: BatchCoalescer,
    schema: SchemaRef,

    // Metrics
    input_batches: Count,
    input_rows: Count,
    baseline_metrics: BaselineMetrics,
    peak_mem_used: Gauge,

    // Memory / spill — only the inner key buffer is tracked via reservation,
    // matching existing SMJ (which tracks only the buffered side). The outer
    // batch is a single batch at a time and cannot be spilled.
    reservation: MemoryReservation,
    spill_manager: SpillManager,
    runtime_env: Arc<datafusion_execution::runtime_env::RuntimeEnv>,
    inner_buffer_size: usize,

    // Cached comparators — pre-built to avoid per-row type dispatch.
    /// Comparator for outer vs inner key comparison
    outer_inner_cmp: Option<JoinKeyComparator>,
    /// Comparator for outer self-comparison (find_key_group_end on outer)
    outer_self_cmp: Option<JoinKeyComparator>,
    /// Comparator for inner self-comparison (find_key_group_end on inner)
    inner_self_cmp: Option<JoinKeyComparator>,
}

impl BitwiseSortMergeJoinStream {
    #[expect(clippy::too_many_arguments)]
    pub fn try_new(
        schema: SchemaRef,
        sort_options: Vec<SortOptions>,
        null_equality: NullEquality,
        outer: SendableRecordBatchStream,
        inner: SendableRecordBatchStream,
        on_outer: Vec<PhysicalExprRef>,
        on_inner: Vec<PhysicalExprRef>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        batch_size: usize,
        partition: usize,
        metrics: &ExecutionPlanMetricsSet,
        reservation: MemoryReservation,
        spill_manager: SpillManager,
        runtime_env: Arc<datafusion_execution::runtime_env::RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        debug_assert!(
            matches!(
                join_type,
                JoinType::LeftSemi
                    | JoinType::RightSemi
                    | JoinType::LeftAnti
                    | JoinType::RightAnti
                    | JoinType::LeftMark
                    | JoinType::RightMark
            ),
            "BitwiseSortMergeJoinStream does not handle {join_type:?}"
        );
        let outer_is_left = matches!(
            join_type,
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark
        );

        // TODO: join_time is registered but not yet populated; time metrics
        // will be re-wired once the generator-based refactor settles.
        let _join_time = MetricBuilder::new(metrics).subset_time("join_time", partition);
        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);
        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);
        let baseline_metrics = BaselineMetrics::new(metrics, partition);
        let peak_mem_used =
            MetricBuilder::new(metrics).peak_memory_usage("peak_mem_used", partition);

        let mut state = Self {
            join_type,
            outer,
            inner,
            outer_batch: None,
            outer_offset: 0,
            outer_key_arrays: vec![],
            inner_batch: None,
            inner_offset: 0,
            inner_key_arrays: vec![],
            matched: BooleanBufferBuilder::new(0),
            inner_key_buffer: vec![],
            inner_key_spill: None,
            on_outer,
            on_inner,
            filter,
            sort_options,
            null_equality,
            outer_is_left,
            coalescer: BatchCoalescer::new(Arc::clone(&schema), batch_size)
                .with_biggest_coalesce_batch_size(Some(batch_size / 2)),
            schema: Arc::clone(&schema),
            input_batches,
            input_rows,
            baseline_metrics,
            peak_mem_used,
            reservation,
            spill_manager,
            runtime_env,
            inner_buffer_size: 0,
            outer_inner_cmp: None,
            outer_self_cmp: None,
            inner_self_cmp: None,
        };

        let stream = async_try_stream(|mut emitter| async move {
            state.join(&mut emitter).await?;
            state.baseline_metrics.done();
            Ok(())
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    /// Resize the memory reservation to match current tracked usage.
    fn try_resize_reservation(&mut self) -> Result<()> {
        let needed = self.inner_buffer_size;
        self.reservation.try_resize(needed)?;
        self.peak_mem_used.set_max(self.reservation.size());
        Ok(())
    }

    /// Get or build the outer vs inner key comparator.
    fn get_outer_inner_cmp(&mut self) -> Result<&JoinKeyComparator> {
        if self.outer_inner_cmp.is_none() {
            self.outer_inner_cmp = Some(JoinKeyComparator::new(
                &self.outer_key_arrays,
                &self.inner_key_arrays,
                &self.sort_options,
                self.null_equality,
            )?);
        }
        Ok(self.outer_inner_cmp.as_ref().unwrap())
    }

    /// Get or build the outer self-comparison comparator.
    fn get_outer_self_cmp(&mut self) -> Result<&JoinKeyComparator> {
        if self.outer_self_cmp.is_none() {
            self.outer_self_cmp = Some(JoinKeyComparator::new(
                &self.outer_key_arrays,
                &self.outer_key_arrays,
                &self.sort_options,
                self.null_equality,
            )?);
        }
        Ok(self.outer_self_cmp.as_ref().unwrap())
    }

    /// Get or build the inner self-comparison comparator.
    fn get_inner_self_cmp(&mut self) -> Result<&JoinKeyComparator> {
        if self.inner_self_cmp.is_none() {
            self.inner_self_cmp = Some(JoinKeyComparator::new(
                &self.inner_key_arrays,
                &self.inner_key_arrays,
                &self.sort_options,
                self.null_equality,
            )?);
        }
        Ok(self.inner_self_cmp.as_ref().unwrap())
    }

    /// Spill the in-memory inner key buffer to disk and clear it.
    fn spill_inner_key_buffer(&mut self) -> Result<()> {
        let spill_file = self
            .spill_manager
            .spill_record_batch_and_finish(
                &self.inner_key_buffer,
                "semi_anti_smj_inner_key_spill",
            )?
            .expect("inner_key_buffer is non-empty when spilling");
        self.inner_key_buffer.clear();
        self.inner_buffer_size = 0;
        self.inner_key_spill = Some(spill_file);
        // Should succeed now — inner buffer has been spilled.
        self.try_resize_reservation()
    }

    /// Clear inner key group state after processing. Does not resize the
    /// reservation — the next key group will resize when buffering, or
    /// the stream's Drop will free it. This avoids unnecessary memory
    /// pool interactions (see apache/datafusion#20729).
    fn clear_inner_key_group(&mut self) {
        self.inner_key_buffer.clear();
        self.inner_key_spill = None;
        self.inner_buffer_size = 0;
    }

    /// Fetch the next outer batch. Returns true if a batch was loaded.
    async fn next_outer_batch(&mut self) -> Result<bool> {
        loop {
            match self.outer.next().await {
                None => {
                    // Release the outer input pipeline's resources.
                    let outer_schema = self.outer.schema();
                    self.outer = Box::pin(EmptyRecordBatchStream::new(outer_schema));
                    return Ok(false);
                }
                Some(Err(e)) => return Err(e),
                Some(Ok(batch)) => {
                    let batch_num_rows = batch.num_rows();
                    self.input_batches.add(1);
                    self.input_rows.add(batch_num_rows);
                    if batch_num_rows == 0 {
                        continue;
                    }
                    let keys = evaluate_join_keys(&batch, &self.on_outer)?;
                    self.outer_batch = Some(batch);
                    self.outer_offset = 0;
                    self.outer_key_arrays = keys;
                    self.outer_inner_cmp = None;
                    self.outer_self_cmp = None;
                    self.matched = BooleanBufferBuilder::new(batch_num_rows);
                    self.matched.append_n(batch_num_rows, false);
                    return Ok(true);
                }
            }
        }
    }

    /// Fetch the next inner batch. Returns true if a batch was loaded.
    async fn next_inner_batch(&mut self) -> Result<bool> {
        loop {
            match self.inner.next().await {
                None => {
                    // Release the inner input pipeline's resources.
                    let inner_schema = self.inner.schema();
                    self.inner = Box::pin(EmptyRecordBatchStream::new(inner_schema));
                    return Ok(false);
                }
                Some(Err(e)) => return Err(e),
                Some(Ok(batch)) => {
                    let batch_num_rows = batch.num_rows();
                    self.input_batches.add(1);
                    self.input_rows.add(batch_num_rows);
                    if batch_num_rows == 0 {
                        continue;
                    }
                    let keys = evaluate_join_keys(&batch, &self.on_inner)?;
                    self.inner_batch = Some(batch);
                    self.inner_offset = 0;
                    self.inner_key_arrays = keys;
                    self.outer_inner_cmp = None;
                    self.inner_self_cmp = None;
                    return Ok(true);
                }
            }
        }
    }

    /// Push the current outer batch into the coalescer, applying the matched
    /// bitset as a selection mask. Consumes the batch (`outer_batch` becomes
    /// `None`).
    fn emit_outer_batch(&mut self) -> Result<()> {
        let batch = self.outer_batch.take().unwrap();

        // finish() converts the bit-packed builder directly to a
        // BooleanBuffer — no iteration or repacking needed.
        let matched_buf = self.matched.finish();

        match self.join_type {
            JoinType::LeftMark | JoinType::RightMark => {
                // Mark joins emit ALL outer rows with a boolean match column appended.
                debug_assert_eq!(
                    self.schema.fields().len(),
                    batch.num_columns() + 1,
                    "Mark join output schema should be outer schema + 1 mark column"
                );
                let mark_col = Arc::new(BooleanArray::new(matched_buf, None)) as ArrayRef;
                let mut columns = Vec::with_capacity(batch.num_columns() + 1);
                columns.extend_from_slice(batch.columns());
                columns.push(mark_col);
                let output = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
                self.coalescer.push_batch(output)?;
            }
            JoinType::LeftSemi | JoinType::RightSemi => {
                let selection = BooleanArray::new(matched_buf, None);
                let filtered = filter_record_batch(&batch, &selection)?;
                if filtered.num_rows() > 0 {
                    self.coalescer.push_batch(filtered)?;
                }
            }
            JoinType::LeftAnti | JoinType::RightAnti => {
                let selection = not(&BooleanArray::new(matched_buf, None))?;
                let filtered = filter_record_batch(&batch, &selection)?;
                if filtered.num_rows() > 0 {
                    self.coalescer.push_batch(filtered)?;
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    /// Mark all outer rows in the current key group as matched and advance
    /// the outer cursor past the group (within the current batch).
    fn mark_outer_key_group_matched(&mut self) -> Result<()> {
        let num_outer = self.outer_batch.as_ref().unwrap().num_rows();
        let from = self.outer_offset;
        let group_end = find_key_group_end(self.get_outer_self_cmp()?, from, num_outer);

        for i in from..group_end {
            self.matched.set_bit(i, true);
        }

        self.outer_offset = group_end;
        Ok(())
    }

    /// Advance the inner cursor past the current key group. The group may
    /// span multiple inner batches. Sets `inner_batch` to `None` if inner
    /// is exhausted.
    async fn advance_inner_past_key_group(&mut self) -> Result<()> {
        loop {
            let Some(inner_batch) = &self.inner_batch else {
                return Ok(());
            };
            let num_inner = inner_batch.num_rows();
            let from = self.inner_offset;
            let group_end =
                find_key_group_end(self.get_inner_self_cmp()?, from, num_inner);

            if group_end < num_inner {
                self.inner_offset = group_end;
                return Ok(());
            }

            // Key group extends to the end of the batch — it may continue
            // into the next one; save the last key so we can check.
            let saved_inner_keys = slice_keys(&self.inner_key_arrays, num_inner - 1);

            if !self.next_inner_batch().await? {
                self.inner_batch = None;
                return Ok(());
            }
            if !keys_match(
                &saved_inner_keys,
                &self.inner_key_arrays,
                &self.sort_options,
                self.null_equality,
            )? {
                return Ok(());
            }
        }
    }

    /// Buffer the inner key group for filter evaluation, advancing the inner
    /// cursor past the group. Collects all inner rows with the current key
    /// across batch boundaries. Sets `inner_batch` to `None` if inner is
    /// exhausted.
    async fn buffer_inner_key_group(&mut self) -> Result<()> {
        self.clear_inner_key_group();

        loop {
            let Some(inner_batch) = &self.inner_batch else {
                return Ok(());
            };
            let num_inner = inner_batch.num_rows();
            let from = self.inner_offset;
            let group_end =
                find_key_group_end(self.get_inner_self_cmp()?, from, num_inner);

            let inner_batch = self.inner_batch.as_ref().unwrap();
            let slice = inner_batch.slice(from, group_end - from);
            self.inner_buffer_size += slice.get_array_memory_size();
            self.inner_key_buffer.push(slice);

            // Reserve memory for the newly buffered slice. If the pool
            // is exhausted, spill the entire buffer to disk.
            if self.try_resize_reservation().is_err() {
                if self.runtime_env.disk_manager.tmp_files_enabled() {
                    self.spill_inner_key_buffer()?;
                } else {
                    // Re-attempt to get the error message
                    self.try_resize_reservation().map_err(|e| {
                        DataFusionError::Execution(format!(
                            "{e}. Disk spilling disabled."
                        ))
                    })?;
                }
            }

            if group_end < num_inner {
                self.inner_offset = group_end;
                return Ok(());
            }

            // Key group extends to the end of the batch — it may continue
            // into the next one; save the last key so we can check.
            let saved_inner_keys = slice_keys(&self.inner_key_arrays, num_inner - 1);

            if !self.next_inner_batch().await? {
                self.inner_batch = None;
                return Ok(());
            }
            if !keys_match(
                &saved_inner_keys,
                &self.inner_key_arrays,
                &self.sort_options,
                self.null_equality,
            )? {
                return Ok(());
            }
        }
    }

    /// Process a key match with a filter. For each inner row in the buffered
    /// key group, evaluates the filter against the outer key group and ORs
    /// the results into the matched bitset using u64-chunked bitwise ops.
    async fn process_key_match_with_filter(&mut self) -> Result<()> {
        let num_outer = self.outer_batch.as_ref().unwrap().num_rows();

        // buffer_inner_key_group must be called before this function
        debug_assert!(
            !self.inner_key_buffer.is_empty() || self.inner_key_spill.is_some(),
            "process_key_match_with_filter called with no inner key data"
        );
        debug_assert!(
            self.outer_offset < num_outer,
            "outer_offset must be within the current batch"
        );
        debug_assert!(
            self.matched.len() == num_outer,
            "matched vector must be sized for the current outer batch"
        );

        let outer_group_start = self.outer_offset;
        let outer_group_end =
            find_key_group_end(self.get_outer_self_cmp()?, outer_group_start, num_outer);
        let outer_group_len = outer_group_end - outer_group_start;

        let filter = self.filter.as_ref().unwrap();
        let outer_batch = self.outer_batch.as_ref().unwrap();
        let outer_slice = outer_batch.slice(outer_group_start, outer_group_len);

        // Count already-matched bits using popcnt on u64 chunks (zero-copy).
        let mut matched_count = UnalignedBitChunk::new(
            self.matched.as_slice(),
            outer_group_start,
            outer_group_len,
        )
        .count_ones();

        // Process spilled inner batches first asynchronously.
        if matched_count < outer_group_len
            && let Some(spill_file) = &self.inner_key_spill
        {
            let mut spill_stream = self
                .spill_manager
                .read_spill_as_stream(Arc::clone(spill_file), None)?;
            let mut spill_stream_has_data = false;

            while matched_count < outer_group_len {
                match spill_stream.next().await {
                    Some(Ok(inner_slice)) => {
                        spill_stream_has_data = true;
                        matched_count = eval_filter_for_inner_slice(
                            self.outer_is_left,
                            filter,
                            &outer_slice,
                            &inner_slice,
                            &mut self.matched,
                            outer_group_start,
                            outer_group_len,
                            matched_count,
                        )?;
                    }
                    Some(Err(e)) => return Err(e),
                    None => {
                        if !spill_stream_has_data {
                            return internal_err!("Spill file was empty");
                        }
                        break;
                    }
                }
            }
        }

        // Then process in-memory inner batches.
        // evaluate_filter_for_inner_row is a free function (not &self method)
        // so that Rust can split the struct borrow: &mut self.matched coexists
        // with &self.inner_key_buffer and &self.filter inside this loop.
        if matched_count < outer_group_len {
            'outer: for inner_slice in &self.inner_key_buffer {
                matched_count = eval_filter_for_inner_slice(
                    self.outer_is_left,
                    filter,
                    &outer_slice,
                    inner_slice,
                    &mut self.matched,
                    outer_group_start,
                    outer_group_len,
                    matched_count,
                )?;
                if matched_count == outer_group_len {
                    break 'outer;
                }
            }
        }

        self.outer_offset = outer_group_end;

        Ok(())
    }

    /// Evaluate the filter for the buffered inner key group against the
    /// outer key group. If the outer key group continues into subsequent
    /// outer batches, keep evaluating there too.
    async fn process_filtered_match_loop(&mut self) -> Result<()> {
        loop {
            self.process_key_match_with_filter().await?;

            let outer_batch = self.outer_batch.as_ref().unwrap();
            if self.outer_offset < outer_batch.num_rows() {
                break;
            }

            // The outer key group may continue into the next outer batch;
            // save the last key so we can check.
            let saved_keys =
                slice_keys(&self.outer_key_arrays, outer_batch.num_rows() - 1);

            self.emit_outer_batch()?;

            if !self.next_outer_batch().await? {
                break;
            }
            if !keys_match(
                &saved_keys,
                &self.outer_key_arrays,
                &self.sort_options,
                self.null_equality,
            )? {
                break;
            }
        }

        self.clear_inner_key_group();
        Ok(())
    }

    /// Mark the outer key group as matched. If the outer key group continues
    /// into subsequent outer batches, keep marking there too.
    async fn process_unfiltered_match_loop(&mut self) -> Result<()> {
        loop {
            self.mark_outer_key_group_matched()?;

            let outer_batch = self.outer_batch.as_ref().unwrap();
            if self.outer_offset < outer_batch.num_rows() {
                return Ok(());
            }

            // The outer key group may continue into the next outer batch;
            // save the last key so we can check.
            let saved_keys =
                slice_keys(&self.outer_key_arrays, outer_batch.num_rows() - 1);

            self.emit_outer_batch()?;

            if !self.next_outer_batch().await? {
                return Ok(());
            }
            if !keys_match(
                &saved_keys,
                &self.outer_key_arrays,
                &self.sort_options,
                self.null_equality,
            )? {
                return Ok(());
            }
        }
    }

    /// Keys at both cursors are equal: determine which outer rows in the key
    /// group have a match. Both key groups may span batch boundaries.
    async fn process_key_match(&mut self) -> Result<()> {
        if self.filter.is_some() {
            // Buffer the inner key group so each inner row can be evaluated
            // against the outer key group, OR-ing filter results into the
            // matched bitset.
            self.buffer_inner_key_group().await?;
            self.process_filtered_match_loop().await
        } else {
            // Without a filter, key equality alone means every outer row in
            // the group matches; the inner rows themselves are not needed.
            self.advance_inner_past_key_group().await?;
            self.process_unfiltered_match_loop().await
        }
    }

    /// Compare the join keys at the outer and inner cursors, returning the
    /// ordering of the outer key relative to the inner key (e.g. `Greater`
    /// means outer key > inner key, per the sort options).
    fn compare_current_keys(&mut self) -> Result<Ordering> {
        let (outer_idx, inner_idx) = (self.outer_offset, self.inner_offset);
        Ok(self.get_outer_inner_cmp()?.compare(outer_idx, inner_idx))
    }

    /// Outer key is unmatched: advance the outer cursor past its key group
    /// (within the current batch). If the group continues into the next
    /// batch, those rows compare Less again and are skipped the same way.
    fn skip_outer_key_group(&mut self) -> Result<()> {
        let num_outer = self.outer_batch.as_ref().unwrap().num_rows();
        let from = self.outer_offset;
        self.outer_offset =
            find_key_group_end(self.get_outer_self_cmp()?, from, num_outer);
        Ok(())
    }

    /// Sync fast path for `Ordering::Greater`: skip the inner key group when
    /// it ends within the current batch. Returns false — leaving all state
    /// unchanged — when the group reaches the batch boundary, in which case
    /// the caller must take [`Self::advance_inner_past_key_group`].
    fn try_skip_inner_key_group(&mut self) -> Result<bool> {
        let num_inner = self.inner_batch.as_ref().unwrap().num_rows();
        let from = self.inner_offset;
        let group_end = find_key_group_end(self.get_inner_self_cmp()?, from, num_inner);
        if group_end >= num_inner {
            return Ok(false);
        }
        self.inner_offset = group_end;
        Ok(true)
    }

    /// Sync fast path for `Ordering::Equal` without a filter: when both key
    /// groups end within their current batches (the common case — a group
    /// only reaches a batch boundary once per batch), mark the outer group
    /// matched and advance both cursors without any async machinery.
    /// Returns false — leaving all state unchanged — when a filter is
    /// present or either group reaches a batch boundary, in which case the
    /// caller must take [`Self::process_key_match`].
    fn try_process_key_match(&mut self) -> Result<bool> {
        if self.filter.is_some() {
            return Ok(false);
        }

        let num_inner = self.inner_batch.as_ref().unwrap().num_rows();
        let inner_from = self.inner_offset;
        let inner_group_end =
            find_key_group_end(self.get_inner_self_cmp()?, inner_from, num_inner);
        if inner_group_end >= num_inner {
            return Ok(false);
        }

        let num_outer = self.outer_batch.as_ref().unwrap().num_rows();
        let outer_from = self.outer_offset;
        let outer_group_end =
            find_key_group_end(self.get_outer_self_cmp()?, outer_from, num_outer);
        if outer_group_end >= num_outer {
            return Ok(false);
        }

        for i in outer_from..outer_group_end {
            self.matched.set_bit(i, true);
        }
        self.outer_offset = outer_group_end;
        self.inner_offset = inner_group_end;
        Ok(true)
    }

    /// True when the outer cursor already points at an unprocessed row: the
    /// sync fast path of [`Self::advance_outer_row`]. Checked inline in the
    /// hot loop so the async helper (and its state machine) is only entered
    /// at batch boundaries — same pattern as `sorts/merge.rs`.
    fn has_current_outer_row(&self) -> bool {
        self.outer_batch
            .as_ref()
            .is_some_and(|batch| self.outer_offset < batch.num_rows())
    }

    /// True when the inner cursor already points at an unprocessed row: the
    /// sync fast path of [`Self::advance_inner_row`].
    fn has_current_inner_row(&self) -> bool {
        self.inner_batch
            .as_ref()
            .is_some_and(|batch| self.inner_offset < batch.num_rows())
    }

    /// Ensure the outer cursor points at an unprocessed row, emitting
    /// finished outer batches and loading new ones as needed. Returns false
    /// when outer is exhausted.
    async fn advance_outer_row(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<bool> {
        loop {
            match &self.outer_batch {
                Some(batch) if self.outer_offset < batch.num_rows() => {
                    return Ok(true);
                }
                Some(_) => {
                    // Current batch fully scanned — emit it and load the next.
                    self.emit_outer_batch()?;
                    self.emit_completed_batches(emitter).await;
                }
                None => {
                    if !self.next_outer_batch().await? {
                        return Ok(false);
                    }
                }
            }
        }
    }

    /// Ensure the inner cursor points at an unprocessed row, loading new
    /// inner batches as needed. Returns false when inner is exhausted.
    async fn advance_inner_row(&mut self) -> Result<bool> {
        loop {
            if let Some(batch) = &self.inner_batch
                && self.inner_offset < batch.num_rows()
            {
                return Ok(true);
            }
            if !self.next_inner_batch().await? {
                self.inner_batch = None;
                return Ok(false);
            }
        }
    }

    /// Inner is exhausted, so no further matches are possible: emit the
    /// current outer batch and all remaining ones with their current matched
    /// bits (semi drops unmatched rows, anti emits them, mark emits them
    /// with mark=false).
    async fn drain_outer(&mut self) -> Result<()> {
        self.emit_outer_batch()?;
        while self.next_outer_batch().await? {
            self.emit_outer_batch()?;
        }
        Ok(())
    }

    /// Emit all completed coalescer batches to the stream consumer.
    async fn emit_completed_batches(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) {
        while let Some(batch) = self.coalescer.next_completed_batch() {
            (&batch).record_output(&self.baseline_metrics);
            emitter.emit(batch).await;
        }
    }

    /// Main loop: a classic merge-scan over the two sorted inputs, emitting
    /// output batches as they complete.
    async fn join(
        &mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        // The `has_current_*` / `has_completed_batch` fast paths keep async
        // state machinery out of the per-key-group hot path; the awaiting
        // helpers are only entered at batch boundaries.
        while self.has_current_outer_row() || self.advance_outer_row(emitter).await? {
            if !(self.has_current_inner_row() || self.advance_inner_row().await?) {
                self.drain_outer().await?;
                break;
            }

            // Each arm handles the common case synchronously (`try_*`); the
            // async continuations only run when a key group reaches a batch
            // boundary or a filter must be evaluated.
            match self.compare_current_keys()? {
                Ordering::Less => self.skip_outer_key_group()?,
                Ordering::Greater => {
                    if !self.try_skip_inner_key_group()? {
                        self.advance_inner_past_key_group().await?;
                    }
                }
                Ordering::Equal => {
                    if !self.try_process_key_match()? {
                        self.process_key_match().await?;
                    }
                }
            }

            if self.coalescer.has_completed_batch() {
                self.emit_completed_batches(emitter).await;
            }
        }

        // Flush whatever is still buffered in the coalescer.
        self.coalescer.finish_buffered_batch()?;
        self.emit_completed_batches(emitter).await;
        Ok(())
    }
}

/// Evaluate the filter for all rows in an inner slice against the outer group,
/// OR-ing results into the matched bitset. Returns the updated matched count.
/// Extracted as a free function so Rust can split borrows on the stream struct.
#[expect(clippy::too_many_arguments)]
fn eval_filter_for_inner_slice(
    outer_is_left: bool,
    filter: &JoinFilter,
    outer_slice: &RecordBatch,
    inner_slice: &RecordBatch,
    matched: &mut BooleanBufferBuilder,
    outer_offset: usize,
    outer_group_len: usize,
    // Passed in to avoid recounting bits we just counted at the call site.
    mut matched_count: usize,
) -> Result<usize> {
    debug_assert_eq!(
        matched_count,
        UnalignedBitChunk::new(matched.as_slice(), outer_offset, outer_group_len)
            .count_ones()
    );
    for inner_row in 0..inner_slice.num_rows() {
        if matched_count == outer_group_len {
            break;
        }

        let filter_result = evaluate_filter_for_inner_row(
            outer_is_left,
            filter,
            outer_slice,
            inner_slice,
            inner_row,
        )?;

        // OR filter results into the matched bitset. Both sides are
        // bit-packed [u8] buffers, so apply_bitwise_binary_op
        // processes 64 bits per loop iteration (not 1 bit at a time).
        //
        // The offsets handle alignment: outer_offset is the bit
        // position within matched where this key group starts,
        // and filter_buf.offset() is the BooleanBuffer's internal
        // bit offset (usually 0, but not guaranteed by Arrow).
        let filter_buf = filter_result.values();
        apply_bitwise_binary_op(
            matched.as_slice_mut(),
            outer_offset,
            filter_buf.inner().as_slice(),
            filter_buf.offset(),
            outer_group_len,
            |a, b| a | b,
        );

        // Recount matched bits after the OR. UnalignedBitChunk is
        // zero-copy — it reads the bytes in place and uses popcnt.
        matched_count =
            UnalignedBitChunk::new(matched.as_slice(), outer_offset, outer_group_len)
                .count_ones();
    }
    Ok(matched_count)
}

/// Slice each key array to a single row at `idx`.
fn slice_keys(keys: &[ArrayRef], idx: usize) -> Vec<ArrayRef> {
    keys.iter().map(|a| a.slice(idx, 1)).collect()
}

/// Compare the first row of two key arrays using sort options to determine
/// equality. The left side is expected to be single-row slices (from
/// `slice_keys`); the right side can be any length (row 0 is compared).
fn keys_match(
    left_arrays: &[ArrayRef],
    right_arrays: &[ArrayRef],
    sort_options: &[SortOptions],
    null_equality: NullEquality,
) -> Result<bool> {
    debug_assert!(left_arrays.iter().all(|a| a.len() == 1));
    let cmp = compare_join_arrays(
        left_arrays,
        0,
        right_arrays,
        0,
        sort_options,
        null_equality,
    )?;
    Ok(cmp == Ordering::Equal)
}

/// Evaluate the join filter for one inner row against a slice of outer rows.
///
/// Free function (not a method on BitwiseSortMergeJoinStream) so that Rust
/// can split the struct borrow in process_key_match_with_filter: the caller
/// holds &mut self.matched and &self.inner_key_buffer simultaneously, which
/// is impossible if this borrows all of &self.
fn evaluate_filter_for_inner_row(
    outer_is_left: bool,
    filter: &JoinFilter,
    outer_slice: &RecordBatch,
    inner_batch: &RecordBatch,
    inner_idx: usize,
) -> Result<BooleanArray> {
    let num_outer_rows = outer_slice.num_rows();

    // Build filter input columns in the order the filter expects
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(filter.column_indices().len());
    for col_idx in filter.column_indices() {
        let (side_batch, side_idx) = if outer_is_left {
            match col_idx.side {
                JoinSide::Left => (outer_slice, None),
                JoinSide::Right => (inner_batch, Some(inner_idx)),
                JoinSide::None => {
                    return internal_err!("Unexpected JoinSide::None in filter");
                }
            }
        } else {
            match col_idx.side {
                JoinSide::Left => (inner_batch, Some(inner_idx)),
                JoinSide::Right => (outer_slice, None),
                JoinSide::None => {
                    return internal_err!("Unexpected JoinSide::None in filter");
                }
            }
        };

        match side_idx {
            None => {
                columns.push(Arc::clone(side_batch.column(col_idx.index)));
            }
            Some(idx) => {
                // Broadcasts inner scalar to N-element array. Arrow's
                // BinaryExpr handles Scalar×Array natively via the Datum
                // trait, but Column::evaluate always returns Array, so
                // we'd need a custom expr to avoid this broadcast.
                let scalar = ScalarValue::try_from_array(
                    side_batch.column(col_idx.index).as_ref(),
                    idx,
                )?;
                columns.push(scalar.to_array_of_size(num_outer_rows)?);
            }
        }
    }

    let filter_batch = RecordBatch::try_new(Arc::clone(filter.schema()), columns)?;
    let result = filter
        .expression()
        .evaluate(&filter_batch)?
        .into_array(num_outer_rows)?;
    let bool_arr = result
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "Filter expression did not return BooleanArray".to_string(),
            )
        })?;
    // Treat nulls as false
    if bool_arr.null_count() > 0 {
        Ok(arrow::compute::prep_null_mask_filter(bool_arr))
    } else {
        Ok(bool_arr.clone())
    }
}
