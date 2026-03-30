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
//! next batch, and continuing if the key matches. The [`PendingBoundary`] enum
//! preserves loop context across async `Poll::Pending` re-entries.
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
use std::fs::File;
use std::io::BufReader;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::RecordBatchStream;
use crate::joins::utils::{JoinFilter, compare_join_arrays};
use crate::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder,
};
use crate::spill::spill_manager::SpillManager;
use arrow::array::{Array, ArrayRef, BooleanArray, BooleanBufferBuilder, RecordBatch};
use arrow::compute::{BatchCoalescer, SortOptions, filter_record_batch, not};
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::util::bit_chunk_iterator::UnalignedBitChunk;
use arrow::util::bit_util::apply_bitwise_binary_op;
use datafusion_common::{
    JoinSide, JoinType, NullEquality, Result, ScalarValue, internal_err,
};
use datafusion_execution::SendableRecordBatchStream;
use datafusion_execution::disk_manager::RefCountedTempFile;
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_physical_expr_common::physical_expr::PhysicalExprRef;

use futures::{Stream, StreamExt, ready};

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
/// differs from the key at `from`. Uses `compare_join_arrays` for zero-alloc
/// ordinal comparison.
///
/// Optimized for join workloads: checks adjacent and boundary keys before
/// falling back to binary search, since most key groups are small (often 1).
fn find_key_group_end(
    key_arrays: &[ArrayRef],
    from: usize,
    len: usize,
    sort_options: &[SortOptions],
    null_equality: NullEquality,
) -> Result<usize> {
    let next = from + 1;
    if next >= len {
        return Ok(len);
    }

    // Fast path: single-row group (common with unique keys).
    if compare_join_arrays(
        key_arrays,
        from,
        key_arrays,
        next,
        sort_options,
        null_equality,
    )? != Ordering::Equal
    {
        return Ok(next);
    }

    // Check if the entire remaining batch shares this key.
    let last = len - 1;
    if compare_join_arrays(
        key_arrays,
        from,
        key_arrays,
        last,
        sort_options,
        null_equality,
    )? == Ordering::Equal
    {
        return Ok(len);
    }

    // Binary search the interior: key at `next` matches, key at `last` doesn't.
    let mut lo = next + 1;
    let mut hi = last;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if compare_join_arrays(
            key_arrays,
            from,
            key_arrays,
            mid,
            sort_options,
            null_equality,
        )? == Ordering::Equal
        {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// When an outer key group spans a batch boundary, the boundary loop emits
/// the current batch, then polls for the next. If that poll returns Pending,
/// `ready!` exits `poll_join` and we re-enter from the top on the next call.
/// Without this state, the new batch would be processed fresh by the
/// merge-scan — but inner already advanced past this key, so the matching
/// outer rows would be skipped via `Ordering::Less` and never marked.
///
/// This enum carries the last key (as single-row sliced arrays) from the
/// previous batch so we can check whether the next batch continues the same
/// key group. Stored as `Option<PendingBoundary>`: `None` means normal
/// processing.
#[derive(Debug)]
enum PendingBoundary {
    /// Resuming a no-filter boundary loop.
    NoFilter { saved_keys: Vec<ArrayRef> },
    /// Resuming a filtered boundary loop. Inner key data remains in the
    /// buffer (or spill file) for the resumed loop.
    Filtered { saved_keys: Vec<ArrayRef> },
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
    inner_key_spill: Option<RefCountedTempFile>,

    // True when buffer_inner_key_group returned Pending after partially
    // filling inner_key_buffer. On re-entry, buffer_inner_key_group
    // must skip clear() and resume from poll_next_inner_batch (the
    // current inner_batch was already sliced and pushed before Pending).
    buffering_inner_pending: bool,

    // Boundary re-entry state — see PendingBoundary doc comment.
    pending_boundary: Option<PendingBoundary>,

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
    join_time: crate::metrics::Time,
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

    // True once the current outer batch has been emitted. The Equal
    // branch's inner loops call emit then `ready!(poll_next_outer_batch)`.
    // If that poll returns Pending, poll_join re-enters from the top
    // on the next poll — with outer_batch still Some and outer_offset
    // past the end. The main loop's step 3 would re-emit without this
    // guard. Cleared when poll_next_outer_batch loads a new batch.
    batch_emitted: bool,
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
    ) -> Result<Self> {
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

        let join_time = MetricBuilder::new(metrics).subset_time("join_time", partition);
        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);
        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);
        let baseline_metrics = BaselineMetrics::new(metrics, partition);
        let peak_mem_used = MetricBuilder::new(metrics).gauge("peak_mem_used", partition);

        Ok(Self {
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
            buffering_inner_pending: false,
            pending_boundary: None,
            on_outer,
            on_inner,
            filter,
            sort_options,
            null_equality,
            outer_is_left,
            coalescer: BatchCoalescer::new(Arc::clone(&schema), batch_size)
                .with_biggest_coalesce_batch_size(Some(batch_size / 2)),
            schema,
            join_time,
            input_batches,
            input_rows,
            baseline_metrics,
            peak_mem_used,
            reservation,
            spill_manager,
            runtime_env,
            inner_buffer_size: 0,
            batch_emitted: false,
        })
    }

    /// Resize the memory reservation to match current tracked usage.
    fn try_resize_reservation(&mut self) -> Result<()> {
        let needed = self.inner_buffer_size;
        self.reservation.try_resize(needed)?;
        self.peak_mem_used.set_max(self.reservation.size());
        Ok(())
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

    /// Poll for the next outer batch. Returns true if a batch was loaded.
    fn poll_next_outer_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool>> {
        loop {
            match ready!(self.outer.poll_next_unpin(cx)) {
                None => return Poll::Ready(Ok(false)),
                Some(Err(e)) => return Poll::Ready(Err(e)),
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
                    self.batch_emitted = false;
                    self.matched = BooleanBufferBuilder::new(batch_num_rows);
                    self.matched.append_n(batch_num_rows, false);
                    return Poll::Ready(Ok(true));
                }
            }
        }
    }

    /// Poll for the next inner batch. Returns true if a batch was loaded.
    fn poll_next_inner_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool>> {
        loop {
            match ready!(self.inner.poll_next_unpin(cx)) {
                None => return Poll::Ready(Ok(false)),
                Some(Err(e)) => return Poll::Ready(Err(e)),
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
                    return Poll::Ready(Ok(true));
                }
            }
        }
    }

    /// Emit the current outer batch through the coalescer, applying the
    /// matched bitset as a selection mask. No-op if already emitted
    /// (see `batch_emitted` field).
    fn emit_outer_batch(&mut self) -> Result<()> {
        if self.batch_emitted {
            return Ok(());
        }
        self.batch_emitted = true;

        let batch = self.outer_batch.as_ref().unwrap();

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
                let filtered = filter_record_batch(batch, &selection)?;
                if filtered.num_rows() > 0 {
                    self.coalescer.push_batch(filtered)?;
                }
            }
            JoinType::LeftAnti | JoinType::RightAnti => {
                let selection = not(&BooleanArray::new(matched_buf, None))?;
                let filtered = filter_record_batch(batch, &selection)?;
                if filtered.num_rows() > 0 {
                    self.coalescer.push_batch(filtered)?;
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    /// Process a key match between outer and inner sides (no filter).
    /// Sets matched bits for all outer rows sharing the current key.
    fn process_key_match_no_filter(&mut self) -> Result<()> {
        let outer_batch = self.outer_batch.as_ref().unwrap();
        let num_outer = outer_batch.num_rows();

        let outer_group_end = find_key_group_end(
            &self.outer_key_arrays,
            self.outer_offset,
            num_outer,
            &self.sort_options,
            self.null_equality,
        )?;

        for i in self.outer_offset..outer_group_end {
            self.matched.set_bit(i, true);
        }

        self.outer_offset = outer_group_end;
        Ok(())
    }

    /// Advance inner past the current key group. Returns Ok(true) if inner
    /// is exhausted.
    fn advance_inner_past_key_group(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<bool>> {
        loop {
            let inner_batch = match &self.inner_batch {
                Some(b) => b,
                None => return Poll::Ready(Ok(true)),
            };
            let num_inner = inner_batch.num_rows();

            let group_end = find_key_group_end(
                &self.inner_key_arrays,
                self.inner_offset,
                num_inner,
                &self.sort_options,
                self.null_equality,
            )?;

            if group_end < num_inner {
                self.inner_offset = group_end;
                return Poll::Ready(Ok(false));
            }

            // Key group extends to end of batch — need to check next batch
            let saved_inner_keys = slice_keys(&self.inner_key_arrays, num_inner - 1);

            match ready!(self.poll_next_inner_batch(cx)) {
                Err(e) => return Poll::Ready(Err(e)),
                Ok(false) => {
                    return Poll::Ready(Ok(true));
                }
                Ok(true) => {
                    if keys_match(
                        &saved_inner_keys,
                        &self.inner_key_arrays,
                        &self.sort_options,
                        self.null_equality,
                    )? {
                        continue;
                    } else {
                        return Poll::Ready(Ok(false));
                    }
                }
            }
        }
    }

    /// Buffer inner key group for filter evaluation. Collects all inner rows
    /// with the current key across batch boundaries.
    ///
    /// If poll_next_inner_batch returns Pending, we save progress via
    /// buffering_inner_pending. On re-entry (from the Equal branch in
    /// poll_join), we skip clear() and the slice+push for the current
    /// batch (which was already buffered before Pending), and go directly
    /// to polling for the next inner batch.
    fn buffer_inner_key_group(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool>> {
        // On re-entry after Pending: don't clear the partially-filled
        // buffer. The current inner_batch was already sliced and pushed
        // before Pending, so jump to polling for the next batch.
        let mut resume_from_poll = false;
        if self.buffering_inner_pending {
            self.buffering_inner_pending = false;
            resume_from_poll = true;
        } else {
            self.clear_inner_key_group();
        }

        loop {
            let inner_batch = match &self.inner_batch {
                Some(b) => b,
                None => return Poll::Ready(Ok(true)),
            };
            let num_inner = inner_batch.num_rows();
            let group_end = find_key_group_end(
                &self.inner_key_arrays,
                self.inner_offset,
                num_inner,
                &self.sort_options,
                self.null_equality,
            )?;

            if !resume_from_poll {
                let slice =
                    inner_batch.slice(self.inner_offset, group_end - self.inner_offset);
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
                            datafusion_common::DataFusionError::Execution(format!(
                                "{e}. Disk spilling disabled."
                            ))
                        })?;
                    }
                }

                if group_end < num_inner {
                    self.inner_offset = group_end;
                    return Poll::Ready(Ok(false));
                }
            }
            resume_from_poll = false;

            // Key group extends to end of batch — check next
            let saved_inner_keys = slice_keys(&self.inner_key_arrays, num_inner - 1);

            // If poll returns Pending, the current batch is already
            // in inner_key_buffer.
            self.buffering_inner_pending = true;
            match ready!(self.poll_next_inner_batch(cx)) {
                Err(e) => {
                    self.buffering_inner_pending = false;
                    return Poll::Ready(Err(e));
                }
                Ok(false) => {
                    self.buffering_inner_pending = false;
                    return Poll::Ready(Ok(true));
                }
                Ok(true) => {
                    self.buffering_inner_pending = false;
                    if keys_match(
                        &saved_inner_keys,
                        &self.inner_key_arrays,
                        &self.sort_options,
                        self.null_equality,
                    )? {
                        continue;
                    } else {
                        return Poll::Ready(Ok(false));
                    }
                }
            }
        }
    }

    /// Process a key match with a filter. For each inner row in the buffered
    /// key group, evaluates the filter against the outer key group and ORs
    /// the results into the matched bitset using u64-chunked bitwise ops.
    fn process_key_match_with_filter(&mut self) -> Result<()> {
        let filter = self.filter.as_ref().unwrap();
        let outer_batch = self.outer_batch.as_ref().unwrap();
        let num_outer = outer_batch.num_rows();

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

        let outer_group_end = find_key_group_end(
            &self.outer_key_arrays,
            self.outer_offset,
            num_outer,
            &self.sort_options,
            self.null_equality,
        )?;
        let outer_group_len = outer_group_end - self.outer_offset;
        let outer_slice = outer_batch.slice(self.outer_offset, outer_group_len);

        // Count already-matched bits using popcnt on u64 chunks (zero-copy).
        let mut matched_count = UnalignedBitChunk::new(
            self.matched.as_slice(),
            self.outer_offset,
            outer_group_len,
        )
        .count_ones();

        // Process spilled inner batches first (read back from disk).
        if let Some(spill_file) = &self.inner_key_spill {
            let file = BufReader::new(File::open(spill_file.path())?);
            let reader = StreamReader::try_new(file, None)?;
            for batch_result in reader {
                let inner_slice = batch_result?;
                matched_count = eval_filter_for_inner_slice(
                    self.outer_is_left,
                    filter,
                    &outer_slice,
                    &inner_slice,
                    &mut self.matched,
                    self.outer_offset,
                    outer_group_len,
                    matched_count,
                )?;
                if matched_count == outer_group_len {
                    break;
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
                    self.outer_offset,
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

    /// Continue processing an outer key group that spans multiple outer
    /// batches. Returns `true` if this outer batch was fully consumed
    /// by the key group and the caller should load another.
    fn resume_boundary(&mut self) -> Result<bool> {
        debug_assert!(
            self.outer_batch.is_some(),
            "caller must load outer_batch first"
        );
        match self.pending_boundary.take() {
            Some(PendingBoundary::NoFilter { saved_keys }) => {
                let same_key = keys_match(
                    &saved_keys,
                    &self.outer_key_arrays,
                    &self.sort_options,
                    self.null_equality,
                )?;
                if same_key {
                    self.process_key_match_no_filter()?;
                    let num_outer = self.outer_batch.as_ref().unwrap().num_rows();
                    if self.outer_offset >= num_outer {
                        self.pending_boundary = Some(PendingBoundary::NoFilter {
                            saved_keys: slice_keys(&self.outer_key_arrays, num_outer - 1),
                        });
                        self.emit_outer_batch()?;
                        self.outer_batch = None;
                        return Ok(true);
                    }
                }
            }
            Some(PendingBoundary::Filtered { saved_keys }) => {
                debug_assert!(
                    !self.inner_key_buffer.is_empty() || self.inner_key_spill.is_some(),
                    "Filtered pending boundary entered but no inner key data exists"
                );
                let same_key = keys_match(
                    &saved_keys,
                    &self.outer_key_arrays,
                    &self.sort_options,
                    self.null_equality,
                )?;
                if same_key {
                    self.process_key_match_with_filter()?;
                    let num_outer = self.outer_batch.as_ref().unwrap().num_rows();
                    if self.outer_offset >= num_outer {
                        self.pending_boundary = Some(PendingBoundary::Filtered {
                            saved_keys: slice_keys(&self.outer_key_arrays, num_outer - 1),
                        });
                        self.emit_outer_batch()?;
                        self.outer_batch = None;
                        return Ok(true);
                    }
                }
                self.clear_inner_key_group();
            }
            None => {}
        }
        Ok(false)
    }

    /// Main loop: drive the merge-scan to produce output batches.
    fn poll_join(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<RecordBatch>>> {
        let join_time = self.join_time.clone();
        let _timer = join_time.timer();

        loop {
            // 1. Ensure we have an outer batch
            if self.outer_batch.is_none() {
                match ready!(self.poll_next_outer_batch(cx)) {
                    Err(e) => return Poll::Ready(Err(e)),
                    Ok(false) => {
                        // Outer exhausted — flush coalescer
                        self.pending_boundary = None;
                        self.coalescer.finish_buffered_batch()?;
                        if let Some(batch) = self.coalescer.next_completed_batch() {
                            return Poll::Ready(Ok(Some(batch)));
                        }
                        return Poll::Ready(Ok(None));
                    }
                    Ok(true) => {
                        if self.resume_boundary()? {
                            continue;
                        }
                    }
                }
            }

            // 2. Ensure we have an inner batch (unless inner is exhausted).
            // Skip this when resuming a pending boundary — inner was already
            // advanced past the key group before the boundary loop started.
            if self.inner_batch.is_none() && self.pending_boundary.is_none() {
                match ready!(self.poll_next_inner_batch(cx)) {
                    Err(e) => return Poll::Ready(Err(e)),
                    Ok(false) => {
                        // Inner exhausted — emit remaining outer batches.
                        // For semi: no more matches possible.
                        // For anti: all remaining outer rows are unmatched.
                        self.emit_outer_batch()?;
                        self.outer_batch = None;

                        loop {
                            match ready!(self.poll_next_outer_batch(cx)) {
                                Err(e) => return Poll::Ready(Err(e)),
                                Ok(false) => break,
                                Ok(true) => {
                                    self.emit_outer_batch()?;
                                    self.outer_batch = None;
                                }
                            }
                        }

                        self.coalescer.finish_buffered_batch()?;
                        if let Some(batch) = self.coalescer.next_completed_batch() {
                            return Poll::Ready(Ok(Some(batch)));
                        }
                        return Poll::Ready(Ok(None));
                    }
                    Ok(true) => {}
                }
            }

            // 3. Main merge-scan loop
            let outer_batch = self.outer_batch.as_ref().unwrap();
            let num_outer = outer_batch.num_rows();

            if self.outer_offset >= num_outer {
                self.emit_outer_batch()?;
                self.outer_batch = None;

                if let Some(batch) = self.coalescer.next_completed_batch() {
                    return Poll::Ready(Ok(Some(batch)));
                }
                continue;
            }

            let inner_batch = match &self.inner_batch {
                Some(b) => b,
                None => {
                    self.emit_outer_batch()?;
                    self.outer_batch = None;
                    continue;
                }
            };
            let num_inner = inner_batch.num_rows();

            if self.inner_offset >= num_inner {
                match ready!(self.poll_next_inner_batch(cx)) {
                    Err(e) => return Poll::Ready(Err(e)),
                    Ok(false) => {
                        self.inner_batch = None;
                        continue;
                    }
                    Ok(true) => continue,
                }
            }

            // 4. Compare keys at current positions
            let cmp = compare_join_arrays(
                &self.outer_key_arrays,
                self.outer_offset,
                &self.inner_key_arrays,
                self.inner_offset,
                &self.sort_options,
                self.null_equality,
            )?;

            match cmp {
                Ordering::Less => {
                    let group_end = find_key_group_end(
                        &self.outer_key_arrays,
                        self.outer_offset,
                        num_outer,
                        &self.sort_options,
                        self.null_equality,
                    )?;
                    self.outer_offset = group_end;
                }
                Ordering::Greater => {
                    let group_end = find_key_group_end(
                        &self.inner_key_arrays,
                        self.inner_offset,
                        num_inner,
                        &self.sort_options,
                        self.null_equality,
                    )?;
                    if group_end >= num_inner {
                        let saved_keys =
                            slice_keys(&self.inner_key_arrays, num_inner - 1);
                        match ready!(self.poll_next_inner_batch(cx)) {
                            Err(e) => return Poll::Ready(Err(e)),
                            Ok(false) => {
                                self.inner_batch = None;
                                continue;
                            }
                            Ok(true) => {
                                if keys_match(
                                    &saved_keys,
                                    &self.inner_key_arrays,
                                    &self.sort_options,
                                    self.null_equality,
                                )? {
                                    match ready!(self.advance_inner_past_key_group(cx)) {
                                        Err(e) => return Poll::Ready(Err(e)),
                                        Ok(_) => continue,
                                    }
                                }
                                continue;
                            }
                        }
                    } else {
                        self.inner_offset = group_end;
                    }
                }
                Ordering::Equal => {
                    if self.filter.is_some() {
                        // Buffer inner key group (may span batches)
                        match ready!(self.buffer_inner_key_group(cx)) {
                            Err(e) => return Poll::Ready(Err(e)),
                            Ok(_inner_exhausted) => {}
                        }

                        // Process outer rows against buffered inner group
                        // (may need to handle outer batch boundary)
                        loop {
                            self.process_key_match_with_filter()?;

                            let outer_batch = self.outer_batch.as_ref().unwrap();
                            if self.outer_offset >= outer_batch.num_rows() {
                                let saved_keys = slice_keys(
                                    &self.outer_key_arrays,
                                    outer_batch.num_rows() - 1,
                                );

                                self.emit_outer_batch()?;
                                debug_assert!(
                                    !self.inner_key_buffer.is_empty()
                                        || self.inner_key_spill.is_some(),
                                    "Filtered pending boundary requires inner key data in buffer or spill"
                                );
                                self.pending_boundary =
                                    Some(PendingBoundary::Filtered { saved_keys });

                                match ready!(self.poll_next_outer_batch(cx)) {
                                    Err(e) => return Poll::Ready(Err(e)),
                                    Ok(false) => {
                                        self.pending_boundary = None;
                                        self.outer_batch = None;
                                        break;
                                    }
                                    Ok(true) => {
                                        let Some(PendingBoundary::Filtered {
                                            saved_keys,
                                        }) = self.pending_boundary.take()
                                        else {
                                            unreachable!()
                                        };
                                        let same = keys_match(
                                            &saved_keys,
                                            &self.outer_key_arrays,
                                            &self.sort_options,
                                            self.null_equality,
                                        )?;
                                        if same {
                                            continue;
                                        }
                                        break;
                                    }
                                }
                            } else {
                                break;
                            }
                        }

                        self.clear_inner_key_group();
                    } else {
                        // No filter: advance inner past key group, then
                        // mark all outer rows with this key as matched.
                        match ready!(self.advance_inner_past_key_group(cx)) {
                            Err(e) => return Poll::Ready(Err(e)),
                            Ok(_inner_exhausted) => {}
                        }

                        loop {
                            self.process_key_match_no_filter()?;

                            let num_outer = self.outer_batch.as_ref().unwrap().num_rows();
                            if self.outer_offset >= num_outer {
                                let saved_keys =
                                    slice_keys(&self.outer_key_arrays, num_outer - 1);

                                self.emit_outer_batch()?;
                                self.pending_boundary =
                                    Some(PendingBoundary::NoFilter { saved_keys });

                                match ready!(self.poll_next_outer_batch(cx)) {
                                    Err(e) => return Poll::Ready(Err(e)),
                                    Ok(false) => {
                                        self.pending_boundary = None;
                                        self.outer_batch = None;
                                        break;
                                    }
                                    Ok(true) => {
                                        let Some(PendingBoundary::NoFilter {
                                            saved_keys,
                                        }) = self.pending_boundary.take()
                                        else {
                                            unreachable!()
                                        };
                                        let same_key = keys_match(
                                            &saved_keys,
                                            &self.outer_key_arrays,
                                            &self.sort_options,
                                            self.null_equality,
                                        )?;
                                        if same_key {
                                            continue;
                                        }
                                        break;
                                    }
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
            }

            // Check for completed coalescer batch
            if let Some(batch) = self.coalescer.next_completed_batch() {
                return Poll::Ready(Ok(Some(batch)));
            }
        }
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
            datafusion_common::DataFusionError::Internal(
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

impl Stream for BitwiseSortMergeJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_join(cx).map(|result| result.transpose());
        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for BitwiseSortMergeJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
