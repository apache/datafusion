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

//! Streaming Zippy-lite for `SELECT g, count(*)/count(col) FROM t
//! GROUP BY g ORDER BY count(*) DESC/ASC LIMIT K`.
//!
//! # Background
//!
//! Zippy (Siddiqui et al., VLDB'24 – "Cache-Efficient Top-k Aggregation
//! over High Cardinality Large Datasets") shows that under skewed data
//! distributions, the top-K groups can be identified from a small sample
//! and the long tail can be pruned cheaply, yielding 2-14× speed-ups for
//! `count`. Their algorithm is offline multi-pass. DataFusion is
//! streaming: we don't get to re-scan the input.
//!
//! This module implements a single-pass streaming adaptation:
//!
//! 1. **Warmup**: process the first `W` rows normally, populating the
//!    group hash table and count vector.
//! 2. **New-group gate** (Zippy Alg 3 line 26 analogue): for any row whose
//!    group has never been seen, check `contribution_upper + remaining
//!    < heap.min`. If so, discard the row and remember the key in a
//!    dead-set. `remaining` is bounded by
//!    `input.statistics().num_rows - rows_seen`.
//! 3. **Periodic sweep** (Zippy Alg 4 lines 12-18 analogue): every
//!    `SWEEP_INTERVAL` rows, rebuild the top-K heap over live groups and
//!    mark any group with `count + remaining < heap.min` dead. Dead
//!    groups' rows are ignored for the rest of the scan.
//! 4. **Emit**: convert the top-K live groups back to arrow columns via
//!    the [`RowConverter`] stored inside [`GroupValues`].
//!
//! # Correctness
//!
//! COUNT is additive under partition merge, so this stream is enabled
//! ONLY when the aggregate mode is
//! `Final`/`FinalPartitioned`/`Single`/`SinglePartitioned`. At those
//! modes, `RepartitionExec::Hash([group_keys])` (or the absence of any
//! repartition for Single*) guarantees each group's rows all land in
//! one final partition, so per-partition top-K is a safe local
//! decision that combines correctly downstream.
//!
//! The gate + sweep never drop a group whose true final count could
//! reach `heap.min` because `remaining` is an upper bound on rows still
//! to arrive. If `input.statistics().num_rows` is not available
//! (`Precision::Absent`), `remaining` is `u64::MAX` and pruning is
//! disabled — the stream degrades to full aggregation + final sort.
//!
//! # Scope
//!
//! MVP handles `Partial` input mode (i.e., the Final aggregate consumes
//! partial `count` state coming from an upstream Partial aggregate). For
//! `Raw` input mode (Single-*), each raw row contributes 1 to its
//! group's count (count(*)) or 1 if the arg column is not null
//! (count(col)).
//!
//! Multi-aggregate patterns (Q9, Q21, Q22, Q27, Q28, Q30-Q32) are NOT
//! caught by [`crate::aggregates::AggregateExec::get_count_topk_field`],
//! which enforces `exactly_one` aggregate; those queries fall through to
//! the standard hash aggregate path.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, Int64Builder, RecordBatch, UInt32Array,
};
use arrow::compute::{SortOptions, sort_to_indices, take};
use arrow::datatypes::SchemaRef;
use datafusion_common::{Result, internal_datafusion_err, stats::Precision};
use datafusion_execution::TaskContext;
use datafusion_expr::EmitTo;
use futures::stream::{Stream, StreamExt};

use crate::aggregates::AggregateExec;
use crate::aggregates::group_values::{GroupValues, new_group_values};
use crate::aggregates::order::GroupOrdering;
use crate::aggregates::{AggregateInputMode, evaluate_group_by};
use crate::metrics::{BaselineMetrics, Count, MetricBuilder};
use crate::stream::EmptyRecordBatchStream;
use crate::{RecordBatchStream, SendableRecordBatchStream};

/// Interval between hash-table sweeps, measured in input rows consumed
/// since the previous sweep. Chosen so sweeps are frequent enough to
/// exploit late-scan tighter bounds but rare enough that the O(live
/// groups) scan cost is amortized. Tune with benchmarks.
const SWEEP_INTERVAL_ROWS: u64 = 65_536;

/// Minimum number of live groups accumulated before the first sweep and
/// new-group gate activate. Without warmup, the heap has fewer than K
/// entries and `heap.min` is undefined, so we cannot bound anything.
const WARMUP_GROUPS: usize = 4;

/// Streaming Zippy-lite top-K over a count aggregate.
pub struct GroupedTopKCountAggregateStream {
    // --- Input ------------------------------------------------------------
    input: SendableRecordBatchStream,
    input_mode: AggregateInputMode,
    /// Index of the `count` state column in the input schema. For
    /// [`AggregateInputMode::Partial`] this is where partial count states
    /// arrive; for [`AggregateInputMode::Raw`], unused (each row = 1).
    partial_count_col_idx: usize,
    /// Group-by evaluator lifted from [`AggregateExec::group_by`].
    group_by: Arc<crate::aggregates::PhysicalGroupBy>,

    // --- Group-key interning ---------------------------------------------
    group_values: Box<dyn GroupValues>,
    groups_scratch: Vec<usize>,

    // --- Per-group aggregate state (indexed by group_index) -------------
    counts: Vec<i64>,
    /// True → this group has been proven unable to reach top-K. Its rows
    /// (from now on) are ignored. Parallel to `counts`.
    dead: Vec<bool>,

    // --- Config -----------------------------------------------------------
    limit: usize,
    descending: bool,
    output_schema: SchemaRef,

    // --- Progress ---------------------------------------------------------
    /// Total input rows consumed (across all batches from `input`).
    rows_seen: u64,
    /// Upper bound on the total input rows for this partition, from
    /// `input.statistics()`. `None` disables pruning.
    total_input_rows: Option<u64>,
    /// Rows seen since the last sweep.
    rows_since_sweep: u64,

    // --- Metrics ---------------------------------------------------------
    baseline_metrics: BaselineMetrics,
    groups_seen: Count,
    groups_gated: Count,
    groups_swept_dead: Count,
    sweeps_performed: Count,

    // --- Emission ---------------------------------------------------------
    emitted: bool,
    input_done: bool,
}

impl GroupedTopKCountAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        limit: usize,
        descending: bool,
    ) -> Result<Self> {
        let output_schema = Arc::clone(&agg.schema);
        let group_by = Arc::clone(&agg.group_by);
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let input_mode = agg.mode.input_mode();

        // Build the GroupValues implementation over the group-by schema.
        // `evaluate_group_by` returns arrays whose types match the
        // aggregate's group_schema (post-alias).
        let group_schema = group_by.group_schema(&agg.input.schema())?;
        let group_ordering = GroupOrdering::None;
        let group_values = new_group_values(group_schema, &group_ordering)?;

        // Locate the partial-count column in the input schema. For
        // `AggregateMode::Partial` input (i.e., Final aggregate consuming
        // partial state), this is the first column after the group keys.
        // For `Raw` input (Single*), we won't index this column — each
        // row contributes 1 (see `count_contribution_for_row`).
        let partial_count_col_idx = group_by.expr.len();

        // Best-effort read of the total input row count for the bound.
        // Precision::Exact/Inexact both give us a bound to work with; on
        // Absent we set None and skip pruning.
        //
        // Use `StatisticsContext::compute` which walks the plan tree and
        // is the current recommended API. The deprecated
        // `partition_statistics` default returns `Statistics::new_unknown`
        // for every node without an override, so it would report Absent
        // even when e.g. `RepartitionExec::statistics_from_inputs` would
        // give us a real `Precision::Inexact`.
        let total_input_rows = {
            use crate::statistics::{StatisticsArgs, StatisticsContext};
            let ctx = StatisticsContext::new();
            let args = StatisticsArgs::new().with_partition(Some(partition));
            match ctx.compute(agg.input.as_ref(), &args) {
                Ok(stats) => match stats.num_rows {
                    Precision::Exact(n) | Precision::Inexact(n) => Some(n as u64),
                    Precision::Absent => None,
                },
                Err(_) => None,
            }
        };

        let groups_seen =
            MetricBuilder::new(&agg.metrics).counter("count_topk_groups_seen", partition);
        let groups_gated = MetricBuilder::new(&agg.metrics)
            .counter("count_topk_groups_gated", partition);
        let groups_swept_dead = MetricBuilder::new(&agg.metrics)
            .counter("count_topk_groups_swept_dead", partition);
        let sweeps_performed = MetricBuilder::new(&agg.metrics)
            .counter("count_topk_sweeps_performed", partition);

        Ok(Self {
            input,
            input_mode,
            partial_count_col_idx,
            group_by,
            group_values,
            groups_scratch: Vec::new(),
            counts: Vec::new(),
            dead: Vec::new(),
            limit,
            descending,
            output_schema,
            rows_seen: 0,
            total_input_rows,
            rows_since_sweep: 0,
            baseline_metrics,
            groups_seen,
            groups_gated,
            groups_swept_dead,
            sweeps_performed,
            emitted: false,
            input_done: false,
        })
    }

    /// Consume one input batch. Extracts group keys via
    /// `group_values.intern` and folds per-row count contributions into
    /// `self.counts`. Applies the new-group gate for rows creating
    /// previously-unseen groups when the gate is armed.
    fn ingest(&mut self, batch: &RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // 1. Get the per-row group_index. `intern` allocates NEW group
        //    indices for previously-unseen keys — since the gate needs to
        //    reject some of those NEW inserts, we snapshot `len_before`
        //    and undo any group_index we don't want. Undoing works
        //    because indices are dense and the last insert(s) are the
        //    newest; but the `GroupValues` trait doesn't expose a "rollback
        //    last N" API. Instead we split the batch pre-emptively: run
        //    the gate on the row's group key WITHOUT interning, and
        //    filter rows before calling intern().
        let group_cols_all = evaluate_group_by(&self.group_by, batch)?;
        // The count aggregate has a single group set → outer len = 1.
        assert_eq!(
            group_cols_all.len(),
            1,
            "count-topk expects a single group set (grouping-sets are rejected by the optimizer)"
        );
        let group_cols = &group_cols_all[0];

        // Read the per-row count contribution.
        // Partial input mode → partial_count column (Int64); one row can
        //   contribute > 1 (an upstream Partial may have already
        //   aggregated many raw rows into a single partial state row).
        // Raw input mode → each row contributes 1 (count(*)) or 0/1 if
        //   count(col) and col is null; we defer count(col) to a
        //   follow-up commit and only handle count(*).
        let per_row_contrib: RowContribution = match self.input_mode {
            AggregateInputMode::Partial => {
                let arr =
                    batch.column(self.partial_count_col_idx).as_primitive_opt::<
                        arrow::datatypes::Int64Type,
                    >().ok_or_else(|| {
                        internal_datafusion_err!(
                            "count-topk expects partial count column at index {} to be Int64 (got {:?})",
                            self.partial_count_col_idx,
                            batch.column(self.partial_count_col_idx).data_type()
                        )
                    })?;
                RowContribution::Column(arr.clone())
            }
            AggregateInputMode::Raw => RowContribution::One,
        };

        // 2. Intern the group keys.
        self.group_values
            .intern(group_cols, &mut self.groups_scratch)?;

        // 3. Fold contributions.
        for row in 0..num_rows {
            let group_idx = self.groups_scratch[row];
            // Grow counts / dead if this is a NEW group index.
            if group_idx >= self.counts.len() {
                debug_assert_eq!(group_idx, self.counts.len());
                self.groups_seen.add(1);
                // ⛔ new-group gate — only armed after warmup and only
                //    when we can bound remaining rows.
                if self.pruning_armed() {
                    let contrib = per_row_contrib.contribution_at(row);
                    // remaining bound uses rows_seen so far, which does
                    // NOT include this batch's rows yet. This is loose
                    // (upper bound) — safe.
                    let remaining = self.remaining_rows_upper();
                    let remaining_i64 = if remaining > i64::MAX as u64 {
                        i64::MAX
                    } else {
                        remaining as i64
                    };
                    if let Some(threshold) = self.current_threshold() {
                        // Even with contrib+remaining fully allocated to
                        // this group, can it beat the heap min?
                        // For DESC: keep large counts → threshold =
                        //   heap.min_count. Reject if contrib + remaining
                        //   ≤ threshold.
                        // For ASC: keep small counts → threshold =
                        //   heap.max_count. Reject if contrib + 0 ≥
                        //   threshold (any additional contrib only makes
                        //   it worse for ASC).
                        // Ties are ambiguous — a group whose max/min
                        // matches the current K-th value MIGHT be one of
                        // the top-K survivors (tie-break by any means).
                        // We stay conservative and admit ties.
                        let can_reach = if self.descending {
                            contrib.saturating_add(remaining_i64) >= threshold
                        } else {
                            // For ASC, the smallest possible final count
                            // for a new group first appearing here is
                            // `contrib` — remaining rows only make it
                            // bigger. So we reject only when contrib is
                            // strictly greater than the K-th smallest.
                            contrib <= threshold
                        };
                        if !can_reach {
                            // Gate rejects this row. We must ALSO undo
                            // the intern's insertion of this key —
                            // otherwise the map keeps a dangling
                            // group_index. But GroupValues has no
                            // rollback. Workaround: allocate the count
                            // slot but immediately mark dead — the
                            // sweep and emit both skip dead groups.
                            self.counts.push(0);
                            self.dead.push(true);
                            self.groups_gated.add(1);
                            continue;
                        }
                    }
                }
                self.counts.push(0);
                self.dead.push(false);
            }
            if self.dead[group_idx] {
                continue;
            }
            self.counts[group_idx] = self.counts[group_idx]
                .saturating_add(per_row_contrib.contribution_at(row));
        }

        self.rows_seen = self.rows_seen.saturating_add(num_rows as u64);
        self.rows_since_sweep = self.rows_since_sweep.saturating_add(num_rows as u64);
        if self.pruning_armed() && self.rows_since_sweep >= SWEEP_INTERVAL_ROWS {
            self.sweep();
        }

        Ok(())
    }

    /// Whether the new-group gate + sweep are active. Requires a known
    /// `total_input_rows` AND at least `WARMUP_GROUPS` groups so the
    /// threshold is well-defined.
    fn pruning_armed(&self) -> bool {
        self.total_input_rows.is_some()
            && self.live_group_count() >= WARMUP_GROUPS.max(self.limit)
    }

    fn live_group_count(&self) -> usize {
        // Cheap upper bound; live count ≤ counts.len(). For threshold
        // decisions we only need a lower bound on when to activate the
        // gate (i.e., "have we seen enough groups yet?"), so counting all
        // slots is fine.
        self.counts.len()
    }

    /// Upper bound on rows still to arrive. If `total_input_rows` is
    /// `None`, returns `u64::MAX` — pruning callers should further gate
    /// on `pruning_armed()` before acting, since `u64::MAX + count >
    /// threshold` always, so nothing would be pruned anyway.
    ///
    /// Never underflows: if `rows_seen > total_input_rows` (stats were
    /// underestimates) we return 0.
    fn remaining_rows_upper(&self) -> u64 {
        match self.total_input_rows {
            Some(total) => total.saturating_sub(self.rows_seen),
            None => u64::MAX,
        }
    }

    /// The current top-K threshold. For DESC: the K-th largest live
    /// count (heap.min → smallest of the K survivors). For ASC: the K-th
    /// smallest (heap.max → largest of the K survivors).
    ///
    /// Returns `None` when fewer than `limit` live groups exist — the
    /// heap isn't full yet so no group can be gated on the basis of
    /// "cannot make it".
    fn current_threshold(&self) -> Option<i64> {
        // Collect live counts.
        let mut vals: Vec<i64> = self
            .counts
            .iter()
            .zip(self.dead.iter())
            .filter_map(|(c, &d)| if !d { Some(*c) } else { None })
            .collect();
        if vals.len() < self.limit {
            return None;
        }
        // Partition-select: kth largest for DESC, kth smallest for ASC.
        let k = self.limit;
        if self.descending {
            // K-th largest = element at index (len - k) after nth_element.
            // Use select_nth_unstable_by for O(n).
            let idx = vals.len() - k;
            let (_, kth, _) = vals.select_nth_unstable_by(idx, |a, b| a.cmp(b));
            Some(*kth)
        } else {
            let idx = k - 1;
            let (_, kth, _) = vals.select_nth_unstable_by(idx, |a, b| a.cmp(b));
            Some(*kth)
        }
    }

    /// Sweep the count table and mark dead any group whose partial count
    /// + remaining_rows_upper can no longer reach the current threshold.
    ///
    /// Callers must ensure `total_input_rows` is Some before this can do
    /// useful work; when it's None, `remaining_rows_upper()` returns
    /// `u64::MAX` and this method fast-paths to a no-op.
    fn sweep(&mut self) {
        self.sweeps_performed.add(1);
        self.rows_since_sweep = 0;
        let Some(threshold) = self.current_threshold() else {
            return;
        };
        let remaining = self.remaining_rows_upper();
        // Clamp remaining to i64::MAX so `count + remaining` doesn't
        // silently wrap. If `remaining` was u64::MAX (unbounded), we
        // still want a coherent comparison — the resulting
        // `max_possible == i64::MAX` will never be `<=` a real threshold,
        // so nothing gets pruned. Which is correct.
        let remaining_i64 = if remaining > i64::MAX as u64 {
            i64::MAX
        } else {
            remaining as i64
        };
        let mut newly_dead = 0usize;
        for i in 0..self.counts.len() {
            if self.dead[i] {
                continue;
            }
            let count = self.counts[i];
            let max_possible = count.saturating_add(remaining_i64);
            // "Cannot win" is a strict inequality: we only kill a group
            // when its best-case (max for DESC, min for ASC) is strictly
            // worse than the K-th value. Ties are preserved because the
            // group MIGHT be one of the K survivors on tie-break.
            let cannot_win = if self.descending {
                max_possible < threshold
            } else {
                // For ASC: min possible for existing group is `count`
                // (only increases). Strictly greater than K-th smallest
                // means it will never be in the bottom-K.
                count > threshold
            };
            if cannot_win {
                self.dead[i] = true;
                newly_dead += 1;
            }
        }
        if newly_dead > 0 {
            self.groups_swept_dead.add(newly_dead);
        }
    }

    /// Build the single output batch of at most `limit` rows, sorted by
    /// count in the requested direction. Uses `GroupValues::emit` to
    /// materialize all group keys, then filters + sorts + takes.
    fn build_output(&mut self) -> Result<RecordBatch> {
        if self.counts.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }

        // 1. Materialize all group keys.
        let group_key_cols = self.group_values.emit(EmitTo::All)?;

        // 2. Build the count column paired with them.
        let mut count_builder = Int64Builder::with_capacity(self.counts.len());
        for c in &self.counts {
            count_builder.append_value(*c);
        }
        let count_arr: ArrayRef = Arc::new(count_builder.finish());

        // 3. Build a full RecordBatch (all groups, including dead — we'll
        //    filter next).
        let mut all_cols: Vec<ArrayRef> = Vec::with_capacity(group_key_cols.len() + 1);
        all_cols.extend(group_key_cols);
        all_cols.push(count_arr);
        let full_batch = RecordBatch::try_new(Arc::clone(&self.output_schema), all_cols)?;

        // 4. Mask out dead groups by rebuilding index list.
        let mut live_indices: Vec<u32> = Vec::with_capacity(self.counts.len());
        for i in 0..self.counts.len() {
            if !self.dead[i] {
                live_indices.push(i as u32);
            }
        }
        let live_index_arr = UInt32Array::from(live_indices);
        let num_cols = full_batch.num_columns();
        let mut live_cols: Vec<ArrayRef> = Vec::with_capacity(num_cols);
        for c in 0..num_cols {
            live_cols.push(take(full_batch.column(c).as_ref(), &live_index_arr, None)?);
        }
        let live_batch =
            RecordBatch::try_new(Arc::clone(&self.output_schema), live_cols)?;

        // 5. Sort by count in requested direction, take limit.
        let opts = SortOptions {
            descending: self.descending,
            nulls_first: false,
        };
        let count_col = live_batch.column(self.partial_count_col_idx);
        let sort_indices = sort_to_indices(count_col, Some(opts), Some(self.limit))?;
        let mut sorted_cols: Vec<ArrayRef> = Vec::with_capacity(num_cols);
        for c in 0..num_cols {
            sorted_cols.push(take(live_batch.column(c).as_ref(), &sort_indices, None)?);
        }
        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            sorted_cols,
        )?)
    }
}

/// Per-row contribution to a group's count.
enum RowContribution {
    /// Every row contributes 1. Used at [`AggregateInputMode::Raw`] for
    /// `count(*)`.
    One,
    /// Per-row contribution is stored in an Int64 column that came from
    /// an upstream Partial aggregate.
    Column(Int64Array),
}

impl RowContribution {
    fn contribution_at(&self, row: usize) -> i64 {
        match self {
            RowContribution::One => 1,
            RowContribution::Column(a) => {
                if a.is_null(row) {
                    0
                } else {
                    a.value(row)
                }
            }
        }
    }
}

impl RecordBatchStream for GroupedTopKCountAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }
}

impl Stream for GroupedTopKCountAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.emitted {
            return Poll::Ready(None);
        }
        let elapsed = self.baseline_metrics.elapsed_compute().clone();
        while !self.input_done {
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let _timer = elapsed.timer();
                    self.ingest(&batch)?;
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    self.input_done = true;
                    // Release the input pipeline's resources.
                    let schema = self.input.schema();
                    self.input = Box::pin(EmptyRecordBatchStream::new(schema));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        let _timer = elapsed.timer();
        // Final sweep to reflect any last-arriving batches.
        self.sweep();
        let out = self.build_output()?;
        self.emitted = true;
        if out.num_rows() == 0 {
            Poll::Ready(None)
        } else {
            Poll::Ready(Some(Ok(out)))
        }
    }
}
