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

//! Partial-side Zippy-lite for `SELECT g, count(*) FROM t GROUP BY g
//! ORDER BY count(*) DESC LIMIT K`.
//!
//! # Role
//!
//! The Final side ([`GroupedTopKCountAggregateStream`]) publishes a
//! **partial-threshold hint** into the shared [`TopKCountSharedState`]
//! as soon as it has seen ≥ K groups. Any Partial aggregate reading
//! that hint can safely drop rows whose bucket's `max_count` cannot
//! grow above `hint`, because the sum-across-Partials bound
//! `N × hint < final_threshold` (with `hint = final_threshold / (N+1)`)
//! guarantees the group cannot be globally top-K.
//!
//! This directly attacks the biggest cost on ClickBench Q33: Partial's
//! main hash table growing to ~18M entries. Once the hint arrives and
//! long-tail buckets are marked dead, most future scan rows never
//! touch the main hash table.
//!
//! # Coarse Aggregate structure (Zippy §4.1.1)
//!
//! Group keys hash into `CA_BUCKETS` (8192) coarse buckets. Each
//! bucket keeps `max_count` — the largest count of any single group
//! that currently lives in the bucket. When the shared hint is
//! available, we sweep the buckets: if `max_count + remaining < hint`
//! for a bucket, no group in it can reach `hint` → mark the bucket
//! dead → future rows hashing to this bucket are skipped.
//!
//! # Correctness
//!
//! Let `N` be the number of Partial partitions and `T` the current
//! Final global top-K threshold. We publish `hint = T / (N+1)` into
//! the shared state. If Partial marks bucket B dead:
//! - All groups Y in B have local count ≤ `max_count(B) < hint`.
//! - After dead we do not increment any Y.
//! - So each Y's local count in this Partial stays `< hint`.
//! - Sum across all N Partials: `Σ local ≤ N × hint < T`.
//! - Any group `Y` with global total `≥ T` therefore had at least one
//!   Partial with local count `≥ hint` — in that Partial, the bucket
//!   was kept LIVE and Y's contribution was fully captured.
//!
//! [`GroupedTopKCountAggregateStream`]: crate::aggregates::grouped_topk_count_stream::GroupedTopKCountAggregateStream

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, Int64Builder, RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::{Result, stats::Precision};
use datafusion_execution::TaskContext;
use datafusion_expr::EmitTo;
use futures::stream::{Stream, StreamExt};

use crate::aggregates::group_values::{GroupValues, new_group_values};
use crate::aggregates::order::GroupOrdering;
use crate::aggregates::{
    AggregateExec, CA_BUCKETS, TopKCountSharedState, evaluate_group_by,
};
use crate::metrics::{BaselineMetrics, Count, MetricBuilder};
use crate::stream::EmptyRecordBatchStream;
use crate::{RecordBatchStream, SendableRecordBatchStream};

/// Sweep bucket dead-set every this-many input rows.
const BUCKET_SWEEP_INTERVAL_ROWS: u64 = 8_192;

/// Deterministic hash seed for the CA bucket index. Chosen distinct
/// from `AGGREGATION_HASH_SEED` so groups that happen to collide in the
/// main hash aggregation don't systematically co-cluster in the CA.
const CA_HASH_SEED: RandomState = RandomState::with_seed(6215194318907240811_u64);

pub struct PartialTopKCountAggregateStream {
    // --- Input ------------------------------------------------------------
    input: SendableRecordBatchStream,
    group_by: Arc<crate::aggregates::PhysicalGroupBy>,

    // --- Group-key hash ---------------------------------------------------
    group_values: Box<dyn GroupValues>,
    groups_scratch: Vec<usize>,
    /// Reused per-batch hash buffer for computing CA bucket indices.
    ca_hash_buffer: Vec<u64>,

    // --- Per-group aggregate state ---------------------------------------
    counts: Vec<i64>,
    /// Parallel to `counts`: `bucket_of_group[i]` is the CA bucket index
    /// the group hashed into when first observed.
    bucket_of_group: Vec<u16>,

    // --- CA (Zippy-style Coarse Aggregate) -------------------------------
    /// Largest single-group count currently living in each bucket. When
    /// the shared hint is available and this value + remaining rows in
    /// the partition cannot reach it, we mark the bucket dead. Held on
    /// the heap (`Vec`) because `[i64; 8192]` on the stack would
    /// overflow when the stream is constructed.
    bucket_max: Vec<i64>,
    /// One-way flag: once a bucket is dead, future rows into it are
    /// skipped entirely (no main-hash insert, no count update). Also
    /// heap-allocated (see `bucket_max`).
    bucket_dead: Vec<bool>,

    // --- Progress ---------------------------------------------------------
    rows_seen: u64,
    /// Upper bound on rows still to arrive at this Partial partition.
    /// From `input.statistics().num_rows`. `None` disables the sweep.
    total_input_rows: Option<u64>,
    rows_since_sweep: u64,
    input_done: bool,

    // --- Shared cross-op state -------------------------------------------
    shared: Arc<TopKCountSharedState>,
    /// Cached snapshot of `shared.partial_threshold_hint()`. Refreshed
    /// once per sweep. Per-row loads of an `AtomicI64` cause enough
    /// bus traffic to matter on 100M-row scans, so we cache.
    cached_hint: Option<i64>,

    // --- Output ----------------------------------------------------------
    output_schema: SchemaRef,
    emitted: bool,

    // --- Metrics ---------------------------------------------------------
    baseline_metrics: BaselineMetrics,
    partial_ca_rows_gated: Count,
    partial_ca_buckets_dead: Count,
    partial_ca_sweeps: Count,
}

impl PartialTopKCountAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        let output_schema = Arc::clone(&agg.schema);
        let group_by = Arc::clone(&agg.group_by);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        let group_schema = group_by.group_schema(&agg.input.schema())?;
        let group_ordering = GroupOrdering::None;
        let group_values = new_group_values(group_schema, &group_ordering)?;

        let shared = agg
            .count_topk_shared()
            .cloned()
            .expect("PartialTopKCountAggregateStream requires shared state");

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

        let mb = |name: &'static str| {
            MetricBuilder::new(&agg.metrics).counter(name, partition)
        };
        let partial_ca_rows_gated = mb("partial_ca_rows_gated");
        let partial_ca_buckets_dead = mb("partial_ca_buckets_dead");
        let partial_ca_sweeps = mb("partial_ca_sweeps");

        Ok(Self {
            input,
            group_by,
            group_values,
            groups_scratch: Vec::new(),
            ca_hash_buffer: Vec::new(),
            counts: Vec::new(),
            bucket_of_group: Vec::new(),
            bucket_max: vec![0; CA_BUCKETS],
            bucket_dead: vec![false; CA_BUCKETS],
            rows_seen: 0,
            total_input_rows,
            rows_since_sweep: 0,
            input_done: false,
            shared,
            cached_hint: None,
            output_schema,
            emitted: false,
            baseline_metrics,
            partial_ca_rows_gated,
            partial_ca_buckets_dead,
            partial_ca_sweeps,
        })
    }

    fn ingest(&mut self, batch: &RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // Evaluate group-by expressions.
        let group_cols_all = evaluate_group_by(&self.group_by, batch)?;
        assert_eq!(group_cols_all.len(), 1);
        let group_cols = &group_cols_all[0];

        // Compute per-row CA bucket index using a hash distinct from the
        // main aggregation hash. This lets us test bucket-dead BEFORE
        // paying the cost of `GroupValues::intern` for the row.
        self.ca_hash_buffer.clear();
        self.ca_hash_buffer.resize(num_rows, 0);
        create_hashes(group_cols, &CA_HASH_SEED, &mut self.ca_hash_buffer)?;

        // First pass: build a keep-mask by bucket_dead lookup. This is
        // O(num_rows) with a cache-friendly access pattern.
        let hint_available = self.cached_hint.is_some();
        let mut kept_indices: Vec<u32> = Vec::with_capacity(num_rows);
        if hint_available {
            let mut gated = 0u64;
            for row in 0..num_rows {
                let bkt = (self.ca_hash_buffer[row] as usize) % CA_BUCKETS;
                if self.bucket_dead[bkt] {
                    gated += 1;
                } else {
                    kept_indices.push(row as u32);
                }
            }
            if gated > 0 {
                self.partial_ca_rows_gated.add(gated as usize);
            }
        } else {
            kept_indices.extend(0..num_rows as u32);
        }

        if kept_indices.is_empty() {
            self.rows_seen = self.rows_seen.saturating_add(num_rows as u64);
            self.rows_since_sweep = self.rows_since_sweep.saturating_add(num_rows as u64);
            self.maybe_sweep();
            return Ok(());
        }

        // Take the kept rows for group-by column materialization. If we
        // kept everything, avoid the take by using the original arrays.
        let taken_cols: Vec<ArrayRef> = if kept_indices.len() == num_rows {
            group_cols.clone()
        } else {
            let indices = UInt32Array::from(kept_indices.clone());
            group_cols
                .iter()
                .map(|c| take(c.as_ref(), &indices, None))
                .collect::<std::result::Result<_, _>>()?
        };

        // Intern the kept rows only.
        self.group_values
            .intern(&taken_cols, &mut self.groups_scratch)?;

        // Fold counts + bucket_max.
        for (out_row, orig_row) in kept_indices.iter().enumerate() {
            let group_idx = self.groups_scratch[out_row];
            if group_idx >= self.counts.len() {
                debug_assert_eq!(group_idx, self.counts.len());
                self.counts.push(0);
                let bkt = (self.ca_hash_buffer[*orig_row as usize] as usize) % CA_BUCKETS;
                self.bucket_of_group.push(bkt as u16);
            }
            let new_count = self.counts[group_idx].saturating_add(1);
            self.counts[group_idx] = new_count;
            let bkt = self.bucket_of_group[group_idx] as usize;
            if new_count > self.bucket_max[bkt] {
                self.bucket_max[bkt] = new_count;
            }
        }

        self.rows_seen = self.rows_seen.saturating_add(num_rows as u64);
        self.rows_since_sweep = self.rows_since_sweep.saturating_add(num_rows as u64);
        self.maybe_sweep();
        Ok(())
    }

    fn maybe_sweep(&mut self) {
        if self.rows_since_sweep < BUCKET_SWEEP_INTERVAL_ROWS && !self.input_done {
            return;
        }
        self.sweep();
    }

    fn sweep(&mut self) {
        self.rows_since_sweep = 0;
        self.partial_ca_sweeps.add(1);
        // Refresh the hint cache from the (relaxed-atomic) shared state.
        self.cached_hint = self.shared.partial_threshold_hint();
        let Some(hint) = self.cached_hint else {
            return;
        };
        let remaining = if self.input_done {
            0
        } else {
            self.total_input_rows
                .map(|t| t.saturating_sub(self.rows_seen))
                .unwrap_or(u64::MAX)
        };
        let remaining_i64 = if remaining > i64::MAX as u64 {
            i64::MAX
        } else {
            remaining as i64
        };
        let mut new_dead = 0u64;
        for bkt in 0..CA_BUCKETS {
            if self.bucket_dead[bkt] {
                continue;
            }
            let max = self.bucket_max[bkt];
            if max.saturating_add(remaining_i64) < hint {
                self.bucket_dead[bkt] = true;
                new_dead += 1;
            }
        }
        if new_dead > 0 {
            self.partial_ca_buckets_dead.add(new_dead as usize);
        }
    }

    /// Build the output batch of `(group_cols…, partial_count)` rows.
    /// Skips groups whose bucket has been marked dead — their local
    /// counts are stale and their global contribution is provably below
    /// the top-K threshold.
    fn build_output(&mut self) -> Result<RecordBatch> {
        if self.counts.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }

        // Live indices only.
        let mut live_indices: Vec<u32> = Vec::with_capacity(self.counts.len());
        for i in 0..self.counts.len() {
            let bkt = self.bucket_of_group[i] as usize;
            if !self.bucket_dead[bkt] {
                live_indices.push(i as u32);
            }
        }

        // Materialize all group keys via GroupValues::emit, then filter.
        let group_key_cols = self.group_values.emit(EmitTo::All)?;
        let indices = UInt32Array::from(live_indices);
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(group_key_cols.len() + 1);
        for c in &group_key_cols {
            cols.push(take(c.as_ref(), &indices, None)?);
        }
        // Emit the filtered counts as an Int64 array.
        let mut count_builder = Int64Builder::with_capacity(indices.len());
        for &row in indices.values().iter() {
            count_builder.append_value(self.counts[row as usize]);
        }
        cols.push(Arc::new(count_builder.finish()));

        Ok(RecordBatch::try_new(Arc::clone(&self.output_schema), cols)?)
    }
}

impl RecordBatchStream for PartialTopKCountAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }
}

impl Stream for PartialTopKCountAggregateStream {
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
                    let schema = self.input.schema();
                    self.input = Box::pin(EmptyRecordBatchStream::new(schema));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        let _timer = elapsed.timer();
        // Final sweep — with `remaining = 0` it may mark previously-live
        // buckets dead and further reduce the emitted row count.
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
