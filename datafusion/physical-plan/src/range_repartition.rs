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

//! Range-partition an input stream on a single Int64 order-key into N
//! output partitions, with halo overlap for bounded RANGE-frame window
//! functions sitting above it.
//!
//! `execute()`'s first call spawns a coordinator that:
//!  1. opens `child.execute(k)` for every input partition `k`,
//!  2. drives each stream to its first batch (which makes the pipeline-
//!     breaking sort child populate its `PartitionExtremes` slot),
//!  3. reads `child.runtime_partition_extremes(k)` per input,
//!  4. lex-reduces those into a single global [`PartitionExtremes`], derives
//!     `N` equal-width Int64 bucket boundaries from `[global.min,
//!     global.max]`, and computes per-bucket expanded ranges by
//!     extending each primary [b_i, b_{i+1}) outward by
//!     `halo_preceding` / `halo_following`,
//!  5. then for every batch flowing out of every input stream, splits
//!     the batch into per-bucket pieces (rows whose order key lies in
//!     bucket `b`'s expanded range), and sends each piece into bucket
//!     `b`'s output channel.
//!
//! Halo rows therefore appear in *two* output partitions (their primary
//! bucket and the neighbor whose expanded range reaches them). That's
//! correct for letting the per-bucket window operator compute frame
//! values at the seams — but it also means rows are duplicated in the
//! merged output until a future `HaloDropExec` strips halo rows after
//! the window.

use std::sync::{Arc, Mutex};

use arrow::array::{Array, Int64Array, RecordBatch, UInt32Array};
use arrow::compute::take_arrays;
use arrow::datatypes::SchemaRef;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_expr_common::sort_expr::OrderingRequirements;
use futures::StreamExt;
use log::info;
use tokio::sync::{mpsc, oneshot};

use datafusion_common::ScalarValue;

use crate::sorts::sort::lex_compare;
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PartitionExtremes, PlanProperties, SendableRecordBatchStream,
};

#[derive(Debug)]
pub struct RangeRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
    /// Required input ordering — passed down from the consumer (window
    /// operator) so EnsureRequirements inserts the pipeline-breaking sort
    /// *below* us, not above. Same key feeds the routing decision.
    ordering: LexOrdering,
    /// Halo distance preceding each bucket's primary range, in
    /// leading-sort-key units. Carried over from the window frame at plan
    /// time so the coordinator can derive per-bucket expanded ranges.
    halo_preceding: i64,
    /// Halo distance following each bucket's primary range.
    halo_following: i64,
    state: Arc<Mutex<State>>,
    /// Per-output-partition primary `[lo, hi_exclusive)` ranges, filled
    /// by the coordinator before any batch is routed. Surfaced through
    /// `runtime_partition_extremes(partition)` so downstream operators
    /// (e.g. HaloDropExec) can read each bucket's intended primary
    /// range without needing the global extremes.
    bucket_primary_ranges: Arc<Mutex<Option<Vec<(i64, i64)>>>>,
}

struct State {
    initialized: bool,
    /// One `oneshot::Receiver` per output partition, populated when the
    /// coordinator hands off this partition's data. `take()`n by the
    /// corresponding `execute(partition)` call.
    handoffs: Vec<Option<oneshot::Receiver<Result<PartitionData>>>>,
}

/// Per-output-partition payload the coordinator hands to its stream.
/// Once the coordinator has computed boundaries it starts router tasks
/// that funnel routed batches into bucket-keyed mpsc channels. Each
/// output partition's stream drains its receiver.
struct PartitionData {
    rx: mpsc::Receiver<Result<RecordBatch>>,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("initialized", &self.initialized)
            .field("handoffs", &self.handoffs.len())
            .finish()
    }
}

impl RangeRepartitionExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        ordering: LexOrdering,
        halo_preceding: i64,
        halo_following: i64,
    ) -> Self {
        let n = input.output_partitioning().partition_count();
        let cache = Arc::clone(input.properties());
        Self {
            input,
            cache,
            ordering,
            halo_preceding,
            halo_following,
            state: Arc::new(Mutex::new(State {
                initialized: false,
                handoffs: (0..n).map(|_| None).collect(),
            })),
            bucket_primary_ranges: Arc::new(Mutex::new(None)),
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for RangeRepartitionExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RangeRepartitionExec")
    }
}

impl ExecutionPlan for RangeRepartitionExec {
    fn name(&self) -> &'static str {
        "RangeRepartitionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children.swap_remove(0),
            self.ordering.clone(),
            self.halo_preceding,
            self.halo_following,
        )))
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::from(self.ordering.clone()))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    /// Returns each output partition's *intended primary range* as
    /// inclusive `[min, max]` — not the actual range of routed data
    /// (which is wider, by `halo_preceding`/`halo_following`). This is a
    /// "useful lie" the downstream `HaloDropExec` consumes to filter
    /// halo rows back out.
    ///
    /// Returns `Ok(None)` if the coordinator hasn't computed boundaries
    /// yet — callers must drive the input stream to first batch before
    /// reading, per the trait contract on `runtime_partition_extremes`.
    fn runtime_partition_extremes(
        &self,
        partition: usize,
    ) -> Result<Option<PartitionExtremes>> {
        let guard = self.bucket_primary_ranges.lock().map_err(|_| {
            internal_datafusion_err!(
                "RangeRepartitionExec bucket_primary_ranges mutex poisoned"
            )
        })?;
        let Some(ranges) = guard.as_ref() else {
            return Ok(None);
        };
        let &(lo, hi_excl) = &ranges[partition];
        // Convert [lo, hi_exclusive) → inclusive [min, max].
        let max = hi_excl.saturating_sub(1);
        Ok(Some(PartitionExtremes {
            min: vec![ScalarValue::Int64(Some(lo))],
            max: vec![ScalarValue::Int64(Some(max))],
            row_count: 0, // not tracked; consumers shouldn't rely on it
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut state = self.state.lock().map_err(|_| {
            internal_datafusion_err!("RangeRepartitionExec mutex poisoned")
        })?;
        if !state.initialized {
            state.initialized = true;
            let n = state.handoffs.len();
            let mut senders = Vec::with_capacity(n);
            for slot in state.handoffs.iter_mut() {
                let (tx, rx) = oneshot::channel();
                senders.push(tx);
                *slot = Some(rx);
            }
            let child = Arc::clone(&self.input);
            let ctx = Arc::clone(&context);
            let halo_preceding = self.halo_preceding;
            let halo_following = self.halo_following;
            let primaries = Arc::clone(&self.bucket_primary_ranges);
            tokio::spawn(coordinator(
                child,
                ctx,
                senders,
                halo_preceding,
                halo_following,
                primaries,
            ));
        }
        let rx = state
            .handoffs
            .get_mut(partition)
            .and_then(Option::take)
            .ok_or_else(|| {
                internal_datafusion_err!("partition {partition} already taken")
            })?;
        drop(state);

        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            partition_stream(schema, rx),
        )))
    }
}

/// Stream that awaits the coordinator's handoff for one output partition,
/// then drains the bucket-keyed mpsc receiver router tasks are pushing
/// into. If the coordinator drops the sender (e.g. setup failed) the
/// stream surfaces an error.
fn partition_stream(
    _schema: SchemaRef,
    rx: oneshot::Receiver<Result<PartitionData>>,
) -> impl futures::Stream<Item = Result<RecordBatch>> + Send {
    use futures::stream::{TryStreamExt, once};
    once(async move {
        let data = rx
            .await
            .map_err(|_| internal_datafusion_err!("coordinator dropped"))??;
        let mut bucket_rx = data.rx;
        let inner = futures::stream::poll_fn(move |cx| bucket_rx.poll_recv(cx));
        Ok::<_, DataFusionError>(inner)
    })
    .try_flatten()
}

/// Coordinator task: drive every input partition to first batch, gather
/// runtime extremes, log the lex-reduced global, then hand off per-input
/// payloads to their corresponding output partition.
async fn coordinator(
    child: Arc<dyn ExecutionPlan>,
    ctx: Arc<TaskContext>,
    mut senders: Vec<oneshot::Sender<Result<PartitionData>>>,
    halo_preceding: i64,
    halo_following: i64,
    bucket_primary_ranges: Arc<Mutex<Option<Vec<(i64, i64)>>>>,
) {
    let n = senders.len();

    // Phase 1: open every input stream and pull the first batch from each.
    let mut firsts: Vec<(Option<RecordBatch>, SendableRecordBatchStream)> =
        Vec::with_capacity(n);
    for k in 0..n {
        let mut stream = match child.execute(k, Arc::clone(&ctx)) {
            Ok(s) => s,
            Err(e) => {
                let msg = format!("input {k} open failed: {e}");
                for tx in senders.drain(..) {
                    let _ = tx.send(Err(internal_datafusion_err!("{msg}")));
                }
                return;
            }
        };
        let first = match stream.next().await {
            Some(Ok(batch)) => Some(batch),
            Some(Err(e)) => {
                let msg = format!("first batch from input {k} failed: {e}");
                for tx in senders.drain(..) {
                    let _ = tx.send(Err(internal_datafusion_err!("{msg}")));
                }
                return;
            }
            None => None,
        };
        firsts.push((first, stream));
    }

    // Phase 2: collect per-input runtime extremes.
    let per_input: Vec<Option<PartitionExtremes>> = (0..n)
        .map(|k| child.runtime_partition_extremes(k).ok().flatten())
        .collect();

    // Phase 3: lex-reduce per-input → global, using the input's declared
    // output ordering so direction and null ordering are honored.
    let ordering: Option<LexOrdering> = child.output_ordering().cloned();
    let global = ordering
        .as_ref()
        .and_then(|o| reduce_global_extremes(&per_input, o));

    info!(
        "RangeRepartitionExec: coordinator gathered {} input partitions; \
         global extremes = {:?}",
        n, global
    );

    // Phase 4: derive bucket boundaries from the global extremes. v1 is
    // Int64-only — if anything's missing we propagate an error to all
    // output streams.
    let (lo, hi) = match int64_range(global.as_ref()) {
        Some(v) => v,
        None => {
            for tx in senders.drain(..) {
                let _ = tx.send(Err(internal_datafusion_err!(
                    "RangeRepartitionExec: leading sort key must be Int64 \
                     with a non-empty global range"
                )));
            }
            return;
        }
    };
    let Some(boundaries) = equal_width_int64_boundaries(lo, hi, n) else {
        for tx in senders.drain(..) {
            let _ = tx.send(Err(internal_datafusion_err!(
                "RangeRepartitionExec: cannot split [{lo}, {hi}] into {n} buckets"
            )));
        }
        return;
    };
    log_buckets(lo, hi, &boundaries, halo_preceding, halo_following);

    // Stash per-bucket primary ranges where `runtime_partition_extremes` can
    // see them. Done *before* any batch is routed so downstream operators
    // that gate on first batch will read populated state.
    if let Ok(mut guard) = bucket_primary_ranges.lock() {
        *guard = Some(primary_ranges_from_boundaries(lo, hi, &boundaries));
    }

    // Phase 5: figure out which column carries the leading sort key.
    let col_idx = match ordering
        .as_ref()
        .and_then(|o| o.first().expr.downcast_ref::<crate::expressions::Column>())
        .map(|c| c.index())
    {
        Some(idx) => idx,
        None => {
            for tx in senders.drain(..) {
                let _ = tx.send(Err(internal_datafusion_err!(
                    "RangeRepartitionExec: leading sort key must be a Column \
                     (got {:?})",
                    ordering
                )));
            }
            return;
        }
    };

    // Phase 6: build N output mpsc channels, one per output partition /
    // bucket. Hand each receiver to the corresponding output stream.
    let mut bucket_txs: Vec<mpsc::Sender<Result<RecordBatch>>> = Vec::with_capacity(n);
    for sender in senders.drain(..) {
        let (tx, rx) = mpsc::channel(4);
        bucket_txs.push(tx);
        let _ = sender.send(Ok(PartitionData { rx }));
    }

    // Phase 7: for each input partition, route its batches into per-bucket
    // pieces and push to the corresponding bucket_txs. Drop the local
    // bucket_txs once all router tasks complete so receivers see EOS.
    let bucket_txs = Arc::new(bucket_txs);
    let mut routers = Vec::with_capacity(firsts.len());
    for (first_batch, rest) in firsts.into_iter() {
        let txs = Arc::clone(&bucket_txs);
        let boundaries = boundaries.clone();
        routers.push(tokio::spawn(run_router(
            first_batch,
            rest,
            txs,
            boundaries,
            col_idx,
            halo_preceding,
            halo_following,
        )));
    }
    // Wait for routers so we can drop the senders here (allowing receivers
    // to observe EOS). Errors are propagated through the channels.
    for handle in routers {
        let _ = handle.await;
    }
    drop(bucket_txs);
}

/// Drain one input partition's stream and split each batch into pieces
/// keyed by the bucket whose expanded range contains the row's leading
/// sort value. Halo rows land in two buckets (their primary and the
/// neighbor that needs them as halo).
async fn run_router(
    first_batch: Option<RecordBatch>,
    mut rest: SendableRecordBatchStream,
    bucket_txs: Arc<Vec<mpsc::Sender<Result<RecordBatch>>>>,
    boundaries: Vec<i64>,
    col_idx: usize,
    halo_preceding: i64,
    halo_following: i64,
) {
    if let Some(batch) = first_batch
        && let Err(_) = route_batch(
            &batch,
            col_idx,
            &boundaries,
            halo_preceding,
            halo_following,
            &bucket_txs,
        )
        .await
    {
        return;
    }
    while let Some(item) = rest.next().await {
        let batch = match item {
            Ok(b) => b,
            Err(e) => {
                // Best-effort propagation: try each bucket sender. The
                // error is rendered to string because `DataFusionError`
                // doesn't `Clone`.
                let msg = e.to_string();
                for tx in bucket_txs.iter() {
                    let _ = tx
                        .send(Err(internal_datafusion_err!("input batch error: {msg}")))
                        .await;
                }
                return;
            }
        };
        if route_batch(
            &batch,
            col_idx,
            &boundaries,
            halo_preceding,
            halo_following,
            &bucket_txs,
        )
        .await
        .is_err()
        {
            return;
        }
    }
}

/// For each output bucket, take rows whose leading-sort-key value lands in
/// `[primary_low - halo_preceding, primary_high + halo_following)` and
/// push that piece into the bucket's channel. Bucket-driven loop so each
/// bucket's filter expression is in one place.
async fn route_batch(
    batch: &RecordBatch,
    col_idx: usize,
    boundaries: &[i64],
    halo_preceding: i64,
    halo_following: i64,
    bucket_txs: &[mpsc::Sender<Result<RecordBatch>>],
) -> Result<()> {
    let n_out = bucket_txs.len();
    let arr = batch.column(col_idx);
    let col = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        internal_datafusion_err!("RangeRepartitionExec: leading sort key not Int64")
    })?;
    let n_rows = batch.num_rows();

    for b in 0..n_out {
        let low: i64 = if b == 0 {
            i64::MIN
        } else {
            boundaries[b - 1].saturating_sub(halo_preceding)
        };
        let high: i64 = if b == n_out - 1 {
            i64::MAX
        } else {
            boundaries[b].saturating_add(halo_following)
        };
        let mut indices: Vec<u32> = Vec::new();
        for r in 0..n_rows {
            if col.is_null(r) {
                continue;
            }
            let s = col.value(r);
            if s >= low && s < high {
                indices.push(r as u32);
            }
        }
        if indices.is_empty() {
            continue;
        }
        let take_idx = UInt32Array::from(indices);
        let cols = take_arrays(batch.columns(), &take_idx, None)?;
        let piece = RecordBatch::try_new(batch.schema(), cols)?;
        if bucket_txs[b].send(Ok(piece)).await.is_err() {
            return Ok(()); // receiver dropped; bail quietly
        }
    }
    Ok(())
}

/// Log the bucket layout the coordinator will route into: the leading-key
/// global range, the interior boundaries, and each output bucket's
/// primary and halo-expanded ranges.
fn log_buckets(
    lo: i64,
    hi: i64,
    boundaries: &[i64],
    halo_preceding: i64,
    halo_following: i64,
) {
    let n = boundaries.len() + 1;
    info!(
        "RangeRepartitionExec: global leading [{lo}, {hi}] split into {n} buckets; \
         interior boundaries: {boundaries:?}; halo: {halo_preceding} preceding, \
         {halo_following} following"
    );
    let mut edges: Vec<i64> = std::iter::once(lo)
        .chain(boundaries.iter().copied())
        .chain(std::iter::once(hi + 1))
        .collect();
    edges.dedup();
    for (i, win) in edges.windows(2).enumerate() {
        let start = win[0] - halo_preceding;
        let end = win[1] + halo_following;
        info!(
            "RangeRepartitionExec:   bucket {i}: primary [{}, {})  expanded [{start}, {end})",
            win[0], win[1]
        );
    }
}

/// Extract `(lo, hi)` from the leading slot of a global [`PartitionExtremes`].
/// Returns `None` if the extremes are missing, the leading key isn't
/// Int64, or either endpoint is null.
fn int64_range(global: Option<&PartitionExtremes>) -> Option<(i64, i64)> {
    let global = global?;
    let (ScalarValue::Int64(Some(lo)), ScalarValue::Int64(Some(hi))) =
        (global.min.first()?, global.max.first()?)
    else {
        return None;
    };
    Some((*lo, *hi))
}

/// Per-bucket primary ranges as `[start, end_exclusive)` derived from
/// the global `[lo, hi]` (inclusive) and the interior cut points. Same
/// edge convention as `log_buckets`: edges = `[lo] ++ boundaries ++ [hi+1]`,
/// each bucket spans `[edges[i], edges[i+1])`.
pub fn primary_ranges_from_boundaries(
    lo: i64,
    hi: i64,
    boundaries: &[i64],
) -> Vec<(i64, i64)> {
    let mut edges: Vec<i64> = std::iter::once(lo)
        .chain(boundaries.iter().copied())
        .chain(std::iter::once(hi.saturating_add(1)))
        .collect();
    edges.dedup();
    edges.windows(2).map(|w| (w[0], w[1])).collect()
}

/// Split the closed `Int64` interval `[lo, hi]` into `n` equal-width
/// buckets, returning the `n - 1` interior cut points.
pub fn equal_width_int64_boundaries(lo: i64, hi: i64, n: usize) -> Option<Vec<i64>> {
    if n <= 1 {
        return Some(vec![]);
    }
    let span = hi.checked_sub(lo)?;
    let n_i = i64::try_from(n).ok()?;
    let mut cuts = Vec::with_capacity(n - 1);
    for i in 1..n_i {
        cuts.push(lo + span * i / n_i);
    }
    Some(cuts)
}

/// Lex-reduce per-input partition extremes into one global [`PartitionExtremes`]
/// honoring `ordering`'s direction / nulls-first per key. Returns `None`
/// when no input partition produced extremes (e.g. all inputs were empty,
/// or no upstream supports the trait method).
fn reduce_global_extremes(
    per_input: &[Option<PartitionExtremes>],
    ordering: &LexOrdering,
) -> Option<PartitionExtremes> {
    let mut iter = per_input.iter().filter_map(Option::clone);
    let mut global = iter.next()?;
    for next in iter {
        if lex_compare(&next.min, &global.min, ordering).is_lt() {
            global.min = next.min;
        }
        if lex_compare(&next.max, &global.max, ordering).is_gt() {
            global.max = next.max;
        }
        global.row_count += next.row_count;
    }
    Some(global)
}
