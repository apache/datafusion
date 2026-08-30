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

//! Measures the **drain phase** of `AggregateExec`: everything that happens
//! after the input is exhausted and the operator starts producing output.
//!
//! Motivated by <https://github.com/apache/datafusion/issues/24704> (blocked /
//! chunked memory management) and <https://github.com/apache/datafusion/issues/19906>
//! (long polls). Today `GroupedHashAggregateStream` drains via
//! `emit(EmitTo::All)`, materializing every group into one giant `RecordBatch`
//! that is then handed downstream as `batch.slice(..)` chunks. Two consequences
//! this benchmark is built to quantify:
//!
//! 1. **One long poll.** The whole output is built inside a single `poll_next`,
//!    so time-to-first-batch ≈ total drain time and the runtime stalls for that
//!    entire span.
//! 2. **No incremental release.** Every slice shares the giant batch's buffers,
//!    so nothing is freed until the last slice is dropped: memory held at the
//!    halfway point of the drain is ~100% of peak instead of ~50%.
//!
//! Criterion is deliberately not used: the interesting quantities are
//! within-run timings and memory samples, not a throughput distribution.
//!
//! # Metrics
//!
//! Per configuration, measured over the drain phase only (input exhausted →
//! last output batch):
//!
//! | metric | meaning |
//! |---|---|
//! | `drain_ms` | input exhausted → last output batch |
//! | `ttfb_ms` | input exhausted → first output batch |
//! | `max_gap_ms` | longest interval between consecutive output batches (the long-poll proxy); the input-exhausted → first-batch interval counts as a gap |
//! | `peak_*_mb` | maximum memory observed over the whole run (build + drain) |
//! | `at50_*_mb` | memory observed at the batch boundary where ≥50% of output rows have been emitted |
//! | `rel_%` | `at50 / first`, where `first` is memory right after the first output batch: ~100% means the drain releases nothing until it ends, ~50% means memory is released as output is produced |
//!
//! `rel_%` is anchored to the first output batch rather than to the peak: the
//! peak is reached while the hash table is still being built, and today's
//! implementations take a one-time step down from build state to materialized
//! output, which makes `at50 / peak` look like ~50% incremental release even
//! though nothing is released incrementally at all.
//!
//! Memory is reported two ways, because they disagree and the disagreement is
//! itself a finding:
//!
//! - `pool` — what the [`MemoryPool`] has reserved. This is the accounting the
//!   rest of the engine makes spill decisions from.
//! - `live` — bytes actually live on the heap, counted by a wrapping global
//!   allocator, relative to a baseline captured before the plan starts. This is
//!   what the process is really holding.
//!
//! A background sampler thread reads both every [`SAMPLE_INTERVAL`] so that
//! peaks reached *inside* a single long poll are not missed; the first-batch
//! and 50%-drained samples are taken by the driver at exact batch boundaries.
//!
//! # Shapes
//!
//! Key layout decides how much real work `emit(EmitTo::All)` does, and the
//! spread is 60x at 10M groups: flat keys drain in tens of milliseconds because
//! emit is close to a buffer move, while keys that go through arrow's row
//! format have to decode every group on the way out and stall for over a
//! second. The four defaults cover both ends; see [`KeySpec`]. Note that
//! `liststr` at 10M groups holds several GB.
//!
//! # Code paths
//!
//! Grouped aggregation is mid-migration (<https://github.com/apache/datafusion/issues/22710>).
//! With `execution.enable_migration_aggregate` on (the default) a single
//! grouping set runs on `SingleHashAggregateStream`; `--legacy` turns the flag
//! off to measure `GroupedHashAggregateStream` instead. Both drain the same
//! way — materialize every group with `EmitTo::All`, then hand out
//! `batch.slice(..)` chunks — so both are in scope for this benchmark.
//!
//! # Usage
//!
//! ```sh
//! cargo bench --bench aggregate_drain --features test_utils
//! cargo bench --bench aggregate_drain --features test_utils -- --json
//! cargo bench --bench aggregate_drain --features test_utils -- --groups 10000,1000000
//! cargo bench --bench aggregate_drain --features test_utils -- --legacy
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::fmt::{self, Formatter};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;

use arrow::array::{
    ArrayRef, DictionaryArray, Float64Array, Int64Array, ListArray, RecordBatch,
    StringArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use datafusion_common::Result;
use datafusion_common::instant::Instant;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    RecordBatchStream, ReplaceChildrenOptions,
};
use futures::StreamExt;

// ---------------------------------------------------------------------------
// Live-heap accounting
// ---------------------------------------------------------------------------

/// Bytes currently live on the heap, maintained by [`CountingAllocator`].
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Wrapping allocator that tracks live bytes.
///
/// The counters are `Relaxed`: they are read by a sampler thread that only
/// needs an approximate, monotonically-updated view, and exactness across
/// threads would cost more than the signal is worth.
struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            if new_size >= layout.size() {
                LIVE_BYTES.fetch_add(new_size - layout.size(), Ordering::Relaxed);
            } else {
                LIVE_BYTES.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn live_bytes() -> usize {
    LIVE_BYTES.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Drain probe + instrumented input
// ---------------------------------------------------------------------------

/// How often the sampler thread reads memory during the drain.
const SAMPLE_INTERVAL: Duration = Duration::from_micros(250);

/// Shared state written by the instrumented input and the sampler thread.
#[derive(Debug, Default)]
struct DrainProbe {
    /// Instant the input stream reported exhaustion, i.e. the drain begins.
    input_done: Mutex<Option<Instant>>,
    /// Live heap bytes just before the plan starts executing. Operator memory
    /// is reported relative to this.
    live_baseline: AtomicUsize,
    /// Maxima observed by the sampler thread over the whole run.
    peak_live: AtomicUsize,
    peak_pool: AtomicUsize,
}

impl DrainProbe {
    /// Called by the instrumented input when the source is exhausted.
    fn mark_input_done(&self) {
        let mut slot = self.input_done.lock().unwrap();
        if slot.is_none() {
            *slot = Some(Instant::now());
        }
    }

    fn drain_start(&self) -> Option<Instant> {
        *self.input_done.lock().unwrap()
    }

    /// Live heap bytes above the pre-execution baseline, floored at zero.
    fn live_above_baseline(&self) -> usize {
        live_bytes().saturating_sub(self.live_baseline.load(Ordering::Relaxed))
    }
}

/// Wraps an input plan to record when its stream is exhausted.
///
/// `AggregateExec` produces nothing until its input is done, so this timestamp
/// is what separates the build phase from the drain phase.
#[derive(Debug)]
struct ProbedInput {
    inner: Arc<dyn ExecutionPlan>,
    probe: Arc<DrainProbe>,
}

impl DisplayAs for ProbedInput {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "ProbedInput")
    }
}

impl ExecutionPlan for ProbedInput {
    fn name(&self) -> &str {
        "ProbedInput"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            inner: children.swap_remove(0),
            probe: Arc::clone(&self.probe),
        }))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(ProbedStream {
            inner: self.inner.execute(partition, context)?,
            probe: Arc::clone(&self.probe),
        }))
    }
}

struct ProbedStream {
    inner: SendableRecordBatchStream,
    probe: Arc<DrainProbe>,
}

impl RecordBatchStream for ProbedStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl futures::Stream for ProbedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let polled = self.inner.poll_next_unpin(cx);
        if matches!(polled, Poll::Ready(None)) {
            self.probe.mark_input_done();
        }
        polled
    }
}

// ---------------------------------------------------------------------------
// Workload shapes
// ---------------------------------------------------------------------------

/// Elements per row for the `liststr` key.
const LIST_LEN: usize = 4;

/// The group key layout.
///
/// Key layout decides which `GroupValues` implementation runs and therefore how
/// much real work `emit(EmitTo::All)` does. These four cover the range: two
/// where emit is close to a buffer move, and two that have to decode every
/// group on the way out.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum KeySpec {
    /// Single `Int64` column — the cheap floor.
    Int64,
    /// Single `Utf8` column — the common case.
    Utf8,
    /// Single `Dictionary(Int32, Utf8)` column. Unsupported by the vectorized
    /// path, so this routes to `GroupValuesRows`, whose `emit` decodes the
    /// whole table with `convert_rows`.
    Dict,
    /// Single `List(Utf8)` column. Nested types use a row-backed `GroupColumn`
    /// inside the vectorized path; this is the shape that stalls.
    ListStr,
}

impl KeySpec {
    const ALL: &'static [KeySpec] = &[
        KeySpec::Int64,
        KeySpec::Utf8,
        KeySpec::Dict,
        KeySpec::ListStr,
    ];

    fn label(&self) -> &'static str {
        match self {
            KeySpec::Int64 => "int64",
            KeySpec::Utf8 => "utf8",
            KeySpec::Dict => "dict",
            KeySpec::ListStr => "liststr",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "int64" | "int" => Some(KeySpec::Int64),
            "utf8" | "string" => Some(KeySpec::Utf8),
            "dict" => Some(KeySpec::Dict),
            "liststr" => Some(KeySpec::ListStr),
            _ => None,
        }
    }

    fn key_field(&self) -> Field {
        let data_type = match self {
            KeySpec::Int64 => DataType::Int64,
            KeySpec::Utf8 => DataType::Utf8,
            KeySpec::Dict => {
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            }
            KeySpec::ListStr => {
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)))
            }
        };
        Field::new("k", data_type, false)
    }

    /// Builds the key array for rows `start..start + len`, one distinct group
    /// per row.
    fn build_keys(&self, start: usize, len: usize) -> ArrayRef {
        let range = start..start + len;
        match self {
            KeySpec::Int64 => {
                Arc::new(Int64Array::from_iter_values(range.map(|i| i as i64)))
            }
            KeySpec::Utf8 => Arc::new(StringArray::from_iter_values(
                range.map(|i| format!("key_{i:010}")),
            )),
            KeySpec::Dict => {
                let values: Vec<String> = range.map(|i| format!("key_{i:010}")).collect();
                Arc::new(
                    values
                        .iter()
                        .map(|v| Some(v.as_str()))
                        .collect::<DictionaryArray<Int32Type>>(),
                )
            }
            KeySpec::ListStr => {
                // Collected first: `StringArray::from_iter_values` needs an
                // exact-size iterator, which `flat_map` is not.
                let items: Vec<String> = range
                    .flat_map(|i| {
                        (0..LIST_LEN)
                            .map(move |e| format!("item_{:010}", i * LIST_LEN + e))
                    })
                    .collect();
                Arc::new(ListArray::new(
                    Arc::new(Field::new("item", DataType::Utf8, false)),
                    OffsetBuffer::from_lengths((0..len).map(|_| LIST_LEN)),
                    Arc::new(StringArray::from_iter_values(items)),
                    None,
                ))
            }
        }
    }
}

/// The aggregate expressions to compute.
///
/// This matters as much as the key layout: `sum` hands its state buffer
/// straight to the output, while `avg` has to divide once per group.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum AggSpec {
    /// `sum(v)` — materialization is close to a buffer move.
    Sum,
    /// `sum(v)`, `avg(v)`, `count(v)`, `min(v)`, `max(v)`.
    Wide,
}

impl AggSpec {
    const ALL: &'static [AggSpec] = &[AggSpec::Sum, AggSpec::Wide];

    fn label(&self) -> &'static str {
        match self {
            AggSpec::Sum => "sum",
            AggSpec::Wide => "wide",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "sum" => Some(AggSpec::Sum),
            "wide" => Some(AggSpec::Wide),
            _ => None,
        }
    }

    fn build(&self, schema: &SchemaRef) -> Vec<Arc<AggregateFunctionExpr>> {
        let build = |udaf, alias: &str| {
            Arc::new(
                AggregateExprBuilder::new(udaf, vec![col("v", schema).unwrap()])
                    .schema(Arc::clone(schema))
                    .alias(alias)
                    .build()
                    .unwrap(),
            )
        };
        match self {
            AggSpec::Sum => vec![build(sum_udaf(), "sum_v")],
            AggSpec::Wide => vec![
                build(sum_udaf(), "sum_v"),
                build(avg_udaf(), "avg_v"),
                build(count_udaf(), "count_v"),
                build(min_udaf(), "min_v"),
                build(max_udaf(), "max_v"),
            ],
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Shape {
    key: KeySpec,
    agg: AggSpec,
    groups: usize,
}

fn schema(key: KeySpec) -> SchemaRef {
    Arc::new(Schema::new(vec![
        key.key_field(),
        Field::new("v", DataType::Float64, false),
    ]))
}

/// Builds input batches containing exactly `groups` distinct keys, one row per
/// group.
///
/// One row per group keeps the build phase as short as possible: this benchmark
/// is about what happens *after* the hash table is full, so the ingest side is
/// deliberately minimal.
fn build_input(key: KeySpec, groups: usize, batch_size: usize) -> Vec<RecordBatch> {
    let schema = schema(key);
    let mut batches = Vec::with_capacity(groups.div_ceil(batch_size));

    let mut start = 0;
    while start < groups {
        let len = batch_size.min(groups - start);
        let values = Arc::new(Float64Array::from_iter_values(
            (start..start + len).map(|i| (i % 1024) as f64),
        ));
        batches.push(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![key.build_keys(start, len), values],
            )
            .unwrap(),
        );
        start += len;
    }

    batches
}

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct DrainMeasurement {
    shape: Shape,
    build_ms: f64,
    drain_ms: f64,
    ttfb_ms: f64,
    max_gap_ms: f64,
    out_rows: usize,
    peak_pool_mb: f64,
    first_pool_mb: f64,
    at50_pool_mb: f64,
    peak_live_mb: f64,
    first_live_mb: f64,
    at50_live_mb: f64,
}

impl DrainMeasurement {
    /// Memory still held halfway through the drain, as a share of the memory
    /// held right after the first output batch.
    fn pool_release_pct(&self) -> f64 {
        percent(self.at50_pool_mb, self.first_pool_mb)
    }

    fn live_release_pct(&self) -> f64 {
        percent(self.at50_live_mb, self.first_live_mb)
    }
}

fn percent(part: f64, whole: f64) -> f64 {
    if whole > 0.0 {
        part / whole * 100.0
    } else {
        0.0
    }
}

const MB: f64 = 1024.0 * 1024.0;

fn to_mb(bytes: usize) -> f64 {
    bytes as f64 / MB
}

fn measure(shape: Shape, batch_size: usize, legacy: bool) -> DrainMeasurement {
    let Shape { key, agg, groups } = shape;
    let schema = schema(key);
    let batches = build_input(key, groups, batch_size);
    let probe = Arc::new(DrainProbe::default());

    let source =
        TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();
    let input: Arc<dyn ExecutionPlan> = Arc::new(ProbedInput {
        inner: source,
        probe: Arc::clone(&probe),
    });

    let group_by =
        PhysicalGroupBy::new_single(vec![(col("k", &schema).unwrap(), "k".to_string())]);
    let aggrs = agg.build(&schema);
    let filters = vec![None; aggrs.len()];

    let exec = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            aggrs,
            filters,
            input,
            Arc::clone(&schema),
        )
        .unwrap(),
    );

    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(DEFAULT_POOL_SIZE));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()
        .unwrap();
    let mut session_config = SessionConfig::new().with_batch_size(batch_size);
    if legacy {
        session_config
            .options_mut()
            .execution
            .enable_migration_aggregate = false;
    }
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(session_config)
            .with_runtime(runtime),
    );

    // Sample memory in the background so peaks reached inside a single long
    // poll are visible; the driver alone would only ever see batch boundaries.
    let sampler_stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let probe = Arc::clone(&probe);
        let pool = Arc::clone(&pool);
        let stop = Arc::clone(&sampler_stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                probe
                    .peak_live
                    .fetch_max(probe.live_above_baseline(), Ordering::Relaxed);
                probe
                    .peak_pool
                    .fetch_max(pool.reserved(), Ordering::Relaxed);
                thread::sleep(SAMPLE_INTERVAL);
            }
        })
    };

    // A current-thread runtime keeps the measurement honest: a stalled poll is
    // a stalled runtime, exactly as it would be for a tokio worker in
    // production.
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let mut out_rows = 0usize;
    let mut max_gap = Duration::ZERO;
    let mut ttfb = Duration::ZERO;
    let mut first_pool = 0usize;
    let mut first_live = 0usize;
    let mut at50_pool = 0usize;
    let mut at50_live = 0usize;
    let mut at50_taken = false;

    // Everything allocated from here on is attributable to executing the plan.
    probe.live_baseline.store(live_bytes(), Ordering::Relaxed);

    let start = Instant::now();
    let (drain_start, drain_end) = tokio_rt.block_on(async {
        let mut stream = exec.execute(0, task_ctx).unwrap();
        let mut last_event: Option<Instant> = None;

        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            let now = Instant::now();

            // The drain begins when the input reports exhaustion; the first
            // output batch cannot precede it.
            let drain_start = probe
                .drain_start()
                .expect("input must be exhausted before the aggregate emits");
            let previous = last_event.unwrap_or(drain_start);
            max_gap = max_gap.max(now.duration_since(previous));
            last_event = Some(now);

            let first = out_rows == 0;
            out_rows += batch.num_rows();

            if first {
                ttfb = now.duration_since(drain_start);
                first_pool = pool.reserved();
                first_live = probe.live_above_baseline();
            }

            if !at50_taken && out_rows * 2 >= groups {
                at50_pool = pool.reserved();
                at50_live = probe.live_above_baseline();
                at50_taken = true;
            }

            // Model a consumer that does not retain output. Under `EmitTo::All`
            // this still frees nothing: the slices handed out share the giant
            // batch's buffers, which the operator holds until the last slice.
            drop(batch);
        }

        (probe.drain_start().unwrap(), Instant::now())
    });

    sampler_stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();

    DrainMeasurement {
        shape,
        build_ms: drain_start.duration_since(start).as_secs_f64() * 1e3,
        drain_ms: drain_end.duration_since(drain_start).as_secs_f64() * 1e3,
        ttfb_ms: ttfb.as_secs_f64() * 1e3,
        max_gap_ms: max_gap.as_secs_f64() * 1e3,
        out_rows,
        peak_pool_mb: to_mb(probe.peak_pool.load(Ordering::Relaxed)),
        first_pool_mb: to_mb(first_pool),
        at50_pool_mb: to_mb(at50_pool),
        peak_live_mb: to_mb(probe.peak_live.load(Ordering::Relaxed)),
        first_live_mb: to_mb(first_live),
        at50_live_mb: to_mb(at50_live),
    }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

fn print_table(results: &[DrainMeasurement]) {
    println!(
        "\n{:>10}  {:>8}  {:>5}  {:>9}  {:>9}  {:>8}  {:>11}  {:>9}  {:>9}  {:>6}  {:>6}",
        "groups",
        "key",
        "agg",
        "build_ms",
        "drain_ms",
        "ttfb_ms",
        "max_gap_ms",
        "peak_pool",
        "peak_live",
        "pool_%",
        "live_%",
    );
    for r in results {
        println!(
            "{:>10}  {:>8}  {:>5}  {:>9.1}  {:>9.1}  {:>8.1}  {:>11.1}  {:>7.1}MB  {:>7.1}MB  {:>5.1}%  {:>5.1}%",
            r.shape.groups,
            r.shape.key.label(),
            r.shape.agg.label(),
            r.build_ms,
            r.drain_ms,
            r.ttfb_ms,
            r.max_gap_ms,
            r.peak_pool_mb,
            r.peak_live_mb,
            r.pool_release_pct(),
            r.live_release_pct(),
        );
    }
    println!(
        "\npool_% / live_% are memory at the 50%-drained mark over memory after the first"
    );
    println!(
        "batch: ~100% means nothing is released until the drain ends, ~50% means memory"
    );
    println!(
        "is released as output is produced. The two disagree because the pool does not"
    );
    println!("track the materialized output batch.");
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

const DEFAULT_GROUPS: &[usize] = &[10_000, 10_000_000];
const DEFAULT_BATCH_SIZE: usize = 8192;
/// Large enough that nothing spills; reservations are still tracked.
const DEFAULT_POOL_SIZE: usize = 64 * 1024 * 1024 * 1024;

fn parse_arg<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    let idx = args.iter().position(|a| a == name)?;
    args.get(idx + 1).map(|s| s.as_str())
}

fn parse_list<T: Copy>(
    args: &[String],
    name: &str,
    default: &[T],
    parse: impl Fn(&str) -> Option<T>,
) -> Vec<T> {
    match parse_arg(args, name) {
        None => default.to_vec(),
        Some(list) => list
            .split(',')
            .map(|item| {
                parse(item.trim())
                    .unwrap_or_else(|| panic!("{name}: unrecognized value {item}"))
            })
            .collect(),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // `cargo test --benches` runs this binary with `--test`; keep that path
    // fast so it works as a smoke test rather than a full run.
    let smoke = args.iter().any(|a| a == "--test");
    let legacy = args.iter().any(|a| a == "--legacy");

    let groups: Vec<usize> = if smoke {
        vec![1_000]
    } else {
        parse_list(&args, "--groups", DEFAULT_GROUPS, |g| g.parse().ok())
    };
    let keys = parse_list(&args, "--keys", KeySpec::ALL, KeySpec::parse);
    let aggs = parse_list(&args, "--aggs", AggSpec::ALL, AggSpec::parse);
    let batch_size = parse_arg(&args, "--batch-size")
        .map(|s| s.parse().expect("--batch-size expects an integer"))
        .unwrap_or(DEFAULT_BATCH_SIZE);

    let mut results = Vec::new();
    for &groups in &groups {
        for &key in &keys {
            for &agg in &aggs {
                eprintln!(
                    "running {groups} groups / {} keys / {} aggs ...",
                    key.label(),
                    agg.label()
                );
                results.push(measure(Shape { key, agg, groups }, batch_size, legacy));
            }
        }
    }

    if smoke {
        for r in &results {
            assert_eq!(
                r.out_rows, r.shape.groups,
                "aggregate must emit one row per group"
            );
        }
        println!("aggregate_drain smoke test ok");
        return;
    }

    print_table(&results);
}
