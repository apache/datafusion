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

//! Snapshot tests that lock down the exact sequence of memory pool calls made
//! by the grouped aggregate streams, for the memory accounting paths that the
//! other tests do not reach: spill headroom and replay, refusals with
//! temporary files disabled, refused output materialization, and holding
//! completed groups in the clustered streams. The early emit of the
//! unclustered Partial and PartialReduce streams is covered by their own tests.
//!
//! Each test runs one single-partition `AggregateExec` under a
//! [`RecordingPool`] and snapshots every pool call in order, then the query
//! outcome. The merge and replay of a spill reuse the stream's consumer, so
//! every call carries the stream's name. Limits were chosen by experiment so
//! that each test really takes its path, which the test also asserts through
//! metrics or the output batch sizes.
//!
//! The snapshots hide byte counts (see [`hide_byte_counts`]), so a change in
//! accounting shows up as a missing or extra call, a flipped refusal or
//! `can_spill`, or a different outcome. The one byte count that is a rule of
//! its own, the spill headroom, has its own test.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion_execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::aggregate::AggregateExprBuilder;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use insta::{Settings, assert_snapshot};

use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateInputMode, AggregateMode, GroupClusteringMode,
    PhysicalGroupBy,
};
use datafusion_physical_plan::common::collect;
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::test::TestMemoryExec;

/// A [`GreedyMemoryPool`] that records every call in order, as
/// `"<consumer name> <op> <bytes>"`, and the peak of its reserved bytes.
#[derive(Debug)]
struct RecordingPool {
    inner: GreedyMemoryPool,
    calls: Mutex<Vec<String>>,
    peak: AtomicUsize,
}

impl RecordingPool {
    fn new(limit: usize) -> Self {
        Self {
            inner: GreedyMemoryPool::new(limit),
            calls: Mutex::new(vec![]),
            peak: AtomicUsize::new(0),
        }
    }

    fn record(&self, consumer: &MemoryConsumer, call: &str) {
        let line = format!("{} {call}", consumer.name());
        self.calls.lock().unwrap().push(line);
    }
}

impl fmt::Display for RecordingPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordingPool({})", self.inner)
    }
}

impl MemoryPool for RecordingPool {
    fn name(&self) -> &str {
        "RecordingPool"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
        self.record(
            consumer,
            &format!("register can_spill={}", consumer.can_spill()),
        );
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
        self.record(consumer, "unregister");
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.peak
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
        self.record(reservation.consumer(), &format!("grow {additional}"));
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.record(reservation.consumer(), &format!("shrink {shrink}"));
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let result = self.inner.try_grow(reservation, additional);
        self.peak
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
        let suffix = if result.is_ok() { "" } else { " -> refused" };
        self.record(
            reservation.consumer(),
            &format!("try_grow {additional}{suffix}"),
        );
        result
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// The single aggregate of a [`Case`].
#[derive(Clone, Copy)]
enum Agg {
    /// `SUM(v)`, whose output is no larger than its accounted state.
    Sum,
    /// [`PaddedOutputUdaf`], whose output is far larger than its accounted state.
    PaddedOutput,
}

/// One aggregate run: `agg` grouped by `group_by` over raw `(a, b, v)` rows.
struct Case {
    mode: AggregateMode,
    agg: Agg,
    /// Group columns, a prefix of `["a", "b"]`.
    group_by: &'static [&'static str],
    /// Declare the input sorted by `a`, enabling group clustering.
    sorted_by_a: bool,
    /// The `(a, b)` keys of each input batch; every row has `v = 1`.
    batches: Vec<Vec<(i64, i64)>>,
    batch_size: usize,
    memory_limit: usize,
    spill_enabled: bool,
}

/// What a [`Case`] did.
struct Run {
    /// Recorded pool calls followed by the query outcome.
    snapshot: String,
    clustering: GroupClusteringMode,
    metrics: MetricsSet,
    output_batch_rows: Vec<usize>,
    /// The most bytes the aggregate held at once.
    peak: usize,
}

impl Run {
    fn metric(&self, name: &str) -> usize {
        self.metrics
            .sum_by_name(name)
            .map(|m| m.as_usize())
            .unwrap_or(0)
    }

    fn spill_count(&self) -> usize {
        self.metrics.spill_count().unwrap_or(0)
    }

    fn max_output_rows(&self) -> usize {
        self.output_batch_rows.iter().copied().max().unwrap_or(0)
    }
}

fn raw_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// Distinct keys `(start..start + n, 0)`.
fn distinct_a(start: i64, n: usize) -> Vec<(i64, i64)> {
    (start..start + n as i64).map(|a| (a, 0)).collect()
}

/// Keys `(a, start..start + n)` with one `a` value.
fn one_a(a: i64, start: i64, n: usize) -> Vec<(i64, i64)> {
    (start..start + n as i64).map(|b| (a, b)).collect()
}

impl Case {
    async fn run(self) -> Result<Run> {
        let raw = raw_schema();
        let group_by = PhysicalGroupBy::new_single(
            self.group_by
                .iter()
                .map(|name| Ok((col(name, &raw)?, name.to_string())))
                .collect::<Result<_>>()?,
        );
        let udaf = match self.agg {
            Agg::Sum => sum_udaf(),
            Agg::PaddedOutput => Arc::new(AggregateUDF::from(PaddedOutputUdaf::new())),
        };
        let aggregates = vec![Arc::new(
            AggregateExprBuilder::new(udaf, vec![col("v", &raw)?])
                .schema(Arc::clone(&raw))
                .alias("s")
                .build()?,
        )];

        // Stages after the first read partial states: the group columns followed
        // by the `SUM` state, which for Int64 is the running sum itself.
        let reads_partial_state = self.mode.input_mode() == AggregateInputMode::Partial;
        let input_schema = if reads_partial_state {
            AggregateExec::try_new(
                AggregateMode::Partial,
                group_by.clone(),
                aggregates.clone(),
                vec![None],
                Arc::new(TestMemoryExec::try_new(&[vec![]], Arc::clone(&raw), None)?),
                Arc::clone(&raw),
            )?
            .schema()
        } else {
            Arc::clone(&raw)
        };
        let batches = self
            .batches
            .iter()
            .map(|keys| {
                let a: ArrayRef =
                    Arc::new(keys.iter().map(|k| k.0).collect::<Int64Array>());
                let b: ArrayRef =
                    Arc::new(keys.iter().map(|k| k.1).collect::<Int64Array>());
                let v: ArrayRef = Arc::new(Int64Array::from(vec![1; keys.len()]));
                let columns = if reads_partial_state {
                    let mut columns = vec![a, b];
                    columns.truncate(self.group_by.len());
                    columns.push(match self.agg {
                        Agg::Sum => v,
                        Agg::PaddedOutput => {
                            Arc::new(StringArray::from(vec!["x"; keys.len()]))
                        }
                    });
                    columns
                } else {
                    vec![a, b, v]
                };
                Ok(RecordBatch::try_new(Arc::clone(&input_schema), columns)?)
            })
            .collect::<Result<Vec<_>>>()?;

        let mut input =
            TestMemoryExec::try_new(&[batches], Arc::clone(&input_schema), None)?;
        if self.sorted_by_a {
            input = input.try_with_sort_information(vec![
                LexOrdering::new([PhysicalSortExpr::new_default(col(
                    "a",
                    &input_schema,
                )?)])
                .unwrap(),
            ])?;
        }
        let aggregate = AggregateExec::try_new(
            self.mode,
            group_by,
            aggregates,
            vec![None],
            Arc::new(input),
            raw,
        )?;

        let pool = Arc::new(RecordingPool::new(self.memory_limit));
        let mut runtime =
            RuntimeEnvBuilder::new().with_memory_pool(Arc::clone(&pool) as _);
        if !self.spill_enabled {
            runtime = runtime.with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            );
        }
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(
                    SessionConfig::new()
                        .with_batch_size(self.batch_size)
                        .set_bool(
                            "datafusion.execution.enable_migration_aggregate",
                            true,
                        ),
                )
                .with_runtime(runtime.build_arc()?),
        );

        let result = match aggregate.execute(0, Arc::clone(&context)) {
            Ok(stream) => collect(stream).await,
            Err(e) => Err(e),
        };
        let output_batch_rows: Vec<usize> = result
            .iter()
            .flatten()
            .map(|batch| batch.num_rows())
            .collect();
        let outcome = match &result {
            Ok(batches) => format!(
                "Ok({} rows in {} batches)",
                output_batch_rows.iter().sum::<usize>(),
                batches.len()
            ),
            Err(e) => format!("Err({})", e.strip_backtrace()),
        };
        drop(result);
        drop(context);
        assert_eq!(pool.reserved(), 0, "every reservation is released");

        Ok(Run {
            snapshot: format!("{}\n=> {outcome}", pool.calls.lock().unwrap().join("\n")),
            clustering: aggregate.group_clustering_mode().clone(),
            metrics: aggregate.metrics().unwrap(),
            output_batch_rows,
            peak: pool.peak.load(Ordering::Relaxed),
        })
    }
}

/// Bytes of every [`PaddedOutputUdaf`] output and state value.
const PAD: usize = 256;

/// An aggregate that accounts almost no memory per group but materializes a
/// `PAD`-byte string per group as its state and its output, so materializing
/// the output can need far more memory than the table held.
#[derive(Debug, PartialEq, Eq, Hash)]
struct PaddedOutputUdaf {
    signature: Signature,
}

impl PaddedOutputUdaf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for PaddedOutputUdaf {
    fn name(&self) -> &str {
        "padded_output"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}[pad]", args.name),
            DataType::Utf8,
            true,
        ))])
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(PaddedOutputAccumulator))
    }
}

#[derive(Debug)]
struct PaddedOutputAccumulator;

impl Accumulator for PaddedOutputAccumulator {
    fn update_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Utf8(Some("x".repeat(PAD))))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Utf8(Some("x".repeat(PAD)))])
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        Ok(())
    }
}

fn case(
    mode: AggregateMode,
    group_by: &'static [&'static str],
    sorted_by_a: bool,
    batches: Vec<Vec<(i64, i64)>>,
) -> Case {
    Case {
        mode,
        agg: Agg::Sum,
        group_by,
        sorted_by_a,
        batches,
        batch_size: 64,
        memory_limit: usize::MAX,
        spill_enabled: true,
    }
}

/// Snapshot settings that replace byte counts with `[bytes]`, in the recorded
/// calls and in the sizes of an error message.
///
/// Byte counts follow the accounting of hash tables and arrays, so they change
/// with dependency upgrades, and they differ between targets: a group hash
/// table includes one control byte per bucket plus hashbrown's SIMD group
/// width, which is 16 with SSE2 (x86_64) and 8 on aarch64.
fn hide_byte_counts() -> Settings {
    let mut settings = Settings::clone_current();
    settings.add_filter(r"(grow|shrink) \d+", "$1 [bytes]");
    settings.add_filter(r"\d+\.\d [KMG]?B\b", "[bytes]");
    settings
}

/// Runs `case` as Final and as Single and returns the Final run.
///
/// The Final and Single streams account in the same way, so this asserts that
/// both make the same pool calls, with the same byte counts, once the stream
/// name is replaced, and the test snapshots only the Final run.
async fn run_final_and_single(case: impl Fn(AggregateMode) -> Case) -> Result<Run> {
    let final_run = case(AggregateMode::Final).run().await?;
    let single_run = case(AggregateMode::Single).run().await?;
    assert_eq!(
        single_run.snapshot.replace("Single", "Final"),
        final_run.snapshot,
        "Final and Single make different pool calls"
    );
    Ok(final_run)
}

/// Every key appears twice, in different spill runs.
fn hash_input_repeated() -> Vec<Vec<(i64, i64)>> {
    vec![
        distinct_a(0, 256),
        distinct_a(256, 256),
        distinct_a(512, 256),
        distinct_a(0, 256),
        distinct_a(256, 256),
        distinct_a(512, 256),
    ]
}

/// `GROUP BY a, b` input sorted by `a`: one `a` per batch, so each batch
/// completes the groups of the previous one.
fn clustered_partial_input() -> Vec<Vec<(i64, i64)>> {
    (0..4).map(|a| one_a(a, 0, 256)).collect()
}

/// `GROUP BY a, b` input sorted by `a` whose first `a` spans several batches.
fn clustered_partial_input_long_run() -> Vec<Vec<(i64, i64)>> {
    vec![
        one_a(0, 0, 256),
        one_a(0, 256, 256),
        one_a(0, 0, 256),
        one_a(0, 512, 256),
        one_a(1, 0, 64),
    ]
}

// ---------------------------------------------------------------------------
// Final, Single (GroupClusteringMode::None): spill or fail
// ---------------------------------------------------------------------------

/// Final and Single: two spills, then the merge and the replay stream run as
/// siblings of the stream's reservation (same consumer name).
#[tokio::test]
async fn spills_twice_then_replays() -> Result<()> {
    let _bound = hide_byte_counts().bind_to_scope();
    let run = run_final_and_single(|mode| Case {
        batch_size: 256,
        memory_limit: 39_000,
        ..case(mode, &["a"], false, hash_input_repeated())
    })
    .await?;
    assert_eq!(run.clustering, GroupClusteringMode::None);
    assert_eq!(run.spill_count(), 2);
    assert_snapshot!(run.snapshot, @r"
    FinalHashAggregateStream[0] register can_spill=true
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes] -> refused
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes] -> refused
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes] -> refused
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] unregister
    => Ok(768 rows in 7 batches)
    ");
    Ok(())
}

/// Final and Single with temporary files disabled: the refusal is returned as
/// an error.
#[tokio::test]
async fn without_spilling_returns_refusal() -> Result<()> {
    let _bound = hide_byte_counts().bind_to_scope();
    let run = run_final_and_single(|mode| Case {
        memory_limit: 6_000,
        spill_enabled: false,
        ..case(mode, &["a"], false, hash_input_repeated())
    })
    .await?;
    assert_eq!(run.spill_count(), 0);
    assert!(run.snapshot.contains("temporary files are not enabled"));
    assert_snapshot!(run.snapshot, @r"
    FinalHashAggregateStream[0] register can_spill=false
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes] -> refused
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] unregister
    => Err(Final hash aggregate cannot spill because temporary files are not enabled in the DiskManager
    caused by
    Resources exhausted: Failed to allocate additional [bytes] for FinalHashAggregateStream[0] with [bytes] already allocated for this reservation - [bytes] remain available for the total memory pool: greedy(used: [bytes], pool_size: [bytes]))
    ");
    Ok(())
}

/// The table fits, but growing the reservation to the materialized output
/// (`try_resize(memory_size())` on the first of several output batches) is
/// refused. The streams disagree: Final returns the error, Single hands the
/// output off unreserved (`reserved = false`, no further calls), and Partial
/// and PartialReduce ignore it (and retry on every batch). Final is the only
/// one that fails the query, although the output is already materialized,
/// which #26701 reports. This pins each behavior as it is.
///
/// Built-in aggregates materialize at most a little more than their accounted
/// state (`string_agg` does, see #26701), so this uses [`PaddedOutputUdaf`],
/// whose output is far larger.
#[tokio::test]
async fn output_materialization_refused() -> Result<()> {
    let _bound = hide_byte_counts().bind_to_scope();
    let padded = |mode| Case {
        agg: Agg::PaddedOutput,
        memory_limit: 50_000,
        ..case(mode, &["a"], false, vec![distinct_a(0, 256)])
    };

    let run = padded(AggregateMode::Final).run().await?;
    assert_eq!(run.spill_count(), 0);
    assert_snapshot!(run.snapshot, @r"
    FinalHashAggregateStream[0] register can_spill=true
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes]
    FinalHashAggregateStream[0] try_grow [bytes] -> refused
    FinalHashAggregateStream[0] shrink [bytes]
    FinalHashAggregateStream[0] unregister
    => Err(Resources exhausted: Failed to allocate additional [bytes] for FinalHashAggregateStream[0] with [bytes] already allocated for this reservation - [bytes] remain available for the total memory pool: greedy(used: [bytes], pool_size: [bytes]))
    ");

    let run = padded(AggregateMode::Single).run().await?;
    assert_eq!(run.spill_count(), 0);
    assert_eq!(run.output_batch_rows, vec![64; 4]);
    assert_snapshot!(run.snapshot, @r"
    SingleHashAggregateStream[0] register can_spill=true
    SingleHashAggregateStream[0] try_grow [bytes]
    SingleHashAggregateStream[0] try_grow [bytes]
    SingleHashAggregateStream[0] try_grow [bytes] -> refused
    SingleHashAggregateStream[0] shrink [bytes]
    SingleHashAggregateStream[0] unregister
    => Ok(256 rows in 4 batches)
    ");

    let run = padded(AggregateMode::Partial).run().await?;
    assert_eq!(run.metric("early_emit_count"), 0);
    assert_eq!(run.output_batch_rows, vec![64; 4]);
    assert_snapshot!(run.snapshot, @r"
    PartialHashAggregateStream[0] register can_spill=true
    PartialHashAggregateStream[0] try_grow [bytes]
    PartialHashAggregateStream[0] try_grow [bytes]
    PartialHashAggregateStream[0] try_grow [bytes] -> refused
    PartialHashAggregateStream[0] try_grow [bytes] -> refused
    PartialHashAggregateStream[0] try_grow [bytes] -> refused
    PartialHashAggregateStream[0] shrink [bytes]
    PartialHashAggregateStream[0] unregister
    => Ok(256 rows in 4 batches)
    ");

    let run = padded(AggregateMode::PartialReduce).run().await?;
    assert_eq!(run.metric("early_emit_count"), 0);
    assert_eq!(run.output_batch_rows, vec![64; 4]);
    assert_snapshot!(run.snapshot, @r"
    PartialReduceHashAggregateStream[0] register can_spill=true
    PartialReduceHashAggregateStream[0] try_grow [bytes]
    PartialReduceHashAggregateStream[0] try_grow [bytes]
    PartialReduceHashAggregateStream[0] try_grow [bytes] -> refused
    PartialReduceHashAggregateStream[0] try_grow [bytes] -> refused
    PartialReduceHashAggregateStream[0] try_grow [bytes] -> refused
    PartialReduceHashAggregateStream[0] shrink [bytes]
    PartialReduceHashAggregateStream[0] unregister
    => Ok(256 rows in 4 batches)
    ");
    Ok(())
}

// ---------------------------------------------------------------------------
// Clustered streams: GROUP BY a, b over input sorted by a
// (GroupClusteringMode::Partial), and GROUP BY a (GroupClusteringMode::Full)
// ---------------------------------------------------------------------------

fn clustered(mode: AggregateMode, memory_limit: usize) -> Case {
    Case {
        memory_limit,
        ..case(mode, &["a", "b"], true, clustered_partial_input())
    }
}

/// ClusteredPartial: the table fits, but the completed groups do not fit next
/// to it, so they are emitted whole.
#[tokio::test]
async fn clustered_partial_emits_completed_groups_whole() -> Result<()> {
    let _bound = hide_byte_counts().bind_to_scope();
    let run = clustered(AggregateMode::Partial, 32_000).run().await?;
    assert_eq!(run.clustering, GroupClusteringMode::Partial(vec![0]));
    assert_eq!(run.max_output_rows(), 256);
    assert_snapshot!(run.snapshot, @r"
    ClusteredPartialAggregateStream[0] register can_spill=true
    ClusteredPartialAggregateStream[0] try_grow [bytes]
    ClusteredPartialAggregateStream[0] try_grow [bytes]
    ClusteredPartialAggregateStream[0] try_grow [bytes]
    ClusteredPartialAggregateStream[0] try_grow [bytes] -> refused
    ClusteredPartialAggregateStream[0] try_grow [bytes] -> refused
    ClusteredPartialAggregateStream[0] try_grow [bytes] -> refused
    ClusteredPartialAggregateStream[0] shrink [bytes]
    ClusteredPartialAggregateStream[0] shrink [bytes]
    ClusteredPartialAggregateStream[0] unregister
    => Ok(1024 rows in 7 batches)
    ");
    Ok(())
}

/// ClusteredFinal and ClusteredSingle: completed groups held while sliced, then
/// emitted whole under a lower limit.
#[tokio::test]
async fn clustered_completed_groups() -> Result<()> {
    let _bound = hide_byte_counts().bind_to_scope();
    let run = run_final_and_single(|mode| clustered(mode, 39_000)).await?;
    assert_eq!(run.clustering, GroupClusteringMode::Partial(vec![0]));
    assert_eq!(run.spill_count(), 0);
    assert_eq!(run.max_output_rows(), 64);
    assert_snapshot!(run.snapshot, @r"
    ClusteredFinalAggregateStream[0] register can_spill=true
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] unregister
    => Ok(1024 rows in 16 batches)
    ");

    let run = run_final_and_single(|mode| clustered(mode, 34_000)).await?;
    assert_eq!(run.spill_count(), 0);
    assert_eq!(run.max_output_rows(), 256);
    assert_snapshot!(run.snapshot, @r"
    ClusteredFinalAggregateStream[0] register can_spill=true
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes] -> refused
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes] -> refused
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes] -> refused
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] unregister
    => Ok(1024 rows in 7 batches)
    ");
    Ok(())
}

/// ClusteredFinal and ClusteredSingle: one long run of `a = 0` overflows the
/// table, which spills; the spills are then merged and replayed.
#[tokio::test]
async fn clustered_spills_then_replays() -> Result<()> {
    let _bound = hide_byte_counts().bind_to_scope();
    let run = run_final_and_single(|mode| Case {
        batch_size: 256,
        memory_limit: 34_000,
        ..case(mode, &["a", "b"], true, clustered_partial_input_long_run())
    })
    .await?;
    assert_eq!(run.clustering, GroupClusteringMode::Partial(vec![0]));
    assert_eq!(run.spill_count(), 2);
    assert_snapshot!(run.snapshot, @r"
    ClusteredFinalAggregateStream[0] register can_spill=true
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes] -> refused
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes] -> refused
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] unregister
    => Ok(832 rows in 5 batches)
    ");
    Ok(())
}

/// Full clustering: a limit too small for even one group returns the refusal
/// in every clustered stream, because none of them spills under full
/// clustering and the one group never completes. ClusteredPartial emits the
/// completed groups on a refusal when there are any (#26576); ClusteredFinal
/// and ClusteredSingle return the refusal even then, which #26628 reports.
#[tokio::test]
async fn full_clustering_refusal_is_an_error() -> Result<()> {
    let _bound = hide_byte_counts().bind_to_scope();
    let one_group = |mode| Case {
        memory_limit: 6_000,
        ..case(mode, &["a"], true, vec![vec![(0, 0); 64]])
    };

    let run = one_group(AggregateMode::Partial).run().await?;
    assert_eq!(run.clustering, GroupClusteringMode::Full);
    assert_snapshot!(run.snapshot, @r"
    ClusteredPartialAggregateStream[0] register can_spill=false
    ClusteredPartialAggregateStream[0] try_grow [bytes]
    ClusteredPartialAggregateStream[0] try_grow [bytes] -> refused
    ClusteredPartialAggregateStream[0] shrink [bytes]
    ClusteredPartialAggregateStream[0] unregister
    => Err(Resources exhausted: Failed to allocate additional [bytes] for ClusteredPartialAggregateStream[0] with [bytes] already allocated for this reservation - [bytes] remain available for the total memory pool: greedy(used: [bytes], pool_size: [bytes]))
    ");

    let run = one_group(AggregateMode::Final).run().await?;
    assert_eq!(run.clustering, GroupClusteringMode::Full);
    assert_snapshot!(run.snapshot, @r"
    ClusteredFinalAggregateStream[0] register can_spill=true
    ClusteredFinalAggregateStream[0] try_grow [bytes]
    ClusteredFinalAggregateStream[0] try_grow [bytes] -> refused
    ClusteredFinalAggregateStream[0] shrink [bytes]
    ClusteredFinalAggregateStream[0] unregister
    => Err(Resources exhausted: Failed to allocate additional [bytes] for ClusteredFinalAggregateStream[0] with [bytes] already allocated for this reservation - [bytes] remain available for the total memory pool: greedy(used: [bytes], pool_size: [bytes]))
    ");

    let run = one_group(AggregateMode::Single).run().await?;
    assert_eq!(run.clustering, GroupClusteringMode::Full);
    assert_snapshot!(run.snapshot, @r"
    ClusteredSingleAggregateStream[0] register can_spill=false
    ClusteredSingleAggregateStream[0] try_grow [bytes]
    ClusteredSingleAggregateStream[0] try_grow [bytes] -> refused
    ClusteredSingleAggregateStream[0] shrink [bytes]
    ClusteredSingleAggregateStream[0] unregister
    => Err(Resources exhausted: Failed to allocate additional [bytes] for ClusteredSingleAggregateStream[0] with [bytes] already allocated for this reservation - [bytes] remain available for the total memory pool: greedy(used: [bytes], pool_size: [bytes]))
    ");
    Ok(())
}

// ---------------------------------------------------------------------------
// Spill headroom
// ---------------------------------------------------------------------------

/// A stream that can spill reserves one `u32` sort index per group on top of
/// its table. Comparing runs with temporary files enabled and disabled cancels
/// out the table, so this byte count is exact on every target.
#[tokio::test]
async fn spill_headroom_is_one_sort_index_per_group() -> Result<()> {
    let hash = |mode, spill_enabled| Case {
        spill_enabled,
        ..case(mode, &["a"], false, vec![distinct_a(0, 256)])
    };
    let clustered = |mode, spill_enabled| Case {
        spill_enabled,
        ..case(mode, &["a", "b"], true, vec![one_a(0, 0, 256)])
    };
    for (mode, clustered_input) in [
        (AggregateMode::Final, false),
        (AggregateMode::Single, false),
        (AggregateMode::Final, true),
        (AggregateMode::Single, true),
    ] {
        let input = |spill_enabled| {
            if clustered_input {
                clustered(mode, spill_enabled)
            } else {
                hash(mode, spill_enabled)
            }
        };
        let spillable = input(true).run().await?;
        let unspillable = input(false).run().await?;
        let expected_clustering = if clustered_input {
            GroupClusteringMode::Partial(vec![0])
        } else {
            GroupClusteringMode::None
        };
        assert_eq!(spillable.clustering, expected_clustering);
        assert_eq!(
            spillable.peak - unspillable.peak,
            256 * size_of::<u32>(),
            "{mode:?}, clustered: {clustered_input}"
        );
    }
    Ok(())
}
