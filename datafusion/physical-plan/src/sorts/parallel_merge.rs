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

//! [`ParallelSortPreservingMergeExec`] merges sorted partitions in parallel.
//!
//! Unlike [`SortPreservingMergeExec`], which performs a single-threaded k-way
//! merge of all input partitions on one thread, this operator splits the merge
//! work across multiple threads using the **Parallel Sorting by Regular
//! Sampling (PSRS)** strategy of Shi & Schaeffer (1992), combined with the
//! merge-path / co-rank splitter idea (Green, McColl & Bader, 2012):
//!
//! 1. Each input partition is already locally sorted (by an upstream
//!    `SortExec`). We materialize the runs and encode their sort keys with a
//!    single shared [`RowConverter`] so all keys are byte-comparable.
//! 2. We draw a regular sample of keys across all runs, sort the sample, and
//!    pick `P - 1` *pivots* (split points) that partition the key space into
//!    `P` ranges.
//! 3. Every run is partitioned by the *same* pivots via binary search
//!    (`lower_bound`). Range `s` is the `s`-th slice taken from each run.
//! 4. The `P` ranges ("buckets") are merged independently and concurrently,
//!    each by the existing optimized [`StreamingMergeBuilder`] k-way merge.
//! 5. Because every run is cut by the same pivot values, bucket `s` contains
//!    exactly the keys in `[pivot_{s-1}, pivot_s)` across all runs, so the
//!    buckets are totally ordered. Emitting bucket 0, then 1, ... then `P - 1`
//!    yields a single globally sorted stream — the same output contract as
//!    [`SortPreservingMergeExec`].
//!
//! Correctness does not depend on the pivots being balanced: any pivots produce
//! correctly sorted output. Balance only affects how evenly work is spread
//! across threads. Regular sampling gives each bucket at most `~2 * R / P` rows
//! (the classic PSRS load-balance bound) for high-cardinality keys; very
//! low-cardinality keys may yield empty buckets and limited parallelism.
//!
//! [`SortPreservingMergeExec`]: crate::sorts::sort_preserving_merge::SortPreservingMergeExec
//! [`StreamingMergeBuilder`]: crate::sorts::streaming_merge::StreamingMergeBuilder

use std::sync::Arc;

use crate::common::spawn_buffered;
use crate::execution_plan::{CardinalityEffect, EmissionType};
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::sorts::streaming_merge::StreamingMergeBuilder;
use crate::statistics::StatisticsArgs;
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PhysicalExpr, PlanProperties, SendableRecordBatchStream, Statistics,
    check_if_same_properties,
};

use crate::execution_plan::{EvaluationType, SchedulingType};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use datafusion_common::utils::memory::get_record_batch_memory_size;
use datafusion_common::{
    DataFusionError, Result, assert_eq_or_internal_err, internal_err,
};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, OrderingRequirements};
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;

use futures::stream::{self, StreamExt, TryStreamExt};
use log::trace;

/// Number of key samples drawn per output bucket when selecting pivots.
///
/// Oversampling (drawing more samples than strictly necessary) tightens the
/// load balance of the resulting buckets. The total number of samples is
/// `OVERSAMPLE_PER_BUCKET * num_buckets`, bounded by the total row count.
const OVERSAMPLE_PER_BUCKET: usize = 50;

/// Parallel order-preserving merge of sorted input partitions.
///
/// See the [module-level documentation](self) for the algorithm. This operator
/// produces a single, globally sorted output partition, exactly like
/// [`SortPreservingMergeExec`](crate::sorts::sort_preserving_merge::SortPreservingMergeExec),
/// but computes it with `target_partitions`-way parallelism. It materializes
/// all of its input (it is pipeline-breaking) and therefore does not support a
/// `fetch`/limit; callers needing early termination should use
/// `SortPreservingMergeExec`.
#[derive(Debug, Clone)]
pub struct ParallelSortPreservingMergeExec {
    /// Input plan with sorted partitions
    input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: LexOrdering,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: Arc<PlanProperties>,
    /// Use round-robin selection of tied winners of the loser tree in each
    /// bucket merge. See
    /// [`SortPreservingMergeExec::with_round_robin_repartition`].
    ///
    /// [`SortPreservingMergeExec::with_round_robin_repartition`]: crate::sorts::sort_preserving_merge::SortPreservingMergeExec::with_round_robin_repartition
    enable_round_robin_repartition: bool,
    /// Target number of parallel merge buckets. `None` means use the session's
    /// `target_partitions`.
    target_buckets: Option<usize>,
}

impl ParallelSortPreservingMergeExec {
    /// Create a new parallel sort preserving merge execution plan
    pub fn new(expr: LexOrdering, input: Arc<dyn ExecutionPlan>) -> Self {
        let cache = Self::compute_properties(&input, expr.clone());
        Self {
            input,
            expr,
            metrics: ExecutionPlanMetricsSet::new(),
            cache: Arc::new(cache),
            enable_round_robin_repartition: true,
            target_buckets: None,
        }
    }

    /// Sets the selection strategy of tied winners of the loser tree algorithm,
    /// applied within each bucket merge. See
    /// [`SortPreservingMergeExec::with_round_robin_repartition`].
    ///
    /// [`SortPreservingMergeExec::with_round_robin_repartition`]: crate::sorts::sort_preserving_merge::SortPreservingMergeExec::with_round_robin_repartition
    pub fn with_round_robin_repartition(
        mut self,
        enable_round_robin_repartition: bool,
    ) -> Self {
        self.enable_round_robin_repartition = enable_round_robin_repartition;
        self
    }

    /// Override the target number of parallel merge buckets. By default the
    /// session's `target_partitions` is used.
    pub fn with_target_buckets(mut self, target_buckets: usize) -> Self {
        self.target_buckets = Some(target_buckets);
        self
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Sort expressions
    pub fn expr(&self) -> &LexOrdering {
        &self.expr
    }

    /// Fast-path child replacement used by [`check_if_same_properties!`] when
    /// the new child preserves this operator's properties.
    fn with_new_children_and_same_properties(
        &self,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        Self {
            input: children.swap_remove(0),
            metrics: ExecutionPlanMetricsSet::new(),
            ..Self::clone(self)
        }
    }

    /// Creates the cache object that stores the plan properties such as schema,
    /// equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        ordering: LexOrdering,
    ) -> PlanProperties {
        // With a single input partition `execute` passes the input through
        // unchanged, so we inherit its pipeline behavior and scheduling. With
        // multiple partitions we materialize all input before emitting, so the
        // operator is pipeline-breaking (`Final`) and eagerly driven.
        let input_partitions = input.output_partitioning().partition_count();
        let (emission, evaluation, scheduling) = if input_partitions > 1 {
            (
                EmissionType::Final,
                EvaluationType::Eager,
                SchedulingType::Cooperative,
            )
        } else {
            (
                input.pipeline_behavior(),
                input.properties().evaluation_type,
                input.properties().scheduling_type,
            )
        };

        let mut eq_properties = input.equivalence_properties().clone();
        eq_properties.clear_per_partition_constants();
        eq_properties.add_ordering(ordering);
        PlanProperties::new(
            eq_properties,                        // Equivalence Properties
            Partitioning::UnknownPartitioning(1), // Output Partitioning
            emission,                             // Pipeline behavior
            input.boundedness(),                  // Boundedness
        )
        .with_evaluation_type(evaluation)
        .with_scheduling_type(scheduling)
    }
}

impl DisplayAs for ParallelSortPreservingMergeExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ParallelSortPreservingMergeExec: [{}]", self.expr)
            }
            DisplayFormatType::TreeRender => {
                for (i, e) in self.expr().iter().enumerate() {
                    e.fmt_sql(f)?;
                    if i != self.expr().len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for ParallelSortPreservingMergeExec {
    fn name(&self) -> &'static str {
        "ParallelSortPreservingMergeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::from(self.expr.clone()))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        check_if_same_properties!(self, children);
        let mut exec = ParallelSortPreservingMergeExec::new(
            self.expr.clone(),
            children.swap_remove(0),
        )
        .with_round_robin_repartition(self.enable_round_robin_repartition);
        exec.target_buckets = self.target_buckets;
        Ok(Arc::new(exec))
    }

    /// The parallel merge materializes all of its input and cannot stop early,
    /// so it never accepts a pushed-down fetch/limit. Returning `None` lets the
    /// limit-pushdown rule keep a `LimitExec` above this operator.
    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start ParallelSortPreservingMergeExec::execute for partition: {partition}"
        );
        assert_eq_or_internal_err!(
            partition,
            0,
            "ParallelSortPreservingMergeExec invalid partition {partition}"
        );

        let input_partitions = self.input.output_partitioning().partition_count();
        let schema = self.schema();

        match input_partitions {
            0 => {
                return internal_err!(
                    "ParallelSortPreservingMergeExec requires at least one input partition"
                );
            }
            // A single sorted input partition is already globally sorted.
            1 => return self.input.execute(0, context),
            _ => {}
        }

        // Run each input partition on its own task so the upstream sorts proceed
        // in parallel while we drain them.
        let input_streams = (0..input_partitions)
            .map(|p| {
                Ok(spawn_buffered(
                    self.input.execute(p, Arc::clone(&context))?,
                    2,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let target_buckets = self
            .target_buckets
            .unwrap_or_else(|| context.session_config().target_partitions());

        let cfg = MergeConfig {
            schema: Arc::clone(&schema),
            expr: self.expr.clone(),
            metrics: self.metrics.clone(),
            batch_size: context.session_config().batch_size(),
            target_buckets,
            enable_round_robin_repartition: self.enable_round_robin_repartition,
            memory_pool: Arc::clone(&context.runtime_env().memory_pool),
            partition,
        };

        // Materialization is async; build the merge lazily on first poll and
        // flatten the resulting stream into the output.
        let fut = async move { parallel_merge(input_streams, cfg).await };
        let stream = stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics_with_args(&self, _args: &StatisticsArgs) -> Result<Arc<Statistics>> {
        // The parallel merge materializes all input rows into a single output
        // partition, so its statistics equal the input's overall statistics
        // regardless of the requested partition.
        self.input.statistics_with_args(&StatisticsArgs::new())
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

/// Configuration captured from `execute` and used to drive the parallel merge.
struct MergeConfig {
    schema: SchemaRef,
    expr: LexOrdering,
    metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    target_buckets: usize,
    enable_round_robin_repartition: bool,
    memory_pool: Arc<dyn MemoryPool>,
    partition: usize,
}

/// A locally sorted input partition, fully materialized in memory.
struct Run {
    /// The sorted record batches of this run (payload kept zero-copy).
    batches: Vec<RecordBatch>,
    /// Prefix sums of row counts: `offsets[k]` is the number of rows in
    /// `batches[0..k]`. `offsets` has length `batches.len() + 1` and is strictly
    /// increasing (empty batches are dropped).
    offsets: Vec<usize>,
    /// Sort-key expressions, used to encode individual rows on demand.
    key_exprs: Arc<[Arc<dyn PhysicalExpr>]>,
    /// Shared converter producing byte-comparable key encodings.
    converter: Arc<RowConverter>,
    /// Keeps the memory of `batches` accounted for the lifetime of the run.
    _reservation: MemoryReservation,
}

impl Run {
    fn len(&self) -> usize {
        self.offsets.last().copied().unwrap_or(0)
    }

    /// Map a global row index to `(batch index, offset within batch)`.
    fn locate(&self, pos: usize) -> (usize, usize) {
        // `offsets` is strictly increasing and starts at 0, so the batch holding
        // `pos` is the one starting at the last offset that is `<= pos`.
        let batch = self.offsets.partition_point(|&o| o <= pos) - 1;
        (batch, pos - self.offsets[batch])
    }

    /// Append the encoded sort key at global row index `pos` to `rows`.
    ///
    /// Keys are encoded on demand: pivot selection only probes
    /// `O(samples + buckets * runs * log n)` rows, so we never encode the whole
    /// run (the per-bucket merge encodes the rows it actually consumes). The
    /// caller controls `rows` so encodings can either accumulate (sampling) or
    /// be cleared and reused per probe (binary search).
    fn append_key(&self, pos: usize, rows: &mut Rows) -> Result<()> {
        let (batch, offset) = self.locate(pos);
        let row = self.batches[batch].slice(offset, 1);
        let cols = evaluate_expressions_to_arrays(self.key_exprs.iter(), &row)?;
        self.converter.append(rows, &cols)?;
        Ok(())
    }

    /// A reusable single-row buffer for [`Self::append_key`].
    fn key_scratch(&self) -> Rows {
        self.converter.empty_rows(1, 0)
    }

    /// First index `k` in `[0, len)` whose encoded key is `>= pivot`.
    ///
    /// Encoded rows are byte-comparable, so a plain `&[u8]` comparison matches
    /// the lexicographic order defined by the [`RowConverter`]. `scratch` is a
    /// reused single-row buffer so the search allocates nothing per probe.
    fn lower_bound(&self, pivot: &[u8], scratch: &mut Rows) -> Result<usize> {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            scratch.clear();
            self.append_key(mid, scratch)?;
            if scratch.row(0).as_ref() < pivot {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    /// Zero-copy slices of this run covering the global row range `[lo, hi)`.
    fn slice(&self, lo: usize, hi: usize) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        for (k, batch) in self.batches.iter().enumerate() {
            let b_start = self.offsets[k];
            let b_end = self.offsets[k + 1];
            if b_end <= lo {
                continue;
            }
            if b_start >= hi {
                break;
            }
            let start = lo.max(b_start) - b_start;
            let end = hi.min(b_end) - b_start;
            if end > start {
                out.push(batch.slice(start, end - start));
            }
        }
        out
    }
}

/// Drive the full parallel merge: materialize the inputs, pick pivots, cut each
/// run, and spawn one concurrent merge per bucket. Returns a single sorted
/// output stream.
async fn parallel_merge(
    input_streams: Vec<SendableRecordBatchStream>,
    cfg: MergeConfig,
) -> Result<SendableRecordBatchStream> {
    let MergeConfig {
        schema,
        expr,
        metrics,
        batch_size,
        target_buckets,
        enable_round_robin_repartition,
        memory_pool,
        partition,
    } = cfg;

    // A single shared converter makes encoded keys comparable across all runs
    // and against the pivots.
    let sort_fields = expr
        .iter()
        .map(|sort| {
            let data_type = sort.expr.data_type(schema.as_ref())?;
            Ok(SortField::new_with_options(data_type, sort.options))
        })
        .collect::<Result<Vec<_>>>()?;
    let converter = Arc::new(RowConverter::new(sort_fields)?);
    let key_exprs: Arc<[Arc<dyn PhysicalExpr>]> =
        expr.iter().map(|sort| Arc::clone(&sort.expr)).collect();

    // Materialize the runs. Each input stream is already on its own task, so
    // polling them concurrently drives the upstream sorts in parallel.
    let collect_futures = input_streams.into_iter().map(|stream| {
        let converter = Arc::clone(&converter);
        let key_exprs = Arc::clone(&key_exprs);
        let reservation = MemoryConsumer::new(format!(
            "ParallelSortPreservingMergeExec[{partition}] run"
        ))
        .register(&memory_pool);
        async move { collect_run(stream, converter, key_exprs, reservation).await }
    });
    let runs: Vec<Run> = futures::future::try_join_all(collect_futures).await?;

    let total_rows: usize = runs.iter().map(Run::len).sum();
    if total_rows == 0 {
        return Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::empty(),
        )));
    }

    // Choose the number of buckets: never more than there are rows to fill a
    // batch, never more than the target.
    let max_useful_buckets = total_rows.div_ceil(batch_size.max(1)).max(1);
    let num_buckets = target_buckets.clamp(1, max_useful_buckets);

    let pivots = choose_pivots(&runs, num_buckets, &converter)?;

    // Cut each run by the pivots: `cuts[run][b]` for b in 0..=num_buckets, with
    // the first cut at 0 and the last at the run length.
    let cuts: Vec<Vec<usize>> = runs
        .iter()
        .map(|run| {
            let mut scratch = run.key_scratch();
            let mut c = Vec::with_capacity(num_buckets + 1);
            c.push(0);
            for pivot in &pivots {
                c.push(run.lower_bound(pivot, &mut scratch)?);
            }
            c.push(run.len());
            Ok(c)
        })
        .collect::<Result<Vec<_>>>()?;

    let runs = Arc::new(runs);

    // Merge each bucket on its own task so the buckets run concurrently across
    // worker threads (a single k-way merge per bucket, reusing the optimized
    // loser-tree merge). Each task fully merges its bucket into a `Vec`.
    let mut handles: Vec<SpawnedTask<Result<Vec<RecordBatch>>>> =
        Vec::with_capacity(num_buckets);
    for bucket in 0..num_buckets {
        let mut sub_streams: Vec<SendableRecordBatchStream> =
            Vec::with_capacity(runs.len());
        for (run_idx, run) in runs.iter().enumerate() {
            let lo = cuts[run_idx][bucket];
            let hi = cuts[run_idx][bucket + 1];
            if hi <= lo {
                continue;
            }
            let slices = run.slice(lo, hi);
            let sub = stream::iter(slices.into_iter().map(Ok));
            sub_streams.push(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                sub,
            )));
        }

        // Empty buckets contribute nothing to the ordered output.
        if sub_streams.is_empty() {
            continue;
        }

        let reservation = MemoryConsumer::new(format!(
            "ParallelSortPreservingMergeExec[{partition}] bucket {bucket}"
        ))
        .register(&memory_pool);
        let merge_stream = StreamingMergeBuilder::new()
            .with_streams(sub_streams)
            .with_schema(Arc::clone(&schema))
            .with_expressions(&expr)
            .with_metrics(BaselineMetrics::new(&metrics, partition))
            .with_batch_size(batch_size)
            .with_reservation(reservation)
            .with_round_robin_tie_breaker(enable_round_robin_repartition)
            .build()?;

        handles.push(SpawnedTask::spawn(async move {
            merge_stream.try_collect::<Vec<_>>().await
        }));
    }

    // Collect the buckets in order. The tasks were spawned up front and run
    // concurrently, so awaiting them sequentially preserves the global order
    // without serializing the merge work. Concatenating the bucket outputs in
    // order yields a single globally sorted stream.
    let mut output = Vec::new();
    for handle in handles {
        let batches = handle.join_unwind().await.map_err(|e| {
            DataFusionError::Execution(format!(
                "ParallelSortPreservingMergeExec bucket task failed: {e}"
            ))
        })??;
        output.extend(batches);
    }
    // The merged output owns freshly interleaved arrays, so the input runs (and
    // their key encodings) can be released now.
    drop(runs);

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        stream::iter(output.into_iter().map(Ok)),
    )))
}

/// Fully drain a sorted input stream into a [`Run`].
///
/// Only the payload batches are buffered; sort keys are encoded lazily by
/// [`Run::append_key`] during pivot selection.
async fn collect_run(
    mut stream: SendableRecordBatchStream,
    converter: Arc<RowConverter>,
    key_exprs: Arc<[Arc<dyn PhysicalExpr>]>,
    reservation: MemoryReservation,
) -> Result<Run> {
    let mut batches = Vec::new();
    let mut offsets = vec![0usize];

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }
        reservation.try_grow(get_record_batch_memory_size(&batch))?;
        offsets.push(offsets.last().unwrap() + num_rows);
        batches.push(batch);
    }

    Ok(Run {
        batches,
        offsets,
        key_exprs,
        converter,
        _reservation: reservation,
    })
}

/// Select `num_buckets - 1` pivots (split points) by regular sampling.
///
/// Samples are drawn at a fixed global stride so that each run contributes a
/// number of samples proportional to its length. The merged sample is sorted
/// and `num_buckets - 1` evenly spaced quantiles are returned as pivots. Each
/// pivot is the raw byte encoding of a sampled key (comparable to run keys).
fn choose_pivots(
    runs: &[Run],
    num_buckets: usize,
    converter: &RowConverter,
) -> Result<Vec<Vec<u8>>> {
    if num_buckets <= 1 {
        return Ok(Vec::new());
    }

    let total_rows: usize = runs.iter().map(Run::len).sum();
    let target_samples = (num_buckets * OVERSAMPLE_PER_BUCKET).min(total_rows).max(1);
    let stride = (total_rows / target_samples).max(1);

    // Accumulate every sample into a single row buffer instead of one `Vec<u8>`
    // per sample.
    let mut samples = converter.empty_rows(target_samples, 0);
    for run in runs {
        let len = run.len();
        let mut pos = stride / 2;
        while pos < len {
            run.append_key(pos, &mut samples)?;
            pos += stride;
        }
    }

    let m = samples.num_rows();
    if m == 0 {
        return Ok(Vec::new());
    }
    // Sort indices into the buffer by their (byte-comparable) encoded key; the
    // byte order of the encoding equals the sort order.
    let mut order: Vec<usize> = (0..m).collect();
    order.sort_unstable_by(|&a, &b| samples.row(a).cmp(&samples.row(b)));

    // Pick `num_buckets - 1` evenly spaced quantiles as pivots, owning only
    // those few bytes for use during binary search.
    Ok((1..num_buckets)
        .map(|j| {
            let idx = ((j * m) / num_buckets).min(m - 1);
            samples.row(order[idx]).as_ref().to_vec()
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect;
    use crate::expressions::col;
    use crate::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use crate::test::TestMemoryExec;

    use arrow::array::{Array, ArrayRef, Int64Array};
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_execution::TaskContext;
    use datafusion_execution::config::SessionConfig;
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

    /// Build a two-column (`k` key, `v` payload == key) schema.
    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]))
    }

    /// One sorted partition from key values. The payload column `v` mirrors the
    /// key so we can detect rows whose payload got separated from their key.
    fn sorted_partition(
        schema: &Arc<Schema>,
        mut keys: Vec<Option<i64>>,
        opts: SortOptions,
        rows_per_batch: usize,
    ) -> Vec<RecordBatch> {
        keys.sort_by(|a, b| match (a, b) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, _) => {
                if opts.nulls_first {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (_, None) => {
                if opts.nulls_first {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            (Some(a), Some(b)) => {
                if opts.descending {
                    b.cmp(a)
                } else {
                    a.cmp(b)
                }
            }
        });
        keys.chunks(rows_per_batch.max(1))
            .map(|chunk| {
                let k: ArrayRef = Arc::new(Int64Array::from(chunk.to_vec()));
                let v: ArrayRef = Arc::new(Int64Array::from(chunk.to_vec()));
                RecordBatch::try_new(Arc::clone(schema), vec![k, v]).unwrap()
            })
            .collect()
    }

    fn key_sequence(batches: &[RecordBatch]) -> Vec<Option<i64>> {
        let mut out = Vec::new();
        for batch in batches {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..arr.len() {
                out.push(if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                });
            }
        }
        out
    }

    /// The payload `v` must equal the key `k` in every output row, otherwise the
    /// merge separated a row's columns.
    fn assert_payload_matches_key(batches: &[RecordBatch]) {
        for batch in batches {
            let k = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let v = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(k, v, "payload column diverged from key column");
        }
    }

    async fn run_and_compare(
        partitions: Vec<Vec<Option<i64>>>,
        opts: SortOptions,
        target_buckets: usize,
        batch_size: usize,
    ) {
        let schema = test_schema();
        let rows_per_batch = 3;
        let data: Vec<Vec<RecordBatch>> = partitions
            .into_iter()
            .map(|keys| sorted_partition(&schema, keys, opts, rows_per_batch))
            .collect();

        let sort: LexOrdering = [PhysicalSortExpr {
            expr: col("k", &schema).unwrap(),
            options: opts,
        }]
        .into();

        let config = SessionConfig::new().with_batch_size(batch_size);
        let ctx = Arc::new(TaskContext::default().with_session_config(config));

        // Reference: single-threaded sort preserving merge.
        let spm_input =
            TestMemoryExec::try_new_exec(&data, Arc::clone(&schema), None).unwrap();
        let spm = Arc::new(SortPreservingMergeExec::new(sort.clone(), spm_input));
        let expected = collect(spm, Arc::clone(&ctx)).await.unwrap();

        // Parallel merge.
        let par_input =
            TestMemoryExec::try_new_exec(&data, Arc::clone(&schema), None).unwrap();
        let par = Arc::new(
            ParallelSortPreservingMergeExec::new(sort, par_input)
                .with_target_buckets(target_buckets),
        );
        let actual = collect(par, ctx).await.unwrap();

        let expected_keys = key_sequence(&expected);
        let actual_keys = key_sequence(&actual);

        // Same number of rows and identical key sequence (ties keep the same key,
        // so the key sequence is deterministic regardless of tie-break order).
        assert_eq!(
            expected_keys.len(),
            actual_keys.len(),
            "row count mismatch: expected {} got {}",
            expected_keys.len(),
            actual_keys.len()
        );
        assert_eq!(expected_keys, actual_keys, "key sequence diverged from SPM");
        assert_payload_matches_key(&actual);
    }

    fn asc() -> SortOptions {
        SortOptions {
            descending: false,
            nulls_first: true,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unique_keys_match_spm() {
        // Four interleaved sorted runs with globally unique keys.
        let partitions = (0..4)
            .map(|p| (0..25).map(|i| Some(p + 4 * i)).collect())
            .collect();
        run_and_compare(partitions, asc(), 4, 4).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn low_cardinality_keys_match_spm() {
        // Heavy ties: only three distinct keys spread across runs. Pivots will
        // collapse; buckets become uneven but output must stay correct.
        let partitions = vec![
            vec![Some(0); 30]
                .into_iter()
                .chain(vec![Some(1); 10])
                .collect(),
            vec![Some(1); 20]
                .into_iter()
                .chain(vec![Some(2); 20])
                .collect(),
            vec![Some(0); 5]
                .into_iter()
                .chain(vec![Some(2); 35])
                .collect(),
        ];
        run_and_compare(partitions, asc(), 8, 4).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn descending_with_nulls_match_spm() {
        let opts = SortOptions {
            descending: true,
            nulls_first: false,
        };
        let partitions = vec![
            vec![Some(5), Some(3), None, Some(1)],
            vec![Some(9), Some(3), Some(3), None, None],
            vec![Some(8), Some(2), Some(0)],
        ];
        run_and_compare(partitions, opts, 4, 2).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn uneven_partition_sizes_match_spm() {
        let partitions = vec![
            (0..100).map(Some).collect(),
            vec![Some(50)],
            vec![],
            (0..7).map(|i| Some(i * 13)).collect(),
        ];
        run_and_compare(partitions, asc(), 8, 5).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn more_buckets_than_rows_match_spm() {
        let partitions = vec![vec![Some(1), Some(4)], vec![Some(2), Some(3)]];
        run_and_compare(partitions, asc(), 64, 1).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn single_partition_passthrough() {
        let partitions = vec![(0..20).map(Some).collect()];
        run_and_compare(partitions, asc(), 4, 4).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn all_empty_partitions() {
        let partitions = vec![vec![], vec![], vec![]];
        run_and_compare(partitions, asc(), 4, 4).await;
    }
}
