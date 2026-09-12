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

//! Sort-merge fallback of [`HashJoinExec`] under memory pressure.
//!
//! A hash join materializes its whole build side in memory. When the memory
//! pool refuses that reservation the join cannot continue as a hash join, but
//! it can still finish: both inputs are sorted on the join keys with an
//! external (spilling) sort, and the join is completed by the sort-merge join
//! streams, which only keep one key group of the buffered side in memory.
//!
//! The fallback is decided per output partition at runtime, after the hash
//! join's own `try_grow` failed (or, with `hash_join_max_build_size` set,
//! once the build side grew past that size), so joins that fit in memory
//! never pay for it.
//! It is restricted to [`PartitionMode::Partitioned`] joins: there every
//! partition owns a disjoint slice of the key space on both sides, so each
//! partition can sort and merge its own inputs independently of its siblings
//! and every join type stays correct.
//!
//! # Limitations
//!
//! The fallback replaces a build side that must fit entirely in memory with a
//! much smaller requirement, but not with none. Each sort pre-reserves
//! `sort_spill_reservation_bytes` so that its merge phase can always run, and a
//! falling-back partition runs two sorts, so one join can hold several such
//! reservations at once on top of the sorts' working set. A budget that does not
//! cover that floor still fails, and it fails inside the sort rather than in the
//! hash join. With the 10 MB default reservation and two partitions, the
//! reservations alone come to tens of megabytes, so a budget below that cannot
//! support the fallback however large the data is. Sizing the fallback's sorts
//! from the budget actually available, instead of inheriting the global default,
//! is left as follow-up work.
//!
//! Partitions also share one pool without coordinating, and a partition that
//! falls back does not make a sibling that still fits release its hash table.
//! That narrows the usable range further. Whether one or both partitions fall
//! back is not by itself decisive: a single falling-back partition completes
//! when the budget covers its sorts.
//!
//! [`HashJoinExec`]: super::HashJoinExec
//! [`PartitionMode::Partitioned`]: crate::joins::PartitionMode::Partitioned

use std::sync::Arc;

use crate::SendableRecordBatchStream;
use crate::expressions::PhysicalSortExpr;
use crate::joins::hash_join::exec::{CollectLeftAccumulator, JoinLeftData};
use crate::joins::hash_join::shared_bounds::PartitionBounds;
use crate::joins::sort_merge_join::{SortMergeJoinInputs, sort_merge_join_stream};
use crate::joins::utils::JoinFilter;
use crate::limit::LimitStream;
use crate::metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, SpillMetrics};
use crate::sorts::sort::ExternalSorter;
use crate::stream::RecordBatchStreamAdapter;

use arrow::array::Array;
use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, JoinType, NullEquality, Result, internal_err};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use futures::StreamExt;
use parking_lot::Mutex;

/// Everything the fallback needs from the join, captured once per partition
/// in `HashJoinExec::execute` and shared by the build future and the stream.
#[derive(Clone)]
pub(super) struct SortMergeFallbackContext {
    pub(super) context: Arc<TaskContext>,
    pub(super) partition: usize,
    /// The join's metrics; the sort-merge join stream registers its metrics
    /// (its output rows included) here.
    pub(super) metrics: ExecutionPlanMetricsSet,
    /// Spill metrics of the join; the fallback's sorts report their spills here.
    pub(super) spill_metrics: SpillMetrics,
    /// Incremented once when a partition falls back.
    pub(super) fallback_count: Count,
    /// Join keys of the left (build) side.
    pub(super) on_left: Vec<PhysicalExprRef>,
    /// Join keys of the right (probe) side.
    pub(super) on_right: Vec<PhysicalExprRef>,
    /// Sort options both sides are sorted with, one per join key.
    pub(super) sort_options: Vec<SortOptions>,
    pub(super) join_type: JoinType,
    pub(super) filter: Option<JoinFilter>,
    pub(super) null_equality: NullEquality,
    /// The join schema before any projection: the schema the sort-merge join
    /// streams produce.
    pub(super) join_schema: SchemaRef,
    /// The join's output schema, after `projection`.
    pub(super) output_schema: SchemaRef,
    pub(super) projection: Option<Vec<usize>>,
    pub(super) fetch: Option<usize>,
    /// Build side size, in reserved bytes, past which the partition falls
    /// back even though the memory pool would allow more; `None` for no limit
    /// (`datafusion.execution.hash_join_max_build_size`)
    pub(super) max_build_size: Option<usize>,
}

impl SortMergeFallbackContext {
    fn sort_ordering(&self, on: &[PhysicalExprRef]) -> Result<LexOrdering> {
        let exprs = on
            .iter()
            .zip(&self.sort_options)
            .map(|(expr, options)| PhysicalSortExpr::new(Arc::clone(expr), *options));
        LexOrdering::new(exprs)
            .ok_or_else(|| DataFusionError::Internal("join without keys".to_string()))
    }
}

/// Result of collecting the build side of one partition.
pub(super) enum BuildSideOutcome {
    /// The build side fits in memory and is ready to be probed.
    InMemory(Arc<JoinLeftData>),
    /// The build side did not fit in memory and was sorted instead; the
    /// partition finishes as a sort-merge join.
    SortMerge(SortedBuildSide),
}

/// The sorted build side of a partition that fell back to a sort-merge join.
pub(super) struct SortedBuildSide {
    /// The build side, sorted on the join keys. Taken by the single stream
    /// of the partition.
    stream: Mutex<Option<SendableRecordBatchStream>>,
    /// Min/max of the join keys, reported to the dynamic filter when one is
    /// pushed down.
    pub(super) bounds: Option<PartitionBounds>,
    /// Whether any build-side join key is NULL.
    pub(super) keys_have_null: bool,
}

impl SortedBuildSide {
    pub(super) fn take_stream(&self) -> Result<SendableRecordBatchStream> {
        match self.stream.lock().take() {
            Some(stream) => Ok(stream),
            None => internal_err!("sorted build side was already consumed"),
        }
    }
}

/// Whether `error` is an exhausted memory pool that the fallback can recover from.
pub(super) fn is_resources_exhausted(error: &DataFusionError) -> bool {
    matches!(error.find_root(), DataFusionError::ResourcesExhausted(_))
}

/// Sorts `buffered` followed by the rest of `input` on `ordering` with an
/// external sort, calling `observe` for every batch on its way in.
async fn sort_batches(
    ctx: &SortMergeFallbackContext,
    schema: SchemaRef,
    ordering: LexOrdering,
    buffered: Vec<RecordBatch>,
    mut input: Option<SendableRecordBatchStream>,
    mut observe: impl FnMut(&RecordBatch) -> Result<()>,
) -> Result<SendableRecordBatchStream> {
    let session_config = ctx.context.session_config();
    let execution_options = &session_config.options().execution;
    // The sorter's own metrics set: its baseline metrics would otherwise count
    // the sorted rows as output rows of the join. Its spills are copied to the
    // join's spill metrics below.
    let sorter_metrics = ExecutionPlanMetricsSet::new();
    let mut sorter = ExternalSorter::new(
        ctx.partition,
        schema,
        ordering,
        session_config.batch_size(),
        execution_options.sort_spill_reservation_bytes,
        execution_options.sort_in_place_threshold_bytes,
        session_config.spill_compression(),
        &sorter_metrics,
        ctx.context.runtime_env(),
    )?;

    for batch in buffered {
        insert_batch(&mut sorter, batch, &mut observe).await?;
    }
    if let Some(input) = input.as_mut() {
        while let Some(batch) = input.next().await {
            let batch = match batch {
                Ok(batch) => batch,
                Err(error) => {
                    sorter.abort_in_progress_spill().await;
                    return Err(error);
                }
            };
            insert_batch(&mut sorter, batch, &mut observe).await?;
        }
    }
    drop(input);

    let sorted = sorter.sort().await?;

    let spills = sorter.spill_metrics();
    ctx.spill_metrics
        .spill_file_count
        .add(spills.spill_file_count.value());
    ctx.spill_metrics
        .spilled_bytes
        .add(spills.spilled_bytes.value());
    ctx.spill_metrics
        .spilled_rows
        .add(spills.spilled_rows.value());

    Ok(sorted)
}

/// Feeds one batch to `sorter`, abandoning its in-progress spill on error.
async fn insert_batch(
    sorter: &mut ExternalSorter,
    batch: RecordBatch,
    observe: &mut impl FnMut(&RecordBatch) -> Result<()>,
) -> Result<()> {
    observe(&batch)?;
    if let Err(error) = sorter.insert_batch(batch).await {
        sorter.abort_in_progress_spill().await;
        return Err(error);
    }
    Ok(())
}

/// Sorts the build side of a partition after its in-memory collection ran
/// out of memory.
///
/// `batches` are the build batches collected so far, `pending` the batch whose
/// reservation failed and `rest` the not yet consumed remainder of the build
/// input (both `None` when the input was fully consumed and the hash table
/// itself did not fit). The caller has already released the reservation held
/// for `batches`; the external sort reserves what it keeps in memory itself.
///
/// The join key bounds needed by a pushed-down dynamic filter are computed
/// with `bounds_accumulators` over every batch (min/max are idempotent, so
/// re-feeding the already accumulated `batches` is harmless), unless the
/// caller already has the final `bounds`.
pub(super) async fn sort_build_side(
    ctx: SortMergeFallbackContext,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    pending: Option<RecordBatch>,
    rest: Option<SendableRecordBatchStream>,
    mut bounds_accumulators: Option<Vec<CollectLeftAccumulator>>,
    bounds: Option<PartitionBounds>,
) -> Result<SortedBuildSide> {
    ctx.fallback_count.add(1);

    let ordering = ctx.sort_ordering(&ctx.on_left)?;
    let mut buffered = batches;
    buffered.extend(pending);

    let on_left = ctx.on_left.clone();
    let mut keys_have_null = false;
    let mut num_rows = 0;
    let observe = |batch: &RecordBatch| -> Result<()> {
        num_rows += batch.num_rows();
        if let Some(accumulators) = bounds_accumulators.as_mut() {
            for accumulator in accumulators {
                accumulator.update_batch(batch)?;
            }
        }
        if !keys_have_null {
            keys_have_null = evaluate_expressions_to_arrays(&on_left, batch)?
                .iter()
                .any(|array| array.logical_null_count() > 0);
        }
        Ok(())
    };

    let stream = sort_batches(&ctx, schema, ordering, buffered, rest, observe)
        .await
        .map_err(|e| {
            e.context("HashJoinExec sort-merge fallback: sorting the build side")
        })?;

    let bounds = match bounds_accumulators {
        Some(accumulators) if num_rows > 0 => Some(PartitionBounds::new(
            accumulators
                .into_iter()
                .map(CollectLeftAccumulator::evaluate)
                .collect::<Result<Vec<_>>>()?,
        )),
        Some(_) => None,
        None => bounds,
    };

    Ok(SortedBuildSide {
        stream: Mutex::new(Some(stream)),
        bounds,
        keys_have_null,
    })
}

/// Sorts the probe side and joins it with the sorted build side as a
/// sort-merge join, returning the join's output stream (projected and limited
/// like the hash join's own output would be).
pub(super) async fn run_sort_merge_fallback(
    ctx: SortMergeFallbackContext,
    build: SendableRecordBatchStream,
    probe: SendableRecordBatchStream,
) -> Result<SendableRecordBatchStream> {
    let ordering = ctx.sort_ordering(&ctx.on_right)?;
    let probe = sort_batches(&ctx, probe.schema(), ordering, vec![], Some(probe), |_| {
        Ok(())
    })
    .await
    .map_err(|e| e.context("HashJoinExec sort-merge fallback: sorting the probe side"))?;

    let joined = sort_merge_join_stream(
        SortMergeJoinInputs {
            schema: Arc::clone(&ctx.join_schema),
            sort_options: ctx.sort_options.clone(),
            null_equality: ctx.null_equality,
            left: build,
            right: probe,
            on_left: ctx.on_left.clone(),
            on_right: ctx.on_right.clone(),
            filter: ctx.filter.clone(),
            join_type: ctx.join_type,
            partition: ctx.partition,
        },
        &ctx.metrics,
        &ctx.context,
    )?;

    let output: SendableRecordBatchStream = match ctx.projection.clone() {
        Some(projection) => Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&ctx.output_schema),
            joined.map(move |batch| Ok(batch?.project(&projection)?)),
        )),
        None => joined,
    };

    // The limit's own baseline metrics would count the output a second time.
    let output = match ctx.fetch {
        Some(_) => Box::pin(LimitStream::new(
            output,
            0,
            ctx.fetch,
            BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), ctx.partition),
        )),
        None => output,
    };

    Ok(output)
}
