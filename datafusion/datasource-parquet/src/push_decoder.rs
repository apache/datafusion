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

//! Push-based Parquet decoder setup and stream driver.
//!
//! This module owns the push-decoder lifecycle:
//!
//! - [`DecoderBuilderConfig`] holds the shared options applied to the
//!   [`ParquetPushDecoderBuilder`] for a file scan, exposing a single `build`
//!   entry point.
//! - [`PushDecoderStreamState`] is the per-file stream driver. It owns a
//!   **single** [`ParquetPushDecoder`] plus an [`RgPlanEntry`] queue
//!   (`rg_plan`) and uses arrow-rs's [`ParquetRecordBatchReader`] iterator
//!   to pause at row-group boundaries. At each boundary the optional
//!   [`RowGroupPruner`] is consulted; row groups it proves unwinnable are
//!   dropped from the head of `rg_plan` and the decoder is rebuilt via
//!   [`ParquetPushDecoder::into_builder`] +
//!   [`ParquetPushDecoderBuilder::with_row_groups`] so the skipped RGs are
//!   bypassed entirely — no decode, no row-filter eval.
//!
//! The opener constructs both halves and hands the state off to
//! [`PushDecoderStreamState::into_stream`] for consumption.

use std::collections::{HashSet, VecDeque};
use std::ops::Range;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use log::debug;
use parquet::DecodeResult;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, RowSelectionPolicy,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::push_decoder::{
    ParquetPushDecoder, ParquetPushDecoderBuilder, PlannedRange, plan_scan_ranges,
};
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{ChunkReader, Length};

use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::expressions::DynamicFilterTracking;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::metrics::{BaselineMetrics, Count, Gauge};
use datafusion_pruning::{PruningPredicate, build_pruning_predicate};

use crate::access_plan::PreparedAccessPlan;
use crate::decoder_projection::DecoderProjection;
use crate::row_group_filter::RowGroupPruningStatistics;

/// Shared options applied to the [`ParquetPushDecoderBuilder`] for a file
/// scan, and to any later rebuilds performed via
/// [`ParquetPushDecoder::into_builder`] at row-group boundaries (e.g. when
/// the [`RowGroupPruner`] drops subsequent row groups).
pub(crate) struct DecoderBuilderConfig<'a> {
    /// Projection mask installed on every decoder in the scan. Sourced from
    /// the file's [`DecoderProjection`].
    pub(crate) projection_mask: &'a ProjectionMask,
    pub(crate) batch_size: usize,
    pub(crate) arrow_reader_metrics: &'a ArrowReaderMetrics,
    pub(crate) force_filter_selections: bool,
    pub(crate) decoder_limit: Option<usize>,
}

impl DecoderBuilderConfig<'_> {
    /// Build a [`ParquetPushDecoderBuilder`] from a prepared access plan.
    ///
    /// The caller is expected to attach the
    /// [`RowFilter`](parquet::arrow::arrow_reader::RowFilter) and predicate
    /// cache size on the returned builder.
    pub(crate) fn build(
        &self,
        prepared_access_plan: PreparedAccessPlan,
        metadata: ArrowReaderMetadata,
    ) -> ParquetPushDecoderBuilder {
        let mut builder = ParquetPushDecoderBuilder::new_with_metadata(metadata)
            .with_projection(self.projection_mask.clone())
            .with_batch_size(self.batch_size)
            .with_metrics(self.arrow_reader_metrics.clone());
        if self.force_filter_selections {
            builder = builder.with_row_selection_policy(RowSelectionPolicy::Selectors);
        }
        if let Some(row_selection) = prepared_access_plan.row_selection {
            builder = builder.with_row_selection(row_selection);
        }
        builder = builder.with_row_groups(prepared_access_plan.row_group_indexes);
        if let Some(limit) = self.decoder_limit {
            builder = builder.with_limit(limit);
        }
        builder
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RgPlanEntry {
    pub(crate) rg_index: usize,
}

/// EXPERIMENT: peak bytes staged in the push decoder (buffered but not yet
/// handed to a reader), max over all streams since last reset. Benchmarks
/// reset and read this between runs.
pub static PEAK_STAGED_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn note_staged_bytes(decoder: &ParquetPushDecoder) {
    PEAK_STAGED_BYTES.fetch_max(
        decoder.buffered_bytes(),
        std::sync::atomic::Ordering::Relaxed,
    );
}

/// EXPERIMENT: how the stream schedules I/O relative to decode.
///
/// - `Off`: current main behavior — fetch exactly what the decoder asks
///   for, when it asks for it. I/O and decode strictly alternate.
/// - `Batched`: PR #23492 behavior — when the decoder asks for the current
///   row group's ranges, append the complete projected ranges of upcoming
///   row groups to the *same* blocking fetch, as long as
///   `buffered + staged <= budget`. Fewer round trips, but no overlap.
/// - `Pipelined`: when a row group's reader is handed over for decode,
///   spawn a *background* fetch for upcoming row groups' projected ranges
///   within the same byte budget. Decode of the current RG overlaps with
///   I/O for the next ones.
/// - `Streaming`: batch-granular readiness. One long-lived *sync*
///   [`ParquetRecordBatchReader`] pulls bytes through a shared in-memory
///   buffer; the stream driver computes, from the offset index, exactly
///   which page ranges the next batch needs, awaits their fetch, and keeps
///   a background readahead of up to `window` bytes in flight. Falls back
///   to `Off` when preconditions don't hold (no offset index, or row
///   filters are active). See [`StreamingScanState`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum FetchPolicy {
    Off,
    Batched { budget: u64 },
    Pipelined { budget: u64 },
    Streaming { window: u64 },
}

impl FetchPolicy {
    pub(crate) fn from_env() -> Self {
        let budget = std::env::var("DF_FETCH_BUDGET")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(20 * 1024 * 1024);
        match std::env::var("DF_FETCH_POLICY").as_deref() {
            Ok("batched") => FetchPolicy::Batched { budget },
            Ok("pipelined") => FetchPolicy::Pipelined { budget },
            Ok("streaming") => FetchPolicy::Streaming { window: budget },
            _ => FetchPolicy::Off,
        }
    }
}

/// Payload returned by a background prefetch task: the lent reader, the
/// ranges it fetched, and the fetch result.
type PrefetchResult = (
    Box<dyn AsyncFileReader>,
    Vec<Range<u64>>,
    parquet::errors::Result<Vec<Bytes>>,
);

/// The file reader is either available inline or lent out to a background
/// prefetch task (only under [`FetchPolicy::Pipelined`]).
pub(crate) enum ReaderSlot {
    Idle(Box<dyn AsyncFileReader>),
    Busy(tokio::task::JoinHandle<PrefetchResult>),
    /// Transient state while ownership moves between the two above.
    Empty,
}

/// Compute the complete projected byte ranges of upcoming row groups that
/// fit in `budget` given `staged_bytes` already accounted for. Mirrors the
/// logic in PR #23492. `entries` must already exclude the row group
/// currently being fetched/decoded.
fn upcoming_row_group_ranges<'a>(
    entries: impl Iterator<Item = &'a RgPlanEntry>,
    projection: &ProjectionMask,
    metadata: &ParquetMetaData,
    prefetched_row_groups: &mut HashSet<usize>,
    mut staged_bytes: u64,
    budget: u64,
) -> Vec<Range<u64>> {
    let mut ranges = Vec::new();
    if staged_bytes >= budget {
        return ranges;
    }
    for entry in entries {
        if prefetched_row_groups.contains(&entry.rg_index) {
            continue;
        }
        let row_group = metadata.row_group(entry.rg_index);
        let row_group_ranges = row_group
            .columns()
            .iter()
            .enumerate()
            .filter(|(column_idx, _)| projection.leaf_included(*column_idx))
            .map(|(_, column)| {
                let (start, len) = column.byte_range();
                start..start + len
            })
            .collect::<Vec<_>>();
        let row_group_bytes = row_group_ranges
            .iter()
            .map(|range| range.end - range.start)
            .sum::<u64>();
        if staged_bytes.saturating_add(row_group_bytes) > budget {
            break;
        }
        ranges.extend(row_group_ranges);
        staged_bytes += row_group_bytes;
        prefetched_row_groups.insert(entry.rg_index);
    }
    ranges
}

/// Runtime row-group pruner driven by a dynamic predicate (e.g. the
/// threshold expression a `TopK` operator pushes down).
///
/// Mirrors the [`FilePruner`](datafusion_pruning::FilePruner) pattern at
/// the row-group level: subscribes once to every still-incomplete dynamic
/// filter inside the predicate via
/// [`DynamicFilterTracker`](datafusion_physical_expr::expressions::DynamicFilterTracker)
/// and only rebuilds the [`PruningPredicate`] when one of those
/// subscriptions reports an update, then evaluates the cached predicate
/// against the statistics of the requested row groups.
pub(crate) struct RowGroupPruner {
    predicate: Arc<dyn PhysicalExpr>,
    arrow_schema: SchemaRef,
    parquet_metadata: Arc<ParquetMetaData>,
    /// Classifies the predicate's dynamic-filter content. The `Watching`
    /// variant carries a tracker that subscribes to every not-yet-complete
    /// dynamic filter; for `Static` / `AllComplete` the predicate cannot
    /// change so a single up-front `pruning_predicate` build suffices.
    tracking: DynamicFilterTracking,
    /// First-call sentinel: forces an initial `pruning_predicate` build
    /// even when `tracking` is `Static` / `AllComplete`.
    needs_initial_build: bool,
    /// Cached pruning predicate. `None` means we couldn't build one for the
    /// current generation (e.g. the predicate has no analyzable bounds);
    /// in that case we conservatively don't prune.
    pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Metric for `build_pruning_predicate` failures (predicate creation).
    predicate_creation_errors: Count,
    /// Metric for `PruningPredicate::prune` failures (evaluating an
    /// already-built predicate against row-group statistics).
    predicate_evaluation_errors: Count,
}

impl RowGroupPruner {
    pub(crate) fn new(
        predicate: Arc<dyn PhysicalExpr>,
        arrow_schema: SchemaRef,
        parquet_metadata: Arc<ParquetMetaData>,
        predicate_creation_errors: Count,
        predicate_evaluation_errors: Count,
    ) -> Self {
        let tracking = DynamicFilterTracking::classify(&predicate);
        Self {
            predicate,
            arrow_schema,
            parquet_metadata,
            tracking,
            needs_initial_build: true,
            pruning_predicate: None,
            predicate_creation_errors,
            predicate_evaluation_errors,
        }
    }

    /// Returns `true` when the statistics for `row_group_indices` prove that
    /// every requested row group can be skipped under the current value of
    /// the dynamic predicate.
    ///
    /// On any error (predicate construction, statistics evaluation) the
    /// pruner conservatively returns `false` and logs the failure, so a
    /// flaky pruning path never silently drops data.
    pub(crate) fn should_prune(&mut self, row_group_indices: &[usize]) -> bool {
        if row_group_indices.is_empty() {
            return false;
        }

        // Refresh the cached `PruningPredicate` on the first call and
        // whenever a watched dynamic filter has advanced since we last
        // looked. `changed()` is a single atomic load per still-incomplete
        // filter — no tree walk on every check.
        let dynamic_changed = self
            .tracking
            .watcher()
            .is_some_and(|tracker| tracker.changed());
        if self.needs_initial_build || dynamic_changed {
            self.pruning_predicate = build_pruning_predicate(
                Arc::clone(&self.predicate),
                &self.arrow_schema,
                &self.predicate_creation_errors,
            );
            self.needs_initial_build = false;
        }

        let Some(pp) = self.pruning_predicate.as_ref() else {
            return false;
        };

        let row_group_metadatas = row_group_indices
            .iter()
            .map(|&i| self.parquet_metadata.row_group(i))
            .collect::<Vec<_>>();
        let stats = RowGroupPruningStatistics {
            parquet_schema: self.parquet_metadata.file_metadata().schema_descr(),
            row_group_metadatas,
            arrow_schema: self.arrow_schema.as_ref(),
            // Match the existing static row-group pruning behavior: when a
            // statistic's null count is missing, treat it as zero. This is
            // sound for runtime pruning because the predicate only needs to
            // prove a row group *cannot* contain matching rows.
            missing_null_counts_as_zero: true,
        };

        match pp.prune(&stats) {
            // `prune` returns `false` per container that the predicate proves
            // cannot contain matching rows. We can skip the run only when
            // every requested row group is in that state.
            Ok(values) => values.iter().all(|&keep| !keep),
            Err(e) => {
                // The predicate was already built successfully (we hold `pp`);
                // this failure is in *evaluating* it against the row-group
                // stats, so it belongs in the evaluation-errors counter, not
                // creation-errors.
                debug!(
                    "Ignoring error evaluating runtime row-group pruning predicate: {e}"
                );
                self.predicate_evaluation_errors.add(1);
                false
            }
        }
    }
}

/// State for a stream that decodes a single Parquet file using a push-based decoder.
///
/// The [`transition`](Self::transition) method drives the decoder in a loop: it requests
/// byte ranges from the [`AsyncFileReader`], pushes the fetched data into the
/// [`ParquetPushDecoder`], and yields projected [`RecordBatch`]es until the file is
/// fully consumed.
pub(crate) struct PushDecoderStreamState {
    pub(crate) decoder: Option<ParquetPushDecoder>,
    pub(crate) active_reader: Option<ParquetRecordBatchReader>,
    pub(crate) rg_plan: VecDeque<RgPlanEntry>,
    pub(crate) reader: ReaderSlot,
    /// EXPERIMENT: fetch scheduling policy (see [`FetchPolicy`]).
    pub(crate) fetch_policy: FetchPolicy,
    /// Parquet metadata used to compute projected ranges of upcoming RGs.
    pub(crate) parquet_metadata: Arc<ParquetMetaData>,
    /// Row groups whose projected ranges were already staged speculatively.
    pub(crate) prefetched_row_groups: HashSet<usize>,
    /// Per-file projection: the mask installed on every decoder and the
    /// per-batch transform applied by [`Self::project_batch`].
    pub(crate) decoder_projection: DecoderProjection,
    pub(crate) arrow_reader_metrics: ArrowReaderMetrics,
    pub(crate) predicate_cache_inner_records: Gauge,
    pub(crate) predicate_cache_records: Gauge,
    pub(crate) baseline_metrics: BaselineMetrics,
    /// Dynamic row-group pruner consulted at every row-group boundary.
    ///
    /// When the file scan was opened with a still-watching dynamic predicate
    /// (typically the threshold expression a `TopK` `SortExec` pushed down),
    /// we re-evaluate that predicate against the next pending RG's
    /// statistics and drop RGs the current threshold proves cannot
    /// contribute. The decoder is rebuilt via
    /// [`ParquetPushDecoder::into_builder`] +
    /// [`ParquetPushDecoderBuilder::with_row_groups`] so the skipped RGs are
    /// bypassed entirely. `None` when the scan has no watching dynamic
    /// predicate or only one row group remains.
    pub(crate) row_group_pruner: Option<RowGroupPruner>,
    /// Count of row groups skipped at runtime by [`Self::row_group_pruner`].
    pub(crate) row_groups_pruned_dynamic: Count,
}

impl PushDecoderStreamState {
    /// Drive the state machine to completion as a [`futures::Stream`] of record batches.
    ///
    /// The returned stream is fused and boxed so the caller can wrap it (for
    /// example, with an early-stopping adapter) without naming the unfold type.
    pub(crate) fn into_stream(self) -> BoxStream<'static, Result<RecordBatch>> {
        futures::stream::unfold(self, |state| async move { state.transition().await })
            .fuse()
            .boxed()
    }

    /// Advances the decoder state machine until the next [`RecordBatch`] is
    /// produced, the file is fully consumed, or an error occurs.
    ///
    /// On each iteration the decoder is polled via [`ParquetPushDecoder::try_decode`]:
    /// - [`NeedsData`](DecodeResult::NeedsData) – the requested byte ranges are
    ///   fetched from the [`AsyncFileReader`] and fed back into the decoder.
    /// - [`Data`](DecodeResult::Data) – a decoded batch is projected and returned.
    /// - [`Finished`](DecodeResult::Finished) – signals end-of-stream (`None`).
    ///
    /// Takes `self` by value (rather than `&mut self`) so the generated future
    /// owns the state directly. This avoids a Stacked Borrows violation under
    /// miri where `&mut self` creates a single opaque borrow that conflicts
    /// with `unfold`'s ownership across yield points.
    async fn transition(mut self) -> Option<(Result<RecordBatch>, Self)> {
        loop {
            // Step 1: drain a batch from the active reader if any.
            if let Some(reader) = self.active_reader.as_mut() {
                match reader.next() {
                    Some(Ok(batch)) => {
                        let mut timer = self.baseline_metrics.elapsed_compute().timer();
                        self.copy_arrow_reader_metrics();
                        let result = self.project_batch(&batch);
                        timer.stop();
                        drop(timer);
                        return Some((result, self));
                    }
                    Some(Err(e)) => {
                        return Some((Err(DataFusionError::from(e)), self));
                    }
                    None => {
                        // Reader exhausted: drop and fall through to per-RG
                        // boundary handling, then try_next_reader.
                        self.active_reader = None;
                    }
                }
            }

            // Step 2: when the decoder is sitting on a row-group boundary,
            // scan the entire `rg_plan` and drop every RG the pruner proves
            // cannot contribute — head, interior, and tail alike. Evaluating
            // per-RG stats against the cached `PruningPredicate` is cheap;
            // the expensive part is the `into_builder` rebuild, so we do at
            // most one rebuild per boundary regardless of how many RGs were
            // dropped. Buffered bytes for already-fetched RGs carry across
            // the rebuild.
            //
            // `into_builder` errors out mid-row-group, so we gate the prune
            // pass on `is_at_row_group_boundary()`. When the decoder is
            // mid-RG (e.g. byte ranges have been pushed but no reader has
            // been handed back yet), step 3 drives it forward and we get
            // another chance at the next boundary — the pruner is stateful
            // and idempotent, so deferring loses nothing.
            let at_boundary = self
                .decoder
                .as_ref()
                .expect("decoder present")
                .is_at_row_group_boundary();
            if at_boundary && !self.rg_plan.is_empty() {
                let mut pruned_count = 0usize;
                if let Some(pruner) = self.row_group_pruner.as_mut() {
                    let mut kept = VecDeque::with_capacity(self.rg_plan.len());
                    while let Some(entry) = self.rg_plan.pop_front() {
                        if pruner.should_prune(&[entry.rg_index]) {
                            pruned_count += 1;
                            self.row_groups_pruned_dynamic.add(1);
                        } else {
                            kept.push_back(entry);
                        }
                    }
                    self.rg_plan = kept;
                }
                if pruned_count > 0 {
                    if self.rg_plan.is_empty() {
                        return None;
                    }
                    let decoder = self.decoder.take().expect("decoder present");
                    let new_indices: Vec<usize> =
                        self.rg_plan.iter().map(|e| e.rg_index).collect();
                    let rebuilt = match decoder.into_builder() {
                        Ok(b) => b.with_row_groups(new_indices).build(),
                        Err(e) => Err(e),
                    };
                    match rebuilt {
                        Ok(d) => self.decoder = Some(d),
                        Err(e) => {
                            return Some((Err(DataFusionError::from(e)), self));
                        }
                    }
                }
            }

            // Step 3: drive the decoder.
            let decoder = self.decoder.as_mut().expect("decoder present");
            match decoder.try_next_reader() {
                Ok(DecodeResult::NeedsData(mut ranges)) => {
                    // EXPERIMENT: if a background prefetch is in flight,
                    // land it first — it may cover (part of) the request.
                    if matches!(self.reader, ReaderSlot::Busy(_)) {
                        let ReaderSlot::Busy(handle) =
                            std::mem::replace(&mut self.reader, ReaderSlot::Empty)
                        else {
                            unreachable!()
                        };
                        let (reader, fetched_ranges, result) = match handle.await {
                            Ok(v) => v,
                            Err(e) => {
                                return Some((
                                    Err(DataFusionError::External(Box::new(e))),
                                    self,
                                ));
                            }
                        };
                        self.reader = ReaderSlot::Idle(reader);
                        match result {
                            Ok(data) => {
                                let decoder =
                                    self.decoder.as_mut().expect("decoder present");
                                if let Err(e) = decoder.push_ranges(fetched_ranges, data)
                                {
                                    return Some((Err(DataFusionError::from(e)), self));
                                }
                                note_staged_bytes(decoder);
                            }
                            Err(e) => {
                                return Some((Err(DataFusionError::from(e)), self));
                            }
                        }
                        // Re-poll the decoder: the prefetched data may have
                        // satisfied this request entirely.
                        continue;
                    }

                    // EXPERIMENT: batched policy (PR #23492) — extend the
                    // blocking fetch with upcoming row groups' ranges.
                    if let FetchPolicy::Batched { budget } = self.fetch_policy {
                        let buffered = self
                            .decoder
                            .as_ref()
                            .expect("decoder present")
                            .buffered_bytes();
                        let required: u64 = ranges.iter().map(|r| r.end - r.start).sum();
                        ranges.extend(upcoming_row_group_ranges(
                            self.rg_plan.iter().skip(1),
                            self.decoder_projection.projection_mask(),
                            &self.parquet_metadata,
                            &mut self.prefetched_row_groups,
                            buffered.saturating_add(required),
                            budget,
                        ));
                    }

                    let ReaderSlot::Idle(reader) = &mut self.reader else {
                        unreachable!("reader is idle here")
                    };
                    let data = reader
                        .get_byte_ranges(ranges.clone())
                        .await
                        .map_err(DataFusionError::from);
                    match data {
                        Ok(data) => {
                            let decoder = self.decoder.as_mut().expect("decoder present");
                            if let Err(e) = decoder.push_ranges(ranges, data) {
                                return Some((Err(DataFusionError::from(e)), self));
                            }
                            note_staged_bytes(decoder);
                        }
                        Err(e) => return Some((Err(e), self)),
                    }
                }
                Ok(DecodeResult::Data(reader)) => {
                    // Pop the RG this reader is for (we already filtered
                    // pruned ones in step 2, so `rg_plan.front()` is the RG
                    // the decoder is about to read).
                    self.rg_plan.pop_front();
                    self.active_reader = Some(reader);

                    // EXPERIMENT: pipelined policy — while this RG decodes,
                    // fetch upcoming RGs' projected ranges in the background
                    // within the byte budget.
                    if let FetchPolicy::Pipelined { budget } = self.fetch_policy
                        && matches!(self.reader, ReaderSlot::Idle(_))
                    {
                        let buffered = self
                            .decoder
                            .as_ref()
                            .expect("decoder present")
                            .buffered_bytes();
                        // Hysteresis: without this, once the budget is
                        // mostly full of staged data every spawn only has
                        // ~one RG of headroom and the fetch degrades to
                        // one round trip per RG. Waiting until at least
                        // half the budget is free keeps gulps large. The
                        // final row groups are always eligible so the
                        // tail doesn't stall.
                        let headroom = budget.saturating_sub(buffered);
                        let remaining_bytes: u64 = self
                            .rg_plan
                            .iter()
                            .filter(|e| !self.prefetched_row_groups.contains(&e.rg_index))
                            .map(|e| {
                                let rg = self.parquet_metadata.row_group(e.rg_index);
                                rg.columns()
                                    .iter()
                                    .enumerate()
                                    .filter(|(i, _)| {
                                        self.decoder_projection
                                            .projection_mask()
                                            .leaf_included(*i)
                                    })
                                    .map(|(_, c)| c.byte_range().1)
                                    .sum::<u64>()
                            })
                            .sum();
                        if headroom < (budget / 2).min(remaining_bytes) {
                            continue;
                        }
                        let prefetch_ranges = upcoming_row_group_ranges(
                            self.rg_plan.iter(),
                            self.decoder_projection.projection_mask(),
                            &self.parquet_metadata,
                            &mut self.prefetched_row_groups,
                            buffered,
                            budget,
                        );
                        if !prefetch_ranges.is_empty() {
                            let ReaderSlot::Idle(mut reader) =
                                std::mem::replace(&mut self.reader, ReaderSlot::Empty)
                            else {
                                unreachable!()
                            };
                            // POC: a raw tokio JoinHandle so the prefetch
                            // detaches (runs to completion) if the stream
                            // is dropped mid-fetch; a mergeable version
                            // would use `SpawnedTask` for cancel-safety.
                            #[expect(clippy::disallowed_methods)]
                            let handle = tokio::task::spawn(async move {
                                let result =
                                    reader.get_byte_ranges(prefetch_ranges.clone()).await;
                                (reader, prefetch_ranges, result)
                            });
                            self.reader = ReaderSlot::Busy(handle);
                        }
                    }
                }
                Ok(DecodeResult::Finished) => return None,
                Err(e) => {
                    return Some((Err(DataFusionError::from(e)), self));
                }
            }
        }
    }

    /// Copies metrics from ArrowReaderMetrics (the metrics collected by the
    /// arrow-rs parquet reader) to the parquet file metrics for DataFusion
    fn copy_arrow_reader_metrics(&self) {
        if let Some(v) = self.arrow_reader_metrics.records_read_from_inner() {
            self.predicate_cache_inner_records.set(v);
        }
        if let Some(v) = self.arrow_reader_metrics.records_read_from_cache() {
            self.predicate_cache_records.set(v);
        }
    }

    fn project_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        self.decoder_projection.map(batch)
    }
}

// ===========================================================================
// EXPERIMENT: `FetchPolicy::Streaming` — batch-granular readiness.
//
// One long-lived *sync* `ParquetRecordBatchReader` pulls bytes through
// [`SharedBuffers`] (an in-memory `ChunkReader`). The stream driver computes
// from the offset index exactly which page ranges the next batch needs,
// awaits their fetch (with up to `window` bytes of background readahead),
// then calls `next()` — which therefore never blocks on I/O. Dictionary
// pages are fetched and decoded once per row group (the reader persists),
// and page bytes are dropped as soon as the decode cursor passes them, so
// resident memory is bounded by the readahead window rather than row-group
// size.
// ===========================================================================

/// In-memory byte store shared between the fetch side (inserts ranges as
/// they land) and the sync parquet reader (reads through `ChunkReader`).
/// Reads must be fully contained in a previously inserted range; the stream
/// driver guarantees this by construction, so a miss is a bug, not a wait.
#[derive(Clone)]
pub(crate) struct SharedBuffers {
    inner: Arc<std::sync::Mutex<std::collections::BTreeMap<u64, Bytes>>>,
    file_len: u64,
}

impl SharedBuffers {
    fn new(file_len: u64) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(Default::default())),
            file_len,
        }
    }

    fn insert(&self, range: &Range<u64>, data: Bytes) {
        self.inner.lock().unwrap().insert(range.start, data);
    }

    fn remove(&self, start: u64) {
        self.inner.lock().unwrap().remove(&start);
    }

    /// Whether the page starting at `start` is already staged.
    fn contains(&self, start: u64) -> bool {
        self.inner.lock().unwrap().contains_key(&start)
    }
}

impl Length for SharedBuffers {
    fn len(&self) -> u64 {
        self.file_len
    }
}

pub(crate) struct SharedBuffersRead {
    buffers: SharedBuffers,
    pos: u64,
}

impl std::io::Read for SharedBuffersRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = buf.len().min((self.buffers.file_len - self.pos) as usize);
        if n == 0 {
            return Ok(0);
        }
        let bytes = self
            .buffers
            .get_bytes(self.pos, n)
            .map_err(std::io::Error::other)?;
        buf[..n].copy_from_slice(&bytes);
        self.pos += n as u64;
        Ok(n)
    }
}

impl ChunkReader for SharedBuffers {
    type T = SharedBuffersRead;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(SharedBuffersRead {
            buffers: self.clone(),
            pos: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let guard = self.inner.lock().unwrap();
        if let Some((&rstart, bytes)) = guard.range(..=start).next_back() {
            let offset = start - rstart;
            if offset as usize + length <= bytes.len() {
                return Ok(bytes.slice(offset as usize..offset as usize + length));
            }
        }
        Err(ParquetError::General(format!(
            "streaming scan buffer miss: {start}..{} not resident",
            start + length as u64
        )))
    }
}

/// A planned page plus the stream's eviction bookkeeping.
///
/// The page itself (byte range + the span of selected rows it serves) comes
/// from arrow-rs's [`plan_scan_ranges`]; the only thing DataFusion adds is
/// whether the decode cursor has passed it and its bytes were released.
struct PlanPage {
    planned: PlannedRange,
    cleared: bool,
}

impl PlanPage {
    fn range(&self) -> &Range<u64> {
        &self.planned.range
    }
}

/// Opaque prebuilt streaming fetch plan.
pub(crate) struct StreamingPlan {
    pages: Vec<PlanPage>,
    total_selected: u64,
    file_end: u64,
}

/// Build the streaming fetch plan by asking arrow-rs which pages this scan
/// will read, in the order decoding needs them.
///
/// Returns `None` when arrow-rs cannot plan at page granularity (no offset
/// index) — the caller falls back to the push-decoder path. Borrows only, so
/// callers can probe feasibility before committing resources.
pub(crate) fn build_streaming_plan(
    metadata: &ParquetMetaData,
    row_group_indexes: &[usize],
    projection: &ProjectionMask,
    selection: Option<&RowSelection>,
) -> Option<StreamingPlan> {
    let plan = plan_scan_ranges(metadata, row_group_indexes, projection, selection)?;
    // `SharedBuffers` reports a file length to the sync reader; the end of the
    // last projected column chunk is an upper bound on anything it will read.
    let file_end = row_group_indexes
        .iter()
        .flat_map(|&rg| {
            metadata.row_group(rg).columns().iter().map(|c| {
                let (start, len) = c.byte_range();
                start + len
            })
        })
        .max()
        .unwrap_or(0);
    Some(StreamingPlan {
        total_selected: plan.total_selected_rows,
        pages: plan
            .ranges
            .into_iter()
            .map(|planned| PlanPage {
                planned,
                cleared: false,
            })
            .collect(),
        file_end,
    })
}

pub(crate) struct StreamingScanConfig {
    pub reader_metadata: ArrowReaderMetadata,
    pub row_group_indexes: Vec<usize>,
    pub row_selection: Option<RowSelection>,
    pub decoder_projection: DecoderProjection,
    pub batch_size: usize,
    pub limit: Option<usize>,
    pub reader: Box<dyn AsyncFileReader>,
    pub baseline_metrics: BaselineMetrics,
    pub window: u64,
}

/// Build the streaming (batch-granular) scan stream from a prebuilt plan.
pub(crate) fn build_streaming_stream(
    plan: StreamingPlan,
    config: StreamingScanConfig,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let StreamingScanConfig {
        reader_metadata,
        row_group_indexes,
        row_selection,
        decoder_projection,
        batch_size,
        limit,
        reader,
        baseline_metrics,
        window,
    } = config;
    let StreamingPlan {
        pages: plan,
        total_selected,
        file_end,
    } = plan;

    let buffers = SharedBuffers::new(file_end);
    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
        buffers.clone(),
        reader_metadata,
    )
    .with_projection(decoder_projection.projection_mask().clone())
    .with_batch_size(batch_size)
    .with_row_groups(row_group_indexes);
    if let Some(selection) = row_selection {
        builder = builder.with_row_selection(selection);
    }
    if let Some(limit) = limit {
        builder = builder.with_limit(limit);
    }
    let sync_reader = builder.build()?;

    let total_plan_bytes: u64 = plan.iter().map(|p| p.planned.len()).sum();
    let state = StreamingScanState {
        plan,
        total_plan_bytes,
        total_selected,
        batch_size: batch_size as u64,
        window,
        fetched_idx: 0,
        inflight_start: 0,
        clear_idx: 0,
        resident_bytes: 0,
        cursor: 0,
        buffers,
        slot: ReaderSlot::Idle(reader),
        sync_reader,
        decoder_projection,
        baseline_metrics,
    };
    Ok(
        futures::stream::unfold(state, |state| async move { state.transition().await })
            .fuse()
            .boxed(),
    )
}

pub(crate) struct StreamingScanState {
    plan: Vec<PlanPage>,
    /// Total bytes across all plan pages. Plans that fit the window are
    /// fetched in one wave (see the inline-fetch site) — splitting a small
    /// file's fetch into a required wave plus a readahead wave costs an
    /// extra round trip per file, which dominates many-small-file workloads
    /// (measured: TPC-DS under simulated latency).
    total_plan_bytes: u64,
    total_selected: u64,
    batch_size: u64,
    window: u64,
    /// Plan pages `[0, fetched_idx)` have been requested (resident or in
    /// the single in-flight background fetch).
    fetched_idx: usize,
    /// Start of the in-flight slice when the slot is `Busy`.
    inflight_start: usize,
    /// Scan start for dropping pages the cursor has passed.
    clear_idx: usize,
    resident_bytes: u64,
    /// Selected rows emitted so far.
    cursor: u64,
    buffers: SharedBuffers,
    slot: ReaderSlot,
    sync_reader: ParquetRecordBatchReader,
    decoder_projection: DecoderProjection,
    baseline_metrics: BaselineMetrics,
}

impl StreamingScanState {
    /// First selected row not yet guaranteed decodable: pages whose
    /// `sel_start` is below this must be resident before the next batch.
    fn needed_end(&self) -> u64 {
        (self.cursor + self.batch_size).min(self.total_selected)
    }

    /// Whether any not-yet-landed plan page is required for the next batch.
    fn required_pending(&self) -> bool {
        let needed = self.needed_end();
        let first_unlanded = match self.slot {
            ReaderSlot::Busy(_) => self.inflight_start,
            _ => self.fetched_idx,
        };
        self.plan
            .get(first_unlanded)
            .is_some_and(|p| p.planned.first_row < needed)
    }

    /// Extent of the next fetch starting at `fetched_idx`. When
    /// `required_only`, stop at the pages the next batch needs (keeps the
    /// blocking inline fetch — and therefore time-to-first-batch — minimal);
    /// otherwise extend with readahead while the window has room.
    fn next_gulp_end(&self, required_only: bool) -> usize {
        let needed = self.needed_end();
        let mut bytes = 0u64;
        let mut end = self.fetched_idx;
        while let Some(page) = self.plan.get(end) {
            let len = page.planned.len();
            let required = page.planned.first_row < needed;
            if !required
                && (required_only || self.resident_bytes + bytes + len > self.window)
            {
                break;
            }
            bytes += len;
            end += 1;
        }
        end
    }

    /// Drop resident pages the decode cursor has fully passed.
    fn clear_consumed(&mut self) {
        let landed_end = match self.slot {
            ReaderSlot::Busy(_) => self.inflight_start,
            _ => self.fetched_idx,
        };
        let mut idx = self.clear_idx;
        while idx < landed_end {
            let page = &mut self.plan[idx];
            if page.planned.first_row > self.cursor {
                break;
            }
            if !page.cleared && page.planned.last_row <= self.cursor {
                self.buffers.remove(page.range().start);
                self.resident_bytes -= page.planned.len();
                page.cleared = true;
            }
            idx += 1;
        }
        while self
            .plan
            .get(self.clear_idx)
            .is_some_and(|page| page.cleared)
        {
            self.clear_idx += 1;
        }
    }

    /// Byte ranges this wave still needs: the plan pages in
    /// `[start_idx, end_idx)` not already staged.
    ///
    /// Deliberately no range merging. `ObjectStore::get_ranges` already
    /// coalesces (1MB gap) for every store using the default implementation —
    /// S3, GCS, Azure — while `LocalFileSystem` and friends override it and
    /// coalesce not at all. A second pass here can therefore only raise the
    /// effective threshold, never lower it, and it merges without knowing the
    /// medium. Measurement agreed: a 4MB gap merged away 59 requests on
    /// ClickBench but pulled 157MB of unprojected columns with them and ran
    /// slower. The merge decision belongs to the layer that knows its own
    /// round-trip cost.
    fn wave_ranges(&self, start_idx: usize, end_idx: usize) -> Vec<Range<u64>> {
        let mut ranges: Vec<Range<u64>> = self.plan[start_idx..end_idx]
            .iter()
            .filter(|p| !self.buffers.contains(p.range().start))
            .map(|p| p.range().clone())
            .collect();
        ranges.sort_by_key(|r| r.start);
        ranges
    }

    /// Stage a landed wave. Each fetched range is exactly one plan page.
    fn install_wave(&mut self, ranges: &[Range<u64>], data: &[Bytes]) {
        for (range, bytes) in ranges.iter().zip(data) {
            self.resident_bytes += range.end - range.start;
            self.buffers.insert(range, bytes.clone());
        }
        PEAK_STAGED_BYTES
            .fetch_max(self.resident_bytes, std::sync::atomic::Ordering::Relaxed);
    }

    async fn transition(mut self) -> Option<(Result<RecordBatch>, Self)> {
        loop {
            // 1. Land the in-flight fetch when the next batch needs it (or
            //    when there is nothing left to decode without it).
            if self.required_pending() {
                match std::mem::replace(&mut self.slot, ReaderSlot::Empty) {
                    ReaderSlot::Busy(handle) => {
                        let (reader, ranges, result) = match handle.await {
                            Ok(v) => v,
                            Err(e) => {
                                return Some((
                                    Err(DataFusionError::External(Box::new(e))),
                                    self,
                                ));
                            }
                        };
                        self.slot = ReaderSlot::Idle(reader);
                        match result {
                            Ok(data) => {
                                self.install_wave(&ranges, &data);
                            }
                            Err(e) => {
                                return Some((Err(DataFusionError::from(e)), self));
                            }
                        }
                        continue;
                    }
                    ReaderSlot::Idle(mut reader) => {
                        // When the whole plan fits the readahead window
                        // (small files), fetch it in a single wave — the
                        // extra round trip of a required-only wave would
                        // dominate. For plans larger than the window, fetch
                        // only what the next batch requires so the blocking
                        // wave — and therefore time-to-first-batch — stays
                        // small; readahead happens in the background
                        // (step 2).
                        // "Fill the cart": a blocking wave is a round trip
                        // we pay either way, so extend it with readahead up
                        // to the window — EXCEPT the file's very first wave
                        // of a larger-than-window plan, which stays
                        // required-only so time-to-first-batch tracks the
                        // first pages rather than the window.
                        let required_only =
                            self.fetched_idx == 0 && self.total_plan_bytes > self.window;
                        let end = self.next_gulp_end(required_only);
                        let ranges = self.wave_ranges(self.fetched_idx, end);
                        let result = reader.get_byte_ranges(ranges.clone()).await;
                        self.slot = ReaderSlot::Idle(reader);
                        match result {
                            Ok(data) => {
                                self.install_wave(&ranges, &data);
                                self.fetched_idx = end;
                            }
                            Err(e) => {
                                return Some((Err(DataFusionError::from(e)), self));
                            }
                        }
                        continue;
                    }
                    ReaderSlot::Empty => unreachable!("slot never left empty"),
                }
            }

            // 2. Required data resident: start background readahead when the
            //    slot is idle and at least half the window is free (or the
            //    tail is all that remains).
            if matches!(self.slot, ReaderSlot::Idle(_))
                && self.fetched_idx < self.plan.len()
            {
                let end = self.next_gulp_end(false);
                let gulp_bytes: u64 = self.plan[self.fetched_idx..end]
                    .iter()
                    .map(|p| p.planned.len())
                    .sum();
                let tail = end == self.plan.len();
                if end > self.fetched_idx && (gulp_bytes >= self.window / 2 || tail) {
                    let ranges = self.wave_ranges(self.fetched_idx, end);
                    let ReaderSlot::Idle(mut reader) =
                        std::mem::replace(&mut self.slot, ReaderSlot::Empty)
                    else {
                        unreachable!()
                    };
                    self.inflight_start = self.fetched_idx;
                    self.fetched_idx = end;
                    // The repo's `SpawnedTask` aborts on drop; this POC
                    // documents detach-on-drop semantics instead.
                    #[expect(clippy::disallowed_methods)]
                    let handle = tokio::task::spawn(async move {
                        let result = reader.get_byte_ranges(ranges.clone()).await;
                        (reader, ranges, result)
                    });
                    self.slot = ReaderSlot::Busy(handle);
                }
            }

            // 3. Decode one batch — never blocks: its pages are resident.
            let timer = self.baseline_metrics.elapsed_compute().timer();
            let next = self.sync_reader.next();
            match next {
                Some(Ok(batch)) => {
                    self.cursor += batch.num_rows() as u64;
                    let result = self.decoder_projection.map(&batch);
                    drop(timer);
                    self.clear_consumed();
                    return Some((result, self));
                }
                Some(Err(e)) => {
                    drop(timer);
                    return Some((Err(DataFusionError::from(e)), self));
                }
                None => {
                    drop(timer);
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{
        BinaryExpr, Column, DynamicFilterPhysicalExpr, lit,
    };
    use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::ParquetMetaDataPushDecoder;
    use parquet::file::properties::WriterProperties;

    /// Build a tiny in-memory Parquet file with three row groups whose `v`
    /// column statistics are disjoint: RG0 → 0..1000, RG1 → 1000..2000,
    /// RG2 → 2000..3000. Returns (metadata, schema).
    fn build_three_rg_file() -> (Arc<ParquetMetaData>, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let mut buf = Vec::new();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1000))
            .build();
        let mut writer =
            ArrowWriter::try_new(&mut buf, Arc::clone(&schema), Some(props)).unwrap();
        for rg in 0..3i64 {
            let base = rg * 1000;
            let vals: Vec<i64> = (base..base + 1000).collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vals))],
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();

        let file = Bytes::from(buf);
        let len = file.len() as u64;
        let mut md = ParquetMetaDataPushDecoder::try_new(len).unwrap();
        // One range covering the whole file. Using `expect` rather than
        // `allow` per this crate's `clippy::allow-attributes` lint.
        #[expect(
            clippy::single_range_in_vec_init,
            reason = "we want a single range covering the whole file"
        )]
        let ranges = vec![0..len];
        md.push_ranges(ranges, vec![file]).unwrap();
        let DecodeResult::Data(meta) = md.try_decode().unwrap() else {
            panic!("decoding metadata");
        };
        assert_eq!(meta.num_row_groups(), 3, "test fixture must have 3 RGs");
        (Arc::new(meta), schema)
    }

    /// Create a fresh `(creation_errors, evaluation_errors)` counter pair
    /// for tests. The names mirror the two metrics
    /// [`RowGroupPruner::new`] consumes — predicate construction is
    /// accounted separately from per-row-group evaluation.
    fn pruner_error_counters() -> (Count, Count) {
        let metrics = ExecutionPlanMetricsSet::new();
        let creation =
            MetricBuilder::new(&metrics).counter("num_predicate_creation_errors", 0);
        let evaluation =
            MetricBuilder::new(&metrics).counter("predicate_evaluation_errors", 0);
        (creation, evaluation)
    }

    /// `v > literal` predicate on a single-column schema.
    fn gt_predicate(threshold: i64) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("v", 0)),
            Operator::Gt,
            lit(ScalarValue::Int64(Some(threshold))),
        ))
    }

    #[test]
    fn row_group_pruner_skips_only_disqualified_row_groups() {
        let (meta, schema) = build_three_rg_file();
        let (creation, evaluation) = pruner_error_counters();
        let mut pruner = RowGroupPruner::new(
            gt_predicate(1500),
            Arc::clone(&schema),
            Arc::clone(&meta),
            creation,
            evaluation,
        );

        // RG0 (0..1000) is entirely below threshold → fully prunable.
        assert!(pruner.should_prune(&[0]), "RG0 should be pruned");
        // RG1 (1000..2000) straddles the threshold → not safe to prune.
        assert!(!pruner.should_prune(&[1]), "RG1 must NOT be pruned");
        // RG2 (2000..3000) is entirely above threshold → keep.
        assert!(!pruner.should_prune(&[2]), "RG2 must NOT be pruned");
        // Run covering both RG0 and RG1 cannot be skipped — RG1 is alive.
        assert!(
            !pruner.should_prune(&[0, 1]),
            "mixed run with a live RG must NOT be pruned"
        );
        // Empty input is a no-op (defensive guard).
        assert!(!pruner.should_prune(&[]));
    }

    #[test]
    fn row_group_pruner_tracks_dynamic_filter_updates() {
        let (meta, schema) = build_three_rg_file();
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("v", 0))],
            gt_predicate(500),
        ));
        let (creation, evaluation) = pruner_error_counters();
        let mut pruner = RowGroupPruner::new(
            Arc::clone(&dynamic) as Arc<dyn PhysicalExpr>,
            Arc::clone(&schema),
            Arc::clone(&meta),
            creation,
            evaluation,
        );

        // Initial threshold 500 → only the lower half of RG0 fails, so RG0
        // (0..1000) straddles the threshold and stays alive.
        assert!(!pruner.should_prune(&[0]));
        assert!(!pruner.should_prune(&[1]));

        // Tighten the threshold via the dynamic filter — TopK fills its
        // heap and updates the threshold to 2500.
        dynamic
            .update(gt_predicate(2500))
            .expect("update threshold");

        // After the update the pruner must rebuild its `PruningPredicate`
        // (driven by the `DynamicFilterTracker`'s change notification) and
        // re-evaluate. RG0 and RG1 are both entirely below 2500 now.
        assert!(
            pruner.should_prune(&[0]),
            "RG0 must be pruned after threshold tightens to 2500"
        );
        assert!(
            pruner.should_prune(&[1]),
            "RG1 must be pruned after threshold tightens to 2500"
        );
        assert!(
            !pruner.should_prune(&[2]),
            "RG2 (2000..3000) still straddles 2500"
        );
    }

    #[test]
    fn row_group_pruner_falls_back_to_conservative_when_predicate_has_no_bounds() {
        // A predicate the pruning analyzer can't decompose (e.g. a bare
        // column reference of bool type would normally be valid, but a
        // non-binary expression on a non-bool column doesn't yield bounds).
        // We use `lit(true)` which produces no column references, so
        // `build_pruning_predicate` will return None.
        let (meta, schema) = build_three_rg_file();
        let (creation, evaluation) = pruner_error_counters();
        let mut pruner = RowGroupPruner::new(
            lit(true) as Arc<dyn PhysicalExpr>,
            Arc::clone(&schema),
            Arc::clone(&meta),
            creation,
            evaluation,
        );
        // No pruning predicate could be built → conservatively keep RGs.
        assert!(!pruner.should_prune(&[0]));
        assert!(!pruner.should_prune(&[1]));
        assert!(!pruner.should_prune(&[2]));
    }
}
