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
//!   [`ParquetPushDecoderBuilder::with_row_group_selections`] so the skipped RGs are
//!   bypassed entirely — no decode, no row-filter eval.
//!
//! The opener constructs both halves and hands the state off to
//! [`PushDecoderStreamState::into_stream`] for consumption.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::compute::BatchCoalescer;
use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use futures::stream::BoxStream;
use log::debug;
use parquet::DecodeResult;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, RowFilter, RowSelectionPolicy,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::push_decoder::{
    ParquetPushDecoder, ParquetPushDecoderBuilder, RowGroupSelection,
};
use parquet::file::metadata::ParquetMetaData;

use datafusion_common::instant::Instant;
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_physical_expr::expressions::DynamicFilterTracking;
use datafusion_physical_expr::utils::split_optional;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::metrics::{BaselineMetrics, Count, Gauge};
use datafusion_pruning::{PruningPredicate, PruningPredicateBuilder};

use crate::ParquetFileMetrics;
use crate::decoder_projection::{
    DecoderProjection, DecoderProjectionBuilder, PostScanSelection,
};
use crate::filter_placement::FilePlacement;
use crate::metrics::{ByteProgress, RowFilterSkippedFullyMatchedMetric};
use crate::optional_filter::OptionalFilterSavings;
use crate::row_filter::{
    OptionalFilterRowFilterContext, PrebuiltRowFilterCandidate,
    prebuild_row_filter_candidates, row_filter_from_prebuilt,
};
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
    /// Build a [`ParquetPushDecoderBuilder`] from row-group-local selections.
    ///
    /// The caller is expected to attach the
    /// [`RowFilter`] and predicate
    /// cache size on the returned builder.
    pub(crate) fn build(
        &self,
        row_group_selections: Vec<RowGroupSelection>,
        metadata: ArrowReaderMetadata,
    ) -> ParquetPushDecoderBuilder {
        let mut builder = ParquetPushDecoderBuilder::new_with_metadata(metadata)
            .with_projection(self.projection_mask.clone())
            .with_batch_size(self.batch_size)
            .with_metrics(self.arrow_reader_metrics.clone());
        if self.force_filter_selections {
            builder = builder.with_row_selection_policy(RowSelectionPolicy::Selectors);
        }
        builder = builder.with_row_group_selections(row_group_selections);
        if let Some(limit) = self.decoder_limit {
            builder = builder.with_limit(limit);
        }
        builder
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RgPlanEntry {
    pub(crate) rg_index: usize,
    /// `true` when static pruning proved every row of this RG satisfies the
    /// predicate, so the per-row `RowFilter` can be skipped as a no-op.
    pub(crate) fully_matched: bool,
    /// On-disk size of this row group, credited to the `bytes_processed` metric documented on
    /// [`ParquetFileMetrics`] once the scan is done with it.
    ///
    /// [`ParquetFileMetrics`]: crate::ParquetFileMetrics
    pub(crate) bytes: u64,
}

/// The initial per-file decoder state the opener builds and hands off to the
/// [`PushDecoderStreamState`] stream driver.
///
/// Named rather than a bare tuple so the fields carried out of the decoder
/// setup block stay self-documenting as more are added.
pub(crate) struct InitialDecoderState {
    /// The freshly built push decoder for this file.
    pub(crate) decoder: ParquetPushDecoder,
    /// The per-row-group plan, in the physical scan order the decoder reads.
    pub(crate) rg_plan: VecDeque<RgPlanEntry>,
    /// Whether a row selection is live for this scan. Runtime row-group
    /// pruning is disabled when it is (see the opener for why).
    pub(crate) has_row_selection: bool,
    /// Whether the freshly built decoder carries a real (non-empty)
    /// [`RowFilter`]. `false` when the first row group is fully matched and
    /// the filter was suppressed at open time.
    pub(crate) filter_installed: bool,
    /// Cache that lets the stream rebuild the [`RowFilter`] at later
    /// row-group boundaries. `None` when the scan has no pushdown predicate.
    pub(crate) row_filter_context: Option<RowFilterContext>,
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
    /// Cap on the `IN (...)` list size that the pruning predicate will
    /// rewrite into per-value statistics checks. Longer lists skip
    /// container-level pruning. Sourced from
    /// `datafusion.execution.parquet.max_in_list_size`.
    max_in_list_size: usize,
}

impl RowGroupPruner {
    pub(crate) fn new(
        predicate: Arc<dyn PhysicalExpr>,
        arrow_schema: SchemaRef,
        parquet_metadata: Arc<ParquetMetaData>,
        predicate_creation_errors: Count,
        predicate_evaluation_errors: Count,
        max_in_list_size: usize,
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
            max_in_list_size,
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
            self.pruning_predicate = PruningPredicateBuilder::new()
                .with_file_schema(Arc::clone(&self.arrow_schema))
                .with_error_counter(&self.predicate_creation_errors)
                .with_max_in_list_size(self.max_in_list_size)
                .build(Arc::clone(&self.predicate));
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
            column_orders: self
                .parquet_metadata
                .file_metadata()
                .column_orders()
                .map(Vec::as_slice),
            row_group_metadatas,
            arrow_schema: self.arrow_schema.as_ref(),
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
    pub(crate) reader: Box<dyn AsyncFileReader>,
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
    /// [`ParquetPushDecoderBuilder::with_row_group_selections`] so the skipped RGs are
    /// bypassed entirely. `None` when the scan has no watching dynamic
    /// predicate or only one row group remains.
    pub(crate) row_group_pruner: Option<RowGroupPruner>,
    /// Count of row groups skipped at runtime by [`Self::row_group_pruner`].
    pub(crate) row_groups_pruned_dynamic: Count,
    /// Cache that lets the per-RG `fully_matched` toggle reinstall the
    /// parquet [`RowFilter`] when it flips from skip → install. `None` when
    /// the scan has no pushdown predicate (the toggle is then a no-op).
    pub(crate) row_filter_context: Option<RowFilterContext>,
    /// Whether the currently-installed decoder is running with a non-empty
    /// row filter. Toggled per RG by the `fully_matched` skip path.
    pub(crate) filter_installed: bool,
    /// Lazily-registered counter of suppression events for the per-row
    /// [`RowFilter`] (registered on first use so scans that never suppress
    /// don't carry a zero-valued counter).
    pub(crate) row_filter_skipped_fully_matched: RowFilterSkippedFullyMatchedMetric,
    /// How much of this file range the scan has finished with. Credited a row
    /// group at a time as they are decoded or skipped, and topped up to the
    /// full range when the stream is dropped.
    pub(crate) byte_progress: ByteProgress,
    /// Stream-level remaining row limit, enforced *after* the post-scan
    /// filter. `Some` only when the file has a post-scan filter (which makes
    /// the decoder-local `with_limit` unsafe — the decoder would short-circuit
    /// before the filter rejects enough rows); `None` otherwise, in which case
    /// the limit is enforced inside the decoder via `DecoderBuilderConfig`.
    pub(crate) remaining_limit: Option<usize>,
    /// Reassembles post-filter batches back to the target batch size.
    ///
    /// `Some` exactly when the file has a post-scan filter. A selective
    /// predicate leaves only a handful of rows per decoded batch (TPC-H q3
    /// yields ~41 rows from each 8192-row batch), and without this every one
    /// of those slivers would be handed to the operator above as its own
    /// batch. `FilterExec` coalesces for the same reason. `None` when there is
    /// no post-scan filter: decoder batches are already full size, so routing
    /// them through the coalescer would only add a copy.
    pub(crate) batch_coalescer: Option<BatchCoalescer>,
    /// Set once [`BatchCoalescer::finish_buffered_batch`] has been called, so
    /// end-of-input flushing happens exactly once no matter which terminal
    /// path reached it.
    pub(crate) flushed: bool,
    /// True if the partial batch in [`Self::batch_coalescer`] is handed
    /// downstream at each row group boundary, before the runtime row group
    /// pruning for the next row groups.
    ///
    /// Set when the predicate has a dynamic filter that can still change.
    /// Its producer (for example a TopK) can tighten the filter only after it
    /// sees rows. If the coalescer held the rows of small row groups until it
    /// has `batch_size` rows, the producer would see no rows until the end of
    /// the file, and the scan could not prune any row group with the filter.
    pub(crate) flush_at_row_group_boundary: bool,
    /// Builds a new [`DecoderProjection`] when the adaptive filter placement
    /// changes the post-scan conjuncts at a row group boundary. `Some`
    /// exactly when [`RowFilterContext::placement`] is `Some`.
    pub(crate) projection_builder: Option<DecoderProjectionBuilder>,
}

/// A reusable, `Arc`-shared list of prebuilt row-filter candidates.
///
/// Wrapping the `Arc<Vec<_>>` keeps the "prebuilt candidates" concept behind
/// a named type and makes cloning it into stream state cheap.
#[derive(Clone)]
pub(crate) struct PrebuiltRowFilterCandidateList {
    inner: Arc<Vec<PrebuiltRowFilterCandidate>>,
}

impl PrebuiltRowFilterCandidateList {
    fn new(candidates: Vec<PrebuiltRowFilterCandidate>) -> Self {
        Self {
            inner: Arc::new(candidates),
        }
    }

    fn as_slice(&self) -> &[PrebuiltRowFilterCandidate] {
        &self.inner
    }
}

/// Cache that lets [`PushDecoderStreamState`] rebuild the parquet
/// [`RowFilter`] mid-scan: it keeps the prebuilt candidate list alongside the
/// stream so a non-fully-matched row group can be re-wrapped into a fresh
/// [`RowFilter`] without redoing the tree walks and column resolution the
/// initial build did.
pub(crate) struct RowFilterContext {
    /// Prebuilt candidates: expression already column-reassigned, projection
    /// mask already resolved. Shared across the file's row groups.
    pub(crate) prebuilt: PrebuiltRowFilterCandidateList,
    pub(crate) reorder_predicates: bool,
    pub(crate) file_metrics: ParquetFileMetrics,
    pub(crate) max_predicate_cache_size: Option<usize>,
    /// Measures the decode time of the output batches for the gates of the
    /// optional filters. `None` if the file has no gated optional filter.
    pub(crate) optional_savings: Option<OptionalFilterSavings>,
    /// The adaptive placement of the conjuncts (see
    /// [`crate::filter_placement`]). `None` when it is disabled or when no
    /// conjunct needs a decision.
    pub(crate) placement: Option<FilePlacement>,
    /// See [`Self::start_reader`].
    decode_measurement: DecodeMeasurement,
}

/// Which output batches measure the decode speed, see
/// [`RowFilterContext::start_reader`].
struct DecodeMeasurement {
    /// Value of `pushdown_rows_pruned` when the decoder handed out the last
    /// reader.
    rows_pruned: usize,
    /// True if the row filter removed no rows of the row group of the
    /// current reader.
    unfiltered: bool,
}

impl RowFilterContext {
    /// Precompute the candidate list from the raw predicate + file schema +
    /// metadata.
    ///
    /// The first element is `None` when the predicate has no push-downable
    /// conjuncts (mirrors the file-open path behaviour). The second element
    /// holds the conjuncts the `RowFilter` machinery could not place on this
    /// file. The caller must evaluate them elsewhere (post-scan), otherwise
    /// the predicate is silently relaxed. On a whole-file build error every
    /// conjunct is returned in that list rather than being dropped.
    pub(crate) fn try_new(
        predicate: &Arc<dyn PhysicalExpr>,
        physical_file_schema: &SchemaRef,
        file_metadata: &Arc<ParquetMetaData>,
        reorder_predicates: bool,
        file_metrics: ParquetFileMetrics,
        max_predicate_cache_size: Option<usize>,
        optional: Option<OptionalFilterRowFilterContext<'_>>,
    ) -> (Option<Self>, Vec<Arc<dyn PhysicalExpr>>) {
        match prebuild_row_filter_candidates(
            predicate,
            physical_file_schema,
            file_metadata.as_ref(),
            optional,
        ) {
            Ok((prebuilt, rejected)) => {
                let context = prebuilt.map(|prebuilt| {
                    let optional_savings = optional.and_then(|optional| {
                        OptionalFilterSavings::try_new(
                            Arc::clone(&optional.options.decode_cost),
                            file_metadata,
                            optional.output_projection?,
                            prebuilt
                                .iter()
                                .filter_map(|c| c.optional_saving().cloned())
                                .collect(),
                        )
                    });
                    let decode_measurement = DecodeMeasurement {
                        rows_pruned: file_metrics.pushdown_rows_pruned.value(),
                        unfiltered: true,
                    };
                    Self {
                        prebuilt: PrebuiltRowFilterCandidateList::new(prebuilt),
                        reorder_predicates,
                        file_metrics,
                        max_predicate_cache_size,
                        optional_savings,
                        placement: None,
                        decode_measurement,
                    }
                });
                (context, rejected)
            }
            Err(e) => {
                // Whole-file build failure: route every required conjunct
                // post-scan rather than silently dropping the predicate.
                // Optional conjuncts are not needed for correctness, thus
                // they are not evaluated after the scan.
                debug!(
                    "Ignoring error prebuilding row filter candidates: {e}; \
                     all required conjuncts will be evaluated post-scan"
                );
                (None, split_optional(predicate).0)
            }
        }
    }

    /// Adds the adaptive filter placement that `make` returns. `make` gets
    /// the prebuilt candidates, before any `RowFilter` is built from them.
    pub(crate) fn with_placement(
        mut self,
        make: impl FnOnce(&mut [PrebuiltRowFilterCandidate]) -> Option<FilePlacement>,
    ) -> Self {
        let candidates = Arc::get_mut(&mut self.prebuilt.inner)
            .expect("no RowFilter was built from the candidates yet");
        self.placement = make(candidates);
        self
    }

    /// The adaptive filter placement of the file, if any.
    pub(crate) fn placement(&self) -> Option<&FilePlacement> {
        self.placement.as_ref()
    }

    /// Call when the decoder hands out the reader of the next row group. The
    /// decoder evaluated the row filter of the row group before: if the row
    /// filter removed rows, the decode time of the output batches of the
    /// reader is not measured. A row filter that removes rows spread over
    /// the row group makes each decoded output row much more expensive
    /// (the decoder decodes all pages and drops most rows), thus the
    /// measured decode speed would be too slow, and the saving of a removed
    /// row too large (see [`crate::optional_filter`]).
    pub(crate) fn start_reader(&mut self) {
        let rows_pruned = self.file_metrics.pushdown_rows_pruned.value();
        let measurement = &mut self.decode_measurement;
        measurement.unfiltered = rows_pruned == measurement.rows_pruned;
        measurement.rows_pruned = rows_pruned;
    }

    /// True if [`Self::record_output_batch`] needs the decode time of the
    /// output batches of the current reader.
    pub(crate) fn measures_decode(&self) -> bool {
        (self.optional_savings.is_some() || self.placement.is_some())
            && self.decode_measurement.unfiltered
    }

    /// Records that the decoder produced an output batch of `rows` rows in
    /// `elapsed`, for the gates of the optional filters and for the adaptive
    /// filter placement.
    pub(crate) fn record_output_batch(&self, rows: usize, elapsed: Duration) {
        if let Some(savings) = &self.optional_savings {
            savings.record_output_batch(rows, elapsed);
        }
        if let Some(placement) = &self.placement {
            placement.record_decode(rows, elapsed);
        }
    }

    /// Whether any pushed-down predicate reads this Parquet leaf column.
    pub(crate) fn reads_leaf(&self, leaf_idx: usize) -> bool {
        self.prebuilt
            .as_slice()
            .iter()
            .any(|candidate| candidate.reads_leaf(leaf_idx))
    }

    /// Build a fresh [`RowFilter`] for the next non-fully-matched run using
    /// the cached candidates. Cheap: no tree walks, only counter allocation
    /// and (optionally) a sort by `required_bytes`.
    ///
    /// Infallible by construction: [`Self::try_new`] only produces a context
    /// when the prebuilt candidate list is non-empty.
    ///
    /// With adaptive filter placement, only the candidates that are placed
    /// in the `RowFilter` are used.
    pub(crate) fn build_row_filter(&self) -> RowFilter {
        let placement = self.placement.as_ref();
        row_filter_from_prebuilt(
            self.prebuilt
                .as_slice()
                .iter()
                .enumerate()
                .filter(|(index, _)| placement.is_none_or(|p| p.in_row_filter(*index)))
                .map(|(_, candidate)| candidate),
            self.reorder_predicates,
            &self.file_metrics,
        )
    }
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
        // Everything below is CPU work (decoding, row group pruning, building
        // readers, projection) except fetching byte ranges, so the timer runs
        // for the whole transition and is paused only across that await.
        // Cloning `Time` shares the underlying counter and keeps the guard
        // from borrowing `self`. The guard records on drop, which covers
        // every return.
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let mut timer = elapsed_compute.timer();
        // Once `finish` has flushed the coalescer, the stream only drains it.
        // The decoder can still point at row groups that the plan dropped
        // (for example, when a dynamic filter pruned every remaining row
        // group at a boundary), so it must not be driven again.
        if self.flushed {
            if self.remaining_limit == Some(0) {
                return None;
            }
            return self.emit_completed();
        }
        loop {
            // Hand out anything the coalescer has already assembled into a
            // full-size batch before doing more decoding work.
            if self
                .batch_coalescer
                .as_ref()
                .is_some_and(BatchCoalescer::has_completed_batch)
            {
                return self.emit_completed();
            }

            // The stream-level limit (set only when a post-scan filter made
            // the decoder-local limit unsafe) is exhausted — stop. Anything
            // still buffered is beyond the limit, so it is dropped rather
            // than flushed.
            if self.remaining_limit == Some(0) {
                return None;
            }

            // Step 1: drain a batch from the active reader if any.
            if let Some(reader) = self.active_reader.as_mut() {
                // The gates of optional filters and the adaptive filter
                // placement need the decode time of the output columns (the
                // reader does not evaluate the row filter: the decoder did
                // that before it returned the reader).
                let decode_ctx = self
                    .row_filter_context
                    .as_ref()
                    .filter(|ctx| ctx.measures_decode());
                let start = decode_ctx.map(|_| Instant::now());
                match reader.next() {
                    Some(Ok(batch)) => {
                        if let (Some(ctx), Some(start)) = (decode_ctx, start) {
                            ctx.record_output_batch(batch.num_rows(), start.elapsed());
                        }
                        self.copy_arrow_reader_metrics();

                        // Apply the in-scan post-scan filter (if any). The
                        // decoder's projection mask already covers the
                        // predicate's columns; the filter's compact-once loop
                        // needs them for every conjunct, but once it is done
                        // those the projector does not also read are dropped by
                        // `narrow` before the residual mask is applied, so we
                        // never filter a column just to discard it. Survivors go
                        // into the coalescer rather than straight downstream, so
                        // a selective predicate does not fragment the stream into
                        // slivers; the limit and the projection are applied to
                        // the full-size batches the coalescer hands back.
                        if let Some(filter) = self.decoder_projection.post_scan_filter() {
                            let pushed = filter.evaluate(batch).and_then(|selection| {
                                let (batch, mask) = match selection {
                                    PostScanSelection::Empty => return Ok(()),
                                    PostScanSelection::Rows { batch, mask } => {
                                        (batch, mask)
                                    }
                                };
                                let narrowed = self.decoder_projection.narrow(batch)?;
                                let coalescer = self
                                    .batch_coalescer
                                    .as_mut()
                                    .expect("coalescer present with a post-scan filter");
                                match mask {
                                    Some(mask) => {
                                        coalescer
                                            .push_batch_with_filter(narrowed, &mask)?;
                                    }
                                    None => coalescer.push_batch(narrowed)?,
                                }
                                Ok(())
                            });
                            if let Err(e) = pushed {
                                return Some((Err(e), self));
                            }
                            continue;
                        }

                        // No post-scan filter, but a coalescer: the adaptive
                        // filter placement can add a post-scan filter at a
                        // later row group, thus the batches go through the
                        // coalescer to keep their order, and the limit is
                        // applied on its output.
                        if let Some(coalescer) = self.batch_coalescer.as_mut() {
                            if let Err(e) = coalescer.push_batch(batch) {
                                return Some((Err(DataFusionError::from(e)), self));
                            }
                            continue;
                        }

                        // No post-scan filter: the decoder's batches are
                        // already the right shape, so project and yield
                        // directly. The limit was pushed into the decoder.
                        let result = self.project_batch(&batch);
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
            // Keep `rg_plan.front()` aligned with the row group the decoder will
            // actually emit next: arrow-rs silently finishes row groups whose
            // post-predicate selection is empty without handing back a reader, so
            // without this sync `rg_plan` trails the decoder by one and a rebuild
            // either re-reads an already-delivered row group (#24352) or toggles
            // the per-RG filter for the wrong row group. Both the runtime pruner
            // and the per-RG `RowFilter` toggle consume `rg_plan`, so sync when
            // either is active; gating avoids the O(remaining row groups) cost of
            // `peek_next_row_group()` on ordinary scans that never rebuild.
            if at_boundary
                && (self.row_group_pruner.is_some() || self.row_filter_context.is_some())
                && let Err(e) = self.sync_rg_plan_to_decoder_frontier()
            {
                return Some((Err(e), self));
            }
            // Before the pruning below, hand the rows of the previous row
            // groups downstream, so that a dynamic filter can use them. The
            // next call comes back to this boundary with an empty coalescer.
            if at_boundary
                && self.flush_at_row_group_boundary
                && let Some(coalescer) = self.batch_coalescer.as_mut()
                && coalescer.get_buffered_rows() > 0
            {
                if let Err(e) = coalescer.finish_buffered_batch() {
                    return Some((Err(DataFusionError::from(e)), self));
                }
                return self.emit_completed();
            }
            if at_boundary && !self.rg_plan.is_empty() {
                let pruned_count = self.prune_boundary_row_groups();
                match self.rebuild_decoder_at_boundary(pruned_count) {
                    Ok(true) => return self.finish(),
                    Ok(false) => {}
                    Err(e) => return Some((Err(e), self)),
                }
            }

            // Step 3: drive the decoder.
            let decoder = self.decoder.as_mut().expect("decoder present");
            match decoder.try_next_reader() {
                Ok(DecodeResult::NeedsData(ranges)) => {
                    // I/O, not compute.
                    timer.stop();
                    // The adaptive filter placement uses the fetch latency.
                    let fetch_start = self
                        .row_filter_context
                        .as_ref()
                        .and_then(|ctx| ctx.placement())
                        .map(|_| Instant::now());
                    let data = self
                        .reader
                        .get_byte_ranges(ranges.clone())
                        .await
                        .map_err(DataFusionError::from);
                    if let (Some(start), Some(placement)) = (
                        fetch_start,
                        self.row_filter_context
                            .as_ref()
                            .and_then(|ctx| ctx.placement()),
                    ) {
                        placement.record_fetch(start.elapsed());
                    }
                    timer.restart();
                    match data {
                        Ok(data) => {
                            if let Err(e) = self
                                .decoder
                                .as_mut()
                                .expect("decoder present")
                                .push_ranges(ranges, data)
                            {
                                return Some((Err(DataFusionError::from(e)), self));
                            }
                        }
                        Err(e) => return Some((Err(e), self)),
                    }
                }
                Ok(DecodeResult::Data(reader)) => {
                    // Pop the RG this reader is for (we already filtered
                    // pruned ones in step 2, so `rg_plan.front()` is the RG
                    // the decoder is about to read).
                    //
                    // Its bytes are credited here rather than once the reader
                    // is drained: the decoder has already fetched them, and a
                    // reader that is abandoned mid-row-group (`LIMIT`, early
                    // stop) would otherwise never credit them until the file
                    // closes.
                    if let Some(entry) = self.rg_plan.pop_front() {
                        self.byte_progress.credit(entry.bytes);
                    }
                    if let Some(ctx) = self.row_filter_context.as_mut() {
                        ctx.start_reader();
                    }
                    self.active_reader = Some(reader);
                }
                Ok(DecodeResult::Finished) => return self.finish(),
                Err(e) => {
                    return Some((Err(DataFusionError::from(e)), self));
                }
            }
        }
    }

    /// Keep `rg_plan.front()` aligned with the row group the decoder will emit
    /// next. `try_next_reader` silently finishes row groups whose post-predicate
    /// selection is empty (no reader handed back) — e.g. page-index pruning
    /// removed every page — which would otherwise leave `rg_plan` trailing the
    /// decoder by one: a later prune/rebuild would then re-include an
    /// already-delivered row group (#24352) or toggle the filter for the wrong RG.
    /// Row-group-local selections keep selections and match status aligned when
    /// preparing or reordering the plan, but do not prevent this decoder-side
    /// advancement, so frontier synchronization is still required.
    fn sync_rg_plan_to_decoder_frontier(&mut self) -> Result<()> {
        match self
            .decoder
            .as_ref()
            .expect("decoder present")
            .peek_next_row_group()
            .map_err(DataFusionError::from)?
        {
            Some(actual) => Self::advance_rg_plan_to(
                &mut self.rg_plan,
                actual,
                &mut self.byte_progress,
            )?,
            // Decoder has nothing left to emit — drain our plan so the stream
            // finishes cleanly, crediting what it will now never read.
            None => {
                for entry in self.rg_plan.drain(..) {
                    self.byte_progress.credit(entry.bytes);
                }
            }
        }
        Ok(())
    }

    /// Pop entries off `rg_plan` until its front is `target`.
    ///
    /// `target` is the RG the decoder will emit next and must still be in the
    /// plan. A missing `target` means the decoder's frontier and `rg_plan` have
    /// diverged; we surface that as an internal error rather than silently
    /// draining the plan, which would truncate the scan. Kept free-standing on
    /// `rg_plan` (rather than `&mut self`) so the pop/guard logic is
    /// unit-testable without constructing a full stream state.
    fn advance_rg_plan_to(
        rg_plan: &mut VecDeque<RgPlanEntry>,
        target: usize,
        byte_progress: &mut ByteProgress,
    ) -> Result<()> {
        while let Some(front) = rg_plan.front() {
            if front.rg_index == target {
                return Ok(());
            }
            // Popped here means arrow-rs finished this row group without
            // handing back a reader, so the scan is done with its bytes.
            let popped = rg_plan.pop_front().expect("front present");
            byte_progress.credit(popped.bytes);
        }
        internal_err!(
            "push decoder frontier RG {target} is not in rg_plan; \
             decoder and plan have diverged"
        )
    }

    /// Drop every `rg_plan` entry the dynamic pruner proves cannot contribute,
    /// returning how many were pruned. The single decoder rebuild that acts on
    /// the survivors is left to the caller (at most one rebuild per boundary).
    fn prune_boundary_row_groups(&mut self) -> usize {
        let Some(pruner) = self.row_group_pruner.as_mut() else {
            return 0;
        };
        let mut pruned_count = 0usize;
        let mut kept = VecDeque::with_capacity(self.rg_plan.len());
        while let Some(entry) = self.rg_plan.pop_front() {
            if pruner.should_prune(&[entry.rg_index]) {
                pruned_count += 1;
                self.row_groups_pruned_dynamic.add(1);
                // The scan is done with this row group's bytes.
                self.byte_progress.credit(entry.bytes);
            } else {
                kept.push_back(entry);
            }
        }
        self.rg_plan = kept;
        pruned_count
    }

    /// At a row-group boundary, rebuild the decoder so it reads only the
    /// surviving `rg_plan` and toggle the per-row `RowFilter` for the upcoming
    /// RG. Rebuilds only when something changed (`pruned_count > 0` or the
    /// filter status flips), doing at most one `into_builder` rebuild per
    /// boundary. Returns `Ok(true)` when the plan is now empty (the stream
    /// should finish).
    fn rebuild_decoder_at_boundary(
        &mut self,
        pruned_count: usize,
    ) -> Result<bool, DataFusionError> {
        // The adaptive filter placement for the next RG. `Some` when it
        // changed: then the decoder needs the new projection and a new
        // `RowFilter`.
        let new_projection = self.update_placement()?;

        // `desired_filter` is `Some(true)` when the next RG needs a real
        // filter, `Some(false)` when it is fully-matched (filter is a no-op, so
        // we suppress it), and `None` when there is no pushdown predicate at
        // all (toggling is meaningless).
        let desired_filter: Option<bool> = self
            .row_filter_context
            .as_ref()
            .and_then(|_| self.rg_plan.front().map(|e| !e.fully_matched));
        let filter_needs_toggle = desired_filter.is_some_and(|want| {
            want != self.filter_installed || (want && new_projection.is_some())
        });

        if pruned_count == 0 && !filter_needs_toggle && new_projection.is_none() {
            return Ok(false);
        }
        if self.rg_plan.is_empty() {
            return Ok(true);
        }

        let decoder = self.decoder.take().expect("decoder present");
        let mut builder = decoder.into_builder().map_err(DataFusionError::from)?;
        // `into_builder` returns the decoder's remaining row group plan with
        // the selection of each row group that is not read yet. Thus a
        // rebuild that changes only the filter or the projection (the
        // fully-matched toggle, the adaptive filter placement) keeps a live
        // row selection (for example from the page index). Runtime pruning
        // is disabled for scans with selections, so pruned plans can be
        // rebuilt from row-group indexes alone.
        if pruned_count > 0 {
            let selections = self
                .rg_plan
                .iter()
                .map(|e| RowGroupSelection::new(e.rg_index, None))
                .collect();
            builder = builder.with_row_group_selections(selections);
        }
        if let Some(projection) = new_projection {
            builder = builder.with_projection(projection.projection_mask().clone());
            self.decoder_projection = projection;
        }
        if filter_needs_toggle {
            let want_filter = desired_filter.expect("filter_needs_toggle ⇒ desired Some");
            if want_filter {
                let ctx = self
                    .row_filter_context
                    .as_ref()
                    .expect("filter_needs_toggle ⇒ context set");
                builder = builder.with_row_filter(ctx.build_row_filter());
                if let Some(cap) = ctx.max_predicate_cache_size {
                    builder = builder.with_max_predicate_cache_size(cap);
                }
                self.filter_installed = true;
            } else {
                // Skip per-row filtering for the upcoming fully-matched RG.
                builder = builder.with_row_filter(RowFilter::new(vec![]));
                self.filter_installed = false;
                self.row_filter_skipped_fully_matched.add_one();
            }
        }
        self.decoder = Some(builder.build().map_err(DataFusionError::from)?);
        Ok(false)
    }

    /// Makes the adaptive filter placement for the next RG
    /// (`rg_plan.front()`). Returns the decoder projection for the new
    /// placement if it changed.
    ///
    /// The coalescer holds batches with the schema of the current
    /// projection. If the new post-scan conjuncts change that schema (this
    /// can happen with nested columns), the file keeps its current placement
    /// until its end.
    fn update_placement(&mut self) -> Result<Option<DecoderProjection>> {
        let (Some(builder), Some(front)) =
            (self.projection_builder.as_ref(), self.rg_plan.front())
        else {
            return Ok(None);
        };
        let Some(placement) = self
            .row_filter_context
            .as_mut()
            .and_then(|ctx| ctx.placement.as_mut())
        else {
            return Ok(None);
        };
        let previous = placement.placements();
        let rows = placement.row_group_rows(front.rg_index);
        if !placement.decide(rows) {
            return Ok(None);
        }
        let projection = builder.build(&placement.post_scan_conjuncts())?;
        if projection.filtered_schema() != self.decoder_projection.filtered_schema() {
            placement.restore_and_freeze(&previous);
            return Ok(None);
        }
        placement.set_decoder_mask(projection.projection_mask());
        Ok(Some(projection))
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

    /// Pop one assembled batch from the coalescer, apply the stream-level
    /// limit, and project it onto the scan's output schema.
    ///
    /// Returns `None` only when the coalescer has nothing left, which ends the
    /// stream. Called from within [`Self::transition`], whose
    /// `elapsed_compute` timer covers this work.
    fn emit_completed(mut self) -> Option<(Result<RecordBatch>, Self)> {
        let batch = self.batch_coalescer.as_mut()?.next_completed_batch()?;

        // Enforce the stream-level limit here rather than in the decoder: the
        // post-scan filter rejects rows the decoder has already counted, so a
        // decoder-local limit would stop short.
        let batch = if let Some(remaining) = self.remaining_limit {
            if batch.num_rows() > remaining {
                self.remaining_limit = Some(0);
                batch.slice(0, remaining)
            } else {
                self.remaining_limit = Some(remaining - batch.num_rows());
                batch
            }
        } else {
            batch
        };
        let result = self.project_batch(&batch);
        Some((result, self))
    }

    /// End of input: flush the partial batch the coalescer is still holding,
    /// then drain it one batch at a time. Idempotent — every terminal path in
    /// `transition` routes through here, but the flush happens once.
    fn finish(mut self) -> Option<(Result<RecordBatch>, Self)> {
        self.batch_coalescer.as_ref()?;
        if !self.flushed {
            self.flushed = true;
            if let Err(e) = self
                .batch_coalescer
                .as_mut()
                .expect("coalescer checked present")
                .finish_buffered_batch()
            {
                return Some((Err(DataFusionError::from(e)), self));
            }
        }
        self.emit_completed()
    }

    fn project_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        self.decoder_projection.map(batch)
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
    use datafusion_pruning::MAX_IN_LIST_SIZE;
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
            MAX_IN_LIST_SIZE,
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
            MAX_IN_LIST_SIZE,
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
            MAX_IN_LIST_SIZE,
        );
        // No pruning predicate could be built → conservatively keep RGs.
        assert!(!pruner.should_prune(&[0]));
        assert!(!pruner.should_prune(&[1]));
        assert!(!pruner.should_prune(&[2]));
    }

    /// A plan whose row group `i` is `100 * (i + 1)` bytes.
    fn rg_plan(indexes: impl IntoIterator<Item = usize>) -> VecDeque<RgPlanEntry> {
        indexes
            .into_iter()
            .map(|rg_index| RgPlanEntry {
                rg_index,
                fully_matched: false,
                bytes: 100 * (rg_index as u64 + 1),
            })
            .collect()
    }

    #[test]
    fn advance_rg_plan_to_pops_up_to_target() {
        let mut plan = rg_plan([0usize, 1, 2, 3]);
        let bytes_processed = Count::new();
        let mut byte_progress = ByteProgress::new(1_000, Count::clone(&bytes_processed));

        PushDecoderStreamState::advance_rg_plan_to(&mut plan, 2, &mut byte_progress)
            .unwrap();

        assert_eq!(
            plan.iter().map(|e| e.rg_index).collect::<Vec<_>>(),
            vec![2, 3],
            "must pop the entries before `target` and stop at it",
        );
        assert_eq!(
            bytes_processed.value(),
            300,
            "row groups finished without a reader must still credit their bytes",
        );
    }

    #[test]
    fn advance_rg_plan_to_errors_when_target_absent() {
        let mut plan = rg_plan([0usize, 1, 2]);
        let mut byte_progress = ByteProgress::new(1_000, Count::new());

        let err =
            PushDecoderStreamState::advance_rg_plan_to(&mut plan, 5, &mut byte_progress)
                .expect_err("a target absent from the plan must be an internal error");
        assert!(
            err.to_string().contains("diverged"),
            "expected a divergence internal error, got: {err}",
        );
    }
}
