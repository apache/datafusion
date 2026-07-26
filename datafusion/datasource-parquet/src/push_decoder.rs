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

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use futures::stream::BoxStream;
use log::debug;
use parquet::DecodeResult;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, RowSelectionPolicy,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::push_decoder::{ParquetPushDecoder, ParquetPushDecoderBuilder};
use parquet::file::metadata::ParquetMetaData;

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
                Ok(DecodeResult::NeedsData(ranges)) => {
                    let data = self
                        .reader
                        .get_byte_ranges(ranges.clone())
                        .await
                        .map_err(DataFusionError::from);
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
                    self.rg_plan.pop_front();
                    self.active_reader = Some(reader);
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
