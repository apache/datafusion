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
//! - [`DecoderBuilderConfig`] holds the shared options applied to every
//!   [`ParquetPushDecoderBuilder`] in a file scan, exposing a single `build`
//!   entry point per decoder run.
//! - [`PushDecoderStreamState`] is the per-file stream driver that polls one
//!   or more decoders to completion, yielding projected [`RecordBatch`]es.
//!   A scan can produce multiple decoders (for example, when fully matched
//!   row groups split it into runs with different filter requirements); the
//!   state machine drains them in order so the output is contiguous.
//!
//! The opener constructs both halves and hands the state off to
//! [`PushDecoderStreamState::into_stream`] for consumption.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::Schema;
use futures::StreamExt;
use futures::stream::BoxStream;
use parquet::DecodeResult;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, RowSelectionPolicy};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::push_decoder::{ParquetPushDecoder, ParquetPushDecoderBuilder};

use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::projection::Projector;
use datafusion_physical_plan::metrics::{BaselineMetrics, Gauge};

use crate::access_plan::PreparedAccessPlan;
use crate::row_filter::ParquetReadPlan;

/// Shared options applied to every [`ParquetPushDecoderBuilder`] in a file scan.
///
/// A single scan may produce multiple decoders (for example, when fully matched
/// row groups split the scan into consecutive runs with different filter
/// requirements). All decoders in that scan share the same projection, batch
/// size, metrics sink, and selection policy.
pub(crate) struct DecoderBuilderConfig<'a> {
    pub(crate) read_plan: &'a ParquetReadPlan,
    pub(crate) batch_size: usize,
    pub(crate) arrow_reader_metrics: &'a ArrowReaderMetrics,
    pub(crate) force_filter_selections: bool,
    pub(crate) decoder_limit: Option<usize>,
}

impl DecoderBuilderConfig<'_> {
    /// Build a [`ParquetPushDecoderBuilder`] for a single decoder run.
    ///
    /// The caller is expected to attach the run-specific
    /// [`RowFilter`](parquet::arrow::arrow_reader::RowFilter) and predicate
    /// cache size on the returned builder.
    pub(crate) fn build(
        &self,
        prepared_access_plan: PreparedAccessPlan,
        metadata: ArrowReaderMetadata,
    ) -> ParquetPushDecoderBuilder {
        let mut builder = ParquetPushDecoderBuilder::new_with_metadata(metadata)
            .with_projection(self.read_plan.projection_mask.clone())
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

/// State for a stream that decodes a single Parquet file using a push-based decoder.
///
/// The [`transition`](Self::transition) method drives the decoder in a loop: it requests
/// byte ranges from the [`AsyncFileReader`], pushes the fetched data into the
/// [`ParquetPushDecoder`], and yields projected [`RecordBatch`]es until the file is
/// fully consumed.
pub(crate) struct PushDecoderStreamState {
    pub(crate) decoder: ParquetPushDecoder,
    /// Additional decoders to process after the current one finishes.
    /// Used when fully matched row groups split the scan into consecutive
    /// runs with different filter configurations, maintaining original order.
    pub(crate) pending_decoders: VecDeque<ParquetPushDecoder>,
    /// Global remaining row limit across all decoder runs.
    ///
    /// Decoder-local limits are only safe for single-run scans. When the scan
    /// is split across multiple decoders, the combined stream limit is enforced
    /// here instead.
    pub(crate) remaining_limit: Option<usize>,
    pub(crate) reader: Box<dyn AsyncFileReader>,
    pub(crate) projector: Projector,
    pub(crate) output_schema: Arc<Schema>,
    pub(crate) replace_schema: bool,
    pub(crate) arrow_reader_metrics: ArrowReaderMetrics,
    pub(crate) predicate_cache_inner_records: Gauge,
    pub(crate) predicate_cache_records: Gauge,
    pub(crate) baseline_metrics: BaselineMetrics,
}

impl PushDecoderStreamState {
    /// Drive the state machine to completion as a [`Stream`] of record batches.
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
            if self.remaining_limit == Some(0) {
                return None;
            }
            match self.decoder.try_decode() {
                Ok(DecodeResult::NeedsData(ranges)) => {
                    let data = self
                        .reader
                        .get_byte_ranges(ranges.clone())
                        .await
                        .map_err(DataFusionError::from);
                    match data {
                        Ok(data) => {
                            if let Err(e) = self.decoder.push_ranges(ranges, data) {
                                return Some((Err(DataFusionError::from(e)), self));
                            }
                        }
                        Err(e) => return Some((Err(e), self)),
                    }
                }
                Ok(DecodeResult::Data(batch)) => {
                    let batch = if let Some(remaining_limit) = self.remaining_limit {
                        if batch.num_rows() > remaining_limit {
                            self.remaining_limit = Some(0);
                            batch.slice(0, remaining_limit)
                        } else {
                            self.remaining_limit =
                                Some(remaining_limit - batch.num_rows());
                            batch
                        }
                    } else {
                        batch
                    };
                    let mut timer = self.baseline_metrics.elapsed_compute().timer();
                    self.copy_arrow_reader_metrics();
                    let result = self.project_batch(&batch);
                    timer.stop();
                    // Release the borrow on baseline_metrics before moving self
                    drop(timer);
                    return Some((result, self));
                }
                Ok(DecodeResult::Finished) => {
                    // If there are pending decoders (e.g. for consecutive runs
                    // with different filter configurations), switch to the next.
                    if let Some(next) = self.pending_decoders.pop_front() {
                        self.decoder = next;
                        continue;
                    }
                    return None;
                }
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
        let mut batch = self.projector.project_batch(batch)?;
        if self.replace_schema {
            // Ensure the output batch has the expected schema.
            // This handles things like schema level and field level metadata, which may not be present
            // in the physical file schema.
            // It is also possible for nullability to differ; some writers create files with
            // OPTIONAL fields even when there are no nulls in the data.
            // In these cases it may make sense for the logical schema to be `NOT NULL`.
            // RecordBatch::try_new_with_options checks that if the schema is NOT NULL
            // the array cannot contain nulls, amongst other checks.
            let (_stream_schema, arrays, num_rows) = batch.into_parts();
            let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
            batch = RecordBatch::try_new_with_options(
                Arc::clone(&self.output_schema),
                arrays,
                &options,
            )?;
        }
        Ok(batch)
    }
}
