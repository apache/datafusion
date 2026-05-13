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

//! Inner stream-state machine that drives a [`ParquetPushDecoder`] to
//! produce projected `RecordBatch`es. Created by
//! [`build_stream`](super::RowGroupsPrunedParquetOpen::build_stream).

use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::Schema;
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::projection::Projector;
use datafusion_physical_plan::metrics::{BaselineMetrics, Gauge};
use parquet::DecodeResult;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::push_decoder::ParquetPushDecoder;

/// State for a stream that decodes a single Parquet file using a push-based decoder.
///
/// The [`transition`](Self::transition) method drives the decoder in a loop: it requests
/// byte ranges from the [`AsyncFileReader`], pushes the fetched data into the
/// [`ParquetPushDecoder`], and yields projected [`RecordBatch`]es until the file is
/// fully consumed.
pub(super) struct PushDecoderStreamState {
    pub(super) decoder: ParquetPushDecoder,
    pub(super) reader: Box<dyn AsyncFileReader>,
    pub(super) projector: Projector,
    pub(super) output_schema: Arc<Schema>,
    pub(super) replace_schema: bool,
    pub(super) arrow_reader_metrics: ArrowReaderMetrics,
    pub(super) predicate_cache_inner_records: Gauge,
    pub(super) predicate_cache_records: Gauge,
    pub(super) baseline_metrics: BaselineMetrics,
}

impl PushDecoderStreamState {
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
    pub(super) async fn transition(mut self) -> Option<(Result<RecordBatch>, Self)> {
        loop {
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
                    let mut timer = self.baseline_metrics.elapsed_compute().timer();
                    self.copy_arrow_reader_metrics();
                    let result = self.project_batch(&batch);
                    timer.stop();
                    // Release the borrow on baseline_metrics before moving self
                    drop(timer);
                    return Some((result, self));
                }
                Ok(DecodeResult::Finished) => {
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
