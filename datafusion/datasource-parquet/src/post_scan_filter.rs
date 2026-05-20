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

//! In-scan equivalent of [`FilterExec`](datafusion_physical_plan::filter::FilterExec)
//! for predicate conjuncts that the parquet [`RowFilter`](parquet::arrow::arrow_reader::RowFilter)
//! machinery cannot apply during decode.
//!
//! The Parquet scan accepts pushable filter conjuncts unconditionally so the
//! parent `FilterExec` can be removed from the plan. Conjuncts the
//! `RowFilter` cannot evaluate — whole-struct references, columns whose
//! per-file physical shape differs from the table schema, etc., plus
//! everything when `pushdown_filters = false` — fall through here. This
//! module wraps two concerns into a small API the opener can call:
//!
//! * [`PostScanFilter`] — evaluates the conjoined post-scan predicate on
//!   decoded record batches.
//! * [`DecoderProjection`] — widens the parquet decoder projection so any
//!   columns referenced by post-scan conjuncts are decoded, rebases the
//!   projection expressions onto the decoder's stream schema, and produces
//!   a rebased [`PostScanFilter`]. When `post_scan_conjuncts` is empty this
//!   is exactly the prior projection-only behaviour, so the opener routes
//!   every file through it.
//!
//! Keeping this complexity in its own module lets `opener/mod.rs` orchestrate
//! the scan with a single `DecoderProjection::build` call rather than the
//! prior block of inline projection / rebase / schema-replace logic.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;

use datafusion_common::cast::as_boolean_array;
use datafusion_common::{Result, internal_err};
use datafusion_physical_expr::conjunction;
use datafusion_physical_expr::projection::{ProjectionExprs, Projector};
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::metrics::{Count, Time};

use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use crate::ParquetFileMetrics;
use crate::row_filter::build_projection_read_plan;

/// Predicate applied to decoded record batches inside the parquet scan.
///
/// Semantically identical to a `FilterExec` over the scan: the predicate is
/// evaluated against the batch as a whole and rows where the predicate is
/// not `true` are dropped. `NULL` predicate results drop the row (SQL `WHERE`
/// semantics — `filter_record_batch` treats null mask entries as false, same
/// as `FilterExec`'s `batch_filter`).
///
/// Holds metric handles so per-batch rows-pruned / matched / time accumulate
/// into [`ParquetFileMetrics`] for `EXPLAIN ANALYZE`.
pub(crate) struct PostScanFilter {
    /// Combined predicate, rebased onto the decoder's stream schema.
    predicate: Arc<dyn PhysicalExpr>,
    rows_pruned: Count,
    rows_matched: Count,
    eval_time: Time,
}

impl PostScanFilter {
    /// Evaluate the predicate on `batch` and return the surviving rows. May
    /// return an empty batch — callers should skip empty batches rather than
    /// yield them downstream.
    pub(crate) fn filter(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // Scoped timer: stops on drop, so the early-return paths still record.
        let _timer = self.eval_time.timer();

        let input_rows = batch.num_rows();
        let array = self.predicate.evaluate(batch)?.into_array(input_rows)?;
        let Ok(mask) = as_boolean_array(array.as_ref()) else {
            return internal_err!(
                "post-scan filter predicate did not evaluate to a BooleanArray"
            );
        };
        let filtered = filter_record_batch(batch, mask)?;

        let kept = filtered.num_rows();
        self.rows_matched.add(kept);
        self.rows_pruned.add(input_rows - kept);
        Ok(filtered)
    }
}

/// The parquet decoder projection together with the post-scan filter (if any).
///
/// Built once per file by the opener. When `post_scan_conjuncts` is empty this
/// is the previous projection-only behaviour: `post_scan_filter` is `None`,
/// the decoder mask covers exactly the user projection, and there is no extra
/// per-batch work.
///
/// When `post_scan_conjuncts` is non-empty the decoder mask is widened to
/// include their columns, the projection expressions and the conjuncts are
/// both rebased onto the (widened) stream schema, and the conjoined predicate
/// becomes the [`PostScanFilter`].
pub(crate) struct DecoderProjection {
    /// Projection mask passed to the parquet decoder.
    pub(crate) projection_mask: ProjectionMask,
    /// Maps decoder output (stream) batches to the user-visible output.
    pub(crate) projector: Projector,
    /// `true` when the projector's output schema differs from `output_schema`
    /// in metadata / nullability and the caller must rebuild the batch with
    /// `output_schema` before yielding it.
    pub(crate) replace_schema: bool,
    /// Predicate to apply on each decoded batch, after any row-level
    /// `RowFilter` and before the projector. `None` when no conjuncts need
    /// post-scan evaluation.
    pub(crate) post_scan_filter: Option<PostScanFilter>,
}

impl DecoderProjection {
    /// Build the decoder projection state for a file.
    ///
    /// `projection` and `post_scan_conjuncts` must both reference columns in
    /// `physical_file_schema` (i.e. already adapted by the per-file expr
    /// adapter); `parquet_schema` is the corresponding parquet `SchemaDescriptor`.
    /// `output_schema` is what consumers of the scan stream expect.
    pub(crate) fn build(
        projection: &ProjectionExprs,
        post_scan_conjuncts: &[Arc<dyn PhysicalExpr>],
        physical_file_schema: &SchemaRef,
        parquet_schema: &SchemaDescriptor,
        output_schema: &SchemaRef,
        file_metrics: &ParquetFileMetrics,
    ) -> Result<Self> {
        // Decoder reads (user projection ∪ post-scan filter columns). Row-level
        // filter columns live inside the parquet RowFilter's per-predicate
        // masks, so they don't need to be in this read plan.
        let read_plan = build_projection_read_plan(
            projection
                .expr_iter()
                .chain(post_scan_conjuncts.iter().map(Arc::clone)),
            physical_file_schema,
            parquet_schema,
        );

        let stream_schema = read_plan.projected_schema;

        // Rebase the projection onto the decoder's stream schema (column
        // indices change because the decoder yields only the masked columns).
        let rebased_projection = projection
            .clone()
            .try_map_exprs(|expr| reassign_expr_columns(expr, &stream_schema))?;
        let projector = rebased_projection.make_projector(&stream_schema)?;

        // Compare against the projector's *output* schema, not the (possibly
        // widened) stream schema — widening for post-scan columns must not
        // force a per-batch schema replacement when the projector output
        // already matches `output_schema`.
        let replace_schema = projector.output_schema() != output_schema;

        let post_scan_filter = if post_scan_conjuncts.is_empty() {
            None
        } else {
            let rebased = post_scan_conjuncts
                .iter()
                .map(|expr| reassign_expr_columns(Arc::clone(expr), &stream_schema))
                .collect::<Result<Vec<_>>>()?;
            let predicate = conjunction(rebased);
            Some(PostScanFilter {
                predicate,
                rows_pruned: file_metrics.post_scan_rows_pruned.clone(),
                rows_matched: file_metrics.post_scan_rows_matched.clone(),
                eval_time: file_metrics.post_scan_filter_eval_time.clone(),
            })
        };

        Ok(Self {
            projection_mask: read_plan.projection_mask,
            projector,
            replace_schema,
            post_scan_filter,
        })
    }
}
