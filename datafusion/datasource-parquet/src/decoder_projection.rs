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

//! Decoder-projection construction for the parquet scan.
//!
//! [`DecoderProjection`] owns the two halves of "project a decoded parquet
//! batch onto the scan's output schema":
//!
//! * the [`ProjectionMask`] installed on the parquet decoder (and on any
//!   rebuild performed via `into_builder` at a row-group boundary), and
//! * the per-batch transform ([`DecoderProjection::map`]) that applies the
//!   projector and, when needed, rebuilds the batch with the user's
//!   `output_schema` to recover metadata / nullability the file schema does
//!   not carry.
//!
//! The opener constructs one [`DecoderProjection`] per file via
//! [`DecoderProjection::try_new`] and hands it to the push-decoder stream,
//! which calls [`map`](DecoderProjection::map) on every decoded batch.

use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;

use datafusion_common::cast::as_boolean_array;
use datafusion_common::{Result, internal_err};
use datafusion_physical_expr::conjunction;
use datafusion_physical_expr::projection::{ProjectionExprs, Projector};
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr_adapter::replace_columns_with_literals;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::metrics::{Count, Time};

use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use crate::ParquetFileMetrics;
use crate::opener::{VirtualColumnsState, append_fields};
use crate::projection_read_plan::build_projection_read_plan;

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

/// Per-file decoder projection: the [`ProjectionMask`] installed on the
/// parquet decoder, plus the per-batch transform that maps the decoder's
/// output onto the scan's `output_schema`.
///
/// Built once per file by the opener via [`Self::try_new`]; the
/// push-decoder stream installs [`Self::projection_mask`] on the decoder
/// (and on any rebuild performed via `into_builder` at a row-group
/// boundary) and calls [`Self::map`] on every decoded batch.
pub(crate) struct DecoderProjection {
    projection_mask: ProjectionMask,
    projector: Projector,
    output_schema: SchemaRef,
    /// `true` when the projector's output schema differs from `output_schema`
    /// in metadata / nullability and [`map`](Self::map) must rebuild the batch
    /// with `output_schema`.
    replace_schema: bool,
    /// Predicate to apply on each decoded batch, after any row-level
    /// `RowFilter` and before the projector. Carries conjuncts the `RowFilter`
    /// machinery could not evaluate, plus the whole predicate when
    /// `pushdown_filters = false`. `None` when no conjunct needs post-scan
    /// evaluation, in which case the decoder mask covers exactly the user
    /// projection and there is no extra per-batch work.
    post_scan_filter: Option<PostScanFilter>,
}

impl DecoderProjection {
    /// Build the decoder projection for a file.
    ///
    /// `projection` references columns in `physical_file_schema` (i.e. already
    /// adapted by the per-file expr adapter); `parquet_schema` is the
    /// corresponding parquet [`SchemaDescriptor`]. `output_schema` is what
    /// consumers of the scan stream expect.
    ///
    /// `virtual_state`, when present, describes virtual columns the reader
    /// will append to each decoded batch (e.g. parquet `row_number`). Virtual
    /// columns are stripped from the projection fed into
    /// `build_projection_read_plan` (which only understands file columns) and
    /// appended to the stream schema so the projector can resolve them.
    ///
    /// `post_scan_conjuncts` are predicate conjuncts that must be evaluated on
    /// decoded batches inside the scan (conjuncts the parquet `RowFilter`
    /// machinery could not place, plus the whole predicate when
    /// `pushdown_filters = false`). They must reference columns in
    /// `physical_file_schema` (virtual-column predicates are never pushed into
    /// the scan). When non-empty the decoder mask is widened to include their
    /// columns, the conjuncts are rebased onto the (widened) stream schema, and
    /// the conjoined predicate becomes [`Self::post_scan_filter`]. When empty
    /// this is exactly the prior projection-only behaviour.
    pub(crate) fn try_new(
        projection: &ProjectionExprs,
        post_scan_conjuncts: &[Arc<dyn PhysicalExpr>],
        physical_file_schema: &SchemaRef,
        parquet_schema: &SchemaDescriptor,
        output_schema: &SchemaRef,
        virtual_state: Option<&VirtualColumnsState>,
        file_metrics: &ParquetFileMetrics,
    ) -> Result<Self> {
        // Virtual columns are produced by the reader separately from the
        // projection mask, so strip them from the expressions we feed into
        // `build_projection_read_plan`. We substitute each virtual column
        // reference with a null literal; that leaves the remaining Column
        // refs (into `physical_file_schema`) intact for
        // `ProjectionMask::roots`, which only understands file columns.
        let projection_for_read_plan = match virtual_state {
            None => projection.clone(),
            Some(state) => projection.clone().try_map_exprs(|expr| {
                replace_columns_with_literals(expr, state.null_replacements())
            })?,
        };
        // Decoder reads (user projection ∪ post-scan filter columns). Row-level
        // filter columns live inside the parquet RowFilter's per-predicate
        // masks, so they don't need to be in this read plan.
        //
        // A post-scan conjunct may reference a virtual column (e.g. parquet
        // `row_number`): the reader produces those separately, so — like the
        // projection — strip them to null literals before feeding the read
        // plan, which only understands file columns. The *original* conjuncts
        // (with the virtual references intact) are still used below to build
        // the post-scan predicate, which is rebased onto the stream schema
        // where the reader has appended the virtual columns.
        let post_scan_for_read_plan: Vec<Arc<dyn PhysicalExpr>> = match virtual_state {
            None => post_scan_conjuncts.to_vec(),
            Some(state) => post_scan_conjuncts
                .iter()
                .map(|expr| {
                    replace_columns_with_literals(
                        Arc::clone(expr),
                        state.null_replacements(),
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        };
        let read_plan = build_projection_read_plan(
            projection_for_read_plan
                .expr_iter()
                .chain(post_scan_for_read_plan.iter().map(Arc::clone)),
            physical_file_schema,
            parquet_schema,
        );

        // The reader produces projected file columns followed by any virtual
        // columns (`ArrowReaderOptions::with_virtual_columns` appends them to
        // each decoded batch).
        let stream_schema = match virtual_state {
            Some(state) => {
                append_fields(&read_plan.projected_schema, state.virtual_columns())
            }
            None => Arc::clone(&read_plan.projected_schema),
        };

        // Rebase the projection onto the decoder's stream schema (column
        // indices change because the decoder yields only the masked columns).
        let rebased_projection = projection
            .clone()
            .try_map_exprs(|expr| reassign_expr_columns(expr, &stream_schema))?;
        let projector = rebased_projection.make_projector(&stream_schema)?;

        // Compare against the projector's *output* schema rather than the
        // (possibly widened) stream schema, so widening the mask for post-scan
        // filter columns does not flip this flag.
        let replace_schema = projector.output_schema() != output_schema;

        // Rebase the post-scan conjuncts onto the same (widened) stream schema
        // and conjoin them into a single predicate for per-batch evaluation.
        let post_scan_filter = if post_scan_conjuncts.is_empty() {
            None
        } else {
            let rebased = post_scan_conjuncts
                .iter()
                .map(|expr| reassign_expr_columns(Arc::clone(expr), &stream_schema))
                .collect::<Result<Vec<_>>>()?;
            Some(PostScanFilter {
                predicate: conjunction(rebased),
                rows_pruned: file_metrics.post_scan_rows_pruned.clone(),
                rows_matched: file_metrics.post_scan_rows_matched.clone(),
                eval_time: file_metrics.post_scan_filter_eval_time.clone(),
            })
        };

        Ok(Self {
            projection_mask: read_plan.projection_mask,
            projector,
            output_schema: Arc::clone(output_schema),
            replace_schema,
            post_scan_filter,
        })
    }

    /// The projection mask to install on every parquet decoder in the scan.
    pub(crate) fn projection_mask(&self) -> &ProjectionMask {
        &self.projection_mask
    }

    /// The post-scan filter for this file, if any conjunct needs per-batch
    /// evaluation. Applied by the push-decoder stream to each decoded batch
    /// (after any row-level `RowFilter`, before the projector).
    pub(crate) fn post_scan_filter(&self) -> Option<&PostScanFilter> {
        self.post_scan_filter.as_ref()
    }

    /// Whether this file has a post-scan filter. Used by the opener to decide
    /// whether a decoder-local LIMIT is safe (it is not, because the filter
    /// can reject rows after the decoder counts them).
    pub(crate) fn has_post_scan_filter(&self) -> bool {
        self.post_scan_filter.is_some()
    }

    /// Map a decoded batch onto the scan's output schema.
    ///
    /// Applies the [`Projector`] and, when the projector's output schema
    /// differs from `output_schema` in metadata or nullability, rebuilds the
    /// batch with `output_schema` (some writers emit OPTIONAL fields even when
    /// the data has no nulls; some logical schemas carry field-level metadata
    /// the file schema does not).
    pub(crate) fn map(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let projected = self.projector.project_batch(batch)?;
        if !self.replace_schema {
            return Ok(projected);
        }
        let (_stream_schema, arrays, num_rows) = projected.into_parts();
        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.output_schema),
            arrays,
            &options,
        )?)
    }
}
