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
//! * the [`ProjectionMask`] installed on every parquet decoder run, and
//! * the per-batch transform ([`DecoderProjection::map`]) that applies the
//!   projector and, when needed, rebuilds the batch with the user's
//!   `output_schema` to recover metadata / nullability the file schema does
//!   not carry.
//!
//! The opener constructs one [`DecoderProjection`] per file via
//! [`DecoderProjection::build`] and hands it to the push-decoder stream,
//! which calls [`map`](DecoderProjection::map) on every decoded batch.

use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::SchemaRef;

use datafusion_common::Result;
use datafusion_physical_expr::projection::{ProjectionExprs, Projector};
use datafusion_physical_expr::utils::reassign_expr_columns;

use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use crate::row_filter::build_projection_read_plan;

/// Per-file decoder projection: the [`ProjectionMask`] installed on every
/// parquet decoder run, plus the per-batch transform that maps the decoder's
/// output onto the scan's `output_schema`.
///
/// Built once per file by the opener via [`Self::build`]; the push-decoder
/// stream installs [`Self::projection_mask`] on each decoder and calls
/// [`Self::map`] on every decoded batch.
pub(crate) struct DecoderProjection {
    projection_mask: ProjectionMask,
    projector: Projector,
    output_schema: SchemaRef,
    /// `true` when the projector's output schema differs from `output_schema`
    /// in metadata / nullability and [`map`](Self::map) must rebuild the batch
    /// with `output_schema`.
    replace_schema: bool,
}

impl DecoderProjection {
    /// Build the decoder projection for a file.
    ///
    /// `projection` references columns in `physical_file_schema` (i.e. already
    /// adapted by the per-file expr adapter); `parquet_schema` is the
    /// corresponding parquet [`SchemaDescriptor`]. `output_schema` is what
    /// consumers of the scan stream expect.
    pub(crate) fn build(
        projection: &ProjectionExprs,
        physical_file_schema: &SchemaRef,
        parquet_schema: &SchemaDescriptor,
        output_schema: &SchemaRef,
    ) -> Result<Self> {
        let read_plan = build_projection_read_plan(
            projection.expr_iter(),
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

        // Compare against the projector's *output* schema rather than the
        // stream schema, so future widening of the mask (e.g. for post-scan
        // filter columns) does not flip this flag.
        let replace_schema = projector.output_schema() != output_schema;

        Ok(Self {
            projection_mask: read_plan.projection_mask,
            projector,
            output_schema: Arc::clone(output_schema),
            replace_schema,
        })
    }

    /// The projection mask to install on every parquet decoder in the scan.
    pub(crate) fn projection_mask(&self) -> &ProjectionMask {
        &self.projection_mask
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
