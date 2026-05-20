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
//! Owns the projection-mask + projector + schema-replacement triple the
//! opener installs on every parquet decoder run, behind a single
//! [`DecoderProjection::build`] entry point. Keeping it here lets the
//! opener orchestrate scans with one call instead of an inline block of
//! `build_projection_read_plan` / `reassign_expr_columns` / `make_projector`,
//! and gives a clean seam for the in-scan post-scan filter that follows in
//! a later change (`PostScanFilter`, when conjuncts the parquet `RowFilter`
//! cannot evaluate fall through to a decoded-batch predicate).

use arrow::datatypes::SchemaRef;

use datafusion_common::Result;
use datafusion_physical_expr::projection::{ProjectionExprs, Projector};
use datafusion_physical_expr::utils::reassign_expr_columns;

use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use crate::row_filter::build_projection_read_plan;

/// The parquet decoder projection: the [`ProjectionMask`] installed on every
/// decoder run in the scan, the [`Projector`] that maps decoder output
/// batches to the user-visible output, and the `replace_schema` flag that
/// tells [`PushDecoderStreamState`](crate::push_decoder::PushDecoderStreamState)
/// whether the projector's output schema must be rebuilt with the requested
/// `output_schema` (e.g. for metadata / nullability mismatches).
///
/// Built once per file by the opener via [`Self::build`].
pub(crate) struct DecoderProjection {
    /// Projection mask passed to the parquet decoder.
    pub(crate) projection_mask: ProjectionMask,
    /// Maps decoder output (stream) batches to the user-visible output.
    pub(crate) projector: Projector,
    /// `true` when the projector's output schema differs from `output_schema`
    /// in metadata / nullability and the caller must rebuild the batch with
    /// `output_schema` before yielding it.
    pub(crate) replace_schema: bool,
}

impl DecoderProjection {
    /// Build the decoder projection state for a file.
    ///
    /// `projection` references columns in `physical_file_schema` (i.e. already
    /// adapted by the per-file expr adapter); `parquet_schema` is the
    /// corresponding parquet `SchemaDescriptor`. `output_schema` is what
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
        // stream schema, so future widening of the mask (for post-scan filter
        // columns) does not flip this flag.
        let replace_schema = projector.output_schema() != output_schema;

        Ok(Self {
            projection_mask: read_plan.projection_mask,
            projector,
            replace_schema,
        })
    }
}
