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
use arrow::datatypes::SchemaRef;

use datafusion_common::Result;
use datafusion_physical_expr::projection::{ProjectionExprs, Projector};
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr_adapter::replace_columns_with_literals;

use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use crate::opener::{VirtualColumnsState, append_fields};
use crate::projection_read_plan::build_projection_read_plan;

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
    /// `None` when the decoder already yields `output_schema` and
    /// [`map`](Self::map) is a pass-through.
    transform: Option<DecoderTransform>,
}

/// The per-batch half of a [`DecoderProjection`].
struct DecoderTransform {
    projector: Projector,
    output_schema: SchemaRef,
    /// `true` when the projector's output schema differs from `output_schema`
    /// in metadata / nullability and [`DecoderProjection::map`] must rebuild
    /// the batch with `output_schema`.
    replace_schema: bool,
}

impl DecoderProjection {
    /// Build the decoder projection for a file.
    ///
    /// `projection` references columns in `physical_file_schema` (i.e. already
    /// adapted by the per-file expr adapter); `parquet_schema` is the
    /// corresponding parquet [`SchemaDescriptor`]. `output_schema` is what
    /// consumers of the scan stream expect.
    ///
    /// `projection` is `None` when the scan reads the table unprojected. When
    /// the decoder's own output then already matches `output_schema` this
    /// installs an all-columns mask and no per-batch transform, avoiding a
    /// column-per-field projector on very wide unprojected scans.
    ///
    /// `virtual_state`, when present, describes virtual columns the reader
    /// will append to each decoded batch (e.g. parquet `row_number`). Virtual
    /// columns are stripped from the projection fed into
    /// `build_projection_read_plan` (which only understands file columns) and
    /// appended to the stream schema so the projector can resolve them.
    pub(crate) fn try_new(
        projection: Option<&ProjectionExprs>,
        physical_file_schema: &SchemaRef,
        parquet_schema: &SchemaDescriptor,
        output_schema: &SchemaRef,
        virtual_state: Option<&VirtualColumnsState>,
    ) -> Result<Self> {
        if projection.is_none() {
            // Unprojected scan: if the decoder's output (the file schema plus
            // any appended virtual columns) already is the output schema,
            // there is no transform to apply.
            let stream_schema = match virtual_state {
                Some(state) => {
                    append_fields(physical_file_schema, state.virtual_columns())
                }
                None => Arc::clone(physical_file_schema),
            };
            if stream_schema == *output_schema {
                return Ok(Self {
                    projection_mask: ProjectionMask::all(),
                    transform: None,
                });
            }
        }

        // Anything else needs a concrete projection, including an unprojected
        // scan whose decoder output does not line up with the output schema.
        let materialized_identity;
        let projection = match projection {
            Some(projection) => projection,
            None => {
                materialized_identity = ProjectionExprs::identity(output_schema);
                &materialized_identity
            }
        };

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
        let read_plan = build_projection_read_plan(
            projection_for_read_plan.expr_iter(),
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
        // stream schema, so future widening of the mask (e.g. for post-scan
        // filter columns) does not flip this flag.
        let replace_schema = projector.output_schema() != output_schema;

        Ok(Self {
            projection_mask: read_plan.projection_mask,
            transform: Some(DecoderTransform {
                projector,
                output_schema: Arc::clone(output_schema),
                replace_schema,
            }),
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
    ///
    /// When the decoder already yields the output schema the batch is returned
    /// untouched.
    pub(crate) fn map(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let Some(transform) = self.transform.as_ref() else {
            return Ok(batch);
        };
        let projected = transform.projector.project_batch(&batch)?;
        if !transform.replace_schema {
            return Ok(projected);
        }
        let (_stream_schema, arrays, num_rows) = projected.into_parts();
        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&transform.output_schema),
            arrays,
            &options,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::projection::ProjectionExpr;
    use parquet::arrow::ArrowSchemaConverter;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]))
    }

    fn test_batch(schema: &SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn absent_projection_reads_all_columns_without_transform() {
        let schema = test_schema();
        let parquet_schema = ArrowSchemaConverter::new().convert(&schema).unwrap();

        let decoder_projection =
            DecoderProjection::try_new(None, &schema, &parquet_schema, &schema, None)
                .unwrap();

        assert!(decoder_projection.transform.is_none());
        assert_eq!(decoder_projection.projection_mask(), &ProjectionMask::all());

        let batch = test_batch(&schema);
        let mapped = decoder_projection.map(batch.clone()).unwrap();
        assert_eq!(mapped, batch);
    }

    #[test]
    fn absent_projection_falls_back_when_the_output_schema_differs() {
        // A file whose column carries metadata the table schema does not: the
        // decoder's own output is not the output schema, so the fallback
        // materializes the identity rather than passing batches through.
        let output_schema = test_schema();
        let physical_file_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true).with_metadata(
                std::iter::once(("k".to_string(), "v".to_string())).collect(),
            ),
            Field::new("b", DataType::Utf8, true),
        ]));
        let parquet_schema = ArrowSchemaConverter::new()
            .convert(&physical_file_schema)
            .unwrap();

        let decoder_projection = DecoderProjection::try_new(
            None,
            &physical_file_schema,
            &parquet_schema,
            &output_schema,
            None,
        )
        .unwrap();

        assert!(decoder_projection.transform.is_some());
        let mapped = decoder_projection
            .map(test_batch(&physical_file_schema))
            .unwrap();
        assert_eq!(mapped.schema(), output_schema);
        assert_eq!(mapped, test_batch(&output_schema));
    }

    #[test]
    fn narrowing_projection_masks_and_transforms() {
        let schema = test_schema();
        let parquet_schema = ArrowSchemaConverter::new().convert(&schema).unwrap();
        let output_schema =
            Arc::new(Schema::new(vec![Field::new("b", DataType::Utf8, true)]));

        let projection = ProjectionExprs::new([ProjectionExpr::new(
            Arc::new(Column::new("b", 1)),
            "b",
        )]);
        let decoder_projection = DecoderProjection::try_new(
            Some(&projection),
            &schema,
            &parquet_schema,
            &output_schema,
            None,
        )
        .unwrap();

        assert!(decoder_projection.transform.is_some());
        assert_ne!(decoder_projection.projection_mask(), &ProjectionMask::all());
    }
}
