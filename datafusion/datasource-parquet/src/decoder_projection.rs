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

use arrow::array::{ArrayData, ArrayRef, RecordBatch, RecordBatchOptions, cast::AsArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema, SchemaRef};

use datafusion_common::Result;
use datafusion_physical_expr::expressions::{CastExpr, Column};
use datafusion_physical_expr::projection::{ProjectionExprs, Projector};
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr_adapter::replace_columns_with_literals;
use parking_lot::Mutex;

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
    projector: Projector,
    /// Input schema and per-column conversions used when `binary_as_string`
    /// exposes a physical binary dictionary as a string dictionary.
    dictionary_value_casts: Option<DictionaryValueCasts>,
    output_schema: SchemaRef,
    /// `true` when the projector's output schema differs from `output_schema`
    /// in metadata / nullability and [`map`](Self::map) must rebuild the batch
    /// with `output_schema`.
    replace_schema: bool,
}

/// Cast dictionary values once per Parquet dictionary instead of once per
/// output batch. A row group's dictionary is shared by all of its batches.
struct DictionaryValueCasts {
    input_schema: SchemaRef,
    columns: Vec<DictionaryValueCast>,
}

struct DictionaryValueCast {
    column_index: usize,
    target_value_type: DataType,
    cached_values: Mutex<Option<CachedDictionaryValues>>,
}

struct CachedDictionaryValues {
    source: ArrayData,
    cast: ArrayRef,
}

impl DictionaryValueCast {
    fn cast(&self, array: &ArrayRef) -> Result<ArrayRef> {
        let dictionary = array.as_any_dictionary();
        let source_values = dictionary.values();
        let source_data = source_values.to_data();
        let mut cached = self.cached_values.lock();
        let cast_values = match cached.as_ref() {
            Some(cached) if cached.source.ptr_eq(&source_data) => {
                Arc::clone(&cached.cast)
            }
            _ => {
                let cast_values = cast(source_values, &self.target_value_type)?;
                *cached = Some(CachedDictionaryValues {
                    source: source_data,
                    cast: Arc::clone(&cast_values),
                });
                cast_values
            }
        };
        Ok(dictionary.with_values(cast_values))
    }
}

impl DictionaryValueCasts {
    fn try_new(projection: &ProjectionExprs, stream_schema: &SchemaRef) -> Option<Self> {
        let mut columns = Vec::new();
        for projection in projection.iter() {
            let Some(cast) = projection.expr.downcast_ref::<CastExpr>() else {
                continue;
            };
            let Some(column) = cast.expr().downcast_ref::<Column>() else {
                continue;
            };
            let source_type = stream_schema.field(column.index()).data_type();
            let Some(target_value_type) =
                binary_dictionary_string_cast(source_type, cast.cast_type())
            else {
                continue;
            };
            if columns.iter().any(|existing: &DictionaryValueCast| {
                existing.column_index == column.index()
            }) {
                continue;
            }
            columns.push(DictionaryValueCast {
                column_index: column.index(),
                target_value_type,
                cached_values: Mutex::new(None),
            });
        }
        if columns.is_empty() {
            return None;
        }

        let mut fields = stream_schema.fields().to_vec();
        for column in &columns {
            let field = stream_schema.field(column.column_index);
            let DataType::Dictionary(key_type, _) = field.data_type() else {
                unreachable!("binary_dictionary_string_cast checked the source type")
            };
            fields[column.column_index] =
                Arc::new(field.as_ref().clone().with_data_type(DataType::Dictionary(
                    key_type.clone(),
                    Box::new(column.target_value_type.clone()),
                )));
        }
        let input_schema = Arc::new(Schema::new_with_metadata(
            fields,
            stream_schema.metadata().clone(),
        ));

        Some(Self {
            input_schema,
            columns,
        })
    }

    fn cast_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut arrays = batch.columns().to_vec();
        for column in &self.columns {
            arrays[column.column_index] = column.cast(&arrays[column.column_index])?;
        }
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.input_schema),
            arrays,
            &options,
        )?)
    }
}

fn binary_dictionary_string_cast(
    source_type: &DataType,
    target_type: &DataType,
) -> Option<DataType> {
    let (
        DataType::Dictionary(source_key, source_value),
        DataType::Dictionary(target_key, target_value),
    ) = (source_type, target_type)
    else {
        return None;
    };
    if source_key != target_key
        || !matches!(
            (source_value.as_ref(), target_value.as_ref()),
            (
                DataType::Binary | DataType::LargeBinary,
                DataType::Utf8 | DataType::LargeUtf8
            )
        )
    {
        return None;
    }
    Some(target_value.as_ref().clone())
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
    pub(crate) fn try_new(
        projection: &ProjectionExprs,
        physical_file_schema: &SchemaRef,
        parquet_schema: &SchemaDescriptor,
        output_schema: &SchemaRef,
        virtual_state: Option<&VirtualColumnsState>,
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
        let dictionary_value_casts =
            DictionaryValueCasts::try_new(&rebased_projection, &stream_schema);
        let projector_input_schema = dictionary_value_casts
            .as_ref()
            .map(|casts| &casts.input_schema)
            .unwrap_or(&stream_schema);
        let projector = rebased_projection.make_projector(projector_input_schema)?;

        // Compare against the projector's *output* schema rather than the
        // stream schema, so future widening of the mask (e.g. for post-scan
        // filter columns) does not flip this flag.
        let replace_schema = projector.output_schema() != output_schema;

        Ok(Self {
            projection_mask: read_plan.projection_mask,
            projector,
            dictionary_value_casts,
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
        let cast_batch;
        let batch = match &self.dictionary_value_casts {
            Some(casts) => {
                cast_batch = casts.cast_batch(batch)?;
                &cast_batch
            }
            None => batch,
        };
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
