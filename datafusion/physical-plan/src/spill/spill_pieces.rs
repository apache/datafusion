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

//! Splits a batch into byte-bounded pieces before it is written to a spill file.

use std::ops::Range;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, RecordBatch, RecordBatchOptions, make_array};
use arrow::datatypes::{DataType, Schema};
use arrow_data::transform::MutableArrayData;
use arrow_select::dictionary::garbage_collect_any_dictionary;
use datafusion_common::Result;

use super::gc_view_arrays;
use super::spill_manager::GetSlicedSize;

/// The pieces that one appended batch is written as, in row order.
///
/// Without a bound, or when the batch is already within it, there is one piece: the batch
/// after [`gc_view_arrays`], as it is written without a bound. Its dictionaries are not
/// changed, so the batches of a spill file can keep sharing one dictionary.
///
/// When the batch is larger than `max_batch_bytes`, it is cut into equal row ranges by its
/// average row size. Each range is copied out with [`compact`], so that it holds only its
/// own rows, and measured with [`GetSlicedSize`]. A range that is still too large (the rows
/// differ in size) is halved until it fits or has one row. Pieces are produced one at a
/// time, so that the caller can write each one before the next is copied.
///
/// Schemas that [`compact`] cannot narrow (see [`can_split`]) are written whole.
pub(crate) struct SpillPieces {
    batch: RecordBatch,
    max_batch_bytes: usize,
    /// Row ranges still to produce, the next one last.
    pending: Vec<Range<usize>>,
    /// The whole batch, when it is written as one piece.
    whole: Option<(RecordBatch, usize)>,
}

impl SpillPieces {
    pub(crate) fn new(
        batch: &RecordBatch,
        max_batch_bytes: Option<usize>,
    ) -> Result<Self> {
        let batch = gc_view_arrays(batch)?;
        let size = batch.get_sliced_size()?;
        let rows = batch.num_rows();
        let whole = |batch, size| Self {
            whole: Some((batch, size)),
            batch: RecordBatch::new_empty(Arc::new(Schema::empty())),
            max_batch_bytes: 0,
            pending: vec![],
        };
        let Some(max_batch_bytes) = max_batch_bytes
            .filter(|max| size > *max && rows > 1 && can_split(batch.schema_ref()))
        else {
            return Ok(whole(batch, size));
        };
        // A shared dictionary can hold many values that no row of this batch uses. Drop
        // them once before choosing the row ranges, so that the ranges are sized by what
        // the batch uses and the pieces do not each prune the whole dictionary again.
        let batch = prune_dictionaries(&batch)?;
        let size = batch.get_sliced_size()?;
        if size <= max_batch_bytes {
            return Ok(whole(batch, size));
        }
        // The sliced size over-counts nested columns, whose children are not narrowed by
        // a slice, so this can give more pieces than needed but not fewer.
        let pieces = size.div_ceil(max_batch_bytes.max(1)).min(rows);
        let rows_per_piece = rows.div_ceil(pieces);
        let pending = (0..rows)
            .step_by(rows_per_piece)
            .map(|start| start..(start + rows_per_piece).min(rows))
            .rev()
            .collect();
        Ok(Self {
            batch,
            max_batch_bytes,
            pending,
            whole: None,
        })
    }
}

impl Iterator for SpillPieces {
    /// A piece and its size, as [`GetSlicedSize`] measures it.
    type Item = Result<(RecordBatch, usize)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(whole) = self.whole.take() {
            return Some(Ok(whole));
        }
        while let Some(range) = self.pending.pop() {
            let piece = match compact(&self.batch.slice(range.start, range.len())) {
                Ok(piece) => piece,
                Err(e) => return Some(Err(e)),
            };
            let size = match piece.get_sliced_size() {
                Ok(size) => size,
                Err(e) => return Some(Err(e)),
            };
            if size > self.max_batch_bytes && range.len() > 1 {
                let mid = range.start + range.len() / 2;
                self.pending.push(mid..range.end);
                self.pending.push(range.start..mid);
                continue;
            }
            return Some(Ok((piece, size)));
        }
        None
    }
}

/// Copies a slice so that it holds only its own rows: list children are narrowed to the
/// range the slice uses, view arrays are garbage collected, and top-level dictionaries drop
/// the values that no key uses.
fn compact(slice: &RecordBatch) -> Result<RecordBatch> {
    let columns = slice
        .columns()
        .iter()
        .map(|column| {
            let data = column.to_data();
            let mut copy = MutableArrayData::new(vec![&data], false, data.len());
            copy.try_extend(0, 0, data.len())?;
            let copied = make_array(copy.freeze());
            Ok(match copied.data_type() {
                DataType::Dictionary(_, _) => {
                    garbage_collect_any_dictionary(copied.as_any_dictionary())?
                }
                _ => copied,
            })
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(slice.num_rows()));
    let piece = RecordBatch::try_new_with_options(slice.schema(), columns, &options)?;
    gc_view_arrays(&piece)
}

/// Drops the values of top-level dictionaries that no key uses.
fn prune_dictionaries(batch: &RecordBatch) -> Result<RecordBatch> {
    if !batch
        .schema_ref()
        .fields()
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Dictionary(_, _)))
    {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            Ok(match column.data_type() {
                DataType::Dictionary(_, _) => {
                    garbage_collect_any_dictionary(column.as_any_dictionary())?
                }
                _ => Arc::clone(column),
            })
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &options,
    )?)
}

/// Whether [`compact`] narrows every column of `schema` to its rows. It does not for list
/// views and unions, whose children a row range does not bound, run-end encoded arrays, or
/// dictionaries below the top level, whose values it does not prune. A batch with such a
/// column is written whole.
fn can_split(schema: &Schema) -> bool {
    fn supported(data_type: &DataType, top_level: bool) -> bool {
        match data_type {
            DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Union(_, _)
            | DataType::RunEndEncoded(_, _) => false,
            DataType::Dictionary(_, values) => top_level && supported(values, false),
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => supported(field.data_type(), false),
            DataType::Struct(fields) => fields
                .iter()
                .all(|field| supported(field.data_type(), false)),
            _ => true,
        }
    }
    schema
        .fields()
        .iter()
        .all(|field| supported(field.data_type(), true))
}
