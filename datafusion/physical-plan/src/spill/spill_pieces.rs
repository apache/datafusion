// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Splits a batch into byte-bounded pieces before it is written to a spill file.

use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, AsArray, RecordBatch, RecordBatchOptions, UInt32Array, make_array,
};
use arrow::compute::take;
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
/// range the slice uses, view arrays are garbage collected, top-level dictionaries drop
/// the values that no key uses, and top-level run-end encoded arrays keep only the runs
/// the slice covers.
fn compact(slice: &RecordBatch) -> Result<RecordBatch> {
    let columns = slice
        .columns()
        .iter()
        .map(|column| {
            if let DataType::RunEndEncoded(_, _) = column.data_type() {
                // `MutableArrayData` does not support run-end encoded arrays; `take`
                // re-encodes the runs the indices select, with their values copied.
                let indices = UInt32Array::from_iter_values(0..column.len() as u32);
                return Ok(take(column.as_ref(), &indices, None)?);
            }
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
/// views and unions, whose children a row range does not bound, run-end encoded arrays
/// below the top level, or dictionaries below the top level, whose values it does not
/// prune. A batch with such a column is written whole.
fn can_split(schema: &Schema) -> bool {
    fn supported(data_type: &DataType, top_level: bool) -> bool {
        match data_type {
            DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Union(_, _) => false,
            DataType::RunEndEncoded(_, values) => {
                top_level && supported(values.data_type(), false)
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, DictionaryArray, Int32Array, ListBuilder, RunArray, StringArray,
        StringBuilder, StringViewArray, StructArray,
    };
    use arrow::datatypes::{Field, Int32Type};
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    fn batch_of(column: ArrayRef) -> RecordBatch {
        RecordBatch::try_from_iter(vec![("c", column)]).unwrap()
    }

    /// Each row of every column, formatted, so pieces can be compared with the
    /// batch they came from whatever their encoding.
    fn rows(batches: &[RecordBatch]) -> Vec<String> {
        let options = FormatOptions::default();
        batches
            .iter()
            .flat_map(|batch| {
                let formatters = batch
                    .columns()
                    .iter()
                    .map(|c| ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
                    .collect::<Vec<_>>();
                (0..batch.num_rows())
                    .map(|row| {
                        formatters
                            .iter()
                            .map(|f| f.value(row).to_string())
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn pieces(batch: &RecordBatch, bound: Option<usize>) -> Vec<(RecordBatch, usize)> {
        SpillPieces::new(batch, bound)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap()
    }

    /// Splits `batch` and checks that the pieces keep every row in order and
    /// that each is within `bound` or holds one row. Returns the piece count.
    fn check_split(batch: &RecordBatch, bound: usize) -> usize {
        let pieces = pieces(batch, Some(bound));
        for (piece, size) in &pieces {
            assert_eq!(*size, piece.get_sliced_size().unwrap());
            assert!(
                *size <= bound || piece.num_rows() == 1,
                "{} rows, {size} bytes, bound {bound}",
                piece.num_rows()
            );
        }
        let batches = pieces.into_iter().map(|(p, _)| p).collect::<Vec<_>>();
        assert_eq!(rows(&batches), rows(std::slice::from_ref(batch)));
        batches.len()
    }

    fn wide_strings(rows: usize, width: usize) -> Vec<String> {
        (0..rows)
            .map(|i| format!("{i}{}", "x".repeat(width)))
            .collect()
    }

    #[test]
    fn without_a_bound_the_batch_is_one_piece() {
        let batch = batch_of(Arc::new(StringArray::from(wide_strings(100, 1000))));
        let pieces = pieces(&batch, None);
        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0].0.num_rows(), 100);
    }

    #[test]
    fn wide_rows_are_split_in_row_order() {
        let batch = batch_of(Arc::new(StringArray::from(wide_strings(100, 1000))));
        assert!(check_split(&batch, 10 * 1024) >= 10);
    }

    #[test]
    fn view_rows_are_split_in_row_order() {
        let values = wide_strings(100, 1000);
        let batch = batch_of(Arc::new(StringViewArray::from_iter_values(&values)));
        assert!(check_split(&batch, 10 * 1024) >= 10);
    }

    #[test]
    fn skewed_rows_are_halved_until_they_fit() {
        let values = (0..256)
            .map(|i| "x".repeat(if i < 4 { 8 * 1024 } else { 8 }))
            .collect::<Vec<_>>();
        let batch = batch_of(Arc::new(StringArray::from(values)));
        assert!(check_split(&batch, 10 * 1024) > 2);
    }

    #[test]
    fn one_row_larger_than_the_bound_is_one_piece() {
        let batch = batch_of(Arc::new(StringArray::from(wide_strings(1, 100 * 1024))));
        assert_eq!(check_split(&batch, 1024), 1);
    }

    #[test]
    fn top_level_dictionaries_keep_only_their_values() {
        let values = StringArray::from(wide_strings(256, 1000));
        let keys = Int32Array::from_iter_values(0..256);
        let dictionary = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values));
        let batch = batch_of(Arc::new(dictionary.unwrap()));
        assert!(check_split(&batch, 16 * 1024) >= 16);
    }

    #[test]
    fn run_end_encoded_rows_are_split() {
        let run_ends = Int32Array::from_iter_values((1..=128).map(|i| i * 2));
        let values = StringArray::from(wide_strings(128, 1000));
        let runs = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let batch = batch_of(Arc::new(runs));
        assert!(check_split(&batch, 16 * 1024) >= 8);
    }

    #[test]
    fn lists_are_split() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in 0..100 {
            for value in wide_strings(4, 250) {
                builder.values().append_value(format!("{row}{value}"));
            }
            builder.append(true);
        }
        let batch = batch_of(Arc::new(builder.finish()));
        assert!(check_split(&batch, 10 * 1024) >= 10);
    }

    #[test]
    fn nested_dictionaries_are_written_whole() {
        let values = StringArray::from(wide_strings(64, 1000));
        let keys = Int32Array::from_iter_values(0..64);
        let dictionary =
            DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let field = Field::new("d", dictionary.data_type().clone(), false);
        let nested = StructArray::try_new(
            vec![field].into(),
            vec![Arc::new(dictionary) as ArrayRef],
            None,
        )
        .unwrap();
        let batch = batch_of(Arc::new(nested));
        let pieces = pieces(&batch, Some(1024));
        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0].0.num_rows(), 64);
    }
}
