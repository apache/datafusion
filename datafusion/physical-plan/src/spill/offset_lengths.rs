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

//! Writes offsets to compressed spill files as lengths, see
//! [`offsets_to_lengths`].

use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, OffsetSizeTrait, RecordBatch, RecordBatchOptions, make_array,
};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow_data::{ArrayData, ArrayDataBuilder};
use datafusion_common::Result;

/// Returns whether [`offsets_to_lengths`] rewrites arrays of `data_type`.
pub(super) fn has_offsets(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::Map(_, _) => true,
        DataType::Struct(fields) => fields.iter().any(|f| has_offsets(f.data_type())),
        DataType::FixedSizeList(field, _) => has_offsets(field.data_type()),
        _ => false,
    }
}

/// Rewrites every offsets buffer of `batch` as the lengths it encodes, before
/// the batch is written to a compressed spill file.
///
/// Offsets are a running total that never repeats, so zstd and lz4 barely
/// compress them, while the lengths of similar values repeat
/// (<https://github.com/apache/datafusion/issues/26627>). For `n` values the
/// buffer holds `[0, len_0, ..., len_(n-2), total]`. The IPC writer only reads
/// the first and last entries, to slice the values, so it writes the rewritten
/// arrays as they are. [`lengths_to_offsets`] restores the offsets.
///
/// The returned batch breaks the offsets invariant, so it must only be passed
/// to the IPC writer.
pub(super) fn offsets_to_lengths(batch: &RecordBatch) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            if has_offsets(column.data_type()) {
                make_array(encode(&column.to_data()))
            } else {
                Arc::clone(column)
            }
        })
        .collect();
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &options,
    )?)
}

/// Restores the offsets of a batch written by [`offsets_to_lengths`], in
/// place unless a buffer is shared.
pub(super) fn lengths_to_offsets(batch: RecordBatch) -> Result<RecordBatch> {
    let (schema, columns, num_rows) = batch.into_parts();
    let columns: Vec<ArrayRef> = columns
        .into_iter()
        .map(|column| {
            if has_offsets(column.data_type()) {
                // `into_data` drops `column`, leaving its buffers unshared
                make_array(decode(column.into_data()))
            } else {
                column
            }
        })
        .collect();
    let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
    Ok(RecordBatch::try_new_with_options(
        schema, columns, &options,
    )?)
}

fn encode(data: &ArrayData) -> ArrayData {
    let (buffers, child_data) = match data.data_type() {
        DataType::Utf8 | DataType::Binary | DataType::List(_) | DataType::Map(_, _) => {
            encode_offsets::<i32>(data)
        }
        DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_) => {
            encode_offsets::<i64>(data)
        }
        DataType::Struct(_) => (
            vec![],
            data.child_data()
                .iter()
                .map(|child| encode_child(&child.slice(data.offset(), data.len())))
                .collect(),
        ),
        DataType::FixedSizeList(_, size) => {
            let size = *size as usize;
            let child =
                data.child_data()[0].slice(data.offset() * size, data.len() * size);
            (vec![], vec![encode_child(&child)])
        }
        _ => return data.clone(),
    };
    // SAFETY: only the IPC writer reads the result, see `offsets_to_lengths`
    unsafe {
        ArrayDataBuilder::new(data.data_type().clone())
            .len(data.len())
            .nulls(data.nulls().cloned())
            .buffers(buffers)
            .child_data(child_data)
            .build_unchecked()
    }
}

fn encode_child(child: &ArrayData) -> ArrayData {
    if has_offsets(child.data_type()) {
        encode(child)
    } else {
        child.clone()
    }
}

/// Returns the buffers and children of an array with offsets, its offsets
/// rewritten as lengths and its values sliced to the ones they cover.
fn encode_offsets<O: OffsetSizeTrait>(data: &ArrayData) -> (Vec<Buffer>, Vec<ArrayData>) {
    // The IPC writer writes empty arrays without reading their buffers
    if data.is_empty() {
        return (data.buffers().to_vec(), data.child_data().to_vec());
    }
    let (lengths, values) = lengths::<O>(data);
    match data.child_data().first() {
        Some(child) => (
            vec![lengths],
            vec![encode_child(&child.slice(values.start, values.len()))],
        ),
        None => (
            vec![
                lengths,
                data.buffers()[1].slice_with_length(values.start, values.len()),
            ],
            vec![],
        ),
    }
}

/// Returns the lengths of the values of `data`, laid out as described in
/// [`offsets_to_lengths`], and the range of values they cover.
fn lengths<O: OffsetSizeTrait>(data: &ArrayData) -> (Buffer, Range<usize>) {
    let len = data.len();
    let offsets = &data.buffers()[0].typed_data::<O>()[data.offset()..][..=len];
    let (first, last) = (offsets[0], offsets[len]);
    let mut lengths = Vec::with_capacity(len + 1);
    lengths.push(O::default());
    lengths.extend(
        offsets[1..len]
            .iter()
            .zip(&offsets[..len - 1])
            .map(|(end, start)| *end - *start),
    );
    lengths.push(last - first);
    (lengths.into(), first.as_usize()..last.as_usize())
}

fn decode(data: ArrayData) -> ArrayData {
    let (data_type, len, nulls, offset, mut buffers, child_data) = data.into_parts();
    match &data_type {
        DataType::Utf8 | DataType::Binary | DataType::List(_) | DataType::Map(_, _) => {
            restore_offsets::<i32>(&mut buffers[0])
        }
        DataType::LargeUtf8 | DataType::LargeBinary | DataType::LargeList(_) => {
            restore_offsets::<i64>(&mut buffers[0])
        }
        _ => {}
    }
    let child_data = child_data
        .into_iter()
        .map(|child| {
            if has_offsets(child.data_type()) {
                decode(child)
            } else {
                child
            }
        })
        .collect();
    // SAFETY: the offsets are the ones of the array that was spilled
    unsafe {
        ArrayDataBuilder::new(data_type)
            .len(len)
            .offset(offset)
            .nulls(nulls)
            .buffers(buffers)
            .child_data(child_data)
            .build_unchecked()
    }
}

/// Turns the lengths written by [`offsets_to_lengths`] back into offsets.
fn restore_offsets<O: OffsetSizeTrait>(buffer: &mut Buffer) {
    fn running_sum<O: OffsetSizeTrait>(lengths: &mut [O]) {
        // The last entry already holds the total
        let len = lengths.len().saturating_sub(1);
        let mut sum = O::default();
        for length in &mut lengths[..len] {
            sum += *length;
            *length = sum;
        }
    }
    *buffer = match std::mem::take(buffer).into_mutable() {
        Ok(mut lengths) => {
            running_sum(lengths.typed_data_mut::<O>());
            lengths.into()
        }
        // The IPC writer stores a buffer uncompressed when compressing it does
        // not help, and the reader then slices it out of the message body
        Err(shared) => {
            let mut lengths = shared.typed_data::<O>().to_vec();
            running_sum(&mut lengths);
            lengths.into()
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_restore_offsets_in_place_unless_shared() {
        // Values of lengths 3, 4 and 5
        let lengths = Buffer::from_vec(vec![0_i32, 3, 4, 12]);
        let offsets = [0, 3, 7, 12];

        let mut shared = lengths.clone();
        restore_offsets::<i32>(&mut shared);
        assert_eq!(shared.typed_data::<i32>(), offsets);
        assert_ne!(shared.as_ptr(), lengths.as_ptr());

        let ptr = lengths.as_ptr();
        let mut unshared = lengths;
        restore_offsets::<i32>(&mut unshared);
        assert_eq!(unshared.typed_data::<i32>(), offsets);
        assert_eq!(unshared.as_ptr(), ptr);
    }
}
