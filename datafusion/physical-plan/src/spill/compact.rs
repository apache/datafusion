// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Copies a batch into fresh buffers that hold only the data of its rows.
//!
//! The memory size of a compacted batch is exact, so a spill piece can be measured with
//! `get_record_batch_memory_size` instead of a slice-aware estimate.
//!
//! `take` with the indices `0..n` copies most types into fresh buffers. It does not compact:
//!
//! | Type              | What `take` keeps                       | Fix                        |
//! |-------------------|-----------------------------------------|----------------------------|
//! | View arrays       | the whole data buffers                  | copy the used bytes        |
//! | Dictionaries      | every value                             | keep only used values      |
//! | List views        | the whole child                         | copy the referenced ranges |
//!
//! [`fixup`] applies these fixes at every nesting level.
//!
//! The fixes never copy data that several rows share more than once into one batch. A
//! caller that splits a batch into pieces can still copy shared data once per piece. To
//! prevent that, it can keep the dictionaries of a column whole (see [`compact_batch`]).
//!
//! Temporary: arrow-rs `minify` (<https://github.com/apache/arrow-rs/pull/12184>) replaces
//! this module.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, GenericByteViewArray, GenericListViewArray,
    OffsetSizeTrait, RecordBatch, RecordBatchOptions, UInt32Array, UInt64Array,
    make_array,
};
use arrow::buffer::{Buffer, NullBuffer, ScalarBuffer};
use arrow::compute::{cast, take};
use arrow::datatypes::ByteViewType;
use arrow::datatypes::{DataType, Field};
use arrow_data::{ByteView, MAX_INLINE_VIEW_LEN};
use datafusion_common::Result;

/// Returns a copy of `batch` whose buffers hold only the data of its rows.
///
/// The dictionaries of column `i` keep all their values when `keep_dictionaries[i]` is
/// true. The piece then shares the values with `batch`, so the IPC writer writes them once
/// for all the pieces of `batch`, and the reader decodes them once.
pub(crate) fn compact_batch(
    batch: &RecordBatch,
    keep_dictionaries: &[bool],
) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .zip(keep_dictionaries)
        .map(|(column, &keep)| compact_array(column, keep))
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &options,
    )?)
}

/// Returns a copy of `array` whose buffers hold only the data of its rows, except the
/// values of its dictionaries when `keep_dictionaries` is true.
pub(crate) fn compact_array(
    array: &ArrayRef,
    keep_dictionaries: bool,
) -> Result<ArrayRef> {
    let taken = match array.data_type() {
        // `fixup` copies a list view's values, which also copies its offsets.
        DataType::ListView(_) | DataType::LargeListView(_) => Arc::clone(array),
        _ => {
            let indices = UInt32Array::from_iter_values(0..array.len() as u32);
            take(array.as_ref(), &indices, None)?
        }
    };
    fixup(&taken, keep_dictionaries)
}

/// Compacts the parts of `array` that `take` keeps whole, at every nesting level. The
/// result has the same length as `array`.
fn fixup(array: &ArrayRef, keep_dictionaries: bool) -> Result<ArrayRef> {
    Ok(match array.data_type() {
        DataType::Utf8View => Arc::new(compact_views(array.as_string_view())?),
        DataType::BinaryView => Arc::new(compact_views(array.as_binary_view())?),
        // `take` copies the keys and shares the values.
        DataType::Dictionary(_, _) if keep_dictionaries => Arc::clone(array),
        DataType::Dictionary(key_type, _) => compact_dictionary(array, key_type)?,
        DataType::ListView(field) => {
            compact_list_view::<i32>(array, field, keep_dictionaries)?
        }
        DataType::LargeListView(field) => {
            compact_list_view::<i64>(array, field, keep_dictionaries)?
        }
        _ if array.to_data().child_data().is_empty() => Arc::clone(array),
        _ => {
            let data = array.to_data();
            let children = data
                .child_data()
                .iter()
                .map(|child| {
                    Ok(fixup(&make_array(child.clone()), keep_dictionaries)?.to_data())
                })
                .collect::<Result<Vec<_>>>()?;
            make_array(data.into_builder().child_data(children).build()?)
        }
    })
}

/// Keeps only the dictionary values that a key references.
fn compact_dictionary(array: &ArrayRef, key_type: &DataType) -> Result<ArrayRef> {
    let dictionary = array.as_any_dictionary();
    let values = dictionary.values();
    if values.is_empty() {
        // Every key is null, and `normalized_keys` panics without values.
        return Ok(Arc::clone(array));
    }
    let mut new_index = vec![u32::MAX; values.len()];
    let mut used = vec![];
    let keys = dictionary.normalized_keys();
    // A valid key can reference a null value, so `logical_nulls` would also skip rows
    // whose value is null. Their keys must keep a null value to reference.
    let nulls = dictionary.keys().nulls();
    let remapped =
        UInt32Array::from_iter_values(keys.iter().enumerate().map(|(row, &key)| {
            if nulls.is_some_and(|n| n.is_null(row)) {
                return 0;
            }
            if new_index[key] == u32::MAX {
                new_index[key] = used.len() as u32;
                used.push(key as u32);
            }
            new_index[key]
        }));
    let new_values = fixup(
        &take(values.as_ref(), &UInt32Array::from(used), None)?,
        false,
    )?;
    let new_keys = cast(&remapped, key_type)?.to_data();
    let data = array
        .to_data()
        .into_builder()
        .buffers(new_keys.buffers().to_vec())
        .offset(0)
        .child_data(vec![new_values.to_data()]);
    Ok(make_array(data.build()?))
}

/// Copies the bytes that the views of `array` reference into new data buffers.
///
/// Views can share bytes, such as the rows that `take` repeats. `gc` copies the bytes of
/// each view, so shared bytes would be copied once per view. Here the referenced ranges
/// of each buffer are merged first, so each byte is copied at most once.
fn compact_views<T: ByteViewType + ?Sized>(
    array: &GenericByteViewArray<T>,
) -> Result<GenericByteViewArray<T>> {
    let views = array.views();
    let long =
        |row: usize| array.is_valid(row) && views[row] as u32 > MAX_INLINE_VIEW_LEN;
    // (buffer, start, end) of each long view, merged where they overlap or touch.
    let mut ranges: Vec<(u32, u32, u32)> = (0..array.len())
        .filter(|&row| long(row))
        .map(|row| {
            let view = ByteView::from(views[row]);
            (view.buffer_index, view.offset, view.offset + view.length)
        })
        .collect();
    ranges.sort_unstable();
    let mut merged: Vec<(u32, u32, u32)> = Vec::with_capacity(ranges.len());
    for (buffer, start, end) in ranges {
        match merged.last_mut() {
            Some(last) if last.0 == buffer && start <= last.2 => last.2 = last.2.max(end),
            _ => merged.push((buffer, start, end)),
        }
    }
    // The new buffer and offset of each merged range. A view offset is a `u32`, so a new
    // buffer starts before one would grow past `u32::MAX` bytes.
    let total: usize = merged.iter().map(|r| (r.2 - r.1) as usize).sum();
    let mut new_buffers: Vec<Vec<u8>> = vec![];
    let mut copied = 0;
    let mut placed: Vec<(u32, u32)> = Vec::with_capacity(merged.len());
    for &(buffer, start, end) in &merged {
        let bytes = &array.data_buffers()[buffer as usize][start as usize..end as usize];
        if new_buffers
            .last()
            .is_none_or(|b| b.len() + bytes.len() > u32::MAX as usize)
        {
            let capacity = (total - copied).min(u32::MAX as usize).max(bytes.len());
            new_buffers.push(Vec::with_capacity(capacity));
        }
        let index = new_buffers.len() - 1;
        let target = &mut new_buffers[index];
        placed.push((index as u32, target.len() as u32));
        target.extend_from_slice(bytes);
        copied += bytes.len();
    }
    let new_views = (0..array.len())
        .map(|row| {
            if !array.is_valid(row) {
                return 0;
            }
            if !long(row) {
                return views[row];
            }
            let view = ByteView::from(views[row]);
            // The last merged range that starts at or before the view holds it.
            let index = merged
                .partition_point(|r| (r.0, r.1) <= (view.buffer_index, view.offset))
                - 1;
            let (new_buffer, new_start) = placed[index];
            ByteView {
                buffer_index: new_buffer,
                offset: new_start + (view.offset - merged[index].1),
                ..view
            }
            .as_u128()
        })
        .collect::<ScalarBuffer<u128>>();
    let new_buffers: Vec<Buffer> = new_buffers.into_iter().map(Buffer::from).collect();
    Ok(GenericByteViewArray::try_new(
        new_views,
        new_buffers,
        array.nulls().cloned(),
    )?)
}

/// Copies the child values that the rows of a list view reference, and rebases the
/// offsets onto them.
///
/// Rows of a list view can share or overlap child ranges, so the ranges of all rows
/// are merged first, and each merged range is copied once. Elements that no row
/// references are dropped. A copy of the whole covered range would keep them: after a
/// sort, the rows of one piece reference elements spread over the whole child, so
/// every piece would copy almost all of it.
fn compact_list_view<O: OffsetSizeTrait>(
    array: &ArrayRef,
    field: &Arc<Field>,
    keep_dictionaries: bool,
) -> Result<ArrayRef> {
    let list = array.as_list_view::<O>();
    let live = |row: usize| list.is_valid(row) && list.sizes()[row].as_usize() > 0;
    let range = |row: usize| {
        let offset = list.offsets()[row].as_usize();
        (offset, offset + list.sizes()[row].as_usize())
    };
    let mut ranges: Vec<(usize, usize)> = (0..list.len())
        .filter(|&row| live(row))
        .map(range)
        .collect();
    ranges.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        match merged.last_mut() {
            Some(last) if start <= last.1 => last.1 = last.1.max(end),
            _ => merged.push((start, end)),
        }
    }
    // The offset of each merged range in the new child.
    let mut placed = Vec::with_capacity(merged.len());
    let mut total = 0;
    for &(start, end) in &merged {
        placed.push(total);
        total += end - start;
    }
    let indices = UInt64Array::from_iter_values(
        merged
            .iter()
            .flat_map(|&(start, end)| (start as u64)..(end as u64)),
    );
    let offsets = (0..list.len())
        .map(|row| {
            if !live(row) {
                return O::usize_as(0);
            }
            let offset = list.offsets()[row].as_usize();
            let index = merged.partition_point(|r| r.0 <= offset) - 1;
            O::usize_as(placed[index] + offset - merged[index].0)
        })
        .collect::<Vec<_>>();
    let sizes = (0..list.len())
        .map(|row| match live(row) {
            true => list.sizes()[row],
            false => O::usize_as(0),
        })
        .collect::<Vec<_>>();
    let values = fixup(
        &take(list.values().as_ref(), &indices, None)?,
        keep_dictionaries,
    )?;
    Ok(Arc::new(GenericListViewArray::<O>::try_new(
        Arc::clone(field),
        ScalarBuffer::from(offsets),
        ScalarBuffer::from(sizes),
        values,
        list.nulls()
            .map(|n| NullBuffer::from(n.iter().collect::<Vec<_>>())),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        DictionaryArray, FixedSizeListArray, Int32Array, ListArray, ListViewArray,
        RunArray, StringArray, StringViewArray, StructArray, UnionArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Int32Type, UnionFields};

    const ROWS: usize = 1000;
    const WIDTH: usize = 1000;

    /// A 10-row slice of each type compacts to about the data of its rows, and keeps its
    /// values. The whole arrays hold about `ROWS * WIDTH` bytes.
    #[test]
    fn compacted_slice_holds_only_its_rows() -> Result<()> {
        let value = |i: usize| format!("{i:08}{}", "x".repeat(WIDTH));
        let strings: ArrayRef =
            Arc::new(StringArray::from_iter_values((0..ROWS).map(value)));
        let views: ArrayRef =
            Arc::new(StringViewArray::from_iter_values((0..ROWS).map(value)));
        let item = |data_type| Arc::new(Field::new_list_field(data_type, false));
        let ones = || OffsetBuffer::from_lengths(vec![1; ROWS]);
        let union_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ],
        )?;
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("Utf8", Arc::clone(&strings)),
            ("Utf8View", Arc::clone(&views)),
            (
                "List",
                Arc::new(ListArray::new(
                    item(DataType::Utf8),
                    ones(),
                    Arc::clone(&strings),
                    None,
                )),
            ),
            (
                "List<Utf8View>",
                Arc::new(ListArray::new(
                    item(DataType::Utf8View),
                    ones(),
                    Arc::clone(&views),
                    None,
                )),
            ),
            (
                "FixedSizeList",
                Arc::new(FixedSizeListArray::new(
                    item(DataType::Utf8),
                    1,
                    Arc::clone(&strings),
                    None,
                )),
            ),
            (
                "ListView",
                Arc::new(ListViewArray::new(
                    item(DataType::Utf8),
                    ScalarBuffer::from((0..ROWS as i32).collect::<Vec<_>>()),
                    ScalarBuffer::from(vec![1; ROWS]),
                    Arc::clone(&strings),
                    None,
                )),
            ),
            (
                "Struct",
                Arc::new(StructArray::new(
                    vec![Field::new("s", DataType::Utf8, false)].into(),
                    vec![Arc::clone(&strings)],
                    None,
                )),
            ),
            (
                "SparseUnion",
                Arc::new(UnionArray::try_new(
                    union_fields.clone(),
                    ScalarBuffer::from(vec![1; ROWS]),
                    None,
                    vec![
                        Arc::new(Int32Array::from(vec![0; ROWS])),
                        Arc::clone(&strings),
                    ],
                )?),
            ),
            (
                "DenseUnion",
                Arc::new(UnionArray::try_new(
                    union_fields,
                    ScalarBuffer::from(vec![1; ROWS]),
                    Some(ScalarBuffer::from((0..ROWS as i32).collect::<Vec<_>>())),
                    vec![
                        Arc::new(Int32Array::from(Vec::<i32>::new())),
                        Arc::clone(&strings),
                    ],
                )?),
            ),
            (
                "RunEndEncoded",
                Arc::new(RunArray::<Int32Type>::try_new(
                    &Int32Array::from_iter_values(1..=ROWS as i32),
                    strings.as_ref(),
                )?),
            ),
            (
                "Dictionary",
                Arc::new(DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from_iter_values(0..ROWS as i32),
                    Arc::clone(&strings),
                )?),
            ),
            (
                "Dictionary<Utf8View>",
                Arc::new(DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from_iter_values(0..ROWS as i32),
                    Arc::clone(&views),
                )?),
            ),
        ];
        for (name, column) in columns {
            let slice = column.slice(10, 10);
            let compacted = compact_array(&slice, false)?;
            assert_eq!(compacted.as_ref(), slice.as_ref(), "{name}");
            let size = compacted.get_array_memory_size();
            assert!(size < 4 * 10 * WIDTH, "{name}: {size} bytes");
        }
        Ok(())
    }

    /// A valid key can reference a null value. Compaction keeps that value, at every
    /// nesting level.
    #[test]
    fn compaction_keeps_null_dictionary_values() -> Result<()> {
        let dictionary = |keys: Vec<i32>| -> ArrayRef {
            let values = StringArray::from(vec![Some("a"), None, Some("b")]);
            Arc::new(DictionaryArray::new(
                Int32Array::from(keys),
                Arc::new(values),
            ))
        };
        let item = |array: &ArrayRef| {
            Arc::new(Field::new_list_field(array.data_type().clone(), true))
        };
        let mixed = dictionary(vec![1, 0, 2, 1, 1, 0]);
        let only_null = dictionary(vec![1, 1, 1]);
        let columns: Vec<ArrayRef> = vec![
            Arc::clone(&mixed),
            Arc::clone(&only_null),
            Arc::new(ListArray::new(
                item(&mixed),
                OffsetBuffer::from_lengths(vec![2, 1, 3]),
                Arc::clone(&mixed),
                None,
            )),
            Arc::new(StructArray::new(
                vec![Field::new("d", mixed.data_type().clone(), true)].into(),
                vec![Arc::clone(&mixed)],
                None,
            )),
            Arc::new(ListViewArray::new(
                item(&only_null),
                ScalarBuffer::from(vec![0, 1]),
                ScalarBuffer::from(vec![1, 2]),
                Arc::clone(&only_null),
                None,
            )),
        ];
        for column in columns {
            for (offset, len) in [(0, column.len()), (1, column.len() - 1)] {
                let slice = column.slice(offset, len);
                let compacted = compact_array(&slice, false)?;
                compacted.to_data().validate_full()?;
                assert_eq!(compacted.as_ref(), slice.as_ref(), "{}", column.data_type());
                assert_eq!(
                    compacted.logical_null_count(),
                    slice.logical_null_count(),
                    "{}",
                    column.data_type()
                );
            }
        }
        Ok(())
    }

    /// Views that share bytes keep sharing them, so a compacted array holds each value
    /// once.
    #[test]
    fn compacted_views_copy_shared_bytes_once() -> Result<()> {
        let values = StringViewArray::from_iter_values(
            (0..3).map(|i| format!("{i}{}", "x".repeat(WIDTH))),
        );
        let indices = UInt32Array::from_iter_values((0..ROWS as u32).map(|i| i % 3));
        let shared = take(&values, &indices, None)?;
        let compacted = compact_array(&shared.slice(1, 100), false)?;
        assert_eq!(compacted.as_ref(), shared.slice(1, 100).as_ref());
        let bytes: usize = compacted
            .as_string_view()
            .data_buffers()
            .iter()
            .map(|b| b.len())
            .sum();
        assert_eq!(bytes, 3 * (WIDTH + 1));
        Ok(())
    }

    /// With `keep_dictionaries`, a piece shares the dictionary values of its batch.
    #[test]
    fn kept_dictionaries_share_their_values() -> Result<()> {
        let values: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..10).map(|i| format!("{i}{}", "x".repeat(WIDTH))),
        ));
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
            Int32Array::from_iter_values((0..ROWS as i32).map(|i| i % 10)),
            Arc::clone(&values),
        )?);
        let compacted = compact_array(&dictionary.slice(5, 3), true)?;
        assert_eq!(compacted.as_ref(), dictionary.slice(5, 3).as_ref());
        assert!(
            compacted
                .as_any_dictionary()
                .values()
                .to_data()
                .ptr_eq(&values.to_data())
        );
        Ok(())
    }
}
