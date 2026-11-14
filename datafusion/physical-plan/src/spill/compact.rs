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
//! | Type              | What `take` keeps                       | Fix                    |
//! |-------------------|-----------------------------------------|------------------------|
//! | View arrays       | the whole data buffers                  | `gc`                   |
//! | Dictionaries      | every value                             | keep only used values  |
//! | List views        | the whole child                         | copy the covered range |
//!
//! [`fixup`] applies these fixes at every nesting level.
//!
//! Temporary: arrow-rs `minify` (<https://github.com/apache/arrow-rs/pull/12184>) replaces
//! this module.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, GenericListViewArray, OffsetSizeTrait, RecordBatch,
    RecordBatchOptions, UInt32Array, make_array,
};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::compute::{cast, take};
use arrow::datatypes::{DataType, Field};
use datafusion_common::Result;

/// Returns a copy of `batch` whose buffers hold only the data of its rows.
pub(crate) fn compact_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(compact_array)
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &options,
    )?)
}

/// Returns a copy of `array` whose buffers hold only the data of its rows.
pub(crate) fn compact_array(array: &ArrayRef) -> Result<ArrayRef> {
    let taken = match array.data_type() {
        // `fixup` copies a list view's values, which also copies its offsets.
        DataType::ListView(_) | DataType::LargeListView(_) => Arc::clone(array),
        _ => {
            let indices = UInt32Array::from_iter_values(0..array.len() as u32);
            take(array.as_ref(), &indices, None)?
        }
    };
    fixup(&taken)
}

/// Compacts the parts of `array` that `take` keeps whole, at every nesting level. The
/// result has the same length as `array`.
fn fixup(array: &ArrayRef) -> Result<ArrayRef> {
    Ok(match array.data_type() {
        DataType::Utf8View => Arc::new(array.as_string_view().gc()),
        DataType::BinaryView => Arc::new(array.as_binary_view().gc()),
        DataType::Dictionary(key_type, _) => compact_dictionary(array, key_type)?,
        DataType::ListView(field) => compact_list_view::<i32>(array, field)?,
        DataType::LargeListView(field) => compact_list_view::<i64>(array, field)?,
        _ if array.to_data().child_data().is_empty() => Arc::clone(array),
        _ => {
            let data = array.to_data();
            let children = data
                .child_data()
                .iter()
                .map(|child| Ok(fixup(&make_array(child.clone()))?.to_data()))
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
    let new_values = fixup(&take(values.as_ref(), &UInt32Array::from(used), None)?)?;
    let new_keys = cast(&remapped, key_type)?.to_data();
    let data = array
        .to_data()
        .into_builder()
        .buffers(new_keys.buffers().to_vec())
        .offset(0)
        .child_data(vec![new_values.to_data()]);
    Ok(make_array(data.build()?))
}

/// Copies the range of child values that the rows of a list view cover, and rebases the
/// offsets onto it. Rows of a list view can share or overlap child ranges, so copying each
/// row's values separately could multiply the data. Unused values inside the range stay.
fn compact_list_view<O: OffsetSizeTrait>(
    array: &ArrayRef,
    field: &Arc<Field>,
) -> Result<ArrayRef> {
    let list = array.as_list_view::<O>();
    let rows = || {
        (0..list.len())
            .filter(|&row| list.is_valid(row) && list.sizes()[row].as_usize() > 0)
    };
    let start = rows()
        .map(|row| list.offsets()[row].as_usize())
        .min()
        .unwrap_or(0);
    let end = rows()
        .map(|row| list.offsets()[row].as_usize() + list.sizes()[row].as_usize())
        .max()
        .unwrap_or(0);
    let offsets = (0..list.len())
        .map(|row| {
            let offset = list.offsets()[row].as_usize();
            O::usize_as(offset.saturating_sub(start).min(end - start))
        })
        .collect::<Vec<_>>();
    let sizes = (0..list.len())
        .map(|row| match list.is_valid(row) {
            true => list.sizes()[row],
            false => O::usize_as(0),
        })
        .collect::<Vec<_>>();
    let values = compact_array(&list.values().slice(start, end - start))?;
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
            let compacted = compact_array(&slice)?;
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
                let compacted = compact_array(&slice)?;
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
}
