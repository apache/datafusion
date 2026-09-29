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

//! Per-row sizes of a batch once it is written to a spill file.

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::DataType;
use arrow_data::MAX_INLINE_VIEW_LEN;

use super::VIEW_SIZE_BYTES;

/// Estimates how many bytes each row of a batch takes once written to a spill
/// file, where view arrays are compacted (see [`super::gc_view_arrays`]).
///
/// A row counts its fixed-width values, the offsets and bytes of its
/// variable-width values, and the elements of its nested values. Buffers that
/// every row shares, such as validity bitmaps and dictionary values, are not
/// counted.
pub(crate) struct SpilledRowSizes(ColumnSize);

impl SpilledRowSizes {
    pub(crate) fn new(batch: &RecordBatch) -> Self {
        Self(ColumnSize::Struct(
            batch
                .columns()
                .iter()
                .map(|column| ColumnSize::new(column.as_ref()))
                .collect(),
        ))
    }

    /// Returns the size in bytes of row `row`.
    pub(crate) fn row(&self, row: usize) -> usize {
        self.0.rows(row, row + 1)
    }

    /// Returns the size in bytes of every row, if they all have the same size.
    pub(crate) fn fixed(&self) -> Option<usize> {
        self.0.fixed()
    }
}

/// Size model of one column, see [`SpilledRowSizes`].
enum ColumnSize {
    /// Every row takes the same number of bytes.
    Fixed(usize),
    /// An offset per row, plus the bytes between consecutive offsets.
    Bytes(Offsets),
    /// A view per row, plus the bytes of values too long to be inlined.
    Views(ScalarBuffer<u128>),
    /// An offset per row, plus the elements between consecutive offsets.
    List(Offsets, Box<ColumnSize>),
    /// A fixed number of elements per row.
    FixedSizeList(usize, Box<ColumnSize>),
    /// A row of each field.
    Struct(Vec<ColumnSize>),
}

impl ColumnSize {
    fn new(array: &dyn Array) -> Self {
        match array.data_type() {
            DataType::Utf8 => Self::Bytes(array.as_string::<i32>().offsets().into()),
            DataType::LargeUtf8 => Self::Bytes(array.as_string::<i64>().offsets().into()),
            DataType::Binary => Self::Bytes(array.as_binary::<i32>().offsets().into()),
            DataType::LargeBinary => {
                Self::Bytes(array.as_binary::<i64>().offsets().into())
            }
            DataType::Utf8View => Self::Views(array.as_string_view().views().clone()),
            DataType::BinaryView => Self::Views(array.as_binary_view().views().clone()),
            DataType::List(_) => {
                let list = array.as_list::<i32>();
                Self::List(list.offsets().into(), Box::new(Self::new(list.values())))
            }
            DataType::LargeList(_) => {
                let list = array.as_list::<i64>();
                Self::List(list.offsets().into(), Box::new(Self::new(list.values())))
            }
            DataType::Map(_, _) => {
                let map = array.as_map();
                Self::List(map.offsets().into(), Box::new(Self::new(map.entries())))
            }
            DataType::FixedSizeList(_, size) => Self::FixedSizeList(
                *size as usize,
                Box::new(Self::new(array.as_fixed_size_list().values())),
            ),
            DataType::Struct(_) => Self::Struct(
                array
                    .as_struct()
                    .columns()
                    .iter()
                    .map(|field| Self::new(field.as_ref()))
                    .collect(),
            ),
            DataType::Dictionary(key, _) => {
                Self::Fixed(key.primitive_width().unwrap_or_default())
            }
            DataType::FixedSizeBinary(size) => Self::Fixed(*size as usize),
            DataType::Null | DataType::Boolean => Self::Fixed(0),
            data_type => Self::Fixed(data_type.primitive_width().unwrap_or_else(|| {
                // Other layouts are spread evenly over their rows
                let bytes = array.to_data().get_slice_memory_size().unwrap_or_default();
                bytes / array.len().max(1)
            })),
        }
    }

    /// Returns the size in bytes of every row, if they all have the same size.
    fn fixed(&self) -> Option<usize> {
        match self {
            Self::Fixed(width) => Some(*width),
            Self::FixedSizeList(size, values) => values.fixed().map(|width| size * width),
            Self::Struct(fields) => fields.iter().map(Self::fixed).sum(),
            Self::Bytes(_) | Self::Views(_) | Self::List(_, _) => None,
        }
    }

    /// Returns the size in bytes of rows `start..end`.
    fn rows(&self, start: usize, end: usize) -> usize {
        let num_rows = end - start;
        match self {
            Self::Fixed(width) => num_rows * width,
            Self::Bytes(offsets) => {
                num_rows * offsets.width() + offsets.get(end) - offsets.get(start)
            }
            Self::Views(views) => views[start..end]
                .iter()
                .map(|&view| {
                    let len = view as u32;
                    if len > MAX_INLINE_VIEW_LEN {
                        VIEW_SIZE_BYTES + len as usize
                    } else {
                        VIEW_SIZE_BYTES
                    }
                })
                .sum(),
            Self::List(offsets, values) => {
                num_rows * offsets.width()
                    + values.rows(offsets.get(start), offsets.get(end))
            }
            Self::FixedSizeList(size, values) => values.rows(start * size, end * size),
            Self::Struct(fields) => {
                fields.iter().map(|field| field.rows(start, end)).sum()
            }
        }
    }
}

/// The offsets of a variable-width or list column.
enum Offsets {
    Small(ScalarBuffer<i32>),
    Large(ScalarBuffer<i64>),
}

impl Offsets {
    /// Returns the size in bytes of one offset.
    fn width(&self) -> usize {
        match self {
            Self::Small(_) => size_of::<i32>(),
            Self::Large(_) => size_of::<i64>(),
        }
    }

    fn get(&self, index: usize) -> usize {
        match self {
            Self::Small(offsets) => offsets[index] as usize,
            Self::Large(offsets) => offsets[index] as usize,
        }
    }
}

impl From<&OffsetBuffer<i32>> for Offsets {
    fn from(offsets: &OffsetBuffer<i32>) -> Self {
        Self::Small(offsets.inner().clone())
    }
}

impl From<&OffsetBuffer<i64>> for Offsets {
    fn from(offsets: &OffsetBuffer<i64>) -> Self {
        Self::Large(offsets.inner().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BooleanArray, DictionaryArray, FixedSizeBinaryArray, Int32Array,
        Int64Array, ListArray, StringArray, StringViewArray, StructArray,
    };
    use arrow::buffer::Buffer;
    use arrow::datatypes::{Field, Int32Type, Int64Type};
    use std::sync::Arc;

    fn batch(columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_from_iter(
            columns
                .into_iter()
                .enumerate()
                .map(|(i, column)| (format!("c{i}"), column)),
        )
        .unwrap()
    }

    fn row_sizes(columns: Vec<ArrayRef>) -> Vec<usize> {
        let batch = batch(columns);
        let sizes = SpilledRowSizes::new(&batch);
        (0..batch.num_rows()).map(|row| sizes.row(row)).collect()
    }

    #[test]
    fn fixed_width_and_offsets() {
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["a", "bbbb"]));
        // 8 bytes of Int64, plus a 4 byte offset and the string bytes
        assert_eq!(row_sizes(vec![ints, strings]), vec![13, 16]);
    }

    #[test]
    fn rows_of_fixed_width_columns_have_one_size() {
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let fixed: ArrayRef = Arc::new(FixedSizeBinaryArray::new(
            3,
            Buffer::from(vec![0u8; 6]),
            None,
        ));
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["a", "bbbb"]));
        let sizes = SpilledRowSizes::new(&batch(vec![Arc::clone(&ints), fixed]));
        assert_eq!(sizes.fixed(), Some(11));
        assert_eq!(
            SpilledRowSizes::new(&batch(vec![ints, strings])).fixed(),
            None
        );
    }

    #[test]
    fn views_count_their_bytes_unless_inlined() {
        let long = "a string longer than twelve";
        let views: ArrayRef = Arc::new(StringViewArray::from(vec!["short", long]));
        assert_eq!(row_sizes(vec![views]), vec![16, 16 + long.len()]);
    }

    #[test]
    fn views_sharing_bytes_count_them_for_every_row() {
        // Three views of the same 100 bytes, stored once
        let value = "x".repeat(100);
        let one = StringViewArray::from(vec![value.as_str()]);
        let view = one.views()[0];
        let shared = StringViewArray::new(
            ScalarBuffer::from(vec![view; 3]),
            one.data_buffers().to_vec(),
            None,
        );
        // Compacting before spilling copies the bytes for each view
        assert_eq!(row_sizes(vec![Arc::new(shared)]), vec![116; 3]);
    }

    #[test]
    fn lists_count_their_elements() {
        let list: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![]),
                Some(vec![Some(4)]),
            ]));
        assert_eq!(row_sizes(vec![Arc::clone(&list)]), vec![28, 4, 12]);
        // A slice keeps the sizes of its own rows
        assert_eq!(row_sizes(vec![list.slice(1, 2)]), vec![4, 12]);
    }

    #[test]
    fn nested_views_count_their_bytes() {
        let values =
            StringViewArray::from(vec!["x".repeat(20), "y".repeat(5), "z".repeat(30)]);
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Utf8View, true)),
            OffsetBuffer::new(vec![0, 2, 3].into()),
            Arc::new(values),
            None,
        ));
        assert_eq!(row_sizes(vec![list]), vec![4 + 36 + 16, 4 + 46]);
    }

    #[test]
    fn struct_fields_add_up() {
        let fields: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["", "abc"])) as ArrayRef,
            ),
        ]));
        assert_eq!(row_sizes(vec![fields]), vec![8, 11]);
    }

    #[test]
    fn dictionaries_count_their_keys() {
        let dictionary: ArrayRef = Arc::new(
            vec!["a long dictionary value", "b"]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        assert_eq!(row_sizes(vec![dictionary]), vec![4, 4]);
    }

    #[test]
    fn other_types() {
        let fixed: ArrayRef = Arc::new(FixedSizeBinaryArray::new(
            3,
            Buffer::from(vec![0u8; 6]),
            None,
        ));
        let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        assert_eq!(row_sizes(vec![fixed, booleans]), vec![3, 3]);
    }
}
