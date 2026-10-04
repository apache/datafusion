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
pub(crate) struct SpilledRowSizes {
    /// Bytes that every row takes in the fixed-width columns
    fixed: usize,
    /// The other columns
    variable: Vec<ColumnSize>,
    /// See [`Self::max_row`]
    max_row: usize,
}

impl SpilledRowSizes {
    pub(crate) fn new(batch: &RecordBatch) -> Self {
        let mut fixed = 0;
        let mut variable = vec![];
        for column in batch.columns() {
            let column = ColumnSize::new(column.as_ref());
            match column.fixed() {
                Some(width) => fixed += width,
                None => variable.push(column),
            }
        }
        let max_row = variable.iter().fold(fixed, |bytes, column| {
            bytes.saturating_add(column.max_row())
        });
        Self {
            fixed,
            variable,
            max_row,
        }
    }

    /// Returns the size in bytes of row `row`.
    pub(crate) fn row(&self, row: usize) -> usize {
        self.variable.iter().fold(self.fixed, |bytes, column| {
            bytes + column.rows(row, row + 1)
        })
    }

    /// Returns the size in bytes of every row, if they all have the same size.
    pub(crate) fn fixed(&self) -> Option<usize> {
        self.variable.is_empty().then_some(self.fixed)
    }

    /// Returns an upper bound on the size in bytes of any row.
    pub(crate) fn max_row(&self) -> usize {
        self.max_row
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

    /// Returns an upper bound on the size in bytes of a row.
    fn max_row(&self) -> usize {
        match self {
            Self::Fixed(width) => *width,
            Self::Bytes(offsets) => offsets.width() + offsets.max_len(),
            Self::Views(views) => views
                .iter()
                .copied()
                .map(view_bytes)
                .max()
                .unwrap_or_default(),
            Self::List(offsets, values) => offsets
                .width()
                .saturating_add(offsets.max_len().saturating_mul(values.max_row())),
            Self::FixedSizeList(size, values) => size.saturating_mul(values.max_row()),
            Self::Struct(fields) => fields
                .iter()
                .fold(0, |bytes, field| bytes.saturating_add(field.max_row())),
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
            Self::Views(views) => views[start..end].iter().copied().map(view_bytes).sum(),
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

/// Returns the size in bytes of a view, plus its value unless it is inlined.
fn view_bytes(view: u128) -> usize {
    let len = view as u32;
    if len > MAX_INLINE_VIEW_LEN {
        VIEW_SIZE_BYTES + len as usize
    } else {
        VIEW_SIZE_BYTES
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

    /// Returns the largest distance between consecutive offsets.
    fn max_len(&self) -> usize {
        match self {
            Self::Small(offsets) => offsets
                .windows(2)
                .map(|pair| (pair[1] - pair[0]) as usize)
                .max(),
            Self::Large(offsets) => offsets
                .windows(2)
                .map(|pair| (pair[1] - pair[0]) as usize)
                .max(),
        }
        .unwrap_or_default()
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

    /// Returns the size of each row of a batch of `columns`, and the upper
    /// bound on the size of any row.
    fn row_sizes(columns: Vec<ArrayRef>) -> (Vec<usize>, usize) {
        let batch = batch(columns);
        let sizes = SpilledRowSizes::new(&batch);
        let rows = (0..batch.num_rows()).map(|row| sizes.row(row)).collect();
        (rows, sizes.max_row())
    }

    #[test]
    fn fixed_width_and_offsets() {
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["a", "bbbb"]));
        // 8 bytes of Int64, plus a 4 byte offset and the string bytes
        assert_eq!(row_sizes(vec![ints, strings]), (vec![13, 16], 16));
    }

    #[test]
    fn rows_of_fixed_width_columns_have_one_size() {
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let fixed: ArrayRef = Arc::new(FixedSizeBinaryArray::new(
            3,
            Buffer::from(vec![0u8; 6]),
            None,
        ));
        let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["a", "bbbb"]));
        // 8 bytes of Int64 and 3 of FixedSizeBinary(3). The bits of booleans
        // are not counted.
        let columns = vec![Arc::clone(&ints), fixed, booleans];
        assert_eq!(row_sizes(columns.clone()), (vec![11, 11], 11));
        assert_eq!(SpilledRowSizes::new(&batch(columns)).fixed(), Some(11));
        assert_eq!(
            SpilledRowSizes::new(&batch(vec![ints, strings])).fixed(),
            None
        );
    }

    #[test]
    fn views_count_their_bytes_unless_inlined() {
        let long = "a string longer than twelve";
        let views: ArrayRef = Arc::new(StringViewArray::from(vec!["short", long]));
        let long_row = 16 + long.len();
        assert_eq!(row_sizes(vec![views]), (vec![16, long_row], long_row));
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
        assert_eq!(row_sizes(vec![Arc::new(shared)]), (vec![116; 3], 116));
    }

    #[test]
    fn lists_count_their_elements() {
        let list: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![]),
                Some(vec![Some(4)]),
            ]));
        // An offset plus the elements of the row. The bound is an offset plus
        // the most elements in a row, each of the largest size.
        assert_eq!(row_sizes(vec![Arc::clone(&list)]), (vec![28, 4, 12], 28));
        // A slice keeps the sizes of its own rows
        assert_eq!(row_sizes(vec![list.slice(1, 2)]), (vec![4, 12], 12));
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
        // The bound takes two elements, the most in a row, of the largest size
        assert_eq!(
            row_sizes(vec![list]),
            (vec![4 + 36 + 16, 4 + 46], 4 + 2 * 46)
        );
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
        assert_eq!(row_sizes(vec![fields]), (vec![8, 11], 11));
    }

    #[test]
    fn dictionaries_count_their_keys() {
        let dictionary: ArrayRef = Arc::new(
            vec!["a long dictionary value", "b"]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );
        assert_eq!(row_sizes(vec![dictionary]), (vec![4, 4], 4));
    }
}
