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

use crate::aggregates::group_values::GroupValues;
use ahash::RandomState;
use arrow::array::AsArray;
use arrow::compute::cast;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use arrow_array::{Array, ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow_buffer::{BufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::proxy::{RawTableAllocExt, VecAllocExt};
use datafusion_expr::EmitTo;
use hashbrown::raw::RawTable;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

/// Maximum size of a value that can be inlined in the hash table
const SHORT_VALUE_LEN: usize = std::mem::size_of::<usize>();
/// The size, in number of entries, of the initial hash table
const INITIAL_MAP_CAPACITY: usize = 128;
/// The initial size, in bytes, of the string data
const INITIAL_BUFFER_CAPACITY: usize = 8 * 1024;

/// A [`GroupValues`] making use of [`Rows`]
pub struct GroupValuesRows {
    /// The output schema
    schema: SchemaRef,

    /// Converter for the group values
    row_converter: RowConverter,

    /// Logically maps group values to a group_index in
    /// [`Self::group_values`] and in each accumulator
    ///
    /// Uses the raw API of hashbrown to avoid actually storing the
    /// keys (group values) in the table
    ///
    /// keys: u64 hashes of the GroupValue
    /// values: (hash, group_index)
    // TODO: extend to i64
    map: hashbrown::raw::RawTable<Entry<i32>>,

    /// The size of `map` in bytes
    map_size: usize,

    /// The actual group by values, stored in arrow [`Row`] format.
    /// `group_values[i]` holds the group value for group_index `i`.
    ///
    /// The row format is used to compare group keys quickly and store
    /// them efficiently in memory. Quick comparison is especially
    /// important for multi-column group keys.
    ///
    /// [`Row`]: arrow::row::Row
    fixed_width_group_values: Option<Rows>,

    // buffer to be reused to store hashes
    hashes_buffer: Vec<u64>,

    /// Random state for creating hashes
    random_state: RandomState,

    // utf8_map: ArrowBytesMap<i32, usize>,

    /// The total number of groups so far (used to assign group_index)
    num_groups: usize,

    /// In progress arrow `Buffer` containing all values
    buffer: BufferBuilder<u8>,
    /// Offsets into `buffer` for each distinct  value. These offsets as used
    /// directly to create the final `GenericBinaryArray`. The `i`th string is
    /// stored in the range `offsets[i]..offsets[i+1]` in `buffer`. Null values
    /// are stored as a zero length string.
    offsets: Vec<i32>,
}

impl GroupValuesRows {
    pub fn try_new(schema: SchemaRef) -> Result<Self> {



        let row_converter = RowConverter::new(
            schema
                .fields()
                .iter()
                .filter(|p| !matches!(p.data_type(), DataType::Utf8))
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;

        let map = RawTable::with_capacity(INITIAL_MAP_CAPACITY);

        Ok(Self {
            schema,
            row_converter,
            map,
            map_size: 0,
            fixed_width_group_values: None,
            hashes_buffer: Default::default(),
            random_state: Default::default(),
            buffer: BufferBuilder::new(INITIAL_BUFFER_CAPACITY),
            offsets: vec![0], // first offset is always 0
            num_groups: 0,
        })
    }
}

impl GroupValues for GroupValuesRows {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        let n_rows = cols[0].len();
        // println!("n_rows: {:?}", n_rows);

        // 1.1 Calculate the group keys for the group values
        let batch_hashes = &mut self.hashes_buffer;
        batch_hashes.clear();
        batch_hashes.resize(n_rows, 0);
        create_hashes(&cols, &self.random_state, batch_hashes)?;
        // println!("cols: {:?}", cols);

        let cols_fixed = &cols[0..1];
        let fixed_width_group_rows = self.row_converter.convert_columns(cols_fixed)?;
        // println!("fixed_width_group_rowsl num_rows : {:?}", fixed_width_group_rows.num_rows());

        let cols_var = &cols[1..];
        let cols_var_len = cols_var.len();

        // let n_rows = group_rows.num_rows();

        let mut fixed_width_group_values = match self.fixed_width_group_values.take() {
            Some(group_values) => group_values,
            None => self.row_converter.empty_rows(0, 0),
        };

        // tracks to which group each of the input rows belongs
        groups.clear();


        for (row, &hash) in batch_hashes.iter().enumerate() {
            let entry = self.map.get_mut(hash, |header| {
                if fixed_width_group_rows.row(row) != fixed_width_group_values.row(header.fixed_width_group_row) {
                    return false;
                }

                for (col_id, column_arr) in cols_var.iter().enumerate() {
                    if column_arr.is_null(row) {
                        if header.len[col_id].is_some() {
                            return false;
                        }
                    } else {
                        let column_arr = column_arr.as_string::<i32>();
                        let value = column_arr.value(row).as_bytes();

                        if header.len[col_id].is_none() || header.len[col_id].unwrap() != value.len() as i32 {
                            return false;
                        }

                        if value.len() <= SHORT_VALUE_LEN {
                            let inline = value.iter().fold(0usize, |acc, &x| acc << 8 | x as usize);
                            if inline != header.offset_or_inline[col_id] {
                                return false;
                            }
                        } else {
                            // Need to compare the bytes in the buffer
                            // SAFETY: buffer is only appended to, and we correctly inserted values and offsets
                            let existing_value =
                                unsafe { self.buffer.as_slice().get_unchecked(header.range(col_id)) };
                            if value != existing_value {
                                return false;
                            }
                        }
                    }
                }

                true
            });

            let group_id = match entry {
                // Existing group_index for this group value
                Some(header) => header.group_id,
                //  1.2 Need to create new entry for the group
                None => {
                    // // Add new entry to aggr_state and save newly created index

                    // There might be duplicated row inserted into group values, but we can save the time to search the expected row id.
                    let fixed_width_group_row_id = fixed_width_group_values.num_rows();
                    fixed_width_group_values.push(fixed_width_group_rows.row(row));

                    let mut len = vec![None;cols_var_len];
                    let mut offset_or_inline = vec![0usize;cols_var_len];
                    for (col_id, column_arr) in cols_var.iter().enumerate() {
                        if column_arr.is_null(row) {
                        } else {
                            let column_arr = column_arr.as_string::<i32>();
                            let value = column_arr.value(row).as_bytes();

                            if value.len() <= SHORT_VALUE_LEN {
                                let inline = value.iter().fold(0usize, |acc, &x| acc << 8 | x as usize);
                                offset_or_inline[col_id] = inline;
                            } else {
                                let offset = self.buffer.len(); // offset of start of data
                                offset_or_inline[col_id] = offset;
                            }

                            // Put the small values into buffer and offsets so it appears
                            // the output array, but store the actual bytes inline for
                            // comparison
                            self.buffer.append_slice(value);
                            self.offsets.push(self.buffer.len() as i32);

                            len[col_id] = Some(value.len() as i32);
                        }
                    }

                    let group_id = self.num_groups;
                    self.num_groups += 1;
                    
                    let new_header = Entry::<i32> {
                        hash,
                        len,
                        offset_or_inline,
                        fixed_width_group_row: fixed_width_group_row_id,
                        group_id,
                    };
                    // for hasher function, use precomputed hash value
                    self.map.insert_accounted(
                        new_header,
                        |header| header.hash,
                        &mut self.map_size,
                    );
                    group_id
                }
            };

            groups.push(group_id);
        }

        self.fixed_width_group_values = Some(fixed_width_group_values);

        Ok(())
    }

    fn size(&self) -> usize {
        let group_values_size = self.fixed_width_group_values.as_ref().map(|v| v.size()).unwrap_or(0);
        self.row_converter.size()
            + group_values_size
            + self.map_size
            + self.hashes_buffer.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.fixed_width_group_values
            .as_ref()
            .map(|group_values| group_values.num_rows())
            .unwrap_or(0)
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut group_values = self
            .fixed_width_group_values
            .take()
            .expect("Can not emit from empty rows");

        // TODO: extend to multiple array
        let offset = self.offsets.clone();

        // SAFETY: the offsets were constructed correctly in `insert_if_new` --
        // monotonically increasing, overflows were checked.
        let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offset)) };
        let values = self.buffer.finish();

        let array = Arc::new(unsafe {
            GenericStringArray::new_unchecked(offsets, values, None)
        }) as ArrayRef;

        // TODO: build each column back into vec of ArrayRef

        let group_values_len = group_values.num_rows();

        let mut output = match emit_to {
            EmitTo::All => {
                let mut output = self.row_converter.convert_rows(&group_values)?;
                group_values.clear();
                assert_eq!(array.len(), group_values_len);
                self.num_groups -= array.len();
                output.push(array);
                output
            }
            EmitTo::First(n) if n == self.num_groups => {
                let mut output = self.row_converter.convert_rows(&group_values)?;
                group_values.clear();
                assert_eq!(array.len(), group_values_len);
                self.num_groups -= array.len();
                output.push(array);
                output
            }
            EmitTo::First(n) => {
                let groups_rows = group_values.iter().take(n);
                let mut output = self.row_converter.convert_rows(groups_rows)?;
                // Clear out first n group keys by copying them to a new Rows.
                // TODO file some ticket in arrow-rs to make this more efficent?
                let mut new_group_values = self.row_converter.empty_rows(0, 0);
                for row in group_values.iter().skip(n) {
                    new_group_values.push(row);
                }
                std::mem::swap(&mut new_group_values, &mut group_values);

                // SAFETY: self.map outlives iterator and is not modified concurrently
                unsafe {
                    for bucket in self.map.iter() {
                        // Decrement group index by n
                        match bucket.as_ref().group_id.checked_sub(n) {
                            // Group index was >= n, shift value down
                            Some(sub) => bucket.as_mut().group_id = sub,
                            // Group index was < n, so remove from table
                            None => self.map.erase(bucket),
                        }
                    }
                }

                // if we only wanted to take the first n, insert the rest back
                // into the map we could potentially avoid this reallocation, at
                // the expense of much more complex code.
                // see https://github.com/apache/datafusion/issues/9195
                let emit_group_values = array.slice(0, n);
                let remaining_group_values =
                array.slice(n, array.len() - n);

                self.num_groups = 0;
                let mut group_indexes = vec![];
                self.intern(&[remaining_group_values], &mut group_indexes)?;

                // Verify that the group indexes were assigned in the correct order
                assert_eq!(0, group_indexes[0]);

                output.push(emit_group_values);
                output
            }
        };

        // TODO: Materialize dictionaries in group keys (#7647)
        for (field, array) in self.schema.fields.iter().zip(&mut output) {
            let expected = field.data_type();
            if let DataType::Dictionary(_, v) = expected {
                let actual = array.data_type();
                if v.as_ref() != actual {
                    return Err(DataFusionError::Internal(format!(
                        "Converted group rows expected dictionary of {v} got {actual}"
                    )));
                }
                *array = cast(array.as_ref(), expected)?;
            }
        }

        self.fixed_width_group_values = Some(group_values);
        Ok(output)
    }

    fn clear_shrink(&mut self, batch: &RecordBatch) {
        let count = batch.num_rows();
        self.fixed_width_group_values = self.fixed_width_group_values.take().map(|mut rows| {
            rows.clear();
            rows
        });
        self.map.clear();
        self.map.shrink_to(count, |_| 0); // hasher does not matter since the map is cleared
        self.map_size = self.map.capacity() * std::mem::size_of::<(u64, usize)>();
        self.hashes_buffer.clear();
        self.hashes_buffer.shrink_to(count);
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct Entry<O>
where
    O: OffsetSizeTrait,
{
    /// hash of the value (stored to avoid recomputing it in hash table check)
    hash: u64,
    /// if len =< [`SHORT_VALUE_LEN`]: the data inlined
    /// if len > [`SHORT_VALUE_LEN`], the offset of where the data starts
    offset_or_inline: Vec<usize>,
    /// length of the value, in bytes (use O here so we use only i32 for
    /// strings, rather 64 bit usize)
    len: Vec<Option<O>>,

    fixed_width_group_row: usize,
    group_id: usize,
}

impl<O> Entry<O>
where
    O: OffsetSizeTrait
{
    /// returns self.offset..self.offset + self.len
    #[inline(always)]
    fn range(&self, col_id: usize) -> Range<usize> {
        self.offset_or_inline[col_id]..self.offset_or_inline[col_id] + self.len[col_id].unwrap().as_usize()
    }
}