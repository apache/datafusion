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
use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, LargeListViewArray, ListArray,
    ListViewArray, MapArray, PrimitiveArray, RunArray, StructArray,
    downcast_run_end_index,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::row::{RowConverter, Rows, SortField};
use datafusion_common::Result;
use datafusion_common::hash_utils::RandomState;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::utils::normalize_float_zero;
use datafusion_execution::memory_pool::proxy::{HashTableAllocExt, VecAllocExt};
use datafusion_expr::EmitTo;
use hashbrown::hash_table::HashTable;
use log::debug;
use std::mem::size_of;
use std::sync::Arc;

/// A [`GroupValues`] making use of [`Rows`]
///
/// This is a general implementation of [`GroupValues`] that works for any
/// combination of data types and number of columns, including nested types such as
/// structs and lists.
///
/// It uses the arrow-rs [`Rows`] to store the group values, which is a row-wise
/// representation.
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
    map: HashTable<(u64, usize)>,

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
    group_values: Option<Rows>,

    /// reused buffer to store hashes
    hashes_buffer: Vec<u64>,

    /// reused buffer to store rows
    rows_buffer: Rows,

    /// Random state for creating hashes
    random_state: RandomState,
}

impl GroupValuesRows {
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        // Print a debugging message, so it is clear when the (slower) fallback
        // GroupValuesRows is used.
        debug!("Creating GroupValuesRows for schema: {schema}");
        let row_converter = RowConverter::new(
            schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;

        let map = HashTable::with_capacity(0);

        let starting_rows_capacity = 1000;

        let starting_data_capacity = 64 * starting_rows_capacity;
        let rows_buffer =
            row_converter.empty_rows(starting_rows_capacity, starting_data_capacity);
        Ok(Self {
            schema,
            row_converter,
            map,
            map_size: 0,
            group_values: None,
            hashes_buffer: Default::default(),
            rows_buffer,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
        })
    }
}

impl GroupValues for GroupValuesRows {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        // Normalize -0.0 → +0.0 so RowConverter (IEEE 754 totalOrder) and
        // primitive hashing both group ±0 together. No-op for non-float
        // columns.
        let normalized_cols: Vec<ArrayRef> =
            cols.iter().map(normalize_float_zero).collect();
        let cols = normalized_cols.as_slice();

        // Convert the group keys into the row format
        let group_rows = &mut self.rows_buffer;
        group_rows.clear();
        self.row_converter.append(group_rows, cols)?;
        let n_rows = group_rows.num_rows();

        let mut group_values = match self.group_values.take() {
            Some(group_values) => group_values,
            None => self.row_converter.empty_rows(0, 0),
        };

        // tracks to which group each of the input rows belongs
        groups.clear();

        // 1.1 Calculate the group keys for the group values
        let batch_hashes = &mut self.hashes_buffer;
        batch_hashes.clear();
        batch_hashes.resize(n_rows, 0);
        create_hashes(cols, &self.random_state, batch_hashes)?;

        for (row, &target_hash) in batch_hashes.iter().enumerate() {
            let entry = self.map.find_mut(target_hash, |(exist_hash, group_idx)| {
                // Somewhat surprisingly, this closure can be called even if the
                // hash doesn't match, so check the hash first with an integer
                // comparison first avoid the more expensive comparison with
                // group value. https://github.com/apache/datafusion/pull/11718
                target_hash == *exist_hash
                    // verify that the group that we are inserting with hash is
                    // actually the same key value as the group in
                    // existing_idx  (aka group_values @ row)
                    && group_rows.row(row) == group_values.row(*group_idx)
            });

            let group_idx = match entry {
                // Existing group_index for this group value
                Some((_hash, group_idx)) => *group_idx,
                //  1.2 Need to create new entry for the group
                None => {
                    // Add new entry to aggr_state and save newly created index
                    let group_idx = group_values.num_rows();
                    group_values.push(group_rows.row(row));

                    // for hasher function, use precomputed hash value
                    self.map.insert_accounted(
                        (target_hash, group_idx),
                        |(hash, _group_index)| *hash,
                        &mut self.map_size,
                    );
                    group_idx
                }
            };
            groups.push(group_idx);
        }

        self.group_values = Some(group_values);

        Ok(())
    }

    fn size(&self) -> usize {
        let group_values_size = self.group_values.as_ref().map(|v| v.size()).unwrap_or(0);
        self.row_converter.size()
            + group_values_size
            + self.map_size
            + self.rows_buffer.size()
            + self.hashes_buffer.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.group_values
            .as_ref()
            .map(|group_values| group_values.num_rows())
            .unwrap_or(0)
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut group_values = self
            .group_values
            .take()
            .expect("Can not emit from empty rows");

        let mut output = match emit_to {
            EmitTo::All => {
                let output = self.row_converter.convert_rows(&group_values)?;
                group_values.clear();
                self.map.clear();
                output
            }
            EmitTo::First(n) => {
                let groups_rows = group_values.iter().take(n);
                let output = self.row_converter.convert_rows(groups_rows)?;
                // Clear out first n group keys by copying them to a new Rows.
                // TODO file some ticket in arrow-rs to make this more efficient?
                let mut new_group_values = self.row_converter.empty_rows(0, 0);
                for row in group_values.iter().skip(n) {
                    new_group_values.push(row);
                }
                std::mem::swap(&mut new_group_values, &mut group_values);

                self.map.retain(|(_exists_hash, group_idx)| {
                    // Decrement group index by n
                    match group_idx.checked_sub(n) {
                        // Group index was >= n, shift value down
                        Some(sub) => {
                            *group_idx = sub;
                            true
                        }
                        // Group index was < n, so remove from table
                        None => false,
                    }
                });
                output
            }
        };

        // TODO: Materialize dictionaries in group keys
        // https://github.com/apache/datafusion/issues/7647
        for (field, array) in self.schema.fields.iter().zip(&mut output) {
            let expected = field.data_type();
            *array = encode_array_if_necessary(array, expected)?;
        }

        self.group_values = Some(group_values);
        Ok(output)
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.group_values = self.group_values.take().map(|mut rows| {
            rows.clear();
            rows
        });
        self.map.clear();
        self.map.shrink_to(num_rows, |_| 0); // hasher does not matter since the map is cleared
        self.map_size = self.map.capacity() * size_of::<(u64, usize)>();
        self.hashes_buffer.clear();
        self.hashes_buffer.shrink_to(num_rows);
    }
}

/// Re-apply dictionary / run-end encoding to `array` so it matches `expected`.
///
/// Arrow's [`RowConverter`] flattens dictionary and run-end-encoded values to
/// their plain value type during row encoding (at [`RowConverter::append`]),
/// so any group-value array produced from the row format is in that plain
/// type and must be re-encoded to match the schema's expected type before
/// being returned. Shared with the generic row-backed `GroupColumn`.
///
/// [`RowConverter`]: arrow::row::RowConverter
/// [`RowConverter::append`]: arrow::row::RowConverter::append
pub(crate) fn encode_array_if_necessary(
    array: &ArrayRef,
    expected: &DataType,
) -> Result<ArrayRef> {
    match (expected, array.data_type()) {
        (DataType::Struct(expected_fields), _) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let arrays = expected_fields
                .iter()
                .zip(struct_array.columns())
                .map(|(expected_field, column)| {
                    encode_array_if_necessary(column, expected_field.data_type())
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Arc::new(StructArray::try_new(
                expected_fields.clone(),
                arrays,
                struct_array.nulls().cloned(),
            )?))
        }
        (DataType::List(expected_field), &DataType::List(_)) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();

            Ok(Arc::new(ListArray::try_new(
                Arc::<arrow::datatypes::Field>::clone(expected_field),
                list.offsets().clone(),
                encode_array_if_necessary(list.values(), expected_field.data_type())?,
                list.nulls().cloned(),
            )?))
        }
        (DataType::LargeList(expected_field), &DataType::LargeList(_)) => {
            let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();

            Ok(Arc::new(LargeListArray::try_new(
                Arc::<arrow::datatypes::Field>::clone(expected_field),
                list.offsets().clone(),
                encode_array_if_necessary(list.values(), expected_field.data_type())?,
                list.nulls().cloned(),
            )?))
        }
        (DataType::ListView(expected_field), &DataType::ListView(_)) => {
            // arrow-row's `decode_list_view` applies the dictionary-flatten
            // `corrected_type` to the child, so a `ListView<Dictionary<..>>`
            // decodes as `ListView<value type>` and the child must be
            // re-encoded here (same as `List` above, plus the `sizes`
            // buffer that view-lists carry).
            let list = array.as_any().downcast_ref::<ListViewArray>().unwrap();

            Ok(Arc::new(ListViewArray::try_new(
                Arc::<arrow::datatypes::Field>::clone(expected_field),
                list.offsets().clone(),
                list.sizes().clone(),
                encode_array_if_necessary(list.values(), expected_field.data_type())?,
                list.nulls().cloned(),
            )?))
        }
        (DataType::LargeListView(expected_field), &DataType::LargeListView(_)) => {
            let list = array.as_any().downcast_ref::<LargeListViewArray>().unwrap();

            Ok(Arc::new(LargeListViewArray::try_new(
                Arc::<arrow::datatypes::Field>::clone(expected_field),
                list.offsets().clone(),
                list.sizes().clone(),
                encode_array_if_necessary(list.values(), expected_field.data_type())?,
                list.nulls().cloned(),
            )?))
        }
        (
            DataType::FixedSizeList(expected_field, expected_size),
            &DataType::FixedSizeList(_, _),
        ) => {
            let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();

            Ok(Arc::new(FixedSizeListArray::try_new(
                Arc::<arrow::datatypes::Field>::clone(expected_field),
                *expected_size,
                encode_array_if_necessary(list.values(), expected_field.data_type())?,
                list.nulls().cloned(),
            )?))
        }
        (DataType::Map(expected_entries_field, ordered), &DataType::Map(_, _)) => {
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            // Re-encode the entries `StructArray` (which holds key/value
            // columns) against the expected entries field's struct type.
            let entries_as_ref: ArrayRef = Arc::new(map.entries().clone());
            let entries = encode_array_if_necessary(
                &entries_as_ref,
                expected_entries_field.data_type(),
            )?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Map entries recurse must yield a StructArray")
                .clone();
            Ok(Arc::new(MapArray::try_new(
                Arc::<arrow::datatypes::Field>::clone(expected_entries_field),
                map.offsets().clone(),
                entries,
                map.nulls().cloned(),
                *ordered,
            )?))
        }
        (DataType::Dictionary(_, _), _) => Ok(cast(array.as_ref(), expected)?),
        (
            DataType::RunEndEncoded(run_ends_field, expected_values_field),
            &DataType::RunEndEncoded(_, _),
        ) => {
            macro_rules! reencode_ree {
                ($run_end_type:ty) => {{
                    let run_array = array
                        .as_any()
                        .downcast_ref::<RunArray<$run_end_type>>()
                        .unwrap();
                    let values = encode_array_if_necessary(
                        &(Arc::clone(run_array.values()) as ArrayRef),
                        expected_values_field.data_type(),
                    )?;
                    let run_ends = PrimitiveArray::<$run_end_type>::new(
                        run_array.run_ends().inner().clone(),
                        None,
                    );
                    Ok(Arc::new(RunArray::try_new(&run_ends, &values)?))
                }};
            }
            downcast_run_end_index! {
                run_ends_field.data_type() => (reencode_ree),
                _ => unreachable!("unsupported run end type: {}", run_ends_field.data_type()),
            }
        }
        (DataType::RunEndEncoded(_, _), _) => Ok(cast(array.as_ref(), expected)?),
        (_, _) => Ok(Arc::<dyn Array>::clone(array)),
    }
}
