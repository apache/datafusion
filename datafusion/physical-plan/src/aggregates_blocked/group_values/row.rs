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
use crate::aggregates_blocked::group_values::BlockedGroupValues;
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
use datafusion_expr::{EmitTo, GroupSelection};
use datafusion_expr_common::blocked_helpers::BlockedRowsBuilder;
use datafusion_expr_common::groups_accumulator::{BlockedEmitTo, BlockedGroupSelection, BlocksIndex};
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
pub struct BlockedGroupValuesRows {
    /// The output schema
    schema: SchemaRef,

    /// Logically maps group values to a group_index in
    /// [`Self::group_values`] and in each accumulator
    ///
    /// Uses the raw API of hashbrown to avoid actually storing the
    /// keys (group values) in the table
    ///
    /// keys: u64 hashes of the GroupValue
    /// values: (hash, group_index)
    map: HashTable<(u64, BlocksIndex)>,

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
    group_values: BlockedRowsBuilder<true>,

    /// reused buffer to store hashes
    hashes_buffer: Vec<u64>,

    /// reused buffer to store rows
    rows_buffer: Rows,

    /// Random state for creating hashes
    random_state: RandomState,
}

impl BlockedGroupValuesRows {
    pub fn try_new(schema: SchemaRef, block_size: usize) -> Result<Self> {
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
            map,
            map_size: 0,
            group_values: BlockedRowsBuilder::new(block_size, row_converter),
            hashes_buffer: Default::default(),
            rows_buffer,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
        })
    }
}

impl BlockedGroupValues for BlockedGroupValuesRows {
    fn block_size(&self) -> usize {
        self.group_values.block_size()
    }

    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<BlocksIndex>) -> Result<()> {
        // Normalize -0.0 → +0.0 so RowConverter (IEEE 754 totalOrder) and
        // primitive hashing both group ±0 together. No-op for non-float
        // columns.
        let normalized_cols: Vec<ArrayRef> =
            cols.iter().map(normalize_float_zero).collect();
        let cols = normalized_cols.as_slice();

        // Convert the group keys into the row format
        let group_rows = &mut self.rows_buffer;
        group_rows.clear();
        self.group_values.row_converter().append(group_rows, cols)?;
        let n_rows = group_rows.num_rows();

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
                    && group_rows.row(row) == self.group_values.value(*group_idx)
            });

            let group_idx = match entry {
                // Existing group_index for this group value
                Some((_hash, group_idx)) => *group_idx,
                //  1.2 Need to create new entry for the group
                None => {
                    // Add new entry to aggr_state and save newly created index
                    let group_idx = self.group_values.next_block_index();
                    self.group_values.push(group_rows.row(row));

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

        Ok(())
    }

    fn size(&self) -> usize {
        self.group_values.allocated_size()
            + self.map_size
            + self.rows_buffer.size()
            + self.hashes_buffer.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.group_values.len()
    }

    /// Emit all group values
    fn emit_all(&mut self) -> Result<Vec<Vec<ArrayRef>>> {
        let mut group_values = self.group_values.take_all();

        self.map.clear();

        let row_converter = self.group_values.row_converter();

        group_values
            .into_iter()
            .map(|block| {
                let mut block_output = row_converter.convert_rows(&block)?;

                // TODO: Materialize dictionaries in group keys
                // https://github.com/apache/datafusion/issues/7647
                for (field, array) in self.schema.fields.iter().zip(&mut block_output) {
                    let expected = field.data_type();
                    *array = encode_array_if_necessary(array, expected)?;
                }

                Ok(block_output)
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Emit the next block
    /// returns Ok(None) when there are no blocks
    fn emit_block(&mut self) -> Result<Option<Vec<ArrayRef>>> {
        let Some(group_values) = self.group_values.take_block() else {
            return Ok(None);
        };

        let block_size = self.block_size();

        self.map.retain(|(_exists_hash, group_idx)| {
            // Decrement group index by block size
            match group_idx.prev_block_checked(block_size) {
                // Group index was >= block size, shift value down
                Some(sub) => {
                    *group_idx = sub;
                    true
                }
                // Group index was < block size, so remove from table
                None => false,
            }
        });

        let mut output = self.group_values.row_converter().convert_rows(group_values.iter())?;

        // TODO: Materialize dictionaries in group keys
        // https://github.com/apache/datafusion/issues/7647
        for (field, array) in self.schema.fields.iter().zip(&mut output) {
            let expected = field.data_type();
            *array = encode_array_if_necessary(array, expected)?;
        }

        Ok(Some(output))
    }

    /// Emit first `n` values and shift all values to fit into blocks
    ///
    /// `n` must be smaller than [`Self::block_size`] and larger than `0`
    /// `n` must be smaller or equal to [`Self::len`]
    fn emit_first_n(&mut self, n: usize) -> Result<Vec<ArrayRef>> {
        let group_values = self.group_values.take_n(n, None::<std::iter::Empty<_>>);

        let block_size = self.block_size();

        // TODO - materialize n to the fastest data structure that will sub will work

        self.map.retain(|(_exists_hash, group_idx)| {
            // Decrement group index by block size
            match group_idx.sub_flat_checked(n, block_size) {
                // Group index was >= block size, shift value down
                Some(sub) => {
                    *group_idx = sub;
                    true
                }
                // Group index was < block size, so remove from table
                None => false,
            }
        });

        let mut output = self.group_values.row_converter().convert_rows(group_values.iter())?;

        // TODO: Materialize dictionaries in group keys
        // https://github.com/apache/datafusion/issues/7647
        for (field, array) in self.schema.fields.iter().zip(&mut output) {
            let expected = field.data_type();
            *array = encode_array_if_necessary(array, expected)?;
        }

        Ok(output)
    }

    fn values_preserving(
        &mut self,
        selection: BlockedGroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        selection.validate_num_groups(self.group_values.len())?;
        let rows = selection.iter().map(|index| self.group_values.value(index));
        let mut output = self.group_values.row_converter().convert_rows(rows)?;

        // TODO: Materialize dictionaries in group keys
        // https://github.com/apache/datafusion/issues/7647
        for (field, array) in self.schema.fields.iter().zip(&mut output) {
            *array = encode_array_if_necessary(array, field.data_type())?;
        }
        Ok(output)
    }

    fn supports_values_preserving(&self) -> bool {
        true
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.group_values.reset();
        self.map.clear();
        self.map.shrink_to(num_rows, |_| 0); // hasher does not matter since the map is cleared
        self.map_size = self.map.capacity() * size_of::<(u64, BlocksIndex)>();
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, ListArray};
    use arrow::datatypes::{Field, Int32Type, Schema};

    const DEFAULT_BATCH_SIZE: usize = 8192;

    #[test]
    fn preserving_nested_row_values() -> Result<()> {
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group",
            DataType::List(field),
            true,
        )]));
        let mut group_values = BlockedGroupValuesRows::try_new(schema, DEFAULT_BATCH_SIZE)?;
        let input = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![Some(3)]),
            Some(vec![Some(1), Some(2)]),
        ])) as ArrayRef;
        let mut groups = vec![];
        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, [0, 1, 2, 0].map(BlocksIndex::new_in_first_block).as_slice());

        let blocked_indices = [2, 0, 1, 2].map(BlocksIndex::new_in_first_block);
        let selection = BlockedGroupSelection::try_from_indices(blocked_indices.as_slice(), 3, DEFAULT_BATCH_SIZE)?;
        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(3)]),
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![Some(3)]),
        ]);
        for _ in 0..2 {
            let actual = group_values.values_preserving(selection)?;
            assert_eq!(actual[0].as_list::<i32>(), &expected);
        }

        let input = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(4)]),
        ])) as ArrayRef;
        group_values.intern(&[input], &mut groups)?;
        assert_eq!(groups, [3].map(BlocksIndex::new_in_first_block).as_slice());
        Ok(())
    }
}
