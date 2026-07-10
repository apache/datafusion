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

use crate::aggregates::group_values::{GroupValues, schema_with_group_values};
use arrow::array::{
    Array, ArrayRef, ListArray, PrimitiveArray, RunArray, StructArray,
    downcast_run_end_index,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use arrow::error::ArrowError;
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
    /// The current output schema. Dictionary key types may grow, but never shrink.
    schema: SchemaRef,

    /// Schema used by the row converter. Top-level dictionaries are materialized
    /// to their value type so input batches with promoted dictionary keys remain
    /// compatible with the same converter.
    row_schema: SchemaRef,

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
        let row_schema = materialized_row_schema(&schema);
        let row_converter = RowConverter::new(
            row_schema
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
            row_schema,
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
        // Dictionary keys are an encoding detail and can be promoted between
        // aggregate stages. Materialize top-level dictionaries before row
        // conversion so the stored row representation remains stable.
        // Also normalize -0.0 → +0.0 so RowConverter (IEEE 754 totalOrder) and
        // primitive hashing both group ±0 together.
        let normalized_cols = cols
            .iter()
            .zip(self.row_schema.fields())
            .map(|(col, field)| {
                let col = if col.data_type() == field.data_type() {
                    Arc::clone(col)
                } else {
                    cast(col.as_ref(), field.data_type())?
                };
                Ok(normalize_float_zero(&col))
            })
            .collect::<Result<Vec<_>>>()?;
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

        // Re-encode dictionary group keys using the current key type. If the
        // emitted cardinality no longer fits, grow to the next key width and
        // retain that promoted type for all later emissions.
        for (field, array) in self.schema.fields().iter().zip(&mut output) {
            *array = dictionary_encode_if_necessary(array, field.data_type())?;
        }
        self.schema = schema_with_group_values(&self.schema, &output);

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

fn materialized_row_schema(schema: &SchemaRef) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Dictionary(_, value_type) => Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(value_type.as_ref().clone()),
            ),
            _ => Arc::clone(field),
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

fn dictionary_encode_if_necessary(
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
                    dictionary_encode_if_necessary(column, expected_field.data_type())
                })
                .collect::<Result<Vec<_>>>()?;
            let fields = expected_fields
                .iter()
                .zip(&arrays)
                .map(|(field, array)| field_with_array_type(field, array))
                .collect::<Vec<_>>()
                .into();

            Ok(Arc::new(StructArray::try_new(
                fields,
                arrays,
                struct_array.nulls().cloned(),
            )?))
        }
        (DataType::List(expected_field), &DataType::List(_)) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = dictionary_encode_if_necessary(
                list.values(),
                expected_field.data_type(),
            )?;
            let field = field_with_array_type(expected_field, &values);

            Ok(Arc::new(ListArray::try_new(
                field,
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        (DataType::Dictionary(_, _), _) => {
            let mut target = expected.clone();
            loop {
                match cast(array.as_ref(), &target) {
                    Ok(array) => return Ok(array),
                    Err(ArrowError::DictionaryKeyOverflowError) => {
                        let Some(promoted) = promote_dictionary_type(&target) else {
                            return Err(ArrowError::DictionaryKeyOverflowError.into());
                        };
                        target = promoted;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
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
                    let values = dictionary_encode_if_necessary(
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

fn field_with_array_type(field: &FieldRef, array: &ArrayRef) -> FieldRef {
    if field.data_type() == array.data_type() {
        Arc::clone(field)
    } else {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(array.data_type().clone()),
        )
    }
}

fn promote_dictionary_type(data_type: &DataType) -> Option<DataType> {
    let DataType::Dictionary(key_type, value_type) = data_type else {
        return None;
    };
    let key_type = match key_type.as_ref() {
        DataType::Int8 => DataType::Int16,
        DataType::Int16 => DataType::Int32,
        DataType::Int32 => DataType::Int64,
        DataType::UInt8 => DataType::UInt16,
        DataType::UInt16 => DataType::UInt32,
        DataType::UInt32 => DataType::UInt64,
        DataType::Int64 | DataType::UInt64 => return None,
        _ => return None,
    };
    Some(DataType::Dictionary(
        Box::new(key_type),
        Box::new(value_type.as_ref().clone()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{DictionaryArray, StringArray, StringDictionaryBuilder};
    use arrow::datatypes::{
        Field, Int8Type, Int16Type, Schema, UInt8Type, UInt16Type,
    };

    #[test]
    fn dict_uint8_utf8_promotes_and_does_not_shrink() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "k",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            false,
        )]));

        let mut group_values = GroupValuesRows::try_new(Arc::clone(&schema))?;
        let mut groups = vec![];
        for i in 0u32..257 {
            let mut builder = StringDictionaryBuilder::<UInt8Type>::new();
            builder.append_value(format!("group_{i}"));
            let array: ArrayRef = Arc::new(builder.finish());
            group_values.intern(&[array], &mut groups)?;
        }

        let arrays = group_values.emit(EmitTo::All)?;
        assert_eq!(
            arrays[0].data_type(),
            &DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
        );
        let dictionary = arrays[0]
            .as_any()
            .downcast_ref::<DictionaryArray<UInt16Type>>()
            .unwrap();
        let values = dictionary
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..257 {
            let key = dictionary.keys().value(row) as usize;
            assert_eq!(values.value(key), format!("group_{row}"));
        }

        let mut builder = StringDictionaryBuilder::<UInt8Type>::new();
        builder.append_value("after_promotion");
        group_values.intern(&[Arc::new(builder.finish())], &mut groups)?;
        let arrays = group_values.emit(EmitTo::All)?;
        assert_eq!(
            arrays[0].data_type(),
            &DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
        );

        Ok(())
    }

    #[test]
    fn dict_uint8_utf8_keeps_key_type_at_capacity() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "k",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            false,
        )]));
        let mut group_values = GroupValuesRows::try_new(schema)?;
        let mut groups = vec![];
        for i in 0u32..256 {
            let mut builder = StringDictionaryBuilder::<UInt8Type>::new();
            builder.append_value(format!("group_{i}"));
            group_values.intern(&[Arc::new(builder.finish())], &mut groups)?;
        }

        let arrays = group_values.emit(EmitTo::All)?;
        assert_eq!(
            arrays[0].data_type(),
            &DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8))
        );
        assert!(arrays[0]
            .as_any()
            .downcast_ref::<DictionaryArray<UInt8Type>>()
            .is_some());
        Ok(())
    }

    #[test]
    fn dict_int8_utf8_promotes_after_capacity() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "k",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        )]));
        let mut group_values = GroupValuesRows::try_new(schema)?;
        let mut groups = vec![];
        for i in 0u32..129 {
            let mut builder = StringDictionaryBuilder::<Int8Type>::new();
            builder.append_value(format!("group_{i}"));
            group_values.intern(&[Arc::new(builder.finish())], &mut groups)?;
        }

        let arrays = group_values.emit(EmitTo::All)?;
        assert_eq!(
            arrays[0].data_type(),
            &DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8))
        );
        assert!(arrays[0]
            .as_any()
            .downcast_ref::<DictionaryArray<Int16Type>>()
            .is_some());
        Ok(())
    }
}
