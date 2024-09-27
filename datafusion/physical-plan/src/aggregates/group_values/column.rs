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

use crate::aggregates::group_values::group_column::{
    ByteGroupValueBuilder, GroupColumn, PrimitiveGroupValueBuilder,
};
use crate::aggregates::group_values::GroupValues;
use ahash::RandomState;
use arrow::compute::cast;
use arrow::datatypes::{
    Date32Type, Date64Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow::record_batch::RecordBatch;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{not_impl_err, DataFusionError, Result};
use datafusion_execution::memory_pool::proxy::{RawTableAllocExt, VecAllocExt};
use datafusion_expr::EmitTo;
use datafusion_physical_expr::binary_map::OutputType;

use hashbrown::raw::RawTable;

/// Compare GroupValue Rows column by column
pub struct GroupValuesColumn {
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
    map: RawTable<(u64, usize)>,

    /// The size of `map` in bytes
    map_size: usize,

    /// The actual group by values, stored column-wise. Compare from
    /// the left to right, each column is stored as `ArrayRowEq`.
    /// This is shown faster than the row format
    group_values: Vec<Box<dyn GroupColumn>>,

    /// reused buffer to store hashes
    hashes_buffer: Vec<u64>,

    /// Random state for creating hashes
    random_state: RandomState,
}

impl GroupValuesColumn {
    /// Create a new instance of GroupValuesColumn if supported for the specified schema
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        let map = RawTable::with_capacity(0);
        Ok(Self {
            schema,
            map,
            map_size: 0,
            group_values: vec![],
            hashes_buffer: Default::default(),
            random_state: Default::default(),
        })
    }

    /// Returns true if [`GroupValuesColumn`] supported for the specified schema
    pub fn supported_schema(schema: &Schema) -> bool {
        schema
            .fields()
            .iter()
            .map(|f| f.data_type())
            .all(Self::supported_type)
    }

    /// Returns true if the specified data type is supported by [`GroupValuesColumn`]
    ///
    /// In order to be supported, there must be a specialized implementation of
    /// [`GroupColumn`] for the data type, instantiated in [`Self::intern`]
    fn supported_type(data_type: &DataType) -> bool {
        matches!(
            *data_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::Date32
                | DataType::Date64
        )
    }
}

impl GroupValues for GroupValuesColumn {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        let n_rows = cols[0].len();

        if self.group_values.is_empty() {
            let mut v = Vec::with_capacity(cols.len());

            for f in self.schema.fields().iter() {
                let nullable = f.is_nullable();
                match f.data_type() {
                    &DataType::Int8 => {
                        let b = PrimitiveGroupValueBuilder::<Int8Type>::new(nullable);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::Int16 => {
                        let b = PrimitiveGroupValueBuilder::<Int16Type>::new(nullable);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::Int32 => {
                        let b = PrimitiveGroupValueBuilder::<Int32Type>::new(nullable);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::Int64 => {
                        let b = PrimitiveGroupValueBuilder::<Int64Type>::new(nullable);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::UInt8 => {
                        let b = PrimitiveGroupValueBuilder::<UInt8Type>::new(nullable);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::UInt16 => {
                        let b = PrimitiveGroupValueBuilder::<UInt16Type>::new(nullable);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::UInt32 => {
                        let b = PrimitiveGroupValueBuilder::<UInt32Type>::new(nullable);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::UInt64 => {
                        let b = PrimitiveGroupValueBuilder::<UInt64Type>::new(nullable);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::Float32 => {
                        let b = PrimitiveGroupValueBuilder::<Float32Type>::new(nullable);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::Float64 => {
                        let b = PrimitiveGroupValueBuilder::<Float64Type>::new(nullable);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::Date32 => {
                        let b = PrimitiveGroupValueBuilder::<Date32Type>::new(nullable);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::Date64 => {
                        let b = PrimitiveGroupValueBuilder::<Date64Type>::new(nullable);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::Utf8 => {
                        let b = ByteGroupValueBuilder::<i32>::new(OutputType::Utf8);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::LargeUtf8 => {
                        let b = ByteGroupValueBuilder::<i64>::new(OutputType::Utf8);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::Binary => {
                        let b = ByteGroupValueBuilder::<i32>::new(OutputType::Binary);
                        v.push(Box::new(b) as _)
                    }
                    &DataType::LargeBinary => {
                        let b = ByteGroupValueBuilder::<i64>::new(OutputType::Binary);
                        v.push(Box::new(b) as _)
                    }
                    dt => {
                        return not_impl_err!("{dt} not supported in GroupValuesColumn")
                    }
                }
            }
            self.group_values = v;
        }

        // tracks to which group each of the input rows belongs
        groups.clear();

        // 1.1 Calculate the group keys for the group values
        let batch_hashes = &mut self.hashes_buffer;
        batch_hashes.clear();
        batch_hashes.resize(n_rows, 0);
        create_hashes(cols, &self.random_state, batch_hashes)?;

        for (row, &target_hash) in batch_hashes.iter().enumerate() {
            let entry = self.map.get_mut(target_hash, |(exist_hash, group_idx)| {
                // Somewhat surprisingly, this closure can be called even if the
                // hash doesn't match, so check the hash first with an integer
                // comparison first avoid the more expensive comparison with
                // group value. https://github.com/apache/datafusion/pull/11718
                if target_hash != *exist_hash {
                    return false;
                }

                fn check_row_equal(
                    array_row: &dyn GroupColumn,
                    lhs_row: usize,
                    array: &ArrayRef,
                    rhs_row: usize,
                ) -> bool {
                    array_row.equal_to(lhs_row, array, rhs_row)
                }

                for (i, group_val) in self.group_values.iter().enumerate() {
                    if !check_row_equal(group_val.as_ref(), *group_idx, &cols[i], row) {
                        return false;
                    }
                }

                true
            });

            let group_idx = match entry {
                // Existing group_index for this group value
                Some((_hash, group_idx)) => *group_idx,
                //  1.2 Need to create new entry for the group
                None => {
                    // Add new entry to aggr_state and save newly created index
                    // let group_idx = group_values.num_rows();
                    // group_values.push(group_rows.row(row));

                    let mut checklen = 0;
                    let group_idx = self.group_values[0].len();
                    for (i, group_value) in self.group_values.iter_mut().enumerate() {
                        group_value.append_val(&cols[i], row);
                        let len = group_value.len();
                        if i == 0 {
                            checklen = len;
                        } else {
                            debug_assert_eq!(checklen, len);
                        }
                    }

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
        let group_values_size: usize = self.group_values.iter().map(|v| v.size()).sum();
        group_values_size + self.map_size + self.hashes_buffer.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        if self.group_values.is_empty() {
            return 0;
        }

        self.group_values[0].len()
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut output = match emit_to {
            EmitTo::All => {
                let group_values = std::mem::take(&mut self.group_values);
                debug_assert!(self.group_values.is_empty());

                group_values
                    .into_iter()
                    .map(|v| v.build())
                    .collect::<Vec<_>>()
            }
            EmitTo::First(n) => {
                let output = self
                    .group_values
                    .iter_mut()
                    .map(|v| v.take_n(n))
                    .collect::<Vec<_>>();

                // SAFETY: self.map outlives iterator and is not modified concurrently
                unsafe {
                    for bucket in self.map.iter() {
                        // Decrement group index by n
                        match bucket.as_ref().1.checked_sub(n) {
                            // Group index was >= n, shift value down
                            Some(sub) => bucket.as_mut().1 = sub,
                            // Group index was < n, so remove from table
                            None => self.map.erase(bucket),
                        }
                    }
                }

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

        Ok(output)
    }

    fn clear_shrink(&mut self, batch: &RecordBatch) {
        let count = batch.num_rows();
        self.group_values.clear();
        self.map.clear();
        self.map.shrink_to(count, |_| 0); // hasher does not matter since the map is cleared
        self.map_size = self.map.capacity() * std::mem::size_of::<(u64, usize)>();
        self.hashes_buffer.clear();
        self.hashes_buffer.shrink_to(count);
    }
}
