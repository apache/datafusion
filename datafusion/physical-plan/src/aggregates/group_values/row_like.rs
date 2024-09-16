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
use arrow::compute::cast;
use arrow::datatypes::{
    Date32Type, Date64Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow::record_batch::RecordBatch;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::proxy::{RawTableAllocExt, VecAllocExt};
use datafusion_expr::EmitTo;
use datafusion_physical_expr::binary_map::OutputType;
use datafusion_physical_expr_common::group_value_row::{
    ArrayRowEq, ByteGroupValueBuilder, PrimitiveGroupValueBuilder,
};
use hashbrown::raw::RawTable;

/// Compare GroupValue Rows column by column
pub struct GroupValuesRowLike {
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

    /// The actual group by values, stored in arrow [`Row`] format.
    /// `group_values[i]` holds the group value for group_index `i`.
    ///
    /// The row format is used to compare group keys quickly and store
    /// them efficiently in memory. Quick comparison is especially
    /// important for multi-column group keys.
    ///
    /// [`Row`]: arrow::row::Row
    // group_values: Option<Rows>,

    /// reused buffer to store hashes
    hashes_buffer: Vec<u64>,

    /// Random state for creating hashes
    random_state: RandomState,
    group_values: Option<Vec<Box<dyn ArrayRowEq>>>,
}

impl GroupValuesRowLike {
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        let map = RawTable::with_capacity(0);
        Ok(Self {
            schema,
            map,
            map_size: 0,
            group_values: None,
            hashes_buffer: Default::default(),
            random_state: Default::default(),
        })
    }
}

impl GroupValues for GroupValuesRowLike {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        let n_rows = cols[0].len();
        let mut group_values = match self.group_values.take() {
            Some(group_values) => group_values,
            None => {
                let len = cols.len();
                let mut v = Vec::with_capacity(len);

                for f in self.schema.fields().iter() {
                    match f.data_type() {
                        &DataType::Int8 => {
                            let b = PrimitiveGroupValueBuilder::<Int8Type>::default();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Int16 => {
                            let b = PrimitiveGroupValueBuilder::<Int16Type>::default();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Int32 => {
                            let b = PrimitiveGroupValueBuilder::<Int32Type>::default();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Int64 => {
                            let b = PrimitiveGroupValueBuilder::<Int64Type>::default();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::UInt8 => {
                            let b = PrimitiveGroupValueBuilder::<UInt8Type>::default();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::UInt16 => {
                            let b = PrimitiveGroupValueBuilder::<UInt16Type>::default();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::UInt32 => {
                            let b = PrimitiveGroupValueBuilder::<UInt32Type>::default();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::UInt64 => {
                            let b = PrimitiveGroupValueBuilder::<UInt64Type>::default();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Float32 => {
                            let b = PrimitiveGroupValueBuilder::<Float32Type>::default();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Float64 => {
                            let b = PrimitiveGroupValueBuilder::<Float64Type>::default();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Date32 => {
                            let b = PrimitiveGroupValueBuilder::<Date32Type>::default();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Date64 => {
                            let b = PrimitiveGroupValueBuilder::<Date64Type>::default();
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
                        dt => todo!("{dt} not impl"),
                    }
                }
                v
            }
        };

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
                // verify that the group that we are inserting with hash is
                // actually the same key value as the group in
                // existing_idx  (aka group_values @ row)
                // && group_rows.row(row) == group_values.row(*group_idx)

                fn compare_equal(
                    arry_eq: &dyn ArrayRowEq,
                    lhs_row: usize,
                    array: &ArrayRef,
                    rhs_row: usize,
                ) -> bool {
                    arry_eq.equal_to(lhs_row, array, rhs_row)
                }

                for (i, group_val) in group_values.iter().enumerate() {
                    if !compare_equal(group_val.as_ref(), *group_idx, &cols[i], row) {
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
                    let group_idx = group_values[0].len();
                    for (i, group_value) in group_values.iter_mut().enumerate() {
                        group_value.append_val(&cols[i], row);
                        let len = group_value.len();
                        if i == 0 {
                            checklen = len;
                        } else {
                            assert_eq!(checklen, len);
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

        self.group_values = Some(group_values);
        Ok(())
    }

    fn size(&self) -> usize {
        let group_values_size = self.group_values.as_ref().map(|v| v.len()).unwrap_or(0);
        group_values_size + self.map_size + self.hashes_buffer.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.group_values.as_ref().map(|v| v[0].len()).unwrap_or(0)
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut group_values_v2 = self
            .group_values
            .take()
            .expect("Can not emit from empty rows");

        let mut output = match emit_to {
            EmitTo::All => {
                let output = group_values_v2
                    .into_iter()
                    .map(|v| v.build())
                    .collect::<Vec<_>>();
                self.group_values = None;
                output
            }
            EmitTo::First(n) => {
                let output = group_values_v2
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
                self.group_values = Some(group_values_v2);
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
        self.group_values = self.group_values.take().map(|mut rows| {
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
