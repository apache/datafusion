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

use std::sync::Arc;

use crate::aggregates::group_values::GroupValues;
use ahash::RandomState;
use arrow::array::AsArray;
use arrow::compute::cast;
use arrow::datatypes::{ArrowPrimitiveType, Int32Type, Int64Type};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use arrow_array::{Array, ArrayRef, PrimitiveArray};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::EmitTo;
use hashbrown::raw::RawTable;

const INITIAL_CAPACITY: usize = 8192;

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
    group_values: Option<Rows>,

    /// reused buffer to store hashes
    hashes_buffer: Vec<u64>,

    /// reused buffer to store rows
    rows_buffer: Rows,

    group_values_v2: Option<Vec<Box<dyn ArrayEq>>>,

    /// Random state for creating hashes
    random_state: RandomState,

    current_hashes: Vec<u64>,
    current_offsets: Vec<usize>,
    new_entries: Vec<usize>,
    need_equality_check: Vec<usize>,
    no_match: Vec<usize>,
    capacity: usize,
    hash_table: Vec<usize>,
    hashes: Vec<u64>,
}

impl GroupValuesRows {
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        let row_converter = RowConverter::new(
            schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )?;

        let map = RawTable::with_capacity(0);

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
            group_values_v2: None,
            random_state: Default::default(),
            current_hashes: Default::default(),
            current_offsets: Default::default(),
            new_entries: Default::default(),
            need_equality_check: Default::default(),
            no_match: Default::default(),
            capacity: INITIAL_CAPACITY,
            hash_table: vec![0; INITIAL_CAPACITY],
            hashes: Vec::with_capacity(INITIAL_CAPACITY),
        })
    }
}

impl GroupValues for GroupValuesRows {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        // Convert the group keys into the row format
        let group_rows = &mut self.rows_buffer;
        group_rows.clear();
        self.row_converter.append(group_rows, cols)?;
        let n_rows = group_rows.num_rows();

        // let mut group_values = match self.group_values.take() {
        //     Some(group_values) => group_values,
        //     None => self.row_converter.empty_rows(0, 0),
        // };

        let mut group_values_v2 = match self.group_values_v2.take() {
            Some(group_values) => group_values,
            None => {
                let len = cols.len();
                let mut v = Vec::with_capacity(len);
                for f in self.schema.fields() {
                    match f.data_type() {
                        &DataType::Int32 => {
                            let b = PrimitiveGroupValueBuilder::<Int32Type>::new();
                            v.push(Box::new(b) as _)
                        }
                        &DataType::Int64 => {
                            let b = PrimitiveGroupValueBuilder::<Int64Type>::new();
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
        let batch_hashes = &mut self.current_hashes;
        batch_hashes.clear();
        batch_hashes.resize(n_rows, 0);
        create_hashes(cols, &self.random_state, batch_hashes)?;

        // rehash if necessary
        let current_n_rows = self.hashes.len();
        if current_n_rows + n_rows > (self.hash_table.capacity() as f64 / 1.5) as usize {
            let new_capacity = current_n_rows + n_rows;
            let new_capacity = std::cmp::max(new_capacity, 2 * self.capacity);
            let new_capacity = new_capacity.next_power_of_two();
            let mut new_table = vec![0; new_capacity];
            let new_bit_mask = new_capacity - 1;

            let table_ptr = self.hash_table.as_ptr();
            let hashes_ptr = self.hashes.as_ptr();
            let new_table_ptr = new_table.as_mut_ptr();

            unsafe {
                for i in 0..self.capacity {
                    let offset = *table_ptr.add(i);
                    if offset != 0 {
                        let hash = *hashes_ptr.add(offset as usize - 1);

                        let mut new_idx = hash as usize & new_bit_mask;
                        let mut num_iter = 0;
                        while *new_table_ptr.add(new_idx) != 0 {
                            num_iter += 1;
                            new_idx += num_iter * num_iter;
                            new_idx &= new_bit_mask;
                        }
                        *new_table_ptr.add(new_idx) = offset;
                    }
                }
            }

            self.hash_table = new_table;
            self.capacity = new_capacity;
        }

        let bit_mask = self.capacity - 1;
        self.current_offsets.resize(n_rows, 0);
        for row_idx in 0..n_rows {
            let hash = self.current_hashes[row_idx];
            let hash_table_idx = (hash as usize) & bit_mask;
            self.current_offsets[row_idx] = hash_table_idx;
        }

        // initially, `selection_vector[i]` = row at `i`
        let mut selection_vector: Vec<usize> = (0..n_rows).collect();
        let mut remaining_entries = n_rows;
        self.new_entries.resize(n_rows, 0);
        self.need_equality_check.resize(n_rows, 0);
        self.no_match.resize(n_rows, 0);
        let mut num_iter = 1;

        while remaining_entries > 0 {
            assert!(self.hashes.len() + remaining_entries <= self.capacity);

            let mut n_new_entries = 0;
            let mut n_need_equality_check = 0;
            let mut n_no_match = 0;
            selection_vector
                .iter()
                .take(remaining_entries)
                .for_each(|&row_idx| {
                    let hash = self.current_hashes[row_idx];
                    let ht_offset = self.current_offsets[row_idx];
                    let offset = self.hash_table[ht_offset];
                    if offset == 0 {
                        // the slot is empty, so we can create a new entry here
                        self.new_entries[n_new_entries] = row_idx;
                        n_new_entries += 1;

                        // we increment the slot entry offset by 1 to reserve the special value
                        // 0 for the scenario when the slot in the
                        // hash table is unoccupied.
                        self.hash_table[ht_offset] = self.hashes.len() + 1;
                        // also update hash for this slot so it can be used later
                        self.hashes.push(hash);
                    } else if self.hashes[offset - 1] == hash {
                        // slot is not empty, and hash value match, now need to do equality
                        // check
                        self.need_equality_check[n_need_equality_check] = row_idx;
                        n_need_equality_check += 1;
                    } else {
                        // slot is not empty, and hash value doesn't match, we have a hash
                        // collision and need to do probing
                        self.no_match[n_no_match] = row_idx;
                        n_no_match += 1;
                    }
                });

            self.new_entries
                .iter()
                .take(n_new_entries)
                .for_each(|row_idx| {
                    for (i, group_value) in group_values_v2.iter_mut().enumerate() {
                        group_value.append_val(&cols[i], *row_idx)
                    }

                    // let row = group_rows.row(*row_idx);
                    // group_values.push(row);
                });
            // assert_eq!(self.hashes.len(), group_values.num_rows());
            assert_eq!(self.hashes.len(), group_values_v2[0].len());

            self.need_equality_check
                .iter()
                .take(n_need_equality_check)
                .for_each(|row_idx| {
                    let row_idx = *row_idx;
                    let ht_offset = self.current_offsets[row_idx];
                    let offset = self.hash_table[ht_offset];

                    let mut all_eq = true;

                    fn compare_equal(
                        arry_eq: &dyn ArrayEq,
                        lhs_row: usize,
                        array: &ArrayRef,
                        rhs_row: usize,
                    ) -> bool {
                        arry_eq.equal_to(lhs_row, array, rhs_row)
                    }

                    for (i, group_val) in group_values_v2.iter().enumerate() {
                        if !compare_equal(
                            group_val.as_ref(),
                            offset - 1,
                            &cols[i],
                            row_idx,
                        ) {
                            all_eq = false;
                            break;
                        }
                    }
                    if !all_eq {
                        self.no_match[n_no_match] = row_idx;
                        n_no_match += 1;
                    }

                    // let existing = group_values.row(offset - 1);
                    // let incoming = group_rows.row(row_idx);
                    // let is_eq = existing != incoming;

                    // assert_eq!(is_eq, !all_eq);
                });

            // now we need to probing for those rows in `no_match`
            let delta = num_iter * num_iter;
            let bit_mask = self.capacity - 1;
            for i in 0..n_no_match {
                let row_idx = self.no_match[i];
                let slot_idx = self.current_offsets[row_idx] + delta;
                self.current_offsets[row_idx] = slot_idx & bit_mask;
            }

            std::mem::swap(&mut self.no_match, &mut selection_vector);
            remaining_entries = n_no_match;
            num_iter += 1;
        }

        self.current_offsets
            .iter()
            .take(n_rows)
            .for_each(|&hash_table_offset| {
                groups.push(self.hash_table[hash_table_offset] - 1);
            });
        // self.group_values = Some(group_values);
        self.group_values_v2 = Some(group_values_v2);

        Ok(())
    }

    fn size(&self) -> usize {
        let group_values_size = self.group_values.as_ref().map(|v| v.size()).unwrap_or(0);
        self.row_converter.size()
            + group_values_size
            + self.map_size
            + self.rows_buffer.size()
            + self.hashes_buffer.allocated_size()
            + self.hash_table.allocated_size()
            + self.current_hashes.allocated_size()
            + self.current_offsets.allocated_size()
            + self.need_equality_check.allocated_size()
            + self.new_entries.allocated_size()
            + self.no_match.allocated_size()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.hashes.len()
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        // let mut group_values = self
        //     .group_values
        //     .take()
        //     .expect("Can not emit from empty rows");

        let mut group_values_v2 = self
            .group_values_v2
            .take()
            .expect("Can not emit from empty rows");

        let mut output = match emit_to {
            EmitTo::All => {
                let output = group_values_v2
                    .into_iter()
                    .map(|v| v.build())
                    .collect::<Vec<_>>();

                // let output_true = self.row_converter.convert_rows(&group_values)?;
                // assert_eq!(output, output_true);
                // group_values.clear();
                self.group_values_v2 = None;
                output
            }
            EmitTo::First(n) => {
                let len = group_values_v2.len();
                let first_n: Vec<Box<dyn ArrayEq>> = group_values_v2.drain(..n).collect();
                let output = first_n.into_iter().map(|v| v.build()).collect::<Vec<_>>();
                assert_eq!(len, group_values_v2.len() + n);
                self.group_values_v2 = Some(group_values_v2);

                // let groups_rows = group_values.iter().take(n);
                // let output_true = self.row_converter.convert_rows(groups_rows)?;
                // assert_eq!(output, output_true);

                // Clear out first n group keys by copying them to a new Rows.
                // TODO file some ticket in arrow-rs to make this more efficient?
                // let mut new_group_values = self.row_converter.empty_rows(0, 0);
                // for row in group_values.iter().skip(n) {
                //     new_group_values.push(row);
                // }
                // std::mem::swap(&mut new_group_values, &mut group_values);

                self.hashes.drain(0..n);

                let hash_table_ptr = self.hash_table.as_mut_ptr();
                unsafe {
                    for i in 0..self.capacity {
                        let offset = *hash_table_ptr.add(i);
                        if offset != 0 {
                            match offset.checked_sub(n + 1) {
                                Some(sub) => *hash_table_ptr.add(i) = sub + 1,
                                None => *hash_table_ptr.add(i) = 0,
                            }
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

        // self.group_values = Some(group_values);
        Ok(output)
    }

    fn clear_shrink(&mut self, batch: &RecordBatch) {
        let count = batch.num_rows();
        self.group_values = self.group_values.take().map(|mut rows| {
            rows.clear();
            rows
        });

        self.group_values_v2 = self.group_values_v2.take().map(|mut group_values| {
            group_values.clear();
            group_values
        });

        self.map.clear();
        self.map.shrink_to(count, |_| 0); // hasher does not matter since the map is cleared
        self.map_size = self.map.capacity() * std::mem::size_of::<(u64, usize)>();
        self.hashes_buffer.clear();
        self.hashes_buffer.shrink_to(count);
    }
}

trait ArrayEq: Send + Sync {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool;
    fn append_val(&mut self, array: &ArrayRef, row: usize);
    fn len(&self) -> usize;
    fn build(self: Box<Self>) -> ArrayRef;
}

struct PrimitiveGroupValueBuilder<T: ArrowPrimitiveType>(Vec<Option<T::Native>>);

impl<T: ArrowPrimitiveType> PrimitiveGroupValueBuilder<T> {
    pub fn new() -> Self {
        Self(vec![])
    }
}

impl<T: ArrowPrimitiveType> ArrayEq for PrimitiveGroupValueBuilder<T> {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        let elem = self.0[lhs_row];
        let arr = array.as_primitive::<T>();
        let rhs_elem = arr.value(rhs_row);
        elem.unwrap() == rhs_elem
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) {
        let arr = array.as_primitive::<T>();
        let elem = arr.value(row);
        self.0.push(Some(elem))
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        Arc::new(PrimitiveArray::<T>::from_iter(self.0))
    }
}
