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

use std::ops::Sub;
use std::{iter, mem, usize};

use crate::aggregates::group_values::group_column::{
    ByteGroupValueBuilder, ByteViewGroupValueBuilder, GroupColumn,
    PrimitiveGroupValueBuilder,
};
use crate::aggregates::group_values::GroupValues;
use ahash::RandomState;
use arrow::compute::{self, cast};
use arrow::datatypes::{
    BinaryViewType, Date32Type, Date64Type, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, StringViewType, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use arrow::record_batch::RecordBatch;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array,
    Date64Array, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeStringArray, StringArray, StringViewArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{not_impl_err, DataFusionError, Result};
use datafusion_execution::memory_pool::proxy::{RawTableAllocExt, VecAllocExt};
use datafusion_expr::EmitTo;
use datafusion_physical_expr::binary_map::OutputType;

use datafusion_physical_expr_common::datum::compare_with_eq;
use hashbrown::raw::{Bucket, RawTable};

const NON_INLINED_FLAG: u64 = 0x8000000000000000;
const VALUE_MASK: u64 = 0x7FFFFFFFFFFFFFFF;

/// `BucketContext` is a packed struct
///
/// ### Format:
///
///   +---------------------+--------------------+
///   | checking flag(1bit) | group index(63bit) |
///   +---------------------+--------------------+
///    
/// ### Checking flag
///
///   It is possible that rows with same hash values exist in `input cols`.
///   And if we `vectorized_equal_to` and `vectorized append` them
///   in the same round, some fault cases will occur especially when
///   they are totally the repeated rows...
///
///   For example:
///     - Two repeated rows exist in `input cols`.
///
///     - We found their hash values equal to one exist group
///
///     - We then perform `vectorized_equal_to` for them to the exist group,
///       and found their values not equal to the exist one
///
///     - Finally when perform `vectorized append`, we decide to build two
///       respective new groups for them, even we actually just need one
///       new group...
///
///   So for solving such cases simply, if some rows with same hash value
///   in `input cols`, just allow to process one of them in a round,
///   and this flag is used to represent that one of them is processing
///   in current round.
///
/// ### Group index
///
///     The group's index in group values
///
#[derive(Debug, Clone, Copy)]
struct GroupIndexView(u64);

impl GroupIndexView {
    #[inline]
    pub fn is_non_inlined(&self) -> bool {
        (self.0 & NON_INLINED_FLAG) > 0
    }

    #[inline]
    pub fn new_inlined(group_index: u64) -> Self {
        Self(group_index)
    }

    #[inline]
    pub fn new_non_inlined(list_offset: u64) -> Self {
        let non_inlined_value = list_offset | NON_INLINED_FLAG;
        Self(non_inlined_value)
    }

    #[inline]
    pub fn value(&self) -> u64 {
        self.0 & VALUE_MASK
    }
}

/// A [`GroupValues`] that stores multiple columns of group values.
///
pub struct VectorizedGroupValuesColumn {
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
    map: RawTable<(u64, GroupIndexView)>,

    /// The size of `map` in bytes
    map_size: usize,

    /// The lists for group indices with the same hash value
    ///
    /// It is possible that hash value collision exists,
    /// and we will chain the `group indices` with same hash value
    ///
    /// The chained indices is like:
    ///   `latest group index -> older group index -> even older group index -> ...`
    group_index_lists: Vec<Vec<usize>>,

    index_lists_updates: Vec<(usize, usize)>,

    /// Similar as `current_indices`, but `remaining_indices`
    /// is used to store the rows will be processed in next round.
    scalarized_indices: Vec<usize>,

    /// The `vectorized_equal_tod` row indices buffer
    vectorized_equal_to_row_indices: Vec<usize>,

    /// The `vectorized_equal_tod` group indices buffer
    vectorized_equal_to_group_indices: Vec<usize>,

    /// The `vectorized_equal_tod` result buffer
    vectorized_equal_to_results: Vec<bool>,

    /// The `vectorized append` row indices buffer
    vectorized_append_row_indices: Vec<usize>,

    /// The actual group by values, stored column-wise. Compare from
    /// the left to right, each column is stored as [`GroupColumn`].
    ///
    /// Performance tests showed that this design is faster than using the
    /// more general purpose [`GroupValuesRows`]. See the ticket for details:
    /// <https://github.com/apache/datafusion/pull/12269>
    ///
    /// [`GroupValuesRows`]: crate::aggregates::group_values::row::GroupValuesRows
    group_values: Vec<Box<dyn GroupColumn>>,

    /// reused buffer to store hashes
    hashes_buffer: Vec<u64>,

    /// Random state for creating hashes
    random_state: RandomState,
}

impl VectorizedGroupValuesColumn {
    /// Create a new instance of GroupValuesColumn if supported for the specified schema
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        let map = RawTable::with_capacity(0);
        Ok(Self {
            schema,
            map,
            group_index_lists: Vec::new(),
            index_lists_updates: Vec::new(),
            map_size: 0,
            group_values: vec![],
            hashes_buffer: Default::default(),
            random_state: Default::default(),
            scalarized_indices: Default::default(),
            vectorized_equal_to_row_indices: Default::default(),
            vectorized_equal_to_group_indices: Default::default(),
            vectorized_equal_to_results: Default::default(),
            vectorized_append_row_indices: Default::default(),
        })
    }

    /// Collect vectorized context by checking hash values of `cols` in `map`
    ///
    /// 1. If bucket not found
    ///   - Build and insert the `new inlined group index view`
    ///     and its hash value to `map`
    ///   - Add row index to `vectorized_append_row_indices`
    ///   - Set group index to row in `groups`
    ///
    /// 2. bucket found
    ///   - Add row index to `vectorized_equal_to_row_indices`
    ///   - Check if the `group index view` is `inlined` or `non_inlined`:
    ///     If it is inlined, add to `vectorized_equal_to_group_indices` directly.
    ///     Otherwise get all group indices from `group_index_lists`, and add them.
    ///
    fn collect_vectorized_process_context(
        &mut self,
        batch_hashes: &[u64],
        groups: &mut Vec<usize>,
    ) {
        self.vectorized_append_row_indices.clear();
        self.vectorized_equal_to_row_indices.clear();
        self.vectorized_equal_to_group_indices.clear();

        let mut group_values_len = self.group_values[0].len();
        for (row, &target_hash) in batch_hashes.iter().enumerate() {
            let entry = self.map.get(target_hash, |(exist_hash, _)| {
                // Somewhat surprisingly, this closure can be called even if the
                // hash doesn't match, so check the hash first with an integer
                // comparison first avoid the more expensive comparison with
                // group value. https://github.com/apache/datafusion/pull/11718
                target_hash == *exist_hash
            });

            let Some((_, group_index_view)) = entry else {
                // 1. Bucket not found case
                // Build `new inlined group index view`
                let current_group_idx = group_values_len;
                let group_index_view =
                    GroupIndexView::new_inlined(current_group_idx as u64);

                // Insert the `group index view` and its hash into `map`
                // for hasher function, use precomputed hash value
                self.map.insert_accounted(
                    (target_hash, group_index_view),
                    |(hash, _)| *hash,
                    &mut self.map_size,
                );

                // Add row index to `vectorized_append_row_indices`
                self.vectorized_append_row_indices.push(row);

                // Set group index to row in `groups`
                groups[row] = current_group_idx;

                group_values_len += 1;
                continue;
            };

            // 2. bucket found
            // Check if the `group index view` is `inlined` or `non_inlined`
            if group_index_view.is_non_inlined() {
                // Non-inlined case, the value of view is offset in `group_index_lists`.
                // We use it to get `group_index_list`, and add related `rows` and `group_indices`
                // into `vectorized_equal_to_row_indices` and `vectorized_equal_to_group_indices`.
                let list_offset = group_index_view.value() as usize;
                let group_index_list = &self.group_index_lists[list_offset];
                for &group_index in group_index_list {
                    self.vectorized_equal_to_row_indices.push(row);
                    self.vectorized_equal_to_group_indices.push(group_index);
                }
            } else {
                let group_index = group_index_view.value() as usize;
                self.vectorized_equal_to_row_indices.push(row);
                self.vectorized_equal_to_group_indices.push(group_index);
            }
        }
    }

    /// Perform `vectorized_append`` for `rows` in `vectorized_append_row_indices`
    fn vectorized_append(&mut self, cols: &[ArrayRef]) {
        if self.vectorized_append_row_indices.is_empty() {
            return;
        }

        let iter = self.group_values.iter_mut().zip(cols.iter());
        for (group_column, col) in iter {
            group_column.vectorized_append(col, &self.vectorized_append_row_indices);
        }
    }

    /// Perform `vectorized_equal_to`
    ///
    /// 1. Perform `vectorized_equal_to` for `rows` in `vectorized_equal_to_group_indices`
    ///    and `group_indices` in `vectorized_equal_to_group_indices`.
    ///
    /// 2. Check `equal_to_results`:
    ///
    ///    If found equal to `rows`, set the `group_indices` to `rows` in `groups`.
    ///
    ///    If found not equal to `row`s, just add them to `scalarized_indices`,
    ///    and perform `scalarized_intern` for them after.
    ///    Usually, such `rows` having same hash but different value with `exists rows`
    ///    are very few.
    fn vectorized_equal_to(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) {
        assert_eq!(
            self.vectorized_equal_to_group_indices.len(),
            self.vectorized_equal_to_row_indices.len()
        );

        if self.vectorized_equal_to_group_indices.is_empty() {
            return;
        }

        // 1. Perform `vectorized_equal_to` for `rows` in `vectorized_equal_to_group_indices`
        //    and `group_indices` in `vectorized_equal_to_group_indices`
        let mut equal_to_results = mem::take(&mut self.vectorized_equal_to_results);
        equal_to_results.clear();
        equal_to_results.resize(self.vectorized_equal_to_group_indices.len(), true);

        for (col_idx, group_col) in self.group_values.iter().enumerate() {
            group_col.vectorized_equal_to(
                &self.vectorized_equal_to_group_indices,
                &cols[col_idx],
                &self.vectorized_equal_to_row_indices,
                &mut equal_to_results,
            );
        }

        // 2. Check `equal_to_results`, if found not equal to `row`s, just add them
        //    to `scalarized_indices`, and perform `scalarized_intern` for them after.
        let mut current_row_equal_to_result = false;
        for (idx, &row) in self.vectorized_equal_to_row_indices.iter().enumerate() {
            let equal_to_result = equal_to_results[idx];

            // Equal to case, set the `group_indices` to `rows` in `groups`
            if equal_to_result {
                groups[row] = self.vectorized_equal_to_group_indices[idx];
            }
            current_row_equal_to_result |= equal_to_result;

            // Look forward next one row to check if have checked all results
            // of current row
            let next_row = self
                .vectorized_equal_to_row_indices
                .get(idx + 1)
                .unwrap_or(&usize::MAX);

            // Have checked all results of current row, check the total result
            if row != *next_row {
                // Not equal to case, add `row` to `scalarized_indices`
                if !current_row_equal_to_result {
                    self.scalarized_indices.push(row);
                }

                // Init the total result for checking next row
                current_row_equal_to_result = false;
            }
        }

        self.vectorized_equal_to_results = equal_to_results;
    }

    fn scalarized_equal_to(
        &self,
        group_index_view: &GroupIndexView,
        cols: &[ArrayRef],
        row: usize,
        groups: &mut Vec<usize>,
    ) -> bool {
        // Check if this row exists in `group_values`
        fn check_row_equal(
            array_row: &dyn GroupColumn,
            lhs_row: usize,
            array: &ArrayRef,
            rhs_row: usize,
        ) -> bool {
            array_row.equal_to(lhs_row, array, rhs_row)
        }

        if group_index_view.is_non_inlined() {
            let list_offset = group_index_view.value() as usize;
            let group_index_list = &self.group_index_lists[list_offset];

            for &group_idx in group_index_list {
                let mut check_result = true;
                for (i, group_val) in self.group_values.iter().enumerate() {
                    if !check_row_equal(group_val.as_ref(), group_idx, &cols[i], row) {
                        check_result = false;
                        break;
                    }
                }

                if check_result {
                    groups[row] = group_idx;
                    return true;
                }
            }

            // All groups unmatched, return false result
            false
        } else {
            let group_idx = group_index_view.value() as usize;
            for (i, group_val) in self.group_values.iter().enumerate() {
                if !check_row_equal(group_val.as_ref(), group_idx, &cols[i], row) {
                    return false;
                }
            }

            groups[row] = group_idx;
            true
        }
    }

    fn scalarized_intern(
        &mut self,
        cols: &[ArrayRef],
        batch_hashes: &[u64],
        groups: &mut Vec<usize>,
    ) {
        if self.scalarized_indices.is_empty() {
            return;
        }

        let mut map = mem::take(&mut self.map);

        for &row in &self.scalarized_indices {
            let target_hash = batch_hashes[row];
            let entry = map.get_mut(target_hash, |(exist_hash, group_index_view)| {
                // Somewhat surprisingly, this closure can be called even if the
                // hash doesn't match, so check the hash first with an integer
                // comparison first avoid the more expensive comparison with
                // group value. https://github.com/apache/datafusion/pull/11718
                target_hash == *exist_hash
            });

            // Only `rows` having the same hash value with `exist rows` but different value
            // will be process in `scalarized_intern`.
            // So related `buckets` in `map` is ensured to be `Some`.
            let Some((_, group_index_view)) = entry else {
                unreachable!()
            };

            // Perform scalarized equal to
            if self.scalarized_equal_to(&group_index_view, cols, row, groups) {
                // Found the row actually exists in group values,
                // don't need to create new group for it.
                continue;
            }

            // Insert the `row` to `group_values` before checking `next row`
            let group_idx = self.group_values[0].len();
            let mut checklen = 0;
            for (i, group_value) in self.group_values.iter_mut().enumerate() {
                group_value.append_val(&cols[i], row);
                let len = group_value.len();
                if i == 0 {
                    checklen = len;
                } else {
                    debug_assert_eq!(checklen, len);
                }
            }

            // Check if the `view` is `inlined` or `non-inlined`
            if group_index_view.is_non_inlined() {
                // Non-inlined case, get `group_index_list` from `group_index_lists`,
                // then add the new `group` with the same hash values into it.
                let list_offset = group_index_view.value() as usize;
                let group_index_list = &mut self.group_index_lists[list_offset];
                group_index_list.push(group_idx);
            } else {
                // Inlined case
                let list_offset = self.group_index_lists.len();

                // Create new `group_index_list` including
                // `exist group index` + `new group index`.
                // Add new `group_index_list` into ``group_index_lists`.
                let exist_group_index = group_index_view.value() as usize;
                let new_group_index_list = vec![exist_group_index, group_idx];
                self.group_index_lists.push(new_group_index_list);

                // Update the `group_index_view` to non-inlined
                let new_group_index_view =
                    GroupIndexView::new_non_inlined(list_offset as u64);
                *group_index_view = new_group_index_view;
            }

            groups[row] = group_idx;
        }

        self.map = map;
    }
}

/// instantiates a [`PrimitiveGroupValueBuilder`] and pushes it into $v
///
/// Arguments:
/// `$v`: the vector to push the new builder into
/// `$nullable`: whether the input can contains nulls
/// `$t`: the primitive type of the builder
///
macro_rules! instantiate_primitive {
    ($v:expr, $nullable:expr, $t:ty) => {
        if $nullable {
            let b = PrimitiveGroupValueBuilder::<$t, true>::new();
            $v.push(Box::new(b) as _)
        } else {
            let b = PrimitiveGroupValueBuilder::<$t, false>::new();
            $v.push(Box::new(b) as _)
        }
    };
}

impl GroupValues for VectorizedGroupValuesColumn {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        let n_rows = cols[0].len();

        if self.group_values.is_empty() {
            let mut v = Vec::with_capacity(cols.len());

            for f in self.schema.fields().iter() {
                let nullable = f.is_nullable();
                match f.data_type() {
                    &DataType::Int8 => instantiate_primitive!(v, nullable, Int8Type),
                    &DataType::Int16 => instantiate_primitive!(v, nullable, Int16Type),
                    &DataType::Int32 => instantiate_primitive!(v, nullable, Int32Type),
                    &DataType::Int64 => instantiate_primitive!(v, nullable, Int64Type),
                    &DataType::UInt8 => instantiate_primitive!(v, nullable, UInt8Type),
                    &DataType::UInt16 => instantiate_primitive!(v, nullable, UInt16Type),
                    &DataType::UInt32 => instantiate_primitive!(v, nullable, UInt32Type),
                    &DataType::UInt64 => instantiate_primitive!(v, nullable, UInt64Type),
                    &DataType::Float32 => {
                        instantiate_primitive!(v, nullable, Float32Type)
                    }
                    &DataType::Float64 => {
                        instantiate_primitive!(v, nullable, Float64Type)
                    }
                    &DataType::Date32 => instantiate_primitive!(v, nullable, Date32Type),
                    &DataType::Date64 => instantiate_primitive!(v, nullable, Date64Type),
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
                    &DataType::Utf8View => {
                        let b = ByteViewGroupValueBuilder::<StringViewType>::new();
                        v.push(Box::new(b) as _)
                    }
                    &DataType::BinaryView => {
                        let b = ByteViewGroupValueBuilder::<BinaryViewType>::new();
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

        let mut batch_hashes = mem::take(&mut self.hashes_buffer);
        batch_hashes.clear();
        batch_hashes.resize(n_rows, 0);
        create_hashes(cols, &self.random_state, &mut batch_hashes)?;

        // General steps for one round `vectorized equal_to & append`:
        //   1. Collect vectorized context by checking hash values of `cols` in `map`
        //   2. Perform `vectorized_equal_to`
        //   3. Perform `vectorized_append`
        //   4. Update `current_indices`
        groups.resize(n_rows, usize::MAX);
        self.scalarized_indices.clear();

        // 1. Collect vectorized context by checking hash values of `cols` in `map`
        self.collect_vectorized_process_context(&batch_hashes, groups);

        // 2. Perform `vectorized_append`
        self.vectorized_append(cols);

        // 3. Perform `vectorized_equal_to`
        self.vectorized_equal_to(cols, groups);

        // 4. Update `current_indices`
        self.scalarized_intern(cols, &batch_hashes, groups);

        self.hashes_buffer = batch_hashes;

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
                let new_group_index_lists =
                    Vec::with_capacity(self.group_index_lists.len());
                let old_group_index_lists =
                    std::mem::replace(&mut self.group_index_lists, new_group_index_lists);

                // SAFETY: self.map outlives iterator and is not modified concurrently
                unsafe {
                    for bucket in self.map.iter() {
                        // Check if it is `inlined` or `non-inlined`
                        if bucket.as_ref().1.is_non_inlined() {
                            // Non-inlined case
                            // We take `group_index_list` from `old_group_index_lists`
                            let list_offset = bucket.as_ref().1.value() as usize;
                            let old_group_index_list =
                                &old_group_index_lists[list_offset];

                            let mut new_group_index_list = Vec::new();
                            for &group_index in old_group_index_list {
                                if let Some(remaining) = group_index.checked_sub(n) {
                                    new_group_index_list.push(remaining);
                                }
                            }

                            // The possible results:
                            //   - `new_group_index_list` is empty, we should erase this bucket
                            //   - only one value in `new_group_index_list`, switch the `view` to `inlined`
                            //   - still multiple values in `new_group_index_list`, build and set the new `unlined view`
                            if new_group_index_list.is_empty() {
                                self.map.erase(bucket);
                            } else if new_group_index_list.len() == 1 {
                                let group_index = new_group_index_list.first().unwrap();
                                bucket.as_mut().1 =
                                    GroupIndexView::new_inlined(*group_index as u64);
                            } else {
                                let new_list_offset = self.group_index_lists.len();
                                self.group_index_lists.push(new_group_index_list);
                                bucket.as_mut().1 = GroupIndexView::new_non_inlined(
                                    new_list_offset as u64,
                                );
                            }
                        } else {
                            // Inlined case, we just decrement group index by n
                            let group_index = bucket.as_ref().1.value() as usize;
                            match group_index.checked_sub(n) {
                                // Group index was >= n, shift value down
                                Some(sub) => {
                                    bucket.as_mut().1 =
                                        GroupIndexView::new_inlined(sub as u64)
                                }
                                // Group index was < n, so remove from table
                                None => self.map.erase(bucket),
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
        self.group_index_lists.clear();
        self.index_lists_updates.clear();
        self.scalarized_indices.clear();
        self.vectorized_append_row_indices.clear();
        self.vectorized_equal_to_row_indices.clear();
        self.vectorized_equal_to_group_indices.clear();
        self.vectorized_equal_to_results.clear();
    }
}

/// A [`GroupValues`] that stores multiple columns of group values.
///
///
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
    /// the left to right, each column is stored as [`GroupColumn`].
    ///
    /// Performance tests showed that this design is faster than using the
    /// more general purpose [`GroupValuesRows`]. See the ticket for details:
    /// <https://github.com/apache/datafusion/pull/12269>
    ///
    /// [`GroupValuesRows`]: crate::aggregates::group_values::row::GroupValuesRows
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
}

impl GroupValues for GroupValuesColumn {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        let n_rows = cols[0].len();

        if self.group_values.is_empty() {
            let mut v = Vec::with_capacity(cols.len());

            for f in self.schema.fields().iter() {
                let nullable = f.is_nullable();
                match f.data_type() {
                    &DataType::Int8 => instantiate_primitive!(v, nullable, Int8Type),
                    &DataType::Int16 => instantiate_primitive!(v, nullable, Int16Type),
                    &DataType::Int32 => instantiate_primitive!(v, nullable, Int32Type),
                    &DataType::Int64 => instantiate_primitive!(v, nullable, Int64Type),
                    &DataType::UInt8 => instantiate_primitive!(v, nullable, UInt8Type),
                    &DataType::UInt16 => instantiate_primitive!(v, nullable, UInt16Type),
                    &DataType::UInt32 => instantiate_primitive!(v, nullable, UInt32Type),
                    &DataType::UInt64 => instantiate_primitive!(v, nullable, UInt64Type),
                    &DataType::Float32 => {
                        instantiate_primitive!(v, nullable, Float32Type)
                    }
                    &DataType::Float64 => {
                        instantiate_primitive!(v, nullable, Float64Type)
                    }
                    &DataType::Date32 => instantiate_primitive!(v, nullable, Date32Type),
                    &DataType::Date64 => instantiate_primitive!(v, nullable, Date64Type),
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

/// Returns true if [`GroupValuesColumn`] supported for the specified schema
pub fn supported_schema(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .map(|f| f.data_type())
        .all(supported_type)
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
            | DataType::Utf8View
            | DataType::BinaryView
    )
}
