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

const CHECKING_FLAG_MASK: u64 = 0x8000000000000000;
const SET_CHECKING_FLAG_MASK: u64 = 0x8000000000000000;
const UNSET_CHECKING_FLAG_MASK: u64 = 0x7FFFFFFFFFFFFFFF;

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
struct BucketContext(u64);

impl BucketContext {
    #[inline]
    pub fn is_checking(&self) -> bool {
        (self.0 & CHECKING_FLAG_MASK) > 0
    }

    #[inline]
    pub fn set_checking(&mut self) {
        self.0 |= SET_CHECKING_FLAG_MASK
    }

    #[inline]
    pub fn unset_checking(&mut self) {
        self.0 &= UNSET_CHECKING_FLAG_MASK
    }

    #[inline]
    pub fn group_index(&self) -> u64 {
        self.0 & UNSET_CHECKING_FLAG_MASK
    }
}

/// A [`GroupValues`] that stores multiple columns of group values.
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
    map: RawTable<(u64, BucketContext)>,

    /// The size of `map` in bytes
    map_size: usize,

    /// The lists for group indices with the same hash value
    ///
    /// It is possible that hash value collision exists,
    /// and we will chain the `group indices` with same hash value
    ///
    /// The chained indices is like:
    ///   `latest group index -> older group index -> even older group index -> ...`
    group_index_lists: Vec<usize>,

    index_lists_updates: Vec<(usize, usize)>,

    /// We need multiple rounds to process the `input cols`,
    /// and the rows processing in current round is stored here.
    current_indices: Vec<usize>,

    /// Similar as `current_indices`, but `remaining_indices`
    /// is used to store the rows will be processed in next round.
    remaining_indices: Vec<usize>,

    /// The marked checking buckets in this round
    ///
    /// About the checking flag you can see [`BucketContext`]
    empty_buckets: Vec<Bucket<(u64, BucketContext)>>,

    /// The marked checking buckets in this round
    ///
    /// About the checking flag you can see [`BucketContext`]
    occupied_buckets: Vec<Bucket<(u64, BucketContext)>>,

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

    group_values_len: usize,

    /// reused buffer to store hashes
    hashes_buffer: Vec<u64>,

    /// Random state for creating hashes
    random_state: RandomState,
}

impl GroupValuesColumn {
    /// Create a new instance of GroupValuesColumn if supported for the specified schema
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        let map = RawTable::with_capacity(0);
        let num_cols = schema.fields.len();
        Ok(Self {
            schema,
            map,
            group_index_lists: Vec::new(),
            index_lists_updates: Vec::new(),
            map_size: 0,
            group_values: vec![],
            hashes_buffer: Default::default(),
            random_state: Default::default(),
            current_indices: Default::default(),
            remaining_indices: Default::default(),
            group_values_len: 0,
            empty_buckets: Default::default(),
            occupied_buckets: Default::default(),
            vectorized_equal_to_row_indices: Default::default(),
            vectorized_equal_to_group_indices: Default::default(),
            vectorized_equal_to_results: Default::default(),
            vectorized_append_row_indices: Default::default(),
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
                | DataType::Utf8View
                | DataType::BinaryView
        )
    }

    /// Collect vectorized context by checking hash values of `cols` in `map`
    ///
    /// 1. If bucket not found
    ///   - Insert the `new bucket` build from the `group index`
    ///     and its hash value to `map`
    ///   - Mark this `new bucket` checking, and add it to `checking_buckets`
    ///   - Add row index to `vectorized_append_row_indices`
    ///
    /// 2. bucket found
    ///   - Check if the `bucket` checking, if so add it to `remaining_indices`,
    ///     and just process it in next round, otherwise we continue the process
    ///   - Mark `bucket` checking, and add it to `checking_buckets`
    ///   - Add row index to `vectorized_equal_to_row_indices`
    ///   - Add group indices(from `group_index_lists`) to `vectorized_equal_to_group_indices`
    ///
    fn collect_vectorized_process_context(&mut self, batch_hashes: &[u64]) {
        self.vectorized_append_row_indices.clear();
        self.vectorized_equal_to_row_indices.clear();
        self.vectorized_equal_to_group_indices.clear();
        self.empty_buckets.clear();
        self.occupied_buckets.clear();
        self.remaining_indices.clear();

        self.group_values_len = self.group_values[0].len();
        for &row in self.current_indices.iter() {
            let target_hash = batch_hashes[row];
            let entry = self.map.find(target_hash, |(exist_hash, _)| {
                // Somewhat surprisingly, this closure can be called even if the
                // hash doesn't match, so check the hash first with an integer
                // comparison first avoid the more expensive comparison with
                // group value. https://github.com/apache/datafusion/pull/11718
                target_hash == *exist_hash
            });

            let Some(bucket) = entry else {
                // 1. Bucket not found case
                // Insert the `new bucket` build from the `group index`
                // Mark this `new bucket` checking
                let current_group_idx = self.group_values_len as u64;

                // for hasher function, use precomputed hash value
                let mut bucket_ctx = BucketContext(current_group_idx);
                bucket_ctx.set_checking();
                let bucket = self.map.insert_accounted(
                    (target_hash, bucket_ctx),
                    |(hash, _)| *hash,
                    &mut self.map_size,
                );
                self.empty_buckets.push(bucket);

                // Add row index to `vectorized_append_row_indices`
                self.vectorized_append_row_indices.push(row);

                self.group_values_len += 1;
                continue;
            };

            // 2. bucket found
            // Check if the `bucket` checking, if so add it to `remaining_indices`,
            // and just process it in next round, otherwise we continue the process
            let mut list_next_group_idx = unsafe {
                let (_, bucket_ctx) = bucket.as_mut();

                if bucket_ctx.is_checking() {
                    self.remaining_indices.push(row);
                    continue;
                }

                // Mark `bucket` checking
                bucket_ctx.set_checking();
                bucket_ctx.group_index() as usize + 1
            };

            // Add it to `checking_buckets`
            // Add row index to `vectorized_equal_to_row_indices`
            // Add group indices(from `group_index_lists`) to `vectorized_equal_to_group_indices`
            while list_next_group_idx > 0 {
                self.occupied_buckets.push(bucket.clone());

                let list_group_idx = list_next_group_idx;
                self.vectorized_equal_to_row_indices.push(row);
                self.vectorized_equal_to_group_indices
                    .push(list_group_idx - 1);
                list_next_group_idx = self.group_index_lists[list_group_idx];
            }
        }

        // Reset empty bucket's checking flag
        self.empty_buckets.iter().for_each(|bucket| unsafe {
            let (_, bucket_ctx) = bucket.as_mut();
            bucket_ctx.unset_checking();
        });
    }

    /// Perform `vectorized_equal_to`
    fn vectorized_equal_to(&mut self, cols: &[ArrayRef]) {
        assert_eq!(
            self.vectorized_equal_to_group_indices.len(),
            self.vectorized_equal_to_row_indices.len()
        );

        if self.vectorized_equal_to_group_indices.is_empty() {
            return;
        }

        // Vectorized equal to `cols` and `group columns`
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

        self.vectorized_equal_to_results = equal_to_results;
    }

    /// Perform `vectorized_append`
    ///
    /// 1. Check equal to results, if found a equal row, nothing to do;
    ///   otherwise, we should create a new group for the row:
    ///
    ///     - Modify the related bucket stored in `checking_buckets`,
    ///     - Store updates for `group_index_lists` in `pending_index_lists_updates`
    ///     - Increase the `group_values_len`
    ///
    /// 2. Resize the `group_index_lists`, apply `pending_index_lists_updates` to it
    ///
    /// 3. Perform `vectorized_append`
    ///
    fn vectorized_append(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) {
        let mut index_lists_updates = mem::take(&mut self.index_lists_updates);
        index_lists_updates.clear();
        // Set the default value to usize::MAX, so when we made a mistake,
        // panic will happen rather than
        groups.resize(cols[0].len(), usize::MAX);

        // 1. Check equal to results, if found a equal row, nothing to do;
        //    otherwise, we should create a new group for the row.
        let mut current_row_equal_to_result = false;
        let mut current_match_group_index = None;
        for (idx, &row) in self.vectorized_equal_to_row_indices.iter().enumerate() {
            let equal_to_result = self.vectorized_equal_to_results[idx];
            if equal_to_result {
                current_match_group_index =
                    Some(self.vectorized_equal_to_group_indices[idx]);
            }
            current_row_equal_to_result |= equal_to_result;

            // Look forward next one row and check
            let next_row = self
                .vectorized_equal_to_row_indices
                .get(idx + 1)
                .unwrap_or(&usize::MAX);
            if row != *next_row {
                // If we should create a new group for the row
                // Store update for `group_index_lists`
                // Update related `BucketContext`(set the group index to latest)
                // Increase the `group_values_len`
                if !current_row_equal_to_result {
                    self.vectorized_append_row_indices.push(row);
                    unsafe {
                        let (_, bucket_ctx) = self.occupied_buckets[idx].as_mut();

                        index_lists_updates.push((
                            self.group_values_len + 1,
                            (bucket_ctx.group_index() + 1) as usize,
                        ));

                        *bucket_ctx = BucketContext(self.group_values_len as u64);
                    }

                    self.group_values_len += 1;
                } else {
                    unsafe {
                        let (_, bucket_ctx) = self.occupied_buckets[idx].as_mut();
                        bucket_ctx.unset_checking();
                    }
                    groups[row] = current_match_group_index.unwrap();
                }

                current_match_group_index = None;
                current_row_equal_to_result = false;
            }
        }

        // 2. Resize the `group_index_lists`, apply `pending_index_lists_updates` to it
        self.group_index_lists.resize(self.group_values_len + 1, 0);
        for &(latest_index, prev_index) in index_lists_updates.iter() {
            self.group_index_lists[latest_index] = prev_index;
        }

        // 3. Perform `vectorized_append`
        if self.vectorized_append_row_indices.is_empty() {
            return;
        }

        let group_len_before_appending = self.group_values[0].len();
        let iter = self.group_values.iter_mut().zip(cols.iter());
        for (group_column, col) in iter {
            group_column.vectorized_append(col, &self.vectorized_append_row_indices);
        }
        assert_eq!(
            self.group_values[0].len(),
            self.group_values_len,
            "group_len_before_appending:{}, vectorized_append_row_indices:{}",
            group_len_before_appending,
            self.vectorized_append_row_indices.len(),
        );

        let iter = self
            .vectorized_append_row_indices
            .iter()
            .zip(group_len_before_appending..self.group_values_len);
        for (&row, group_idx) in iter {
            groups[row] = group_idx;
        }

        // Set back `index_lists_updates`.
        self.index_lists_updates = index_lists_updates;
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

        self.map.reserve(n_rows, |(hash, _)| *hash);

        // General steps for one round `vectorized equal_to & append`:
        //   1. Collect vectorized context by checking hash values of `cols` in `map`
        //   2. Perform `vectorized_equal_to`
        //   3. Perform `vectorized_append`
        //   4. Update `current_indices`
        let num_rows = cols[0].len();
        self.current_indices.clear();
        self.current_indices.extend(0..num_rows);
        let mut count = 0;
        while self.current_indices.len() > 0 {
            // 1. Collect vectorized context by checking hash values of `cols` in `map`
            self.collect_vectorized_process_context(&batch_hashes);

            // 2. Perform `vectorized_equal_to`
            self.vectorized_equal_to(cols);

            // 3. Perform `vectorized_append`
            self.vectorized_append(cols, groups);

            // 4. Update `current_indices`
            mem::swap(&mut self.current_indices, &mut self.remaining_indices);
            count += 1;
        }

        dbg!(&count);
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

                // Update `map`
                // SAFETY: self.map outlives iterator and is not modified concurrently
                unsafe {
                    for bucket in self.map.iter() {
                        let group_index = {
                            let (_, bucket_ctx) = bucket.as_ref();
                            debug_assert!(!bucket_ctx.is_checking());
                            bucket_ctx.group_index()
                        };

                        // Decrement group index in map by n
                        match group_index.checked_sub(n as u64) {
                            // Group index was >= n, shift value down
                            Some(sub) => bucket.as_mut().1 = BucketContext(sub),
                            // Group index was < n, so remove from table
                            None => self.map.erase(bucket),
                        }
                    }
                }

                // Update `group_index_lists`
                // Loop and decrement the [n+1..] list nodes
                let start_idx = n + 1;
                let list_len = self.group_index_lists.len();
                for idx in start_idx..list_len {
                    let new_idx = idx - n;

                    let next_idx = self.group_index_lists[idx];
                    let new_next_idx = next_idx.checked_sub(n).unwrap_or(0);

                    self.group_index_lists[new_idx] = new_next_idx;
                }
                self.group_index_lists
                    .resize(self.group_values[0].len() + 1, 0);

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
        self.current_indices.clear();
        self.remaining_indices.clear();
        self.empty_buckets.clear();
        self.occupied_buckets.clear();
        self.vectorized_append_row_indices.clear();
        self.vectorized_equal_to_row_indices.clear();
        self.vectorized_equal_to_group_indices.clear();
        self.vectorized_equal_to_results.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use crate::aggregates::group_values::{column::GroupValuesColumn, GroupValues};

    #[test]
    fn test() {
        // ***************************************************************
        // The test group cols, the schema is `a(Int64) + b(String)`.
        // It should cover following input rows situations:
        //   - a: null + b: null
        //   - a: not null + b: null
        //   - a: null + b: not null
        //   - a: not null + b: not null
        //
        // And it should cover following repeating situations:
        //   - Rows unique
        //   - Rows repeating in two `cols`
        //   - Rows repeating in single `cols`
        // ***************************************************************

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        // // Case 1
        // Cols 1
        let a: ArrayRef = Arc::new(Int64Array::from(vec![
            None,
            Some(42),
            None,
            Some(24),
            Some(4224),
        ]));
        let b: ArrayRef = Arc::new(StringArray::from(vec![
            None,
            None,
            Some("42"),
            Some("24"),
            Some("4224"),
        ]));
        let cols1 = vec![a, b];

        // Cols 2
        let a: ArrayRef = Arc::new(Int64Array::from(vec![
            None,
            Some(42),
            None,
            Some(24),
            Some(2442),
        ]));
        let b: ArrayRef = Arc::new(StringArray::from(vec![
            None,
            None,
            Some("42"),
            Some("24"),
            Some("2442"),
        ]));
        let cols2 = vec![a, b];

        // Cols 3
        let a: ArrayRef = Arc::new(Int64Array::from(vec![
            None,
            Some(42),
            None,
            Some(24),
            None,
            Some(42),
            None,
            Some(24),
            Some(4224),
        ]));
        let b: ArrayRef = Arc::new(StringArray::from(vec![
            None,
            None,
            Some("42"),
            Some("24"),
            None,
            None,
            Some("42"),
            Some("24"),
            Some("4224"),
        ]));
        let cols3 = vec![a, b];

        let mut group_values = GroupValuesColumn::try_new(schema).unwrap();
        let mut groups = Vec::new();
        group_values.intern(&cols1, &mut groups).unwrap();
        group_values.intern(&cols2, &mut groups).unwrap();
        group_values.intern(&cols3, &mut groups).unwrap();
    }
}
