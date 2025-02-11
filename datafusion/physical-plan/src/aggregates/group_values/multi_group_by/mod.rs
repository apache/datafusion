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

//! `GroupValues` implementations for multi group by cases

mod bytes;
mod bytes_view;
mod primitive;

use std::mem::{self, size_of};

use crate::aggregates::group_values::multi_group_by::{
    bytes::ByteGroupValueBuilder, bytes_view::ByteViewGroupValueBuilder,
    primitive::PrimitiveGroupValueBuilder,
};
use crate::aggregates::group_values::GroupValues;
use ahash::RandomState;
use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::compute::cast;
use arrow::datatypes::{
    BinaryViewType, DataType, Date32Type, Date64Type, Decimal128Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Schema, SchemaRef,
    StringViewType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{not_impl_err, DataFusionError, Result};
use datafusion_execution::memory_pool::proxy::{HashTableAllocExt, VecAllocExt};
use datafusion_expr::EmitTo;
use datafusion_physical_expr::binary_map::OutputType;

use hashbrown::hash_table::HashTable;

const NON_INLINED_FLAG: u64 = 0x8000000000000000;
const VALUE_MASK: u64 = 0x7FFFFFFFFFFFFFFF;

/// Trait for storing a single column of group values in [`GroupValuesColumn`]
///
/// Implementations of this trait store an in-progress collection of group values
/// (similar to various builders in Arrow-rs) that allow for quick comparison to
/// incoming rows.
///
/// [`GroupValuesColumn`]: crate::aggregates::group_values::GroupValuesColumn
pub trait GroupColumn: Send + Sync {
    /// Returns equal if the row stored in this builder at `lhs_row` is equal to
    /// the row in `array` at `rhs_row`
    ///
    /// Note that this comparison returns true if both elements are NULL
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool;

    /// Appends the row at `row` in `array` to this builder
    fn append_val(&mut self, array: &ArrayRef, row: usize);

    /// The vectorized version equal to
    ///
    /// When found nth row stored in this builder at `lhs_row`
    /// is equal to the row in `array` at `rhs_row`,
    /// it will record the `true` result at the corresponding
    /// position in `equal_to_results`.
    ///
    /// And if found nth result in `equal_to_results` is already
    /// `false`, the check for nth row will be skipped.
    ///
    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    );

    /// The vectorized version `append_val`
    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]);

    /// Returns the number of rows stored in this builder
    fn len(&self) -> usize;

    /// Returns the number of bytes used by this [`GroupColumn`]
    fn size(&self) -> usize;

    /// Builds a new array from all of the stored rows
    fn build(self: Box<Self>) -> ArrayRef;

    /// Builds a new array from the first `n` stored rows, shifting the
    /// remaining rows to the start of the builder
    fn take_n(&mut self, n: usize) -> ArrayRef;
}

/// Determines if the nullability of the existing and new input array can be used
/// to short-circuit the comparison of the two values.
///
/// Returns `Some(result)` if the result of the comparison can be determined
/// from the nullness of the two values, and `None` if the comparison must be
/// done on the values themselves.
pub fn nulls_equal_to(lhs_null: bool, rhs_null: bool) -> Option<bool> {
    match (lhs_null, rhs_null) {
        (true, true) => Some(true),
        (false, true) | (true, false) => Some(false),
        _ => None,
    }
}

/// The view of indices pointing to the actual values in `GroupValues`
///
/// If only single `group index` represented by view,
/// value of view is just the `group index`, and we call it a `inlined view`.
///
/// If multiple `group indices` represented by view,
/// value of view is the actually the index pointing to `group indices`,
/// and we call it `non-inlined view`.
///
/// The view(a u64) format is like:
///   +---------------------+---------------------------------------------+
///   | inlined flag(1bit)  | group index / index to group indices(63bit) |
///   +---------------------+---------------------------------------------+
///
/// `inlined flag`: 1 represents `non-inlined`, and 0 represents `inlined`
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// A [`GroupValues`] that stores multiple columns of group values,
/// and supports vectorized operators for them
///
pub struct GroupValuesColumn<const STREAMING: bool> {
    /// The output schema
    schema: SchemaRef,

    /// Logically maps group values to a group_index in
    /// [`Self::group_values`] and in each accumulator
    ///
    /// It is a `hashtable` based on `hashbrown`.
    ///
    /// Key and value in the `hashtable`:
    ///   - The `key` is `hash value(u64)` of the `group value`
    ///   - The `value` is the `group values` with the same `hash value`
    ///
    /// We don't really store the actual `group values` in `hashtable`,
    /// instead we store the `group indices` pointing to values in `GroupValues`.
    /// And we use [`GroupIndexView`] to represent such `group indices` in table.
    ///
    ///
    map: HashTable<(u64, GroupIndexView)>,

    /// The size of `map` in bytes
    map_size: usize,

    /// The lists for group indices with the same hash value
    ///
    /// It is possible that hash value collision exists,
    /// and we will chain the `group indices` with same hash value
    ///
    /// The chained indices is like:
    ///   `latest group index -> older group index -> even older group index -> ...`
    ///
    group_index_lists: Vec<Vec<usize>>,

    /// When emitting first n, we need to decrease/erase group indices in
    /// `map` and `group_index_lists`.
    ///
    /// This buffer is used to temporarily store the remaining group indices in
    /// a specific list in `group_index_lists`.
    emit_group_index_list_buffer: Vec<usize>,

    /// Buffers for `vectorized_append` and `vectorized_equal_to`
    vectorized_operation_buffers: VectorizedOperationBuffers,

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

/// Buffers to store intermediate results in `vectorized_append`
/// and `vectorized_equal_to`, for reducing memory allocation
#[derive(Default)]
struct VectorizedOperationBuffers {
    /// The `vectorized append` row indices buffer
    append_row_indices: Vec<usize>,

    /// The `vectorized_equal_to` row indices buffer
    equal_to_row_indices: Vec<usize>,

    /// The `vectorized_equal_to` group indices buffer
    equal_to_group_indices: Vec<usize>,

    /// The `vectorized_equal_to` result buffer
    equal_to_results: Vec<bool>,

    /// The buffer for storing row indices found not equal to
    /// exist groups in `group_values` in `vectorized_equal_to`.
    /// We will perform `scalarized_intern` for such rows.
    remaining_row_indices: Vec<usize>,
}

impl VectorizedOperationBuffers {
    fn clear(&mut self) {
        self.append_row_indices.clear();
        self.equal_to_row_indices.clear();
        self.equal_to_group_indices.clear();
        self.equal_to_results.clear();
        self.remaining_row_indices.clear();
    }
}

impl<const STREAMING: bool> GroupValuesColumn<STREAMING> {
    // ========================================================================
    // Initialization functions
    // ========================================================================

    /// Create a new instance of GroupValuesColumn if supported for the specified schema
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        let map = HashTable::with_capacity(0);
        Ok(Self {
            schema,
            map,
            group_index_lists: Vec::new(),
            emit_group_index_list_buffer: Vec::new(),
            vectorized_operation_buffers: VectorizedOperationBuffers::default(),
            map_size: 0,
            group_values: vec![],
            hashes_buffer: Default::default(),
            random_state: Default::default(),
        })
    }

    // ========================================================================
    // Scalarized intern
    // ========================================================================

    /// Scalarized intern
    ///
    /// This is used only for `streaming aggregation`, because `streaming aggregation`
    /// depends on the order between `input rows` and their corresponding `group indices`.
    ///
    /// For example, assuming `input rows` in `cols` with 4 new rows
    /// (not equal to `exist rows` in `group_values`, and need to create
    /// new groups for them):
    ///
    /// ```text
    ///   row1 (hash collision with the exist rows)
    ///   row2
    ///   row3 (hash collision with the exist rows)
    ///   row4
    /// ```
    ///
    /// # In `scalarized_intern`, their `group indices` will be
    ///
    /// ```text
    ///   row1 --> 0
    ///   row2 --> 1
    ///   row3 --> 2
    ///   row4 --> 3
    /// ```
    ///
    /// `Group indices` order agrees with their input order, and the `streaming aggregation`
    /// depends on this.
    ///
    /// # However In `vectorized_intern`, their `group indices` will be
    ///
    /// ```text
    ///   row1 --> 2
    ///   row2 --> 0
    ///   row3 --> 3
    ///   row4 --> 1
    /// ```
    ///
    /// `Group indices` order are against with their input order, and this will lead to error
    /// in `streaming aggregation`.
    ///
    fn scalarized_intern(
        &mut self,
        cols: &[ArrayRef],
        groups: &mut Vec<usize>,
    ) -> Result<()> {
        let n_rows = cols[0].len();

        // tracks to which group each of the input rows belongs
        groups.clear();

        // 1.1 Calculate the group keys for the group values
        let batch_hashes = &mut self.hashes_buffer;
        batch_hashes.clear();
        batch_hashes.resize(n_rows, 0);
        create_hashes(cols, &self.random_state, batch_hashes)?;

        for (row, &target_hash) in batch_hashes.iter().enumerate() {
            let entry = self
                .map
                .find_mut(target_hash, |(exist_hash, group_idx_view)| {
                    // It is ensured to be inlined in `scalarized_intern`
                    debug_assert!(!group_idx_view.is_non_inlined());

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
                        if !check_row_equal(
                            group_val.as_ref(),
                            group_idx_view.value() as usize,
                            &cols[i],
                            row,
                        ) {
                            return false;
                        }
                    }

                    true
                });

            let group_idx = match entry {
                // Existing group_index for this group value
                Some((_hash, group_idx_view)) => group_idx_view.value() as usize,
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
                        (target_hash, GroupIndexView::new_inlined(group_idx as u64)),
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

    // ========================================================================
    // Vectorized intern
    // ========================================================================

    /// Vectorized intern
    ///
    /// This is used in `non-streaming aggregation` without requiring the order between
    /// rows in `cols` and corresponding groups in `group_values`.
    ///
    /// The vectorized approach can offer higher performance for avoiding row by row
    /// downcast for `cols` and being able to implement even more optimizations(like simd).
    ///
    fn vectorized_intern(
        &mut self,
        cols: &[ArrayRef],
        groups: &mut Vec<usize>,
    ) -> Result<()> {
        let n_rows = cols[0].len();

        // tracks to which group each of the input rows belongs
        groups.clear();
        groups.resize(n_rows, usize::MAX);

        let mut batch_hashes = mem::take(&mut self.hashes_buffer);
        batch_hashes.clear();
        batch_hashes.resize(n_rows, 0);
        create_hashes(cols, &self.random_state, &mut batch_hashes)?;

        // General steps for one round `vectorized equal_to & append`:
        //   1. Collect vectorized context by checking hash values of `cols` in `map`,
        //      mainly fill `vectorized_append_row_indices`, `vectorized_equal_to_row_indices`
        //      and `vectorized_equal_to_group_indices`
        //
        //   2. Perform `vectorized_append` for `vectorized_append_row_indices`.
        //     `vectorized_append` must be performed before `vectorized_equal_to`,
        //      because some `group indices` in `vectorized_equal_to_group_indices`
        //      maybe still point to no actual values in `group_values` before performing append.
        //
        //   3. Perform `vectorized_equal_to` for `vectorized_equal_to_row_indices`
        //      and `vectorized_equal_to_group_indices`. If found some rows in input `cols`
        //      not equal to `exist rows` in `group_values`, place them in `remaining_row_indices`
        //      and perform `scalarized_intern_remaining` for them similar as `scalarized_intern`
        //      after.
        //
        //   4. Perform `scalarized_intern_remaining` for rows mentioned above, about in what situation
        //      we will process this can see the comments of `scalarized_intern_remaining`.
        //

        // 1. Collect vectorized context by checking hash values of `cols` in `map`
        self.collect_vectorized_process_context(&batch_hashes, groups);

        // 2. Perform `vectorized_append`
        self.vectorized_append(cols);

        // 3. Perform `vectorized_equal_to`
        self.vectorized_equal_to(cols, groups);

        // 4. Perform scalarized inter for remaining rows
        // (about remaining rows, can see comments for `remaining_row_indices`)
        self.scalarized_intern_remaining(cols, &batch_hashes, groups);

        self.hashes_buffer = batch_hashes;

        Ok(())
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
        groups: &mut [usize],
    ) {
        self.vectorized_operation_buffers.append_row_indices.clear();
        self.vectorized_operation_buffers
            .equal_to_row_indices
            .clear();
        self.vectorized_operation_buffers
            .equal_to_group_indices
            .clear();

        let mut group_values_len = self.group_values[0].len();
        for (row, &target_hash) in batch_hashes.iter().enumerate() {
            let entry = self
                .map
                .find(target_hash, |(exist_hash, _)| target_hash == *exist_hash);

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
                self.vectorized_operation_buffers
                    .append_row_indices
                    .push(row);

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
                    self.vectorized_operation_buffers
                        .equal_to_row_indices
                        .push(row);
                    self.vectorized_operation_buffers
                        .equal_to_group_indices
                        .push(group_index);
                }
            } else {
                let group_index = group_index_view.value() as usize;
                self.vectorized_operation_buffers
                    .equal_to_row_indices
                    .push(row);
                self.vectorized_operation_buffers
                    .equal_to_group_indices
                    .push(group_index);
            }
        }
    }

    /// Perform `vectorized_append`` for `rows` in `vectorized_append_row_indices`
    fn vectorized_append(&mut self, cols: &[ArrayRef]) {
        if self
            .vectorized_operation_buffers
            .append_row_indices
            .is_empty()
        {
            return;
        }

        let iter = self.group_values.iter_mut().zip(cols.iter());
        for (group_column, col) in iter {
            group_column.vectorized_append(
                col,
                &self.vectorized_operation_buffers.append_row_indices,
            );
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
    fn vectorized_equal_to(&mut self, cols: &[ArrayRef], groups: &mut [usize]) {
        assert_eq!(
            self.vectorized_operation_buffers
                .equal_to_group_indices
                .len(),
            self.vectorized_operation_buffers.equal_to_row_indices.len()
        );

        self.vectorized_operation_buffers
            .remaining_row_indices
            .clear();

        if self
            .vectorized_operation_buffers
            .equal_to_group_indices
            .is_empty()
        {
            return;
        }

        // 1. Perform `vectorized_equal_to` for `rows` in `vectorized_equal_to_group_indices`
        //    and `group_indices` in `vectorized_equal_to_group_indices`
        let mut equal_to_results =
            mem::take(&mut self.vectorized_operation_buffers.equal_to_results);
        equal_to_results.clear();
        equal_to_results.resize(
            self.vectorized_operation_buffers
                .equal_to_group_indices
                .len(),
            true,
        );

        for (col_idx, group_col) in self.group_values.iter().enumerate() {
            group_col.vectorized_equal_to(
                &self.vectorized_operation_buffers.equal_to_group_indices,
                &cols[col_idx],
                &self.vectorized_operation_buffers.equal_to_row_indices,
                &mut equal_to_results,
            );
        }

        // 2. Check `equal_to_results`, if found not equal to `row`s, just add them
        //    to `scalarized_indices`, and perform `scalarized_intern` for them after.
        let mut current_row_equal_to_result = false;
        for (idx, &row) in self
            .vectorized_operation_buffers
            .equal_to_row_indices
            .iter()
            .enumerate()
        {
            let equal_to_result = equal_to_results[idx];

            // Equal to case, set the `group_indices` to `rows` in `groups`
            if equal_to_result {
                groups[row] =
                    self.vectorized_operation_buffers.equal_to_group_indices[idx];
            }
            current_row_equal_to_result |= equal_to_result;

            // Look forward next one row to check if have checked all results
            // of current row
            let next_row = self
                .vectorized_operation_buffers
                .equal_to_row_indices
                .get(idx + 1)
                .unwrap_or(&usize::MAX);

            // Have checked all results of current row, check the total result
            if row != *next_row {
                // Not equal to case, add `row` to `scalarized_indices`
                if !current_row_equal_to_result {
                    self.vectorized_operation_buffers
                        .remaining_row_indices
                        .push(row);
                }

                // Init the total result for checking next row
                current_row_equal_to_result = false;
            }
        }

        self.vectorized_operation_buffers.equal_to_results = equal_to_results;
    }

    /// It is possible that some `input rows` have the same
    /// hash values with the `exist rows`, but have the different
    /// actual values the exists.
    ///
    /// We can found them in `vectorized_equal_to`, and put them
    /// into `scalarized_indices`. And for these `input rows`,
    /// we will perform the `scalarized_intern` similar as what in
    /// [`GroupValuesColumn`].
    ///
    /// This design can make the process simple and still efficient enough:
    ///
    /// # About making the process simple
    ///
    /// Some corner cases become really easy to solve, like following cases:
    ///
    /// ```text
    ///   input row1 (same hash value with exist rows, but value different)
    ///   input row1
    ///   ...
    ///   input row1
    /// ```
    ///
    /// After performing `vectorized_equal_to`, we will found multiple `input rows`
    /// not equal to the `exist rows`. However such `input rows` are repeated, only
    /// one new group should be create for them.
    ///
    /// If we don't fallback to `scalarized_intern`, it is really hard for us to
    /// distinguish the such `repeated rows` in `input rows`. And if we just fallback,
    /// it is really easy to solve, and the performance is at least not worse than origin.
    ///
    /// # About performance
    ///
    /// The hash collision may be not frequent, so the fallback will indeed hardly happen.
    /// In most situations, `scalarized_indices` will found to be empty after finishing to
    /// preform `vectorized_equal_to`.
    ///
    fn scalarized_intern_remaining(
        &mut self,
        cols: &[ArrayRef],
        batch_hashes: &[u64],
        groups: &mut [usize],
    ) {
        if self
            .vectorized_operation_buffers
            .remaining_row_indices
            .is_empty()
        {
            return;
        }

        let mut map = mem::take(&mut self.map);

        for &row in &self.vectorized_operation_buffers.remaining_row_indices {
            let target_hash = batch_hashes[row];
            let entry = map.find_mut(target_hash, |(exist_hash, _)| {
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
            if self.scalarized_equal_to_remaining(group_index_view, cols, row, groups) {
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

    fn scalarized_equal_to_remaining(
        &self,
        group_index_view: &GroupIndexView,
        cols: &[ArrayRef],
        row: usize,
        groups: &mut [usize],
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

    /// Return group indices of the hash, also if its `group_index_view` is non-inlined
    #[cfg(test)]
    fn get_indices_by_hash(&self, hash: u64) -> Option<(Vec<usize>, GroupIndexView)> {
        let entry = self.map.find(hash, |(exist_hash, _)| hash == *exist_hash);

        match entry {
            Some((_, group_index_view)) => {
                if group_index_view.is_non_inlined() {
                    let list_offset = group_index_view.value() as usize;
                    Some((
                        self.group_index_lists[list_offset].clone(),
                        *group_index_view,
                    ))
                } else {
                    let group_index = group_index_view.value() as usize;
                    Some((vec![group_index], *group_index_view))
                }
            }
            None => None,
        }
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
    ($v:expr, $nullable:expr, $t:ty, $data_type:ident) => {
        if $nullable {
            let b = PrimitiveGroupValueBuilder::<$t, true>::new($data_type.to_owned());
            $v.push(Box::new(b) as _)
        } else {
            let b = PrimitiveGroupValueBuilder::<$t, false>::new($data_type.to_owned());
            $v.push(Box::new(b) as _)
        }
    };
}

impl<const STREAMING: bool> GroupValues for GroupValuesColumn<STREAMING> {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        if self.group_values.is_empty() {
            let mut v = Vec::with_capacity(cols.len());

            for f in self.schema.fields().iter() {
                let nullable = f.is_nullable();
                let data_type = f.data_type();
                match data_type {
                    &DataType::Int8 => {
                        instantiate_primitive!(v, nullable, Int8Type, data_type)
                    }
                    &DataType::Int16 => {
                        instantiate_primitive!(v, nullable, Int16Type, data_type)
                    }
                    &DataType::Int32 => {
                        instantiate_primitive!(v, nullable, Int32Type, data_type)
                    }
                    &DataType::Int64 => {
                        instantiate_primitive!(v, nullable, Int64Type, data_type)
                    }
                    &DataType::UInt8 => {
                        instantiate_primitive!(v, nullable, UInt8Type, data_type)
                    }
                    &DataType::UInt16 => {
                        instantiate_primitive!(v, nullable, UInt16Type, data_type)
                    }
                    &DataType::UInt32 => {
                        instantiate_primitive!(v, nullable, UInt32Type, data_type)
                    }
                    &DataType::UInt64 => {
                        instantiate_primitive!(v, nullable, UInt64Type, data_type)
                    }
                    &DataType::Float32 => {
                        instantiate_primitive!(v, nullable, Float32Type, data_type)
                    }
                    &DataType::Float64 => {
                        instantiate_primitive!(v, nullable, Float64Type, data_type)
                    }
                    &DataType::Date32 => {
                        instantiate_primitive!(v, nullable, Date32Type, data_type)
                    }
                    &DataType::Date64 => {
                        instantiate_primitive!(v, nullable, Date64Type, data_type)
                    }
                    &DataType::Time32(t) => match t {
                        TimeUnit::Second => {
                            instantiate_primitive!(
                                v,
                                nullable,
                                Time32SecondType,
                                data_type
                            )
                        }
                        TimeUnit::Millisecond => {
                            instantiate_primitive!(
                                v,
                                nullable,
                                Time32MillisecondType,
                                data_type
                            )
                        }
                        _ => {}
                    },
                    &DataType::Time64(t) => match t {
                        TimeUnit::Microsecond => {
                            instantiate_primitive!(
                                v,
                                nullable,
                                Time64MicrosecondType,
                                data_type
                            )
                        }
                        TimeUnit::Nanosecond => {
                            instantiate_primitive!(
                                v,
                                nullable,
                                Time64NanosecondType,
                                data_type
                            )
                        }
                        _ => {}
                    },
                    &DataType::Timestamp(t, _) => match t {
                        TimeUnit::Second => {
                            instantiate_primitive!(
                                v,
                                nullable,
                                TimestampSecondType,
                                data_type
                            )
                        }
                        TimeUnit::Millisecond => {
                            instantiate_primitive!(
                                v,
                                nullable,
                                TimestampMillisecondType,
                                data_type
                            )
                        }
                        TimeUnit::Microsecond => {
                            instantiate_primitive!(
                                v,
                                nullable,
                                TimestampMicrosecondType,
                                data_type
                            )
                        }
                        TimeUnit::Nanosecond => {
                            instantiate_primitive!(
                                v,
                                nullable,
                                TimestampNanosecondType,
                                data_type
                            )
                        }
                    },
                    &DataType::Decimal128(_, _) => {
                        instantiate_primitive! {
                            v,
                            nullable,
                            Decimal128Type,
                            data_type
                        }
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

        if !STREAMING {
            self.vectorized_intern(cols, groups)
        } else {
            self.scalarized_intern(cols, groups)
        }
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
                let group_values = mem::take(&mut self.group_values);
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
                let mut next_new_list_offset = 0;

                self.map.retain(|(_exist_hash, group_idx_view)| {
                    // In non-streaming case, we need to check if the `group index view`
                    // is `inlined` or `non-inlined`
                    if !STREAMING && group_idx_view.is_non_inlined() {
                        // Non-inlined case
                        // We take `group_index_list` from `old_group_index_lists`

                        // list_offset is incrementally
                        self.emit_group_index_list_buffer.clear();
                        let list_offset = group_idx_view.value() as usize;
                        for group_index in self.group_index_lists[list_offset].iter() {
                            if let Some(remaining) = group_index.checked_sub(n) {
                                self.emit_group_index_list_buffer.push(remaining);
                            }
                        }

                        // The possible results:
                        //   - `new_group_index_list` is empty, we should erase this bucket
                        //   - only one value in `new_group_index_list`, switch the `view` to `inlined`
                        //   - still multiple values in `new_group_index_list`, build and set the new `unlined view`
                        if self.emit_group_index_list_buffer.is_empty() {
                            false
                        } else if self.emit_group_index_list_buffer.len() == 1 {
                            let group_index =
                                self.emit_group_index_list_buffer.first().unwrap();
                            *group_idx_view =
                                GroupIndexView::new_inlined(*group_index as u64);
                            true
                        } else {
                            let group_index_list =
                                &mut self.group_index_lists[next_new_list_offset];
                            group_index_list.clear();
                            group_index_list
                                .extend(self.emit_group_index_list_buffer.iter());
                            *group_idx_view = GroupIndexView::new_non_inlined(
                                next_new_list_offset as u64,
                            );
                            next_new_list_offset += 1;
                            true
                        }
                    } else {
                        // In `streaming case`, the `group index view` is ensured to be `inlined`
                        debug_assert!(!group_idx_view.is_non_inlined());

                        // Inlined case, we just decrement group index by n)
                        let group_index = group_idx_view.value() as usize;
                        match group_index.checked_sub(n) {
                            // Group index was >= n, shift value down
                            Some(sub) => {
                                *group_idx_view = GroupIndexView::new_inlined(sub as u64);
                                true
                            }
                            // Group index was < n, so remove from table
                            None => false,
                        }
                    }
                });

                if !STREAMING {
                    self.group_index_lists.truncate(next_new_list_offset);
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
        self.map_size = self.map.capacity() * size_of::<(u64, usize)>();
        self.hashes_buffer.clear();
        self.hashes_buffer.shrink_to(count);

        // Such structures are only used in `non-streaming` case
        if !STREAMING {
            self.group_index_lists.clear();
            self.emit_group_index_list_buffer.clear();
            self.vectorized_operation_buffers.clear();
        }
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
/// [`GroupColumn`] for the data type, instantiated in [`GroupValuesColumn::intern`]
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
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Timestamp(_, _)
            | DataType::Utf8View
            | DataType::BinaryView
    )
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::{compute::concat_batches, util::pretty::pretty_format_batches};
    use datafusion_common::utils::proxy::HashTableAllocExt;
    use datafusion_expr::EmitTo;

    use crate::aggregates::group_values::{
        multi_group_by::GroupValuesColumn, GroupValues,
    };

    use super::GroupIndexView;

    #[test]
    fn test_intern_for_vectorized_group_values() {
        let data_set = VectorizedTestDataSet::new();
        let mut group_values =
            GroupValuesColumn::<false>::try_new(data_set.schema()).unwrap();

        data_set.load_to_group_values(&mut group_values);
        let actual_batch = group_values.emit(EmitTo::All).unwrap();
        let actual_batch = RecordBatch::try_new(data_set.schema(), actual_batch).unwrap();

        check_result(&actual_batch, &data_set.expected_batch);
    }

    #[test]
    fn test_emit_first_n_for_vectorized_group_values() {
        let data_set = VectorizedTestDataSet::new();
        let mut group_values =
            GroupValuesColumn::<false>::try_new(data_set.schema()).unwrap();

        // 1~num_rows times to emit the groups
        let num_rows = data_set.expected_batch.num_rows();
        let schema = data_set.schema();
        for times_to_take in 1..=num_rows {
            // Write data after emitting
            data_set.load_to_group_values(&mut group_values);

            // Emit `times_to_take` times, collect and concat the sub-results to total result,
            // then check it
            let suggest_num_emit = data_set.expected_batch.num_rows() / times_to_take;
            let mut num_remaining_rows = num_rows;
            let mut actual_sub_batches = Vec::new();

            for nth_time in 0..times_to_take {
                let num_emit = if nth_time == times_to_take - 1 {
                    num_remaining_rows
                } else {
                    suggest_num_emit
                };

                let sub_batch = group_values.emit(EmitTo::First(num_emit)).unwrap();
                let sub_batch =
                    RecordBatch::try_new(Arc::clone(&schema), sub_batch).unwrap();
                actual_sub_batches.push(sub_batch);

                num_remaining_rows -= num_emit;
            }
            assert!(num_remaining_rows == 0);

            let actual_batch = concat_batches(&schema, &actual_sub_batches).unwrap();
            check_result(&actual_batch, &data_set.expected_batch);
        }
    }

    #[test]
    fn test_hashtable_modifying_in_emit_first_n() {
        // Situations should be covered:
        //   1. Erase inlined group index view
        //   2. Erase whole non-inlined group index view
        //   3. Erase + decrease group indices in non-inlined group index view
        //      + view still non-inlined after decreasing
        //   4. Erase + decrease group indices in non-inlined group index view
        //      + view switch to inlined after decreasing
        //   5. Only decrease group index in inlined group index view
        //   6. Only decrease group indices in non-inlined group index view
        //   7. Erase all things

        let field = Field::new_list_field(DataType::Int32, true);
        let schema = Arc::new(Schema::new_with_metadata(vec![field], HashMap::new()));
        let mut group_values = GroupValuesColumn::<false>::try_new(schema).unwrap();

        // Insert group index views and check if success to insert
        insert_inline_group_index_view(&mut group_values, 0, 0);
        insert_non_inline_group_index_view(&mut group_values, 1, vec![1, 2]);
        insert_non_inline_group_index_view(&mut group_values, 2, vec![3, 4, 5]);
        insert_inline_group_index_view(&mut group_values, 3, 6);
        insert_non_inline_group_index_view(&mut group_values, 4, vec![7, 8]);
        insert_non_inline_group_index_view(&mut group_values, 5, vec![9, 10, 11]);

        assert_eq!(
            group_values.get_indices_by_hash(0).unwrap(),
            (vec![0], GroupIndexView::new_inlined(0))
        );
        assert_eq!(
            group_values.get_indices_by_hash(1).unwrap(),
            (vec![1, 2], GroupIndexView::new_non_inlined(0))
        );
        assert_eq!(
            group_values.get_indices_by_hash(2).unwrap(),
            (vec![3, 4, 5], GroupIndexView::new_non_inlined(1))
        );
        assert_eq!(
            group_values.get_indices_by_hash(3).unwrap(),
            (vec![6], GroupIndexView::new_inlined(6))
        );
        assert_eq!(
            group_values.get_indices_by_hash(4).unwrap(),
            (vec![7, 8], GroupIndexView::new_non_inlined(2))
        );
        assert_eq!(
            group_values.get_indices_by_hash(5).unwrap(),
            (vec![9, 10, 11], GroupIndexView::new_non_inlined(3))
        );
        assert_eq!(group_values.map.len(), 6);

        // Emit first 4 to test cases 1~3, 5~6
        let _ = group_values.emit(EmitTo::First(4)).unwrap();
        assert!(group_values.get_indices_by_hash(0).is_none());
        assert!(group_values.get_indices_by_hash(1).is_none());
        assert_eq!(
            group_values.get_indices_by_hash(2).unwrap(),
            (vec![0, 1], GroupIndexView::new_non_inlined(0))
        );
        assert_eq!(
            group_values.get_indices_by_hash(3).unwrap(),
            (vec![2], GroupIndexView::new_inlined(2))
        );
        assert_eq!(
            group_values.get_indices_by_hash(4).unwrap(),
            (vec![3, 4], GroupIndexView::new_non_inlined(1))
        );
        assert_eq!(
            group_values.get_indices_by_hash(5).unwrap(),
            (vec![5, 6, 7], GroupIndexView::new_non_inlined(2))
        );
        assert_eq!(group_values.map.len(), 4);

        // Emit first 1 to test case 4, and cases 5~6 again
        let _ = group_values.emit(EmitTo::First(1)).unwrap();
        assert_eq!(
            group_values.get_indices_by_hash(2).unwrap(),
            (vec![0], GroupIndexView::new_inlined(0))
        );
        assert_eq!(
            group_values.get_indices_by_hash(3).unwrap(),
            (vec![1], GroupIndexView::new_inlined(1))
        );
        assert_eq!(
            group_values.get_indices_by_hash(4).unwrap(),
            (vec![2, 3], GroupIndexView::new_non_inlined(0))
        );
        assert_eq!(
            group_values.get_indices_by_hash(5).unwrap(),
            (vec![4, 5, 6], GroupIndexView::new_non_inlined(1))
        );
        assert_eq!(group_values.map.len(), 4);

        // Emit first 5 to test cases 1~3 again
        let _ = group_values.emit(EmitTo::First(5)).unwrap();
        assert_eq!(
            group_values.get_indices_by_hash(5).unwrap(),
            (vec![0, 1], GroupIndexView::new_non_inlined(0))
        );
        assert_eq!(group_values.map.len(), 1);

        // Emit first 1 to test cases 4 again
        let _ = group_values.emit(EmitTo::First(1)).unwrap();
        assert_eq!(
            group_values.get_indices_by_hash(5).unwrap(),
            (vec![0], GroupIndexView::new_inlined(0))
        );
        assert_eq!(group_values.map.len(), 1);

        // Emit first 1 to test cases 7
        let _ = group_values.emit(EmitTo::First(1)).unwrap();
        assert!(group_values.map.is_empty());
    }

    /// Test data set for [`GroupValuesColumn::vectorized_intern`]
    ///
    /// Define the test data and support loading them into test [`GroupValuesColumn::vectorized_intern`]
    ///
    /// The covering situations:
    ///
    /// Array type:
    ///   - Primitive array
    ///   - String(byte) array
    ///   - String view(byte view) array
    ///
    /// Repeation and nullability in single batch:
    ///   - All not null rows
    ///   - Mixed null + not null rows
    ///   - All null rows
    ///   - All not null rows(repeated)
    ///   - Null + not null rows(repeated)
    ///   - All not null rows(repeated)
    ///
    /// If group exists in `map`:
    ///   - Group exists in inlined group view
    ///   - Group exists in non-inlined group view
    ///   - Group not exist + bucket not found in `map`
    ///   - Group not exist + not equal to inlined group view(tested in hash collision)
    ///   - Group not exist + not equal to non-inlined group view(tested in hash collision)
    ///
    struct VectorizedTestDataSet {
        test_batches: Vec<Vec<ArrayRef>>,
        expected_batch: RecordBatch,
    }

    impl VectorizedTestDataSet {
        fn new() -> Self {
            // Intern batch 1
            let col1 = Int64Array::from(vec![
                // Repeated rows in batch
                Some(42),   // all not nulls + repeated rows + exist in map case
                None,       // mixed + repeated rows + exist in map case
                None,       // mixed + repeated rows + not exist in map case
                Some(1142), // mixed + repeated rows + not exist in map case
                None,       // all nulls + repeated rows + exist in map case
                Some(42),
                None,
                None,
                Some(1142),
                None,
                // Unique rows in batch
                Some(4211), // all not nulls + unique rows + exist in map case
                None,       // mixed + unique rows + exist in map case
                None,       // mixed + unique rows + not exist in map case
                Some(4212), // mixed + unique rows + not exist in map case
            ]);

            let col2 = StringArray::from(vec![
                // Repeated rows in batch
                Some("string1"), // all not nulls + repeated rows + exist in map case
                None,            // mixed + repeated rows + exist in map case
                Some("string2"), // mixed + repeated rows + not exist in map case
                None,            // mixed + repeated rows + not exist in map case
                None,            // all nulls + repeated rows + exist in map case
                Some("string1"),
                None,
                Some("string2"),
                None,
                None,
                // Unique rows in batch
                Some("string3"), // all not nulls + unique rows + exist in map case
                None,            // mixed + unique rows + exist in map case
                Some("string4"), // mixed + unique rows + not exist in map case
                None,            // mixed + unique rows + not exist in map case
            ]);

            let col3 = StringViewArray::from(vec![
                // Repeated rows in batch
                Some("stringview1"), // all not nulls + repeated rows + exist in map case
                Some("stringview2"), // mixed + repeated rows + exist in map case
                None,                // mixed + repeated rows + not exist in map case
                None,                // mixed + repeated rows + not exist in map case
                None,                // all nulls + repeated rows + exist in map case
                Some("stringview1"),
                Some("stringview2"),
                None,
                None,
                None,
                // Unique rows in batch
                Some("stringview3"), // all not nulls + unique rows + exist in map case
                Some("stringview4"), // mixed + unique rows + exist in map case
                None,                // mixed + unique rows + not exist in map case
                None,                // mixed + unique rows + not exist in map case
            ]);
            let batch1 = vec![
                Arc::new(col1) as _,
                Arc::new(col2) as _,
                Arc::new(col3) as _,
            ];

            // Intern batch 2
            let col1 = Int64Array::from(vec![
                // Repeated rows in batch
                Some(42),    // all not nulls + repeated rows + exist in map case
                None,        // mixed + repeated rows + exist in map case
                None,        // mixed + repeated rows + not exist in map case
                Some(21142), // mixed + repeated rows + not exist in map case
                None,        // all nulls + repeated rows + exist in map case
                Some(42),
                None,
                None,
                Some(21142),
                None,
                // Unique rows in batch
                Some(4211),  // all not nulls + unique rows + exist in map case
                None,        // mixed + unique rows + exist in map case
                None,        // mixed + unique rows + not exist in map case
                Some(24212), // mixed + unique rows + not exist in map case
            ]);

            let col2 = StringArray::from(vec![
                // Repeated rows in batch
                Some("string1"), // all not nulls + repeated rows + exist in map case
                None,            // mixed + repeated rows + exist in map case
                Some("2string2"), // mixed + repeated rows + not exist in map case
                None,            // mixed + repeated rows + not exist in map case
                None,            // all nulls + repeated rows + exist in map case
                Some("string1"),
                None,
                Some("2string2"),
                None,
                None,
                // Unique rows in batch
                Some("string3"), // all not nulls + unique rows + exist in map case
                None,            // mixed + unique rows + exist in map case
                Some("2string4"), // mixed + unique rows + not exist in map case
                None,            // mixed + unique rows + not exist in map case
            ]);

            let col3 = StringViewArray::from(vec![
                // Repeated rows in batch
                Some("stringview1"), // all not nulls + repeated rows + exist in map case
                Some("stringview2"), // mixed + repeated rows + exist in map case
                None,                // mixed + repeated rows + not exist in map case
                None,                // mixed + repeated rows + not exist in map case
                None,                // all nulls + repeated rows + exist in map case
                Some("stringview1"),
                Some("stringview2"),
                None,
                None,
                None,
                // Unique rows in batch
                Some("stringview3"), // all not nulls + unique rows + exist in map case
                Some("stringview4"), // mixed + unique rows + exist in map case
                None,                // mixed + unique rows + not exist in map case
                None,                // mixed + unique rows + not exist in map case
            ]);
            let batch2 = vec![
                Arc::new(col1) as _,
                Arc::new(col2) as _,
                Arc::new(col3) as _,
            ];

            // Intern batch 3
            let col1 = Int64Array::from(vec![
                // Repeated rows in batch
                Some(42),    // all not nulls + repeated rows + exist in map case
                None,        // mixed + repeated rows + exist in map case
                None,        // mixed + repeated rows + not exist in map case
                Some(31142), // mixed + repeated rows + not exist in map case
                None,        // all nulls + repeated rows + exist in map case
                Some(42),
                None,
                None,
                Some(31142),
                None,
                // Unique rows in batch
                Some(4211),  // all not nulls + unique rows + exist in map case
                None,        // mixed + unique rows + exist in map case
                None,        // mixed + unique rows + not exist in map case
                Some(34212), // mixed + unique rows + not exist in map case
            ]);

            let col2 = StringArray::from(vec![
                // Repeated rows in batch
                Some("string1"), // all not nulls + repeated rows + exist in map case
                None,            // mixed + repeated rows + exist in map case
                Some("3string2"), // mixed + repeated rows + not exist in map case
                None,            // mixed + repeated rows + not exist in map case
                None,            // all nulls + repeated rows + exist in map case
                Some("string1"),
                None,
                Some("3string2"),
                None,
                None,
                // Unique rows in batch
                Some("string3"), // all not nulls + unique rows + exist in map case
                None,            // mixed + unique rows + exist in map case
                Some("3string4"), // mixed + unique rows + not exist in map case
                None,            // mixed + unique rows + not exist in map case
            ]);

            let col3 = StringViewArray::from(vec![
                // Repeated rows in batch
                Some("stringview1"), // all not nulls + repeated rows + exist in map case
                Some("stringview2"), // mixed + repeated rows + exist in map case
                None,                // mixed + repeated rows + not exist in map case
                None,                // mixed + repeated rows + not exist in map case
                None,                // all nulls + repeated rows + exist in map case
                Some("stringview1"),
                Some("stringview2"),
                None,
                None,
                None,
                // Unique rows in batch
                Some("stringview3"), // all not nulls + unique rows + exist in map case
                Some("stringview4"), // mixed + unique rows + exist in map case
                None,                // mixed + unique rows + not exist in map case
                None,                // mixed + unique rows + not exist in map case
            ]);
            let batch3 = vec![
                Arc::new(col1) as _,
                Arc::new(col2) as _,
                Arc::new(col3) as _,
            ];

            // Expected batch
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Utf8View, true),
            ]));

            let col1 = Int64Array::from(vec![
                // Repeated rows in batch
                Some(42),
                None,
                None,
                Some(1142),
                None,
                Some(21142),
                None,
                Some(31142),
                None,
                // Unique rows in batch
                Some(4211),
                None,
                None,
                Some(4212),
                None,
                Some(24212),
                None,
                Some(34212),
            ]);

            let col2 = StringArray::from(vec![
                // Repeated rows in batch
                Some("string1"),
                None,
                Some("string2"),
                None,
                Some("2string2"),
                None,
                Some("3string2"),
                None,
                None,
                // Unique rows in batch
                Some("string3"),
                None,
                Some("string4"),
                None,
                Some("2string4"),
                None,
                Some("3string4"),
                None,
            ]);

            let col3 = StringViewArray::from(vec![
                // Repeated rows in batch
                Some("stringview1"),
                Some("stringview2"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                // Unique rows in batch
                Some("stringview3"),
                Some("stringview4"),
                None,
                None,
                None,
                None,
                None,
                None,
            ]);
            let expected_batch = vec![
                Arc::new(col1) as _,
                Arc::new(col2) as _,
                Arc::new(col3) as _,
            ];
            let expected_batch = RecordBatch::try_new(schema, expected_batch).unwrap();

            Self {
                test_batches: vec![batch1, batch2, batch3],
                expected_batch,
            }
        }

        fn load_to_group_values(&self, group_values: &mut impl GroupValues) {
            for batch in self.test_batches.iter() {
                group_values.intern(batch, &mut vec![]).unwrap();
            }
        }

        fn schema(&self) -> SchemaRef {
            self.expected_batch.schema()
        }
    }

    fn check_result(actual_batch: &RecordBatch, expected_batch: &RecordBatch) {
        let formatted_actual_batch = pretty_format_batches(&[actual_batch.clone()])
            .unwrap()
            .to_string();
        let mut formatted_actual_batch_sorted: Vec<&str> =
            formatted_actual_batch.trim().lines().collect();
        formatted_actual_batch_sorted.sort_unstable();

        let formatted_expected_batch = pretty_format_batches(&[expected_batch.clone()])
            .unwrap()
            .to_string();
        let mut formatted_expected_batch_sorted: Vec<&str> =
            formatted_expected_batch.trim().lines().collect();
        formatted_expected_batch_sorted.sort_unstable();

        for (i, (actual_line, expected_line)) in formatted_actual_batch_sorted
            .iter()
            .zip(&formatted_expected_batch_sorted)
            .enumerate()
        {
            assert_eq!(
                (i, actual_line),
                (i, expected_line),
                "Inconsistent result\n\n\
                 Actual batch:\n{}\n\
                 Expected batch:\n{}\n\
                 ",
                formatted_actual_batch,
                formatted_expected_batch,
            );
        }
    }

    fn insert_inline_group_index_view(
        group_values: &mut GroupValuesColumn<false>,
        hash_key: u64,
        group_index: u64,
    ) {
        let group_index_view = GroupIndexView::new_inlined(group_index);
        group_values.map.insert_accounted(
            (hash_key, group_index_view),
            |(hash, _)| *hash,
            &mut group_values.map_size,
        );
    }

    fn insert_non_inline_group_index_view(
        group_values: &mut GroupValuesColumn<false>,
        hash_key: u64,
        group_indices: Vec<usize>,
    ) {
        let list_offset = group_values.group_index_lists.len();
        let group_index_view = GroupIndexView::new_non_inlined(list_offset as u64);
        group_values.group_index_lists.push(group_indices);
        group_values.map.insert_accounted(
            (hash_key, group_index_view),
            |(hash, _)| *hash,
            &mut group_values.map_size,
        );
    }
}
