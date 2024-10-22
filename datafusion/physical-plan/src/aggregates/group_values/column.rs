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
///   And if we `vectorized compare` and `vectorized append` them
///   in the same round, some fault cases will occur especially when
///   they are totally the repeated rows...
///
///   For example:
///     - Two repeated rows exist in `input cols`.
///
///     - We found their hash values equal to one exist group
///
///     - We then perform `vectorized compare` for them to the exist group,
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

    /// The marked checking buckets in this round
    ///
    /// About the checking flag you can see [`BucketContext`]
    checking_buckets: Vec<Bucket<(u64, BucketContext)>>,

    /// We need multiple rounds to process the `input cols`,
    /// and the rows processing in current round is stored here.
    current_indices: Vec<usize>,

    /// Similar as `current_indices`, but `remaining_indices`
    /// is used to store the rows will be processed in next round.
    remaining_indices: Vec<usize>,

    /// The `vectorized compared` row indices buffer
    vectorized_compare_row_indices: Vec<usize>,

    /// The `vectorized compared` group indices buffer
    vectorized_compare_group_indices: Vec<usize>,

    /// The `vectorized compared` result buffer
    vectorized_compare_results: Vec<bool>,

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

    column_nullables_buffer: Vec<bool>,

    append_rows_buffer: Vec<usize>,
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
            checking_buckets: Default::default(),
            map_size: 0,
            group_values: vec![],
            hashes_buffer: Default::default(),
            random_state: Default::default(),
            column_nullables_buffer: vec![false; num_cols],
            append_rows_buffer: Default::default(),
            current_indices: Default::default(),
            remaining_indices: Default::default(),
            vectorized_compare_row_indices: Default::default(),
            vectorized_compare_group_indices: Default::default(),
            vectorized_compare_results: Default::default(),
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

    #[inline]
    fn get_group_indices_from_list(
        &self,
        start_group_index: usize,
    ) -> GroupIndicesIterator {
        GroupIndicesIterator::new(start_group_index, &self.group_index_lists)
    }

    fn collect_vectorized_process_context(&mut self, batch_hashes: &[u64]) {
        let mut next_group_idx = self.group_values[0].len() as u64;
        for &row in self.current_indices.iter() {
            let target_hash = batch_hashes[row];
            let entry = self.map.get_mut(target_hash, |(exist_hash, _)| {
                // Somewhat surprisingly, this closure can be called even if the
                // hash doesn't match, so check the hash first with an integer
                // comparison first avoid the more expensive comparison with
                // group value. https://github.com/apache/datafusion/pull/11718
                target_hash == *exist_hash
            });

            let Some((_, bucket_ctx)) = entry else {
                // 1.1 Bucket not found case
                // Insert the `new bucket` build from the `group index`
                // Mark this `new bucket` checking, and add it to `checking_buckets`
                let current_group_idx = next_group_idx;

                // for hasher function, use precomputed hash value
                let mut bucket_ctx = BucketContext(current_group_idx);
                bucket_ctx.set_checking();
                let bucket = self.map.insert_accounted(
                    (target_hash, bucket_ctx),
                    |(hash, _)| *hash,
                    &mut self.map_size,
                );
                self.checking_buckets.push(bucket);

                // Add row index to `vectorized_append_row_indices`
                self.vectorized_append_row_indices.push(row);

                next_group_idx += 1;
                continue;
            };

            // 1.2 bucket found
            // Check if the `bucket` checking, if so add it to `remaining_indices`,
            // and just process it in next round, otherwise we continue the process
            if bucket_ctx.is_checking() {
                self.remaining_indices.push(row);
                continue;
            }
            // Mark `bucket` checking, and add it to `checking_buckets`
            bucket_ctx.set_checking();

            // Add row index to `vectorized_compare_row_indices`
            // Add group indices(from `group_index_lists`) to `vectorized_compare_group_indices`
            let mut next_group_index = bucket_ctx.group_index() as usize + 1;
            while next_group_index > 0 {
                let current_group_index = next_group_index;
                self.vectorized_compare_row_indices.push(row);
                self.vectorized_compare_group_indices.push(current_group_index - 1);
                next_group_index = self.group_index_lists[current_group_index];
            }
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

        // General steps for one round `vectorized compare & append`:
        //   1. Calculate and check hash values of `cols` in `map`
        //   2. Perform `vectorized compare`
        //   3. Perform `vectorized append`
        //   4. Reset the checking flag in `BucketContext`

        // 1. Calculate and check hash values of `cols` in `map`
        //
        // 1.1 If bucket not found
        //   - Insert the `new bucket` build from the `group index`
        //     and its hash value to `map`
        //   - Mark this `new bucket` checking, and add it to `checking_buckets`
        //   - Add row index to `vectorized_append_row_indices`
        //
        // 1.2 bucket found
        //   - Check if the `bucket` checking, if so add it to `remaining_indices`,
        //     and just process it in next round, otherwise we continue the process
        //   - Mark `bucket` checking, and add it to `checking_buckets`
        //   - Add row index to `vectorized_compare_row_indices`
        //   - Add group indices(from `group_index_lists`) to `vectorized_compare_group_indices`
        //
        let batch_hashes = &mut self.hashes_buffer;
        batch_hashes.clear();
        batch_hashes.resize(n_rows, 0);
        create_hashes(cols, &self.random_state, batch_hashes)?;

        let num_rows = cols[0].len();
        self.current_indices.clear();
        self.current_indices.extend(0..num_rows);
        while self.current_indices.len() > 0 {
            self.vectorized_append_row_indices.clear();
            self.vectorized_compare_row_indices.clear();
            self.vectorized_compare_group_indices.clear();
            self.vectorized_compare_results.clear();

            // 2. Perform `vectorized compare`
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
                // unsafe {
                //     for bucket in self.map.iter() {
                //         // Decrement group index by n
                //         match bucket.as_ref().1.0.checked_sub(n) {
                //             // Group index was >= n, shift value down
                //             Some(sub) => bucket.as_mut().1 = sub,
                //             // Group index was < n, so remove from table
                //             None => self.map.erase(bucket),
                //         }
                //     }
                // }

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

fn is_rows_eq(
    left_arrays: &[ArrayRef],
    left: usize,
    right_arrays: &[ArrayRef],
    right: usize,
) -> Result<bool> {
    let mut is_equal = true;
    for (left_array, right_array) in left_arrays.iter().zip(right_arrays) {
        macro_rules! compare_value {
            ($T:ty) => {{
                match (left_array.is_null(left), right_array.is_null(right)) {
                    (false, false) => {
                        let left_array =
                            left_array.as_any().downcast_ref::<$T>().unwrap();
                        let right_array =
                            right_array.as_any().downcast_ref::<$T>().unwrap();
                        if left_array.value(left) != right_array.value(right) {
                            is_equal = false;
                        }
                    }
                    (true, false) => is_equal = false,
                    (false, true) => is_equal = false,
                    _ => {}
                }
            }};
        }

        match left_array.data_type() {
            DataType::Null => {}
            DataType::Boolean => compare_value!(BooleanArray),
            DataType::Int8 => compare_value!(Int8Array),
            DataType::Int16 => compare_value!(Int16Array),
            DataType::Int32 => compare_value!(Int32Array),
            DataType::Int64 => compare_value!(Int64Array),
            DataType::UInt8 => compare_value!(UInt8Array),
            DataType::UInt16 => compare_value!(UInt16Array),
            DataType::UInt32 => compare_value!(UInt32Array),
            DataType::UInt64 => compare_value!(UInt64Array),
            DataType::Float32 => compare_value!(Float32Array),
            DataType::Float64 => compare_value!(Float64Array),
            DataType::Utf8 => compare_value!(StringArray),
            DataType::LargeUtf8 => compare_value!(LargeStringArray),
            DataType::Binary => compare_value!(BinaryArray),
            DataType::Utf8View => compare_value!(StringViewArray),
            DataType::BinaryView => compare_value!(BinaryViewArray),
            DataType::Decimal128(..) => compare_value!(Decimal128Array),
            DataType::Timestamp(time_unit, None) => match time_unit {
                TimeUnit::Second => compare_value!(TimestampSecondArray),
                TimeUnit::Millisecond => compare_value!(TimestampMillisecondArray),
                TimeUnit::Microsecond => compare_value!(TimestampMicrosecondArray),
                TimeUnit::Nanosecond => compare_value!(TimestampNanosecondArray),
            },
            DataType::Date32 => compare_value!(Date32Array),
            DataType::Date64 => compare_value!(Date64Array),
            dt => {
                return not_impl_err!(
                    "Unsupported data type in sort merge join comparator: {}",
                    dt
                );
            }
        }
        if !is_equal {
            return Ok(false);
        }
    }
    Ok(true)
}
