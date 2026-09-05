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

//! [`BlockedGroupValues`] trait for storing and interning group keys

use arrow::array::types::{
    Date32Type, Date64Type, Decimal128Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow::array::{ArrayRef, downcast_primitive};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion_common::{assert_ne_or_internal_err, Result, assert_or_internal_err, unwrap_or_internal_err, assert_eq_or_internal_err, not_impl_err};

use datafusion_expr::EmitTo;
use datafusion_expr_common::groups_accumulator::{BlockedEmitTo, BlockedGroupSelection, BlocksIndex, GroupSelection};
// pub mod multi_group_by;

// mod row;
// pub use row::GroupValuesRows;
// mod single_group_by;
use datafusion_physical_expr::binary_map::OutputType;
// use multi_group_by::GroupValuesColumn;
//
// pub(crate) use single_group_by::primitive::HashValue;
pub(crate) use crate::aggregates::group_values::HashValue;

use crate::aggregates_blocked::{
    // group_values::single_group_by::{
    //     boolean::GroupValuesBoolean, bytes::GroupValuesBytes,
    //     bytes_view::GroupValuesBytesView, primitive::GroupValuesPrimitive,
    // },
    order::GroupOrdering,
};

mod metrics;
mod null_builder;
mod single_group_by;
mod multi_group_by;

pub(crate) use metrics::{
    AccumulatorPhase, AggregateAccumulatorMetrics, AggregateArgumentMetrics,
    GroupByMetrics,
};
use single_group_by::boolean::GroupValuesBoolean;
use single_group_by::primitive::GroupValuesPrimitive;
use single_group_by::bytes::GroupValuesBytes;
use single_group_by::bytes_view::GroupValuesBytesView;
use crate::aggregates_blocked::group_values::multi_group_by::BlockedGroupValuesColumn;

/// Stores the group values during hash aggregation.
///
/// # Background
///
/// In a query such as `SELECT a, b, count(*) FROM t GROUP BY a, b`, the group values
/// identify each group, and correspond to all the distinct values of `(a,b)`.
///
/// ```sql
/// -- Input has 4 rows with 3 distinct combinations of (a,b) ("groups")
/// create table t(a int, b varchar)
/// as values (1, 'a'), (2, 'b'), (1, 'a'), (3, 'c');
///
/// select a, b, count(*) from t group by a, b;
/// ----
/// 1 a 2
/// 2 b 1
/// 3 c 1
/// ```
///
/// # Design
///
/// Managing group values is a performance critical operation in hash
/// aggregation. The major operations are:
///
/// 1. Intern: Quickly finding existing and adding new group values
/// 2. Emit: Returning the group values as an array
///
/// There are multiple specialized implementations of this trait optimized for
/// different data types and number of columns, optimized for these operations.
/// See [`new_group_values`] for details.
///
/// # Group Ids
///
/// Each distinct group in a hash aggregation is identified by a unique group id
/// (usize) which is assigned by instances of this trait. Group ids are
/// continuous without gaps, starting from 0.
pub trait BlockedGroupValues: Send {
    fn block_size(&self) -> usize;

    /// Calculates the group id for each input row of `cols`, assigning new
    /// group ids as necessary.
    ///
    /// When the function returns, `groups`  must contain the group id for each
    /// row in `cols`.
    ///
    /// If a row has the same value as a previous row, the same group id is
    /// assigned. If a row has a new value, the next available group id is
    /// assigned.
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<BlocksIndex>) -> Result<()>;

    /// Returns the number of bytes of memory used by this [`BlockedGroupValues`].
    ///
    /// May be expensive; check the implementation before calling on hot paths.
    fn size(&self) -> usize;

    /// Returns true if this [`BlockedGroupValues`] is empty
    fn is_empty(&self) -> bool;

    /// The number of values (distinct group values) stored in this [`BlockedGroupValues`]
    fn len(&self) -> usize;

    /// Materializes selected group values without changing the stored values or
    /// their group indices.
    ///
    /// Rows are returned in the order specified by `selection`. An empty
    /// selection returns one correctly typed empty array per group-value column.
    ///
    /// This method requires exclusive access because implementations may mutate
    /// internal caches or builders, even though stored values are unchanged.
    fn values_preserving(
        &mut self,
        _selection: BlockedGroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        not_impl_err!("Preserving group values are not implemented")
    }

    /// Returns `true` if [`Self::values_preserving`] is implemented.
    fn supports_values_preserving(&self) -> bool {
        false
    }

    /// Emits the group values
    fn emit(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<Vec<ArrayRef>>> {
        match emit_to {
            BlockedEmitTo::All => {
                self.emit_all()
            }
            BlockedEmitTo::NextBlock => {
                let len = self.len();
                let block_size = self.block_size();

                if len == 0 {
                    return Ok(vec![]);
                }

                if len <= block_size {
                    return self.emit_all();
                }

                let block = self.emit_block()?;
                let block = unwrap_or_internal_err!(block);

                // Assert that all arrays length equal block size since length is greater than block size
                for arr in &block {
                    assert_eq_or_internal_err!(arr.len(), block_size);
                }

                Ok(vec![block])
            }
            BlockedEmitTo::First(n) => {
                assert_ne_or_internal_err!(n, 0);
                assert_or_internal_err!(n <= self.len(), "n ({n}) must be less than or equal current length ({})", self.len());
                assert_or_internal_err!(n < self.block_size(), "n ({n}) must be less than current block size ({})", self.block_size());

                if n == self.len() {
                    self.emit_all()
                } else {
                    self.emit_first_n(n).map(|first_n| vec![first_n])
                }
            }
        }
    }

    /// Emit all group values
    fn emit_all(&mut self) -> Result<Vec<Vec<ArrayRef>>>;

    /// Emit the next block
    /// returns Ok(None) when there are no blocks
    fn emit_block(&mut self) -> Result<Option<Vec<ArrayRef>>>;

    /// Emit first `n` values and shift all values to fit into blocks
    ///
    /// `n` must be smaller than [`Self::block_size`] and larger than `0`
    /// `n` must be smaller or equal to [`Self::len`]
    fn emit_first_n(&mut self, n: usize) -> Result<Vec<ArrayRef>>;

    /// Clear the contents and shrink the capacity to the size of the batch (free up memory usage)
    fn clear_shrink(&mut self, num_rows: usize);
}


pub struct BlockedGroupValuesAdapter {
    block_size: usize,
    inner: Box<dyn crate::aggregates::group_values::GroupValues>,
}

impl BlockedGroupValuesAdapter {
    pub fn new(block_size: usize, inner: Box<dyn crate::aggregates::group_values::GroupValues>) -> Self {
        Self { block_size, inner }
    }
}

impl BlockedGroupValues for BlockedGroupValuesAdapter {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<BlocksIndex>) -> Result<()> {
        let block_size = self.block_size;
        let mut group_indices_flattened = groups.iter().map(|i| i.into_index_in_fixed_block_size(block_size)).collect::<Vec<_>>();
        self.inner.intern(cols, &mut group_indices_flattened)?;
        *groups = group_indices_flattened.iter().map(|index| BlocksIndex::from_index_in_fixed_block_size(*index, block_size)).collect::<Vec<_>>();

        Ok(())
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn emit_all(&mut self) -> Result<Vec<Vec<ArrayRef>>> {
        let mut blocks = vec![];

        while self.len() > self.block_size {
            blocks.push(self.inner.emit(EmitTo::First(self.block_size))?);
        }

        if self.len() > 0 {
            blocks.push(self.inner.emit(EmitTo::All)?);
        }

        Ok(blocks)
    }

    fn emit_block(&mut self) -> Result<Option<Vec<ArrayRef>>> {
        if self.len() == 0 {
            return Ok(None);
        }

        let output = if self.len() <= self.block_size {
            self.inner.emit(EmitTo::All)
        } else {
            self.inner.emit(EmitTo::First(self.block_size))
        };

        Ok(Some(output?))
    }

    fn emit_first_n(&mut self, n: usize) -> Result<Vec<ArrayRef>> {
        assert_ne_or_internal_err!(n, 0);
        assert_or_internal_err!(n <= self.len(), "n ({n}) must be less than or equal current length ({})", self.len());
        assert_or_internal_err!(n < self.block_size(), "n ({n}) must be less than current block size ({})", self.block_size());

        let output = if self.len() == n {
            self.inner.emit(EmitTo::All)
        } else {
            self.inner.emit(EmitTo::First(n))
        };

        Ok(output?)
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.inner.clear_shrink(num_rows)
    }
}

/// Return a specialized implementation of [`BlockedGroupValues`] for the given schema.
///
/// [`BlockedGroupValues`] implementations choosing logic:
///
///   - If group by single column, and type of this column has
///     the specific [`BlockedGroupValues`] implementation, such implementation
///     will be chosen.
///
///   - If group by multiple columns, and all column types have the specific
///     `GroupColumn` implementations, `GroupValuesColumn` will be chosen.
///
///   - Otherwise, the general implementation `GroupValuesRows` will be chosen.
///
/// `GroupColumn`:  crate::aggregates_blocked::group_values::multi_group_by::GroupColumn
/// `GroupValuesColumn`: crate::aggregates_blocked::group_values::multi_group_by::GroupValuesColumn
/// `GroupValuesRows`: crate::aggregates_blocked::group_values::GroupValuesRows
pub fn new_group_values(
    schema: SchemaRef,
    group_ordering: &GroupOrdering,
    block_size: usize,
) -> Result<Box<dyn BlockedGroupValues>> {
    if schema.fields.len() == 1 {
        let d = schema.fields[0].data_type();

        macro_rules! downcast_helper {
            ($t:ty, $d:ident) => {
                return Ok(Box::new(GroupValuesPrimitive::<$t>::new($d.clone(), block_size)))
            };
        }

        downcast_primitive! {
            d => (downcast_helper, d),
            _ => {}
        }

        match d {
            DataType::Date32 => {
                downcast_helper!(Date32Type, d);
            }
            DataType::Date64 => {
                downcast_helper!(Date64Type, d);
            }
            DataType::Time32(t) => match t {
                TimeUnit::Second => downcast_helper!(Time32SecondType, d),
                TimeUnit::Millisecond => downcast_helper!(Time32MillisecondType, d),
                _ => {}
            },
            DataType::Time64(t) => match t {
                TimeUnit::Microsecond => downcast_helper!(Time64MicrosecondType, d),
                TimeUnit::Nanosecond => downcast_helper!(Time64NanosecondType, d),
                _ => {}
            },
            DataType::Timestamp(t, _tz) => match t {
                TimeUnit::Second => downcast_helper!(TimestampSecondType, d),
                TimeUnit::Millisecond => downcast_helper!(TimestampMillisecondType, d),
                TimeUnit::Microsecond => downcast_helper!(TimestampMicrosecondType, d),
                TimeUnit::Nanosecond => downcast_helper!(TimestampNanosecondType, d),
            },
            DataType::Decimal128(_, _) => {
                downcast_helper!(Decimal128Type, d);
            }
            DataType::Utf8 => {
                return Ok(Box::new(GroupValuesBytes::<i32>::new(OutputType::Utf8, block_size)));
            }
            DataType::LargeUtf8 => {
                return Ok(Box::new(GroupValuesBytes::<i64>::new(OutputType::Utf8, block_size)));
            }
            DataType::Utf8View => {
                return Ok(Box::new(GroupValuesBytesView::new(OutputType::Utf8View, block_size)));
            }
            DataType::Binary => {
                return Ok(Box::new(GroupValuesBytes::<i32>::new(OutputType::Binary, block_size)));
            }
            DataType::LargeBinary => {
                return Ok(Box::new(GroupValuesBytes::<i64>::new(OutputType::Binary, block_size)));
            }
            DataType::BinaryView => {
                return Ok(Box::new(GroupValuesBytesView::new(OutputType::BinaryView, block_size)));
            }
            DataType::Boolean => {
                return Ok(Box::new(GroupValuesBoolean::new(block_size)));
            }
            _ => {}
        }
    }

    if multi_group_by::supported_schema(schema.as_ref()) {
        if matches!(group_ordering, GroupOrdering::None) {
            Ok(Box::new(BlockedGroupValuesColumn::<false>::try_new(schema, block_size)?))
        } else {
            Ok(Box::new(BlockedGroupValuesColumn::<true>::try_new(schema, block_size)?))
        }
    } else {
        let mapped_group_ordering: crate::aggregates::order::GroupOrdering = group_ordering.clone().into();
        let mapped = crate::aggregates::group_values::new_group_values(schema, &mapped_group_ordering)?;
        Ok(Box::new(BlockedGroupValuesAdapter::new(block_size, mapped)))
    }
}
