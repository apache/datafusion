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
use datafusion_common::Result;

use datafusion_expr::EmitTo;
use datafusion_expr_common::groups_accumulator::BlockedEmitTo;
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

pub(crate) use metrics::{
    AccumulatorPhase, AggregateAccumulatorMetrics, AggregateArgumentMetrics,
    GroupByMetrics,
};

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
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()>;

    /// Returns the number of bytes of memory used by this [`BlockedGroupValues`].
    ///
    /// May be expensive; check the implementation before calling on hot paths.
    fn size(&self) -> usize;

    /// Returns true if this [`BlockedGroupValues`] is empty
    fn is_empty(&self) -> bool;

    /// The number of values (distinct group values) stored in this [`BlockedGroupValues`]
    fn len(&self) -> usize;

    /// Emits the group values
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>>;

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

    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        self.inner.intern(cols, groups)
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

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.emit(emit_to)
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
    let mapped_group_ordering: crate::aggregates::order::GroupOrdering = group_ordering.clone().into();
    let mapped = crate::aggregates::group_values::new_group_values(schema, &mapped_group_ordering)?;
    Ok(Box::new(BlockedGroupValuesAdapter::new(block_size, mapped)))
}
