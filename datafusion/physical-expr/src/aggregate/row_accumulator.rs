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

//! Accumulator over row format

use crate::aggregate::average::AvgRowAccumulator;
use crate::aggregate::bit_and_or_xor::{
    BitAndRowAccumulator, BitOrRowAccumulator, BitXorRowAccumulator,
};
use crate::aggregate::bool_and_or::{BoolAndRowAccumulator, BoolOrRowAccumulator};
use crate::aggregate::count::CountRowAccumulator;
use crate::aggregate::min_max::{MaxRowAccumulator, MinRowAccumulator};
use crate::aggregate::row_agg_macros::matches_all_supported_data_types;
use crate::aggregate::sum::SumRowAccumulator;
use arrow::array::ArrayRef;
use arrow_array::BooleanArray;
use arrow_schema::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_row::accessor::{RowAccessor, RowAccumulatorNativeType};
use std::fmt::Debug;

/// Row-based accumulator where the internal aggregate state(s) are stored using row format.
///
/// Unlike the [`datafusion_expr::Accumulator`], the [`RowAccumulator`] does not store the state internally.
/// Instead, it knows how to access/update the state stored in a row via the the provided accessor and
/// its state's starting field index in the row.
///
/// For example, we are evaluating `SELECT a, sum(b), avg(c), count(d) from GROUP BY a;`, we would have one row used as
/// aggregation state for each distinct `a` value, the index of the first and the only state of `sum(b)` would be 0,
/// the index of the first state of `avg(c)` would be 1, and the index of the first and only state of `cound(d)` would be 3:
///
/// sum(b) state_index = 0              count(d) state_index = 3
///        |                            |
///        v                            v
///        +--------+----------+--------+----------+
///        | sum(b) | count(c) | sum(c) | count(d) |
///        +--------+----------+--------+----------+
///                 ^
///                 |
///           avg(c) state_index = 1
///
pub trait RowAccumulator:
    Send + Sync + Debug + TryFrom<RowAccumulatorItem> + Into<RowAccumulatorItem>
{
    /// updates the accumulator's state from a vector of arrays.
    fn update_batch(&self, values: &[ArrayRef], accessor: &mut RowAccessor)
        -> Result<()>;

    /// updates the accumulator's state from rows with the specified indices.
    fn update_row_indices(
        &self,
        values: &[ArrayRef],
        filter: &Option<&BooleanArray>,
        row_indices: &[usize],
        accessor: &mut RowAccessor,
    ) -> Result<()>;

    /// updates the accumulator's state from a rust native value.
    fn update_value<N: RowAccumulatorNativeType>(
        &self,
        native_value: Option<N>,
        accessor: &mut RowAccessor,
    );

    /// updates the accumulator's state from a vector of states.
    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()>;

    /// returns its value based on its current state.
    fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue>;

    /// State's starting field index in the row.
    fn state_index(&self) -> usize;
}

/// Returns if `data_type` is supported with `RowAccumulator`
pub fn is_row_accumulator_support_dtype(data_type: &DataType) -> bool {
    matches_all_supported_data_types!(data_type)
}

/// Enum to dispatch the RowAccumulator, RowAccumulator contains generic methods and can not be used as the trait objects
pub enum RowAccumulatorItem {
    AVG(AvgRowAccumulator),
    SUM(SumRowAccumulator),
    COUNT(CountRowAccumulator),
    MIN(MinRowAccumulator),
    MAX(MaxRowAccumulator),
    BITAND(BitAndRowAccumulator),
    BITOR(BitOrRowAccumulator),
    BITXOR(BitXorRowAccumulator),
    BOOLAND(BoolAndRowAccumulator),
    BOOLOR(BoolOrRowAccumulator),
}

impl RowAccumulatorItem {
    pub fn update_batch(
        &self,
        values: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        match self {
            RowAccumulatorItem::AVG(acc) => acc.update_batch(values, accessor),
            RowAccumulatorItem::SUM(acc) => acc.update_batch(values, accessor),
            RowAccumulatorItem::COUNT(acc) => acc.update_batch(values, accessor),
            RowAccumulatorItem::MIN(acc) => acc.update_batch(values, accessor),
            RowAccumulatorItem::MAX(acc) => acc.update_batch(values, accessor),
            RowAccumulatorItem::BITAND(acc) => acc.update_batch(values, accessor),
            RowAccumulatorItem::BITOR(acc) => acc.update_batch(values, accessor),
            RowAccumulatorItem::BITXOR(acc) => acc.update_batch(values, accessor),
            RowAccumulatorItem::BOOLAND(acc) => acc.update_batch(values, accessor),
            RowAccumulatorItem::BOOLOR(acc) => acc.update_batch(values, accessor),
        }
    }

    pub fn update_row_indices(
        &self,
        values: &[ArrayRef],
        filter: &Option<&BooleanArray>,
        row_indices: &[usize],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        match self {
            RowAccumulatorItem::AVG(acc) => {
                acc.update_row_indices(values, filter, row_indices, accessor)
            }
            RowAccumulatorItem::SUM(acc) => {
                acc.update_row_indices(values, filter, row_indices, accessor)
            }
            RowAccumulatorItem::COUNT(acc) => {
                acc.update_row_indices(values, filter, row_indices, accessor)
            }
            RowAccumulatorItem::MIN(acc) => {
                acc.update_row_indices(values, filter, row_indices, accessor)
            }
            RowAccumulatorItem::MAX(acc) => {
                acc.update_row_indices(values, filter, row_indices, accessor)
            }
            RowAccumulatorItem::BITAND(acc) => {
                acc.update_row_indices(values, filter, row_indices, accessor)
            }
            RowAccumulatorItem::BITOR(acc) => {
                acc.update_row_indices(values, filter, row_indices, accessor)
            }
            RowAccumulatorItem::BITXOR(acc) => {
                acc.update_row_indices(values, filter, row_indices, accessor)
            }
            RowAccumulatorItem::BOOLAND(acc) => {
                acc.update_row_indices(values, filter, row_indices, accessor)
            }
            RowAccumulatorItem::BOOLOR(acc) => {
                acc.update_row_indices(values, filter, row_indices, accessor)
            }
        }
    }

    #[inline(always)]
    pub fn update_value<N: RowAccumulatorNativeType>(
        &self,
        native_value: Option<N>,
        accessor: &mut RowAccessor,
    ) {
        match self {
            RowAccumulatorItem::AVG(acc) => acc.update_value(native_value, accessor),
            RowAccumulatorItem::SUM(acc) => acc.update_value(native_value, accessor),
            RowAccumulatorItem::COUNT(acc) => acc.update_value(native_value, accessor),
            RowAccumulatorItem::MIN(acc) => acc.update_value(native_value, accessor),
            RowAccumulatorItem::MAX(acc) => acc.update_value(native_value, accessor),
            RowAccumulatorItem::BITAND(acc) => acc.update_value(native_value, accessor),
            RowAccumulatorItem::BITOR(acc) => acc.update_value(native_value, accessor),
            RowAccumulatorItem::BITXOR(acc) => acc.update_value(native_value, accessor),
            RowAccumulatorItem::BOOLAND(acc) => acc.update_value(native_value, accessor),
            RowAccumulatorItem::BOOLOR(acc) => acc.update_value(native_value, accessor),
        }
    }

    pub fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        match self {
            RowAccumulatorItem::AVG(acc) => acc.merge_batch(states, accessor),
            RowAccumulatorItem::SUM(acc) => acc.merge_batch(states, accessor),
            RowAccumulatorItem::COUNT(acc) => acc.merge_batch(states, accessor),
            RowAccumulatorItem::MIN(acc) => acc.merge_batch(states, accessor),
            RowAccumulatorItem::MAX(acc) => acc.merge_batch(states, accessor),
            RowAccumulatorItem::BITAND(acc) => acc.merge_batch(states, accessor),
            RowAccumulatorItem::BITOR(acc) => acc.merge_batch(states, accessor),
            RowAccumulatorItem::BITXOR(acc) => acc.merge_batch(states, accessor),
            RowAccumulatorItem::BOOLAND(acc) => acc.merge_batch(states, accessor),
            RowAccumulatorItem::BOOLOR(acc) => acc.merge_batch(states, accessor),
        }
    }

    pub fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue> {
        match self {
            RowAccumulatorItem::AVG(acc) => acc.evaluate(accessor),
            RowAccumulatorItem::SUM(acc) => acc.evaluate(accessor),
            RowAccumulatorItem::COUNT(acc) => acc.evaluate(accessor),
            RowAccumulatorItem::MIN(acc) => acc.evaluate(accessor),
            RowAccumulatorItem::MAX(acc) => acc.evaluate(accessor),
            RowAccumulatorItem::BITAND(acc) => acc.evaluate(accessor),
            RowAccumulatorItem::BITOR(acc) => acc.evaluate(accessor),
            RowAccumulatorItem::BITXOR(acc) => acc.evaluate(accessor),
            RowAccumulatorItem::BOOLAND(acc) => acc.evaluate(accessor),
            RowAccumulatorItem::BOOLOR(acc) => acc.evaluate(accessor),
        }
    }

    #[inline(always)]
    pub fn state_index(&self) -> usize {
        match self {
            RowAccumulatorItem::AVG(acc) => acc.state_index(),
            RowAccumulatorItem::SUM(acc) => acc.state_index(),
            RowAccumulatorItem::COUNT(acc) => acc.state_index(),
            RowAccumulatorItem::MIN(acc) => acc.state_index(),
            RowAccumulatorItem::MAX(acc) => acc.state_index(),
            RowAccumulatorItem::BITAND(acc) => acc.state_index(),
            RowAccumulatorItem::BITOR(acc) => acc.state_index(),
            RowAccumulatorItem::BITXOR(acc) => acc.state_index(),
            RowAccumulatorItem::BOOLAND(acc) => acc.state_index(),
            RowAccumulatorItem::BOOLOR(acc) => acc.state_index(),
        }
    }
}

impl TryFrom<RowAccumulatorItem> for AvgRowAccumulator {
    type Error = ();

    fn try_from(that: RowAccumulatorItem) -> Result<Self, Self::Error> {
        match that {
            RowAccumulatorItem::AVG(avg) => Ok(avg),
            _ => Err(()),
        }
    }
}

impl From<AvgRowAccumulator> for RowAccumulatorItem {
    fn from(that: AvgRowAccumulator) -> Self {
        RowAccumulatorItem::AVG(that)
    }
}

impl TryFrom<RowAccumulatorItem> for SumRowAccumulator {
    type Error = ();

    fn try_from(that: RowAccumulatorItem) -> Result<Self, Self::Error> {
        match that {
            RowAccumulatorItem::SUM(sum) => Ok(sum),
            _ => Err(()),
        }
    }
}

impl From<SumRowAccumulator> for RowAccumulatorItem {
    fn from(that: SumRowAccumulator) -> Self {
        RowAccumulatorItem::SUM(that)
    }
}

impl TryFrom<RowAccumulatorItem> for CountRowAccumulator {
    type Error = ();

    fn try_from(that: RowAccumulatorItem) -> Result<Self, Self::Error> {
        match that {
            RowAccumulatorItem::COUNT(count) => Ok(count),
            _ => Err(()),
        }
    }
}

impl From<CountRowAccumulator> for RowAccumulatorItem {
    fn from(that: CountRowAccumulator) -> Self {
        RowAccumulatorItem::COUNT(that)
    }
}

impl TryFrom<RowAccumulatorItem> for MinRowAccumulator {
    type Error = ();

    fn try_from(that: RowAccumulatorItem) -> Result<Self, Self::Error> {
        match that {
            RowAccumulatorItem::MIN(min) => Ok(min),
            _ => Err(()),
        }
    }
}

impl From<MinRowAccumulator> for RowAccumulatorItem {
    fn from(that: MinRowAccumulator) -> Self {
        RowAccumulatorItem::MIN(that)
    }
}

impl TryFrom<RowAccumulatorItem> for MaxRowAccumulator {
    type Error = ();

    fn try_from(that: RowAccumulatorItem) -> Result<Self, Self::Error> {
        match that {
            RowAccumulatorItem::MAX(max) => Ok(max),
            _ => Err(()),
        }
    }
}

impl From<MaxRowAccumulator> for RowAccumulatorItem {
    fn from(that: MaxRowAccumulator) -> Self {
        RowAccumulatorItem::MAX(that)
    }
}

impl TryFrom<RowAccumulatorItem> for BitAndRowAccumulator {
    type Error = ();

    fn try_from(that: RowAccumulatorItem) -> Result<Self, Self::Error> {
        match that {
            RowAccumulatorItem::BITAND(bitand) => Ok(bitand),
            _ => Err(()),
        }
    }
}

impl From<BitAndRowAccumulator> for RowAccumulatorItem {
    fn from(that: BitAndRowAccumulator) -> Self {
        RowAccumulatorItem::BITAND(that)
    }
}

impl TryFrom<RowAccumulatorItem> for BitOrRowAccumulator {
    type Error = ();

    fn try_from(that: RowAccumulatorItem) -> Result<Self, Self::Error> {
        match that {
            RowAccumulatorItem::BITOR(bitor) => Ok(bitor),
            _ => Err(()),
        }
    }
}

impl From<BitOrRowAccumulator> for RowAccumulatorItem {
    fn from(that: BitOrRowAccumulator) -> Self {
        RowAccumulatorItem::BITOR(that)
    }
}

impl TryFrom<RowAccumulatorItem> for BitXorRowAccumulator {
    type Error = ();

    fn try_from(that: RowAccumulatorItem) -> Result<Self, Self::Error> {
        match that {
            RowAccumulatorItem::BITXOR(bitxor) => Ok(bitxor),
            _ => Err(()),
        }
    }
}

impl From<BitXorRowAccumulator> for RowAccumulatorItem {
    fn from(that: BitXorRowAccumulator) -> Self {
        RowAccumulatorItem::BITXOR(that)
    }
}

impl TryFrom<RowAccumulatorItem> for BoolAndRowAccumulator {
    type Error = ();

    fn try_from(that: RowAccumulatorItem) -> Result<Self, Self::Error> {
        match that {
            RowAccumulatorItem::BOOLAND(bool_and) => Ok(bool_and),
            _ => Err(()),
        }
    }
}

impl From<BoolAndRowAccumulator> for RowAccumulatorItem {
    fn from(that: BoolAndRowAccumulator) -> Self {
        RowAccumulatorItem::BOOLAND(that)
    }
}

impl TryFrom<RowAccumulatorItem> for BoolOrRowAccumulator {
    type Error = ();

    fn try_from(that: RowAccumulatorItem) -> Result<Self, Self::Error> {
        match that {
            RowAccumulatorItem::BOOLOR(bool_or) => Ok(bool_or),
            _ => Err(()),
        }
    }
}

impl From<BoolOrRowAccumulator> for RowAccumulatorItem {
    fn from(that: BoolOrRowAccumulator) -> Self {
        RowAccumulatorItem::BOOLOR(that)
    }
}
