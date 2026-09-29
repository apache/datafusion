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

//! [`BlockedPrimitiveGroupsAccumulator`]: [`PrimitiveGroupsAccumulator`] with
//! state stored in blocks.
//!
//! [`PrimitiveGroupsAccumulator`]: super::prim_op::PrimitiveGroupsAccumulator

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use datafusion_common::Result;
use datafusion_expr_common::blocked_groups_accumulator::{
    BlockedEmitTo, BlockedGroupsAccumulator, BlocksIndex,
};

use super::blocked_null_state::BlockedNullState;
use super::blocked_vec::BlockedVec;
use super::prim_op::convert_to_state;

/// [`PrimitiveGroupsAccumulator`] with per-group values stored in a
/// [`BlockedVec`], for aggregates whose state is the same as the input type
/// (such as `SUM`).
///
/// F: The function to apply to two elements. The first argument is the
/// existing value and should be updated with the second value (e.g.
/// [`BitAndAssign`] style).
///
/// [`PrimitiveGroupsAccumulator`]: super::prim_op::PrimitiveGroupsAccumulator
/// [`BitAndAssign`]: std::ops::BitAndAssign
#[derive(Debug)]
pub struct BlockedPrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync + 'static,
{
    /// values per group, stored as the native type
    values: BlockedVec<T::Native>,

    /// The output type (needed for Decimal precision and scale)
    data_type: DataType,

    /// The starting value for new groups
    starting_value: T::Native,

    /// Track nulls in the input / filters
    null_state: BlockedNullState,

    /// Function that computes the primitive result
    prim_fn: F,
}

impl<T, F> BlockedPrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync + 'static,
{
    pub fn new(data_type: &DataType, block_size: usize, prim_fn: F) -> Self {
        Self {
            values: BlockedVec::new(block_size),
            data_type: data_type.clone(),
            null_state: BlockedNullState::new(block_size),
            starting_value: T::default_value(),
            prim_fn,
        }
    }

    /// Set the starting values for new groups
    pub fn with_starting_value(mut self, starting_value: T::Native) -> Self {
        self.starting_value = starting_value;
        self
    }

    fn take(&mut self, emit_to: BlockedEmitTo) -> Vec<Vec<T::Native>> {
        match emit_to {
            BlockedEmitTo::All => self.values.take_all(),
            BlockedEmitTo::NextBlock => {
                self.values.take_next_block().into_iter().collect()
            }
            BlockedEmitTo::First(n) => vec![self.values.take_first(n)],
        }
    }
}

impl<T, F> BlockedGroupsAccumulator for BlockedPrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync + 'static,
{
    fn block_size(&self) -> usize {
        self.values.block_size()
    }

    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[BlocksIndex],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<T>();
        let prim_fn = &self.prim_fn;
        self.null_state.accumulate(
            &mut self.values,
            total_num_groups,
            self.starting_value,
            group_indices,
            values,
            opt_filter,
            |value, new_value| prim_fn(value, new_value),
        );
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[BlocksIndex],
        total_num_groups: usize,
    ) -> Result<()> {
        // update / merge are the same
        self.update_batch(values, group_indices, None, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<ArrayRef>> {
        let blocks = self.take(emit_to);
        let nulls = self.null_state.build(emit_to);
        debug_assert_eq!(blocks.len(), nulls.len());
        Ok(blocks
            .into_iter()
            .zip(nulls)
            .map(|(values, nulls)| {
                // no copy
                Arc::new(
                    PrimitiveArray::<T>::new(values.into(), nulls)
                        .with_data_type(self.data_type.clone()),
                ) as ArrayRef
            })
            .collect())
    }

    fn state(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<Vec<ArrayRef>>> {
        Ok(self
            .evaluate(emit_to)?
            .into_iter()
            .map(|values| vec![values])
            .collect())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        convert_to_state::<T, F>(
            values,
            opt_filter,
            self.starting_value,
            &self.prim_fn,
            &self.data_type,
        )
    }

    fn size(&self) -> usize {
        self.values.allocated_size() + self.null_state.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::Int64Type;

    fn sum() -> BlockedPrimitiveGroupsAccumulator<Int64Type, fn(&mut i64, i64)> {
        BlockedPrimitiveGroupsAccumulator::new(&DataType::Int64, 4, |x, y| *x += y)
    }

    fn indices(flats: &[usize]) -> Vec<BlocksIndex> {
        flats
            .iter()
            .map(|&f| BlocksIndex::from_flat(f, 4))
            .collect()
    }

    fn values(arrays: &[ArrayRef]) -> Vec<Vec<Option<i64>>> {
        arrays
            .iter()
            .map(|a| a.as_primitive::<Int64Type>().iter().collect())
            .collect()
    }

    #[test]
    fn sums_across_blocks_with_nulls_for_unseen_groups() -> Result<()> {
        let mut acc = sum();
        let input: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));
        acc.update_batch(&[input], &indices(&[0, 5, 1, 5, 0]), None, 6)?;
        let partial: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
        acc.merge_batch(&[partial], &indices(&[2, 5]), 6)?;

        assert_eq!(
            values(&acc.evaluate(BlockedEmitTo::All)?),
            vec![vec![Some(6), None, Some(10), None], vec![None, Some(26)],]
        );
        assert_eq!(acc.size(), 0);
        Ok(())
    }

    #[test]
    fn all_valid_input_has_no_nulls() -> Result<()> {
        let mut acc = sum();
        let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        acc.update_batch(&[input], &indices(&[0, 1, 2, 3, 4]), None, 5)?;
        let out = acc.evaluate(BlockedEmitTo::NextBlock)?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].null_count(), 0);
        assert_eq!(values(&out), vec![vec![Some(1), Some(2), Some(3), Some(4)]]);
        let state = acc.state(BlockedEmitTo::All)?;
        assert_eq!(state.len(), 1);
        assert_eq!(values(&state[0]), vec![vec![Some(5)]]);
        Ok(())
    }
}
