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

use arrow::{array::AsArray, datatypes::ArrowPrimitiveType};
use arrow_array::{ArrayRef, BooleanArray, PrimitiveArray};
use datafusion_common::Result;

use crate::GroupsAccumulator;

use super::accumulate::NullState;

/// An accumulator that implements a single operation over
/// [`ArrowPrimitiveType`] where the accumulated state is the same as
/// the input type (such as [`Sum`])
///
/// F: The function to apply to two elements. The first argument is
/// the existing value and should be updated with the second value
/// (e.g. [`BitAndAssign`] style).
///
/// [`BitAndAssign`]: std::ops::BitAndAssign
#[derive(Debug)]
pub struct PrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync,
{
    /// values per group, stored as the native type
    values: Vec<T::Native>,

    /// Track nulls in the input / filters
    null_state: NullState,

    /// Function that computes the bitwise function
    bitop_fn: F,
}

impl<T, F> PrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync,
{
    pub fn new(bitop_fn: F) -> Self {
        Self {
            values: vec![],
            null_state: NullState::new(),
            bitop_fn,
        }
    }
}

impl<T, F> GroupsAccumulator for PrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values.get(0).unwrap().as_primitive::<T>();

        // update values
        self.values
            .resize_with(total_num_groups, || T::default_value());

        // NullState dispatches / handles tracking nulls and groups that saw no values
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                let value = &mut self.values[group_index];
                (self.bitop_fn)(value, new_value);
            },
        );

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ArrayRef> {
        let values = std::mem::take(&mut self.values);
        let nulls = self.null_state.build();
        let values = PrimitiveArray::<T>::new(values.into(), nulls); // no copy
        Ok(Arc::new(values))
    }

    fn state(&mut self) -> Result<Vec<ArrayRef>> {
        self.evaluate().map(|arr| vec![arr])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        // update / merge are the same
        self.update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn size(&self) -> usize {
        self.values.capacity() * std::mem::size_of::<T::Native>() + self.null_state.size()
    }
}
