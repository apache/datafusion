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

//! Adapter that makes [`GroupsAccumulator`] out of [`Accumulator`]

use super::GroupsAccumulator;
use arrow_array::{ArrayRef, BooleanArray};
use datafusion_common::Result;
use datafusion_expr::Accumulator;

/// An adpater that implements [`GroupsAccumulator`] for any [`Accumulator`]
///
/// While [`Accumulator`] are simpler to implement and can support
/// more general calculations (like retractable), but are not as fast
/// as `GroupsAccumulator`. This interface bridges the gap.
pub struct GroupsAccumulatorAdapter {
    factory: Box<dyn Fn() -> Result<Box<dyn Accumulator>> + Send>,

    /// [`Accumulators`] for each group, stored in group_index order
    accumulators: Vec<Box<dyn Accumulator>>,
}

impl GroupsAccumulatorAdapter {
    /// Create a new adapter that will create a new [`Accumulator`]
    /// for each group, using the specified factory function
    pub fn new<F>(factory: F) -> Self
    where
        F: Fn() -> Result<Box<dyn Accumulator>> + Send + 'static,
    {
        Self {
            factory: Box::new(factory),
            accumulators: vec![],
        }
    }
}

impl GroupsAccumulator for GroupsAccumulatorAdapter {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        todo!()
    }

    fn evaluate(&mut self) -> Result<ArrayRef> {
        todo!()
    }

    fn state(&mut self) -> Result<Vec<ArrayRef>> {
        todo!()
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        todo!()
    }

    fn size(&self) -> usize {
        self.accumulators.iter().map(|a| a.size()).sum::<usize>()
            //include the size of self and self.accumulators itself
            + self.accumulators.len() * std::mem::size_of::<Box<dyn Accumulator>>()
            + std::mem::size_of_val(&self.factory)
    }
}
