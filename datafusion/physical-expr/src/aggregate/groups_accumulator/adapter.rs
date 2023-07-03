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
use arrow::{
    array::{AsArray, UInt32Builder},
    compute,
    datatypes::UInt32Type,
};
use arrow_array::{ArrayRef, BooleanArray, PrimitiveArray};
use datafusion_common::{utils::get_arrayref_at_indices, DataFusionError, Result};
use datafusion_expr::Accumulator;

/// An adpater that implements [`GroupsAccumulator`] for any [`Accumulator`]
///
/// While [`Accumulator`] are simpler to implement and can support
/// more general calculations (like retractable), but are not as fast
/// as `GroupsAccumulator`. This interface bridges the gap.
pub struct GroupsAccumulatorAdapter {
    factory: Box<dyn Fn() -> Result<Box<dyn Accumulator>> + Send>,

    /// [`Accumulators`] for each group, stored in group_index order
    states: Vec<AccumulatorState>,
}

struct AccumulatorState {
    /// [`Accumulators`]
    accumulator: Box<dyn Accumulator>,

    // scratch space for holding the indexes in the input array that
    // will be fed to this accumulator. Use u32 to match take kernel
    // input
    indices: Vec<u32>,
}

impl AccumulatorState {
    fn new(accumulator: Box<dyn Accumulator>) -> Self {
        Self {
            accumulator,
            indices: vec![],
        }
    }

    fn size(&self) -> usize {
        self.accumulator.size()
            + std::mem::size_of_val(self)
            + std::mem::size_of::<u32>() * self.indices.capacity()
    }
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
            states: vec![],
        }
    }

    /// Ensure that self.accumulators has total_num_groups
    fn make_accumulators_if_needed(&mut self, total_num_groups: usize) -> Result<()> {
        // can't shrink
        assert!(total_num_groups >= self.states.len());
        let new_accumulators = total_num_groups - self.states.len();
        for _ in 0..new_accumulators {
            let accumulator = (self.factory)()?;
            // todo update allocation
            self.states.push(AccumulatorState::new(accumulator));
        }
        Ok(())
    }

    /// invokes f(accumulator, values) for the correct slices of the
    /// input values of this array.
    ///
    /// This first reorders the input and filter so that values for group_indexes
    /// are contiguous and then invokes f on the contiguous ranges
    ///
    /// ```text
    /// ┌─────────┐   ┌─────────┐   ┌ ─ ─ ─ ─ ┐                       ┌─────────┐   ┌ ─ ─ ─ ─ ┐
    /// │ ┌─────┐ │   │ ┌─────┐ │     ┌─────┐              ┏━━━━━┓    │ ┌─────┐ │     ┌─────┐
    /// │ │  2  │ │   │ │ 200 │ │   │ │  t  │ │            ┃  0  ┃    │ │ 200 │ │   │ │  t  │ │
    /// │ ├─────┤ │   │ ├─────┤ │     ├─────┤              ┣━━━━━┫    │ ├─────┤ │     ├─────┤
    /// │ │  2  │ │   │ │ 100 │ │   │ │  f  │ │            ┃  0  ┃    │ │ 300 │ │   │ │  t  │ │
    /// │ ├─────┤ │   │ ├─────┤ │     ├─────┤              ┣━━━━━┫    │ ├─────┤ │     ├─────┤
    /// │ │  0  │ │   │ │ 200 │ │   │ │  t  │ │            ┃  1  ┃    │ │ 200 │ │   │ │NULL │ │
    /// │ ├─────┤ │   │ ├─────┤ │     ├─────┤   ────────▶  ┣━━━━━┫    │ ├─────┤ │     ├─────┤
    /// │ │  1  │ │   │ │ 200 │ │   │ │NULL │ │            ┃  2  ┃    │ │ 200 │ │   │ │  t  │ │
    /// │ ├─────┤ │   │ ├─────┤ │     ├─────┤              ┣━━━━━┫    │ ├─────┤ │     ├─────┤
    /// │ │  0  │ │   │ │ 300 │ │   │ │  t  │ │            ┃  2  ┃    │ │ 100 │ │   │ │  f  │ │
    /// │ └─────┘ │   │ └─────┘ │     └─────┘              ┗━━━━━┛    │ └─────┘ │     └─────┘
    /// └─────────┘   └─────────┘   └ ─ ─ ─ ─ ┘                       └─────────┘   └ ─ ─ ─ ─ ┘
    ///
    ///   values        opt_filter         logical group  values        opt_filter
    ///                                                 index
    /// ```
    fn invoke_per_accumulator<F>(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
        f: F,
    ) -> Result<()>
    where
        F: Fn(&mut dyn Accumulator, &[ArrayRef]) -> Result<()>,
    {
        self.make_accumulators_if_needed(total_num_groups)?;

        // reorderes the input and filter so that values for group_indexes are contiguous.
        // Then it invokes Accumulator::update / merge for each of those contiguous ranges
        assert_eq!(values[0].len(), group_indices.len());

        // figure out which input rows correspond to which groups
        for (idx, group_index) in group_indices.iter().enumerate() {
            self.states[*group_index].indices.push(idx as u32);
        }

        // groups_per_rows holds a list of group indexes that have
        // any rows that need to be accumulated, stored in order of group_index

        let mut groups_with_rows = vec![];

        // batch_indices holds indices in values, each group contiguously
        let mut batch_indices = UInt32Builder::with_capacity(0);

        // offsets[i] is index into batch_indices where the rows for
        // group_index i starts
        let mut offsets = vec![0];

        let mut offset_so_far = 0;
        for (group_index, state) in self.states.iter_mut().enumerate() {
            let indices = &state.indices;
            if indices.is_empty() {
                continue;
            }

            groups_with_rows.push(group_index);
            batch_indices.append_slice(indices);
            offset_so_far += indices.len();
            offsets.push(offset_so_far);
        }
        let batch_indices = batch_indices.finish();

        // reorder the values and opt_filter by batch_indices so that
        // all values for each group are contiguous, then invoke the
        // accumulator once per group with values
        let values = get_arrayref_at_indices(values, &batch_indices)?;
        let opt_filter = get_filter_at_indices(opt_filter, &batch_indices)?;

        // invoke each accumulator with the appropriate rows, first
        // pulling the input arguments for this group into their own
        // RecordBatch(es)
        let iter = groups_with_rows.iter().zip(offsets.windows(2));

        // TODO memory accounting
        let mut allocated = 0;
        for (group_idx, offsets) in iter {
            let state = &mut self.states[*group_idx as usize];

            //let size_pre = accumulator.size();

            let values_to_accumulate =
                slice_and_maybe_filter(&values, opt_filter.as_ref(), &offsets)?;

            (f)(state.accumulator.as_mut(), &values_to_accumulate)?;

            // clear out the state
            state.indices.clear();

            //let size_post = accumulator.size();
            //*allocated += size_post.saturating_sub(size_pre);
        }
        Ok(())
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
        self.invoke_per_accumulator(
            values,
            group_indices,
            opt_filter,
            total_num_groups,
            |accumulator, values_to_accumulate| {
                accumulator.update_batch(values_to_accumulate)
            },
        )?;
        Ok(())
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
        self.invoke_per_accumulator(
            values,
            group_indices,
            opt_filter,
            total_num_groups,
            |accumulator, values_to_accumulate| {
                accumulator.merge_batch(values_to_accumulate)
            },
        )?;
        Ok(())
    }

    fn size(&self) -> usize {
        // TODO should calculate size incrementally during update and just return value here
        self.states.iter().map(|a| a.size()).sum::<usize>()
            //include the size of self and self.accumulators itself
            + self.states.len() * std::mem::size_of::<AccumulatorState>()
            + std::mem::size_of_val(&self.factory)
    }
}

fn get_filter_at_indices(
    opt_filter: Option<&BooleanArray>,
    indices: &PrimitiveArray<UInt32Type>,
) -> Result<Option<ArrayRef>> {
    opt_filter
        .map(|filter| {
            compute::take(
                &filter, indices, None, // None: no index check
            )
        })
        .transpose()
        .map_err(DataFusionError::ArrowError)
}

// Copied from physical-plan
pub(crate) fn slice_and_maybe_filter(
    aggr_array: &[ArrayRef],
    filter_opt: Option<&ArrayRef>,
    offsets: &[usize],
) -> Result<Vec<ArrayRef>> {
    let (offset, length) = (offsets[0], offsets[1] - offsets[0]);
    let sliced_arrays: Vec<ArrayRef> = aggr_array
        .iter()
        .map(|array| array.slice(offset, length))
        .collect();

    if let Some(f) = filter_opt {
        let filter_array = f.slice(offset, length);
        let filter_array = filter_array.as_boolean();

        sliced_arrays
            .iter()
            .map(|array| {
                compute::filter(array, &filter_array).map_err(DataFusionError::ArrowError)
            })
            .collect()
    } else {
        Ok(sliced_arrays)
    }
}
