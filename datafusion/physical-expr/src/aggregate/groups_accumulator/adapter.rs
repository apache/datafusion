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
use datafusion_common::{
    utils::get_arrayref_at_indices, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::Accumulator;

/// An adpater that implements [`GroupsAccumulator`] for any [`Accumulator`]
///
/// While [`Accumulator`] are simpler to implement and can support
/// more general calculations (like retractable window functions),
/// they are not as fast as a specialized `GroupsAccumulator`. This
/// interface bridges the gap so the group by operator only operates
/// in terms of [`Accumulator`].
pub struct GroupsAccumulatorAdapter {
    factory: Box<dyn Fn() -> Result<Box<dyn Accumulator>> + Send>,

    /// state for each group, stored in group_index order
    states: Vec<AccumulatorState>,

    /// Current memory usage, in bytes.
    ///
    /// Note this is incrementally updated to avoid size() being a
    /// bottleneck, which we saw in earlier implementations.
    allocation_bytes: usize,
}

struct AccumulatorState {
    /// [`Accumulator`] that stores the per-group state
    accumulator: Box<dyn Accumulator>,

    // scratch space: indexes in the input array that will be fed to
    // this accumulator. Stores indexes as `u32` to match the arrow
    // `take` kernel input.
    indices: Vec<u32>,
}

impl AccumulatorState {
    fn new(accumulator: Box<dyn Accumulator>) -> Self {
        Self {
            accumulator,
            indices: vec![],
        }
    }

    /// Returns the amount of memory taken by this structre and its accumulator
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
        let mut new_self = Self {
            factory: Box::new(factory),
            states: vec![],
            allocation_bytes: 0,
        };
        new_self.reset_allocation();
        new_self
    }

    // Reset the allocation bytes to empty state
    fn reset_allocation(&mut self) {
        assert!(self.states.is_empty());
        self.allocation_bytes = std::mem::size_of::<GroupsAccumulatorAdapter>();
    }

    /// Ensure that self.accumulators has total_num_groups
    fn make_accumulators_if_needed(&mut self, total_num_groups: usize) -> Result<()> {
        // can't shrink
        assert!(total_num_groups >= self.states.len());
        let vec_size_pre =
            std::mem::size_of::<AccumulatorState>() * self.states.capacity();

        // instanatiate new accumulators
        let new_accumulators = total_num_groups - self.states.len();
        for _ in 0..new_accumulators {
            let accumulator = (self.factory)()?;
            let state = AccumulatorState::new(accumulator);
            self.allocation_bytes += state.size();
            self.states.push(state);
        }

        self.allocation_bytes +=
            std::mem::size_of::<AccumulatorState>() * self.states.capacity();
        self.allocation_bytes -= vec_size_pre;
        Ok(())
    }

    /// invokes f(accumulator, values) for each group that has values
    /// in group_indices.
    ///
    /// This function first reorders the input and filter so that
    /// values for each group_index are contiguous and then invokes f
    /// on the contiguous ranges, to minimize per-row overhead
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

        assert_eq!(values[0].len(), group_indices.len());

        // figure out which input rows correspond to which groups.
        // Note that self.state.indices starts empty for all groups
        // (it is cleared out below)
        for (idx, group_index) in group_indices.iter().enumerate() {
            self.states[*group_index].indices.push(idx as u32);
        }

        // groups_with_rows holds a list of group indexes that have
        // any rows that need to be accumulated, stored in order of
        // group_index

        let mut groups_with_rows = vec![];

        // batch_indices holds indices into values, each group is contiguous
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

        for (&group_idx, offsets) in iter {
            let state = &mut self.states[group_idx];
            let size_pre = state.size();

            let values_to_accumulate =
                slice_and_maybe_filter(&values, opt_filter.as_ref(), offsets)?;
            (f)(state.accumulator.as_mut(), &values_to_accumulate)?;

            // clear out the state so they are empty for next
            // iteration
            state.indices.clear();

            self.allocation_bytes += state.size();
            self.allocation_bytes -= size_pre;
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
        let states = std::mem::take(&mut self.states);

        let results: Vec<ScalarValue> = states
            .into_iter()
            .map(|state| state.accumulator.evaluate())
            .collect::<Result<_>>()?;

        let result = ScalarValue::iter_to_array(results);
        self.reset_allocation();
        result
    }

    fn state(&mut self) -> Result<Vec<ArrayRef>> {
        let states = std::mem::take(&mut self.states);

        // each accumulator produces a potential vector of values
        // which we need to form into columns
        let mut results: Vec<Vec<ScalarValue>> = vec![];

        for state in states {
            let accumulator_state = state.accumulator.state()?;
            results.resize_with(accumulator_state.len(), Vec::new);
            for (idx, state_val) in accumulator_state.into_iter().enumerate() {
                results[idx].push(state_val);
            }
        }

        // create an array for each intermediate column
        let arrays = results
            .into_iter()
            .map(ScalarValue::iter_to_array)
            .collect::<Result<Vec<_>>>()?;

        // double check each array has the same length (aka the
        // accumulator was implemented correctly
        if let Some(first_col) = arrays.get(0) {
            for arr in &arrays {
                assert_eq!(arr.len(), first_col.len())
            }
        }

        self.reset_allocation();
        Ok(arrays)
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
        self.allocation_bytes
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
                compute::filter(array, filter_array).map_err(DataFusionError::ArrowError)
            })
            .collect()
    } else {
        Ok(sliced_arrays)
    }
}
