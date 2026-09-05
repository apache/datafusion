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

use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::NullBuffer;
use arrow::compute;
use arrow::datatypes::ArrowPrimitiveType;
use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use datafusion_expr_common::groups_accumulator::{
    EmitTo, GroupSelection, GroupsAccumulator,
};

use super::accumulate::NullState;

/// An accumulator that implements a single operation over
/// [`ArrowPrimitiveType`] where the accumulated state is the same as
/// the input type (such as `Sum`)
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
    F: Fn(&mut T::Native, T::Native) + Send + Sync + 'static,
{
    /// values per group, stored as the native type
    values: Vec<T::Native>,

    /// The output type (needed for Decimal precision and scale)
    data_type: DataType,

    /// The starting value for new groups
    starting_value: T::Native,

    /// Track nulls in the input / filters
    null_state: NullState,

    /// Function that computes the primitive result
    prim_fn: F,
}

impl<T, F> PrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync + 'static,
{
    pub fn new(data_type: &DataType, prim_fn: F) -> Self {
        Self {
            values: vec![],
            data_type: data_type.clone(),
            null_state: NullState::new(),
            starting_value: T::default_value(),
            prim_fn,
        }
    }

    /// Set the starting values for new groups
    pub fn with_starting_value(mut self, starting_value: T::Native) -> Self {
        self.starting_value = starting_value;
        self
    }
}

impl<T, F> GroupsAccumulator for PrimitiveGroupsAccumulator<T, F>
where
    T: ArrowPrimitiveType + Send,
    F: Fn(&mut T::Native, T::Native) + Send + Sync + 'static,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<T>();

        // update values
        self.values.resize(total_num_groups, self.starting_value);

        // NullState dispatches / handles tracking nulls and groups that saw no values
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                // SAFETY: group_index is guaranteed to be in bounds
                let value = unsafe { self.values.get_unchecked_mut(group_index) };
                (self.prim_fn)(value, new_value);
            },
        );

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = emit_to.take_needed(&mut self.values);
        let nulls = self.null_state.build(emit_to);
        let values = PrimitiveArray::<T>::new(values.into(), nulls) // no copy
            .with_data_type(self.data_type.clone());
        Ok(Arc::new(values))
    }

    fn evaluate_preserving(&mut self, selection: GroupSelection<'_>) -> Result<ArrayRef> {
        selection.validate_num_groups(self.values.len())?;
        let values: Vec<T::Native> =
            selection.iter().map(|index| self.values[index]).collect();
        let nulls = self.null_state.build_preserving(selection)?;
        let values = PrimitiveArray::<T>::new(values.into(), nulls)
            .with_data_type(self.data_type.clone());
        Ok(Arc::new(values))
    }

    fn supports_evaluate_preserving(&self) -> bool {
        true
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.evaluate(emit_to).map(|arr| vec![arr])
    }

    fn state_preserving(
        &mut self,
        selection: GroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        self.evaluate_preserving(selection).map(|arr| vec![arr])
    }

    fn supports_state_preserving(&self) -> bool {
        true
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        // update / merge are the same
        self.update_batch(values, group_indices, None, total_num_groups)
    }

    /// Converts an input batch directly to a state batch
    ///
    /// The state is:
    /// - self.prim_fn for all non null, non filtered values
    /// - null otherwise
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = values[0].as_primitive::<T>().clone();

        // Initializing state with starting values
        let initial_state =
            PrimitiveArray::<T>::from_value(self.starting_value, values.len());

        // Recalculating values in case there is filter
        let values = match opt_filter {
            None => values,
            Some(filter) => {
                let (filter_values, filter_nulls) = filter.clone().into_parts();
                // Calculating filter mask as a result of bitand of filter, and converting it to null buffer
                let filter_bool = match filter_nulls {
                    Some(filter_nulls) => filter_nulls.inner() & &filter_values,
                    None => filter_values,
                };
                let filter_nulls = NullBuffer::from(filter_bool);

                // Rebuilding input values with a new nulls mask, which is equal to
                // the union of original nulls and filter mask
                let (dt, values_buf, original_nulls) = values.into_parts();
                let nulls_buf =
                    NullBuffer::union(original_nulls.as_ref(), Some(&filter_nulls));
                PrimitiveArray::<T>::new(values_buf, nulls_buf).with_data_type(dt)
            }
        };

        let state_values = compute::binary_mut(initial_state, &values, |mut x, y| {
            (self.prim_fn)(&mut x, y);
            x
        });
        let state_values = state_values
            .map_err(|_| {
                internal_datafusion_err!(
                    "initial_values underlying buffer must not be shared"
                )
            })?
            .map_err(DataFusionError::from)?
            .with_data_type(self.data_type.clone());

        Ok(vec![Arc::new(state_values)])
    }
    fn size(&self) -> usize {
        self.values.capacity() * size_of::<T::Native>() + self.null_state.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::Int64Type;

    #[test]
    fn preserving_reads_do_not_change_accumulator_state() -> Result<()> {
        let mut accumulator = PrimitiveGroupsAccumulator::<Int64Type, _>::new(
            &DataType::Int64,
            |current, value| *current += value,
        );
        let values = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
        accumulator.update_batch(&[values], &[0, 1, 2], None, 4)?;

        let selection = GroupSelection::try_from_indices(&[3, 0, 1, 1], 4)?;
        let expected = Int64Array::from(vec![None, Some(1), None, None]);
        for _ in 0..2 {
            let actual = accumulator.evaluate_preserving(selection)?;
            assert_eq!(actual.as_primitive::<Int64Type>(), &expected);
            let state = accumulator.state_preserving(selection)?;
            assert_eq!(state[0].as_primitive::<Int64Type>(), &expected);
        }

        // Group indices and unselected state remain valid after repeated reads.
        let values = Arc::new(Int64Array::from(vec![5, 7]));
        accumulator.update_batch(&[values], &[1, 3], None, 4)?;
        let expected = Int64Array::from(vec![Some(1), Some(5), Some(3), Some(7)]);
        let actual = accumulator.evaluate_preserving(GroupSelection::all(4))?;
        assert_eq!(actual.as_primitive::<Int64Type>(), &expected);

        // A destructive read still sees all state after preserving reads.
        let actual = accumulator.evaluate(EmitTo::All)?;
        assert_eq!(actual.as_primitive::<Int64Type>(), &expected);
        assert!(accumulator.supports_evaluate_preserving());
        assert!(accumulator.supports_state_preserving());
        Ok(())
    }
}
