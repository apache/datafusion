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

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int64Array, ListArray, ListBuilder,
    PrimitiveArray, PrimitiveBuilder,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{ArrowPrimitiveType, Field};
use datafusion_common::HashSet;
use datafusion_common::hash_utils::RandomState;
use datafusion_expr_common::groups_accumulator::{EmitTo, GroupsAccumulator};
use std::hash::Hash;
use std::mem::size_of;
use std::sync::Arc;

use crate::aggregate::groups_accumulator::accumulate::accumulate;

pub struct PrimitiveDistinctCountGroupsAccumulator<T: ArrowPrimitiveType>
where
    T::Native: Eq + Hash,
{
    seen: HashSet<(usize, T::Native), RandomState>,
    counts: Vec<i64>,
}

impl<T: ArrowPrimitiveType> PrimitiveDistinctCountGroupsAccumulator<T>
where
    T::Native: Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            seen: HashSet::default(),
            counts: Vec::new(),
        }
    }
}

impl<T: ArrowPrimitiveType> Default for PrimitiveDistinctCountGroupsAccumulator<T>
where
    T::Native: Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ArrowPrimitiveType + Send + std::fmt::Debug> GroupsAccumulator
    for PrimitiveDistinctCountGroupsAccumulator<T>
where
    T::Native: Eq + Hash,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> datafusion_common::Result<()> {
        debug_assert_eq!(values.len(), 1);
        self.counts.resize(total_num_groups, 0);
        let arr = values[0].as_primitive::<T>();
        accumulate(group_indices, arr, opt_filter, |group_idx, value| {
            if self.seen.insert((group_idx, value)) {
                self.counts[group_idx] += 1;
            }
        });
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> datafusion_common::Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);

        match emit_to {
            EmitTo::All => {
                self.seen.clear();
            }
            EmitTo::First(n) => {
                let mut remaining = HashSet::default();
                for (group_idx, value) in self.seen.drain() {
                    if group_idx >= n {
                        remaining.insert((group_idx - n, value));
                    }
                }
                self.seen = remaining;
            }
        }

        Ok(Arc::new(Int64Array::from(counts)))
    }

    fn state(&mut self, emit_to: EmitTo) -> datafusion_common::Result<Vec<ArrayRef>> {
        let num_emitted = match emit_to {
            EmitTo::All => self.counts.len(),
            EmitTo::First(n) => n,
        };

        // Prefix-sum counts[..num_emitted] into offsets
        let mut offsets = Vec::with_capacity(num_emitted + 1);
        offsets.push(0i32);
        let mut total = 0i32;
        for &c in &self.counts[..num_emitted] {
            total += c as i32;
            offsets.push(total);
        }

        let mut all_values = vec![T::Native::default(); total as usize];
        let mut cursors: Vec<i32> = offsets[..num_emitted].to_vec();

        if matches!(emit_to, EmitTo::All) {
            for (group_idx, value) in self.seen.drain() {
                let pos = cursors[group_idx] as usize;
                all_values[pos] = value;
                cursors[group_idx] += 1;
            }
            self.counts.clear();
        } else {
            let mut remaining = HashSet::default();
            for (group_idx, value) in self.seen.drain() {
                if group_idx < num_emitted {
                    let pos = cursors[group_idx] as usize;
                    all_values[pos] = value;
                    cursors[group_idx] += 1;
                } else {
                    remaining.insert((group_idx - num_emitted, value));
                }
            }
            self.seen = remaining;
            let _ = emit_to.take_needed(&mut self.counts);
        }

        let values_array = Arc::new(PrimitiveArray::<T>::new(
            ScalarBuffer::from(all_values),
            None,
        ));
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(T::DATA_TYPE, true)),
            OffsetBuffer::new(offsets.into()),
            values_array,
            None,
        );

        Ok(vec![Arc::new(list_array)])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> datafusion_common::Result<()> {
        debug_assert_eq!(values.len(), 1);
        self.counts.resize(total_num_groups, 0);
        let list_array = values[0].as_list::<i32>();
        let inner = list_array.values().as_primitive::<T>();
        let inner_values = inner.values();
        let offsets = list_array.offsets();

        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;
            for &value in &inner_values[start..end] {
                if self.seen.insert((group_idx, value)) {
                    self.counts[group_idx] += 1;
                }
            }
        }

        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> datafusion_common::Result<Vec<ArrayRef>> {
        debug_assert_eq!(values.len(), 1);
        let arr = values[0].as_primitive::<T>();

        let values_builder = PrimitiveBuilder::<T>::with_capacity(arr.len());
        let mut builder = ListBuilder::new(values_builder)
            .with_field(Arc::new(Field::new_list_field(T::DATA_TYPE, true)));

        for row in 0..arr.len() {
            let included = arr.is_valid(row)
                && opt_filter
                    .is_none_or(|filter| filter.is_valid(row) && filter.value(row));
            if included {
                builder.values().append_value(arr.value(row));
            }
            builder.append(true);
        }

        Ok(vec![Arc::new(builder.finish())])
    }
    fn size(&self) -> usize {
        size_of::<Self>()
            + self.seen.capacity() * (size_of::<(usize, T::Native)>() + size_of::<u64>())
            + self.counts.capacity() * size_of::<i64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::Int32Type;
    use datafusion_common::Result;

    #[test]
    fn convert_to_state_roundtrips_through_merge() -> Result<()> {
        let values = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(2),
            None,
            Some(3),
            Some(4),
            Some(5),
            Some(5),
        ])) as ArrayRef;
        let filter = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            None,
            Some(true),
            Some(true),
            Some(true),
        ]);
        let group_indices = vec![0usize, 1, 0, 1, 0, 0, 0, 0];

        let mut direct = PrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new();
        direct.update_batch(
            std::slice::from_ref(&values),
            &group_indices,
            Some(&filter),
            2,
        )?;
        let direct = direct.evaluate(EmitTo::All)?;

        let converter = PrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new();
        let state =
            converter.convert_to_state(std::slice::from_ref(&values), Some(&filter))?;
        assert_eq!(state[0].null_count(), 0);
        let mut merged = PrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new();
        merged.merge_batch(&state, &group_indices, 2)?;
        let merged = merged.evaluate(EmitTo::All)?;

        assert_eq!(
            direct.as_any().downcast_ref::<Int64Array>().unwrap(),
            merged.as_any().downcast_ref::<Int64Array>().unwrap()
        );
        Ok(())
    }

    #[test]
    fn convert_to_state_preserves_empty_and_filtered_rows() -> Result<()> {
        let converter = PrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new();
        let empty_values =
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())) as ArrayRef;
        let state =
            converter.convert_to_state(std::slice::from_ref(&empty_values), None)?;
        assert_eq!(state[0].len(), 0);
        assert_eq!(state[0].null_count(), 0);

        let values = Arc::new(Int32Array::from(vec![Some(1), Some(2), None])) as ArrayRef;
        let filter = BooleanArray::from(vec![Some(false), None, Some(false)]);
        let group_indices = vec![0usize, 1, 0];

        let state =
            converter.convert_to_state(std::slice::from_ref(&values), Some(&filter))?;
        assert_eq!(state[0].len(), values.len());
        assert_eq!(state[0].null_count(), 0);
        let list_state = state[0].as_list::<i32>();
        for row in 0..list_state.len() {
            assert_eq!(list_state.value_length(row), 0);
        }

        let mut merged = PrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new();
        merged.merge_batch(&state, &group_indices, 2)?;
        let result = merged.evaluate(EmitTo::All)?;
        assert_eq!(
            result.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![0, 0])
        );
        Ok(())
    }
}
