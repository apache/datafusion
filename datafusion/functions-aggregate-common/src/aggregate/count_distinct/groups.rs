// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int64Array, ListArray, ListBuilder,
    PrimitiveArray, PrimitiveBuilder,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{ArrowPrimitiveType, Field};
use datafusion_common::HashSet;
use datafusion_common::hash_utils::RandomState;
use datafusion_expr_common::groups_accumulator::{
    DistinctCountValues, EmitTo, GroupSelection, GroupsAccumulator,
};
use std::hash::Hash;
use std::mem::size_of;
use std::sync::Arc;

use crate::aggregate::groups_accumulator::accumulate::accumulate;

// Limit inline checks when the average number of additional distinct values grows.
const INLINE_AVERAGE_DISTINCT_LIMIT: usize = 8;

pub struct PrimitiveDistinctCountGroupsAccumulator<T: ArrowPrimitiveType>
where
    T::Native: Eq + Hash,
{
    // While present, this buffer keeps each group's first non-null value out of
    // `seen`. None means all values have been promoted into the hash set.
    first_values: Option<Vec<T::Native>>,
    seen: HashSet<(usize, T::Native), RandomState>,
    counts: Vec<i64>,
}

impl<T: ArrowPrimitiveType> PrimitiveDistinctCountGroupsAccumulator<T>
where
    T::Native: Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            first_values: Some(Vec::new()),
            seen: HashSet::default(),
            counts: Vec::new(),
        }
    }

    fn insert_inline(
        first_value: &mut T::Native,
        count: &mut i64,
        seen: &mut HashSet<(usize, T::Native), RandomState>,
        group_idx: usize,
        value: T::Native,
    ) {
        if *count == 0 {
            *first_value = value;
            *count = 1;
        } else if *first_value != value && seen.insert((group_idx, value)) {
            *count += 1;
        }
    }

    fn promote_inline_values(&mut self) {
        if self.seen.len()
            > self
                .counts
                .len()
                .saturating_mul(INLINE_AVERAGE_DISTINCT_LIMIT)
            && let Some(first_values) = self.first_values.take()
        {
            // Pay for moving the first values once, then use the hash-only loop
            // for subsequent batches with large distinct sets.
            for (group_idx, (value, count)) in
                first_values.into_iter().zip(&self.counts).enumerate()
            {
                if *count > 0 {
                    self.seen.insert((group_idx, value));
                }
            }
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
        if let Some(first_values) = &mut self.first_values {
            first_values.resize(total_num_groups, T::Native::default());
            accumulate(group_indices, arr, opt_filter, |group_idx, value| {
                Self::insert_inline(
                    &mut first_values[group_idx],
                    &mut self.counts[group_idx],
                    &mut self.seen,
                    group_idx,
                    value,
                );
            });
            self.promote_inline_values();
        } else {
            accumulate(group_indices, arr, opt_filter, |group_idx, value| {
                if self.seen.insert((group_idx, value)) {
                    self.counts[group_idx] += 1;
                }
            });
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> datafusion_common::Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);
        if let Some(first_values) = &mut self.first_values {
            let _ = emit_to.take_needed(first_values);
        }

        match emit_to {
            EmitTo::All => {
                // Release the capacity, not just the entries: `size()` reports
                // capacity, and the aggregate streams rely on it dropping after
                // emitting everything.
                self.seen = HashSet::default();
                self.first_values = Some(Vec::new());
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

    fn evaluate_preserving(
        &mut self,
        selection: GroupSelection<'_>,
    ) -> datafusion_common::Result<ArrayRef> {
        selection.validate_num_groups(self.counts.len())?;
        let counts = selection
            .iter()
            .map(|index| self.counts[index])
            .collect::<Vec<_>>();
        Ok(Arc::new(Int64Array::from(counts)))
    }

    fn supports_evaluate_preserving(&self) -> bool {
        true
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
        let mut seen_emitted = 0;
        for &c in &self.counts[..num_emitted] {
            total += c as i32;
            offsets.push(total);
            seen_emitted += if self.first_values.is_some() {
                (c as usize).saturating_sub(1)
            } else {
                c as usize
            };
        }

        let mut all_values = vec![T::Native::default(); total as usize];
        let mut cursors: Vec<i32> = offsets[..num_emitted].to_vec();
        if let Some(first_values) = &mut self.first_values {
            let first_values = emit_to.take_needed(first_values);
            for (group_idx, value) in first_values.into_iter().enumerate() {
                if self.counts[group_idx] > 0 {
                    let pos = cursors[group_idx] as usize;
                    all_values[pos] = value;
                    cursors[group_idx] += 1;
                }
            }
        }

        if matches!(emit_to, EmitTo::All) {
            for (group_idx, value) in self.seen.drain() {
                let pos = cursors[group_idx] as usize;
                all_values[pos] = value;
                cursors[group_idx] += 1;
            }
            // Release the capacity, see `evaluate`.
            self.seen = HashSet::default();
            self.counts = Vec::new();
            self.first_values = Some(Vec::new());
        } else {
            let mut remaining = HashSet::with_capacity_and_hasher(
                self.seen.len().saturating_sub(seen_emitted),
                RandomState::default(),
            );
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

    fn supports_distinct_count_spill(&self) -> bool {
        true
    }

    fn take_distinct_count_values(
        &mut self,
        batch_size: usize,
    ) -> datafusion_common::Result<DistinctCountValues> {
        if batch_size == 0 {
            return datafusion_common::exec_err!(
                "Distinct spill batch size must be positive"
            );
        }
        let mut first_values = self.first_values.take().map(Vec::into_iter);
        let groups = std::mem::take(&mut self.counts).into_iter().enumerate();
        let initial = groups.filter_map(move |(group, count)| {
            let first = first_values.as_mut().and_then(Iterator::next);
            if count == 0 {
                Some((group, None))
            } else {
                first.map(|value| (group, Some(value)))
            }
        });
        let additional = std::mem::take(&mut self.seen)
            .into_iter()
            .map(|(group, value)| (group, Some(value)));
        let mut values = initial.chain(additional);
        self.first_values = Some(Vec::new());

        Ok(Box::new(std::iter::from_fn(move || {
            let mut groups = Vec::with_capacity(batch_size);
            let mut array = PrimitiveBuilder::<T>::with_capacity(batch_size);
            for (group, value) in values.by_ref().take(batch_size) {
                groups.push(group);
                array.append_option(value);
            }
            if groups.is_empty() {
                None
            } else {
                Some(Ok((groups, Arc::new(array.finish()) as ArrayRef)))
            }
        })))
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> datafusion_common::Result<()> {
        debug_assert_eq!(values.len(), 1);
        self.counts.resize(total_num_groups, 0);
        if let Some(first_values) = &mut self.first_values {
            first_values.resize(total_num_groups, T::Native::default());
        }
        let list_array = values[0].as_list::<i32>();
        let inner = list_array.values().as_primitive::<T>();
        let inner_values = inner.values();
        let offsets = list_array.offsets();

        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;
            if let Some(first_values) = &mut self.first_values {
                for &value in &inner_values[start..end] {
                    Self::insert_inline(
                        &mut first_values[group_idx],
                        &mut self.counts[group_idx],
                        &mut self.seen,
                        group_idx,
                        value,
                    );
                }
            } else {
                for &value in &inner_values[start..end] {
                    if self.seen.insert((group_idx, value)) {
                        self.counts[group_idx] += 1;
                    }
                }
            }
        }

        self.promote_inline_values();
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
            + self
                .first_values
                .as_ref()
                .map_or(0, |values| values.capacity() * size_of::<T::Native>())
            + self.seen.capacity() * (size_of::<(usize, T::Native)>() + size_of::<u64>())
            + self.counts.capacity() * size_of::<i64>()
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of_val;

    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::Int32Type;
    use datafusion_common::Result;

    #[test]
    fn flat_distinct_spill_is_bounded_and_preserves_empty_groups() -> Result<()> {
        for distinct_values in [3, 1000] {
            let mut accumulator =
                PrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new();
            let values = Arc::new(Int32Array::from_iter(
                (0..distinct_values)
                    .map(Some)
                    .chain([Some(7), None, Some(42)]),
            ));
            let groups = (0..distinct_values)
                .map(|_| 0)
                .chain([1, 2, 3])
                .collect::<Vec<_>>();
            let filter = BooleanArray::from_iter(
                (0..distinct_values + 2)
                    .map(|_| Some(true))
                    .chain([Some(false)]),
            );
            accumulator.update_batch(&[values], &groups, Some(&filter), 4)?;
            let drained = accumulator.take_distinct_count_values(7)?;
            assert_eq!(accumulator.size(), size_of_val(&accumulator));
            let mut observed = std::collections::BTreeSet::new();
            for batch in drained {
                let (groups, values) = batch?;
                assert!(!groups.is_empty() && groups.len() <= 7);
                for (group, value) in groups
                    .into_iter()
                    .zip(values.as_primitive::<Int32Type>().iter())
                {
                    assert!(observed.insert((group, value)));
                }
            }
            let expected = (0..distinct_values)
                .map(|value| (0, Some(value)))
                .chain([(1, Some(7)), (2, None), (3, None)])
                .collect();
            assert_eq!(observed, expected);
            accumulator.update_batch(
                &[Arc::new(Int32Array::from(vec![9, 9]))],
                &[0, 0],
                None,
                1,
            )?;
            assert_eq!(
                accumulator
                    .evaluate(EmitTo::All)?
                    .as_primitive::<arrow::datatypes::Int64Type>(),
                &Int64Array::from(vec![1])
            );
        }
        Ok(())
    }

    #[test]
    fn promoted_values_survive_partial_emission_and_merge() -> Result<()> {
        let mut accumulator = PrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new();
        let values = Arc::new(Int32Array::from_iter(
            (0..128).map(Some).chain([Some(7), None]),
        ));
        let groups = vec![0; 128].into_iter().chain([1, 2]).collect::<Vec<_>>();
        accumulator.update_batch(&[values], &groups, None, 3)?;
        assert!(accumulator.first_values.is_none());
        assert_eq!(
            accumulator
                .evaluate_preserving(GroupSelection::all(3))?
                .as_primitive::<arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![128, 1, 0])
        );

        let states = accumulator.state(EmitTo::First(1))?;
        let mut merged = PrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new();
        merged.merge_batch(&states, &[0], 1)?;
        assert!(merged.first_values.is_none());
        merged.merge_batch(&states, &[0], 1)?;
        merged.merge_batch(&states, &[1], 2)?;
        assert_eq!(
            merged
                .evaluate(EmitTo::All)?
                .as_primitive::<arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![128, 128])
        );

        // The promoted singleton and empty group now have indices 0 and 1.
        let values = Arc::new(Int32Array::from(vec![Some(7), Some(9), None]));
        accumulator.update_batch(&[values], &[0, 0, 1], None, 2)?;
        assert_eq!(
            accumulator
                .evaluate(EmitTo::First(1))?
                .as_primitive::<arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![2])
        );
        let values = Arc::new(Int32Array::from(vec![7, 7]));
        accumulator.update_batch(&[values], &[0, 0], None, 1)?;
        let states = accumulator.state(EmitTo::All)?;
        assert_eq!(accumulator.size(), size_of_val(&accumulator));
        merged.merge_batch(&states, &[0], 1)?;
        assert_eq!(
            merged
                .evaluate(EmitTo::All)?
                .as_primitive::<arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![1])
        );

        let values = Arc::new(Int32Array::from(vec![0, 0]));
        accumulator.update_batch(&[values], &[0, 0], None, 1)?;
        assert!(accumulator.first_values.is_some());
        assert_eq!(
            accumulator
                .evaluate(EmitTo::All)?
                .as_primitive::<arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![1])
        );
        Ok(())
    }

    #[test]
    fn partial_evaluation_keeps_first_values_aligned() -> Result<()> {
        let mut accumulator = PrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new();
        let values = Arc::new(Int32Array::from(vec![
            None,
            Some(0),
            Some(0),
            Some(1),
            Some(7),
            Some(7),
            Some(-1),
            Some(0),
            Some(2),
            None,
        ]));
        accumulator.update_batch(&[values], &[0, 1, 1, 1, 2, 2, 3, 3, 3, 4], None, 5)?;

        assert_eq!(
            accumulator
                .evaluate(EmitTo::First(2))?
                .as_primitive::<arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![0, 2])
        );

        // The former groups 2, 3 and 4 now have indices 0, 1 and 2.
        let values = Arc::new(Int32Array::from(vec![7, 8, -1, 5, 0, 9, 9]));
        accumulator.update_batch(&[values], &[0, 0, 1, 1, 2, 3, 3], None, 4)?;
        assert_eq!(
            accumulator
                .evaluate(EmitTo::All)?
                .as_primitive::<arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![2, 4, 1, 1])
        );
        assert_eq!(accumulator.size(), size_of_val(&accumulator));

        let values = Arc::new(Int32Array::from(vec![0, 0]));
        accumulator.update_batch(&[values], &[0, 0], None, 1)?;
        assert_eq!(
            accumulator
                .evaluate(EmitTo::All)?
                .as_primitive::<arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![1])
        );
        Ok(())
    }

    #[test]
    fn partial_state_roundtrips_with_overlapping_values() -> Result<()> {
        let mut accumulator = PrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new();
        let values = Arc::new(Int32Array::from(vec![
            None,
            Some(0),
            Some(1),
            Some(7),
            Some(8),
            None,
        ]));
        accumulator.update_batch(&[values], &[0, 1, 1, 2, 2, 3], None, 4)?;

        // Emit both an empty state and a state with inline and hashed values.
        let states = accumulator.state(EmitTo::First(2))?;
        let mut merged = PrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new();
        let values = Arc::new(Int32Array::from(vec![1, 2]));
        merged.update_batch(&[values], &[1, 1], None, 2)?;
        merged.merge_batch(&states, &[0, 1], 2)?;
        merged.merge_batch(&states, &[0, 1], 2)?;
        assert_eq!(
            merged
                .evaluate(EmitTo::All)?
                .as_primitive::<arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![0, 3])
        );

        // Update the shifted groups, preserving an all-NULL group's empty state.
        let values = Arc::new(Int32Array::from(vec![Some(7), Some(9), None]));
        accumulator.update_batch(&[values], &[0, 0, 1], None, 2)?;
        let states = accumulator.state(EmitTo::All)?;
        assert_eq!(accumulator.size(), size_of_val(&accumulator));
        merged.merge_batch(&states, &[0, 1], 2)?;
        assert_eq!(
            merged
                .evaluate(EmitTo::All)?
                .as_primitive::<arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![3, 0])
        );
        Ok(())
    }

    #[test]
    fn singleton_values_are_accounted_for() -> Result<()> {
        let mut accumulator = PrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new();
        let values = Arc::new(Int32Array::from(vec![0; 64]));
        let groups = (0..64).collect::<Vec<_>>();
        accumulator.update_batch(&[values], &groups, None, 64)?;
        assert!(
            accumulator.size()
                >= size_of_val(&accumulator) + 64 * (size_of::<i32>() + size_of::<i64>())
        );
        let states = accumulator.state(EmitTo::All)?;
        assert_eq!(accumulator.size(), size_of_val(&accumulator));
        accumulator.merge_batch(&states, &groups, 64)?;
        assert_eq!(
            accumulator
                .evaluate(EmitTo::All)?
                .as_primitive::<arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![1; 64])
        );
        Ok(())
    }

    #[test]
    fn preserving_reads_keep_distinct_state() -> Result<()> {
        let mut accumulator = PrimitiveDistinctCountGroupsAccumulator::<Int32Type>::new();
        let values = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(1),
            None,
            Some(3),
        ]));
        accumulator.update_batch(&[values], &[0, 0, 1, 2, 2], None, 4)?;

        let selection = GroupSelection::try_from_indices(&[2, 0, 3, 2], 4)?;
        let expected = Int64Array::from(vec![1, 2, 0, 1]);
        for _ in 0..2 {
            assert_eq!(
                accumulator
                    .evaluate_preserving(selection)?
                    .as_primitive::<arrow::datatypes::Int64Type>(),
                &expected
            );
        }
        assert!(
            accumulator
                .evaluate_preserving(GroupSelection::try_from_indices(&[], 4)?)?
                .is_empty()
        );

        let values = Arc::new(Int32Array::from(vec![2, 4, 1]));
        accumulator.update_batch(&[values], &[0, 0, 1], None, 4)?;
        assert_eq!(
            accumulator
                .evaluate_preserving(GroupSelection::all(4))?
                .as_primitive::<arrow::datatypes::Int64Type>(),
            &Int64Array::from(vec![3, 1, 1, 0])
        );
        assert!(accumulator.supports_evaluate_preserving());
        assert!(!accumulator.supports_state_preserving());
        Ok(())
    }

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
