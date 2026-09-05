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
use datafusion_expr_common::groups_accumulator::{BlockedEmitTo, BlockedGroupSelection, BlockedGroupsAccumulator, BlocksIndex, EmitTo, GroupSelection, GroupsAccumulator};
use std::hash::Hash;
use std::mem::size_of;
use std::sync::Arc;
use datafusion_expr_common::blocked_helpers::CopyItemBlockedVecBuilder;
use crate::aggregate::groups_accumulator::accumulate::accumulate;

fn convert_to_state<T: ArrowPrimitiveType>(
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
                // Release the capacity, not just the entries: `size()` reports
                // capacity, and the aggregate streams rely on it dropping after
                // emitting everything.
                self.seen = HashSet::default();
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
            // Release the capacity, see `evaluate`.
            self.seen = HashSet::default();
            self.counts = Vec::new();
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
        convert_to_state::<T>(values, opt_filter)
    }

    fn size(&self) -> usize {
        size_of::<Self>()
          + self.seen.capacity() * (size_of::<(usize, T::Native)>() + size_of::<u64>())
          + self.counts.capacity() * size_of::<i64>()
    }
}

pub struct PrimitiveDistinctCountBlockedGroupsAccumulator<T: ArrowPrimitiveType>
where
  T::Native: Eq + Hash,
{
    seen: HashSet<(BlocksIndex, T::Native), RandomState>,
    counts: CopyItemBlockedVecBuilder<true, i64>,
}

impl<T: ArrowPrimitiveType + Send + std::fmt::Debug> PrimitiveDistinctCountBlockedGroupsAccumulator<T>
where
  T::Native: Eq + Hash,
{
    pub fn new(block_size: usize) -> Self {
        Self {
            seen: HashSet::default(),
            counts: CopyItemBlockedVecBuilder::new(block_size),
        }
    }

    fn ensure_groups(&mut self, total_num_groups: usize) {
        let prev_groups = self.counts.len();
        assert!(prev_groups <= total_num_groups);
        self.counts.push_value_n(0, total_num_groups - prev_groups);
    }

    fn evaluate_next_block(&mut self, update_seen: bool) -> Option<ArrayRef> {
        let Some(counts) = self.counts.take_block() else {
            return None;
        };

        if update_seen {
            let mut remaining = HashSet::default();

            for (group_idx, value) in self.seen.drain() {
                if let Some(group_idx) = group_idx.prev_block_checked() {
                    // SAFETY: this is unique as it came from unique set
                    unsafe {remaining.insert_unique_unchecked((group_idx, value)); }
                }
            }
            self.seen = remaining;
        }

        Some(Arc::new(Int64Array::from(counts)))
    }

    fn state_block_or_n<const IS_FIRST_N: bool>(&mut self, n: usize) -> Option<Vec<ArrayRef>> {
        let batch_size = self.batch_size();

        if IS_FIRST_N {
            assert!(n < self.counts.len(), "n ({n}) must be less than len ({})", self.counts.len());
            assert!(n < batch_size, "n ({n}) must be less than batch size ({batch_size})");
        } else {
            assert_eq!(n, batch_size);
        }

        // Prefix-sum counts[..num_emitted] into offsets
        let mut offsets = Vec::with_capacity(n + 1);
        offsets.push(0i32);
        let mut total = 0i32;
        let counts_block = if IS_FIRST_N {
            self.counts.take_n(n, None::<std::iter::Empty<_>>)
        } else {
            self.counts.take_block()?
        };
        for c in counts_block {
            total += c as i32;
            offsets.push(total);
        }

        let mut all_values = vec![T::Native::default(); total as usize];
        let mut cursors: Vec<i32> = offsets[..offsets.len() - 1].to_vec();

        let mut remaining = HashSet::default();
        for (group_idx, value) in self.seen.drain() {
            let updated_group_idx = if IS_FIRST_N {
                group_idx.sub_flat_checked(n, batch_size)
            } else {
                group_idx.prev_block_checked()
            };
            if let Some(group_idx) = updated_group_idx {
                // SAFETY: safe as this came from unique set and all group indexes are shifted by the same amount
                unsafe { remaining.insert_unique_unchecked((group_idx, value)) };
            } else {
                let pos = cursors[group_idx.index_in_block()] as usize;
                all_values[pos] = value;
                cursors[group_idx.index_in_block()] += 1;
            }
        }
        self.seen = remaining;

        Some(Self::build_state_from_parts(all_values, offsets))
    }

    fn state_first_n(&mut self, n: usize) -> Vec<ArrayRef> {
        self.state_block_or_n::<true>(n).expect("must have at least one in progress block")
    }

    fn state_next_block(&mut self) -> Option<Vec<ArrayRef>> {
        self.state_block_or_n::<false>(self.batch_size())
    }

    fn state_all(&mut self) -> Vec<Vec<ArrayRef>> {
        let batch_size = self.batch_size();
        let counts_len = self.counts.len();
        let counts_blocks = self.counts.take_all();

        let mut blocks_all_values = Vec::with_capacity(counts_blocks.len());
        let mut blocks_offsets = Vec::with_capacity(counts_blocks.len());
        let mut flat_cursors = Vec::with_capacity(counts_len);
        // SAFETY: this is save as we just reserved for it
        unsafe {
            flat_cursors.set_len(counts_len)
        };

        let mut flat_cursor_index = 0;

        for block in counts_blocks {
            // Prefix-sum counts[..num_emitted] into offsets
            let mut offsets = Vec::with_capacity(block.len() + 1);
            offsets.push(0i32);
            let mut total = 0i32;

            for c in block {
                total += c as i32;
                offsets.push(total);
            }

            blocks_all_values.push(vec![T::Native::default(); total as usize]);
            {
                let end = offsets.len() - 1;
                flat_cursors[flat_cursor_index..flat_cursor_index + end].copy_from_slice(&offsets[..end]);
                flat_cursor_index += end;
            }
            blocks_offsets.push(offsets);
        }

        {
            for (group_idx, value) in self.seen.drain() {
                let pos = &mut flat_cursors[group_idx.into_index_in_fixed_block_size(batch_size)];
                blocks_all_values[group_idx.block_index()][*pos as usize] = value;
                *pos += 1;
            }
        }

        drop(flat_cursors);

        blocks_all_values
          .into_iter()
          .zip(blocks_offsets.into_iter())
          .map(|(values, offsets)| Self::build_state_from_parts(values, offsets))
          .collect::<Vec<_>>()
    }

    fn build_state_from_parts(values: Vec<T::Native>, offsets: Vec<i32>) -> Vec<ArrayRef> {
        let values_array = Arc::new(PrimitiveArray::<T>::new(
            ScalarBuffer::from(values),
            None,
        ));
        let list_array = ListArray::new(
            // TODO - this has a bug with data types that the const cant represent like decimal or timestamp
            Arc::new(Field::new_list_field(T::DATA_TYPE, true)),
            OffsetBuffer::new(offsets.into()),
            values_array,
            None,
        );

        vec![Arc::new(list_array) as ArrayRef]
    }
}

impl<T: ArrowPrimitiveType + Send + std::fmt::Debug> BlockedGroupsAccumulator
for PrimitiveDistinctCountBlockedGroupsAccumulator<T>
where
  T::Native: Eq + Hash,
{
    fn batch_size(&self) -> usize {
        self.counts.block_size()
    }

    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[BlocksIndex],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> datafusion_common::Result<()> {
        debug_assert_eq!(values.len(), 1);

        self.ensure_groups(total_num_groups);
        let arr = values[0].as_primitive::<T>();
        accumulate(group_indices, arr, opt_filter, |group_idx, value| {
            if self.seen.insert((group_idx, value)) {
                self.counts[group_idx] += 1;
            }
        });
        Ok(())
    }

    fn evaluate(&mut self, emit_to: BlockedEmitTo) -> datafusion_common::Result<Vec<ArrayRef>> {
        match emit_to {
            BlockedEmitTo::All => {
                self.seen.clear();

                let mut blocks = vec![];

                while let Some(block) = self.evaluate_next_block(false) {
                    blocks.push(block);
                }

                Ok(blocks)
            }
            BlockedEmitTo::NextBlock => {
                Ok(self.evaluate_next_block(true).into_iter().collect::<Vec<_>>())
            }
            BlockedEmitTo::First(n) => {
                assert!(n < self.counts.len(), "n ({n}) must be less than len ({})", self.counts.len());
                assert!(n < self.batch_size(), "n ({n}) must be less than batch size ({})", self.batch_size());

                let first = self.counts.take_n(n, None::<std::iter::Empty<_>>);

                let mut remaining = HashSet::default();

                let batch_size = self.batch_size();

                for (group_idx, value) in self.seen.drain() {
                    if let Some(group_idx) = group_idx.sub_flat_checked(n, batch_size) {
                        // SAFETY: this is unique as it came from unique set
                        unsafe {remaining.insert_unique_unchecked((group_idx, value)); }
                    }
                }
                self.seen = remaining;

                Ok(vec![Arc::new(Int64Array::from(first))])
            }
        }
    }

    fn evaluate_preserving(
        &mut self,
        selection: BlockedGroupSelection<'_>,
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

    fn state(&mut self, emit_to: BlockedEmitTo) -> datafusion_common::Result<Vec<Vec<ArrayRef>>> {
        match emit_to {
            BlockedEmitTo::All => {
                Ok(self.state_all())
            }
            BlockedEmitTo::NextBlock => {
                Ok(self.state_next_block().into_iter().collect::<Vec<_>>())
            }
            BlockedEmitTo::First(n) => {
                Ok(vec![self.state_first_n(n)])

            }
        }
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[BlocksIndex],
        total_num_groups: usize,
    ) -> datafusion_common::Result<()> {
        debug_assert_eq!(values.len(), 1);
        self.ensure_groups(total_num_groups);
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
        convert_to_state::<T>(values, opt_filter)
    }

    fn size(&self) -> usize {
        size_of::<Self>()
          + self.seen.capacity() * (size_of::<(BlocksIndex, T::Native)>() + size_of::<u64>())
          + self.counts.allocated_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::Int32Type;
    use datafusion_common::Result;

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
