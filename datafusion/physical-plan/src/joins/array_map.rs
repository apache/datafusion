// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow_schema::DataType;
use num_traits::AsPrimitive;
use std::mem::size_of;

use crate::joins::chain::{grouped_matched_indices, traverse_chain};
use crate::joins::join_hash_map::ProbeOffset;
use crate::joins::join_hash_map::ProbeOffset::{AtRow, MidChain};
use crate::joins::join_hash_map::group_rows_range;
use crate::metrics::Gauge;
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::ArrowNumericType;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_execution::memory_pool::MemoryReservation;

/// A macro to downcast only supported integer types (up to 64-bit) and invoke a generic function.
///
/// Usage: `downcast_supported_integer!(data_type => (Method, arg1, arg2, ...))`
///
/// The `Method` must be an associated method of [`ArrayMap`] that is generic over
/// `<T: ArrowNumericType>` and allow `T::Native: AsPrimitive<u64>`.
macro_rules! downcast_supported_integer {
    ($DATA_TYPE:expr => ($METHOD:ident $(, $ARGS:expr)*)) => {
        match $DATA_TYPE {
            arrow::datatypes::DataType::Int8 => ArrayMap::$METHOD::<arrow::datatypes::Int8Type>($($ARGS),*),
            arrow::datatypes::DataType::Int16 => ArrayMap::$METHOD::<arrow::datatypes::Int16Type>($($ARGS),*),
            arrow::datatypes::DataType::Int32 => ArrayMap::$METHOD::<arrow::datatypes::Int32Type>($($ARGS),*),
            arrow::datatypes::DataType::Int64 => ArrayMap::$METHOD::<arrow::datatypes::Int64Type>($($ARGS),*),
            arrow::datatypes::DataType::UInt8 => ArrayMap::$METHOD::<arrow::datatypes::UInt8Type>($($ARGS),*),
            arrow::datatypes::DataType::UInt16 => ArrayMap::$METHOD::<arrow::datatypes::UInt16Type>($($ARGS),*),
            arrow::datatypes::DataType::UInt32 => ArrayMap::$METHOD::<arrow::datatypes::UInt32Type>($($ARGS),*),
            arrow::datatypes::DataType::UInt64 => ArrayMap::$METHOD::<arrow::datatypes::UInt64Type>($($ARGS),*),
            _ => {
                return internal_err!(
                    "Unsupported type for ArrayMap: {:?}",
                    $DATA_TYPE
                );
            }
        }
    };
}

/// A dense map for single-column integer join keys within a limited range.
///
/// Maps join keys to build-side indices using direct array indexing, at the
/// slot `val - min_val_in_build_side`, see [`KeyRows`] for how a slot leads to
/// its build rows.
///
/// NULL values are ignored on both the build side and the probe side.
///
/// # Handling Negative Numbers with `wrapping_sub`
///
/// This implementation supports signed integer ranges (e.g., `[-5, 5]`) efficiently by
/// treating them as `u64` (Two's Complement) and relying on the bitwise properties of
/// wrapping arithmetic (`wrapping_sub`).
///
/// In Two's Complement representation, `a_signed - b_signed` produces the same bit pattern
/// as `a_unsigned.wrapping_sub(b_unsigned)` (modulo 2^N). This allows us to perform
/// range calculations and zero-based index mapping uniformly for both signed and unsigned
/// types without branching.
///
/// ## Examples
///
/// Consider an `Int64` range `[-5, 5]`.
/// * `min_val (-5)` casts to `u64`: `...11111011` (`u64::MAX - 4`)
/// * `max_val (5)` casts to `u64`: `...00000101` (`5`)
///
/// **1. Range Calculation**
///
/// ```text
/// In modular arithmetic, this is equivalent to:
///   (5 - (2^64 - 5)) mod 2^64
/// = (5 - 2^64 + 5) mod 2^64
/// = (10 - 2^64) mod 2^64
/// = 10
///
/// ```
/// The resulting `range` (10) correctly represents the size of the interval `[-5, 5]`.
///
/// **2. Index Lookup (in `get_matched_indices_with_limit_offset`)**
///
/// For a probe value of `0` (which is stored as `0u64`):
/// ```text
/// In modular arithmetic, this is equivalent to:
///   (0 - (2^64 - 5)) mod 2^64
/// = (-2^64 + 5) mod 2^64
/// = 5
/// ```
/// This correctly maps `-5` to index `0`, `0` to index `5`, etc.
#[derive(Debug)]
pub struct ArrayMap {
    // The build rows of each key
    rows: KeyRows,
    // min val in buildSide
    offset: u64,
    num_of_distinct_key: usize,
}

/// The build rows of the keys of an [`ArrayMap`], by slot (`key - offset`).
#[derive(Debug)]
enum KeyRows {
    /// No key has more than one build row: `data[slot]` is the key's build
    /// row + 1, or `0` if the key is absent.
    Unique { data: Vec<u32> },
    /// `data[slot]` is the key's first build row + 1, or `0` if the key is
    /// absent, and `next[row]` is the key's build row after `row` + 1, or `0`
    /// after its last, as in the chain of a `JoinHashMapType`.
    Chain { data: Vec<u32>, next: Vec<u32> },
    /// The keys have at least two build rows for each slot, so the rows were
    /// sorted by key when the map was built, see [`ArrayMap::sort_rows`]: the
    /// build rows of the key in `slot` are `rows[starts[slot]..starts[slot + 1]]`,
    /// in build order, and `starts` has one more element than there are slots.
    Sorted { starts: Vec<u32>, rows: Vec<u32> },
    /// The rows of a chain grouped by [`ArrayMap::group_rows`]: `data[slot]`
    /// is the position of the key's group in `groups`, or `0` if the key is
    /// absent, and a group holds its number of rows and then its rows, in
    /// build order, as in a `JoinHashMapType`. `groups[0]` is unused, so that
    /// no group is at `0`.
    Grouped { data: Vec<u32>, groups: Vec<u32> },
}

impl ArrayMap {
    pub fn is_supported_type(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        )
    }

    pub(crate) fn key_to_u64(v: &ScalarValue) -> Option<u64> {
        match v {
            ScalarValue::Int8(Some(v)) => Some(*v as u64),
            ScalarValue::Int16(Some(v)) => Some(*v as u64),
            ScalarValue::Int32(Some(v)) => Some(*v as u64),
            ScalarValue::Int64(Some(v)) => Some(*v as u64),
            ScalarValue::UInt8(Some(v)) => Some(*v as u64),
            ScalarValue::UInt16(Some(v)) => Some(*v as u64),
            ScalarValue::UInt32(Some(v)) => Some(*v as u64),
            ScalarValue::UInt64(Some(v)) => Some(*v),
            _ => None,
        }
    }

    /// Estimates the maximum memory usage for an `ArrayMap` with the given parameters.
    ///
    pub fn estimate_memory_size(min_val: u64, max_val: u64, num_rows: usize) -> usize {
        let range = Self::calculate_range(min_val, max_val);
        if range >= usize::MAX as u64 {
            return usize::MAX;
        }
        // One slot more than the key range, for the end of the last key's rows
        // when they are sorted
        let slots = (range + 2) as usize;
        slots
            .saturating_mul(size_of::<u32>())
            .saturating_add(num_rows.saturating_mul(size_of::<u32>()))
    }

    pub fn calculate_range(min_val: u64, max_val: u64) -> u64 {
        max_val.wrapping_sub(min_val)
    }

    #[inline]
    fn key_to_index(key: u64, offset: u64, data_len: usize) -> Option<usize> {
        let idx = key.wrapping_sub(offset);
        if idx < data_len as u64 {
            Some(idx as usize)
        } else {
            None
        }
    }

    /// Creates a new [`ArrayMap`] from the given array of join keys.
    ///
    /// Note: This function processes only the non-null values in the input `array`,
    /// ignoring any rows where the key is `NULL`.
    ///
    pub(crate) fn try_new(array: &ArrayRef, min_val: u64, max_val: u64) -> Result<Self> {
        let range = Self::calculate_range(min_val, max_val);
        if range >= usize::MAX as u64 {
            return internal_err!("ArrayMap key range is too large to be allocated.");
        }
        let size = (range + 1) as usize;

        // With two rows for every slot, the keys have two rows each on average
        // whatever they are, so the rows are sorted by key without a chain.
        if array.len() - array.null_count() >= size.saturating_mul(2) {
            return downcast_supported_integer!(
                array.data_type() => (sort_rows, array, min_val, size)
            );
        }

        let mut data: Vec<u32> = vec![0; size];
        let mut next: Vec<u32> = vec![];
        let mut num_of_distinct_key = 0;

        downcast_supported_integer!(
            array.data_type() => (
                fill_data,
                array,
                min_val,
                &mut data,
                &mut next,
                &mut num_of_distinct_key
            )
        )?;

        let rows = if next.is_empty() {
            KeyRows::Unique { data }
        } else {
            KeyRows::Chain { data, next }
        };
        Ok(Self {
            rows,
            offset: min_val,
            num_of_distinct_key,
        })
    }

    fn fill_data<T: ArrowNumericType>(
        array: &ArrayRef,
        offset_val: u64,
        data: &mut [u32],
        next: &mut Vec<u32>,
        num_of_distinct_key: &mut usize,
    ) -> Result<()>
    where
        T::Native: AsPrimitive<u64>,
    {
        let arr = array.as_primitive::<T>();
        let nulls = arr.nulls();
        // Iterate in reverse to maintain FIFO order when there are duplicate keys.
        // Read the values directly rather than with the array's iterator, whose
        // `Option` for each row the compiler may keep in memory, which slows
        // the loop down.
        for (i, &val) in arr.values().iter().enumerate().rev() {
            if !nulls.is_some_and(|nulls| nulls.is_null(i)) {
                let key: u64 = val.as_();
                let Some(idx) = Self::key_to_index(key, offset_val, data.len()) else {
                    return internal_err!("failed build Array idx >= data.len()");
                };

                if data[idx] != 0 {
                    if next.is_empty() {
                        *next = vec![0; array.len()]
                    }
                    next[i] = data[idx]
                } else {
                    *num_of_distinct_key += 1;
                }
                data[idx] = (i) as u32 + 1;
            }
        }
        Ok(())
    }

    /// Builds an [`ArrayMap`] with the rows sorted by key with a counting
    /// sort, see [`KeyRows::Sorted`].
    fn sort_rows<T: ArrowNumericType>(
        array: &ArrayRef,
        offset: u64,
        size: usize,
    ) -> Result<Self>
    where
        T::Native: AsPrimitive<u64>,
    {
        let arr = array.as_primitive::<T>();
        let nulls = arr.nulls();
        let is_null = |i| nulls.is_some_and(|nulls| nulls.is_null(i));
        // First count each key's rows, with one more slot for the end of the
        // last key's rows...
        let mut starts: Vec<u32> = vec![0; size + 1];
        let mut num_of_distinct_key = 0;
        for (i, &val) in arr.values().iter().enumerate() {
            if is_null(i) {
                continue;
            }
            let Some(idx) = Self::key_to_index(val.as_(), offset, size) else {
                return internal_err!("failed build Array idx >= data.len()");
            };
            if starts[idx] == 0 {
                num_of_distinct_key += 1;
            }
            starts[idx] += 1;
        }
        // ...then turn the counts into the end of each key's rows...
        let mut end = 0;
        for count in &mut starts[..size] {
            end += *count;
            *count = end;
        }
        starts[size] = end;
        // ...and place the rows from the last to the first, which moves the
        // end of each key's rows back to their start and keeps them in order.
        let mut rows = vec![0; end as usize];
        for (i, &val) in arr.values().iter().enumerate().rev() {
            if is_null(i) {
                continue;
            }
            // In range: every key was checked when it was counted.
            let idx = val.as_().wrapping_sub(offset) as usize;
            starts[idx] -= 1;
            rows[starts[idx] as usize] = i as u32;
        }

        Ok(Self {
            rows: KeyRows::Sorted { starts, rows },
            offset,
            num_of_distinct_key,
        })
    }

    pub fn num_of_distinct_key(&self) -> usize {
        self.num_of_distinct_key
    }

    /// Stores the build rows of each key next to each other, under the same
    /// conditions as [`JoinHashMapType::group_rows`] does for a hash map, and
    /// in the same layout. `keys` are the build keys the map was created from.
    /// Returns whether the rows were grouped.
    ///
    /// [`JoinHashMapType::group_rows`]: crate::joins::join_hash_map::JoinHashMapType::group_rows
    pub(crate) fn group_rows(
        &mut self,
        keys: &ArrayRef,
        reservation: &MemoryReservation,
        mem_used: &Gauge,
    ) -> Result<bool> {
        let KeyRows::Chain { .. } = &self.rows else {
            return Ok(false);
        };
        // Every row with a key is in the chain. The groups hold the number of
        // rows of each key and the rows, after the unused position `0`.
        let rows = keys.len() - keys.null_count();
        let len = 1 + self.num_of_distinct_key + rows;
        if rows < self.num_of_distinct_key * 2 || u32::try_from(len).is_err() {
            return Ok(false);
        }
        // The groups, and the slots of the keys while they are placed
        let size = (len + self.num_of_distinct_key) * size_of::<u32>();
        if reservation.try_grow(size).is_err() {
            return Ok(false);
        }
        mem_used.add(size);
        let KeyRows::Chain { mut data, next } =
            std::mem::replace(&mut self.rows, KeyRows::Unique { data: vec![] })
        else {
            unreachable!("the rows are in a chain");
        };
        let mut groups = vec![0; len];
        downcast_supported_integer!(
            keys.data_type() => (
                place_rows,
                keys,
                self.offset,
                self.num_of_distinct_key,
                &mut data,
                &mut groups
            )
        )?;
        let freed = (next.capacity() + self.num_of_distinct_key) * size_of::<u32>();
        drop(next);
        self.rows = KeyRows::Grouped { data, groups };
        reservation.shrink(freed);
        mem_used.sub(freed);
        Ok(true)
    }

    /// Places the rows of `keys` in `groups`, which have room for them, see
    /// [`KeyRows::Grouped`], and points `data`, which holds the first row
    /// (+1) of each key as [`Self::fill_data`] leaves it, at their groups. The
    /// chain is not followed: the rows are counted and then placed in two
    /// passes over `keys` that touch only the slots of the keys in it, so that
    /// grouping costs the same whether the slots are dense or sparse.
    fn place_rows<T: ArrowNumericType>(
        keys: &ArrayRef,
        offset: u64,
        num_of_distinct_key: usize,
        data: &mut [u32],
        groups: &mut [u32],
    ) -> Result<()>
    where
        T::Native: AsPrimitive<u64>,
    {
        let arr = keys.as_primitive::<T>();
        let nulls = arr.nulls();
        // Every key was checked against the slots when the chain was built.
        let slots = arr
            .values()
            .iter()
            .enumerate()
            .filter(|&(i, _)| !nulls.is_some_and(|nulls| nulls.is_null(i)))
            .map(|(i, &val)| (i, val.as_().wrapping_sub(offset) as usize));
        // Count the rows of each key in its slot, which holds the key's first
        // row (+1) until this pass reaches that row: the count of a key's rows
        // so far is always below the next row (+1)...
        let mut present = Vec::with_capacity(num_of_distinct_key);
        for (i, slot) in slots.clone() {
            if data[slot] as usize == i + 1 {
                present.push(slot);
                data[slot] = 1;
            } else {
                data[slot] += 1;
            }
        }
        // ...then point each key at its group...
        let mut pos = 1;
        for slot in present {
            let count = data[slot];
            data[slot] = pos;
            pos += 1 + count;
        }
        // ...and place the rows in build order, counting them again
        for (i, slot) in slots {
            let group = data[slot] as usize;
            groups[group] += 1;
            groups[group + groups[group] as usize] = i as u32;
        }
        Ok(())
    }

    /// Returns whether the rows were sorted by key when the map was built,
    /// see [`KeyRows::Sorted`].
    pub(crate) fn is_sorted(&self) -> bool {
        matches!(self.rows, KeyRows::Sorted { .. })
    }

    /// Returns the memory usage of this [`ArrayMap`] in bytes.
    pub fn size(&self) -> usize {
        match &self.rows {
            KeyRows::Unique { data } => data.capacity() * size_of::<u32>(),
            KeyRows::Chain { data, next } => {
                (data.capacity() + next.capacity()) * size_of::<u32>()
            }
            KeyRows::Sorted { starts, rows } => {
                (starts.capacity() + rows.capacity()) * size_of::<u32>()
            }
            KeyRows::Grouped { data, groups } => {
                (data.capacity() + groups.capacity()) * size_of::<u32>()
            }
        }
    }

    pub fn get_matched_indices_with_limit_offset(
        &self,
        prob_side_keys: &[ArrayRef],
        limit: usize,
        current_offset: ProbeOffset,
        probe_indices: &mut Vec<u32>,
        build_indices: &mut Vec<u64>,
    ) -> Result<Option<ProbeOffset>> {
        if prob_side_keys.len() != 1 {
            return internal_err!(
                "ArrayMap expects 1 join key, but got {}",
                prob_side_keys.len()
            );
        }
        let array = &prob_side_keys[0];

        downcast_supported_integer!(
            array.data_type() => (
                lookup_and_get_indices,
                self,
                array,
                limit,
                current_offset,
                probe_indices,
                build_indices
            )
        )
    }

    fn lookup_and_get_indices<T: ArrowNumericType>(
        &self,
        array: &ArrayRef,
        limit: usize,
        current_offset: ProbeOffset,
        probe_indices: &mut Vec<u32>,
        build_indices: &mut Vec<u64>,
    ) -> Result<Option<ProbeOffset>>
    where
        T::Native: Copy + AsPrimitive<u64>,
    {
        probe_indices.clear();
        build_indices.clear();

        let arr = array.as_primitive::<T>();

        let have_null = arr.null_count() > 0;

        match &self.rows {
            KeyRows::Unique { data } => {
                debug_assert!(
                    matches!(current_offset, AtRow { .. }),
                    "unique keys cannot have mid-chain"
                );
                let rows = match current_offset {
                    AtRow { row } => row,
                    MidChain { row, .. } => row,
                };
                for prob_idx in rows..arr.len() {
                    if build_indices.len() == limit {
                        return Ok(Some(AtRow { row: prob_idx }));
                    }

                    // short circuit
                    if have_null && arr.is_null(prob_idx) {
                        continue;
                    }
                    // SAFETY: prob_idx is guaranteed to be within bounds by the loop range.
                    let prob_val: u64 = unsafe { arr.value_unchecked(prob_idx) }.as_();
                    let Some(slot) =
                        Self::key_to_index(prob_val, self.offset, data.len())
                    else {
                        continue;
                    };
                    let build_value = data[slot];
                    if build_value == 0 {
                        continue;
                    }
                    build_indices.push((build_value - 1) as u64);
                    probe_indices.push(prob_idx as u32);
                }
            }
            KeyRows::Chain { data, next } => {
                let mut remaining_output = limit;
                let to_skip = match current_offset {
                    // `AtRow` indicates that `idx` processing hasn't been started
                    AtRow { row: idx } => idx,
                    // Otherwise, process remaining `idx` matches by traversing `next_chain`,
                    // to start with the next index
                    MidChain {
                        row: idx,
                        next: next_idx,
                    } => {
                        let is_last = idx == arr.len() - 1;
                        if let Some(next_offset) = traverse_chain(
                            next,
                            idx,
                            next_idx as u32,
                            &mut remaining_output,
                            probe_indices,
                            build_indices,
                            is_last,
                        ) {
                            return Ok(Some(next_offset));
                        }
                        idx + 1
                    }
                };

                for prob_side_idx in to_skip..arr.len() {
                    if remaining_output == 0 {
                        return Ok(Some(AtRow { row: prob_side_idx }));
                    }

                    if have_null && arr.is_null(prob_side_idx) {
                        continue;
                    }

                    let is_last = prob_side_idx == arr.len() - 1;

                    // SAFETY: prob_idx is guaranteed to be within bounds by the loop range.
                    let prob_val: u64 =
                        unsafe { arr.value_unchecked(prob_side_idx) }.as_();
                    let Some(slot) =
                        Self::key_to_index(prob_val, self.offset, data.len())
                    else {
                        continue;
                    };
                    let build_idx = data[slot];
                    if build_idx == 0 {
                        continue;
                    }

                    if let Some(offset) = traverse_chain(
                        next,
                        prob_side_idx,
                        build_idx,
                        &mut remaining_output,
                        probe_indices,
                        build_indices,
                        is_last,
                    ) {
                        return Ok(Some(offset));
                    }
                }
            }
            KeyRows::Sorted { starts, rows } => {
                let slots = starts.len() - 1;
                return Ok(grouped_matched_indices(
                    rows,
                    arr.len(),
                    |row| {
                        if have_null && arr.is_null(row) {
                            return None;
                        }
                        // SAFETY: `row` is within bounds, below `arr.len()`.
                        let prob_val: u64 = unsafe { arr.value_unchecked(row) }.as_();
                        let slot = Self::key_to_index(prob_val, self.offset, slots)?;
                        let (start, end) =
                            (starts[slot] as usize, starts[slot + 1] as usize);
                        (start != end).then_some((start, end))
                    },
                    limit,
                    current_offset,
                    probe_indices,
                    build_indices,
                ));
            }
            KeyRows::Grouped { data, groups } => {
                return Ok(grouped_matched_indices(
                    groups,
                    arr.len(),
                    |row| {
                        if have_null && arr.is_null(row) {
                            return None;
                        }
                        // SAFETY: `row` is within bounds, below `arr.len()`.
                        let prob_val: u64 = unsafe { arr.value_unchecked(row) }.as_();
                        let slot = Self::key_to_index(prob_val, self.offset, data.len())?;
                        let pos = data[slot] as usize;
                        (pos != 0).then(|| group_rows_range(groups, pos))
                    },
                    limit,
                    current_offset,
                    probe_indices,
                    build_indices,
                ));
            }
        }
        Ok(None)
    }

    pub fn contain_keys(&self, probe_side_keys: &[ArrayRef]) -> Result<BooleanArray> {
        if probe_side_keys.len() != 1 {
            return internal_err!(
                "ArrayMap join expects 1 join key, but got {}",
                probe_side_keys.len()
            );
        }
        let array = &probe_side_keys[0];

        downcast_supported_integer!(
            array.data_type() => (
                contain_keys_helper,
                self,
                array
            )
        )
    }

    fn contain_keys_helper<T: ArrowNumericType>(
        &self,
        array: &ArrayRef,
    ) -> Result<BooleanArray>
    where
        T::Native: AsPrimitive<u64>,
    {
        let arr = array.as_primitive::<T>();
        // One loop for each layout, so that the probe rows don't branch on it
        let buffer = match &self.rows {
            KeyRows::Unique { data }
            | KeyRows::Chain { data, .. }
            | KeyRows::Grouped { data, .. } => {
                self.collect_contains(arr, data.len(), |slot| data[slot] != 0)
            }
            KeyRows::Sorted { starts, .. } => {
                self.collect_contains(arr, starts.len() - 1, |slot| {
                    starts[slot] != starts[slot + 1]
                })
            }
        };
        Ok(BooleanArray::new(buffer, None))
    }

    /// Returns which rows of `arr` have a key in one of the first `slots` for
    /// which `has_rows` is `true`.
    #[inline(always)]
    fn collect_contains<T: ArrowNumericType>(
        &self,
        arr: &PrimitiveArray<T>,
        slots: usize,
        has_rows: impl Fn(usize) -> bool,
    ) -> BooleanBuffer
    where
        T::Native: AsPrimitive<u64>,
    {
        BooleanBuffer::collect_bool(arr.len(), |i| {
            if arr.is_null(i) {
                return false;
            }
            // SAFETY: i is within bounds [0, arr.len())
            let key: u64 = unsafe { arr.value_unchecked(i) }.as_();
            Self::key_to_index(key, self.offset, slots).is_some_and(&has_rows)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::array::Int64Array;
    use arrow::array::UInt64Array;
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool, UnboundedMemoryPool,
    };
    use std::sync::Arc;

    #[test]
    fn test_array_map_limit_offset_duplicate_elements() -> Result<()> {
        let build: ArrayRef = Arc::new(Int32Array::from(vec![1, 1, 2]));
        let map = ArrayMap::try_new(&build, 1, 2)?;
        let probe = [Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef];

        let mut prob_idx = Vec::new();
        let mut build_idx = Vec::new();
        let mut next = Some(AtRow { row: 0 });
        let mut results = vec![];

        while let Some(o) = next {
            next = map.get_matched_indices_with_limit_offset(
                &probe,
                1,
                o,
                &mut prob_idx,
                &mut build_idx,
            )?;
            results.push((prob_idx.clone(), build_idx.clone(), next));
        }

        let expected = vec![
            (vec![0], vec![0], Some(MidChain { row: 0, next: 2 })),
            (vec![0], vec![1], Some(AtRow { row: 1 })),
            (vec![1], vec![2], None),
        ];
        assert_eq!(results, expected);
        Ok(())
    }

    /// Collects every match of `probe`, `limit` at a time.
    fn all_matches(
        map: &ArrayMap,
        probe: &[ArrayRef],
        limit: usize,
    ) -> Result<Vec<(u32, u64)>> {
        let mut matches = vec![];
        let (mut probe_indices, mut build_indices) = (vec![], vec![]);
        let mut offset = AtRow { row: 0 };
        loop {
            let next = map.get_matched_indices_with_limit_offset(
                probe,
                limit,
                offset,
                &mut probe_indices,
                &mut build_indices,
            )?;
            assert!(probe_indices.len() <= limit);
            matches.extend(
                probe_indices
                    .iter()
                    .copied()
                    .zip(build_indices.iter().copied()),
            );
            match next {
                Some(next) => offset = next,
                None => return Ok(matches),
            }
        }
    }

    /// Builds an [`ArrayMap`] from `build_keys` and groups its rows if they
    /// qualify, and asserts that `layout` holds for its rows, that the
    /// reservation covers it, and that each probe row matches the build rows
    /// with its key, in build order.
    fn assert_array_map(
        build_keys: &[Option<i32>],
        layout: fn(&KeyRows) -> bool,
    ) -> Result<()> {
        let keys = build_keys.iter().flatten();
        let (min, max) = (*keys.clone().min().unwrap(), *keys.clone().max().unwrap());
        let build: ArrayRef = Arc::new(Int32Array::from(build_keys.to_vec()));
        let mut map = ArrayMap::try_new(&build, min as u64, max as u64)?;
        // As `array_map_key_range` reserves the map before it is built
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("test").register(&pool);
        reservation.grow(ArrayMap::estimate_memory_size(
            min as u64,
            max as u64,
            build_keys.len(),
        ));
        let mem_used = Gauge::new();
        mem_used.add(map.size());
        map.group_rows(&build, &reservation, &mem_used)?;
        assert!(layout(&map.rows), "{build_keys:?}: {:?}", map.rows);
        assert_eq!(mem_used.value(), map.size());
        assert!(map.size() <= reservation.size());
        let distinct = keys.collect::<std::collections::HashSet<_>>().len();
        assert_eq!(map.num_of_distinct_key(), distinct);

        // The build keys, NULLs included, last to first, then keys out of
        // range, and one in range that may have no rows
        let mut probe_keys: Vec<Option<i32>> = build_keys.iter().rev().copied().collect();
        probe_keys.extend([Some(min - 1), Some(max + 1), Some(i32::midpoint(min, max))]);
        let probe = [Arc::new(Int32Array::from(probe_keys.clone())) as ArrayRef];

        let expected: Vec<(u32, u64)> = probe_keys
            .iter()
            .enumerate()
            .flat_map(|(p, probe_key)| {
                build_keys
                    .iter()
                    .enumerate()
                    .filter_map(move |(b, build_key)| {
                        (probe_key.is_some() && probe_key == build_key)
                            .then_some((p as u32, b as u64))
                    })
            })
            .collect();
        assert!(!expected.is_empty());
        for limit in [1, 2, 3, 7, 13, 8192] {
            assert_eq!(all_matches(&map, &probe, limit)?, expected, "limit {limit}");
        }

        let contains = map.contain_keys(&probe)?;
        for (p, probe_key) in probe_keys.iter().enumerate() {
            let has_match = probe_key.is_some() && build_keys.contains(probe_key);
            assert_eq!(contains.value(p), has_match, "probe row {p}");
        }
        Ok(())
    }

    #[test]
    fn test_array_map_layouts() -> Result<()> {
        // Each key once
        let unique: Vec<_> = (0..50).map(Some).collect();
        assert_array_map(&unique, |rows| matches!(rows, KeyRows::Unique { .. }))?;
        // 113 rows over 100 keys, with NULLs: fewer than two rows each on
        // average, so the rows stay in a chain
        let short: Vec<_> = (0..120).map(|i| (i % 17 != 3).then_some(i % 100)).collect();
        assert_array_map(&short, |rows| matches!(rows, KeyRows::Chain { .. }))?;
        // Two rows each on average, over a range of 1,000 keys, so the chain
        // is grouped
        let sparse = [Some(0), Some(999), Some(0), Some(999), Some(0), Some(5)];
        assert_array_map(&sparse, |rows| matches!(rows, KeyRows::Grouped { .. }))?;
        // Keys -5 to 17 that repeat, with NULLs: more than two rows for every
        // key in the range, so they are sorted when the map is built
        let dense: Vec<_> = (0..300)
            .map(|i| (i % 11 != 0).then_some((i * 37 % 23) - 5))
            .collect();
        assert_array_map(&dense, |rows| matches!(rows, KeyRows::Sorted { .. }))
    }

    #[test]
    fn test_array_map_rows_stay_chained_without_memory() -> Result<()> {
        // Two rows each for keys 1 and 5, fewer than two for each slot
        let build: ArrayRef = Arc::new(Int32Array::from(vec![1, 5, 1, 5]));
        let mut map = ArrayMap::try_new(&build, 1, 5)?;
        // A pool with room for the map only
        let size = ArrayMap::estimate_memory_size(1, 5, 4);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(size));
        let reservation = MemoryConsumer::new("test").register(&pool);
        reservation.try_grow(size)?;
        let mem_used = Gauge::new();

        assert!(!map.group_rows(&build, &reservation, &mem_used)?);
        assert!(matches!(map.rows, KeyRows::Chain { .. }));
        assert_eq!(reservation.size(), size);
        assert_eq!(mem_used.value(), 0);
        let probe = [Arc::new(Int32Array::from(vec![1, 5])) as ArrayRef];
        assert_eq!(
            all_matches(&map, &probe, 8192)?,
            vec![(0, 0), (0, 2), (1, 1), (1, 3)]
        );
        Ok(())
    }

    #[test]
    fn test_array_map_with_limit_and_misses() -> Result<()> {
        let build: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let map = ArrayMap::try_new(&build, 1, 2)?;
        let probe = [Arc::new(Int32Array::from(vec![10, 1, 2])) as ArrayRef];

        let (mut p_idx, mut b_idx) = (vec![], vec![]);
        // Skip 10, find 1, next is 2
        let next = map.get_matched_indices_with_limit_offset(
            &probe,
            1,
            AtRow { row: 0 },
            &mut p_idx,
            &mut b_idx,
        )?;
        assert_eq!(p_idx, vec![1]);
        assert_eq!(b_idx, vec![0]);
        assert_eq!(next, Some(AtRow { row: 2 }));

        // Find 2, end
        let next = map.get_matched_indices_with_limit_offset(
            &probe,
            1,
            next.unwrap(),
            &mut p_idx,
            &mut b_idx,
        )?;
        assert_eq!(p_idx, vec![2]);
        assert_eq!(b_idx, vec![1]);
        assert!(next.is_none());
        Ok(())
    }

    #[test]
    fn test_array_map_with_build_duplicates_and_misses() -> Result<()> {
        let build_array: ArrayRef = Arc::new(Int32Array::from(vec![1, 1]));
        let array_map = ArrayMap::try_new(&build_array, 1, 1)?;
        // prob: 10(m), 1(h1, h2), 20(m), 1(h1, h2)
        let probe_array: ArrayRef = Arc::new(Int32Array::from(vec![10, 1, 20, 1]));
        let prob_side_keys = [probe_array];

        let mut prob_indices = Vec::new();
        let mut build_indices = Vec::new();

        // batch_size=3, should get 2 matches from first '1' and 1 match from second '1'
        let result_offset = array_map.get_matched_indices_with_limit_offset(
            &prob_side_keys,
            3,
            AtRow { row: 0 },
            &mut prob_indices,
            &mut build_indices,
        )?;

        assert_eq!(prob_indices, vec![1, 1, 3]);
        assert_eq!(build_indices, vec![0, 1, 0]);
        // Two rows for the one slot, so they are sorted: resume at position 1
        // of the sorted rows.
        assert_eq!(result_offset, Some(MidChain { row: 3, next: 1 }));
        Ok(())
    }

    #[test]
    fn test_array_map_rejects_large_out_of_range_probe_key() -> Result<()> {
        let build: ArrayRef =
            Arc::new(UInt64Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        let map = ArrayMap::try_new(&build, 0, 10)?;

        assert_eq!(ArrayMap::key_to_index(3, 0, 11), Some(3));

        // Pick a key for which the computed bucket offset is larger than
        // u32::MAX but has low 32 bits equal to 3. It must be bounds-checked
        // before casting to usize, otherwise 32-bit targets can truncate it
        // into range.
        let out_of_range_key = (1_u64 << 32) + 3;
        assert_eq!(ArrayMap::key_to_index(out_of_range_key, 0, 11), None);

        let probe = [Arc::new(UInt64Array::from(vec![
            Some(3),
            Some(out_of_range_key),
            Some(11),
            None,
        ])) as ArrayRef];

        let mut matched_probe_indices = vec![];
        let mut matched_build_indices = vec![];
        let next = map.get_matched_indices_with_limit_offset(
            &probe,
            10,
            AtRow { row: 0 },
            &mut matched_probe_indices,
            &mut matched_build_indices,
        )?;
        assert_eq!(matched_probe_indices, vec![0]);
        assert_eq!(matched_build_indices, vec![3]);
        assert!(next.is_none());

        let contains = map.contain_keys(&probe)?;
        assert!(contains.value(0));
        assert!(!contains.value(1));
        assert!(!contains.value(2));
        assert!(!contains.value(3));

        Ok(())
    }

    #[test]
    fn test_array_map_i64_with_negative_and_positive_numbers() -> Result<()> {
        // Build array with a mix of negative and positive i64 values, no duplicates
        let build_array: ArrayRef = Arc::new(Int64Array::from(vec![-5, 0, 5, -2, 3, 10]));
        let min_val = -5_i128;
        let max_val = 10_i128;

        let array_map = ArrayMap::try_new(&build_array, min_val as u64, max_val as u64)?;

        // Probe array
        let probe_array: ArrayRef = Arc::new(Int64Array::from(vec![0, -5, 10, -1]));
        let prob_side_keys = [Arc::clone(&probe_array)];

        let mut prob_indices = Vec::new();
        let mut build_indices = Vec::new();

        // Call once to get all matches
        let result_offset = array_map.get_matched_indices_with_limit_offset(
            &prob_side_keys,
            10, // A batch size larger than number of probes
            AtRow { row: 0 },
            &mut prob_indices,
            &mut build_indices,
        )?;

        // Expected matches, in probe-side order:
        // Probe 0 (value 0) -> Build 1 (value 0)
        // Probe 1 (value -5) -> Build 0 (value -5)
        // Probe 2 (value 10) -> Build 5 (value 10)
        let expected_prob_indices = vec![0, 1, 2];
        let expected_build_indices = vec![1, 0, 5];

        assert_eq!(prob_indices, expected_prob_indices);
        assert_eq!(build_indices, expected_build_indices);
        assert!(result_offset.is_none());

        Ok(())
    }
}
