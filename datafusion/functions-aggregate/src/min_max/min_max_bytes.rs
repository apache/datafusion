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
    Array, ArrayRef, AsArray, BinaryBuilder, BinaryViewBuilder, BooleanArray,
    LargeBinaryBuilder, LargeStringBuilder, StringBuilder, StringViewBuilder,
};
use arrow::datatypes::DataType;
use datafusion_common::{internal_err, HashMap, Result};
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls::apply_filter_as_nulls;
use hashbrown::hash_map::Entry;
use std::mem::size_of;
use std::sync::Arc;

/// Implements fast Min/Max [`GroupsAccumulator`] for "bytes" types ([`StringArray`],
/// [`BinaryArray`], [`StringViewArray`], etc)
///
/// This implementation dispatches to the appropriate specialized code in
/// [`MinMaxBytesState`] based on data type and comparison function
///
/// [`StringArray`]: arrow::array::StringArray
/// [`BinaryArray`]: arrow::array::BinaryArray
/// [`StringViewArray`]: arrow::array::StringViewArray
#[derive(Debug)]
pub(crate) struct MinMaxBytesAccumulator {
    /// Inner data storage.
    inner: MinMaxBytesState,
    /// if true, is `MIN` otherwise is `MAX`
    is_min: bool,
}

impl MinMaxBytesAccumulator {
    /// Create a new accumulator for computing `min(val)`
    pub fn new_min(data_type: DataType) -> Self {
        Self {
            inner: MinMaxBytesState::new(data_type),
            is_min: true,
        }
    }

    /// Create a new accumulator fo computing `max(val)`
    pub fn new_max(data_type: DataType) -> Self {
        Self {
            inner: MinMaxBytesState::new(data_type),
            is_min: false,
        }
    }
}

impl GroupsAccumulator for MinMaxBytesAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let array = &values[0];
        assert_eq!(array.len(), group_indices.len());
        assert_eq!(array.data_type(), &self.inner.data_type);

        // apply filter if needed
        let array = apply_filter_as_nulls(array, opt_filter)?;

        // dispatch to appropriate kernel / specialized implementation
        fn string_min(a: &[u8], b: &[u8]) -> bool {
            // safety: only called from this function, which ensures a and b come
            // from an array with valid utf8 data
            unsafe {
                let a = std::str::from_utf8_unchecked(a);
                let b = std::str::from_utf8_unchecked(b);
                a < b
            }
        }
        fn string_max(a: &[u8], b: &[u8]) -> bool {
            // safety: only called from this function, which ensures a and b come
            // from an array with valid utf8 data
            unsafe {
                let a = std::str::from_utf8_unchecked(a);
                let b = std::str::from_utf8_unchecked(b);
                a > b
            }
        }
        fn binary_min(a: &[u8], b: &[u8]) -> bool {
            a < b
        }

        fn binary_max(a: &[u8], b: &[u8]) -> bool {
            a > b
        }

        fn str_to_bytes<'a>(
            it: impl Iterator<Item = Option<&'a str>>,
        ) -> impl Iterator<Item = Option<&'a [u8]>> {
            it.map(|s| s.map(|s| s.as_bytes()))
        }

        match (self.is_min, &self.inner.data_type) {
            // Utf8/LargeUtf8/Utf8View Min
            (true, &DataType::Utf8) => self.inner.update_batch(
                str_to_bytes(array.as_string::<i32>().iter()),
                group_indices,
                total_num_groups,
                string_min,
            ),
            (true, &DataType::LargeUtf8) => self.inner.update_batch(
                str_to_bytes(array.as_string::<i64>().iter()),
                group_indices,
                total_num_groups,
                string_min,
            ),
            (true, &DataType::Utf8View) => self.inner.update_batch(
                str_to_bytes(array.as_string_view().iter()),
                group_indices,
                total_num_groups,
                string_min,
            ),

            // Utf8/LargeUtf8/Utf8View Max
            (false, &DataType::Utf8) => self.inner.update_batch(
                str_to_bytes(array.as_string::<i32>().iter()),
                group_indices,
                total_num_groups,
                string_max,
            ),
            (false, &DataType::LargeUtf8) => self.inner.update_batch(
                str_to_bytes(array.as_string::<i64>().iter()),
                group_indices,
                total_num_groups,
                string_max,
            ),
            (false, &DataType::Utf8View) => self.inner.update_batch(
                str_to_bytes(array.as_string_view().iter()),
                group_indices,
                total_num_groups,
                string_max,
            ),

            // Binary/LargeBinary/BinaryView Min
            (true, &DataType::Binary) => self.inner.update_batch(
                array.as_binary::<i32>().iter(),
                group_indices,
                total_num_groups,
                binary_min,
            ),
            (true, &DataType::LargeBinary) => self.inner.update_batch(
                array.as_binary::<i64>().iter(),
                group_indices,
                total_num_groups,
                binary_min,
            ),
            (true, &DataType::BinaryView) => self.inner.update_batch(
                array.as_binary_view().iter(),
                group_indices,
                total_num_groups,
                binary_min,
            ),

            // Binary/LargeBinary/BinaryView Max
            (false, &DataType::Binary) => self.inner.update_batch(
                array.as_binary::<i32>().iter(),
                group_indices,
                total_num_groups,
                binary_max,
            ),
            (false, &DataType::LargeBinary) => self.inner.update_batch(
                array.as_binary::<i64>().iter(),
                group_indices,
                total_num_groups,
                binary_max,
            ),
            (false, &DataType::BinaryView) => self.inner.update_batch(
                array.as_binary_view().iter(),
                group_indices,
                total_num_groups,
                binary_max,
            ),

            _ => internal_err!(
                "Unexpected combination for MinMaxBytesAccumulator: ({:?}, {:?})",
                self.is_min,
                self.inner.data_type
            ),
        }
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (data_capacity, min_maxes) = self.inner.emit_to(emit_to);

        // Convert the Vec of bytes to a vec of Strings (at no cost)
        fn bytes_to_str(
            min_maxes: Vec<Option<Vec<u8>>>,
        ) -> impl Iterator<Item = Option<String>> {
            min_maxes.into_iter().map(|opt| {
                opt.map(|bytes| {
                    // Safety: only called on data added from update_batch which ensures
                    // the input type matched the output type
                    unsafe { String::from_utf8_unchecked(bytes) }
                })
            })
        }

        let result: ArrayRef = match self.inner.data_type {
            DataType::Utf8 => {
                let mut builder =
                    StringBuilder::with_capacity(min_maxes.len(), data_capacity);
                for opt in bytes_to_str(min_maxes) {
                    match opt {
                        None => builder.append_null(),
                        Some(s) => builder.append_value(s.as_str()),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::LargeUtf8 => {
                let mut builder =
                    LargeStringBuilder::with_capacity(min_maxes.len(), data_capacity);
                for opt in bytes_to_str(min_maxes) {
                    match opt {
                        None => builder.append_null(),
                        Some(s) => builder.append_value(s.as_str()),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Utf8View => {
                let block_size = capacity_to_view_block_size(data_capacity);

                let mut builder = StringViewBuilder::with_capacity(min_maxes.len())
                    .with_fixed_block_size(block_size);
                for opt in bytes_to_str(min_maxes) {
                    match opt {
                        None => builder.append_null(),
                        Some(s) => builder.append_value(s.as_str()),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Binary => {
                let mut builder =
                    BinaryBuilder::with_capacity(min_maxes.len(), data_capacity);
                for opt in min_maxes {
                    match opt {
                        None => builder.append_null(),
                        Some(s) => builder.append_value(s.as_ref() as &[u8]),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::LargeBinary => {
                let mut builder =
                    LargeBinaryBuilder::with_capacity(min_maxes.len(), data_capacity);
                for opt in min_maxes {
                    match opt {
                        None => builder.append_null(),
                        Some(s) => builder.append_value(s.as_ref() as &[u8]),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::BinaryView => {
                let block_size = capacity_to_view_block_size(data_capacity);

                let mut builder = BinaryViewBuilder::with_capacity(min_maxes.len())
                    .with_fixed_block_size(block_size);
                for opt in min_maxes {
                    match opt {
                        None => builder.append_null(),
                        Some(s) => builder.append_value(s.as_ref() as &[u8]),
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                return internal_err!(
                    "Unexpected data type for MinMaxBytesAccumulator: {:?}",
                    self.inner.data_type
                );
            }
        };

        assert_eq!(&self.inner.data_type, result.data_type());
        Ok(result)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        // min/max are their own states (no transition needed)
        self.evaluate(emit_to).map(|arr| vec![arr])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        // min/max are their own states (no transition needed)
        self.update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        // Min/max do not change the values as they are their own states
        // apply the filter by combining with the null mask, if any
        let output = apply_filter_as_nulls(&values[0], opt_filter)?;
        Ok(vec![output])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

/// Returns the block size in (contiguous buffer size) to use
/// for a given data capacity (total string length)
///
/// This is a heuristic to avoid allocating too many small buffers
fn capacity_to_view_block_size(data_capacity: usize) -> u32 {
    let max_block_size = 2 * 1024 * 1024;
    // Avoid block size equal to zero when calling `with_fixed_block_size()`.
    if data_capacity == 0 {
        return 1;
    }
    if let Ok(block_size) = u32::try_from(data_capacity) {
        block_size.min(max_block_size)
    } else {
        max_block_size
    }
}

/// Stores internal Min/Max state for "bytes" types.
///
/// This implementation is general and stores the minimum/maximum for each
/// groups in an individual byte array, which balances allocations and memory
/// fragmentation (aka garbage).
///
/// ```text
///                    ┌─────────────────────────────────┐
///   ┌─────┐    ┌────▶│Option<Vec<u8>> (["A"])          │───────────▶   "A"
///   │  0  │────┘     └─────────────────────────────────┘
///   ├─────┤          ┌─────────────────────────────────┐
///   │  1  │─────────▶│Option<Vec<u8>> (["Z"])          │───────────▶   "Z"
///   └─────┘          └─────────────────────────────────┘               ...
///     ...               ...
///   ┌─────┐          ┌────────────────────────────────┐
///   │ N-2 │─────────▶│Option<Vec<u8>> (["A"])         │────────────▶   "A"
///   ├─────┤          └────────────────────────────────┘
///   │ N-1 │────┐     ┌────────────────────────────────┐
///   └─────┘    └────▶│Option<Vec<u8>> (["Q"])         │────────────▶   "Q"
///                    └────────────────────────────────┘
///
///                      min_max: Vec<Option<Vec<u8>>
/// ```
///
/// Note that for `StringViewArray` and `BinaryViewArray`, there are potentially
/// more efficient implementations (e.g. by managing a string data buffer
/// directly), but then garbage collection, memory management, and final array
/// construction becomes more complex.
///
/// See discussion on <https://github.com/apache/datafusion/issues/6906>
#[derive(Debug)]
struct MinMaxBytesState {
    /// The minimum/maximum value for each group
    min_max: Vec<Option<Vec<u8>>>,
    /// The data type of the array
    data_type: DataType,
    /// The total bytes of the string data (for pre-allocating the final array,
    /// and tracking memory usage)
    total_data_bytes: usize,
    /// Scratch storage tracking which groups were updated in the current batch
    scratch_group_ids: Vec<usize>,
    /// Dense scratch table indexed by group id. Entries are tagged with an
    /// epoch so we can reuse the allocation across batches without clearing it.
    scratch_dense: Vec<ScratchEntry>,
    /// Epoch corresponding to the current batch.
    scratch_epoch: u64,
    /// Sparse scratch entries keyed by group id describing where the candidate
    /// value for the group is stored during the current batch.
    scratch_sparse: HashMap<usize, ScratchLocation>,
    /// Upper bound on the dense scratch size we are willing to allocate. The
    /// bound is updated after each batch based on how "dense" the accessed
    /// groups were so that we only pay for dense initialisation when we have
    /// evidence that it will be reused.
    scratch_dense_limit: usize,
    /// Whether the dense scratch table has been initialised. We defer creating
    /// the dense table until the accumulator has processed at least one batch
    /// so that short-lived accumulators can stick to the sparse path and avoid
    /// zeroing large dense allocations upfront.
    scratch_dense_enabled: bool,
}

#[derive(Debug, Clone, Copy)]
enum ScratchLocation {
    Existing,
    Batch(usize),
}

#[derive(Debug, Clone, Copy)]
struct ScratchEntry {
    epoch: u64,
    location: ScratchLocation,
}

impl ScratchEntry {
    fn new() -> Self {
        Self {
            epoch: 0,
            location: ScratchLocation::Existing,
        }
    }
}

/// Grow the dense scratch table by at least this many entries whenever we need
/// to expand it. Chunked growth keeps the amortized cost low while capping the
/// amount of zeroing we do per batch.
const SCRATCH_DENSE_GROWTH_STEP: usize = 1024;

/// Heuristic multiplier that determines whether a batch of groups should be
/// considered "dense". If the maximum group index touched is within this
/// multiple of the number of unique groups observed, we enable the dense
/// scratch for subsequent batches.
const SCRATCH_DENSE_ENABLE_MULTIPLIER: usize = 8;

/// Implement the MinMaxBytesAccumulator with a comparison function
/// for comparing strings
impl MinMaxBytesState {
    /// Create a new MinMaxBytesAccumulator
    ///
    /// # Arguments:
    /// * `data_type`: The data type of the arrays that will be passed to this accumulator
    fn new(data_type: DataType) -> Self {
        Self {
            min_max: vec![],
            data_type,
            total_data_bytes: 0,
            scratch_group_ids: vec![],
            scratch_dense: vec![],
            scratch_epoch: 0,
            scratch_sparse: HashMap::new(),
            scratch_dense_limit: 0,
            scratch_dense_enabled: false,
        }
    }

    /// Set the specified group to the given value, updating memory usage appropriately
    fn set_value(&mut self, group_index: usize, new_val: &[u8]) {
        match self.min_max[group_index].as_mut() {
            None => {
                self.min_max[group_index] = Some(new_val.to_vec());
                self.total_data_bytes += new_val.len();
            }
            Some(existing_val) => {
                // Copy data over to avoid re-allocating
                self.total_data_bytes -= existing_val.len();
                self.total_data_bytes += new_val.len();
                existing_val.clear();
                existing_val.extend_from_slice(new_val);
            }
        }
    }

    /// Updates the min/max values for the given string values
    ///
    /// `cmp` is the  comparison function to use, called like `cmp(new_val, existing_val)`
    /// returns true if the `new_val` should replace `existing_val`
    fn update_batch<'a, F, I>(
        &mut self,
        iter: I,
        group_indices: &[usize],
        total_num_groups: usize,
        mut cmp: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send + Sync,
        I: IntoIterator<Item = Option<&'a [u8]>>,
    {
        self.min_max.resize(total_num_groups, None);

        self.scratch_epoch = self.scratch_epoch.wrapping_add(1);
        if self.scratch_epoch == 0 {
            for entry in &mut self.scratch_dense {
                entry.epoch = 0;
                entry.location = ScratchLocation::Existing;
            }
            self.scratch_epoch = 1;
        }

        let mut use_dense = (self.scratch_dense_enabled || self.total_data_bytes > 0)
            && self.scratch_dense_limit > 0;

        debug_assert!(self.scratch_sparse.is_empty());
        let mut scratch_sparse = std::mem::take(&mut self.scratch_sparse);
        let mut scratch_group_ids = std::mem::take(&mut self.scratch_group_ids);

        let values: Vec<_> = iter.into_iter().collect();

        if !use_dense {
            let mut pre_max_group_index: Option<usize> = None;
            for (group_index, value) in group_indices.iter().copied().zip(&values) {
                let Some(_) = value else {
                    continue;
                };
                match scratch_sparse.entry(group_index) {
                    Entry::Occupied(_) => {}
                    Entry::Vacant(entry) => {
                        entry.insert(ScratchLocation::Existing);
                        match pre_max_group_index {
                            Some(current_max) if current_max >= group_index => {}
                            _ => pre_max_group_index = Some(group_index),
                        }
                    }
                }
            }

            let unique_groups = scratch_sparse.len();
            scratch_sparse.clear();

            if let (Some(max_group_index), true) =
                (pre_max_group_index, unique_groups > 0)
            {
                let candidate_limit = (max_group_index + 1).min(total_num_groups);
                if candidate_limit <= unique_groups * SCRATCH_DENSE_ENABLE_MULTIPLIER {
                    self.scratch_dense_limit = candidate_limit;
                    use_dense = candidate_limit > 0;
                } else if !self.scratch_dense_enabled {
                    self.scratch_dense_limit = 0;
                }
            }
        }

        self.scratch_dense_limit = self.scratch_dense_limit.min(total_num_groups);

        // Minimize value copies by calculating the new min/maxes for each group
        // in this batch (either the existing min/max or the new input value)
        // and updating the owned values in `self.min_max` at most once
        let mut batch_inputs: Vec<&[u8]> = Vec::with_capacity(group_indices.len());
        let mut batch_unique_groups = 0_usize;
        let mut batch_max_group_index: Option<usize> = None;
        let mut register_first_touch = |group_index: usize| {
            batch_unique_groups += 1;
            match batch_max_group_index {
                Some(current_max) if current_max >= group_index => {}
                _ => batch_max_group_index = Some(group_index),
            }
        };

        // Figure out the new min value for each group
        for (group_index, new_val) in
            group_indices.iter().copied().zip(values.iter().copied())
        {
            let Some(new_val) = new_val else {
                continue; // skip nulls
            };

            let location = if use_dense && group_index < self.scratch_dense_limit {
                if group_index >= self.scratch_dense.len() {
                    let current_len = self.scratch_dense.len();
                    let mut target_len = group_index + 1;
                    if target_len < current_len + SCRATCH_DENSE_GROWTH_STEP {
                        target_len = (current_len + SCRATCH_DENSE_GROWTH_STEP)
                            .min(self.scratch_dense_limit);
                    }
                    target_len = target_len.min(self.scratch_dense_limit);
                    if target_len > current_len {
                        self.scratch_dense.resize(target_len, ScratchEntry::new());
                    }
                }
                if group_index < self.scratch_dense.len() {
                    let entry = &mut self.scratch_dense[group_index];
                    let mut first_touch = false;
                    if entry.epoch != self.scratch_epoch {
                        entry.epoch = self.scratch_epoch;
                        entry.location = ScratchLocation::Existing;
                        scratch_group_ids.push(group_index);
                        first_touch = true;
                    }
                    if first_touch {
                        register_first_touch(group_index);
                    }
                    &mut entry.location
                } else {
                    // The requested group exceeded the dense limit, fall back to the sparse map.
                    match scratch_sparse.entry(group_index) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(vacant) => {
                            scratch_group_ids.push(group_index);
                            register_first_touch(group_index);
                            vacant.insert(ScratchLocation::Existing)
                        }
                    }
                }
            } else {
                match scratch_sparse.entry(group_index) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(vacant) => {
                        scratch_group_ids.push(group_index);
                        register_first_touch(group_index);
                        vacant.insert(ScratchLocation::Existing)
                    }
                }
            };

            let existing_val = match *location {
                ScratchLocation::Existing => {
                    let Some(existing_val) = self.min_max[group_index].as_ref() else {
                        // no existing min/max, so this is the new min/max
                        let batch_index = batch_inputs.len();
                        batch_inputs.push(new_val);
                        *location = ScratchLocation::Batch(batch_index);
                        continue;
                    };
                    existing_val.as_ref()
                }
                // previous input value was the min/max, so compare it
                ScratchLocation::Batch(existing_idx) => batch_inputs[existing_idx],
            };

            // Compare the new value to the existing value, replacing if necessary
            if cmp(new_val, existing_val) {
                let batch_index = batch_inputs.len();
                batch_inputs.push(new_val);
                *location = ScratchLocation::Batch(batch_index);
            }
        }

        drop(register_first_touch);
        if use_dense {
            self.scratch_dense_enabled = true;
        }
        // Update self.min_max with any new min/max values we found in the input
        let mut max_group_index = batch_max_group_index;
        for group_index in scratch_group_ids.iter().copied() {
            match max_group_index {
                Some(current_max) if current_max >= group_index => {}
                _ => max_group_index = Some(group_index),
            }
            if group_index < self.scratch_dense.len() {
                let entry = &mut self.scratch_dense[group_index];
                if entry.epoch == self.scratch_epoch {
                    if let ScratchLocation::Batch(batch_index) = entry.location {
                        self.set_value(group_index, batch_inputs[batch_index]);
                    }
                    continue;
                }
            }

            if let Some(ScratchLocation::Batch(batch_index)) =
                scratch_sparse.remove(&group_index)
            {
                self.set_value(group_index, batch_inputs[batch_index]);
            }
        }
        let unique_groups = batch_unique_groups;
        scratch_group_ids.clear();
        scratch_sparse.clear();
        self.scratch_sparse = scratch_sparse;
        self.scratch_group_ids = scratch_group_ids;
        if let (Some(max_group_index), true) = (max_group_index, unique_groups > 0) {
            let candidate_limit = (max_group_index + 1).min(total_num_groups);
            if candidate_limit <= unique_groups * SCRATCH_DENSE_ENABLE_MULTIPLIER {
                self.scratch_dense_limit = candidate_limit;
            } else if !self.scratch_dense_enabled {
                // Keep the dense limit disabled for sparse workloads until we see
                // evidence that growing the dense scratch would pay off.
                self.scratch_dense_limit = 0;
            }
        }
        self.scratch_dense_limit = self.scratch_dense_limit.min(total_num_groups);
        Ok(())
    }

    /// Emits the specified min_max values
    ///
    /// Returns (data_capacity, min_maxes), updating the current value of total_data_bytes
    ///
    /// - `data_capacity`: the total length of all strings and their contents,
    /// - `min_maxes`: the actual min/max values for each group
    fn emit_to(&mut self, emit_to: EmitTo) -> (usize, Vec<Option<Vec<u8>>>) {
        match emit_to {
            EmitTo::All => {
                (
                    std::mem::take(&mut self.total_data_bytes), // reset total bytes and min_max
                    std::mem::take(&mut self.min_max),
                )
            }
            EmitTo::First(n) => {
                let first_min_maxes: Vec<_> = self.min_max.drain(..n).collect();
                let first_data_capacity: usize = first_min_maxes
                    .iter()
                    .map(|opt| opt.as_ref().map(|s| s.len()).unwrap_or(0))
                    .sum();
                self.total_data_bytes -= first_data_capacity;
                (first_data_capacity, first_min_maxes)
            }
        }
    }

    fn size(&self) -> usize {
        self.total_data_bytes
            + self.min_max.len() * size_of::<Option<Vec<u8>>>()
            + self.scratch_group_ids.capacity() * size_of::<usize>()
            + self.scratch_dense.capacity() * size_of::<ScratchEntry>()
            + self.scratch_sparse.capacity()
                * (size_of::<usize>() + size_of::<ScratchLocation>())
            + size_of::<usize>()
            + size_of::<bool>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_groups_do_not_allocate_per_total_group() {
        let mut state = MinMaxBytesState::new(DataType::Utf8);
        let groups = vec![10_usize, 20_usize];
        let values = vec![Some("b".as_bytes()), Some("a".as_bytes())];

        state
            .update_batch(values.iter().copied(), &groups, 1_000_000, |a, b| a < b)
            .expect("update batch");

        assert_eq!(state.min_max.len(), 1_000_000);
        assert_eq!(state.scratch_group_ids.len(), 0);
        assert!(state.scratch_group_ids.capacity() >= groups.len());
        assert!(state.scratch_sparse.is_empty());
        assert_eq!(state.scratch_dense.len(), 0);
        assert_eq!(state.scratch_dense_limit, 0);
        assert!(!state.scratch_dense_enabled);
        assert_eq!(state.min_max[10].as_deref(), Some("b".as_bytes()));
        assert_eq!(state.min_max[20].as_deref(), Some("a".as_bytes()));

        // Re-run with a single group to ensure the scratch state resets cleanly
        let groups_second = vec![20_usize];
        let values_second = vec![Some("c".as_bytes())];

        state
            .update_batch(
                values_second.iter().copied(),
                &groups_second,
                1_000_000,
                |a, b| a > b,
            )
            .expect("update batch");

        assert_eq!(state.scratch_group_ids.len(), 0);
        assert!(state.scratch_group_ids.capacity() >= groups_second.len());
        assert!(state.scratch_sparse.is_empty());
        assert_eq!(state.scratch_dense.len(), 0);
        assert_eq!(state.scratch_dense_limit, 0);
        assert!(!state.scratch_dense_enabled);
        assert_eq!(state.min_max[20].as_deref(), Some("c".as_bytes()));
    }

    #[test]
    fn dense_groups_use_dense_scratch() {
        let mut state = MinMaxBytesState::new(DataType::Utf8);
        let groups: Vec<_> = (0..16).collect();
        let values: Vec<Vec<u8>> =
            (0..16).map(|idx| format!("v{idx}").into_bytes()).collect();
        let value_refs: Vec<_> = values.iter().map(|v| Some(v.as_slice())).collect();

        state
            .update_batch(value_refs.iter().copied(), &groups, 16, |a, b| a < b)
            .expect("dense update batch");

        assert!(state.scratch_sparse.is_empty());
        assert!(state.scratch_dense_enabled);
        assert_eq!(state.scratch_dense_limit, 16);
        assert!(state.scratch_dense.len() >= 16);
        for (i, expected) in values.iter().enumerate() {
            assert_eq!(state.min_max[i].as_deref(), Some(expected.as_slice()));
        }
        let total_first: usize = state
            .min_max
            .iter()
            .map(|opt| opt.as_ref().map(|v| v.len()).unwrap_or(0))
            .sum();
        assert_eq!(state.total_data_bytes, total_first);

        // Update some of the groups with larger values to ensure the dense table
        // resets via the epoch rather than clearing the entire allocation.
        let updated_groups = vec![0, 5, 10, 15];
        let updated_values = vec![
            Some("zz".as_bytes()),
            Some("yy".as_bytes()),
            Some("xx".as_bytes()),
            Some("ww".as_bytes()),
        ];

        state
            .update_batch(
                updated_values.iter().copied(),
                &updated_groups,
                16,
                |a, b| a > b,
            )
            .expect("second dense update");

        assert!(state.scratch_sparse.is_empty());
        assert!(state.scratch_dense_enabled);
        assert!(state.scratch_dense.len() >= 16);
        assert_eq!(state.scratch_dense_limit, 16);
        assert_eq!(state.min_max[0].as_deref(), Some("zz".as_bytes()));
        assert_eq!(state.min_max[5].as_deref(), Some("yy".as_bytes()));
        assert_eq!(state.min_max[10].as_deref(), Some("xx".as_bytes()));
        assert_eq!(state.min_max[15].as_deref(), Some("ww".as_bytes()));
        let total_second: usize = state
            .min_max
            .iter()
            .map(|opt| opt.as_ref().map(|v| v.len()).unwrap_or(0))
            .sum();
        assert_eq!(state.total_data_bytes, total_second);
    }

    #[test]
    fn sparse_groups_still_use_sparse_scratch() {
        let mut state = MinMaxBytesState::new(DataType::Utf8);
        let groups = vec![1_000_000_usize, 2_000_000_usize];
        let values = vec![Some("left".as_bytes()), Some("right".as_bytes())];

        state
            .update_batch(values.iter().copied(), &groups, 2_000_001, |a, b| a < b)
            .expect("sparse update");

        assert!(state.scratch_sparse.is_empty());
        assert_eq!(state.scratch_dense.len(), 0);
        assert_eq!(state.scratch_dense_limit, 0);
        assert!(!state.scratch_dense_enabled);
        assert_eq!(state.min_max[1_000_000].as_deref(), Some("left".as_bytes()));
        assert_eq!(
            state.min_max[2_000_000].as_deref(),
            Some("right".as_bytes())
        );
        let total_third: usize = state
            .min_max
            .iter()
            .map(|opt| opt.as_ref().map(|v| v.len()).unwrap_or(0))
            .sum();
        assert_eq!(state.total_data_bytes, total_third);
    }

    #[test]
    fn dense_then_sparse_batches_share_limit() {
        let mut state = MinMaxBytesState::new(DataType::Utf8);
        let dense_groups: Vec<_> = (0..32).collect();
        let dense_values: Vec<Vec<u8>> =
            (0..32).map(|idx| format!("d{idx}").into_bytes()).collect();
        let dense_refs: Vec<_> =
            dense_values.iter().map(|v| Some(v.as_slice())).collect();

        state
            .update_batch(dense_refs.iter().copied(), &dense_groups, 32, |a, b| a < b)
            .expect("initial dense batch");

        assert_eq!(state.scratch_dense_limit, 32);
        assert!(state.scratch_dense_enabled);
        assert!(state.scratch_dense.len() >= 32);

        let sparse_groups = vec![1_000_000_usize];
        let sparse_values = vec![Some("tail".as_bytes())];

        state
            .update_batch(
                sparse_values.iter().copied(),
                &sparse_groups,
                1_000_100,
                |a, b| a < b,
            )
            .expect("sparse follow-up");

        // The sparse batch should not inflate the dense allocation.
        assert_eq!(state.scratch_dense_limit, 32);
        assert!(state.scratch_dense.len() >= 32);
        assert!(state.scratch_dense_enabled);

        // Another dense batch should now reuse the stored limit and grow the
        // dense scratch chunk-by-chunk instead of jumping straight to the
        // global total number of groups.
        let follow_up_values: Vec<Vec<u8>> =
            (0..32).map(|idx| format!("u{idx}").into_bytes()).collect();
        let follow_up_refs: Vec<_> = follow_up_values
            .iter()
            .map(|v| Some(v.as_slice()))
            .collect();

        state
            .update_batch(follow_up_refs.iter().copied(), &dense_groups, 32, |a, b| {
                a > b
            })
            .expect("dense reuse");

        assert!(state.scratch_dense_enabled);
        assert!(state.scratch_dense.len() >= 32);
        assert_eq!(state.scratch_dense_limit, 32);
        for (idx, expected) in follow_up_values.iter().enumerate() {
            assert_eq!(state.min_max[idx].as_deref(), Some(expected.as_slice()));
        }
    }
}
