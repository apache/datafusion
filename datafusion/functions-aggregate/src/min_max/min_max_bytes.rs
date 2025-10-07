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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkloadMode {
    /// The accumulator has not yet observed any non-null values and therefore
    /// cannot decide between the simple dense path and the sparse-optimised
    /// implementation.
    Undecided,
    /// Use an inline dense path that updates the accumulator directly without
    /// any per-batch scratch allocation. This path is optimised for small,
    /// repeatedly accessed group domains where the group ids are densely
    /// populated.
    DenseInline,
    /// Use the original per-batch dense array that favours cache locality and
    /// straight-line execution. This is ideal for workloads that repeatedly
    /// touch most groups ("dense" workloads).
    Simple,
    /// Use the sparse/dense scratch machinery introduced to cope with
    /// high-cardinality workloads that would otherwise allocate
    /// `total_num_groups` scratch entries on every batch.
    SparseOptimized,
}

#[derive(Debug, Clone, Copy, Default)]
struct BatchStats {
    unique_groups: usize,
    max_group_index: Option<usize>,
}

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
    /// Tracks which implementation should be used for future batches.
    workload_mode: WorkloadMode,
    /// Number of batches processed so far. Used in conjunction with
    /// `total_groups_seen` when evaluating mode switches.
    processed_batches: usize,
    /// Total number of groups observed across the lifetime of the accumulator.
    total_groups_seen: usize,
    /// Highest group index seen so far.
    lifetime_max_group_index: Option<usize>,
    /// Number of groups that currently have a materialised min/max value.
    populated_groups: usize,
    /// Scratch entries reused by the classic simple implementation.
    simple_slots: Vec<SimpleSlot>,
    /// Epoch used to lazily reset `simple_slots` between batches.
    simple_epoch: u64,
    /// Reusable list of groups touched by the simple path.
    simple_touched_groups: Vec<usize>,
    /// Marker vector used by the dense inline implementation to detect first
    /// touches without clearing a bitmap on every batch.
    dense_inline_marks: Vec<u64>,
    /// Epoch associated with `dense_inline_marks`.
    dense_inline_epoch: u64,
    /// Number of consecutive batches processed while remaining in
    /// `DenseInline` mode.
    dense_inline_stable_batches: usize,
    /// Whether the accumulator has committed to the dense inline fast path and
    /// no longer needs to track per-batch statistics.
    dense_inline_committed: bool,
    #[cfg(test)]
    dense_enable_invocations: usize,
    #[cfg(test)]
    dense_sparse_detours: usize,
}

#[derive(Debug, Clone, Copy)]
struct SimpleSlot {
    epoch: u64,
    location: SimpleLocation,
}

impl SimpleSlot {
    fn new() -> Self {
        Self {
            epoch: 0,
            location: SimpleLocation::Untouched,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SimpleLocation {
    Untouched,
    Existing,
    Batch(usize),
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

/// Maximum number of groups for which the inline dense path is considered.
///
/// Mode selection overview:
/// | Mode            | Optimal For                | Memory Footprint | Description                       |
/// | --------------- | -------------------------- | ---------------- | --------------------------------- |
/// | DenseInline     | `N ≤ 100k`, ≥ 50% density  | `O(N)`           | Epoch-tracked, zero additional allocation. |
/// | Simple          | `N > 100k`, medium density | `≈ 3 × O(N)`     | Deferred materialization with scratch staging. |
/// | SparseOptimized | Very sparse or huge `N`    | `O(touched)`     | Hash-based tracking of populated groups. |
/// | Undecided       | Initial batch              | -                | Gathers statistics then picks a mode. |
///
/// Flowchart:
/// ```text
/// Undecided
///  ├─ N ≤ threshold & density ≥ 50% → DenseInline
///  ├─ N ≤ 100k & density ≥ 10%      → Simple
///  └─ otherwise                      → SparseOptimized
/// ```
///
/// `100_000` was chosen from benchmark analysis. Even in the worst case the
/// DenseInline epoch vector consumes ≈ 800 KiB, which is still significantly
/// smaller than the multi-vector Simple mode and avoids its cache penalties.
const DENSE_INLINE_MAX_TOTAL_GROUPS: usize = 100_000;
/// Minimum observed density (in percent) required to remain on the inline dense
/// path.
const DENSE_INLINE_MIN_DENSITY_PERCENT: usize = 50;

/// Maximum number of groups for which the simple dense path is considered.
const SIMPLE_MODE_MAX_TOTAL_GROUPS: usize = 100_000;
/// Minimum observed density (in percent) required to remain on the simple path.
const SIMPLE_MODE_MIN_DENSITY_PERCENT: usize = 10;
/// Threshold after which the accumulator reevaluates whether it should switch
/// to the sparse implementation.
const SPARSE_SWITCH_GROUP_THRESHOLD: usize = 100_000;
/// Maximum density (in percent) tolerated before switching from the simple path
/// to the sparse implementation.
const SPARSE_SWITCH_MAX_DENSITY_PERCENT: usize = 1;

/// Heuristic multiplier that determines whether a batch of groups should be
/// considered "dense". If the maximum group index touched is within this
/// multiple of the number of unique groups observed, we enable the dense
/// scratch for subsequent batches.
const SCRATCH_DENSE_ENABLE_MULTIPLIER: usize = 8;

/// After this many consecutive batches we consider DenseInline stable and
/// disable per-batch statistics tracking.
const DENSE_INLINE_STABILITY_THRESHOLD: usize = 3;

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
            workload_mode: WorkloadMode::Undecided,
            processed_batches: 0,
            total_groups_seen: 0,
            lifetime_max_group_index: None,
            populated_groups: 0,
            simple_slots: vec![],
            simple_epoch: 0,
            simple_touched_groups: vec![],
            dense_inline_marks: vec![],
            dense_inline_epoch: 0,
            dense_inline_stable_batches: 0,
            dense_inline_committed: false,
            #[cfg(test)]
            dense_enable_invocations: 0,
            #[cfg(test)]
            dense_sparse_detours: 0,
        }
    }

    /// Set the specified group to the given value, updating memory usage appropriately
    fn set_value(&mut self, group_index: usize, new_val: &[u8]) {
        match self.min_max[group_index].as_mut() {
            None => {
                self.min_max[group_index] = Some(new_val.to_vec());
                self.total_data_bytes += new_val.len();
                self.populated_groups += 1;
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

    fn update_batch<'a, F, I>(
        &mut self,
        iter: I,
        group_indices: &[usize],
        total_num_groups: usize,
        cmp: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send + Sync,
        I: IntoIterator<Item = Option<&'a [u8]>>,
    {
        let mut cmp = cmp;
        match self.workload_mode {
            WorkloadMode::SparseOptimized => {
                let stats = self.update_batch_sparse_impl(
                    iter,
                    group_indices,
                    total_num_groups,
                    &mut cmp,
                )?;
                self.record_batch_stats(stats, total_num_groups);
                Ok(())
            }
            WorkloadMode::DenseInline => {
                if self.dense_inline_committed {
                    self.update_batch_dense_inline_committed(
                        iter,
                        group_indices,
                        total_num_groups,
                        &mut cmp,
                    )
                } else {
                    let stats = self.update_batch_dense_inline_impl(
                        iter,
                        group_indices,
                        total_num_groups,
                        &mut cmp,
                    )?;
                    self.record_batch_stats(stats, total_num_groups);
                    Ok(())
                }
            }
            WorkloadMode::Simple => {
                let stats = self.update_batch_simple_impl(
                    iter,
                    group_indices,
                    total_num_groups,
                    &mut cmp,
                )?;
                self.record_batch_stats(stats, total_num_groups);
                Ok(())
            }
            WorkloadMode::Undecided => {
                let stats = if total_num_groups <= DENSE_INLINE_MAX_TOTAL_GROUPS {
                    self.update_batch_dense_inline_impl(
                        iter,
                        group_indices,
                        total_num_groups,
                        &mut cmp,
                    )?
                } else {
                    self.update_batch_simple_impl(
                        iter,
                        group_indices,
                        total_num_groups,
                        &mut cmp,
                    )?
                };
                self.record_batch_stats(stats, total_num_groups);
                Ok(())
            }
        }
    }

    fn update_batch_dense_inline_impl<'a, F, I>(
        &mut self,
        iter: I,
        group_indices: &[usize],
        total_num_groups: usize,
        cmp: &mut F,
    ) -> Result<BatchStats>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send + Sync,
        I: IntoIterator<Item = Option<&'a [u8]>>,
    {
        self.min_max.resize(total_num_groups, None);

        if self.dense_inline_marks.len() < total_num_groups {
            self.dense_inline_marks.resize(total_num_groups, 0_u64);
        }

        self.dense_inline_epoch = self.dense_inline_epoch.wrapping_add(1);
        if self.dense_inline_epoch == 0 {
            for mark in &mut self.dense_inline_marks {
                *mark = 0;
            }
            self.dense_inline_epoch = 1;
        }

        let mut unique_groups = 0_usize;
        let mut max_group_index: Option<usize> = None;
        let mut fast_path = true;
        let mut fast_rows = 0_usize;
        let mut fast_start = 0_usize;
        let mut fast_last = 0_usize;

        let mut last_group_index: Option<usize> = None;

        for (group_index, new_val) in group_indices.iter().copied().zip(iter.into_iter())
        {
            let Some(new_val) = new_val else {
                continue;
            };

            if group_index >= self.min_max.len() {
                return internal_err!(
                    "group index {group_index} out of bounds for {} groups",
                    self.min_max.len()
                );
            }

            let is_consecutive_duplicate = last_group_index == Some(group_index);
            last_group_index = Some(group_index);

            if fast_path {
                if fast_rows == 0 {
                    fast_start = group_index;
                    fast_last = group_index;
                } else if group_index == fast_last + 1 {
                    fast_last = group_index;
                } else {
                    fast_path = false;
                    if fast_rows > 0 {
                        unique_groups = fast_rows;
                        max_group_index = Some(match max_group_index {
                            Some(current_max) => current_max.max(fast_last),
                            None => fast_last,
                        });

                        let epoch = self.dense_inline_epoch;
                        let marks = &mut self.dense_inline_marks;
                        for idx in fast_start..=fast_last {
                            marks[idx] = epoch;
                        }
                    }
                }

                if fast_path {
                    fast_rows = fast_rows.saturating_add(1);
                }
            }

            if !fast_path && !is_consecutive_duplicate {
                let mark = &mut self.dense_inline_marks[group_index];
                if *mark != self.dense_inline_epoch {
                    *mark = self.dense_inline_epoch;
                    unique_groups = unique_groups.saturating_add(1);
                    max_group_index = Some(match max_group_index {
                        Some(current_max) => current_max.max(group_index),
                        None => group_index,
                    });
                }
            }

            let should_replace = match self.min_max[group_index].as_ref() {
                Some(existing_val) => cmp(new_val, existing_val.as_ref()),
                None => true,
            };

            if should_replace {
                self.set_value(group_index, new_val);
            }
        }

        if fast_path {
            if fast_rows > 0 {
                unique_groups = fast_rows;
                max_group_index = Some(match max_group_index {
                    Some(current_max) => current_max.max(fast_last),
                    None => fast_last,
                });
            }
        }

        Ok(BatchStats {
            unique_groups,
            max_group_index,
        })
    }

    /// Fast path for DenseInline once the workload has been deemed stable.
    ///
    /// No statistics or mark tracking is required: simply update the
    /// materialised values in place.
    fn update_batch_dense_inline_committed<'a, F, I>(
        &mut self,
        iter: I,
        group_indices: &[usize],
        total_num_groups: usize,
        cmp: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send + Sync,
        I: IntoIterator<Item = Option<&'a [u8]>>,
    {
        self.min_max.resize(total_num_groups, None);

        for (group_index, new_val) in group_indices.iter().copied().zip(iter.into_iter())
        {
            let Some(new_val) = new_val else {
                continue;
            };

            if group_index >= self.min_max.len() {
                return internal_err!(
                    "group index {group_index} out of bounds for {} groups",
                    self.min_max.len()
                );
            }

            let should_replace = match self.min_max[group_index].as_ref() {
                Some(existing_val) => cmp(new_val, existing_val.as_ref()),
                None => true,
            };

            if should_replace {
                self.set_value(group_index, new_val);
            }
        }

        Ok(())
    }

    fn update_batch_simple_impl<'a, F, I>(
        &mut self,
        iter: I,
        group_indices: &[usize],
        total_num_groups: usize,
        cmp: &mut F,
    ) -> Result<BatchStats>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send + Sync,
        I: IntoIterator<Item = Option<&'a [u8]>>,
    {
        self.min_max.resize(total_num_groups, None);

        if self.simple_slots.len() < total_num_groups {
            self.simple_slots
                .resize_with(total_num_groups, SimpleSlot::new);
        }

        self.simple_epoch = self.simple_epoch.wrapping_add(1);
        if self.simple_epoch == 0 {
            for slot in &mut self.simple_slots {
                slot.epoch = 0;
                slot.location = SimpleLocation::Untouched;
            }
            self.simple_epoch = 1;
        }

        let mut touched_groups = std::mem::take(&mut self.simple_touched_groups);
        touched_groups.clear();
        let mut batch_inputs: Vec<&[u8]> = Vec::with_capacity(group_indices.len());
        let mut unique_groups = 0_usize;
        let mut max_group_index: Option<usize> = None;

        for (group_index, new_val) in group_indices.iter().copied().zip(iter.into_iter())
        {
            let Some(new_val) = new_val else {
                continue;
            };

            if group_index >= self.simple_slots.len() {
                return internal_err!(
                    "group index {group_index} out of bounds for {} simple slots",
                    self.simple_slots.len()
                );
            }

            let slot = &mut self.simple_slots[group_index];
            if slot.epoch != self.simple_epoch {
                slot.epoch = self.simple_epoch;
                slot.location = SimpleLocation::Untouched;
                touched_groups.push(group_index);
                unique_groups += 1;
                max_group_index = Some(match max_group_index {
                    Some(current_max) => current_max.max(group_index),
                    None => group_index,
                });
            }

            match slot.location {
                SimpleLocation::Untouched => match self.min_max[group_index].as_ref() {
                    Some(existing_val) => {
                        if cmp(new_val, existing_val.as_ref()) {
                            let batch_index = batch_inputs.len();
                            batch_inputs.push(new_val);
                            slot.location = SimpleLocation::Batch(batch_index);
                        } else {
                            slot.location = SimpleLocation::Existing;
                        }
                    }
                    None => {
                        let batch_index = batch_inputs.len();
                        batch_inputs.push(new_val);
                        slot.location = SimpleLocation::Batch(batch_index);
                    }
                },
                SimpleLocation::Existing => {
                    let existing_val = self.min_max[group_index]
                        .as_ref()
                        .expect("existing value must be present")
                        .as_ref();
                    if cmp(new_val, existing_val) {
                        let batch_index = batch_inputs.len();
                        batch_inputs.push(new_val);
                        slot.location = SimpleLocation::Batch(batch_index);
                    }
                }
                SimpleLocation::Batch(existing_index) => {
                    let existing_val = batch_inputs[existing_index];
                    if cmp(new_val, existing_val) {
                        let batch_index = batch_inputs.len();
                        batch_inputs.push(new_val);
                        slot.location = SimpleLocation::Batch(batch_index);
                    }
                }
            }
        }

        for &group_index in &touched_groups {
            if let SimpleLocation::Batch(batch_index) =
                self.simple_slots[group_index].location
            {
                self.set_value(group_index, batch_inputs[batch_index]);
            }
        }

        touched_groups.clear();
        self.simple_touched_groups = touched_groups;

        Ok(BatchStats {
            unique_groups,
            max_group_index,
        })
    }

    fn record_batch_stats(&mut self, stats: BatchStats, total_num_groups: usize) {
        self.processed_batches = self.processed_batches.saturating_add(1);
        if stats.unique_groups == 0 {
            return;
        }

        self.total_groups_seen =
            self.total_groups_seen.saturating_add(stats.unique_groups);
        if let Some(max_group_index) = stats.max_group_index {
            self.lifetime_max_group_index = Some(match self.lifetime_max_group_index {
                Some(previous) => previous.max(max_group_index),
                None => max_group_index,
            });
        }

        match self.workload_mode {
            WorkloadMode::Undecided => {
                if let Some(max_group_index) = stats.max_group_index {
                    let domain = max_group_index + 1;
                    if self.should_use_dense_inline(total_num_groups, stats.unique_groups)
                    {
                        if !matches!(self.workload_mode, WorkloadMode::DenseInline) {
                            self.enter_dense_inline_mode();
                        }
                        self.workload_mode = WorkloadMode::DenseInline;
                    } else if self.should_use_simple(
                        total_num_groups,
                        stats.unique_groups,
                        domain,
                    ) {
                        if !matches!(self.workload_mode, WorkloadMode::Simple) {
                            self.enter_simple_mode();
                        }
                        self.workload_mode = WorkloadMode::Simple;
                    } else {
                        if !matches!(self.workload_mode, WorkloadMode::SparseOptimized) {
                            self.enter_sparse_mode();
                        }
                        self.workload_mode = WorkloadMode::SparseOptimized;
                    }
                }
            }
            WorkloadMode::DenseInline => {
                if self.dense_inline_committed {
                    return;
                }

                if self.should_switch_to_sparse() {
                    self.enter_sparse_mode();
                    self.workload_mode = WorkloadMode::SparseOptimized;
                    self.dense_inline_stable_batches = 0;
                } else if let Some(max_group_index) = stats.max_group_index {
                    let domain = max_group_index + 1;
                    if !self
                        .should_use_dense_inline(total_num_groups, stats.unique_groups)
                    {
                        self.dense_inline_stable_batches = 0;
                        if self.should_use_simple(
                            total_num_groups,
                            stats.unique_groups,
                            domain,
                        ) {
                            self.enter_simple_mode();
                            self.workload_mode = WorkloadMode::Simple;
                        } else {
                            self.enter_sparse_mode();
                            self.workload_mode = WorkloadMode::SparseOptimized;
                        }
                    } else {
                        self.dense_inline_stable_batches =
                            self.dense_inline_stable_batches.saturating_add(1);
                        if self.dense_inline_stable_batches
                            >= DENSE_INLINE_STABILITY_THRESHOLD
                        {
                            self.dense_inline_committed = true;
                            self.dense_inline_marks.clear();
                        }
                    }
                }
            }
            WorkloadMode::Simple => {
                if self.should_switch_to_sparse() {
                    self.enter_sparse_mode();
                    self.workload_mode = WorkloadMode::SparseOptimized;
                }
            }
            WorkloadMode::SparseOptimized => {
                // Remain in sparse mode. We currently do not switch back to the
                // simple mode because sparse workloads tend to stay sparse.
            }
        }
    }

    fn should_use_dense_inline(
        &self,
        total_num_groups: usize,
        unique_groups: usize,
    ) -> bool {
        if total_num_groups == 0 || total_num_groups > DENSE_INLINE_MAX_TOTAL_GROUPS {
            return false;
        }

        unique_groups * 100 >= total_num_groups * DENSE_INLINE_MIN_DENSITY_PERCENT
    }

    fn should_use_simple(
        &self,
        total_num_groups: usize,
        unique_groups: usize,
        domain: usize,
    ) -> bool {
        if total_num_groups > SIMPLE_MODE_MAX_TOTAL_GROUPS || domain == 0 {
            return false;
        }
        unique_groups * SIMPLE_MODE_MIN_DENSITY_PERCENT >= domain
    }

    fn should_switch_to_sparse(&self) -> bool {
        if self.populated_groups <= SPARSE_SWITCH_GROUP_THRESHOLD {
            return false;
        }
        let Some(max_group_index) = self.lifetime_max_group_index else {
            return false;
        };
        let domain = max_group_index + 1;
        domain > 0 && self.populated_groups * SPARSE_SWITCH_MAX_DENSITY_PERCENT < domain
    }

    fn enter_simple_mode(&mut self) {
        self.scratch_group_ids.clear();
        self.scratch_sparse.clear();
        self.scratch_dense.clear();
        self.scratch_dense_limit = 0;
        self.scratch_dense_enabled = false;
        self.simple_touched_groups.clear();
        self.dense_inline_stable_batches = 0;
        self.dense_inline_committed = false;
    }

    fn enter_sparse_mode(&mut self) {
        // Ensure the dense scratch table starts from a clean slate when we
        // enter sparse mode. Subsequent batches will lazily enable and grow the
        // dense scratch as required by the existing heuristics.
        self.scratch_dense_enabled = false;
        self.scratch_dense_limit = 0;
        self.scratch_dense.clear();
        self.dense_inline_stable_batches = 0;
        self.dense_inline_committed = false;
    }

    fn enter_dense_inline_mode(&mut self) {
        self.enter_simple_mode();
        self.dense_inline_stable_batches = 0;
        self.dense_inline_committed = false;
    }

    /// Updates the min/max values for the given string values
    ///
    /// `cmp` is the  comparison function to use, called like `cmp(new_val, existing_val)`
    /// returns true if the `new_val` should replace `existing_val`
    fn update_batch_sparse_impl<'a, F, I>(
        &mut self,
        iter: I,
        group_indices: &[usize],
        total_num_groups: usize,
        cmp: &mut F,
    ) -> Result<BatchStats>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send + Sync,
        I: IntoIterator<Item = Option<&'a [u8]>>,
    {
        self.min_max.resize(total_num_groups, None);

        #[cfg(test)]
        {
            self.dense_sparse_detours = 0;
        }

        self.scratch_epoch = self.scratch_epoch.wrapping_add(1);
        if self.scratch_epoch == 0 {
            for entry in &mut self.scratch_dense {
                entry.epoch = 0;
                entry.location = ScratchLocation::Existing;
            }
            self.scratch_epoch = 1;
        }

        debug_assert!(self.scratch_sparse.is_empty());
        let mut scratch_sparse = std::mem::take(&mut self.scratch_sparse);
        let mut sparse_used_this_batch = false;
        let mut scratch_group_ids = std::mem::take(&mut self.scratch_group_ids);
        // Track whether the dense scratch table has already been initialised for
        // this batch. Once the dense path is active we avoid re-running the
        // migration logic and simply expand the dense limit as needed.
        let mut dense_activated_this_batch = false;

        self.scratch_dense_limit = self.scratch_dense_limit.min(total_num_groups);
        let mut use_dense = (self.scratch_dense_enabled || self.total_data_bytes > 0)
            && self.scratch_dense_limit > 0;

        // The iterator feeding `new_val` must remain streaming so that the inner
        // retry loop can re-evaluate the current value after switching between the
        // sparse and dense scratch paths. Avoid buffering values up front – only
        // the `batch_inputs` vector below may grow with the number of *touched*
        // groups, not with `total_num_groups`.
        let mut values_iter = iter.into_iter();

        // Minimize value copies by calculating the new min/maxes for each group
        // in this batch (either the existing min/max or the new input value)
        // and updating the owned values in `self.min_max` at most once
        let mut batch_inputs: Vec<&[u8]> = Vec::with_capacity(group_indices.len());
        let mut batch_unique_groups = 0_usize;
        let mut batch_max_group_index: Option<usize> = None;

        // Figure out the new min value for each group
        for (group_index, new_val) in group_indices.iter().copied().zip(&mut values_iter)
        {
            let Some(new_val) = new_val else {
                continue; // skip nulls
            };

            loop {
                let mut first_touch = false;
                let mut processed_via_dense = false;
                enum ScratchTarget {
                    Dense(usize),
                    Sparse(*mut ScratchLocation),
                }
                let target: ScratchTarget;
                let mut pending_dense_growth: Option<usize> = None;

                if use_dense {
                    let mut allow_dense = group_index < self.scratch_dense_limit;

                    if !allow_dense {
                        let potential_unique = batch_unique_groups + 1;
                        let potential_max = match batch_max_group_index {
                            Some(current_max) if current_max >= group_index => {
                                current_max
                            }
                            _ => group_index,
                        };
                        if let Some(candidate_limit) = self.evaluate_dense_candidate(
                            potential_unique,
                            Some(potential_max),
                            total_num_groups,
                        ) {
                            let mut desired_limit = candidate_limit;
                            if desired_limit
                                < self.scratch_dense_limit + SCRATCH_DENSE_GROWTH_STEP
                            {
                                desired_limit = (self.scratch_dense_limit
                                    + SCRATCH_DENSE_GROWTH_STEP)
                                    .min(total_num_groups);
                            }
                            desired_limit = desired_limit.min(total_num_groups);
                            self.expand_dense_limit(desired_limit);
                            allow_dense = group_index < self.scratch_dense_limit;
                        }
                    }

                    if allow_dense {
                        {
                            let entry = &mut self.scratch_dense[group_index];
                            if entry.epoch != self.scratch_epoch {
                                entry.epoch = self.scratch_epoch;
                                entry.location = ScratchLocation::Existing;
                                scratch_group_ids.push(group_index);
                                first_touch = true;
                            }
                        }
                        target = ScratchTarget::Dense(group_index);
                        processed_via_dense = true;
                    } else {
                        #[cfg(test)]
                        {
                            debug_assert!(self.scratch_dense_enabled);
                            self.dense_sparse_detours += 1;
                        }

                        match scratch_sparse.entry(group_index) {
                            Entry::Occupied(entry) => {
                                sparse_used_this_batch = true;
                                target =
                                    ScratchTarget::Sparse(entry.into_mut() as *mut _);
                            }
                            Entry::Vacant(vacant) => {
                                scratch_group_ids.push(group_index);
                                first_touch = true;
                                sparse_used_this_batch = true;
                                target = ScratchTarget::Sparse(
                                    vacant.insert(ScratchLocation::Existing) as *mut _,
                                );
                            }
                        }
                    }
                } else {
                    let seen_before = scratch_sparse.contains_key(&group_index);
                    if !seen_before {
                        let potential_unique = batch_unique_groups + 1;
                        let potential_max = match batch_max_group_index {
                            Some(current_max) if current_max >= group_index => {
                                current_max
                            }
                            _ => group_index,
                        };
                        if let Some(candidate_limit) = self.evaluate_dense_candidate(
                            potential_unique,
                            Some(potential_max),
                            total_num_groups,
                        ) {
                            if !dense_activated_this_batch
                                && self.enable_dense_for_batch(
                                    candidate_limit,
                                    &mut scratch_sparse,
                                    &mut scratch_group_ids,
                                )
                            {
                                dense_activated_this_batch = true;
                                use_dense = true;
                                continue;
                            } else if dense_activated_this_batch
                                && self.expand_dense_limit(candidate_limit)
                            {
                                continue;
                            }
                        }
                    }
                    match scratch_sparse.entry(group_index) {
                        Entry::Occupied(entry) => {
                            sparse_used_this_batch = true;
                            target = ScratchTarget::Sparse(entry.into_mut() as *mut _);
                        }
                        Entry::Vacant(vacant) => {
                            scratch_group_ids.push(group_index);
                            first_touch = true;
                            sparse_used_this_batch = true;
                            target = ScratchTarget::Sparse(
                                vacant.insert(ScratchLocation::Existing) as *mut _,
                            );
                        }
                    }
                }

                if first_touch {
                    batch_unique_groups += 1;
                    match batch_max_group_index {
                        Some(current_max) if current_max >= group_index => {}
                        _ => batch_max_group_index = Some(group_index),
                    }
                    if processed_via_dense {
                        if let Some(max_group_index) = batch_max_group_index {
                            let mut desired_limit = max_group_index + 1;
                            if desired_limit
                                < self.scratch_dense_limit + SCRATCH_DENSE_GROWTH_STEP
                            {
                                desired_limit = (self.scratch_dense_limit
                                    + SCRATCH_DENSE_GROWTH_STEP)
                                    .min(total_num_groups);
                            }
                            pending_dense_growth =
                                Some(desired_limit.min(total_num_groups));
                        }
                    } else {
                        if let Some(candidate_limit) = self.evaluate_dense_candidate(
                            batch_unique_groups,
                            batch_max_group_index,
                            total_num_groups,
                        ) {
                            if !dense_activated_this_batch
                                && self.enable_dense_for_batch(
                                    candidate_limit,
                                    &mut scratch_sparse,
                                    &mut scratch_group_ids,
                                )
                            {
                                dense_activated_this_batch = true;
                                use_dense = true;
                                continue;
                            } else if dense_activated_this_batch
                                && self.expand_dense_limit(candidate_limit)
                            {
                                continue;
                            }
                        }
                    }
                }

                if let Some(desired_limit) = pending_dense_growth {
                    self.expand_dense_limit(desired_limit);
                }

                let location = match target {
                    ScratchTarget::Dense(index) => {
                        &mut self.scratch_dense[index].location
                    }
                    ScratchTarget::Sparse(ptr) => unsafe { &mut *ptr },
                };

                let existing_val = match *location {
                    ScratchLocation::Existing => {
                        let Some(existing_val) = self.min_max[group_index].as_ref()
                        else {
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
                break;
            }
        }
        debug_assert!(
            values_iter.next().is_none(),
            "value iterator longer than group indices"
        );

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
        if sparse_used_this_batch {
            self.scratch_sparse = scratch_sparse;
        } else {
            self.scratch_sparse = HashMap::new();
        }
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
        Ok(BatchStats {
            unique_groups,
            max_group_index,
        })
    }

    fn evaluate_dense_candidate(
        &self,
        batch_unique_groups: usize,
        batch_max_group_index: Option<usize>,
        total_num_groups: usize,
    ) -> Option<usize> {
        if batch_unique_groups == 0 {
            return None;
        }
        let max_group_index = batch_max_group_index?;
        let candidate_limit = (max_group_index + 1).min(total_num_groups);
        if candidate_limit == 0 {
            return None;
        }
        if candidate_limit <= batch_unique_groups * SCRATCH_DENSE_ENABLE_MULTIPLIER {
            Some(candidate_limit)
        } else {
            None
        }
    }

    /// Enable the dense scratch table for the current batch, migrating any
    /// existing scratch entries that fall within the dense limit. This method is
    /// intentionally invoked at most once per batch to avoid repeatedly
    /// scanning `scratch_group_ids`.
    fn enable_dense_for_batch(
        &mut self,
        candidate_limit: usize,
        scratch_sparse: &mut HashMap<usize, ScratchLocation>,
        scratch_group_ids: &mut Vec<usize>,
    ) -> bool {
        if candidate_limit == 0 {
            return false;
        }

        let candidate_limit = candidate_limit.min(self.min_max.len());
        if candidate_limit == 0 {
            return false;
        }

        self.scratch_dense_limit = candidate_limit;
        if self.scratch_dense.len() < self.scratch_dense_limit {
            self.scratch_dense
                .resize(self.scratch_dense_limit, ScratchEntry::new());
        }

        for &group_index in scratch_group_ids.iter() {
            if group_index >= self.scratch_dense_limit {
                continue;
            }

            let entry = &mut self.scratch_dense[group_index];
            if entry.epoch != self.scratch_epoch {
                let location = scratch_sparse
                    .remove(&group_index)
                    .unwrap_or(ScratchLocation::Existing);
                entry.epoch = self.scratch_epoch;
                entry.location = location;
            } else if let Some(location) = scratch_sparse.remove(&group_index) {
                entry.location = location;
            }
        }

        #[cfg(test)]
        {
            self.dense_enable_invocations += 1;
        }

        true
    }

    /// Increase the dense limit for the current batch without remigrating
    /// previously processed groups. Returns `true` if the limit was expanded so
    /// the caller can retry handling the current group using the dense path.
    fn expand_dense_limit(&mut self, candidate_limit: usize) -> bool {
        if candidate_limit <= self.scratch_dense_limit {
            return false;
        }

        let candidate_limit = candidate_limit.min(self.min_max.len());
        if candidate_limit <= self.scratch_dense_limit {
            return false;
        }

        self.scratch_dense_limit = candidate_limit;
        if self.scratch_dense.len() < self.scratch_dense_limit {
            self.scratch_dense
                .resize(self.scratch_dense_limit, ScratchEntry::new());
        }

        true
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
            + self.simple_slots.capacity() * size_of::<SimpleSlot>()
            + self.simple_touched_groups.capacity() * size_of::<usize>()
            + self.dense_inline_marks.capacity() * size_of::<u64>()
            + size_of::<usize>()
            + size_of::<bool>()
            + size_of::<WorkloadMode>()
            + 3 * size_of::<usize>()
            + 2 * size_of::<u64>()
            + size_of::<Option<usize>>()
            + size_of::<usize>()
            + size_of::<bool>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_batches_use_dense_inline_mode() {
        let mut state = MinMaxBytesState::new(DataType::Utf8);
        let total_groups = 32_usize;
        let groups: Vec<usize> = (0..total_groups).collect();
        let raw_values: Vec<Vec<u8>> = groups
            .iter()
            .map(|idx| format!("value_{idx:02}").into_bytes())
            .collect();

        state
            .update_batch(
                raw_values.iter().map(|value| Some(value.as_slice())),
                &groups,
                total_groups,
                |a, b| a < b,
            )
            .expect("update batch");

        assert!(matches!(state.workload_mode, WorkloadMode::DenseInline));
        assert!(!state.scratch_dense_enabled);
        assert_eq!(state.scratch_dense_limit, 0);
        assert!(state.scratch_sparse.is_empty());
        assert!(state.dense_inline_marks.len() >= total_groups);
        assert_eq!(state.populated_groups, total_groups);

        for (i, expected) in raw_values.iter().enumerate() {
            assert_eq!(state.min_max[i].as_deref(), Some(expected.as_slice()));
        }
    }

    #[test]
    fn dense_inline_commits_after_stable_batches() {
        let mut state = MinMaxBytesState::new(DataType::Utf8);
        let group_indices = vec![0_usize, 1, 2];
        let values = vec!["a", "b", "c"];

        for batch in 0..5 {
            let iter = values.iter().map(|value| Some(value.as_bytes()));
            state
                .update_batch(iter, &group_indices, 3, |a, b| a < b)
                .expect("update batch");

            if batch < DENSE_INLINE_STABILITY_THRESHOLD {
                assert!(!state.dense_inline_committed);
            } else {
                assert!(state.dense_inline_committed);
                assert!(state.dense_inline_marks.is_empty());
            }
        }
    }

    #[test]
    fn sparse_batch_switches_mode_after_first_update() {
        let mut state = MinMaxBytesState::new(DataType::Utf8);
        let groups = vec![10_usize, 20_usize];
        let values = vec![Some("b".as_bytes()), Some("a".as_bytes())];

        state
            .update_batch(values.iter().copied(), &groups, 1_000_000, |a, b| a < b)
            .expect("first batch");

        assert!(matches!(state.workload_mode, WorkloadMode::SparseOptimized));
        assert_eq!(state.min_max[10].as_deref(), Some("b".as_bytes()));
        assert_eq!(state.min_max[20].as_deref(), Some("a".as_bytes()));

        let groups_second = vec![20_usize];
        let values_second = vec![Some("c".as_bytes())];

        state
            .update_batch(
                values_second.iter().copied(),
                &groups_second,
                1_000_000,
                |a, b| a > b,
            )
            .expect("second batch");

        assert!(matches!(state.workload_mode, WorkloadMode::SparseOptimized));
        assert!(state.scratch_sparse.capacity() >= groups_second.len());
        assert_eq!(state.scratch_dense_limit, 0);
        assert_eq!(state.min_max[20].as_deref(), Some("c".as_bytes()));
    }

    #[test]
    fn sparse_mode_updates_values_from_start() {
        let mut state = MinMaxBytesState::new(DataType::Utf8);
        state.workload_mode = WorkloadMode::SparseOptimized;

        let groups = vec![1_000_000_usize, 2_000_000_usize];
        let values = vec![Some("left".as_bytes()), Some("right".as_bytes())];

        state
            .update_batch(values.iter().copied(), &groups, 2_000_001, |a, b| a < b)
            .expect("sparse update");

        assert!(matches!(state.workload_mode, WorkloadMode::SparseOptimized));
        assert_eq!(state.scratch_dense.len(), 0);
        assert_eq!(state.scratch_dense_limit, 0);
        assert!(state.scratch_sparse.capacity() >= groups.len());
        assert_eq!(state.min_max[1_000_000].as_deref(), Some("left".as_bytes()));
        assert_eq!(
            state.min_max[2_000_000].as_deref(),
            Some("right".as_bytes())
        );
    }

    #[test]
    fn simple_mode_switches_to_sparse_on_low_density() {
        let mut state = MinMaxBytesState::new(DataType::Utf8);

        state.record_batch_stats(
            BatchStats {
                unique_groups: 32,
                max_group_index: Some(31),
            },
            DENSE_INLINE_MAX_TOTAL_GROUPS,
        );
        assert!(matches!(state.workload_mode, WorkloadMode::Simple));

        state.populated_groups = SPARSE_SWITCH_GROUP_THRESHOLD + 1;
        state.lifetime_max_group_index = Some(SPARSE_SWITCH_GROUP_THRESHOLD * 20);

        state.record_batch_stats(
            BatchStats {
                unique_groups: 1,
                max_group_index: Some(SPARSE_SWITCH_GROUP_THRESHOLD * 20),
            },
            SPARSE_SWITCH_GROUP_THRESHOLD * 20 + 1,
        );

        assert!(matches!(state.workload_mode, WorkloadMode::SparseOptimized));
    }
}
