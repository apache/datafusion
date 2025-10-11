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
/// Captures the heuristic driven execution strategy for a given accumulator.
///
/// The state machine starts in [`WorkloadMode::Undecided`] until the first
/// non-null values arrive. Once the workload shape is known we switch to one of
/// the specialised implementations:
///
/// * [`WorkloadMode::DenseInline`] – enabled for dense group domains with a
///   stable `total_num_groups` (≤ 100k) **and** evidence that the accumulator is
///   reused across batches. Marks used to detect first touches are allocated
///   lazily: they are prepared once the accumulator has observed a previous
///   processed batch (i.e. on the second processed batch), so single-batch
///   workloads avoid the allocation cost. After a small number of consecutive
///   stable batches the implementation "commits" to the dense-inline fast
///   path and disables per-batch statistics and mark tracking.
/// * [`WorkloadMode::Simple`] – chosen for single-batch dense workloads where
///   reuse is unlikely. This path stages updates per-batch and then writes
///   results in-place without using the dense-inline marks.
/// * [`WorkloadMode::SparseOptimized`] – kicks in when the cardinality is high
///   or the batches are sparse/irregular; it retains and reuses the sparse
///   scratch machinery (hash-based tracking) introduced by the dense-inline
///   heuristics. Optimized for sparse access patterns.
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
    /// Number of **unique** group ids observed in the processed batch. The
    /// counter is strictly per-batch – duplicates within the batch do not
    /// contribute multiple times and the value intentionally ignores groups
    /// touched in prior batches. This makes the density heuristics resilient to
    /// workloads that repeatedly touch the same domain across many batches.
    unique_groups: usize,
    /// Highest group index encountered in the batch. Unlike `unique_groups`
    /// duplicates matter here because it is used to derive the effective domain
    /// size for density comparisons.
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
    /// Whether the dense inline marks vector should be prepared for the current
    /// batch. We keep this disabled for the very first batch processed in dense
    /// inline mode so that short-lived accumulators avoid the upfront
    /// allocation and zeroing costs. Once a batch with values has been
    /// observed we enable the flag so that subsequent batches allocate the mark
    /// table on demand.
    dense_inline_marks_ready: bool,
    /// Epoch associated with `dense_inline_marks`.
    dense_inline_epoch: u64,
    /// Number of consecutive batches processed while remaining in
    /// `DenseInline` mode.
    dense_inline_stable_batches: usize,
    /// Whether the accumulator has committed to the dense inline fast path and
    /// no longer needs to track per-batch statistics.
    dense_inline_committed: bool,
    /// Total number of groups observed when the dense inline fast path was
    /// committed. If the group domain grows beyond this value we need to
    /// reconsider the workload mode.
    dense_inline_committed_groups: usize,
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
///
const DENSE_INLINE_MAX_TOTAL_GROUPS: usize = 100_000;
/// Minimum observed density (in percent) required to remain on the inline dense
/// path.
const DENSE_INLINE_MIN_DENSITY_PERCENT: usize = 50;

/// Maximum number of groups for which the simple dense path is considered.
const SIMPLE_MODE_MAX_TOTAL_GROUPS: usize = 100_000;
/// Minimum observed density (in percent) required to remain on the simple path.
///
/// The density calculation compares the per-batch `unique_groups` against the
/// effective domain derived from `max_group_index`. Prior to fixing a
/// statistics bug that caused inflated per-batch unique counts (where every
/// non-null row was incorrectly counted), the thresholds used incorrect
/// values. Re-validating with the corrected per-batch counts shows that a
/// 10% density remains the tipping point where the simple path starts to
/// outperform the sparse implementation while avoiding the inline dense
/// path's mark bookkeeping.
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
            dense_inline_marks_ready: false,
            dense_inline_epoch: 0,
            dense_inline_stable_batches: 0,
            dense_inline_committed: false,
            dense_inline_committed_groups: 0,
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

    fn resize_min_max(&mut self, total_num_groups: usize) {
        if total_num_groups < self.min_max.len() {
            let truncated = self.min_max.split_off(total_num_groups);
            // iterate only over Some variants
            for bytes in truncated.into_iter().flatten() {
                debug_assert!(self.total_data_bytes >= bytes.len());
                debug_assert!(self.populated_groups > 0);
                self.total_data_bytes -= bytes.len();
                self.populated_groups -= 1;
            }
        } else if total_num_groups > self.min_max.len() {
            self.min_max.resize(total_num_groups, None);
        }
    }

    /// Dispatch to the appropriate implementation based on workload mode.
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
        // Fast path: detect perfectly sequential dense group indices [0, 1, 2, ..., N-1]
        // This is the common case for dense aggregations and matches the original
        // pre-optimization algorithm behavior with zero overhead.
        //
        // We use a lightweight heuristic check: verify the batch covers every group
        // exactly once by ensuring it spans the full domain and the indices are
        // strictly sequential.
        if group_indices.len() == total_num_groups
            && !group_indices.is_empty()
            && group_indices[0] == 0
            && group_indices[total_num_groups - 1] == total_num_groups - 1
            && group_indices.windows(2).all(|pair| pair[1] == pair[0] + 1)
        {
            let stats = self.update_batch_sequential_dense(
                iter,
                group_indices,
                total_num_groups,
                cmp,
            )?;
            self.record_batch_stats(stats, total_num_groups);
            return Ok(());
        }

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
                if self.dense_inline_committed
                    && total_num_groups > self.dense_inline_committed_groups
                {
                    self.dense_inline_committed = false;
                    self.dense_inline_committed_groups = 0;
                    self.dense_inline_stable_batches = 0;
                    self.dense_inline_marks_ready = false;
                }

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
                    self.update_batch_sparse_impl(
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
        self.resize_min_max(total_num_groups);

        let mut marks_prepared = false;

        let mut unique_groups = 0_usize;
        let mut max_group_index: Option<usize> = None;
        let mut fast_path = true;
        let mut fast_rows = 0_usize;
        let mut fast_start = 0_usize;
        let mut fast_last = 0_usize;

        let mut last_group_index: Option<usize> = None;
        let mut processed_any = false;

        for (&group_index, new_val) in group_indices.iter().zip(iter.into_iter()) {
            let Some(new_val) = new_val else {
                continue;
            };

            processed_any = true;

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
                    if !marks_prepared {
                        self.prepare_dense_inline_marks(total_num_groups);
                        marks_prepared = true;
                    }
                    fast_path = false;
                    if fast_rows > 0 {
                        let fast_unique =
                            fast_last.saturating_sub(fast_start).saturating_add(1);
                        unique_groups = fast_unique;
                        max_group_index = Some(match max_group_index {
                            Some(current_max) => current_max.max(fast_last),
                            None => fast_last,
                        });

                        let epoch = self.dense_inline_epoch;
                        // iterate over the mutable slice instead of indexing by range
                        let marks = &mut self.dense_inline_marks;
                        for mark in marks.iter_mut().take(fast_last + 1).skip(fast_start)
                        {
                            *mark = epoch;
                        }
                    }
                }

                if fast_path {
                    fast_rows = fast_rows.saturating_add(1);
                }
            }

            if !fast_path && !is_consecutive_duplicate {
                if !marks_prepared {
                    self.prepare_dense_inline_marks(total_num_groups);
                    marks_prepared = true;
                }
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

        if fast_path && fast_rows > 0 {
            let fast_unique = fast_last.saturating_sub(fast_start).saturating_add(1);
            unique_groups = fast_unique;
            max_group_index = Some(match max_group_index {
                Some(current_max) => current_max.max(fast_last),
                None => fast_last,
            });
        }

        // Only prepare marks if we've processed at least one batch already.
        // This indicates the accumulator is being reused across multiple batches.
        // For single-batch scenarios, we avoid the allocation overhead entirely.
        if processed_any && self.processed_batches > 0 {
            self.dense_inline_marks_ready = true;
        }

        Ok(BatchStats {
            unique_groups,
            max_group_index,
        })
    }

    fn prepare_dense_inline_marks(&mut self, total_num_groups: usize) {
        if !self.dense_inline_marks_ready {
            self.dense_inline_marks_ready = true;
        }

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
    }

    /// Fast path for perfectly sequential dense group indices [0, 1, 2, ..., N-1].
    ///
    /// This implementation exactly replicates the original pre-optimization algorithm
    /// to achieve zero overhead for the common dense case. Each group appears at most
    /// once per batch so we can evaluate the winning value in a single pass and update
    /// `self.min_max` immediately when the new value beats the current minimum/maximum.
    fn update_batch_sequential_dense<'a, F, I>(
        &mut self,
        iter: I,
        group_indices: &[usize],
        total_num_groups: usize,
        mut cmp: F,
    ) -> Result<BatchStats>
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send + Sync,
        I: IntoIterator<Item = Option<&'a [u8]>>,
    {
        self.resize_min_max(total_num_groups);

        let mut unique_groups = 0_usize;
        let mut max_group_index: Option<usize> = None;

        // Figure out the new min/max value for each group. The sequential fast
        // path is only selected when `group_indices` is exactly `[0, 1, ..., N-1]`
        // for the supplied `total_num_groups`, so each non-null row corresponds
        // to a unique group id. This keeps the loop read-mostly: we only write
        // into `self.min_max` when a new value actually wins.
        for (position, (new_val, group_index)) in
            iter.into_iter().zip(group_indices.iter()).enumerate()
        {
            let group_index = *group_index;
            debug_assert_eq!(
                group_index, position,
                "sequential dense path expects strictly sequential group ids"
            );

            // Track the largest group index encountered in this batch. Unlike
            // `unique_groups`, this intentionally considers every row (including
            // duplicates) because the domain size we derive from
            // `max_group_index` only depends on the highest index touched, not on
            // how many distinct groups contributed to it. This must happen even
            // for null rows to ensure the dense fast path sees the full domain.
            max_group_index = Some(match max_group_index {
                Some(current_max) => current_max.max(group_index),
                None => group_index,
            });

            let Some(new_val) = new_val else {
                continue; // skip nulls
            };

            unique_groups = unique_groups.saturating_add(1);

            let should_replace = match self.min_max[group_index].as_ref() {
                Some(existing_val) => cmp(new_val, existing_val.as_ref()),
                None => true,
            };

            if should_replace {
                self.set_value(group_index, new_val);
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
        self.resize_min_max(total_num_groups);

        for (&group_index, new_val) in group_indices.iter().zip(iter.into_iter()) {
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
        self.resize_min_max(total_num_groups);

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

        for (&group_index, new_val) in group_indices.iter().zip(iter.into_iter()) {
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

    /// Record batch statistics and adaptively select or transition workload mode.
    ///
    /// This function implements the adaptive mode selection heuristic that
    /// improves performance in multi-batch workloads at the cost of some
    /// overhead in single-batch scenarios. The overhead comes from tracking
    /// `unique_groups` and `max_group_index` statistics needed to evaluate
    /// density and choose the optimal execution path.
    /// Capture per-batch statistics and feed them into the adaptive mode
    /// selection heuristic.
    ///
    /// * `stats.unique_groups` counts the distinct group ids in **this** batch.
    ///   It is accumulated into `self.total_groups_seen` so the sparse path can
    ///   reason about long-lived density trends.
    /// * `stats.max_group_index` captures the largest identifier touched in the
    ///   batch and therefore the effective domain size used for density
    ///   comparisons.
    /// * `total_num_groups` is the logical domain configured by the execution
    ///   plan. It acts as an upper bound for allocations and is used alongside
    ///   `unique_groups` to reason about per-batch density.
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
                        self.dense_inline_marks_ready = true;
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
                            self.dense_inline_committed_groups = total_num_groups;
                            self.dense_inline_marks.clear();
                            self.dense_inline_marks_ready = false;
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

        Self::density_at_least(
            unique_groups,
            total_num_groups,
            DENSE_INLINE_MIN_DENSITY_PERCENT,
        )
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
        Self::density_at_least(unique_groups, domain, SIMPLE_MODE_MIN_DENSITY_PERCENT)
    }

    fn should_switch_to_sparse(&self) -> bool {
        if self.populated_groups <= SPARSE_SWITCH_GROUP_THRESHOLD {
            return false;
        }
        let Some(max_group_index) = self.lifetime_max_group_index else {
            return false;
        };
        let domain = max_group_index + 1;
        if domain == 0 {
            return false;
        }

        !Self::density_at_least(
            self.populated_groups,
            domain,
            SPARSE_SWITCH_MAX_DENSITY_PERCENT,
        )
    }

    /// Returns `true` when the observed population covers at least `percent`
    /// percent of the provided domain.
    #[inline]
    fn density_at_least(observed: usize, domain: usize, percent: usize) -> bool {
        if domain == 0 || percent == 0 {
            return false;
        }

        let observed_scaled = observed.saturating_mul(100);
        let required_scaled = domain.saturating_mul(percent);
        observed_scaled >= required_scaled
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
        self.dense_inline_committed_groups = 0;
        self.dense_inline_marks_ready = false;
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
        self.dense_inline_committed_groups = 0;
        self.dense_inline_marks_ready = false;
    }

    fn enter_dense_inline_mode(&mut self) {
        self.enter_simple_mode();
        self.dense_inline_stable_batches = 0;
        self.dense_inline_committed = false;
        self.dense_inline_committed_groups = 0;
        self.dense_inline_marks_ready = false;
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
        let prepared = self.prepare_sparse_batch(total_num_groups);
        let mut state = SparseBatchState::new(prepared, group_indices.len());

        let mut values_iter = iter.into_iter();
        let mut processed = 0usize;
        for (&group_index, new_val) in group_indices.iter().zip(&mut values_iter) {
            processed += 1;

            let Some(new_val) = new_val else {
                continue;
            };

            if group_index >= self.min_max.len() {
                return internal_err!(
                    "group index {group_index} out of bounds for {} groups",
                    self.min_max.len()
                );
            }

            self.process_sparse_value(
                group_index,
                new_val,
                total_num_groups,
                &mut state,
                cmp,
            );
        }

        debug_assert!(
            values_iter.next().is_none(),
            "value iterator longer than group indices"
        );

        if processed != group_indices.len() {
            return internal_err!(
                "value iterator shorter than group indices (processed {processed}, expected {})",
                group_indices.len()
            );
        }

        self.finalize_sparse_batch(state, total_num_groups)
    }

    fn prepare_sparse_batch(&mut self, total_num_groups: usize) -> PreparedSparseBatch {
        self.resize_min_max(total_num_groups);

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
        let scratch_sparse = std::mem::take(&mut self.scratch_sparse);
        let scratch_group_ids = std::mem::take(&mut self.scratch_group_ids);

        self.scratch_dense_limit = self.scratch_dense_limit.min(total_num_groups);
        let use_dense = self.scratch_dense_enabled && self.scratch_dense_limit > 0;

        PreparedSparseBatch {
            scratch_sparse,
            scratch_group_ids,
            use_dense,
        }
    }

    fn process_sparse_value<'a, F>(
        &mut self,
        group_index: usize,
        new_val: &'a [u8],
        total_num_groups: usize,
        state: &mut SparseBatchState<'a>,
        cmp: &mut F,
    ) where
        F: FnMut(&[u8], &[u8]) -> bool + Send + Sync,
    {
        loop {
            match self.apply_sparse_value(
                group_index,
                new_val,
                total_num_groups,
                state,
                cmp,
            ) {
                ProcessResult::Processed => break,
                ProcessResult::Retry => continue,
            }
        }
    }

    fn apply_sparse_value<'a, F>(
        &mut self,
        group_index: usize,
        new_val: &'a [u8],
        total_num_groups: usize,
        state: &mut SparseBatchState<'a>,
        cmp: &mut F,
    ) -> ProcessResult
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send + Sync,
    {
        if state.use_dense {
            match self.try_process_dense_path(
                group_index,
                new_val,
                total_num_groups,
                state,
                cmp,
            ) {
                DenseResult::Handled => return ProcessResult::Processed,
                DenseResult::Retry => return ProcessResult::Retry,
                DenseResult::Fallback => {}
            }
        }

        self.process_sparse_path(group_index, new_val, total_num_groups, state, cmp)
    }

    fn try_process_dense_path<'a, F>(
        &mut self,
        group_index: usize,
        new_val: &'a [u8],
        total_num_groups: usize,
        state: &mut SparseBatchState<'a>,
        cmp: &mut F,
    ) -> DenseResult
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send + Sync,
    {
        let mut allow_dense = group_index < self.scratch_dense_limit;

        if !allow_dense {
            let (potential_unique, potential_max) =
                state.potential_first_touch_metrics(group_index);
            if let Some(candidate_limit) = self.evaluate_dense_candidate(
                potential_unique,
                potential_max,
                total_num_groups,
            ) {
                let mut desired_limit = candidate_limit;
                if desired_limit < self.scratch_dense_limit + SCRATCH_DENSE_GROWTH_STEP {
                    desired_limit = (self.scratch_dense_limit
                        + SCRATCH_DENSE_GROWTH_STEP)
                        .min(total_num_groups);
                }
                desired_limit = desired_limit.min(total_num_groups);
                if self.expand_dense_limit(desired_limit, state) {
                    return DenseResult::Retry;
                }
                allow_dense = group_index < self.scratch_dense_limit;
            }
        }

        if !allow_dense {
            #[cfg(test)]
            {
                debug_assert!(self.scratch_dense_enabled);
                self.dense_sparse_detours += 1;
            }
            return DenseResult::Fallback;
        }

        let mut pending_dense_growth = None;
        let mut first_touch = false;
        {
            let entry = &mut self.scratch_dense[group_index];
            if entry.epoch != self.scratch_epoch {
                entry.epoch = self.scratch_epoch;
                entry.location = ScratchLocation::Existing;
                first_touch = true;
            }

            Self::update_scratch_location(
                &mut entry.location,
                group_index,
                new_val,
                cmp,
                &mut state.batch_inputs,
                &self.min_max,
            );
        }

        if first_touch {
            state.scratch_group_ids.push(group_index);
            state.record_first_touch(group_index);
            if let Some(max_group_index) = state.batch_max_group_index {
                let mut desired_limit = max_group_index + 1;
                if desired_limit < self.scratch_dense_limit + SCRATCH_DENSE_GROWTH_STEP {
                    desired_limit = (self.scratch_dense_limit
                        + SCRATCH_DENSE_GROWTH_STEP)
                        .min(total_num_groups);
                }
                pending_dense_growth = Some(desired_limit.min(total_num_groups));
            }
        }

        if let Some(desired_limit) = pending_dense_growth {
            self.expand_dense_limit(desired_limit, state);
        }

        DenseResult::Handled
    }

    fn process_sparse_path<'a, F>(
        &mut self,
        group_index: usize,
        new_val: &'a [u8],
        total_num_groups: usize,
        state: &mut SparseBatchState<'a>,
        cmp: &mut F,
    ) -> ProcessResult
    where
        F: FnMut(&[u8], &[u8]) -> bool + Send + Sync,
    {
        let mut first_touch = false;
        let mut evaluated_dense_candidate = false;
        loop {
            match state.scratch_sparse.entry(group_index) {
                Entry::Occupied(_) => {
                    break;
                }
                Entry::Vacant(_) => {
                    first_touch = true;

                    if !evaluated_dense_candidate {
                        evaluated_dense_candidate = true;
                        // To avoid holding the VacantEntry guard across an
                        // immutable call on `state`, re-acquire metrics first
                        // by snapshotting what we need and then decide.
                        let (potential_unique, potential_max) =
                            state.potential_first_touch_metrics(group_index);
                        if let Some(candidate_limit) = self.evaluate_dense_candidate(
                            potential_unique,
                            potential_max,
                            total_num_groups,
                        ) {
                            if !state.dense_activated_this_batch
                                && self.enable_dense_for_batch(
                                    candidate_limit,
                                    &mut state.scratch_sparse,
                                    &mut state.scratch_group_ids[..],
                                )
                            {
                                state.dense_activated_this_batch = true;
                                state.use_dense = true;
                                return ProcessResult::Retry;
                            } else if state.dense_activated_this_batch
                                && self.expand_dense_limit(candidate_limit, state)
                            {
                                return ProcessResult::Retry;
                            }

                            // candidate not accepted -> continue the loop and
                            // re-check the entry so we can insert below.
                            continue;
                        }

                        // insert into the vacant slot now that we've finished
                        // the immutable checks
                        match state.scratch_sparse.entry(group_index) {
                            Entry::Vacant(vacant) => {
                                vacant.insert(ScratchLocation::Existing);
                                break;
                            }
                            Entry::Occupied(_) => break,
                        }
                    }

                    // If we've already evaluated the dense candidate, we still
                    // need to insert into the vacant slot. Acquire the vacant
                    // entry fresh and insert.
                    match state.scratch_sparse.entry(group_index) {
                        Entry::Vacant(vacant) => {
                            vacant.insert(ScratchLocation::Existing);
                            break;
                        }
                        Entry::Occupied(_) => break,
                    }
                }
            }
        }

        if first_touch {
            state.scratch_group_ids.push(group_index);
            state.record_first_touch(group_index);
            if let Some(candidate_limit) = self.evaluate_dense_candidate(
                state.batch_unique_groups,
                state.batch_max_group_index,
                total_num_groups,
            ) {
                if !state.dense_activated_this_batch
                    && self.enable_dense_for_batch(
                        candidate_limit,
                        &mut state.scratch_sparse,
                        &mut state.scratch_group_ids[..],
                    )
                {
                    state.dense_activated_this_batch = true;
                    state.use_dense = true;
                    return ProcessResult::Retry;
                } else if state.dense_activated_this_batch
                    && self.expand_dense_limit(candidate_limit, state)
                {
                    return ProcessResult::Retry;
                }
            }
        }

        let location = state
            .scratch_sparse
            .entry(group_index)
            .or_insert(ScratchLocation::Existing);
        Self::update_scratch_location(
            location,
            group_index,
            new_val,
            cmp,
            &mut state.batch_inputs,
            &self.min_max,
        );
        ProcessResult::Processed
    }

    fn update_scratch_location<'a, F>(
        location: &mut ScratchLocation,
        group_index: usize,
        new_val: &'a [u8],
        cmp: &mut F,
        batch_inputs: &mut Vec<&'a [u8]>,
        min_max: &[Option<Vec<u8>>],
    ) where
        F: FnMut(&[u8], &[u8]) -> bool + Send + Sync,
    {
        match *location {
            ScratchLocation::Existing => {
                let Some(existing_val) = min_max[group_index].as_ref() else {
                    let batch_index = batch_inputs.len();
                    batch_inputs.push(new_val);
                    *location = ScratchLocation::Batch(batch_index);
                    return;
                };
                if cmp(new_val, existing_val.as_ref()) {
                    let batch_index = batch_inputs.len();
                    batch_inputs.push(new_val);
                    *location = ScratchLocation::Batch(batch_index);
                }
            }
            ScratchLocation::Batch(existing_idx) => {
                let existing_val = batch_inputs[existing_idx];
                if cmp(new_val, existing_val) {
                    let batch_index = batch_inputs.len();
                    batch_inputs.push(new_val);
                    *location = ScratchLocation::Batch(batch_index);
                }
            }
        }
    }

    fn finalize_sparse_batch<'a>(
        &mut self,
        state: SparseBatchState<'a>,
        total_num_groups: usize,
    ) -> Result<BatchStats> {
        let SparseBatchState {
            mut scratch_sparse,
            mut scratch_group_ids,
            batch_inputs,
            batch_unique_groups,
            batch_max_group_index,
            dense_activated_this_batch: _,
            use_dense,
        } = state;

        if use_dense {
            self.scratch_dense_enabled = true;
        }

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
        scratch_group_ids: &mut [usize],
    ) -> bool {
        if candidate_limit == 0 {
            return false;
        }

        let candidate_limit = candidate_limit.min(self.min_max.len());
        if candidate_limit == 0 {
            return false;
        }

        self.scratch_dense_limit = candidate_limit;
        self.scratch_dense_enabled = true;
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
    fn expand_dense_limit<'a>(
        &mut self,
        candidate_limit: usize,
        state: &mut SparseBatchState<'a>,
    ) -> bool {
        if candidate_limit <= self.scratch_dense_limit {
            return false;
        }

        let candidate_limit = candidate_limit.min(self.min_max.len());
        if candidate_limit <= self.scratch_dense_limit {
            return false;
        }

        let previous_limit = self.scratch_dense_limit;
        self.scratch_dense_limit = candidate_limit;
        if self.scratch_dense.len() < self.scratch_dense_limit {
            self.scratch_dense
                .resize(self.scratch_dense_limit, ScratchEntry::new());
        }

        if self.scratch_dense_enabled {
            // Preserve staged candidates for groups that move from the sparse map into
            // the newly expanded dense range so we do not lose per-batch minima when
            // reprocessing the current row.
            for &group_index in state.scratch_group_ids.iter() {
                if group_index >= self.scratch_dense_limit {
                    continue;
                }

                let entry = &mut self.scratch_dense[group_index];
                if entry.epoch != self.scratch_epoch {
                    let location = state
                        .scratch_sparse
                        .remove(&group_index)
                        .unwrap_or(ScratchLocation::Existing);
                    entry.epoch = self.scratch_epoch;
                    entry.location = location;
                } else if let Some(location) = state.scratch_sparse.remove(&group_index) {
                    entry.location = location;
                }
            }

            // If we are expanding from a zero limit, enable dense tracking so future
            // iterations can reuse the migrated state without reactivation.
            if previous_limit == 0 {
                self.scratch_dense_enabled = true;
            }
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
                let total_bytes = std::mem::take(&mut self.total_data_bytes);
                let min_max = std::mem::take(&mut self.min_max);
                self.reset_after_full_emit();
                (total_bytes, min_max)
            }
            EmitTo::First(n) => {
                let first_min_maxes: Vec<_> = self.min_max.drain(..n).collect();
                let drained_populated = first_min_maxes
                    .iter()
                    .filter(|value| value.is_some())
                    .count();
                let first_data_capacity: usize = first_min_maxes
                    .iter()
                    .map(|opt| opt.as_ref().map(|s| s.len()).unwrap_or(0))
                    .sum();
                self.total_data_bytes =
                    self.total_data_bytes.saturating_sub(first_data_capacity);
                self.populated_groups =
                    self.populated_groups.saturating_sub(drained_populated);
                self.realign_after_partial_emit(n);
                if self.min_max.is_empty() {
                    self.reset_after_full_emit();
                }
                (first_data_capacity, first_min_maxes)
            }
        }
    }

    fn reset_after_full_emit(&mut self) {
        self.total_data_bytes = 0;
        self.populated_groups = 0;
        self.scratch_group_ids.clear();
        self.scratch_dense.clear();
        self.scratch_sparse.clear();
        self.scratch_epoch = 0;
        self.scratch_dense_limit = 0;
        self.scratch_dense_enabled = false;
        self.workload_mode = WorkloadMode::Undecided;
        self.processed_batches = 0;
        self.total_groups_seen = 0;
        self.lifetime_max_group_index = None;
        self.simple_slots.clear();
        self.simple_epoch = 0;
        self.simple_touched_groups.clear();
        self.dense_inline_marks.clear();
        self.dense_inline_marks_ready = false;
        self.dense_inline_epoch = 0;
        self.dense_inline_stable_batches = 0;
        self.dense_inline_committed = false;
        self.dense_inline_committed_groups = 0;
        #[cfg(test)]
        {
            self.dense_enable_invocations = 0;
            self.dense_sparse_detours = 0;
        }
    }

    fn realign_after_partial_emit(&mut self, emitted: usize) {
        if emitted == 0 {
            return;
        }

        let remaining = self.min_max.len();
        if remaining == 0 {
            return;
        }

        self.processed_batches = 0;
        self.total_groups_seen = self.populated_groups;
        self.lifetime_max_group_index = Some(remaining - 1);

        self.scratch_group_ids.clear();
        self.scratch_sparse.clear();
        self.scratch_epoch = 0;
        self.scratch_dense_enabled = false;
        self.scratch_dense_limit = 0;
        self.scratch_dense.clear();

        if emitted >= self.simple_slots.len() {
            self.simple_slots.clear();
        } else {
            self.simple_slots.drain(..emitted);
        }
        self.simple_slots.truncate(remaining);
        for slot in &mut self.simple_slots {
            slot.epoch = 0;
            slot.location = SimpleLocation::Untouched;
        }
        self.simple_epoch = 0;
        self.simple_touched_groups.clear();

        if emitted >= self.dense_inline_marks.len() {
            self.dense_inline_marks.clear();
        } else {
            self.dense_inline_marks.drain(..emitted);
        }
        self.dense_inline_marks.truncate(remaining);
        self.dense_inline_marks_ready = false;
        self.dense_inline_epoch = 0;
        self.dense_inline_stable_batches = 0;
        self.dense_inline_committed = false;
        self.dense_inline_committed_groups = 0;
    }

    fn size(&self) -> usize {
        let mut size = size_of::<Self>();

        size = size.saturating_add(self.total_data_bytes);
        size = size.saturating_add(vec_allocation_bytes(&self.min_max));
        size = size.saturating_add(vec_allocation_bytes(&self.scratch_group_ids));
        size = size.saturating_add(vec_allocation_bytes(&self.scratch_dense));
        size = size.saturating_add(scratch_sparse_allocation_bytes(&self.scratch_sparse));
        size = size.saturating_add(vec_allocation_bytes(&self.simple_slots));
        size = size.saturating_add(vec_allocation_bytes(&self.simple_touched_groups));
        size = size.saturating_add(vec_allocation_bytes(&self.dense_inline_marks));

        size
    }
}
fn vec_allocation_bytes<T>(vec: &Vec<T>) -> usize {
    vec.capacity().saturating_mul(size_of::<T>())
}
fn scratch_sparse_allocation_bytes(map: &HashMap<usize, ScratchLocation>) -> usize {
    // `HashMap` growth strategy and control byte layout are implementation
    // details of hashbrown. Rather than duplicating that logic (which can
    // change across compiler versions or architectures), approximate the
    // allocation using only public APIs. `capacity()` returns the number of
    // buckets currently reserved which bounds the total tuple storage and the
    // control byte array. Each bucket stores the key/value pair plus an
    // implementation defined control byte. We round that control byte up to a
    // full `usize` so the estimate remains an upper bound even if hashbrown
    // widens its groups.
    let capacity = map.capacity();
    let tuple_bytes =
        capacity.saturating_mul(size_of::<usize>() + size_of::<ScratchLocation>());
    let ctrl_bytes = capacity.saturating_mul(size_of::<usize>());

    // Use a simple capacity-based upper bound to approximate the HashMap
    // allocation. The precise control-byte layout and grouping strategy are
    // internal implementation details of `hashbrown` and may change across
    // versions or architectures. Rounding the control area up to a full
    // `usize` per bucket produces a conservative upper bound without
    // depending on internal constants.
    tuple_bytes.saturating_add(ctrl_bytes)
}

struct PreparedSparseBatch {
    scratch_sparse: HashMap<usize, ScratchLocation>,
    scratch_group_ids: Vec<usize>,
    use_dense: bool,
}

struct SparseBatchState<'a> {
    scratch_sparse: HashMap<usize, ScratchLocation>,
    scratch_group_ids: Vec<usize>,
    batch_inputs: Vec<&'a [u8]>,
    batch_unique_groups: usize,
    batch_max_group_index: Option<usize>,
    dense_activated_this_batch: bool,
    use_dense: bool,
}

impl<'a> SparseBatchState<'a> {
    fn new(prepared: PreparedSparseBatch, capacity: usize) -> Self {
        Self {
            scratch_sparse: prepared.scratch_sparse,
            scratch_group_ids: prepared.scratch_group_ids,
            batch_inputs: Vec::with_capacity(capacity),
            batch_unique_groups: 0,
            batch_max_group_index: None,
            dense_activated_this_batch: false,
            use_dense: prepared.use_dense,
        }
    }

    fn potential_first_touch_metrics(
        &self,
        group_index: usize,
    ) -> (usize, Option<usize>) {
        let potential_unique = self.batch_unique_groups + 1;
        let potential_max = match self.batch_max_group_index {
            Some(current_max) if current_max >= group_index => Some(current_max),
            _ => Some(group_index),
        };
        (potential_unique, potential_max)
    }

    fn record_first_touch(&mut self, group_index: usize) {
        self.batch_unique_groups += 1;
        match self.batch_max_group_index {
            Some(current_max) if current_max >= group_index => {}
            _ => self.batch_max_group_index = Some(group_index),
        }
    }
}

enum ProcessResult {
    Processed,
    Retry,
}

enum DenseResult {
    Handled,
    Retry,
    Fallback,
}
