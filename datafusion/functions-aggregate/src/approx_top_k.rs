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

//! Approximate top-k aggregate function using the Filtered Space-Saving algorithm.
//!
//! This implements a distributed-friendly approximate top-k aggregation using
//! the Filtered Space-Saving algorithm. The algorithm maintains a fixed-size summary
//! of counters plus an alpha map (filter) that estimates unmonitored items'
//! frequencies.
//!
//! Usage: `approx_top_k(column, k)`
//! - `column`: The column to find the most frequent values from
//! - `k`: The number of top elements to track (required, literal integer)
//!
//! Returns: `List<Struct { value: <input_type>, count: UInt64 }>` ranked by
//! estimated count, then by error, then by value for deterministic ties (see
//! `Counter` for the exact ordering).
//!
//! `count` is an *upper bound* on the true frequency: each counter also tracks
//! an `error` such that the true frequency lies in `[count - error, count]`.
//! Counters that were never evicted have `error == 0` and are therefore exact.
//! The `error` is used internally to break estimated-count ties but is not
//! exposed in the output.
//!
//! Algorithm references:
//! - Filtered Space-Saving: Homem, Carvalho. "Finding top-k elements in data
//!   streams" (Information Sciences, 2010),
//!   <https://doi.org/10.1016/j.ins.2010.08.024>. Section 8 / equation (24)
//!   gives the alpha map sizing used by `ALPHA_MAP_ELEMENTS_PER_COUNTER`.
//! - Parallel Space Saving: <https://arxiv.org/pdf/1401.0702.pdf>
//! - Space-Saving: Metwally, Agrawal, El Abbadi. "Efficient Computation of Frequent
//!   and Top-k Elements in Data Streams" (ICDT 2005)

use std::cmp::{Ordering, max, min};
use std::hash::BuildHasher;
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BinaryViewArray, BinaryViewBuilder,
    BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeBinaryBuilder,
    LargeStringArray, LargeStringBuilder, ListArray, NullArray, StringArray,
    StringBuilder, StringViewArray, StringViewBuilder, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit};
use hashbrown::{HashMap, HashTable};

use datafusion_common::hash_utils::QualityRandomState;
use datafusion_common::types::{
    logical_binary, logical_date, logical_float32, logical_float64, logical_string,
};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_err, not_impl_err, plan_err,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Coercion, Documentation, EmitTo, GroupsAccumulator,
    Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

make_udaf_expr_and_func!(
    ApproxTopK,
    approx_top_k,
    "Returns the approximate most frequent (top-k) values and their counts.",
    approx_top_k_udaf
);

// ---------------------------------------------------------------------------
// Algorithm constants
// ---------------------------------------------------------------------------

/// Suggested constant from Homem & Carvalho, "Finding top-k elements in data
/// streams", section 8, equation (24). Determines the size of the alpha map
/// relative to the capacity.
const ALPHA_MAP_ELEMENTS_PER_COUNTER: usize = 6;

/// Maximum allowed value for k in `approx_top_k(column, k)`.
const APPROX_TOP_K_MAX_K: usize = 10_000;

/// Capacity multiplier for internal tracking.
///
/// We track more items internally than k to improve accuracy.
/// If user asks for top-5, we internally track top `5 * 3 = 15` items.
const CAPACITY_MULTIPLIER: usize = 3;

/// Fixed high-quality hash state used for both counter lookup and alpha buckets.
/// The seed is part of the serialized sketch semantics: all partial summaries
/// must assign an item to the same alpha bucket in order to merge their maps.
const APPROX_TOP_K_HASH_STATE: QualityRandomState = QualityRandomState::with_seed(0);

const STATE_MAGIC: &[u8; 4] = b"DFTK";
const STATE_VERSION: u8 = 1;
const STATE_HEADER_LEN: usize = 28;
const SINGLETON_HEADER_LEN: usize = 16;
const ALPHA_SPARSE: u8 = 0;
const ALPHA_BITMAP: u8 = 1;
const STATE_SINGLETON: u8 = 2;

/// Stable type descriptor embedded in every intermediate state.
fn state_type_descriptor(data_type: &DataType) -> Result<(u8, u8, Vec<u8>)> {
    let descriptor = match data_type {
        DataType::Null => (0, 0, vec![]),
        DataType::Utf8 => (1, 0, vec![]),
        DataType::LargeUtf8 => (2, 0, vec![]),
        DataType::Utf8View => (3, 0, vec![]),
        DataType::Binary => (4, 0, vec![]),
        DataType::LargeBinary => (5, 0, vec![]),
        DataType::BinaryView => (6, 0, vec![]),
        DataType::Int8 => (10, 1, vec![]),
        DataType::Int16 => (11, 2, vec![]),
        DataType::Int32 => (12, 4, vec![]),
        DataType::Int64 => (13, 8, vec![]),
        DataType::UInt8 => (14, 1, vec![]),
        DataType::UInt16 => (15, 2, vec![]),
        DataType::UInt32 => (16, 4, vec![]),
        DataType::UInt64 => (17, 8, vec![]),
        DataType::Float32 => (18, 4, vec![]),
        DataType::Float64 => (19, 8, vec![]),
        DataType::Date32 => (20, 4, vec![]),
        DataType::Date64 => (21, 8, vec![]),
        DataType::Timestamp(unit, timezone) => {
            let unit_offset = match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 1,
                TimeUnit::Microsecond => 2,
                TimeUnit::Nanosecond => 3,
            };
            let tag = if timezone.is_some() {
                28 + unit_offset
            } else {
                24 + unit_offset
            };
            let params = timezone
                .as_ref()
                .map(|tz| tz.as_bytes().to_vec())
                .unwrap_or_default();
            (tag, 8, params)
        }
        other => return exec_err!("Unsupported data type for approx_top_k: {other}"),
    };
    Ok(descriptor)
}

fn validate_item_bytes(data_type: &DataType, item: &[u8]) -> Result<()> {
    let (_, width, _) = state_type_descriptor(data_type)?;
    if width != 0 && item.len() != width as usize {
        return exec_err!(
            "approx_top_k: corrupt intermediate state (expected {width}-byte value, got {})",
            item.len()
        );
    }
    if matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        std::str::from_utf8(item).map_err(|_| {
            DataFusionError::Execution(
                "approx_top_k: corrupt intermediate state (invalid UTF-8 value)"
                    .to_string(),
            )
        })?;
    }
    Ok(())
}

struct StateReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> StateReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_bytes(&mut self, len: usize, field: &str) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "approx_top_k: corrupt intermediate state ({field} length overflow)"
            ))
        })?;
        if end > self.bytes.len() {
            return exec_err!(
                "approx_top_k: corrupt intermediate state (truncated {field})"
            );
        }
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn read_u8(&mut self, field: &str) -> Result<u8> {
        Ok(self.read_bytes(1, field)?[0])
    }

    fn read_u32(&mut self, field: &str) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.read_bytes(4, field)?.try_into().unwrap(),
        ))
    }

    fn read_u64(&mut self, field: &str) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.read_bytes(8, field)?.try_into().unwrap(),
        ))
    }

    fn finish(self) -> Result<()> {
        if self.offset != self.bytes.len() {
            return exec_err!(
                "approx_top_k: corrupt intermediate state ({} trailing bytes)",
                self.bytes.len() - self.offset
            );
        }
        Ok(())
    }
}

fn serialize_single_counter_state(
    output: &mut Vec<u8>,
    capacity: usize,
    data_type: &DataType,
    item: Option<&[u8]>,
    count: u64,
    error: u64,
) -> Result<()> {
    let (type_tag, item_width, type_params) = state_type_descriptor(data_type)?;
    if let Some(item) = item {
        validate_item_bytes(data_type, item)?;
        if count == 0 || error > count {
            return exec_err!("approx_top_k: invalid singleton count/error interval");
        }
        if count == 1 && error == 0 {
            output.clear();
            let total = SINGLETON_HEADER_LEN
                .checked_add(type_params.len())
                .and_then(|size| size.checked_add(item.len()))
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "approx_top_k singleton state size overflow".to_string(),
                    )
                })?;
            output.try_reserve_exact(total).map_err(|e| {
                DataFusionError::ResourcesExhausted(format!(
                    "Unable to allocate approx_top_k singleton state: {e}"
                ))
            })?;
            output.extend_from_slice(STATE_MAGIC);
            output.push(STATE_VERSION);
            output.push(STATE_SINGLETON);
            output.push(type_tag);
            output.push(item_width);
            output.extend_from_slice(&(capacity as u32).to_le_bytes());
            output.extend_from_slice(&(type_params.len() as u32).to_le_bytes());
            output.extend_from_slice(&type_params);
            output.extend_from_slice(item);
            return Ok(());
        }
    }

    output.clear();
    let item_bytes = item.map_or(0, |item| item.len());
    let total = STATE_HEADER_LEN
        .checked_add(type_params.len())
        .and_then(|size| size.checked_add(item_bytes))
        .and_then(|size| size.checked_add(item.map_or(0, |_| 16)))
        .and_then(|size| {
            size.checked_add(usize::from(item_width == 0 && item.is_some()) * 8)
        })
        .ok_or_else(|| {
            DataFusionError::Execution("approx_top_k state size overflow".to_string())
        })?;
    output.try_reserve_exact(total).map_err(|e| {
        DataFusionError::ResourcesExhausted(format!(
            "Unable to allocate approx_top_k singleton state: {e}"
        ))
    })?;
    output.extend_from_slice(STATE_MAGIC);
    output.push(STATE_VERSION);
    output.push(ALPHA_SPARSE);
    output.push(type_tag);
    output.push(item_width);
    output.extend_from_slice(&(capacity as u32).to_le_bytes());
    output.extend_from_slice(&(u32::from(item.is_some())).to_le_bytes());
    output.extend_from_slice(
        &(SpaceSavingSummary::compute_alpha_map_size(capacity) as u32).to_le_bytes(),
    );
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(&(type_params.len() as u32).to_le_bytes());
    output.extend_from_slice(&type_params);
    if let Some(item) = item {
        if item_width == 0 {
            output.extend_from_slice(&(item.len() as u64).to_le_bytes());
        }
        output.extend_from_slice(item);
        output.extend_from_slice(&count.to_le_bytes());
        output.extend_from_slice(&error.to_le_bytes());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// SpaceSavingSummary  (core algorithm)
// ---------------------------------------------------------------------------

/// Counter entry in the Filtered Space-Saving summary.
///
/// Each entry tracks an item, its estimated count, and the error bound.
/// The algorithm guarantees that the true count lies within `[count - error, count]`.
///
/// [`Ord`] ranks counters *best first*, so sorting a slice of counters yields
/// them in the order they should be reported. The ordering from the original
/// Filtered Space-Saving algorithm is:
///
/// 1. Higher estimated `count` first.
/// 2. Then lower `error`.
/// 3. Then `item` bytes ascending. This last key only exists to make the
///    ordering *total*, which keeps `Ord` consistent with the derived [`Eq`] and
///    makes the reported top-k deterministic when counts tie.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Counter {
    /// The serialized bytes representing the tracked item.
    item: Vec<u8>,
    /// Hash of the item (cached to avoid recomputation).
    hash: u64,
    /// The estimated frequency count (may overestimate due to eviction handling).
    count: u64,
    /// The maximum possible overestimation (error bound).
    error: u64,
}

impl Ord for Counter {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed on count so sorting places the best counter first.
        other
            .count
            .cmp(&self.count)
            .then_with(|| self.error.cmp(&other.error))
            .then_with(|| self.item.cmp(&other.item))
    }
}

impl PartialOrd for Counter {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Filtered Space-Saving algorithm summary for approximate top-k / heavy hitters.
///
/// Uses a [`HashTable`] that stores `(hash, index)` tuples for O(1) counter
/// lookups without duplicating the key bytes.  The actual item data lives in
/// `counters[index].item`. An alpha map estimates the frequencies of filtered
/// and evicted items.
///
/// All heap buffers grow on demand.  Nothing is pre-allocated from `k`, because
/// under `GROUP BY` there is one accumulator (and therefore one summary) per
/// group: eagerly sizing the alpha map from `k` would cost
/// `next_power_of_two(k * CAPACITY_MULTIPLIER * ALPHA_MAP_ELEMENTS_PER_COUNTER) * 8`
/// bytes for every group, including singleton groups that never evict anything.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AlphaMap {
    Empty,
    Sparse(HashMap<usize, u64>),
    Dense(Vec<u64>),
}

impl AlphaMap {
    fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    fn get(&self, slot: usize) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Sparse(values) => values.get(&slot).copied().unwrap_or(0),
            Self::Dense(values) => values[slot],
        }
    }

    fn add(&mut self, slot: usize, increment: u64, slots: usize) -> u64 {
        if increment == 0 {
            return self.get(slot);
        }
        match self {
            Self::Empty => {
                let mut values = HashMap::new();
                values.insert(slot, increment);
                *self = Self::Sparse(values);
                increment
            }
            Self::Sparse(values) => {
                let value = values.entry(slot).or_default();
                *value = value.saturating_add(increment);
                let result = *value;
                if values.len() >= max(8, slots / 4) {
                    let Self::Sparse(values) = std::mem::replace(self, Self::Empty)
                    else {
                        unreachable!()
                    };
                    let mut dense = vec![0; slots];
                    for (slot, value) in values {
                        dense[slot] = value;
                    }
                    *self = Self::Dense(dense);
                }
                result
            }
            Self::Dense(values) => {
                values[slot] = values[slot].saturating_add(increment);
                values[slot]
            }
        }
    }

    fn set(&mut self, slot: usize, value: u64, slots: usize) -> u64 {
        debug_assert_ne!(value, 0);
        match self {
            Self::Empty => {
                let mut values = HashMap::new();
                values.insert(slot, value);
                *self = Self::Sparse(values);
            }
            Self::Sparse(values) => {
                values.insert(slot, value);
                if values.len() >= max(8, slots / 4) {
                    let Self::Sparse(values) = std::mem::replace(self, Self::Empty)
                    else {
                        unreachable!()
                    };
                    let mut dense = vec![0; slots];
                    for (slot, value) in values {
                        dense[slot] = value;
                    }
                    *self = Self::Dense(dense);
                }
            }
            Self::Dense(values) => values[slot] = value,
        }
        value
    }

    fn max_value(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Sparse(values) => values.values().copied().max().unwrap_or(0),
            Self::Dense(values) => values.iter().copied().max().unwrap_or(0),
        }
    }

    fn for_each(&self, mut visit: impl FnMut(usize, u64)) {
        match self {
            Self::Empty => {}
            Self::Sparse(values) => {
                for (&slot, &value) in values {
                    visit(slot, value);
                }
            }
            Self::Dense(values) => {
                for (slot, &value) in values.iter().enumerate() {
                    if value != 0 {
                        visit(slot, value);
                    }
                }
            }
        }
    }

    fn sorted_entries(&self) -> Vec<(usize, u64)> {
        let mut entries = Vec::new();
        self.for_each(|slot, value| entries.push((slot, value)));
        entries.sort_unstable_by_key(|&(slot, _)| slot);
        entries
    }

    fn allocation_size(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Sparse(values) => values.allocation_size(),
            Self::Dense(values) => values.capacity() * size_of::<u64>(),
        }
    }
}

#[derive(Debug, Clone)]
struct SpaceSavingSummary {
    counters: Vec<Counter>,
    /// Maps `(cached_hash, counter_index)`.  Lookups use the cached hash for
    /// the fast path and fall back to byte equality via the `counters` vec.
    counter_map: HashTable<(u64, usize)>,
    /// Counter indices in a min-heap ordered with the least desirable counter
    /// at the root. Counter storage stays stable so hash-table indices remain
    /// valid while heap entries move.
    min_heap: Vec<usize>,
    /// Reverse mapping from counter index to its position in `min_heap`.
    heap_positions: Vec<usize>,
    /// Frequency estimate for unmonitored items, indexed by `hash & (len - 1)`.
    ///
    /// Sparse until enough buckets are populated for a dense representation to
    /// use less memory. An empty map reads as all-zero alphas.
    alpha_map: AlphaMap,
    /// Exact maximum filter estimate, cached for distributed merge corrections.
    alpha_max: u64,
    alpha_nonzero: usize,
    requested_capacity: usize,
    /// Running total of heap bytes owned by counter item `Vec`s.
    /// Updated on push / evict / clone so that `size()` is O(1).
    item_heap_bytes: usize,
}

impl SpaceSavingSummary {
    fn compute_alpha_map_size(capacity: usize) -> usize {
        capacity
            .saturating_mul(ALPHA_MAP_ELEMENTS_PER_COUNTER)
            .next_power_of_two()
    }

    /// Fixed high-quality hash for item bytes.
    fn hash_item(item: &[u8]) -> u64 {
        APPROX_TOP_K_HASH_STATE.hash_one(item)
    }

    /// Create an empty summary that will track up to `capacity` counters.
    ///
    /// No heap allocation happens here; see the note on [`SpaceSavingSummary`].
    fn new(capacity: usize) -> Self {
        Self {
            counters: Vec::new(),
            counter_map: HashTable::new(),
            min_heap: Vec::new(),
            heap_positions: Vec::new(),
            alpha_map: AlphaMap::Empty,
            alpha_max: 0,
            alpha_nonzero: 0,
            requested_capacity: capacity,
            item_heap_bytes: 0,
        }
    }

    /// Read the alpha (historical evicted frequency) for `hash`.
    ///
    /// Returns 0 while the alpha map is still unallocated, which is the correct
    /// value: nothing has been filtered or evicted yet.
    fn alpha_for(&self, hash: u64) -> u64 {
        let mask = Self::compute_alpha_map_size(self.requested_capacity) - 1;
        self.alpha_map.get((hash as usize) & mask)
    }

    fn alpha_slots(&self) -> usize {
        Self::compute_alpha_map_size(self.requested_capacity)
    }

    fn alpha_slot(&self, hash: u64) -> usize {
        (hash as usize) & (self.alpha_slots() - 1)
    }

    fn add_alpha(&mut self, hash: u64, increment: u64) -> u64 {
        let slots = self.alpha_slots();
        let slot = self.alpha_slot(hash);
        if increment != 0 && self.alpha_map.get(slot) == 0 {
            self.alpha_nonzero += 1;
        }
        let alpha = self.alpha_map.add(slot, increment, slots);
        self.alpha_max = max(self.alpha_max, alpha);
        alpha
    }

    /// Store the evicted counter estimate in its filter bucket, as specified by
    /// Filtered Space-Saving. Assignment is intentional: a filter cell estimates
    /// unmonitored items in that bucket rather than accumulating evicted counts.
    fn record_eviction(&mut self, hash: u64, count: u64) {
        let slots = self.alpha_slots();
        let slot = self.alpha_slot(hash);
        let previous = self.alpha_map.get(slot);
        if previous == 0 {
            self.alpha_nonzero += 1;
        }
        self.alpha_map.set(slot, count, slots);
        if count >= self.alpha_max {
            self.alpha_max = count;
        } else if previous == self.alpha_max {
            self.alpha_max = self.alpha_map.max_value();
        }
    }

    /// Preserve an upper bound when a distributed merge discards a candidate.
    /// Merge is an extension to the original single-stream algorithm, so an
    /// existing combined partition bound must never be reduced.
    fn record_omission_bound(&mut self, hash: u64, count: u64) {
        let slots = self.alpha_slots();
        let slot = self.alpha_slot(hash);
        let count = max(self.alpha_map.get(slot), count);
        if self.alpha_map.get(slot) == 0 {
            self.alpha_nonzero += 1;
        }
        self.alpha_map.set(slot, count, slots);
        self.alpha_max = max(self.alpha_max, count);
    }

    fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.counters.len()
    }

    #[cfg(test)]
    fn capacity(&self) -> usize {
        self.requested_capacity
    }

    /// Find a counter by item bytes, using the pre-computed hash for fast
    /// lookup and falling back to byte equality for collision resolution.
    fn find_counter_idx(&self, item: &[u8], hash: u64) -> Option<usize> {
        self.counter_map
            .find(hash, |&(h, idx)| {
                h == hash && self.counters[idx].item == item
            })
            .map(|&(_, idx)| idx)
    }

    #[cfg(test)]
    fn find_counter(&self, item: &[u8]) -> Option<&Counter> {
        let hash = Self::hash_item(item);
        self.find_counter_idx(item, hash)
            .map(|idx| &self.counters[idx])
    }

    /// Process one stream item using the original Filtered Space-Saving update.
    fn add(&mut self, item: &[u8]) {
        let hash = Self::hash_item(item);

        // Fast path: item already tracked.
        if let Some(idx) = self.find_counter_idx(item, hash) {
            self.counters[idx].count = self.counters[idx].count.saturating_add(1);
            self.repair_heap(idx);
            return;
        }

        // Below capacity: add directly.
        if self.counters.len() < self.requested_capacity {
            self.push_counter(item, hash, 1, 0);
            return;
        }

        // Filter an unmonitored item until its bucket estimate reaches the
        // minimum tracked estimate. This is the defining admission rule of the
        // original Filtered Space-Saving algorithm.
        let alpha = self.alpha_for(hash);
        let min_idx = self.min_heap[0];
        let min_count = self.counters[min_idx].count;
        if alpha.saturating_add(1) < min_count {
            self.add_alpha(hash, 1);
            return;
        }

        let victim_hash = self.counters[min_idx].hash;
        self.record_eviction(victim_hash, min_count);
        // Read alpha after recording the victim. When both values hash to the
        // same bucket, this ordering is required by the original algorithm.
        let admission_alpha = self.alpha_for(hash);
        self.replace_min_counter(
            item,
            hash,
            admission_alpha.saturating_add(1),
            admission_alpha,
        );
    }

    fn push_counter(&mut self, item: &[u8], hash: u64, count: u64, error: u64) {
        self.push_owned_counter(Counter {
            item: item.to_vec(),
            hash,
            count,
            error,
        });
    }

    fn push_owned_counter(&mut self, counter: Counter) {
        let idx = self.counters.len();
        self.item_heap_bytes =
            self.item_heap_bytes.saturating_add(counter.item.capacity());
        self.counter_map
            .insert_unique(counter.hash, (counter.hash, idx), |&(h, _)| h);
        self.counters.push(counter);
        let heap_pos = self.min_heap.len();
        self.min_heap.push(idx);
        self.heap_positions.push(heap_pos);
        self.sift_up(heap_pos);
    }

    fn replace_min_counter(&mut self, item: &[u8], hash: u64, count: u64, error: u64) {
        let idx = self.min_heap[0];
        let old_hash = self.counters[idx].hash;
        self.counter_map
            .find_entry(old_hash, |&(_, counter_idx)| counter_idx == idx)
            .expect("counter map must contain the minimum counter")
            .remove();

        let old_capacity = self.counters[idx].item.capacity();
        if old_capacity > max(1_024, item.len().saturating_mul(4)) {
            self.counters[idx].item = item.to_vec();
        } else {
            self.counters[idx].item.clear();
            self.counters[idx].item.extend_from_slice(item);
        }
        let new_capacity = self.counters[idx].item.capacity();
        self.item_heap_bytes = self
            .item_heap_bytes
            .saturating_sub(old_capacity)
            .saturating_add(new_capacity);
        self.counters[idx].hash = hash;
        self.counters[idx].count = count;
        self.counters[idx].error = error;
        self.counter_map
            .insert_unique(hash, (hash, idx), |&(h, _)| h);
        self.sift_down(0);
    }

    fn counter_is_worse(&self, left: usize, right: usize) -> bool {
        self.counters[left] > self.counters[right]
    }

    fn swap_heap_entries(&mut self, left: usize, right: usize) {
        self.min_heap.swap(left, right);
        self.heap_positions[self.min_heap[left]] = left;
        self.heap_positions[self.min_heap[right]] = right;
    }

    fn sift_up(&mut self, mut pos: usize) {
        while pos != 0 {
            let parent = (pos - 1) / 2;
            if !self.counter_is_worse(self.min_heap[pos], self.min_heap[parent]) {
                break;
            }
            self.swap_heap_entries(pos, parent);
            pos = parent;
        }
    }

    fn sift_down(&mut self, mut pos: usize) {
        loop {
            let left = pos * 2 + 1;
            if left >= self.min_heap.len() {
                break;
            }
            let right = left + 1;
            let mut worst = left;
            if right < self.min_heap.len()
                && self.counter_is_worse(self.min_heap[right], self.min_heap[left])
            {
                worst = right;
            }
            if !self.counter_is_worse(self.min_heap[worst], self.min_heap[pos]) {
                break;
            }
            self.swap_heap_entries(pos, worst);
            pos = worst;
        }
    }

    fn repair_heap(&mut self, counter_idx: usize) {
        let pos = self.heap_positions[counter_idx];
        if pos != 0 && self.counter_is_worse(counter_idx, self.min_heap[(pos - 1) / 2]) {
            self.sift_up(pos);
        } else {
            self.sift_down(pos);
        }
    }

    fn rebuild_min_heap(&mut self) {
        self.min_heap.clear();
        self.heap_positions.clear();
        self.min_heap.extend(0..self.counters.len());
        self.heap_positions.extend(0..self.counters.len());
        for pos in (0..self.min_heap.len() / 2).rev() {
            self.sift_down(pos);
        }
    }

    /// Reduce candidates produced by a distributed merge. Streaming updates
    /// never exceed capacity; merge is the only operation requiring reduction.
    fn reduce_to_requested_capacity(&mut self) {
        let k = self.requested_capacity;
        if k == 0 || k >= self.counters.len() {
            self.rebuild_min_heap();
            return;
        }

        self.counters.select_nth_unstable(k - 1);
        let omitted: Vec<_> = self.counters[k..]
            .iter()
            .map(|counter| (counter.hash, counter.count))
            .collect();
        for (hash, count) in omitted {
            self.record_omission_bound(hash, count);
        }
        let released = self.counters[k..]
            .iter()
            .map(|counter| counter.item.capacity())
            .sum::<usize>();
        self.item_heap_bytes = self.item_heap_bytes.saturating_sub(released);
        self.counters.truncate(k);
        self.rebuild_counter_map();
        self.rebuild_min_heap();
    }

    /// Rebuild the `counter_map` from the current `counters` vec.
    fn rebuild_counter_map(&mut self) {
        self.counter_map.clear();
        for (idx, counter) in self.counters.iter().enumerate() {
            self.counter_map.insert_unique(
                counter.hash,
                (counter.hash, idx),
                |&(h, _)| h,
            );
        }
    }

    #[cfg(test)]
    fn get(&self, item: &[u8]) -> Option<(u64, u64)> {
        self.find_counter(item).map(|c| (c.count, c.error))
    }

    /// Borrow the `n` highest-ranked counters, sorted best-first.
    ///
    /// Because [`Counter`]'s ordering is total, the result is deterministic even
    /// when counts and errors tie.
    fn ranked_counters(&self, n: usize) -> Vec<&Counter> {
        if n == 0 || self.counters.is_empty() {
            return Vec::new();
        }

        let mut ranked: Vec<&Counter> = self.counters.iter().collect();
        let keep = min(ranked.len(), n);

        if keep < ranked.len() {
            ranked.select_nth_unstable(keep - 1);
            ranked.truncate(keep);
        }

        ranked.sort_unstable();
        ranked
    }

    /// Get the top-k items as `(item, count, error)` in [`Counter`] order.
    fn top_k(&self, k: usize) -> Vec<(&[u8], u64, u64)> {
        self.ranked_counters(k)
            .into_iter()
            .map(|c| (c.item.as_slice(), c.count, c.error))
            .collect()
    }

    /// Merge another summary using the Parallel Space-Saving reduce-and-combine
    /// algorithm from <https://arxiv.org/pdf/1401.0702.pdf>, extended with the
    /// Filtered Space-Saving alpha bounds.
    fn merge(&mut self, other: SpaceSavingSummary) -> Result<()> {
        if other.is_empty() {
            return Ok(());
        }

        if self.is_empty() {
            *self = other;
            return Ok(());
        }

        // Compute m1/m2: the largest alpha in each full summary. In Filtered
        // Space-Saving this bounds the frequency hidden in an omitted item.
        let m1 = if self.counters.len() >= self.requested_capacity {
            self.alpha_max
        } else {
            0
        };
        let m2 = if other.counters.len() >= other.requested_capacity {
            other.alpha_max
        } else {
            0
        };

        // Merge existing counters first. Common items combine directly, while
        // items omitted by `other` receive its omission bound. Applying the
        // correction only to omitted items avoids trying to undo a saturating
        // addition for common counters.
        for counter in &mut self.counters {
            if let Some(other_idx) = other.find_counter_idx(&counter.item, counter.hash) {
                let other_counter = &other.counters[other_idx];
                counter.count = counter.count.saturating_add(other_counter.count);
                counter.error = counter.error.saturating_add(other_counter.error);
            } else if m2 > 0 {
                counter.count = counter.count.saturating_add(m2);
                counter.error = counter.error.saturating_add(m2);
            }
        }

        // Add counters that are present only in `other`, correcting them with
        // this summary's omission bound.
        let unmatched = other
            .counters
            .iter()
            .filter(|counter| {
                self.find_counter_idx(&counter.item, counter.hash).is_none()
            })
            .count();
        self.counters.try_reserve(unmatched).map_err(|e| {
            DataFusionError::ResourcesExhausted(format!(
                "Unable to grow approx_top_k counters during merge: {e}"
            ))
        })?;
        self.counter_map
            .try_reserve(unmatched, |&(hash, _)| hash)
            .map_err(|e| {
                DataFusionError::ResourcesExhausted(format!(
                    "Unable to grow approx_top_k counter map during merge: {e}"
                ))
            })?;
        for mut other_counter in other.counters {
            if self
                .find_counter_idx(&other_counter.item, other_counter.hash)
                .is_none()
            {
                other_counter.count = other_counter.count.saturating_add(m1);
                other_counter.error = other_counter.error.saturating_add(m1);
                self.push_owned_counter(other_counter);
            }
        }

        // Alpha buckets represent disjoint input partitions, so combine them
        // additively. An unallocated map is the all-zero map.
        if !other.alpha_map.is_empty() {
            let alpha_slots = self.alpha_slots();
            other.alpha_map.for_each(|slot, other_alpha| {
                if self.alpha_map.get(slot) == 0 {
                    self.alpha_nonzero += 1;
                }
                let alpha = self.alpha_map.add(slot, other_alpha, alpha_slots);
                self.alpha_max = max(self.alpha_max, alpha);
            });
        }

        self.reduce_to_requested_capacity();
        Ok(())
    }

    /// Serialize a complete, mergeable Filtered Space-Saving summary.
    ///
    /// The alpha map is encoded sparsely or as a bitmap plus packed `u64`
    /// values, whichever is smaller. This preserves the omission bounds needed
    /// by distributed merges without writing a dense zero-filled map.
    fn serialize(&self, data_type: &DataType) -> Result<Vec<u8>> {
        if self.is_empty() {
            return Ok(Vec::new());
        }

        let (type_tag, item_width, type_params) = state_type_descriptor(data_type)?;
        let alpha_slots = Self::compute_alpha_map_size(self.requested_capacity);
        let alpha_nonzero = self.alpha_nonzero;
        let bitmap_bytes = alpha_slots / 8;
        let sparse_bytes = alpha_nonzero.checked_mul(12).ok_or_else(|| {
            DataFusionError::Execution("approx_top_k state size overflow".to_string())
        })?;
        let packed_bitmap_bytes = bitmap_bytes
            .checked_add(alpha_nonzero.checked_mul(8).ok_or_else(|| {
                DataFusionError::Execution("approx_top_k state size overflow".to_string())
            })?)
            .ok_or_else(|| {
                DataFusionError::Execution("approx_top_k state size overflow".to_string())
            })?;
        let alpha_encoding = if packed_bitmap_bytes < sparse_bytes {
            ALPHA_BITMAP
        } else {
            ALPHA_SPARSE
        };

        let variable_items = item_width == 0;
        let counter_bytes = self.counters.iter().try_fold(0usize, |total, counter| {
            validate_item_bytes(data_type, &counter.item)?;
            let record = counter
                .item
                .len()
                .checked_add(16 + usize::from(variable_items) * 8)
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "approx_top_k state size overflow".to_string(),
                    )
                })?;
            total.checked_add(record).ok_or_else(|| {
                DataFusionError::Execution("approx_top_k state size overflow".to_string())
            })
        })?;
        let alpha_bytes = if alpha_encoding == ALPHA_SPARSE {
            sparse_bytes
        } else {
            packed_bitmap_bytes
        };
        let total = STATE_HEADER_LEN
            .checked_add(type_params.len())
            .and_then(|n| n.checked_add(counter_bytes))
            .and_then(|n| n.checked_add(alpha_bytes))
            .ok_or_else(|| {
                DataFusionError::Execution("approx_top_k state size overflow".to_string())
            })?;

        let mut bytes = Vec::new();
        bytes.try_reserve_exact(total).map_err(|e| {
            DataFusionError::ResourcesExhausted(format!(
                "Unable to allocate approx_top_k state: {e}"
            ))
        })?;
        bytes.extend_from_slice(STATE_MAGIC);
        bytes.push(STATE_VERSION);
        bytes.push(alpha_encoding);
        bytes.push(type_tag);
        bytes.push(item_width);
        bytes.extend_from_slice(&(self.requested_capacity as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.counters.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(alpha_slots as u32).to_le_bytes());
        bytes.extend_from_slice(&(alpha_nonzero as u32).to_le_bytes());
        bytes.extend_from_slice(&(type_params.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&type_params);

        let ranked = self.ranked_counters(self.requested_capacity);
        for counter in ranked {
            if variable_items {
                bytes.extend_from_slice(&(counter.item.len() as u64).to_le_bytes());
            }
            bytes.extend_from_slice(&counter.item);
            bytes.extend_from_slice(&counter.count.to_le_bytes());
            bytes.extend_from_slice(&counter.error.to_le_bytes());
        }

        if alpha_encoding == ALPHA_SPARSE {
            match &self.alpha_map {
                AlphaMap::Dense(values) => {
                    for (slot, &alpha) in values.iter().enumerate() {
                        if alpha != 0 {
                            bytes.extend_from_slice(&(slot as u32).to_le_bytes());
                            bytes.extend_from_slice(&alpha.to_le_bytes());
                        }
                    }
                }
                _ => {
                    for (slot, alpha) in self.alpha_map.sorted_entries() {
                        bytes.extend_from_slice(&(slot as u32).to_le_bytes());
                        bytes.extend_from_slice(&alpha.to_le_bytes());
                    }
                }
            }
        } else {
            let bitmap_start = bytes.len();
            bytes.resize(bitmap_start + bitmap_bytes, 0);
            match &self.alpha_map {
                AlphaMap::Dense(values) => {
                    for (slot, &alpha) in values.iter().enumerate() {
                        if alpha != 0 {
                            bytes[bitmap_start + slot / 8] |= 1 << (slot % 8);
                            bytes.extend_from_slice(&alpha.to_le_bytes());
                        }
                    }
                }
                _ => {
                    for (slot, alpha) in self.alpha_map.sorted_entries() {
                        bytes[bitmap_start + slot / 8] |= 1 << (slot % 8);
                        bytes.extend_from_slice(&alpha.to_le_bytes());
                    }
                }
            }
        }

        debug_assert_eq!(bytes.len(), total);
        Ok(bytes)
    }

    /// Deserialize a versioned summary, validating it against the receiving
    /// accumulator's exact capacity and input type.
    fn from_bytes(
        bytes: &[u8],
        expected_capacity: usize,
        expected_data_type: &DataType,
    ) -> Result<Self> {
        let mut reader = StateReader::new(bytes);
        if reader.read_bytes(4, "magic")? != STATE_MAGIC {
            return exec_err!("approx_top_k: corrupt intermediate state (bad magic)");
        }
        let version = reader.read_u8("version")?;
        if version != STATE_VERSION {
            return exec_err!(
                "approx_top_k: unsupported intermediate state version {version}"
            );
        }
        let alpha_encoding = reader.read_u8("alpha encoding")?;
        if alpha_encoding == STATE_SINGLETON {
            let type_tag = reader.read_u8("type tag")?;
            let item_width = reader.read_u8("item width")?;
            let requested_capacity = reader.read_u32("capacity")? as usize;
            let type_params_len = reader.read_u32("type parameters length")? as usize;
            let type_params = reader.read_bytes(type_params_len, "type parameters")?;
            let (expected_tag, expected_width, expected_params) =
                state_type_descriptor(expected_data_type)?;
            if type_tag != expected_tag
                || item_width != expected_width
                || type_params != expected_params
                || matches!(expected_data_type, DataType::Null)
            {
                return exec_err!(
                    "approx_top_k: corrupt intermediate state (singleton value type does not match {expected_data_type})"
                );
            }
            if requested_capacity != expected_capacity
                || requested_capacity == 0
                || requested_capacity > APPROX_TOP_K_MAX_K * CAPACITY_MULTIPLIER
            {
                return exec_err!(
                    "approx_top_k: corrupt intermediate state (invalid singleton capacity {requested_capacity})"
                );
            }
            let item = reader.read_bytes(
                reader.bytes.len().saturating_sub(reader.offset),
                "singleton item",
            )?;
            validate_item_bytes(expected_data_type, item)?;
            reader.finish()?;

            let mut owned = Vec::new();
            owned.try_reserve_exact(item.len()).map_err(|e| {
                DataFusionError::ResourcesExhausted(format!(
                    "Unable to allocate approx_top_k singleton item: {e}"
                ))
            })?;
            owned.extend_from_slice(item);
            let hash = Self::hash_item(&owned);
            let mut summary = Self::new(requested_capacity);
            summary.counters.try_reserve_exact(1).map_err(|e| {
                DataFusionError::ResourcesExhausted(format!(
                    "Unable to allocate approx_top_k singleton counter: {e}"
                ))
            })?;
            summary
                .counter_map
                .try_reserve(1, |&(hash, _)| hash)
                .map_err(|e| {
                    DataFusionError::ResourcesExhausted(format!(
                        "Unable to allocate approx_top_k singleton counter map: {e}"
                    ))
                })?;
            summary.push_owned_counter(Counter {
                item: owned,
                hash,
                count: 1,
                error: 0,
            });
            return Ok(summary);
        }
        if !matches!(alpha_encoding, ALPHA_SPARSE | ALPHA_BITMAP) {
            return exec_err!(
                "approx_top_k: corrupt intermediate state (unknown alpha encoding {alpha_encoding})"
            );
        }
        let type_tag = reader.read_u8("type tag")?;
        let item_width = reader.read_u8("item width")?;
        let requested_capacity = reader.read_u32("capacity")? as usize;
        let num_counters = reader.read_u32("counter count")? as usize;
        let alpha_slots = reader.read_u32("alpha slot count")? as usize;
        let alpha_nonzero = reader.read_u32("alpha nonzero count")? as usize;
        let type_params_len = reader.read_u32("type parameters length")? as usize;
        let type_params = reader.read_bytes(type_params_len, "type parameters")?;

        let (expected_tag, expected_width, expected_params) =
            state_type_descriptor(expected_data_type)?;
        if type_tag != expected_tag
            || item_width != expected_width
            || type_params != expected_params
        {
            return exec_err!(
                "approx_top_k: corrupt intermediate state (value type does not match {expected_data_type})"
            );
        }
        if requested_capacity != expected_capacity {
            return exec_err!(
                "approx_top_k: corrupt intermediate state (capacity {requested_capacity} does not match expected {expected_capacity})"
            );
        }
        if requested_capacity == 0
            || requested_capacity > APPROX_TOP_K_MAX_K * CAPACITY_MULTIPLIER
        {
            return exec_err!(
                "approx_top_k: corrupt intermediate state (invalid capacity {requested_capacity})"
            );
        }
        if num_counters > requested_capacity {
            return exec_err!(
                "approx_top_k: corrupt intermediate state ({num_counters} entries exceeds capacity {requested_capacity})"
            );
        }
        if matches!(expected_data_type, DataType::Null) && num_counters != 0 {
            return exec_err!(
                "approx_top_k: corrupt intermediate state (Null state contains values)"
            );
        }
        let expected_alpha_slots = Self::compute_alpha_map_size(requested_capacity);
        if alpha_slots != expected_alpha_slots || !alpha_slots.is_power_of_two() {
            return exec_err!(
                "approx_top_k: corrupt intermediate state (invalid alpha slot count {alpha_slots})"
            );
        }
        if alpha_nonzero > alpha_slots
            || (alpha_nonzero != 0 && num_counters != requested_capacity)
        {
            return exec_err!(
                "approx_top_k: corrupt intermediate state (invalid alpha nonzero count {alpha_nonzero})"
            );
        }

        let min_record_bytes = if item_width == 0 {
            24
        } else {
            item_width as usize + 16
        };
        if num_counters
            > reader.bytes.len().saturating_sub(reader.offset) / min_record_bytes
        {
            return exec_err!(
                "approx_top_k: corrupt intermediate state (counter count exceeds payload)"
            );
        }

        let mut counters: Vec<Counter> = Vec::new();
        counters.try_reserve_exact(num_counters).map_err(|e| {
            DataFusionError::ResourcesExhausted(format!(
                "Unable to allocate approx_top_k counters: {e}"
            ))
        })?;
        let mut counter_map: HashTable<(u64, usize)> = HashTable::new();
        counter_map
            .try_reserve(num_counters, |&(hash, _)| hash)
            .map_err(|e| {
                DataFusionError::ResourcesExhausted(format!(
                    "Unable to allocate approx_top_k counter map: {e}"
                ))
            })?;
        let mut item_heap_bytes = 0usize;

        for idx in 0..num_counters {
            let item_len = if item_width == 0 {
                usize::try_from(reader.read_u64("item length")?).map_err(|_| {
                    DataFusionError::Execution(
                        "approx_top_k: corrupt intermediate state (item length overflow)"
                            .to_string(),
                    )
                })?
            } else {
                item_width as usize
            };
            let item_bytes = reader.read_bytes(item_len, "item")?;
            validate_item_bytes(expected_data_type, item_bytes)?;
            let count = reader.read_u64("count")?;
            let error = reader.read_u64("error")?;
            if count == 0 || error > count {
                return exec_err!(
                    "approx_top_k: corrupt intermediate state (invalid count/error interval {count}/{error})"
                );
            }

            let hash = Self::hash_item(item_bytes);
            if counter_map
                .find(hash, |&(h, existing_idx)| {
                    h == hash && counters[existing_idx].item == item_bytes
                })
                .is_some()
            {
                return exec_err!(
                    "approx_top_k: corrupt intermediate state (duplicate value)"
                );
            }
            let mut item = Vec::new();
            item.try_reserve_exact(item_len).map_err(|e| {
                DataFusionError::ResourcesExhausted(format!(
                    "Unable to allocate approx_top_k item: {e}"
                ))
            })?;
            item.extend_from_slice(item_bytes);
            item_heap_bytes = item_heap_bytes.saturating_add(item.capacity());
            counter_map.insert_unique(hash, (hash, idx), |&(h, _)| h);
            counters.push(Counter {
                item,
                hash,
                count,
                error,
            });
        }

        let mut alpha_map = AlphaMap::Empty;
        let mut alpha_max = 0;

        if alpha_encoding == ALPHA_SPARSE {
            let mut previous_slot = None;
            for _ in 0..alpha_nonzero {
                let slot = reader.read_u32("alpha slot")? as usize;
                let alpha = reader.read_u64("alpha value")?;
                if slot >= alpha_slots
                    || alpha == 0
                    || previous_slot.is_some_and(|previous| slot <= previous)
                {
                    return exec_err!(
                        "approx_top_k: corrupt intermediate state (invalid sparse alpha entry)"
                    );
                }
                alpha_max = max(alpha_max, alpha);
                alpha_map.add(slot, alpha, alpha_slots);
                previous_slot = Some(slot);
            }
        } else {
            let bitmap = reader.read_bytes(alpha_slots / 8, "alpha bitmap")?;
            let popcount: usize =
                bitmap.iter().map(|byte| byte.count_ones() as usize).sum();
            if popcount != alpha_nonzero {
                return exec_err!(
                    "approx_top_k: corrupt intermediate state (alpha bitmap count mismatch)"
                );
            }
            for slot in 0..alpha_slots {
                if bitmap[slot / 8] & (1 << (slot % 8)) != 0 {
                    let alpha = reader.read_u64("alpha value")?;
                    if alpha == 0 {
                        return exec_err!(
                            "approx_top_k: corrupt intermediate state (zero packed alpha value)"
                        );
                    }
                    alpha_max = max(alpha_max, alpha);
                    alpha_map.add(slot, alpha, alpha_slots);
                }
            }
        }
        reader.finish()?;

        let mut summary = Self {
            counters,
            counter_map,
            min_heap: Vec::new(),
            heap_positions: Vec::new(),
            alpha_map,
            alpha_max,
            alpha_nonzero,
            requested_capacity,
            item_heap_bytes,
        };
        summary.rebuild_min_heap();
        Ok(summary)
    }

    /// Approximate size in bytes of this summary.  O(1) thanks to
    /// incremental `item_heap_bytes` tracking.
    fn size(&self) -> usize {
        size_of::<Self>()
            + self.counters.capacity() * size_of::<Counter>()
            + self.min_heap.capacity() * size_of::<usize>()
            + self.heap_positions.capacity() * size_of::<usize>()
            + self.item_heap_bytes
            + self.counter_map.allocation_size()
            + self.alpha_map.allocation_size()
    }

    fn heap_size(&self) -> usize {
        self.size().saturating_sub(size_of::<Self>())
    }
}

// ---------------------------------------------------------------------------
// ApproxTopK  UDAF struct
// ---------------------------------------------------------------------------

/// Approximate top-k UDAF using the Filtered Space-Saving algorithm.
#[user_doc(
    doc_section(label = "Approximate Functions"),
    description = r#"Returns the approximate most frequent (top-k) values with their estimated counts as a list of `{value, count}` structs. Values are ranked by estimated count, with lower-error estimates preferred when counts tie.

Because the aggregate uses bounded memory, `count` is an upper-bound estimate. Within one summary it is exact for a value that was tracked for the whole scan; distributed merging may add an omission bound from partitions where that value was not tracked. Values whose frequency is far from the top-k boundary are the ones reported reliably; ties and near-ties may be resolved arbitrarily.

NULL values are skipped; an empty or all-NULL input returns an empty list `[]`. For float columns, -0.0 and +0.0 are treated as the same value, while different NaN representations are tracked separately."#,
    syntax_example = "approx_top_k(expression, k)",
    sql_example = r#"```sql
> SELECT approx_top_k(column_name, 3) FROM table_name;
+-----------------------------------------------------------------------------+
| approx_top_k(column_name,Int64(3))                                          |
+-----------------------------------------------------------------------------+
| [{value: foo, count: 3}, {value: bar, count: 2}, {value: baz, count: 1}]    |
+-----------------------------------------------------------------------------+
```"#,
    standard_argument(name = "expression",),
    argument(
        name = "k",
        description = "The number of top elements to return. Must be a literal integer between 1 and 10,000."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ApproxTopK {
    signature: Signature,
}

impl Default for ApproxTopK {
    fn default() -> Self {
        Self::new()
    }
}

impl ApproxTopK {
    pub fn new() -> Self {
        let with_integer_k = |value| {
            TypeSignature::Coercible(vec![
                Coercion::new_exact(value),
                Coercion::new_exact(TypeSignatureClass::Integer),
            ])
        };
        let variants = vec![
            with_integer_k(TypeSignatureClass::Native(logical_string())),
            with_integer_k(TypeSignatureClass::Native(logical_binary())),
            with_integer_k(TypeSignatureClass::Integer),
            with_integer_k(TypeSignatureClass::Native(logical_float32())),
            with_integer_k(TypeSignatureClass::Native(logical_float64())),
            with_integer_k(TypeSignatureClass::Native(logical_date())),
            with_integer_k(TypeSignatureClass::Timestamp),
        ];

        Self {
            signature: Signature::one_of(variants, Volatility::Immutable),
        }
    }
}

fn get_approx_top_k_k(args: &AccumulatorArgs) -> Result<usize> {
    if args.exprs.len() < 2 {
        return plan_err!("approx_top_k requires two arguments: column and k");
    }

    let k = args.exprs[1]
        .downcast_ref::<datafusion_physical_expr::expressions::Literal>()
        .and_then(|lit| match lit.value() {
            // Reject negatives before `as usize` can wrap them. Zero converts
            // losslessly and is handled by the range check below.
            ScalarValue::Int8(Some(v)) if *v >= 0 => Some(*v as usize),
            ScalarValue::Int16(Some(v)) if *v >= 0 => Some(*v as usize),
            ScalarValue::Int32(Some(v)) if *v >= 0 => Some(*v as usize),
            ScalarValue::Int64(Some(v)) if *v >= 0 => Some(*v as usize),
            ScalarValue::UInt8(Some(v)) => Some(*v as usize),
            ScalarValue::UInt16(Some(v)) => Some(*v as usize),
            ScalarValue::UInt32(Some(v)) => Some(*v as usize),
            ScalarValue::UInt64(Some(v)) => usize::try_from(*v).ok(),
            _ => None,
        });

    let Some(k) = k else {
        return plan_err!(
            "approx_top_k requires k to be a positive literal integer between 1 and 10000"
        );
    };
    if k == 0 || k > APPROX_TOP_K_MAX_K {
        return plan_err!(
            "approx_top_k requires k to be between 1 and {APPROX_TOP_K_MAX_K}, got {k}"
        );
    }
    Ok(k)
}

impl AggregateUDFImpl for ApproxTopK {
    fn name(&self) -> &str {
        "approx_top_k"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let value_type = arg_types.first().cloned().unwrap_or(DataType::Utf8);

        let struct_fields = Fields::from(vec![
            Field::new("value", value_type, true),
            Field::new("count", DataType::UInt64, false),
        ]);
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(struct_fields),
            true,
        ))))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "summary"),
            DataType::LargeBinary,
            false,
        ))])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            return not_impl_err!("approx_top_k does not support DISTINCT");
        }
        let k = get_approx_top_k_k(&args)?;
        let data_type = args.expr_fields[0].data_type().clone();
        Ok(Box::new(ApproxTopKAccumulator::new_with_data_type(
            k, data_type,
        )))
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if args.is_distinct {
            return not_impl_err!("approx_top_k does not support DISTINCT");
        }
        let k = get_approx_top_k_k(&args)?;
        let data_type = args.expr_fields[0].data_type().clone();
        Ok(Box::new(ApproxTopKGroupsAccumulator::new(k, data_type)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

// ---------------------------------------------------------------------------
// Accumulator
// ---------------------------------------------------------------------------

/// Accumulator for `approx_top_k` using the Filtered Space-Saving algorithm.
#[derive(Debug)]
struct ApproxTopKAccumulator {
    summary: SpaceSavingSummary,
    k: usize,
    /// The data type of the input column.
    input_data_type: DataType,
}

impl ApproxTopKAccumulator {
    fn new_with_data_type(k: usize, input_data_type: DataType) -> Self {
        let capacity = k * CAPACITY_MULTIPLIER;
        Self {
            summary: SpaceSavingSummary::new(capacity),
            k,
            input_data_type,
        }
    }

    /// Build the value array for the result based on the input data type.
    fn build_value_array(&self, top_items: &[(&[u8], u64, u64)]) -> Result<ArrayRef> {
        match &self.input_data_type {
            DataType::Null => Ok(Arc::new(NullArray::new(top_items.len()))),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let total_bytes = top_items.iter().try_fold(0usize, |total, item| {
                    total.checked_add(item.0.len()).ok_or_else(|| {
                        DataFusionError::Execution(
                            "approx_top_k output size overflow".to_string(),
                        )
                    })
                })?;
                if matches!(&self.input_data_type, DataType::Utf8)
                    && total_bytes > i32::MAX as usize
                {
                    return exec_err!("approx_top_k Utf8 output exceeds 2 GiB");
                }
                match &self.input_data_type {
                    DataType::Utf8 => {
                        let mut builder =
                            StringBuilder::with_capacity(top_items.len(), total_bytes);
                        for (bytes, _, _) in top_items {
                            builder.append_value(std::str::from_utf8(bytes).map_err(
                                |_| {
                                    DataFusionError::Execution(
                                        "approx_top_k state contains invalid UTF-8"
                                            .to_string(),
                                    )
                                },
                            )?);
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    DataType::LargeUtf8 => {
                        let mut builder = LargeStringBuilder::with_capacity(
                            top_items.len(),
                            total_bytes,
                        );
                        for (bytes, _, _) in top_items {
                            builder.append_value(std::str::from_utf8(bytes).map_err(
                                |_| {
                                    DataFusionError::Execution(
                                        "approx_top_k state contains invalid UTF-8"
                                            .to_string(),
                                    )
                                },
                            )?);
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    DataType::Utf8View => {
                        let mut builder =
                            StringViewBuilder::with_capacity(top_items.len());
                        for (bytes, _, _) in top_items {
                            builder.append_value(std::str::from_utf8(bytes).map_err(
                                |_| {
                                    DataFusionError::Execution(
                                        "approx_top_k state contains invalid UTF-8"
                                            .to_string(),
                                    )
                                },
                            )?);
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    _ => unreachable!(),
                }
            }
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                let total_bytes = top_items.iter().try_fold(0usize, |total, item| {
                    total.checked_add(item.0.len()).ok_or_else(|| {
                        DataFusionError::Execution(
                            "approx_top_k output size overflow".to_string(),
                        )
                    })
                })?;
                if matches!(&self.input_data_type, DataType::Binary)
                    && total_bytes > i32::MAX as usize
                {
                    return exec_err!("approx_top_k Binary output exceeds 2 GiB");
                }
                match &self.input_data_type {
                    DataType::Binary => {
                        let mut builder =
                            BinaryBuilder::with_capacity(top_items.len(), total_bytes);
                        for (bytes, _, _) in top_items {
                            builder.append_value(bytes);
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    DataType::LargeBinary => {
                        let mut builder = LargeBinaryBuilder::with_capacity(
                            top_items.len(),
                            total_bytes,
                        );
                        for (bytes, _, _) in top_items {
                            builder.append_value(bytes);
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    DataType::BinaryView => {
                        let mut builder =
                            BinaryViewBuilder::with_capacity(top_items.len());
                        for (bytes, _, _) in top_items {
                            builder.append_value(bytes);
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    _ => unreachable!(),
                }
            }
            DataType::Int8 => {
                let values: Vec<Option<i8>> = top_items
                    .iter()
                    .map(|(bytes, _, _)| {
                        <[u8; 1]>::try_from(*bytes).ok().map(i8::from_le_bytes)
                    })
                    .collect();
                Ok(Arc::new(Int8Array::from(values)) as ArrayRef)
            }
            DataType::Int16 => {
                let values: Vec<Option<i16>> = top_items
                    .iter()
                    .map(|(bytes, _, _)| {
                        <[u8; 2]>::try_from(*bytes).ok().map(i16::from_le_bytes)
                    })
                    .collect();
                Ok(Arc::new(Int16Array::from(values)) as ArrayRef)
            }
            DataType::Int32 => {
                let values: Vec<Option<i32>> = top_items
                    .iter()
                    .map(|(bytes, _, _)| {
                        <[u8; 4]>::try_from(*bytes).ok().map(i32::from_le_bytes)
                    })
                    .collect();
                Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
            }
            DataType::Int64 => {
                let values: Vec<Option<i64>> = top_items
                    .iter()
                    .map(|(bytes, _, _)| {
                        <[u8; 8]>::try_from(*bytes).ok().map(i64::from_le_bytes)
                    })
                    .collect();
                Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
            }
            DataType::UInt8 => {
                let values: Vec<Option<u8>> = top_items
                    .iter()
                    .map(|(bytes, _, _)| {
                        <[u8; 1]>::try_from(*bytes).ok().map(u8::from_le_bytes)
                    })
                    .collect();
                Ok(Arc::new(UInt8Array::from(values)) as ArrayRef)
            }
            DataType::UInt16 => {
                let values: Vec<Option<u16>> = top_items
                    .iter()
                    .map(|(bytes, _, _)| {
                        <[u8; 2]>::try_from(*bytes).ok().map(u16::from_le_bytes)
                    })
                    .collect();
                Ok(Arc::new(UInt16Array::from(values)) as ArrayRef)
            }
            DataType::UInt32 => {
                let values: Vec<Option<u32>> = top_items
                    .iter()
                    .map(|(bytes, _, _)| {
                        <[u8; 4]>::try_from(*bytes).ok().map(u32::from_le_bytes)
                    })
                    .collect();
                Ok(Arc::new(UInt32Array::from(values)) as ArrayRef)
            }
            DataType::UInt64 => {
                let values: Vec<Option<u64>> = top_items
                    .iter()
                    .map(|(bytes, _, _)| {
                        <[u8; 8]>::try_from(*bytes).ok().map(u64::from_le_bytes)
                    })
                    .collect();
                Ok(Arc::new(UInt64Array::from(values)) as ArrayRef)
            }
            DataType::Float32 => {
                let values: Vec<Option<f32>> = top_items
                    .iter()
                    .map(|(bytes, _, _)| {
                        <[u8; 4]>::try_from(*bytes).ok().map(f32::from_le_bytes)
                    })
                    .collect();
                Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
            }
            DataType::Float64 => {
                let values: Vec<Option<f64>> = top_items
                    .iter()
                    .map(|(bytes, _, _)| {
                        <[u8; 8]>::try_from(*bytes).ok().map(f64::from_le_bytes)
                    })
                    .collect();
                Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
            }
            DataType::Date32 => {
                let values: Vec<Option<i32>> = top_items
                    .iter()
                    .map(|(bytes, _, _)| {
                        <[u8; 4]>::try_from(*bytes).ok().map(i32::from_le_bytes)
                    })
                    .collect();
                Ok(Arc::new(Date32Array::from(values)) as ArrayRef)
            }
            DataType::Date64 | DataType::Timestamp(_, _) => {
                let values: Vec<Option<i64>> = top_items
                    .iter()
                    .map(|(bytes, _, _)| {
                        <[u8; 8]>::try_from(*bytes).ok().map(i64::from_le_bytes)
                    })
                    .collect();
                // Date64 and all Timestamp variants share i64 storage.
                match &self.input_data_type {
                    DataType::Date64 => {
                        Ok(Arc::new(Date64Array::from(values)) as ArrayRef)
                    }
                    DataType::Timestamp(unit, tz) => match unit {
                        TimeUnit::Second => {
                            let mut arr = TimestampSecondArray::from(values);
                            if let Some(tz) = tz {
                                arr = arr.with_timezone(tz.as_ref());
                            }
                            Ok(Arc::new(arr) as ArrayRef)
                        }
                        TimeUnit::Millisecond => {
                            let mut arr = TimestampMillisecondArray::from(values);
                            if let Some(tz) = tz {
                                arr = arr.with_timezone(tz.as_ref());
                            }
                            Ok(Arc::new(arr) as ArrayRef)
                        }
                        TimeUnit::Microsecond => {
                            let mut arr = TimestampMicrosecondArray::from(values);
                            if let Some(tz) = tz {
                                arr = arr.with_timezone(tz.as_ref());
                            }
                            Ok(Arc::new(arr) as ArrayRef)
                        }
                        TimeUnit::Nanosecond => {
                            let mut arr = TimestampNanosecondArray::from(values);
                            if let Some(tz) = tz {
                                arr = arr.with_timezone(tz.as_ref());
                            }
                            Ok(Arc::new(arr) as ArrayRef)
                        }
                    },
                    _ => unreachable!(),
                }
            }
            other => exec_err!("Unsupported data type for approx_top_k: {other}"),
        }
    }

    /// Get the output data type for the value field.
    fn output_value_data_type(&self) -> DataType {
        self.input_data_type.clone()
    }
}

/// Visit every non-null value as the canonical bytes used by the sketch.
fn for_each_encoded_value<F>(data_array: &ArrayRef, mut visit: F) -> Result<()>
where
    F: FnMut(usize, Option<&[u8]>) -> Result<()>,
{
    macro_rules! process_array {
        ($array_type:ty) => {{
            let Some(arr) = data_array.as_any().downcast_ref::<$array_type>() else {
                return exec_err!(
                    "approx_top_k: failed to downcast array to {}",
                    stringify!($array_type)
                );
            };
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    visit(i, None)?;
                } else {
                    visit(i, Some(&arr.value(i).to_le_bytes()))?;
                }
            }
        }};
    }

    macro_rules! process_bytes_array {
        ($array_type:ty) => {{
            let Some(arr) = data_array.as_any().downcast_ref::<$array_type>() else {
                return exec_err!(
                    "approx_top_k: failed to downcast array to {}",
                    stringify!($array_type)
                );
            };
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    visit(i, None)?;
                } else {
                    visit(i, Some(arr.value(i).as_ref()))?;
                }
            }
        }};
    }

    macro_rules! process_float_array {
        ($array_type:ty, $bits_type:ty) => {{
            let Some(arr) = data_array.as_any().downcast_ref::<$array_type>() else {
                return exec_err!(
                    "approx_top_k: failed to downcast array to {}",
                    stringify!($array_type)
                );
            };
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    visit(i, None)?;
                } else {
                    let bits: $bits_type = arr.value(i).to_bits();
                    let bits = if bits << 1 == 0 { 0 } else { bits };
                    visit(i, Some(&bits.to_le_bytes()))?;
                }
            }
        }};
    }

    match data_array.data_type() {
        DataType::Null => {
            for row in 0..data_array.len() {
                visit(row, None)?;
            }
        }
        DataType::Utf8 => process_bytes_array!(StringArray),
        DataType::LargeUtf8 => process_bytes_array!(LargeStringArray),
        DataType::Utf8View => process_bytes_array!(StringViewArray),
        DataType::Binary => process_bytes_array!(BinaryArray),
        DataType::LargeBinary => process_bytes_array!(LargeBinaryArray),
        DataType::BinaryView => process_bytes_array!(BinaryViewArray),
        DataType::Int8 => process_array!(Int8Array),
        DataType::Int16 => process_array!(Int16Array),
        DataType::Int32 => process_array!(Int32Array),
        DataType::Int64 => process_array!(Int64Array),
        DataType::UInt8 => process_array!(UInt8Array),
        DataType::UInt16 => process_array!(UInt16Array),
        DataType::UInt32 => process_array!(UInt32Array),
        DataType::UInt64 => process_array!(UInt64Array),
        DataType::Float32 => process_float_array!(Float32Array, u32),
        DataType::Float64 => process_float_array!(Float64Array, u64),
        DataType::Date32 => process_array!(Date32Array),
        DataType::Date64 => process_array!(Date64Array),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => process_array!(TimestampSecondArray),
            TimeUnit::Millisecond => process_array!(TimestampMillisecondArray),
            TimeUnit::Microsecond => process_array!(TimestampMicrosecondArray),
            TimeUnit::Nanosecond => process_array!(TimestampNanosecondArray),
        },
        other => return exec_err!("Unsupported data type for approx_top_k: {other}"),
    }
    Ok(())
}

impl Accumulator for ApproxTopKAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        for_each_encoded_value(&values[0], |_, bytes| {
            if let Some(bytes) = bytes {
                self.summary.add(bytes);
            }
            Ok(())
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() || states[0].is_empty() {
            return Ok(());
        }

        let Some(summary_array) = states[0].as_any().downcast_ref::<LargeBinaryArray>()
        else {
            return exec_err!("Expected LargeBinary array for approx_top_k state");
        };

        for i in 0..summary_array.len() {
            if summary_array.is_null(i) {
                continue;
            }
            let bytes = summary_array.value(i);
            if bytes.is_empty() {
                continue;
            }
            let other_summary = SpaceSavingSummary::from_bytes(
                bytes,
                self.k * CAPACITY_MULTIPLIER,
                &self.input_data_type,
            )?;
            self.summary.merge(other_summary)?;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let top_items = self.summary.top_k(self.k);

        let value_data_type = self.output_value_data_type();
        let struct_fields = Fields::from(vec![
            Field::new("value", value_data_type, true),
            Field::new("count", DataType::UInt64, false),
        ]);

        let value_array = self.build_value_array(&top_items)?;
        let counts: Vec<u64> = top_items.iter().map(|(_, count, _)| *count).collect();
        let count_array = Arc::new(UInt64Array::from(counts)) as ArrayRef;

        let struct_array =
            StructArray::new(struct_fields.clone(), vec![value_array, count_array], None);

        let list_field = Field::new("item", DataType::Struct(struct_fields), true);

        Ok(ScalarValue::List(Arc::new(ListArray::new(
            Arc::new(list_field),
            OffsetBuffer::from_lengths([top_items.len()]),
            Arc::new(struct_array),
            None,
        ))))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::LargeBinary(Some(
            self.summary.serialize(&self.input_data_type)?,
        ))])
    }

    fn size(&self) -> usize {
        size_of::<Self>() - size_of::<SpaceSavingSummary>() + self.summary.size()
    }
}

#[derive(Debug)]
enum GroupSummary {
    Empty,
    Singleton(Counter),
    Full(Box<SpaceSavingSummary>),
}

impl GroupSummary {
    fn heap_size(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Singleton(counter) => counter.item.capacity(),
            Self::Full(summary) => size_of::<SpaceSavingSummary>() + summary.heap_size(),
        }
    }

    fn add(&mut self, item: &[u8], capacity: usize) {
        if let Self::Full(summary) = self {
            summary.add(item);
            return;
        }
        let hash = SpaceSavingSummary::hash_item(item);
        match self {
            Self::Empty => {
                *self = Self::Singleton(Counter {
                    item: item.to_vec(),
                    hash,
                    count: 1,
                    error: 0,
                });
            }
            Self::Singleton(counter) if counter.hash == hash && counter.item == item => {
                counter.count = counter.count.saturating_add(1);
            }
            Self::Singleton(_) => {
                let Self::Singleton(counter) = std::mem::replace(self, Self::Empty)
                else {
                    unreachable!()
                };
                let mut summary = SpaceSavingSummary::new(capacity);
                summary.push_owned_counter(counter);
                summary.add(item);
                *self = Self::Full(Box::new(summary));
            }
            Self::Full(_) => unreachable!(),
        }
    }

    fn merge(&mut self, mut other: SpaceSavingSummary, capacity: usize) -> Result<()> {
        if other.is_empty() {
            return Ok(());
        }

        if other.alpha_nonzero == 0 && other.counters.len() == 1 {
            let counter = other.counters.pop().unwrap();
            match self {
                Self::Empty => *self = Self::Singleton(counter),
                Self::Singleton(existing)
                    if existing.hash == counter.hash && existing.item == counter.item =>
                {
                    existing.count = existing.count.saturating_add(counter.count);
                    existing.error = existing.error.saturating_add(counter.error);
                }
                Self::Singleton(_) => {
                    let Self::Singleton(existing) = std::mem::replace(self, Self::Empty)
                    else {
                        unreachable!()
                    };
                    let mut summary = SpaceSavingSummary::new(capacity);
                    summary.push_owned_counter(existing);
                    summary.push_owned_counter(counter);
                    *self = Self::Full(Box::new(summary));
                }
                Self::Full(summary) => {
                    let mut singleton = SpaceSavingSummary::new(capacity);
                    singleton.push_owned_counter(counter);
                    summary.merge(singleton)?;
                }
            }
            return Ok(());
        }

        match std::mem::replace(self, Self::Empty) {
            Self::Empty => *self = Self::Full(Box::new(other)),
            Self::Singleton(counter) => {
                let mut summary = SpaceSavingSummary::new(capacity);
                summary.push_owned_counter(counter);
                summary.merge(other)?;
                *self = Self::Full(Box::new(summary));
            }
            Self::Full(mut summary) => {
                summary.merge(other)?;
                *self = Self::Full(summary);
            }
        }
        Ok(())
    }

    fn top_k(&self, k: usize) -> Vec<(&[u8], u64, u64)> {
        match self {
            Self::Empty => vec![],
            Self::Singleton(counter) if k != 0 => {
                vec![(counter.item.as_slice(), counter.count, counter.error)]
            }
            Self::Singleton(_) => vec![],
            Self::Full(summary) => summary.top_k(k),
        }
    }

    fn serialize(
        &self,
        output: &mut Vec<u8>,
        capacity: usize,
        data_type: &DataType,
    ) -> Result<()> {
        match self {
            Self::Empty => output.clear(),
            Self::Singleton(counter) => serialize_single_counter_state(
                output,
                capacity,
                data_type,
                Some(&counter.item),
                counter.count,
                counter.error,
            )?,
            Self::Full(summary) => *output = summary.serialize(data_type)?,
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ApproxTopKGroupsAccumulator {
    summaries: Vec<GroupSummary>,
    allocated_bytes: usize,
    k: usize,
    input_data_type: DataType,
}

impl ApproxTopKGroupsAccumulator {
    fn new(k: usize, input_data_type: DataType) -> Self {
        Self {
            summaries: vec![],
            allocated_bytes: 0,
            k,
            input_data_type,
        }
    }

    fn capacity(&self) -> usize {
        self.k * CAPACITY_MULTIPLIER
    }

    fn ensure_groups(&mut self, total_num_groups: usize) {
        if total_num_groups > self.summaries.len() {
            self.summaries
                .resize_with(total_num_groups, || GroupSummary::Empty);
        }
    }

    fn update_allocated_bytes(&mut self, before: usize, after: usize) {
        if after >= before {
            self.allocated_bytes = self.allocated_bytes.saturating_add(after - before);
        } else {
            self.allocated_bytes = self.allocated_bytes.saturating_sub(before - after);
        }
    }

    fn take_groups(&mut self, emit_to: EmitTo) -> Vec<GroupSummary> {
        let groups = emit_to.take_needed(&mut self.summaries);
        let freed = groups.iter().map(GroupSummary::heap_size).sum();
        self.allocated_bytes = self.allocated_bytes.saturating_sub(freed);
        groups
    }

    fn consume_top_values(
        groups: Vec<GroupSummary>,
        k: usize,
        mut append: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<(Vec<usize>, Vec<u64>)> {
        let mut lengths = Vec::with_capacity(groups.len());
        let mut counts = Vec::new();
        let mut total_items = 0usize;
        for group in groups {
            let top = group.top_k(k);
            total_items = total_items.checked_add(top.len()).ok_or_else(|| {
                DataFusionError::Execution(
                    "approx_top_k result size overflow".to_string(),
                )
            })?;
            if total_items > i32::MAX as usize {
                return exec_err!("approx_top_k grouped result exceeds List capacity");
            }
            lengths.push(top.len());
            for (item, count, _) in top {
                append(item)?;
                counts.push(count);
            }
        }
        Ok((lengths, counts))
    }
}

impl GroupsAccumulator for ApproxTopKGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.ensure_groups(total_num_groups);
        let capacity = self.capacity();
        for_each_encoded_value(&values[0], |row, item| {
            let included =
                opt_filter.is_none_or(|filter| filter.is_valid(row) && filter.value(row));
            if included && let Some(item) = item {
                let group = group_indices[row];
                let before = self.summaries[group].heap_size();
                self.summaries[group].add(item, capacity);
                let after = self.summaries[group].heap_size();
                self.update_allocated_bytes(before, after);
            }
            Ok(())
        })
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.ensure_groups(total_num_groups);
        let capacity = self.capacity();
        let Some(states) = values[0].as_any().downcast_ref::<LargeBinaryArray>() else {
            return exec_err!("Expected LargeBinary array for approx_top_k state");
        };
        for (row, &group) in group_indices.iter().enumerate() {
            if states.is_null(row) {
                continue;
            }
            if states.value(row).is_empty() {
                continue;
            }
            let other = SpaceSavingSummary::from_bytes(
                states.value(row),
                self.capacity(),
                &self.input_data_type,
            )?;
            let before = self.summaries[group].heap_size();
            self.summaries[group].merge(other, capacity)?;
            let after = self.summaries[group].heap_size();
            self.update_allocated_bytes(before, after);
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let groups = self.take_groups(emit_to);
        let (value_array, lengths, counts) = match &self.input_data_type {
            DataType::Utf8 => {
                let mut builder = StringBuilder::new();
                let mut total_bytes = 0usize;
                let (lengths, counts) =
                    Self::consume_top_values(groups, self.k, |item| {
                        total_bytes =
                            total_bytes.checked_add(item.len()).ok_or_else(|| {
                                DataFusionError::Execution(
                                    "approx_top_k output size overflow".to_string(),
                                )
                            })?;
                        if total_bytes > i32::MAX as usize {
                            return exec_err!("approx_top_k Utf8 output exceeds 2 GiB");
                        }
                        builder.append_value(std::str::from_utf8(item).map_err(
                            |_| {
                                DataFusionError::Execution(
                                    "approx_top_k state contains invalid UTF-8"
                                        .to_string(),
                                )
                            },
                        )?);
                        Ok(())
                    })?;
                (Arc::new(builder.finish()) as ArrayRef, lengths, counts)
            }
            DataType::LargeUtf8 => {
                let mut builder = LargeStringBuilder::new();
                let mut total_bytes = 0usize;
                let (lengths, counts) =
                    Self::consume_top_values(groups, self.k, |item| {
                        total_bytes =
                            total_bytes.checked_add(item.len()).ok_or_else(|| {
                                DataFusionError::Execution(
                                    "approx_top_k output size overflow".to_string(),
                                )
                            })?;
                        if total_bytes > i64::MAX as usize {
                            return exec_err!(
                                "approx_top_k LargeUtf8 output exceeds 8 EiB"
                            );
                        }
                        builder.append_value(std::str::from_utf8(item).map_err(
                            |_| {
                                DataFusionError::Execution(
                                    "approx_top_k state contains invalid UTF-8"
                                        .to_string(),
                                )
                            },
                        )?);
                        Ok(())
                    })?;
                (Arc::new(builder.finish()) as ArrayRef, lengths, counts)
            }
            DataType::Utf8View => {
                let mut builder = StringViewBuilder::new();
                let (lengths, counts) =
                    Self::consume_top_values(groups, self.k, |item| {
                        builder.append_value(std::str::from_utf8(item).map_err(
                            |_| {
                                DataFusionError::Execution(
                                    "approx_top_k state contains invalid UTF-8"
                                        .to_string(),
                                )
                            },
                        )?);
                        Ok(())
                    })?;
                (Arc::new(builder.finish()) as ArrayRef, lengths, counts)
            }
            DataType::Binary => {
                let mut builder = BinaryBuilder::new();
                let mut total_bytes = 0usize;
                let (lengths, counts) =
                    Self::consume_top_values(groups, self.k, |item| {
                        total_bytes =
                            total_bytes.checked_add(item.len()).ok_or_else(|| {
                                DataFusionError::Execution(
                                    "approx_top_k output size overflow".to_string(),
                                )
                            })?;
                        if total_bytes > i32::MAX as usize {
                            return exec_err!("approx_top_k Binary output exceeds 2 GiB");
                        }
                        builder.append_value(item);
                        Ok(())
                    })?;
                (Arc::new(builder.finish()) as ArrayRef, lengths, counts)
            }
            DataType::LargeBinary => {
                let mut builder = LargeBinaryBuilder::new();
                let mut total_bytes = 0usize;
                let (lengths, counts) =
                    Self::consume_top_values(groups, self.k, |item| {
                        total_bytes =
                            total_bytes.checked_add(item.len()).ok_or_else(|| {
                                DataFusionError::Execution(
                                    "approx_top_k output size overflow".to_string(),
                                )
                            })?;
                        if total_bytes > i64::MAX as usize {
                            return exec_err!(
                                "approx_top_k LargeBinary output exceeds 8 EiB"
                            );
                        }
                        builder.append_value(item);
                        Ok(())
                    })?;
                (Arc::new(builder.finish()) as ArrayRef, lengths, counts)
            }
            DataType::BinaryView => {
                let mut builder = BinaryViewBuilder::new();
                let (lengths, counts) =
                    Self::consume_top_values(groups, self.k, |item| {
                        builder.append_value(item);
                        Ok(())
                    })?;
                (Arc::new(builder.finish()) as ArrayRef, lengths, counts)
            }
            DataType::Null => {
                let (lengths, counts) =
                    Self::consume_top_values(groups, self.k, |_| Ok(()))?;
                (Arc::new(NullArray::new(0)) as ArrayRef, lengths, counts)
            }
            _ => {
                let (_, item_width, _) = state_type_descriptor(&self.input_data_type)?;
                let item_width = item_width as usize;
                let mut item_bytes = Vec::new();
                let (lengths, counts) =
                    Self::consume_top_values(groups, self.k, |item| {
                        item_bytes.try_reserve(item.len()).map_err(|e| {
                            DataFusionError::ResourcesExhausted(format!(
                                "Unable to allocate approx_top_k grouped values: {e}"
                            ))
                        })?;
                        item_bytes.extend_from_slice(item);
                        Ok(())
                    })?;
                let top_items: Vec<_> = item_bytes
                    .chunks_exact(item_width)
                    .map(|item| (item, 0, 0))
                    .collect();
                let helper = ApproxTopKAccumulator::new_with_data_type(
                    self.k,
                    self.input_data_type.clone(),
                );
                (helper.build_value_array(&top_items)?, lengths, counts)
            }
        };
        let count_array = Arc::new(UInt64Array::from(counts)) as ArrayRef;
        let struct_fields = Fields::from(vec![
            Field::new("value", self.input_data_type.clone(), true),
            Field::new("count", DataType::UInt64, false),
        ]);
        let struct_array = StructArray::try_new(
            struct_fields.clone(),
            vec![value_array, count_array],
            None,
        )?;
        let list_field =
            Arc::new(Field::new("item", DataType::Struct(struct_fields), true));
        let list = ListArray::try_new(
            list_field,
            OffsetBuffer::from_lengths(lengths),
            Arc::new(struct_array),
            None,
        )?;
        Ok(Arc::new(list))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let groups = self.take_groups(emit_to);
        let mut builder = LargeBinaryBuilder::new();
        let mut scratch = Vec::new();
        for summary in groups {
            summary.serialize(&mut scratch, self.capacity(), &self.input_data_type)?;
            builder.append_value(&scratch);
        }
        Ok(vec![Arc::new(builder.finish())])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let mut builder = LargeBinaryBuilder::new();
        let mut scratch = Vec::new();
        for_each_encoded_value(&values[0], |row, item| {
            let included =
                opt_filter.is_none_or(|filter| filter.is_valid(row) && filter.value(row));
            if included && item.is_some() {
                serialize_single_counter_state(
                    &mut scratch,
                    self.capacity(),
                    &self.input_data_type,
                    item,
                    1,
                    0,
                )?;
            } else {
                scratch.clear();
            }
            builder.append_value(&scratch);
            Ok(())
        })?;
        Ok(vec![Arc::new(builder.finish())])
    }

    fn size(&self) -> usize {
        self.summaries.capacity() * size_of::<GroupSummary>() + self.allocated_bytes
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_space_saving_basic() {
        let mut summary = SpaceSavingSummary::new(3);

        summary.add(b"apple");
        summary.add(b"apple");
        summary.add(b"apple");
        summary.add(b"banana");
        summary.add(b"banana");
        summary.add(b"cherry");

        let (count, error) = summary.get(b"apple").unwrap();
        assert_eq!(count, 3);
        assert_eq!(error, 0);

        let (count, error) = summary.get(b"banana").unwrap();
        assert_eq!(count, 2);
        assert_eq!(error, 0);

        let (count, error) = summary.get(b"cherry").unwrap();
        assert_eq!(count, 1);
        assert_eq!(error, 0);

        let top = summary.top_k(3);
        assert_eq!(top.len(), 3);
        assert_eq!(top[0].0, b"apple");
        assert_eq!(top[0].1, 3);
        assert_eq!(top[1].0, b"banana");
        assert_eq!(top[1].1, 2);
        assert_eq!(top[2].0, b"cherry");
        assert_eq!(top[2].1, 1);
    }

    #[test]
    fn test_space_saving_eviction() {
        let mut summary = SpaceSavingSummary::new(2);

        for _ in 0..100 {
            summary.add(b"frequent");
        }

        // Distinct values exercise filtering, eviction, and key-buffer reuse.
        for i in 0..63u64 {
            let item = format!("rare_{i}");
            summary.add(item.as_bytes());
        }

        assert_eq!(summary.len(), 2);

        // The dominant item is retained with an exact count: it was never
        // evicted, so it carries no error.
        let (count, error) = summary.get(b"frequent").unwrap();
        assert_eq!(count, 100);
        assert_eq!(error, 0);

        // Exactly one rare candidate survives alongside it.
        let survivors: Vec<String> = (0..63u64)
            .map(|i| format!("rare_{i}"))
            .filter(|item| summary.get(item.as_bytes()).is_some())
            .collect();
        assert_eq!(survivors.len(), 1);

        // Eviction is what allocates the alpha map.
        assert!(!summary.alpha_map.is_empty());
    }

    /// The filter must reject weak candidates and admit one once its bucket
    /// estimate reaches the minimum tracked frequency.
    #[test]
    fn test_alpha_filter_controls_admission() {
        let mut summary = SpaceSavingSummary::new(2);
        for _ in 0..3 {
            summary.add(b"a");
            summary.add(b"b");
        }

        let candidate = b"candidate";
        let candidate_hash = SpaceSavingSummary::hash_item(candidate);
        summary.add(candidate);
        assert!(summary.get(candidate).is_none());
        assert_eq!(summary.alpha_for(candidate_hash), 1);

        summary.add(candidate);
        assert!(summary.get(candidate).is_none());
        assert_eq!(summary.alpha_for(candidate_hash), 2);

        summary.add(candidate);
        assert_eq!(summary.get(candidate), Some((3, 2)));
        assert_eq!(summary.len(), 2);
    }

    #[test]
    fn test_same_bucket_replacement_uses_evicted_estimate() {
        let mut summary = SpaceSavingSummary::new(1);
        for _ in 0..3 {
            summary.add(b"victim");
        }

        let victim_slot = summary.alpha_slot(SpaceSavingSummary::hash_item(b"victim"));
        let candidate = (0..1_000_000u64)
            .map(|i| format!("candidate_{i}"))
            .find(|candidate| {
                summary.alpha_slot(SpaceSavingSummary::hash_item(candidate.as_bytes()))
                    == victim_slot
            })
            .expect("should find an item in the victim's alpha bucket");

        summary.add(candidate.as_bytes());
        summary.add(candidate.as_bytes());
        assert!(summary.get(candidate.as_bytes()).is_none());

        summary.add(candidate.as_bytes());
        // The victim assignment changes alpha from 2 to 3 before insertion.
        assert_eq!(summary.get(candidate.as_bytes()), Some((4, 3)));
        assert_eq!(
            summary.alpha_for(SpaceSavingSummary::hash_item(b"victim")),
            3
        );
    }

    /// Once eviction has happened the reported counts are genuine upper-bound
    /// estimates rather than exact frequencies.
    #[test]
    fn test_counts_are_upper_bounds_after_eviction() {
        const TRUE_FREQUENCY: u64 = 3;
        let mut summary = SpaceSavingSummary::new(2);

        for i in 0..64u64 {
            for _ in 0..TRUE_FREQUENCY {
                summary.add(format!("filler_{i}").as_bytes());
            }
        }

        let top = summary.top_k(8);
        assert!(
            top.iter().any(|&(_, _, error)| error > 0),
            "expected at least one inflated estimate, got {top:?}"
        );

        for &(item, count, error) in &top {
            let name = String::from_utf8_lossy(item);
            assert!(
                count >= TRUE_FREQUENCY,
                "{name}: count {count} is below the true frequency"
            );
            assert!(
                count.saturating_sub(error) <= TRUE_FREQUENCY,
                "{name}: lower bound {} exceeds the true frequency",
                count - error
            );
        }

        // Counts must be non-increasing, which is what the documented ordering
        // promises.
        for pair in top.windows(2) {
            assert!(pair[0].1 >= pair[1].1, "counts are not descending: {top:?}");
        }
    }

    #[test]
    fn test_space_saving_serialization() {
        let mut summary = SpaceSavingSummary::new(3);
        summary.add(b"test");
        summary.add(b"test");
        summary.add(b"value");

        let bytes = summary.serialize(&DataType::Utf8).unwrap();
        let restored =
            SpaceSavingSummary::from_bytes(&bytes, 3, &DataType::Utf8).unwrap();

        assert_eq!(restored.capacity(), summary.capacity());
        assert_eq!(restored.len(), summary.len());

        let (count, _) = restored.get(b"test").unwrap();
        assert_eq!(count, 2);
        let (count, _) = restored.get(b"value").unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_rejects_malformed_serialized_states() {
        let mut summary = SpaceSavingSummary::new(3);
        summary.add(b"x");
        let valid = summary.serialize(&DataType::Utf8).unwrap();

        for end in 0..valid.len() {
            assert!(
                SpaceSavingSummary::from_bytes(&valid[..end], 3, &DataType::Utf8)
                    .is_err(),
                "accepted state truncated at byte {end}"
            );
        }

        let mut bad = valid.clone();
        bad[0] ^= 0xff;
        assert!(SpaceSavingSummary::from_bytes(&bad, 3, &DataType::Utf8).is_err());

        let mut bad = valid.clone();
        bad.extend_from_slice(&[0]);
        assert!(SpaceSavingSummary::from_bytes(&bad, 3, &DataType::Utf8).is_err());

        let mut bad_null = valid.clone();
        bad_null[6] = 0;
        assert!(SpaceSavingSummary::from_bytes(&bad_null, 3, &DataType::Null).is_err());

        let mut bad = valid.clone();
        bad[8..12].copy_from_slice(&4u32.to_le_bytes());
        assert!(SpaceSavingSummary::from_bytes(&bad, 3, &DataType::Utf8).is_err());
        assert!(SpaceSavingSummary::from_bytes(&valid, 3, &DataType::Binary).is_err());

        // Header (28), variable item length (8), one byte item, then count/error.
        let mut bad = valid.clone();
        bad[36] = 0xff;
        assert!(SpaceSavingSummary::from_bytes(&bad, 3, &DataType::Utf8).is_err());

        let mut bad = valid.clone();
        bad[45..53].copy_from_slice(&2u64.to_le_bytes());
        assert!(SpaceSavingSummary::from_bytes(&bad, 3, &DataType::Utf8).is_err());

        let mut duplicate = valid.clone();
        duplicate[12..16].copy_from_slice(&2u32.to_le_bytes());
        duplicate.extend_from_slice(&valid[28..]);
        assert!(SpaceSavingSummary::from_bytes(&duplicate, 3, &DataType::Utf8).is_err());

        let mut impossible = Vec::new();
        serialize_single_counter_state(&mut impossible, 3, &DataType::Utf8, None, 0, 0)
            .unwrap();
        impossible[16..20].copy_from_slice(&1u32.to_le_bytes());
        impossible.extend_from_slice(&0u32.to_le_bytes());
        impossible.extend_from_slice(&1u64.to_le_bytes());
        assert!(SpaceSavingSummary::from_bytes(&impossible, 3, &DataType::Utf8).is_err());
    }

    #[test]
    fn test_space_saving_merge() {
        let mut summary1 = SpaceSavingSummary::new(4);
        let mut summary2 = SpaceSavingSummary::new(4);

        summary1.add(b"apple");
        summary1.add(b"apple");
        summary2.add(b"apple");
        summary2.add(b"banana");

        summary1.merge(summary2).unwrap();

        let (count, _) = summary1.get(b"apple").unwrap();
        assert_eq!(count, 3);

        let (count, _) = summary1.get(b"banana").unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_merge_common_exact_counter_keeps_zero_error() {
        let mut left = SpaceSavingSummary::new(3);
        let mut right = SpaceSavingSummary::new(3);
        for _ in 0..100 {
            left.add(b"shared");
        }
        left.add(b"left_a");
        left.add(b"left_b");
        for _ in 0..99 {
            right.add(b"shared");
        }
        right.add(b"right_a");
        right.add(b"right_b");

        left.merge(right).unwrap();
        assert_eq!(left.get(b"shared"), Some((199, 0)));
    }

    #[test]
    fn test_merge_common_counter_saturates_without_losing_count() {
        let mut left = SpaceSavingSummary::new(1);
        left.push_owned_counter(Counter {
            item: b"shared".to_vec(),
            hash: SpaceSavingSummary::hash_item(b"shared"),
            count: u64::MAX - 5,
            error: 0,
        });

        let mut right = SpaceSavingSummary::new(1);
        right.push_owned_counter(Counter {
            item: b"shared".to_vec(),
            hash: SpaceSavingSummary::hash_item(b"shared"),
            count: 10,
            error: 0,
        });
        right.record_eviction(SpaceSavingSummary::hash_item(b"evicted"), 100);

        left.merge(right).unwrap();
        assert_eq!(left.get(b"shared"), Some((u64::MAX, 0)));
    }

    #[test]
    fn test_space_saving_merge_with_eviction() {
        let mut summary1 = SpaceSavingSummary::new(2);
        let mut summary2 = SpaceSavingSummary::new(2);

        for i in 0..40u64 {
            let item = format!("s1_item_{i}");
            summary1.add(item.as_bytes());
        }
        for _ in 0..10 {
            summary1.add(b"top_item");
        }

        for i in 0..40u64 {
            let item = format!("s2_item_{i}");
            summary2.add(item.as_bytes());
        }
        for _ in 0..5 {
            summary2.add(b"second_top");
        }

        summary1.merge(summary2).unwrap();

        let top = summary1.top_k(2);
        assert!(!top.is_empty());
        let top_item_result = top.iter().find(|(item, _, _)| *item == b"top_item");
        assert!(top_item_result.is_some());
    }

    /// An item can be frequent overall while being evicted from one partition
    /// because that partition happened to see mostly other traffic.  The merge
    /// must still report it, and must not report a lower bound above the true
    /// total.
    #[test]
    fn test_merge_recovers_item_evicted_from_one_partition() {
        let mut evicting = SpaceSavingSummary::new(2);
        evicting.add(b"shared");
        for i in 0..64u64 {
            for _ in 0..4 {
                evicting.add(format!("noise_{i}").as_bytes());
            }
        }
        assert!(
            evicting.get(b"shared").is_none(),
            "a single occurrence should lose to the count-4 noise"
        );

        let mut keeping = SpaceSavingSummary::new(2);
        for _ in 0..5 {
            keeping.add(b"shared");
        }

        keeping.merge(evicting).unwrap();

        let (count, error) = keeping
            .get(b"shared")
            .expect("merge must keep an item that survived in one partition");

        // True total across both partitions is 1 + 5 = 6.
        const TRUE_TOTAL: u64 = 6;
        assert!(
            count >= 5,
            "count {count} lost the 5 occurrences seen locally"
        );
        assert!(
            count.saturating_sub(error) <= TRUE_TOTAL,
            "lower bound {} exceeds the true total",
            count - error
        );
    }

    /// Recall check on a skewed stream: the heavy hitters must all be reported.
    ///
    /// Space-Saving guarantees that any item with frequency above
    /// `total / capacity` is retained.  Here `capacity` is `k * 3 = 15` and the
    /// stream has 6500 items, so the guarantee threshold is ~433 and every item
    /// in the true top 5 (frequencies 1000..600) clears it.
    #[test]
    fn test_skewed_stream_top_k_recall() {
        const HEAVY: usize = 10;
        const TAIL: usize = 1000;
        let k = 5usize;

        let mut stream: Vec<String> = Vec::new();
        for i in 0..HEAVY {
            let frequency = (HEAVY - i) * 100;
            for _ in 0..frequency {
                stream.push(format!("heavy_{i}"));
            }
        }
        for i in 0..TAIL {
            stream.push(format!("tail_{i:04}"));
        }

        // Interleave deterministically by striding with a prime step coprime to
        // the length, so the heavy hitters are spread through the stream rather
        // than arriving in convenient runs.
        let n = stream.len();
        let step = 7919;
        assert_eq!(n, 5500 + TAIL);

        let mut summary = SpaceSavingSummary::new(k * CAPACITY_MULTIPLIER);
        for j in 0..n {
            summary.add(stream[(j * step) % n].as_bytes());
        }

        let reported: Vec<String> = summary
            .top_k(k)
            .iter()
            .map(|(item, _, _)| String::from_utf8_lossy(item).into_owned())
            .collect();
        let expected: Vec<String> = (0..k).map(|i| format!("heavy_{i}")).collect();

        assert_eq!(reported, expected, "top-{k} on a skewed stream");
    }

    /// Helper to extract top-k results from a ScalarValue::List result.
    fn extract_top_k_results(result: &ScalarValue) -> Vec<(String, u64)> {
        if let ScalarValue::List(list_array) = result {
            let struct_array = list_array
                .values()
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Expected StructArray");

            let count_array = struct_array
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Expected UInt64Array for counts");

            (0..struct_array.len())
                .map(|i| {
                    let value = match struct_array.column(0).data_type() {
                        DataType::Utf8 => struct_array
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(i),
                        DataType::LargeUtf8 => struct_array
                            .column(0)
                            .as_any()
                            .downcast_ref::<LargeStringArray>()
                            .unwrap()
                            .value(i),
                        DataType::Utf8View => struct_array
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringViewArray>()
                            .unwrap()
                            .value(i),
                        other => panic!("Expected string values, got {other}"),
                    }
                    .to_string();
                    let count = count_array.value(i);
                    (value, count)
                })
                .collect()
        } else {
            panic!("Expected ScalarValue::List, got {result:?}");
        }
    }

    #[test]
    fn test_accumulator_update_and_evaluate() {
        let mut acc = ApproxTopKAccumulator::new_with_data_type(3, DataType::Utf8);

        let values: ArrayRef = Arc::new(StringArray::from(vec![
            "apple", "apple", "apple", "banana", "banana", "cherry",
        ]));

        acc.update_batch(&[values]).unwrap();

        let result = acc.evaluate().unwrap();
        let top_k = extract_top_k_results(&result);

        assert_eq!(top_k.len(), 3);
        assert_eq!(top_k[0], ("apple".to_string(), 3));
        assert_eq!(top_k[1], ("banana".to_string(), 2));
        assert_eq!(top_k[2], ("cherry".to_string(), 1));
    }

    #[test]
    fn test_accumulator_merge_batch() {
        let mut acc1 = ApproxTopKAccumulator::new_with_data_type(3, DataType::Utf8);
        let mut acc2 = ApproxTopKAccumulator::new_with_data_type(3, DataType::Utf8);

        let values1: ArrayRef =
            Arc::new(StringArray::from(vec!["apple", "apple", "banana"]));
        let values2: ArrayRef =
            Arc::new(StringArray::from(vec!["apple", "cherry", "cherry"]));

        acc1.update_batch(&[values1]).unwrap();
        acc2.update_batch(&[values2]).unwrap();

        let state2 = acc2.state().unwrap();

        let summary_bytes = if let ScalarValue::LargeBinary(Some(bytes)) = &state2[0] {
            bytes.clone()
        } else {
            panic!("Expected LargeBinary for summary")
        };

        let summary_array: ArrayRef =
            Arc::new(LargeBinaryArray::from(vec![Some(summary_bytes.as_slice())]));

        acc1.merge_batch(&[summary_array]).unwrap();

        let result = acc1.evaluate().unwrap();
        let top_k = extract_top_k_results(&result);

        assert!(!top_k.is_empty());
        assert_eq!(top_k[0].0, "apple");
        assert_eq!(top_k[0].1, 3);
    }

    #[test]
    fn test_distributed_merge_simulation() {
        let mut worker1_acc =
            ApproxTopKAccumulator::new_with_data_type(3, DataType::Utf8);
        let mut worker2_acc =
            ApproxTopKAccumulator::new_with_data_type(3, DataType::Utf8);
        let mut worker3_acc =
            ApproxTopKAccumulator::new_with_data_type(3, DataType::Utf8);

        let values1: ArrayRef =
            Arc::new(StringArray::from(vec!["apple", "apple", "apple", "banana"]));
        worker1_acc.update_batch(&[values1]).unwrap();

        let values2: ArrayRef = Arc::new(StringArray::from(vec![
            "apple", "apple", "cherry", "cherry",
        ]));
        worker2_acc.update_batch(&[values2]).unwrap();

        let values3: ArrayRef = Arc::new(StringArray::from(vec![
            "banana", "banana", "banana", "durian",
        ]));
        worker3_acc.update_batch(&[values3]).unwrap();

        let state1 = worker1_acc.state().unwrap();
        let state2 = worker2_acc.state().unwrap();
        let state3 = worker3_acc.state().unwrap();

        let summary_bytes: Vec<Option<&[u8]>> = vec![
            if let ScalarValue::LargeBinary(Some(ref b)) = state1[0] {
                Some(b.as_slice())
            } else {
                None
            },
            if let ScalarValue::LargeBinary(Some(ref b)) = state2[0] {
                Some(b.as_slice())
            } else {
                None
            },
            if let ScalarValue::LargeBinary(Some(ref b)) = state3[0] {
                Some(b.as_slice())
            } else {
                None
            },
        ];

        let summary_array: ArrayRef = Arc::new(LargeBinaryArray::from(summary_bytes));

        let mut coord_acc = ApproxTopKAccumulator::new_with_data_type(3, DataType::Utf8);
        coord_acc.merge_batch(&[summary_array]).unwrap();

        let result = coord_acc.evaluate().unwrap();
        let top_k = extract_top_k_results(&result);

        assert!(top_k.len() >= 2);
        assert_eq!(top_k[0], ("apple".to_string(), 5));
        assert_eq!(top_k[1], ("banana".to_string(), 4));
    }

    #[test]
    fn test_merge_orders_preserve_frequency_intervals() {
        use std::collections::HashMap;

        let capacity = 9;
        let mut partitions: Vec<_> =
            (0..4).map(|_| SpaceSavingSummary::new(capacity)).collect();
        let mut truth = HashMap::<String, u64>::new();
        let mut rng = 0x1234_5678_9abc_def0u64;
        for row in 0..10_000usize {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let item = if rng % 10 < 6 {
                "hot".to_string()
            } else {
                format!("tail_{}", rng % 100)
            };
            partitions[row % 4].add(item.as_bytes());
            *truth.entry(item).or_default() += 1;
        }

        let serialized: Vec<_> = partitions
            .iter_mut()
            .map(|summary| summary.serialize(&DataType::Utf8).unwrap())
            .collect();
        for order in [[0, 1, 2, 3], [3, 2, 1, 0], [1, 3, 0, 2]] {
            let mut merged = SpaceSavingSummary::new(capacity);
            for index in order {
                let other = SpaceSavingSummary::from_bytes(
                    &serialized[index],
                    capacity,
                    &DataType::Utf8,
                )
                .unwrap();
                merged.merge(other).unwrap();
            }
            let top = merged.top_k(3);
            assert_eq!(top[0].0, b"hot");
            for &(item, count, error) in &top {
                let item = std::str::from_utf8(item).unwrap();
                let true_count = truth[item];
                assert!(count >= true_count, "{item}: {count} < {true_count}");
                assert!(
                    count.saturating_sub(error) <= true_count,
                    "{item}: lower bound {} > {true_count}",
                    count - error
                );
            }
        }
    }

    #[test]
    fn test_accumulator_multiple_update_batches() {
        let mut acc = ApproxTopKAccumulator::new_with_data_type(2, DataType::Utf8);

        // First batch: a=2, b=1
        let batch1: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "a"]));
        acc.update_batch(&[batch1]).unwrap();

        // Second batch: b=2, c=1
        let batch2: ArrayRef = Arc::new(StringArray::from(vec!["b", "c", "b"]));
        acc.update_batch(&[batch2]).unwrap();

        // Combined: b=3, a=2, c=1 → top-2 should be b, a
        let result = acc.evaluate().unwrap();
        let top_k = extract_top_k_results(&result);
        assert_eq!(top_k.len(), 2);
        assert_eq!(top_k[0], ("b".to_string(), 3));
        assert_eq!(top_k[1], ("a".to_string(), 2));
    }

    #[test]
    fn test_accumulator_large_utf8_input() {
        let mut acc = ApproxTopKAccumulator::new_with_data_type(2, DataType::LargeUtf8);

        let batch: ArrayRef = Arc::new(LargeStringArray::from(vec![
            "hello", "world", "hello", "hello", "world",
        ]));
        acc.update_batch(&[batch]).unwrap();

        let result = acc.evaluate().unwrap();
        let top_k = extract_top_k_results(&result);
        assert_eq!(top_k.len(), 2);
        assert_eq!(top_k[0], ("hello".to_string(), 3));
        assert_eq!(top_k[1], ("world".to_string(), 2));
    }

    #[test]
    fn test_accumulator_view_inputs() {
        let mut acc = ApproxTopKAccumulator::new_with_data_type(2, DataType::Utf8View);
        let batch: ArrayRef = Arc::new(StringViewArray::from(vec![
            "hello", "world", "hello", "hello", "world",
        ]));
        acc.update_batch(&[batch]).unwrap();

        assert_eq!(acc.output_value_data_type(), DataType::Utf8View);
        let result = acc.evaluate().unwrap();
        let ScalarValue::List(list) = result else {
            panic!("expected list")
        };
        assert_eq!(
            list.values().data_type(),
            &DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Utf8View, true),
                Field::new("count", DataType::UInt64, false),
            ]))
        );

        let mut acc = ApproxTopKAccumulator::new_with_data_type(1, DataType::BinaryView);
        let batch: ArrayRef = Arc::new(BinaryViewArray::from(vec![
            b"a".as_slice(),
            b"b".as_slice(),
            b"a".as_slice(),
        ]));
        acc.update_batch(&[batch]).unwrap();
        assert_eq!(acc.output_value_data_type(), DataType::BinaryView);
        assert_eq!(acc.summary.top_k(1)[0], (b"a".as_slice(), 2, 0));
    }

    #[test]
    fn test_all_supported_physical_types_and_timestamp_metadata() {
        fn assert_round_trip(values: ArrayRef) {
            let data_type = values.data_type().clone();
            let mut partial =
                ApproxTopKAccumulator::new_with_data_type(2, data_type.clone());
            partial.update_batch(std::slice::from_ref(&values)).unwrap();
            let state = partial.state().unwrap();
            let state = state[0].to_array_of_size(1).unwrap();

            let mut final_acc =
                ApproxTopKAccumulator::new_with_data_type(2, data_type.clone());
            final_acc.merge_batch(&[state]).unwrap();
            let ScalarValue::List(result) = final_acc.evaluate().unwrap() else {
                panic!("expected List result for {data_type}");
            };
            let values = result
                .values()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert_eq!(values.column(0).data_type(), &data_type);
        }

        let binary = b"a".as_slice();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["a", "a"])),
            Arc::new(LargeStringArray::from(vec!["a", "a"])),
            Arc::new(StringViewArray::from(vec!["a", "a"])),
            Arc::new(BinaryArray::from(vec![binary, binary])),
            Arc::new(LargeBinaryArray::from(vec![binary, binary])),
            Arc::new(BinaryViewArray::from(vec![binary, binary])),
            Arc::new(Int8Array::from(vec![1, 1])),
            Arc::new(Int16Array::from(vec![1, 1])),
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(UInt8Array::from(vec![1, 1])),
            Arc::new(UInt16Array::from(vec![1, 1])),
            Arc::new(UInt32Array::from(vec![1, 1])),
            Arc::new(UInt64Array::from(vec![1, 1])),
            Arc::new(Float32Array::from(vec![1.0, 1.0])),
            Arc::new(Float64Array::from(vec![1.0, 1.0])),
            Arc::new(Date32Array::from(vec![1, 1])),
            Arc::new(Date64Array::from(vec![1, 1])),
            Arc::new(TimestampSecondArray::from(vec![1, 1])),
            Arc::new(TimestampMillisecondArray::from(vec![1, 1])),
            Arc::new(TimestampMicrosecondArray::from(vec![1, 1])),
            Arc::new(
                TimestampNanosecondArray::from(vec![1, 1])
                    .with_timezone("America/New_York"),
            ),
            Arc::new(NullArray::new(2)),
        ];
        for values in arrays {
            assert_round_trip(values);
        }
    }

    #[test]
    fn test_float_zero_and_nan_equality() {
        let mut left = ApproxTopKAccumulator::new_with_data_type(3, DataType::Float64);
        let mut right = ApproxTopKAccumulator::new_with_data_type(3, DataType::Float64);
        left.update_batch(&[Arc::new(Float64Array::from(vec![0.0, -0.0]))])
            .unwrap();
        right
            .update_batch(&[Arc::new(Float64Array::from(vec![-0.0]))])
            .unwrap();
        let state = right.state().unwrap();
        let ScalarValue::LargeBinary(Some(bytes)) = &state[0] else {
            panic!("expected LargeBinary state")
        };
        left.merge_batch(&[Arc::new(LargeBinaryArray::from(vec![Some(
            bytes.as_slice(),
        )]))])
        .unwrap();
        assert_eq!(
            left.summary.get(&0.0f64.to_bits().to_le_bytes()),
            Some((3, 0))
        );
        assert!(
            left.summary
                .get(&(-0.0f64).to_bits().to_le_bytes())
                .is_none()
        );

        let nan1 = f64::from_bits(0x7ff8_0000_0000_0001);
        let nan2 = f64::from_bits(0x7ff8_0000_0000_0002);
        let mut acc = ApproxTopKAccumulator::new_with_data_type(2, DataType::Float64);
        acc.update_batch(&[Arc::new(Float64Array::from(vec![nan1, nan1, nan2]))])
            .unwrap();
        assert_eq!(acc.summary.get(&nan1.to_bits().to_le_bytes()), Some((2, 0)));
        assert_eq!(acc.summary.get(&nan2.to_bits().to_le_bytes()), Some((1, 0)));
    }

    #[test]
    fn test_serialized_state_preserves_compact_alpha_map() {
        let mut summary = SpaceSavingSummary::new(3);
        for i in 0..8u64 {
            summary.add(format!("item_{i}").as_bytes());
        }
        assert!(!summary.alpha_map.is_empty());

        let bytes = summary.serialize(&DataType::Utf8).unwrap();
        let restored =
            SpaceSavingSummary::from_bytes(&bytes, 3, &DataType::Utf8).unwrap();
        assert_eq!(restored.len(), summary.len());
        assert_eq!(restored.alpha_map, summary.alpha_map);

        let dense_alpha_bytes = summary.alpha_slots() * size_of::<u64>();
        assert!(bytes.len() < STATE_HEADER_LEN + dense_alpha_bytes + 200);
    }

    /// Equal counts must not produce arbitrary output order, otherwise results
    /// vary run to run and SQL-level tests cannot assert on them.
    #[test]
    fn test_tied_counts_are_ordered_deterministically() {
        let mut summary = SpaceSavingSummary::new(8);
        for item in ["delta", "alpha", "charlie", "bravo"] {
            summary.add(item.as_bytes());
        }

        let reported: Vec<String> = summary
            .top_k(4)
            .iter()
            .map(|(item, _, _)| String::from_utf8_lossy(item).into_owned())
            .collect();
        assert_eq!(reported, vec!["alpha", "bravo", "charlie", "delta"]);
    }

    #[test]
    fn test_native_groups_accumulator_update_state_merge_and_filter() {
        fn values_for_group(array: &ArrayRef, group: usize) -> Vec<(String, u64)> {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = list.value(group);
            let values = values.as_any().downcast_ref::<StructArray>().unwrap();
            let strings = values
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let counts = values
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            (0..values.len())
                .map(|row| (strings.value(row).to_string(), counts.value(row)))
                .collect()
        }

        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "x", "a", "y", "b"]));
        let filter = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            None,
            Some(false),
        ]);
        let groups = [0, 1, 0, 1, 0];
        let mut partial = ApproxTopKGroupsAccumulator::new(2, DataType::Utf8);
        partial
            .update_batch(&[values], &groups, Some(&filter), 2)
            .unwrap();
        let allocated = partial.allocated_bytes;
        assert!(allocated > 0);

        let state = partial.state(EmitTo::All).unwrap();
        assert_eq!(partial.allocated_bytes, 0);
        assert!(state[0].as_any().is::<LargeBinaryArray>());

        let mut final_acc = ApproxTopKGroupsAccumulator::new(2, DataType::Utf8);
        final_acc.merge_batch(&state, &[0, 1], 2).unwrap();
        let result = final_acc.evaluate(EmitTo::All).unwrap();
        assert_eq!(values_for_group(&result, 0), vec![("a".to_string(), 2)]);
        assert_eq!(values_for_group(&result, 1), vec![("x".to_string(), 1)]);

        let raw: ArrayRef = Arc::new(StringArray::from(vec![Some("z"), None, Some("q")]));
        let filter = BooleanArray::from(vec![true, true, false]);
        let converter = ApproxTopKGroupsAccumulator::new(1, DataType::Utf8);
        let states = converter.convert_to_state(&[raw], Some(&filter)).unwrap();
        let compact_states = states[0]
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(compact_states.value(0).len(), SINGLETON_HEADER_LEN + 1);
        assert!(compact_states.value(1).is_empty());
        assert!(compact_states.value(2).is_empty());
        let mut merged = ApproxTopKGroupsAccumulator::new(1, DataType::Utf8);
        merged.merge_batch(&states, &[0, 1, 2], 3).unwrap();
        let result = merged.evaluate(EmitTo::All).unwrap();
        assert_eq!(values_for_group(&result, 0), vec![("z".to_string(), 1)]);
        assert!(values_for_group(&result, 1).is_empty());
        assert!(values_for_group(&result, 2).is_empty());

        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let mut emit = ApproxTopKGroupsAccumulator::new(1, DataType::Utf8);
        emit.update_batch(&[values], &[0, 1, 2], None, 3).unwrap();
        let initial_size = emit.size();

        let first_state = emit.state(EmitTo::First(1)).unwrap();
        assert_eq!(first_state[0].len(), 1);
        assert!(emit.size() < initial_size);
        let mut first = ApproxTopKGroupsAccumulator::new(1, DataType::Utf8);
        first.merge_batch(&first_state, &[0], 1).unwrap();
        let result = first.evaluate(EmitTo::All).unwrap();
        assert_eq!(values_for_group(&result, 0), vec![("a".to_string(), 1)]);

        let result = emit.evaluate(EmitTo::First(1)).unwrap();
        assert_eq!(values_for_group(&result, 0), vec![("b".to_string(), 1)]);
        let shifted: ArrayRef = Arc::new(StringArray::from(vec!["c"]));
        emit.update_batch(&[shifted], &[0], None, 1).unwrap();
        let result = emit.evaluate(EmitTo::All).unwrap();
        assert_eq!(values_for_group(&result, 0), vec![("c".to_string(), 2)]);
        assert_eq!(emit.allocated_bytes, 0);
    }
}
