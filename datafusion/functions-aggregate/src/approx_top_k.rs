// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
//! estimated count, then by error, then by the value's encoded bytes so that
//! ties are deterministic (see `Counter` for the exact ordering).
//!
//! `count` is an *upper bound* on the true frequency: each counter also tracks
//! an `error` such that the true frequency lies in `[count - error, count]`.
//! Values first seen while the summary still had free counters start with
//! `error == 0`; a value admitted later inherits its filter bucket's estimate
//! as `error`, and merging full summaries may add an omission bound.
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
    Array, ArrayRef, BinaryArray, BinaryViewArray, BinaryViewBuilder, BooleanArray,
    Date32Array, Date64Array, Decimal32Array, Decimal64Array, Decimal128Array,
    Decimal256Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float32Array,
    Float64Array, GenericBinaryBuilder, GenericStringBuilder, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeBinaryBuilder, LargeStringArray,
    ListArray, NullArray, OffsetSizeTrait, PrimitiveArray, StringArray, StringViewArray,
    StringViewBuilder, StructArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type,
    Decimal256Type, Field, FieldRef, Fields, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type, i256,
};
use hashbrown::{HashMap, HashTable};

use datafusion_common::hash_utils::QualityRandomState;
use datafusion_common::types::{
    logical_boolean, logical_date, logical_float32, logical_float64, logical_string,
};
use datafusion_common::{
    Result, ScalarValue, exec_datafusion_err, exec_err, internal_datafusion_err,
    internal_err, not_impl_err, plan_datafusion_err, plan_err, resources_datafusion_err,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::{AggregateOrderSensitivity, format_state_name};
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Coercion, Documentation, EmitTo, GroupsAccumulator,
    Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

use crate::utils::get_scalar_value;

make_udaf_expr_and_func!(
    ApproxTopK,
    approx_top_k,
    expression k,
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

/// Type tag, value width (0 for variable-length values) and type parameters
/// identifying the input type of an intermediate state.
type TypeDescriptor = (u8, u8, Vec<u8>);

/// Stable type descriptor embedded in every intermediate state.
fn state_type_descriptor(data_type: &DataType) -> Result<TypeDescriptor> {
    let descriptor = match data_type {
        DataType::Null => (0, 0, vec![]),
        DataType::Utf8 => (1, 0, vec![]),
        DataType::LargeUtf8 => (2, 0, vec![]),
        DataType::Utf8View => (3, 0, vec![]),
        DataType::Binary => (4, 0, vec![]),
        DataType::LargeBinary => (5, 0, vec![]),
        DataType::BinaryView => (6, 0, vec![]),
        DataType::Boolean => (7, 1, vec![]),
        // Stored as variable-length values because the size may not fit the
        // one-byte item width.
        DataType::FixedSizeBinary(size) => (8, 0, size.to_le_bytes().to_vec()),
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
        DataType::Time32(TimeUnit::Second) => (40, 4, vec![]),
        DataType::Time32(TimeUnit::Millisecond) => (41, 4, vec![]),
        DataType::Time64(TimeUnit::Microsecond) => (42, 8, vec![]),
        DataType::Time64(TimeUnit::Nanosecond) => (43, 8, vec![]),
        DataType::Decimal32(precision, scale) => (44, 4, vec![*precision, *scale as u8]),
        DataType::Decimal64(precision, scale) => (45, 8, vec![*precision, *scale as u8]),
        DataType::Decimal128(precision, scale) => {
            (46, 16, vec![*precision, *scale as u8])
        }
        DataType::Decimal256(precision, scale) => {
            (47, 32, vec![*precision, *scale as u8])
        }
        other => return exec_err!("Unsupported data type for approx_top_k: {other}"),
    };
    Ok(descriptor)
}

/// Validate one encoded value read from an intermediate state. `item_width` is
/// the width from [`state_type_descriptor`] (0 for variable-length values).
fn validate_item_bytes(data_type: &DataType, item_width: u8, item: &[u8]) -> Result<()> {
    if item_width != 0 && item.len() != item_width as usize {
        return exec_err!(
            "approx_top_k: corrupt intermediate state (expected {item_width}-byte value, got {})",
            item.len()
        );
    }
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            std::str::from_utf8(item).map_err(|_| {
                exec_datafusion_err!(
                    "approx_top_k: corrupt intermediate state (invalid UTF-8 value)"
                )
            })?;
        }
        DataType::Boolean if item[0] > 1 => {
            return exec_err!(
                "approx_top_k: corrupt intermediate state (invalid boolean value)"
            );
        }
        DataType::FixedSizeBinary(size) if item.len() != *size as usize => {
            return exec_err!(
                "approx_top_k: corrupt intermediate state (expected {size}-byte value, got {})",
                item.len()
            );
        }
        _ => {}
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
            exec_datafusion_err!(
                "approx_top_k: corrupt intermediate state ({field} length overflow)"
            )
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

/// Append the header of a `STATE_SINGLETON` state: a single value seen once,
/// whose encoded bytes follow the header.
fn extend_with_singleton_prefix(
    output: &mut Vec<u8>,
    capacity: usize,
    descriptor: &TypeDescriptor,
) {
    let (type_tag, item_width, type_params) = descriptor;
    output.extend_from_slice(STATE_MAGIC);
    output.push(STATE_VERSION);
    output.push(STATE_SINGLETON);
    output.push(*type_tag);
    output.push(*item_width);
    output.extend_from_slice(&(capacity as u32).to_le_bytes());
    output.extend_from_slice(&(type_params.len() as u32).to_le_bytes());
    output.extend_from_slice(type_params);
}

/// Recognizes the `STATE_SINGLETON` states that one accumulator accepts.
///
/// They all start with the same header, so merging compares that header
/// instead of decoding it, and adds the value without building a summary.
/// This is the per-row state produced when partial aggregation is skipped.
struct SingletonStates {
    /// `None` for `Null` input, which never has values.
    prefix: Option<Vec<u8>>,
    data_type: DataType,
    item_width: u8,
}

impl SingletonStates {
    fn new(capacity: usize, data_type: &DataType) -> Result<Self> {
        let descriptor = state_type_descriptor(data_type)?;
        let prefix = (*data_type != DataType::Null).then(|| {
            let mut prefix = Vec::new();
            extend_with_singleton_prefix(&mut prefix, capacity, &descriptor);
            prefix
        });
        Ok(Self {
            prefix,
            data_type: data_type.clone(),
            item_width: descriptor.1,
        })
    }

    /// The value held by `state`, or `None` if it must be decoded with
    /// [`SpaceSavingSummary::from_bytes`], which also rejects corrupt states.
    fn value<'b>(&self, state: &'b [u8]) -> Result<Option<&'b [u8]>> {
        let Some(item) = self
            .prefix
            .as_deref()
            .and_then(|prefix| state.strip_prefix(prefix))
        else {
            return Ok(None);
        };
        validate_item_bytes(&self.data_type, self.item_width, item)?;
        Ok(Some(item))
    }
}

/// Serialize a summary holding at most one counter into `output`.
///
/// A single value seen once (the common case for `convert_to_state` and for
/// high-cardinality `GROUP BY`) uses a compact header without counts; anything
/// else falls back to the general format.
fn serialize_single_counter_state(
    output: &mut Vec<u8>,
    capacity: usize,
    descriptor: &TypeDescriptor,
    item: Option<&[u8]>,
    count: u64,
    error: u64,
) -> Result<()> {
    let &(type_tag, item_width, ref type_params) = descriptor;
    if let Some(item) = item {
        if count == 0 || error > count {
            return internal_err!("approx_top_k: invalid singleton count/error interval");
        }
        if count == 1 && error == 0 {
            output.clear();
            output
                .try_reserve_exact(SINGLETON_HEADER_LEN + type_params.len() + item.len())
                .map_err(|e| {
                    resources_datafusion_err!(
                        "Unable to allocate approx_top_k state: {e}"
                    )
                })?;
            extend_with_singleton_prefix(output, capacity, descriptor);
            output.extend_from_slice(item);
            return Ok(());
        }
    }

    output.clear();
    let record_bytes =
        item.map_or(0, |item| item.len() + 16 + usize::from(item_width == 0) * 8);
    output
        .try_reserve_exact(STATE_HEADER_LEN + type_params.len() + record_bytes)
        .map_err(|e| {
            resources_datafusion_err!("Unable to allocate approx_top_k state: {e}")
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
    output.extend_from_slice(type_params);
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
#[derive(Debug, PartialEq, Eq)]
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

/// The Filtered Space-Saving filter: one `alpha` cell per hash bucket, holding
/// an upper bound on the frequency of any unmonitored item in that bucket.
///
/// The map starts empty (all cells read as zero) and stays sparse until enough
/// cells are populated for a dense `Vec` to use less memory, so summaries that
/// never filter or evict an item pay nothing for it.
#[derive(Debug, PartialEq, Eq)]
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

    /// Mutable access to the cell for `slot`, creating it if needed. `slots`
    /// is the total number of cells, used to pick the representation.
    fn cell_mut(&mut self, slot: usize, slots: usize) -> &mut u64 {
        if let Self::Sparse(values) = self
            && values.len() + 1 >= max(8, slots / 4)
            && !values.contains_key(&slot)
        {
            let mut dense = vec![0; slots];
            for (&slot, &value) in values.iter() {
                dense[slot] = value;
            }
            *self = Self::Dense(dense);
        }
        if self.is_empty() {
            *self = Self::Sparse(HashMap::new());
        }
        match self {
            Self::Empty => unreachable!("replaced above"),
            Self::Sparse(values) => values.entry(slot).or_default(),
            Self::Dense(values) => &mut values[slot],
        }
    }

    fn add(&mut self, slot: usize, increment: u64, slots: usize) -> u64 {
        if increment == 0 {
            return self.get(slot);
        }
        let cell = self.cell_mut(slot, slots);
        *cell = cell.saturating_add(increment);
        *cell
    }

    fn set(&mut self, slot: usize, value: u64, slots: usize) -> u64 {
        debug_assert_ne!(value, 0);
        *self.cell_mut(slot, slots) = value;
        value
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

/// Filtered Space-Saving algorithm summary for approximate top-k / heavy hitters.
///
/// Uses a [`HashTable`] that stores `(hash, index)` tuples for O(1) counter
/// lookups without duplicating the key bytes.  The actual item data lives in
/// `counters[index].item`. An [`AlphaMap`] estimates the frequencies of filtered
/// and evicted items.
///
/// All heap buffers grow on demand.  Nothing is pre-allocated from `k`, because
/// under `GROUP BY` there is one summary per group: eagerly sizing the alpha
/// map from `k` would cost
/// `next_power_of_two(k * CAPACITY_MULTIPLIER * ALPHA_MAP_ELEMENTS_PER_COUNTER) * 8`
/// bytes for every group, including singleton groups that never evict anything.
#[derive(Debug)]
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
    /// Number of non-zero cells in `alpha_map`, used to size the serialized
    /// alpha map without scanning it.
    alpha_nonzero: usize,
    /// Maximum number of counters (`k * CAPACITY_MULTIPLIER`).
    requested_capacity: usize,
    /// Running total of heap bytes owned by counter item `Vec`s.
    /// Updated on push / evict / merge so that `size()` is O(1).
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
        debug_assert!(
            capacity > 0,
            "approx_top_k summary capacity must be positive"
        );
        Self {
            counters: Vec::new(),
            counter_map: HashTable::new(),
            min_heap: Vec::new(),
            heap_positions: Vec::new(),
            alpha_map: AlphaMap::Empty,
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

    fn add_alpha(&mut self, hash: u64, increment: u64) {
        let slots = self.alpha_slots();
        let slot = self.alpha_slot(hash);
        if increment != 0 && self.alpha_map.get(slot) == 0 {
            self.alpha_nonzero += 1;
        }
        self.alpha_map.add(slot, increment, slots);
    }

    /// Raise the filter bucket of `hash` to at least `count`, the estimate of a
    /// value that stops being tracked (evicted, or dropped by a merge).
    ///
    /// Filtered Space-Saving assigns the evicted count. Within one stream a
    /// bucket never exceeds the minimum tracked count, so taking the maximum is
    /// the same; after merges a bucket can exceed it, and the maximum keeps the
    /// bucket an upper bound for every untracked value in it.
    fn raise_alpha(&mut self, hash: u64, count: u64) {
        let slots = self.alpha_slots();
        let slot = self.alpha_slot(hash);
        let previous = self.alpha_map.get(slot);
        if count > previous {
            if previous == 0 {
                self.alpha_nonzero += 1;
            }
            self.alpha_map.set(slot, count, slots);
        }
    }

    fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }

    /// Whether every count is exact. A summary that never filled up has not
    /// filtered or evicted anything, so its alpha map is all zero
    /// (`from_bytes` rejects states that violate this).
    fn is_exact(&self) -> bool {
        self.counters.len() < self.requested_capacity
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
        self.add_weighted(item, Self::hash_item(item), 1, 0);
    }

    /// Filtered Space-Saving update for `count` occurrences of `item` whose
    /// estimate already carries `error`. [`Self::add`] is the unit case.
    #[inline]
    fn add_weighted(&mut self, item: &[u8], hash: u64, count: u64, error: u64) {
        // Fast path: item already tracked.
        if let Some(idx) = self.find_counter_idx(item, hash) {
            let counter = &mut self.counters[idx];
            counter.count = counter.count.saturating_add(count);
            counter.error = counter.error.saturating_add(error);
            self.repair_heap(idx);
            return;
        }

        // Below capacity: add directly.
        if self.counters.len() < self.requested_capacity {
            self.push_counter(item, hash, count, error);
            return;
        }

        // Filter an unmonitored item until its bucket estimate reaches the
        // minimum tracked estimate. This is the defining admission rule of the
        // original Filtered Space-Saving algorithm.
        let alpha = self.alpha_for(hash);
        let min_idx = self.min_heap[0];
        let min_count = self.counters[min_idx].count;
        if alpha.saturating_add(count) < min_count {
            self.add_alpha(hash, count);
            return;
        }

        let victim_hash = self.counters[min_idx].hash;
        self.raise_alpha(victim_hash, min_count);
        // Read alpha after recording the victim. When both values hash to the
        // same bucket, this ordering is required by the original algorithm.
        let admission_alpha = self.alpha_for(hash);
        self.replace_min_counter(
            item,
            hash,
            admission_alpha.saturating_add(count),
            admission_alpha.saturating_add(error),
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
            self.raise_alpha(hash, count);
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

    /// Merge another summary.
    ///
    /// An exact summary is just a multiset of observations, so it is folded
    /// into the other one with the weighted stream update. Merging the
    /// single-value states produced by `convert_to_state` therefore gives the
    /// same result as accumulating the rows directly.
    ///
    /// Two full summaries are merged with the Parallel Space-Saving
    /// reduce-and-combine algorithm from <https://arxiv.org/pdf/1401.0702.pdf>,
    /// using each summary's Filtered Space-Saving alphas as omission bounds.
    fn merge(&mut self, mut other: SpaceSavingSummary) -> Result<()> {
        if other.is_empty() {
            return Ok(());
        }

        if self.is_empty() {
            *self = other;
            return Ok(());
        }

        if self.is_exact() && !other.is_exact() {
            std::mem::swap(self, &mut other);
        }
        if other.is_exact() {
            // Best first, so that if this summary fills up, the values that
            // keep exact counts are the most frequent ones.
            other.counters.sort_unstable();
            for counter in &other.counters {
                self.add_weighted(
                    &counter.item,
                    counter.hash,
                    counter.count,
                    counter.error,
                );
            }
            return Ok(());
        }

        // Both summaries are full. A value that one summary does not track
        // occurred in its input at most as often as the alpha of the value's
        // bucket there, so that alpha is added as the omission bound. Common
        // values combine directly.
        for counter in &mut self.counters {
            let (count, error) = match other.find_counter_idx(&counter.item, counter.hash)
            {
                Some(idx) => (other.counters[idx].count, other.counters[idx].error),
                None => {
                    let bound = other.alpha_for(counter.hash);
                    (bound, bound)
                }
            };
            counter.count = counter.count.saturating_add(count);
            counter.error = counter.error.saturating_add(error);
        }

        // Add the values only `other` tracks, bounded by this summary's alphas
        // (read before the alpha maps are combined below).
        let unmatched = other
            .counters
            .iter()
            .filter(|counter| {
                self.find_counter_idx(&counter.item, counter.hash).is_none()
            })
            .count();
        self.counters.try_reserve(unmatched).map_err(|e| {
            resources_datafusion_err!(
                "Unable to grow approx_top_k counters during merge: {e}"
            )
        })?;
        self.counter_map
            .try_reserve(unmatched, |&(hash, _)| hash)
            .map_err(|e| {
                resources_datafusion_err!(
                    "Unable to grow approx_top_k counter map during merge: {e}"
                )
            })?;
        for mut other_counter in other.counters {
            if self
                .find_counter_idx(&other_counter.item, other_counter.hash)
                .is_none()
            {
                let bound = self.alpha_for(other_counter.hash);
                other_counter.count = other_counter.count.saturating_add(bound);
                other_counter.error = other_counter.error.saturating_add(bound);
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
                self.alpha_map.add(slot, other_alpha, alpha_slots);
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
        // Both alpha sizes are bounded by the alpha map size for the maximum
        // capacity (a few MiB), so they cannot overflow.
        let alpha_slots = Self::compute_alpha_map_size(self.requested_capacity);
        let alpha_nonzero = self.alpha_nonzero;
        let bitmap_bytes = alpha_slots / 8;
        let sparse_bytes = alpha_nonzero * 12;
        let packed_bitmap_bytes = bitmap_bytes + alpha_nonzero * 8;
        let (alpha_encoding, alpha_bytes) = if packed_bitmap_bytes < sparse_bytes {
            (ALPHA_BITMAP, packed_bitmap_bytes)
        } else {
            (ALPHA_SPARSE, sparse_bytes)
        };

        let variable_items = item_width == 0;
        let record_overhead = 16 + usize::from(variable_items) * 8;
        let total = STATE_HEADER_LEN
            + type_params.len()
            + alpha_bytes
            + self
                .counters
                .iter()
                .map(|c| c.item.len() + record_overhead)
                .sum::<usize>();

        let mut bytes = Vec::new();
        bytes.try_reserve_exact(total).map_err(|e| {
            resources_datafusion_err!("Unable to allocate approx_top_k state: {e}")
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

        // Streaming updates never exceed capacity and merges reduce back to it,
        // so every counter is written. Order does not matter to `from_bytes`.
        debug_assert!(self.counters.len() <= self.requested_capacity);
        for counter in &self.counters {
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
        let (expected_tag, expected_width, expected_params) =
            state_type_descriptor(expected_data_type)?;
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
            validate_item_bytes(expected_data_type, expected_width, item)?;
            reader.finish()?;

            let mut summary = Self::new(requested_capacity);
            summary.push_counter(item, Self::hash_item(item), 1, 0);
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

        // `num_counters` was checked against the payload size above and every
        // item is read from the payload, so allocations are bounded by `bytes`.
        let mut counters: Vec<Counter> = Vec::with_capacity(num_counters);
        let mut counter_map: HashTable<(u64, usize)> =
            HashTable::with_capacity(num_counters);
        let mut item_heap_bytes = 0usize;

        for idx in 0..num_counters {
            let item_len = if item_width == 0 {
                usize::try_from(reader.read_u64("item length")?).map_err(|_| {
                    exec_datafusion_err!(
                        "approx_top_k: corrupt intermediate state (item length overflow)"
                    )
                })?
            } else {
                item_width as usize
            };
            let item_bytes = reader.read_bytes(item_len, "item")?;
            validate_item_bytes(expected_data_type, item_width, item_bytes)?;
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
            let item = item_bytes.to_vec();
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

Because the aggregate uses bounded memory, `count` is an upper-bound estimate. It is exact for values first seen while the summary still had room for them. A value first seen later starts from an estimate of how often it may already have occurred, and distributed merging may add an omission bound from partitions where that value was not tracked. Values whose frequency is far from the top-k boundary are the ones reported reliably; ties and near-ties may be resolved arbitrarily.

NULL values are skipped; an empty or all-NULL input returns an empty list `[]`. For float columns, -0.0 and +0.0 are treated as the same value, while different NaN representations are tracked separately.

Supported input types are strings, binary (including fixed-size binary), booleans, integers, 32- and 64-bit floats, decimals, dates, times, and timestamps. `value` has the same type as `expression`, except that dictionary-encoded inputs are returned as their value type."#,
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
            with_integer_k(TypeSignatureClass::Binary),
            with_integer_k(TypeSignatureClass::Native(logical_boolean())),
            with_integer_k(TypeSignatureClass::Integer),
            with_integer_k(TypeSignatureClass::Native(logical_float32())),
            with_integer_k(TypeSignatureClass::Native(logical_float64())),
            with_integer_k(TypeSignatureClass::Decimal),
            with_integer_k(TypeSignatureClass::Native(logical_date())),
            with_integer_k(TypeSignatureClass::Time),
            with_integer_k(TypeSignatureClass::Timestamp),
        ];

        Self {
            signature: Signature::one_of(variants, Volatility::Immutable),
        }
    }
}

/// Evaluate and validate the constant `k` argument.
fn get_approx_top_k_k(args: &AccumulatorArgs) -> Result<usize> {
    let [_, k_expr] = args.exprs else {
        return internal_err!(
            "approx_top_k expects 2 arguments, got {}",
            args.exprs.len()
        );
    };
    let k_value = get_scalar_value(k_expr).map_err(|_| {
        plan_datafusion_err!(
            "approx_top_k requires k to be a literal integer, got {k_expr}"
        )
    })?;
    // Widen to i128 so every signed and unsigned integer type shares the range
    // check below.
    let k = match k_value {
        ScalarValue::Int8(Some(v)) => i128::from(v),
        ScalarValue::Int16(Some(v)) => i128::from(v),
        ScalarValue::Int32(Some(v)) => i128::from(v),
        ScalarValue::Int64(Some(v)) => i128::from(v),
        ScalarValue::UInt8(Some(v)) => i128::from(v),
        ScalarValue::UInt16(Some(v)) => i128::from(v),
        ScalarValue::UInt32(Some(v)) => i128::from(v),
        ScalarValue::UInt64(Some(v)) => i128::from(v),
        other => {
            return plan_err!(
                "approx_top_k requires k to be a literal integer, got {other}"
            );
        }
    };
    match usize::try_from(k) {
        Ok(k) if (1..=APPROX_TOP_K_MAX_K).contains(&k) => Ok(k),
        _ => plan_err!(
            "approx_top_k requires k to be between 1 and {APPROX_TOP_K_MAX_K}, got {k}"
        ),
    }
}

/// Fields of each `{value, count}` struct in the result list.
fn result_struct_fields(value_type: DataType) -> Fields {
    Fields::from(vec![
        Field::new("value", value_type, true),
        Field::new("count", DataType::UInt64, false),
    ])
}

/// Field of the result `List`.
fn result_list_field(value_type: DataType) -> FieldRef {
    Arc::new(Field::new_list_field(
        DataType::Struct(result_struct_fields(value_type)),
        true,
    ))
}

impl AggregateUDFImpl for ApproxTopK {
    fn name(&self) -> &str {
        "approx_top_k"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let Some(value_type) = arg_types.first() else {
            return internal_err!("approx_top_k expects 2 arguments, got none");
        };
        // The signature matches on logical types, so it also accepts physical
        // encodings (such as run-end encoding) that the sketch cannot store.
        state_type_descriptor(value_type).map_err(|_| {
            plan_datafusion_err!("approx_top_k does not support input type {value_type}")
        })?;
        Ok(DataType::List(result_list_field(value_type.clone())))
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

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }

    /// An empty list, matching what [`Accumulator::evaluate`] returns for an
    /// empty input. Used for empty window frames and decorrelated subqueries.
    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        let DataType::List(field) = data_type else {
            return internal_err!("approx_top_k must return a List, got {data_type}");
        };
        let DataType::Struct(fields) = field.data_type() else {
            return internal_err!(
                "approx_top_k must return a List of Structs, got {data_type}"
            );
        };
        let Some(value_field) = fields.first() else {
            return internal_err!("approx_top_k result struct has no value field");
        };
        let list = build_result_list(value_field.data_type(), &[], vec![], vec![0])?;
        Ok(ScalarValue::List(Arc::new(list)))
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
}

fn utf8_value(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes).map_err(|_| {
        internal_datafusion_err!("approx_top_k state contains invalid UTF-8")
    })
}

/// Total length of `items`, checked against the offset capacity of `O`.
fn total_value_bytes<O: OffsetSizeTrait>(
    data_type: &DataType,
    items: &[&[u8]],
) -> Result<usize> {
    items
        .iter()
        .try_fold(0usize, |total, item| total.checked_add(item.len()))
        .filter(|&total| O::from_usize(total).is_some())
        .ok_or_else(|| {
            exec_datafusion_err!(
                "approx_top_k {data_type} output exceeds the maximum array size"
            )
        })
}

/// Decode sketch values back into an Arrow array of `data_type`.
///
/// `items` must hold encodings produced by [`for_each_encoded_value`] for the
/// same type (or validated by [`SpaceSavingSummary::from_bytes`]).
fn build_value_array(data_type: &DataType, items: &[&[u8]]) -> Result<ArrayRef> {
    macro_rules! primitive {
        ($arrow_type:ty, $native:ty) => {{
            const WIDTH: usize = size_of::<$native>();
            let values = items
                .iter()
                .map(|bytes| {
                    <[u8; WIDTH]>::try_from(*bytes)
                        .map(<$native>::from_le_bytes)
                        .map_err(|_| {
                            internal_datafusion_err!(
                                "approx_top_k: expected {WIDTH}-byte {data_type} value, got {} bytes",
                                bytes.len()
                            )
                        })
                })
                .collect::<Result<Vec<$native>>>()?;
            Arc::new(
                PrimitiveArray::<$arrow_type>::new(values.into(), None)
                    .with_data_type(data_type.clone()),
            ) as ArrayRef
        }};
    }

    fn strings<O: OffsetSizeTrait>(
        data_type: &DataType,
        items: &[&[u8]],
    ) -> Result<ArrayRef> {
        let total = total_value_bytes::<O>(data_type, items)?;
        let mut builder = GenericStringBuilder::<O>::with_capacity(items.len(), total);
        for bytes in items {
            builder.append_value(utf8_value(bytes)?);
        }
        Ok(Arc::new(builder.finish()))
    }

    fn binaries<O: OffsetSizeTrait>(
        data_type: &DataType,
        items: &[&[u8]],
    ) -> Result<ArrayRef> {
        let total = total_value_bytes::<O>(data_type, items)?;
        let mut builder = GenericBinaryBuilder::<O>::with_capacity(items.len(), total);
        for bytes in items {
            builder.append_value(bytes);
        }
        Ok(Arc::new(builder.finish()))
    }

    let array = match data_type {
        DataType::Null => Arc::new(NullArray::new(items.len())) as ArrayRef,
        DataType::Boolean => Arc::new(
            items
                .iter()
                .map(|bytes| match bytes {
                    [0] => Ok(Some(false)),
                    [1] => Ok(Some(true)),
                    _ => internal_err!("approx_top_k: invalid encoded boolean value"),
                })
                .collect::<Result<BooleanArray>>()?,
        ),
        DataType::Utf8 => strings::<i32>(data_type, items)?,
        DataType::LargeUtf8 => strings::<i64>(data_type, items)?,
        DataType::Utf8View => {
            let mut builder = StringViewBuilder::with_capacity(items.len());
            for bytes in items {
                builder.append_value(utf8_value(bytes)?);
            }
            Arc::new(builder.finish())
        }
        DataType::Binary => binaries::<i32>(data_type, items)?,
        DataType::LargeBinary => binaries::<i64>(data_type, items)?,
        DataType::BinaryView => {
            let mut builder = BinaryViewBuilder::with_capacity(items.len());
            for bytes in items {
                builder.append_value(bytes);
            }
            Arc::new(builder.finish())
        }
        DataType::FixedSizeBinary(size) => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(items.len(), *size);
            for bytes in items {
                builder.append_value(bytes)?;
            }
            Arc::new(builder.finish())
        }
        DataType::Int8 => primitive!(Int8Type, i8),
        DataType::Int16 => primitive!(Int16Type, i16),
        DataType::Int32 => primitive!(Int32Type, i32),
        DataType::Int64 => primitive!(Int64Type, i64),
        DataType::UInt8 => primitive!(UInt8Type, u8),
        DataType::UInt16 => primitive!(UInt16Type, u16),
        DataType::UInt32 => primitive!(UInt32Type, u32),
        DataType::UInt64 => primitive!(UInt64Type, u64),
        DataType::Float32 => primitive!(Float32Type, f32),
        DataType::Float64 => primitive!(Float64Type, f64),
        DataType::Decimal32(_, _) => primitive!(Decimal32Type, i32),
        DataType::Decimal64(_, _) => primitive!(Decimal64Type, i64),
        DataType::Decimal128(_, _) => primitive!(Decimal128Type, i128),
        DataType::Decimal256(_, _) => primitive!(Decimal256Type, i256),
        DataType::Date32 => primitive!(Date32Type, i32),
        DataType::Date64 => primitive!(Date64Type, i64),
        DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondType, i32),
        DataType::Time32(TimeUnit::Millisecond) => {
            primitive!(Time32MillisecondType, i32)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            primitive!(Time64MicrosecondType, i64)
        }
        DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondType, i64),
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondType, i64),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            primitive!(TimestampMillisecondType, i64)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            primitive!(TimestampMicrosecondType, i64)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive!(TimestampNanosecondType, i64)
        }
        other => return internal_err!("Unsupported data type for approx_top_k: {other}"),
    };
    Ok(array)
}

/// Build the `List<Struct{value, count}>` result with one list per entry in
/// `lengths`, taking values and counts in order from `items` and `counts`.
fn build_result_list(
    data_type: &DataType,
    items: &[&[u8]],
    counts: Vec<u64>,
    lengths: Vec<usize>,
) -> Result<ListArray> {
    if items.len() > i32::MAX as usize {
        return exec_err!("approx_top_k result exceeds List capacity");
    }
    let values = StructArray::try_new(
        result_struct_fields(data_type.clone()),
        vec![
            build_value_array(data_type, items)?,
            Arc::new(UInt64Array::from(counts)),
        ],
        None,
    )?;
    Ok(ListArray::try_new(
        result_list_field(data_type.clone()),
        OffsetBuffer::from_lengths(lengths),
        Arc::new(values),
        None,
    )?)
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
        DataType::Boolean => {
            let Some(arr) = data_array.as_any().downcast_ref::<BooleanArray>() else {
                return exec_err!(
                    "approx_top_k: failed to downcast array to BooleanArray"
                );
            };
            for (i, value) in arr.iter().enumerate() {
                match value {
                    Some(value) => visit(i, Some(&[u8::from(value)]))?,
                    None => visit(i, None)?,
                }
            }
        }
        DataType::Utf8 => process_bytes_array!(StringArray),
        DataType::LargeUtf8 => process_bytes_array!(LargeStringArray),
        DataType::Utf8View => process_bytes_array!(StringViewArray),
        DataType::Binary => process_bytes_array!(BinaryArray),
        DataType::LargeBinary => process_bytes_array!(LargeBinaryArray),
        DataType::BinaryView => process_bytes_array!(BinaryViewArray),
        DataType::FixedSizeBinary(_) => process_bytes_array!(FixedSizeBinaryArray),
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
        DataType::Decimal32(_, _) => process_array!(Decimal32Array),
        DataType::Decimal64(_, _) => process_array!(Decimal64Array),
        DataType::Decimal128(_, _) => process_array!(Decimal128Array),
        DataType::Decimal256(_, _) => process_array!(Decimal256Array),
        DataType::Date32 => process_array!(Date32Array),
        DataType::Date64 => process_array!(Date64Array),
        DataType::Time32(TimeUnit::Second) => process_array!(Time32SecondArray),
        DataType::Time32(TimeUnit::Millisecond) => {
            process_array!(Time32MillisecondArray)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            process_array!(Time64MicrosecondArray)
        }
        DataType::Time64(TimeUnit::Nanosecond) => process_array!(Time64NanosecondArray),
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

        let capacity = self.k * CAPACITY_MULTIPLIER;
        let singletons = SingletonStates::new(capacity, &self.input_data_type)?;
        for i in 0..summary_array.len() {
            if summary_array.is_null(i) {
                continue;
            }
            let bytes = summary_array.value(i);
            if bytes.is_empty() {
                continue;
            }
            if let Some(item) = singletons.value(bytes)? {
                self.summary.add(item);
            } else {
                self.summary.merge(SpaceSavingSummary::from_bytes(
                    bytes,
                    capacity,
                    &self.input_data_type,
                )?)?;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let (items, counts): (Vec<_>, Vec<_>) = self
            .summary
            .top_k(self.k)
            .into_iter()
            .map(|(item, count, _)| (item, count))
            .unzip();
        let lengths = vec![items.len()];
        let list = build_result_list(&self.input_data_type, &items, counts, lengths)?;
        Ok(ScalarValue::List(Arc::new(list)))
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
                Self::Full(summary) => summary.add_weighted(
                    &counter.item,
                    counter.hash,
                    counter.count,
                    counter.error,
                ),
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
        descriptor: &TypeDescriptor,
    ) -> Result<()> {
        match self {
            Self::Empty => output.clear(),
            Self::Singleton(counter) => serialize_single_counter_state(
                output,
                capacity,
                descriptor,
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
        let singletons = SingletonStates::new(capacity, &self.input_data_type)?;
        for (row, &group) in group_indices.iter().enumerate() {
            if states.is_null(row) {
                continue;
            }
            let bytes = states.value(row);
            if bytes.is_empty() {
                continue;
            }
            let before = self.summaries[group].heap_size();
            if let Some(item) = singletons.value(bytes)? {
                self.summaries[group].add(item, capacity);
            } else {
                let other = SpaceSavingSummary::from_bytes(
                    bytes,
                    capacity,
                    &self.input_data_type,
                )?;
                self.summaries[group].merge(other, capacity)?;
            }
            let after = self.summaries[group].heap_size();
            self.update_allocated_bytes(before, after);
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let groups = self.take_groups(emit_to);
        let mut lengths = Vec::with_capacity(groups.len());
        let mut items = Vec::new();
        let mut counts = Vec::new();
        for group in &groups {
            let top = group.top_k(self.k);
            lengths.push(top.len());
            for (item, count, _) in top {
                items.push(item);
                counts.push(count);
            }
        }
        let list = build_result_list(&self.input_data_type, &items, counts, lengths)?;
        Ok(Arc::new(list))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let groups = self.take_groups(emit_to);
        let descriptor = state_type_descriptor(&self.input_data_type)?;
        let mut builder = LargeBinaryBuilder::new();
        let mut scratch = Vec::new();
        for summary in groups {
            summary.serialize(
                &mut scratch,
                self.capacity(),
                &self.input_data_type,
                &descriptor,
            )?;
            builder.append_value(&scratch);
        }
        Ok(vec![Arc::new(builder.finish())])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let descriptor = state_type_descriptor(&self.input_data_type)?;
        let mut builder = LargeBinaryBuilder::new();
        let mut scratch = Vec::new();
        for_each_encoded_value(&values[0], |row, item| {
            let included =
                opt_filter.is_none_or(|filter| filter.is_valid(row) && filter.value(row));
            if included && item.is_some() {
                serialize_single_counter_state(
                    &mut scratch,
                    self.capacity(),
                    &descriptor,
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
        serialize_single_counter_state(
            &mut impossible,
            3,
            &state_type_descriptor(&DataType::Utf8).unwrap(),
            None,
            0,
            0,
        )
        .unwrap();
        impossible[16..20].copy_from_slice(&1u32.to_le_bytes());
        impossible.extend_from_slice(&0u32.to_le_bytes());
        impossible.extend_from_slice(&1u64.to_le_bytes());
        assert!(SpaceSavingSummary::from_bytes(&impossible, 3, &DataType::Utf8).is_err());
    }

    /// `merge_batch` recognizes single-value states by their header, so it
    /// must still reject corrupt ones.
    #[test]
    fn test_merge_batch_rejects_corrupt_singleton_states() {
        let state = |data_type: &DataType, item: &[u8]| {
            let mut bytes = Vec::new();
            let descriptor = state_type_descriptor(data_type).unwrap();
            serialize_single_counter_state(&mut bytes, 3, &descriptor, Some(item), 1, 0)
                .unwrap();
            bytes
        };
        let valid = state(&DataType::Utf8, b"x");
        let mut other_capacity = valid.clone();
        other_capacity[8..12].copy_from_slice(&6u32.to_le_bytes());

        let cases = [
            (DataType::Utf8, valid.clone(), true),
            (DataType::Utf8, state(&DataType::Utf8, b"\xff"), false),
            (DataType::Boolean, state(&DataType::Boolean, &[2]), false),
            (DataType::Int32, state(&DataType::Int32, &[1, 2]), false),
            (DataType::Binary, valid.clone(), false),
            (DataType::Null, state(&DataType::Null, b""), false),
            (DataType::Utf8, other_capacity, false),
            (DataType::Utf8, valid[..10].to_vec(), false),
        ];
        for (data_type, bytes, valid) in cases {
            let states: ArrayRef =
                Arc::new(LargeBinaryArray::from_vec(vec![bytes.as_slice()]));
            let mut acc = ApproxTopKAccumulator::new_with_data_type(1, data_type.clone());
            assert_eq!(
                acc.merge_batch(std::slice::from_ref(&states)).is_ok(),
                valid,
                "{data_type} state {bytes:?}"
            );
            let mut groups = ApproxTopKGroupsAccumulator::new(1, data_type.clone());
            assert_eq!(
                groups.merge_batch(&[states], &[0], 1).is_ok(),
                valid,
                "{data_type} state {bytes:?} (grouped)"
            );
        }
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
        right.raise_alpha(SpaceSavingSummary::hash_item(b"evicted"), 100);

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
    /// must still report it with an upper bound of its total count, and must
    /// not report a lower bound above the true total.
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

        // More occurrences than the 257 values the other partition saw, so no
        // estimate there can outrank it. This partition is filled too, so the
        // summaries are combined with Parallel Space-Saving rather than by
        // folding in an exact summary.
        let mut keeping = SpaceSavingSummary::new(2);
        for _ in 0..300 {
            keeping.add(b"shared");
        }
        keeping.add(b"keeping_other");
        assert!(!keeping.is_exact());

        keeping.merge(evicting).unwrap();

        let (count, error) = keeping
            .get(b"shared")
            .expect("merge must keep an item that survived in one partition");

        // True total across both partitions is 1 + 300 = 301.
        const TRUE_TOTAL: u64 = 301;
        assert!(
            count >= TRUE_TOTAL,
            "count {count} is not an upper bound of the true total"
        );
        assert!(
            count.saturating_sub(error) <= TRUE_TOTAL,
            "lower bound {} exceeds the true total",
            count - error
        );
    }

    /// Merging the single-value states produced by `convert_to_state` gives
    /// the same summary as accumulating the rows directly, including the
    /// filter state once values start being evicted.
    #[test]
    fn test_merging_singletons_matches_stream_updates() {
        let mut streamed = SpaceSavingSummary::new(6);
        let mut merged = SpaceSavingSummary::new(6);
        for i in 0..2_000u64 {
            let item = format!("item_{}", (i * i) % 97);
            streamed.add(item.as_bytes());
            let mut singleton = SpaceSavingSummary::new(6);
            singleton.add(item.as_bytes());
            merged.merge(singleton).unwrap();
        }

        assert!(streamed.alpha_nonzero > 0, "the stream must evict values");
        assert_eq!(merged.top_k(6), streamed.top_k(6));
        assert_eq!(
            merged.alpha_map.sorted_entries(),
            streamed.alpha_map.sorted_entries()
        );
    }

    /// An exact summary is folded into a full one regardless of which side of
    /// the merge it is on, using the filter bucket of each folded value.
    #[test]
    fn test_merge_folds_exact_summary_on_either_side() {
        let full = || {
            let mut summary = SpaceSavingSummary::new(3);
            for i in 0..50u64 {
                summary.add(format!("noise_{}", i % 7).as_bytes());
            }
            summary
        };
        let exact = || {
            let mut summary = SpaceSavingSummary::new(3);
            for _ in 0..100 {
                summary.add(b"heavy");
            }
            summary.add(b"light");
            summary
        };
        assert!(!full().is_exact());
        assert!(exact().is_exact());

        let mut left = full();
        left.merge(exact()).unwrap();
        let mut right = exact();
        right.merge(full()).unwrap();

        assert_eq!(left.top_k(3), right.top_k(3));
        assert_eq!(
            left.alpha_map.sorted_entries(),
            right.alpha_map.sorted_entries()
        );
        let (item, count, _) = left.top_k(1)[0];
        assert_eq!(item, b"heavy");
        assert!(count >= 100, "count {count} lost exact occurrences");
    }

    /// A value missing from one full summary is bounded by that summary's
    /// alpha for the value's bucket, not by its largest alpha anywhere.
    #[test]
    fn test_merge_bounds_missing_values_by_their_bucket() {
        let full = |items: [&[u8]; 3]| {
            let mut summary = SpaceSavingSummary::new(3);
            for item in items {
                for _ in 0..10 {
                    summary.add(item);
                }
            }
            summary
        };
        let mut left = full([b"a", b"b", b"c"]);
        let mut right = full([b"x", b"y", b"z"]);

        // A large estimate in a bucket that none of the left values use.
        let left_slots: Vec<_> = left
            .counters
            .iter()
            .map(|counter| right.alpha_slot(counter.hash))
            .collect();
        let hot = (0u64..1_000)
            .map(|i| SpaceSavingSummary::hash_item(format!("hot_{i}").as_bytes()))
            .find(|&hash| !left_slots.contains(&right.alpha_slot(hash)))
            .expect("most buckets are unused");
        right.raise_alpha(hot, 1_000);

        left.merge(right).unwrap();
        for item in [b"a", b"b", b"c"] {
            assert_eq!(left.get(item), Some((10, 0)));
        }
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

        let result = acc.evaluate().unwrap();
        let ScalarValue::List(list) = result else {
            panic!("expected list")
        };
        assert_eq!(
            list.values().data_type(),
            &DataType::Struct(result_struct_fields(DataType::Utf8View))
        );

        let mut acc = ApproxTopKAccumulator::new_with_data_type(1, DataType::BinaryView);
        let batch: ArrayRef = Arc::new(BinaryViewArray::from(vec![
            b"a".as_slice(),
            b"b".as_slice(),
            b"a".as_slice(),
        ]));
        acc.update_batch(&[batch]).unwrap();
        assert_eq!(acc.summary.top_k(1)[0], (b"a".as_slice(), 2, 0));
        let ScalarValue::List(list) = acc.evaluate().unwrap() else {
            panic!("expected list")
        };
        assert_eq!(
            list.values().data_type(),
            &DataType::Struct(result_struct_fields(DataType::BinaryView))
        );
    }

    #[test]
    fn test_all_supported_physical_types_and_timestamp_metadata() {
        fn assert_round_trip(input: ArrayRef) {
            let data_type = input.data_type().clone();
            let mut partial =
                ApproxTopKAccumulator::new_with_data_type(2, data_type.clone());
            partial.update_batch(std::slice::from_ref(&input)).unwrap();
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
            if data_type != DataType::Null {
                assert_eq!(values.len(), 1, "{data_type}");
                assert_eq!(values.column(0).as_ref(), &input.slice(0, 1), "{data_type}");
            }
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
            Arc::new(BooleanArray::from(vec![true, true])),
            Arc::new(Time32SecondArray::from(vec![1, 1])),
            Arc::new(Time32MillisecondArray::from(vec![1, 1])),
            Arc::new(Time64MicrosecondArray::from(vec![1, 1])),
            Arc::new(Time64NanosecondArray::from(vec![1, 1])),
            Arc::new(
                Decimal32Array::from(vec![123, 123])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal64Array::from(vec![123, 123])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![123, 123])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![i256::from(-123), i256::from(-123)])
                    .with_precision_and_scale(76, 3)
                    .unwrap(),
            ),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([[7u8; 16], [7u8; 16]].into_iter())
                    .unwrap(),
            ),
            // Wider than the one-byte item width of the state format.
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([[7u8; 300], [7u8; 300]].into_iter())
                    .unwrap(),
            ),
        ];
        for input in arrays {
            assert_round_trip(input);
        }
    }

    #[test]
    fn test_fixed_size_binary_state_rejects_other_sizes() {
        let mut partial =
            ApproxTopKAccumulator::new_with_data_type(1, DataType::FixedSizeBinary(2));
        let values: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_iter([[1u8, 2]].into_iter()).unwrap(),
        );
        partial.update_batch(&[values]).unwrap();
        let state = partial.state().unwrap()[0].to_array_of_size(1).unwrap();

        let mut other_size =
            ApproxTopKAccumulator::new_with_data_type(1, DataType::FixedSizeBinary(3));
        assert!(other_size.merge_batch(&[state]).is_err());
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
