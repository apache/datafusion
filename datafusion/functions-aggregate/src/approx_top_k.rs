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
//! of counters plus an alpha map (filter) that remembers evicted items' frequencies.
//!
//! Usage: `approx_top_k(column, k)`
//! - `column`: The column to find the most frequent values from
//! - `k`: The number of top elements to track (required, literal integer)
//!
//! Returns: `List<Struct { value: <input_type>, count: UInt64 }>` ordered by count descending.
//!
//! Algorithm references:
//! - Filtered Space-Saving: <http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf>
//! - Parallel Space Saving: <https://arxiv.org/pdf/1401.0702.pdf>
//! - Space-Saving: Metwally, Agrawal, El Abbadi. "Efficient Computation of Frequent
//!   and Top-k Elements in Data Streams" (ICDT 2005)

use std::cmp::{Ordering, max, min};
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    ListArray, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit};
use hashbrown::HashTable;

use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, TIMEZONE_WILDCARD,
    TypeSignature, Volatility,
};
use datafusion_macros::user_doc;

make_udaf_expr_and_func!(
    ApproxTopK,
    approx_top_k,
    "Returns the approximate most frequent (top-k) values and their counts using the Filtered Space-Saving algorithm.",
    approx_top_k_udaf
);

// ---------------------------------------------------------------------------
// Algorithm constants
// ---------------------------------------------------------------------------

/// Suggested constant from the paper "Finding top-k elements in data streams",
/// chap 6, equation (24). Determines the size of the alpha map relative to the capacity.
const ALPHA_MAP_ELEMENTS_PER_COUNTER: usize = 6;

/// Limit the max alpha value to avoid overflow with merges or weighted additions.
const MAX_ALPHA_VALUE: u64 = u32::MAX as u64;

/// Maximum allowed value for k in `approx_top_k(column, k)`.
const APPROX_TOP_K_MAX_K: usize = 10_000;

/// Capacity multiplier for internal tracking (matches ClickHouse's default).
///
/// We track more items internally than k to improve accuracy.
/// If user asks for top-5, we internally track top `5 * 3 = 15` items.
/// Memory impact: ~100 bytes per counter; target_capacity is 2x the tracked
/// counter count, so top-100 uses ~60 KB per accumulator.
const CAPACITY_MULTIPLIER: usize = 3;

// ---------------------------------------------------------------------------
// SpaceSavingSummary  (core algorithm)
// ---------------------------------------------------------------------------

/// Counter entry in the Filtered Space-Saving summary.
///
/// Each entry tracks an item, its estimated count, and the error bound.
/// The algorithm guarantees that the true count lies within `[count - error, count]`.
#[derive(Debug, Clone)]
struct Counter {
    /// The serialized bytes representing the tracked item.
    item: Vec<u8>,
    /// FNV-1a hash of the item (cached to avoid recomputation).
    hash: u64,
    /// The estimated frequency count (may overestimate due to eviction handling).
    count: u64,
    /// The maximum possible overestimation (error bound).
    error: u64,
}

impl Counter {
    /// Compare counters for sorting: higher `(count - error)` wins,
    /// then higher `count` breaks ties.
    fn is_greater_than(&self, other: &Counter) -> bool {
        let self_lb = self.count.saturating_sub(self.error);
        let other_lb = other.count.saturating_sub(other.error);
        (self_lb > other_lb) || (self_lb == other_lb && self.count > other.count)
    }

    /// Ordering for top-k selection: highest-ranked counters sort first.
    fn cmp_by_rank(&self, other: &Counter) -> Ordering {
        if other.is_greater_than(self) {
            Ordering::Greater
        } else if self.is_greater_than(other) {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }
}

/// Filtered Space-Saving algorithm summary for approximate top-k / heavy hitters.
///
/// Uses a [`HashTable`] that stores `(hash, index)` tuples for O(1) counter
/// lookups without duplicating the key bytes.  The actual item data lives in
/// `counters[index].item`.  An alpha map (filter) remembers evicted items'
/// frequencies.
#[derive(Debug, Clone)]
struct SpaceSavingSummary {
    counters: Vec<Counter>,
    /// Maps `(cached_hash, counter_index)`.  Lookups use the cached hash for
    /// the fast path and fall back to byte equality via the `counters` vec.
    counter_map: HashTable<(u64, usize)>,
    alpha_map: Vec<u64>,
    requested_capacity: usize,
    /// Internal target capacity to avoid frequent truncations.
    /// Set to `max(64, requested_capacity * 2)`.
    target_capacity: usize,
    /// Running total of heap bytes owned by counter item `Vec`s.
    /// Updated on push / evict / clone so that `size()` is O(1).
    item_heap_bytes: usize,
}

impl SpaceSavingSummary {
    fn compute_alpha_map_size(capacity: usize) -> usize {
        (capacity * ALPHA_MAP_ELEMENTS_PER_COUNTER).next_power_of_two()
    }

    /// FNV-1a hash for item bytes.
    fn hash_item(item: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for &byte in item {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    fn new(capacity: usize) -> Self {
        Self {
            counters: Vec::new(),
            counter_map: HashTable::new(),
            alpha_map: Vec::new(),
            requested_capacity: 0,
            target_capacity: 0,
            item_heap_bytes: 0,
        }
        .resized(capacity)
    }

    fn resized(mut self, new_capacity: usize) -> Self {
        if self.requested_capacity != new_capacity {
            debug_assert!(self.counters.is_empty());
            let alpha_map_size = Self::compute_alpha_map_size(new_capacity);
            self.alpha_map = vec![0u64; alpha_map_size];
            self.requested_capacity = new_capacity;
            self.target_capacity = max(64, new_capacity.saturating_mul(2));
            self.counters.reserve(self.target_capacity);
        }
        self
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

    /// Add an item with increment 1.
    fn add(&mut self, item: &[u8]) {
        self.insert(item, 1, 0);
    }

    /// Core insertion algorithm from Filtered Space-Saving.
    fn insert(&mut self, item: &[u8], increment: u64, error: u64) {
        let hash = Self::hash_item(item);

        // Fast path: item already tracked.
        if let Some(idx) = self.find_counter_idx(item, hash) {
            self.counters[idx].count = self.counters[idx].count.saturating_add(increment);
            self.counters[idx].error = self.counters[idx].error.saturating_add(error);
            return;
        }

        // Below capacity: add directly.
        if self.counters.len() < self.requested_capacity {
            self.push_counter(item.to_vec(), hash, increment, error);
            return;
        }

        // At capacity: use alpha map for historical frequency.
        let alpha_mask = self.alpha_map.len() - 1;
        let alpha_idx = (hash as usize) & alpha_mask;
        let alpha = self.alpha_map[alpha_idx];

        self.push_counter(
            item.to_vec(),
            hash,
            alpha.saturating_add(increment),
            alpha.saturating_add(error),
        );
    }

    fn push_counter(&mut self, item: Vec<u8>, hash: u64, count: u64, error: u64) {
        let idx = self.counters.len();
        self.item_heap_bytes += item.capacity();
        self.counter_map
            .insert_unique(hash, (hash, idx), |&(h, _)| h);
        self.counters.push(Counter {
            item,
            hash,
            count,
            error,
        });
        self.truncate_if_needed(false);
    }

    /// Truncate counters when `target_capacity` is reached,
    /// updating the alpha map with evicted items' true counts.
    fn truncate_if_needed(&mut self, force_rebuild: bool) {
        let need_truncate = self.counters.len() >= self.target_capacity;

        if need_truncate {
            let k = self.requested_capacity;
            if k < self.counters.len() {
                self.counters
                    .select_nth_unstable_by(k - 1, |a, b| a.cmp_by_rank(b));

                let alpha_mask = self.alpha_map.len() - 1;
                for counter in self.counters.drain(k..) {
                    let alpha_idx = (counter.hash as usize) & alpha_mask;
                    let true_count = counter.count.saturating_sub(counter.error);
                    self.alpha_map[alpha_idx] = min(
                        self.alpha_map[alpha_idx].saturating_add(true_count),
                        MAX_ALPHA_VALUE,
                    );
                    self.item_heap_bytes -= counter.item.capacity();
                }
            }
        }

        if need_truncate || force_rebuild {
            self.rebuild_counter_map();
        }
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

    /// Get the top-k items sorted by (count - error) descending.
    fn top_k(&self, k: usize) -> Vec<(&[u8], u64, u64)> {
        if k == 0 || self.counters.is_empty() {
            return Vec::new();
        }

        let mut sorted: Vec<_> = self.counters.iter().collect();
        let return_size = min(sorted.len(), k);

        if return_size < sorted.len() {
            sorted.select_nth_unstable_by(return_size - 1, |a, b| a.cmp_by_rank(b));
            sorted.truncate(return_size);
        }

        sorted.sort_by(|a, b| a.cmp_by_rank(b));

        sorted
            .into_iter()
            .map(|c| (c.item.as_slice(), c.count, c.error))
            .collect()
    }

    /// Merge another summary into this one using the Parallel Space-Saving
    /// reduce-and-combine algorithm from <https://arxiv.org/pdf/1401.0702.pdf>,
    /// matching ClickHouse's `SpaceSaving::merge()` implementation.
    fn merge(&mut self, other: &SpaceSavingSummary) {
        if other.is_empty() {
            return;
        }

        if self.is_empty() {
            self.counters.clone_from(&other.counters);
            self.counter_map.clone_from(&other.counter_map);
            self.alpha_map.clone_from(&other.alpha_map);
            self.requested_capacity = other.requested_capacity;
            self.target_capacity = other.target_capacity;
            // Recompute from cloned vecs since clone may allocate exact-size
            // (capacity == len), which can differ from the original's capacity.
            self.item_heap_bytes = self.counters.iter().map(|c| c.item.capacity()).sum();
            return;
        }

        // Compute m1/m2: the minimum counter count in each summary.
        // Per the Parallel Space-Saving paper (Theorem 1), if a summary has
        // reached capacity, items not in its counter list could have had at
        // most min(counter.count) frequency.  This is the merge correction.
        let m1 = if self.counters.len() >= self.requested_capacity {
            self.counters.iter().map(|c| c.count).min().unwrap_or(0)
        } else {
            0
        };
        let m2 = if other.counters.len() >= other.requested_capacity {
            other.counters.iter().map(|c| c.count).min().unwrap_or(0)
        } else {
            0
        };

        // Step 1: Bump all self counters by m2 (upper bound of what they
        // could have counted in the other partition).
        if m2 > 0 {
            for counter in &mut self.counters {
                counter.count = counter.count.saturating_add(m2);
                counter.error = counter.error.saturating_add(m2);
            }
        }

        // Step 2: Merge other's counters into self.
        for other_counter in &other.counters {
            if let Some(idx) =
                self.find_counter_idx(&other_counter.item, other_counter.hash)
            {
                // Item exists in both: add other's count, subtract the m2 we
                // already added in step 1 (guaranteed non-negative).
                self.counters[idx].count = self.counters[idx]
                    .count
                    .saturating_add(other_counter.count.saturating_sub(m2));
                self.counters[idx].error = self.counters[idx]
                    .error
                    .saturating_add(other_counter.error.saturating_sub(m2));
            } else {
                // Item only in other: add with m1 (upper bound of what it
                // could have counted in self's partition).
                let item = other_counter.item.clone();
                self.item_heap_bytes += item.capacity();
                self.counters.push(Counter {
                    item,
                    hash: other_counter.hash,
                    count: other_counter.count.saturating_add(m1),
                    error: other_counter.error.saturating_add(m1),
                });
            }
        }

        // Merge alpha maps element-wise.
        if self.alpha_map.len() == other.alpha_map.len() {
            for (i, &other_alpha) in other.alpha_map.iter().enumerate() {
                self.alpha_map[i] = min(
                    self.alpha_map[i].saturating_add(other_alpha),
                    MAX_ALPHA_VALUE,
                );
            }
        }

        self.truncate_if_needed(true);
    }

    /// Serialize the summary to bytes (matches ClickHouse's `write()` format).
    ///
    /// Only the top `requested_capacity` counters are written.  The alpha map
    /// carries evicted frequency information for the coordinator merge.
    fn serialize(&mut self) -> Vec<u8> {
        self.truncate_if_needed(false);

        // If there are still more counters than requested_capacity (because
        // target_capacity wasn't reached), partition out the top ones and
        // fold the tail into the alpha map before serializing.
        let k = self.requested_capacity;
        if k > 0 && k < self.counters.len() {
            self.counters
                .select_nth_unstable_by(k - 1, |a, b| a.cmp_by_rank(b));

            let alpha_mask = self.alpha_map.len() - 1;
            for counter in self.counters.drain(k..) {
                let alpha_idx = (counter.hash as usize) & alpha_mask;
                let true_count = counter.count.saturating_sub(counter.error);
                self.alpha_map[alpha_idx] = min(
                    self.alpha_map[alpha_idx].saturating_add(true_count),
                    MAX_ALPHA_VALUE,
                );
                self.item_heap_bytes -= counter.item.capacity();
            }
            self.rebuild_counter_map();
        }

        let num_counters = self.counters.len();

        // Pre-compute total size for a single allocation.
        let counter_bytes: usize =
            self.counters.iter().map(|c| 4 + c.item.len() + 16).sum();
        let total = 16 + counter_bytes + 8 + self.alpha_map.len() * 8;
        let mut bytes = Vec::with_capacity(total);

        bytes.extend_from_slice(&(self.requested_capacity as u64).to_le_bytes());
        bytes.extend_from_slice(&(num_counters as u64).to_le_bytes());

        for counter in &self.counters {
            bytes.extend_from_slice(&(counter.item.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&counter.item);
            bytes.extend_from_slice(&counter.count.to_le_bytes());
            bytes.extend_from_slice(&counter.error.to_le_bytes());
        }

        bytes.extend_from_slice(&(self.alpha_map.len() as u64).to_le_bytes());
        for &alpha in &self.alpha_map {
            bytes.extend_from_slice(&alpha.to_le_bytes());
        }

        bytes
    }

    /// Deserialize a summary from bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 16 {
            return exec_err!("Invalid Space-Saving summary bytes: too short");
        }

        let requested_capacity =
            u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
        let num_counters = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;

        // Validate against reasonable upper bounds to prevent OOM from
        // malformed state.
        if requested_capacity == 0 {
            return exec_err!("Invalid Space-Saving summary: requested_capacity is 0");
        }
        let max_capacity = APPROX_TOP_K_MAX_K * CAPACITY_MULTIPLIER;
        if requested_capacity > max_capacity {
            return exec_err!(
                "Invalid Space-Saving summary: requested_capacity {requested_capacity} \
                 exceeds maximum {max_capacity}"
            );
        }
        if num_counters > requested_capacity {
            return exec_err!(
                "Invalid Space-Saving summary: num_counters {num_counters} exceeds \
                 requested_capacity {requested_capacity}"
            );
        }
        // Each counter needs at least 20 bytes (4 len + 0 item + 8 count + 8 error).
        let max_possible = (bytes.len().saturating_sub(16)) / 20;
        if num_counters > max_possible {
            return exec_err!(
                "Invalid Space-Saving summary: num_counters {num_counters} exceeds \
                 what fits in {} bytes",
                bytes.len()
            );
        }

        let mut counters = Vec::with_capacity(num_counters);
        let mut counter_map = HashTable::with_capacity(num_counters);
        let mut item_heap_bytes: usize = 0;
        let mut offset: usize = 16;

        for idx in 0..num_counters {
            if offset.checked_add(4).is_none_or(|end| end > bytes.len()) {
                return exec_err!(
                    "Invalid Space-Saving summary bytes: truncated item length"
                );
            }
            let item_len =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
                    as usize;
            offset += 4;

            let needed = item_len.checked_add(16);
            if needed
                .is_none_or(|n| offset.checked_add(n).is_none_or(|end| end > bytes.len()))
            {
                return exec_err!(
                    "Invalid Space-Saving summary bytes: truncated counter"
                );
            }

            let item = bytes[offset..offset + item_len].to_vec();
            offset += item_len;

            let count = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;

            let error = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;

            let hash = Self::hash_item(&item);
            item_heap_bytes += item.capacity();
            counter_map.insert_unique(hash, (hash, idx), |&(h, _)| h);
            counters.push(Counter {
                item,
                hash,
                count,
                error,
            });
        }

        if offset.checked_add(8).is_none_or(|end| end > bytes.len()) {
            return exec_err!(
                "Invalid Space-Saving summary bytes: missing alpha map size"
            );
        }
        let alpha_map_size =
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;

        // Validate alpha_map_size is a power of two (required for bitmask indexing).
        if alpha_map_size == 0 || !alpha_map_size.is_power_of_two() {
            return exec_err!(
                "Invalid Space-Saving summary: alpha_map_size {alpha_map_size} \
                 is not a positive power of two"
            );
        }

        let alpha_bytes = alpha_map_size
            .checked_mul(8)
            .and_then(|n| offset.checked_add(n));
        if alpha_bytes.is_none_or(|end| end > bytes.len()) {
            return exec_err!("Invalid Space-Saving summary bytes: truncated alpha map");
        }

        let mut alpha_map = Vec::with_capacity(alpha_map_size);
        for _ in 0..alpha_map_size {
            let alpha = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            alpha_map.push(alpha);
        }

        let target_capacity = max(64, requested_capacity.saturating_mul(2));

        Ok(Self {
            counters,
            counter_map,
            alpha_map,
            requested_capacity,
            target_capacity,
            item_heap_bytes,
        })
    }

    /// Approximate size in bytes of this summary.  O(1) thanks to
    /// incremental `item_heap_bytes` tracking.
    fn size(&self) -> usize {
        size_of::<Self>()
            + self.counters.capacity() * size_of::<Counter>()
            + self.item_heap_bytes
            // HashTable<(u64, usize)>: each bucket stores (hash, idx) + 1 control byte.
            + self.counter_map.capacity()
                * (size_of::<(u64, usize)>() + size_of::<u8>())
            + self.alpha_map.capacity() * size_of::<u64>()
    }
}

// ---------------------------------------------------------------------------
// ApproxTopK  UDAF struct
// ---------------------------------------------------------------------------

/// Approximate top-k UDAF using the Filtered Space-Saving algorithm.
#[user_doc(
    doc_section(label = "Approximate Functions"),
    description = r#"Returns the approximate most frequent (top-k) values with their estimated counts, using the Filtered Space-Saving algorithm. The returned counts are upper-bound estimates; the true frequency lies in `[count - error, count]`. NULL values are skipped; an empty or all-NULL input returns an empty list `[]`. For float columns, -0.0 and +0.0 are treated as distinct values, and different NaN representations are tracked separately."#,
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
        // Supported value types for the first argument.
        let value_types = &[
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Date64,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Second, Some(TIMEZONE_WILDCARD.into())),
            DataType::Timestamp(TimeUnit::Millisecond, Some(TIMEZONE_WILDCARD.into())),
            DataType::Timestamp(TimeUnit::Microsecond, Some(TIMEZONE_WILDCARD.into())),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMEZONE_WILDCARD.into())),
        ];
        // k must be a literal integer; accept any integer type for convenience.
        let k_types = &[
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ];

        let mut variants = Vec::with_capacity(value_types.len() * k_types.len());
        for vt in value_types {
            for kt in k_types {
                variants.push(TypeSignature::Exact(vec![vt.clone(), kt.clone()]));
            }
        }

        Self {
            signature: Signature::one_of(variants, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ApproxTopK {
    fn name(&self) -> &str {
        "approx_top_k"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let value_type = if !arg_types.is_empty() {
            match &arg_types[0] {
                // Large variants are narrowed: the summary stores in-memory
                // byte slices, not large column offsets, so i32 offsets suffice.
                DataType::LargeUtf8 => DataType::Utf8,
                DataType::LargeBinary => DataType::Binary,
                other => other.clone(),
            }
        } else {
            DataType::Utf8
        };

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
            DataType::Binary,
            true,
        ))])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.exprs.len() < 2 {
            return plan_err!("approx_top_k requires two arguments: column and k");
        }

        let k_expr = &args.exprs[1];
        let k = k_expr
            .downcast_ref::<datafusion_physical_expr::expressions::Literal>()
            .and_then(|lit| match lit.value() {
                // Guard against negative values before casting to usize to
                // avoid wrapping (e.g. -1i32 as usize → u64::MAX).
                // Zero is allowed through and caught by the bounds check below.
                ScalarValue::Int8(Some(v)) if *v >= 0 => Some(*v as usize),
                ScalarValue::Int16(Some(v)) if *v >= 0 => Some(*v as usize),
                ScalarValue::Int32(Some(v)) if *v >= 0 => Some(*v as usize),
                ScalarValue::Int64(Some(v)) if *v >= 0 => Some(*v as usize),
                ScalarValue::UInt8(Some(v)) => Some(*v as usize),
                ScalarValue::UInt16(Some(v)) => Some(*v as usize),
                ScalarValue::UInt32(Some(v)) => Some(*v as usize),
                ScalarValue::UInt64(Some(v)) => Some(*v as usize),
                _ => None,
            });

        let Some(k) = k else {
            return plan_err!(
                "approx_top_k requires k to be a positive literal integer \
                 between 1 and 10000"
            );
        };

        if k == 0 || k > APPROX_TOP_K_MAX_K {
            return plan_err!(
                "approx_top_k requires k to be between 1 and {APPROX_TOP_K_MAX_K}, got {k}"
            );
        }

        let data_type = args.expr_fields[0].data_type().clone();
        Ok(Box::new(ApproxTopKAccumulator::new_with_data_type(
            k, data_type,
        )))
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
            DataType::Utf8 | DataType::LargeUtf8 => {
                let values: Vec<Option<String>> = top_items
                    .iter()
                    .map(|(bytes, _, _)| String::from_utf8(bytes.to_vec()).ok())
                    .collect();
                Ok(Arc::new(StringArray::from(values)) as ArrayRef)
            }
            DataType::Binary | DataType::LargeBinary => {
                let values: Vec<Option<&[u8]>> =
                    top_items.iter().map(|(bytes, _, _)| Some(*bytes)).collect();
                Ok(Arc::new(BinaryArray::from(values)) as ArrayRef)
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
            _ => {
                let values: Vec<Option<String>> = top_items
                    .iter()
                    .map(|(bytes, _, _)| String::from_utf8(bytes.to_vec()).ok())
                    .collect();
                Ok(Arc::new(StringArray::from(values)) as ArrayRef)
            }
        }
    }

    /// Get the output data type for the value field.
    fn output_value_data_type(&self) -> DataType {
        match &self.input_data_type {
            // LargeUtf8 is narrowed to Utf8: the summary stores in-memory
            // byte slices, not large column offsets, so i32 offsets suffice.
            DataType::LargeUtf8 => DataType::Utf8,
            DataType::LargeBinary => DataType::Binary,
            other => other.clone(),
        }
    }
}

impl Accumulator for ApproxTopKAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let data_array = &values[0];

        // Downcast once and iterate directly to avoid per-row ScalarValue allocation.
        macro_rules! process_array {
            ($array_type:ty, $data_array:expr) => {{
                let Some(arr) = $data_array.as_any().downcast_ref::<$array_type>() else {
                    return exec_err!(
                        "approx_top_k: failed to downcast array to {}",
                        stringify!($array_type)
                    );
                };
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.summary.add(&arr.value(i).to_le_bytes());
                    }
                }
            }};
        }

        macro_rules! process_bytes_array {
            ($array_type:ty, $data_array:expr) => {{
                let Some(arr) = $data_array.as_any().downcast_ref::<$array_type>() else {
                    return exec_err!(
                        "approx_top_k: failed to downcast array to {}",
                        stringify!($array_type)
                    );
                };
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.summary.add(arr.value(i).as_ref());
                    }
                }
            }};
        }

        match data_array.data_type() {
            DataType::Utf8 => process_bytes_array!(StringArray, data_array),
            DataType::LargeUtf8 => {
                process_bytes_array!(LargeStringArray, data_array)
            }
            DataType::Binary => process_bytes_array!(BinaryArray, data_array),
            DataType::LargeBinary => {
                process_bytes_array!(LargeBinaryArray, data_array)
            }
            DataType::Int8 => process_array!(Int8Array, data_array),
            DataType::Int16 => process_array!(Int16Array, data_array),
            DataType::Int32 => process_array!(Int32Array, data_array),
            DataType::Int64 => process_array!(Int64Array, data_array),
            DataType::UInt8 => process_array!(UInt8Array, data_array),
            DataType::UInt16 => process_array!(UInt16Array, data_array),
            DataType::UInt32 => process_array!(UInt32Array, data_array),
            DataType::UInt64 => process_array!(UInt64Array, data_array),
            // Note: floats are compared by their byte representation, so -0.0 and +0.0
            // are treated as distinct values, and different NaN bit patterns are tracked
            // separately. This matches ClickHouse's behavior for topK with floats.
            DataType::Float32 => process_array!(Float32Array, data_array),
            DataType::Float64 => process_array!(Float64Array, data_array),
            DataType::Date32 => process_array!(Date32Array, data_array),
            DataType::Date64 => process_array!(Date64Array, data_array),
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => {
                    process_array!(TimestampSecondArray, data_array)
                }
                TimeUnit::Millisecond => {
                    process_array!(TimestampMillisecondArray, data_array)
                }
                TimeUnit::Microsecond => {
                    process_array!(TimestampMicrosecondArray, data_array)
                }
                TimeUnit::Nanosecond => {
                    process_array!(TimestampNanosecondArray, data_array)
                }
            },
            other => {
                return exec_err!("Unsupported data type for approx_top_k: {other}");
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() || states[0].is_empty() {
            return Ok(());
        }

        let Some(summary_array) = states[0].as_any().downcast_ref::<BinaryArray>() else {
            return exec_err!("Expected Binary array for summary state");
        };

        for i in 0..summary_array.len() {
            if summary_array.is_null(i) {
                continue;
            }
            let bytes = summary_array.value(i);
            let other_summary = SpaceSavingSummary::from_bytes(bytes)?;
            self.summary.merge(&other_summary);
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
        Ok(vec![ScalarValue::Binary(Some(self.summary.serialize()))])
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.summary.size()
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

        for i in 0..63u64 {
            let item = format!("rare_{i}");
            summary.add(item.as_bytes());
        }

        assert_eq!(summary.len(), 2);

        let (count, error) = summary.get(b"frequent").unwrap();
        assert_eq!(count, 100);
        assert_eq!(error, 0);

        let evicted_count = (0..63u64)
            .filter(|i| {
                let item = format!("rare_{i}");
                summary.get(item.as_bytes()).is_none()
            })
            .count();
        assert!(evicted_count >= 61);
    }

    #[test]
    fn test_space_saving_alpha_map() {
        let mut summary = SpaceSavingSummary::new(2);

        for i in 0..64u64 {
            let item = format!("item_{i}");
            summary.add(item.as_bytes());
        }

        assert_eq!(summary.len(), 2);

        let alpha_sum: u64 = summary.alpha_map.iter().sum();
        assert!(alpha_sum > 0);

        summary.add(b"item_0");
        assert_eq!(summary.len(), 3);

        let (count, error) = summary.get(b"item_0").unwrap();
        assert!(count > 1);
        assert_eq!(count, error + 1);
    }

    #[test]
    fn test_space_saving_serialization() {
        let mut summary = SpaceSavingSummary::new(3);
        summary.add(b"test");
        summary.add(b"test");
        summary.add(b"value");

        let bytes = summary.serialize();
        let restored = SpaceSavingSummary::from_bytes(&bytes).unwrap();

        assert_eq!(restored.capacity(), summary.capacity());
        assert_eq!(restored.len(), summary.len());

        let (count, _) = restored.get(b"test").unwrap();
        assert_eq!(count, 2);
        let (count, _) = restored.get(b"value").unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_space_saving_merge() {
        let mut summary1 = SpaceSavingSummary::new(4);
        let mut summary2 = SpaceSavingSummary::new(4);

        summary1.add(b"apple");
        summary1.add(b"apple");
        summary2.add(b"apple");
        summary2.add(b"banana");

        summary1.merge(&summary2);

        let (count, _) = summary1.get(b"apple").unwrap();
        assert_eq!(count, 3);

        let (count, _) = summary1.get(b"banana").unwrap();
        assert_eq!(count, 1);
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

        summary1.merge(&summary2);

        let top = summary1.top_k(2);
        assert!(!top.is_empty());
        let top_item_result = top.iter().find(|(item, _, _)| *item == b"top_item");
        assert!(top_item_result.is_some());
    }

    /// Helper to extract top-k results from a ScalarValue::List result.
    fn extract_top_k_results(result: &ScalarValue) -> Vec<(String, u64)> {
        if let ScalarValue::List(list_array) = result {
            let struct_array = list_array
                .values()
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Expected StructArray");

            let value_array = struct_array
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray for values");
            let count_array = struct_array
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("Expected UInt64Array for counts");

            (0..struct_array.len())
                .map(|i| {
                    let value = value_array.value(i).to_string();
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

        let summary_bytes = if let ScalarValue::Binary(Some(bytes)) = &state2[0] {
            bytes.clone()
        } else {
            panic!("Expected Binary for summary")
        };

        let summary_array: ArrayRef =
            Arc::new(BinaryArray::from(vec![Some(summary_bytes.as_slice())]));

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
            if let ScalarValue::Binary(Some(ref b)) = state1[0] {
                Some(b.as_slice())
            } else {
                None
            },
            if let ScalarValue::Binary(Some(ref b)) = state2[0] {
                Some(b.as_slice())
            } else {
                None
            },
            if let ScalarValue::Binary(Some(ref b)) = state3[0] {
                Some(b.as_slice())
            } else {
                None
            },
        ];

        let summary_array: ArrayRef = Arc::new(BinaryArray::from(summary_bytes));

        let mut coord_acc = ApproxTopKAccumulator::new_with_data_type(3, DataType::Utf8);
        coord_acc.merge_batch(&[summary_array]).unwrap();

        let result = coord_acc.evaluate().unwrap();
        let top_k = extract_top_k_results(&result);

        assert!(top_k.len() >= 2);
        assert_eq!(top_k[0], ("apple".to_string(), 5));
        assert_eq!(top_k[1], ("banana".to_string(), 4));
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
}
