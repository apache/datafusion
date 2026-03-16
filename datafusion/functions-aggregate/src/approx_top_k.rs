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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    ListArray, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, TypeSignature, Volatility,
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
    fn cmp_by_rank(&self, other: &Counter) -> std::cmp::Ordering {
        if other.is_greater_than(self) {
            std::cmp::Ordering::Greater
        } else if self.is_greater_than(other) {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Equal
        }
    }
}

/// Filtered Space-Saving algorithm summary for approximate top-k / heavy hitters.
///
/// Uses a hash map for O(1) counter lookups and maintains an alpha map (filter)
/// to remember evicted items' frequencies.
#[derive(Debug, Clone)]
struct SpaceSavingSummary {
    counters: Vec<Counter>,
    counter_map: HashMap<Vec<u8>, usize>,
    alpha_map: Vec<u64>,
    requested_capacity: usize,
    /// Internal target capacity to avoid frequent truncations.
    /// Set to `max(64, requested_capacity * 2)`.
    target_capacity: usize,
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
            counter_map: HashMap::new(),
            alpha_map: Vec::new(),
            requested_capacity: 0,
            target_capacity: 0,
        }
        .resized(capacity)
    }

    fn resized(mut self, new_capacity: usize) -> Self {
        if self.requested_capacity != new_capacity {
            debug_assert!(self.counters.is_empty());
            let alpha_map_size = Self::compute_alpha_map_size(new_capacity);
            self.alpha_map = vec![0u64; alpha_map_size];
            self.requested_capacity = new_capacity;
            self.target_capacity = std::cmp::max(64, new_capacity * 2);
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

    fn find_counter_mut(&mut self, item: &[u8]) -> Option<&mut Counter> {
        self.counter_map
            .get(item)
            .copied()
            .map(|idx| &mut self.counters[idx])
    }

    #[cfg(test)]
    fn find_counter(&self, item: &[u8]) -> Option<&Counter> {
        self.counter_map.get(item).map(|&idx| &self.counters[idx])
    }

    /// Add an item with increment 1.
    fn add(&mut self, item: &[u8]) {
        self.insert(item, 1, 0);
    }

    /// Core insertion algorithm from Filtered Space-Saving.
    fn insert(&mut self, item: &[u8], increment: u64, error: u64) {
        let hash = Self::hash_item(item);

        // Fast path: item already tracked.
        if let Some(counter) = self.find_counter_mut(item) {
            counter.count += increment;
            counter.error += error;
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

        self.push_counter(item.to_vec(), hash, alpha + increment, alpha + error);
    }

    fn push_counter(&mut self, item: Vec<u8>, hash: u64, count: u64, error: u64) {
        let idx = self.counters.len();
        self.counter_map.insert(item.clone(), idx);
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
                    self.alpha_map[alpha_idx] = std::cmp::min(
                        self.alpha_map[alpha_idx] + true_count,
                        MAX_ALPHA_VALUE,
                    );
                }
            }
        }

        if need_truncate || force_rebuild {
            self.counter_map.clear();
            for (idx, counter) in self.counters.iter().enumerate() {
                self.counter_map.insert(counter.item.clone(), idx);
            }
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
        let return_size = std::cmp::min(sorted.len(), k);

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

    /// Merge another summary into this one.
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
            return;
        }

        for other_counter in &other.counters {
            if let Some(idx) = self.counter_map.get(&other_counter.item).copied() {
                self.counters[idx].count += other_counter.count;
                self.counters[idx].error += other_counter.error;
            } else {
                self.counters.push(Counter {
                    item: other_counter.item.clone(),
                    hash: other_counter.hash,
                    count: other_counter.count,
                    error: other_counter.error,
                });
            }
        }

        // Merge alpha maps element-wise. Sizes should always match because the
        // planner guarantees the same k (and thus the same capacity/alpha map size)
        // across all partitions. If they differ due to a bug, we skip the merge
        // which only degrades accuracy without affecting correctness.
        if self.alpha_map.len() == other.alpha_map.len() {
            for (i, &other_alpha) in other.alpha_map.iter().enumerate() {
                self.alpha_map[i] =
                    std::cmp::min(self.alpha_map[i] + other_alpha, MAX_ALPHA_VALUE);
            }
        }

        self.truncate_if_needed(true);
    }

    /// Serialize the summary to bytes.
    fn serialize(&mut self) -> Vec<u8> {
        // Ensure counters are truncated and alpha map is up to date before
        // serializing, in case to_bytes is called without a prior truncation.
        self.truncate_if_needed(false);

        let counters_to_write: Vec<_> = {
            let mut sorted: Vec<_> = self.counters.iter().collect();
            let return_size = std::cmp::min(sorted.len(), self.requested_capacity);
            if return_size > 0 && return_size < sorted.len() {
                // After select_nth_unstable_by, the top-k counters are in
                // sorted[..return_size] but in arbitrary order. This is fine
                // since deserialization doesn't depend on counter ordering.
                sorted.select_nth_unstable_by(return_size - 1, |a, b| a.cmp_by_rank(b));
            }
            sorted.truncate(return_size);
            sorted
        };

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(self.requested_capacity as u64).to_le_bytes());
        bytes.extend_from_slice(&(counters_to_write.len() as u64).to_le_bytes());

        for counter in counters_to_write {
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
            return Err(datafusion_common::DataFusionError::Execution(
                "Invalid Space-Saving summary bytes: too short".to_string(),
            ));
        }

        let requested_capacity =
            u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
        let num_counters = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;

        let mut counters = Vec::with_capacity(num_counters);
        let mut counter_map = HashMap::with_capacity(num_counters);
        let mut offset = 16;

        for idx in 0..num_counters {
            if offset + 4 > bytes.len() {
                return Err(datafusion_common::DataFusionError::Execution(
                    "Invalid Space-Saving summary bytes: truncated item length"
                        .to_string(),
                ));
            }
            let item_len =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
                    as usize;
            offset += 4;

            if offset + item_len + 16 > bytes.len() {
                return Err(datafusion_common::DataFusionError::Execution(
                    "Invalid Space-Saving summary bytes: truncated counter".to_string(),
                ));
            }

            let item = bytes[offset..offset + item_len].to_vec();
            offset += item_len;

            let count = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;

            let error = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;

            let hash = Self::hash_item(&item);
            counter_map.insert(item.clone(), idx);
            counters.push(Counter {
                item,
                hash,
                count,
                error,
            });
        }

        if offset + 8 > bytes.len() {
            return Err(datafusion_common::DataFusionError::Execution(
                "Invalid Space-Saving summary bytes: missing alpha map size".to_string(),
            ));
        }
        let alpha_map_size =
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;

        if offset + alpha_map_size.saturating_mul(8) > bytes.len() {
            return Err(datafusion_common::DataFusionError::Execution(
                "Invalid Space-Saving summary bytes: truncated alpha map".to_string(),
            ));
        }

        let mut alpha_map = Vec::with_capacity(alpha_map_size);
        for _ in 0..alpha_map_size {
            let alpha = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
            offset += 8;
            alpha_map.push(alpha);
        }

        let target_capacity = std::cmp::max(64, requested_capacity * 2);

        Ok(Self {
            counters,
            counter_map,
            alpha_map,
            requested_capacity,
            target_capacity,
        })
    }

    /// Approximate size in bytes of this summary.
    fn size(&self) -> usize {
        // Heap bytes owned by each counter's item Vec.
        let item_heap_bytes: usize = self
            .counters
            .iter()
            .map(|c| c.item.capacity())
            .sum::<usize>();

        size_of::<Self>()
            // Vec<Counter> backing storage.
            + self.counters.capacity() * size_of::<Counter>()
            // Heap allocations for counter item bytes (owned by counters).
            + item_heap_bytes
            // HashMap<Vec<u8>, usize>: bucket overhead + control bytes.
            + self.counter_map.capacity()
                * (size_of::<(Vec<u8>, usize)>() + size_of::<u8>())
            // HashMap keys are clones of counter items, so count their heap bytes again.
            + item_heap_bytes
            // Vec<u64> alpha map.
            + self.alpha_map.capacity() * size_of::<u64>()
    }
}

// ---------------------------------------------------------------------------
// ApproxTopK  UDAF struct
// ---------------------------------------------------------------------------

/// Approximate top-k UDAF using the Filtered Space-Saving algorithm.
#[user_doc(
    doc_section(label = "Approximate Functions"),
    description = "Returns the approximate most frequent (top-k) values and their counts using the Filtered Space-Saving algorithm. Note: for float columns, -0.0 and +0.0 are treated as distinct values, and different NaN representations are tracked separately.",
    syntax_example = "approx_top_k(expression, k)",
    sql_example = r#"```sql
> SELECT approx_top_k(column_name, 3) FROM table_name;
+-------------------------------------------+
| approx_top_k(column_name, 3)              |
+-------------------------------------------+
| [{value: foo, count: 3}, {value: bar, count: 2}, {value: baz, count: 1}] |
+-------------------------------------------+
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
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "summary"),
                DataType::Binary,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "k"),
                DataType::UInt64,
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "data_type"),
                DataType::Utf8,
                true,
            )),
        ])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.exprs.len() < 2 {
            return Err(datafusion_common::DataFusionError::Plan(
                "approx_top_k requires two arguments: column and k".to_string(),
            ));
        }

        let k_expr = &args.exprs[1];
        let k = k_expr
            .as_any()
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
            })
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Plan(
                    "approx_top_k requires k to be a positive literal integer"
                        .to_string(),
                )
            })?;

        if k == 0 || k > APPROX_TOP_K_MAX_K {
            return Err(datafusion_common::DataFusionError::Plan(format!(
                "approx_top_k requires k to be between 1 and {APPROX_TOP_K_MAX_K}, got {k}"
            )));
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
                    DataType::Timestamp(unit, tz) => {
                        use arrow::datatypes::TimeUnit;
                        let arr: ArrayRef = match unit {
                            TimeUnit::Second => {
                                Arc::new(TimestampSecondArray::from(values))
                            }
                            TimeUnit::Millisecond => {
                                Arc::new(TimestampMillisecondArray::from(values))
                            }
                            TimeUnit::Microsecond => {
                                Arc::new(TimestampMicrosecondArray::from(values))
                            }
                            TimeUnit::Nanosecond => {
                                Arc::new(TimestampNanosecondArray::from(values))
                            }
                        };
                        if tz.is_some() {
                            // Preserve timezone in the output.
                            Ok(arrow::compute::cast(&arr, &self.input_data_type)?)
                        } else {
                            Ok(arr)
                        }
                    }
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

    /// Serialize a DataType to a string using Arrow's Display impl.
    fn data_type_to_string(dt: &DataType) -> String {
        dt.to_string()
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
                let arr = $data_array.as_any().downcast_ref::<$array_type>().unwrap();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.summary.add(&arr.value(i).to_le_bytes());
                    }
                }
            }};
        }

        match data_array.data_type() {
            DataType::Utf8 => {
                let arr = data_array.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.summary.add(arr.value(i).as_bytes());
                    }
                }
            }
            DataType::LargeUtf8 => {
                let arr = data_array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.summary.add(arr.value(i).as_bytes());
                    }
                }
            }
            DataType::Binary => {
                let arr = data_array.as_any().downcast_ref::<BinaryArray>().unwrap();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.summary.add(arr.value(i));
                    }
                }
            }
            DataType::LargeBinary => {
                let arr = data_array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.summary.add(arr.value(i));
                    }
                }
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
            DataType::Timestamp(_, _) => {
                // All timestamp variants are stored as i64 internally.
                match data_array.data_type() {
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
                        process_array!(TimestampSecondArray, data_array)
                    }
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                        process_array!(TimestampMillisecondArray, data_array)
                    }
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                        process_array!(TimestampMicrosecondArray, data_array)
                    }
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
                        process_array!(TimestampNanosecondArray, data_array)
                    }
                    _ => unreachable!(),
                }
            }
            other => {
                return Err(datafusion_common::DataFusionError::Execution(format!(
                    "Unsupported data type for approx_top_k: {other}"
                )));
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // State layout: [summary (Binary), k (UInt64), data_type (Utf8)].
        // The `k` field (states[1]) is carried for completeness but not read here
        // because the planner guarantees all partial accumulators use the same `k`.
        if states.is_empty() || states[0].is_empty() {
            return Ok(());
        }

        let summary_array = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "Expected Binary array for summary state".to_string(),
                )
            })?;

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
        let summary_bytes = self.summary.serialize();

        Ok(vec![
            ScalarValue::Binary(Some(summary_bytes)),
            ScalarValue::UInt64(Some(self.k as u64)),
            ScalarValue::Utf8(Some(Self::data_type_to_string(&self.input_data_type))),
        ])
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
        let k = if let ScalarValue::UInt64(Some(k)) = &state2[1] {
            *k
        } else {
            panic!("Expected UInt64 for k")
        };

        let summary_array: ArrayRef =
            Arc::new(BinaryArray::from(vec![Some(summary_bytes.as_slice())]));
        let k_array: ArrayRef = Arc::new(UInt64Array::from(vec![k]));

        acc1.merge_batch(&[summary_array, k_array]).unwrap();

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
        let k_values: Vec<u64> = vec![
            if let ScalarValue::UInt64(Some(k)) = state1[1] {
                k
            } else {
                0
            },
            if let ScalarValue::UInt64(Some(k)) = state2[1] {
                k
            } else {
                0
            },
            if let ScalarValue::UInt64(Some(k)) = state3[1] {
                k
            } else {
                0
            },
        ];

        let summary_array: ArrayRef = Arc::new(BinaryArray::from(summary_bytes));
        let k_array: ArrayRef = Arc::new(UInt64Array::from(k_values));

        let mut coord_acc = ApproxTopKAccumulator::new_with_data_type(3, DataType::Utf8);
        coord_acc.merge_batch(&[summary_array, k_array]).unwrap();

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
        let mut acc = ApproxTopKAccumulator::new_with_data_type(2, DataType::Utf8);

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
