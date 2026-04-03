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

//! Native (non-RowConverter) TopK heap for single primitive column sorts.
//!
//! For ORDER BY on a single primitive column, this avoids the overhead of
//! Arrow's RowConverter by encoding sort keys as inline `u128` values with
//! order-preserving encoding that handles ASC/DESC and NULLS FIRST/LAST.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::mem::size_of;

use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray, RecordBatch};
use arrow::compute::interleave_record_batch;
use arrow::datatypes::*;
use arrow_schema::SortOptions;
use datafusion_common::{HashMap, Result};

use super::{RecordBatchEntry, RecordBatchStore};

// ---------------------------------------------------------------------------
// Order-preserving encoding into u128
// ---------------------------------------------------------------------------

/// Encode a signed integer value into u64 preserving ascending order.
#[inline]
fn encode_signed(v: i64) -> u64 {
    (v as u64) ^ (1u64 << 63)
}

/// Encode an f32 value into u64 preserving total ascending order.
#[inline]
fn encode_f32(v: f32) -> u64 {
    // f32 → f64 is lossless, reuse f64 encoding
    encode_f64(v as f64)
}

/// Encode an f64 value into u64 preserving total ascending order
/// (including NaN ordering consistent with `total_cmp`).
#[inline]
fn encode_f64(v: f64) -> u64 {
    let bits = v.to_bits();
    if bits >> 63 == 1 {
        // Negative: flip all bits (maps most-negative → 0)
        !bits
    } else {
        // Non-negative: flip sign bit (maps 0.0 → 2^63)
        bits ^ (1u64 << 63)
    }
}

/// Wrap an encoded u64 value with null handling and sort options into a
/// comparable u128 key.
///
/// Layout:
/// - `NULLS FIRST`: null → `0`, non-null → `encoded + 1`
/// - `NULLS LAST`:  non-null → `encoded + 1`, null → `u128::MAX`
/// - `DESC`:        non-null value bits are flipped before offsetting
#[inline]
fn encode_key(is_null: bool, encoded_value: u64, options: SortOptions) -> u128 {
    if is_null {
        return if options.nulls_first {
            0u128
        } else {
            u128::MAX
        };
    }
    let v = if options.descending {
        !encoded_value as u128
    } else {
        encoded_value as u128
    };
    // +1 ensures non-null values are always > 0 (the NULLS FIRST sentinel)
    // and < u128::MAX (the NULLS LAST sentinel).
    v + 1
}

/// Returns `true` if `dt` can be encoded into a u128 native TopK key.
pub fn supports_native_topk(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
    )
}

// ---------------------------------------------------------------------------
// NativeTopKRow
// ---------------------------------------------------------------------------

/// A TopK row with an inline sort key for single-primitive-column sorts.
///
/// Unlike [`super::TopKRow`] which heap-allocates a `Vec<u8>` of
/// RowConverter-encoded bytes, this stores the key as an inline `u128`.
#[derive(Debug, PartialEq, Eq)]
pub struct NativeTopKRow {
    /// Order-preserving encoded sort key.
    pub key: u128,
    /// The [`RecordBatch`] this row came from (id into [`RecordBatchStore`]).
    pub batch_id: u32,
    /// Row index inside that batch.
    pub index: usize,
}

impl NativeTopKRow {
    fn new(key: u128, batch_id: u32, index: usize) -> Self {
        Self {
            key,
            batch_id,
            index,
        }
    }
}

impl Ord for NativeTopKRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for NativeTopKRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// ---------------------------------------------------------------------------
// NativeTopKHeap
// ---------------------------------------------------------------------------

/// Min-heap that keeps the smallest K encoded keys, backed by a max-heap
/// ([`BinaryHeap`] is a max-heap; the *largest* key in the heap is the
/// current threshold).
pub struct NativeTopKHeap {
    k: usize,
    batch_size: usize,
    inner: BinaryHeap<NativeTopKRow>,
    pub store: RecordBatchStore,
}

impl NativeTopKHeap {
    pub fn new(k: usize, batch_size: usize) -> Self {
        assert!(k > 0);
        Self {
            k,
            batch_size,
            inner: BinaryHeap::new(),
            store: RecordBatchStore::new(),
        }
    }

    pub fn register_batch(&mut self, batch: RecordBatch) -> RecordBatchEntry {
        self.store.register(batch)
    }

    pub fn insert_batch_entry(&mut self, entry: RecordBatchEntry) {
        self.store.insert(entry)
    }

    /// Returns the current threshold row (the largest / worst key in the
    /// heap) if the heap already contains `k` items.
    pub fn max(&self) -> Option<&NativeTopKRow> {
        if self.inner.len() < self.k {
            None
        } else {
            self.inner.peek()
        }
    }

    /// Insert a new row. If the heap is full, evicts the worst entry.
    pub fn add(&mut self, batch_entry: &mut RecordBatchEntry, key: u128, index: usize) {
        let batch_id = batch_entry.id;
        batch_entry.uses += 1;

        debug_assert!(self.inner.len() <= self.k);

        if self.inner.len() == self.k {
            let mut prev = self.inner.peek_mut().unwrap();
            if prev.batch_id == batch_entry.id {
                batch_entry.uses -= 1;
            } else {
                self.store.unuse(prev.batch_id);
            }
            prev.key = key;
            prev.batch_id = batch_id;
            prev.index = index;
            // PeekMut drop will sift down
        } else {
            self.inner.push(NativeTopKRow::new(key, batch_id, index));
        }
    }

    /// Drain the heap into a single sorted [`RecordBatch`].
    pub fn emit(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.emit_with_state()?.0)
    }

    pub fn emit_with_state(
        &mut self,
    ) -> Result<(Option<RecordBatch>, Vec<NativeTopKRow>)> {
        let rows = std::mem::take(&mut self.inner).into_sorted_vec();

        if self.store.is_empty() {
            return Ok((None, rows));
        }

        let mut record_batches = Vec::new();
        let mut id_to_pos = HashMap::new();
        for (pos, (batch_id, batch)) in self.store.batches.iter().enumerate() {
            record_batches.push(&batch.batch);
            id_to_pos.insert(*batch_id, pos);
        }

        let indices: Vec<_> = rows
            .iter()
            .map(|r| (id_to_pos[&r.batch_id], r.index))
            .collect();

        let batch = interleave_record_batch(&record_batches, &indices)?;
        Ok((Some(batch), rows))
    }

    /// Compact stored batches to reclaim memory from unused rows.
    pub fn maybe_compact(&mut self) -> Result<()> {
        let max_unused_rows = (20 * self.batch_size) + self.k;
        let unused_rows = self.store.unused_rows();

        if self.store.len() <= 2 || unused_rows < max_unused_rows {
            return Ok(());
        }

        let num_rows = self.inner.len();
        let (new_batch, mut rows) = self.emit_with_state()?;
        let Some(new_batch) = new_batch else {
            return Ok(());
        };

        self.store.clear();
        let mut batch_entry = self.register_batch(new_batch);
        batch_entry.uses = num_rows;

        for (i, row) in rows.iter_mut().enumerate() {
            row.batch_id = batch_entry.id;
            row.index = i;
        }
        self.insert_batch_entry(batch_entry);
        self.inner = BinaryHeap::from(rows);

        Ok(())
    }

    pub fn size(&self) -> usize {
        size_of::<Self>()
            + (self.inner.capacity() * size_of::<NativeTopKRow>())
            + self.store.size()
    }
}

// ---------------------------------------------------------------------------
// Batch-level encoding + insertion
// ---------------------------------------------------------------------------

/// Encode values from `sort_key` and insert qualifying rows into `heap`.
///
/// Returns the number of heap replacements.
pub fn find_new_native_topk_items(
    heap: &mut NativeTopKHeap,
    sort_key: &ArrayRef,
    options: SortOptions,
    items: impl Iterator<Item = usize>,
    batch_entry: &mut RecordBatchEntry,
) -> usize {
    macro_rules! dispatch_signed {
        ($arrow_ty:ty, $array:expr, $items:expr) => {{
            let typed = $array.as_primitive::<$arrow_ty>();
            find_items_inner(
                heap,
                typed,
                |v| encode_signed(v as i64),
                options,
                $items,
                batch_entry,
            )
        }};
    }

    macro_rules! dispatch_unsigned {
        ($arrow_ty:ty, $array:expr, $items:expr) => {{
            let typed = $array.as_primitive::<$arrow_ty>();
            find_items_inner(heap, typed, |v| v as u64, options, $items, batch_entry)
        }};
    }

    match sort_key.data_type() {
        DataType::Int8 => dispatch_signed!(Int8Type, sort_key, items),
        DataType::Int16 => dispatch_signed!(Int16Type, sort_key, items),
        DataType::Int32 => dispatch_signed!(Int32Type, sort_key, items),
        DataType::Int64 => dispatch_signed!(Int64Type, sort_key, items),
        DataType::UInt8 => dispatch_unsigned!(UInt8Type, sort_key, items),
        DataType::UInt16 => dispatch_unsigned!(UInt16Type, sort_key, items),
        DataType::UInt32 => dispatch_unsigned!(UInt32Type, sort_key, items),
        DataType::UInt64 => dispatch_unsigned!(UInt64Type, sort_key, items),
        DataType::Float32 => {
            let typed = sort_key.as_primitive::<Float32Type>();
            find_items_inner(heap, typed, encode_f32, options, items, batch_entry)
        }
        DataType::Float64 => {
            let typed = sort_key.as_primitive::<Float64Type>();
            find_items_inner(heap, typed, encode_f64, options, items, batch_entry)
        }
        // Date/Time/Timestamp/Duration are stored as i32 or i64
        DataType::Date32 => dispatch_signed!(Date32Type, sort_key, items),
        DataType::Date64 => dispatch_signed!(Date64Type, sort_key, items),
        DataType::Time32(TimeUnit::Second) => {
            dispatch_signed!(Time32SecondType, sort_key, items)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            dispatch_signed!(Time32MillisecondType, sort_key, items)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            dispatch_signed!(Time64MicrosecondType, sort_key, items)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            dispatch_signed!(Time64NanosecondType, sort_key, items)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            dispatch_signed!(TimestampSecondType, sort_key, items)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            dispatch_signed!(TimestampMillisecondType, sort_key, items)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            dispatch_signed!(TimestampMicrosecondType, sort_key, items)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            dispatch_signed!(TimestampNanosecondType, sort_key, items)
        }
        DataType::Duration(TimeUnit::Second) => {
            dispatch_signed!(DurationSecondType, sort_key, items)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            dispatch_signed!(DurationMillisecondType, sort_key, items)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            dispatch_signed!(DurationMicrosecondType, sort_key, items)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            dispatch_signed!(DurationNanosecondType, sort_key, items)
        }
        other => unreachable!("unsupported native TopK type: {other}"),
    }
}

/// Inner loop: iterate candidate indices, encode each value, and insert
/// into the heap when it beats the current threshold.
#[inline]
fn find_items_inner<T: ArrowPrimitiveType>(
    heap: &mut NativeTopKHeap,
    array: &arrow::array::PrimitiveArray<T>,
    encode: impl Fn(T::Native) -> u64,
    options: SortOptions,
    items: impl Iterator<Item = usize>,
    batch_entry: &mut RecordBatchEntry,
) -> usize {
    let mut replacements = 0;
    for index in items {
        let key = if array.is_null(index) {
            if options.nulls_first {
                0u128
            } else {
                u128::MAX
            }
        } else {
            encode_key(false, encode(array.value(index)), options)
        };

        match heap.max() {
            Some(max_row) if key >= max_row.key => {}
            _ => {
                heap.add(batch_entry, key, index);
                replacements += 1;
            }
        }
    }
    replacements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_signed_order() {
        // i64 order should be preserved
        assert!(encode_signed(i64::MIN) < encode_signed(-1));
        assert!(encode_signed(-1) < encode_signed(0));
        assert!(encode_signed(0) < encode_signed(1));
        assert!(encode_signed(1) < encode_signed(i64::MAX));
    }

    #[test]
    fn test_encode_f64_order() {
        assert!(encode_f64(f64::NEG_INFINITY) < encode_f64(-1.0));
        assert!(encode_f64(-1.0) < encode_f64(-0.0));
        // -0.0 and +0.0 have different encodings (matching total_cmp)
        assert!(encode_f64(-0.0) < encode_f64(0.0));
        assert!(encode_f64(0.0) < encode_f64(1.0));
        assert!(encode_f64(1.0) < encode_f64(f64::INFINITY));
        assert!(encode_f64(f64::INFINITY) < encode_f64(f64::NAN));
    }

    #[test]
    fn test_encode_key_ascending_nulls_last() {
        let opts = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let null_key = encode_key(true, 0, opts);
        let val_key = encode_key(false, 42, opts);
        assert!(val_key < null_key, "non-null should sort before null");
    }

    #[test]
    fn test_encode_key_ascending_nulls_first() {
        let opts = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let null_key = encode_key(true, 0, opts);
        let val_key = encode_key(false, 42, opts);
        assert!(null_key < val_key, "null should sort before non-null");
    }

    #[test]
    fn test_encode_key_descending_nulls_first() {
        let opts = SortOptions {
            descending: true,
            nulls_first: true,
        };
        let null_key = encode_key(true, 0, opts);
        let small = encode_key(false, encode_signed(1), opts);
        let large = encode_key(false, encode_signed(100), opts);
        assert!(null_key < small, "null first in desc");
        assert!(
            large < small,
            "larger value should have smaller key in desc"
        );
    }

    #[test]
    fn test_encode_key_descending_nulls_last() {
        let opts = SortOptions {
            descending: true,
            nulls_first: false,
        };
        let null_key = encode_key(true, 0, opts);
        let val_key = encode_key(false, encode_signed(1), opts);
        assert!(val_key < null_key, "null last in desc");
    }
}
