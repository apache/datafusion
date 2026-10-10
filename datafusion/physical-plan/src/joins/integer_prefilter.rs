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

//! Exact membership filtering for a single integer hash join key.
//!
//! This only avoids hash table lookups. Probe rows remain in their original
//! batch so outer, anti and mark joins can still produce unmatched results.

use crate::joins::array_map::ArrayMap;
use arrow::array::Array;
use arrow::array::downcast_integer_array;
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::DataType;
use datafusion_common::config::ExecutionOptions;
use datafusion_common::{Result, ScalarValue, config_err};
use datafusion_execution::memory_pool::MemoryReservation;

/// One bit per integer in the build key range, shared by all probe streams.
#[derive(Debug)]
pub(super) struct IntegerPrefilter {
    offset: u64,
    key_count: u64,
    words: Vec<u64>,
    data_type: DataType,
}

impl IntegerPrefilter {
    /// Uses already computed build bounds. Failure to reserve this optional
    /// allocation leaves the normal hash join available.
    #[expect(
        clippy::unnecessary_cast,
        reason = "the integer downcast macro also expands for UInt64"
    )]
    pub(super) fn try_new(
        values: &dyn Array,
        bounds: (&ScalarValue, &ScalarValue),
        max_key_range: u64,
        reservation: &MemoryReservation,
    ) -> Option<Self> {
        if !ArrayMap::is_supported_type(values.data_type()) || max_key_range == 0 {
            return None;
        }
        let offset = ArrayMap::key_to_u64(bounds.0)?;
        let max = ArrayMap::key_to_u64(bounds.1)?;
        let key_count = max.wrapping_sub(offset).checked_add(1)?;
        if key_count > max_key_range {
            return None;
        }
        let word_count = usize::try_from(key_count.div_ceil(64)).ok()?;
        let bytes = word_count.checked_mul(size_of::<u64>())?;
        reservation.try_grow(bytes).ok()?;
        let mut words = Vec::new();
        if words.try_reserve_exact(word_count).is_err() {
            reservation.shrink(bytes);
            return None;
        }
        let extra = (words.capacity() - word_count) * size_of::<u64>();
        if reservation.try_grow(extra).is_err() {
            reservation.shrink(bytes);
            return None;
        }
        words.resize(word_count, 0);
        downcast_integer_array!(values => {
            for value in values.iter().flatten() {
                let index = (value as u64).wrapping_sub(offset);
                if index >= key_count {
                    reservation.shrink(bytes + extra);
                    return None;
                }
                words[(index / 64) as usize] |= 1 << (index % 64);
            }
        }
        _ => unreachable!("integer type checked above"));
        Some(Self {
            offset,
            key_count,
            words,
            data_type: values.data_type().clone(),
        })
    }

    pub(super) fn size(&self) -> usize {
        self.words.capacity() * size_of::<u64>()
    }

    #[inline]
    fn contains(&self, value: u64) -> bool {
        // As in ArrayMap, wrapping subtraction also handles signed ranges
        // crossing zero and values below the build minimum.
        let index = value.wrapping_sub(self.offset);
        index < self.key_count
            && self.words[(index / 64) as usize] & (1 << (index % 64)) != 0
    }
}

/// Each probe stream independently measures additional skipped lookups.
/// Windows end at batch boundaries after at least `sample_rows` input rows,
/// including NULLs. Unprofitable windows pause for 10 to 128 windows of input
/// rows, doubling the pause until a profitable sample resets it.
#[derive(Debug)]
pub(super) struct PrefilterState {
    rows_observed: usize,
    rows_pruned: usize,
    rows_to_skip: usize,
    pause_rows: usize,
    sample_rows: usize,
    min_pruning_ratio: f64,
}

impl PrefilterState {
    fn new(sample_rows: usize, min_pruning_ratio: f64) -> Self {
        Self {
            rows_observed: 0,
            rows_pruned: 0,
            rows_to_skip: 0,
            pause_rows: sample_rows.saturating_mul(10),
            sample_rows,
            min_pruning_ratio,
        }
    }

    pub(super) fn try_from_config(config: &ExecutionOptions) -> Result<Option<Self>> {
        if !config.enable_join_integer_prefilter {
            return Ok(None);
        }
        let ratio = config.join_integer_prefilter_min_pruning_ratio;
        if !(0.0..=1.0).contains(&ratio) {
            return config_err!(
                "join_integer_prefilter_min_pruning_ratio must be finite and in [0, 1]"
            );
        }
        Ok(Some(Self::new(
            config.join_integer_prefilter_sample_rows.get(),
            ratio,
        )))
    }

    /// Call before reserving or constructing a lookup mask. A paused batch
    /// advances only the cooldown and keeps the original validity mask.
    pub(super) fn should_filter(&mut self, rows: usize) -> bool {
        if self.rows_to_skip > 0 {
            self.rows_to_skip = self.rows_to_skip.saturating_sub(rows);
            return false;
        }
        rows > 0
    }

    /// Upper bound for simultaneously live membership and combined masks.
    /// A bitmap union with differing bit offsets may double its allocation
    /// when appending a partial word. No union allocation is needed without NULLs.
    pub(super) fn mask_memory_size(rows: usize, has_validity: bool) -> Option<usize> {
        rows.div_ceil(8)
            .checked_next_multiple_of(64)?
            .checked_mul(if has_validity { 3 } else { 1 })
    }

    /// Returns a membership mask that retains NULLs. The caller combines it
    /// with the original validity and counts excluded rows with `null_count`.
    pub(super) fn filter(
        &mut self,
        filter: &IntegerPrefilter,
        values: &dyn Array,
    ) -> Option<NullBuffer> {
        if values.data_type() != &filter.data_type {
            return None;
        }
        if values.null_count() == values.len() {
            self.record_sample(values.len(), 0);
            return None;
        }
        let mask = downcast_integer_array!(values => {
            if values.null_count() == 0 {
                let values = values.values();
                BooleanBuffer::collect_bool(values.len(), |i| {
                    filter.contains(values[i] as u64)
                })
            } else {
                BooleanBuffer::collect_bool(values.len(), |i| {
                    values.is_null(i) || filter.contains(values.value(i) as u64)
                })
            }
        }
        _ => return None);
        let mask = NullBuffer::new(mask);
        let pruned = mask.null_count();
        self.record_sample(values.len(), pruned);
        (pruned > 0).then_some(mask)
    }

    fn record_sample(&mut self, rows: usize, pruned: usize) {
        self.rows_observed = self.rows_observed.saturating_add(rows);
        self.rows_pruned = self.rows_pruned.saturating_add(pruned);
        if self.rows_observed >= self.sample_rows {
            if (self.rows_pruned as f64 / self.rows_observed as f64)
                <= self.min_pruning_ratio
            {
                self.rows_to_skip = self.pause_rows;
                self.pause_rows = self
                    .pause_rows
                    .saturating_mul(2)
                    .min(self.sample_rows.saturating_mul(128));
            } else {
                self.pause_rows = self.sample_rows.saturating_mul(10);
            }
            self.rows_observed = 0;
            self.rows_pruned = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array,
        UInt32Array, UInt64Array,
    };
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };
    use std::sync::Arc;

    fn reservation(limit: usize) -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        MemoryConsumer::new("integer prefilter test").register(&pool)
    }

    #[test]
    fn membership_and_integer_boundaries() {
        macro_rules! check {
            ($array:ty, $scalar:ident, $min:expr) => {{
                let min = $min;
                let build =
                    <$array>::from(vec![Some(min), Some(min + 2), Some(min), None]);
                let memory = reservation(8);
                let filter = IntegerPrefilter::try_new(
                    &build,
                    (
                        &ScalarValue::$scalar(Some(min)),
                        &ScalarValue::$scalar(Some(min + 2)),
                    ),
                    3,
                    &memory,
                )
                .unwrap();
                assert_eq!(memory.size(), 8);
                assert!(!filter.contains((min as u64).wrapping_sub(1)));
                let probe = <$array>::from(vec![
                    None,
                    Some(min),
                    Some(min + 1),
                    Some(min + 2),
                    Some(min + 3),
                ]);
                let mask = PrefilterState::new(10, 0.5)
                    .filter(&filter, &probe)
                    .unwrap();
                assert_eq!(mask.null_count(), 2);
                assert_eq!(
                    mask.iter().collect::<Vec<_>>(),
                    vec![true, true, false, true, false]
                );
            }};
        }
        check!(Int8Array, Int8, i8::MIN);
        check!(Int16Array, Int16, i16::MIN);
        check!(Int32Array, Int32, -1_i32);
        check!(Int64Array, Int64, i64::MIN);
        check!(Int64Array, Int64, i64::MAX - 3);
        check!(UInt8Array, UInt8, 0_u8);
        check!(UInt16Array, UInt16, 0_u16);
        check!(UInt32Array, UInt32, 0_u32);
        check!(UInt64Array, UInt64, u64::MAX - 3);
    }

    #[test]
    fn mismatched_probe_type_is_not_filtered() {
        let memory = reservation(8);
        let filter = IntegerPrefilter::try_new(
            &Int32Array::from(vec![1]),
            (&ScalarValue::Int32(Some(1)), &ScalarValue::Int32(Some(1))),
            1,
            &memory,
        )
        .unwrap();
        assert_eq!(
            PrefilterState::new(4, 0.5).filter(&filter, &UInt64Array::from(vec![1, 2])),
            None
        );
    }

    #[test]
    fn range_overflow() {
        let build = Int64Array::from(vec![i64::MIN, i64::MAX]);
        let memory = reservation(1024);
        assert!(
            IntegerPrefilter::try_new(
                &build,
                (
                    &ScalarValue::Int64(Some(i64::MIN)),
                    &ScalarValue::Int64(Some(i64::MAX))
                ),
                u64::MAX,
                &memory
            )
            .is_none()
        );
        assert_eq!(memory.size(), 0);
        let build = UInt64Array::from(vec![0, u64::MAX]);
        assert!(
            IntegerPrefilter::try_new(
                &build,
                (
                    &ScalarValue::UInt64(Some(0)),
                    &ScalarValue::UInt64(Some(u64::MAX))
                ),
                u64::MAX,
                &memory
            )
            .is_none()
        );
    }

    #[test]
    fn bitmap_word_boundaries() {
        let build = Int32Array::from(vec![-1, 62, 63, 64]);
        let memory = reservation(16);
        let filter = IntegerPrefilter::try_new(
            &build,
            (&ScalarValue::Int32(Some(-1)), &ScalarValue::Int32(Some(64))),
            66,
            &memory,
        )
        .unwrap();
        assert_eq!(filter.size(), 16);
        assert_eq!(memory.size(), filter.size());
        for key in -2_i32..=65 {
            assert_eq!(filter.contains(key as u64), [-1, 62, 63, 64].contains(&key));
        }
    }

    #[test]
    fn sampling_counts_nulls_and_pauses_at_threshold() {
        let memory = reservation(8);
        let filter = IntegerPrefilter::try_new(
            &Int32Array::from(vec![1]),
            (&ScalarValue::Int32(Some(1)), &ScalarValue::Int32(Some(1))),
            1,
            &memory,
        )
        .unwrap();
        let mut state = PrefilterState::new(8, 0.5);
        let values = Int32Array::from(vec![Some(2), Some(2), None, None]);
        for _ in 0..2 {
            assert!(state.should_filter(values.len()));
            let mask = state.filter(&filter, &values);
            assert_eq!(mask.as_ref().unwrap().null_count(), 2);
            assert_eq!(
                mask.unwrap().iter().collect::<Vec<_>>(),
                vec![false, false, true, true]
            );
        }
        // Four misses out of eight input rows reaches, but does not exceed,
        // the threshold. The two batches finish one window despite the NULLs.
        assert_eq!(state.rows_to_skip, 80);
        assert!(!state.should_filter(79));
        assert!(!state.should_filter(2));
        assert!(state.should_filter(1));
        assert_eq!(state.rows_observed, 0);
        assert_eq!(state.rows_pruned, 0);

        // The all-NULL fast path must also consume the sampling window.
        let mut state = PrefilterState::new(4, 0.5);
        assert!(!state.should_filter(0));
        let nulls = Int32Array::from(vec![None; 4]);
        assert!(state.should_filter(nulls.len()));
        assert_eq!(state.filter(&filter, &nulls), None);
        assert_eq!(state.rows_to_skip, 40);
    }

    #[test]
    fn sampling_backoff_caps_and_resets() {
        let mut state = PrefilterState::new(4, 0.5);
        for multiplier in [10, 20, 40, 80, 128, 128] {
            assert!(state.should_filter(4));
            state.record_sample(4, 0);
            assert_eq!(state.rows_to_skip, 4 * multiplier);
            assert!(!state.should_filter(4 * multiplier));
        }
        assert!(state.should_filter(4));
        state.record_sample(4, 3);
        assert_eq!(state.rows_to_skip, 0);
        assert_eq!(state.pause_rows, 40);
        state.record_sample(4, 0);
        assert_eq!(state.rows_to_skip, 40);

        // Threshold endpoints and saturating row arithmetic.
        let mut state = PrefilterState::new(1, 0.0);
        state.record_sample(1, 0);
        assert_eq!(state.rows_to_skip, 10);
        let mut state = PrefilterState::new(1, 1.0);
        state.record_sample(1, 1);
        assert_eq!(state.rows_to_skip, 10);
        let mut state = PrefilterState::new(usize::MAX, 0.5);
        state.record_sample(usize::MAX - 1, 0);
        state.record_sample(4, 0);
        assert_eq!(state.rows_to_skip, usize::MAX);
        assert_eq!(state.pause_rows, usize::MAX);
        assert!(!state.should_filter(usize::MAX));
        state.record_sample(usize::MAX - 1, usize::MAX - 1);
        state.record_sample(4, 4);
        assert_eq!(state.rows_to_skip, 0);
    }

    #[test]
    fn sliced_validity_mask_memory() {
        // A trailing partial word forces Arrow's differently aligned bitmap
        // union to grow. Account for both allocations while they coexist.
        let rows = 8193;
        let values = Int32Array::from_iter(
            (0..=rows).map(|i| (i % 3 != 0).then_some((i % 2) as i32)),
        )
        .slice(1, rows);
        let memory = reservation(8);
        let filter = IntegerPrefilter::try_new(
            &Int32Array::from(vec![0]),
            (&ScalarValue::Int32(Some(0)), &ScalarValue::Int32(Some(0))),
            1,
            &memory,
        )
        .unwrap();
        let mask = PrefilterState::new(rows, 0.5)
            .filter(&filter, &values)
            .unwrap();
        let pruned = mask.null_count();
        let lookup = NullBuffer::union(values.nulls(), Some(&mask)).unwrap();
        let peak = mask.inner().inner().capacity() + lookup.inner().inner().capacity();
        assert!(peak <= PrefilterState::mask_memory_size(rows, true).unwrap());
        assert_eq!(pruned, values.iter().filter(|v| *v == Some(1)).count());
        assert_eq!(
            lookup.iter().collect::<Vec<_>>(),
            values.iter().map(|v| v == Some(0)).collect::<Vec<_>>()
        );
    }
}
