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

use arrow::array::Array;
use arrow::array::downcast_integer_array;
use arrow::buffer::{BooleanBuffer, NullBuffer};
use datafusion_common::config::ExecutionOptions;
use datafusion_common::{NullEquality, Result, ScalarValue, config_err};
use datafusion_execution::memory_pool::MemoryReservation;

/// One bit per integer in the build key range, shared by all probe streams.
#[derive(Debug)]
pub(super) struct IntegerPrefilter {
    bits: Vec<u8>,
    min: u64,
    range: usize,
    null_matches: bool,
}

impl IntegerPrefilter {
    /// Uses already computed build bounds. Failure to reserve this optional
    /// allocation leaves the normal hash join available.
    pub(super) fn try_new(
        values: &dyn Array,
        bounds: (&ScalarValue, &ScalarValue),
        max_key_range: usize,
        null_equality: NullEquality,
        reservation: &MemoryReservation,
    ) -> Option<Self> {
        if !values.data_type().is_integer() {
            return None;
        }
        let min = integer_value(bounds.0)?;
        let max = integer_value(bounds.1)?;
        let range = usize::try_from(max - min).ok()?.checked_add(1)?;
        if range > max_key_range {
            return None;
        }
        let bytes = range.div_ceil(8);
        reservation.try_grow(bytes).ok()?;
        let mut bits = Vec::new();
        if bits.try_reserve_exact(bytes).is_err() {
            reservation.shrink(bytes);
            return None;
        }
        let extra = bits.capacity() - bytes;
        if reservation.try_grow(extra).is_err() {
            reservation.shrink(bytes);
            return None;
        }
        bits.resize(bytes, 0);
        downcast_integer_array!(values => {
            for value in values.iter().flatten() {
                let offset = (i128::from(value) - min) as usize;
                bits[offset / 8] |= 1 << (offset % 8);
            }
        }
        _ => unreachable!("integer type checked above"));
        Some(Self {
            bits,
            min: min as u64,
            range,
            null_matches: null_equality == NullEquality::NullEqualsNull
                && values.null_count() > 0,
        })
    }

    pub(super) fn size(&self) -> usize {
        self.bits.capacity()
    }

    #[inline]
    fn contains(&self, value: u64) -> bool {
        // Build range validation uses i128. Here, same-type integer keys can
        // use wrapping offsets: values below min wrap beyond the valid range,
        // including signed keys whose range crosses zero.
        let offset = value.wrapping_sub(self.min);
        offset < self.range as u64
            && self.bits[offset as usize / 8] & (1 << (offset as usize % 8)) != 0
    }
}

fn integer_value(value: &ScalarValue) -> Option<i128> {
    match value {
        ScalarValue::Int8(v) => v.map(i128::from),
        ScalarValue::Int16(v) => v.map(i128::from),
        ScalarValue::Int32(v) => v.map(i128::from),
        ScalarValue::Int64(v) => v.map(i128::from),
        ScalarValue::UInt8(v) => v.map(i128::from),
        ScalarValue::UInt16(v) => v.map(i128::from),
        ScalarValue::UInt32(v) => v.map(i128::from),
        ScalarValue::UInt64(v) => v.map(i128::from),
        _ => None,
    }
}

/// Each probe stream independently measures additional skipped lookups.
/// Windows end at batch boundaries after at least `sample_rows` input rows,
/// including already invalid keys. Savings only count eligible lookups.
/// Unprofitable windows pause filtering for eight windows of input rows,
/// then retry to accommodate changing probe distributions.
#[derive(Debug)]
pub(super) struct PrefilterState {
    sample_rows: usize,
    min_pruning_ratio: f64,
    scanned_rows: usize,
    eligible_rows: usize,
    pruned_rows: usize,
    skip_rows: usize,
}

impl PrefilterState {
    fn new(sample_rows: usize, min_pruning_ratio: f64) -> Self {
        Self {
            sample_rows,
            min_pruning_ratio,
            scanned_rows: 0,
            eligible_rows: 0,
            pruned_rows: 0,
            skip_rows: 0,
        }
    }

    pub(super) fn try_from_config(config: &ExecutionOptions) -> Result<Option<Self>> {
        if !config.enable_join_integer_prefilter {
            return Ok(None);
        }
        if config.join_integer_prefilter_max_key_range == 0 {
            return config_err!(
                "join_integer_prefilter_max_key_range must be greater than zero"
            );
        }
        if config.join_integer_prefilter_sample_rows == 0 {
            return config_err!(
                "join_integer_prefilter_sample_rows must be greater than zero"
            );
        }
        let ratio = config.join_integer_prefilter_min_pruning_ratio;
        if !(0.0..=1.0).contains(&ratio) {
            return config_err!(
                "join_integer_prefilter_min_pruning_ratio must be finite and in [0, 1]"
            );
        }
        Ok(Some(Self::new(
            config.join_integer_prefilter_sample_rows,
            ratio,
        )))
    }

    /// Upper bound for simultaneously live membership and combined masks.
    /// A bitmap union with differing bit offsets may double its allocation
    /// when appending a partial word. No union allocation is needed without NULLs.
    pub(super) fn mask_memory_size(rows: usize, has_validity: bool) -> Option<usize> {
        rows.div_ceil(8)
            .checked_next_multiple_of(64)?
            .checked_mul(if has_validity { 3 } else { 1 })
    }

    /// Returns the additional lookup mask and number of newly excluded rows.
    /// Already invalid keys stay set here: the caller combines this mask with
    /// the original validity without counting those keys as prefilter savings.
    pub(super) fn filter(
        &mut self,
        filter: &IntegerPrefilter,
        values: &dyn Array,
        valid_keys: Option<&NullBuffer>,
    ) -> (Option<NullBuffer>, usize) {
        if self.skip_rows > 0 {
            self.skip_rows = self.skip_rows.saturating_sub(values.len());
            return (None, 0);
        }
        let eligible_rows = values.len() - valid_keys.map_or(0, NullBuffer::null_count);
        if eligible_rows == 0 {
            self.record_sample(values.len(), 0, 0);
            return (None, 0);
        }
        let mask = downcast_integer_array!(values => {
            if values.null_count() == 0 && valid_keys.is_none() {
                let values = values.values();
                BooleanBuffer::collect_bool(values.len(), |i| {
                    filter.contains(values[i] as u64)
                })
            } else {
                BooleanBuffer::collect_bool(values.len(), |i| {
                    if valid_keys.is_some_and(|valid| valid.is_null(i)) {
                        return true;
                    }
                    if values.is_null(i) {
                        filter.null_matches
                    } else {
                        filter.contains(values.value(i) as u64)
                    }
                })
            }
        }
        _ => return (None, 0));
        let mask = NullBuffer::new(mask);
        let pruned = mask.null_count();
        self.record_sample(values.len(), eligible_rows, pruned);
        ((pruned > 0).then_some(mask), pruned)
    }

    fn record_sample(
        &mut self,
        scanned_rows: usize,
        eligible_rows: usize,
        pruned: usize,
    ) {
        self.scanned_rows = self.scanned_rows.saturating_add(scanned_rows);
        self.eligible_rows = self.eligible_rows.saturating_add(eligible_rows);
        self.pruned_rows = self.pruned_rows.saturating_add(pruned);
        if self.scanned_rows >= self.sample_rows {
            if self.eligible_rows == 0
                || (self.pruned_rows as f64)
                    < self.min_pruning_ratio * self.eligible_rows as f64
            {
                self.skip_rows = self.sample_rows.saturating_mul(8);
            }
            self.scanned_rows = 0;
            self.eligible_rows = 0;
            self.pruned_rows = 0;
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
                let memory = reservation(1);
                let filter = IntegerPrefilter::try_new(
                    &build,
                    (
                        &ScalarValue::$scalar(Some(min)),
                        &ScalarValue::$scalar(Some(min + 2)),
                    ),
                    3,
                    NullEquality::NullEqualsNothing,
                    &memory,
                )
                .unwrap();
                assert_eq!(memory.size(), 1);
                assert!(!filter.contains((min as u64).wrapping_sub(1)));
                let probe = <$array>::from(vec![
                    None,
                    Some(min),
                    Some(min + 1),
                    Some(min + 2),
                    Some(min + 3),
                ]);
                let valid = probe.nulls();
                let (mask, pruned) =
                    PrefilterState::new(10, 0.5).filter(&filter, &probe, valid);
                assert_eq!(pruned, 2);
                let mask = NullBuffer::union(valid, mask.as_ref()).unwrap();
                assert_eq!(
                    mask.iter().collect::<Vec<_>>(),
                    vec![false, true, false, true, false]
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
    fn null_equals_null() {
        for has_null in [false, true] {
            let build = if has_null {
                Int32Array::from(vec![Some(1), None])
            } else {
                Int32Array::from(vec![1])
            };
            let memory = reservation(1);
            let filter = IntegerPrefilter::try_new(
                &build,
                (&ScalarValue::Int32(Some(1)), &ScalarValue::Int32(Some(1))),
                1,
                NullEquality::NullEqualsNull,
                &memory,
            )
            .unwrap();
            let probe = Int32Array::from(vec![None, Some(1), Some(2)]);
            let (mask, count) =
                PrefilterState::new(10, 0.5).filter(&filter, &probe, None);
            assert_eq!(count, if has_null { 1 } else { 2 });
            assert_eq!(
                mask.unwrap().iter().collect::<Vec<_>>(),
                vec![has_null, true, false]
            );
        }
    }

    #[test]
    fn range_and_memory_fallback() {
        let build = Int64Array::from(vec![i64::MIN, i64::MAX]);
        let memory = reservation(1024);
        assert!(
            IntegerPrefilter::try_new(
                &build,
                (
                    &ScalarValue::Int64(Some(i64::MIN)),
                    &ScalarValue::Int64(Some(i64::MAX))
                ),
                usize::MAX,
                NullEquality::NullEqualsNothing,
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
                usize::MAX,
                NullEquality::NullEqualsNothing,
                &memory
            )
            .is_none()
        );
        let build = Int32Array::from(vec![0, 8]);
        let bounds = (ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(8)));
        assert!(
            IntegerPrefilter::try_new(
                &build,
                (&bounds.0, &bounds.1),
                8,
                NullEquality::NullEqualsNothing,
                &memory
            )
            .is_none()
        );
        let memory = reservation(1);
        memory.try_grow(1).unwrap();
        assert!(
            IntegerPrefilter::try_new(
                &build,
                (&bounds.0, &bounds.1),
                9,
                NullEquality::NullEqualsNothing,
                &memory
            )
            .is_none()
        );
        assert_eq!(memory.size(), 1, "existing reservations must be preserved");
    }

    #[test]
    fn sampling_pauses_retries_and_ignores_invalid_keys() {
        let memory = reservation(1);
        let filter = IntegerPrefilter::try_new(
            &Int32Array::from(vec![1]),
            (&ScalarValue::Int32(Some(1)), &ScalarValue::Int32(Some(1))),
            1,
            NullEquality::NullEqualsNothing,
            &memory,
        )
        .unwrap();
        let mut state = PrefilterState::new(8, 0.5);
        let hits = Int32Array::from(vec![Some(1), None, None, None]);
        // Both batches consume the scan budget even though only two keys are
        // eligible for lookup. NULLs must not prolong an unprofitable sample.
        assert_eq!(state.filter(&filter, &hits, hits.nulls()).1, 0);
        assert_eq!(state.skip_rows, 0);
        state.filter(&filter, &hits, hits.nulls());
        assert_eq!(state.skip_rows, 64);
        let misses = Int32Array::from(vec![2; 4]);
        for _ in 0..16 {
            assert_eq!(state.filter(&filter, &misses, None).1, 0);
        }
        // Savings use eligible lookups, not scanned rows: eliminating both
        // eligible keys is profitable even with six NULLs in the window.
        let sparse_misses = Int32Array::from(vec![Some(2), None, None, None]);
        for _ in 0..2 {
            let (mask, pruned) =
                state.filter(&filter, &sparse_misses, sparse_misses.nulls());
            assert_eq!(pruned, 1);
            assert_eq!(
                mask.unwrap().iter().collect::<Vec<_>>(),
                vec![false, true, true, true]
            );
        }
        assert_eq!(state.skip_rows, 0);
        // Re-evaluate even after a profitable window.
        assert!(
            state
                .filter(&filter, &Int32Array::from(vec![1; 8]), None)
                .0
                .is_none()
        );
        assert_eq!(state.skip_rows, 64);
    }

    #[test]
    fn sampling_without_eligible_keys_pauses() {
        let memory = reservation(1);
        let filter = IntegerPrefilter::try_new(
            &Int32Array::from(vec![1]),
            (&ScalarValue::Int32(Some(1)), &ScalarValue::Int32(Some(1))),
            1,
            NullEquality::NullEqualsNothing,
            &memory,
        )
        .unwrap();
        let mut state = PrefilterState::new(4, 0.5);
        let empty = Int32Array::from(Vec::<i32>::new());
        assert_eq!(state.filter(&filter, &empty, None), (None, 0));
        assert_eq!(state.skip_rows, 0);
        let nulls = Int32Array::from(vec![None; 4]);
        assert_eq!(state.filter(&filter, &nulls, nulls.nulls()), (None, 0));
        assert_eq!(state.skip_rows, 32);
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
        let memory = reservation(1);
        let filter = IntegerPrefilter::try_new(
            &Int32Array::from(vec![0]),
            (&ScalarValue::Int32(Some(0)), &ScalarValue::Int32(Some(0))),
            1,
            NullEquality::NullEqualsNothing,
            &memory,
        )
        .unwrap();
        let (mask, pruned) =
            PrefilterState::new(rows, 0.5).filter(&filter, &values, values.nulls());
        let mask = mask.unwrap();
        let lookup = NullBuffer::union(values.nulls(), Some(&mask)).unwrap();
        let peak = mask.inner().inner().capacity() + lookup.inner().inner().capacity();
        assert!(peak <= PrefilterState::mask_memory_size(rows, true).unwrap());
        assert_eq!(pruned, values.iter().filter(|v| *v == Some(1)).count());
        assert_eq!(
            lookup.iter().collect::<Vec<_>>(),
            values.iter().map(|v| v == Some(0)).collect::<Vec<_>>()
        );
    }

    #[test]
    fn validates_configuration_only_when_enabled() {
        let mut config = ExecutionOptions {
            join_integer_prefilter_sample_rows: 0,
            ..Default::default()
        };
        assert!(PrefilterState::try_from_config(&config).unwrap().is_none());
        config.enable_join_integer_prefilter = true;
        assert!(PrefilterState::try_from_config(&config).is_err());
        config.join_integer_prefilter_sample_rows = 1;
        config.join_integer_prefilter_max_key_range = 0;
        assert!(PrefilterState::try_from_config(&config).is_err());
        config.join_integer_prefilter_max_key_range = 1;
        for ratio in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
            config.join_integer_prefilter_min_pruning_ratio = ratio;
            assert!(PrefilterState::try_from_config(&config).is_err());
        }
        for ratio in [0.0, 0.5, 1.0] {
            config.join_integer_prefilter_min_pruning_ratio = ratio;
            assert!(PrefilterState::try_from_config(&config).unwrap().is_some());
        }
    }
}
