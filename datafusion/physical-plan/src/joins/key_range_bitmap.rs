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

//! Bitmap over a hash join build side's key range, for container pruning.

use arrow::array::{Array, ArrayRef, BooleanArray, downcast_integer_array};
use arrow::buffer::{BooleanBuffer, MutableBuffer};
use arrow::util::bit_util;
use datafusion_common::ScalarValue;
use num_traits::AsPrimitive;

use crate::joins::array_map::ArrayMap;

/// Largest bitmap, in bits (128 KiB), which bounds what one build side can hold.
const MAX_BUCKETS: u64 = 1 << 20;

/// Which parts of a build side's key range hold at least one key.
///
/// Keys map order-preservingly onto buckets spanning `[min, max]`, one bit each,
/// so "can a container holding `[lo, hi]` match?" is a scan of the bits spanning
/// `[lo, hi]`: all clear proves no build key lies in it.
///
/// Below, keys 3 to 97 over eight buckets: `offset` is 3, `span` is 97 - 3, and
/// eight buckets need `shift` = 4, so each covers `1 << 4` keys starting at
/// `offset`. Only buckets 0 to `span >> shift` can hold a key. Real bitmaps run
/// to `MAX_BUCKETS`; eight is what fits on a line.
///
/// ```text
///   keys      3  12              42                        97
///             v  v               v                         v
///   bucket   |   0   |   1   |   2   |   3   |   4   |   5   |   6   |   7   |
///   covers      3-18   19-34   35-50   51-66   67-82   83-98  unused  unused
///   bit          1       0       1       0       0       1       0       0
/// ```
///
/// A container is kept when every bucket its `[min, max]` touches has a bit set.
#[derive(Debug)]
pub struct KeyRangeBitmap {
    /// Smallest key, in the `u64` ordering the join's array map uses.
    offset: u64,
    /// `max - min`: a key is in range iff `key.wrapping_sub(offset) <= span`.
    span: u64,
    /// Each bucket covers `1 << shift` keys.
    shift: u32,
    /// One bit per bucket.
    bits: BooleanBuffer,
}

impl KeyRangeBitmap {
    /// Maps `array`'s non-null values, which must lie within `[min, max]`.
    ///
    /// `None` when the key type has no `u64` ordering, or when the bitmap could
    /// not exclude anything the `[min, max]` bounds predicate does not already.
    pub fn try_new(
        array: &ArrayRef,
        min: &ScalarValue,
        max: &ScalarValue,
        distinct_keys: usize,
    ) -> Option<Self> {
        if !ArrayMap::is_supported_type(array.data_type()) {
            return None;
        }
        let offset = ArrayMap::key_to_u64(min)?;
        let span = ArrayMap::key_to_u64(max)?.wrapping_sub(offset);

        // One bucket per key value where the range allows it, so the bitmap is
        // exact; otherwise as many as the cap permits.
        let buckets = span.saturating_add(1).min(MAX_BUCKETS).next_power_of_two();

        // Use a heuristic to bail-out for the common shape which renders each bucket
        // set (contiguous surrogate keys, a date range) before actually computing it.
        if (distinct_keys as u64).saturating_mul(2) > span.min(buckets) {
            return None;
        }

        let shift = (0u32..63).find(|s| (span >> s) < buckets).unwrap_or(63);

        let bits = bucket_bits(array, offset, shift, buckets);

        // Every bucket set means nothing can be excluded.
        let reachable = ((span >> shift) + 1) as usize;
        bits.slice(0, reachable).has_false().then_some(Self {
            offset,
            span,
            shift,
            bits,
        })
    }

    /// One Boolean per `[min[i], max[i]]` container interval: `false` proves no
    /// build key lies in that container, `None` where a bound is absent or the
    /// statistics are not integer keys.
    pub fn may_contain_ranges(&self, min: &dyn Array, max: &dyn Array) -> BooleanArray {
        // Both bounds must be the same integer type for the tuple pattern to match.
        downcast_integer_array!(
            (min, max) => {
                min.iter()
                    .zip(max.iter())
                    .map(|bounds| match bounds {
                        (Some(lo), Some(hi)) => {
                            Some(self.may_contain_range(lo.as_(), hi.as_()))
                        }
                        _ => None,
                    })
                    .collect()
            }
            _ => BooleanArray::new_null(min.len()),
        )
    }

    /// Might a container spanning `[lo, hi]` hold a build key? `false` proves it
    /// cannot. Bounds use the same `u64` key ordering as the join's array map.
    pub fn may_contain_range(&self, lo: u64, hi: u64) -> bool {
        // Offsets from the minimum; anything outside `[min, max]` wraps to
        // something huge, which the clamping below turns into "past the end".
        let lo_off = lo.wrapping_sub(self.offset);
        let hi_off = hi.wrapping_sub(self.offset);
        if lo_off > self.span && hi_off > self.span && lo_off <= hi_off {
            return false;
        }
        let lo_b = if lo_off > self.span {
            0
        } else {
            lo_off >> self.shift
        };
        let hi_b = (hi_off.min(self.span) >> self.shift).max(lo_b);
        self.bits
            .slice(lo_b as usize, (hi_b - lo_b + 1) as usize)
            .has_true()
    }

    /// Bytes held, for the build-side memory reservation.
    pub fn size(&self) -> usize {
        self.bits.inner().capacity()
    }

    /// Buckets set and buckets total, for `EXPLAIN`.
    pub fn fill_stats(&self) -> (usize, usize) {
        (self.bits.count_set_bits(), self.bits.len())
    }
}

/// One bit per bucket, set for each bucket holding a non-null key of `keys`.
fn bucket_bits(keys: &dyn Array, offset: u64, shift: u32, buckets: u64) -> BooleanBuffer {
    let mut words = MutableBuffer::new_null(buckets as usize);
    downcast_integer_array!(
        keys => {
            let bytes = words.as_slice_mut();
            let mut set = |key: u64| {
                let bucket = (key.wrapping_sub(offset) >> shift).min(buckets - 1);
                bit_util::set_bit(bytes, bucket as usize);
            };
            if keys.null_count() == 0 {
                keys.values().iter().for_each(|key| set((*key).as_()));
            } else {
                keys.iter().flatten().for_each(|key| set(key.as_()));
            }
        }
        _ => unreachable!("guarded by ArrayMap::is_supported_type"),
    );
    BooleanBuffer::new(words.into(), 0, buckets as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use std::sync::Arc;

    fn bitmap(keys: &[i64]) -> Option<KeyRangeBitmap> {
        let a: ArrayRef = Arc::new(Int64Array::from(keys.to_vec()));
        KeyRangeBitmap::try_new(
            &a,
            &ScalarValue::Int64(Some(*keys.iter().min().unwrap())),
            &ScalarValue::Int64(Some(*keys.iter().max().unwrap())),
            keys.iter().collect::<std::collections::HashSet<_>>().len(),
        )
    }
    fn may(b: &KeyRangeBitmap, lo: i64, hi: i64) -> bool {
        b.may_contain_range(lo as u64, hi as u64)
    }

    #[test]
    fn prunes_gaps_and_never_drops_a_match() {
        let keys: Vec<i64> = (0..2_000_000).filter(|k| k % 10000 < 200).collect();
        let s = bitmap(&keys).expect("not saturated");
        let mut kept = 0;
        for i in 0..2000i64 {
            let (lo, hi) = (1000 * i, 1000 * i + 999);
            let truth = keys
                .binary_search(&lo)
                .map_or_else(|p| keys.get(p).is_some_and(|k| *k <= hi), |_| true);
            let got = may(&s, lo, hi);
            assert!(
                !truth || got,
                "dropped a container holding a key: {lo}..{hi}"
            );
            kept += got as usize;
        }
        assert!(kept <= 220, "expected ~200 kept, got {kept}");
    }

    #[test]
    fn contiguous_keys_no_bitmap() {
        assert!(bitmap(&(0..5000i64).collect::<Vec<_>>()).is_none());
    }

    #[test]
    fn test_bitmap_range_probing() {
        let s = bitmap(&[0, 1_000_000]).expect("not saturated");
        assert!(!may(&s, -500, -10));
        assert!(!may(&s, 2_000_000, 3_000_000));
        assert!(may(&s, -500, 10));

        let s = bitmap(&[-1_000_000, -5, 0, 5, 1_000_000]).expect("not saturated");
        assert!(may(&s, -10, 10));
        assert!(!may(&s, -900_000, -800_000));

        // The widest possible span must not overflow the bucket sizing.
        let s = bitmap(&[i64::MIN, 0, i64::MAX]).expect("not saturated");
        assert!(may(&s, -1, 1));
        assert!(!may(&s, 1 << 50, 1 << 51));
    }
}
