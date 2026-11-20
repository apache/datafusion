// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Bitmap over a hash join build side's key range, for container pruning.

use arrow::array::{Array, ArrayRef, BooleanArray, downcast_integer_array};
use arrow::buffer::{BooleanBuffer, MutableBuffer};
use arrow::util::bit_util;
use datafusion_common::ScalarValue;
use num_traits::AsPrimitive;

use crate::joins::array_map::ArrayMap;

/// Largest bitmap, in bits (128 KiB), which bounds what one build side can hold.
const MAX_BUCKETS: u64 = 1 << 20;

/// One build partition's contribution to [`KeyRangeBitmap::union`].
pub(crate) enum KeyRangePart<'a> {
    /// The partition's own bitmap.
    Bitmap(&'a KeyRangeBitmap),
    /// The partition's build keys, which must lie within its `[min, max]`.
    Keys(&'a dyn Array),
    /// The partition has no bitmap, so any key in `[min, max]` may be present.
    Range {
        min: &'a ScalarValue,
        max: &'a ScalarValue,
    },
}

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

    /// One bitmap over `[min, max]` marking every bucket that any of `parts`
    /// may hold a key in. `[min, max]` must cover every part.
    ///
    /// Coarser than the parts' own bitmaps where its buckets are wider, but
    /// never clears a bucket a part may hold a key in. `None` when the key
    /// type has no `u64` ordering, or when nothing can be excluded.
    pub(crate) fn union(
        min: &ScalarValue,
        max: &ScalarValue,
        parts: &[KeyRangePart<'_>],
    ) -> Option<Self> {
        let offset = ArrayMap::key_to_u64(min)?;
        let span = ArrayMap::key_to_u64(max)?.wrapping_sub(offset);
        let buckets = span.saturating_add(1).min(MAX_BUCKETS).next_power_of_two();
        let shift = (0u32..63).find(|s| (span >> s) < buckets).unwrap_or(63);

        // One bit per bucket, in words, so a part's bitmap ORs in a word at a time.
        let mut words =
            MutableBuffer::from_len_zeroed((buckets as usize).div_ceil(64) * 8);
        let reachable = ((span >> shift) + 1) as usize;
        for part in parts {
            match part {
                KeyRangePart::Bitmap(part) => {
                    let words = words.typed_data_mut::<u64>();
                    // Every key below `part.offset` is outside `[min, max]`, so
                    // this wrapping distance is the part's true position.
                    let base = part.offset.wrapping_sub(offset);
                    let last = (part.span >> part.shift) as usize;
                    if part.shift == shift {
                        // Same bucket width: part bucket `b` covers union bucket
                        // `b + q`, and `b + q + 1` too when the part's buckets
                        // start `r` keys into a union bucket. Only the last part
                        // bucket, which may be narrower, can stop short of it.
                        let q = (base >> shift) as usize;
                        let r = base & ((1u64 << shift) - 1);
                        or_shifted(words, &part.bits, last + 1, q);
                        if r != 0 {
                            or_shifted(words, &part.bits, last, q + 1);
                            if part.bits.value(last) {
                                let lo = base + ((last as u64) << shift);
                                let hi = (base + part.span).min(span);
                                set_bit_range(
                                    words,
                                    (lo >> shift) as usize,
                                    (hi >> shift) as usize,
                                );
                            }
                        }
                    } else {
                        let width = (1u64 << part.shift) - 1;
                        for bucket in part.bits.slice(0, last + 1).set_indices() {
                            let lo = base + ((bucket as u64) << part.shift);
                            let hi = (base
                                + ((bucket as u64) << part.shift)
                                    .saturating_add(width)
                                    .min(part.span))
                            .min(span);
                            set_bit_range(
                                words,
                                (lo >> shift) as usize,
                                (hi >> shift) as usize,
                            );
                        }
                    }
                }
                KeyRangePart::Keys(keys) => {
                    if !ArrayMap::is_supported_type(keys.data_type()) {
                        return None;
                    }
                    set_bucket_bits(*keys, offset, shift, buckets, words.as_slice_mut());
                }
                KeyRangePart::Range { min, max } => {
                    let lo = ArrayMap::key_to_u64(min)?.wrapping_sub(offset);
                    let hi = ArrayMap::key_to_u64(max)?.wrapping_sub(offset).min(span);
                    if lo <= hi {
                        set_bit_range(
                            words.typed_data_mut::<u64>(),
                            (lo >> shift) as usize,
                            (hi >> shift) as usize,
                        );
                    }
                }
            }
            // Bits are only ever added, so once every reachable bucket is set
            // the remaining parts cannot make the bitmap exclude anything.
            if all_set(words.typed_data::<u64>(), reachable) {
                return None;
            }
        }

        Some(Self {
            offset,
            span,
            shift,
            bits: BooleanBuffer::new(words.into(), 0, buckets as usize),
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
    set_bucket_bits(keys, offset, shift, buckets, words.as_slice_mut());
    BooleanBuffer::new(words.into(), 0, buckets as usize)
}

/// Sets the bit of each bucket holding a non-null key of `keys` in `bytes`.
fn set_bucket_bits(
    keys: &dyn Array,
    offset: u64,
    shift: u32,
    buckets: u64,
    bytes: &mut [u8],
) {
    downcast_integer_array!(
        keys => {
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
}

/// ORs bits `0..len` of `src` into `words`, moved `by` bits further along.
fn or_shifted(words: &mut [u64], src: &BooleanBuffer, len: usize, by: usize) {
    let (skip, bit) = (by / 64, by % 64);
    let chunks = src.inner().bit_chunks(src.offset(), len);
    for (i, chunk) in chunks.iter_padded().enumerate() {
        if chunk == 0 {
            continue;
        }
        words[skip + i] |= chunk << bit;
        if bit != 0 {
            // A set bit always lands on a bucket, so it never spills past the end.
            if let Some(next) = words.get_mut(skip + i + 1) {
                *next |= chunk >> (64 - bit);
            }
        }
    }
}

/// Sets bits `from..=to` of `words`.
fn set_bit_range(words: &mut [u64], from: usize, to: usize) {
    let (first, last) = (from / 64, to / 64);
    let head = u64::MAX << (from % 64);
    let tail = u64::MAX >> (63 - to % 64);
    if first == last {
        words[first] |= head & tail;
    } else {
        words[first] |= head;
        words[first + 1..last].fill(u64::MAX);
        words[last] |= tail;
    }
}

/// Whether bits `0..len` of `words` are all set.
fn all_set(words: &[u64], len: usize) -> bool {
    let (full, rest) = (len / 64, len % 64);
    words[..full].iter().all(|word| *word == u64::MAX)
        && (rest == 0 || words[full] | (u64::MAX << rest) == u64::MAX)
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

    /// Partitions spanning negative and positive keys, one without a bitmap:
    /// the union must keep every container holding a key of any partition, and
    /// still prune the gaps no partition covers.
    #[test]
    fn union_never_drops_a_partition_key() {
        let clustered: Vec<i64> = (-1_000_000..-999_000).chain(0..1000).collect();
        let sparse: Vec<i64> = (0..400).map(|i| 500_000 + 1000 * i).collect();
        let dense = (-200_000i64, -190_000i64);
        let a = bitmap(&clustered).expect("not saturated");
        let b = bitmap(&sparse).expect("not saturated");
        let (min, max) = (
            ScalarValue::Int64(Some(-1_000_000)),
            ScalarValue::Int64(Some(899_000)),
        );
        let (dense_min, dense_max) = (
            ScalarValue::Int64(Some(dense.0)),
            ScalarValue::Int64(Some(dense.1)),
        );
        let union = KeyRangeBitmap::union(
            &min,
            &max,
            &[
                KeyRangePart::Bitmap(&a),
                KeyRangePart::Bitmap(&b),
                KeyRangePart::Range {
                    min: &dense_min,
                    max: &dense_max,
                },
            ],
        )
        .expect("not saturated");

        for key in clustered
            .iter()
            .chain(&sparse)
            .chain(&[dense.0, -195_000, dense.1])
        {
            assert!(may(&union, *key, *key), "dropped key {key}");
        }
        assert!(!may(&union, -900_000, -800_000));
        assert!(!may(&union, 100_000, 400_000));
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

    /// `(offset, span, shift, bits)`, to compare bitmaps by value.
    fn parts_of(b: &KeyRangeBitmap) -> (u64, u64, u32, &BooleanBuffer) {
        (b.offset, b.span, b.shift, &b.bits)
    }

    /// One randomly generated scenario, see [`union_fuzz`].
    fn run_union_scenario(seed: u64) {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        let mut rng = StdRng::seed_from_u64(seed);
        // Domain widths from one key per bucket to many keys per bucket, so
        // the union's buckets are sometimes exact and sometimes coarse.
        let width = [64i64, 1 << 12, 1 << 24, 1 << 40][rng.random_range(0..4)];
        let start = rng.random_range(-(1i64 << 50)..(1i64 << 50));
        let key = |rng: &mut StdRng| start + rng.random_range(0..width);
        let keys_array =
            |keys: &[i64]| -> ArrayRef { Arc::new(Int64Array::from(keys.to_vec())) };
        let scalar = |key: i64| ScalarValue::Int64(Some(key));

        enum Part {
            Keys(ArrayRef),
            Range(ScalarValue, ScalarValue),
            Bitmap(KeyRangeBitmap),
        }
        // Each part, and every key any part may hold.
        let mut parts = vec![];
        let mut all_keys = vec![];
        let mut has_bitmap = false;
        for _ in 0..rng.random_range(1..5) {
            if rng.random_bool(1.0 / 3.0) {
                // Narrow, so every key it may hold can be listed.
                let lo = key(&mut rng);
                let hi = lo + rng.random_range(0..64);
                parts.push(Part::Range(scalar(lo), scalar(hi)));
                all_keys.extend(lo..=hi);
                continue;
            }
            let keys: Vec<i64> = (0..rng.random_range(1..50))
                .map(|_| key(&mut rng))
                .collect();
            let (lo, hi) = (*keys.iter().min().unwrap(), *keys.iter().max().unwrap());
            let bitmap = rng
                .random_bool(0.5)
                .then(|| {
                    KeyRangeBitmap::try_new(
                        &keys_array(&keys),
                        &scalar(lo),
                        &scalar(hi),
                        0,
                    )
                })
                .flatten();
            parts.push(match bitmap {
                Some(bitmap) => {
                    has_bitmap = true;
                    Part::Bitmap(bitmap)
                }
                None => Part::Keys(keys_array(&keys)),
            });
            all_keys.extend(keys);
        }
        let min = ScalarValue::Int64(all_keys.iter().min().copied());
        let max = ScalarValue::Int64(all_keys.iter().max().copied());
        let context = format!(
            "seed {seed}: width {width}, {} parts, {} keys",
            parts.len(),
            all_keys.len()
        );

        let union = KeyRangeBitmap::union(
            &min,
            &max,
            &parts
                .iter()
                .map(|part| match part {
                    Part::Keys(keys) => KeyRangePart::Keys(keys.as_ref()),
                    Part::Range(min, max) => KeyRangePart::Range { min, max },
                    Part::Bitmap(bitmap) => KeyRangePart::Bitmap(bitmap),
                })
                .collect::<Vec<_>>(),
        );
        // Reference: one bitmap built straight from every key any part may hold.
        let exact = KeyRangeBitmap::try_new(&keys_array(&all_keys), &min, &max, 0);

        // Never excludes a key any part may hold.
        if let Some(union) = &union {
            for key in &all_keys {
                assert!(may(union, *key, *key), "{context}: dropped key {key}");
            }
        }
        if has_bitmap {
            // A part's bitmap can only widen buckets: every bucket the exact
            // bitmap sets, the union sets too.
            match (&union, &exact) {
                (_, None) => assert!(union.is_none(), "{context}"),
                (None, Some(_)) => {}
                (Some(union), Some(exact)) => {
                    assert_eq!(
                        (union.offset, union.span, union.shift),
                        (exact.offset, exact.span, exact.shift),
                        "{context}"
                    );
                    for bucket in exact.bits.set_indices() {
                        assert!(union.bits.value(bucket), "{context}: bucket {bucket}");
                    }
                }
            }
            // Each set bucket of a part's bitmap stands for its whole key range,
            // so the union equals the one over those ranges given as `Range`s.
            let ranges: Vec<(ScalarValue, ScalarValue)> = parts
                .iter()
                .flat_map(|part| match part {
                    Part::Bitmap(bitmap) => bitmap
                        .bits
                        .set_indices()
                        .map(|bucket| {
                            let lo = (bucket as u64) << bitmap.shift;
                            let hi = lo
                                .saturating_add((1u64 << bitmap.shift) - 1)
                                .min(bitmap.span);
                            let key =
                                |at: u64| scalar(bitmap.offset.wrapping_add(at) as i64);
                            (key(lo), key(hi))
                        })
                        .collect(),
                    _ => vec![],
                })
                .collect();
            let expanded = KeyRangeBitmap::union(
                &min,
                &max,
                &parts
                    .iter()
                    .filter_map(|part| match part {
                        Part::Keys(keys) => Some(KeyRangePart::Keys(keys.as_ref())),
                        Part::Range(min, max) => Some(KeyRangePart::Range { min, max }),
                        Part::Bitmap(_) => None,
                    })
                    .chain(
                        ranges
                            .iter()
                            .map(|(min, max)| KeyRangePart::Range { min, max }),
                    )
                    .collect::<Vec<_>>(),
            );
            assert_eq!(
                union.as_ref().map(parts_of),
                expanded.as_ref().map(parts_of),
                "{context}: bitmap parts differ from their bucket ranges"
            );
        } else {
            // Keys and ranges mark exactly the buckets their keys fall in.
            assert_eq!(
                union.as_ref().map(parts_of),
                exact.as_ref().map(parts_of),
                "{context}"
            );
        }
    }

    /// Randomized partitions against [`KeyRangeBitmap::union`]'s contract: it
    /// never excludes a key any part may hold, equals the bitmap built from all
    /// parts' keys when no part brings its own bitmap, and only widens that
    /// bitmap's buckets when one does.
    ///
    /// `DATAFUSION_KEY_RANGE_UNION_FUZZ_ITERATIONS` and
    /// `DATAFUSION_KEY_RANGE_UNION_FUZZ_SEED` select how many scenarios run and
    /// from which seed; a failure names its seed so it can be replayed.
    #[test]
    fn union_fuzz() {
        let env_u64 = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        let first_seed = env_u64("DATAFUSION_KEY_RANGE_UNION_FUZZ_SEED", 0);
        let iterations = env_u64("DATAFUSION_KEY_RANGE_UNION_FUZZ_ITERATIONS", 256);
        // `saturating_add` so a replay seed near `u64::MAX` cannot overflow.
        for seed in first_seed..first_seed.saturating_add(iterations) {
            run_union_scenario(seed);
        }
    }
}
