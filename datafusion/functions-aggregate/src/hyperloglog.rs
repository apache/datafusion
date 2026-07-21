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

//! # HyperLogLog
//!
//! `hyperloglog` is a module that contains a modified version
//! of [redis's implementation](https://github.com/redis/redis/blob/4930d19e70c391750479951022e207e19111eb55/src/hyperloglog.c)
//! with some modification based on strong assumption of usage
//! within datafusion, so that function can
//! be efficiently implemented.
//!
//! Specifically, like Redis's version, the default HLL structure uses
//! 2**14 = 16384 registers, which means the standard error is
//! 1.04/(16384**0.5) = 0.8125%. The precision `p` is now a runtime
//! parameter; supported range is 4 ≤ p ≤ 18. Unlike Redis, the register
//! takes up full [`u8`] size instead of a raw int* and thus saves some
//! tricky bit shifting techniques used in the original version.
//! Also only the dense version is adopted, so there's no automatic
//! conversion, largely to simplify the code.
//!
//! This module also borrows some code structure from [pdatastructs.rs](https://github.com/crepererum/pdatastructs.rs/blob/3997ed50f6b6871c9e53c4c5e0f48f431405fc63/src/hyperloglog.rs).

use std::hash::BuildHasher;
use std::hash::Hash;
use std::marker::PhantomData;

/// Default precision — matches the historical hardcoded value.
pub(crate) const DEFAULT_HLL_P: usize = 14_usize;
/// Number of registers at the default precision.
pub(crate) const NUM_REGISTERS: usize = 1_usize << DEFAULT_HLL_P;

/// Minimum and maximum supported precision values.
pub(crate) const HLL_P_MIN: usize = 4;
pub(crate) const HLL_P_MAX: usize = 18;

#[derive(Clone, Debug)]
pub(crate) struct HyperLogLog<T>
where
    T: Hash + ?Sized,
{
    registers: Vec<u8>,
    p: usize,
    q: usize,     // 64 - p
    p_mask: u64,  // (1 << p) - 1
    phantom: PhantomData<T>,
}

pub(crate) use datafusion_common::hash_utils::HLL_RANDOM_STATE as HLL_HASH_STATE;

impl<T> Default for HyperLogLog<T>
where
    T: Hash + ?Sized,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> HyperLogLog<T>
where
    T: Hash + ?Sized,
{
    /// Creates a new, empty HyperLogLog with the default precision (14).
    pub fn new() -> Self {
        Self::with_precision(DEFAULT_HLL_P)
    }

    /// Creates a new, empty HyperLogLog with the given precision `p`.
    ///
    /// The number of registers is `2^p`. Supported range: `HLL_P_MIN..=HLL_P_MAX`.
    pub fn with_precision(p: usize) -> Self {
        assert!(
            (HLL_P_MIN..=HLL_P_MAX).contains(&p),
            "HLL precision must be in {HLL_P_MIN}..={HLL_P_MAX}, got {p}",
        );
        let num_registers = 1_usize << p;
        let q = 64 - p;
        let p_mask = (num_registers as u64) - 1;
        Self {
            registers: vec![0u8; num_registers],
            p,
            q,
            p_mask,
            phantom: PhantomData,
        }
    }

    /// Creates a HyperLogLog from already populated registers.
    ///
    /// The precision is inferred from the register slice length, which must be
    /// a power of two in the range `2^HLL_P_MIN..=2^HLL_P_MAX`.
    ///
    /// Note that this method should not be invoked in an untrusted environment
    /// because the internal structure of registers is not examined.
    pub(crate) fn from_registers(registers: Vec<u8>) -> Self {
        let len = registers.len();
        assert!(
            len.is_power_of_two(),
            "register slice length must be a power of two, got {len}",
        );
        let p = len.ilog2() as usize;
        assert!(
            (HLL_P_MIN..=HLL_P_MAX).contains(&p),
            "inferred precision {p} is outside {HLL_P_MIN}..={HLL_P_MAX}",
        );
        let q = 64 - p;
        let p_mask = (len as u64) - 1;
        Self {
            registers,
            p,
            q,
            p_mask,
            phantom: PhantomData,
        }
    }

    /// The precision of this sketch.
    #[inline]
    pub(crate) fn precision(&self) -> usize {
        self.p
    }

    /// The HLL hash state is shared through `datafusion_common::hash_utils`
    /// so sketches remain compatible across accumulators.
    #[inline]
    fn hash_value(&self, obj: &T) -> u64 {
        HLL_HASH_STATE.hash_one(obj)
    }

    /// Adds an element to the HyperLogLog.
    pub fn add(&mut self, obj: &T) {
        let hash = self.hash_value(obj);
        self.add_hashed(hash);
    }

    /// Adds a pre-computed hash value directly to the HyperLogLog.
    ///
    /// The hash should be computed using [`HLL_HASH_STATE`], the same hasher used
    /// by [`Self::add`].
    #[inline]
    pub(crate) fn add_hashed(&mut self, hash: u64) {
        let index = (hash & self.p_mask) as usize;
        let rho = ((hash >> self.p) | (1_u64 << self.q)).trailing_zeros() + 1;
        self.registers[index] = self.registers[index].max(rho as u8);
    }

    #[inline]
    fn get_histogram(&self) -> [u32; 64 - HLL_P_MIN + 2] {
        let mut histogram = [0u32; 64 - HLL_P_MIN + 2];
        for r in &self.registers {
            histogram[*r as usize] += 1;
        }
        histogram
    }

    /// Merge the other [`HyperLogLog`] into this one.
    pub fn merge(&mut self, other: &HyperLogLog<T>) {
        assert_eq!(
            self.p, other.p,
            "cannot merge HLL sketches with different precisions ({} vs {})",
            self.p, other.p
        );
        for i in 0..self.registers.len() {
            self.registers[i] = self.registers[i].max(other.registers[i]);
        }
    }

    /// Guess the number of unique elements seen by the HyperLogLog.
    pub fn count(&self) -> usize {
        count_from_histogram(&self.get_histogram()[..self.q + 2], self.p)
    }
}

/// Compute `index` and `rho` (register value) for a precomputed hash at a given
/// precision, exactly as [`HyperLogLog::add_hashed`] does.
#[inline]
pub(crate) fn register_for_hash(hash: u64, p: usize) -> (usize, u8) {
    let q = 64 - p;
    let p_mask: u64 = ((1_usize << p) as u64) - 1;
    let index = (hash & p_mask) as usize;
    let rho = (((hash >> p) | (1_u64 << q)).trailing_zeros() + 1) as u8;
    (index, rho)
}

/// Estimate the cardinality of a set of precomputed hashes without
/// materializing a full register array.
///
/// This is equivalent to adding every hash to a fresh [`HyperLogLog`] via
/// [`HyperLogLog::add_hashed`] and calling [`HyperLogLog::count`], but only does
/// work proportional to the number of hashes. It is used to cheaply estimate the
/// many small groups produced by a high-cardinality `GROUP BY`, where allocating
/// and scanning a sketch per group would dominate the runtime.
///
/// `hashes` may contain duplicates (duplicate hashes are idempotent).
pub(crate) fn count_from_hashes(hashes: &[u64], p: usize) -> usize {
    if hashes.is_empty() {
        return 0;
    }
    let num_registers = 1_usize << p;
    let q = 64 - p;
    // For each touched register index keep the maximum rho. Sorting by
    // (index, rho) groups equal indices together with the max rho last.
    let mut idx_rho: Vec<(usize, u8)> =
        hashes.iter().map(|&hash| register_for_hash(hash, p)).collect();
    idx_rho.sort_unstable();

    let mut histogram = [0u32; 64 - HLL_P_MIN + 2];
    let mut touched = 0u32;
    let mut i = 0;
    while i < idx_rho.len() {
        let index = idx_rho[i].0;
        let mut max_rho = idx_rho[i].1;
        i += 1;
        while i < idx_rho.len() && idx_rho[i].0 == index {
            max_rho = idx_rho[i].1; // ascending rho => last is the max
            i += 1;
        }
        histogram[max_rho as usize] += 1;
        touched += 1;
    }
    // All remaining registers are still zero.
    histogram[0] = num_registers as u32 - touched;
    count_from_histogram(&histogram[..q + 2], p)
}

/// Apply the HyperLogLog cardinality estimator to a register histogram.
#[inline]
fn count_from_histogram(histogram: &[u32], p: usize) -> usize {
    let q = 64 - p;
    let m = (1_usize << p) as f64;
    let mut z = m * hll_tau((m - histogram[q + 1] as f64) / m);
    for i in histogram[1..=q].iter().rev() {
        z += *i as f64;
        z *= 0.5;
    }
    z += m * hll_sigma(histogram[0] as f64 / m);
    (0.5 / 2_f64.ln() * m * m / z).round() as usize
}

/// Helper function sigma as defined in
/// "New cardinality estimation algorithms for HyperLogLog sketches"
/// Otmar Ertl, arXiv:1702.01284
#[inline]
fn hll_sigma(x: f64) -> f64 {
    if x == 1. {
        f64::INFINITY
    } else {
        let mut y = 1.0;
        let mut z = x;
        let mut x = x;
        loop {
            x *= x;
            let z_prime = z;
            z += x * y;
            y += y;
            if z_prime == z {
                break;
            }
        }
        z
    }
}

/// Helper function tau as defined in
/// "New cardinality estimation algorithms for HyperLogLog sketches"
/// Otmar Ertl, arXiv:1702.01284
#[inline]
fn hll_tau(x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        0.0
    } else {
        let mut y = 1.0;
        let mut z = 1.0 - x;
        let mut x = x;
        loop {
            x = x.sqrt();
            let z_prime = z;
            y *= 0.5;
            z -= (1.0 - x).powi(2) * y;
            if z_prime == z {
                break;
            }
        }
        z / 3.0
    }
}

impl<T> AsRef<[u8]> for HyperLogLog<T>
where
    T: Hash + ?Sized,
{
    fn as_ref(&self) -> &[u8] {
        &self.registers
    }
}

impl<T> Extend<T> for HyperLogLog<T>
where
    T: Hash,
{
    fn extend<S: IntoIterator<Item = T>>(&mut self, iter: S) {
        for elem in iter {
            self.add(&elem);
        }
    }
}

impl<'a, T> Extend<&'a T> for HyperLogLog<T>
where
    T: 'a + Hash + ?Sized,
{
    fn extend<S: IntoIterator<Item = &'a T>>(&mut self, iter: S) {
        for elem in iter {
            self.add(elem);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_HLL_P, HLL_P_MAX, HLL_P_MIN, HyperLogLog, NUM_REGISTERS};

    fn compare_with_delta(got: usize, expected: usize, p: usize) {
        let expected = expected as f64;
        let diff = (got as f64) - expected;
        let diff = diff.abs() / expected;
        // times 6 because we want the tests to be stable
        // so we allow a rather large margin of error
        // this is adopted from redis's unit test version as well
        let margin = 1.04 / (((1_usize << p) as f64).sqrt()) * 6.0;
        assert!(
            diff <= margin,
            "{} is not near {} percent of {} which is ({}, {})",
            got,
            margin,
            expected,
            expected * (1.0 - margin),
            expected * (1.0 + margin)
        );
    }

    macro_rules! sized_number_test {
        ($SIZE: expr, $T: tt) => {{
            let mut hll = HyperLogLog::<$T>::new();
            for i in 0..$SIZE {
                hll.add(&i);
            }
            compare_with_delta(hll.count(), $SIZE, DEFAULT_HLL_P);
        }};
    }

    macro_rules! typed_large_number_test {
        ($SIZE: expr) => {{
            sized_number_test!($SIZE, u64);
            sized_number_test!($SIZE, u128);
            sized_number_test!($SIZE, i64);
            sized_number_test!($SIZE, i128);
        }};
    }

    macro_rules! typed_number_test {
        ($SIZE: expr) => {{
            sized_number_test!($SIZE, u16);
            sized_number_test!($SIZE, u32);
            sized_number_test!($SIZE, i16);
            sized_number_test!($SIZE, i32);
            typed_large_number_test!($SIZE);
        }};
    }

    #[test]
    fn test_empty() {
        let hll = HyperLogLog::<u64>::new();
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn test_one() {
        let mut hll = HyperLogLog::<u64>::new();
        hll.add(&1);
        assert_eq!(hll.count(), 1);
    }

    #[test]
    fn test_number_100() {
        typed_number_test!(100);
    }

    #[test]
    fn test_number_1k() {
        typed_number_test!(1_000);
    }

    #[test]
    fn test_number_10k() {
        typed_number_test!(10_000);
    }

    #[test]
    fn test_number_100k() {
        typed_large_number_test!(100_000);
    }

    #[test]
    fn test_number_1m() {
        typed_large_number_test!(1_000_000);
    }

    #[test]
    fn test_u8() {
        let mut hll = HyperLogLog::<[u8]>::new();
        for i in 0..1000 {
            let s = i.to_string();
            let b = s.as_bytes();
            hll.add(b);
        }
        compare_with_delta(hll.count(), 1000, DEFAULT_HLL_P);
    }

    #[test]
    fn test_string() {
        let mut hll = HyperLogLog::<String>::new();
        hll.extend((0..1000).map(|i| i.to_string()));
        compare_with_delta(hll.count(), 1000, DEFAULT_HLL_P);
    }

    #[test]
    fn test_empty_merge() {
        let mut hll = HyperLogLog::<u64>::new();
        hll.merge(&HyperLogLog::<u64>::new());
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn test_merge_overlapped() {
        let mut hll = HyperLogLog::<String>::new();
        hll.extend((0..1000).map(|i| i.to_string()));

        let mut other = HyperLogLog::<String>::new();
        other.extend((0..1000).map(|i| i.to_string()));

        hll.merge(&other);
        compare_with_delta(hll.count(), 1000, DEFAULT_HLL_P);
    }

    #[test]
    fn test_repetition() {
        let mut hll = HyperLogLog::<u32>::new();
        for i in 0..1_000_000 {
            hll.add(&(i % 1000));
        }
        compare_with_delta(hll.count(), 1000, DEFAULT_HLL_P);
    }

    // --- precision-parameter tests ---

    #[test]
    fn test_precision_default_matches_new() {
        assert_eq!(HyperLogLog::<u64>::new().precision(), DEFAULT_HLL_P);
        assert_eq!(NUM_REGISTERS, 1 << DEFAULT_HLL_P);
    }

    #[test]
    fn test_precision_12_accuracy() {
        let p = 12;
        let mut hll = HyperLogLog::<u64>::with_precision(p);
        for i in 0..10_000u64 {
            hll.add(&i);
        }
        compare_with_delta(hll.count(), 10_000, p);
    }

    #[test]
    fn test_precision_10_accuracy() {
        let p = 10;
        let mut hll = HyperLogLog::<u64>::with_precision(p);
        for i in 0..10_000u64 {
            hll.add(&i);
        }
        compare_with_delta(hll.count(), 10_000, p);
    }

    #[test]
    fn test_from_registers_roundtrip() {
        for p in [10, 12, 14] {
            let mut src = HyperLogLog::<u64>::with_precision(p);
            for i in 0..1000u64 {
                src.add(&i);
            }
            let bytes = src.as_ref().to_vec();
            assert_eq!(bytes.len(), 1 << p);

            let dst = HyperLogLog::<u64>::from_registers(bytes);
            assert_eq!(dst.precision(), p);
            assert_eq!(dst.count(), src.count());
        }
    }

    #[test]
    fn test_merge_same_precision() {
        for p in [10, 12, 14] {
            let mut a = HyperLogLog::<u64>::with_precision(p);
            a.extend(0..500u64);
            let mut b = HyperLogLog::<u64>::with_precision(p);
            b.extend(500..1000u64);
            a.merge(&b);
            compare_with_delta(a.count(), 1000, p);
        }
    }

    #[test]
    #[should_panic(expected = "cannot merge HLL sketches with different precisions")]
    fn test_merge_different_precision_panics() {
        let mut a = HyperLogLog::<u64>::with_precision(12);
        let b = HyperLogLog::<u64>::with_precision(14);
        a.merge(&b);
    }

    #[test]
    fn test_precision_range_bounds() {
        // boundary values must not panic
        let _ = HyperLogLog::<u64>::with_precision(HLL_P_MIN);
        let _ = HyperLogLog::<u64>::with_precision(HLL_P_MAX);
    }

    #[test]
    #[should_panic(expected = "HLL precision must be in")]
    fn test_precision_below_min_panics() {
        let _ = HyperLogLog::<u64>::with_precision(HLL_P_MIN - 1);
    }

    #[test]
    #[should_panic(expected = "HLL precision must be in")]
    fn test_precision_above_max_panics() {
        let _ = HyperLogLog::<u64>::with_precision(HLL_P_MAX + 1);
    }

    #[test]
    #[should_panic(expected = "register slice length must be a power of two")]
    fn test_from_registers_non_power_of_two_panics() {
        let _ = HyperLogLog::<u64>::from_registers(vec![0u8; 3]);
    }

    #[test]
    #[should_panic(expected = "inferred precision")]
    fn test_from_registers_out_of_range_precision_panics() {
        // 2 bytes => p = 1, below HLL_P_MIN
        let _ = HyperLogLog::<u64>::from_registers(vec![0u8; 2]);
    }
}
