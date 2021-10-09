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

//! This module contains a modified version specifically for the
//! implementation of `approx_distinct` function.
//!
//! https://github.com/crepererum/pdatastructs.rs/blob/3997ed50f6b6871c9e53c4c5e0f48f431405fc63/src/hyperloglog.rs
//! https://github.com/redis/redis/blob/4930d19e70c391750479951022e207e19111eb55/src/hyperloglog.c

// TODO remove this when hooked up with the rest
#![allow(dead_code)]

use ahash::AHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

const PRECISION: usize = 14_usize;
/// mask to obtain index into the registers
const HLL_P_MASK: u64 = (1 << PRECISION) as u64 - 1;
/// the number of bits of the hash value used determining the number of leading zeros
const HLL_Q: usize = 64_usize - PRECISION;
const NUM_REGISTERS: usize = 1_usize << PRECISION;

#[derive(Clone, Debug)]
pub(crate) struct HyperLogLog<T>
where
    T: Hash + ?Sized,
{
    registers: [u8; NUM_REGISTERS],
    phantom: PhantomData<T>,
}

impl<T> HyperLogLog<T>
where
    T: Hash + ?Sized,
{
    /// Creates a new, empty HyperLogLog.
    pub fn new() -> Self {
        let registers = [0; NUM_REGISTERS];
        Self {
            registers,
            phantom: PhantomData,
        }
    }

    /// choice of hash function: ahash is already an dependency
    /// and it fits the requirements of being a 64bit hash with
    /// reasonable performance.
    #[inline]
    fn hash_value(&self, obj: &T) -> u64 {
        let mut hasher = AHasher::default();
        obj.hash(&mut hasher);
        hasher.finish()
    }

    /// Adds an element to the HyperLogLog.
    pub fn add(&mut self, obj: &T) {
        let hash = self.hash_value(obj);
        let index = (hash & HLL_P_MASK) as usize;
        // count the 0000...1 pattern length
        let p = ((hash >> PRECISION) | (1_u64 << HLL_Q)).leading_zeros() + 1;
        self.registers[index] = self.registers[index].max(p as u8);
    }

    /// Get the register histogram (each value in register index into
    /// the histogram for its leading 000...1 pattern length); u32 is enough because we only have
    /// 16384 registers
    #[inline]
    fn get_hll_histogram(&self) -> [u32; HLL_Q + 2] {
        let mut hll_histo = [0; HLL_Q + 2];
        // build the histogram, hopefully this can be unrolled
        (0..NUM_REGISTERS).for_each(|i| {
            let index = self.registers[i] as usize;
            hll_histo[index] += 1;
        });
        hll_histo
    }

    /// Guess the number of unique elements seen by the HyperLogLog.
    pub fn count(&self) -> usize {
        let hll_histo = self.get_hll_histogram();
        let m = NUM_REGISTERS as f64;
        let mut z: f64 = m * hll_tau((m - hll_histo[HLL_Q + 1] as f64) / m);
        (1..=HLL_Q).rev().for_each(|i| {
            z += hll_histo[i] as f64;
            z *= 0.5;
        });
        z += m * hll_sigma(hll_histo[0] as f64 / m);
        ((0.5 / 2_f64.ln()) * m * m / z).round() as u64 as usize
    }
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
            z -= (1.0 - x).powf(2.0) * y;
            if z_prime == z {
                break;
            }
        }
        z / 3.0
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
    use super::HyperLogLog;

    const ERROR_RATE: f64 = 0.81;

    fn compare_with_delta(got: usize, expected: usize) {
        let diff = (got as i64) - (expected as i64);
        let diff = diff.abs() as usize;
        let delta = ((expected as f64) * ERROR_RATE).ceil() as usize;
        assert!(
            diff <= delta,
            "{} is not near {} percent of {}",
            got,
            ERROR_RATE,
            expected
        );
    }

    macro_rules! sized_number_test {
        ($SIZE: expr, $T: tt) => {{
            let mut hll = HyperLogLog::<$T>::new();
            for i in 0..$SIZE {
                hll.add(&i);
            }
            compare_with_delta(hll.count(), $SIZE);
        }};
    }

    macro_rules! typed_number_test {
        ($SIZE: expr) => {{
            sized_number_test!($SIZE, u16);
            sized_number_test!($SIZE, u32);
            sized_number_test!($SIZE, u64);
            sized_number_test!($SIZE, u128);
            sized_number_test!($SIZE, i16);
            sized_number_test!($SIZE, i32);
            sized_number_test!($SIZE, i64);
            sized_number_test!($SIZE, i128);
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
        typed_number_test!(1000);
    }

    #[test]
    fn test_number_10k() {
        typed_number_test!(10000);
    }
}
