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

//! Integer membership with a byte index and hash fallback.

use std::{hash::Hash, mem::size_of};

use arrow::buffer::BooleanBuffer;
use datafusion_common::HashSet;

const BUCKETS: usize = 256;
const SLOTS: usize = 4;

pub(super) trait IntegerKey: Copy + Eq + Hash {
    fn byte(self, index: usize) -> usize;
}

macro_rules! integer_key {
    ($($t:ty),+ $(,)?) => {
        $(
            impl IntegerKey for $t {
                #[inline(always)]
                fn byte(self, index: usize) -> usize {
                    ((self as u64 >> (index * 8)) & 0xff) as usize
                }
            }
        )+
    };
}

integer_key!(i32, u32, i64, u64);

pub(super) enum IntegerSet<V> {
    Indexed {
        byte: usize,
        buckets: Box<[[V; SLOTS]; BUCKETS]>,
    },
    Hash(HashSet<V>),
}

impl<V: IntegerKey> IntegerSet<V> {
    pub(super) fn new(input: impl IntoIterator<Item = V>) -> Self {
        let input = input.into_iter();
        let mut set = HashSet::with_capacity(input.size_hint().1.unwrap_or(0));
        set.extend(input);
        if !set.is_empty() && set.len() <= BUCKETS * SLOTS {
            for byte in 0..size_of::<V>() {
                let mut counts = [0_u16; BUCKETS];
                for &value in &set {
                    counts[value.byte(byte)] += 1;
                }
                if counts.iter().all(|&count| count <= SLOTS as u16) {
                    let sentinel = *set.iter().next().expect("set is not empty");
                    // Padding cannot match a value from another bucket.
                    let mut buckets = Box::new([[sentinel; SLOTS]; BUCKETS]);
                    let mut next = [0_u8; BUCKETS];
                    for &value in &set {
                        let bucket = value.byte(byte);
                        buckets[bucket][next[bucket] as usize] = value;
                        next[bucket] += 1;
                    }
                    return Self::Indexed { byte, buckets };
                }
            }
        }
        Self::Hash(set)
    }

    pub(super) fn contains_values(&self, values: &[V]) -> BooleanBuffer {
        match self {
            Self::Indexed { byte, buckets } => {
                BooleanBuffer::collect_bool(values.len(), |i| {
                    let value = values[i];
                    let [a, b, c, d] = buckets[value.byte(*byte)];
                    // Compare all four values so the compiler can use SIMD.
                    (value == a) | (value == b) | (value == c) | (value == d)
                })
            }
            Self::Hash(set) => {
                BooleanBuffer::collect_bool(values.len(), |i| set.contains(&values[i]))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_matches<V: IntegerKey + std::fmt::Debug>(values: &[V], needles: &[V]) {
        let expected = values
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let actual = IntegerSet::new(values.to_vec()).contains_values(needles);
        for (i, value) in needles.iter().enumerate() {
            assert_eq!(actual.value(i), expected.contains(value), "value {value:?}");
        }
    }

    #[test]
    fn matches_standard_set() {
        let mut state = 1_u64;
        for len in [0, 1, 7, 32, 129, 256, 257, 1025] {
            let values = (0..len)
                .map(|_| {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1);
                    state % 2049
                })
                .collect::<Vec<_>>();
            let needles = (0..2200).collect::<Vec<_>>();
            assert_matches(&values, &needles);
        }
    }

    #[test]
    fn handles_bounds_and_duplicates() {
        assert_matches(
            &[i32::MIN, -1, 0, 1, i32::MAX, 0],
            &[i32::MIN, i32::MIN + 1, -1, 0, 1, i32::MAX],
        );
        assert_matches(
            &[i64::MIN, -1, 0, 1, i64::MAX, 0],
            &[i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX],
        );
        assert_matches(&[u32::MIN, 1, u32::MAX, 1], &[u32::MIN, 1, 2, u32::MAX]);
        assert_matches(&[u64::MIN, 1, u64::MAX, 1], &[u64::MIN, 1, 2, u64::MAX]);
    }

    #[test]
    fn selects_index_or_hash() {
        assert!(matches!(
            IntegerSet::new(vec![0_u32, 256, 512, 768, 1024]),
            IntegerSet::Indexed { byte: 1, .. }
        ));

        let mut values = Vec::with_capacity(625);
        for a in 0..5 {
            for b in 0..5 {
                for c in 0..5 {
                    for d in 0..5 {
                        values.push(a | b << 8 | c << 16 | d << 24);
                    }
                }
            }
        }
        assert!(matches!(IntegerSet::new(values), IntegerSet::Hash(_)));
    }
}
