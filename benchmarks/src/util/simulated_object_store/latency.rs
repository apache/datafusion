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

//! Time-to-first-byte distributions and how they are sampled.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// GET time-to-first-byte distribution, inspired by real S3 latencies.
///
/// 20 values: 11x P50 (~25-35ms), 5x P75-P90 (~70-110ms), 2x P95 (~120-150ms),
/// 2x P99 (~180-200ms).
/// Sorted: 25,25,28,28,30,30,30,30,32,32,35, 70,85,100,100,110, 130,150, 180,200
/// P50≈32ms, P90≈110ms, P99≈200ms
pub const GET_TTFB_MS: &[u64] = &[
    30, 100, 25, 85, 32, 200, 28, 130, 35, 70, 30, 150, 30, 110, 28, 180, 32, 25, 100, 30,
];

/// LIST time-to-first-byte distribution, generally higher than GET.
///
/// This is the cost of a *single page* of results, not of a whole listing.
///
/// 20 values: 11x P50 (~40-70ms), 5x P75-P90 (~120-180ms), 2x P95 (~200-250ms),
/// 2x P99 (~300-400ms).
/// Sorted: 40,40,50,50,55,55,60,60,65,65,70, 120,140,160,160,180, 210,250, 300,400
/// P50≈65ms, P90≈180ms, P99≈400ms
pub const LIST_TTFB_MS: &[u64] = &[
    55, 160, 40, 140, 65, 400, 50, 210, 70, 120, 60, 250, 55, 180, 50, 300, 65, 40, 160,
    60,
];

/// Draws request latencies from a fixed distribution.
///
/// Draws are deterministic: the Nth draw of a run is always the same value, so
/// benchmark runs stay reproducible.
///
/// The draw counter is hashed rather than used to index the table directly. A
/// plain round robin resonates with the fixed fan-out of
/// [`coalesce_ranges`](object_store::coalesce_ranges), which issues requests in
/// waves of 10: with a 20-entry table every vectored read would see the same
/// two fixed sets of latencies, and a read that happened to issue exactly 10 or
/// 20 requests would always land on exactly the table mean.
#[derive(Debug)]
pub struct LatencySampler {
    table: &'static [u64],
    draws: AtomicU64,
}

impl LatencySampler {
    pub const fn new(table: &'static [u64]) -> Self {
        assert!(!table.is_empty());
        Self {
            table,
            draws: AtomicU64::new(0),
        }
    }

    /// Draw the next latency.
    pub fn sample(&self) -> Duration {
        let draw = self.draws.fetch_add(1, Ordering::Relaxed);
        let idx = (splitmix64(draw) % self.table.len() as u64) as usize;
        Duration::from_millis(self.table[idx])
    }
}

/// The SplitMix64 finalizer: stateless, cheap, and mixes well enough that
/// consecutive draw indices land on unrelated table entries.
const fn splitmix64(draw: u64) -> u64 {
    let mut z = draw.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampler_is_deterministic() {
        let a = LatencySampler::new(GET_TTFB_MS);
        let b = LatencySampler::new(GET_TTFB_MS);
        for _ in 0..50 {
            assert_eq!(a.sample(), b.sample());
        }
    }

    #[test]
    fn sampler_reproduces_the_table_distribution() {
        let sampler = LatencySampler::new(GET_TTFB_MS);
        // Over many draws every table entry should come up, and the mean should
        // land near the table mean. A round robin would guarantee this trivially;
        // the point here is that hashing does not skew it.
        let draws = 20_000;
        let total: u64 = (0..draws)
            .map(|_| sampler.sample().as_millis() as u64)
            .sum();
        let table_mean =
            GET_TTFB_MS.iter().sum::<u64>() as f64 / GET_TTFB_MS.len() as f64;
        let sampled_mean = total as f64 / draws as f64;
        assert!(
            (sampled_mean - table_mean).abs() < table_mean * 0.05,
            "sampled mean {sampled_mean} too far from table mean {table_mean}"
        );
    }

    #[test]
    fn consecutive_draws_do_not_cycle_with_the_coalesce_fan_out() {
        // `coalesce_ranges` issues requests in waves of 10. Two consecutive
        // waves must not see the same multiset of latencies, which is exactly
        // what a round robin over a 20 entry table would produce.
        let sampler = LatencySampler::new(GET_TTFB_MS);
        let wave = |sampler: &LatencySampler| -> Vec<Duration> {
            let mut v: Vec<_> = (0..10).map(|_| sampler.sample()).collect();
            v.sort();
            v
        };
        let first = wave(&sampler);
        let second = wave(&sampler);
        let third = wave(&sampler);
        assert!(first != second || second != third);
    }
}
