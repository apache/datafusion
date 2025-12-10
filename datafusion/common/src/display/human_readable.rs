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

//! Helpers for rendering sizes, counts, and durations in human readable form.

/// Common data size units
pub mod units {
    pub const TB: u64 = 1 << 40;
    pub const GB: u64 = 1 << 30;
    pub const MB: u64 = 1 << 20;
    pub const KB: u64 = 1 << 10;
}

/// Present size in human-readable form
pub fn human_readable_size(size: usize) -> String {
    use units::*;

    let size = size as u64;
    let (value, unit) = {
        if size >= 2 * TB {
            (size as f64 / TB as f64, "TB")
        } else if size >= 2 * GB {
            (size as f64 / GB as f64, "GB")
        } else if size >= 2 * MB {
            (size as f64 / MB as f64, "MB")
        } else if size >= 2 * KB {
            (size as f64 / KB as f64, "KB")
        } else {
            (size as f64, "B")
        }
    };
    format!("{value:.1} {unit}")
}

/// Present count in human-readable form with K, M, B, T suffixes
pub fn human_readable_count(count: usize) -> String {
    let count = count as u64;
    let (value, unit) = {
        if count >= 1_000_000_000_000 {
            (count as f64 / 1_000_000_000_000.0, " T")
        } else if count >= 1_000_000_000 {
            (count as f64 / 1_000_000_000.0, " B")
        } else if count >= 1_000_000 {
            (count as f64 / 1_000_000.0, " M")
        } else if count >= 1_000 {
            (count as f64 / 1_000.0, " K")
        } else {
            return count.to_string();
        }
    };

    // Format with appropriate precision
    // For values >= 100, show 1 decimal place (e.g., 123.4 K)
    // For values < 100, show 2 decimal places (e.g., 10.12 K)
    if value >= 100.0 {
        format!("{value:.1}{unit}")
    } else {
        format!("{value:.2}{unit}")
    }
}

/// Present duration in human-readable form with 2 decimal places
pub fn human_readable_duration(nanos: u64) -> String {
    const NANOS_PER_SEC: f64 = 1_000_000_000.0;
    const NANOS_PER_MILLI: f64 = 1_000_000.0;
    const NANOS_PER_MICRO: f64 = 1_000.0;

    let nanos_f64 = nanos as f64;

    if nanos >= 1_000_000_000 {
        // >= 1 second: show in seconds
        format!("{:.2}s", nanos_f64 / NANOS_PER_SEC)
    } else if nanos >= 1_000_000 {
        // >= 1 millisecond: show in milliseconds
        format!("{:.2}ms", nanos_f64 / NANOS_PER_MILLI)
    } else if nanos >= 1_000 {
        // >= 1 microsecond: show in microseconds
        format!("{:.2}µs", nanos_f64 / NANOS_PER_MICRO)
    } else {
        // < 1 microsecond: show in nanoseconds
        format!("{nanos}ns")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_human_readable_count() {
        assert_eq!(human_readable_count(0), "0");
        assert_eq!(human_readable_count(1), "1");
        assert_eq!(human_readable_count(999), "999");
        assert_eq!(human_readable_count(1_000), "1.00 K");
        assert_eq!(human_readable_count(10_100), "10.10 K");
        assert_eq!(human_readable_count(1_532), "1.53 K");
        assert_eq!(human_readable_count(99_999), "100.00 K");
        assert_eq!(human_readable_count(1_000_000), "1.00 M");
        assert_eq!(human_readable_count(1_532_000), "1.53 M");
        assert_eq!(human_readable_count(99_000_000), "99.00 M");
        assert_eq!(human_readable_count(123_456_789), "123.5 M");
        assert_eq!(human_readable_count(1_000_000_000), "1.00 B");
        assert_eq!(human_readable_count(1_532_000_000), "1.53 B");
        assert_eq!(human_readable_count(999_999_999_999), "1000.0 B");
        assert_eq!(human_readable_count(1_000_000_000_000), "1.00 T");
        assert_eq!(human_readable_count(42_000_000_000_000), "42.00 T");
    }

    #[test]
    fn test_human_readable_duration() {
        assert_eq!(human_readable_duration(0), "0ns");
        assert_eq!(human_readable_duration(1), "1ns");
        assert_eq!(human_readable_duration(999), "999ns");
        assert_eq!(human_readable_duration(1_000), "1.00µs");
        assert_eq!(human_readable_duration(1_234), "1.23µs");
        assert_eq!(human_readable_duration(999_999), "1000.00µs");
        assert_eq!(human_readable_duration(1_000_000), "1.00ms");
        assert_eq!(human_readable_duration(11_295_377), "11.30ms");
        assert_eq!(human_readable_duration(1_234_567), "1.23ms");
        assert_eq!(human_readable_duration(999_999_999), "1000.00ms");
        assert_eq!(human_readable_duration(1_000_000_000), "1.00s");
        assert_eq!(human_readable_duration(1_234_567_890), "1.23s");
        assert_eq!(human_readable_duration(42_000_000_000), "42.00s");
    }
}
