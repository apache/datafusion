// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Upper bounds on the memory that growing a buffer holds at its peak.
//!
//! A growing buffer allocates its new storage before it frees the old
//! storage. So while a [`HashTable`] rehashes or a [`Vec`] reallocates, both
//! allocations are live:
//!
//! ```text
//! bytes
//!   ^          +---------+
//!   |          |   old   |
//!   |  +-------+  + new  +------------------+
//!   |  |  old  |         |       new        |
//!   +--+-------+---------+------------------+--> time
//!        before   grow       after
//! ```
//!
//! Each function returns the largest number of bytes that the buffer holds
//! above its current allocation while it grows to hold `additional` more
//! items one at a time. A caller reserves this before it inserts, so the
//! reservation covers the peak and not only the final size.

use hashbrown::hash_table::HashTable;

/// The largest SIMD group width that hashbrown uses on any platform. Using
/// the largest width makes [`hash_table_alloc_bytes`] an upper bound on
/// platforms with a smaller width.
const MAX_GROUP_WIDTH: usize = 16;

/// Returns the bucket count that hashbrown allocates for `capacity` items.
fn buckets_for_capacity(capacity: usize) -> usize {
    match capacity {
        0..4 => 4,
        4..8 => 8,
        8..15 => 16,
        // hashbrown keeps 1/8 of the buckets empty.
        _ => (capacity * 8 / 7).next_power_of_two(),
    }
}

/// Returns the number of items that a table with `buckets` buckets holds
/// before it grows.
fn capacity_for_buckets(buckets: usize) -> usize {
    if buckets < 8 {
        buckets - 1
    } else {
        buckets / 8 * 7
    }
}

/// Returns an upper bound on the bytes that hashbrown allocates for a table
/// of `T` with `buckets` buckets: the slots, the padding before the control
/// bytes, one control byte per bucket, and one trailing group.
fn hash_table_alloc_bytes<T>(buckets: usize) -> usize {
    let ctrl_align = align_of::<T>().max(MAX_GROUP_WIDTH);
    let slots = (buckets * size_of::<T>()).next_multiple_of(ctrl_align);
    slots + buckets + MAX_GROUP_WIDTH
}

/// Returns an upper bound on the bytes, above
/// [`HashTable::allocation_size`], that `table` holds at its peak while
/// `additional` more items are inserted one at a time.
///
/// Each insert into a full table grows it to the next bucket count, which
/// doubles the buckets. During each growth the old and the new table are
/// both allocated.
pub(crate) fn hash_table_growth<T>(table: &HashTable<T>, additional: usize) -> usize {
    let needed = table.len().saturating_add(additional);
    // `capacity` excludes deleted slots, so it never overstates the room
    // left. A table with deleted slots may rehash in place instead, which
    // allocates less than this bound.
    let mut capacity = table.capacity();
    if needed <= capacity {
        return 0;
    }

    let current = table.allocation_size();
    let mut old = current;
    let mut peak = current;
    let mut buckets = if current == 0 {
        0
    } else {
        buckets_for_capacity(capacity)
    };
    while capacity < needed {
        buckets = if buckets == 0 {
            buckets_for_capacity(1)
        } else {
            buckets * 2
        };
        capacity = capacity_for_buckets(buckets);
        let new = hash_table_alloc_bytes::<T>(buckets);
        peak = peak.max(old + new);
        old = new;
    }
    peak - current
}

/// Returns an upper bound on the bytes, above the current allocation, that
/// `vec` holds at its peak while `additional` more items are pushed one at a
/// time.
///
/// [`Vec`] at least doubles its capacity when it is full. The allocator may
/// grow the storage in place, but this bound assumes it copies.
pub(crate) fn vec_growth<T>(vec: &Vec<T>, additional: usize) -> usize {
    let elem = size_of::<T>();
    let needed = vec.len().saturating_add(additional);
    let mut capacity = vec.capacity();
    if elem == 0 || needed <= capacity {
        return 0;
    }

    let current = capacity * elem;
    let mut peak = current;
    while capacity < needed {
        let new_capacity = (capacity * 2).max(4);
        peak = peak.max((capacity + new_capacity) * elem);
        capacity = new_capacity;
    }
    peak - current
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Inserts `additional` items one at a time and returns the peak bytes
    /// above the starting allocation. Each growth allocates the new table
    /// before it frees the old one, so the peak of a growth is `old + new`.
    fn measure_table(table: &mut HashTable<u64>, additional: usize) -> usize {
        let start = table.allocation_size();
        let mut peak = start;
        let base = table.len() as u64;
        for key in base..base + additional as u64 {
            let old = table.allocation_size();
            table.insert_unique(key, key, |&k| k);
            let new = table.allocation_size();
            if new != old {
                peak = peak.max(old + new);
            }
        }
        peak - start
    }

    /// Same as [`measure_table`] for a [`Vec`].
    fn measure_vec(vec: &mut Vec<u64>, additional: usize) -> usize {
        let start = vec.capacity() * 8;
        let mut peak = start;
        for v in 0..additional as u64 {
            let old = vec.capacity() * 8;
            vec.push(v);
            let new = vec.capacity() * 8;
            if new != old {
                peak = peak.max(old + new);
            }
        }
        peak - start
    }

    #[test]
    fn hash_table_growth_matches_peak() {
        // (initial items, items to add)
        let cases = [
            // Fits in the free slots: no growth.
            (0, 28),
            // One doubling.
            (28, 1),
            // Several doublings in one batch.
            (28, 200),
            // A large table that doubles once during one 8192-row batch.
            (1_835_008, 8192),
            // An unallocated table.
            (0, 3),
        ];
        for (initial, additional) in cases {
            let mut table = HashTable::new();
            measure_table(&mut table, initial);
            let bound = hash_table_growth(&table, additional);
            let measured = measure_table(&mut table, additional);
            // hashbrown uses a group width of 16 on x86_64 with SSE2 and 8
            // elsewhere. The bound assumes 16, so it is exact only there.
            if cfg!(target_feature = "sse2") {
                assert_eq!(bound, measured, "initial={initial} additional={additional}");
            } else {
                assert!(
                    bound >= measured && bound - measured <= 8 * 2,
                    "initial={initial} additional={additional}: bound {bound}, measured {measured}"
                );
            }
        }
    }

    #[test]
    fn vec_growth_matches_peak() {
        // (initial capacity, initial len, items to add, expected peak)
        let cases = [
            (16, 0, 16, 0),
            (16, 16, 1, (16 + 32) * 8 - 16 * 8),
            // 16 -> 32 -> 64 -> 128: the last doubling is the peak.
            (16, 16, 100, (64 + 128) * 8 - 16 * 8),
            (0, 0, 1, 4 * 8),
        ];
        for (capacity, len, additional, expected) in cases {
            let mut vec: Vec<u64> = Vec::with_capacity(capacity);
            vec.extend(0..len as u64);
            assert_eq!(vec_growth(&vec, additional), expected);
            assert_eq!(measure_vec(&mut vec, additional), expected);
        }
    }
}
