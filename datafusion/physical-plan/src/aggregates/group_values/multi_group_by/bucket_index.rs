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

use std::mem::size_of;

use datafusion_execution::memory_pool::proxy::HashTableAllocExt;
use hashbrown::hash_table::HashTable;

use crate::aggregates::group_values::multi_group_by::GroupIndexView;

type Entry = (u64, GroupIndexView);
// A real group or collision-list index cannot reach 2^63 - 1 entries.
const EMPTY: Entry = (0, GroupIndexView(u64::MAX));

#[derive(Default)]
pub(super) enum GroupIndexMap {
    #[default]
    HashbrownDefault,
    Hashbrown {
        table: HashTable<Entry>,
        half_full: bool,
    },
    Linear(LinearIndex),
}

impl GroupIndexMap {
    pub(super) fn new(is_bucket_table: bool) -> Self {
        let variant = if is_bucket_table {
            std::env::var("DATAFUSION_BUCKET_INDEX").unwrap_or_default()
        } else {
            String::new()
        };
        match variant.as_str() {
            "hashbrown-half" => Self::Hashbrown {
                table: HashTable::new(),
                half_full: true,
            },
            "linear" => Self::Linear(LinearIndex::default()),
            _ => Self::Hashbrown {
                table: HashTable::new(),
                half_full: false,
            },
        }
    }

    #[cfg(test)]
    pub(super) fn hashbrown_half() -> Self {
        Self::Hashbrown {
            table: HashTable::new(),
            half_full: true,
        }
    }

    #[cfg(test)]
    pub(super) fn linear() -> Self {
        Self::Linear(LinearIndex::default())
    }

    pub(super) fn find(
        &self,
        hash: u64,
        eq: impl FnMut(&Entry) -> bool,
    ) -> Option<&Entry> {
        match self {
            Self::Hashbrown { table, .. } => table.find(hash, eq),
            Self::Linear(table) => table.find(hash, eq),
            Self::HashbrownDefault => None,
        }
    }

    pub(super) fn find_mut(
        &mut self,
        hash: u64,
        eq: impl FnMut(&Entry) -> bool,
    ) -> Option<&mut Entry> {
        match self {
            Self::Hashbrown { table, .. } => table.find_mut(hash, eq),
            Self::Linear(table) => table.find_mut(hash, eq),
            Self::HashbrownDefault => None,
        }
    }

    pub(super) fn insert_accounted(
        &mut self,
        entry: Entry,
        hash: impl Fn(&Entry) -> u64,
        accounting: &mut usize,
    ) {
        match self {
            Self::Hashbrown {
                table,
                half_full: false,
            } => table.insert_accounted(entry, hash, accounting),
            Self::Hashbrown {
                table,
                half_full: true,
            } => {
                // HashTable::capacity counts elements allowed at its native
                // load factor. Its power-of-two ceiling is the physical slot
                // count; reserve past capacity to force the next allocation.
                let slots = table.capacity().next_power_of_two();
                if slots == 0 || table.len() + 1 > slots / 2 {
                    let additional = table.capacity() + 1 - table.len();
                    table.reserve(additional, &hash);
                }
                table.insert_unique(hash(&entry), entry, hash);
                *accounting = hashbrown_bytes(table.capacity());
            }
            Self::Linear(table) => {
                table.insert(entry);
                *accounting = table.allocated_size();
            }
            Self::HashbrownDefault => unreachable!(),
        }
    }

    pub(super) fn retain(&mut self, keep: impl FnMut(&mut Entry) -> bool) {
        match self {
            Self::Hashbrown { table, .. } => table.retain(keep),
            Self::Linear(table) => table.retain(keep),
            Self::HashbrownDefault => {}
        }
    }

    pub(super) fn clear(&mut self) {
        match self {
            Self::Hashbrown { table, .. } => table.clear(),
            Self::Linear(table) => table.clear(),
            Self::HashbrownDefault => {}
        }
    }

    pub(super) fn shrink_to(&mut self, num_rows: usize, hash: impl Fn(&Entry) -> u64) {
        match self {
            Self::Hashbrown {
                table,
                half_full: false,
            } => table.shrink_to(num_rows, hash),
            Self::Hashbrown {
                table,
                half_full: true,
            } => table.shrink_to(num_rows.saturating_mul(7) / 4, hash),
            Self::Linear(table) => table.shrink_to(num_rows),
            Self::HashbrownDefault => {}
        }
    }

    pub(super) fn allocated_size(&self) -> usize {
        match self {
            Self::Hashbrown {
                table,
                half_full: false,
            } => table.capacity() * size_of::<Entry>(),
            Self::Hashbrown {
                table,
                half_full: true,
            } => hashbrown_bytes(table.capacity()),
            Self::Linear(table) => table.allocated_size(),
            Self::HashbrownDefault => 0,
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Hashbrown { table, .. } => table.len(),
            Self::Linear(table) => table.len,
            Self::HashbrownDefault => 0,
        }
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(test)]
    pub(super) fn slot_count(&self) -> usize {
        match self {
            Self::Hashbrown { table, .. } => {
                if table.capacity() == 0 {
                    0
                } else {
                    table.capacity().next_power_of_two()
                }
            }
            Self::Linear(table) => table.slots.len(),
            Self::HashbrownDefault => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::instant::Instant;
    use std::hint::black_box;

    use crate::aggregates::group_values::multi_group_by::GroupIndexView;
    use crate::aggregates::group_values::multi_group_by::bucket_index::{
        EMPTY, GroupIndexMap, LinearIndex,
    };

    #[test]
    fn probe_growth_collision_and_reuse() {
        for mut map in [GroupIndexMap::hashbrown_half(), GroupIndexMap::linear()] {
            let mut bytes = 0;
            for group in 0..257 {
                // Identical low bits force long probe sequences in the linear
                // table while preserving distinct full hash values.
                let hash = ((group as u64) << 16) | 7;
                map.insert_accounted(
                    (hash, GroupIndexView::new_inlined(group as u64)),
                    |entry| entry.0,
                    &mut bytes,
                );
                assert!(map.slot_count() >= map.len() * 2);
                assert_eq!(
                    map.find(hash, |entry| entry.0 == hash).unwrap().1.value(),
                    group as u64
                );
                assert!(map.find(hash ^ 1, |entry| entry.0 == (hash ^ 1)).is_none());
            }
            assert_eq!(map.len(), 257);
            assert!(bytes >= 257 * 16);
            map.retain(|entry| entry.1.value() % 2 == 0);
            assert_eq!(map.len(), 129);
            for group in 0..257 {
                let hash = ((group as u64) << 16) | 7;
                assert_eq!(
                    map.find(hash, |entry| entry.0 == hash).is_some(),
                    group % 2 == 0
                );
            }
            map.clear();
            assert!(map.is_empty());
            map.shrink_to(16, |entry| entry.0);
            for group in 0..16 {
                let hash = group as u64;
                map.insert_accounted(
                    (hash, GroupIndexView::new_inlined(hash)),
                    |entry| entry.0,
                    &mut bytes,
                );
            }
            assert_eq!(map.len(), 16);
        }
    }

    #[test]
    #[ignore = "manual release-mode index timing diagnostic"]
    fn measure_index_build_hit_and_miss() {
        fn mixed_hash(mut key: u64) -> u64 {
            key = (key ^ (key >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            key = (key ^ (key >> 27)).wrapping_mul(0x94d049bb133111eb);
            key ^ (key >> 31)
        }

        fn probes(table: &LinearIndex, hash: u64) -> usize {
            let mask = table.slots.len() - 1;
            let mut index = (hash as usize) & mask;
            let mut count = 1;
            while table.slots[index].1 != EMPTY.1 && table.slots[index].0 != hash {
                index = (index + 1) & mask;
                count += 1;
            }
            count
        }

        for count in [768usize, 6_144, 24_576, 98_304, 393_216] {
            let hashes = (0..count)
                .map(|key| mixed_hash(key as u64 + 1))
                .collect::<Vec<_>>();
            let missing = (0..count)
                .map(|key| mixed_hash(key as u64 + count as u64 + 1))
                .collect::<Vec<_>>();
            for (name, mut table) in [
                ("default", GroupIndexMap::new(false)),
                ("hashbrown-half", GroupIndexMap::hashbrown_half()),
                ("linear", GroupIndexMap::linear()),
            ] {
                let mut bytes = 0;
                let start = Instant::now();
                for (key, &hash) in hashes.iter().enumerate() {
                    table.insert_accounted(
                        (hash, GroupIndexView::new_inlined(key as u64)),
                        |entry| entry.0,
                        &mut bytes,
                    );
                }
                let build_ns = start.elapsed().as_nanos() as f64 / count as f64;

                let start = Instant::now();
                let mut sum = 0;
                for _ in 0..8 {
                    for &hash in &hashes {
                        sum += black_box(table.find(hash, |entry| entry.0 == hash))
                            .is_some() as usize;
                    }
                }
                black_box(sum);
                let hit_ns = start.elapsed().as_nanos() as f64 / (count * 8) as f64;

                let start = Instant::now();
                for _ in 0..8 {
                    for &hash in &missing {
                        sum += black_box(table.find(hash, |entry| entry.0 == hash))
                            .is_some() as usize;
                    }
                }
                black_box(sum);
                let miss_ns = start.elapsed().as_nanos() as f64 / (count * 8) as f64;
                println!(
                    "index={name},groups={count},slots={},occupancy={:.3},build_ns={build_ns:.2},hit_ns={hit_ns:.2},miss_ns={miss_ns:.2},accounted_bytes={bytes}",
                    table.slot_count(),
                    count as f64 / table.slot_count() as f64,
                );
                if let GroupIndexMap::Linear(linear) = &table {
                    let mut hit_probes = hashes
                        .iter()
                        .map(|&hash| probes(linear, hash))
                        .collect::<Vec<_>>();
                    let mut miss_probes = missing
                        .iter()
                        .map(|&hash| probes(linear, hash))
                        .collect::<Vec<_>>();
                    hit_probes.sort_unstable();
                    miss_probes.sort_unstable();
                    println!(
                        "linear_probes groups={count} hit_p50={} hit_p95={} hit_p99={} miss_p50={} miss_p95={} miss_p99={}",
                        hit_probes[count / 2],
                        hit_probes[count * 95 / 100],
                        hit_probes[count * 99 / 100],
                        miss_probes[count / 2],
                        miss_probes[count * 95 / 100],
                        miss_probes[count * 99 / 100],
                    );
                }
            }
        }
    }
}

fn hashbrown_bytes(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    let slots = capacity.next_power_of_two();
    slots * size_of::<Entry>() + slots + 16
}

#[derive(Default)]
pub(super) struct LinearIndex {
    slots: Vec<Entry>,
    len: usize,
}

impl LinearIndex {
    fn find(&self, hash: u64, mut eq: impl FnMut(&Entry) -> bool) -> Option<&Entry> {
        if self.slots.is_empty() {
            return None;
        }
        let mask = self.slots.len() - 1;
        let mut index = (hash as usize) & mask;
        loop {
            let entry = &self.slots[index];
            if entry.1 == EMPTY.1 {
                return None;
            }
            if eq(entry) {
                return Some(entry);
            }
            index = (index + 1) & mask;
        }
    }

    fn find_mut(
        &mut self,
        hash: u64,
        mut eq: impl FnMut(&Entry) -> bool,
    ) -> Option<&mut Entry> {
        if self.slots.is_empty() {
            return None;
        }
        let mask = self.slots.len() - 1;
        let mut index = (hash as usize) & mask;
        loop {
            let entry = &self.slots[index];
            if entry.1 == EMPTY.1 {
                return None;
            }
            if eq(entry) {
                return Some(&mut self.slots[index]);
            }
            index = (index + 1) & mask;
        }
    }

    fn insert(&mut self, entry: Entry) {
        if self.slots.is_empty() || self.len + 1 > self.slots.len() / 2 {
            self.grow();
        }
        self.insert_without_grow(entry);
    }

    fn insert_without_grow(&mut self, entry: Entry) {
        let mask = self.slots.len() - 1;
        let mut index = (entry.0 as usize) & mask;
        while self.slots[index].1 != EMPTY.1 {
            index = (index + 1) & mask;
        }
        self.slots[index] = entry;
        self.len += 1;
    }

    fn grow(&mut self) {
        let new_slots = self.slots.len().max(2) * 2;
        let old = std::mem::replace(&mut self.slots, vec![EMPTY; new_slots]);
        self.len = 0;
        for entry in old {
            if entry.1 != EMPTY.1 {
                self.insert_without_grow(entry);
            }
        }
    }

    fn retain(&mut self, mut keep: impl FnMut(&mut Entry) -> bool) {
        let slots = self.slots.len();
        let old = std::mem::replace(&mut self.slots, vec![EMPTY; slots]);
        self.len = 0;
        for mut entry in old {
            if entry.1 != EMPTY.1 && keep(&mut entry) {
                self.insert_without_grow(entry);
            }
        }
    }

    fn clear(&mut self) {
        self.slots.fill(EMPTY);
        self.len = 0;
    }

    fn shrink_to(&mut self, num_rows: usize) {
        debug_assert_eq!(self.len, 0);
        let slots = num_rows.saturating_mul(2).max(4).next_power_of_two();
        if self.slots.len() > slots {
            self.slots = vec![EMPTY; slots];
        }
    }

    fn allocated_size(&self) -> usize {
        self.slots.capacity() * size_of::<Entry>()
    }
}
