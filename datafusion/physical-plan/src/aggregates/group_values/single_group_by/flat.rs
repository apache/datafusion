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

use crate::aggregates::group_values::GroupValues;
use crate::aggregates::group_values::single_group_by::primitive::{
    GroupValuesPrimitive, HashValue, build_primitive,
};
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray, cast::AsArray};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_common::utils::split_vec_min_alloc;
use datafusion_common::{HashMap, HashSet};
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::EmitTo;
use std::hash::Hash;
use std::mem::size_of;
use std::sync::Arc;

const MAX_FLAT_RANGE: u64 = 1 << 16;
const SPARSE_FACTOR: u64 = 4; // fall back when range > distinct * this (too sparse to pack)
pub(crate) trait FlatKey: Copy {
    /// Order-preserving map into `u64`, so offset math is one 64-bit subtract.
    fn to_ordered_u64(self) -> u64;
}

macro_rules! impl_flat_key_signed {
      ($($t:ty),+) => {$(
          impl FlatKey for $t {
              #[inline]
              fn to_ordered_u64(self) -> u64 {
                  (self as i64 as u64) ^ (1 << 63)
              }
          }
      )+};
  }

macro_rules! impl_flat_key_unsigned {
      ($($t:ty),+) => {$(
          impl FlatKey for $t {
              #[inline]
              fn to_ordered_u64(self) -> u64 {
                  self as u64
              }
          }
      )+};
  }

impl_flat_key_signed!(i8, i16, i32, i64);
impl_flat_key_unsigned!(u8, u16, u32, u64);

enum Mode<T: ArrowPrimitiveType> {
    Uninit,
    Flat {
        offset: u64,
        data: Vec<u32>,
        occupied: usize,
    },
    Fallback(GroupValuesPrimitive<T>),
}

pub(crate) struct GroupValuesFlatPrimitive<T: ArrowPrimitiveType> {
    data_type: DataType,
    mode: Mode<T>,
    values: Vec<T::Native>,
    overflow: HashMap<T::Native, usize>,
    null_group: Option<usize>,
}

impl<T: ArrowPrimitiveType> GroupValuesFlatPrimitive<T>
where
    T::Native: FlatKey + Hash + Eq,
{
    pub(crate) fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            mode: Mode::Uninit,
            values: Vec::new(),
            overflow: HashMap::default(),
            null_group: None,
        }
    }

    fn init(&mut self, values: &PrimitiveArray<T>) {
        if values.is_empty() {
            self.mode = Mode::Uninit;
            return;
        }
        let mut min = u64::MAX;
        let mut max = u64::MIN;
        let mut seen: HashSet<u64> = HashSet::default();
        for v in values.iter().flatten() {
            let k = v.to_ordered_u64();
            min = min.min(k);
            max = max.max(k);
            seen.insert(k);
        }
        let distinct = seen.len() as u64;
        self.mode = match max.checked_sub(min) {
            Some(range)
                if range < MAX_FLAT_RANGE
                    && range < distinct.saturating_mul(SPARSE_FACTOR) =>
            {
                Mode::Flat {
                    offset: min,
                    data: vec![0; (range + 1) as usize],
                    occupied: 0,
                }
            }
            _ => Mode::Fallback(GroupValuesPrimitive::new(self.data_type.clone())),
        };
    }

    #[inline]
    fn intern_key(
        offset: u64,
        data: &mut Vec<u32>,
        occupied: &mut usize,
        values: &mut Vec<T::Native>,
        overflow: &mut HashMap<T::Native, usize>,
        key: T::Native,
    ) -> usize {
        // Out of window (below offset wraps huge, above exceeds len)
        let raw = key.to_ordered_u64().wrapping_sub(offset);
        if raw >= data.len() as u64 {
            return Self::intern_key_outside(raw, data, occupied, values, overflow, key);
        }

        // in window
        let slot = &mut data[raw as usize];
        if *slot != 0 {
            return (*slot - 1) as usize;
        }
        let g = values.len();
        values.push(key);
        *slot = g as u32 + 1;
        *occupied += 1;
        g
    }

    /// Cold path: grow the window up to `MAX_FLAT_RANGE`, else spill to overflow.
    #[cold]
    #[inline(never)]
    fn intern_key_outside(
        idx: u64,
        data: &mut Vec<u32>,
        occupied: &mut usize,
        values: &mut Vec<T::Native>,
        overflow: &mut HashMap<T::Native, usize>,
        key: T::Native,
    ) -> usize {
        if idx < MAX_FLAT_RANGE && idx < (*occupied as u64).saturating_mul(SPARSE_FACTOR)
        {
            let idx = idx as usize;
            data.resize(idx + 1, 0);
            let g = values.len();
            values.push(key);
            data[idx] = g as u32 + 1;
            *occupied += 1;
            g
        } else {
            *overflow.entry(key).or_insert_with(|| {
                let g = values.len();
                values.push(key);
                g
            })
        }
    }

    fn null_gid(values: &mut Vec<T::Native>, null_group: &mut Option<usize>) -> usize {
        *null_group.get_or_insert_with(|| {
            let g = values.len();
            values.push(Default::default());
            g
        })
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesFlatPrimitive<T>
where
    T::Native: FlatKey + HashValue + Hash + Eq,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        let values = cols[0].as_primitive::<T>();

        if matches!(self.mode, Mode::Uninit) {
            self.init(values);
        }

        match &mut self.mode {
            // Wide / sparse / all-null columns run on the hash grouper.
            Mode::Fallback(inner) => inner.intern(cols, groups),
            // After init, Uninit means an empty (zero-row) batch: nothing to assign.
            Mode::Uninit => {
                groups.clear();
                Ok(())
            }
            Mode::Flat {
                offset,
                data,
                occupied,
            } => {
                let offset = *offset;
                groups.clear();

                // Fast path: no nulls.
                if values.null_count() == 0 {
                    for &key in values.values().iter() {
                        let g = Self::intern_key(
                            offset,
                            data,
                            occupied,
                            &mut self.values,
                            &mut self.overflow,
                            key,
                        );
                        groups.push(g);
                    }
                    return Ok(());
                }

                for v in values {
                    let g = match v {
                        None => Self::null_gid(&mut self.values, &mut self.null_group),
                        Some(key) => Self::intern_key(
                            offset,
                            data,
                            occupied,
                            &mut self.values,
                            &mut self.overflow,
                            key,
                        ),
                    };
                    groups.push(g);
                }
                Ok(())
            }
        }
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        if let Mode::Fallback(inner) = &mut self.mode {
            return inner.emit(emit_to);
        }
        let array: PrimitiveArray<T> = match emit_to {
            EmitTo::All => {
                self.mode = Mode::Uninit;
                self.overflow.clear();
                let values = std::mem::take(&mut self.values);
                let null = self.null_group.take();
                build_primitive(values, null)
            }
            EmitTo::First(n) => {
                if let Mode::Flat { data, occupied, .. } = &mut self.mode {
                    // flushed group ids and keeps the rest, a slot with
                    // id < n is freed and surviving ids shift down
                    let mut live: usize = 0;
                    for slot in data.iter_mut() {
                        if *slot == 0 {
                            continue; // unseen
                        }
                        let gid = (*slot - 1) as usize;
                        // survivor: shift id down by n; emitted (gid < n): free the slot
                        match gid.checked_sub(n) {
                            Some(sub) => {
                                *slot = sub as u32 + 1;
                                live += 1;
                            }
                            None => *slot = 0,
                        }
                    }
                    if live == 0 {
                        *data = Vec::new();
                    }
                    *occupied = live;
                }
                self.overflow.retain(|_, g| match g.checked_sub(n) {
                    Some(sub) => {
                        *g = sub;
                        true
                    }
                    None => false,
                });

                if self.overflow.capacity() > self.overflow.len().saturating_mul(2) {
                    self.overflow.shrink_to_fit();
                }
                let null_group = match &mut self.null_group {
                    Some(v) if *v >= n => {
                        *v -= n;
                        None
                    }
                    Some(_) => self.null_group.take(),
                    None => None,
                };
                build_primitive(split_vec_min_alloc(&mut self.values, n), null_group)
            }
        };
        Ok(vec![Arc::new(array)])
    }

    fn size(&self) -> usize {
        if let Mode::Fallback(inner) = &self.mode {
            return inner.size();
        }
        // Uninit still retains values/overflow capacity after emit/clear_shrink;
        // count it so the memory pool sees the real footprint.
        let data = match &self.mode {
            Mode::Flat { data, .. } => data.allocated_size(),
            _ => 0,
        };
        data + self.values.allocated_size()
            + self.overflow.capacity() * size_of::<(T::Native, usize)>()
    }

    fn is_empty(&self) -> bool {
        match &self.mode {
            Mode::Fallback(inner) => inner.is_empty(),
            _ => self.values.is_empty(),
        }
    }

    fn len(&self) -> usize {
        match &self.mode {
            Mode::Fallback(inner) => inner.len(),
            _ => self.values.len(),
        }
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        if let Mode::Fallback(inner) = &mut self.mode {
            inner.clear_shrink(num_rows);
            return;
        }
        self.mode = Mode::Uninit;
        self.values.clear();
        self.values.shrink_to_fit();
        self.overflow.clear();
        self.null_group = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::array::types::Int32Type;

    fn new_gv() -> GroupValuesFlatPrimitive<Int32Type> {
        GroupValuesFlatPrimitive::new(DataType::Int32)
    }

    fn intern(
        gv: &mut GroupValuesFlatPrimitive<Int32Type>,
        vals: &[Option<i32>],
    ) -> Vec<usize> {
        let col: ArrayRef = Arc::new(Int32Array::from(vals.to_vec()));
        let mut groups = vec![];
        gv.intern(&[col], &mut groups).unwrap();
        groups
    }

    fn emit_all(gv: &mut GroupValuesFlatPrimitive<Int32Type>) -> Vec<Option<i32>> {
        let out = gv.emit(EmitTo::All).unwrap();
        out[0].as_primitive::<Int32Type>().iter().collect()
    }

    #[test]
    fn flat_path_assigns_dense_ids() {
        let mut gv = new_gv();
        assert_eq!(
            intern(&mut gv, &[Some(10), Some(11), Some(10), Some(13)]),
            vec![0, 1, 0, 2]
        );
        assert_eq!(emit_all(&mut gv), vec![Some(10), Some(11), Some(13)]);
    }

    #[test]
    fn handles_nulls() {
        let mut gv = new_gv();
        assert_eq!(
            intern(&mut gv, &[Some(5), None, Some(5), None]),
            vec![0, 1, 0, 1]
        );
        assert_eq!(emit_all(&mut gv), vec![Some(5), None]);
    }

    #[test]
    fn out_of_window_keys_use_overflow() {
        let mut gv = new_gv();
        // first batch sizes the window to [0, 1]
        assert_eq!(intern(&mut gv, &[Some(0), Some(1)]), vec![0, 1]);
        // 1_000_000 is far outside the window -> overflow; in-window keys stay fast
        assert_eq!(
            intern(&mut gv, &[Some(1_000_000), Some(0), Some(1_000_000)]),
            vec![2, 0, 2]
        );
        assert_eq!(emit_all(&mut gv), vec![Some(0), Some(1), Some(1_000_000)]);
    }

    #[test]
    fn wide_first_batch_falls_back_to_hash() {
        let mut gv = new_gv();
        // range 0..=200_000 exceeds MAX_FLAT_RANGE -> Fallback to the hash impl
        assert_eq!(
            intern(&mut gv, &[Some(0), Some(200_000), Some(0)]),
            vec![0, 1, 0]
        );
        assert!(matches!(gv.mode, Mode::Fallback(_)));
        assert_eq!(emit_all(&mut gv), vec![Some(0), Some(200_000)]);
    }

    #[test]
    fn emit_first_shifts_remaining_ids() {
        let mut gv = new_gv();
        intern(&mut gv, &[Some(0), Some(1), Some(2), Some(3)]);
        let emitted = gv.emit(EmitTo::First(2)).unwrap();
        assert_eq!(
            emitted[0]
                .as_primitive::<Int32Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(0), Some(1)]
        );
        // survivors 2,3 renumber to 0,1; a fresh key gets 2
        assert_eq!(intern(&mut gv, &[Some(2), Some(3), Some(9)]), vec![0, 1, 2]);
    }

    #[test]
    fn emit_first_releases_dead_window_and_overflow() {
        let mut gv = new_gv();
        // window sized to [0, 1000)
        let first: Vec<Option<i32>> = (0..1000).map(Some).collect();
        intern(&mut gv, &first);
        // keys far past the window land in overflow (a sorted stream marching
        // past the window, as in ordered final aggregation)
        let second: Vec<Option<i32>> = (100_000..101_000).map(Some).collect();
        intern(&mut gv, &second);
        let before = gv.size();
        // drain every group seen so far
        gv.emit(EmitTo::First(2000)).unwrap();
        let after = gv.size();
        assert!(
            after < before,
            "size must shrink after a full drain: {before} -> {after}"
        );
        // the fully-emitted window is dropped, not kept as a dead allocation
        match &gv.mode {
            Mode::Flat { data, .. } => assert!(data.is_empty()),
            _ => panic!("expected Flat mode"),
        }
        // still functional after the drain
        assert_eq!(intern(&mut gv, &[Some(5), Some(5), Some(7)]), vec![0, 0, 1]);
    }

    #[test]
    fn matches_hash_impl() {
        let data = &[Some(3), Some(1), None, Some(3), Some(7), Some(1), None];
        let mut flat = new_gv();
        let mut hash = GroupValuesPrimitive::<Int32Type>::new(DataType::Int32);
        let hash_groups = {
            let col: ArrayRef = Arc::new(Int32Array::from(data.to_vec()));
            let mut g = vec![];
            hash.intern(&[col], &mut g).unwrap();
            g
        };
        assert_eq!(intern(&mut flat, data), hash_groups);
    }

    #[test]
    fn signed_negative_keys_match_hash() {
        // Negative / sign-spanning keys exercise the ordered-u64 map + wrapping_sub.
        let data = &[
            Some(-5),
            Some(0),
            Some(5),
            Some(-5),
            Some(-3),
            Some(5),
            None,
        ];
        let mut flat = new_gv();
        let mut hash = GroupValuesPrimitive::<Int32Type>::new(DataType::Int32);
        let hash_groups = {
            let col: ArrayRef = Arc::new(Int32Array::from(data.to_vec()));
            let mut g = vec![];
            hash.intern(&[col], &mut g).unwrap();
            g
        };
        assert_eq!(intern(&mut flat, data), hash_groups);
        assert_eq!(
            emit_all(&mut flat),
            vec![Some(-5), Some(0), Some(5), Some(-3), None]
        );
    }

    #[test]
    fn negative_key_below_window_uses_overflow() {
        // A very-negative key wraps below the window offset -> must go to overflow.
        let mut gv = new_gv();
        assert_eq!(intern(&mut gv, &[Some(0), Some(1)]), vec![0, 1]);
        assert_eq!(intern(&mut gv, &[Some(-1_000_000), Some(0)]), vec![2, 0]);
        assert_eq!(emit_all(&mut gv), vec![Some(0), Some(1), Some(-1_000_000)]);
    }

    #[test]
    fn all_null_first_batch_then_wide_matches_hash() {
        // Regression (C1): an all-null first batch parks the grouper before it can
        // size a window. A later wide batch triggers Fallback; the fresh inner must
        // not re-use the null group's id. Flat must match the hash impl exactly,
        // batch-for-batch and on emit (a dropped/merged null fails this).
        let batch1: &[Option<i32>] = &[None, None];
        let batch2: &[Option<i32>] = &[Some(0), Some(200_000), Some(0)];

        let mut flat = new_gv();
        let flat_g1 = intern(&mut flat, batch1);
        let flat_g2 = intern(&mut flat, batch2);
        let flat_out = emit_all(&mut flat);

        let mut hash = GroupValuesPrimitive::<Int32Type>::new(DataType::Int32);
        let (hash_g1, hash_g2) = {
            let mut run = |vals: &[Option<i32>]| {
                let col: ArrayRef = Arc::new(Int32Array::from(vals.to_vec()));
                let mut g = vec![];
                hash.intern(&[col], &mut g).unwrap();
                g
            };
            (run(batch1), run(batch2))
        };
        let hash_out = {
            let out = hash.emit(EmitTo::All).unwrap();
            out[0]
                .as_primitive::<Int32Type>()
                .iter()
                .collect::<Vec<_>>()
        };

        assert_eq!(flat_g1, hash_g1, "first (all-null) batch group ids");
        assert_eq!(flat_g2, hash_g2, "second (wide) batch group ids");
        assert_eq!(
            flat_out, hash_out,
            "emitted group values (null must survive)"
        );
    }

    #[test]
    fn sparse_first_batch_falls_back_to_hash() {
        let mut gv = new_gv();
        // range 100 over only 2 distinct values: 100 >= 2 * SPARSE_FACTOR -> Fallback,
        // even though 100 < MAX_FLAT_RANGE. (Distinct, not row count, is the signal.)
        assert_eq!(
            intern(&mut gv, &[Some(0), Some(100), Some(0)]),
            vec![0, 1, 0]
        );
        assert!(matches!(gv.mode, Mode::Fallback(_)));
        assert_eq!(emit_all(&mut gv), vec![Some(0), Some(100)]);
    }

    #[test]
    fn window_grows_when_dense_overflows_when_sparse() {
        let mut gv = new_gv();
        // Dense first batch: window [0, 9], occupied = 10.
        let first: Vec<Option<i32>> = (0..10).map(Some).collect();
        assert_eq!(intern(&mut gv, &first), (0..10_usize).collect::<Vec<_>>());

        // 30 is out of window but dense enough to grow: 30 < occupied(10) * SPARSE_FACTOR.
        assert_eq!(intern(&mut gv, &[Some(30)]), vec![10]);
        assert!(gv.overflow.is_empty());
        match &gv.mode {
            Mode::Flat { data, occupied, .. } => {
                assert_eq!(data.len(), 31); // grew to include idx 30
                assert_eq!(*occupied, 11);
            }
            _ => panic!("expected Flat"),
        }

        // 100 is within MAX_FLAT_RANGE but too sparse to grow: 100 >= occupied(11) * 4,
        // so it goes to overflow instead of ballooning the window.
        assert_eq!(intern(&mut gv, &[Some(100)]), vec![11]);
        assert_eq!(gv.overflow.len(), 1);
        match &gv.mode {
            Mode::Flat { data, .. } => assert_eq!(data.len(), 31), // did NOT grow
            _ => panic!("expected Flat"),
        }

        let expected: Vec<Option<i32>> =
            (0..10).map(Some).chain([Some(30), Some(100)]).collect();
        assert_eq!(emit_all(&mut gv), expected);
    }

    #[test]
    fn emit_first_with_overflow_and_null() {
        let mut gv = new_gv();
        // window [0, 1] plus a null (gid 2), then an out-of-window overflow key (gid 3)
        assert_eq!(intern(&mut gv, &[Some(0), Some(1), None]), vec![0, 1, 2]);
        assert_eq!(intern(&mut gv, &[Some(1_000_000)]), vec![3]);

        // emit gids 0,1 (values 0,1); survivors null(2->0) and 1_000_000(3->1) renumber
        let emitted = gv.emit(EmitTo::First(2)).unwrap();
        assert_eq!(
            emitted[0]
                .as_primitive::<Int32Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(0), Some(1)]
        );

        // null is now gid 0, the overflow key gid 1, and a fresh in-range key gets gid 2
        assert_eq!(
            intern(&mut gv, &[None, Some(1_000_000), Some(5)]),
            vec![0, 1, 2]
        );
        assert_eq!(emit_all(&mut gv), vec![None, Some(1_000_000), Some(5)]);
    }

    #[test]
    fn unsigned_u64_flat_matches_hash() {
        use arrow::array::UInt64Array;
        use arrow::array::types::UInt64Type;

        // Dense unsigned keys exercise the identity ordered-u64 map on the flat path.
        let data: &[Option<u64>] = &[Some(10), Some(13), Some(10), None, Some(11)];
        let col: ArrayRef = Arc::new(UInt64Array::from(data.to_vec()));

        let mut flat = GroupValuesFlatPrimitive::<UInt64Type>::new(DataType::UInt64);
        let mut flat_groups = vec![];
        flat.intern(&[Arc::clone(&col)], &mut flat_groups).unwrap();
        assert!(matches!(flat.mode, Mode::Flat { .. }));

        let mut hash = GroupValuesPrimitive::<UInt64Type>::new(DataType::UInt64);
        let mut hash_groups = vec![];
        hash.intern(&[col], &mut hash_groups).unwrap();
        assert_eq!(flat_groups, hash_groups);

        let flat_out = flat.emit(EmitTo::All).unwrap();
        let hash_out = hash.emit(EmitTo::All).unwrap();
        assert_eq!(
            flat_out[0]
                .as_primitive::<UInt64Type>()
                .iter()
                .collect::<Vec<_>>(),
            hash_out[0]
                .as_primitive::<UInt64Type>()
                .iter()
                .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn growth_gate_is_exclusive_at_occupied_times_sparse_factor() {
        // Dense first batch -> window [0, 9], occupied = 10.
        // Grow gate: grow iff idx < occupied * SPARSE_FACTOR  => threshold 40.
        let first: Vec<Option<i32>> = (0..10).map(Some).collect();

        // idx == 40 (== threshold): must OVERFLOW — the gate is a strict `<`.
        let mut gv = new_gv();
        intern(&mut gv, &first);
        assert_eq!(intern(&mut gv, &[Some(40)]), vec![10]);
        assert_eq!(
            gv.overflow.len(),
            1,
            "idx == occupied*SPARSE_FACTOR must overflow"
        );
        match &gv.mode {
            Mode::Flat { data, .. } => {
                assert_eq!(data.len(), 10, "window must not grow at the boundary")
            }
            _ => panic!("expected Flat"),
        }

        // idx == 39 (threshold - 1): must GROW.
        let mut gv = new_gv();
        intern(&mut gv, &first);
        assert_eq!(intern(&mut gv, &[Some(39)]), vec![10]);
        assert!(
            gv.overflow.is_empty(),
            "idx just below the threshold must grow"
        );
        match &gv.mode {
            Mode::Flat { data, .. } => {
                assert_eq!(data.len(), 40, "window grows to include idx 39")
            }
            _ => panic!("expected Flat"),
        }
    }

    #[test]
    fn emit_first_releases_slack_overflow_capacity() {
        let mut gv = new_gv();
        // small window, then a wide key stream fills the overflow map
        intern(&mut gv, &[Some(0), Some(1)]);
        let wide: Vec<Option<i32>> = (0..500).map(|i| Some(1_000_000 + i)).collect();
        intern(&mut gv, &wide);
        let cap_before = gv.overflow.capacity();
        assert!(cap_before >= 500, "overflow should have grown its capacity");

        // drain almost everything, leaving 2 groups -> overflow capacity is now slack
        let total = gv.len();
        gv.emit(EmitTo::First(total - 2)).unwrap();

        assert!(gv.overflow.len() <= 2);
        assert!(
            gv.overflow.capacity() < cap_before,
            "slack overflow capacity must be released after the drain: {} -> {}",
            cap_before,
            gv.overflow.capacity()
        );
    }
}
