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
    Flat { offset: u64, data: Vec<u32> },
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
        self.mode = if min > max {
            // All-null/empty first batch: stay Uninit so a later batch anchors the window.
            Mode::Uninit
        } else if max - min >= MAX_FLAT_RANGE
            || max - min >= distinct.saturating_mul(SPARSE_FACTOR)
        {
            Mode::Fallback(GroupValuesPrimitive::new(self.data_type.clone()))
        } else {
            let len = (max - min + 1) as usize;
            Mode::Flat {
                offset: min,
                data: vec![0; len],
            }
        };
    }

    #[inline]
    fn intern_key(
        offset: u64,
        data: &mut Vec<u32>,
        values: &mut Vec<T::Native>,
        overflow: &mut HashMap<T::Native, usize>,
        key: T::Native,
    ) -> usize {
        // Below-offset keys wrap huge, so this one get_mut checks underflow and length.
        let raw = key.to_ordered_u64().wrapping_sub(offset);
        if let Ok(idx) = usize::try_from(raw)
            && let Some(slot) = data.get_mut(idx)
        {
            return if *slot != 0 {
                (*slot - 1) as usize
            } else {
                let g = values.len();
                values.push(key);
                *slot = g as u32 + 1;
                g
            };
        }
        Self::intern_key_outside(raw, data, values, overflow, key)
    }

    /// Cold path: grow the window up to `MAX_FLAT_RANGE`, else spill to overflow.
    #[cold]
    #[inline(never)]
    fn intern_key_outside(
        idx: u64,
        data: &mut Vec<u32>,
        values: &mut Vec<T::Native>,
        overflow: &mut HashMap<T::Native, usize>,
        key: T::Native,
    ) -> usize {
        if idx < MAX_FLAT_RANGE {
            let idx = idx as usize;
            data.resize(idx + 1, 0);
            let g = values.len();
            values.push(key);
            data[idx] = g as u32 + 1;
            g
        } else {
            *overflow.entry(key).or_insert_with(|| {
                let g = values.len();
                values.push(key);
                g
            })
        }
    }
}

impl<T: ArrowPrimitiveType> GroupValues for GroupValuesFlatPrimitive<T>
where
    T::Native: FlatKey + HashValue + Hash + Eq,
{
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        assert_eq!(cols.len(), 1);

        let values = cols[0].as_primitive::<T>();

        if matches!(self.mode, Mode::Uninit) {
            self.init(values);
        }
        if let Mode::Fallback(inner) = &mut self.mode {
            return inner.intern(cols, groups);
        }

        groups.clear();

        // All-null/empty first batch left mode Uninit: every row is the null group.
        if matches!(self.mode, Mode::Uninit) {
            if !values.is_empty() {
                let null_gid = *self.null_group.get_or_insert_with(|| {
                    let g = self.values.len();
                    self.values.push(Default::default());
                    g
                });
                groups.resize(values.len(), null_gid);
            }
            return Ok(());
        }

        let Mode::Flat { offset, data } = &mut self.mode else {
            unreachable!("mode is Flat after init unless Fallback")
        };
        let offset = *offset;
        if values.null_count() == 0 {
            // Fast path: no nulls.
            for &key in values.values().iter() {
                let group_id = Self::intern_key(
                    offset,
                    data,
                    &mut self.values,
                    &mut self.overflow,
                    key,
                );
                groups.push(group_id);
            }
        } else {
            for v in values {
                let group_id = match v {
                    None => *self.null_group.get_or_insert_with(|| {
                        let g = self.values.len();
                        self.values.push(Default::default());
                        g
                    }),
                    Some(key) => Self::intern_key(
                        offset,
                        data,
                        &mut self.values,
                        &mut self.overflow,
                        key,
                    ),
                };
                groups.push(group_id);
            }
        }
        Ok(())
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
                if let Mode::Flat { data, .. } = &mut self.mode {
                    for slot in data.iter_mut() {
                        if *slot != 0 {
                            let gid = (*slot - 1) as usize;
                            *slot = match gid.checked_sub(n) {
                                Some(sub) => sub as u32 + 1,
                                None => 0,
                            };
                        }
                    }
                }
                self.overflow.retain(|_, g| match g.checked_sub(n) {
                    Some(sub) => {
                        *g = sub;
                        true
                    }
                    None => false,
                });
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
        self.values.shrink_to(num_rows);
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
}
