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

//! A custom binary heap implementation for performant top K aggregation.
//!
//! the `new_heap` //! factory function selects an appropriate heap implementation
//! based on the Arrow data type.
//!
//! Supported value types include Arrow primitives (integers, floats, decimals, intervals)
//! and UTF-8 strings (`Utf8`, `LargeUtf8`, `Utf8View`) using lexicographic ordering.

use arrow::array::types::{IntervalDayTime, IntervalMonthDayNano};
use arrow::array::{
    Array, ArrayAccessor, ArrayRef, ArrowPrimitiveType, AsArray, LargeStringArray,
    PrimitiveArray, StringArray, StringArrayType, StringViewArray, downcast_primitive,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{DataType, i256};
use datafusion_common::Result;
use datafusion_common::exec_datafusion_err;

use half::f16;
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

/// A custom version of `Ord` that only exists to we can implement it for the Values in our heap
pub trait Comparable<Rhs: ?Sized = Self> {
    fn comp(&self, other: &Rhs) -> Ordering;
}

impl Comparable<String> for str {
    fn comp(&self, other: &String) -> Ordering {
        self.cmp(other.as_str())
    }
}

impl Comparable for String {
    fn comp(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

impl Comparable for Option<String> {
    fn comp(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

/// A "type alias" for Values which are stored in our heap
pub trait ValueType: Comparable + Clone + Debug + Default {}

impl<T> ValueType for T where T: Comparable + Clone + Debug + Default {}

const VACANT_MAP_IDX: usize = usize::MAX;

/// An entry in our heap, which contains both the value and a index into an external HashTable
struct HeapItem<VAL: ValueType> {
    val: VAL,
    map_idx: usize,
}
impl<VAL: ValueType> HeapItem<VAL> {
    fn is_vacant(&self) -> bool {
        self.map_idx == VACANT_MAP_IDX
    }

    fn is_occupied(&self) -> bool {
        self.map_idx != VACANT_MAP_IDX
    }

    fn map_idx(&self) -> Option<usize> {
        self.is_occupied().then_some(self.map_idx)
    }

    fn take(&mut self) -> Option<Self> {
        self.is_occupied().then(|| std::mem::take(self))
    }

    fn as_ref(&self) -> &Self {
        debug_assert!(self.is_occupied());
        self
    }

    fn as_mut(&mut self) -> &mut Self {
        debug_assert!(self.is_occupied());
        self
    }
}
impl<VAL: ValueType> Default for HeapItem<VAL> {
    fn default() -> Self {
        Self {
            val: VAL::default(),
            map_idx: VACANT_MAP_IDX,
        }
    }
}

/// A custom heap implementation that allows several things that couldn't be achieved with
/// `collections::BinaryHeap`:
/// 1. It allows values to be updated at arbitrary positions (when group values change)
/// 2. It can be either a min or max heap
/// 3. It can use our `HeapItem` type & `Comparable` trait
/// 4. It is specialized to grow to a certain limit, then always replace without grow & shrink
struct TopKHeap<VAL: ValueType> {
    desc: bool,
    len: usize,
    capacity: usize,
    heap: Vec<HeapItem<VAL>>,
}

/// An interface to hide the generic type signature of TopKHeap behind arrow arrays
pub trait ArrowHeap {
    fn value_type(&self) -> &DataType;
    fn set_batch(&mut self, vals: ArrayRef);
    fn is_worse(&self, idx: usize) -> bool;
    fn worst_map_idx(&self) -> usize;
    fn insert(&mut self, row_idx: usize, map_idx: usize, map: &mut Vec<(usize, usize)>);
    fn replace_if_better(
        &mut self,
        heap_idx: usize,
        row_idx: usize,
        map: &mut Vec<(usize, usize)>,
    );
    fn drain(&mut self) -> (ArrayRef, Vec<usize>);
}

/// An implementation of `ArrowHeap` that deals with primitive values
pub struct PrimitiveHeap<VAL: ArrowPrimitiveType>
where
    <VAL as ArrowPrimitiveType>::Native: Comparable,
{
    batch: PrimitiveArray<VAL>,
    heap: TopKHeap<VAL::Native>,
    desc: bool,
    value_type: DataType,
}

impl<VAL: ArrowPrimitiveType> PrimitiveHeap<VAL>
where
    <VAL as ArrowPrimitiveType>::Native: Comparable,
{
    pub fn new(limit: usize, desc: bool, value_type: DataType) -> Self {
        Self {
            batch: PrimitiveArray::<VAL>::new_null(0).with_data_type(value_type.clone()),
            heap: TopKHeap::new(limit, desc),
            desc,
            value_type,
        }
    }
}

impl<VAL: ArrowPrimitiveType> ArrowHeap for PrimitiveHeap<VAL>
where
    <VAL as ArrowPrimitiveType>::Native: Comparable,
{
    fn value_type(&self) -> &DataType {
        &self.value_type
    }

    fn set_batch(&mut self, vals: ArrayRef) {
        self.batch = vals.as_primitive().clone();
    }

    fn is_worse(&self, row_idx: usize) -> bool {
        if !self.heap.is_full() {
            return false;
        }
        let new_val = self.batch.value(row_idx);
        let worst_val = self.heap.worst_val().expect("Missing root");
        (!self.desc && new_val > *worst_val) || (self.desc && new_val < *worst_val)
    }

    fn worst_map_idx(&self) -> usize {
        self.heap.worst_map_idx()
    }

    fn insert(&mut self, row_idx: usize, map_idx: usize, map: &mut Vec<(usize, usize)>) {
        let new_val = self.batch.value(row_idx);
        self.heap.append_or_replace(&new_val, map_idx, map);
    }

    fn replace_if_better(
        &mut self,
        heap_idx: usize,
        row_idx: usize,
        map: &mut Vec<(usize, usize)>,
    ) {
        let new_val = self.batch.value(row_idx);
        self.heap.replace_if_better(&new_val, heap_idx, map);
    }

    fn drain(&mut self) -> (ArrayRef, Vec<usize>) {
        let nulls = None;
        let (vals, map_idxs) = self.heap.drain();
        let arr = PrimitiveArray::<VAL>::new(ScalarBuffer::from(vals), nulls)
            .with_data_type(self.value_type.clone());
        (Arc::new(arr), map_idxs)
    }
}

/// An implementation of `ArrowHeap` that deals with string values.
///
/// Supports all three UTF-8 string types: `Utf8`, `LargeUtf8`, and `Utf8View`.
/// String values are compared lexicographically using the compare-first pattern:
/// borrowed strings are compared before allocation, and only allocated when the
/// heap confirms they improve the top-K set.
///
pub struct StringHeap<S: Array>
where
    for<'a> &'a S: StringArrayType<'a>,
{
    batch: S,
    heap: TopKHeap<String>,
    desc: bool,
}

impl<S> StringHeap<S>
where
    S: Array + From<Vec<Option<String>>>,
    for<'a> &'a S: StringArrayType<'a>,
{
    pub fn new(limit: usize, desc: bool) -> Self {
        let batch = S::from(Vec::new());
        Self {
            batch,
            heap: TopKHeap::new(limit, desc),
            desc,
        }
    }
}

impl<S> ArrowHeap for StringHeap<S>
where
    S: Array + Clone + From<Vec<String>> + 'static,
    for<'a> &'a S: StringArrayType<'a>,
{
    fn value_type(&self) -> &DataType {
        // Strings don't store any metadata.
        self.batch.data_type()
    }

    fn set_batch(&mut self, vals: ArrayRef) {
        self.batch = vals
            .as_any()
            .downcast_ref::<S>()
            .expect("Unsupported data type")
            .clone();
    }

    fn is_worse(&self, row_idx: usize) -> bool {
        if !self.heap.is_full() {
            return false;
        }
        // Compare borrowed `&str` against the worst heap value first to avoid
        // allocating a `String` unless this row would actually replace an
        // existing heap entry.
        let new_val = (&self.batch).value(row_idx);
        let worst_val = self.heap.worst_val().expect("Missing root").as_str();
        (!self.desc && new_val > worst_val) || (self.desc && new_val < worst_val)
    }

    fn worst_map_idx(&self) -> usize {
        self.heap.worst_map_idx()
    }

    fn insert(&mut self, row_idx: usize, map_idx: usize, map: &mut Vec<(usize, usize)>) {
        let new_val = (&self.batch).value(row_idx);
        self.heap.append_or_replace(new_val, map_idx, map);
    }

    fn replace_if_better(
        &mut self,
        heap_idx: usize,
        row_idx: usize,
        map: &mut Vec<(usize, usize)>,
    ) {
        let new_val = (&self.batch).value(row_idx);
        self.heap.replace_if_better(new_val, heap_idx, map);
    }

    fn drain(&mut self) -> (ArrayRef, Vec<usize>) {
        let (vals, map_idxs) = self.heap.drain();
        let vals = Arc::new(S::from(vals));
        (vals, map_idxs)
    }
}

impl<VAL: ValueType> TopKHeap<VAL> {
    pub fn new(limit: usize, desc: bool) -> Self {
        Self {
            desc,
            capacity: limit,
            len: 0,
            heap: (0..=limit).map(|_| HeapItem::default()).collect::<Vec<_>>(),
        }
    }

    pub fn worst_val(&self) -> Option<&VAL> {
        let root = self.heap.first()?;
        root.is_occupied().then_some(&root.val)
    }

    pub fn worst_map_idx(&self) -> usize {
        self.heap[0].map_idx().unwrap_or(0)
    }

    pub fn is_full(&self) -> bool {
        self.len >= self.capacity
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn append_or_replace<Q>(
        &mut self,
        new_val: &Q,
        map_idx: usize,
        map: &mut Vec<(usize, usize)>,
    ) where
        Q: ToOwned<Owned = VAL> + ?Sized,
    {
        if self.is_full() {
            self.replace_root(new_val, map_idx, map);
        } else {
            self.append(new_val, map_idx, map);
        }
    }

    fn append<Q>(&mut self, new_val: &Q, map_idx: usize, mapper: &mut Vec<(usize, usize)>)
    where
        Q: ToOwned<Owned = VAL> + ?Sized,
    {
        let hi = &mut self.heap[self.len];
        debug_assert!(hi.is_vacant());
        hi.map_idx = map_idx;
        new_val.clone_into(&mut hi.val);
        self.heapify_up(self.len, mapper);
        self.len += 1;
    }

    fn pop(&mut self, map: &mut Vec<(usize, usize)>) -> Option<HeapItem<VAL>> {
        if self.len() == 0 {
            return None;
        }
        if self.len() == 1 {
            self.len = 0;
            return self.heap[0].take();
        }
        self.swap(0, self.len - 1, map);
        let former_root = self.heap[self.len - 1].take();
        self.len -= 1;
        self.heapify_down(0, map);
        former_root
    }

    pub fn drain(&mut self) -> (Vec<VAL>, Vec<usize>) {
        let mut map = Vec::new();
        let mut vals = Vec::with_capacity(self.len);
        let mut map_idxs = Vec::with_capacity(self.len);
        while let Some(worst_hi) = self.pop(&mut map) {
            vals.push(worst_hi.val);
            map_idxs.push(worst_hi.map_idx);
            map.clear();
        }
        vals.reverse();
        map_idxs.reverse();
        (vals, map_idxs)
    }

    fn replace_root<Q>(
        &mut self,
        new_val: &Q,
        map_idx: usize,
        mapper: &mut Vec<(usize, usize)>,
    ) where
        Q: ToOwned<Owned = VAL> + ?Sized,
    {
        let hi = self.heap[0].as_mut();
        new_val.clone_into(&mut hi.val);
        hi.map_idx = map_idx;
        self.heapify_down(0, mapper);
    }

    pub fn replace_if_better<Q>(
        &mut self,
        new_val: &Q,
        heap_idx: usize,
        mapper: &mut Vec<(usize, usize)>,
    ) where
        Q: ToOwned<Owned = VAL> + Comparable<VAL> + ?Sized,
    {
        let existing = self.heap[heap_idx].as_mut();
        if (!self.desc && new_val.comp(&existing.val) != Ordering::Less)
            || (self.desc && new_val.comp(&existing.val) != Ordering::Greater)
        {
            return;
        }
        new_val.clone_into(&mut existing.val);
        self.heapify_down(heap_idx, mapper);
    }

    fn heapify_up(&mut self, mut idx: usize, mapper: &mut Vec<(usize, usize)>) {
        let desc = self.desc;
        while idx != 0 {
            let parent_idx = (idx - 1) / 2;
            let node = self.heap[idx].as_ref();
            let parent = self.heap[parent_idx].as_ref();
            if (!desc && node.val.comp(&parent.val) != Ordering::Greater)
                || (desc && node.val.comp(&parent.val) != Ordering::Less)
            {
                return;
            }
            self.swap(idx, parent_idx, mapper);
            idx = parent_idx;
        }
    }

    #[inline]
    fn swap(&mut self, a_idx: usize, b_idx: usize, mapper: &mut Vec<(usize, usize)>) {
        self.heap.swap(a_idx, b_idx);

        let b_hi = self.heap[b_idx].as_ref();
        let a_hi = self.heap[a_idx].as_ref();

        mapper.extend([(b_hi.map_idx, b_idx), (a_hi.map_idx, a_idx)]);
    }

    fn heapify_down(&mut self, mut node_idx: usize, mapper: &mut Vec<(usize, usize)>) {
        let desc = self.desc;
        loop {
            let left_child = node_idx * 2 + 1;
            let entry = self.heap[node_idx].as_ref();
            let mut best_idx = node_idx;
            let mut best_val = &entry.val;
            for child_idx in left_child..=left_child + 1 {
                if let Some(child) = self.heap.get(child_idx)
                    && child.is_occupied()
                    && ((!desc && child.val.comp(best_val) == Ordering::Greater)
                        || (desc && child.val.comp(best_val) == Ordering::Less))
                {
                    best_val = &child.val;
                    best_idx = child_idx;
                }
            }
            if best_val.comp(&entry.val) == Ordering::Equal {
                break;
            }
            self.swap(best_idx, node_idx, mapper);
            node_idx = best_idx;
        }
    }

    fn _tree_print(&self, idx: usize, prefix: &str, is_tail: bool, output: &mut String) {
        if let Some(hi) = self.heap.get(idx)
            && hi.is_occupied()
        {
            let connector = if idx != 0 {
                if is_tail { "└── " } else { "├── " }
            } else {
                ""
            };
            output.push_str(&format!(
                "{}{}val={:?} idx={}, bucket={}\n",
                prefix, connector, hi.val, idx, hi.map_idx
            ));
            let new_prefix = if is_tail { "" } else { "│   " };
            let child_prefix = format!("{prefix}{new_prefix}");

            let left_idx = idx * 2 + 1;
            let right_idx = idx * 2 + 2;

            let left_exists = left_idx < self.len;
            let right_exists = right_idx < self.len;

            if left_exists {
                self._tree_print(left_idx, &child_prefix, !right_exists, output);
            }
            if right_exists {
                self._tree_print(right_idx, &child_prefix, true, output);
            }
        }
    }
}

impl<VAL: ValueType> Display for TopKHeap<VAL> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut output = String::new();
        if !self.heap.is_empty() {
            self._tree_print(0, "", true, &mut output);
        }
        write!(f, "{output}")
    }
}

impl<VAL: ValueType> Debug for HeapItem<VAL> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("bucket=")?;
        Debug::fmt(&self.map_idx, f)?;
        f.write_str(" val=")?;
        Debug::fmt(&self.val, f)?;
        f.write_str("\n")?;
        Ok(())
    }
}

impl<VAL: ValueType> Eq for HeapItem<VAL> {}

impl<VAL: ValueType> PartialEq<Self> for HeapItem<VAL> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<VAL: ValueType> PartialOrd<Self> for HeapItem<VAL> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<VAL: ValueType> Ord for HeapItem<VAL> {
    fn cmp(&self, other: &Self) -> Ordering {
        let res = self.val.comp(&other.val);
        if res != Ordering::Equal {
            return res;
        }
        self.map_idx.cmp(&other.map_idx)
    }
}

macro_rules! compare_float {
    ($($t:ty),+) => {
        $(impl Comparable for Option<$t> {
            fn comp(&self, other: &Self) -> Ordering {
                match (self, other) {
                    (Some(me), Some(other)) => me.total_cmp(other),
                    (Some(_), None) => Ordering::Greater,
                    (None, Some(_)) => Ordering::Less,
                    (None, None) => Ordering::Equal,
                }
            }
        })+

        $(impl Comparable for $t {
            fn comp(&self, other: &Self) -> Ordering {
                self.total_cmp(other)
            }
        })+
    };
}

macro_rules! compare_integer {
    ($($t:ty),+) => {
        $(impl Comparable for Option<$t> {
            fn comp(&self, other: &Self) -> Ordering {
                self.cmp(other)
            }
        })+

        $(impl Comparable for $t {
            fn comp(&self, other: &Self) -> Ordering {
                self.cmp(other)
            }
        })+
    };
}

compare_integer!(i8, i16, i32, i64, i128, i256);
compare_integer!(u8, u16, u32, u64);
compare_integer!(IntervalDayTime, IntervalMonthDayNano);
compare_float!(f16, f32, f64);

/// Returns true if the given data type can be stored in a top-K aggregation heap.
///
/// Supported types include Arrow primitives (integers, floats, decimals, intervals)
/// and UTF-8 strings (`Utf8`, `LargeUtf8`, `Utf8View`). This is used internally by
/// `PriorityMap::supports()` to validate aggregate value type compatibility.
pub fn is_supported_heap_type(vt: &DataType) -> bool {
    vt.is_primitive()
        || matches!(
            vt,
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
        )
}

pub fn new_heap(
    limit: usize,
    desc: bool,
    vt: DataType,
) -> Result<Box<dyn ArrowHeap + Send>> {
    macro_rules! downcast_helper {
        ($vt:ty, $d:ident) => {
            return Ok(Box::new(PrimitiveHeap::<$vt>::new(limit, desc, vt)))
        };
    }

    downcast_primitive! {
        vt => (downcast_helper, vt),
        DataType::Utf8 => return Ok(Box::new(StringHeap::<StringArray>::new(limit, desc))),
        DataType::LargeUtf8 => return Ok(Box::new(StringHeap::<LargeStringArray>::new(limit, desc))),
        DataType::Utf8View => return Ok(Box::new(StringHeap::<StringViewArray>::new(limit, desc))),
        _ => {}
    }

    Err(exec_datafusion_err!(
        "Unsupported TopK aggregate value type: {vt:?}"
    ))
}

#[cfg(test)]
mod tests {
    use insta::assert_snapshot;

    use super::*;

    #[test]
    fn should_append() -> Result<()> {
        let mut map = vec![];
        let mut heap = TopKHeap::new(10, false);
        heap.append_or_replace(&1, 1, &mut map);

        let actual = heap.to_string();
        assert_snapshot!(actual, @"val=1 idx=0, bucket=1");

        Ok(())
    }

    #[test]
    fn should_heapify_up() -> Result<()> {
        let mut map = vec![];
        let mut heap = TopKHeap::new(10, false);

        heap.append_or_replace(&1, 1, &mut map);
        assert_eq!(map, vec![]);

        heap.append_or_replace(&2, 2, &mut map);
        assert_eq!(map, vec![(2, 0), (1, 1)]);

        let actual = heap.to_string();
        assert_snapshot!(actual, @r"
        val=2 idx=0, bucket=2
        └── val=1 idx=1, bucket=1
        ");

        Ok(())
    }

    #[test]
    fn should_heapify_down() -> Result<()> {
        let mut map = vec![];
        let mut heap = TopKHeap::new(3, false);

        heap.append_or_replace(&1, 1, &mut map);
        heap.append_or_replace(&2, 2, &mut map);
        heap.append_or_replace(&3, 3, &mut map);
        let actual = heap.to_string();
        assert_snapshot!(actual, @r"
        val=3 idx=0, bucket=3
        ├── val=1 idx=1, bucket=1
        └── val=2 idx=2, bucket=2
        ");

        let mut map = vec![];
        heap.append_or_replace(&0, 0, &mut map);
        let actual = heap.to_string();
        assert_snapshot!(actual, @r"
        val=2 idx=0, bucket=2
        ├── val=1 idx=1, bucket=1
        └── val=0 idx=2, bucket=0
        ");
        assert_eq!(map, vec![(2, 0), (0, 2)]);

        Ok(())
    }

    #[test]
    fn should_replace() -> Result<()> {
        let mut map = vec![];
        let mut heap = TopKHeap::new(4, false);

        heap.append_or_replace(&1, 1, &mut map);
        heap.append_or_replace(&2, 2, &mut map);
        heap.append_or_replace(&3, 3, &mut map);
        heap.append_or_replace(&4, 4, &mut map);
        let actual = heap.to_string();
        assert_snapshot!(actual, @r"
        val=4 idx=0, bucket=4
        ├── val=3 idx=1, bucket=3
        │   └── val=1 idx=3, bucket=1
        └── val=2 idx=2, bucket=2
        ");

        let mut map = vec![];
        heap.replace_if_better(&0, 1, &mut map);
        let actual = heap.to_string();
        assert_snapshot!(actual, @r"
        val=4 idx=0, bucket=4
        ├── val=1 idx=1, bucket=1
        │   └── val=0 idx=3, bucket=3
        └── val=2 idx=2, bucket=2
        ");
        assert_eq!(map, vec![(1, 1), (3, 3)]);

        Ok(())
    }

    #[test]
    fn should_find_worst() -> Result<()> {
        let mut map = vec![];
        let mut heap = TopKHeap::new(10, false);

        heap.append_or_replace(&1, 1, &mut map);
        heap.append_or_replace(&2, 2, &mut map);

        let actual = heap.to_string();
        assert_snapshot!(actual, @r"
        val=2 idx=0, bucket=2
        └── val=1 idx=1, bucket=1
        ");

        assert_eq!(heap.worst_val(), Some(&2));
        assert_eq!(heap.worst_map_idx(), 2);

        Ok(())
    }

    #[test]
    fn should_drain() -> Result<()> {
        let mut map = vec![];
        let mut heap = TopKHeap::new(10, false);

        heap.append_or_replace(&1, 1, &mut map);
        heap.append_or_replace(&2, 2, &mut map);

        let actual = heap.to_string();
        assert_snapshot!(actual, @r"
        val=2 idx=0, bucket=2
        └── val=1 idx=1, bucket=1
        ");

        let (vals, map_idxs) = heap.drain();
        assert_eq!(vals, vec![1, 2]);
        assert_eq!(map_idxs, vec![1, 2]);
        assert_eq!(heap.len(), 0);

        Ok(())
    }

    // TODO: test TopKHeap of String?
}
