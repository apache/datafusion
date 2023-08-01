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

//! A memory-conscious aggregation implementation that limits group buckets to a fixed number

use crate::physical_plan::aggregates::{
    aggregate_expressions, evaluate_group_by, evaluate_many, AggregateExec,
    PhysicalGroupBy,
};
use crate::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use ahash::RandomState;
use arrow::util::pretty::print_batches;
use arrow_array::cast::AsArray;
use arrow_array::downcast_primitive;
use arrow_array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, PrimitiveArray, RecordBatch,
    StringArray,
};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use futures::stream::{Stream, StreamExt};
use hashbrown::raw::RawTable;
use itertools::Itertools;
use log::{trace, Level};
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct GroupedTopKAggregateStream {
    partition: usize,
    row_count: usize,
    started: bool,
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    aggregate_arguments: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    group_by: PhysicalGroupBy,
    aggregator: Box<dyn LimitedAggregator>,
}

impl GroupedTopKAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: Arc<TaskContext>,
        partition: usize,
        limit: usize,
    ) -> Result<Self> {
        let agg_schema = Arc::clone(&agg.schema);
        let group_by = agg.group_by.clone();

        let input = agg.input.execute(partition, Arc::clone(&context))?;

        let aggregate_arguments =
            aggregate_expressions(&agg.aggr_expr, &agg.mode, group_by.expr.len())?;

        let (val_field, descending) = agg
            .get_minmax_desc()
            .ok_or_else(|| DataFusionError::Execution("Min/max required".to_string()))?;

        let vt = val_field.data_type().clone();
        let ag = new_group_values(limit, descending, vt)?;

        Ok(GroupedTopKAggregateStream {
            partition,
            started: false,
            row_count: 0,
            schema: agg_schema,
            input,
            aggregate_arguments,
            group_by,
            aggregator: ag,
        })
    }
}

pub fn new_group_values(
    limit: usize,
    desc: bool,
    vt: DataType,
) -> Result<Box<dyn LimitedAggregator>> {
    macro_rules! downcast_helper {
        ($vt:ty, $d:ident) => {
            return Ok(Box::new(PrimitiveAggregator::<$vt>::new(
                limit,
                limit * 10,
                desc,
            )))
        };
    }

    downcast_primitive! {
        vt => (downcast_helper, vt),
        _ => {}
    }

    Err(DataFusionError::Execution(format!(
        "Can't group type: {vt:?}"
    )))
}

impl RecordBatchStream for GroupedTopKAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub trait LimitedAggregator: Send {
    fn intern(&mut self, ids: ArrayRef, vals: ArrayRef) -> Result<()>;
    fn emit(&mut self) -> Result<Vec<ArrayRef>>;
    fn is_empty(&self) -> bool;
}

pub trait ValueType: ArrowNativeTypeOp + Clone {}

impl<T> ValueType for T where T: ArrowNativeTypeOp + Clone {}

pub trait KeyType: Clone + Eq + Hash {}

impl<T> KeyType for T where T: Clone + Eq + Hash {}

struct PrimitiveAggregator<VAL: ArrowPrimitiveType>
where
    <VAL as ArrowPrimitiveType>::Native: Clone,
{
    priority_map: PriorityMap<Option<String>, VAL::Native>,
}

impl<VAL: ArrowPrimitiveType> PrimitiveAggregator<VAL>
where
    <VAL as ArrowPrimitiveType>::Native: Clone,
{
    pub fn new(limit: usize, capacity: usize, descending: bool) -> Self {
        Self {
            priority_map: PriorityMap::new(limit, capacity, descending),
        }
    }
}

unsafe impl<VAL: ArrowPrimitiveType> Send for PrimitiveAggregator<VAL> where
    <VAL as ArrowPrimitiveType>::Native: Clone
{
}

impl<VAL: ArrowPrimitiveType> LimitedAggregator for PrimitiveAggregator<VAL>
where
    <VAL as ArrowPrimitiveType>::Native: Clone,
{
    fn intern(&mut self, ids: ArrayRef, vals: ArrayRef) -> Result<()> {
        let ids = ids.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            DataFusionError::Execution("Expected StringArray".to_string())
        })?;
        let vals = vals.as_primitive::<VAL>();
        let null_count = vals.null_count();
        let desc = self.priority_map.desc;
        for row_idx in 0..ids.len() {
            if null_count > 0 && vals.is_null(row_idx) {
                continue;
            }
            let val = vals.value(row_idx);
            let id = if ids.is_null(row_idx) {
                None
            } else {
                // Check goes here, because it is generalizable between str/String and Row/OwnedRow
                let id = ids.value(row_idx);
                if self.priority_map.is_full() {
                    if let Some(worst) = self.priority_map.worst_val() {
                        if desc {
                            if val < *worst {
                                continue;
                            }
                        } else {
                            if val > *worst {
                                continue;
                            }
                        }
                    }
                }
                Some(id.to_string())
            };

            self.priority_map.insert(id, val)?;
        }
        Ok(())
    }

    fn emit(&mut self) -> Result<Vec<ArrayRef>> {
        let (keys, vals): (Vec<_>, Vec<_>) =
            self.priority_map.drain().into_iter().unzip();
        let keys = Arc::new(StringArray::from(keys));
        let vals = Arc::new(PrimitiveArray::<VAL>::from_iter_values(vals));
        Ok(vec![keys, vals])
    }

    fn is_empty(&self) -> bool {
        self.priority_map.is_empty()
    }
}

impl Stream for GroupedTopKAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        while let Poll::Ready(res) = self.input.poll_next_unpin(cx) {
            match res {
                // got a batch, convert to rows and append to our TreeMap
                Some(Ok(batch)) => {
                    self.started = true;
                    trace!(
                        "partition {} has {} rows and got batch with {} rows",
                        self.partition,
                        self.row_count,
                        batch.num_rows()
                    );
                    if log::log_enabled!(Level::Trace) && batch.num_rows() < 20 {
                        print_batches(&[batch.clone()])?;
                    }
                    self.row_count += batch.num_rows();
                    let batches = &[batch];
                    let group_by_values =
                        evaluate_group_by(&self.group_by, batches.first().unwrap())?;
                    let group_by_values =
                        group_by_values.into_iter().last().expect("values");
                    let group_by_values =
                        group_by_values.into_iter().last().expect("values");
                    let input_values = evaluate_many(
                        &self.aggregate_arguments,
                        batches.first().unwrap(),
                    )?;
                    let input_values = match input_values.as_slice() {
                        [] => {
                            Err(DataFusionError::Execution("vals required".to_string()))?
                        }
                        [vals] => vals,
                        _ => {
                            Err(DataFusionError::Execution("1 val required".to_string()))?
                        }
                    };
                    let input_values = match input_values.as_slice() {
                        [] => {
                            Err(DataFusionError::Execution("vals required".to_string()))?
                        }
                        [vals] => vals,
                        _ => {
                            Err(DataFusionError::Execution("1 val required".to_string()))?
                        }
                    }
                    .clone();

                    // iterate over each column of group_by values
                    (*self.aggregator).intern(group_by_values, input_values)?;
                }
                // inner is done, emit all rows and switch to producing output
                None => {
                    if self.aggregator.is_empty() {
                        trace!("partition {} emit None", self.partition);
                        return Poll::Ready(None);
                    }
                    let cols = self.aggregator.emit()?;
                    let batch = RecordBatch::try_new(self.schema.clone(), cols)?;
                    trace!(
                        "partition {} emit batch with {} rows",
                        self.partition,
                        batch.num_rows()
                    );
                    if log::log_enabled!(Level::Trace) {
                        print_batches(&[batch.clone()])?;
                    }
                    return Poll::Ready(Some(Ok(batch)));
                }
                // inner had error, return to caller
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
        Poll::Pending
    }
}

/// A dual data structure consisting of a bi-directionally linked Map & Heap
///
/// The implementation is optimized for performance because `insert()` will be called on billions of
/// rows. Because traversing between the map & heap will happen frequently, it is important to
/// be highly optimized.
///
/// In order to quickly traverse from heap to map, we use the unsafe raw indexes that `RawTable`
/// exposes to us to avoid needing to find buckets based on their hash.
pub struct PriorityMap<ID: KeyType, VAL: ValueType> {
    limit: usize,
    desc: bool,
    rnd: RandomState,
    id_to_hi: RawTable<MapItem<ID, VAL>>,
    root: Option<*mut HeapItem<VAL>>,
}

pub struct MapItem<ID: KeyType, VAL: ValueType> {
    hash: u64,
    pub id: ID,
    hi: *mut HeapItem<VAL>, // TODO: *mut void
}

impl<ID: KeyType, VAL: ValueType> MapItem<ID, VAL> {
    pub fn new(hash: u64, id: ID, val: *mut HeapItem<VAL>) -> Self {
        Self { hash, id, hi: val }
    }
}

pub struct HeapItem<VAL: ValueType> {
    val: VAL,
    buk_idx: usize,
    parent: Option<*mut HeapItem<VAL>>,
    left: Option<*mut HeapItem<VAL>>,
    right: Option<*mut HeapItem<VAL>>,
}

impl<VAL: ValueType> HeapItem<VAL> {
    pub fn new(val: VAL, buk_idx: usize) -> Self {
        Self {
            val,
            buk_idx,
            parent: None,
            left: None,
            right: None,
        }
    }

    #[cfg(test)]
    pub fn tree_print(&self, builder: &mut ptree::TreeBuilder) {
        unsafe {
            let ptext = self
                .parent
                .map(|p| (*p).buk_idx.to_string())
                .unwrap_or("".to_string());
            let label = format!(
                "bucket={:?} val={:?} parent={}",
                self.buk_idx, self.val, ptext
            );
            builder.begin_child(label);
        }
        for child in [&self.left, &self.right] {
            if let Some(child) = child {
                unsafe { (**child).tree_print(builder) }
            } else {
                builder.add_empty_child("None".to_string());
            }
        }
        builder.end_child();
    }
}

impl<VAL: ValueType> Debug for HeapItem<VAL> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("bucket=")?;
        self.buk_idx.fmt(f)?;
        f.write_str(" val=")?;
        self.val.fmt(f)?;
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
        let res = self.val.compare(other.val);
        if res != Ordering::Equal {
            return res;
        }
        self.buk_idx.cmp(&other.buk_idx)
    }
}

impl<ID: KeyType, VAL: ValueType> PriorityMap<ID, VAL>
where
    VAL: PartialEq<VAL>,
{
    pub fn new(limit: usize, capacity: usize, desc: bool) -> Self {
        Self {
            limit,
            desc,
            rnd: Default::default(),
            id_to_hi: RawTable::with_capacity(capacity),
            root: None,
        }
    }

    pub fn insert(&mut self, new_id: ID, new_val: VAL) -> Result<()> {
        let is_full = self.is_full();
        let desc = self.desc;
        assert!(self.len() <= self.limit, "Overflow");

        // if we're full, and the new val is worse than all our values, just bail
        if is_full {
            let worst_val = self.worst_val().expect("Missing value!");
            if (!desc && new_val > *worst_val) || (desc && new_val < *worst_val) {
                return Ok(());
            }
        }

        // handle new groups we haven't seen yet
        let new_hash = self.rnd.hash_one(&new_id);
        let old_bucket = match self.id_to_hi.find(new_hash, |mi| new_id == mi.id) {
            None => {
                // we're full and this is a better value, so remove the worst from the map
                if is_full {
                    let worst_hi = self.root.expect("Missing value!");
                    unsafe {
                        self.id_to_hi
                            .erase(self.id_to_hi.bucket((*worst_hi).buk_idx))
                    };
                }

                // add the new group to the map
                let new_hi = Box::into_raw(Box::new(HeapItem::new(new_val, 0)));
                let mi = MapItem::new(new_hash, new_id, new_hi);
                let bucket = self.id_to_hi.try_insert_no_grow(new_hash, mi);
                let bucket = match bucket {
                    Ok(bucket) => bucket,
                    Err(new_item) => {
                        // this should basically never happen, but if it does, we must rebuild
                        println!("rebuilding");
                        let bucket =
                            self.id_to_hi.insert(new_hash, new_item, |mi| mi.hash);
                        unsafe {
                            for bucket in self.id_to_hi.iter() {
                                let existing_mi = bucket.as_mut();
                                let existing_hi = &mut *existing_mi.hi;
                                existing_hi.buk_idx = self.id_to_hi.bucket_index(&bucket);
                            }
                        }
                        bucket
                    }
                };
                unsafe {
                    (*new_hi).buk_idx = self.id_to_hi.bucket_index(&bucket);
                }

                // update heap
                if let Some(root) = self.root {
                    let root = unsafe { &mut *root };
                    if self.is_full() {
                        // replace top node
                        self.take_children(new_hi, root);
                        if let Some(_old_root) = self.root.replace(new_hi) {
                            // TODO: audit all the places to free things
                        }
                        self.heapify_down(new_hi);
                    } else {
                        // append to end of tree
                        println!("Appending child at {}", self.len() - 1);
                        let old = self.put_child(
                            root,
                            new_hi.clone(),
                            tree_path(self.len() - 1),
                        );
                        assert!(old.is_none(), "Overwrote node!");
                        self.heapify_up(new_hi);
                    }
                } else {
                    // first entry ever
                    self.root = Some(new_hi);
                }
                return Ok(());
            }
            Some(bucket) => bucket,
        };

        // this is a value for an existing group
        let existing_mi = unsafe { old_bucket.as_mut() };
        let existing_hi = unsafe { &mut *existing_mi.hi };
        if (!desc && new_val >= existing_hi.val) || (desc && new_val <= existing_hi.val) {
            // worse than the existing value _for this group_
            return Ok(());
        }

        // update heap
        existing_hi.val = new_val;

        Ok(())
    }

    fn take_children(
        &self,
        new_parent: *mut HeapItem<VAL>,
        old_parent: &mut HeapItem<VAL>,
    ) {
        unsafe {
            (*new_parent).left = old_parent.left.take();
            (*new_parent).right = old_parent.right.take();
            (*new_parent).left.map(|n| (*n).parent.replace(new_parent));
            (*new_parent).right.map(|n| (*n).parent.replace(new_parent));
        }
    }

    pub fn put_child(
        &self,
        node_ptr: *mut HeapItem<VAL>,
        new_child: *mut HeapItem<VAL>,
        mut path: Vec<bool>,
    ) -> Option<*mut HeapItem<VAL>> {
        let dir = path.pop().expect("empty path");
        if path.is_empty() {
            let old_parent = unsafe { (*new_child).parent.replace(node_ptr) };
            assert!(old_parent.is_none(), "Replaced parent!");
            if dir {
                unsafe { (*node_ptr).right.replace(new_child) }
            } else {
                unsafe { (*node_ptr).left.replace(new_child) }
            }
        } else {
            if dir {
                unsafe { self.put_child((*node_ptr).right.unwrap(), new_child, path) }
            } else {
                unsafe { self.put_child((*node_ptr).left.unwrap(), new_child, path) }
            }
        }
    }

    fn swap(&mut self, child_ptr: *mut HeapItem<VAL>, parent_ptr: *mut HeapItem<VAL>) {
        let child = unsafe { &mut *child_ptr };
        let parent = unsafe { &mut *parent_ptr };

        if child.parent != Some(parent_ptr) {
            panic!("Child is not of this parent");
        }

        // store the grand parents and grand children - they are outside the swap
        let grand_left = child.left.take();
        let grand_right = child.right.take();
        let grand_parent = parent.parent.take();

        // transfer parent's children to child
        if parent.left == Some(child_ptr) {
            child.left = Some(parent);
            child.right = parent.right.take();
            let _ = unsafe { child.right.map(|n| (*n).parent.replace(child)) };
        } else if parent.right == Some(child_ptr) {
            child.right = Some(parent);
            child.left = parent.left.take();
            let _ = unsafe { child.left.map(|n| (*n).parent.replace(child)) };
        } else {
            panic!("Child is illegitimate");
        }
        parent.parent = Some(child_ptr);

        // transfer child's children to parent
        parent.left = grand_left;
        parent.right = grand_right;
        let _ = unsafe { grand_left.map(|n| (*n).parent.replace(parent)) };
        let _ = unsafe { grand_right.map(|n| (*n).parent.replace(parent)) };

        // make the child of the grandparent
        if let Some(gp_ptr) = grand_parent {
            let gp = unsafe { &mut *gp_ptr };
            if gp.left == Some(parent_ptr) {
                gp.left = Some(child);
            } else if gp.right == Some(parent_ptr) {
                gp.right = Some(child);
            } else {
                panic!("Parent is illegitimate");
            }
            assert_eq!(child.parent.replace(gp_ptr), Some(parent_ptr));
        } else {
            let _ = child.parent.take();
            self.root = Some(child);
        }
    }

    fn heapify_up(&mut self, node_ptr: *mut HeapItem<VAL>) {
        let node = unsafe { &mut *node_ptr };
        let parent_ptr = match node.parent {
            None => return,
            Some(parent) => parent,
        };
        let parent = unsafe { &mut *parent_ptr };
        if !self.desc && node.val <= parent.val {
            return;
        }
        if self.desc && node.val >= parent.val {
            return;
        }

        self.swap(node, parent_ptr);
        self.heapify_up(parent_ptr);
    }

    fn heapify_down(&mut self, node: *mut HeapItem<VAL>) {
        unsafe {
            let mut best_node = node;
            if let Some(child) = (*node).left {
                if !self.desc && (*child).val > (*best_node).val {
                    best_node = child;
                }
                if self.desc && (*child).val < (*best_node).val {
                    best_node = child;
                }
            }
            if let Some(child) = (*node).right {
                if !self.desc && (*child).val > (*best_node).val {
                    best_node = child;
                }
                if self.desc && (*child).val < (*best_node).val {
                    best_node = child;
                }
            }
            if node != best_node {
                self.swap(best_node, node);
                self.heapify_down(node);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.id_to_hi.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_to_hi.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.len() >= self.limit
    }

    pub fn worst_val(&mut self) -> Option<&VAL> {
        self.root.map(|hi| unsafe { &(*hi).val })
    }

    pub fn drain(&mut self) -> Vec<(ID, VAL)> {
        // TODO: drain heap to sort
        let tups: Vec<_> = unsafe {
            self.id_to_hi
                .drain()
                .map(|mi| (mi.id, (*mi.hi).val))
                .collect()
        };
        let mut tups: Vec<_> = tups
            .into_iter()
            .sorted_by(|a, b| a.1.compare(b.1))
            .collect();
        if self.desc {
            tups.reverse();
        }
        tups
    }

    #[cfg(test)]
    pub fn tree_print(&self) -> String {
        let mut builder = if let Some(root) = &self.root {
            let mut builder = ptree::TreeBuilder::new("BinaryHeap".to_string());
            unsafe { (**root).tree_print(&mut builder) };
            builder
        } else {
            let builder = ptree::TreeBuilder::new("Empty BinaryHeap".to_string());
            builder
        };
        let mut actual = Vec::new();
        ptree::write_tree(&builder.build(), &mut actual).unwrap();
        String::from_utf8(actual).unwrap()
    }
}

fn tree_path(mut idx: usize) -> Vec<bool> {
    let mut path = vec![];
    while idx != 0 {
        path.push(idx % 2 == 0);
        idx = (idx - 1) / 2;
    }
    path.reverse();
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use arrow::util::pretty::pretty_format_batches;
    use arrow_array::types::Int64Type;
    use arrow_array::Int64Array;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;

    #[test]
    fn should_swap() -> Result<()> {
        let mut map = PriorityMap::<String, i64>::new(10, 100, false);
        let root = Box::into_raw(Box::new(HeapItem::new(1, 1)));
        let child = Box::into_raw(Box::new(HeapItem::new(2, 2)));
        unsafe {
            (*root).left = Some(child);
            (*child).parent = Some(root);
        }
        map.root = Some(root);
        let actual = map.tree_print();
        let expected = r#"
BinaryHeap
└─ bucket=1 val=1 parent=
   ├─ bucket=2 val=2 parent=1
   │  ├─ None
   │  └─ None
   └─ None
        "#
        .trim();
        assert_eq!(actual.trim(), expected);

        // exercise
        map.swap(child, root);

        // assert
        let actual = map.tree_print();
        let expected = r#"
BinaryHeap
└─ bucket=2 val=2 parent=
   ├─ bucket=1 val=1 parent=2
   │  ├─ None
   │  └─ None
   └─ None
        "#
        .trim();
        assert_eq!(actual.trim(), expected);

        Ok(())
    }

    #[test]
    fn should_swap_grandchildren() -> Result<()> {
        let mut map = PriorityMap::<String, i64>::new(10, 100, false);
        let root = Box::into_raw(Box::new(HeapItem::new(1, 1)));
        let l = Box::into_raw(Box::new(HeapItem::new(2, 2)));
        let r = Box::into_raw(Box::new(HeapItem::new(5, 5)));
        let ll = Box::into_raw(Box::new(HeapItem::new(3, 3)));
        let lr = Box::into_raw(Box::new(HeapItem::new(4, 4)));
        unsafe {
            (*root).left = Some(l);
            (*l).parent = Some(root);

            (*root).right = Some(r);
            (*r).parent = Some(root);

            (*l).left = Some(ll);
            (*ll).parent = Some(l);

            (*l).right = Some(lr);
            (*lr).parent = Some(l);
        }
        map.root = Some(root);
        let actual = map.tree_print();
        let expected = r#"
BinaryHeap
└─ bucket=1 val=1 parent=
   ├─ bucket=2 val=2 parent=1
   │  ├─ bucket=3 val=3 parent=2
   │  │  ├─ None
   │  │  └─ None
   │  └─ bucket=4 val=4 parent=2
   │     ├─ None
   │     └─ None
   └─ bucket=5 val=5 parent=1
      ├─ None
      └─ None
        "#
        .trim();
        assert_eq!(actual.trim(), expected);

        // exercise
        map.swap(l, root);

        // assert
        let actual = map.tree_print();
        let expected = r#"
BinaryHeap
└─ bucket=2 val=2 parent=
   ├─ bucket=1 val=1 parent=2
   │  ├─ bucket=3 val=3 parent=1
   │  │  ├─ None
   │  │  └─ None
   │  └─ bucket=4 val=4 parent=1
   │     ├─ None
   │     └─ None
   └─ bucket=5 val=5 parent=2
      ├─ None
      └─ None
        "#
        .trim();
        assert_eq!(actual.trim(), expected);

        Ok(())
    }

    #[test]
    fn should_append() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let mut agg = PrimitiveAggregator::<Int64Type>::new(1, 10, false);
        agg.intern(ids, vals)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        let expected = r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 1        | 1            |
+----------+--------------+
        "#
        .trim();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn should_ignore_higher_group() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1", "2"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let mut agg = PrimitiveAggregator::<Int64Type>::new(1, 10, false);
        agg.intern(ids, vals)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        let expected = r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 1        | 1            |
+----------+--------------+
        "#
        .trim();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn should_ignore_lower_group() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["2", "1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![2, 1]));
        let mut agg = PrimitiveAggregator::<Int64Type>::new(1, 10, true);
        agg.intern(ids, vals)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        let expected = r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 2        | 2            |
+----------+--------------+
        "#
        .trim();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn should_ignore_higher_same_group() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1", "1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let mut agg = PrimitiveAggregator::<Int64Type>::new(2, 10, false);
        agg.intern(ids, vals)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        let expected = r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 1        | 1            |
+----------+--------------+
        "#
        .trim();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn should_ignore_lower_same_group() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1", "1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![2, 1]));
        let mut agg = PrimitiveAggregator::<Int64Type>::new(2, 10, true);
        agg.intern(ids, vals)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        let expected = r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 1        | 2            |
+----------+--------------+
        "#
        .trim();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn should_accept_lower_group() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["2", "1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![2, 1]));
        let mut agg = PrimitiveAggregator::<Int64Type>::new(1, 10, false);
        agg.intern(ids, vals)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        let expected = r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 1        | 1            |
+----------+--------------+
        "#
        .trim();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn should_accept_higher_group() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1", "2"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let mut agg = PrimitiveAggregator::<Int64Type>::new(1, 10, true);
        agg.intern(ids, vals)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        let expected = r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 2        | 2            |
+----------+--------------+
        "#
        .trim();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn should_accept_lower_for_group() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1", "1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![2, 1]));
        let mut agg = PrimitiveAggregator::<Int64Type>::new(2, 10, false);
        agg.intern(ids, vals)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        let expected = r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 1        | 1            |
+----------+--------------+
        "#
        .trim();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn should_accept_higher_for_group() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1", "1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let mut agg = PrimitiveAggregator::<Int64Type>::new(2, 10, true);
        agg.intern(ids, vals)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        let expected = r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 1        | 2            |
+----------+--------------+
        "#
        .trim();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn should_handle_null_ids() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec![Some("1"), None, None]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let mut agg = PrimitiveAggregator::<Int64Type>::new(2, 10, true);
        agg.intern(ids, vals)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        let expected = r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
|          | 3            |
| 1        | 1            |
+----------+--------------+
        "#
        .trim();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn should_ignore_null_vals() -> Result<()> {
        let ids: ArrayRef =
            Arc::new(StringArray::from(vec![Some("1"), Some("1"), Some("3")]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
        let mut agg = PrimitiveAggregator::<Int64Type>::new(2, 10, false);
        agg.intern(ids, vals)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        let expected = r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 1        | 1            |
| 3        | 3            |
+----------+--------------+
        "#
        .trim();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn should_retain_state_after_resize() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1", "2", "3", "4", "5"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let mut agg = PrimitiveAggregator::<Int64Type>::new(5, 3, false);
        agg.intern(ids, vals)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        let expected = r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 1        | 1            |
| 2        | 2            |
| 3        | 3            |
| 4        | 4            |
| 5        | 5            |
+----------+--------------+
        "#
        .trim();
        assert_eq!(actual, expected);

        Ok(())
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("timestamp_ms", DataType::Int64, true),
        ]))
    }
}
