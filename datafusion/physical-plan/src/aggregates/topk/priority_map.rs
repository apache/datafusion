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

//! A `Map<K, V>` / `PriorityQueue` combo that evicts the worst values after reaching `capacity`

use crate::aggregates::topk::hash_table::{new_hash_table, ArrowHashTable};
use crate::aggregates::topk::heap::{new_heap, ArrowHeap};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion_common::Result;

/// A `Map<K, V>` / `PriorityQueue` combo that evicts the worst values after reaching `capacity`
pub struct PriorityMap {
    map: Box<dyn ArrowHashTable + Send>,
    heap: Box<dyn ArrowHeap + Send>,
    capacity: usize,
    mapper: Vec<(usize, usize)>,
}

impl PriorityMap {
    pub fn new(
        key_type: DataType,
        val_type: DataType,
        capacity: usize,
        descending: bool,
    ) -> Result<Self> {
        Ok(Self {
            map: new_hash_table(capacity, key_type)?,
            heap: new_heap(capacity, descending, val_type)?,
            capacity,
            mapper: Vec::with_capacity(capacity),
        })
    }

    pub fn set_batch(&mut self, ids: ArrayRef, vals: ArrayRef) {
        self.map.set_batch(ids);
        self.heap.set_batch(vals);
    }

    pub fn insert(&mut self, row_idx: usize) -> Result<()> {
        assert!(self.map.len() <= self.capacity, "Overflow");

        // if we're full, and the new val is worse than all our values, just bail
        if self.heap.is_worse(row_idx) {
            return Ok(());
        }
        let map = &mut self.mapper;

        // handle new groups we haven't seen yet
        map.clear();
        let replace_idx = self.heap.worst_map_idx();
        // JUSTIFICATION
        //  Benefit:  ~15% speedup + required to index into RawTable from binary heap
        //  Soundness: replace_idx kept valid during resizes
        let (map_idx, did_insert) =
            unsafe { self.map.find_or_insert(row_idx, replace_idx, map) };
        if did_insert {
            self.heap.renumber(map);
            map.clear();
            self.heap.insert(row_idx, map_idx, map);
            // JUSTIFICATION
            //  Benefit:  ~15% speedup + required to index into RawTable from binary heap
            //  Soundness: the map was created on the line above, so all the indexes should be valid
            unsafe { self.map.update_heap_idx(map) };
            return Ok(());
        };

        // this is a value for an existing group
        map.clear();
        // JUSTIFICATION
        //  Benefit:  ~15% speedup + required to index into RawTable from binary heap
        //  Soundness: map_idx was just found, so it is valid
        let heap_idx = unsafe { self.map.heap_idx_at(map_idx) };
        self.heap.replace_if_better(heap_idx, row_idx, map);
        // JUSTIFICATION
        //  Benefit:  ~15% speedup + required to index into RawTable from binary heap
        //  Soundness: the index map was just built, so it will be valid
        unsafe { self.map.update_heap_idx(map) };

        Ok(())
    }

    pub fn emit(&mut self) -> Result<Vec<ArrayRef>> {
        let (vals, map_idxs) = self.heap.drain();
        let ids = unsafe { self.map.take_all(map_idxs) };
        Ok(vec![ids, vals])
    }

    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{Field, Schema, SchemaRef};
    use arrow::util::pretty::pretty_format_batches;
    use std::sync::Arc;

    #[test]
    fn should_append() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 1, false)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;

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
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 1, false)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;
        agg.insert(1)?;

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
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 1, true)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;
        agg.insert(1)?;

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
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 2, false)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;
        agg.insert(1)?;

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
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 2, true)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;
        agg.insert(1)?;

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
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 1, false)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;
        agg.insert(1)?;

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
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 1, true)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;
        agg.insert(1)?;

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
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 2, false)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;
        agg.insert(1)?;

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
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 2, true)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;
        agg.insert(1)?;

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
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 2, true)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;
        agg.insert(1)?;
        agg.insert(2)?;

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

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("timestamp_ms", DataType::Int64, true),
        ]))
    }
}
