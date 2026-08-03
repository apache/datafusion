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

use crate::aggregates::topk::hash_table::{ArrowHashTable, InsertKind, new_hash_table};
use crate::aggregates::topk::heap::{ArrowHeap, new_heap};
use arrow::array::{ArrayRef, new_null_array};
use arrow::compute::concat;
use arrow::datatypes::DataType;
use datafusion_common::Result;

/// A `Map<K, V>` / `PriorityQueue` combo that evicts the worst values after reaching `capacity`
pub struct PriorityMap {
    map: Box<dyn ArrowHashTable + Send>,
    heap: Box<dyn ArrowHeap + Send>,
    capacity: usize,
    mapper: Vec<(usize, usize)>,
    val_type: DataType,
    /// Mirror of the map's all-NULL group count, kept as a plain field so the
    /// per-row `insert` path can check it without a `dyn` call (measured to
    /// regress the topk_aggregate benchmarks when read through the trait)
    null_count: usize,
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
            heap: new_heap(capacity, descending, val_type.clone())?,
            capacity,
            mapper: Vec::with_capacity(capacity),
            val_type,
            null_count: 0,
        })
    }

    pub fn set_batch(&mut self, ids: ArrayRef, vals: ArrayRef) {
        self.map.set_batch(ids);
        self.heap.set_batch(vals);
    }

    pub fn insert(&mut self, row_idx: usize) -> Result<()> {
        assert!(self.map.len() <= self.capacity, "Overflow");
        debug_assert_eq!(self.null_count, 0);

        // if we're full, and the new val is worse than all our values, just bail
        if self.heap.is_worse(row_idx) {
            return Ok(());
        }
        self.insert_eligible(row_idx)
    }

    /// Insert a value while all-NULL groups are being tracked. This is kept
    /// separate from [`Self::insert`] so the common no-NULL path does not pay
    /// for NULL bookkeeping on every row.
    pub fn insert_with_null_groups(&mut self, row_idx: usize) -> Result<()> {
        // valued groups are capped at `capacity`; up to `capacity` additional
        // all-NULL groups may be tracked alongside them
        assert!(self.map.len() <= 2 * self.capacity, "Overflow");

        if self.heap.is_worse(row_idx) {
            // A group that was registered as all-NULL now has a value that
            // loses to the current top-k: it can no longer reach the top-k,
            // but it must not be emitted with a NULL value either
            if self.null_count > 0 && self.map.remove_if_null(row_idx) {
                self.null_count -= 1;
            }
            return Ok(());
        }
        self.insert_eligible(row_idx)
    }

    fn insert_eligible(&mut self, row_idx: usize) -> Result<()> {
        let map = &mut self.mapper;

        // handle new groups we haven't seen yet
        map.clear();
        let replace_idx = self.heap.worst_map_idx();

        let (map_idx, kind) = self.map.find_or_insert(row_idx, replace_idx);
        if kind == InsertKind::ReplacedNull {
            self.null_count -= 1;
        }
        if kind != InsertKind::Existing {
            self.heap.insert(row_idx, map_idx, map);
            self.map.update_heap_idx(map);
            return Ok(());
        };

        // this is a value for an existing group
        map.clear();
        let heap_idx = self.map.heap_idx_at(map_idx);
        self.heap.replace_if_better(heap_idx, row_idx, map);
        self.map.update_heap_idx(map);

        Ok(())
    }

    pub fn has_null_groups(&self) -> bool {
        self.null_count > 0
    }

    /// Track a group whose aggregate values are all NULL, so it can be emitted
    /// with a NULL value. MIN/MAX ignore NULL inputs, but an all-NULL group
    /// must still appear in the aggregation output; such groups all tie on the
    /// sort key, so tracking up to `capacity` of them preserves top-k semantics.
    pub fn insert_null(&mut self, row_idx: usize) {
        assert!(self.map.len() <= 2 * self.capacity, "Overflow");
        if self.map.insert_null(row_idx) {
            self.null_count += 1;
        }
    }

    pub fn emit(&mut self) -> Result<Vec<ArrayRef>> {
        let (vals, mut map_idxs) = self.heap.drain();
        // Groups whose values are all NULL are tracked in the map only;
        // append them with a NULL value so they are not lost from the output
        let null_idxs = self.map.null_map_idxs();
        let vals = if null_idxs.is_empty() {
            vals
        } else {
            map_idxs.extend(null_idxs.iter().copied());
            let nulls = new_null_array(&self.val_type, null_idxs.len());
            concat(&[vals.as_ref(), nulls.as_ref()])?
        };
        let ids = self.map.take_all(map_idxs);
        self.null_count = 0;
        Ok(vec![ids, vals])
    }

    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
    };
    use arrow::datatypes::{Field, Schema, SchemaRef};
    use arrow::util::pretty::pretty_format_batches;
    use insta::assert_snapshot;
    use std::sync::Arc;

    #[test]
    fn should_append_with_utf8view() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringViewArray::from(vec!["1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let mut agg = PriorityMap::new(DataType::Utf8View, DataType::Int64, 1, false)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema_utf8view(), cols)?;
        let batch_schema = batch.schema();
        assert_eq!(batch_schema.fields[0].data_type(), &DataType::Utf8View);

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
    fn should_append_with_large_utf8() -> Result<()> {
        let ids: ArrayRef = Arc::new(LargeStringArray::from(vec!["1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let mut agg = PriorityMap::new(DataType::LargeUtf8, DataType::Int64, 1, false)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_large_schema(), cols)?;
        let batch_schema = batch.schema();
        assert_eq!(batch_schema.fields[0].data_type(), &DataType::LargeUtf8);

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
    fn should_append() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 1, false)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);

        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 1        | 1            |
        +----------+--------------+
        "
        );

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

        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 1        | 1            |
        +----------+--------------+
        "
        );

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
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 2        | 2            |
        +----------+--------------+
        "
        );

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
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 1        | 1            |
        +----------+--------------+
        "
        );

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
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 1        | 2            |
        +----------+--------------+
        "
        );

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
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 1        | 1            |
        +----------+--------------+
        "
        );

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
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 2        | 2            |
        +----------+--------------+
        "
        );

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
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 1        | 1            |
        +----------+--------------+
        "
        );

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
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 1        | 2            |
        +----------+--------------+
        "
        );

        Ok(())
    }

    #[test]
    fn should_track_lexicographic_min_utf8_value() -> Result<()> {
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 1]));
        let vals: ArrayRef = Arc::new(StringArray::from(vec!["zulu", "alpha"]));
        let mut agg = PriorityMap::new(DataType::Int64, DataType::Utf8, 1, false)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;
        agg.insert(1)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema_value(DataType::Utf8), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);

        assert_snapshot!(actual, @r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 1        | alpha        |
+----------+--------------+
        "#);

        Ok(())
    }

    #[test]
    fn should_track_lexicographic_max_utf8_value_desc() -> Result<()> {
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 1]));
        let vals: ArrayRef = Arc::new(StringArray::from(vec!["alpha", "zulu"]));
        let mut agg = PriorityMap::new(DataType::Int64, DataType::Utf8, 1, true)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;
        agg.insert(1)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema_value(DataType::Utf8), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);

        assert_snapshot!(actual, @r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 1        | zulu         |
+----------+--------------+
        "#);

        Ok(())
    }

    #[test]
    fn should_track_large_utf8_values() -> Result<()> {
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 1]));
        let vals: ArrayRef = Arc::new(LargeStringArray::from(vec!["zulu", "alpha"]));
        let mut agg = PriorityMap::new(DataType::Int64, DataType::LargeUtf8, 1, false)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;
        agg.insert(1)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema_value(DataType::LargeUtf8), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);

        assert_snapshot!(actual, @r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 1        | alpha        |
+----------+--------------+
        "#);

        Ok(())
    }

    #[test]
    fn should_track_utf8_view_values() -> Result<()> {
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 1]));
        let vals: ArrayRef = Arc::new(StringViewArray::from(vec!["alpha", "zulu"]));
        let mut agg = PriorityMap::new(DataType::Int64, DataType::Utf8View, 1, true)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;
        agg.insert(1)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema_value(DataType::Utf8View), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);

        assert_snapshot!(actual, @r#"
+----------+--------------+
| trace_id | timestamp_ms |
+----------+--------------+
| 1        | zulu         |
+----------+--------------+
        "#);

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
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        |          | 3            |
        | 1        | 1            |
        +----------+--------------+
        "
        );

        Ok(())
    }

    #[test]
    fn should_emit_all_null_groups() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1", "2"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![None, None]));
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 2, true)?;
        agg.set_batch(ids, vals);
        agg.insert_null(0);
        agg.insert_null(1);
        // re-registering an existing NULL group is a no-op
        agg.insert_null(0);

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 1        |              |
        | 2        |              |
        +----------+--------------+
        "
        );

        Ok(())
    }

    #[test]
    fn should_emit_null_groups_alongside_valued_groups() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1", "2", "3"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None, Some(3)]));
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 3, true)?;
        agg.set_batch(ids, vals);
        agg.insert(0)?;
        agg.insert_null(1);
        agg.insert_with_null_groups(2)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 1        | 7            |
        | 3        | 3            |
        | 2        |              |
        +----------+--------------+
        "
        );

        Ok(())
    }

    #[test]
    fn should_cap_null_groups_at_limit() -> Result<()> {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1", "2", "3", "4", "5"]));
        let vals: ArrayRef =
            Arc::new(Int64Array::from(vec![None, None, None, None, None]));
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 2, false)?;
        agg.set_batch(ids, vals);
        for row_idx in 0..5 {
            agg.insert_null(row_idx);
        }

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 1        |              |
        | 2        |              |
        +----------+--------------+
        "
        );

        Ok(())
    }

    #[test]
    fn should_convert_null_group_to_valued() -> Result<()> {
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 2, true)?;

        // group "1" only produces NULLs in the first batch
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        agg.set_batch(ids, vals);
        agg.insert_null(0);

        // group "1" produces a value in a later batch
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![5]));
        agg.set_batch(ids, vals);
        agg.insert_with_null_groups(0)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 1        | 5            |
        +----------+--------------+
        "
        );

        Ok(())
    }

    #[test]
    fn should_not_duplicate_valued_group_as_null() -> Result<()> {
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 2, false)?;

        // group "1" produces a value in the first batch
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![5]));
        agg.set_batch(ids, vals);
        agg.insert(0)?;

        // group "1" only produces NULLs in a later batch
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        agg.set_batch(ids, vals);
        agg.insert_null(0);

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 1        | 5            |
        +----------+--------------+
        "
        );

        Ok(())
    }

    #[test]
    fn should_evict_worst_when_converting_null_group() -> Result<()> {
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 1, true)?;

        // group "2" holds the single top-k slot
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["2"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![10]));
        agg.set_batch(ids, vals);
        agg.insert(0)?;

        // group "1" starts out all-NULL
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        agg.set_batch(ids, vals);
        agg.insert_null(0);

        // group "1" produces a better value and evicts group "2"
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![20]));
        agg.set_batch(ids, vals);
        agg.insert_with_null_groups(0)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 1        | 20           |
        +----------+--------------+
        "
        );

        Ok(())
    }

    #[test]
    fn should_drop_null_group_that_loses_to_topk() -> Result<()> {
        let mut agg = PriorityMap::new(DataType::Utf8, DataType::Int64, 1, true)?;

        // group "1" starts out all-NULL
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        agg.set_batch(ids, vals);
        agg.insert_null(0);

        // group "2" fills the single top-k slot
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["2"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![10]));
        agg.set_batch(ids, vals);
        agg.insert_with_null_groups(0)?;

        // group "1" produces a value that loses to the current top-k: the
        // group can no longer reach the top-k and must not be emitted as NULL
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["1"]));
        let vals: ArrayRef = Arc::new(Int64Array::from(vec![5]));
        agg.set_batch(ids, vals);
        agg.insert_with_null_groups(0)?;

        let cols = agg.emit()?;
        let batch = RecordBatch::try_new(test_schema(), cols)?;
        let actual = format!("{}", pretty_format_batches(&[batch])?);
        assert_snapshot!(actual, @r"
        +----------+--------------+
        | trace_id | timestamp_ms |
        +----------+--------------+
        | 2        | 10           |
        +----------+--------------+
        "
        );

        Ok(())
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("timestamp_ms", DataType::Int64, true),
        ]))
    }

    fn test_schema_utf8view() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8View, true),
            Field::new("timestamp_ms", DataType::Int64, true),
        ]))
    }

    fn test_large_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::LargeUtf8, true),
            Field::new("timestamp_ms", DataType::Int64, true),
        ]))
    }

    fn test_schema_value(value_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Int64, true),
            Field::new("timestamp_ms", value_type, true),
        ]))
    }
}
