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

use crate::aggregates::group_values::multi_group_by::GroupColumn;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBufferBuilder, DictionaryArray, PrimitiveArray,
};
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, DataType, Field};
use arrow::error::ArrowError;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::{DataFusionError, Result, exec_err};
use datafusion_execution::memory_pool::proxy::HashTableAllocExt;
use hashbrown::hash_table::HashTable;
use std::marker::PhantomData;
use std::mem::size_of;
use std::sync::Arc;

use crate::aggregates::AGGREGATION_HASH_SEED;

/// [`GroupColumn`] for dictionary-encoded columns.
///
/// `inner` holds one slot per distinct value seen across all batches.
/// `group_to_inner[group_idx]` maps each group to its slot in `inner`,
/// so groups with the same value share a slot rather than duplicating data.
pub struct DictionaryGroupValuesColumn<K: ArrowDictionaryKeyType + Send + Sync> {
    /// Deduplicated store of distinct values.
    inner: Box<dyn GroupColumn>,
    /// Single-element null array for appending null entries to `inner`.
    null_array: ArrayRef,
    /// Maps each group index to its slot in `inner`.
    group_to_inner: Vec<usize>,
    /// Lookup table mapping `(value_hash, inner_slot)` for each non-null distinct value.
    value_dedup: HashTable<(u64, usize)>,
    /// Tracked allocation size of `value_dedup` for memory accounting via `size()`.
    value_dedup_size: usize,
    /// Slot in `inner` for the null group; `None` until the first null is seen.
    null_inner_slot: Option<usize>,
    /// Hash seed — must match `create_hashes` so hashes are consistent across calls.
    random_state: RandomState,
    /// Reusable scratch buffer mapping `val_idx → inner_slot` across batches.
    val_to_inner: Vec<usize>,
    /// Reusable hash buffer for the dictionary values array.
    val_hashes: Vec<u64>,
    _phantom: PhantomData<K>,
}

impl<K: ArrowDictionaryKeyType + Send + Sync> DictionaryGroupValuesColumn<K> {
    pub fn new(inner: Box<dyn GroupColumn>, field: &Field) -> Self {
        let null_array = arrow::array::new_null_array(field.data_type(), 1);
        Self {
            inner,
            null_array,
            group_to_inner: Vec::new(),
            value_dedup: HashTable::new(),
            value_dedup_size: 0,
            null_inner_slot: None,
            random_state: AGGREGATION_HASH_SEED,
            val_to_inner: Vec::default(),
            val_hashes: Vec::default(),
            _phantom: PhantomData,
        }
    }

    fn into_dict(values: ArrayRef, group_to_inner: &[usize]) -> ArrayRef {
        let keys: PrimitiveArray<K> = group_to_inner
            .iter()
            .map(|&slot| {
                if values.is_null(slot) {
                    None
                } else {
                    Some(K::Native::usize_as(slot))
                }
            })
            .collect();
        Arc::new(DictionaryArray::<K>::new(keys, values))
    }

    // https://github.com/apache/datafusion/issues/23127
    fn check_key_overflow(num_inner_slots: usize) -> Result<()> {
        if !Self::key_type_fits(num_inner_slots) {
            return exec_err!(
                "Dictionary key type {:?} cannot represent {} distinct values",
                K::DATA_TYPE,
                num_inner_slots
            );
        }
        Ok(())
    }

    fn key_type_fits(num_values: usize) -> bool {
        let max: usize = match K::DATA_TYPE {
            DataType::Int8 => i8::MAX as usize,
            DataType::Int16 => i16::MAX as usize,
            DataType::Int32 => i32::MAX as usize,
            DataType::Int64 => i64::MAX as usize,
            DataType::UInt8 => u8::MAX as usize,
            DataType::UInt16 => u16::MAX as usize,
            DataType::UInt32 => u32::MAX as usize,
            DataType::UInt64 => usize::MAX,
            _ => return false,
        };
        num_values == 0 || num_values - 1 <= max
    }

    fn hash_values(&mut self, values: &ArrayRef) {
        self.val_hashes.clear();
        self.val_hashes.resize(values.len(), 0);
        create_hashes(
            std::slice::from_ref(values),
            &self.random_state,
            &mut self.val_hashes,
        )
        .unwrap();
    }

    fn find_or_insert_value(
        &mut self,
        dict_values: &ArrayRef,
        val_idx: usize,
        hash: u64,
    ) -> Result<usize> {
        let inner = &*self.inner;
        let existing = self
            .value_dedup
            .find(hash, |&(entry_hash, slot)| {
                entry_hash == hash && inner.equal_to(slot, dict_values, val_idx)
            })
            .map(|&(_, slot)| slot);

        match existing {
            Some(slot) => Ok(slot),
            None => {
                let slot = self.inner.len();
                self.inner.append_val(dict_values, val_idx)?;
                self.value_dedup.insert_accounted(
                    (hash, slot),
                    |&(entry_hash, _)| entry_hash,
                    &mut self.value_dedup_size,
                );
                Ok(slot)
            }
        }
    }

    fn find_or_insert_null(&mut self) -> Result<usize> {
        if let Some(slot) = self.null_inner_slot {
            return Ok(slot);
        }
        let slot = self.inner.len();
        self.inner.append_val(&self.null_array, 0)?;
        self.null_inner_slot = Some(slot);
        Ok(slot)
    }

    fn build_lookup_table(
        &self,
        dict_values: &ArrayRef,
        val_hashes: &[u64],
    ) -> Vec<usize> {
        let num_distinct = dict_values.len();
        let mut table = vec![usize::MAX; num_distinct + 1];
        let inner = &*self.inner;
        for val_idx in 0..num_distinct {
            if dict_values.is_null(val_idx) {
                table[val_idx] = self.null_inner_slot.unwrap_or(usize::MAX);
            } else {
                let hash = val_hashes[val_idx];
                if let Some(&(_, slot)) =
                    self.value_dedup.find(hash, |&(entry_hash, slot)| {
                        entry_hash == hash && inner.equal_to(slot, dict_values, val_idx)
                    })
                {
                    table[val_idx] = slot;
                }
            }
        }
        table[num_distinct] = self.null_inner_slot.unwrap_or(usize::MAX);
        table
    }
}

impl<K: ArrowDictionaryKeyType + Send + Sync> GroupColumn
    for DictionaryGroupValuesColumn<K>
{
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        let lhs_slot = self.group_to_inner[lhs_row];
        let dict = array.as_dictionary::<K>();
        match dict.key(rhs_row) {
            None => self.inner.equal_to(lhs_slot, &self.null_array, 0),
            Some(val_idx) if dict.values().is_null(val_idx) => {
                self.inner.equal_to(lhs_slot, &self.null_array, 0)
            }
            Some(val_idx) => self.inner.equal_to(lhs_slot, dict.values(), val_idx),
        }
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        let dict = array.as_dictionary::<K>();
        let inner_slot = match dict.key(row) {
            None => self.find_or_insert_null()?,
            Some(val_idx) if dict.values().is_null(val_idx) => {
                self.find_or_insert_null()?
            }
            Some(val_idx) => {
                let dict_values = dict.values();
                self.hash_values(dict_values);
                self.find_or_insert_value(dict_values, val_idx, self.val_hashes[val_idx])?
            }
        };
        self.group_to_inner.push(inner_slot);
        Self::check_key_overflow(self.inner.len())
    }

    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut BooleanBufferBuilder,
    ) {
        let dict = array.as_dictionary::<K>();
        let dict_keys = dict.keys();
        let dict_values = dict.values();
        let num_distinct = dict_values.len();

        let mut val_hashes = vec![0u64; dict_values.len()];
        create_hashes(
            std::slice::from_ref(dict_values),
            &self.random_state,
            &mut val_hashes,
        )
        .unwrap();
        let lookup = self.build_lookup_table(dict_values, &val_hashes);

        let group_to_inner = self.group_to_inner.as_slice();

        if dict_keys.null_count() == 0 {
            // No null keys — skip the get_bit guard: we only ever write false,
            // so overwriting an already-false bit is a no-op.
            let raw_keys = dict_keys.values();
            for (idx, (&lhs_row, &rhs_row)) in
                lhs_rows.iter().zip(rhs_rows.iter()).enumerate()
            {
                let rhs_slot = lookup[raw_keys[rhs_row].as_usize()];
                if rhs_slot == usize::MAX || group_to_inner[lhs_row] != rhs_slot {
                    equal_to_results.set_bit(idx, false);
                }
            }
        } else {
            let null_buf = dict_keys.nulls().unwrap();
            let raw_keys = dict_keys.values();
            for (idx, (&lhs_row, &rhs_row)) in
                lhs_rows.iter().zip(rhs_rows.iter()).enumerate()
            {
                if equal_to_results.get_bit(idx) {
                    let val_idx = if null_buf.is_null(rhs_row) {
                        num_distinct
                    } else {
                        raw_keys[rhs_row].as_usize()
                    };
                    let rhs_slot = lookup[val_idx];
                    if rhs_slot == usize::MAX || group_to_inner[lhs_row] != rhs_slot {
                        equal_to_results.set_bit(idx, false);
                    }
                }
            }
        }
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) -> Result<()> {
        let dict = array.as_dictionary::<K>();
        let dict_keys = dict.keys();
        let dict_values = dict.values();
        let num_distinct = dict_values.len();

        self.hash_values(dict_values);
        self.val_to_inner.clear();
        self.val_to_inner.resize(num_distinct, usize::MAX);

        self.group_to_inner.try_reserve(rows.len()).map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(ArrowError::MemoryError(e.to_string())),
                None,
            )
        })?;

        if dict_keys.null_count() == 0 {
            let raw_keys = dict_keys.values();
            for &row in rows {
                let val_idx = raw_keys[row].as_usize();
                if self.val_to_inner[val_idx] == usize::MAX {
                    // A non-null key can still point to a null value in the values array.
                    self.val_to_inner[val_idx] = if dict_values.is_null(val_idx) {
                        self.find_or_insert_null()?
                    } else {
                        self.find_or_insert_value(
                            dict_values,
                            val_idx,
                            self.val_hashes[val_idx],
                        )?
                    };
                }
                self.group_to_inner.push(self.val_to_inner[val_idx]);
            }
        } else {
            let raw_keys = dict_keys.values();
            let null_buf = dict_keys.nulls().unwrap();
            for &row in rows {
                let slot = if null_buf.is_null(row) {
                    self.find_or_insert_null()?
                } else {
                    let val_idx = raw_keys[row].as_usize();
                    if self.val_to_inner[val_idx] == usize::MAX {
                        self.val_to_inner[val_idx] = if dict_values.is_null(val_idx) {
                            self.find_or_insert_null()?
                        } else {
                            self.find_or_insert_value(
                                dict_values,
                                val_idx,
                                self.val_hashes[val_idx],
                            )?
                        };
                    }
                    self.val_to_inner[val_idx]
                };
                self.group_to_inner.push(slot);
            }
        }

        Self::check_key_overflow(self.inner.len())
    }

    fn len(&self) -> usize {
        self.group_to_inner.len()
    }

    fn size(&self) -> usize {
        self.inner.size()
            + self.value_dedup_size
            + self.group_to_inner.capacity() * size_of::<usize>()
            + self.val_to_inner.capacity() * size_of::<usize>()
            + self.val_hashes.capacity() * size_of::<u64>()
            + self.null_array.get_array_memory_size()
            + size_of::<Self>()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let values = self.inner.build();
        Self::into_dict(values, &self.group_to_inner)
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        // `inner` is a trait object — the only way to extract its data is via `take_n`.
        // Because group→inner slot mappings are non-contiguous, we drain all of `inner`
        // at once, then re-append only the slots still referenced by the remaining groups.
        let old_inner_len = self.inner.len();
        let all_inner_values = self.inner.take_n(old_inner_len);

        let emitted =
            Self::into_dict(Arc::clone(&all_inner_values), &self.group_to_inner[..n]);

        let remaining = self.group_to_inner[n..].to_vec();

        // Map each referenced old slot to a new contiguous index.
        let mut old_to_new = vec![usize::MAX; old_inner_len];
        let mut new_to_old: Vec<usize> = Vec::new();
        for &old in &remaining {
            if old_to_new[old] == usize::MAX {
                old_to_new[old] = new_to_old.len();
                new_to_old.push(old);
            }
        }

        self.value_dedup = HashTable::new();
        self.value_dedup_size = 0;
        self.null_inner_slot = None;

        for (new_slot, &old_slot) in new_to_old.iter().enumerate() {
            if all_inner_values.is_null(old_slot) {
                self.inner
                    .append_val(&self.null_array, 0)
                    .expect("append null failed in take_n");
                self.null_inner_slot = Some(new_slot);
            } else {
                self.inner
                    .append_val(&all_inner_values, old_slot)
                    .expect("append value failed in take_n");
                let single = all_inner_values.slice(old_slot, 1);
                self.hash_values(&single);
                self.value_dedup.insert_accounted(
                    (self.val_hashes[0], new_slot),
                    |&(entry_hash, _)| entry_hash,
                    &mut self.value_dedup_size,
                );
            }
        }

        self.group_to_inner = remaining.iter().map(|&old| old_to_new[old]).collect();
        Self::check_key_overflow(self.inner.len()).expect("key overflow in take_n");

        emitted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregates::group_values::multi_group_by::bytes::ByteGroupValueBuilder;
    use arrow::array::{
        Array, ArrayRef, BooleanBufferBuilder, DictionaryArray, Int32Array, StringArray,
        UInt8Array,
    };
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Int32Type, UInt8Type};
    use datafusion_physical_expr::binary_map::OutputType;
    use std::sync::Arc;

    fn utf8_col() -> DictionaryGroupValuesColumn<Int32Type> {
        let field = Field::new("", DataType::Utf8, true);
        DictionaryGroupValuesColumn::<Int32Type>::new(
            Box::new(ByteGroupValueBuilder::<i32>::new(OutputType::Utf8)),
            &field,
        )
    }

    fn dict_arr(keys: &[Option<i32>], values: &[Option<&str>]) -> ArrayRef {
        Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(keys.to_vec()),
            Arc::new(StringArray::from(values.to_vec())),
        ))
    }

    fn str_values(arr: &ArrayRef) -> Vec<Option<String>> {
        let plain = cast(arr.as_ref(), &DataType::Utf8).unwrap();
        let strings = plain.as_any().downcast_ref::<StringArray>().unwrap();
        (0..strings.len())
            .map(|i| strings.is_valid(i).then(|| strings.value(i).to_owned()))
            .collect()
    }

    fn bool_vec(buf: &BooleanBufferBuilder) -> Vec<bool> {
        (0..buf.len()).map(|i| buf.get_bit(i)).collect()
    }

    fn all_true(len: usize) -> BooleanBufferBuilder {
        let mut buf = BooleanBufferBuilder::new(len);
        buf.append_n(len, true);
        buf
    }

    // Null key and null-valued dict entry both map to the null group.
    #[test]
    fn null_key_and_null_value_in_dict() {
        let mut col = utf8_col();
        // Row 0: null key, Row 1: key→null value, Row 2: key→"b"
        let input = dict_arr(&[None, Some(0), Some(1)], &[None, Some("b")]);
        for row in 0..3 {
            col.append_val(&input, row).unwrap();
        }

        assert!(col.equal_to(0, &input, 1));
        assert!(col.equal_to(1, &input, 0));
        assert!(!col.equal_to(0, &input, 2));
        assert!(!col.equal_to(2, &input, 0));

        let out = Box::new(col).build();
        assert_eq!(out.as_dictionary::<Int32Type>().values().len(), 2);
        assert_eq!(str_values(&out), vec![None, None, Some("b".into())]);
    }

    #[test]
    fn take_n_remaps_slots_across_batches() {
        use crate::aggregates::group_values::multi_group_by::primitive::PrimitiveGroupValueBuilder;
        use arrow::array::UInt64Array;
        use arrow::datatypes::UInt64Type;

        let field = Field::new("", DataType::UInt64, true);
        let mut col = DictionaryGroupValuesColumn::<Int32Type>::new(
            Box::new(PrimitiveGroupValueBuilder::<UInt64Type, true>::new(
                DataType::UInt64,
            )),
            &field,
        );

        let u64_val = |arr: &ArrayRef, pos: usize| {
            let dict = arr.as_dictionary::<Int32Type>();
            dict.values()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(dict.key(pos).unwrap())
        };

        let batch1: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![Some(0), Some(1), None, Some(2), Some(0)]),
            Arc::new(UInt64Array::from(vec![10u64, 20, 30])),
        ));
        col.vectorized_append(&batch1, &[0, 1, 2, 3, 4]).unwrap();

        let emitted = col.take_n(3);
        assert_eq!(u64_val(&emitted, 0), 10);
        assert_eq!(u64_val(&emitted, 1), 20);
        assert!(emitted.as_dictionary::<Int32Type>().key(2).is_none());

        let batch2: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![None, Some(0)]),
            Arc::new(UInt64Array::from(vec![99u64])),
        ));
        col.vectorized_append(&batch2, &[0, 1]).unwrap();

        let mut buf = all_true(2);
        col.vectorized_equal_to(&[2, 3], &batch2, &[0, 1], &mut buf);
        assert_eq!(bool_vec(&buf), vec![true, true]);

        let out = Box::new(col).build();
        assert_eq!(u64_val(&out, 0), 30);
        assert_eq!(u64_val(&out, 1), 10);
        assert!(out.as_dictionary::<Int32Type>().key(2).is_none());
        assert_eq!(u64_val(&out, 3), 99);
    }

    // Regression: https://github.com/apache/datafusion/issues/23127
    #[test]
    fn key_type_overflow_returns_error() {
        let field = Field::new("", DataType::Utf8, true);
        let mut col = DictionaryGroupValuesColumn::<UInt8Type>::new(
            Box::new(ByteGroupValueBuilder::<i32>::new(OutputType::Utf8)),
            &field,
        );

        let strs: Vec<String> = (0..=255u16).map(|i| i.to_string()).collect();
        let str_refs: Vec<Option<&str>> = strs.iter().map(|s| Some(s.as_str())).collect();
        let full: ArrayRef = Arc::new(DictionaryArray::<UInt8Type>::new(
            UInt8Array::from((0..=255u8).map(Some).collect::<Vec<_>>()),
            Arc::new(StringArray::from(str_refs)),
        ));
        col.vectorized_append(&full, &(0..256).collect::<Vec<_>>())
            .unwrap();

        let extra: ArrayRef = Arc::new(DictionaryArray::<UInt8Type>::new(
            UInt8Array::from(vec![Some(0u8)]),
            Arc::new(StringArray::from(vec![Some("overflow")])),
        ));
        assert!(col.append_val(&extra, 0).is_err());
    }

    // build_lookup_table must use the incoming batch's hashes, not
    // stale ones left by the last vectorized_append call.
    #[test]
    fn vectorized_equal_to_uses_current_batch_hashes() {
        let mut col = utf8_col();

        let batch1 = dict_arr(&[Some(0)], &[Some("a"), Some("b")]);
        col.vectorized_append(&batch1, &[0]).unwrap();

        // values = ["z", "a"]; key 0 → "a" at val_idx 1.
        // Stale hashes would probe val_idx 1 with hash("b") and miss.
        let batch2 = dict_arr(&[Some(1)], &[Some("z"), Some("a")]);
        let mut buf = all_true(1);
        col.vectorized_equal_to(&[0], &batch2, &[0], &mut buf);
        assert_eq!(bool_vec(&buf), vec![true]);
    }

    #[test]
    fn append_only_stores_referenced_values() {
        let mut col = utf8_col();
        let values = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
            Some("f"),
            Some("g"),
            Some("h"),
            Some("i"),
            Some("j"),
        ]));
        let keys = Int32Array::from(vec![
            Some(0), // a
            Some(2), // c
            Some(7), // h
            Some(0),
            Some(2),
            Some(7),
            Some(7),
            Some(0),
            Some(2),
        ]);
        let input: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(keys, values));

        col.vectorized_append(&input, &[0, 1, 2, 3, 4, 5, 6, 7, 8])
            .unwrap();

        let out = Box::new(col).build();
        assert_eq!(out.as_dictionary::<Int32Type>().values().len(), 3);
        assert_eq!(
            str_values(&out),
            vec![
                Some("a".into()),
                Some("c".into()),
                Some("h".into()),
                Some("a".into()),
                Some("c".into()),
                Some("h".into()),
                Some("h".into()),
                Some("a".into()),
                Some("c".into()),
            ]
        );
    }
}
