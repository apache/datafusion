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
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, DictionaryArray, Int8Array, Int16Array, Int32Array,
    Int64Array, ListBuilder, NullArray, StringBuilder, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{
    ArrowNativeType, DataType, Int8Type, Int16Type, Int32Type, Int64Type, Schema,
    SchemaRef, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::downcast_dictionary_array;
use datafusion_common::hash_utils::{RandomState, combine_hashes, create_hashes};
use datafusion_common::{Result, internal_datafusion_err};
use datafusion_execution::memory_pool::proxy::HashTableAllocExt;
use datafusion_expr::EmitTo;
use hashbrown::hash_table::HashTable;

use crate::aggregates::group_values::GroupValues;

/// Caches the hashes for one dictionary column's values array.
/// Rebuilt only when the `Arc` pointer changes (i.e. a new values array arrives).
struct ColumnCache {
    /// Keeps the values `Arc` alive and is compared with `Arc::ptr_eq` to detect staleness.
    values: ArrayRef,
    /// `value_hashes[k]` = hash of the value at dictionary index `k`.
    value_hashes: Vec<u64>,
}

impl ColumnCache {
    fn empty() -> Self {
        Self {
            values: Arc::new(NullArray::new(0)),
            value_hashes: vec![],
        }
    }

    fn update(&mut self, new_values: ArrayRef, random_state: &RandomState) -> Result<()> {
        if Arc::ptr_eq(&new_values, &self.values) {
            return Ok(());
        }
        let num_values = new_values.len();
        // Reuse the allocation; only grows capacity when a larger values array arrives.
        self.value_hashes.clear();
        self.value_hashes.resize(num_values, 0u64);
        create_hashes(
            std::slice::from_ref(&new_values),
            random_state,
            &mut self.value_hashes,
        )?;
        self.values = new_values;
        Ok(())
    }

    fn size(&self) -> usize {
        self.value_hashes.len() * size_of::<u64>()
    }

    fn clear_shrink(&mut self, shrink_to: usize) {
        self.values = Arc::new(NullArray::new(0));
        self.value_hashes.clear();
        self.value_hashes.shrink_to(shrink_to);
    }
}

/// [`GroupValues`] for GROUP BY over **two or more** dictionary-typed columns.
pub struct GroupDictionaryColumn {
    schema: SchemaRef,
    col_caches: Vec<ColumnCache>,
    /// `(row_hash, group_id)`.  Multiple entries may share the same hash value;
    /// byte-level comparison is used to resolve collisions.
    map: HashTable<(u64, usize)>,
    /// Tracked allocation size of `map` in bytes, updated on every insert and shrink.
    map_size: usize,
    /// All group rows packed back-to-back into a single contiguous buffer.
    ///
    /// CSR-style layout: `row_offsets[g]` is the start of group `g` and
    /// `row_offsets[g+1]` is its end.  The last group has no `g+1` entry; its
    /// end is `row_buffer.len()`.
    row_buffer: Vec<u8>,
    /// `row_offsets[g]` = start byte of group `g` inside `row_buffer`.
    row_offsets: Vec<usize>,
    /// Reused scratch buffer for encoding the current row.
    row_scratch: Vec<u8>,
    row_decoder: RowSetDecoder,
    random_state: RandomState,
}

/// Returns `true` when every field in `schema` is `DataType::Dictionary`.
pub fn all_dictionary_schema(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .all(|field| matches!(field.data_type(), DataType::Dictionary(_, _)))
}

fn is_supported_value_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8)
        || matches!(data_type, DataType::List(f) if f.data_type() == &DataType::Utf8)
}

impl GroupDictionaryColumn {
    pub fn new(schema: SchemaRef) -> Result<Self> {
        if schema.fields().len() < 2 {
            return Err(internal_datafusion_err!(
                "GroupDictionaryColumn requires at least 2 columns, got {}",
                schema.fields().len()
            ));
        }
        for field in schema.fields() {
            match field.data_type() {
                DataType::Dictionary(_, value_type) => {
                    if !is_supported_value_type(value_type) {
                        return Err(internal_datafusion_err!(
                            "GroupDictionaryColumn: unsupported dictionary value type \
                             '{}' in column '{}'",
                            value_type,
                            field.name()
                        ));
                    }
                }
                _ => {
                    return Err(internal_datafusion_err!(
                        "GroupDictionaryColumn requires all columns to be Dictionary, \
                         but '{}' has type {}",
                        field.name(),
                        field.data_type()
                    ));
                }
            }
        }
        let n_cols = schema.fields().len();
        let row_decoder = RowSetDecoder::new(&schema);
        Ok(Self {
            schema,
            col_caches: (0..n_cols).map(|_| ColumnCache::empty()).collect(),
            map: HashTable::with_capacity(128),
            map_size: 0,
            row_buffer: Vec::new(),
            row_offsets: Vec::new(),
            row_scratch: Vec::new(),
            row_decoder,
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
        })
    }
}

fn dict_values_array(col: &dyn Array) -> ArrayRef {
    downcast_dictionary_array!(
        col => Arc::clone(col.values()),
        _ => unreachable!("schema validated in GroupDictionaryColumn::new")
    )
}

// Box is required: different key widths (Int8/Int16/Int32/Int64) produce different concrete iterator types.
fn fill_keys(col: &dyn Array) -> Box<dyn Iterator<Item = Option<usize>> + '_> {
    downcast_dictionary_array!(
        col => {
            let keys = col.keys();
            Box::new((0..keys.len()).map(move |row_idx| {
                if keys.is_valid(row_idx) {
                    Some(keys.value(row_idx).as_usize())
                } else {
                    None
                }
            }))
        },
        _ => unreachable!("schema validated in GroupDictionaryColumn::new")
    )
}

impl GroupValues for GroupDictionaryColumn {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        debug_assert_eq!(cols.len(), self.schema.fields().len());
        groups.clear();

        if cols.is_empty() || cols[0].is_empty() {
            return Ok(());
        }
        let n_rows = cols[0].len();

        for (col_idx, col) in cols.iter().enumerate() {
            self.col_caches[col_idx]
                .update(dict_values_array(col.as_ref()), &self.random_state)?;
        }

        // Downcast once per column; advance with .next() per row to avoid per-row downcast.
        let mut key_iters: Vec<_> =
            cols.iter().map(|col| fill_keys(col.as_ref())).collect();

        let _ = groups.try_reserve(n_rows);
        for _row in 0..n_rows {
            let mut hash = 0u64;
            self.row_scratch.clear();

            for (col_idx, key_iter) in key_iters.iter_mut().enumerate() {
                let key = key_iter.next().unwrap();
                let cache = &self.col_caches[col_idx];
                let value_hash = key.map_or(0, |key_idx| cache.value_hashes[key_idx]);
                hash = combine_hashes(hash, value_hash);
                encode_value(key, cache.values.as_ref(), &mut self.row_scratch);
            }

            let combined_hash = hash;
            let found = {
                let row_scratch = self.row_scratch.as_slice();
                let row_buffer = self.row_buffer.as_slice();
                let row_offsets = self.row_offsets.as_slice();
                self.map
                    .find(combined_hash, |&(stored_hash, group_id)| {
                        stored_hash == combined_hash && {
                            let end = row_offsets
                                .get(group_id + 1)
                                .copied()
                                .unwrap_or(row_buffer.len()); // last group has no g+1 entry
                            row_buffer[row_offsets[group_id]..end] == *row_scratch
                        }
                    })
                    .map(|&(_, group_id)| group_id)
            };

            let group_id = match found {
                Some(existing_id) => existing_id,
                None => {
                    let new_id = self.row_offsets.len();
                    self.row_offsets.push(self.row_buffer.len());
                    self.row_buffer.extend_from_slice(&self.row_scratch);
                    self.map.insert_accounted(
                        (combined_hash, new_id),
                        |(stored_hash, _)| *stored_hash,
                        &mut self.map_size,
                    );
                    new_id
                }
            };

            groups.push(group_id);
        }

        Ok(())
    }

    fn size(&self) -> usize {
        let cache_bytes: usize = self.col_caches.iter().map(|c| c.size()).sum();
        self.map_size
            + self.row_buffer.len()
            + self.row_offsets.len() * size_of::<usize>()
            + self.row_scratch.capacity()
            + cache_bytes
    }

    fn is_empty(&self) -> bool {
        self.row_offsets.is_empty()
    }

    fn len(&self) -> usize {
        self.row_offsets.len()
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let n_total = self.row_offsets.len();
        if n_total == 0 {
            return Ok(self.row_decoder.finish());
        }
        let n_emit = match emit_to {
            EmitTo::All => n_total,
            EmitTo::First(n) => n.min(n_total),
        };

        for row_idx in 0..n_emit {
            let start = self.row_offsets[row_idx];
            let end = self
                .row_offsets
                .get(row_idx + 1)
                .copied()
                .unwrap_or(self.row_buffer.len());
            self.row_decoder.decode(&self.row_buffer[start..end]);
        }
        let inner = self.row_decoder.finish();
        let arrays: Vec<ArrayRef> = inner
            .into_iter()
            .zip(self.schema.fields())
            .map(|(values, field)| match field.data_type() {
                DataType::Dictionary(key_type, _) => wrap_as_dictionary(
                    values,
                    &make_sequential_keys(n_emit, key_type),
                    key_type,
                ),
                _ => unreachable!("schema validated in GroupDictionaryColumn::new"),
            })
            .collect();

        if n_emit == n_total {
            self.row_buffer.clear();
            self.row_offsets.clear();
            self.map.clear();
            self.map_size = 0;
        } else {
            let retain_start = self.row_offsets[n_emit];
            self.row_offsets.drain(0..n_emit);
            for offset in &mut self.row_offsets {
                *offset -= retain_start;
            }
            self.row_buffer.drain(0..retain_start);
            // avoiding this somehow would be nice. worse case this runs once
            // VecDeque?
            // Shift remaining group ids in-place; retain gives &mut access so no rehashing occurs.
            self.map.retain(|(_, gid)| {
                if *gid < n_emit {
                    return false;
                }
                *gid -= n_emit;
                true
            });
        }

        Ok(arrays)
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.map.clear();
        self.map.shrink_to(num_rows, |_| 0);
        self.map_size = self.map.capacity() * size_of::<(u64, usize)>();
        self.row_buffer.clear();
        self.row_offsets.clear();
        self.row_offsets.shrink_to(num_rows);
        for cache in &mut self.col_caches {
            cache.clear_shrink(num_rows);
        }
    }
}

// ── encoding / decoding ───────────────────────────────────────────────────────

/// Wire format per column:
///   null:              `[0x00]`
///   non-null scalar:   `[0x01][len: u32 LE][utf8_bytes…]`
///   non-null list:     `[0x01][content_len: u32 LE][n: u32 LE][elem…]`
///                      where each elem is `[0x00]` (null) or `[0x01][len: u32 LE][utf8_bytes…]`
fn encode_value(key: Option<usize>, values: &dyn Array, buf: &mut Vec<u8>) {
    let key_idx = match key {
        None => {
            buf.push(0);
            return;
        }
        Some(k) => k,
    };
    if values.is_null(key_idx) {
        buf.push(0);
        return;
    }
    buf.push(1);
    match values.data_type() {
        DataType::Utf8 => {
            let bytes = values.as_string::<i32>().value(key_idx).as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        DataType::List(_) => {
            // Back-fill content_len after encoding all elements.
            let len_pos = buf.len();
            buf.extend_from_slice(&[0u8; 4]);
            let content_start = buf.len();

            let list_element = values.as_list::<i32>().value(key_idx);
            let str_array = list_element.as_string::<i32>();
            buf.extend_from_slice(&(str_array.len() as u32).to_le_bytes());
            for elem_idx in 0..str_array.len() {
                if str_array.is_null(elem_idx) {
                    buf.push(0);
                } else {
                    let elem_bytes = str_array.value(elem_idx).as_bytes();
                    buf.push(1);
                    buf.extend_from_slice(&(elem_bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(elem_bytes);
                }
            }
            let content_len = (buf.len() - content_start) as u32;
            buf[len_pos..len_pos + 4].copy_from_slice(&content_len.to_le_bytes());
        }
        dt => panic!("unsupported dictionary value type: {dt}"),
    }
}

#[derive(Debug)]
enum ColumnBuilder {
    Utf8(StringBuilder),
    ListUtf8(ListBuilder<StringBuilder>),
}

macro_rules! dispatch_builder {
    ($self:expr, $b:ident => $body:expr) => {
        match $self {
            ColumnBuilder::Utf8($b) => $body,
            ColumnBuilder::ListUtf8($b) => $body,
        }
    };
}

impl ColumnBuilder {
    fn from_value_type(value_type: &DataType) -> Self {
        match value_type {
            DataType::Utf8 => Self::Utf8(StringBuilder::new()),
            DataType::List(_) => Self::ListUtf8(ListBuilder::new(StringBuilder::new())),
            _ => unreachable!("value type validated in GroupDictionaryColumn::new"),
        }
    }

    fn append_null(&mut self) {
        dispatch_builder!(self, b => b.append_null())
    }

    fn append_bytes(&mut self, bytes: &[u8]) {
        match self {
            // SAFETY: bytes come from Arrow string arrays, always valid UTF-8.
            Self::Utf8(b) => {
                b.append_value(unsafe { std::str::from_utf8_unchecked(bytes) })
            }
            Self::ListUtf8(b) => {
                let mut cursor = 0;
                let n = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap())
                    as usize;
                cursor += 4;
                for _ in 0..n {
                    match bytes[cursor] {
                        0 => {
                            b.values().append_null();
                            cursor += 1;
                        }
                        _ => {
                            cursor += 1;
                            let len = u32::from_le_bytes(
                                bytes[cursor..cursor + 4].try_into().unwrap(),
                            ) as usize;
                            cursor += 4;
                            // SAFETY: bytes come from Arrow string arrays, always valid UTF-8.
                            b.values().append_value(unsafe {
                                std::str::from_utf8_unchecked(
                                    &bytes[cursor..cursor + len],
                                )
                            });
                            cursor += len;
                        }
                    }
                }
                b.append(true);
            }
        }
    }

    fn finish(&mut self) -> ArrayRef {
        dispatch_builder!(self, b => Arc::new(b.finish()))
    }
}

/// Accumulates encoded row slices and reconstructs them into Arrow arrays on `finish`.
#[derive(Debug)]
struct RowSetDecoder {
    builders: Vec<ColumnBuilder>,
}

impl RowSetDecoder {
    fn new(schema: &Schema) -> Self {
        let builders = schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Dictionary(_, value_type) => {
                    ColumnBuilder::from_value_type(value_type)
                }
                _ => unreachable!("schema validated in GroupDictionaryColumn::new"),
            })
            .collect();
        Self { builders }
    }

    /// Expected format: one column entry per schema field, in schema order.
    /// Each column: `[0x00]` (null) or `[0x01][content_len: u32 LE][content…]`.
    /// Utf8 content: raw UTF-8 bytes.
    /// List<Utf8> content: `[n: u32 LE][elem…]` where each elem is `[0x00]` or `[0x01][len: u32 LE][utf8_bytes…]`.
    fn decode(&mut self, encoded: &[u8]) {
        let mut cursor = 0;
        for builder in &mut self.builders {
            match encoded[cursor] {
                0 => {
                    builder.append_null();
                    cursor += 1;
                }
                _ => {
                    cursor += 1;
                    let len = u32::from_le_bytes(
                        encoded[cursor..cursor + 4].try_into().unwrap(),
                    ) as usize;
                    cursor += 4;
                    builder.append_bytes(&encoded[cursor..cursor + len]);
                    cursor += len;
                }
            }
        }
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        self.builders.iter_mut().map(|b| b.finish()).collect()
    }
}

/// Build sequential keys `[0, 1, ..., n-1]` with the key type taken from the schema.
/// All columns share the same key type, so callers should build this once and clone the Arc.
fn make_sequential_keys(n: usize, key_type: &DataType) -> ArrayRef {
    match key_type {
        DataType::Int8 => Arc::new(Int8Array::from_iter_values((0..n).map(|i| i as i8))),
        DataType::Int16 => {
            Arc::new(Int16Array::from_iter_values((0..n).map(|i| i as i16)))
        }
        DataType::Int32 => {
            Arc::new(Int32Array::from_iter_values((0..n).map(|i| i as i32)))
        }
        DataType::Int64 => {
            Arc::new(Int64Array::from_iter_values((0..n).map(|i| i as i64)))
        }
        DataType::UInt8 => {
            Arc::new(UInt8Array::from_iter_values((0..n).map(|i| i as u8)))
        }
        DataType::UInt16 => {
            Arc::new(UInt16Array::from_iter_values((0..n).map(|i| i as u16)))
        }
        DataType::UInt32 => {
            Arc::new(UInt32Array::from_iter_values((0..n).map(|i| i as u32)))
        }
        DataType::UInt64 => {
            Arc::new(UInt64Array::from_iter_values((0..n).map(|i| i as u64)))
        }
        _ => unreachable!("schema validated in GroupDictionaryColumn::new"),
    }
}

fn wrap_as_dictionary(
    values: ArrayRef,
    keys: &ArrayRef,
    key_type: &DataType,
) -> ArrayRef {
    match key_type {
        DataType::Int8 => Arc::new(DictionaryArray::<Int8Type>::new(
            keys.as_primitive::<Int8Type>().clone(),
            values,
        )),
        DataType::Int16 => Arc::new(DictionaryArray::<Int16Type>::new(
            keys.as_primitive::<Int16Type>().clone(),
            values,
        )),
        DataType::Int32 => Arc::new(DictionaryArray::<Int32Type>::new(
            keys.as_primitive::<Int32Type>().clone(),
            values,
        )),
        DataType::Int64 => Arc::new(DictionaryArray::<Int64Type>::new(
            keys.as_primitive::<Int64Type>().clone(),
            values,
        )),
        DataType::UInt8 => Arc::new(DictionaryArray::<UInt8Type>::new(
            keys.as_primitive::<UInt8Type>().clone(),
            values,
        )),
        DataType::UInt16 => Arc::new(DictionaryArray::<UInt16Type>::new(
            keys.as_primitive::<UInt16Type>().clone(),
            values,
        )),
        DataType::UInt32 => Arc::new(DictionaryArray::<UInt32Type>::new(
            keys.as_primitive::<UInt32Type>().clone(),
            values,
        )),
        DataType::UInt64 => Arc::new(DictionaryArray::<UInt64Type>::new(
            keys.as_primitive::<UInt64Type>().clone(),
            values,
        )),
        _ => unreachable!("schema validated in GroupDictionaryColumn::new"),
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregates::group_values::GroupValuesRows;
    use arrow::array::StringArray;
    use arrow::datatypes::{Field, Schema};

    fn make_dict(values: &[&str], keys: &[Option<i32>]) -> ArrayRef {
        let vals = Arc::new(StringArray::from(values.to_vec()));
        let keys_arr = Int32Array::from(keys.to_vec());
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys_arr, vals).unwrap())
    }

    fn dict_schema() -> SchemaRef {
        let dt =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        Arc::new(Schema::new(vec![
            Field::new("a", dt.clone(), true),
            Field::new("b", dt, true),
        ]))
    }

    /// Same row twice in one batch → same group id; distinct rows → different ids.
    #[test]
    fn test_basic_dedup() {
        let mut gv = GroupDictionaryColumn::new(dict_schema()).unwrap();
        let col_a = make_dict(&["x", "y"], &[Some(0), Some(1), Some(0)]);
        let col_b = make_dict(&["p", "q"], &[Some(0), Some(1), Some(0)]);
        let mut groups = vec![];
        gv.intern(&[col_a, col_b], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 0]);
        assert_eq!(gv.len(), 2);
    }

    /// Null keys collapse to the same group regardless of which column is null.
    #[test]
    fn test_null_keys() {
        let mut gv = GroupDictionaryColumn::new(dict_schema()).unwrap();
        // Row 0: (null, "p") — key for col_a is null
        // Row 1: (null, "p") — same as row 0
        // Row 2: ("x",  "p") — distinct from row 0
        let col_a = make_dict(&["x"], &[None, None, Some(0)]);
        let col_b = make_dict(&["p"], &[Some(0), Some(0), Some(0)]);
        let mut groups = vec![];
        gv.intern(&[col_a, col_b], &mut groups).unwrap();
        assert_eq!(
            groups[0], groups[1],
            "both null-key rows must be the same group"
        );
        assert_ne!(
            groups[0], groups[2],
            "non-null row must be a different group"
        );
    }

    /// When the values array changes between batches (different Arc), the key
    /// space re-translates correctly: logical values still deduplicate.
    #[test]
    fn test_values_array_change_between_batches() {
        let mut gv = GroupDictionaryColumn::new(dict_schema()).unwrap();

        // Batch 1: col_a values = ["a", "b"], col_b values = ["x"]
        let col_a1 = make_dict(&["a", "b"], &[Some(0), Some(1)]);
        let col_b1 = make_dict(&["x"], &[Some(0), Some(0)]);
        let mut groups1 = vec![];
        gv.intern(&[col_a1, col_b1], &mut groups1).unwrap();
        assert_eq!(groups1, vec![0, 1]);

        // Batch 2: same logical rows but DIFFERENT Arc (values order swapped).
        // key=0 now means "b", key=1 means "a"  — opposite of batch 1.
        let col_a2 = make_dict(&["b", "a"], &[Some(0), Some(1)]);
        let col_b2 = make_dict(&["x"], &[Some(0), Some(0)]);
        let mut groups2 = vec![];
        gv.intern(&[col_a2, col_b2], &mut groups2).unwrap();

        // ("b", "x") was group 1 in batch 1; ("a", "x") was group 0.
        assert_eq!(
            groups2[0], 1,
            "('b','x') should map to the existing group for 'b'"
        );
        assert_eq!(
            groups2[1], 0,
            "('a','x') should map to the existing group for 'a'"
        );
        assert_eq!(gv.len(), 2, "no new groups should have been created");
    }

    fn list_dict_schema() -> SchemaRef {
        let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let dt = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::List(item_field)),
        );
        Arc::new(Schema::new(vec![
            Field::new("a", dt.clone(), true),
            Field::new("b", dt, true),
        ]))
    }

    fn make_list_dict(lists: &[Option<Vec<&str>>], keys: &[Option<i32>]) -> ArrayRef {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for list_opt in lists {
            match list_opt {
                None => builder.append_null(),
                Some(strings) => {
                    for s in strings {
                        builder.values().append_value(s);
                    }
                    builder.append(true);
                }
            }
        }
        let values = Arc::new(builder.finish());
        let keys_arr = Int32Array::from(keys.to_vec());
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys_arr, values).unwrap())
    }

    fn dict_str(arr: &ArrayRef, group: usize) -> &str {
        let d = arr.as_dictionary::<Int32Type>();
        // Keys are sequential (0..n), so key value == group index.
        let val_idx = d.keys().value(group) as usize;
        d.values().as_string::<i32>().value(val_idx)
    }

    fn dict_list(arr: &ArrayRef, group: usize) -> ArrayRef {
        let d = arr.as_dictionary::<Int32Type>();
        let val_idx = d.keys().value(group) as usize;
        d.values().as_list::<i32>().value(val_idx)
    }

    /// emit(All) reconstructs the correct string values for each group.
    #[test]
    fn test_emit_all_utf8() {
        let mut gv = GroupDictionaryColumn::new(dict_schema()).unwrap();
        let col_a = make_dict(&["x", "y"], &[Some(0), Some(1), Some(0)]);
        let col_b = make_dict(&["p", "q"], &[Some(0), Some(1), Some(0)]);
        gv.intern(&[col_a, col_b], &mut vec![]).unwrap();

        let arrays = gv.emit(EmitTo::All).unwrap();
        assert_eq!(arrays[0].len(), 2);
        assert_eq!(dict_str(&arrays[0], 0), "x");
        assert_eq!(dict_str(&arrays[0], 1), "y");
        assert_eq!(dict_str(&arrays[1], 0), "p");
        assert_eq!(dict_str(&arrays[1], 1), "q");
        assert_eq!(gv.len(), 0);
    }

    /// emit(First n) emits exactly the first n groups and keeps the rest accessible.
    #[test]
    fn test_emit_first_n_utf8() {
        let mut gv = GroupDictionaryColumn::new(dict_schema()).unwrap();
        let col_a = make_dict(&["a", "b", "c"], &[Some(0), Some(1), Some(2)]);
        let col_b = make_dict(&["x", "y", "z"], &[Some(0), Some(1), Some(2)]);
        gv.intern(&[col_a, col_b], &mut vec![]).unwrap();

        let arrays = gv.emit(EmitTo::First(2)).unwrap();
        assert_eq!(arrays[0].len(), 2);
        assert_eq!(dict_str(&arrays[0], 0), "a");
        assert_eq!(dict_str(&arrays[0], 1), "b");

        assert_eq!(gv.len(), 1);
        let mut groups = vec![];
        let col_a2 = make_dict(&["c"], &[Some(0)]);
        let col_b2 = make_dict(&["z"], &[Some(0)]);
        gv.intern(&[col_a2, col_b2], &mut groups).unwrap();
        assert_eq!(
            groups,
            vec![0],
            "retained group must map to id 0 after shift"
        );
    }

    /// emit(All) correctly reconstructs List<Utf8> values for each group.
    #[test]
    fn test_emit_list_utf8() {
        let mut gv = GroupDictionaryColumn::new(list_dict_schema()).unwrap();
        let col_a = make_list_dict(
            &[Some(vec!["hello", "world"]), Some(vec!["foo"])],
            &[Some(0), Some(0), Some(1)],
        );
        let col_b = make_list_dict(&[Some(vec!["foo"])], &[Some(0), Some(0), Some(0)]);
        let mut groups = vec![];
        gv.intern(&[col_a, col_b], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 0, 1]);

        let arrays = gv.emit(EmitTo::All).unwrap();
        assert_eq!(arrays[0].len(), 2);
        let g0 = dict_list(&arrays[0], 0);
        let g0_strs = g0.as_string::<i32>();
        assert_eq!(g0_strs.value(0), "hello");
        assert_eq!(g0_strs.value(1), "world");
        assert_eq!(dict_list(&arrays[0], 1).as_string::<i32>().value(0), "foo");
    }

    /// List<Utf8> with null elements inside the list round-trips correctly.
    #[test]
    fn test_emit_list_with_null_elements() {
        let mut gv = GroupDictionaryColumn::new(list_dict_schema()).unwrap();
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("a");
        builder.values().append_null();
        builder.values().append_value("b");
        builder.append(true);
        let list_values = Arc::new(builder.finish());
        let keys_arr = Int32Array::from(vec![Some(0i32)]);
        let col = Arc::new(
            DictionaryArray::<Int32Type>::try_new(keys_arr, list_values).unwrap(),
        ) as ArrayRef;

        gv.intern(&[Arc::clone(&col), col], &mut vec![]).unwrap();
        let arrays = gv.emit(EmitTo::All).unwrap();
        let elems = dict_list(&arrays[0], 0);
        let strs = elems.as_string::<i32>();
        assert_eq!(strs.len(), 3);
        assert_eq!(strs.value(0), "a");
        assert!(strs.is_null(1));
        assert_eq!(strs.value(2), "b");
    }

    /// emit(First n) on List<Utf8>: keeps remaining groups intact after the shift.
    #[test]
    fn test_emit_first_n_list_utf8() {
        let mut gv = GroupDictionaryColumn::new(list_dict_schema()).unwrap();
        // Three distinct groups: 0=(["a"],["x"]), 1=(["b"],["y"]), 2=(["c"],["z"])
        let col_a = make_list_dict(
            &[Some(vec!["a"]), Some(vec!["b"]), Some(vec!["c"])],
            &[Some(0), Some(1), Some(2)],
        );
        let col_b = make_list_dict(
            &[Some(vec!["x"]), Some(vec!["y"]), Some(vec!["z"])],
            &[Some(0), Some(1), Some(2)],
        );
        gv.intern(&[col_a, col_b], &mut vec![]).unwrap();

        let arrays = gv.emit(EmitTo::First(2)).unwrap();
        assert_eq!(arrays[0].len(), 2);
        assert_eq!(dict_list(&arrays[0], 0).as_string::<i32>().value(0), "a");
        assert_eq!(dict_list(&arrays[0], 1).as_string::<i32>().value(0), "b");

        // Group 2 must survive as group 0 after the shift.
        assert_eq!(gv.len(), 1);
        let col_a2 = make_list_dict(&[Some(vec!["c"])], &[Some(0)]);
        let col_b2 = make_list_dict(&[Some(vec!["z"])], &[Some(0)]);
        let mut groups = vec![];
        gv.intern(&[col_a2, col_b2], &mut groups).unwrap();
        assert_eq!(groups, vec![0]);
    }

    /// Resolve the logical string at position `i` in a Dictionary<Int32, Utf8> array.
    /// Works for any key ordering, not just sequential.
    fn logical_str(arr: &ArrayRef, i: usize) -> &str {
        let d = arr.as_dictionary::<Int32Type>();
        let key = d.keys().value(i) as usize;
        d.values().as_string::<i32>().value(key)
    }

    /// GroupDictionaryColumn and GroupValuesRows must assign identical group IDs
    /// and produce identical emitted values for the same inputs.
    #[test]
    fn test_parity_with_group_values_rows_utf8() {
        let schema = dict_schema();
        let mut gdc = GroupDictionaryColumn::new(Arc::clone(&schema)).unwrap();
        let mut gvr = GroupValuesRows::try_new(Arc::clone(&schema)).unwrap();

        let batches: Vec<[ArrayRef; 2]> = vec![
            [
                make_dict(&["a", "b"], &[Some(0), Some(1), Some(0)]),
                make_dict(&["x", "y"], &[Some(0), Some(1), Some(0)]),
            ],
            // second batch reuses same logical values via a fresh values Arc
            [
                make_dict(&["b", "c"], &[Some(0), Some(1), Some(0)]),
                make_dict(&["y", "z"], &[Some(0), Some(1), Some(0)]),
            ],
        ];

        for [col_a, col_b] in &batches {
            let mut gdc_groups = vec![];
            let mut gvr_groups = vec![];
            gdc.intern(&[Arc::clone(col_a), Arc::clone(col_b)], &mut gdc_groups)
                .unwrap();
            gvr.intern(&[Arc::clone(col_a), Arc::clone(col_b)], &mut gvr_groups)
                .unwrap();
            assert_eq!(gdc_groups, gvr_groups, "group IDs must agree");
        }

        let gdc_out = gdc.emit(EmitTo::All).unwrap();
        let gvr_out = gvr.emit(EmitTo::All).unwrap();

        assert_eq!(gdc_out.len(), gvr_out.len());
        for col_idx in 0..gdc_out.len() {
            let n = gdc_out[col_idx].len();
            assert_eq!(n, gvr_out[col_idx].len(), "col {col_idx} length mismatch");
            for i in 0..n {
                assert_eq!(
                    logical_str(&gdc_out[col_idx], i),
                    logical_str(&gvr_out[col_idx], i),
                    "col {col_idx} group {i} value mismatch"
                );
            }
        }
    }
}
