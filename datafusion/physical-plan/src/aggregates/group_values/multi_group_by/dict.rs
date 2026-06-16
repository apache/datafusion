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
    Array, ArrayRef, AsArray, GenericStringArray, NullArray, OffsetSizeTrait,
};
use arrow::datatypes::{ArrowNativeType, DataType, SchemaRef};
use arrow::downcast_dictionary_array;
use datafusion_common::hash_utils::{RandomState, combine_hashes, create_hashes};
use datafusion_common::{Result, internal_datafusion_err};
use datafusion_execution::memory_pool::proxy::HashTableAllocExt;
use datafusion_expr::EmitTo;
use hashbrown::hash_table::HashTable;

use crate::aggregates::group_values::GroupValues;

/// Cached hashes and raw bytes for one dictionary column's values array.
/// Rebuilt only when the `Arc` pointer changes (i.e. a new values array arrives).
struct ColumnCache {
    /// Keeps the values `Arc` alive; compared with `Arc::ptr_eq` for staleness.
    values: ArrayRef,
    /// `value_hashes[k]` = hash of the value at dictionary index `k`.
    value_hashes: Vec<u64>,
    /// `value_bytes[k]` = encoded bytes for dictionary index `k`; `None` for null values.
    value_bytes: Vec<Option<Vec<u8>>>,
}

impl ColumnCache {
    fn empty() -> Self {
        Self {
            values: Arc::new(NullArray::new(0)),
            value_hashes: vec![],
            value_bytes: vec![],
        }
    }

    fn update(&mut self, new_values: ArrayRef, rs: &RandomState) -> Result<()> {
        if Arc::ptr_eq(&new_values, &self.values) {
            return Ok(());
        }
        let n = new_values.len();
        let mut hashes = vec![0u64; n];
        create_hashes(&[new_values.clone()], rs, &mut hashes)?;
        let bytes = (0..n)
            .map(|i| get_value_bytes(new_values.as_ref(), i))
            .collect::<Result<_>>()?;
        self.value_hashes = hashes;
        self.value_bytes = bytes;
        self.values = new_values;
        Ok(())
    }
}

/// [`GroupValues`] for GROUP BY over **two or more** dictionary-typed columns.
///
/// Rather than decoding dictionary values on every row, this implementation
/// works on the compact integer keys.  The cost grows with the number of
/// distinct value *combinations*, not with the number of input rows.
pub struct GroupDictionaryColumn {
    schema: SchemaRef,
    col_caches: Vec<ColumnCache>,
    /// `(row_hash, group_id)`.  Multiple entries may share the same hash value;
    /// byte-level comparison is used to resolve collisions.
    map: HashTable<(u64, usize)>,
    map_size: usize,
    /// All group rows packed into a single contiguous buffer.
    row_buffer: Vec<u8>,
    /// `row_offsets[g]` = start byte of group `g` inside `row_buffer`.
    row_offsets: Vec<usize>,
    /// Reused scratch buffer for encoding the current row during `intern`.
    row_scratch: Vec<u8>,
    random_state: RandomState,
}

/// Returns `true` when every field in `schema` is `DataType::Dictionary`.
pub fn all_dictionary_schema(schema: &arrow::datatypes::Schema) -> bool {
    schema
        .fields()
        .iter()
        .all(|f| matches!(f.data_type(), DataType::Dictionary(_, _)))
}

impl GroupDictionaryColumn {
    pub fn new(schema: SchemaRef) -> Result<Self> {
        if schema.fields().len() < 2 {
            return Err(internal_datafusion_err!(
                "GroupDictionaryColumn requires at least 2 columns, got {}",
                schema.fields().len()
            ));
        }
        for f in schema.fields() {
            if !matches!(f.data_type(), DataType::Dictionary(_, _)) {
                return Err(internal_datafusion_err!(
                    "GroupDictionaryColumn requires all columns to be Dictionary, \
                     but '{}' has type {}",
                    f.name(),
                    f.data_type()
                ));
            }
        }
        let n_cols = schema.fields().len();
        Ok(Self {
            schema,
            col_caches: (0..n_cols).map(|_| ColumnCache::empty()).collect(),
            map: HashTable::with_capacity(128),
            map_size: 0,
            row_buffer: Vec::new(),
            row_offsets: Vec::new(),
            row_scratch: Vec::new(),
            random_state: crate::aggregates::AGGREGATION_HASH_SEED,
        })
    }
}

// ── value-byte extraction ─────────────────────────────────────────────────────

/// Return the encoded bytes for `values[idx]`, or `None` if the value is null.
///
/// **Adding new value types**: add a match arm here.  The encoding only needs
/// to be injective (distinct values → distinct byte sequences); it does not
/// need to be reversible for `intern` purposes (only `emit` needs decoding).
fn get_value_bytes(values: &dyn Array, idx: usize) -> Result<Option<Vec<u8>>> {
    if values.is_null(idx) {
        return Ok(None);
    }
    match values.data_type() {
        DataType::Utf8 => Ok(Some(
            values.as_string::<i32>().value(idx).as_bytes().to_vec(),
        )),
        DataType::LargeUtf8 => Ok(Some(
            values.as_string::<i64>().value(idx).as_bytes().to_vec(),
        )),
        DataType::Utf8View => {
            Ok(Some(values.as_string_view().value(idx).as_bytes().to_vec()))
        }
        DataType::List(f) if matches!(f.data_type(), DataType::Utf8) => {
            let list = values.as_list::<i32>().value(idx);
            Ok(Some(encode_string_list(list.as_string::<i32>())))
        }
        DataType::LargeList(f) if matches!(f.data_type(), DataType::LargeUtf8) => {
            let list = values.as_list::<i64>().value(idx);
            Ok(Some(encode_string_list(list.as_string::<i64>())))
        }
        t => Err(internal_datafusion_err!(
            "GroupDictionaryColumn: unsupported dictionary value type {t}"
        )),
    }
}

/// Encode a string list as `[n: u32 LE]` then for each item
/// `[0x00]` (null) or `[0x01][len: u32 LE][bytes]`.
fn encode_string_list<O: OffsetSizeTrait>(arr: &GenericStringArray<O>) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());
    for j in 0..arr.len() {
        if arr.is_null(j) {
            buf.push(0);
        } else {
            let s = arr.value(j).as_bytes();
            buf.push(1);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s);
        }
    }
    buf
}

// ── dictionary key access ─────────────────────────────────────────────────────

fn dict_values_array(col: &dyn Array) -> ArrayRef {
    downcast_dictionary_array!(
        col => col.values().clone(),
        _ => unreachable!("schema validated in GroupDictionaryColumn::new")
    )
}

/// Pre-collect all row keys for one dictionary column as `Option<usize>`.
/// Doing this upfront avoids repeated macro dispatch inside the hot row loop.
fn collect_keys(col: &dyn Array) -> Vec<Option<usize>> {
    downcast_dictionary_array!(
        col => col.keys().iter().map(|k| k.map(|v| v.as_usize())).collect(),
        _ => unreachable!("schema validated in GroupDictionaryColumn::new")
    )
}

// ── row encoding ──────────────────────────────────────────────────────────────

/// Append one column's contribution to the scratch row buffer.
///
/// Per-column wire format:
///   null (null key **or** null dictionary value):  `[0x00]`
///   non-null value:                                `[0x01][len: u32 LE][bytes…]`
#[inline]
fn push_col_bytes(buf: &mut Vec<u8>, cache: &ColumnCache, key: Option<usize>) {
    match key.and_then(|k| cache.value_bytes[k].as_deref()) {
        None => buf.push(0),
        Some(vb) => {
            buf.push(1);
            buf.extend_from_slice(&(vb.len() as u32).to_le_bytes());
            buf.extend_from_slice(vb);
        }
    }
}

// ── GroupValues impl ──────────────────────────────────────────────────────────

impl GroupValues for GroupDictionaryColumn {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        debug_assert_eq!(cols.len(), self.schema.fields().len());
        groups.clear();

        if cols.is_empty() || cols[0].is_empty() {
            return Ok(());
        }
        let n_rows = cols[0].len();

        // Refresh column caches; a cache hit (same Arc pointer) is a no-op.
        for (c, col) in cols.iter().enumerate() {
            self.col_caches[c]
                .update(dict_values_array(col.as_ref()), &self.random_state)?;
        }

        // Pre-collect keys for all columns: avoids per-row macro dispatch.
        let all_keys: Vec<Vec<Option<usize>>> =
            cols.iter().map(|c| collect_keys(c.as_ref())).collect();

        groups.reserve(n_rows);

        for row in 0..n_rows {
            // 1. Combine per-column value hashes into one row hash.
            let combined_hash = {
                let mut h = 0u64;
                for (c, cache) in self.col_caches.iter().enumerate() {
                    let vh = all_keys[c][row].map_or(0, |k| cache.value_hashes[k]);
                    h = combine_hashes(h, vh);
                }
                h
            };

            // 2. Encode the row into the reusable scratch buffer.
            self.row_scratch.clear();
            for (c, cache) in self.col_caches.iter().enumerate() {
                push_col_bytes(&mut self.row_scratch, cache, all_keys[c][row]);
            }

            // 3. Look up an existing group whose bytes match.
            let found = {
                let scratch = self.row_scratch.as_slice();
                let rb = self.row_buffer.as_slice();
                let ro = self.row_offsets.as_slice();
                self.map
                    .find(combined_hash, |&(h, g)| {
                        h == combined_hash && {
                            let end = ro.get(g + 1).copied().unwrap_or(rb.len());
                            rb[ro[g]..end] == *scratch
                        }
                    })
                    .map(|&(_, g)| g)
            };

            // 4. Reuse existing group or create a new one.
            let group_id = match found {
                Some(g) => g,
                None => {
                    let g = self.row_offsets.len();
                    self.row_offsets.push(self.row_buffer.len());
                    self.row_buffer.extend_from_slice(&self.row_scratch);
                    self.map.insert_accounted(
                        (combined_hash, g),
                        |(h, _)| *h,
                        &mut self.map_size,
                    );
                    g
                }
            };

            groups.push(group_id);
        }

        Ok(())
    }

    fn size(&self) -> usize {
        let cache_bytes: usize = self
            .col_caches
            .iter()
            .map(|c| {
                c.value_hashes.len() * size_of::<u64>()
                    + c.value_bytes
                        .iter()
                        .map(|b| b.as_ref().map_or(0, |v| v.len()))
                        .sum::<usize>()
            })
            .sum();
        self.map_size
            + self.row_buffer.len()
            + self.row_offsets.len() * size_of::<usize>()
            + cache_bytes
    }

    fn is_empty(&self) -> bool {
        self.row_offsets.is_empty()
    }

    fn len(&self) -> usize {
        self.row_offsets.len()
    }

    fn emit(&mut self, _emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        todo!("GroupDictionaryColumn::emit")
    }

    fn clear_shrink(&mut self, num_rows: usize) {
        self.map.clear();
        self.map.shrink_to(num_rows, |_| 0);
        self.map_size = self.map.capacity() * size_of::<(u64, usize)>();
        self.row_buffer.clear();
        self.row_buffer.shrink_to(num_rows * 16);
        self.row_offsets.clear();
        self.row_offsets.shrink_to(num_rows);
        for cache in &mut self.col_caches {
            *cache = ColumnCache::empty();
        }
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::{Field, Int32Type, Schema};

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
        let a1 = make_dict(&["a", "b"], &[Some(0), Some(1)]);
        let b1 = make_dict(&["x"], &[Some(0), Some(0)]);
        let mut g1 = vec![];
        gv.intern(&[a1, b1], &mut g1).unwrap();
        assert_eq!(g1, vec![0, 1]);

        // Batch 2: same logical rows but DIFFERENT Arc (values order swapped).
        // key=0 now means "b", key=1 means "a"  — opposite of batch 1.
        let a2 = make_dict(&["b", "a"], &[Some(0), Some(1)]);
        let b2 = make_dict(&["x"], &[Some(0), Some(0)]);
        let mut g2 = vec![];
        gv.intern(&[a2, b2], &mut g2).unwrap();

        // ("b", "x") was group 1 in batch 1; ("a", "x") was group 0.
        assert_eq!(
            g2[0], 1,
            "('b','x') should map to the existing group for 'b'"
        );
        assert_eq!(
            g2[1], 0,
            "('a','x') should map to the existing group for 'a'"
        );
        assert_eq!(gv.len(), 2, "no new groups should have been created");
    }
}
