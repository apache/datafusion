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

//! [`DictionaryGroupValueBuilder`] for dictionary-encoded GROUP BY columns.

use std::marker::PhantomData;

use arrow::array::{Array, ArrayRef, DictionaryArray, new_null_array};
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, DataType};
use datafusion_common::Result;

use super::GroupColumn;

/// A [`GroupColumn`] wrapper that transparently handles dictionary-encoded
/// input arrays by resolving dictionary keys on-demand.
///
/// Instead of materializing the full decoded array via `cast()` (which copies
/// O(batch_size) strings per batch), this builder looks up values through
/// dictionary keys, only copying data for rows that are actually appended as
/// new groups. Comparisons index directly into the dictionary's values array.
///
/// The inner builder stores decoded values. On emit, the existing code in
/// [`GroupValuesColumn::emit`] re-encodes back to dictionary via `cast()`.
///
/// [`GroupValuesColumn::emit`]: super::GroupValuesColumn
pub struct DictionaryGroupValueBuilder<K: ArrowDictionaryKeyType> {
    /// Inner builder that operates on the dictionary's value type
    inner: Box<dyn GroupColumn>,
    /// A single-element null array of the value type, used to represent null
    /// dictionary keys to the inner builder
    null_array: ArrayRef,
    _phantom: PhantomData<K>,
}

impl<K: ArrowDictionaryKeyType> DictionaryGroupValueBuilder<K> {
    pub fn new(inner: Box<dyn GroupColumn>, value_type: &DataType) -> Self {
        let null_array = new_null_array(value_type, 1);
        Self {
            inner,
            null_array,
            _phantom: PhantomData,
        }
    }
}

impl<K: ArrowDictionaryKeyType + Send + Sync> GroupColumn for DictionaryGroupValueBuilder<K> {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        let dict = array
            .as_any()
            .downcast_ref::<DictionaryArray<K>>()
            .unwrap();
        if dict.is_null(rhs_row) {
            return self.inner.equal_to(lhs_row, &self.null_array, 0);
        }
        let key = dict.keys().value(rhs_row).as_usize();
        self.inner.equal_to(lhs_row, dict.values(), key)
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        let dict = array
            .as_any()
            .downcast_ref::<DictionaryArray<K>>()
            .unwrap();
        if dict.is_null(row) {
            return self.inner.append_val(&self.null_array, 0);
        }
        let key = dict.keys().value(row).as_usize();
        self.inner.append_val(dict.values(), key)
    }

    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        let dict = array
            .as_any()
            .downcast_ref::<DictionaryArray<K>>()
            .unwrap();
        let keys = dict.keys();
        let values = dict.values();

        if dict.null_count() == 0 {
            // Fast path: no null keys, remap indices and delegate to inner
            let mapped_rhs: Vec<usize> = rhs_rows
                .iter()
                .map(|&row| keys.value(row).as_usize())
                .collect();
            self.inner
                .vectorized_equal_to(lhs_rows, values, &mapped_rhs, equal_to_results);
        } else {
            // Null keys present: fall back to scalar comparison
            for (i, (&lhs_row, &rhs_row)) in
                lhs_rows.iter().zip(rhs_rows.iter()).enumerate()
            {
                if !equal_to_results[i] {
                    continue;
                }
                if dict.is_null(rhs_row) {
                    equal_to_results[i] =
                        self.inner.equal_to(lhs_row, &self.null_array, 0);
                } else {
                    let key = keys.value(rhs_row).as_usize();
                    equal_to_results[i] =
                        self.inner.equal_to(lhs_row, values, key);
                }
            }
        }
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) -> Result<()> {
        let dict = array
            .as_any()
            .downcast_ref::<DictionaryArray<K>>()
            .unwrap();
        let keys = dict.keys();
        let values = dict.values();

        if dict.null_count() == 0 {
            // Fast path: no null keys, remap indices and delegate to inner
            let mapped_rows: Vec<usize> = rows
                .iter()
                .map(|&row| keys.value(row).as_usize())
                .collect();
            self.inner.vectorized_append(values, &mapped_rows)
        } else {
            // Null keys present: process in order, chunking consecutive
            // non-null rows for vectorized processing
            let mut i = 0;
            while i < rows.len() {
                if dict.is_null(rows[i]) {
                    self.inner.append_val(&self.null_array, 0)?;
                    i += 1;
                } else {
                    // Collect consecutive non-null rows
                    let mut chunk = Vec::new();
                    while i < rows.len() && !dict.is_null(rows[i]) {
                        chunk.push(keys.value(rows[i]).as_usize());
                        i += 1;
                    }
                    self.inner.vectorized_append(values, &chunk)?;
                }
            }
            Ok(())
        }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn size(&self) -> usize {
        self.inner.size() + self.null_array.get_array_memory_size()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        self.inner.build()
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        self.inner.take_n(n)
    }
}
