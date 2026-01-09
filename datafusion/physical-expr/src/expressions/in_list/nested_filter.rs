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

//! Fallback filter for nested/complex types (List, Struct, Map, Union, etc.)

use arrow::array::{
    Array, ArrayRef, BooleanArray, downcast_array, downcast_dictionary_array,
    make_comparator,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::{SortOptions, take};
use arrow::datatypes::DataType;
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::Result;
use datafusion_common::hash_utils::with_hashes;

use ahash::RandomState;
use hashbrown::HashTable;

use super::result::build_in_list_result;
use super::static_filter::StaticFilter;

/// Fallback filter for nested/complex types (List, Struct, Map, Union, etc.)
///
/// Uses dynamic comparator via `make_comparator` since these types don't have
/// a simple typed comparison. For primitive and byte array types, use the
/// specialized filters instead (PrimitiveFilter, ByteArrayFilter, etc.)
#[derive(Debug, Clone)]
pub(crate) struct NestedTypeFilter {
    in_array: ArrayRef,
    state: RandomState,
    /// Stores indices into `in_array` for O(1) lookups.
    table: HashTable<usize>,
}

impl NestedTypeFilter {
    /// Creates a filter for nested/complex array types.
    ///
    /// This filter uses dynamic comparison and should only be used for types
    /// that don't have specialized filters (List, Struct, Map, Union).
    pub(crate) fn try_new(in_array: ArrayRef) -> Result<Self> {
        // Null type has no natural order - return empty hash set
        if in_array.data_type() == &DataType::Null {
            return Ok(Self {
                in_array,
                state: RandomState::new(),
                table: HashTable::new(),
            });
        }

        let state = RandomState::new();
        let table = Self::build_haystack_table(&in_array, &state)?;

        Ok(Self {
            in_array,
            state,
            table,
        })
    }

    /// Build a hash table from haystack values for O(1) lookups.
    ///
    /// Each unique non-null value's index is stored, keyed by its hash.
    /// Uses dynamic comparison via `make_comparator` for complex types.
    fn build_haystack_table(
        haystack: &ArrayRef,
        state: &RandomState,
    ) -> Result<HashTable<usize>> {
        let mut table = HashTable::new();

        with_hashes([haystack.as_ref()], state, |hashes| -> Result<()> {
            let cmp = make_comparator(haystack, haystack, SortOptions::default())?;

            let insert_value = |idx| {
                let hash = hashes[idx];
                // Only insert if not already present (deduplication)
                if table.find(hash, |&x| cmp(x, idx).is_eq()).is_none() {
                    table.insert_unique(hash, idx, |&x| hashes[x]);
                }
            };

            match haystack.nulls() {
                Some(nulls) => {
                    BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                        .for_each(insert_value)
                }
                None => (0..haystack.len()).for_each(insert_value),
            }

            Ok(())
        })?;

        Ok(table)
    }

    /// Check which needle values exist in the haystack.
    ///
    /// Hashes each needle value and looks it up in the pre-built haystack table.
    /// Uses dynamic comparison via `make_comparator` for complex types.
    fn find_needles_in_haystack(
        &self,
        needles: &dyn Array,
        negated: bool,
    ) -> Result<BooleanArray> {
        let needle_nulls = needles.logical_nulls();
        let haystack_has_nulls = self.in_array.null_count() != 0;

        with_hashes([needles], &self.state, |needle_hashes| {
            let cmp = make_comparator(needles, &self.in_array, SortOptions::default())?;

            Ok(build_in_list_result(
                needles.len(),
                needle_nulls.as_ref(),
                haystack_has_nulls,
                negated,
                #[inline(always)]
                |i| {
                    let hash = needle_hashes[i];
                    self.table.find(hash, |&idx| cmp(i, idx).is_eq()).is_some()
                },
            ))
        })
    }
}

impl StaticFilter for NestedTypeFilter {
    fn null_count(&self) -> usize {
        self.in_array.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        // Null type comparisons always return null (SQL three-valued logic)
        if v.data_type() == &DataType::Null
            || self.in_array.data_type() == &DataType::Null
        {
            let nulls = NullBuffer::new_null(v.len());
            return Ok(BooleanArray::new(
                BooleanBuffer::new_unset(v.len()),
                Some(nulls),
            ));
        }

        downcast_dictionary_array! {
            v => {
                let values_contains = self.contains(v.values().as_ref(), negated)?;
                let result = take(&values_contains, v.keys(), None)?;
                return Ok(downcast_array(result.as_ref()))
            }
            _ => {}
        }

        self.find_needles_in_haystack(v, negated)
    }
}
