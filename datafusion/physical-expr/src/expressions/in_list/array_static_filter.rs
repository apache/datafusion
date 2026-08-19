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

use arrow::array::{
    Array, ArrayRef, BooleanArray, downcast_array, downcast_dictionary_array,
    make_comparator,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::{SortOptions, take};
use arrow::datatypes::DataType;
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::Result;
use datafusion_common::hash_utils::{RandomState, with_hashes};
use hashbrown::HashTable;

use super::result::build_in_list_result;
use super::static_filter::StaticFilter;

/// Static filter for InList that stores the array and hash set for O(1) lookups
#[derive(Debug, Clone)]
pub(super) struct ArrayStaticFilter {
    in_array: ArrayRef,
    state: RandomState,
    /// Stores indices into `in_array` for O(1) lookups.
    table: HashTable<usize>,
}

impl ArrayStaticFilter {
    /// Computes a [`StaticFilter`] for the provided [`Array`] if there
    /// are nulls present or there are more than the configured number of
    /// elements.
    ///
    /// Note: This is split into a separate function as higher-rank trait bounds currently
    /// cause type inference to misbehave
    pub(super) fn try_new(in_array: ArrayRef) -> Result<ArrayStaticFilter> {
        // Null type has no natural order - return empty hash set
        if in_array.data_type() == &DataType::Null {
            return Ok(ArrayStaticFilter {
                in_array,
                state: RandomState::default(),
                table: HashTable::new(),
            });
        }

        let state = RandomState::default();
        let table = Self::build_haystack_table(&in_array, &state)?;

        Ok(Self {
            in_array,
            state,
            table,
        })
    }

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

impl StaticFilter for ArrayStaticFilter {
    fn null_count(&self) -> usize {
        self.in_array.null_count()
    }

    /// Checks if values in `v` are contained in the `in_array` using this hash set for lookup.
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

        // Unwrap dictionary-encoded needles when the value type matches
        // in_array, evaluating against the dictionary values and mapping
        // back via keys.
        downcast_dictionary_array! {
            v => {
                // Only unwrap when the haystack (in_array) type matches
                // the dictionary value type
                if v.values().data_type() == self.in_array.data_type() {
                    let values_contains = self.contains(v.values().as_ref(), negated)?;
                    let result = take(&values_contains, v.keys(), None)?;
                    return Ok(downcast_array(result.as_ref()));
                }
            }
            _ => {}
        }

        self.find_needles_in_haystack(v, negated)
    }
}
