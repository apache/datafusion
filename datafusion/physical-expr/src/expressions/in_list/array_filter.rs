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

//! Generic array-based static filter using hash lookups

use arrow::array::*;
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::{SortOptions, take};
use arrow::datatypes::DataType;
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::Result;
use datafusion_common::hash_utils::with_hashes;

use ahash::RandomState;
use hashbrown::HashTable;

/// Trait for InList static filters
pub(crate) trait StaticFilter {
    fn null_count(&self) -> usize;

    /// Checks if values in `v` are contained in the filter
    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray>;
}

/// Static filter for InList that stores the array and hash set for O(1) lookups
#[derive(Debug, Clone)]
pub(crate) struct ArrayStaticFilter {
    in_array: ArrayRef,
    state: RandomState,
    /// Stores indices into `in_array` for O(1) lookups.
    ///
    /// Uses pre-computed hashes and custom equality based on array values
    /// rather than the indices themselves.
    table: HashTable<usize>,
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

        downcast_dictionary_array! {
            v => {
                let values_contains = self.contains(v.values().as_ref(), negated)?;
                let result = take(&values_contains, v.keys(), None)?;
                return Ok(downcast_array(result.as_ref()))
            }
            _ => {}
        }

        let needle_nulls = v.logical_nulls();
        let needle_nulls = needle_nulls.as_ref();
        let haystack_has_nulls = self.in_array.null_count() != 0;

        with_hashes([v], &self.state, |hashes| {
            let cmp = make_comparator(v, &self.in_array, SortOptions::default())?;
            Ok((0..v.len())
                .map(|i| {
                    // SQL three-valued logic: null IN (...) is always null
                    if needle_nulls.is_some_and(|nulls| nulls.is_null(i)) {
                        return None;
                    }

                    let hash = hashes[i];
                    let contains =
                        self.table.find(hash, |idx| cmp(i, *idx).is_eq()).is_some();

                    match contains {
                        true => Some(!negated),
                        false if haystack_has_nulls => None,
                        false => Some(negated),
                    }
                })
                .collect())
        })
    }
}

impl ArrayStaticFilter {
    /// Computes a [`StaticFilter`] for the provided [`Array`] if there
    /// are nulls present or there are more than the configured number of
    /// elements.
    ///
    /// Note: This is split into a separate function as higher-rank trait bounds currently
    /// cause type inference to misbehave
    pub(crate) fn try_new(in_array: ArrayRef) -> Result<ArrayStaticFilter> {
        // Null type has no natural order - return empty hash set
        if in_array.data_type() == &DataType::Null {
            return Ok(ArrayStaticFilter {
                in_array,
                state: RandomState::new(),
                table: HashTable::new(),
            });
        }

        let state = RandomState::new();
        let mut table = HashTable::new();

        with_hashes([&in_array], &state, |hashes| -> Result<()> {
            let cmp = make_comparator(&in_array, &in_array, SortOptions::default())?;

            let insert_value = |idx| {
                let hash = hashes[idx];
                // Only insert if not already present
                if table.find(hash, |x| cmp(*x, idx).is_eq()).is_none() {
                    table.insert_unique(hash, idx, |x| hashes[*x]);
                }
            };

            match in_array.nulls() {
                Some(nulls) => {
                    BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                        .for_each(insert_value)
                }
                None => (0..in_array.len()).for_each(insert_value),
            }

            Ok(())
        })?;

        Ok(Self {
            in_array,
            state,
            table,
        })
    }
}
