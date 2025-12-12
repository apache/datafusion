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
//!
//! This filter is the fallback for complex types that don't have specialized
//! implementations (e.g., structs, nested types).

use arrow::array::*;
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::{SortOptions, take};
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::hash_utils::with_hashes;
use datafusion_common::{HashMap, Result};

use ahash::RandomState;
use hashbrown::hash_map::RawEntryMut;

use super::filter::StaticFilter;

/// Static filter for InList that stores the array and hash set for O(1) lookups
///
/// This is the generic fallback filter that works with any Arrow array type
/// that supports comparison and hashing.
#[derive(Debug, Clone)]
pub(crate) struct ArrayStaticFilter {
    in_array: ArrayRef,
    state: RandomState,
    /// Used to provide a lookup from value to in list index
    ///
    /// Note: usize::hash is not used, instead the raw entry
    /// API is used to store entries w.r.t their value
    map: HashMap<usize, (), ()>,
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
        if in_array.data_type() == &arrow::datatypes::DataType::Null {
            return Ok(ArrayStaticFilter {
                in_array,
                state: RandomState::new(),
                map: HashMap::with_hasher(()),
            });
        }

        let state = RandomState::new();
        let mut map: HashMap<usize, (), ()> = HashMap::with_hasher(());

        with_hashes([&in_array], &state, |hashes| -> Result<()> {
            let cmp = make_comparator(&in_array, &in_array, SortOptions::default())?;

            let insert_value = |idx| {
                let hash = hashes[idx];
                if let RawEntryMut::Vacant(v) = map
                    .raw_entry_mut()
                    .from_hash(hash, |x| cmp(*x, idx).is_eq())
                {
                    v.insert_with_hasher(hash, idx, (), |x| hashes[*x]);
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
            map,
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
        if v.data_type() == &arrow::datatypes::DataType::Null
            || self.in_array.data_type() == &arrow::datatypes::DataType::Null
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
                    let contains = self
                        .map
                        .raw_entry()
                        .from_hash(hash, |idx| cmp(i, *idx).is_eq())
                        .is_some();

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
