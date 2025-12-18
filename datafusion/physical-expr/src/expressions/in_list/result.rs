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

//! Result building helpers for InList operations
//!
//! This module provides unified logic for building BooleanArray results
//! from IN list membership tests, handling null propagation correctly
//! according to SQL three-valued logic.

use arrow::array::BooleanArray;
use arrow::buffer::{BooleanBuffer, NullBuffer};

// =============================================================================
// RESULT BUILDER FOR IN LIST OPERATIONS
// =============================================================================
//
// Truth table for (needle_nulls, haystack_has_nulls, negated):
// (Some, true,  false) → values: valid & contains,           nulls: valid & contains
// (None, true,  false) → values: contains,                   nulls: contains
// (Some, true,  true)  → values: valid ^ (valid & contains), nulls: valid & contains
// (None, true,  true)  → values: !contains,                  nulls: contains
// (Some, false, false) → values: valid & contains,           nulls: valid
// (Some, false, true)  → values: valid & !contains,          nulls: valid
// (None, false, false) → values: contains,                   nulls: none
// (None, false, true)  → values: !contains,                  nulls: none

/// Builds a BooleanArray result for IN list operations.
///
/// This function handles the complex null propagation logic for SQL IN lists:
/// - If the needle value is null, the result is null
/// - If the needle is not in the set AND the haystack has nulls, the result is null
/// - Otherwise, the result is true/false based on membership and negation
#[inline]
pub(crate) fn build_in_list_result<C>(
    len: usize,
    needle_nulls: Option<&NullBuffer>,
    haystack_has_nulls: bool,
    negated: bool,
    contains: C,
) -> BooleanArray
where
    C: FnMut(usize) -> bool,
{
    // Pass closure by value to avoid indirection on each call
    let contains_buf = BooleanBuffer::collect_bool(len, contains);
    build_result_from_contains(needle_nulls, haystack_has_nulls, negated, contains_buf)
}

/// Builds a BooleanArray result from a pre-computed contains buffer.
#[inline]
pub(crate) fn build_result_from_contains(
    needle_nulls: Option<&NullBuffer>,
    haystack_has_nulls: bool,
    negated: bool,
    contains_buf: BooleanBuffer,
) -> BooleanArray {
    match (needle_nulls, haystack_has_nulls, negated) {
        (Some(v), true, false) => {
            let buf = v.inner() & &contains_buf;
            BooleanArray::new(buf.clone(), Some(NullBuffer::new(buf)))
        }
        (None, true, false) => {
            BooleanArray::new(contains_buf.clone(), Some(NullBuffer::new(contains_buf)))
        }
        (Some(v), true, true) => {
            let nulls = v.inner() & &contains_buf;
            BooleanArray::new(v.inner() ^ &nulls, Some(NullBuffer::new(nulls)))
        }
        (None, true, true) => {
            BooleanArray::new(!&contains_buf, Some(NullBuffer::new(contains_buf)))
        }
        (Some(v), false, false) => {
            BooleanArray::new(v.inner() & &contains_buf, Some(v.clone()))
        }
        (Some(v), false, true) => {
            BooleanArray::new(v.inner() & &(!&contains_buf), Some(v.clone()))
        }
        (None, false, false) => BooleanArray::new(contains_buf, None),
        (None, false, true) => BooleanArray::new(!&contains_buf, None),
    }
}

// =============================================================================
// DICTIONARY ARRAY HANDLING
// =============================================================================

/// Macro to handle dictionary arrays in StaticFilter::contains implementations.
///
/// This macro extracts the dictionary values, performs the contains check on
/// the values array, and then uses `take` to map the results back to the
/// dictionary keys.
macro_rules! handle_dictionary {
    ($self:ident, $v:ident, $negated:ident) => {
        arrow::array::downcast_dictionary_array! {
            $v => {
                let values_contains = $self.contains($v.values().as_ref(), $negated)?;
                let result = arrow::compute::take(&values_contains, $v.keys(), None)?;
                return Ok(arrow::array::downcast_array(result.as_ref()))
            }
            _ => {}
        }
    };
}

pub(crate) use handle_dictionary;
