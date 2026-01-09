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

/// Builds a BooleanArray result for IN list operations (optimized for cheap contains).
///
/// This function handles the complex null propagation logic for SQL IN lists:
/// - If the needle value is null, the result is null
/// - If the needle is not in the set AND the haystack has nulls, the result is null
/// - Otherwise, the result is true/false based on membership and negation
///
/// This version computes contains for ALL positions (including nulls), then applies
/// null masking via bitmap operations. This is optimal for cheap contains checks
/// (like DirectProbeFilter) where the branch overhead exceeds the check cost.
///
/// For expensive contains checks (like ByteViewMaskedFilter with string comparison),
/// use `build_in_list_result_with_null_shortcircuit` instead.
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
    // Always compute the contains buffer without checking nulls in the loop.
    // The null check inside the loop hurts vectorization and branch prediction.
    // Nulls are handled by build_result_from_contains using bitmap operations.
    let contains_buf = BooleanBuffer::collect_bool(len, contains);
    build_result_from_contains(needle_nulls, haystack_has_nulls, negated, contains_buf)
}

/// Builds a BooleanArray result with null short-circuit (optimized for expensive contains).
///
/// Unlike `build_in_list_result`, this version checks nulls INSIDE the loop and
/// skips the contains check for null positions. This is optimal for expensive
/// contains checks (like ByteViewMaskedFilter with hash lookup + string comparison) where
/// skipping lookups outweighs the branch overhead.
///
/// The shortcircuit is only applied when `needle_null_count > 0` - if there are
/// no actual nulls, we avoid the branch overhead entirely.
///
/// Use this for: ByteViewMaskedFilter, Utf8TwoStageFilter (string/binary types)
/// Use `build_in_list_result` for: DirectProbeFilter, BranchlessFilter (primitive types)
#[inline]
pub(crate) fn build_in_list_result_with_null_shortcircuit<C>(
    len: usize,
    needle_nulls: Option<&NullBuffer>,
    needle_null_count: usize,
    haystack_has_nulls: bool,
    negated: bool,
    mut contains: C,
) -> BooleanArray
where
    C: FnMut(usize) -> bool,
{
    // When null_count=0, treat as no validity buffer to avoid extra work.
    // The validity buffer might exist but have all bits set to true.
    let effective_nulls = needle_nulls.filter(|_| needle_null_count > 0);

    match effective_nulls {
        Some(nulls) => {
            // Has nulls: check validity inside loop to skip expensive contains()
            let contains_buf =
                BooleanBuffer::collect_bool(len, |i| nulls.is_valid(i) && contains(i));
            build_result_from_contains_premasked(
                Some(nulls),
                haystack_has_nulls,
                negated,
                contains_buf,
            )
        }
        None => {
            // No nulls: compute contains for all positions without branch overhead
            let contains_buf = BooleanBuffer::collect_bool(len, contains);
            // Use premasked path since contains_buf is "trivially premasked" (no nulls to mask)
            build_result_from_contains_premasked(
                None,
                haystack_has_nulls,
                negated,
                contains_buf,
            )
        }
    }
}

/// Builds result from a contains buffer that was pre-masked at null positions.
///
/// This is used by `build_in_list_result_with_null_shortcircuit` where the
/// contains buffer already has `false` at null positions due to the short-circuit.
///
/// Since contains_buf is pre-masked (false at null positions), we can simplify:
/// - `valid & contains_buf` = `contains_buf` (already 0 where valid is 0)
/// - XOR can replace AND+NOT for the negated case: `valid ^ contains = valid & !contains`
#[inline]
fn build_result_from_contains_premasked(
    needle_nulls: Option<&NullBuffer>,
    haystack_has_nulls: bool,
    negated: bool,
    contains_buf: BooleanBuffer,
) -> BooleanArray {
    match (needle_nulls, haystack_has_nulls, negated) {
        // Haystack has nulls: result is null unless value is found
        (_, true, false) => {
            // contains_buf is already masked (false at null positions)
            BooleanArray::new(contains_buf.clone(), Some(NullBuffer::new(contains_buf)))
        }
        (Some(v), true, true) => {
            // NOT IN with nulls: true if valid and not found, null if found or needle null
            // XOR: valid ^ contains = 1 iff valid=1 and contains=0 (not found)
            BooleanArray::new(
                v.inner() ^ &contains_buf,
                Some(NullBuffer::new(contains_buf)),
            )
        }
        (None, true, true) => {
            BooleanArray::new(!&contains_buf, Some(NullBuffer::new(contains_buf)))
        }
        // Haystack has no nulls: result validity follows needle validity
        (Some(v), false, false) => {
            // contains_buf is already masked, just use needle validity for nulls
            BooleanArray::new(contains_buf, Some(v.clone()))
        }
        (Some(v), false, true) => {
            // Need AND because !contains_buf is 1 at null positions
            BooleanArray::new(v.inner() & &(!&contains_buf), Some(v.clone()))
        }
        (None, false, false) => BooleanArray::new(contains_buf, None),
        (None, false, true) => BooleanArray::new(!&contains_buf, None),
    }
}

/// Builds a BooleanArray result from a pre-computed contains buffer.
///
/// This version does NOT assume contains_buf is pre-masked at null positions.
/// It handles nulls using bitmap operations which are more vectorization-friendly.
#[inline]
pub(crate) fn build_result_from_contains(
    needle_nulls: Option<&NullBuffer>,
    haystack_has_nulls: bool,
    negated: bool,
    contains_buf: BooleanBuffer,
) -> BooleanArray {
    match (needle_nulls, haystack_has_nulls, negated) {
        // Haystack has nulls: result is null unless value is found
        (Some(v), true, false) => {
            // values: valid & contains, nulls: valid & contains
            // Result is valid (not null) only when needle is valid AND found in haystack
            let values = v.inner() & &contains_buf;
            BooleanArray::new(values.clone(), Some(NullBuffer::new(values)))
        }
        (None, true, false) => {
            BooleanArray::new(contains_buf.clone(), Some(NullBuffer::new(contains_buf)))
        }
        (Some(v), true, true) => {
            // NOT IN with nulls: true if valid and not found, null if found or needle null
            // values: valid & !contains, nulls: valid & contains
            // Result is valid only when needle is valid AND found (because NOT IN with
            // haystack nulls returns NULL when value isn't definitively excluded)
            let valid = v.inner();
            let values = valid & &(!&contains_buf);
            let nulls = valid & &contains_buf;
            BooleanArray::new(values, Some(NullBuffer::new(nulls)))
        }
        (None, true, true) => {
            BooleanArray::new(!&contains_buf, Some(NullBuffer::new(contains_buf)))
        }
        // Haystack has no nulls: result validity follows needle validity
        (Some(v), false, false) => {
            // values: valid & contains (mask out nulls), nulls: valid
            BooleanArray::new(v.inner() & &contains_buf, Some(v.clone()))
        }
        (Some(v), false, true) => {
            // values: valid & !contains, nulls: valid
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
