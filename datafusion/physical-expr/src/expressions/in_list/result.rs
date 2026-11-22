// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Result building helpers for InList operations.
//!
//! This module provides unified logic for building BooleanArray results
//! from IN list membership tests, handling null propagation correctly
//! according to SQL three-valued logic.

use arrow::array::BooleanArray;
use arrow::buffer::{BooleanBuffer, NullBuffer};

// Truth table for `value [NOT] IN (set)` with SQL three-valued logic:
// ("-" means the value does not affect the result)
//
// | needle null | set has null | negated | found in set | result |
// |-------------|--------------|---------|--------------|--------|
// | true        | -            | false   | -            | null   |
// | true        | -            | true    | -            | null   |
// | false       | true         | false   | true         | true   |
// | false       | true         | false   | false        | null   |
// | false       | true         | true    | true         | false  |
// | false       | true         | true    | false        | null   |
// | false       | false        | false   | true         | true   |
// | false       | false        | false   | false        | false  |
// | false       | false        | true    | true         | false  |
// | false       | false        | true    | false        | true   |

/// Builds a BooleanArray result for IN list operations.
///
/// This function handles the null propagation logic for SQL IN lists:
/// - If the needle value is null, the result is null
/// - If the needle is not in the set and the haystack has nulls, the result is null
/// - Otherwise, the result is true/false based on membership and negation
///
/// This version computes contains for all positions, including nulls, then applies
/// null masking via bitmap operations.
#[inline]
pub(super) fn build_in_list_result<C>(
    len: usize,
    needle_nulls: Option<&NullBuffer>,
    haystack_has_nulls: bool,
    negated: bool,
    contains: C,
) -> BooleanArray
where
    C: FnMut(usize) -> bool,
{
    let contains_buf = BooleanBuffer::collect_bool(len, contains);
    build_result_from_contains(needle_nulls, haystack_has_nulls, negated, contains_buf)
}

/// Builds a BooleanArray result from a pre-computed contains buffer.
///
/// This version does not assume contains_buf is pre-masked at null positions.
/// It handles nulls using bitmap operations.
#[inline]
pub(super) fn build_result_from_contains(
    needle_nulls: Option<&NullBuffer>,
    haystack_has_nulls: bool,
    negated: bool,
    contains_buf: BooleanBuffer,
) -> BooleanArray {
    match (needle_nulls, haystack_has_nulls, negated) {
        // Haystack has nulls: result is null unless value is found.
        (Some(v), true, false) => {
            // values: valid & contains, nulls: valid & contains
            let values = v.inner() & &contains_buf;
            BooleanArray::new(values.clone(), Some(NullBuffer::new(values)))
        }
        (None, true, false) => {
            BooleanArray::new(contains_buf.clone(), Some(NullBuffer::new(contains_buf)))
        }
        (Some(v), true, true) => {
            // NOT IN with nulls: false if found, null if not found or needle null.
            // values: valid & !contains, nulls: valid & contains
            let valid = v.inner();
            let values = valid & &(!&contains_buf);
            let nulls = valid & &contains_buf;
            BooleanArray::new(values, Some(NullBuffer::new(nulls)))
        }
        (None, true, true) => {
            BooleanArray::new(!&contains_buf, Some(NullBuffer::new(contains_buf)))
        }
        // Haystack has no nulls: result validity follows needle validity.
        (Some(v), false, false) => {
            // values: valid & contains, nulls: valid
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
