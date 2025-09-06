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

//! Index cache for MATCH_RECOGNIZE classifier masks.
//!
//! This module provides `MaskIndexCache`, a small helper used by the
//! MATCH_RECOGNIZE window evaluators (see
//! `MatchRecognizeEdgeEvaluator` and `MatchRecognizeShiftEvaluator`).
//! Given a boolean classifier mask column (as an Arrow `BooleanArray`),
//! it lazily precomputes, in a single pass, two kinds of indices:
//!
//! - nearest prior-or-equal match: for each row `i`, the index of the most
//!   recent row at or before `i` where the mask is true
//! - first match index: the lowest row index where the mask is true
//!
//! These indices let evaluators answer queries like "last match up to row i"
//! and "first match overall" in O(1) time per row during window evaluation,
//! avoiding repeated scans of the mask. Building the cache is O(n) time and
//! O(n) memory for `n` rows. Mask nulls are treated as non-matches.
//!
//! When no classifier mask is provided (`mask_present == false`), evaluators
//! take unmasked fast paths and this cache remains disabled. Callers should
//! invoke `ensure_built` once per mask before querying, and may call `clear`
//! when pruning/memoizing between chunks to bound memory and avoid stale state.

use arrow::array::{Array, BooleanArray};

#[derive(Debug)]
pub(super) struct MaskIndexCache {
    /// Whether a classifier mask column is present for the current evaluation.
    /// When false, all cache logic is bypassed and evaluators take their
    /// unmasked fast-paths.
    pub(super) mask_present: bool,
    /// For each row i, the index of the nearest mask match at or before i.
    /// None means no match exists up to and including i.
    pub(super) nearest_match_index_by_row: Option<Vec<Option<usize>>>,
    /// The first (lowest) row index where the mask is true. None if no match.
    pub(super) first_match_index: Option<usize>,
}

impl MaskIndexCache {
    /// Create a new cache configured for whether a mask column is present.
    pub(super) fn new(mask_provided: bool) -> Self {
        Self {
            mask_present: mask_provided,
            nearest_match_index_by_row: None,
            first_match_index: None,
        }
    }

    #[inline]
    /// Returns true if a classifier mask is present.
    pub(super) fn has_mask(&self) -> bool {
        self.mask_present
    }

    /// Ensure the cache is populated for the provided mask. No-op if already
    /// built or if no mask is present.
    pub(super) fn ensure_built(&mut self, mask_arr: &BooleanArray) {
        if self.nearest_match_index_by_row.is_none() && self.mask_present {
            let (vec, first) = Self::compute_nearest_before(mask_arr);
            self.nearest_match_index_by_row = Some(vec);
            self.first_match_index = first;
        }
    }

    #[inline]
    /// Returns the nearest matched row index at or before `idx`, or None if
    /// no match exists up to and including `idx`.
    pub(super) fn nearest_match_index_at_or_before(&self, idx: usize) -> Option<usize> {
        self.nearest_match_index_by_row
            .as_ref()
            .and_then(|v| v[idx])
    }

    #[inline]
    /// Returns the first (lowest) matched row index, or None if there is no match.
    pub(super) fn first_match_index(&self) -> Option<usize> {
        self.first_match_index
    }

    /// Clears any computed cache. Intended to be called when pruning or after
    /// finalizing results to bound memory and avoid stale state.
    pub(super) fn clear(&mut self) {
        self.nearest_match_index_by_row = None;
        self.first_match_index = None;
    }

    /// Precompute, for each row i, the nearest prior-or-equal match index.
    /// Also returns the first match index across the entire mask.
    fn compute_nearest_before(
        mask_arr: &BooleanArray,
    ) -> (Vec<Option<usize>>, Option<usize>) {
        let mut nearest = Vec::with_capacity(mask_arr.len());
        let mut last_match: Option<usize> = None;
        let mut first_match: Option<usize> = None;

        for i in 0..mask_arr.len() {
            if mask_arr.is_valid(i) && mask_arr.value(i) {
                last_match = Some(i);
                if first_match.is_none() {
                    first_match = last_match;
                }
            }
            nearest.push(last_match);
        }
        (nearest, first_match)
    }
}
