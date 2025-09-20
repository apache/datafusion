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

//! Candidate ordering helper used by `PatternMatcher`.
//!
//! When multiple complete matches compete for the **same start row** the SQL
//! standard dictates a two-tiered tie-break:
//!
//! 1. **Greedy** – Prefer the path that consumes more rows (`PathScore`).
//! 2. **Left-most** – If scores tie, pick the match whose final accepting state
//!    appears earlier in the pattern (lower `state_id`).
//! 3. As a last resort prefer the match that ends **earlier** in the input.
//!
//! `Candidate` encodes those rules directly in its `Ord` implementation so we
//! can use standard `std::cmp` helpers for comparison and sorting.
//!
//! This module is intentionally tiny – it keeps the ordering logic separate
//! from the rest of the matcher making the tie-break semantics easy to audit.

use std::cmp::{Ordering, Reverse};

use crate::match_recognize::nfa::{PathScore, RowIdx};

/// `(Reverse(score), state_id, row)` ordering:
///   1. Higher `PathScore` (greedy) wins.
///   2. If scores tie the smaller `state_id` (left‐most) wins.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct Candidate {
    pub(crate) score: PathScore,
    pub(crate) state_id: usize,
    pub(crate) row: RowIdx,
    pub(crate) alt_idx: Option<u32>,
}

impl Ord for Candidate {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        // Primary comparison: alternation branch precedence.
        match (self.alt_idx, other.alt_idx) {
            (Some(a), Some(b)) if a != b => return a.cmp(&b),
            (Some(_), None) => return Ordering::Less,
            (None, Some(_)) => return Ordering::Greater,
            _ => {}
        }

        (Reverse(self.score), self.state_id, self.row).cmp(&(
            Reverse(other.score),
            other.state_id,
            other.row,
        ))
    }
}

impl PartialOrd for Candidate {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
