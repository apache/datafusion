//! Candidate ordering helper used by `PatternMatcher`.
//!
//! When multiple complete matches compete for the **same start row** the SQL
//! standard dictates a two-tiered tie-break:
//!
//! 1. **Greedy** – Prefer the path that consumes more rows (implemented via
//!    a higher [`PathScore`]).
//! 2. **Left-most** – If the score is identical prefer the match whose final
//!    accepting state appears earlier in the pattern definition (lower
//!    `state_id`).
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
///   2. For equal scores the smaller `state_id` (left‐most) wins.
///   3. Finally the earlier `row` wins.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct Candidate {
    pub(crate) score: PathScore,
    pub(crate) state_id: usize,
    pub(crate) row: RowIdx,
}

impl Ord for Candidate {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
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
