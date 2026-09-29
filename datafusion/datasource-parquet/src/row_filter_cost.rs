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

//! What a predicate of the Parquet `RowFilter` costs and what it saves,
//! in addition to its evaluation time.
//!
//! * The *cost*: each row filter stage has a fixed cost for each row that it
//!   evaluates ([`ROW_FILTER_STAGE_NS_PER_ROW`]).
//! * The *saving*: the decoder does not decode the other columns of the
//!   rows that the predicate removes only when these rows make long runs.
//!   [`skippable_rows`] counts the rows in such runs.
//!
//! It also has the order of the predicates: by measured rows removed for
//! each nanosecond ([`rank`], [`evaluation_order`]), with hysteresis on the
//! changes of a file ([`ChangeHysteresis`]).

use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{Array, BooleanArray};
use arrow::buffer::BooleanBuffer;
use datafusion_physical_expr::filter_stats::{FilterCost, MIN_OBSERVED_ROWS};

/// The fixed cost of a row filter stage for each row that it evaluates,
/// in nanoseconds. It does not depend on the predicate: the decoder builds a
/// `RowSelection` from the result, skips or reads the records of the other
/// columns with it (`skip_records`), fills the predicate cache
/// (`CachedArrayReader`) and fetches the columns of the stage before the
/// other columns. Profiles of TPC-H SF1 Q6, Q12 and Q4 on an Apple M-series
/// laptop showed 4 to 16 ns for each row and stage, about 20 times the time
/// of the predicate. The post-scan filter has none of these costs.
pub(crate) const ROW_FILTER_STAGE_NS_PER_ROW: f64 = 8.0;

/// Number of rows in one window of [`skippable_rows`].
///
/// The Parquet decoder skips the rows that a row filter removes only when
/// the removed rows make long runs. Its default selection policy
/// (`RowSelectionPolicy::Auto { threshold: 32 }`) decodes all rows and then
/// filters them when the runs are shorter than 32 rows on average. A window
/// of 64 rows is one `u64` of the filter result.
pub(crate) const SKIP_WINDOW_ROWS: usize = 64;

/// Rows of `result` in windows of [`SKIP_WINDOW_ROWS`] rows where no row
/// passes (a `null` does not pass). These are the rows that a row filter
/// lets the decoder skip for the other columns. A partial window at the end
/// is counted if no row in it passes.
pub(crate) fn skippable_rows(result: &BooleanArray) -> usize {
    let values = result.values();
    match result.nulls() {
        Some(nulls) => skippable_in(&(values & nulls.inner())),
        None => skippable_in(values),
    }
}

/// Rows of `passed` in windows of [`SKIP_WINDOW_ROWS`] rows where no bit is
/// set, see [`skippable_rows`].
pub(crate) fn skippable_in(passed: &BooleanBuffer) -> usize {
    let chunks = passed.bit_chunks();
    let full = chunks.iter().filter(|chunk| *chunk == 0).count() * SKIP_WINDOW_ROWS;
    let remainder = if chunks.remainder_len() > 0 && chunks.remainder_bits() == 0 {
        chunks.remainder_len()
    } else {
        0
    };
    full + remainder
}

/// The measured rows in, rows out and evaluation time of one predicate.
/// Lock-free.
#[derive(Debug, Default)]
pub(crate) struct MeasuredCost {
    rows_in: AtomicU64,
    rows_out: AtomicU64,
    nanos: AtomicU64,
}

impl MeasuredCost {
    /// Records one evaluation on `rows_in` rows, of which `rows_out` passed,
    /// in `nanos` nanoseconds.
    pub(crate) fn record(&self, rows_in: usize, rows_out: usize, nanos: u64) {
        self.rows_in.fetch_add(rows_in as u64, Ordering::Relaxed);
        self.rows_out.fetch_add(rows_out as u64, Ordering::Relaxed);
        self.nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    /// The measured values. They are not read at the same instant.
    pub(crate) fn cost(&self) -> FilterCost {
        FilterCost {
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            nanos: self.nanos.load(Ordering::Relaxed),
        }
    }
}

/// The rank of a predicate in the evaluation order of its stage: the rows
/// that it removed for each nanosecond of evaluation time
/// ([`FilterCost::rows_removed_per_nano`], the ranking key of the adaptive
/// conjunct order of `FilterExec`). `None` before [`MIN_OBSERVED_ROWS`]
/// evaluated rows.
pub(crate) fn rank(cost: &FilterCost) -> Option<f64> {
    if cost.rows_in < MIN_OBSERVED_ROWS {
        return None;
    }
    cost.rows_removed_per_nano()
}

/// The evaluation order of predicates, as indexes into `costs`. The same
/// rule for required and optional predicates: a larger [`rank`] first. A
/// predicate without a rank comes first, in the given order, so that it is
/// measured on all rows of its stage. Thus before any measurement the order
/// is the given order.
pub(crate) fn evaluation_order(costs: &[FilterCost]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..costs.len()).collect();
    // A stable sort keeps the given order for equal keys.
    order.sort_by(|&a, &b| {
        let key = |i: usize| rank(&costs[i]).unwrap_or(f64::INFINITY);
        key(b).total_cmp(&key(a))
    });
    order
}

/// Hysteresis on the changes of the row filter (its predicates or their
/// order) of one file. Each change rebuilds the decoder, and the
/// measurements are noisy near a decision boundary: after the `n`-th change,
/// the next change waits for `2^(n-1) - 1` row group boundaries. A file
/// whose decision flips again and again changes at most
/// `log2(row groups) + 1` times.
#[derive(Debug, Default)]
pub(crate) struct ChangeHysteresis {
    /// Number of changes.
    changes: u32,
    /// Row group boundaries since the last change.
    boundaries_since_change: usize,
}

impl ChangeHysteresis {
    /// Call at each row group boundary. Returns true if a change is
    /// allowed at this boundary.
    pub(crate) fn boundary(&mut self) -> bool {
        self.boundaries_since_change += 1;
        let hold = 1usize
            .checked_shl(self.changes)
            .map_or(usize::MAX, |doubled| doubled / 2)
            .saturating_sub(1);
        self.boundaries_since_change > hold
    }

    /// Call when a change happened at this boundary.
    pub(crate) fn changed(&mut self) {
        self.changes += 1;
        self.boundaries_since_change = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn measured(rows_in: u64, rows_out: u64, nanos: u64) -> FilterCost {
        FilterCost {
            rows_in,
            rows_out,
            nanos,
        }
    }

    #[test]
    fn order_by_rows_removed_per_nanosecond() {
        let rows = MIN_OBSERVED_ROWS * 10;
        // Removes 90% at 1 ns for each row: 0.9 rows for each ns.
        let cheap_selective = measured(rows, rows / 10, rows);
        // Removes 95% at 16 ns for each row: about 0.06 rows for each ns.
        let expensive = measured(rows, rows / 20, 16 * rows);
        // Removes nothing at 1 ns for each row.
        let useless = measured(rows, rows, rows);
        let unmeasured = measured(MIN_OBSERVED_ROWS - 1, 0, 1);
        assert_eq!(rank(&unmeasured), None);
        assert_eq!(
            evaluation_order(&[expensive, useless, cheap_selective]),
            vec![2, 0, 1]
        );
        // Unmeasured predicates first, in the given order.
        assert_eq!(
            evaluation_order(&[expensive, unmeasured, cheap_selective, unmeasured]),
            vec![1, 3, 2, 0]
        );
        assert_eq!(evaluation_order(&[unmeasured; 3]), vec![0, 1, 2]);
    }

    #[test]
    fn hysteresis_doubles_the_hold() {
        let mut hysteresis = ChangeHysteresis::default();
        // Boundaries where a change is allowed, with a change at each of
        // them.
        let mut allowed = vec![];
        for boundary in 1..=16 {
            if hysteresis.boundary() {
                allowed.push(boundary);
                hysteresis.changed();
            }
        }
        // Holds of 0, 0, 1, 3 and 7 boundaries after the changes.
        assert_eq!(allowed, vec![1, 2, 4, 8, 16]);
    }

    #[test]
    fn measured_cost_accumulates() {
        let cost = MeasuredCost::default();
        cost.record(100, 10, 50);
        cost.record(100, 30, 150);
        assert_eq!(cost.cost(), measured(200, 40, 200));
    }

    fn bools(values: impl IntoIterator<Item = Option<bool>>) -> BooleanArray {
        values.into_iter().collect()
    }

    #[test]
    fn skippable_rows_counts_empty_windows() {
        // Window 0 (rows 0..64) has one passing row, window 1 has none, the
        // partial window (rows 128..160) has none.
        let mut values = vec![Some(false); 160];
        values[10] = Some(true);
        assert_eq!(skippable_rows(&bools(values.clone())), 64 + 32);

        // A null does not pass.
        values[70] = None;
        assert_eq!(skippable_rows(&bools(values.clone())), 64 + 32);

        // One passing row in the partial window.
        values[150] = Some(true);
        assert_eq!(skippable_rows(&bools(values)), 64);

        // Scattered passing rows: no window is empty.
        let scattered = (0..640).map(|i| Some(i % 50 == 0));
        assert_eq!(skippable_rows(&bools(scattered)), 0);
    }
}
