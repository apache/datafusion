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

use arrow::array::{Array, BooleanArray};
use arrow::buffer::BooleanBuffer;

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

#[cfg(test)]
mod tests {
    use super::*;

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
