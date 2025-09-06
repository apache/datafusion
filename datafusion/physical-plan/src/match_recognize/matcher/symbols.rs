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

//! Symbol evaluation helpers for `PatternMatcher`.
//!
//! Provides hot-path utilities to
//! 1. evaluate Boolean symbol predicate columns for a single physical row, and
//! 2. collect the numeric IDs of symbols that matched that row.
//!
//! Keeping this logic in a dedicated module isolates Arrow-specific code and
//! keeps `row_loop.rs` free from low-level column handling details.
//!
//! Performance considerations:
//! * **Zero allocations** – the caller supplies scratch buffers that are
//!   reused across rows.
//! * **Branch-free fast path** – virtual rows (before the first / after the
//!   last physical row) exit early to avoid touching column data.
//!
//! All routines in this file are expected to be _inlined_ into the caller.
use super::*;
use arrow::array::BooleanArray;
use datafusion_common::Result;

impl PatternMatcher {
    /// Helper that converts a `&[bool]` bitmap into a list of symbol IDs that
    /// evaluated to `true` on the current row.
    #[inline]
    pub(crate) fn collect_matched_symbol_ids(symbol_matches: &[bool]) -> Vec<usize> {
        let mut matched = Vec::with_capacity(8);
        for (id, &is_matched) in symbol_matches.iter().enumerate() {
            if is_matched {
                matched.push(id);
            }
        }
        matched
    }

    /// Populate `out` with symbol matches for `row_idx` (buffer reuse avoids allocs).
    pub(crate) fn evaluate_symbols_static(
        &self,
        bool_columns: &[&BooleanArray],
        row_idx: usize,
        num_rows: usize,
        out: &mut [bool],
    ) -> Result<()> {
        // Ensure the caller provided a correctly sized buffer.
        debug_assert_eq!(out.len(), self.compiled.id_to_symbol.len());

        // Reset all positions to false efficiently.
        out.fill(false);

        // Directly use the provided `row_idx` as it now refers to the physical row.
        if row_idx >= num_rows {
            // Virtual position after the last row – no symbols match.
            return Ok(());
        }

        let phys_row = row_idx;

        // Sub-metric timing
        let _symbol_timer_guard =
            self.metrics.as_ref().map(|m| m.symbol_eval_time.timer());

        for (sc, bool_col) in self.symbol_columns.iter().zip(bool_columns.iter()) {
            let matched = bool_col.value(phys_row);
            out[sc.id] = matched;
        }

        // Timer for predicate evaluation per call
        if let Some(m) = &self.metrics {
            if row_idx < num_rows {
                m.define_pred_evals.add(self.symbol_columns.len());
            }
        }

        Ok(())
    }
}
