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

//! The placement decision for one conjunct. Pure functions, see the
//! [module documentation](super) for the model.

use super::stats::Observation;

/// Where the scan evaluates one conjunct of its predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Placement {
    /// A predicate of the Parquet `RowFilter` (late materialization).
    RowFilter,
    /// A conjunct of the post-scan filter, on the decoded batches.
    PostScan,
    /// Not evaluated. Only for optional conjuncts.
    Skip,
}

/// Minimum number of evaluated rows before the measurements replace the
/// initial rule.
pub(crate) const MIN_OBSERVED_ROWS: u64 = 8192;

/// A conjunct that the statistics pruned in at least this fraction of the
/// row groups starts in the post-scan filter. See [`place_required`].
pub(crate) const CLUSTERED_PRUNED_FRACTION: f64 = 0.5;

/// A row filter conjunct moves to the post-scan filter only if its cost is
/// larger than this multiple of its benefit. The same margin as the
/// optional filter gate.
const LEAVE_ROW_FILTER_MARGIN: f64 = 1.1;

/// A post-scan conjunct moves to the row filter only if its cost is smaller
/// than this multiple of its benefit.
const ENTER_ROW_FILTER_MARGIN: f64 = 0.9;

/// What the decision for a required conjunct uses.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RequiredConjunctInputs {
    /// The pooled measurements of the conjunct.
    pub(crate) observation: Observation,
    /// Compressed bytes for each row of the output columns that the
    /// conjunct does not read. A row filter saves their decode for the rows
    /// that it removes.
    pub(crate) unread_output_bytes_per_row: f64,
    /// Measured decode time for each compressed byte.
    pub(crate) decode_ns_per_byte: f64,
    /// The extra fetch latency of a row filter stage, for each row of the
    /// next row group: the decoder fetches the columns of each row filter
    /// predicate before the other columns.
    pub(crate) fetch_ns_per_row: f64,
}

impl RequiredConjunctInputs {
    /// Decode time, in nanoseconds for each row, that a row filter saves.
    /// `None` without enough measurements.
    pub(crate) fn benefit_ns_per_row(&self) -> Option<f64> {
        if self.observation.rows_in < MIN_OBSERVED_ROWS {
            return None;
        }
        let skippable = self.observation.skippable_fraction()?;
        Some(skippable * self.unread_output_bytes_per_row * self.decode_ns_per_byte)
    }
}

/// The placement of a required conjunct: [`Placement::RowFilter`] or
/// [`Placement::PostScan`].
///
/// With enough measurements, the conjunct is a row filter if the decode
/// time that it saves is larger than the extra fetch latency:
///
/// ```text
/// benefit = skippable fraction * unread output bytes * decode ns for each byte
/// cost    = fetch latency / rows of the next row group
/// ```
///
/// The time to evaluate the conjunct is the same in both placements, thus
/// it is not in the model. `current` is the placement for the last row
/// group, `None` at the first decision. A change needs a 10% margin, so that
/// a conjunct does not move at each row group when the benefit and the cost
/// are almost equal.
///
/// Without enough measurements (the initial rule), the conjunct is a post-scan
/// filter if the row group statistics pruned it in at least
/// [`CLUSTERED_PRUNED_FRACTION`] of the row groups (the data is clustered on
/// its columns, thus the row groups that are left pass most rows), or if
/// the conjunct reads all output columns (a row filter cannot save decode
/// time). Otherwise it is a row filter, as without adaptive placement.
pub(crate) fn place_required(
    inputs: &RequiredConjunctInputs,
    current: Option<Placement>,
) -> Placement {
    let Some(benefit) = inputs.benefit_ns_per_row() else {
        if let Some(current) = current {
            return current;
        }
        let clustered = inputs
            .observation
            .pruned_fraction()
            .is_some_and(|pruned| pruned >= CLUSTERED_PRUNED_FRACTION);
        return if clustered || inputs.unread_output_bytes_per_row <= 0.0 {
            Placement::PostScan
        } else {
            Placement::RowFilter
        };
    };
    let cost = inputs.fetch_ns_per_row;
    let row_filter = match current {
        Some(Placement::RowFilter) => cost <= benefit * LEAVE_ROW_FILTER_MARGIN,
        _ => cost < benefit * ENTER_ROW_FILTER_MARGIN,
    };
    // A conjunct that saves nothing is never a row filter.
    if row_filter && benefit > 0.0 {
        Placement::RowFilter
    } else {
        Placement::PostScan
    }
}

/// The placement of an optional conjunct in the `adaptive` optional filter
/// mode: [`Placement::Skip`] while its gate is paused, otherwise
/// [`Placement::RowFilter`].
pub(crate) fn place_optional(gate_paused: bool) -> Placement {
    if gate_paused {
        Placement::Skip
    } else {
        Placement::RowFilter
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs(
        rows_in: u64,
        skippable_rows: u64,
        pruned: u64,
        kept: u64,
        unread_output_bytes_per_row: f64,
        fetch_ns_per_row: f64,
    ) -> RequiredConjunctInputs {
        RequiredConjunctInputs {
            observation: Observation {
                rows_in,
                rows_out: rows_in - skippable_rows,
                skippable_rows,
                row_groups_pruned: pruned,
                row_groups_kept: kept,
            },
            unread_output_bytes_per_row,
            decode_ns_per_byte: 4.0,
            fetch_ns_per_row,
        }
    }

    #[test]
    fn initial_rule() {
        // Output columns that the conjunct does not read: row filter.
        let wide = inputs(0, 0, 0, 0, 10.0, 0.0);
        assert_eq!(place_required(&wide, None), Placement::RowFilter);
        // The conjunct reads all output columns: post-scan.
        let narrow = inputs(0, 0, 0, 0, 0.0, 0.0);
        assert_eq!(place_required(&narrow, None), Placement::PostScan);
        // Statistics pruned most row groups: post-scan.
        let clustered = inputs(0, 0, 6, 4, 10.0, 0.0);
        assert_eq!(place_required(&clustered, None), Placement::PostScan);
        let not_clustered = inputs(0, 0, 4, 6, 10.0, 0.0);
        assert_eq!(place_required(&not_clustered, None), Placement::RowFilter);
        // Too few measured rows: the initial rule, and no change later.
        let few = inputs(MIN_OBSERVED_ROWS - 1, 0, 0, 0, 10.0, 0.0);
        assert_eq!(place_required(&few, None), Placement::RowFilter);
        assert_eq!(
            place_required(&few, Some(Placement::PostScan)),
            Placement::PostScan
        );
    }

    #[test]
    fn measured_benefit() {
        let rows = 100_000;
        // Half of the rows are in empty windows: a row filter saves decode
        // time, and the fetch latency is small.
        let clustered = inputs(rows, rows / 2, 0, 0, 10.0, 1.0);
        assert_eq!(place_required(&clustered, None), Placement::RowFilter);
        assert_eq!(
            place_required(&clustered, Some(Placement::PostScan)),
            Placement::RowFilter
        );
        // No empty windows (for example a uniform 50% filter): nothing to
        // save, thus post-scan. Also without any fetch latency.
        let scattered = inputs(rows, 0, 0, 0, 10.0, 0.0);
        assert_eq!(
            place_required(&scattered, Some(Placement::RowFilter)),
            Placement::PostScan
        );
        // The measurements replace the statistics prior.
        let clustered_by_stats = inputs(rows, rows / 2, 9, 1, 10.0, 0.0);
        assert_eq!(
            place_required(&clustered_by_stats, None),
            Placement::RowFilter
        );
        // A high fetch latency (a remote object store) is more than the
        // saving: benefit = 0.5 * 10 * 4 = 20 ns, cost = 30 ns.
        let remote = inputs(rows, rows / 2, 0, 0, 10.0, 30.0);
        assert_eq!(
            place_required(&remote, Some(Placement::RowFilter)),
            Placement::PostScan
        );
    }

    #[test]
    fn margin_prevents_flapping() {
        let rows = 100_000;
        // benefit = 0.5 * 10 * 4 = 20 ns; cost = 21 ns is inside the margin.
        let close = inputs(rows, rows / 2, 0, 0, 10.0, 21.0);
        assert_eq!(
            place_required(&close, Some(Placement::RowFilter)),
            Placement::RowFilter
        );
        assert_eq!(
            place_required(&close, Some(Placement::PostScan)),
            Placement::PostScan
        );
        // Outside the margin in both directions.
        let expensive = inputs(rows, rows / 2, 0, 0, 10.0, 23.0);
        assert_eq!(
            place_required(&expensive, Some(Placement::RowFilter)),
            Placement::PostScan
        );
        let cheap = inputs(rows, rows / 2, 0, 0, 10.0, 17.0);
        assert_eq!(
            place_required(&cheap, Some(Placement::PostScan)),
            Placement::RowFilter
        );
    }

    #[test]
    fn optional_follows_the_gate() {
        assert_eq!(place_optional(true), Placement::Skip);
        assert_eq!(place_optional(false), Placement::RowFilter);
    }
}
