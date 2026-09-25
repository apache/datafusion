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

use datafusion_physical_expr::filter_stats::MIN_OBSERVED_ROWS;

use super::stats::Observation;
use crate::row_filter_cost;
use crate::row_filter_cost::ROW_FILTER_STAGE_NS_PER_ROW;

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

/// A row filter conjunct moves to the post-scan filter only if its cost is
/// larger than this multiple of its benefit. The same margin as the
/// optional filter gate.
const LEAVE_ROW_FILTER_MARGIN: f64 = 1.1;

/// A post-scan conjunct moves to the row filter only if its cost is smaller
/// than this multiple of its benefit.
const ENTER_ROW_FILTER_MARGIN: f64 = 0.9;

/// The measured evidence for the placement of a conjunct.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ConjunctInputs {
    /// The pooled measurements of the conjunct.
    pub(crate) observation: Observation,
    /// Compressed bytes for each row of the output columns that the
    /// conjunct does not read. A row filter saves their decode for the rows
    /// that it removes.
    pub(crate) unread_output_bytes_per_row: f64,
    /// Compressed bytes for each row of the output columns that the
    /// conjunct reads. A row filter decodes them for all rows to evaluate
    /// the conjunct, and again for the output of the rows that pass (or
    /// copies them from the predicate cache).
    pub(crate) read_output_bytes_per_row: f64,
    /// Measured decode time for each compressed byte.
    pub(crate) decode_ns_per_byte: f64,
    /// The extra fetch latency of a row filter stage, for each row of the
    /// next row group: the decoder fetches the columns of each row filter
    /// predicate before the other columns.
    pub(crate) fetch_ns_per_row: f64,
}

impl ConjunctInputs {
    /// Decode time, in nanoseconds for each evaluated row, that a row
    /// filter saves. `None` without enough measurements.
    pub(crate) fn benefit_ns_per_row(&self) -> Option<f64> {
        if self.observation.rows_in < MIN_OBSERVED_ROWS {
            return None;
        }
        let skippable = self.observation.skippable_fraction()?;
        Some(skippable * self.unread_output_bytes_per_row * self.decode_ns_per_byte)
    }

    /// The extra time, in nanoseconds for each evaluated row, of a row
    /// filter stage compared to the post-scan filter: the fixed stage cost,
    /// the fetch latency and the second decode of the output columns that
    /// the conjunct reads, for the rows that pass.
    pub(crate) fn cost_ns_per_row(&self) -> f64 {
        let pass_ratio = self.observation.pass_ratio().unwrap_or(1.0);
        ROW_FILTER_STAGE_NS_PER_ROW
            + self.fetch_ns_per_row
            + pass_ratio * self.read_output_bytes_per_row * self.decode_ns_per_byte
    }
}

/// The placement of a required conjunct: [`Placement::RowFilter`] or
/// [`Placement::PostScan`].
///
/// A conjunct starts in the post-scan filter. It moves to the row filter
/// only on measured evidence: after [`MIN_OBSERVED_ROWS`] evaluated rows
/// (in either placement), if the decode time that it saves is larger than
/// the extra cost of a row filter stage:
///
/// ```text
/// benefit = skippable rows / rows in * unread output bytes * decode ns for each byte
/// cost    = fixed stage cost
///         + fetch latency / rows of the next row group
///         + rows out / rows in * read output bytes * decode ns for each byte
/// ```
///
/// The time to evaluate the conjunct is the same in both placements, thus
/// it is not in the model. `current` is the placement for the last row
/// group, `None` at the first decision. A change needs a 10% margin, so that
/// a conjunct does not move at each row group when the benefit and the cost
/// are almost equal.
pub(crate) fn place_required(
    inputs: &ConjunctInputs,
    current: Option<Placement>,
) -> Placement {
    let Some(benefit) = inputs.benefit_ns_per_row() else {
        return current.unwrap_or(Placement::PostScan);
    };
    let cost = inputs.cost_ns_per_row();
    let row_filter = match current {
        Some(Placement::RowFilter) => cost <= benefit * LEAVE_ROW_FILTER_MARGIN,
        _ => cost < benefit * ENTER_ROW_FILTER_MARGIN,
    };
    if row_filter {
        Placement::RowFilter
    } else {
        Placement::PostScan
    }
}

/// The placement of an optional conjunct (in the `adaptive` optional
/// filter mode): [`Placement::Skip`] while its gate is paused, else the same
/// decision as for a required conjunct ([`place_required`]). A skipped
/// conjunct is not a row filter: it needs the evidence again to become one.
pub(crate) fn place_optional(
    inputs: &ConjunctInputs,
    gate_paused: bool,
    current: Option<Placement>,
) -> Placement {
    if gate_paused {
        return Placement::Skip;
    }
    let current = current.map(|current| match current {
        Placement::Skip => Placement::PostScan,
        other => other,
    });
    place_required(inputs, current)
}

/// The placement of a conjunct that has no [`rank`] yet, when the row
/// filter has other predicates: it is measured first. A conjunct that runs
/// after the row filter sees only the rows that the row filter lets pass,
/// thus it could never get the measurements for a better placement or
/// order (for example a TopK filter behind an expensive `LIKE` row filter,
/// which could remove most rows before the `LIKE`). As first predicate of
/// the row filter it is measured on all rows. This costs one row filter
/// stage until [`MIN_OBSERVED_ROWS`] rows are measured (pooled over the
/// scan), then the evidence decides.
pub(crate) fn explore(
    placement: Placement,
    observation: &Observation,
    row_filter_used: bool,
) -> Placement {
    if row_filter_used && placement == Placement::PostScan && rank(observation).is_none()
    {
        Placement::RowFilter
    } else {
        placement
    }
}

/// The rank of a conjunct in the evaluation order of its stage, see
/// [`row_filter_cost::rank`].
pub(crate) fn rank(observation: &Observation) -> Option<f64> {
    row_filter_cost::rank(&observation.cost())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Inputs with a decode speed of 4 ns for each byte and no output
    /// column that the conjunct reads.
    fn inputs(
        rows_in: u64,
        skippable_rows: u64,
        unread_output_bytes_per_row: f64,
        fetch_ns_per_row: f64,
    ) -> ConjunctInputs {
        ConjunctInputs {
            observation: Observation {
                rows_in,
                rows_out: rows_in - skippable_rows,
                skippable_rows,
                nanos: 0,
            },
            unread_output_bytes_per_row,
            read_output_bytes_per_row: 0.0,
            decode_ns_per_byte: 4.0,
            fetch_ns_per_row,
        }
    }

    #[test]
    fn starts_post_scan() {
        // Without measurements a conjunct is a post-scan filter, also with
        // wide output columns that it does not read.
        let wide = inputs(0, 0, 100.0, 0.0);
        assert_eq!(place_required(&wide, None), Placement::PostScan);
        let few = inputs(MIN_OBSERVED_ROWS - 1, MIN_OBSERVED_ROWS - 1, 100.0, 0.0);
        assert_eq!(place_required(&few, None), Placement::PostScan);
        // Too few measured rows: no change.
        assert_eq!(
            place_required(&few, Some(Placement::RowFilter)),
            Placement::RowFilter
        );
    }

    #[test]
    fn measured_benefit() {
        let rows = 100_000;
        // Half of the rows are skippable: benefit = 0.5 * 10 * 4 = 20 ns,
        // cost = 8 + 1 = 9 ns.
        let clustered = inputs(rows, rows / 2, 10.0, 1.0);
        assert_eq!(place_required(&clustered, None), Placement::RowFilter);
        assert_eq!(
            place_required(&clustered, Some(Placement::PostScan)),
            Placement::RowFilter
        );
        // No skippable rows (for example a uniform 50% filter): nothing to
        // save, thus post-scan. Also without any fetch latency.
        let scattered = inputs(rows, 0, 10.0, 0.0);
        assert_eq!(
            place_required(&scattered, Some(Placement::RowFilter)),
            Placement::PostScan
        );
        // The saving is smaller than the fixed stage cost: benefit =
        // 0.1 * 10 * 4 = 4 ns.
        let small = inputs(rows, rows / 10, 10.0, 0.0);
        assert_eq!(
            place_required(&small, Some(Placement::PostScan)),
            Placement::PostScan
        );
        // A high fetch latency (a remote object store) is more than the
        // saving: benefit = 20 ns, cost = 8 + 30 ns.
        let remote = inputs(rows, rows / 2, 10.0, 30.0);
        assert_eq!(
            place_required(&remote, Some(Placement::RowFilter)),
            Placement::PostScan
        );
    }

    #[test]
    fn double_decode_of_read_output_columns() {
        let rows = 100_000;
        // benefit = 0.5 * 10 * 4 = 20 ns. Without read output columns,
        // cost = 8 ns: row filter.
        let mut shared = inputs(rows, rows / 2, 10.0, 0.0);
        assert_eq!(place_required(&shared, None), Placement::RowFilter);
        // The conjunct also reads 4 bytes of output columns for each row:
        // the rows that pass (50%) decode them again, cost = 8 + 0.5 * 4 *
        // 4 = 16 ns, still less than 0.9 * 20.
        shared.read_output_bytes_per_row = 4.0;
        assert_eq!(place_required(&shared, None), Placement::RowFilter);
        // 8 bytes: cost = 8 + 16 = 24 ns.
        shared.read_output_bytes_per_row = 8.0;
        assert_eq!(place_required(&shared, None), Placement::PostScan);
    }

    #[test]
    fn margin_prevents_flapping() {
        let rows = 100_000;
        // benefit = 0.5 * 10 * 4 = 20 ns; cost = 8 + 13 = 21 ns is inside
        // the margin.
        let close = inputs(rows, rows / 2, 10.0, 13.0);
        assert_eq!(
            place_required(&close, Some(Placement::RowFilter)),
            Placement::RowFilter
        );
        assert_eq!(
            place_required(&close, Some(Placement::PostScan)),
            Placement::PostScan
        );
        // Outside the margin in both directions: cost = 23 ns and 17 ns.
        let expensive = inputs(rows, rows / 2, 10.0, 15.0);
        assert_eq!(
            place_required(&expensive, Some(Placement::RowFilter)),
            Placement::PostScan
        );
        let cheap = inputs(rows, rows / 2, 10.0, 9.0);
        assert_eq!(
            place_required(&cheap, Some(Placement::PostScan)),
            Placement::RowFilter
        );
    }

    #[test]
    fn optional_is_skipped_while_paused_else_placed_like_required() {
        let rows = 100_000;
        let clustered = inputs(rows, rows / 2, 10.0, 1.0);
        for current in [None, Some(Placement::RowFilter), Some(Placement::PostScan)] {
            assert_eq!(place_optional(&clustered, true, current), Placement::Skip);
        }
        // Not paused: the evidence decides. A skipped conjunct is not a row
        // filter, thus it moves to the row filter with the entry margin.
        assert_eq!(
            place_optional(&clustered, false, Some(Placement::Skip)),
            Placement::RowFilter
        );
        let unmeasured = inputs(0, 0, 100.0, 0.0);
        for current in [None, Some(Placement::Skip)] {
            assert_eq!(
                place_optional(&unmeasured, false, current),
                Placement::PostScan
            );
        }
        // No output column that the conjunct does not read: a row filter
        // saves no decode time.
        let reads_all_outputs = inputs(rows, rows / 2, 0.0, 0.0);
        assert_eq!(
            place_optional(&reads_all_outputs, false, Some(Placement::RowFilter)),
            Placement::PostScan
        );
    }

    fn measured(rows_in: u64, rows_out: u64, nanos: u64) -> Observation {
        Observation {
            rows_in,
            rows_out,
            nanos,
            ..Default::default()
        }
    }

    #[test]
    fn unmeasured_conjunct_explores_the_row_filter() {
        let unmeasured = measured(MIN_OBSERVED_ROWS - 1, 0, 1);
        let known = measured(MIN_OBSERVED_ROWS, 0, 1);
        // Only a post-scan conjunct without a rank, and only when the row
        // filter has other predicates.
        assert_eq!(
            explore(Placement::PostScan, &unmeasured, true),
            Placement::RowFilter
        );
        assert_eq!(
            explore(Placement::PostScan, &unmeasured, false),
            Placement::PostScan
        );
        assert_eq!(
            explore(Placement::PostScan, &known, true),
            Placement::PostScan
        );
        assert_eq!(explore(Placement::Skip, &unmeasured, true), Placement::Skip);
    }
}
