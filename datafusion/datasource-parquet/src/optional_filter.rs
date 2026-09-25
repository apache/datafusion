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

//! How the Parquet scan handles *optional filters* in its row filter.
//!
//! An optional filter is a conjunct of the scan predicate that is wrapped in
//! an [`OptionalFilterPhysicalExpr`]: the scan can skip it and the query
//! result stays the same (for example, a hash join dynamic filter, which the
//! join applies again). See [`OptionalFilterMode`] for the three modes.
//!
//! Statistics pruning (file, row group and page pruning) always uses the
//! complete predicate, thus optional filters. Only the row-level
//! `RowFilter` (used when `pushdown_filters` is true) changes with the mode.
//!
//! # The cost and the saving of a row filter predicate
//!
//! In [`OptionalFilterMode::Adaptive`], the [`OptionalFilterGate`] of an
//! optional filter pauses the filter when it costs more than the work that
//! the rows it removes save. The gate adds the configured minimum saving
//! (`datafusion.execution.optional_filter_min_saving_ns_per_row`, for the
//! work after the scan) to the saving that the scan measures, and the cost
//! of the row filter stage to the evaluation time:
//!
//! ```text
//! measured saving   = skippable rows / removed rows * ns_per_byte
//!                     * (compressed bytes for each row of the output
//!                        columns that the filter does not read)
//! measured overhead = ROW_FILTER_STAGE_NS_PER_ROW
//! ```
//!
//! The decoder does not decode the other output columns of a removed row
//! only when the removed rows make long runs: *skippable rows* are the
//! removed rows in windows of 64 rows where no row passes (see
//! [`skippable_rows`]). A filter that keeps 5% of the rows, spread over the
//! file, removes 95% of the rows, but the decoder still decodes all pages.
//! The gate of each filter counts its removed and skippable rows.
//!
//! The bytes for each row come from the metadata of the file (the average
//! over its row groups). `ns_per_byte` is the decode speed of the output
//! columns. It is measured over all files and partitions of the scan
//! ([`DecodeCost`]): the scan times each call that decodes an output batch
//! of a row group where the row filter removed no rows. Before the first
//! measurement, `ns_per_byte` is [`DEFAULT_DECODE_NS_PER_BYTE`].
//!
//! This is an estimate: the decode time of a row depends on the encoding,
//! not only on the compressed size. But it is cheap (a clock read and a few
//! atomic operations for each output batch, and a count of the empty 64-row
//! windows of each filter result), it adapts to the data and the hardware,
//! and it separates the important cases: a filter that reads all the output
//! columns (for example a hash join filter on the join keys) or removes
//! rows that are spread over the file saves only the work after the scan,
//! and a filter on one column of a wide table that removes long runs of
//! rows (for example a TopK filter with `SELECT *` on sorted data) saves the
//! decode of all the other columns.
//!
//! [`skippable_rows`]: crate::row_filter_cost::skippable_rows
//! [`OptionalFilterPhysicalExpr`]: datafusion_physical_expr::expressions::OptionalFilterPhysicalExpr
//! [`OptionalFilterGate`]: datafusion_physical_expr::optional_filter_gate::OptionalFilterGate

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use arrow::array::BooleanArray;

use datafusion_common::config::OptionalFilterMode;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::filter_stats::duration_nanos;
use datafusion_physical_expr::optional_filter_gate::{
    MeasuredRowSaving, OptionalFilterGateConfig, SharedGateVerdict,
};
use parking_lot::Mutex;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;

use crate::row_filter_cost::{ROW_FILTER_STAGE_NS_PER_ROW, skippable_rows};

/// The decode time for each compressed byte that the scan assumes before it
/// measures it (see the [module documentation](self)). The TPC-DS SF1 scans
/// on one core of a laptop take 4 to 8 ns for each compressed byte (scan
/// compute time divided by bytes scanned); this is the low end. The
/// measurement replaces it after the first output batch of the scan.
///
/// The value is important: the decoder evaluates the row filter of a whole
/// row group before it decodes the first output batch of that row group,
/// thus the gates of the first row group of a scan decide with this value.
pub(crate) const DEFAULT_DECODE_NS_PER_BYTE: f64 = 4.0;

/// The measured decode speed of the output columns of one scan, shared by
/// all partitions and files of the scan. See the [module
/// documentation](self).
///
/// Only the output batches of row groups where the row filter removed no
/// rows are measured. A row filter that removes rows spread over the row
/// group makes the decode of each output row much more expensive (the
/// decoder decodes all pages and then drops most rows): TPC-H Q9 measured
/// 21 to 28 ns for each byte with a filter that keeps 5% of the rows,
/// against 1 to 4 ns without a row filter.
#[derive(Debug, Default)]
pub(crate) struct DecodeCost {
    /// Time to decode output batches, in nanoseconds.
    nanos: AtomicU64,
    /// Sum, over the output batches, of the rows times the compressed bytes
    /// for each row of the output columns.
    bytes: AtomicU64,
}

impl DecodeCost {
    /// Adds the decode time of one output batch, and its estimated
    /// compressed size.
    pub(crate) fn record(&self, nanos: u64, bytes: u64) {
        self.nanos.fetch_add(nanos, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// The estimated compressed bytes of the measured output batches.
    #[cfg(test)]
    pub(crate) fn measured_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// The decode time for each compressed byte, or
    /// [`DEFAULT_DECODE_NS_PER_BYTE`] before the first measurement.
    pub(crate) fn ns_per_byte(&self) -> f64 {
        let bytes = self.bytes.load(Ordering::Relaxed);
        if bytes == 0 {
            return DEFAULT_DECODE_NS_PER_BYTE;
        }
        self.nanos.load(Ordering::Relaxed) as f64 / bytes as f64
    }
}

/// Average compressed bytes for each row of the leaf columns for which
/// `include` is true, over all row groups of the file. 0 if the file has no
/// rows.
pub(crate) fn compressed_bytes_per_row(
    metadata: &ParquetMetaData,
    include: impl Fn(usize) -> bool,
) -> f64 {
    let mut rows: i64 = 0;
    let mut bytes: i64 = 0;
    for row_group in metadata.row_groups() {
        rows += row_group.num_rows();
        bytes += row_group
            .columns()
            .iter()
            .enumerate()
            .filter(|(leaf, _)| include(*leaf))
            .map(|(_, column)| column.compressed_size())
            .sum::<i64>();
    }
    if rows <= 0 {
        0.0
    } else {
        bytes.max(0) as f64 / rows as f64
    }
}

/// The measured saving and overhead of the gate of one optional filter of
/// one file, see the [module documentation](self).
#[derive(Debug)]
pub(crate) struct OptionalFilterSaving {
    /// The values that the gate of the filter reads.
    measured: Arc<MeasuredRowSaving>,
    /// The decode speed of the scan.
    decode_cost: Arc<DecodeCost>,
    /// Compressed bytes for each row of the output columns that the filter
    /// does not read.
    pub(crate) unread_bytes_per_row: f64,
    /// Rows that the filter removed as a row filter predicate.
    removed_rows: AtomicU64,
    /// The removed rows that the decoder can skip, see
    /// [`skippable_rows`](crate::row_filter_cost::skippable_rows).
    skippable_rows: AtomicU64,
    /// True if the filter is a row filter predicate (the default). The
    /// adaptive filter placement can put the filter in the post-scan filter:
    /// then it saves no decode time and has no stage cost.
    row_filter: AtomicBool,
}

impl OptionalFilterSaving {
    /// A saving for a filter that does not read `unread_bytes_per_row`
    /// compressed bytes of the output columns. The decode speed comes from
    /// `decode_cost`.
    pub(crate) fn new(unread_bytes_per_row: f64, decode_cost: Arc<DecodeCost>) -> Self {
        let saving = Self {
            measured: Arc::new(MeasuredRowSaving::new()),
            decode_cost,
            unread_bytes_per_row,
            removed_rows: AtomicU64::new(0),
            skippable_rows: AtomicU64::new(0),
            row_filter: AtomicBool::new(true),
        };
        saving.update();
        saving
    }

    /// Sets if the filter is a row filter predicate (`true`) or not (in the
    /// post-scan filter, or not evaluated). Only a row filter predicate
    /// saves decode time and has a stage cost.
    pub(crate) fn set_row_filter(&self, row_filter: bool) {
        self.row_filter.store(row_filter, Ordering::Relaxed);
        self.update();
    }

    /// The values that the gate reads, see
    /// [`OptionalFilterGate::with_measured_saving`](datafusion_physical_expr::optional_filter_gate::OptionalFilterGate::with_measured_saving).
    pub(crate) fn measured(&self) -> &Arc<MeasuredRowSaving> {
        &self.measured
    }

    /// Records one evaluation of the filter as a row filter predicate, with
    /// one value in `result` for each evaluated row.
    pub(crate) fn record_evaluation(&self, result: &BooleanArray) {
        let rows_in = result.len();
        // `true_count` does not count nulls.
        let rows_out = result.true_count();
        if rows_out == rows_in {
            return;
        }
        let skippable = if rows_out == 0 {
            rows_in
        } else {
            skippable_rows(result)
        };
        self.removed_rows
            .fetch_add((rows_in - rows_out) as u64, Ordering::Relaxed);
        self.skippable_rows
            .fetch_add(skippable as u64, Ordering::Relaxed);
        self.update();
    }

    /// The fraction of the removed rows that the decoder can skip, 0 before
    /// the filter removed a row.
    fn skippable_fraction(&self) -> f64 {
        let removed = self.removed_rows.load(Ordering::Relaxed);
        if removed == 0 {
            return 0.0;
        }
        self.skippable_rows.load(Ordering::Relaxed) as f64 / removed as f64
    }

    /// Updates the measured saving and overhead with the placement, the
    /// current decode speed and the skippable fraction.
    fn update(&self) {
        if !self.row_filter.load(Ordering::Relaxed) {
            self.measured.set_ns_per_row(0.0);
            self.measured.set_overhead_ns_per_row(0.0);
            return;
        }
        self.measured.set_ns_per_row(
            self.skippable_fraction()
                * self.unread_bytes_per_row
                * self.decode_cost.ns_per_byte(),
        );
        self.measured
            .set_overhead_ns_per_row(ROW_FILTER_STAGE_NS_PER_ROW);
    }
}

/// Measures the decode time of the output batches of one file, and updates
/// the measured saving of the gated optional filters of the file. See the
/// [module documentation](self).
#[derive(Debug)]
pub(crate) struct OptionalFilterSavings {
    /// The decode speed of the scan.
    decode_cost: Arc<DecodeCost>,
    /// Compressed bytes for each row of the output columns of the file.
    output_bytes_per_row: f64,
    /// The saving of each gated optional filter of the file.
    filters: Vec<Arc<OptionalFilterSaving>>,
}

impl OptionalFilterSavings {
    /// Returns `None` if `filters` is empty: then nothing needs the
    /// measurement.
    pub(crate) fn try_new(
        decode_cost: Arc<DecodeCost>,
        metadata: &ParquetMetaData,
        output_projection: &ProjectionMask,
        filters: Vec<Arc<OptionalFilterSaving>>,
    ) -> Option<Self> {
        if filters.is_empty() {
            return None;
        }
        let output_bytes_per_row = compressed_bytes_per_row(metadata, |leaf| {
            output_projection.leaf_included(leaf)
        });
        Some(Self {
            decode_cost,
            output_bytes_per_row,
            filters,
        })
    }

    /// Records that the decoder produced an output batch of `rows` rows in
    /// `elapsed`, and updates the savings. Call only for the batches of row
    /// groups where the row filter removed no rows, see [`DecodeCost`].
    pub(crate) fn record_output_batch(&self, rows: usize, elapsed: Duration) {
        let bytes = rows as f64 * self.output_bytes_per_row;
        if bytes < 1.0 {
            return;
        }
        self.decode_cost
            .record(duration_nanos(elapsed), bytes as u64);
        for filter in &self.filters {
            filter.update();
        }
    }
}

/// The shared pauses of the gates of the optional filters of one scan, one
/// [`SharedGateVerdict`] for each optional filter, shared by all partitions
/// and files of the scan.
///
/// The scan rewrites its predicate for each file, thus a filter is
/// identified by its [`PhysicalExpr::expression_id`] (a
/// `DynamicFilterPhysicalExpr` has one, and the rewrites keep it). The
/// gates of a filter without an id do not share their pauses.
#[derive(Debug, Default)]
pub(crate) struct OptionalFilterSites {
    verdicts: Mutex<HashMap<u64, Arc<SharedGateVerdict>>>,
}

impl OptionalFilterSites {
    /// The shared verdict of the optional filter `filter` (the expression
    /// inside the `OptionalFilterPhysicalExpr`), or `None` if it has no
    /// expression id.
    pub(crate) fn verdict_for(
        &self,
        filter: &Arc<dyn PhysicalExpr>,
    ) -> Option<Arc<SharedGateVerdict>> {
        let id = filter.expression_id()?;
        Some(Arc::clone(self.verdicts.lock().entry(id).or_default()))
    }
}

/// The optional filter settings of one scan, see the [module
/// documentation](self).
///
/// Each file makes its own gates. The gates of one optional filter share
/// their pauses in all partitions and files of the scan
/// ([`OptionalFilterSites`]), and the decode speed ([`DecodeCost`]) is
/// shared by all partitions and files of the scan.
#[derive(Debug, Clone, Default)]
pub(crate) struct OptionalFilterOptions {
    /// How the row filter handles optional conjuncts.
    pub(crate) mode: OptionalFilterMode,
    /// Configuration of each gate in [`OptionalFilterMode::Adaptive`].
    pub(crate) gate_config: OptionalFilterGateConfig,
    /// The decode speed of the output columns of the scan.
    pub(crate) decode_cost: Arc<DecodeCost>,
    /// The shared pauses of the gates of each optional filter.
    pub(crate) sites: Arc<OptionalFilterSites>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_physical_expr::expressions::{DynamicFilterPhysicalExpr, lit};

    /// Only a row filter predicate saves decode time and has a stage cost.
    #[test]
    fn saving_depends_on_placement() {
        let saving = OptionalFilterSaving::new(10.0, Arc::default());
        // One window of 64 removed rows: all removed rows are skippable.
        saving.record_evaluation(&BooleanArray::from(vec![false; 64]));
        let row_filter = (
            DEFAULT_DECODE_NS_PER_BYTE * 10.0,
            ROW_FILTER_STAGE_NS_PER_ROW,
        );
        let measured = |s: &OptionalFilterSaving| {
            (
                s.measured().ns_per_row(),
                s.measured().overhead_ns_per_row(),
            )
        };
        assert_eq!(measured(&saving), row_filter);
        saving.set_row_filter(false);
        assert_eq!(measured(&saving), (0.0, 0.0));
        saving.set_row_filter(true);
        assert_eq!(measured(&saving), row_filter);
    }

    #[test]
    fn sites_share_verdicts_by_expression_id() {
        let sites = OptionalFilterSites::default();
        let dynamic: Arc<dyn PhysicalExpr> =
            Arc::new(DynamicFilterPhysicalExpr::new(vec![], lit(true)));
        let other: Arc<dyn PhysicalExpr> =
            Arc::new(DynamicFilterPhysicalExpr::new(vec![], lit(true)));
        let first = sites.verdict_for(&dynamic).unwrap();
        assert!(Arc::ptr_eq(&first, &sites.verdict_for(&dynamic).unwrap()));
        assert!(!Arc::ptr_eq(&first, &sites.verdict_for(&other).unwrap()));
        // A filter without an expression id has no shared verdict.
        assert!(sites.verdict_for(&lit(true)).is_none());
    }
}
