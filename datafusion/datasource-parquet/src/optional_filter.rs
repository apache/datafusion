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
//! # The saving of a removed row
//!
//! In [`OptionalFilterMode::Adaptive`], the [`OptionalFilterGate`] of an
//! optional filter pauses the filter when its evaluation costs more than the
//! work that the rows it removes save. The gate adds the configured minimum
//! saving (`datafusion.execution.optional_filter_min_saving_ns_per_row`, for
//! the work after the scan) to the saving that the scan measures: the time
//! to decode the output columns that the filter does not read, for each
//! row. The scan does not decode these columns for a removed row.
//!
//! The scan estimates this decode time from the compressed size of the
//! column chunks and a measured decode speed:
//!
//! ```text
//! measured saving = ns_per_byte * (compressed bytes for each row of the
//!                                  output columns that the filter does not read)
//! ns_per_byte     = time to decode output batches
//!                   / (output rows * compressed bytes for each row of the
//!                      output columns)
//! ```
//!
//! The bytes for each row come from the metadata of the file (the average
//! over its row groups). `ns_per_byte` is measured over all files and
//! partitions of the scan ([`DecodeCost`]): the scan times each call that
//! decodes an output batch. This time does not include the row filter,
//! because the Parquet decoder evaluates the row filter of a row group
//! before it decodes the output columns. Before the first measurement,
//! `ns_per_byte` is [`DEFAULT_DECODE_NS_PER_BYTE`].
//!
//! This is an estimate: the decode time of a row depends on the encoding and
//! on the rows that are selected (a sparse selection costs more for each
//! row), not only on the compressed size. But it is cheap (a clock read and
//! a few atomic operations for each output batch), it adapts to the data and
//! the hardware, and it separates the two important cases: a filter that
//! reads all the output columns (for example a hash join filter on the join
//! keys) saves only the work after the scan, and a filter on one column of
//! a wide table (for example a TopK filter with `SELECT *`) saves the decode
//! of all the other columns.
//!
//! [`OptionalFilterPhysicalExpr`]: datafusion_physical_expr::expressions::OptionalFilterPhysicalExpr
//! [`OptionalFilterGate`]: datafusion_physical_expr::optional_filter_gate::OptionalFilterGate

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use datafusion_common::config::OptionalFilterMode;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::filter_stats::duration_nanos;
use datafusion_physical_expr::optional_filter_gate::{
    MeasuredRowSaving, OptionalFilterGateConfig, OptionalFilterSiteStats,
};
use datafusion_physical_expr::utils::split_optional;
use parking_lot::Mutex;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;

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

/// The measured saving of one gated optional filter of one file, see the
/// [module documentation](self).
#[derive(Debug, Clone)]
pub(crate) struct OptionalFilterSaving {
    /// The value that the gate of the filter reads.
    pub(crate) saving: Arc<MeasuredRowSaving>,
    /// Compressed bytes for each row of the output columns that the filter
    /// does not read.
    pub(crate) unread_bytes_per_row: f64,
}

impl OptionalFilterSaving {
    /// A saving for a filter that does not read `unread_bytes_per_row`
    /// compressed bytes of the output columns, with the current decode
    /// speed of `decode_cost`.
    pub(crate) fn new(unread_bytes_per_row: f64, decode_cost: &DecodeCost) -> Self {
        let saving = Self {
            saving: Arc::new(MeasuredRowSaving::new()),
            unread_bytes_per_row,
        };
        saving.update(decode_cost.ns_per_byte());
        saving
    }

    fn update(&self, ns_per_byte: f64) {
        self.saving
            .set_ns_per_row(ns_per_byte * self.unread_bytes_per_row);
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
    filters: Vec<OptionalFilterSaving>,
}

impl OptionalFilterSavings {
    /// Returns `None` if `filters` is empty: then nothing needs the
    /// measurement.
    pub(crate) fn try_new(
        decode_cost: Arc<DecodeCost>,
        metadata: &ParquetMetaData,
        output_projection: &ProjectionMask,
        filters: Vec<OptionalFilterSaving>,
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
    /// `elapsed`, and updates the savings.
    pub(crate) fn record_output_batch(&self, rows: usize, elapsed: Duration) {
        let bytes = rows as f64 * self.output_bytes_per_row;
        if bytes < 1.0 {
            return;
        }
        self.decode_cost
            .record(duration_nanos(elapsed), bytes as u64);
        let ns_per_byte = self.decode_cost.ns_per_byte();
        for filter in &self.filters {
            filter.update(ns_per_byte);
        }
    }
}

/// Identifies one optional filter of one scan, to find its pooled
/// [`OptionalFilterSiteStats`].
///
/// The scan rewrites its predicate for each file (schema adaptation,
/// partition values, simplification). Thus the key must identify the same
/// optional filter in each rewritten predicate:
///
/// * A filter with a [`PhysicalExpr::expression_id`] (for example a
///   `DynamicFilterPhysicalExpr`) uses that id. The id is kept by
///   `with_new_children`, thus by all rewrites, and it is unique.
/// * Other filters use their position in the list of optional conjuncts
///   (see [`split_optional`]). A rewrite does not reorder the conjuncts, but
///   the simplifier can fold an optional conjunct to a literal (for example,
///   when a partition column makes it constant). Then the positions of the
///   next conjuncts change. Thus the position is used only when the rewritten
///   predicate has as many optional conjuncts as the scan predicate. If not,
///   the filter gets statistics that are not shared (see
///   [`OptionalFilterOptions::site_for`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum OptionalFilterSiteKey {
    ExpressionId(u64),
    Position(usize),
}

/// The pooled [`OptionalFilterSiteStats`] of all optional filters of one
/// scan, shared by all partitions and files of that scan.
///
/// `ParquetSource` holds this in an [`Arc`] and makes a new one each time its
/// predicate changes.
#[derive(Debug, Default)]
pub(crate) struct OptionalFilterSites {
    sites: Mutex<HashMap<OptionalFilterSiteKey, Arc<OptionalFilterSiteStats>>>,
    /// The decode speed of the output columns of the scan.
    decode_cost: Arc<DecodeCost>,
}

impl OptionalFilterSites {
    fn get(&self, key: OptionalFilterSiteKey) -> Arc<OptionalFilterSiteStats> {
        Arc::clone(self.sites.lock().entry(key).or_default())
    }

    /// The decode speed of the output columns of the scan.
    pub(crate) fn decode_cost(&self) -> &Arc<DecodeCost> {
        &self.decode_cost
    }

    /// Number of sites that have statistics.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.sites.lock().len()
    }
}

/// The optional filter settings of one scan, see the [module
/// documentation](self).
#[derive(Debug, Clone)]
pub(crate) struct OptionalFilterOptions {
    /// How the row filter handles optional conjuncts.
    pub(crate) mode: OptionalFilterMode,
    /// Configuration of each gate in [`OptionalFilterMode::Adaptive`].
    pub(crate) gate_config: OptionalFilterGateConfig,
    /// Pooled statistics, shared by all partitions and files of the scan.
    pub(crate) sites: Arc<OptionalFilterSites>,
    /// Number of optional conjuncts in the scan predicate (before the
    /// rewrite for each file).
    pub(crate) scan_optional_count: usize,
}

impl Default for OptionalFilterOptions {
    fn default() -> Self {
        Self {
            mode: OptionalFilterMode::Always,
            gate_config: OptionalFilterGateConfig::default(),
            sites: Arc::default(),
            scan_optional_count: 0,
        }
    }
}

impl OptionalFilterOptions {
    pub(crate) fn new(
        mode: OptionalFilterMode,
        gate_config: OptionalFilterGateConfig,
        sites: Arc<OptionalFilterSites>,
        predicate: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let scan_optional_count =
            predicate.map_or(0, |predicate| split_optional(predicate).1.len());
        Self {
            mode,
            gate_config,
            sites,
            scan_optional_count,
        }
    }

    /// Returns the statistics for an optional conjunct of a file predicate.
    ///
    /// `inner` is the inner expression of the conjunct, `position` its
    /// position in the list of optional conjuncts of the file predicate, and
    /// `file_optional_count` the length of that list.
    pub(crate) fn site_for(
        &self,
        inner: &Arc<dyn PhysicalExpr>,
        position: usize,
        file_optional_count: usize,
    ) -> Arc<OptionalFilterSiteStats> {
        if let Some(id) = inner.expression_id() {
            return self.sites.get(OptionalFilterSiteKey::ExpressionId(id));
        }
        if file_optional_count == self.scan_optional_count {
            return self.sites.get(OptionalFilterSiteKey::Position(position));
        }
        // The rewrite removed an optional conjunct, thus the position does
        // not identify the filter. Do not share the statistics.
        Arc::new(OptionalFilterSiteStats::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr::expressions::{
        DynamicFilterPhysicalExpr, OptionalFilterPhysicalExpr, col, lit,
    };

    fn options(predicate: &Arc<dyn PhysicalExpr>) -> OptionalFilterOptions {
        OptionalFilterOptions::new(
            OptionalFilterMode::Adaptive,
            OptionalFilterGateConfig::default(),
            Arc::default(),
            Some(predicate),
        )
    }

    fn optional(inner: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(OptionalFilterPhysicalExpr::new(inner))
    }

    #[test]
    fn site_keys() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]);
        let a = col("a", &schema).unwrap();
        let b = col("b", &schema).unwrap();
        let dynamic: Arc<dyn PhysicalExpr> = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&a)],
            lit(true),
        ));
        let predicate = datafusion_physical_expr::conjunction([
            optional(Arc::clone(&a)),
            optional(Arc::clone(&b)),
            optional(Arc::clone(&dynamic)),
        ]);
        let options = options(&predicate);
        assert_eq!(options.scan_optional_count, 3);

        // Same position and count: same site.
        let s1 = options.site_for(&a, 0, 3);
        let s2 = options.site_for(&a, 0, 3);
        assert!(Arc::ptr_eq(&s1, &s2));
        // Other position: other site.
        let s3 = options.site_for(&b, 1, 3);
        assert!(!Arc::ptr_eq(&s1, &s3));
        // A count that is different: the site is not shared.
        let s4 = options.site_for(&a, 0, 2);
        assert!(!Arc::ptr_eq(&s1, &s4));
        // A dynamic filter uses its expression id, at each position.
        let d1 = options.site_for(&dynamic, 2, 3);
        let d2 = options.site_for(&dynamic, 0, 1);
        assert!(Arc::ptr_eq(&d1, &d2));
        assert_eq!(options.sites.len(), 3);
    }
}
