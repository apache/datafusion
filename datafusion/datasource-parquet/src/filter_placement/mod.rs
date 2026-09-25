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

//! Adaptive placement of the filter conjuncts of a Parquet scan
//! (`datafusion.execution.adaptive_filter_placement`).
//!
//! With `pushdown_filters = true`, the scan can evaluate each conjunct of
//! its predicate in one of these places:
//!
//! | Placement | Where | For |
//! |---|---|---|
//! | [`Placement::RowFilter`] | A predicate of the Parquet `RowFilter`: the decoder skips the other columns of the rows that it removes | Required and optional conjuncts |
//! | [`Placement::PostScan`] | The post-scan filter, on the decoded batches | Required conjuncts |
//! | [`Placement::Skip`] | Not evaluated, and its columns are not decoded | Optional conjuncts in the `adaptive` optional filter mode, while the gate is paused |
//!
//! A row filter is not always better. It saves decode time only when it
//! removes long runs of rows, each row filter stage has a fixed cost for
//! each row, and the decoder fetches the columns of each row filter
//! predicate before the other columns (one more round trip for each row
//! group). Thus a required conjunct starts in the post-scan filter and
//! becomes a row filter only on measured evidence. See [`model`] for the
//! decision.
//!
//! [`FilePlacement`] decides at file open and again at each row group
//! boundary. The stream then rebuilds the decoder
//! (`ParquetPushDecoder::into_builder`) with the new `RowFilter` and
//! projection mask, and rebuilds the post-scan filter. The rebuilt decoder
//! keeps the row selections of the remaining row groups (for example from
//! the page index). The measurements are pooled over all files and
//! partitions of the scan ([`PlacementSites`]).
//!
//! The placement of a file stops changing only when a change would change
//! the schema of the decoded batches (possible with nested columns): the
//! batch coalescer holds batches with the current schema.
//!
//! Conjuncts that `build_row_filter` rejects for a file always run in the
//! post-scan filter (required) or are not used (optional). Optional
//! conjuncts in the `always` mode are always row filters.

mod model;
mod stats;

use std::sync::Arc;
use std::time::Duration;

use datafusion_physical_expr::utils::is_optional_filter;
use datafusion_physical_expr::{PhysicalExpr, split_conjunction};
use datafusion_physical_expr_common::metrics::Count;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;

use crate::decoder_projection::PostScanConjunct;
use crate::optional_filter::compressed_bytes_per_row;
use crate::row_filter::{PrebuiltRowFilterCandidate, SharedOptionalFilterGate};

pub(crate) use model::Placement;
use model::{RequiredConjunctInputs, place_optional, place_required};
pub(crate) use stats::{ConjunctStats, PlacementSites, StageSelection};

/// The adaptive placement settings of one scan.
#[derive(Debug, Clone, Default)]
pub(crate) struct PlacementOptions {
    /// `datafusion.execution.adaptive_filter_placement`. Only used when
    /// `pushdown_filters` is true.
    pub(crate) enabled: bool,
    /// Pooled measurements, shared by all partitions and files of the scan.
    pub(crate) sites: Arc<PlacementSites>,
    /// Number of conjuncts in the root `AND` chain of the scan predicate
    /// (before the rewrite for each file).
    pub(crate) scan_conjunct_count: usize,
}

impl PlacementOptions {
    pub(crate) fn new(
        enabled: bool,
        sites: Arc<PlacementSites>,
        predicate: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let scan_conjunct_count =
            predicate.map_or(0, |predicate| split_conjunction(predicate).len());
        Self {
            enabled,
            sites,
            scan_conjunct_count,
        }
    }
}

/// A conjunct whose placement [`FilePlacement`] decides.
#[derive(Debug)]
struct ManagedConjunct {
    /// Index of the row filter candidate of the conjunct.
    candidate: usize,
    kind: ConjunctKind,
    placement: Placement,
}

#[derive(Debug)]
enum ConjunctKind {
    Required {
        /// The conjunct in terms of the file schema, for the post-scan
        /// filter.
        expr: Arc<dyn PhysicalExpr>,
        stats: Arc<ConjunctStats>,
        /// Compressed bytes for each row of the output columns that the
        /// conjunct does not read.
        unread_output_bytes_per_row: f64,
        /// Compressed bytes for each row of the output columns that the
        /// conjunct reads.
        read_output_bytes_per_row: f64,
    },
    /// An optional conjunct with a gate (the `adaptive` optional filter
    /// mode).
    Optional { gate: SharedOptionalFilterGate },
}

/// The placement of the conjuncts of one file. See the [module
/// documentation](self).
#[derive(Debug)]
pub(crate) struct FilePlacement {
    conjuncts: Vec<ManagedConjunct>,
    sites: Arc<PlacementSites>,
    metadata: Arc<ParquetMetaData>,
    batch_size: usize,
    /// Rows of the row group that the current placement is for.
    row_group_rows: usize,
    /// Compressed bytes for each row of the columns that the decoder
    /// produces with the current projection mask.
    decoded_bytes_per_row: f64,
    /// When true, the placement does not change any more.
    frozen: bool,
    /// Number of row group boundaries where the placement changed.
    changes: Count,
}

impl FilePlacement {
    /// Returns `None` if no conjunct needs a placement decision.
    ///
    /// Adds the pooled measurements of each required conjunct to its
    /// candidate, so that the row filter records its evaluations. Makes the
    /// placement for the first row group, which has `first_row_group_rows`
    /// rows.
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        candidates: &mut [PrebuiltRowFilterCandidate],
        predicate: &Arc<dyn PhysicalExpr>,
        options: &PlacementOptions,
        output_projection: &ProjectionMask,
        metadata: &Arc<ParquetMetaData>,
        batch_size: usize,
        first_row_group_rows: usize,
        changes: Count,
    ) -> Option<Self> {
        if !options.enabled {
            return None;
        }
        let file_conjuncts = split_conjunction(predicate);
        let mut conjuncts = vec![];
        for (index, candidate) in candidates.iter_mut().enumerate() {
            let kind = if let Some(gate) = candidate.gate() {
                ConjunctKind::Optional {
                    gate: Arc::clone(gate),
                }
            } else if is_optional_filter(candidate.source_expr())
                || candidate.position() >= file_conjuncts.len()
            {
                // An optional conjunct in the `always` mode.
                continue;
            } else {
                let stats = options.sites.stats_for(
                    &file_conjuncts,
                    candidate.position(),
                    options.scan_conjunct_count,
                );
                candidate.set_placement_stats(Arc::clone(&stats));
                let unread_output_bytes_per_row =
                    compressed_bytes_per_row(metadata, |leaf| {
                        output_projection.leaf_included(leaf)
                            && !candidate.reads_leaf(leaf)
                    });
                let read_output_bytes_per_row =
                    compressed_bytes_per_row(metadata, |leaf| {
                        output_projection.leaf_included(leaf)
                            && candidate.reads_leaf(leaf)
                    });
                ConjunctKind::Required {
                    expr: Arc::clone(candidate.source_expr()),
                    stats,
                    unread_output_bytes_per_row,
                    read_output_bytes_per_row,
                }
            };
            conjuncts.push(ManagedConjunct {
                candidate: index,
                kind,
                placement: Placement::RowFilter,
            });
        }
        if conjuncts.is_empty() {
            return None;
        }
        let mut placement = Self {
            conjuncts,
            sites: Arc::clone(&options.sites),
            metadata: Arc::clone(metadata),
            batch_size: batch_size.max(1),
            row_group_rows: first_row_group_rows,
            decoded_bytes_per_row: 0.0,
            frozen: false,
            changes,
        };
        placement.place_all(first_row_group_rows, true);
        Some(placement)
    }

    /// Makes the placement for the next row group, which has
    /// `next_row_group_rows` rows. Call at each row group boundary. Returns
    /// true if the placement changed: then rebuild the `RowFilter` and the
    /// decoder projection.
    pub(crate) fn decide(&mut self, next_row_group_rows: usize) -> bool {
        if self.frozen {
            return false;
        }
        // A skipped optional conjunct did not see the batches of the last
        // row group. Count them down on its gate, so that the pause can
        // end.
        let batches = self.row_group_rows.div_ceil(self.batch_size);
        for conjunct in &self.conjuncts {
            if let (Placement::Skip, ConjunctKind::Optional { gate }) =
                (conjunct.placement, &conjunct.kind)
            {
                gate.lock().skip_batches(batches, self.batch_size);
            }
        }
        self.row_group_rows = next_row_group_rows;
        let changed = self.place_all(next_row_group_rows, false);
        if changed {
            self.changes.add(1);
        }
        changed
    }

    /// Decides the placement of each conjunct. Returns true if a placement
    /// changed.
    fn place_all(&mut self, row_group_rows: usize, first: bool) -> bool {
        let decode_ns_per_byte = self.sites.decode().ns_per_byte();
        let fetch_ns_per_row =
            self.sites.fetch().mean_nanos() / row_group_rows.max(1) as f64;
        let mut changed = false;
        for conjunct in &mut self.conjuncts {
            let placement = match &conjunct.kind {
                ConjunctKind::Required {
                    stats,
                    unread_output_bytes_per_row,
                    read_output_bytes_per_row,
                    ..
                } => place_required(
                    &RequiredConjunctInputs {
                        observation: stats.observation(),
                        unread_output_bytes_per_row: *unread_output_bytes_per_row,
                        read_output_bytes_per_row: *read_output_bytes_per_row,
                        decode_ns_per_byte,
                        fetch_ns_per_row,
                    },
                    (!first).then_some(conjunct.placement),
                ),
                ConjunctKind::Optional { gate } => {
                    place_optional(gate.lock().is_paused())
                }
            };
            changed |= placement != conjunct.placement;
            conjunct.placement = placement;
        }
        changed
    }

    /// Number of rows of the row group at `index` in the file.
    pub(crate) fn row_group_rows(&self, index: usize) -> usize {
        usize::try_from(self.metadata.row_group(index).num_rows()).unwrap_or(0)
    }

    /// The current placement of each managed conjunct.
    pub(crate) fn placements(&self) -> Vec<Placement> {
        self.conjuncts.iter().map(|c| c.placement).collect()
    }

    /// Sets the placement of each managed conjunct to `placements` (from
    /// [`Self::placements`]) and stops all later changes. For a file where
    /// the scan cannot apply a change.
    pub(crate) fn restore_and_freeze(&mut self, placements: &[Placement]) {
        for (conjunct, placement) in self.conjuncts.iter_mut().zip(placements) {
            conjunct.placement = *placement;
        }
        self.frozen = true;
    }

    /// True if the candidate at `index` is a predicate of the `RowFilter`.
    pub(crate) fn in_row_filter(&self, index: usize) -> bool {
        self.conjuncts
            .iter()
            .find(|c| c.candidate == index)
            .is_none_or(|c| c.placement == Placement::RowFilter)
    }

    /// The managed conjuncts that the post-scan filter evaluates.
    pub(crate) fn post_scan_conjuncts(&self) -> Vec<PostScanConjunct> {
        self.conjuncts
            .iter()
            .filter(|c| c.placement == Placement::PostScan)
            .filter_map(|c| match &c.kind {
                ConjunctKind::Required { expr, stats, .. } => Some(PostScanConjunct {
                    expr: Arc::clone(expr),
                    stats: Some(Arc::clone(stats)),
                }),
                ConjunctKind::Optional { .. } => None,
            })
            .collect()
    }

    /// Sets the projection mask of the decoder, for the decode time
    /// measurement.
    pub(crate) fn set_decoder_mask(&mut self, mask: &ProjectionMask) {
        self.decoded_bytes_per_row =
            compressed_bytes_per_row(&self.metadata, |leaf| mask.leaf_included(leaf));
    }

    /// Records that the decoder produced a batch of `rows` rows in
    /// `elapsed`.
    pub(crate) fn record_decode(&self, rows: usize, elapsed: Duration) {
        let bytes = rows as f64 * self.decoded_bytes_per_row;
        if bytes >= 1.0 {
            self.sites.decode().record(elapsed, bytes as u64);
        }
    }

    /// Records one fetch of the scan that took `elapsed`.
    pub(crate) fn record_fetch(&self, elapsed: Duration) {
        self.sites.fetch().record(elapsed);
    }
}
