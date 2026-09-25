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
//! | [`Placement::PostScan`] | The post-scan filter, on the decoded batches | Required and optional conjuncts |
//! | [`Placement::Skip`] | Not evaluated, and its columns are not decoded | Optional conjuncts in the `adaptive` optional filter mode, while the gate is paused |
//!
//! A row filter is not always better. It saves decode time only when it
//! removes long runs of rows, each row filter stage has a fixed cost for
//! each row, and the decoder fetches the columns of each row filter
//! predicate before the other columns (one more round trip for each row
//! group). Thus a conjunct starts in the post-scan filter and becomes a row
//! filter only on measured evidence. See [`model`] for the decision.
//!
//! Required and optional conjuncts use the same model and the same
//! measurements. The only difference: an optional conjunct (in the
//! `adaptive` optional filter mode) is skipped while its gate is paused.
//! In each stage (the row filter and the post-scan filter), the conjuncts
//! are evaluated in the order of their measured rows removed for each
//! nanosecond ([`model::evaluation_order`]), thus a cheap conjunct that
//! removes many rows runs before an expensive one, required or optional.
//!
//! [`FilePlacement`] decides at file open and again at each row group
//! boundary. When the decoder must change (its projection mask, or the
//! predicates or order of the `RowFilter`), the stream rebuilds it
//! (`ParquetPushDecoder::into_builder`). The rebuilt decoder keeps the row
//! selections of the remaining row groups (for example from the page
//! index). When only the post-scan filter changes, the stream rebuilds
//! only the post-scan filter. The measurements are pooled over all files
//! and partitions of the scan ([`PlacementSites`]).
//!
//! The measurements are noisy near a decision boundary, thus the changes of
//! one file have hysteresis: after the `n`-th change, the next change waits
//! for `2^(n-1) - 1` row group boundaries. A file whose decision flips again
//! and again changes its placement at most `log2(row groups) + 1` times.
//!
//! The placement of a file stops changing when a change would change the
//! schema of the decoded batches (possible with nested columns): the batch
//! coalescer holds batches with the current schema.
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
use datafusion_pruning::ConjunctPruningStats;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;

use crate::decoder_projection::PostScanConjunct;
use crate::optional_filter::{OptionalFilterSaving, compressed_bytes_per_row};
use crate::row_filter::{PrebuiltRowFilterCandidate, SharedOptionalFilterGate};

pub(crate) use model::Placement;
use model::{ConjunctInputs, evaluation_order, place_optional, place_required};
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

    /// Adds the row group statistics pruning result of each conjunct of the
    /// file predicate `predicate` to the pooled measurements (the statistics
    /// prior of the placement decision). `stats` has one entry for each
    /// conjunct of `split_conjunction(predicate)`.
    pub(crate) fn record_pruning(
        &self,
        predicate: &Arc<dyn PhysicalExpr>,
        stats: &[ConjunctPruningStats],
    ) {
        let conjuncts = split_conjunction(predicate);
        if conjuncts.len() != stats.len() {
            return;
        }
        for (position, stats) in stats.iter().enumerate() {
            self.sites
                .stats_for(&conjuncts, position, self.scan_conjunct_count)
                .record_pruning(*stats);
        }
    }
}

/// A conjunct whose placement [`FilePlacement`] decides.
#[derive(Debug)]
struct ManagedConjunct {
    /// Index of the row filter candidate of the conjunct.
    candidate: usize,
    /// The conjunct in terms of the file schema, for the post-scan filter
    /// (the inner expression of an optional conjunct).
    expr: Arc<dyn PhysicalExpr>,
    /// The pooled measurements of the conjunct.
    stats: Arc<ConjunctStats>,
    /// Compressed bytes for each row of the output columns that the
    /// conjunct does not read.
    unread_output_bytes_per_row: f64,
    /// Compressed bytes for each row of the output columns that the
    /// conjunct reads.
    read_output_bytes_per_row: f64,
    /// `Some` for an optional conjunct with a gate (the `adaptive` optional
    /// filter mode).
    optional: Option<OptionalConjunct>,
    placement: Placement,
}

/// The parts of a managed optional conjunct.
#[derive(Debug)]
struct OptionalConjunct {
    gate: SharedOptionalFilterGate,
    /// The measured saving of the gate, which depends on the placement.
    saving: Option<Arc<OptionalFilterSaving>>,
}

/// The placement and the evaluation order of all managed conjuncts of a
/// file, see [`FilePlacement::snapshot`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlacementSnapshot {
    placements: Vec<Placement>,
    order: Vec<usize>,
}

/// A change of the placement at a row group boundary, see
/// [`FilePlacement::decide`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlacementChange {
    /// True if the predicates of the `RowFilter` or their order changed.
    pub(crate) row_filter: bool,
}

/// The placement of the conjuncts of one file. See the [module
/// documentation](self).
#[derive(Debug)]
pub(crate) struct FilePlacement {
    conjuncts: Vec<ManagedConjunct>,
    /// The evaluation order of `conjuncts` (indexes), see
    /// [`model::evaluation_order`].
    order: Vec<usize>,
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
    /// Number of changes of the placement of this file.
    changes_made: u32,
    /// Row group boundaries since the last change.
    boundaries_since_change: usize,
    /// Number of row group boundaries where the placement changed.
    changes: Count,
}

impl FilePlacement {
    /// Returns `None` if no conjunct needs a placement decision.
    ///
    /// Adds the pooled measurements of each managed conjunct to its
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
            if candidate.position() >= file_conjuncts.len()
                || (candidate.gate().is_none()
                    && is_optional_filter(candidate.source_expr()))
            {
                // An optional conjunct in the `always` mode.
                continue;
            }
            let stats = options.sites.stats_for(
                &file_conjuncts,
                candidate.position(),
                options.scan_conjunct_count,
            );
            candidate.set_placement_stats(Arc::clone(&stats));
            let unread_output_bytes_per_row =
                compressed_bytes_per_row(metadata, |leaf| {
                    output_projection.leaf_included(leaf) && !candidate.reads_leaf(leaf)
                });
            let read_output_bytes_per_row = compressed_bytes_per_row(metadata, |leaf| {
                output_projection.leaf_included(leaf) && candidate.reads_leaf(leaf)
            });
            let optional = candidate.gate().map(|gate| OptionalConjunct {
                gate: Arc::clone(gate),
                saving: candidate.optional_saving().cloned(),
            });
            conjuncts.push(ManagedConjunct {
                candidate: index,
                expr: Arc::clone(candidate.source_expr()),
                stats,
                unread_output_bytes_per_row,
                read_output_bytes_per_row,
                optional,
                placement: Placement::RowFilter,
            });
        }
        if conjuncts.is_empty() {
            return None;
        }
        let mut placement = Self {
            order: (0..conjuncts.len()).collect(),
            conjuncts,
            sites: Arc::clone(&options.sites),
            metadata: Arc::clone(metadata),
            batch_size: batch_size.max(1),
            row_group_rows: first_row_group_rows,
            decoded_bytes_per_row: 0.0,
            frozen: false,
            changes_made: 0,
            boundaries_since_change: 0,
            changes,
        };
        placement.place_all(first_row_group_rows, true);
        Some(placement)
    }

    /// Makes the placement for the next row group, which has
    /// `next_row_group_rows` rows. Call at each row group boundary. Returns
    /// `Some` if the placement or the evaluation order changed: then rebuild
    /// the post-scan filter, and the decoder if its projection mask or its
    /// `RowFilter` changed. Too soon after a change, nothing changes (see
    /// the [module documentation](self)).
    pub(crate) fn decide(
        &mut self,
        next_row_group_rows: usize,
    ) -> Option<PlacementChange> {
        if self.frozen {
            return None;
        }
        // A skipped optional conjunct did not see the batches of the last
        // row group. Count them down on its gate, so that the pause can
        // end.
        for conjunct in &self.conjuncts {
            if let (Placement::Skip, Some(optional)) =
                (conjunct.placement, &conjunct.optional)
            {
                optional
                    .gate
                    .lock()
                    .skip_rows(self.row_group_rows, self.batch_size);
            }
        }
        self.row_group_rows = next_row_group_rows;
        self.boundaries_since_change += 1;
        if self.boundaries_since_change <= self.change_hold() {
            return None;
        }
        let row_filter = self.row_filter_candidates();
        let before = self.snapshot();
        self.place_all(next_row_group_rows, false);
        if self.snapshot() == before {
            return None;
        }
        self.changes.add(1);
        self.changes_made += 1;
        self.boundaries_since_change = 0;
        Some(PlacementChange {
            row_filter: self.row_filter_candidates() != row_filter,
        })
    }

    /// Row group boundaries to wait after the last change before the next
    /// change: `2^(n-1) - 1` after the `n`-th change.
    fn change_hold(&self) -> usize {
        1usize
            .checked_shl(self.changes_made)
            .map_or(usize::MAX, |doubled| doubled / 2)
            .saturating_sub(1)
    }

    /// Decides the placement of each conjunct and the evaluation order.
    fn place_all(&mut self, row_group_rows: usize, first: bool) {
        let decode_ns_per_byte = self.sites.decode().ns_per_byte();
        let fetch_ns_per_row =
            self.sites.fetch().mean_nanos() / row_group_rows.max(1) as f64;
        let mut observations = Vec::with_capacity(self.conjuncts.len());
        for conjunct in &mut self.conjuncts {
            let observation = conjunct.stats.observation();
            observations.push(observation);
            let inputs = ConjunctInputs {
                observation,
                unread_output_bytes_per_row: conjunct.unread_output_bytes_per_row,
                read_output_bytes_per_row: conjunct.read_output_bytes_per_row,
                decode_ns_per_byte,
                fetch_ns_per_row,
            };
            let current = (!first).then_some(conjunct.placement);
            conjunct.placement = match &conjunct.optional {
                None => place_required(&inputs, current),
                Some(optional) => {
                    let paused = optional.gate.lock().is_paused();
                    let placement = place_optional(&inputs, paused, current);
                    if let Some(saving) = &optional.saving {
                        saving.set_row_filter(placement == Placement::RowFilter);
                    }
                    placement
                }
            };
        }
        self.order = evaluation_order(&observations);
    }

    /// The placement and the evaluation order of the managed conjuncts.
    pub(crate) fn snapshot(&self) -> PlacementSnapshot {
        PlacementSnapshot {
            placements: self.conjuncts.iter().map(|c| c.placement).collect(),
            order: self.order.clone(),
        }
    }

    /// Sets the placement and the order to `snapshot` (from
    /// [`Self::snapshot`]) and stops all later changes. For a file where the
    /// scan cannot apply a change.
    pub(crate) fn restore_and_freeze(&mut self, snapshot: &PlacementSnapshot) {
        for (conjunct, placement) in self.conjuncts.iter_mut().zip(&snapshot.placements) {
            conjunct.placement = *placement;
        }
        self.order.clone_from(&snapshot.order);
        self.frozen = true;
    }

    /// True if the placement manages the candidate at `index`.
    pub(crate) fn manages(&self, index: usize) -> bool {
        self.conjuncts.iter().any(|c| c.candidate == index)
    }

    /// The candidates of the managed conjuncts that are predicates of the
    /// `RowFilter`, in evaluation order.
    pub(crate) fn row_filter_candidates(&self) -> Vec<usize> {
        self.in_order(Placement::RowFilter)
            .map(|conjunct| conjunct.candidate)
            .collect()
    }

    /// The managed conjuncts that the post-scan filter evaluates, in
    /// evaluation order. The optional conjuncts have their gate.
    pub(crate) fn post_scan_conjuncts(&self) -> Vec<PostScanConjunct> {
        self.in_order(Placement::PostScan)
            .map(|c| PostScanConjunct {
                expr: Arc::clone(&c.expr),
                stats: Some(Arc::clone(&c.stats)),
                gate: c
                    .optional
                    .as_ref()
                    .map(|optional| Arc::clone(&optional.gate)),
            })
            .collect()
    }

    /// The managed conjuncts with `placement`, in evaluation order.
    fn in_order(&self, placement: Placement) -> impl Iterator<Item = &ManagedConjunct> {
        self.order
            .iter()
            .map(|&i| &self.conjuncts[i])
            .filter(move |c| c.placement == placement)
    }

    /// Number of rows of the row group at `index` in the file.
    pub(crate) fn row_group_rows(&self, index: usize) -> usize {
        usize::try_from(self.metadata.row_group(index).num_rows()).unwrap_or(0)
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
